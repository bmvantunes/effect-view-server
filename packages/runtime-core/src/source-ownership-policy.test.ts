import { describe, expect, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  kafka,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { grpcSourceMarkers } from "@effect-view-server/config/internal";
import { Effect, Schema } from "effect";
import { makeSourceOwnershipPolicy } from "./source-ownership-policy";

const Row = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
  region: Schema.String,
  status: Schema.String,
});

const sourceOwnedMutationError = (topic: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "UnsupportedQuery",
  topic,
  message:
    "Source-owned topics do not support direct runtime mutations; publish through the configured Kafka/gRPC source or use an externally-published topic.",
});

const sourceOwnedResetError: ViewServerRuntimeError = {
  _tag: "ViewServerRuntimeError",
  code: "UnsupportedQuery",
  message:
    "Source-owned topics do not support direct runtime reset; close the runtime or reset source-free topics through their owner.",
};

const runtimeCoreLeasedAccessError = (topic: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "UnsupportedQuery",
  topic,
  message:
    "Leased gRPC topics do not support direct runtime mutations, one-shot snapshots, or runtime-core subscriptions; use the runtime gRPC lease manager so it owns lease lifecycle.",
});

const managedRuntimeLeasedAccessError = (topic: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "UnsupportedQuery",
  topic,
  message:
    "Leased gRPC topics do not support direct runtime mutations or one-shot snapshots; use a live subscription so the runtime can own lease lifecycle.",
});

const managedRuntimeLeasedResetError: ViewServerRuntimeError = {
  _tag: "ViewServerRuntimeError",
  code: "UnsupportedQuery",
  message:
    "Leased gRPC topics do not support direct runtime reset; close the runtime or leased subscriptions so the lease manager owns cleanup.",
};

const sourceFreeViewServer = defineViewServerConfig({
  topics: {
    externalOrders: {
      schema: Row,
      key: "id",
    },
  },
});

const sourceOwnedViewServer = defineViewServerConfig({
  kafka: {
    usa: "localhost:9092",
  },
  topics: {
    externalOrders: {
      schema: Row,
      key: "id",
    },
    kafkaOrders: {
      schema: Row,
      key: "id",
      kafkaSource: kafka.source({
        topic: "orders-source",
        regions: ["usa"],
        value: kafka.json(Row),
        key: kafka.stringKey(),
        rowKey: ({ key }) => key,
        map: ({ value }) => ({
          price: value.price,
          region: value.region,
          status: value.status,
        }),
      }),
    },
    leasedOrders: {
      schema: Row,
      key: "id",
      grpcSource: grpcSourceMarkers.leased({
        routeBy: ["region"],
      }),
    },
    materializedOrders: {
      schema: Row,
      key: "id",
      grpcSource: grpcSourceMarkers.materialized(),
    },
  },
});

describe("SourceOwnershipPolicy", () => {
  it("classifies source-owned and leased topics behind one Interface", () => {
    const policy = makeSourceOwnershipPolicy(sourceOwnedViewServer);

    expect([...policy.sourceOwnedTopics]).toStrictEqual([
      "kafkaOrders",
      "leasedOrders",
      "materializedOrders",
    ]);
    expect([...policy.grpcLeasedTopics]).toStrictEqual(["leasedOrders"]);
    expect(policy.hasSourceOwnedTopics).toBe(true);
    expect(policy.isSourceOwnedTopic("externalOrders")).toBe(false);
    expect(policy.isSourceOwnedTopic("kafkaOrders")).toBe(true);
    expect(policy.isSourceOwnedTopic("materializedOrders")).toBe(true);
    expect(policy.isGrpcLeasedTopic("leasedOrders")).toBe(true);
    expect(policy.isGrpcLeasedTopic("kafkaOrders")).toBe(false);
  });

  it.effect("allows direct public mutations, reads, and reset for source-free topics", () =>
    Effect.gen(function* () {
      const policy = makeSourceOwnershipPolicy(sourceFreeViewServer);

      yield* policy.requirePublicMutationAllowed("externalOrders", "runtimeCore");
      yield* policy.requirePublicReadAllowed("externalOrders", "runtimeCore");
      yield* policy.requirePublicResetAllowed("runtimeCore");

      expect([...policy.sourceOwnedTopics]).toStrictEqual([]);
      expect([...policy.grpcLeasedTopics]).toStrictEqual([]);
      expect(policy.hasSourceOwnedTopics).toBe(false);
    }),
  );

  it.effect(
    "rejects source-owned runtime-core mutations and reset while keeping reads allowed",
    () =>
      Effect.gen(function* () {
        const policy = makeSourceOwnershipPolicy(sourceOwnedViewServer);

        yield* policy.requirePublicReadAllowed("kafkaOrders", "runtimeCore");
        yield* policy.requirePublicReadAllowed("materializedOrders", "runtimeCore");

        const kafkaMutationError = yield* policy
          .requirePublicMutationAllowed("kafkaOrders", "runtimeCore")
          .pipe(Effect.flip);
        const materializedMutationError = yield* policy
          .requirePublicMutationAllowed("materializedOrders", "runtimeCore")
          .pipe(Effect.flip);
        const leasedMutationError = yield* policy
          .requirePublicMutationAllowed("leasedOrders", "runtimeCore")
          .pipe(Effect.flip);
        const resetError = yield* policy.requirePublicResetAllowed("runtimeCore").pipe(Effect.flip);

        expect(kafkaMutationError).toStrictEqual(sourceOwnedMutationError("kafkaOrders"));
        expect(materializedMutationError).toStrictEqual(
          sourceOwnedMutationError("materializedOrders"),
        );
        expect(leasedMutationError).toStrictEqual(sourceOwnedMutationError("leasedOrders"));
        expect(resetError).toStrictEqual(sourceOwnedResetError);
      }),
  );

  it.effect("rejects leased reads and managed-runtime mutations with leased lifecycle errors", () =>
    Effect.gen(function* () {
      const policy = makeSourceOwnershipPolicy(sourceOwnedViewServer);

      const runtimeCoreReadError = yield* policy
        .requirePublicReadAllowed("leasedOrders", "runtimeCore")
        .pipe(Effect.flip);
      const managedReadError = yield* policy
        .requirePublicReadAllowed("leasedOrders", "managedRuntime")
        .pipe(Effect.flip);
      const managedMutationError = yield* policy
        .requirePublicMutationAllowed("leasedOrders", "managedRuntime")
        .pipe(Effect.flip);
      const managedResetError = yield* policy
        .requirePublicResetAllowed("managedRuntime")
        .pipe(Effect.flip);

      expect(runtimeCoreReadError).toStrictEqual(runtimeCoreLeasedAccessError("leasedOrders"));
      expect(managedReadError).toStrictEqual(managedRuntimeLeasedAccessError("leasedOrders"));
      expect(managedMutationError).toStrictEqual(managedRuntimeLeasedAccessError("leasedOrders"));
      expect(managedResetError).toStrictEqual(managedRuntimeLeasedResetError);
    }),
  );
});
