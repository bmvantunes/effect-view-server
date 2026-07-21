import { describe, expect, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  kafka,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { grpcSourceMarkers } from "@effect-view-server/config/internal";
import { Effect, Schema } from "effect";
import {
  collectSourceOwnershipConflicts,
  makeSourceOwnershipPolicy,
} from "./source-ownership-policy";

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
        value: kafka.json(() => Schema.toCodecJson(Row)),
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

    expect([...policy.topics]).toStrictEqual([
      [
        "externalOrders",
        {
          grpcLeased: false,
          owners: [],
          sourceOwned: false,
          topic: "externalOrders",
        },
      ],
      [
        "kafkaOrders",
        {
          grpcLeased: false,
          owners: [{ _tag: "kafka" }],
          sourceOwned: true,
          topic: "kafkaOrders",
        },
      ],
      [
        "leasedOrders",
        {
          grpcLeased: true,
          owners: [{ _tag: "grpc", lifecycle: "leased" }],
          sourceOwned: true,
          topic: "leasedOrders",
        },
      ],
      [
        "materializedOrders",
        {
          grpcLeased: false,
          owners: [{ _tag: "grpc", lifecycle: "materialized" }],
          sourceOwned: true,
          topic: "materializedOrders",
        },
      ],
    ]);
    expect([...policy.sourceOwnedTopics]).toStrictEqual([
      "kafkaOrders",
      "leasedOrders",
      "materializedOrders",
    ]);
    expect([...policy.grpcLeasedTopics]).toStrictEqual(["leasedOrders"]);
    expect(policy.hasSourceOwnedTopics).toStrictEqual(true);
    expect(policy.isSourceOwnedTopic("externalOrders")).toStrictEqual(false);
    expect(policy.isSourceOwnedTopic("kafkaOrders")).toStrictEqual(true);
    expect(policy.isSourceOwnedTopic("materializedOrders")).toStrictEqual(true);
    expect(policy.isGrpcLeasedTopic("leasedOrders")).toStrictEqual(true);
    expect(policy.isGrpcLeasedTopic("kafkaOrders")).toStrictEqual(false);
  });

  it.effect("allows direct public mutations, reads, and reset for source-free topics", () =>
    Effect.gen(function* () {
      const policy = makeSourceOwnershipPolicy(sourceFreeViewServer);

      yield* policy.requirePublicMutationAllowed("externalOrders", "runtimeCore");
      yield* policy.requirePublicReadAllowed("externalOrders", "runtimeCore");
      yield* policy.requirePublicResetAllowed("runtimeCore");

      expect([...policy.sourceOwnedTopics]).toStrictEqual([]);
      expect([...policy.grpcLeasedTopics]).toStrictEqual([]);
      expect(policy.hasSourceOwnedTopics).toStrictEqual(false);
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
        expect(policy.publicMutationDecision("kafkaOrders", "runtimeCore")).toStrictEqual({
          _tag: "rejected",
          error: sourceOwnedMutationError("kafkaOrders"),
        });
        expect(policy.publicReadDecision("kafkaOrders", "runtimeCore")).toStrictEqual({
          _tag: "allowed",
        });
        expect(policy.publicResetDecision("runtimeCore")).toStrictEqual({
          _tag: "rejected",
          error: sourceOwnedResetError,
        });
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
      expect(policy.publicReadDecision("leasedOrders", "managedRuntime")).toStrictEqual({
        _tag: "rejected",
        error: managedRuntimeLeasedAccessError("leasedOrders"),
      });
      expect(policy.publicMutationDecision("leasedOrders", "managedRuntime")).toStrictEqual({
        _tag: "rejected",
        error: managedRuntimeLeasedAccessError("leasedOrders"),
      });
      expect(policy.publicResetDecision("managedRuntime")).toStrictEqual({
        _tag: "rejected",
        error: managedRuntimeLeasedResetError,
      });
    }),
  );

  it.effect("preserves leased runtime protection for invalid declared leased metadata", () =>
    Effect.gen(function* () {
      const malformedLeasedOrders: {
        schema: typeof Row;
        key: "id";
      } = {
        schema: Row,
        key: "id",
      };
      Object.defineProperty(malformedLeasedOrders, "grpcSource", {
        value: {
          _tag: "GrpcLeasedTopicSource",
          kind: "grpc",
          lifecycle: "leased",
          routeBy: [],
        },
      });
      // Config admission rejects empty leased routing, while the policy still defends its
      // structural seam for callers that construct an internal view server directly.
      const malformedLeasedViewServer = {
        topics: {
          malformedLeasedOrders,
        },
      };
      const policy = makeSourceOwnershipPolicy(malformedLeasedViewServer);

      const runtimeCoreReadError = yield* policy
        .requirePublicReadAllowed("malformedLeasedOrders", "runtimeCore")
        .pipe(Effect.flip);

      expect([...policy.grpcLeasedTopics]).toStrictEqual(["malformedLeasedOrders"]);
      expect([...policy.topics]).toStrictEqual([
        [
          "malformedLeasedOrders",
          {
            grpcLeased: true,
            owners: [{ _tag: "grpc", lifecycle: "leased" }],
            sourceOwned: true,
            topic: "malformedLeasedOrders",
          },
        ],
      ]);
      expect(runtimeCoreReadError).toStrictEqual(
        runtimeCoreLeasedAccessError("malformedLeasedOrders"),
      );
    }),
  );

  it("classifies malformed and conflicting source declarations without caller reflection", () => {
    const topicDefinition = (): {
      schema: typeof Row;
      key: "id";
    } => ({
      schema: Row,
      key: "id",
    });
    const malformedGrpcOrders = topicDefinition();
    const primitiveGrpcOrders = topicDefinition();
    const multiOwnedOrders = {
      ...topicDefinition(),
      kafkaSource: kafka.source({
        topic: "multi-owned-orders-source",
        regions: ["usa"],
        value: kafka.json(() => Schema.toCodecJson(Row)),
        key: kafka.stringKey(),
        rowKey: ({ key }) => key,
        map: ({ value }) => ({
          price: value.price,
          region: value.region,
          status: value.status,
        }),
      }),
    };
    Object.defineProperty(malformedGrpcOrders, "grpcSource", {
      value: { kind: "grpc", lifecycle: "wat" },
    });
    Object.defineProperty(primitiveGrpcOrders, "grpcSource", {
      value: "not-a-grpc-source",
    });
    Object.defineProperty(multiOwnedOrders, "grpcSource", {
      value: grpcSourceMarkers.materialized(),
    });
    const malformedViewServer = defineViewServerConfig({
      kafka: {
        usa: "localhost:9092",
      },
      topics: {
        malformedGrpcOrders,
        primitiveGrpcOrders,
      },
    });
    // Config admission rejects dual owners, while the policy still defends its structural seam.
    const policy = makeSourceOwnershipPolicy({
      topics: {
        ...malformedViewServer.topics,
        multiOwnedOrders,
      },
    });

    expect([...policy.topics]).toStrictEqual([
      [
        "malformedGrpcOrders",
        {
          grpcLeased: false,
          owners: [{ _tag: "grpc", lifecycle: "unknown" }],
          sourceOwned: true,
          topic: "malformedGrpcOrders",
        },
      ],
      [
        "multiOwnedOrders",
        {
          grpcLeased: false,
          owners: [{ _tag: "kafka" }, { _tag: "grpc", lifecycle: "materialized" }],
          sourceOwned: true,
          topic: "multiOwnedOrders",
        },
      ],
      [
        "primitiveGrpcOrders",
        {
          grpcLeased: false,
          owners: [{ _tag: "grpc", lifecycle: "unknown" }],
          sourceOwned: true,
          topic: "primitiveGrpcOrders",
        },
      ],
    ]);
  });

  it("collects resolved source owner conflicts without runtime-specific errors", () => {
    expect(
      collectSourceOwnershipConflicts(
        {
          topics: {
            "orders-source": {
              viewServerTopic: "orders",
            },
            "positions-source": {
              viewServerTopic: "positions",
            },
            "trades-source": {
              viewServerTopic: "trades",
            },
          },
        },
        {
          feeds: {
            ordersFeed: {
              topic: "orders",
            },
            positionsFeed: {
              topic: "positions",
            },
          },
        },
      ),
    ).toStrictEqual([
      {
        grpcFeed: "ordersFeed",
        kafkaSource: "orders-source",
        topic: "orders",
      },
      {
        grpcFeed: "positionsFeed",
        kafkaSource: "positions-source",
        topic: "positions",
      },
    ]);
    expect(collectSourceOwnershipConflicts(undefined, { feeds: {} })).toStrictEqual([]);
    expect(collectSourceOwnershipConflicts({ topics: {} }, undefined)).toStrictEqual([]);
  });
});
