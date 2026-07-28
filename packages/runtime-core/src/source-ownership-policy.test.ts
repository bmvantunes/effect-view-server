import { describe, expect, it } from "@effect/vitest";
import {
  ViewServerId,
  defineViewServerConfig,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { Effect, Schema } from "effect";
import { makeSourceOwnershipPolicy } from "./source-ownership-policy";

const Row = Schema.Struct({
  id: ViewServerId,
  price: Schema.Number,
  region: Schema.String,
});

const Failure = Schema.TaggedStruct("OwnershipSourceFailure", {
  message: Schema.String,
});
const Metrics = Schema.Struct({
  observed: Schema.BigInt,
});
const Location = Schema.Struct({
  offset: Schema.BigInt,
});
const adapter = SourceAdapter.make({
  identity: { name: "ownership-source" },
  failure: Failure,
  materialized: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<void>(),
  },
  leased: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<void>(),
  },
});

const viewServer = defineViewServerConfig({
  topics: {
    externalOrders: {
      schema: Row,
    },
    leasedOrders: {
      schema: Row,
      source: adapter.leasedSource(["region"], undefined),
    },
    materializedOrders: {
      schema: Row,
      source: adapter.materializedSource(undefined),
    },
  },
});

const sourceOwnedMutationError = (topic: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "UnsupportedQuery",
  topic,
  message:
    "Source-owned topics do not support direct runtime mutations; publish through the configured Source Adapter or use an externally-published topic.",
});

const sourceOwnedResetError: ViewServerRuntimeError = {
  _tag: "ViewServerRuntimeError",
  code: "UnsupportedQuery",
  message:
    "Source-owned topics do not support direct runtime reset; close the runtime or reset source-free topics through their owner.",
};

const sourceLeasedReadError = (topic: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "UnsupportedQuery",
  topic,
  message:
    "Leased Source topics do not support one-shot snapshots; use a live subscription so Runtime Core owns the source lease lifecycle.",
});

describe("SourceOwnershipPolicy", () => {
  it("classifies canonical source-owned and leased topics behind one Interface", () => {
    const policy = makeSourceOwnershipPolicy(viewServer);

    expect([...policy.topics]).toStrictEqual([
      [
        "externalOrders",
        {
          owners: [],
          sourceLeased: false,
          sourceOwned: false,
          topic: "externalOrders",
        },
      ],
      [
        "leasedOrders",
        {
          owners: [{ _tag: "source", lifecycle: "leased" }],
          sourceLeased: true,
          sourceOwned: true,
          topic: "leasedOrders",
        },
      ],
      [
        "materializedOrders",
        {
          owners: [{ _tag: "source", lifecycle: "materialized" }],
          sourceLeased: false,
          sourceOwned: true,
          topic: "materializedOrders",
        },
      ],
    ]);
    expect([...policy.leasedTopics]).toStrictEqual(["leasedOrders"]);
    expect([...policy.sourceOwnedTopics]).toStrictEqual(["leasedOrders", "materializedOrders"]);
    expect(policy.isLeasedTopic("leasedOrders")).toBe(true);
    expect(policy.isLeasedTopic("materializedOrders")).toBe(false);
    expect(policy.isSourceOwnedTopic("materializedOrders")).toBe(true);
    expect(policy.isSourceOwnedTopic("externalOrders")).toBe(false);
    expect(policy.hasSourceOwnedTopics).toBe(true);
  });

  it.effect("allows every operation for source-free topics", () =>
    Effect.gen(function* () {
      const sourceFree = defineViewServerConfig({
        topics: {
          externalOrders: {
            schema: Row,
          },
        },
      });
      const policy = makeSourceOwnershipPolicy(sourceFree);

      yield* policy.requirePublicMutationAllowed("externalOrders");
      yield* policy.requirePublicReadAllowed("externalOrders");
      yield* policy.requirePublicSubscriptionAllowed("externalOrders");
      yield* policy.requirePublicResetAllowed();

      expect(policy.hasSourceOwnedTopics).toBe(false);
      expect(policy.publicMutationDecision("externalOrders")).toStrictEqual({
        _tag: "allowed",
      });
      expect(policy.publicReadDecision("externalOrders")).toStrictEqual({
        _tag: "allowed",
      });
      expect(policy.publicResetDecision()).toStrictEqual({
        _tag: "allowed",
      });
    }),
  );

  it.effect("rejects direct source mutations, leased reads, and source-owned reset", () =>
    Effect.gen(function* () {
      const policy = makeSourceOwnershipPolicy(viewServer);

      yield* policy.requirePublicReadAllowed("materializedOrders");
      yield* policy.requirePublicSubscriptionAllowed("leasedOrders");
      const mutationError = yield* policy
        .requirePublicMutationAllowed("materializedOrders")
        .pipe(Effect.flip);
      const readError = yield* policy.requirePublicReadAllowed("leasedOrders").pipe(Effect.flip);
      const resetError = yield* policy.requirePublicResetAllowed().pipe(Effect.flip);

      expect(mutationError).toStrictEqual(sourceOwnedMutationError("materializedOrders"));
      expect(readError).toStrictEqual(sourceLeasedReadError("leasedOrders"));
      expect(resetError).toStrictEqual(sourceOwnedResetError);
      expect(policy.publicMutationDecision("leasedOrders")).toStrictEqual({
        _tag: "rejected",
        error: sourceOwnedMutationError("leasedOrders"),
      });
      expect(policy.publicReadDecision("leasedOrders")).toStrictEqual({
        _tag: "rejected",
        error: sourceLeasedReadError("leasedOrders"),
      });
      expect(policy.publicSubscriptionDecision("leasedOrders")).toStrictEqual({
        _tag: "allowed",
      });
      expect(policy.publicResetDecision()).toStrictEqual({
        _tag: "rejected",
        error: sourceOwnedResetError,
      });
    }),
  );
});
