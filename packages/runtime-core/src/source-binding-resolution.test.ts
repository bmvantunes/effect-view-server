import { describe, expect, it } from "@effect/vitest";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { Schema } from "effect";
import { makeTopicSourceBindings } from "./source-binding-resolution";

const Row = Schema.Struct({
  id: ViewServerId,
  price: Schema.Number,
  region: Schema.String,
});

const Failure = Schema.TaggedStruct("BindingFailure", {
  message: Schema.String,
});
const Metrics = Schema.Struct({
  observed: Schema.BigInt,
});
const Location = Schema.Struct({
  offset: Schema.BigInt,
});
const adapter = SourceAdapter.make({
  identity: { name: "binding-source" },
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

describe("source binding resolution", () => {
  it("derives canonical bindings from the one topic-owned source property", () => {
    const bindings = makeTopicSourceBindings(viewServer);

    expect(
      [...bindings].map(([topic, binding]) => ({
        owners: binding.owners,
        sourceLeased: binding.sourceLeased,
        sourceLifecycle: binding.sourceLifecycle,
        sourceOwned: binding.sourceOwned,
        topic,
      })),
    ).toStrictEqual([
      {
        owners: [],
        sourceLeased: false,
        sourceLifecycle: "unknown",
        sourceOwned: false,
        topic: "externalOrders",
      },
      {
        owners: [{ _tag: "source", lifecycle: "leased" }],
        sourceLeased: true,
        sourceLifecycle: "leased",
        sourceOwned: true,
        topic: "leasedOrders",
      },
      {
        owners: [{ _tag: "source", lifecycle: "materialized" }],
        sourceLeased: false,
        sourceLifecycle: "materialized",
        sourceOwned: true,
        topic: "materializedOrders",
      },
    ]);
    expect(bindings.get("materializedOrders")?.schema).toBe(
      viewServer.topics.materializedOrders.schema,
    );
    expect(bindings.get("materializedOrders")?.source).toBe(
      viewServer.topics.materializedOrders.source,
    );
  });

  it("marks a hostile structural source as owned without accepting it as a Source Definition", () => {
    const topics = new Proxy(
      { ...viewServer.topics },
      {
        get: (target, property, receiver) =>
          property === "externalOrders"
            ? {
                ...target.externalOrders,
                source: {
                  lifecycle: "materialized",
                },
              }
            : Reflect.get(target, property, receiver),
      },
    );
    const binding = makeTopicSourceBindings({
      topics,
    }).get("externalOrders");

    expect(binding).toStrictEqual({
      schema: Row,
      source: undefined,
      sourceLifecycle: "unknown",
      sourceLeased: false,
      owners: [{ _tag: "source", lifecycle: "unknown" }],
      sourceOwned: true,
      topic: "externalOrders",
    });
  });

  it("treats a hostile non-object topic definition as source-free and schema-free", () => {
    const topics = new Proxy(
      { ...viewServer.topics },
      {
        get: (target, property, receiver) =>
          property === "externalOrders" ? null : Reflect.get(target, property, receiver),
      },
    );

    expect(makeTopicSourceBindings({ topics }).get("externalOrders")).toStrictEqual({
      schema: undefined,
      source: undefined,
      sourceLifecycle: "unknown",
      sourceLeased: false,
      owners: [],
      sourceOwned: false,
      topic: "externalOrders",
    });
  });
});
