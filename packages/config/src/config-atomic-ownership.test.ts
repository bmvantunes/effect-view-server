import { describe, expect, it } from "@effect/vitest";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { Schema } from "effect";
import { snapshotViewServerTopics } from "./config-ownership";
import { ViewServerId, defineViewServerConfig } from "./index";

const Failure = Schema.TaggedStruct("ConfigOwnershipFailure", {
  message: Schema.String,
});
const adapter = SourceAdapter.make({
  identity: { name: "config-ownership" },
  failure: Failure,
  materialized: {
    metrics: Schema.Struct({ connected: Schema.Boolean }),
    rejectionLocation: Schema.Struct({ offset: Schema.BigInt }),
    definitionOptions: SourceAdapter.definitionOptions<{
      readonly stream: string;
    }>(),
  },
  leased: undefined,
});

describe("View Server config atomic ownership", () => {
  it("snapshots changing Topic definitions once before validating the owned graph", () => {
    const SafeRow = Schema.Struct({ id: ViewServerId, value: Schema.String });
    const UnsupportedRow = Schema.Struct({ id: ViewServerId, value: Schema.Date });
    const safeSource = adapter.materializedSource({ stream: "safe" });
    const unsafeSource = adapter.materializedSource({ stream: "unsafe" });
    const reads = {
      topic: 0,
      schema: 0,
      source: 0,
    };
    const definition = {
      get schema() {
        reads.schema += 1;
        return reads.schema === 1 ? SafeRow : UnsupportedRow;
      },
      get source() {
        reads.source += 1;
        return reads.source === 1 ? safeSource : unsafeSource;
      },
    };
    const topics = new Proxy(
      { orders: definition },
      {
        get: (target, property, receiver) => {
          if (property !== "orders") {
            return Reflect.get(target, property, receiver);
          }
          reads.topic += 1;
          return reads.topic === 1
            ? definition
            : {
                schema: UnsupportedRow,
                source: unsafeSource,
              };
        },
      },
    );

    const config = defineViewServerConfig({ topics });

    expect(reads).toStrictEqual({
      topic: 1,
      schema: 1,
      source: 1,
    });
    expect({
      schemaField: config.topics.orders.schema.fields.value,
      source: config.topics.orders.source,
      configFrozen: Object.isFrozen(config),
      topicsFrozen: Object.isFrozen(config.topics),
      definitionFrozen: Object.isFrozen(config.topics.orders),
    }).toStrictEqual({
      schemaField: Schema.String,
      source: safeSource,
      configFrozen: true,
      topicsFrozen: true,
      definitionFrozen: true,
    });
  });

  it("preserves hostile non-enumerable properties so exact runtime validation rejects them", () => {
    const definition = {
      schema: Schema.Struct({ id: ViewServerId }),
    };
    Object.defineProperty(definition, "grpcSource", {
      value: "removed",
    });

    const topics = snapshotViewServerTopics({ rows: definition });

    expect({
      value: Reflect.get(topics.rows, "grpcSource"),
      own: Object.hasOwn(topics.rows, "grpcSource"),
    }).toStrictEqual({
      value: "removed",
      own: true,
    });
    expect(() =>
      defineViewServerConfig({
        topics,
      }),
    ).toThrow("View Server topic rows contains unsupported property: grpcSource.");
  });
});
