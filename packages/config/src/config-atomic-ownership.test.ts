import { describe, expect, it } from "@effect/vitest";
import { Schema } from "effect";
import { snapshotViewServerTopics } from "./config-ownership";
import { defineViewServerConfig } from "./index";

describe("View Server config atomic ownership", () => {
  it("snapshots changing topic definitions once before validating the owned graph", () => {
    const SafeRow = Schema.Struct({ id: Schema.String, value: Schema.String });
    const UnsupportedRow = Schema.Struct({ id: Schema.String, value: Schema.Date });
    const reads = {
      topic: 0,
      schema: 0,
      key: 0,
    };
    const definition = {
      get schema() {
        reads.schema += 1;
        return reads.schema === 1 ? SafeRow : UnsupportedRow;
      },
      get key(): "id" {
        reads.key += 1;
        if (reads.key > 1) {
          throw new Error("topic key was read after ownership capture");
        }
        return "id";
      },
    };
    const unsafeDefinition = {
      schema: UnsupportedRow,
      key: "id" as const,
    };
    const topics = new Proxy(
      { orders: definition },
      {
        get: (target, property, receiver) => {
          if (property !== "orders") {
            return Reflect.get(target, property, receiver);
          }
          reads.topic += 1;
          return reads.topic === 1 ? definition : unsafeDefinition;
        },
      },
    );

    const config = defineViewServerConfig({ topics });

    expect(reads).toStrictEqual({
      topic: 1,
      schema: 1,
      key: 1,
    });
    expect({
      key: config.topics.orders.key,
      schemaField: config.topics.orders.schema.fields.value,
    }).toStrictEqual({
      key: "id",
      schemaField: Schema.String,
    });
  });

  it("captures source definitions and nested ownership arrays exactly once", () => {
    const Row = Schema.Struct({ id: Schema.String });
    let sourceReads = 0;
    let regionsReads = 0;
    const safeSource = {
      topic: "safe-source",
      get regions() {
        regionsReads += 1;
        return regionsReads === 1 ? ["usa"] : ["london"];
      },
    };
    const unsafeSource = { topic: "unsafe-source", regions: ["london"] };
    const definition = {
      schema: Row,
      key: "id",
      get kafkaSource() {
        sourceReads += 1;
        return sourceReads === 1 ? safeSource : unsafeSource;
      },
    };

    const topics = snapshotViewServerTopics({ orders: definition });

    expect({
      sourceReads,
      regionsReads,
      topic: topics.orders.kafkaSource?.topic,
      regions: topics.orders.kafkaSource?.regions,
      sourceFrozen: Object.isFrozen(topics.orders.kafkaSource),
      regionsFrozen: Object.isFrozen(topics.orders.kafkaSource?.regions),
    }).toStrictEqual({
      sourceReads: 1,
      regionsReads: 1,
      topic: "safe-source",
      regions: ["usa"],
      sourceFrozen: true,
      regionsFrozen: true,
    });
  });

  it("preserves non-enumerable and primitive source declarations for structural consumers", () => {
    const Row = Schema.Struct({ id: Schema.String });
    const malformedGrpcSource = { kind: "grpc", lifecycle: "wat" };
    const malformedOrders = {
      schema: Row,
      key: "id" as const,
    };
    const primitiveOrders = {
      schema: Row,
      key: "id" as const,
    };
    Object.defineProperty(malformedOrders, "grpcSource", {
      value: malformedGrpcSource,
    });
    Object.defineProperty(primitiveOrders, "grpcSource", {
      value: "not-a-source",
    });

    const topics = snapshotViewServerTopics({ malformedOrders, primitiveOrders });

    expect({
      malformed: Reflect.get(topics.malformedOrders, "grpcSource"),
      malformedFrozen: Object.isFrozen(Reflect.get(topics.malformedOrders, "grpcSource")),
      malformedOwn: Object.hasOwn(topics.malformedOrders, "grpcSource"),
      primitive: Reflect.get(topics.primitiveOrders, "grpcSource"),
      primitiveOwn: Object.hasOwn(topics.primitiveOrders, "grpcSource"),
    }).toStrictEqual({
      malformed: malformedGrpcSource,
      malformedFrozen: true,
      malformedOwn: true,
      primitive: "not-a-source",
      primitiveOwn: true,
    });
  });
});
