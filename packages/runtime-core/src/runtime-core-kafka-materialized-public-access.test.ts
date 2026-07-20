import { describe, expect, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  kafka,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import { makeViewServerRuntimeCoreInternal } from "./internal";
import {
  Order,
  order,
  publicSourceOwnedRuntimeMutationError,
  publicSourceOwnedRuntimeResetError,
} from "./runtime-core-test-fixtures";

const kafkaOwnedViewServer = defineViewServerConfig({
  kafka: {
    usa: "localhost:9092",
  },
  topics: {
    orders: {
      schema: Order,
      key: "id",
      kafkaSource: kafka.source({
        topic: "orders-source",
        regions: ["usa"],
        value: kafka.json(() => Schema.toCodecJson(Order)),
        key: kafka.stringKey(),
        rowKey: ({ key }) => key,
        map: ({ value }) => ({
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: value.region,
          updatedAt: value.updatedAt,
        }),
      }),
    },
  },
});

describe("@effect-view-server/runtime-core", () => {
  it.effect(
    "keeps Kafka source-owned materialized topics readable but blocks public mutations",
    () =>
      Effect.gen(function* () {
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(kafkaOwnedViewServer, {});

        yield* runtimeCore.internalClient.publish("orders", order("kafka", 10));

        const snapshot = yield* runtimeCore.publicClient.snapshot("orders", {
          select: ["id", "price"],
          limit: 10,
        });
        const publishEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          runtimeCore.publicClient.publish,
          runtimeCore.publicClient,
          ["orders", order("blocked-kafka", 30)],
        );
        const publishManyEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          runtimeCore.publicClient.publishMany,
          runtimeCore.publicClient,
          ["orders", [order("blocked-kafka-many", 35)]],
        );
        const patchEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          runtimeCore.publicClient.patch,
          runtimeCore.publicClient,
          ["orders", "kafka", { price: 35 }],
        );
        const deleteEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          runtimeCore.publicClient.delete,
          runtimeCore.publicClient,
          ["orders", "kafka"],
        );
        const resetEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          runtimeCore.publicClient.reset,
          runtimeCore.publicClient,
          [],
        );

        expect(snapshot).toStrictEqual({
          rows: [{ id: "kafka", price: 10 }],
          totalRows: 1,
          version: 1,
          status: "ready",
          statusCode: "Ready",
        });
        expect(yield* Effect.flip(publishEffect)).toStrictEqual(
          publicSourceOwnedRuntimeMutationError,
        );
        expect(yield* Effect.flip(publishManyEffect)).toStrictEqual(
          publicSourceOwnedRuntimeMutationError,
        );
        expect(yield* Effect.flip(patchEffect)).toStrictEqual(
          publicSourceOwnedRuntimeMutationError,
        );
        expect(yield* Effect.flip(deleteEffect)).toStrictEqual(
          publicSourceOwnedRuntimeMutationError,
        );
        expect(yield* Effect.flip(resetEffect)).toStrictEqual(publicSourceOwnedRuntimeResetError);

        yield* runtimeCore.close;
      }),
  );
});
