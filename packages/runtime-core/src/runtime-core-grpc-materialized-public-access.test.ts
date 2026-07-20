import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, type ViewServerRuntimeError } from "@effect-view-server/config";
import { grpcSourceMarkers } from "@effect-view-server/config/internal";
import { Effect } from "effect";
import { makeViewServerRuntimeCoreInternal } from "./internal";
import {
  Order,
  order,
  publicSourceOwnedRuntimeMutationError,
  publicSourceOwnedRuntimeResetError,
} from "./runtime-core-test-fixtures";

const materializedGrpcSourceViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
      grpcSource: grpcSourceMarkers.materialized(),
    },
  },
});

describe("@effect-view-server/runtime-core", () => {
  it.effect(
    "keeps gRPC source-owned materialized topics readable but blocks public mutations",
    () =>
      Effect.gen(function* () {
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(
          materializedGrpcSourceViewServer,
          {},
        );

        yield* runtimeCore.internalClient.publish("orders", order("grpc", 20));

        const snapshot = yield* runtimeCore.publicClient.snapshot("orders", {
          select: ["id", "price"],
          limit: 10,
        });
        const publishEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          runtimeCore.publicClient.publish,
          runtimeCore.publicClient,
          ["orders", order("blocked-grpc", 40)],
        );
        const publishManyEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          runtimeCore.publicClient.publishMany,
          runtimeCore.publicClient,
          ["orders", [order("blocked-grpc-many", 45)]],
        );
        const patchEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          runtimeCore.publicClient.patch,
          runtimeCore.publicClient,
          ["orders", "grpc", { price: 45 }],
        );
        const deleteEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          runtimeCore.publicClient.delete,
          runtimeCore.publicClient,
          ["orders", "grpc"],
        );
        const resetEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          runtimeCore.publicClient.reset,
          runtimeCore.publicClient,
          [],
        );

        expect(snapshot).toStrictEqual({
          rows: [{ id: "grpc", price: 20 }],
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
