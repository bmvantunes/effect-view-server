import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { makeViewServerRuntimeCoreInternal } from "./internal";
import { order, viewServer } from "./runtime-core-test-fixtures";

describe("@effect-view-server/runtime-core", () => {
  it.effect("allows internal decoded field patching for source-owned runtime internals", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      yield* runtimeCore.internalClient.publishManyDecodedRows("orders", [order("decoded", 30)]);
      yield* runtimeCore.internalClient.patchDecodedFields("orders", "decoded", {
        price: 31,
        status: "closed",
      });
      const snapshot = yield* runtimeCore.internalClient.snapshot("orders", {
        select: ["id", "price", "status"],
        limit: 1,
      });

      expect(snapshot).toStrictEqual({
        rows: [{ id: "decoded", price: 31, status: "closed" }],
        totalRows: 1,
        version: 2,
        status: "ready",
        statusCode: "Ready",
      });

      yield* runtimeCore.close;
    }),
  );
});
