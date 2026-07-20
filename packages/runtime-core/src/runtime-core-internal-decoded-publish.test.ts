import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { makeViewServerRuntimeCoreInternal } from "./internal";
import { order, viewServer } from "./runtime-core-test-fixtures";

describe("@effect-view-server/runtime-core", () => {
  it.effect("allows internal decoded row publishing for source-owned runtime internals", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      yield* runtimeCore.internalClient.publishManyDecodedRows("orders", [order("decoded", 30)]);
      yield* runtimeCore.internalClient.publishManyDecodedRowsWithStorageKeys("orders", [
        {
          storageKey: "orders/source/row/storage-decoded",
          row: order("public-decoded", 40),
        },
      ]);

      const decodedSnapshot = yield* runtimeCore.internalClient.snapshot("orders", {
        where: [{ field: "customerId", type: "equals", filter: "customer-decoded" }],
        select: ["id", "price"],
        limit: 1,
      });
      const storageKeySnapshot = yield* runtimeCore.internalClient.snapshot("orders", {
        where: [{ field: "customerId", type: "equals", filter: "customer-public-decoded" }],
        select: ["id", "price"],
        limit: 1,
      });

      expect(decodedSnapshot).toStrictEqual({
        rows: [{ id: "decoded", price: 30 }],
        totalRows: 1,
        version: 2,
        status: "ready",
        statusCode: "Ready",
      });
      expect(storageKeySnapshot).toStrictEqual({
        rows: [{ id: "public-decoded", price: 40 }],
        totalRows: 1,
        version: 2,
        status: "ready",
        statusCode: "Ready",
      });

      yield* runtimeCore.close;
    }),
  );
});
