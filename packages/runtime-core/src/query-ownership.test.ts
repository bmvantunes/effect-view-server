import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import { makeViewServerRuntimeCoreInternal } from "./internal";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

const order = (id: string, price: number): typeof Order.Type => ({
  id,
  customerId: `customer-${id}`,
  status: "open",
  price,
  region: "usa",
  updatedAt: price,
});

describe("runtime query ownership", () => {
  it.effect("captures one-shot queries at call time and rejects hostile input", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      yield* runtimeCore.internalClient.publishMany("orders", [order("b", 20), order("a", 10)]);

      const orderBy: Array<{
        field: "price";
        direction: "asc" | "desc";
      }> = [{ field: "price", direction: "asc" }];
      const select: ["id", "price"] = ["id", "price"];
      const query = {
        select,
        orderBy,
        offset: 0,
        limit: 1,
      };
      const snapshot = runtimeCore.internalClient.snapshotRuntimeInternal("orders", query);
      orderBy[0]!.direction = "desc";
      query.offset = 1;
      query.limit = 2;

      expect(yield* snapshot).toStrictEqual({
        rows: [{ id: "a", price: 10 }],
        status: "ready",
        statusCode: "Ready",
        totalRows: 2,
        version: 1,
      });

      let accessorReads = 0;
      const hostileQuery = {};
      Object.defineProperty(hostileQuery, "select", {
        enumerable: true,
        get: () => {
          accessorReads += 1;
          throw new Error("snapshot accessor must not run");
        },
      });
      const hostileSnapshot = runtimeCore.internalClient.snapshotRuntimeInternal(
        "orders",
        hostileQuery,
      );

      expect(yield* Effect.flip(hostileSnapshot)).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        message: "Query input could not be snapshotted.",
        topic: "orders",
      });
      expect(accessorReads).toBe(0);

      yield* runtimeCore.close;
    }),
  );
});
