import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, viewSchema } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import { createColumnLiveViewEngine } from "./index";
import { createColumnLiveViewEngineInternal } from "./internal";

class RootClassOrder extends Schema.Class<RootClassOrder>("RootClassOrder")({
  id: Schema.String,
  quantity: Schema.BigInt,
  status: Schema.String,
}) {}
viewSchema.admitClass(RootClassOrder);

const rootClassViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: RootClassOrder,
      key: "id",
    },
  },
});

describe("ColumnLiveViewEngine root Class rows", () => {
  it.effect("publishes, replaces, and patches public root Class rows", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngine({
        topics: rootClassViewServer.topics,
      });

      yield* engine.publish(
        "orders",
        new RootClassOrder({ id: "a", quantity: 1n, status: "published" }),
      );
      yield* engine.publishMany("orders", [
        new RootClassOrder({ id: "b", quantity: 2n, status: "published-many" }),
        new RootClassOrder({ id: "c", quantity: 3n, status: "published-many" }),
      ]);
      yield* engine.publish(
        "orders",
        new RootClassOrder({ id: "a", quantity: 10n, status: "replaced" }),
      );
      yield* engine.patch("orders", "b", {
        quantity: 20n,
        status: "patched",
      });
      const snapshot = yield* engine.snapshot("orders", {
        select: ["id", "quantity", "status"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(snapshot).toStrictEqual({
        rows: [
          { id: "a", quantity: 10n, status: "replaced" },
          { id: "b", quantity: 20n, status: "patched" },
          { id: "c", quantity: 3n, status: "published-many" },
        ],
        status: "ready",
        statusCode: "Ready",
        totalRows: 3,
        version: 4,
      });
      yield* engine.close();
    }),
  );

  it.effect("reconstructs root Class rows after decoded patches", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({
        topics: rootClassViewServer.topics,
      });

      yield* engine.publishManyDecodedRows("orders", [
        new RootClassOrder({ id: "decoded", quantity: 41n, status: "published" }),
      ]);
      yield* engine.patchDecodedFields("orders", "decoded", {
        quantity: 42n,
        status: "patched",
      });
      const snapshot = yield* engine.snapshot("orders", {
        select: ["id", "quantity", "status"],
        limit: 10,
      });

      expect(snapshot).toStrictEqual({
        rows: [{ id: "decoded", quantity: 42n, status: "patched" }],
        status: "ready",
        statusCode: "Ready",
        totalRows: 1,
        version: 2,
      });
      yield* engine.close();
    }),
  );
});
