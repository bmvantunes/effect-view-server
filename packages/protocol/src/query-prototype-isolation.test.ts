import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { viewServer } from "../test-harness/protocol";
import {
  defineViewServerLiveEventQuery,
  viewServerDecodeLiveEvent,
  viewServerEncodeLiveEvent,
} from "./protocol-event-codec";
import { viewServerEncodeGroupedQuery } from "./protocol-grouped-query-codec";

const withObjectPrototypeValue = <Value, Error, Requirements>(
  field: string,
  value: unknown,
  effect: Effect.Effect<Value, Error, Requirements>,
): Effect.Effect<Value, Error, Requirements> =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      Reflect.set(Object.prototype, field, value);
    }),
    () => effect,
    () =>
      Effect.sync(() => {
        Reflect.deleteProperty(Object.prototype, field);
      }),
  );

describe("protocol query prototype isolation", () => {
  it.effect("validates grouped order discriminators using only owned fields", () =>
    withObjectPrototypeValue(
      "field",
      "polluted",
      withObjectPrototypeValue(
        "aggregate",
        "polluted",
        Effect.gen(function* () {
          const aggregateOrder = yield* viewServerEncodeGroupedQuery(viewServer, "orders", {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: [{ aggregate: "rowCount", direction: "asc" }],
          });
          const fieldOrder = yield* viewServerEncodeGroupedQuery(viewServer, "orders", {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: [{ field: "status", direction: "asc" }],
          });

          expect(aggregateOrder.orderBy).toStrictEqual([
            { aggregate: "rowCount", direction: "asc" },
          ]);
          expect(fieldOrder.orderBy).toStrictEqual([{ field: "status", direction: "asc" }]);
        }),
      ),
    ),
  );

  it.effect("keeps raw event rows isolated from inherited grouped markers", () =>
    withObjectPrototypeValue(
      "groupBy",
      ["status"],
      Effect.gen(function* () {
        const query = defineViewServerLiveEventQuery(viewServer, "orders", { select: ["id"] });
        const encoded = yield* viewServerEncodeLiveEvent(viewServer, "orders", query, {
          type: "snapshot",
          topic: "orders",
          queryId: "query-1",
          version: 1,
          keys: ["order-1"],
          rows: [{ id: "order-1" }],
          totalRows: 1,
        });
        const decoded = yield* viewServerDecodeLiveEvent(viewServer, "orders", query, encoded);

        expect(decoded).toStrictEqual({
          type: "snapshot",
          topic: "orders",
          queryId: "query-1",
          version: 1,
          keys: ["order-1"],
          rows: [{ id: "order-1" }],
          totalRows: 1,
        });
      }),
    ),
  );
});
