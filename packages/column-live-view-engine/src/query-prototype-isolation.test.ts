import { describe, expect, it } from "@effect/vitest";
import { BigDecimal, Effect, Schema } from "effect";
import {
  expectDeltaConverges,
  firstEvent,
  makeEventReader,
  stateFromSnapshot,
} from "../test-harness/events";
import { makeEngine, order, withObjectPrototypeValue } from "../test-harness/public-engine";
import { decodeGroupedQuery } from "./grouped-query-decoder";
import { InvalidQueryError } from "./index";
import { isGroupedQuery } from "./query-execution";
import { decodeRawQuery } from "./raw-query-decoder";
import { rawQueryCompilerMetadata } from "./raw-query-metadata";

const Row = Schema.Struct({
  value: Schema.String,
  quantity: Schema.Number,
});

const metadata = rawQueryCompilerMetadata(Row);

describe("query prototype isolation", () => {
  it.effect("classifies queries using only owned grouped markers", () =>
    withObjectPrototypeValue(
      "groupBy",
      ["value"],
      withObjectPrototypeValue(
        "aggregates",
        { rowCount: { aggFunc: "count" } },
        Effect.sync(() => {
          expect(isGroupedQuery({ select: ["value"] })).toBe(false);
          expect(isGroupedQuery({ groupBy: ["value"] })).toBe(true);
          expect(isGroupedQuery({ aggregates: { rowCount: { aggFunc: "count" } } })).toBe(true);
          expect(
            isGroupedQuery({
              groupBy: ["value"],
              aggregates: { rowCount: { aggFunc: "count" } },
            }),
          ).toBe(true);
        }),
      ),
    ),
  );

  it.effect("routes either grouped marker to the grouped decoder", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const missingAggregates = yield* Effect.flip(
        engine.subscribeRuntime("orders", { groupBy: ["region"] }),
      );
      expect(missingAggregates).toStrictEqual(
        InvalidQueryError.make({
          topic: "orders",
          message: "Grouped query aggregates must be a non-empty plain object.",
        }),
      );

      const missingGroupBy = yield* Effect.flip(
        engine.subscribeRuntime("orders", {
          aggregates: { rowCount: { aggFunc: "count" } },
        }),
      );
      expect(missingGroupBy).toStrictEqual(
        InvalidQueryError.make({
          topic: "orders",
          message: "Grouped query groupBy must be a non-empty array of strings.",
        }),
      );
      yield* engine.close();
    }),
  );

  it.effect("ignores inherited raw query fields and rejects inherited order fields", () =>
    withObjectPrototypeValue(
      "select",
      ["value"],
      withObjectPrototypeValue(
        "orderBy",
        [{ field: "quantity", direction: "desc" }],
        withObjectPrototypeValue(
          "offset",
          1,
          withObjectPrototypeValue(
            "limit",
            1,
            Effect.gen(function* () {
              const missingSelect = yield* Effect.flip(decodeRawQuery("rows", metadata, {}));
              expect(missingSelect.message).toBe(
                "Raw query select must be a non-empty array of strings.",
              );

              const decoded = yield* decodeRawQuery("rows", metadata, { select: ["value"] });
              expect(decoded).toStrictEqual({ select: ["value"] });
            }),
          ),
        ),
      ),
    ),
  );

  it.effect("requires owned raw order entry fields", () =>
    withObjectPrototypeValue(
      "field",
      "value",
      withObjectPrototypeValue(
        "direction",
        "asc",
        Effect.gen(function* () {
          const error = yield* Effect.flip(
            decodeRawQuery("rows", metadata, { select: ["value"], orderBy: [{}] }),
          );
          expect(error.message).toBe("Raw query orderBy field must be a string.");
        }),
      ),
    ),
  );

  it.effect("ignores inherited grouped query fields", () =>
    withObjectPrototypeValue(
      "groupBy",
      ["value"],
      withObjectPrototypeValue(
        "aggregates",
        { rowCount: { aggFunc: "count" } },
        withObjectPrototypeValue(
          "orderBy",
          [{ field: "value", direction: "desc" }],
          withObjectPrototypeValue(
            "offset",
            1,
            withObjectPrototypeValue(
              "limit",
              1,
              Effect.gen(function* () {
                const missingGroupBy = yield* Effect.flip(decodeGroupedQuery("rows", metadata, {}));
                expect(missingGroupBy.message).toBe(
                  "Grouped query groupBy must be a non-empty array of strings.",
                );

                const decoded = yield* decodeGroupedQuery("rows", metadata, {
                  groupBy: ["value"],
                  aggregates: { rowCount: { aggFunc: "count" } },
                });
                expect(decoded).toStrictEqual({
                  groupBy: ["value"],
                  aggregates: { rowCount: { aggFunc: "count" } },
                });
              }),
            ),
          ),
        ),
      ),
    ),
  );

  it.effect("requires owned grouped aggregate and order entry fields", () =>
    withObjectPrototypeValue(
      "aggFunc",
      "sum",
      withObjectPrototypeValue(
        "field",
        "quantity",
        withObjectPrototypeValue(
          "direction",
          "asc",
          Effect.gen(function* () {
            const inheritedAggFunc = yield* Effect.flip(
              decodeGroupedQuery("rows", metadata, {
                groupBy: ["value"],
                aggregates: { total: { field: "quantity" } },
              }),
            );
            expect(inheritedAggFunc.message).toBe(
              "Grouped query aggregate total has an unsupported aggFunc.",
            );

            const inheritedField = yield* Effect.flip(
              decodeGroupedQuery("rows", metadata, {
                groupBy: ["value"],
                aggregates: { total: { aggFunc: "sum" } },
              }),
            );
            expect(inheritedField.message).toBe(
              "Grouped query aggregate total field must be a string.",
            );

            const inheritedDirection = yield* Effect.flip(
              decodeGroupedQuery("rows", metadata, {
                groupBy: ["value"],
                aggregates: { rowCount: { aggFunc: "count" } },
                orderBy: [{ field: "value" }],
              }),
            );
            expect(inheritedDirection.message).toBe(
              "Grouped query orderBy direction must be asc or desc.",
            );
          }),
        ),
      ),
    ),
  );

  it.effect("keeps aggregate ordering isolated from inherited field discriminators", () =>
    withObjectPrototypeValue(
      "field",
      "region",
      Effect.scoped(
        Effect.gen(function* () {
          const engine = yield* makeEngine();
          yield* engine.publishMany("orders", [
            order("emea", "open", 10, 1, "emea"),
            order("amer", "open", 20, 2, "amer"),
          ]);
          const query = {
            groupBy: ["region"],
            aggregates: { totalPrice: { aggFunc: "sum", field: "price" } },
            orderBy: [{ aggregate: "totalPrice", direction: "desc" }],
          } as const;
          const subscription = yield* engine.subscribe("orders", query);
          const read = yield* makeEventReader(subscription);
          const initial = firstEvent(yield* read(1));
          const state = stateFromSnapshot(initial);
          expect(state.rows).toStrictEqual([
            { region: "amer", totalPrice: BigDecimal.fromStringUnsafe("20") },
            { region: "emea", totalPrice: BigDecimal.fromStringUnsafe("10") },
          ]);

          yield* engine.publish("orders", order("emea", "open", 30, 3, "emea"));
          const delta = firstEvent(yield* read(1));
          const fresh = yield* engine.snapshot("orders", query);
          expectDeltaConverges(state, delta, fresh.rows);
          expect(fresh.rows).toStrictEqual([
            { region: "emea", totalPrice: BigDecimal.fromStringUnsafe("30") },
            { region: "amer", totalPrice: BigDecimal.fromStringUnsafe("20") },
          ]);

          yield* subscription.close();
          yield* engine.close();
        }),
      ),
    ),
  );
});
