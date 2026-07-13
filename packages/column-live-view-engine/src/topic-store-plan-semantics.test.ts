import { describe, expect, it } from "@effect/vitest";
import { Effect, Equivalence, Schema } from "effect";
import { evaluateRawQuery } from "./active-query";
import { InvalidRowError } from "./index";
import { prepareRuntimeRawQuery, rawQueryCompilerMetadata } from "./raw-query-compiler";
import { publishTopicStoreRows, TopicStore } from "./topic-store";
import { topicStoreReadModel } from "./topic-store-state";
import { position, Position } from "../test-harness/public-engine";

describe("Topic Store plan semantics", () => {
  it.effect("rejects generic storage-order hints and uses the plan comparator", () =>
    Effect.gen(function* () {
      const store = new TopicStore("positions", Position, "id", () => {});
      yield* publishTopicStoreRows(
        store,
        [position("1", "aaa", 1n, "1", false), position("2", "bbb", 2n, "2", true)],
        (topic, message) => InvalidRowError.make({ topic, message }),
      );

      const result = topicStoreReadModel(store).scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [{ field: "active", direction: "asc" }],
        storageOrderBy: [{ field: "active", direction: "asc" }],
        matches: () => true,
        compare: (left, right) => right.key.localeCompare(left.key),
        offset: 0,
        limit: undefined,
      });

      expect(result.keys).toStrictEqual(["2", "1"]);
    }),
  );

  it.effect("keeps unproven scalar equivalence callback-only at the storage seam", () =>
    Effect.gen(function* () {
      const CaseInsensitiveString = Schema.String.pipe(
        Schema.overrideToEquivalence(() =>
          Equivalence.make((left, right) => left.toLowerCase() === right.toLowerCase()),
        ),
      );
      const CaseInsensitiveRows = Schema.Struct({
        id: Schema.String,
        label: CaseInsensitiveString,
        suspendedLabel: Schema.suspend(() => Schema.String),
      });
      const compiled = yield* prepareRuntimeRawQuery(
        "caseInsensitiveRows",
        rawQueryCompilerMetadata(CaseInsensitiveRows),
        {
          select: ["id"],
          where: {
            label: { eq: "alpha" },
          },
          orderBy: [{ field: "label", direction: "asc" }],
        },
      );

      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [],
              callbackRequired: true,
              callbackSkippable: false,
            });
            expect(Object.hasOwn(plan, "storageOrderBy")).toBe(false);
            expect(plan.matches({ id: "upper", label: "ALPHA" })).toBe(true);
            expect(plan.matches({ id: "other", label: "beta" })).toBe(false);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 19,
        },
        compiled,
      );

      expect(evaluation).toStrictEqual({
        keys: [],
        rows: [],
        window: [],
        totalRows: 0,
        version: 19,
      });
    }),
  );
});
