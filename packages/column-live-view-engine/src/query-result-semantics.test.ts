import { describe, expect, it } from "@effect/vitest";
import { Schema } from "effect";
import {
  groupedResultAggregateSemantics,
  makeQueryResultSemantics,
} from "./query-result-semantics";
import { makeTopicRowValueSemantics } from "./topic-row-value-semantics";

const OptionalNumberRow = Schema.Struct({
  id: Schema.String,
  value: Schema.optionalKey(Schema.Number),
});

const topicSemantics = makeTopicRowValueSemantics(OptionalNumberRow);
const optionalMinimumSemantics = groupedResultAggregateSemantics(topicSemantics, {
  aggFunc: "min",
  field: "value",
});

describe("query result semantics", () => {
  it("borrows immutable primitives and materializes structured result values", () => {
    const structuredSemantics = makeTopicRowValueSemantics(
      Schema.Struct({ value: Schema.Struct({ count: Schema.Number }) }),
    ).field("value");
    const semantics = makeQueryResultSemantics([
      {
        field: "value",
        semantics: structuredSemantics,
      },
    ]);
    const structured = { count: 1 };

    for (const value of [null, undefined, "immutable", 1, 1n, true]) {
      const primitiveResult = semantics.materializeRow({ value });
      expect(Reflect.get(primitiveResult, "value")).toBe(value);
    }
    const result = semantics.materializeRow({ value: structured });
    expect(result).toStrictEqual({ value: structured });
    expect(Reflect.get(result, "value")).not.toBe(structured);
  });

  it("keeps an undefined min/max result outside the optional field codec", () => {
    expect(optionalMinimumSemantics.materialize(undefined)).toBeUndefined();
    expect(optionalMinimumSemantics.materialize(1)).toBe(1);
    expect(optionalMinimumSemantics.decodeEncoded(undefined)).toBeUndefined();
    expect(optionalMinimumSemantics.decodeEncoded(1)).toBe(1);

    expect(optionalMinimumSemantics.equivalent(undefined, undefined)).toBe(true);
    expect(optionalMinimumSemantics.equivalent(undefined, 1)).toBe(false);
    expect(optionalMinimumSemantics.equivalent(1, undefined)).toBe(false);
    expect(optionalMinimumSemantics.equivalent(1, 1)).toBe(true);

    expect(optionalMinimumSemantics.compare(undefined, undefined)).toBe(0);
    expect(optionalMinimumSemantics.compare(undefined, 1)).toBe(-1);
    expect(optionalMinimumSemantics.compare(1, undefined)).toBe(1);
    expect(optionalMinimumSemantics.compare(1, 2)).toBe(-1);

    expect(optionalMinimumSemantics.canonicalKey(undefined)).toBe("undefined:");
    expect(optionalMinimumSemantics.canonicalKey(1)).toBe(
      `value:${topicSemantics.field("value").canonicalKey(1)}`,
    );
  });
});
