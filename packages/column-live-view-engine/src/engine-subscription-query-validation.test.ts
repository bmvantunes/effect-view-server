import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { makeEngine, order } from "../test-harness/public-engine";

describe("ColumnLiveViewEngine subscription query validation", () => {
  it.effect("rejects invalid canonical operands instead of broadening results", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));

      const wrongNumericDomain: object = {
        select: ["id"],
        where: [{ field: "price", type: "greaterThan", filter: "9" }],
      };
      const numericDomainError = yield* Effect.flip(
        // @ts-expect-error hostile untyped queries are rejected at runtime.
        engine.snapshot("orders", wrongNumericDomain),
      );
      expect(numericDomainError.message).toBe(
        "Filter field price does not support this numeric operand domain.",
      );

      const nonFiniteError = yield* Effect.flip(
        engine.snapshot("orders", {
          select: ["id"],
          where: [{ field: "price", type: "greaterThan", filter: Number.NaN }],
        }),
      );
      expect(nonFiniteError.message).toBe("Filter numbers must be finite.");

      const nonArrayIn: object = {
        select: ["id"],
        where: [{ field: "status", type: "in", filter: "open" }],
      };
      const nonArrayInError = yield* Effect.flip(
        // @ts-expect-error hostile untyped queries are rejected at runtime.
        engine.snapshot("orders", nonArrayIn),
      );
      expect(nonArrayInError.message).toBe("Filter condition status in.filter must be an array.");

      const undefinedOperand: object = {
        select: ["id"],
        where: [{ field: "status", type: "equals", filter: undefined }],
      };
      const undefinedOperandError = yield* Effect.flip(
        // @ts-expect-error hostile untyped queries are rejected at runtime.
        engine.snapshot("orders", undefinedOperand),
      );
      expect(undefinedOperandError.message).toBe("Filter operands must not be undefined.");

      const unknownField: object = {
        select: ["id"],
        where: [{ field: "statuz", type: "equals", filter: "open" }],
      };
      const unknownFieldError = yield* Effect.flip(
        // @ts-expect-error hostile untyped queries are rejected at runtime.
        engine.snapshot("orders", unknownField),
      );
      expect(unknownFieldError.message).toBe(
        "Filter condition references unknown or non-filterable field: statuz.",
      );
    }),
  );

  it.effect("rejects invalid filters before opening a subscription", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const invalidQuery: object = {
        select: ["id"],
        where: [{ field: "price", type: "startsWith", filter: "1" }],
      };
      const error = yield* Effect.flip(
        // @ts-expect-error hostile untyped queries are rejected at runtime.
        engine.subscribe("orders", invalidQuery),
      );

      expect(error).toMatchObject({
        _tag: "InvalidQueryError",
        message: "Filter field price does not support startsWith.",
      });
    }),
  );
});
