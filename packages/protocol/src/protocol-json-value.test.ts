import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import {
  isProtocolJson,
  requireProtocolJson,
  requireProtocolJsonArray,
} from "./protocol-json-value";

describe("protocol JSON values", () => {
  it.effect("accepts deep JSON DAGs and returns a typed JSON value", () =>
    Effect.gen(function* () {
      const shared = { value: "shared" };
      const value = [null, "text", true, 1, { left: shared, right: shared }];

      expect(isProtocolJson(value)).toBe(true);
      expect(yield* requireProtocolJson("values", value)).toBe(value);
      expect(yield* requireProtocolJsonArray("values", value)).toBe(value);
    }),
  );

  it.effect("rejects non-JSON primitives, cycles, and hostile containers", () =>
    Effect.gen(function* () {
      class ArraySubclass extends Array<unknown> {}
      class RecordSubclass {
        readonly value = true;
      }
      const cycle: Array<unknown> = [];
      cycle.push(cycle);
      const symbolicArray: Array<unknown> = [];
      Object.defineProperty(symbolicArray, Symbol("metadata"), {
        enumerable: true,
        value: true,
      });
      const sparseArray: Array<unknown> = [];
      sparseArray.length = 1;
      const accessorArray: Array<unknown> = [];
      Object.defineProperty(accessorArray, "0", {
        enumerable: true,
        get: () => "value",
      });
      accessorArray.length = 1;
      const extraArray: Array<unknown> = [];
      Object.defineProperty(extraArray, "extra", { enumerable: true, value: true });
      const symbolicRecord = { value: true };
      Object.defineProperty(symbolicRecord, Symbol("metadata"), {
        enumerable: true,
        value: true,
      });
      const hiddenRecord = { value: true };
      Object.defineProperty(hiddenRecord, "hidden", { enumerable: false, value: true });
      const accessorRecord = {};
      Object.defineProperty(accessorRecord, "value", {
        enumerable: true,
        get: () => true,
      });

      const invalidValues: ReadonlyArray<unknown> = [
        undefined,
        1n,
        Number.POSITIVE_INFINITY,
        cycle,
        new RecordSubclass(),
        new ArraySubclass(),
        symbolicArray,
        sparseArray,
        accessorArray,
        extraArray,
        symbolicRecord,
        hiddenRecord,
        accessorRecord,
      ];
      expect(invalidValues.map(isProtocolJson)).toStrictEqual(invalidValues.map(() => false));

      expect(yield* Effect.flip(requireProtocolJson("values", undefined))).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        message: "Encoded filter is not JSON-safe",
        topic: "values",
      });
      expect(yield* Effect.flip(requireProtocolJsonArray("values", [undefined]))).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        message: "Encoded filter is not JSON-safe",
        topic: "values",
      });
    }),
  );
});
