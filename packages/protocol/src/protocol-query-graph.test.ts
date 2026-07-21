import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { ViewServerSubscribePayloadSchema } from "./index";
import { decodeQueryGraph, encodeQueryGraph } from "./protocol-query-schema";

const graphEnvelope = (root: unknown, nodes: ReadonlyArray<unknown>): string =>
  JSON.stringify({ format: "effect-view-server-query-graph-v1", nodes, root });

describe("subscription query graph codec", () => {
  it("round-trips primitives, arrays, records, and shared DAG nodes", () => {
    const shared = { value: "shared" };
    const query = {
      nil: null,
      text: "value",
      enabled: true,
      count: -0,
      values: [shared, shared],
    };

    const decoded = decodeQueryGraph(encodeQueryGraph(query));

    expect(decoded).toStrictEqual({
      nil: null,
      text: "value",
      enabled: true,
      count: -0,
      values: [{ value: "shared" }, { value: "shared" }],
    });
    expect(
      typeof decoded === "object" &&
        decoded !== null &&
        "count" in decoded &&
        Object.is(decoded.count, -0),
    ).toBe(true);
    expect(
      typeof decoded === "object" &&
        decoded !== null &&
        "values" in decoded &&
        Array.isArray(decoded.values) &&
        decoded.values[0] === decoded.values[1],
    ).toBe(true);
    expect(decodeQueryGraph(encodeQueryGraph(null))).toBe(null);
    expect(decodeQueryGraph(encodeQueryGraph("root"))).toBe("root");
    expect(decodeQueryGraph(encodeQueryGraph(false))).toBe(false);
    expect(decodeQueryGraph(encodeQueryGraph(42))).toBe(42);
    expect(Object.is(decodeQueryGraph(encodeQueryGraph(-0)), -0)).toBe(true);
  });

  it("rejects cyclic, non-JSON, and hostile query graphs during encoding", () => {
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
    Object.defineProperty(accessorArray, "0", { enumerable: true, get: () => true });
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
    Object.defineProperty(accessorRecord, "value", { enumerable: true, get: () => true });

    const invalid: ReadonlyArray<unknown> = [
      undefined,
      1n,
      Number.POSITIVE_INFINITY,
      cycle,
      new ArraySubclass(),
      symbolicArray,
      sparseArray,
      accessorArray,
      extraArray,
      new RecordSubclass(),
      symbolicRecord,
      hiddenRecord,
      accessorRecord,
    ];
    for (const value of invalid) {
      expect(() => encodeQueryGraph(value)).toThrow();
    }
  });

  it.effect("rejects malformed wire graphs through typed schema errors", () =>
    Effect.gen(function* () {
      const malformedGraphs = [
        "null",
        JSON.stringify({ format: "wrong", nodes: [], root: ["null"] }),
        JSON.stringify({
          format: "effect-view-server-query-graph-v1",
          nodes: {},
          root: ["null"],
        }),
        graphEnvelope(["reference", 0], [["invalid", []]]),
        graphEnvelope(["invalid"], []),
        graphEnvelope(["negativeZero", 0], []),
        graphEnvelope(["reference", 1], [["array", []]]),
        graphEnvelope(["reference", 0], [["array", {}]]),
        graphEnvelope(["reference", 0], [["record", {}]]),
        graphEnvelope(["reference", 0], [["record", [["key"]]]]),
        graphEnvelope(["reference", 0], [["array", [["reference", 0]]]]),
        graphEnvelope(
          ["reference", 0],
          [
            ["array", [["reference", 1]]],
            ["array", [["reference", 0]]],
          ],
        ),
        graphEnvelope(["null"], [["array", []]]),
        graphEnvelope(
          ["reference", 0],
          [
            [
              "record",
              [
                ["key", ["null"]],
                ["key", ["null"]],
              ],
            ],
          ],
        ),
      ];

      for (const query of malformedGraphs) {
        const error = yield* Schema.decodeUnknownEffect(ViewServerSubscribePayloadSchema)({
          topic: "orders",
          query,
        }).pipe(Effect.flip);
        expect(Schema.isSchemaError(error)).toBe(true);
      }

      const encodeError = yield* Schema.encodeUnknownEffect(ViewServerSubscribePayloadSchema)({
        topic: "orders",
        query: undefined,
      }).pipe(Effect.flip);
      expect(Schema.isSchemaError(encodeError)).toBe(true);
    }),
  );
});
