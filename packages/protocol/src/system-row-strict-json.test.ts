import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { decodeSystemLiveEvent, encodeSystemLiveEvent } from "./protocol-event-codec";

const SystemRow = Schema.Struct({
  id: Schema.String,
  payload: Schema.ObjectKeyword,
});

const expectedSystemMapError = {
  _tag: "ViewServerRuntimeError",
  code: "InvalidRow",
  message: "System row is not JSON-safe: Expected a plain data record or dense array at $.payload.",
  topic: "system",
} as const;

describe("System row strict JSON boundary", () => {
  it.effect("rejects opaque values during system-row encoding and hostile direct decoding", () =>
    Effect.gen(function* () {
      const encodeError = yield* Effect.flip(
        encodeSystemLiveEvent("system", SystemRow, {
          type: "snapshot",
          topic: "system",
          queryId: "system-encode",
          version: 1,
          keys: ["1"],
          rows: [{ id: "1", payload: new Map([["venue", "xnys"]]) }],
          totalRows: 1,
        }),
      );
      expect(encodeError).toStrictEqual(expectedSystemMapError);

      const hostileEvent = {
        type: "snapshot",
        topic: "system",
        queryId: "system-decode",
        version: 1,
        keys: ["1"],
        rows: [{ id: "1", payload: new Map([["venue", "xnys"]]) }],
        totalRows: 1,
      };
      const decodeError = yield* Effect.flip(
        decodeSystemLiveEvent(
          "system",
          SystemRow,
          // @ts-expect-error hostile callers can bypass the public JSON wire type.
          hostileEvent,
        ),
      );
      expect(decodeError).toStrictEqual({
        ...expectedSystemMapError,
        message: "Invalid system row: Expected a plain data record or dense array at $.payload.",
      });
    }),
  );

  it.effect("round-trips plain system rows and rejects non-record schema encodings", () =>
    Effect.gen(function* () {
      const event = {
        type: "snapshot",
        topic: "system",
        queryId: "system-plain",
        version: 1,
        keys: ["1"],
        rows: [{ id: "1", payload: { venue: "xnys" } }],
        totalRows: 1,
      } as const;
      const encoded = yield* encodeSystemLiveEvent("system", SystemRow, event);
      expect(encoded).toStrictEqual(event);
      expect(yield* decodeSystemLiveEvent("system", SystemRow, encoded)).toStrictEqual(event);

      const nonRecordError = yield* Effect.flip(
        encodeSystemLiveEvent("system", Schema.String, {
          type: "snapshot",
          topic: "system",
          queryId: "system-non-record",
          version: 1,
          keys: ["1"],
          rows: ["not-a-row"],
          totalRows: 1,
        }),
      );
      expect(nonRecordError).toMatchObject({
        _tag: "ViewServerRuntimeError",
        code: "InvalidRow",
        message: expect.stringContaining("Invalid system row"),
        topic: "system",
      });
    }),
  );
});
