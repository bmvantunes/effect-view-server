import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { decodeWhere, encodeWhere } from "./protocol-query-common";
import {
  viewServerDecodeGroupedQuery,
  viewServerDecodeRawQuery,
  viewServerEncodeGroupedQuery,
  viewServerEncodeRawQuery,
} from "./index";
import { ViewServerId } from "@effect-view-server/config";

const Row = Schema.Struct({
  id: ViewServerId,
  status: Schema.Literals(["open", "closed"]),
  value: Schema.Number,
});

const config = { topics: { rows: { schema: Row } } } as const;

describe("explicit match-none wire codec", () => {
  it.effect("round-trips FALSE without inventing a field operand", () =>
    Effect.gen(function* () {
      const where = [
        { type: "FALSE" },
        {
          type: "OR",
          conditions: [{ type: "FALSE" }, { field: "status", type: "equals", filter: "open" }],
        },
        { type: "NOT", condition: { type: "FALSE" } },
      ];
      const encoded = yield* encodeWhere(config, "rows", where);
      const decoded = yield* decodeWhere("rows", Row, encoded);

      expect(encoded).toStrictEqual(where);
      expect(decoded).toStrictEqual(where);
      expect(decoded).toBeDefined();
      expect(decoded).toHaveLength(3);
      expect(decoded?.[0]).toStrictEqual({ type: "FALSE" });
      expect(Object.isFrozen(decoded?.[0])).toBe(true);
    }),
  );

  it.effect("preserves FALSE through raw and grouped query encode/decode", () =>
    Effect.gen(function* () {
      const raw = {
        select: ["id"],
        where: [{ type: "FALSE" }],
      };
      const encodedRaw = yield* viewServerEncodeRawQuery(config, "rows", raw);
      const decodedRaw = yield* viewServerDecodeRawQuery(config, "rows", encodedRaw);
      expect(encodedRaw).toStrictEqual(raw);
      expect(decodedRaw).toStrictEqual(raw);

      const grouped = {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        where: [{ type: "FALSE" }],
      };
      const encodedGrouped = yield* viewServerEncodeGroupedQuery(config, "rows", grouped);
      const decodedGrouped = yield* viewServerDecodeGroupedQuery(config, "rows", encodedGrouped);
      expect(encodedGrouped).toStrictEqual(grouped);
      expect(decodedGrouped).toStrictEqual(grouped);
    }),
  );

  it.effect("rejects FALSE expressions with extra keys on encode and decode", () =>
    Effect.gen(function* () {
      const malformed = [{ type: "FALSE", extra: true }];
      const encodeError = yield* Effect.flip(encodeWhere(config, "rows", malformed));
      const decodeError = yield* Effect.flip(decodeWhere("rows", Row, malformed));

      expect(encodeError.code).toBe("InvalidQuery");
      expect(encodeError.message).toBe("Filter FALSE has invalid keys");
      expect(decodeError.code).toBe("InvalidQuery");
      expect(decodeError.message).toBe("Filter FALSE has invalid keys");
    }),
  );
});
