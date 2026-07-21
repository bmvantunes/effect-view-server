import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import { viewServerDecodeRawQuery, viewServerEncodeRawQuery } from "./index";

const viewServer = defineViewServerConfig({
  topics: {
    values: {
      schema: Schema.Struct({
        id: Schema.String,
        number: Schema.Number,
        bigint: Schema.BigInt,
      }),
      key: "id",
    },
  },
});

describe("filter wire codec", () => {
  it.effect("preserves text options and every admitted numeric domain", () =>
    Effect.gen(function* () {
      const text = yield* viewServerEncodeRawQuery(viewServer, "values", {
        select: ["id"],
        where: [
          {
            field: "id",
            type: "contains",
            filter: "Résumé",
            caseSensitive: true,
            accentSensitive: true,
          },
        ],
      });
      const number = yield* viewServerEncodeRawQuery(viewServer, "values", {
        select: ["id"],
        where: [{ field: "number", type: "greaterThan", filter: 1 }],
      });
      const bigint = yield* viewServerEncodeRawQuery(viewServer, "values", {
        select: ["id"],
        where: [{ field: "bigint", type: "greaterThan", filter: 1n }],
      });

      expect(yield* viewServerDecodeRawQuery(viewServer, "values", text)).toStrictEqual({
        select: ["id"],
        where: [
          {
            field: "id",
            type: "contains",
            filter: "Résumé",
            caseSensitive: true,
            accentSensitive: true,
          },
        ],
      });
      expect(yield* viewServerDecodeRawQuery(viewServer, "values", number)).toStrictEqual({
        select: ["id"],
        where: [{ field: "number", type: "greaterThan", filter: 1 }],
      });
      expect(yield* viewServerDecodeRawQuery(viewServer, "values", bigint)).toStrictEqual({
        select: ["id"],
        where: [{ field: "bigint", type: "greaterThan", filter: 1n }],
      });
    }),
  );
});
