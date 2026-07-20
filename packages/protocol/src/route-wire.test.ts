import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema, SchemaGetter } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import {
  viewServerDecodeGroupedQuery,
  viewServerDecodeRawQuery,
  viewServerEncodeGroupedQuery,
  viewServerEncodeRawQuery,
} from "./index";
import { decodeRouteBy, encodeRouteBy } from "./protocol-query-common";

const TransformingString = Schema.String.pipe(
  Schema.encodeTo(Schema.String, {
    decode: SchemaGetter.transform((value) => value.toLowerCase()),
    encode: SchemaGetter.transform((value) => value.toUpperCase()),
  }),
);

const RouteRow = Schema.Struct({
  id: Schema.String,
  region: TransformingString,
  sequence: Schema.BigInt,
  amount: Schema.BigDecimal,
  zero: Schema.Number,
  empty: Schema.Null,
  enabled: Schema.Boolean,
  rank: Schema.Number,
});

const leasedViewServer = {
  topics: {
    orders: {
      schema: RouteRow,
      key: "id",
      grpcSource: {
        kind: "grpc",
        lifecycle: "leased",
        routeBy: ["region", "sequence", "amount", "zero"],
      },
    },
  },
} as const;

describe("leased route wire codec", () => {
  it.effect("round-trips exact scalars without applying schema transformations", () =>
    Effect.gen(function* () {
      const amount = BigDecimal.make(1230n, -0);
      const query = {
        select: ["id"],
        routeBy: {
          region: "ÁbCDEfgh",
          sequence: 9_007_199_254_740_993n,
          amount,
          zero: -0,
        },
      };
      const encoded = yield* viewServerEncodeRawQuery(leasedViewServer, "orders", query);
      const transported = JSON.parse(JSON.stringify(encoded));
      const decoded = yield* viewServerDecodeRawQuery(leasedViewServer, "orders", transported);

      expect(encoded).toStrictEqual({
        select: ["id"],
        routeBy: {
          region: "ÁbCDEfgh",
          sequence: {
            "$effect-view-server/route-scalar": "bigint",
            value: "9007199254740993",
          },
          amount: {
            "$effect-view-server/route-scalar": "bigDecimal",
            coefficient: "1230",
            scale: "-0",
          },
          zero: { "$effect-view-server/route-scalar": "negativeZero" },
        },
      });
      expect(decoded.routeBy).toStrictEqual(query.routeBy);
      expect(
        BigDecimal.isBigDecimal(decoded.routeBy?.["amount"]) &&
          Object.is(decoded.routeBy["amount"].scale, -0),
      ).toBe(true);
      expect(Object.is(decoded.routeBy?.["zero"], -0)).toBe(true);
    }),
  );

  it.effect("enforces leased and ordinary route ownership at the wire boundary", () =>
    Effect.gen(function* () {
      const missing = yield* Effect.flip(
        viewServerEncodeRawQuery(leasedViewServer, "orders", { select: ["id"] }),
      );
      const ordinary = {
        topics: { orders: { schema: RouteRow, key: "id" } },
      } as const;
      const unexpected = yield* Effect.flip(
        viewServerEncodeRawQuery(ordinary, "orders", {
          select: ["id"],
          routeBy: {
            region: "ÁbCDEfgh",
            sequence: 9_007_199_254_740_993n,
            amount: BigDecimal.make(1230n, 3),
            zero: -0,
          },
        }),
      );

      expect(missing.message).toBe(
        "Leased topic orders requires routeBy fields: region, sequence, amount, zero.",
      );
      expect(unexpected.message).toBe("Topic orders does not accept routeBy.");
    }),
  );

  it.effect("accepts every exact route scalar without normalizing text", () =>
    Effect.gen(function* () {
      const routeBy = {
        region: "RÉSUMÉ",
        empty: null,
        enabled: false,
        rank: 1.5,
      };
      const encoded = yield* encodeRouteBy(leasedViewServer, "orders", routeBy);
      const decoded = yield* decodeRouteBy("orders", RouteRow, encoded);

      expect(encoded).toStrictEqual(routeBy);
      expect(decoded).toStrictEqual(routeBy);
      expect(yield* encodeRouteBy(leasedViewServer, "orders", undefined)).toBeUndefined();
      expect(yield* decodeRouteBy("orders", RouteRow, undefined)).toBeUndefined();
    }),
  );

  it.effect("preserves exact route values for grouped queries", () =>
    Effect.gen(function* () {
      const routeBy = {
        region: "ÁbCDEfgh",
        sequence: 1n,
        amount: BigDecimal.make(123n, 2),
        zero: -0,
      };
      const query = {
        groupBy: ["region"],
        aggregates: { rowCount: { aggFunc: "count" } },
        routeBy,
      };
      const encoded = yield* viewServerEncodeGroupedQuery(leasedViewServer, "orders", query);
      const decoded = yield* viewServerDecodeGroupedQuery(leasedViewServer, "orders", encoded);

      expect(decoded.routeBy).toStrictEqual(routeBy);
      expect(Object.is(decoded.routeBy?.["zero"], -0)).toBe(true);
    }),
  );

  it.effect("rejects hostile route objects, scalars, and tagged envelopes", () =>
    Effect.gen(function* () {
      const symbolicRoute = { region: "usa" };
      Object.defineProperty(symbolicRoute, Symbol("metadata"), {
        enumerable: true,
        value: true,
      });
      const accessorRoute = {};
      Object.defineProperty(accessorRoute, "region", {
        enumerable: true,
        get: () => "usa",
      });
      const taggedSymbol = {
        "$effect-view-server/route-scalar": "negativeZero",
      };
      Object.defineProperty(taggedSymbol, Symbol("metadata"), {
        enumerable: true,
        value: true,
      });

      const encodeCases: ReadonlyArray<readonly [Readonly<Record<string, unknown>>, string]> = [
        [symbolicRoute, "Query routeBy must be a plain object"],
        [accessorRoute, "Invalid routeBy field: region"],
        [{ missing: "usa" }, "Invalid routeBy field: missing"],
        [
          { region: { value: "usa" } },
          "routeBy field region does not satisfy its configured scalar schema",
        ],
        [
          { rank: Number.POSITIVE_INFINITY },
          "routeBy field rank does not satisfy its configured scalar schema",
        ],
        [
          { amount: BigDecimal.make(1n, Number.POSITIVE_INFINITY) },
          "routeBy field amount does not satisfy its configured scalar schema",
        ],
        [
          { amount: BigDecimal.make(1n, Number.NaN) },
          "routeBy field amount does not satisfy its configured scalar schema",
        ],
        [
          { amount: BigDecimal.make(1n, 1.5) },
          "routeBy field amount does not satisfy its configured scalar schema",
        ],
        [{ rank: "1" }, "routeBy field rank does not satisfy its configured scalar schema"],
      ];
      for (const [routeBy, message] of encodeCases) {
        const error = yield* Effect.flip(encodeRouteBy(leasedViewServer, "orders", routeBy));
        expect(error.message).toBe(message);
      }

      const decodeCases: ReadonlyArray<readonly [Readonly<Record<string, unknown>>, string]> = [
        [{ sequence: [] }, "routeBy field sequence does not satisfy its configured scalar schema"],
        [{ sequence: {} }, "routeBy field sequence does not satisfy its configured scalar schema"],
        [
          { sequence: { "$effect-view-server/route-scalar": "unknown" } },
          "routeBy field sequence does not satisfy its configured scalar schema",
        ],
        [
          { sequence: { "$effect-view-server/route-scalar": "bigint", value: "01" } },
          "routeBy field sequence does not satisfy its configured scalar schema",
        ],
        [
          { sequence: { "$effect-view-server/route-scalar": "bigint", value: 1 } },
          "routeBy field sequence does not satisfy its configured scalar schema",
        ],
        [
          { sequence: { "$effect-view-server/route-scalar": "bigint", value: "1", extra: true } },
          "routeBy field sequence does not satisfy its configured scalar schema",
        ],
        [
          {
            amount: {
              "$effect-view-server/route-scalar": "bigDecimal",
              coefficient: "x",
              scale: "2",
            },
          },
          "routeBy field amount does not satisfy its configured scalar schema",
        ],
        [
          {
            amount: {
              "$effect-view-server/route-scalar": "bigDecimal",
              coefficient: "1",
              scale: 2,
            },
          },
          "routeBy field amount does not satisfy its configured scalar schema",
        ],
        [
          {
            amount: {
              "$effect-view-server/route-scalar": "bigDecimal",
              coefficient: "1",
              scale: String(Number.MAX_SAFE_INTEGER + 1),
            },
          },
          "routeBy field amount does not satisfy its configured scalar schema",
        ],
        [
          {
            amount: {
              "$effect-view-server/route-scalar": "bigDecimal",
              coefficient: "1",
              scale: "02",
            },
          },
          "routeBy field amount does not satisfy its configured scalar schema",
        ],
        [
          { zero: { "$effect-view-server/route-scalar": "negativeZero", extra: true } },
          "routeBy field zero does not satisfy its configured scalar schema",
        ],
        [
          { zero: taggedSymbol },
          "routeBy field zero does not satisfy its configured scalar schema",
        ],
      ];
      for (const [routeBy, message] of decodeCases) {
        const error = yield* Effect.flip(decodeRouteBy("orders", RouteRow, routeBy));
        expect(error.message).toBe(message);
      }

      const arrayRoute: Array<unknown> = [];
      const arrayError = yield* Effect.flip(
        // @ts-expect-error hostile wire callers can provide values outside the declared boundary.
        decodeRouteBy("orders", RouteRow, arrayRoute),
      );
      expect(arrayError.message).toBe("Query routeBy must be a plain object");
    }),
  );

  it.effect("turns throwing schema predicates into typed route errors", () =>
    Effect.gen(function* () {
      const ThrowingScalar = Schema.declare((value): value is string => {
        throw new Error(`cannot inspect ${String(value)}`);
      });
      const ThrowingRow = Schema.Struct({ id: Schema.String, broken: ThrowingScalar });
      const throwingConfig = {
        topics: { broken: { schema: ThrowingRow, key: "id" } },
      } as const;
      const error = yield* Effect.flip(
        encodeRouteBy(throwingConfig, "broken", { broken: "unchanged" }),
      );

      expect(error.message).toBe(
        "routeBy field broken does not satisfy its configured scalar schema",
      );
    }),
  );
});
