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
      const revokedRoute = Proxy.revocable({ region: "usa" }, {});
      revokedRoute.revoke();
      const prototypeFailureRoute = new Proxy(
        { region: "usa" },
        {
          getPrototypeOf: () => {
            throw new Error("route prototype reflection failed");
          },
        },
      );
      const keysFailureRoute = new Proxy(
        { region: "usa" },
        {
          ownKeys: () => {
            throw new Error("route key reflection failed");
          },
        },
      );
      const descriptorFailureRoute = new Proxy(
        { region: "usa" },
        {
          getOwnPropertyDescriptor: () => {
            throw new Error("route descriptor reflection failed");
          },
        },
      );
      const taggedSymbol = {
        "$effect-view-server/route-scalar": "negativeZero",
      };
      Object.defineProperty(taggedSymbol, Symbol("metadata"), {
        enumerable: true,
        value: true,
      });
      const revokedEnvelope = Proxy.revocable(
        { "$effect-view-server/route-scalar": "bigint", value: "1" },
        {},
      );
      revokedEnvelope.revoke();

      const encodeCases: ReadonlyArray<readonly [Readonly<Record<string, unknown>>, string]> = [
        [symbolicRoute, "Query routeBy must be a plain object"],
        [accessorRoute, "Query routeBy must be a plain object"],
        [revokedRoute.proxy, "Query routeBy must be a plain object"],
        [prototypeFailureRoute, "Query routeBy must be a plain object"],
        [keysFailureRoute, "Query routeBy must be a plain object"],
        [descriptorFailureRoute, "Query routeBy must be a plain object"],
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
        [
          { amount: BigDecimal.make(111n, Number.MIN_SAFE_INTEGER) },
          "routeBy field amount does not satisfy its configured scalar schema",
        ],
        [
          { amount: BigDecimal.make(111n, Number.MIN_SAFE_INTEGER + 1) },
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
          { sequence: revokedEnvelope.proxy },
          "routeBy field sequence does not satisfy its configured scalar schema",
        ],
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
      const arrayError = yield* Effect.flip(decodeRouteBy("orders", RouteRow, arrayRoute));
      expect(arrayError.message).toBe("Query routeBy must be a plain object");
    }),
  );

  it.effect("uses one immutable route snapshot for field values", () =>
    Effect.gen(function* () {
      let regionDescriptorReads = 0;
      const routeBy = new Proxy(
        { region: "first" },
        {
          getOwnPropertyDescriptor: (target, key) => {
            const descriptor = Reflect.getOwnPropertyDescriptor(target, key);
            if (key !== "region" || descriptor === undefined) {
              return descriptor;
            }
            regionDescriptorReads += 1;
            return {
              ...descriptor,
              value: regionDescriptorReads === 1 ? "first" : "mutated",
            };
          },
        },
      );

      const encoded = yield* encodeRouteBy(leasedViewServer, "orders", routeBy);

      expect(encoded).toStrictEqual({ region: "first" });
      expect(regionDescriptorReads).toBe(1);
    }),
  );

  it.effect("owns a stateful BigDecimal route from one descriptor capture", () =>
    Effect.gen(function* () {
      let coefficientDescriptorReads = 0;
      let scaleDescriptorReads = 0;
      const amount = new Proxy(BigDecimal.make(123n, 2), {
        getOwnPropertyDescriptor: (target, key) => {
          if (key === "value") {
            coefficientDescriptorReads += 1;
            if (coefficientDescriptorReads > 1) {
              throw new Error("coefficient descriptor was read twice");
            }
          }
          if (key === "scale") {
            scaleDescriptorReads += 1;
            if (scaleDescriptorReads > 1) {
              throw new Error("scale descriptor was read twice");
            }
          }
          return Reflect.getOwnPropertyDescriptor(target, key);
        },
      });

      const encoded = yield* encodeRouteBy(leasedViewServer, "orders", { amount });

      expect(encoded).toStrictEqual({
        amount: {
          "$effect-view-server/route-scalar": "bigDecimal",
          coefficient: "123",
          scale: "2",
        },
      });
      expect(coefficientDescriptorReads).toBe(1);
      expect(scaleDescriptorReads).toBe(1);
    }),
  );

  it.effect("rejects hostile route values before public query schemas inspect them", () =>
    Effect.gen(function* () {
      const revoked = Proxy.revocable({ region: "usa" }, {});
      revoked.revoke();
      const rawQuery = { select: ["id"], routeBy: revoked.proxy };
      const groupedQuery = {
        groupBy: ["region"],
        aggregates: { rowCount: { aggFunc: "count" } },
        routeBy: revoked.proxy,
      };
      const rawEncodeError = yield* Effect.flip(
        viewServerEncodeRawQuery(leasedViewServer, "orders", rawQuery),
      );
      const rawDecodeError = yield* Effect.flip(
        viewServerDecodeRawQuery(leasedViewServer, "orders", rawQuery),
      );
      const groupedEncodeError = yield* Effect.flip(
        viewServerEncodeGroupedQuery(leasedViewServer, "orders", groupedQuery),
      );
      const groupedDecodeError = yield* Effect.flip(
        viewServerDecodeGroupedQuery(leasedViewServer, "orders", groupedQuery),
      );

      for (const error of [
        rawEncodeError,
        rawDecodeError,
        groupedEncodeError,
        groupedDecodeError,
      ]) {
        expect(error).toStrictEqual({
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          message: "Query input could not be inspected",
          topic: "orders",
        });
      }

      const explicitUndefined = yield* Effect.flip(
        viewServerEncodeRawQuery(leasedViewServer, "orders", {
          select: ["id"],
          routeBy: undefined,
        }),
      );
      const arrayRoute = yield* Effect.flip(
        viewServerEncodeRawQuery(leasedViewServer, "orders", {
          select: ["id"],
          routeBy: [],
        }),
      );
      expect(explicitUndefined.message).toBe("Query input could not be inspected");
      expect(arrayRoute.message).toBe("Query routeBy must be a plain object");
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
