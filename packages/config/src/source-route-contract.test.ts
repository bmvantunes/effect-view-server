import { describe, expect, it } from "@effect/vitest";
import { BigDecimal, Effect, Schema } from "effect";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { ViewServerId, defineViewServerConfig, validateLiveQuerySourceRoute } from "./index";
import { sourceLeasedRouteBy } from "./source-query-contract";

const Failure = Schema.TaggedStruct("SourceRouteFailure", {
  message: Schema.String,
});
const Declaration = {
  metrics: Schema.Struct({ connected: Schema.Boolean }),
  rejectionLocation: Schema.Struct({ offset: Schema.BigInt }),
  definitionOptions: SourceAdapter.definitionOptions<{
    readonly stream: string;
  }>(),
};
const adapter = SourceAdapter.make({
  identity: { name: "source-route-test" },
  failure: Failure,
  materialized: Declaration,
  leased: Declaration,
});

const RoutedRow = Schema.Struct({
  id: ViewServerId,
  region: Schema.String,
  status: Schema.Literals(["open", "closed"]),
});

const withObjectPrototypeValue = <Value, Error, Requirements, PropertyValue>(
  field: string,
  value: PropertyValue,
  effect: Effect.Effect<Value, Error, Requirements>,
): Effect.Effect<Value, Error, Requirements> =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      Reflect.set(Object.prototype, field, value);
    }),
    () => effect,
    () =>
      Effect.sync(() => {
        Reflect.deleteProperty(Object.prototype, field);
      }),
  );

describe("Source route contracts", () => {
  it("totalizes and snapshots leased route metadata reflection", () => {
    const revokedSource = Proxy.revocable({}, {});
    revokedSource.revoke();
    const hostileLifecycle = new Proxy(
      {},
      {
        get(_target, property) {
          if (property === "lifecycle") {
            throw new Error("lifecycle cannot be inspected");
          }
          return undefined;
        },
      },
    );
    const hostileRouteBy = new Proxy(
      { lifecycle: "leased" },
      {
        get(target, property, receiver) {
          if (property === "routeBy") {
            throw new Error("routeBy cannot be inspected");
          }
          return Reflect.get(target, property, receiver);
        },
      },
    );
    const hostileRouteArray = new Proxy(["region"], {
      get(target, property, receiver) {
        if (property === Symbol.iterator) {
          throw new Error("route array cannot be iterated");
        }
        return Reflect.get(target, property, receiver);
      },
    });
    const mutableRouteFields = ["region"];
    const snapshottedRouteFields = sourceLeasedRouteBy({
      lifecycle: "leased",
      routeBy: mutableRouteFields,
    });
    mutableRouteFields[0] = "desk";

    expect({
      ordinary: sourceLeasedRouteBy({ lifecycle: "materialized" }),
      revoked: sourceLeasedRouteBy(revokedSource.proxy),
      hostileLifecycle: sourceLeasedRouteBy(hostileLifecycle),
      hostileRouteBy: sourceLeasedRouteBy(hostileRouteBy),
      nonArrayRouteBy: sourceLeasedRouteBy({ lifecycle: "leased", routeBy: "region" }),
      emptyRouteBy: sourceLeasedRouteBy({ lifecycle: "leased", routeBy: [] }),
      duplicateRouteBy: sourceLeasedRouteBy({
        lifecycle: "leased",
        routeBy: ["region", "region"],
      }),
      nonStringRouteBy: sourceLeasedRouteBy({ lifecycle: "leased", routeBy: [1] }),
      hostileRouteArray: sourceLeasedRouteBy({
        lifecycle: "leased",
        routeBy: hostileRouteArray,
      }),
      snapshottedRouteFields,
    }).toStrictEqual({
      ordinary: undefined,
      revoked: "invalid",
      hostileLifecycle: "invalid",
      hostileRouteBy: "invalid",
      nonArrayRouteBy: "invalid",
      emptyRouteBy: "invalid",
      duplicateRouteBy: "invalid",
      nonStringRouteBy: "invalid",
      hostileRouteArray: "invalid",
      snapshottedRouteFields: ["region"],
    });
  });

  it.effect("rejects route fields inherited through the schema field prototype", () =>
    withObjectPrototypeValue(
      "inheritedRoute",
      Schema.String,
      Effect.sync(() => {
        const RowWithoutInheritedRoute = Schema.Struct({ id: ViewServerId });

        expect(() =>
          // @ts-expect-error Runtime admission protects untyped callers from inherited route fields.
          defineViewServerConfig({
            topics: {
              inherited: {
                schema: RowWithoutInheritedRoute,
                source: adapter.leasedSource(["inheritedRoute"], {
                  stream: "inherited",
                }),
              },
            },
          }),
        ).toThrow(
          "View Server topic inherited leased source route field inheritedRoute must have a complete supported scalar schema domain.",
        );
      }),
    ),
  );

  it("validates exact leased routes independently from filters", () => {
    const viewServer = defineViewServerConfig({
      topics: {
        orders: {
          schema: RoutedRow,
          source: adapter.leasedSource(["region", "status"], { stream: "orders" }),
        },
        trades: {
          schema: RoutedRow,
          source: adapter.materializedSource({ stream: "trades" }),
        },
        positions: {
          schema: RoutedRow,
        },
      },
    });

    expect(validateLiveQuerySourceRoute(viewServer.topics, "missing", {})).toBeUndefined();
    expect(validateLiveQuerySourceRoute(viewServer.topics, "positions", {})).toBeUndefined();
    expect(validateLiveQuerySourceRoute(viewServer.topics, "trades", {})).toBeUndefined();

    const hostileOrdinaryQuery = new Proxy(
      {},
      {
        getOwnPropertyDescriptor: () => {
          throw new Error("query descriptor reflection failed");
        },
      },
    );
    expect(validateLiveQuerySourceRoute(viewServer.topics, "positions", hostileOrdinaryQuery)).toBe(
      "Query for topic positions contains unsupported reflective properties.",
    );

    expect(validateLiveQuerySourceRoute(viewServer.topics, "orders", null)).toBe(
      "Leased topic orders requires a query object.",
    );
    expect(validateLiveQuerySourceRoute(viewServer.topics, "orders", {})).toBe(
      "Leased topic orders requires routeBy fields: region, status.",
    );

    const hostileLeasedQuery = new Proxy(
      {},
      {
        getOwnPropertyDescriptor: () => {
          throw new Error("leased query descriptor reflection failed");
        },
      },
    );
    expect(validateLiveQuerySourceRoute(viewServer.topics, "orders", hostileLeasedQuery)).toBe(
      "Query for topic orders contains unsupported reflective properties.",
    );

    const hostileRouteKeys = new Proxy(
      { region: "UsÁ", status: "open" },
      {
        ownKeys: () => {
          throw new Error("route key reflection failed");
        },
      },
    );
    expect(
      validateLiveQuerySourceRoute(viewServer.topics, "orders", {
        routeBy: hostileRouteKeys,
      }),
    ).toBe("Query for topic orders contains unsupported reflective properties.");
    expect(
      validateLiveQuerySourceRoute(viewServer.topics, "orders", {
        routeBy: { region: "UsÁ" },
      }),
    ).toBe("Leased topic orders routeBy must contain all and only: region, status.");
    expect(
      validateLiveQuerySourceRoute(viewServer.topics, "orders", {
        routeBy: { region: "UsÁ", status: "open", desk: "equities" },
      }),
    ).toBe("Leased topic orders routeBy must contain all and only: region, status.");
    expect(
      validateLiveQuerySourceRoute(viewServer.topics, "orders", {
        routeBy: { region: "UsÁ", status: "missing" },
      }),
    ).toBe("Leased topic orders routeBy field status does not satisfy its configured schema.");
    expect(
      validateLiveQuerySourceRoute(viewServer.topics, "orders", {
        routeBy: { region: { value: "UsÁ" }, status: "open" },
      }),
    ).toBe("Leased topic orders routeBy field region must be a supported scalar value.");

    const routeByWithSymbol = { region: "UsÁ", status: "open" };
    Object.defineProperty(routeByWithSymbol, Symbol("metadata"), {
      enumerable: true,
      value: true,
    });
    expect(
      validateLiveQuerySourceRoute(viewServer.topics, "orders", {
        routeBy: routeByWithSymbol,
      }),
    ).toBe("Leased topic orders routeBy contains unsupported symbol properties.");
    expect(
      validateLiveQuerySourceRoute(viewServer.topics, "orders", {
        routeBy: { region: "UsÁ", status: "open" },
        where: [{ field: "status", type: "notEqual", filter: "closed" }],
      }),
    ).toBeUndefined();
    expect(
      validateLiveQuerySourceRoute(viewServer.topics, "positions", {
        routeBy: { region: "UsÁ" },
      }),
    ).toBe("Topic positions does not accept routeBy.");
  });

  it("accepts every supported route scalar and rejects unsafe values", () => {
    const ScalarRouteRow = Schema.Struct({
      id: ViewServerId,
      nil: Schema.Null,
      text: Schema.String,
      count: Schema.BigInt,
      enabled: Schema.Boolean,
      amount: Schema.BigDecimal,
      score: Schema.Number,
    });
    const scalarRouteConfig = defineViewServerConfig({
      topics: {
        scalarRoute: {
          schema: ScalarRouteRow,
          source: adapter.leasedSource(["nil", "text", "count", "enabled", "amount", "score"], {
            stream: "scalars",
          }),
        },
      },
    });
    const scalarRoute = {
      nil: null,
      text: "AbÇ",
      count: 1n,
      enabled: true,
      amount: BigDecimal.make(1230n, 3),
      score: -0,
    };

    expect(
      validateLiveQuerySourceRoute(scalarRouteConfig.topics, "scalarRoute", {
        routeBy: scalarRoute,
      }),
    ).toBeUndefined();
    expect(
      validateLiveQuerySourceRoute(scalarRouteConfig.topics, "scalarRoute", {
        routeBy: {
          ...scalarRoute,
          amount: BigDecimal.make(1n, Number.MIN_SAFE_INTEGER),
        },
      }),
    ).toBeUndefined();
    expect(
      validateLiveQuerySourceRoute(scalarRouteConfig.topics, "scalarRoute", {
        routeBy: { ...scalarRoute, score: Number.POSITIVE_INFINITY },
      }),
    ).toBe("Leased topic scalarRoute routeBy field score must be a supported scalar value.");
    for (const scale of [
      Number.POSITIVE_INFINITY,
      Number.NaN,
      1.5,
      Number.MIN_SAFE_INTEGER,
      Number.MIN_SAFE_INTEGER + 1,
    ]) {
      expect(
        validateLiveQuerySourceRoute(scalarRouteConfig.topics, "scalarRoute", {
          routeBy: { ...scalarRoute, amount: BigDecimal.make(123n, scale) },
        }),
      ).toBe("Leased topic scalarRoute routeBy field amount must be a supported scalar value.");
    }

    expect(
      validateLiveQuerySourceRoute(scalarRouteConfig.topics, "scalarRoute", {
        routeBy: null,
      }),
    ).toBe(
      "Leased topic scalarRoute requires routeBy fields: nil, text, count, enabled, amount, score.",
    );
    const hostileRouteBy = new Proxy(
      {},
      {
        getPrototypeOf: () => {
          throw new Error("route prototype reflection failed");
        },
      },
    );
    expect(
      validateLiveQuerySourceRoute(scalarRouteConfig.topics, "scalarRoute", {
        routeBy: hostileRouteBy,
      }),
    ).toBe(
      "Leased topic scalarRoute requires routeBy fields: nil, text, count, enabled, amount, score.",
    );

    const bigDecimalPrototype = Object.getPrototypeOf(BigDecimal.make(1n, 0));
    const forgedBigDecimal = (
      coefficient: PropertyDescriptor | undefined,
      scale: PropertyDescriptor | undefined,
    ): object => {
      const value = Object.create(bigDecimalPrototype);
      if (coefficient !== undefined) {
        Object.defineProperty(value, "value", coefficient);
      }
      if (scale !== undefined) {
        Object.defineProperty(value, "scale", scale);
      }
      return value;
    };
    const data = <Value>(value: Value): PropertyDescriptor => ({ enumerable: true, value });
    const invalidBigDecimals: ReadonlyArray<object> = [
      Object.create(null),
      Object.create({}),
      Object.create(
        Object.defineProperty({}, "~effect/BigDecimal", {
          enumerable: false,
          get: () => "~effect/BigDecimal",
        }),
      ),
      Object.create(
        Object.defineProperty({}, "~effect/BigDecimal", {
          enumerable: false,
          value: "wrong",
        }),
      ),
      forgedBigDecimal(undefined, undefined),
      forgedBigDecimal({ enumerable: false, value: 1n }, undefined),
      forgedBigDecimal({ enumerable: true, get: () => 1n }, undefined),
      forgedBigDecimal(data("1"), undefined),
      forgedBigDecimal(data(1n), undefined),
      forgedBigDecimal(data(1n), { enumerable: false, value: 0 }),
      forgedBigDecimal(data(1n), { enumerable: true, get: () => 0 }),
      forgedBigDecimal(data(1n), data("0")),
      new Proxy(BigDecimal.make(1n, 0), {
        getOwnPropertyDescriptor: () => {
          throw new Error("BigDecimal descriptor reflection failed");
        },
      }),
    ];
    for (const amount of invalidBigDecimals) {
      expect(
        validateLiveQuerySourceRoute(scalarRouteConfig.topics, "scalarRoute", {
          routeBy: { ...scalarRoute, amount },
        }),
      ).toBe("Leased topic scalarRoute routeBy field amount must be a supported scalar value.");
    }
  });

  it("totalizes malformed source definitions and defective route schemas", () => {
    const malformedEmpty = {
      malformed: {
        schema: RoutedRow,
        source: { lifecycle: "leased", routeBy: [] },
      },
    };
    const malformedMixed = {
      malformed: {
        schema: RoutedRow,
        source: { lifecycle: "leased", routeBy: ["region", 1] },
      },
    };
    const malformedMissingField = {
      malformed: {
        schema: RoutedRow,
        source: { lifecycle: "leased", routeBy: ["missing"] },
      },
    };
    expect(
      // @ts-expect-error Runtime validation totalizes structurally malformed source metadata.
      validateLiveQuerySourceRoute(malformedEmpty, "malformed", {}),
    ).toBe("Leased topic malformed has invalid route metadata.");
    expect(
      // @ts-expect-error Runtime validation totalizes structurally malformed source metadata.
      validateLiveQuerySourceRoute(malformedMixed, "malformed", {}),
    ).toBe("Leased topic malformed has invalid route metadata.");
    expect(
      // @ts-expect-error Runtime validation totalizes structurally malformed source metadata.
      validateLiveQuerySourceRoute(malformedMissingField, "malformed", {
        routeBy: { missing: "value" },
      }),
    ).toBe("Leased topic malformed routeBy field missing does not satisfy its configured schema.");

    const DefectiveRouteRow = Schema.Struct({
      id: ViewServerId,
      route: Schema.String.check(
        Schema.makeFilter(() => {
          throw new Error("route predicate defect");
        }),
      ),
    });
    const defective = {
      defective: {
        schema: DefectiveRouteRow,
        source: { lifecycle: "leased", routeBy: ["route"] },
      },
    };
    expect(
      // @ts-expect-error Runtime validation totalizes defective schemas from untyped callers.
      validateLiveQuerySourceRoute(defective, "defective", {
        routeBy: { route: "AbÇ" },
      }),
    ).toBe("Leased topic defective routeBy field route does not satisfy its configured schema.");
  });
});
