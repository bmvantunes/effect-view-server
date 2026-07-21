import { describe, expect, it } from "@effect/vitest";
import { BigDecimal, Effect, Schema, Stream } from "effect";
import { defineViewServerConfig, grpc, validateLiveQuerySourceRoute } from "./index";
import { grpcSourceMarkers } from "./internal";
import { sourceLeasedRouteBy } from "./source-query-contract";
import {
  grpcOrdersByRegionStatusTopic,
  grpcOrdersMaterializedTopic,
  grpcTestClients,
  grpcTradesMaterializedTopic,
} from "../test-harness/grpc";
import { ordersService, tradesOnlyService } from "../test-harness/protobuf";
import { Order, Position, Trade } from "../test-harness/schemas";

const withObjectPrototypeValue = <Value, Error, Requirements>(
  field: string,
  value: unknown,
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

describe("gRPC configuration runtime behavior", () => {
  it("keeps gRPC source markers exact at runtime", () => {
    expect(grpcSourceMarkers.leased({ routeBy: ["region"] }).routeBy).toStrictEqual(["region"]);
  });

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
      revoked: sourceLeasedRouteBy(revokedSource.proxy),
      hostileLifecycle: sourceLeasedRouteBy(hostileLifecycle),
      hostileRouteBy: sourceLeasedRouteBy(hostileRouteBy),
      nonArrayRouteBy: sourceLeasedRouteBy({ lifecycle: "leased", routeBy: "region" }),
      hostileRouteArray: sourceLeasedRouteBy({
        lifecycle: "leased",
        routeBy: hostileRouteArray,
      }),
      snapshottedRouteFields,
    }).toStrictEqual({
      revoked: "invalid",
      hostileLifecycle: "invalid",
      hostileRouteBy: "invalid",
      nonArrayRouteBy: "invalid",
      hostileRouteArray: "invalid",
      snapshottedRouteFields: ["region"],
    });
  });

  it("rejects leased route metadata outside the complete scalar field contract", () => {
    const StructuredRoute = Schema.Struct({
      id: Schema.String,
      profile: Schema.Struct({ country: Schema.String }),
    });
    const MixedRoute = Schema.Struct({
      id: Schema.String,
      value: Schema.Union([Schema.String, Schema.Struct({ code: Schema.String })]),
    });
    const AnyRoute = Schema.Struct({ id: Schema.String, value: Schema.Any });
    const emptyRouteSource = grpcSourceMarkers.leased({ routeBy: ["id"] });
    const duplicateRouteSource = grpcSourceMarkers.leased({ routeBy: ["id"] });
    const nonStringRouteSource = grpcSourceMarkers.leased({ routeBy: ["id"] });
    expect(Reflect.set(emptyRouteSource, "routeBy", [])).toBe(true);
    expect(Reflect.set(duplicateRouteSource, "routeBy", ["id", "id"])).toBe(true);
    expect(Reflect.set(nonStringRouteSource, "routeBy", [1])).toBe(true);

    expect(() =>
      // @ts-expect-error Runtime admission protects untyped callers from structured route fields.
      defineViewServerConfig({
        topics: {
          structured: {
            schema: StructuredRoute,
            key: "id",
            grpcSource: grpcSourceMarkers.leased({ routeBy: ["profile"] }),
          },
        },
      }),
    ).toThrow(
      "View Server topic structured leased gRPC route field profile must have a complete supported scalar schema domain.",
    );
    expect(() =>
      // @ts-expect-error Runtime admission protects untyped callers from mixed route domains.
      defineViewServerConfig({
        topics: {
          mixed: {
            schema: MixedRoute,
            key: "id",
            grpcSource: grpcSourceMarkers.leased({ routeBy: ["value"] }),
          },
        },
      }),
    ).toThrow(
      "View Server topic mixed leased gRPC route field value must have a complete supported scalar schema domain.",
    );
    expect(() =>
      // @ts-expect-error Runtime admission protects untyped callers from Schema.Any routes.
      defineViewServerConfig({
        topics: {
          any: {
            schema: AnyRoute,
            key: "id",
            grpcSource: grpcSourceMarkers.leased({ routeBy: ["value"] }),
          },
        },
      }),
    ).toThrow(
      "View Server topic any leased gRPC route field value must have a complete supported scalar schema domain.",
    );
    expect(() =>
      // @ts-expect-error Runtime admission protects untyped callers from absent route fields.
      defineViewServerConfig({
        topics: {
          absent: {
            schema: StructuredRoute,
            key: "id",
            grpcSource: grpcSourceMarkers.leased({ routeBy: ["missing"] }),
          },
        },
      }),
    ).toThrow(
      "View Server topic absent leased gRPC route field missing must have a complete supported scalar schema domain.",
    );
    expect(() =>
      defineViewServerConfig({
        topics: {
          empty: {
            schema: StructuredRoute,
            key: "id",
            grpcSource: emptyRouteSource,
          },
        },
      }),
    ).toThrow("View Server topic empty declares invalid leased gRPC route metadata.");
    expect(() =>
      defineViewServerConfig({
        topics: {
          duplicate: {
            schema: StructuredRoute,
            key: "id",
            grpcSource: duplicateRouteSource,
          },
        },
      }),
    ).toThrow("View Server topic duplicate declares invalid leased gRPC route metadata.");
    expect(() =>
      defineViewServerConfig({
        topics: {
          nonString: {
            schema: StructuredRoute,
            key: "id",
            grpcSource: nonStringRouteSource,
          },
        },
      }),
    ).toThrow("View Server topic nonString declares invalid leased gRPC route metadata.");

    expect(() =>
      defineViewServerConfig({
        topics: {
          optional: {
            schema: Schema.Struct({
              id: Schema.String,
              region: Schema.optionalKey(Schema.String),
            }),
            key: "id",
            grpcSource: grpcSourceMarkers.leased({ routeBy: ["region"] }),
          },
        },
      }),
    ).not.toThrow();

    expect(() =>
      defineViewServerConfig({
        topics: {
          scalarUnion: {
            schema: Schema.Struct({
              id: Schema.String,
              route: Schema.Union([
                Schema.Literal("global"),
                Schema.Enum({ Europe: "emea", America: "amer" }),
              ]),
            }),
            key: "id",
            grpcSource: grpcSourceMarkers.leased({ routeBy: ["route"] }),
          },
        },
      }),
    ).not.toThrow();
  });

  it.effect("rejects route fields inherited through the schema field prototype", () =>
    withObjectPrototypeValue(
      "inheritedRoute",
      Schema.String,
      Effect.sync(() => {
        const RowWithoutInheritedRoute = Schema.Struct({ id: Schema.String });

        expect(() =>
          // @ts-expect-error Runtime admission protects untyped callers from inherited route fields.
          defineViewServerConfig({
            topics: {
              inherited: {
                schema: RowWithoutInheritedRoute,
                key: "id",
                grpcSource: grpcSourceMarkers.leased({ routeBy: ["inheritedRoute"] }),
              },
            },
          }),
        ).toThrow(
          "View Server topic inherited leased gRPC route field inheritedRoute must have a complete supported scalar schema domain.",
        );
      }),
    ),
  );

  it("validates exact leased gRPC route values independently from filters", () => {
    const grpcViewServer = defineViewServerConfig({
      grpc: {
        clients: grpcTestClients,
      },
      topics: {
        orders: grpcOrdersByRegionStatusTopic,
      },
    });

    const grpcRouteValidationViewServer = defineViewServerConfig({
      grpc: {
        clients: grpcTestClients,
      },
      topics: {
        orders: grpcOrdersByRegionStatusTopic,
        trades: grpcTradesMaterializedTopic,
        positions: {
          schema: Position,
          key: "id",
        },
      },
    });

    expect(validateLiveQuerySourceRoute(grpcViewServer.topics, "missing", {})).toBeUndefined();

    expect(
      validateLiveQuerySourceRoute(grpcRouteValidationViewServer.topics, "positions", {}),
    ).toBeUndefined();

    expect(
      validateLiveQuerySourceRoute(grpcRouteValidationViewServer.topics, "trades", {}),
    ).toBeUndefined();

    const hostileOrdinaryQuery = new Proxy(
      {},
      {
        getOwnPropertyDescriptor: () => {
          throw new Error("query descriptor reflection failed");
        },
      },
    );
    expect(
      validateLiveQuerySourceRoute(
        grpcRouteValidationViewServer.topics,
        "positions",
        hostileOrdinaryQuery,
      ),
    ).toBe("Query for topic positions contains unsupported reflective properties.");

    expect(validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", null)).toBe(
      "Leased topic orders requires a query object.",
    );

    expect(validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", {})).toBe(
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
    expect(validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", hostileLeasedQuery)).toBe(
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
      validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", {
        routeBy: hostileRouteKeys,
      }),
    ).toBe("Query for topic orders contains unsupported reflective properties.");

    expect(
      validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", {
        routeBy: { region: "UsÁ" },
      }),
    ).toBe("Leased topic orders routeBy must contain all and only: region, status.");

    expect(
      validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", {
        routeBy: { region: "UsÁ", status: "open", desk: "equities" },
      }),
    ).toBe("Leased topic orders routeBy must contain all and only: region, status.");

    expect(
      validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", {
        routeBy: { region: "UsÁ", status: "missing" },
      }),
    ).toBe("Leased topic orders routeBy field status does not satisfy its configured schema.");

    expect(
      validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", {
        routeBy: { region: { value: "UsÁ" }, status: "open" },
      }),
    ).toBe("Leased topic orders routeBy field region must be a supported scalar value.");

    const routeByWithSymbol = { region: "UsÁ", status: "open" };
    Object.defineProperty(routeByWithSymbol, Symbol("metadata"), {
      enumerable: true,
      value: true,
    });
    expect(
      validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", {
        routeBy: routeByWithSymbol,
      }),
    ).toBe("Leased topic orders routeBy contains unsupported symbol properties.");

    const ScalarRouteRow = Schema.Struct({
      id: Schema.String,
      nil: Schema.Null,
      text: Schema.String,
      count: Schema.BigInt,
      enabled: Schema.Boolean,
      amount: Schema.BigDecimal,
      score: Schema.Number,
    });
    const scalarRouteTopic = {
      scalarRoute: {
        schema: ScalarRouteRow,
        key: "id",
        grpcSource: {
          kind: "grpc",
          lifecycle: "leased",
          routeBy: ["nil", "text", "count", "enabled", "amount", "score"],
        },
      },
    };
    const scalarRoute = {
      nil: null,
      text: "AbÇ",
      count: 1n,
      enabled: true,
      amount: BigDecimal.make(1230n, 3),
      score: -0,
    };
    expect(
      validateLiveQuerySourceRoute(scalarRouteTopic, "scalarRoute", { routeBy: scalarRoute }),
    ).toBe(undefined);
    expect(
      validateLiveQuerySourceRoute(scalarRouteTopic, "scalarRoute", {
        routeBy: {
          ...scalarRoute,
          amount: BigDecimal.make(1n, Number.MIN_SAFE_INTEGER),
        },
      }),
    ).toBe(undefined);
    expect(
      validateLiveQuerySourceRoute(scalarRouteTopic, "scalarRoute", {
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
        validateLiveQuerySourceRoute(scalarRouteTopic, "scalarRoute", {
          routeBy: { ...scalarRoute, amount: BigDecimal.make(123n, scale) },
        }),
      ).toBe("Leased topic scalarRoute routeBy field amount must be a supported scalar value.");
    }

    const supportedScalarError =
      "Leased topic scalarRoute routeBy field amount must be a supported scalar value.";
    expect(validateLiveQuerySourceRoute(scalarRouteTopic, "scalarRoute", { routeBy: null })).toBe(
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
      validateLiveQuerySourceRoute(scalarRouteTopic, "scalarRoute", {
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
    const data = (value: unknown): PropertyDescriptor => ({ enumerable: true, value });
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
        validateLiveQuerySourceRoute(scalarRouteTopic, "scalarRoute", {
          routeBy: { ...scalarRoute, amount },
        }),
      ).toBe(supportedScalarError);
    }

    expect(
      validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", {
        routeBy: { region: "UsÁ", status: "open" },
        where: [{ field: "status", type: "notEqual", filter: "closed" }],
      }),
    ).toBeUndefined();

    expect(
      validateLiveQuerySourceRoute(grpcRouteValidationViewServer.topics, "positions", {
        routeBy: { region: "UsÁ" },
      }),
    ).toBe("Topic positions does not accept routeBy.");

    expect(
      validateLiveQuerySourceRoute(
        {
          malformed: {
            schema: Order,
            key: "id",
            grpcSource: { kind: "grpc", lifecycle: "leased", routeBy: [] },
          },
        },
        "malformed",
        {},
      ),
    ).toBe("Leased topic malformed has invalid route metadata.");

    expect(
      validateLiveQuerySourceRoute(
        {
          malformed: {
            schema: Order,
            key: "id",
            grpcSource: { kind: "grpc", lifecycle: "leased", routeBy: ["region", 1] },
          },
        },
        "malformed",
        {},
      ),
    ).toBe("Leased topic malformed has invalid route metadata.");
    expect(
      validateLiveQuerySourceRoute(
        {
          malformed: {
            schema: Order,
            key: "id",
            grpcSource: { kind: "grpc", lifecycle: "leased", routeBy: ["missing"] },
          },
        },
        "malformed",
        { routeBy: { missing: "value" } },
      ),
    ).toBe("Leased topic malformed routeBy field missing does not satisfy its configured schema.");

    const DefectiveRouteRow = Schema.Struct({
      id: Schema.String,
      route: Schema.String.check(
        Schema.makeFilter(() => {
          throw new Error("route predicate defect");
        }),
      ),
    });
    expect(
      validateLiveQuerySourceRoute(
        {
          defective: {
            schema: DefectiveRouteRow,
            key: "id",
            grpcSource: { kind: "grpc", lifecycle: "leased", routeBy: ["route"] },
          },
        },
        "defective",
        { routeBy: { route: "AbÇ" } },
      ),
    ).toBe("Leased topic defective routeBy field route does not satisfy its configured schema.");
  });

  it("rejects malformed gRPC source metadata and preserves runtime source shape", () => {
    const clients = {
      orders: grpc.connectClient({
        service: ordersService,
        baseUrl: "https://orders.example.test",
      }),
      trades: grpc.connectClient({
        service: tradesOnlyService,
        baseUrl: "https://trades.example.test",
      }),
    };

    const topicSources = grpc.topicSources(clients);

    const otherClients = {
      otherOrders: grpc.connectClient({
        service: ordersService,
        baseUrl: "https://other-orders.example.test",
      }),
    };

    const otherTopicSources = grpc.topicSources(otherClients);

    const topicOwnedGrpcViewServer = defineViewServerConfig({
      grpc: {
        clients,
      },
      topics: {
        orders: topicSources.leased({
          schema: Order,
          key: "id",
          client: "orders",
          method: "streamOrders",
          routeBy: ["region", "status"],
          request: ({ region, status }) => ({ orderId: `${region}:${status}` }),
          acquire: () =>
            Stream.make({
              $typeName: "viewserver.test.OrderValue",
              customerId: "customer-topic-source-1",
              status: "open",
              price: 10,
              updatedAt: 1,
            }),
          release: () => Effect.void,
          map: ({ value, route }) => ({
            id: `${route.region}:${value.customerId}`,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: route.region,
            updatedAt: value.updatedAt,
          }),
        }),
        trades: topicSources.materialized({
          schema: Trade,
          key: "id",
          client: "orders",
          method: "streamTrades",
          request: () => ({ orderId: "all-topic-source-trades" }),
          acquire: () =>
            Stream.make({
              $typeName: "viewserver.test.TradeValue",
              symbol: "AAPL",
              quantity: 1,
              price: 10,
            }),
          release: () => Effect.void,
          map: ({ value }) => ({
            id: value.symbol,
            symbol: value.symbol,
            quantity: value.quantity,
            price: value.price,
            region: "usa",
          }),
        }),
      },
    });

    expect(() =>
      // @ts-expect-error topic-owned gRPC sources must be bound to this config's grpc.clients.
      defineViewServerConfig({
        grpc: {
          clients,
        },
        topics: {
          orders: otherTopicSources.materialized({
            schema: Order,
            key: "id",
            client: "otherOrders",
            method: "streamOrders",
            request: () => ({ orderId: "wrong-client-set" }),
            acquire: () =>
              Stream.make({
                $typeName: "viewserver.test.OrderValue",
                customerId: "customer-wrong-client-set",
                status: "open",
                price: 10,
                updatedAt: 1,
              }),
            map: ({ value }) => ({
              id: value.customerId,
              customerId: value.customerId,
              status: value.status,
              price: value.price,
              region: "usa",
              updatedAt: value.updatedAt,
            }),
          }),
        },
      }),
    ).toThrow(
      "View Server topic orders declares grpcSource client otherOrders, but defineViewServerConfig.grpc.clients does not define it.",
    );

    expect(() =>
      // @ts-expect-error topic-owned gRPC sources must use the same row key as the topic.
      defineViewServerConfig({
        grpc: {
          clients: grpcTestClients,
        },
        topics: {
          orders: {
            schema: Order,
            key: "customerId",
            grpcSource: grpcOrdersMaterializedTopic.grpcSource,
          },
        },
      }),
    ).toThrow(
      "View Server topic orders declares grpcSource for row key id, but topic key is customerId.",
    );

    expect(() =>
      defineViewServerConfig({
        grpc: {
          clients: grpcTestClients,
        },
        topics: {
          orders: {
            schema: Order,
            key: "id",
            grpcSource: Object.assign({}, grpcOrdersMaterializedTopic.grpcSource, {
              method: 1,
            }),
          },
        },
      }),
    ).toThrow(
      "View Server topic orders declares grpcSource method 1, but grpc client orders does not define it.",
    );

    expect(() =>
      defineViewServerConfig({
        grpc: {
          clients: grpcTestClients,
        },
        topics: {
          orders: {
            schema: Order,
            key: "id",
            grpcSource: Object.assign({}, grpcOrdersMaterializedTopic.grpcSource, {
              method: "missingMethod",
            }),
          },
        },
      }),
    ).toThrow(
      "View Server topic orders declares grpcSource method missingMethod, but grpc client orders does not define it.",
    );

    expect(() =>
      defineViewServerConfig({
        grpc: {
          clients: grpcTestClients,
        },
        topics: {
          orders: {
            schema: Order,
            key: "id",
            grpcSource: Object.assign({}, grpcOrdersMaterializedTopic.grpcSource, {
              method: "getOrder",
            }),
          },
        },
      }),
    ).toThrow(
      "View Server topic orders declares grpcSource method getOrder, but grpc client orders method is not server-streaming.",
    );

    expect(() =>
      // @ts-expect-error topic-owned gRPC sources must use the same schema as the topic.
      defineViewServerConfig({
        grpc: {
          clients: grpcTestClients,
        },
        topics: {
          orders: {
            schema: Trade,
            key: "id",
            grpcSource: grpcOrdersMaterializedTopic.grpcSource,
          },
        },
      }),
    ).toThrow(
      "View Server topic orders declares grpcSource for a different schema than the topic schema.",
    );

    expect(() =>
      defineViewServerConfig({
        grpc: {
          clients: grpcTestClients,
        },
        topics: {
          orders: {
            schema: Order,
            key: "id",
            grpcSource: grpcSourceMarkers.materialized(),
          },
        },
      }),
    ).not.toThrow();

    expect(() =>
      // @ts-expect-error unbranded gRPC source objects are not valid public TypeScript config.
      defineViewServerConfig({
        grpc: {
          clients: grpcTestClients,
        },
        topics: {
          orders: {
            schema: Order,
            key: "id",
            grpcSource: Object.assign(grpcSourceMarkers.materialized(), {
              client: "orders",
              method: "streamOrders",
              request: () => ({ orderId: "unbranded-source" }),
              acquire: () =>
                Stream.make({
                  $typeName: "viewserver.test.OrderValue",
                  customerId: "customer-unbranded-source",
                  status: "open",
                  price: 10,
                  updatedAt: 1,
                }),
              map: () => ({
                id: "customer-unbranded-source",
                customerId: "customer-unbranded-source",
                status: "open",
                price: 10,
                region: "usa",
                updatedAt: 1,
              }),
            }),
          },
        },
      }),
    ).not.toThrow();

    expect(() =>
      // @ts-expect-error unbranded gRPC source objects are not valid public TypeScript config.
      defineViewServerConfig({
        grpc: {
          clients: grpcTestClients,
        },
        topics: {
          orders: {
            schema: Order,
            key: "id",
            grpcSource: Object.assign(grpcSourceMarkers.materialized(), {
              client: "orders",
              method: "streamOrders",
            }),
          },
        },
      }),
    ).toThrow("View Server topic orders declares invalid gRPC source metadata.");

    expect(() =>
      // @ts-expect-error incomplete concrete gRPC source bindings remain runtime-guarded.
      defineViewServerConfig({
        grpc: { clients },
        topics: {
          orders: {
            schema: Order,
            key: "id",
            grpcSource: Object.assign(grpcSourceMarkers.materialized(), {
              method: "streamOrders",
              request: () => ({ orderId: "missing-client" }),
              acquire: () => Stream.never,
              map: () => ({
                id: "missing-client",
                customerId: "missing-client",
                status: "open",
                price: 1,
                region: "usa",
                updatedAt: 1,
              }),
            }),
          },
        },
      }),
    ).toThrow("View Server topic orders declares invalid gRPC source metadata.");

    expect(() =>
      // @ts-expect-error topic-owned gRPC sources require defineViewServerConfig.grpc.clients.
      defineViewServerConfig({
        topics: {
          orders: topicSources.materialized({
            schema: Order,
            key: "id",
            client: "orders",
            method: "streamOrders",
            request: () => ({ orderId: "missing-clients" }),
            acquire: () => Stream.never,
            map: ({ value }) => ({
              id: value.customerId,
              customerId: value.customerId,
              status: value.status,
              price: value.price,
              region: "usa",
              updatedAt: value.updatedAt,
            }),
          }),
        },
      }),
    ).toThrow(
      "View Server topic orders declares grpcSource, but defineViewServerConfig.grpc.clients was not provided.",
    );

    const grpcInfraViewServer = defineViewServerConfig({
      grpc: {
        clients,
      },
      topics: {
        orders: topicSources.materialized({
          schema: Order,
          key: "id",
          client: "orders",
          method: "streamOrders",
          request: () => ({ orderId: "all-infra-orders" }),
          acquire: () => Stream.never,
          map: ({ value }) => ({
            id: value.customerId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: "usa",
            updatedAt: value.updatedAt,
          }),
        }),
      },
    });

    expect(grpcInfraViewServer.grpc).toStrictEqual({
      clients,
    });

    expect(topicOwnedGrpcViewServer.topics.orders.grpcSource.lifecycle).toBe("leased");

    expect(topicOwnedGrpcViewServer.topics.orders.grpcSource.client).toBe("orders");

    expect(topicOwnedGrpcViewServer.topics.orders.grpcSource.method).toBe("streamOrders");

    expect(topicOwnedGrpcViewServer.topics.trades.grpcSource.lifecycle).toBe("materialized");

    expect(topicOwnedGrpcViewServer.topics.trades.grpcSource.client).toBe("orders");

    expect(topicOwnedGrpcViewServer.topics.trades.grpcSource.method).toBe("streamTrades");

    expect(
      Object.getOwnPropertySymbols(topicOwnedGrpcViewServer.topics.orders.grpcSource).some(
        (symbol) =>
          Reflect.get(topicOwnedGrpcViewServer.topics.orders.grpcSource, symbol) === clients,
      ),
    ).toBe(false);

    expect(
      Object.getOwnPropertySymbols(topicOwnedGrpcViewServer.topics.trades.grpcSource).some(
        (symbol) =>
          Reflect.get(topicOwnedGrpcViewServer.topics.trades.grpcSource, symbol) === clients,
      ),
    ).toBe(false);
  });
});
