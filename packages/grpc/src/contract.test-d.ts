import {
  create,
  toBinary,
  type JsonObject,
  type JsonValue,
  type Message,
} from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import {
  FieldDescriptorProto_Type,
  FileDescriptorProtoSchema,
  StringValueSchema,
  StructSchema,
  type StringValue,
} from "@bufbuild/protobuf/wkt";
import { defineViewServerConfig } from "@effect-view-server/config";
import {
  makeViewServerRuntimeCore,
  type ViewServerSourceRequirements,
} from "@effect-view-server/runtime-core";
import type {
  SourceDefinitionRetryServices,
  SourceDefinitionRow,
  SourceDefinitionRouteFields,
} from "@effect-view-server/source-adapter";
import { expectTypeOf } from "@effect/vitest";
import { Config, Context, Effect, Option, Schema } from "effect";
import {
  GrpcSourceAdapter,
  grpc,
  type GrpcAdapterFailure,
  type GrpcMaterializedMetrics,
  type GrpcMethodRequest,
  type GrpcMethodValue,
  type GrpcRejectionLocation,
  type GrpcServerStreamingMethodName,
} from "./contract";
import {
  grpcNode,
  grpcNodeLayerConfig,
  type GrpcNodeClientNames,
  type GrpcNodeOptions,
} from "./node";

type OrderRequestMessage = Message<"grpc.contract.OrderRequest"> & {
  readonly region: string;
  readonly filter?: FilterMessage;
  readonly metadata?: JsonObject;
  readonly ordinary?: CaseValueMessage;
  readonly selector:
    | {
        readonly case: "textChoice";
        readonly value: string;
      }
    | {
        readonly case: "filterChoice";
        readonly value: FilterMessage;
      }
    | {
        readonly case: "wrappedChoice";
        readonly value: StringValue;
      }
    | {
        readonly case: undefined;
        readonly value?: undefined;
      };
};

type FilterMessage = Message<"grpc.contract.Filter"> & {
  readonly status: string;
};

type OrderEventMessage = Message<"grpc.contract.OrderEvent"> & {
  readonly orderId: string;
  readonly price: number;
  readonly region: string;
};

type StrategyRequestMessage = Message<"grpc.contract.StrategyRequest"> & {
  readonly strategyId: string;
};

type StrategyEventMessage = Message<"grpc.contract.StrategyEvent"> & {
  readonly strategyId: string;
};

type CaseValueMessage = Message<"grpc.contract.CaseValue"> & {
  readonly case: string;
  readonly value: string;
};

type RecursiveRequestMessage = Message<"grpc.contract.RecursiveRequest"> & {
  readonly child?: RecursiveRequestMessage;
  readonly filter?: FilterMessage;
  readonly label: string;
};

const protoFile = fileDesc(
  globalThis.btoa(
    Array.from(
      toBinary(
        FileDescriptorProtoSchema,
        create(FileDescriptorProtoSchema, {
          name: "grpc/contract.proto",
          package: "grpc.contract",
          syntax: "proto3",
          dependency: ["google/protobuf/struct.proto", "google/protobuf/wrappers.proto"],
          messageType: [
            {
              name: "Filter",
              field: [
                {
                  name: "status",
                  number: 1,
                  type: FieldDescriptorProto_Type.STRING,
                },
              ],
            },
            {
              name: "OrderRequest",
              oneofDecl: [{ name: "selector" }],
              field: [
                {
                  name: "region",
                  number: 1,
                  type: FieldDescriptorProto_Type.STRING,
                },
                {
                  name: "filter",
                  number: 2,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.contract.Filter",
                },
                {
                  name: "text_choice",
                  number: 3,
                  type: FieldDescriptorProto_Type.STRING,
                  oneofIndex: 0,
                },
                {
                  name: "filter_choice",
                  number: 4,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.contract.Filter",
                  oneofIndex: 0,
                },
                {
                  name: "ordinary",
                  number: 5,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.contract.CaseValue",
                },
                {
                  name: "metadata",
                  number: 6,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".google.protobuf.Struct",
                },
                {
                  name: "wrapped_choice",
                  number: 7,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".google.protobuf.StringValue",
                  oneofIndex: 0,
                },
              ],
            },
            {
              name: "OrderEvent",
              field: [
                {
                  name: "order_id",
                  number: 1,
                  type: FieldDescriptorProto_Type.STRING,
                },
                {
                  name: "price",
                  number: 2,
                  type: FieldDescriptorProto_Type.DOUBLE,
                },
                {
                  name: "region",
                  number: 3,
                  type: FieldDescriptorProto_Type.STRING,
                },
              ],
            },
            {
              name: "StrategyRequest",
              field: [
                {
                  name: "strategy_id",
                  number: 1,
                  type: FieldDescriptorProto_Type.STRING,
                },
              ],
            },
            {
              name: "StrategyEvent",
              field: [
                {
                  name: "strategy_id",
                  number: 1,
                  type: FieldDescriptorProto_Type.STRING,
                },
              ],
            },
            {
              name: "CaseValue",
              field: [
                {
                  name: "case",
                  number: 1,
                  type: FieldDescriptorProto_Type.STRING,
                },
                {
                  name: "value",
                  number: 2,
                  type: FieldDescriptorProto_Type.STRING,
                },
              ],
            },
            {
              name: "RecursiveRequest",
              field: [
                {
                  name: "child",
                  number: 1,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.contract.RecursiveRequest",
                },
                {
                  name: "label",
                  number: 2,
                  type: FieldDescriptorProto_Type.STRING,
                },
                {
                  name: "filter",
                  number: 3,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.contract.Filter",
                },
              ],
            },
          ],
          service: [
            {
              name: "Orders",
              method: [
                {
                  name: "StreamOrders",
                  inputType: ".grpc.contract.OrderRequest",
                  outputType: ".grpc.contract.OrderEvent",
                  serverStreaming: true,
                },
                {
                  name: "GetOrder",
                  inputType: ".grpc.contract.OrderRequest",
                  outputType: ".grpc.contract.OrderEvent",
                },
              ],
            },
            {
              name: "Strategies",
              method: [
                {
                  name: "StreamStrategies",
                  inputType: ".grpc.contract.StrategyRequest",
                  outputType: ".grpc.contract.StrategyEvent",
                  serverStreaming: true,
                },
              ],
            },
            {
              name: "Recursive",
              method: [
                {
                  name: "StreamRecursive",
                  inputType: ".grpc.contract.RecursiveRequest",
                  outputType: ".grpc.contract.OrderEvent",
                  serverStreaming: true,
                },
              ],
            },
          ],
        }),
      ),
      (byte) => String.fromCharCode(byte),
    ).join(""),
  ),
  [StructSchema.file, StringValueSchema.file],
);

const OrderRequestSchema = messageDesc<OrderRequestMessage>(protoFile, 1);
const OrderEventSchema = messageDesc<OrderEventMessage>(protoFile, 2);
const StrategyRequestSchema = messageDesc<StrategyRequestMessage>(protoFile, 3);
const StrategyEventSchema = messageDesc<StrategyEventMessage>(protoFile, 4);
const RecursiveRequestSchema = messageDesc<RecursiveRequestMessage>(protoFile, 6);

const OrdersService = serviceDesc<{
  readonly streamOrders: {
    readonly input: typeof OrderRequestSchema;
    readonly output: typeof OrderEventSchema;
    readonly methodKind: "server_streaming";
  };
  readonly getOrder: {
    readonly input: typeof OrderRequestSchema;
    readonly output: typeof OrderEventSchema;
    readonly methodKind: "unary";
  };
}>(protoFile, 0);

const StrategiesService = serviceDesc<{
  readonly streamStrategies: {
    readonly input: typeof StrategyRequestSchema;
    readonly output: typeof StrategyEventSchema;
    readonly methodKind: "server_streaming";
  };
}>(protoFile, 1);

const RecursiveService = serviceDesc<{
  readonly streamRecursive: {
    readonly input: typeof RecursiveRequestSchema;
    readonly output: typeof OrderEventSchema;
    readonly methodKind: "server_streaming";
  };
}>(protoFile, 2);

const sources = grpc.topicSources({
  orders: OrdersService,
  strategies: StrategiesService,
});

expectTypeOf<GrpcServerStreamingMethodName<typeof OrdersService>>().toEqualTypeOf<"streamOrders">();
type ExpectedOrdersRequest = {
  readonly region?: string;
  readonly filter?: {
    readonly status?: string;
  };
  readonly ordinary?: {
    readonly case?: string;
    readonly value?: string;
  };
  readonly metadata?: Readonly<Record<string, JsonValue>>;
  readonly selector?:
    | {
        readonly case: "textChoice";
        readonly value: string;
      }
    | {
        readonly case: "filterChoice";
        readonly value: {
          readonly status?: string;
        };
      }
    | {
        readonly case: "wrappedChoice";
        readonly value: {
          readonly value?: string;
        };
      }
    | {
        readonly case: undefined;
        readonly value?: undefined;
      };
};

expectTypeOf<
  GrpcMethodRequest<typeof OrdersService, "streamOrders">
>().toExtend<ExpectedOrdersRequest>();
expectTypeOf<ExpectedOrdersRequest>().toExtend<
  GrpcMethodRequest<typeof OrdersService, "streamOrders">
>();
expectTypeOf<
  NonNullable<GrpcMethodRequest<typeof OrdersService, "streamOrders">["metadata"]>[string]
>().toEqualTypeOf<JsonValue>();
const invalidMetadataEntry: NonNullable<
  GrpcMethodRequest<typeof OrdersService, "streamOrders">["metadata"]
> = {
  // @ts-expect-error protobuf map and Struct dictionary entries cannot be undefined.
  invalid: undefined,
};
void invalidMetadataEntry;
expectTypeOf<
  GrpcMethodValue<typeof OrdersService, "streamOrders">
>().toEqualTypeOf<OrderEventMessage>();

const materialized = sources.materialized({
  client: "orders",
  method: "streamOrders",
  request: () => ({
    region: "all",
    filter: {
      status: "open",
    },
  }),
  map: ({ value }) => ({
    id: value.orderId,
    price: value.price,
    region: value.region,
  }),
});

sources.materialized({
  client: "orders",
  method: "streamOrders",
  request: () => ({
    selector: {
      case: "textChoice",
      value: "selected",
    },
  }),
  map: ({ value }) => ({
    id: value.orderId,
  }),
});

sources.materialized({
  client: "orders",
  method: "streamOrders",
  request: () => ({
    ordinary: {},
  }),
  map: ({ value }) => ({
    id: value.orderId,
  }),
});

const recursiveSources = grpc.topicSources({
  recursive: RecursiveService,
});

recursiveSources.materialized({
  client: "recursive",
  method: "streamRecursive",
  request: () => ({ child: { child: {} } }),
  map: ({ value }) => ({ id: value.orderId }),
});

const leased = sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  map: ({
    value,
    route,
  }): {
    readonly id: string;
    readonly price: number;
    readonly region: string;
  } => {
    expectTypeOf(route.region).toEqualTypeOf<string>();
    return {
      id: value.orderId,
      price: value.price,
      region: value.region,
    };
  },
  request: (route) => {
    expectTypeOf(route.region).toEqualTypeOf<string>();
    return {
      region: route.region,
    };
  },
});

sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  request: () => ({
    selector: {
      case: "filterChoice",
      value: {
        status: "open",
      },
    },
  }),
  map: ({ value }) => ({
    id: value.orderId,
    region: value.region,
  }),
});

const materializedStrategies = sources.materialized({
  client: "strategies",
  method: "streamStrategies",
  request: () => ({ strategyId: "all" }),
  map: ({
    value,
  }): {
    readonly id: string;
    readonly price: number;
    readonly region: string;
  } => ({
    id: value.strategyId,
    price: 0,
    region: "global",
  }),
});

const Row = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
  region: Schema.String,
});

const viewServer = defineViewServerConfig({
  topics: {
    materializedOrders: {
      schema: Row,
      source: materialized,
    },
    leasedOrders: {
      schema: Row,
      source: leased,
    },
  },
});
const mixedViewServer = defineViewServerConfig({
  topics: {
    materializedOrders: {
      schema: Row,
      source: materialized,
    },
    sourceFreeOrders: {
      schema: Row,
      key: "id",
    },
  },
});
const multiClientViewServer = defineViewServerConfig({
  topics: {
    materializedOrders: {
      schema: Row,
      source: materialized,
    },
    materializedStrategies: {
      schema: Row,
      source: materializedStrategies,
    },
  },
});
const runtimeCoreEffect = makeViewServerRuntimeCore(viewServer, {});

declare const deliberatelyAny: any;
declare const erasedCallback: any;
type MixedMappingRow = {
  readonly id: string;
  readonly region: string;
};
declare const mixedPromiseMapping: (input: {
  readonly value: OrderEventMessage;
}) => MixedMappingRow | Promise<MixedMappingRow>;
declare const mixedEffectMapping: (input: {
  readonly value: OrderEventMessage;
}) => MixedMappingRow | Effect.Effect<MixedMappingRow>;
declare const mixedOptionMapping: (input: {
  readonly value: OrderEventMessage;
}) => MixedMappingRow | Option.Option<MixedMappingRow>;
declare const requestUnion:
  | {
      readonly region: string;
    }
  | {
      readonly extra: true;
    };
type RecursiveCandidateLeaf = {
  readonly child?: RecursiveCandidateLeaf;
  readonly extra: true;
};
declare const recursiveCandidate: {
  readonly child?: RecursiveCandidateLeaf;
};
type RecursiveWrongScalar = {
  readonly child?: RecursiveWrongScalar;
  readonly label: number;
};
declare const recursiveWrongScalar: {
  readonly child?: RecursiveWrongScalar;
};
type RecursiveWrongMessage = {
  readonly child?: RecursiveWrongMessage | string;
  readonly label?: string;
};
declare const recursiveWrongMessage: {
  readonly child?: RecursiveWrongMessage;
};
type RecursiveValid = {
  readonly child?: RecursiveValid;
  readonly label?: string;
};
declare const recursiveValid: RecursiveValid;
type RecursiveNestedExtra = {
  readonly child?: RecursiveNestedExtra;
  readonly filter?: {
    readonly status?: string;
    readonly extra: true;
  };
  readonly label?: string;
};
declare const recursiveNestedExtra: RecursiveNestedExtra;
type RecursiveAliasOne = {
  readonly child?: RecursiveAliasTwo;
  readonly filter?: {
    readonly status?: string;
  };
  readonly label?: string;
};
type RecursiveAliasTwo = {
  readonly child?: RecursiveAliasThree;
  readonly filter?: {
    readonly status?: string;
  };
  readonly label?: string;
};
type RecursiveAliasThree = {
  readonly child?: RecursiveAliasOne;
  readonly filter?: {
    readonly status?: string;
    readonly extra?: true;
  };
  readonly label?: string;
};
declare const recursiveAliasChainExtra: RecursiveAliasOne;

// @ts-expect-error descriptor records must retain generated service types
grpc.topicSources(deliberatelyAny);

// @ts-expect-error descriptor values must retain generated service types
grpc.topicSources({ orders: deliberatelyAny });

// @ts-expect-error at least one logical client descriptor is required
grpc.topicSources({});

// @ts-expect-error message descriptors are not service descriptors
grpc.topicSources({ orders: OrderRequestSchema });

expectTypeOf(materialized.adapter).toEqualTypeOf(GrpcSourceAdapter);
expectTypeOf<SourceDefinitionRow<typeof materialized>>().toEqualTypeOf<{
  readonly id: string;
  readonly price: number;
  readonly region: string;
}>();
expectTypeOf<SourceDefinitionRow<typeof leased>>().toEqualTypeOf<{
  readonly id: string;
  readonly price: number;
  readonly region: string;
}>();
expectTypeOf<SourceDefinitionRouteFields<typeof leased>>().toEqualTypeOf<readonly ["region"]>();
expectTypeOf<SourceDefinitionRetryServices<typeof materialized>>().toEqualTypeOf<never>();
expectTypeOf<SourceDefinitionRetryServices<typeof leased>>().toEqualTypeOf<never>();
expectTypeOf<ViewServerSourceRequirements<typeof viewServer.topics>>().toEqualTypeOf<
  Context.Service.Identifier<typeof GrpcSourceAdapter.runtimeService>
>();
expectTypeOf<Effect.Services<typeof runtimeCoreEffect>>().toEqualTypeOf<
  Context.Service.Identifier<typeof GrpcSourceAdapter.runtimeService>
>();
expectTypeOf<GrpcNodeClientNames<typeof viewServer>>().toEqualTypeOf<"orders">();
type ExpectedGrpcNodeClientOptions = {
  readonly baseUrl: string;
  readonly interceptors?: ReadonlyArray<import("@connectrpc/connect").Interceptor>;
  readonly transport?: Omit<
    import("@connectrpc/connect-node").GrpcTransportOptions,
    "baseUrl" | "interceptors"
  >;
};
expectTypeOf<GrpcNodeOptions<typeof viewServer>>().toEqualTypeOf<{
  readonly orders: ExpectedGrpcNodeClientOptions;
}>();
expectTypeOf<GrpcNodeClientNames<typeof multiClientViewServer>>().toEqualTypeOf<
  "orders" | "strategies"
>();
expectTypeOf<GrpcNodeOptions<typeof multiClientViewServer>>().toEqualTypeOf<{
  readonly orders: ExpectedGrpcNodeClientOptions;
  readonly strategies: ExpectedGrpcNodeClientOptions;
}>();
expectTypeOf<GrpcAdapterFailure>().not.toBeAny();
expectTypeOf<GrpcMaterializedMetrics>().not.toBeAny();
expectTypeOf<GrpcRejectionLocation>().not.toBeAny();

grpcNode.layer(viewServer, {
  orders: {
    baseUrl: "https://orders.example",
  },
});
grpcNode.layer(mixedViewServer, {
  orders: {
    baseUrl: "https://orders.example",
  },
});
grpcNode.layer(multiClientViewServer, {
  orders: {
    baseUrl: "https://orders.example",
  },
  strategies: {
    baseUrl: "https://strategies.example",
  },
});
grpcNode.layerConfig(viewServer, {
  orders: {
    baseUrl: Config.string("ORDERS_GRPC_URL"),
  },
});
grpcNode.layerConfig(
  viewServer,
  Config.succeed({
    orders: {
      baseUrl: "https://orders.example",
    },
  }),
);
grpcNode.layerConfig(multiClientViewServer, {
  orders: {
    baseUrl: Config.string("ORDERS_GRPC_URL"),
  },
  strategies: {
    baseUrl: Config.string("STRATEGIES_GRPC_URL"),
  },
});

// @ts-expect-error resolved aggregate Node options require the orders client
grpcNode.layer(multiClientViewServer, {
  strategies: {
    baseUrl: "https://strategies.example",
  },
});

// @ts-expect-error resolved aggregate Node options require the strategies client
grpcNode.layer(multiClientViewServer, {
  orders: {
    baseUrl: "https://orders.example",
  },
});

// @ts-expect-error Config aggregate Node options require the orders client
grpcNode.layerConfig(multiClientViewServer, {
  strategies: {
    baseUrl: Config.string("STRATEGIES_GRPC_URL"),
  },
});

// @ts-expect-error Config aggregate Node options require the strategies client
grpcNode.layerConfig(multiClientViewServer, {
  orders: {
    baseUrl: Config.string("ORDERS_GRPC_URL"),
  },
});

// @ts-expect-error resolved aggregate Node options reject extra clients
grpcNode.layer(multiClientViewServer, {
  orders: {
    baseUrl: "https://orders.example",
  },
  strategies: {
    baseUrl: "https://strategies.example",
  },
  inventory: {
    baseUrl: "https://inventory.example",
  },
});

// @ts-expect-error whole-object Config values require every referenced logical client
grpcNode.layerConfig(viewServer, Config.succeed({}));

// @ts-expect-error whole-object Config values reject extra logical clients
grpcNode.layerConfig(
  viewServer,
  Config.succeed({
    orders: {
      baseUrl: "https://orders.example",
    },
    strategies: {
      baseUrl: "https://strategies.example",
    },
  }),
);

// @ts-expect-error whole-object Config values reject nested extra client fields
grpcNode.layerConfig(
  viewServer,
  Config.succeed({
    orders: {
      baseUrl: "https://orders.example",
      headers: {
        authorization: "not-a-browser-header",
      },
    },
  }),
);

// @ts-expect-error resolved aggregate Node options must not erase to any
grpcNode.layer(viewServer, deliberatelyAny);

// @ts-expect-error Config-wrapped aggregate Node options must not erase to any
grpcNode.layerConfig(viewServer, deliberatelyAny);

// @ts-expect-error resolved logical-client options must not erase to any
grpcNode.layer(viewServer, { orders: deliberatelyAny });

// @ts-expect-error Config-wrapped logical-client options must not erase to any
grpcNode.layerConfig(viewServer, { orders: deliberatelyAny });

// @ts-expect-error whole-object Config values must not erase to any
grpcNode.layerConfig(viewServer, Config.succeed(deliberatelyAny));

// @ts-expect-error direct whole-object Config values must not erase to any
grpcNodeLayerConfig(viewServer, Config.succeed(deliberatelyAny));

// @ts-expect-error nested values inside whole-object Config values must not erase to any
grpcNode.layerConfig(viewServer, Config.succeed({ orders: deliberatelyAny }));

// @ts-expect-error nested values inside per-client Config values must not erase to any
grpcNode.layerConfig(viewServer, {
  orders: Config.succeed({
    baseUrl: "https://orders.example",
    transport: deliberatelyAny,
  }),
});

// @ts-expect-error nested resolved transport options must not erase to any
grpcNode.layer(viewServer, {
  orders: {
    baseUrl: "https://orders.example",
    transport: deliberatelyAny,
  },
});

// @ts-expect-error nested Config-wrapped interceptor options must not erase to any
grpcNode.layerConfig(viewServer, {
  orders: {
    baseUrl: Config.string("ORDERS_GRPC_URL"),
    interceptors: deliberatelyAny,
  },
});

sources.materialized({
  // @ts-expect-error unknown logical client
  client: "unknown",
  // @ts-expect-error an unknown client has no selectable methods
  method: "streamOrders",
  // @ts-expect-error an unknown client has no generated request
  request: () => ({ region: "all" }),
  // @ts-expect-error an unknown client has no generated response value
  map: ({ value }) => ({ id: value.orderId }),
});

sources.materialized({
  client: "orders",
  method: "streamOrders",
  // @ts-expect-error every named union branch must be an exact generated request.
  request: () => requestUnion,
  map: ({ value }) => ({ id: value.orderId }),
});

recursiveSources.materialized({
  client: "recursive",
  method: "streamRecursive",
  // @ts-expect-error recursive request values reject visible extras at every depth.
  request: () => recursiveCandidate,
  map: ({ value }) => ({ id: value.orderId }),
});

recursiveSources.materialized({
  client: "recursive",
  method: "streamRecursive",
  // @ts-expect-error recursive request values keep generated scalar types at cycle boundaries.
  request: () => recursiveWrongScalar,
  map: ({ value }) => ({ id: value.orderId }),
});

recursiveSources.materialized({
  client: "recursive",
  method: "streamRecursive",
  // @ts-expect-error recursive request values keep generated message types at cycle boundaries.
  request: () => recursiveWrongMessage,
  map: ({ value }) => ({ id: value.orderId }),
});

recursiveSources.materialized({
  client: "recursive",
  method: "streamRecursive",
  request: () => recursiveValid,
  map: ({ value }) => ({ id: value.orderId }),
});

recursiveSources.materialized({
  client: "recursive",
  method: "streamRecursive",
  // @ts-expect-error recursive aliases retain exact optional nested message fields at cycle boundaries.
  request: () => recursiveNestedExtra,
  map: ({ value }) => ({ id: value.orderId }),
});

recursiveSources.materialized({
  client: "recursive",
  method: "streamRecursive",
  // @ts-expect-error distinct recursive alias chains retain exact nested message fields.
  request: () => recursiveAliasChainExtra,
  map: ({ value }) => ({ id: value.orderId }),
});

// @ts-expect-error the entire materialized request callback must not erase to any.
sources.materialized({
  client: "orders",
  method: "streamOrders",
  request: erasedCallback,
  map: ({ value }: { readonly value: OrderEventMessage }) => ({ id: value.orderId }),
});

// @ts-expect-error the entire materialized Mapping callback must not erase to any.
sources.materialized({
  client: "orders",
  method: "streamOrders",
  request: () => ({ region: "all" }),
  map: erasedCallback,
});

sources.materialized({
  // @ts-expect-error unary methods do not have a gRPC Source client
  client: "orders",
  // @ts-expect-error unary methods are not Source streams
  method: "getOrder",
  // @ts-expect-error unary methods have no Source request factory
  request: () => ({ region: "all" }),
  // @ts-expect-error unary methods have no Source Mapping value
  map: ({ value }) => ({ id: value.orderId }),
});

sources.materialized({
  // @ts-expect-error cross-client methods do not have a matching logical client
  client: "orders",
  // @ts-expect-error method belongs to another selected client
  method: "streamStrategies",
  // @ts-expect-error cross-client methods have no matching request
  request: () => ({ region: "all" }),
  // @ts-expect-error cross-client methods have no matching response
  map: ({ value }) => ({ id: value.orderId }),
});

sources.materialized({
  client: "orders",
  method: "streamOrders",
  // @ts-expect-error recursively extra request field
  request: () => ({ region: "all", filter: { status: "open", extra: true } }),
  map: ({ value }) => ({ id: value.orderId }),
});

sources.materialized({
  client: "orders",
  method: "streamOrders",
  // @ts-expect-error request values must retain generated request typing
  request: () => deliberatelyAny,
  map: ({ value }) => ({ id: value.orderId }),
});

sources.materialized({
  client: "orders",
  method: "streamOrders",
  // @ts-expect-error request from another generated method is recursively inexact
  request: () => ({ strategyId: "strategy" }),
  map: ({ value }) => ({ id: value.orderId }),
});

sources.materialized({
  client: "orders",
  method: "streamOrders",
  // @ts-expect-error nested generated scalar types remain exact
  request: () => ({ filter: { status: 1 } }),
  map: ({ value }) => ({ id: value.orderId }),
});

sources.materialized({
  client: "orders",
  method: "streamOrders",
  request: () => ({
    metadata: {
      enabled: true,
      nested: ["value", { count: 1 }, null],
    },
    selector: {
      case: "wrappedChoice",
      value: {
        value: "selected",
      },
    },
  }),
  map: ({ value }) => ({ id: value.orderId }),
});

sources.materialized({
  client: "orders",
  method: "streamOrders",
  // @ts-expect-error wrapper messages inside oneofs remain wrapped request-init values.
  request: () => ({ selector: { case: "wrappedChoice", value: "selected" } }),
  map: ({ value }) => ({ id: value.orderId }),
});

sources.materialized({
  client: "orders",
  method: "streamOrders",
  // @ts-expect-error generated oneofs require one exact discriminated case
  request: () => ({ selector: {} }),
  map: ({ value }) => ({ id: value.orderId }),
});

sources.materialized({
  client: "orders",
  method: "streamOrders",
  // @ts-expect-error generated oneof values must match the selected case
  request: () => ({ selector: { case: "textChoice", value: { status: "open" } } }),
  map: ({ value }) => ({ id: value.orderId }),
});

sources.materialized({
  client: "orders",
  method: "streamOrders",
  // @ts-expect-error top-level extra request field
  request: () => ({ region: "all", extra: true }),
  map: ({ value }) => ({ id: value.orderId }),
});

sources.materialized({
  client: "orders",
  method: "streamOrders",
  // @ts-expect-error request factories are synchronous
  request: async () => ({ region: "all" }),
  map: ({ value }) => ({ id: value.orderId }),
});

sources.materialized({
  client: "orders",
  method: "streamOrders",
  // @ts-expect-error Effect is not a request-init object
  request: () => Effect.succeed({ region: "all" }),
  map: ({ value }) => ({ id: value.orderId }),
});

sources.materialized({
  client: "orders",
  method: "streamOrders",
  // @ts-expect-error Option is not a request-init object
  request: () => Option.some({ region: "all" }),
  map: ({ value }) => ({ id: value.orderId }),
});

sources.materialized({
  client: "orders",
  method: "streamOrders",
  // @ts-expect-error undefined is not a request-init object
  request: () => undefined,
  map: ({ value }) => ({ id: value.orderId }),
});

// @ts-expect-error Mapping must be synchronous
sources.materialized({
  client: "orders",
  method: "streamOrders",
  request: () => ({ region: "all" }),
  map: async ({ value }) => ({ id: value.orderId }),
});

// @ts-expect-error Mapping cannot return Effect
sources.materialized({
  client: "orders",
  method: "streamOrders",
  request: () => ({ region: "all" }),
  map: ({ value }) => Effect.succeed({ id: value.orderId }),
});

// @ts-expect-error Mapping cannot return Option
sources.materialized({
  client: "orders",
  method: "streamOrders",
  request: () => ({ region: "all" }),
  map: ({ value }) => Option.some({ id: value.orderId }),
});

// @ts-expect-error Mapping cannot mix a plain Row with Promise.
sources.materialized({
  client: "orders",
  method: "streamOrders",
  request: () => ({ region: "all" }),
  map: mixedPromiseMapping,
});

// @ts-expect-error Mapping cannot mix a plain Row with Effect.
sources.materialized({
  client: "orders",
  method: "streamOrders",
  request: () => ({ region: "all" }),
  map: mixedEffectMapping,
});

// @ts-expect-error Mapping cannot mix a plain Row with Option.
sources.materialized({
  client: "orders",
  method: "streamOrders",
  request: () => ({ region: "all" }),
  map: mixedOptionMapping,
});

sources.materialized({
  client: "orders",
  method: "streamOrders",
  request: () => ({ region: "all" }),
  // @ts-expect-error Mapping cannot return undefined
  map: () => undefined,
});

// @ts-expect-error Mapping must not erase its result to any
sources.materialized({
  client: "orders",
  method: "streamOrders",
  request: () => ({ region: "all" }),
  map: () => deliberatelyAny,
});

sources.materialized({
  client: "orders",
  method: "streamOrders",
  request: () => ({ region: "all" }),
  map: (input) => {
    // @ts-expect-error Mapping receives no client metadata
    void input.client;
    // @ts-expect-error Mapping receives no request object
    void input.request;
    // @ts-expect-error Mapping receives no session
    void input.session;
    return {
      id: input.value.orderId,
    };
  },
});

sources.materialized({
  client: "orders",
  method: "streamOrders",
  request: () => ({ region: "all" }),
  map: ({ value }) => ({ id: value.orderId }),
  // @ts-expect-error Source Definition inputs are exact
  session: "browser-session",
});

const materializedVariableWithExtra = {
  client: "orders" as const,
  method: "streamOrders" as const,
  request: () => ({ region: "all" }),
  map: ({ value }: { readonly value: OrderEventMessage }) => ({ id: value.orderId }),
  session: "browser-session",
};
// @ts-expect-error Source Definition variables are exact.
sources.materialized(materializedVariableWithExtra);

// @ts-expect-error the entire Materialized input must not erase to any.
sources.materialized(deliberatelyAny);

// @ts-expect-error Materialized logical-client selection must not erase to any.
sources.materialized({
  client: deliberatelyAny,
  method: "streamOrders",
  request: () => ({ region: "all" }),
  map: ({ value }: { readonly value: OrderEventMessage }) => ({ id: value.orderId }),
});

// @ts-expect-error Materialized method selection must not erase to any.
sources.materialized({
  client: "orders",
  method: deliberatelyAny,
  request: () => ({ region: "all" }),
  map: ({ value }: { readonly value: OrderEventMessage }) => ({ id: value.orderId }),
});

sources.leased({
  client: "orders",
  method: "streamOrders",
  // @ts-expect-error leased Route Fields must be non-empty
  routeBy: [],
  request: () => ({ region: "all" }),
  map: ({ value }) => ({
    id: value.orderId,
    price: value.price,
    region: value.region,
  }),
});

sources.leased({
  // @ts-expect-error unknown logical client
  client: "unknown",
  // @ts-expect-error an unknown client has no selectable methods
  method: "streamOrders",
  // @ts-expect-error an unknown client has no Route Fields
  routeBy: ["region"],
  // @ts-expect-error an unknown client has no generated request
  request: () => ({ region: "all" }),
  // @ts-expect-error an unknown client has no generated response value
  map: ({ value }) => ({ id: value.orderId }),
});

// @ts-expect-error Leased logical-client selection must not erase to any.
sources.leased({
  client: deliberatelyAny,
  method: "streamOrders",
  routeBy: ["region"],
  request: () => ({ region: "all" }),
  map: ({ value }: { readonly value: OrderEventMessage }) => ({
    id: value.orderId,
    region: value.region,
  }),
});

// @ts-expect-error Leased method selection must not erase to any.
sources.leased({
  client: "orders",
  method: deliberatelyAny,
  routeBy: ["region"],
  request: () => ({ region: "all" }),
  map: ({ value }: { readonly value: OrderEventMessage }) => ({
    id: value.orderId,
    region: value.region,
  }),
});

sources.leased({
  // @ts-expect-error unary methods do not have a gRPC Source client
  client: "orders",
  // @ts-expect-error unary methods are not Source streams
  method: "getOrder",
  // @ts-expect-error unary methods have no leased Route Fields
  routeBy: ["region"],
  // @ts-expect-error unary methods have no Source request factory
  request: () => ({ region: "all" }),
  // @ts-expect-error unary methods have no Source Mapping value
  map: ({ value }) => ({ id: value.orderId }),
});

sources.leased({
  // @ts-expect-error cross-client methods do not have a matching logical client
  client: "orders",
  // @ts-expect-error method belongs to another selected client
  method: "streamStrategies",
  // @ts-expect-error cross-client methods have no leased Route Fields
  routeBy: ["region"],
  // @ts-expect-error cross-client methods have no matching request
  request: () => ({ region: "all" }),
  // @ts-expect-error cross-client methods have no matching response
  map: ({ value }) => ({ id: value.orderId }),
});

// @ts-expect-error the entire leased request callback must not erase to any.
sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  request: erasedCallback,
  map: ({ value }: { readonly value: OrderEventMessage }) => ({
    id: value.orderId,
    region: value.region,
  }),
});

// @ts-expect-error the entire leased Mapping callback must not erase to any.
sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  request: (route: { readonly region: string }) => ({ region: route.region }),
  map: erasedCallback,
});

const leasedVariableWithExtra = {
  client: "orders" as const,
  method: "streamOrders" as const,
  routeBy: ["region"] as const,
  request: (route: { readonly region: string }) => ({ region: route.region }),
  map: ({
    value,
    route,
  }: {
    readonly value: OrderEventMessage;
    readonly route: { readonly region: string };
  }) => ({
    id: value.orderId,
    price: value.price,
    region: route.region,
  }),
  session: "browser-session",
};
// @ts-expect-error Leased Source Definition variables are exact.
sources.leased(leasedVariableWithExtra);

// @ts-expect-error the entire Leased input must not erase to any.
sources.leased(deliberatelyAny);

sources.leased({
  client: "orders",
  method: "streamOrders",
  // @ts-expect-error Leased Route Fields must not erase to any.
  routeBy: deliberatelyAny,
  request: () => ({ region: "all" }),
  map: ({ value }: { readonly value: OrderEventMessage }) => ({
    id: value.orderId,
    price: value.price,
    region: value.region,
  }),
});

sources.leased({
  client: "orders",
  method: "streamOrders",
  // @ts-expect-error Leased Route Field items must not erase to any.
  routeBy: [deliberatelyAny],
  request: () => ({ region: "all" }),
  map: ({ value }: { readonly value: OrderEventMessage }) => ({
    id: value.orderId,
    price: value.price,
    region: value.region,
  }),
});

sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  // @ts-expect-error Leased request factories reject top-level extras.
  request: (route) => ({ region: route.region, extra: true }),
  map: ({ value }) => ({
    id: value.orderId,
    price: value.price,
    region: value.region,
  }),
});

sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  // @ts-expect-error Leased request factories reject nested extras.
  request: (route) => ({
    region: route.region,
    filter: {
      status: "open",
      extra: true,
    },
  }),
  map: ({ value }) => ({
    id: value.orderId,
    price: value.price,
    region: value.region,
  }),
});

sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  // @ts-expect-error Leased request factories reject erased any results.
  request: () => deliberatelyAny,
  map: ({ value }) => ({
    id: value.orderId,
    price: value.price,
    region: value.region,
  }),
});

sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  // @ts-expect-error Leased request factories are synchronous.
  request: async (route) => ({ region: route.region }),
  map: ({ value }) => ({ id: value.orderId, region: value.region }),
});

sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  // @ts-expect-error Effect is not a leased request-init object.
  request: (route) => Effect.succeed({ region: route.region }),
  map: ({ value }) => ({ id: value.orderId, region: value.region }),
});

sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  // @ts-expect-error Option is not a leased request-init object.
  request: (route) => Option.some({ region: route.region }),
  map: ({ value }) => ({ id: value.orderId, region: value.region }),
});

sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  // @ts-expect-error undefined is not a leased request-init object.
  request: () => undefined,
  map: ({ value }) => ({ id: value.orderId, region: value.region }),
});

sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  // @ts-expect-error leased request values must retain generated scalar typing.
  request: () => ({ filter: { status: 1 } }),
  map: ({ value }) => ({ id: value.orderId, region: value.region }),
});

sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  // @ts-expect-error a request from another generated method is inexact.
  request: () => ({ strategyId: "strategy" }),
  map: ({ value }) => ({ id: value.orderId, region: value.region }),
});

sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  // @ts-expect-error generated oneofs require one exact discriminated case
  request: () => ({ selector: {} }),
  map: ({ value }) => ({
    id: value.orderId,
    region: value.region,
  }),
});

sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  // @ts-expect-error generated oneof values must match the selected case
  request: () => ({ selector: { case: "filterChoice", value: "open" } }),
  map: ({ value }) => ({
    id: value.orderId,
    region: value.region,
  }),
});

sources.leased({
  client: "orders",
  method: "streamOrders",
  // @ts-expect-error duplicate Route Fields are invalid
  routeBy: ["region", "region"],
  // @ts-expect-error duplicate Route Fields make the Request Route invalid too
  request: (route: { readonly region: string }) => ({ region: route.region }),
  map: ({ value }) => ({
    id: value.orderId,
    price: value.price,
    region: value.region,
  }),
});

sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  request: (route: { readonly region: string }) => ({ region: route.region }),
  map: (input) => {
    // @ts-expect-error leased Mapping receives no client metadata
    void input.client;
    // @ts-expect-error leased Mapping receives no reference count
    void input.referenceCount;
    // @ts-expect-error leased Mapping receives no transport options
    void input.transport;
    return {
      id: input.value.orderId,
      price: input.value.price,
      region: input.value.region,
    };
  },
});

// @ts-expect-error leased Mapping must not erase its result to any
sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  request: () => ({ region: "all" }),
  map: () => deliberatelyAny,
});

// @ts-expect-error leased Mapping must be synchronous
sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  request: (route: { readonly region: string }) => ({ region: route.region }),
  map: async ({ value }) => ({ id: value.orderId, region: value.region }),
});

// @ts-expect-error leased Mapping cannot return Effect
sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  request: (route: { readonly region: string }) => ({ region: route.region }),
  map: ({ value }) => Effect.succeed({ id: value.orderId, region: value.region }),
});

// @ts-expect-error leased Mapping cannot return Option
sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  request: (route: { readonly region: string }) => ({ region: route.region }),
  map: ({ value }) => Option.some({ id: value.orderId, region: value.region }),
});

// @ts-expect-error leased Mapping cannot mix a plain Row with Promise.
sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  request: (route: { readonly region: string }) => ({ region: route.region }),
  map: mixedPromiseMapping,
});

// @ts-expect-error leased Mapping cannot mix a plain Row with Effect.
sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  request: (route: { readonly region: string }) => ({ region: route.region }),
  map: mixedEffectMapping,
});

// @ts-expect-error leased Mapping cannot mix a plain Row with Option.
sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  request: (route: { readonly region: string }) => ({ region: route.region }),
  map: mixedOptionMapping,
});

sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  request: (route: { readonly region: string }) => ({ region: route.region }),
  // @ts-expect-error leased Mapping cannot return undefined
  map: () => undefined,
});

const missingMappedField = sources.materialized({
  client: "orders",
  method: "streamOrders",
  request: () => ({ region: "all" }),
  map: ({ value }) => ({
    id: value.orderId,
    region: value.region,
  }),
});
// @ts-expect-error Mapping result must contain the complete Topic Row
defineViewServerConfig({
  topics: {
    invalid: {
      schema: Row,
      source: missingMappedField,
    },
  },
});

const extraMappedField = sources.materialized({
  client: "orders",
  method: "streamOrders",
  request: () => ({ region: "all" }),
  map: ({ value }) => ({
    id: value.orderId,
    price: value.price,
    region: value.region,
    extra: true,
  }),
});
// @ts-expect-error Mapping result cannot exceed the exact Topic Row
defineViewServerConfig({
  topics: {
    invalid: {
      schema: Row,
      source: extraMappedField,
    },
  },
});

const incompatibleMappedField = sources.materialized({
  client: "orders",
  method: "streamOrders",
  request: () => ({ region: "all" }),
  map: ({ value }) => ({
    id: value.orderId,
    price: String(value.price),
    region: value.region,
  }),
});
// @ts-expect-error Mapping result field types must match the Topic Row
defineViewServerConfig({
  topics: {
    invalid: {
      schema: Row,
      source: incompatibleMappedField,
    },
  },
});

const missingLeasedMappedField = sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  request: (route) => ({ region: route.region }),
  map: ({ value }) => ({
    id: value.orderId,
    region: value.region,
  }),
});
// @ts-expect-error leased Mapping result must contain the complete Topic Row
defineViewServerConfig({
  topics: {
    invalid: {
      schema: Row,
      source: missingLeasedMappedField,
    },
  },
});

const extraLeasedMappedField = sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  request: (route) => ({ region: route.region }),
  map: ({ value }) => ({
    id: value.orderId,
    price: value.price,
    region: value.region,
    extra: true,
  }),
});
// @ts-expect-error leased Mapping result cannot exceed the exact Topic Row
defineViewServerConfig({
  topics: {
    invalid: {
      schema: Row,
      source: extraLeasedMappedField,
    },
  },
});

const incompatibleLeasedMappedField = sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["region"],
  request: (route) => ({ region: route.region }),
  map: ({ value }) => ({
    id: value.orderId,
    price: String(value.price),
    region: value.region,
  }),
});
// @ts-expect-error leased Mapping result field types must match the Topic Row
defineViewServerConfig({
  topics: {
    invalid: {
      schema: Row,
      source: incompatibleLeasedMappedField,
    },
  },
});

const unknownRouteField = sources.leased({
  client: "orders",
  method: "streamOrders",
  routeBy: ["unknown"],
  request: () => ({ region: "all" }),
  map: ({ value }) => ({
    id: value.orderId,
    price: value.price,
    region: value.region,
  }),
});
// @ts-expect-error Route Fields must be exact Topic Row fields
defineViewServerConfig({
  topics: {
    invalid: {
      schema: Row,
      source: unknownRouteField,
    },
  },
});

const sourceFreeViewServer = defineViewServerConfig({
  topics: {
    sourceFreeOrders: {
      schema: Row,
      key: "id",
    },
  },
});

// @ts-expect-error gRPC Node composition requires at least one gRPC Source Definition.
grpcNode.layer(sourceFreeViewServer, {});

// @ts-expect-error Config-backed gRPC Node composition requires at least one gRPC Source Definition.
grpcNode.layerConfig(sourceFreeViewServer, Config.succeed({}));

// @ts-expect-error aggregate Layer rejects extra logical clients
grpcNode.layer(viewServer, {
  orders: {
    baseUrl: "https://orders.example",
  },
  strategies: {
    baseUrl: "https://strategies.example",
  },
});

// @ts-expect-error Config-backed aggregate client options reject extra fields.
grpcNode.layerConfig(viewServer, {
  orders: {
    baseUrl: Config.string("ORDERS_GRPC_URL"),
    headers: Config.string("ORDERS_GRPC_HEADERS"),
  },
});

// @ts-expect-error aggregate Layer requires every referenced logical client
grpcNode.layer(viewServer, {});

// @ts-expect-error aggregate client options reject extra fields
grpcNode.layer(viewServer, {
  orders: {
    baseUrl: "https://orders.example",
    headers: {
      authorization: "not-a-browser-header",
    },
  },
});
