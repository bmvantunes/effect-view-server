import type {
  DescMessage,
  DescMethodServerStreaming,
  DescService,
  MessageInitShape,
  MessageShape,
} from "@bufbuild/protobuf";
import type { Client } from "@connectrpc/connect";
import type { Config, Effect, Stream } from "effect";
import type {
  FieldKey,
  RowFromSchema,
  RowSchema,
  StringFieldKey,
  TopicDefinition,
  TopicRow,
} from "./query-core";
import type { RejectExtraKeys } from "./query-exact";
import type { TopicRouteByTuple } from "./source-query-contract";
import type {
  NonEmptyRouteBy,
  TopicLeasedSourceDefinition,
  TopicMaterializedSourceDefinition,
} from "./source-contract";

const GrpcTopicSourceTypeId: unique symbol = Symbol("@effect-view-server/config/GrpcTopicSource");
const GrpcTopicSourceClientsTypeId: unique symbol = Symbol(
  "@effect-view-server/config/GrpcTopicSourceClients",
);
const GrpcTopicSourceKeyTypeId: unique symbol = Symbol(
  "@effect-view-server/config/GrpcTopicSourceKey",
);
const GrpcTopicSourceSchemaTypeId: unique symbol = Symbol(
  "@effect-view-server/config/GrpcTopicSourceSchema",
);
const GrpcFeedDefinitionTypeId: unique symbol = Symbol(
  "@effect-view-server/config/GrpcFeedDefinition",
);
const GrpcFeedMapTypeId: unique symbol = Symbol("@effect-view-server/config/GrpcFeedMap");
const GrpcFeedRequestTypeId: unique symbol = Symbol("@effect-view-server/config/GrpcFeedRequest");

class GrpcTopicSourceClientsBrand<Clients extends GrpcRuntimeClients> {
  declare readonly [GrpcTopicSourceClientsTypeId]: Clients;
}

const grpcFeedMapBrand = { [GrpcFeedMapTypeId]: true } as const;
const grpcFeedRequestBrand = { [GrpcFeedRequestTypeId]: true } as const;

const brandGrpcFeedMap = <Mapping extends (...args: ReadonlyArray<never>) => unknown>(
  mapping: Mapping,
) => Object.assign(mapping, grpcFeedMapBrand);

const brandGrpcFeedRequest = <Request extends (...args: ReadonlyArray<never>) => unknown>(
  request: Request,
) => Object.assign(request, grpcFeedRequestBrand);

export type GrpcTopicSourceLifecycle = "materialized" | "leased";

export type GrpcMaterializedTopicSource = TopicMaterializedSourceDefinition & {
  readonly _tag: "GrpcMaterializedTopicSource";
  readonly [GrpcTopicSourceTypeId]: true;
  readonly kind: "grpc";
  readonly lifecycle: "materialized";
};

export type GrpcLeasedTopicSource<RouteBy extends ReadonlyArray<string> = ReadonlyArray<string>> =
  TopicLeasedSourceDefinition<RouteBy> & {
    readonly _tag: "GrpcLeasedTopicSource";
    readonly [GrpcTopicSourceTypeId]: true;
    readonly kind: "grpc";
    readonly lifecycle: "leased";
    readonly routeBy: RouteBy;
  };

export type GrpcTopicSource = GrpcMaterializedTopicSource | GrpcLeasedTopicSource;

type ExactObject<Candidate, Shape> = Candidate & Shape & RejectExtraKeys<Candidate, Shape>;
type IsAny<Value> = 0 extends 1 & Value ? true : false;
type NonUndefined<Value> = Exclude<Value, undefined>;
type PublicGrpcMessageInitShape<Value> =
  NonUndefined<Value> extends ReadonlyArray<infer Item>
    ? ReadonlyArray<PublicGrpcMessageInitShape<Item>>
    : NonUndefined<Value> extends object
      ? {
          readonly [Key in keyof NonUndefined<Value> as Key extends "$typeName" | "$unknown"
            ? never
            : Key]?: PublicGrpcMessageInitShape<NonUndefined<Value>[Key]>;
        }
      : Value;
type DeepGrpcRequestMismatch<Candidate, Shape> =
  IsAny<Candidate> extends true
    ? true
    : NonUndefined<Candidate> extends ReadonlyArray<infer CandidateItem>
      ? NonUndefined<Shape> extends ReadonlyArray<infer ShapeItem>
        ? DeepGrpcRequestMismatch<CandidateItem, ShapeItem>
        : false
      : NonUndefined<Candidate> extends object
        ? NonUndefined<Shape> extends object
          ? Exclude<keyof NonUndefined<Candidate>, keyof NonUndefined<Shape>> extends never
            ? true extends {
                readonly [Key in Extract<
                  keyof NonUndefined<Candidate>,
                  keyof NonUndefined<Shape>
                >]: DeepGrpcRequestMismatch<NonUndefined<Candidate>[Key], NonUndefined<Shape>[Key]>;
              }[Extract<keyof NonUndefined<Candidate>, keyof NonUndefined<Shape>>]
              ? true
              : false
            : true
          : false
        : false;
type TypeEquals<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
    ? (<T>() => T extends B ? 1 : 2) extends <T>() => T extends A ? 1 : 2
      ? true
      : false
    : false;

type ExactGrpcLeasedTopicSourceInput<Input> = Input &
  RejectExtraKeys<
    Input,
    {
      readonly routeBy: NonEmptyRouteBy;
    }
  >;

export type GrpcMaterializedTopic<Topics> = Extract<
  {
    readonly [Topic in keyof Topics]: Topics[Topic] extends {
      readonly grpcSource: GrpcMaterializedTopicSource;
    }
      ? Topic
      : never;
  }[keyof Topics],
  string
>;

export type GrpcLeasedTopic<Topics> = Extract<
  {
    readonly [Topic in keyof Topics]: [TopicRouteByTuple<Topics, Topic>] extends [never]
      ? never
      : TopicRouteByTuple<Topics, Topic> extends NonEmptyRouteBy
        ? Topic
        : never;
  }[keyof Topics],
  string
>;

type GrpcLeasedRouteBy<Topics, Topic extends GrpcLeasedTopic<Topics>> =
  TopicRouteByTuple<Topics, Topic> extends NonEmptyRouteBy
    ? TopicRouteByTuple<Topics, Topic>
    : never;

type RouteShape<
  Topics extends Record<string, { readonly schema: RowSchema }>,
  Topic extends Extract<keyof Topics, string>,
  RouteBy extends ReadonlyArray<string>,
> = Pick<TopicRow<Topics, Topic>, Extract<RouteBy[number], FieldKey<TopicRow<Topics, Topic>>>>;

type RouteShapeForRow<Row, RouteBy extends ReadonlyArray<string>> = Pick<
  Row,
  Extract<RouteBy[number], FieldKey<Row>>
>;
type NonEmptyRouteByForRow<Row> = readonly [FieldKey<Row>, ...ReadonlyArray<FieldKey<Row>>];

type ExactGrpcFeedMap<
  Input,
  Row,
  Mapping extends (...args: ReadonlyArray<never>) => unknown,
> = Mapping extends (input: Input) => infer Output
  ? IsAny<Output> extends true
    ? never
    : [Output] extends [never]
      ? Mapping
      : Output extends ExactObject<Output, Row>
        ? Mapping
        : never
  : never;

type ExactGrpcRequest<
  Args extends Array<unknown>,
  RequestShape,
  Request extends (...args: Args) => unknown,
> = Request extends (...args: Args) => infer Output
  ? IsAny<Output> extends true
    ? never
    : [Output] extends [never]
      ? Request
      : Output extends ExactObject<Output, RequestShape>
        ? DeepGrpcRequestMismatch<Output, RequestShape> extends true
          ? never
          : Request
        : never
  : never;

type GrpcFeedRequestDefinition<
  Args extends Array<unknown>,
  RequestShape,
  Request extends (...args: Args) => unknown,
> = ExactGrpcRequest<Args, RequestShape, Request> & {
  readonly [GrpcFeedRequestTypeId]: true;
};

type GrpcFeedMapDefinition<
  Input,
  Row,
  Mapping extends (input: Input) => unknown,
> = ExactGrpcFeedMap<Input, Row, Mapping> & {
  readonly [GrpcFeedMapTypeId]: true;
};

export type GrpcHelper = {
  readonly topicSources: <const Clients extends GrpcRuntimeClients>(
    clients: Clients,
  ) => GrpcTopicSourceHelper<Clients>;
  readonly connectClient: <
    const Input extends {
      readonly service: DescService;
      readonly baseUrl: GrpcRuntimeValue<string>;
    },
  >(
    input: ExactGrpcConnectClientInput<Input>,
  ) => GrpcConnectClientDefinition<Input["service"]>;
};

export type GrpcTopicSourceMarkerHelper = {
  readonly materialized: () => GrpcMaterializedTopicSource;
  readonly leased: <const Input extends { readonly routeBy: NonEmptyRouteBy }>(
    input: ExactGrpcLeasedTopicSourceInput<Input>,
  ) => GrpcLeasedTopicSource<Input["routeBy"]>;
};

export type GrpcRuntimeValue<A> = A | Config.Config<A>;

export type GrpcConnectClientDefinition<Service extends DescService = DescService> = {
  readonly _tag: "GrpcConnectClientDefinition";
  readonly service: Service;
  readonly baseUrl: GrpcRuntimeValue<string>;
  readonly protocol: "grpc";
};

export type GrpcRuntimeClients = Record<string, GrpcConnectClientDefinition>;

export type GrpcClientDefinitionService<ClientDefinition extends GrpcConnectClientDefinition> =
  ClientDefinition extends GrpcConnectClientDefinition<infer Service> ? Service : never;

export type GrpcClientValue<ClientDefinition extends GrpcConnectClientDefinition> = Client<
  GrpcClientDefinitionService<ClientDefinition>
>;

export type GrpcServerStreamingMethodName<ClientDefinition> =
  ClientDefinition extends GrpcConnectClientDefinition<infer Service>
    ? DescService extends Service
      ? string
      : {
          readonly [MethodName in keyof Service["method"]]: Service["method"][MethodName] extends DescMethodServerStreaming<
            infer _Input extends DescMessage,
            infer _Output extends DescMessage
          >
            ? MethodName
            : never;
        }[keyof Service["method"]] &
          string
    : never;

export type GrpcMethodRequest<
  ClientDefinition,
  MethodName extends GrpcServerStreamingMethodName<ClientDefinition>,
> =
  ClientDefinition extends GrpcConnectClientDefinition<infer Service>
    ? DescService extends Service
      ? unknown
      : Service["method"][MethodName] extends DescMethodServerStreaming<
            infer Input extends DescMessage,
            infer _Output extends DescMessage
          >
        ? MessageInitShape<Input>
        : never
    : never;

export type GrpcMethodValue<
  ClientDefinition,
  MethodName extends GrpcServerStreamingMethodName<ClientDefinition>,
> =
  ClientDefinition extends GrpcConnectClientDefinition<infer Service>
    ? DescService extends Service
      ? unknown
      : Service["method"][MethodName] extends DescMethodServerStreaming<
            infer _Input extends DescMessage,
            infer Output extends DescMessage
          >
        ? MessageShape<Output>
        : never
    : never;

type GrpcMethodRequestExactShape<
  ClientDefinition,
  MethodName extends GrpcServerStreamingMethodName<ClientDefinition>,
> =
  ClientDefinition extends GrpcConnectClientDefinition<infer Service>
    ? DescService extends Service
      ? Record<string, unknown>
      : Service["method"][MethodName] extends DescMethodServerStreaming<
            infer Input extends DescMessage,
            infer _Output extends DescMessage
          >
        ? PublicGrpcMessageInitShape<MessageInitShape<Input>>
        : never
    : never;

export type GrpcFeedSession = {
  readonly id: string | null;
  readonly forwardedHeaders: Readonly<Record<string, string>>;
  readonly systemHeaders: Readonly<Record<string, string>>;
};

export type GrpcFeedAcquireInput<ClientValue, Request, Route> = {
  readonly client: ClientValue;
  readonly request: Request;
  readonly route: Route;
  readonly session: GrpcFeedSession;
};

export type GrpcFeedReleaseInput<ClientValue, Request, Route> = GrpcFeedAcquireInput<
  ClientValue,
  Request,
  Route
>;

export type GrpcFeedMapInput<Value, Route, SchemaValue extends RowSchema> = {
  readonly value: Value;
  readonly route: Route;
  readonly schema: SchemaValue;
};

type ExactGrpcMaterializedTopicInput<
  Clients extends GrpcRuntimeClients,
  SchemaValue extends RowSchema,
  Key extends StringFieldKey<RowFromSchema<SchemaValue>>,
  ClientName extends Extract<keyof Clients, string>,
  MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
  Request extends () => GrpcMethodRequestExactShape<Clients[ClientName], MethodName>,
  Mapping extends (
    input: GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      undefined,
      SchemaValue
    >,
  ) => RowFromSchema<SchemaValue>,
> = {
  readonly schema: SchemaValue;
  readonly key: Key;
  readonly client: ClientName;
  readonly method: MethodName;
  readonly request: ExactGrpcRequest<
    [],
    GrpcMethodRequestExactShape<Clients[ClientName], MethodName>,
    Request
  >;
  readonly acquire: (
    input: GrpcFeedAcquireInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      undefined
    >,
  ) => Stream.Stream<GrpcMethodValue<Clients[ClientName], MethodName>, unknown, never>;
  readonly release?: (
    input: GrpcFeedReleaseInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      undefined
    >,
  ) => Effect.Effect<void, unknown, never>;
  readonly map: ExactGrpcFeedMap<
    GrpcFeedMapInput<GrpcMethodValue<Clients[ClientName], MethodName>, undefined, SchemaValue>,
    RowFromSchema<SchemaValue>,
    Mapping
  >;
};

type ExactGrpcLeasedTopicInput<
  Clients extends GrpcRuntimeClients,
  SchemaValue extends RowSchema,
  Key extends StringFieldKey<RowFromSchema<SchemaValue>>,
  RouteBy extends NonEmptyRouteByForRow<RowFromSchema<SchemaValue>>,
  ClientName extends Extract<keyof Clients, string>,
  MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
  Request extends (
    route: RouteShapeForRow<RowFromSchema<SchemaValue>, RouteBy>,
  ) => GrpcMethodRequestExactShape<Clients[ClientName], MethodName>,
  Mapping extends (
    input: GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      RouteShapeForRow<RowFromSchema<SchemaValue>, RouteBy>,
      SchemaValue
    >,
  ) => RowFromSchema<SchemaValue>,
> = {
  readonly schema: SchemaValue;
  readonly key: Key;
  readonly client: ClientName;
  readonly method: MethodName;
  readonly routeBy: RouteBy;
  readonly request: ExactGrpcRequest<
    [route: RouteShapeForRow<RowFromSchema<SchemaValue>, RouteBy>],
    GrpcMethodRequestExactShape<Clients[ClientName], MethodName>,
    Request
  >;
  readonly acquire: (
    input: GrpcFeedAcquireInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      RouteShapeForRow<RowFromSchema<SchemaValue>, RouteBy>
    >,
  ) => Stream.Stream<GrpcMethodValue<Clients[ClientName], MethodName>, unknown, never>;
  readonly release?: (
    input: GrpcFeedReleaseInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      RouteShapeForRow<RowFromSchema<SchemaValue>, RouteBy>
    >,
  ) => Effect.Effect<void, unknown, never>;
  readonly map: ExactGrpcFeedMap<
    GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      RouteShapeForRow<RowFromSchema<SchemaValue>, RouteBy>,
      SchemaValue
    >,
    RowFromSchema<SchemaValue>,
    Mapping
  >;
};

export type GrpcMaterializedTopicSourceDefinition<
  Clients extends GrpcRuntimeClients,
  SchemaValue extends RowSchema,
  Key extends StringFieldKey<RowFromSchema<SchemaValue>>,
  ClientName extends Extract<keyof Clients, string>,
  MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
  Request extends () => GrpcMethodRequestExactShape<Clients[ClientName], MethodName>,
  Mapping extends (
    input: GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      undefined,
      SchemaValue
    >,
  ) => RowFromSchema<SchemaValue>,
> = GrpcMaterializedTopicSource & {
  readonly [GrpcTopicSourceClientsTypeId]: Clients;
  readonly [GrpcTopicSourceKeyTypeId]: Key;
  readonly [GrpcTopicSourceSchemaTypeId]: SchemaValue;
  readonly client: ClientName;
  readonly method: MethodName;
  readonly request: GrpcFeedRequestDefinition<
    [],
    GrpcMethodRequestExactShape<Clients[ClientName], MethodName>,
    Request
  >;
  readonly acquire: (
    input: GrpcFeedAcquireInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      undefined
    >,
  ) => Stream.Stream<GrpcMethodValue<Clients[ClientName], MethodName>, unknown, never>;
  readonly release?: (
    input: GrpcFeedReleaseInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      undefined
    >,
  ) => Effect.Effect<void, unknown, never>;
  readonly map: GrpcFeedMapDefinition<
    GrpcFeedMapInput<GrpcMethodValue<Clients[ClientName], MethodName>, undefined, SchemaValue>,
    RowFromSchema<SchemaValue>,
    Mapping
  >;
};

export type GrpcLeasedTopicSourceDefinition<
  Clients extends GrpcRuntimeClients,
  SchemaValue extends RowSchema,
  Key extends StringFieldKey<RowFromSchema<SchemaValue>>,
  RouteBy extends NonEmptyRouteBy,
  ClientName extends Extract<keyof Clients, string>,
  MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
  Request extends (
    route: RouteShapeForRow<RowFromSchema<SchemaValue>, RouteBy>,
  ) => GrpcMethodRequestExactShape<Clients[ClientName], MethodName>,
  Mapping extends (
    input: GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      RouteShapeForRow<RowFromSchema<SchemaValue>, RouteBy>,
      SchemaValue
    >,
  ) => RowFromSchema<SchemaValue>,
> = GrpcLeasedTopicSource<RouteBy> & {
  readonly [GrpcTopicSourceClientsTypeId]: Clients;
  readonly [GrpcTopicSourceKeyTypeId]: Key;
  readonly [GrpcTopicSourceSchemaTypeId]: SchemaValue;
  readonly client: ClientName;
  readonly method: MethodName;
  readonly request: GrpcFeedRequestDefinition<
    [route: RouteShapeForRow<RowFromSchema<SchemaValue>, RouteBy>],
    GrpcMethodRequestExactShape<Clients[ClientName], MethodName>,
    Request
  >;
  readonly acquire: (
    input: GrpcFeedAcquireInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      RouteShapeForRow<RowFromSchema<SchemaValue>, RouteBy>
    >,
  ) => Stream.Stream<GrpcMethodValue<Clients[ClientName], MethodName>, unknown, never>;
  readonly release?: (
    input: GrpcFeedReleaseInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      RouteShapeForRow<RowFromSchema<SchemaValue>, RouteBy>
    >,
  ) => Effect.Effect<void, unknown, never>;
  readonly map: GrpcFeedMapDefinition<
    GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      RouteShapeForRow<RowFromSchema<SchemaValue>, RouteBy>,
      SchemaValue
    >,
    RowFromSchema<SchemaValue>,
    Mapping
  >;
};

export type GrpcTopicSourceIsBoundToClients<
  Source,
  Clients extends GrpcRuntimeClients,
> = Source extends { readonly [GrpcTopicSourceClientsTypeId]: infer SourceClients }
  ? string extends keyof Clients
    ? true
    : TypeEquals<SourceClients, Clients>
  : false;

export type GrpcTopicSourceHasKey<Source, Key extends string> = Source extends {
  readonly [GrpcTopicSourceKeyTypeId]: infer SourceKey;
}
  ? TypeEquals<SourceKey, Key>
  : false;

export const grpcTopicSourceDefinitionKey = (source: object): string | undefined => {
  const key = Reflect.get(source, GrpcTopicSourceKeyTypeId);
  return typeof key === "string" ? key : undefined;
};

export const grpcTopicSourceDefinitionSchema = (source: object): object | undefined => {
  const schema = Reflect.get(source, GrpcTopicSourceSchemaTypeId);
  return typeof schema === "object" && schema !== null ? schema : undefined;
};

export type GrpcTopicSourceHelper<Clients extends GrpcRuntimeClients> = {
  readonly materialized: <
    const SchemaValue extends RowSchema,
    const Key extends StringFieldKey<RowFromSchema<SchemaValue>>,
    const ClientName extends Extract<keyof Clients, string>,
    const MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
    const Request extends () => GrpcMethodRequestExactShape<Clients[ClientName], MethodName>,
    const Mapping extends (
      input: GrpcFeedMapInput<
        GrpcMethodValue<Clients[ClientName], MethodName>,
        undefined,
        SchemaValue
      >,
    ) => RowFromSchema<SchemaValue>,
  >(
    input: ExactGrpcMaterializedTopicInput<
      Clients,
      SchemaValue,
      Key,
      ClientName,
      MethodName,
      Request,
      Mapping
    >,
  ) => TopicDefinition<SchemaValue, Key> & {
    readonly grpcSource: GrpcMaterializedTopicSourceDefinition<
      Clients,
      SchemaValue,
      Key,
      ClientName,
      MethodName,
      Request,
      Mapping
    >;
  };
  readonly leased: <
    const SchemaValue extends RowSchema,
    const Key extends StringFieldKey<RowFromSchema<SchemaValue>>,
    const RouteBy extends NonEmptyRouteByForRow<RowFromSchema<SchemaValue>>,
    const ClientName extends Extract<keyof Clients, string>,
    const MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
    const Request extends (
      route: RouteShapeForRow<RowFromSchema<SchemaValue>, RouteBy>,
    ) => GrpcMethodRequestExactShape<Clients[ClientName], MethodName>,
    const Mapping extends (
      input: GrpcFeedMapInput<
        GrpcMethodValue<Clients[ClientName], MethodName>,
        RouteShapeForRow<RowFromSchema<SchemaValue>, RouteBy>,
        SchemaValue
      >,
    ) => RowFromSchema<SchemaValue>,
  >(
    input: ExactGrpcLeasedTopicInput<
      Clients,
      SchemaValue,
      Key,
      RouteBy,
      ClientName,
      MethodName,
      Request,
      Mapping
    >,
  ) => TopicDefinition<SchemaValue, Key> & {
    readonly grpcSource: GrpcLeasedTopicSourceDefinition<
      Clients,
      SchemaValue,
      Key,
      RouteBy,
      ClientName,
      MethodName,
      Request,
      Mapping
    >;
  };
};

export type GrpcLeasedFeedDefinition<
  Topics extends Record<string, { readonly schema: RowSchema }>,
  Clients extends GrpcRuntimeClients,
  Topic extends Extract<keyof Topics, string>,
  ClientName extends Extract<keyof Clients, string>,
  RouteBy extends NonEmptyRouteBy,
  MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
  Mapping extends (
    input: GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      RouteShape<Topics, Topic, RouteBy>,
      Topics[Topic]["schema"]
    >,
  ) => TopicRow<Topics, Topic> = (
    input: GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      RouteShape<Topics, Topic, RouteBy>,
      Topics[Topic]["schema"]
    >,
  ) => TopicRow<Topics, Topic>,
> = {
  readonly _tag: "GrpcLeasedFeedDefinition";
  readonly [GrpcFeedDefinitionTypeId]: {
    readonly topic: Topic;
    readonly client: ClientName;
    readonly method: MethodName;
  };
  readonly lifecycle: "leased";
  readonly topic: Topic;
  readonly client: ClientName;
  readonly method: MethodName;
  readonly routeBy: RouteBy;
  readonly request: (route: RouteShape<Topics, Topic, RouteBy>) => unknown;
  readonly acquire: (
    input: GrpcFeedAcquireInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      RouteShape<Topics, Topic, RouteBy>
    >,
  ) => Stream.Stream<GrpcMethodValue<Clients[ClientName], MethodName>, unknown, never>;
  readonly release?: (
    input: GrpcFeedReleaseInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      RouteShape<Topics, Topic, RouteBy>
    >,
  ) => Effect.Effect<void, unknown, never>;
  readonly map: GrpcFeedMapDefinition<
    GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      RouteShape<Topics, Topic, RouteBy>,
      Topics[Topic]["schema"]
    >,
    TopicRow<Topics, Topic>,
    Mapping
  >;
};

export type GrpcMaterializedFeedDefinition<
  Topics extends Record<string, { readonly schema: RowSchema }>,
  Clients extends GrpcRuntimeClients,
  Topic extends GrpcMaterializedTopic<Topics>,
  ClientName extends Extract<keyof Clients, string>,
  MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
  Mapping extends (
    input: GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      undefined,
      Topics[Topic]["schema"]
    >,
  ) => TopicRow<Topics, Topic> = (
    input: GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      undefined,
      Topics[Topic]["schema"]
    >,
  ) => TopicRow<Topics, Topic>,
> = {
  readonly _tag: "GrpcMaterializedFeedDefinition";
  readonly [GrpcFeedDefinitionTypeId]: {
    readonly topic: Topic;
    readonly client: ClientName;
    readonly method: MethodName;
  };
  readonly lifecycle: "materialized";
  readonly topic: Topic;
  readonly client: ClientName;
  readonly method: MethodName;
  readonly request: () => unknown;
  readonly acquire: (
    input: GrpcFeedAcquireInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      undefined
    >,
  ) => Stream.Stream<GrpcMethodValue<Clients[ClientName], MethodName>, unknown, never>;
  readonly release?: (
    input: GrpcFeedReleaseInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      undefined
    >,
  ) => Effect.Effect<void, unknown, never>;
  readonly map: GrpcFeedMapDefinition<
    GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      undefined,
      Topics[Topic]["schema"]
    >,
    TopicRow<Topics, Topic>,
    Mapping
  >;
};

type ExactGrpcConnectClientInput<Input> = Input &
  RejectExtraKeys<
    Input,
    {
      readonly service: DescService;
      readonly baseUrl: GrpcRuntimeValue<string>;
    }
  >;

type ExactGrpcLeasedFeedInput<
  Topics extends Record<string, { readonly schema: RowSchema }>,
  Clients extends GrpcRuntimeClients,
  Topic extends Extract<keyof Topics, string>,
  ClientName extends Extract<keyof Clients, string>,
  RouteBy extends TopicRouteByTuple<Topics, Topic>,
  MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
  Request extends (
    route: RouteShape<Topics, Topic, RouteBy>,
  ) => GrpcMethodRequestExactShape<Clients[ClientName], MethodName>,
  Mapping extends (
    input: GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      RouteShape<Topics, Topic, RouteBy>,
      Topics[Topic]["schema"]
    >,
  ) => TopicRow<Topics, Topic>,
> = {
  readonly topic: Topic;
  readonly client: ClientName;
  readonly method: MethodName;
  readonly routeBy: RouteBy;
  readonly request: ExactGrpcRequest<
    [route: RouteShape<Topics, Topic, RouteBy>],
    GrpcMethodRequestExactShape<Clients[ClientName], MethodName>,
    Request
  >;
  readonly acquire: (
    input: GrpcFeedAcquireInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      RouteShape<Topics, Topic, RouteBy>
    >,
  ) => Stream.Stream<GrpcMethodValue<Clients[ClientName], MethodName>, unknown, never>;
  readonly release?: (
    input: GrpcFeedReleaseInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      RouteShape<Topics, Topic, RouteBy>
    >,
  ) => Effect.Effect<void, unknown, never>;
  readonly map: ExactGrpcFeedMap<
    GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      RouteShape<Topics, Topic, RouteBy>,
      Topics[Topic]["schema"]
    >,
    TopicRow<Topics, Topic>,
    Mapping
  >;
};

type ExactGrpcMaterializedFeedInput<
  Topics extends Record<string, { readonly schema: RowSchema }>,
  Clients extends GrpcRuntimeClients,
  Topic extends GrpcMaterializedTopic<Topics>,
  ClientName extends Extract<keyof Clients, string>,
  MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
  Request extends () => GrpcMethodRequestExactShape<Clients[ClientName], MethodName>,
  Mapping extends (
    input: GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      undefined,
      Topics[Topic]["schema"]
    >,
  ) => TopicRow<Topics, Topic>,
> = {
  readonly topic: Topic;
  readonly client: ClientName;
  readonly method: MethodName;
  readonly request: ExactGrpcRequest<
    [],
    GrpcMethodRequestExactShape<Clients[ClientName], MethodName>,
    Request
  >;
  readonly acquire: (
    input: GrpcFeedAcquireInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      undefined
    >,
  ) => Stream.Stream<GrpcMethodValue<Clients[ClientName], MethodName>, unknown, never>;
  readonly release?: (
    input: GrpcFeedReleaseInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      undefined
    >,
  ) => Effect.Effect<void, unknown, never>;
  readonly map: ExactGrpcFeedMap<
    GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      undefined,
      Topics[Topic]["schema"]
    >,
    TopicRow<Topics, Topic>,
    Mapping
  >;
};

export type GrpcFeedHelper<
  Topics extends Record<string, { readonly schema: RowSchema }>,
  Clients extends GrpcRuntimeClients,
> = {
  readonly leasedFeed: <
    const Topic extends GrpcLeasedTopic<Topics>,
    const ClientName extends Extract<keyof Clients, string>,
    const RouteBy extends GrpcLeasedRouteBy<Topics, Topic>,
    const MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
    const Request extends (
      route: RouteShape<Topics, Topic, RouteBy>,
    ) => GrpcMethodRequestExactShape<Clients[ClientName], MethodName>,
    const Mapping extends (
      input: GrpcFeedMapInput<
        GrpcMethodValue<Clients[ClientName], MethodName>,
        RouteShape<Topics, Topic, RouteBy>,
        Topics[Topic]["schema"]
      >,
    ) => TopicRow<Topics, Topic>,
  >(
    input: ExactGrpcLeasedFeedInput<
      Topics,
      Clients,
      Topic,
      ClientName,
      RouteBy,
      MethodName,
      Request,
      Mapping
    >,
  ) => GrpcLeasedFeedDefinition<Topics, Clients, Topic, ClientName, RouteBy, MethodName, Mapping>;
  readonly materializedFeed: <
    const Topic extends GrpcMaterializedTopic<Topics>,
    const ClientName extends Extract<keyof Clients, string>,
    const MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
    const Request extends () => GrpcMethodRequestExactShape<Clients[ClientName], MethodName>,
    const Mapping extends (
      input: GrpcFeedMapInput<
        GrpcMethodValue<Clients[ClientName], MethodName>,
        undefined,
        Topics[Topic]["schema"]
      >,
    ) => TopicRow<Topics, Topic>,
  >(
    input: ExactGrpcMaterializedFeedInput<
      Topics,
      Clients,
      Topic,
      ClientName,
      MethodName,
      Request,
      Mapping
    >,
  ) => GrpcMaterializedFeedDefinition<Topics, Clients, Topic, ClientName, MethodName, Mapping>;
};

export type AnyGrpcMaterializedFeedDefinition<
  Topics extends Record<string, { readonly schema: RowSchema }>,
  Clients extends GrpcRuntimeClients,
> = {
  readonly [Topic in GrpcMaterializedTopic<Topics>]: {
    readonly [ClientName in Extract<keyof Clients, string>]: {
      readonly [MethodName in GrpcServerStreamingMethodName<
        Clients[ClientName]
      >]: GrpcMaterializedFeedDefinition<Topics, Clients, Topic, ClientName, MethodName>;
    }[GrpcServerStreamingMethodName<Clients[ClientName]>];
  }[Extract<keyof Clients, string>];
}[GrpcMaterializedTopic<Topics>];

export type AnyGrpcLeasedFeedDefinition<
  Topics extends Record<string, { readonly schema: RowSchema }>,
  Clients extends GrpcRuntimeClients,
> = {
  readonly [Topic in GrpcLeasedTopic<Topics>]: {
    readonly [ClientName in Extract<keyof Clients, string>]: {
      readonly [MethodName in GrpcServerStreamingMethodName<
        Clients[ClientName]
      >]: GrpcLeasedFeedDefinition<
        Topics,
        Clients,
        Topic,
        ClientName,
        GrpcLeasedRouteBy<Topics, Topic>,
        MethodName
      >;
    }[GrpcServerStreamingMethodName<Clients[ClientName]>];
  }[Extract<keyof Clients, string>];
}[GrpcLeasedTopic<Topics>];

export type GrpcFeedDefinition<
  Topics extends Record<string, { readonly schema: RowSchema }>,
  Clients extends GrpcRuntimeClients,
> =
  | AnyGrpcMaterializedFeedDefinition<Topics, Clients>
  | AnyGrpcLeasedFeedDefinition<Topics, Clients>;

export const grpcSourceMarkers: GrpcTopicSourceMarkerHelper = {
  materialized: () => ({
    _tag: "GrpcMaterializedTopicSource",
    [GrpcTopicSourceTypeId]: true,
    kind: "grpc",
    lifecycle: "materialized",
  }),
  leased: (input) => ({
    _tag: "GrpcLeasedTopicSource",
    [GrpcTopicSourceTypeId]: true,
    kind: "grpc",
    lifecycle: "leased",
    routeBy: input.routeBy,
  }),
};

export const grpc: GrpcHelper = {
  topicSources: (_clients) => ({
    materialized: (input) => ({
      schema: input.schema,
      key: input.key,
      grpcSource: Object.assign(new GrpcTopicSourceClientsBrand<typeof _clients>(), {
        _tag: "GrpcMaterializedTopicSource" as const,
        [GrpcTopicSourceTypeId]: true as const,
        [GrpcTopicSourceKeyTypeId]: input.key,
        [GrpcTopicSourceSchemaTypeId]: input.schema,
        kind: "grpc" as const,
        lifecycle: "materialized" as const,
        client: input.client,
        method: input.method,
        request: brandGrpcFeedRequest(input.request),
        acquire: input.acquire,
        map: brandGrpcFeedMap(input.map),
        ...(input.release === undefined ? {} : { release: input.release }),
      }),
    }),
    leased: (input) => ({
      schema: input.schema,
      key: input.key,
      grpcSource: Object.assign(new GrpcTopicSourceClientsBrand<typeof _clients>(), {
        _tag: "GrpcLeasedTopicSource" as const,
        [GrpcTopicSourceTypeId]: true as const,
        [GrpcTopicSourceKeyTypeId]: input.key,
        [GrpcTopicSourceSchemaTypeId]: input.schema,
        kind: "grpc" as const,
        lifecycle: "leased" as const,
        routeBy: input.routeBy,
        client: input.client,
        method: input.method,
        request: brandGrpcFeedRequest(input.request),
        acquire: input.acquire,
        map: brandGrpcFeedMap(input.map),
        ...(input.release === undefined ? {} : { release: input.release }),
      }),
    }),
  }),
  connectClient: (input) => ({
    _tag: "GrpcConnectClientDefinition",
    service: input.service,
    baseUrl: input.baseUrl,
    protocol: "grpc",
  }),
};

export const defineGrpcFeed = <
  const Topics extends Record<string, { readonly schema: RowSchema }>,
  const Clients extends GrpcRuntimeClients,
>(): GrpcFeedHelper<Topics, Clients> => ({
  leasedFeed: (input) => ({
    _tag: "GrpcLeasedFeedDefinition",
    [GrpcFeedDefinitionTypeId]: {
      topic: input.topic,
      client: input.client,
      method: input.method,
    },
    lifecycle: "leased",
    topic: input.topic,
    client: input.client,
    method: input.method,
    routeBy: input.routeBy,
    request: input.request,
    acquire: input.acquire,
    map: brandGrpcFeedMap(input.map),
    ...(input.release === undefined ? {} : { release: input.release }),
  }),
  materializedFeed: (input) => ({
    _tag: "GrpcMaterializedFeedDefinition",
    [GrpcFeedDefinitionTypeId]: {
      topic: input.topic,
      client: input.client,
      method: input.method,
    },
    lifecycle: "materialized",
    topic: input.topic,
    client: input.client,
    method: input.method,
    request: input.request,
    acquire: input.acquire,
    map: brandGrpcFeedMap(input.map),
    ...(input.release === undefined ? {} : { release: input.release }),
  }),
});
