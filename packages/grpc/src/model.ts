import type {
  DescMessage,
  DescMethodServerStreaming,
  DescService,
  MessageInitShape,
  MessageShape,
} from "@bufbuild/protobuf";
import {
  SourceAdapter,
  type SourceDefinition,
  type SourceDefinitionOptionsFamily,
  type SourceRetryPolicy,
} from "effect-view-server/source-adapter";
import type { Effect, Option } from "effect";
import { Schema } from "effect";
import { exactArrayValues, exactDataEntries } from "./exact-shape";

export const GrpcAdapterFailure = Schema.Union([
  Schema.TaggedStruct("GrpcConfigurationFailure", {
    client: Schema.String,
    message: Schema.String,
    phase: Schema.Literals(["configuration", "client-construction", "client-lookup"]),
  }),
  Schema.TaggedStruct("GrpcRequestConstructionFailure", {
    client: Schema.String,
    message: Schema.String,
    method: Schema.String,
  }),
  Schema.TaggedStruct("GrpcInvocationFailure", {
    client: Schema.String,
    code: Schema.String,
    message: Schema.String,
    method: Schema.String,
  }),
  Schema.TaggedStruct("GrpcStreamFailure", {
    client: Schema.String,
    code: Schema.String,
    message: Schema.String,
    method: Schema.String,
  }),
  Schema.TaggedStruct("GrpcMappingFailure", {
    client: Schema.String,
    message: Schema.String,
    method: Schema.String,
  }),
  Schema.TaggedStruct("GrpcCancellationFailure", {
    client: Schema.String,
    message: Schema.String,
    method: Schema.String,
  }),
]);
export type GrpcAdapterFailure = typeof GrpcAdapterFailure.Type;

const grpcMetricFields = {
  logicalClient: Schema.String,
  method: Schema.String,
  activeInvocations: Schema.BigInt,
  acquisitionCount: Schema.BigInt,
  reconnectCount: Schema.BigInt,
  completionCount: Schema.BigInt,
  streamFailureCount: Schema.BigInt,
  messageCount: Schema.BigInt,
  mappedMessageCount: Schema.BigInt,
  rejectedMessageCount: Schema.BigInt,
  cancellationCount: Schema.BigInt,
  finalizationCount: Schema.BigInt,
  finalizationFailureCount: Schema.BigInt,
};

export const GrpcMaterializedMetrics = Schema.Struct(grpcMetricFields);
export type GrpcMaterializedMetrics = typeof GrpcMaterializedMetrics.Type;

export const GrpcLeasedMetrics = Schema.Struct({
  ...grpcMetricFields,
  activeFeeds: Schema.BigInt,
});
export type GrpcLeasedMetrics = typeof GrpcLeasedMetrics.Type;

export const GrpcRejectionLocation = Schema.Struct({
  client: Schema.String,
  method: Schema.String,
  phase: Schema.Literals(["mapping", "row", "route"]),
  streamItemIndex: Schema.BigInt,
});
export type GrpcRejectionLocation = typeof GrpcRejectionLocation.Type;

export type GrpcMaterializedDefinitionOptions<
  Client extends string = string,
  Row extends object = object,
> = {
  readonly client: Client;
  readonly mapValue: (value: unknown) => Row;
  readonly method: string;
  readonly request: () => unknown;
  readonly service: () => DescService;
};

export type GrpcLeasedDefinitionOptions<
  Client extends string = string,
  Row extends object = object,
> = {
  readonly client: Client;
  readonly mapValue: (value: unknown, route: Readonly<Record<string, unknown>>) => Row;
  readonly method: string;
  readonly request: (route: Readonly<Record<string, unknown>>) => unknown;
  readonly service: () => DescService;
};

interface GrpcMaterializedDefinitionOptionsFamily extends SourceDefinitionOptionsFamily {
  readonly type: GrpcMaterializedDefinitionOptions<string, this["Row"]>;
}

interface GrpcLeasedDefinitionOptionsFamily extends SourceDefinitionOptionsFamily {
  readonly type: GrpcLeasedDefinitionOptions<string, this["Row"]>;
}

export const GrpcSourceAdapter = SourceAdapter.make({
  identity: {
    name: "grpc",
    version: "1",
  },
  failure: GrpcAdapterFailure,
  materialized: {
    metrics: GrpcMaterializedMetrics,
    rejectionLocation: GrpcRejectionLocation,
    definitionOptions:
      SourceAdapter.definitionOptionsFamily<GrpcMaterializedDefinitionOptionsFamily>(),
  },
  leased: {
    metrics: GrpcLeasedMetrics,
    rejectionLocation: GrpcRejectionLocation,
    definitionOptions: SourceAdapter.definitionOptionsFamily<GrpcLeasedDefinitionOptionsFamily>(),
  },
});

export type GrpcDescriptorRecord = Readonly<Record<string, DescService>>;

declare const GrpcSourceClientTypeId: unique symbol;

type GrpcSourceClientMetadata<Client extends string> = {
  readonly [GrpcSourceClientTypeId]?: Client;
};

export type GrpcSourceDefinitionClient<Definition> =
  Definition extends GrpcSourceClientMetadata<infer Client> ? Client : never;

type StringKey<Value> = Extract<keyof Value, string>;
type IsAny<Value> = 0 extends 1 & Value ? true : false;
type IsUnknown<Value> = IsAny<Value> extends true ? false : unknown extends Value ? true : false;
type IsNever<Value> = [Value] extends [never] ? true : false;
type NonUndefined<Value> = Exclude<Value, undefined>;
type ArrayShape<Expected> =
  Expected extends ReadonlyArray<infer Item> ? ReadonlyArray<Item> : never;
type ObjectShape<Expected> = Expected extends unknown
  ? NonUndefined<Expected> extends Uint8Array | ReadonlyArray<unknown>
    ? never
    : NonUndefined<Expected> extends object
      ? NonUndefined<Expected>
      : never
  : never;
type ShapeForCandidate<Candidate, Expected> =
  IsGeneratedOneof<NonUndefined<Expected>> extends true
    ? NonUndefined<Candidate> extends {
        readonly case: infer Case;
      }
      ? Extract<NonUndefined<Expected>, { readonly case: Case }>
      : never
    : NonUndefined<Candidate> extends ReadonlyArray<unknown>
      ? ArrayShape<NonUndefined<Expected>>
      : NonUndefined<Candidate> extends object
        ? ObjectShape<NonUndefined<Expected>>
        : Expected extends unknown
          ? NonUndefined<Candidate> extends NonUndefined<Expected>
            ? NonUndefined<Expected>
            : never
          : never;

type ExactGrpcDescriptorRecord<Descriptors extends GrpcDescriptorRecord> =
  IsAny<Descriptors> extends true
    ? never
    : keyof Descriptors extends never
      ? never
      : string extends keyof Descriptors
        ? never
        : Exclude<keyof Descriptors, string> extends never
          ? true extends {
              readonly [Client in keyof Descriptors]: IsAny<Descriptors[Client]>;
            }[keyof Descriptors]
            ? never
            : Descriptors
          : never;

type OneofSelectedCase<Value> = Value extends {
  readonly case: infer Case extends string;
  readonly value: unknown;
}
  ? Case
  : never;

type IsGeneratedOneof<Value> = [Extract<Value, { readonly case: undefined }>] extends [never]
  ? false
  : [OneofSelectedCase<Value>] extends [never]
    ? false
    : true;

type OneofValueForCase<Value, Case extends string> = Value extends {
  readonly case: Case;
  readonly value: infer OneofValue;
}
  ? OneofValue
  : never;

type PublicOneof<Value> =
  | {
      readonly [Case in OneofSelectedCase<Value>]: {
        readonly case: Case;
        readonly value: PublicMessageInit<OneofValueForCase<Value, Case>>;
      };
    }[OneofSelectedCase<Value>]
  | ([Extract<Value, { readonly case: undefined }>] extends [never]
      ? never
      : {
          readonly case: undefined;
          readonly value?: undefined;
        });

type PublicMessageProperty<Value> =
  IsGeneratedOneof<NonUndefined<Value>> extends true
    ? PublicOneof<NonUndefined<Value>>
    : PublicMessageInit<Value>;

type PublicObjectInit<Value extends object> = string extends keyof Value
  ? {
      readonly [Key in keyof Value]: PublicMessageProperty<Value[Key]>;
    }
  : {
      readonly [Key in keyof Value as Key extends "$typeName" | "$unknown"
        ? never
        : Key]?: PublicMessageProperty<Value[Key]>;
    };

type PublicMessageInit<Value> =
  NonUndefined<Value> extends Uint8Array
    ? Value
    : NonUndefined<Value> extends ReadonlyArray<infer Item>
      ? ReadonlyArray<PublicMessageInit<Item>>
      : NonUndefined<Value> extends object
        ? PublicObjectInit<NonUndefined<Value>>
        : Value;

type ForbiddenFactoryOutput<Value> =
  IsAny<Value> extends true
    ? true
    : IsUnknown<Value> extends true
      ? true
      : IsNever<Value> extends true
        ? true
        : [Value] extends [undefined]
          ? true
          : Value extends PromiseLike<unknown>
            ? true
            : Value extends Effect.Effect<unknown, unknown, unknown>
              ? true
              : Value extends Option.Option<unknown>
                ? true
                : false;

export type GrpcServerStreamingMethodName<Service extends DescService> = DescService extends Service
  ? string
  : {
      readonly [Method in keyof Service["method"]]: Service["method"][Method] extends {
        readonly methodKind: "server_streaming";
      }
        ? Method
        : never;
    }[keyof Service["method"]] &
      string;

export type GrpcMethodRequest<
  Service extends DescService,
  Method extends GrpcServerStreamingMethodName<Service>,
> = Service["method"][Method] extends {
  readonly input: infer Input extends DescMessage;
}
  ? PublicMessageInit<MessageInitShape<Input>>
  : never;

export type GrpcMethodValue<
  Service extends DescService,
  Method extends GrpcServerStreamingMethodName<Service>,
> = Service["method"][Method] extends {
  readonly output: infer Output extends DescMessage;
}
  ? MessageShape<Output>
  : never;

type MappingRow<Mapping> = Mapping extends (...arguments_: infer _Arguments) => infer Row
  ? Row extends object
    ? Row
    : never
  : never;

type ExactFactoryOutputBranch<Candidate, Expected> =
  ForbiddenFactoryOutput<Candidate> extends true
    ? never
    : NonUndefined<Candidate> extends Uint8Array
      ? NonUndefined<Candidate> extends NonUndefined<Expected>
        ? Candidate
        : never
      : NonUndefined<Candidate> extends ReadonlyArray<infer CandidateItem>
        ? ShapeForCandidate<Candidate, Expected> extends ReadonlyArray<infer ShapeItem>
          ? ReadonlyArray<ExactFactoryOutput<CandidateItem, ShapeItem>>
          : never
        : NonUndefined<Candidate> extends object
          ? [ShapeForCandidate<Candidate, Expected>] extends [never]
            ? never
            : ShapeForCandidate<Candidate, Expected> extends object
              ? Candidate &
                  Record<
                    Exclude<
                      keyof NonUndefined<Candidate>,
                      keyof ShapeForCandidate<Candidate, Expected>
                    >,
                    never
                  > & {
                    readonly [Key in keyof NonUndefined<Candidate>]: Key extends keyof ShapeForCandidate<
                      Candidate,
                      Expected
                    >
                      ? ExactFactoryOutput<
                          NonUndefined<Candidate>[Key],
                          ShapeForCandidate<Candidate, Expected>[Key]
                        >
                      : never;
                  }
              : never
          : Candidate extends Expected
            ? Candidate
            : never;

type FactoryOutputBranchIsCompatible<Candidate, Expected> = Candidate extends undefined
  ? true
  : NonUndefined<Candidate> extends Uint8Array
    ? NonUndefined<Candidate> extends NonUndefined<Expected>
      ? true
      : false
    : NonUndefined<Candidate> extends ReadonlyArray<unknown>
      ? [ArrayShape<NonUndefined<Expected>>] extends [never]
        ? false
        : true
      : NonUndefined<Candidate> extends object
        ? ShapeForCandidate<Candidate, Expected> extends infer SelectedShape extends object
          ? Exclude<keyof NonUndefined<Candidate>, keyof SelectedShape> extends never
            ? true
            : false
          : false
        : Candidate extends Expected
          ? true
          : false;

type ExactFactoryOutput<Candidate, Expected> =
  IsAny<Candidate> extends true
    ? never
    : false extends (
          Candidate extends unknown ? FactoryOutputBranchIsCompatible<Candidate, Expected> : never
        )
      ? never
      : Candidate extends unknown
        ? ExactFactoryOutputBranch<Candidate, Expected>
        : never;

type ExactSourceKeys<Keys extends PropertyKey, Expected> = {
  readonly [Key in Exclude<Keys, keyof Expected>]: never;
};

type RouteHasDuplicates<
  RouteBy extends ReadonlyArray<string>,
  Seen extends string = never,
> = RouteBy extends readonly [
  infer Field extends string,
  ...infer Rest extends ReadonlyArray<string>,
]
  ? Field extends Seen
    ? true
    : RouteHasDuplicates<Rest, Seen | Field>
  : false;

type UniqueRoute<RouteBy extends readonly [string, ...ReadonlyArray<string>]> =
  RouteHasDuplicates<RouteBy> extends true ? never : RouteBy;

type ValidRouteBy<RouteBy extends readonly [unknown, ...ReadonlyArray<unknown>]> =
  IsAny<RouteBy> extends true
    ? never
    : IsAny<RouteBy[number]> extends true
      ? never
      : RouteBy extends readonly [string, ...ReadonlyArray<string>]
        ? UniqueRoute<RouteBy>
        : never;

type RouteFor<Row extends object, RouteBy extends readonly [string, ...ReadonlyArray<string>]> = {
  readonly [Field in RouteBy[number]]: Field extends keyof Row ? Row[Field] : never;
};

type MaterializedInputShape<
  Descriptors extends GrpcDescriptorRecord,
  Client extends StringKey<Descriptors>,
  Method extends GrpcServerStreamingMethodName<Descriptors[Client]>,
  RequestOutput,
  Row extends object,
  RequestFactory extends () => RequestOutput,
  Mapping extends (input: { readonly value: GrpcMethodValue<Descriptors[Client], Method> }) => Row,
> = {
  readonly client: Client;
  readonly method: Method;
  readonly request: RequestFactory &
    (() => RequestOutput &
      ExactFactoryOutput<RequestOutput, GrpcMethodRequest<Descriptors[Client], Method>>);
  readonly map: Mapping &
    ((input: { readonly value: GrpcMethodValue<Descriptors[Client], Method> }) => Row);
};

type LeasedInputShape<
  Descriptors extends GrpcDescriptorRecord,
  Client extends StringKey<Descriptors>,
  Method extends GrpcServerStreamingMethodName<Descriptors[Client]>,
  RouteBy extends readonly [string, ...ReadonlyArray<string>],
  RequestOutput,
  Row extends object,
  RouteRow extends object,
  RequestFactory extends (route: RouteFor<NoInfer<RouteRow>, RouteBy>) => RequestOutput,
  Mapping extends (input: {
    readonly value: GrpcMethodValue<Descriptors[Client], Method>;
    readonly route: RouteFor<NoInfer<RouteRow>, RouteBy>;
  }) => Row,
> = {
  readonly client: Client;
  readonly method: Method;
  readonly routeBy: UniqueRoute<RouteBy>;
  readonly request: RequestFactory &
    ((
      route: RouteFor<NoInfer<RouteRow>, RouteBy>,
    ) => RequestOutput &
      ExactFactoryOutput<RequestOutput, GrpcMethodRequest<Descriptors[Client], Method>>);
  readonly map: Mapping &
    ((input: {
      readonly value: GrpcMethodValue<Descriptors[Client], Method>;
      readonly route: RouteFor<NoInfer<RouteRow>, RouteBy>;
    }) => Row);
};

export type GrpcMaterializedSource<
  Row extends object = object,
  Client extends string = string,
> = SourceDefinition<
  typeof GrpcSourceAdapter,
  "materialized",
  GrpcMaterializedDefinitionOptions<string, Row>,
  readonly [],
  unknown,
  Row
> &
  GrpcSourceClientMetadata<Client>;

export type GrpcLeasedSource<
  RouteBy extends readonly [string, ...ReadonlyArray<string>] = readonly [
    string,
    ...ReadonlyArray<string>,
  ],
  Row extends object = object,
  Client extends string = string,
> = SourceDefinition<
  typeof GrpcSourceAdapter,
  "leased",
  GrpcLeasedDefinitionOptions<string, Row>,
  RouteBy,
  unknown,
  Row
> &
  GrpcSourceClientMetadata<Client>;

export type GrpcTopicSources<Descriptors extends GrpcDescriptorRecord> = {
  readonly materialized: <
    const Client extends StringKey<Descriptors>,
    const Method extends GrpcServerStreamingMethodName<Descriptors[Client]>,
    const RequestOutput,
    const Row extends object,
    const RequestFactory extends () => RequestOutput,
    const Mapping extends (input: {
      readonly value: GrpcMethodValue<Descriptors[Client], Method>;
    }) => Row,
    const InputKeys extends PropertyKey,
    const InputClient,
    const InputMethod,
    RetryServices = never,
  >(
    input: MaterializedInputShape<
      Descriptors,
      Client,
      Method,
      RequestOutput,
      Row,
      RequestFactory,
      Mapping
    > & {
      readonly [Key in InputKeys]: unknown;
    } & {
      readonly client: InputClient;
      readonly method: InputMethod;
    } & ExactSourceKeys<
        InputKeys,
        MaterializedInputShape<
          Descriptors,
          Client,
          Method,
          RequestOutput,
          Row,
          RequestFactory,
          Mapping
        >
      > &
      (IsAny<RequestFactory> extends true ? never : unknown) &
      (IsAny<Mapping> extends true ? never : unknown) &
      (true extends ForbiddenFactoryOutput<Row> ? never : unknown),
    retry?: SourceRetryPolicy<GrpcAdapterFailure, RetryServices>,
    ..._invalid: PropertyKey extends InputKeys
      ? [invalidInput: never]
      : IsAny<InputClient> extends true
        ? [invalidClient: never]
        : IsAny<InputMethod> extends true
          ? [invalidMethod: never]
          : []
  ) => SourceDefinition<
    typeof GrpcSourceAdapter,
    "materialized",
    GrpcMaterializedDefinitionOptions<string, Row>,
    readonly [],
    RetryServices,
    Row
  > &
    GrpcSourceClientMetadata<Client>;
  readonly leased: <
    const Client extends StringKey<Descriptors>,
    const Method extends GrpcServerStreamingMethodName<Descriptors[Client]>,
    const InputRouteBy extends readonly [unknown, ...ReadonlyArray<unknown>],
    const InputKeys extends PropertyKey,
    const InputClient,
    const InputMethod,
    const Row extends object,
    const RequestOutput,
    const RouteRow extends object = Row,
    const RequestFactory extends (
      route: RouteFor<NoInfer<RouteRow>, ValidRouteBy<InputRouteBy>>,
    ) => RequestOutput = (
      route: RouteFor<NoInfer<RouteRow>, ValidRouteBy<InputRouteBy>>,
    ) => RequestOutput,
    const Mapping extends (input: {
      readonly value: GrpcMethodValue<Descriptors[Client], Method>;
      readonly route: RouteFor<NoInfer<RouteRow>, ValidRouteBy<InputRouteBy>>;
    }) => Row = (input: {
      readonly value: GrpcMethodValue<Descriptors[Client], Method>;
      readonly route: RouteFor<NoInfer<RouteRow>, ValidRouteBy<InputRouteBy>>;
    }) => Row,
    RetryServices = never,
  >(
    input: Omit<
      LeasedInputShape<
        Descriptors,
        Client,
        Method,
        ValidRouteBy<InputRouteBy>,
        RequestOutput,
        Row,
        RouteRow,
        RequestFactory,
        Mapping
      >,
      "routeBy"
    > & {
      readonly [Key in InputKeys]: unknown;
    } & {
      readonly client: InputClient;
      readonly method: InputMethod;
      readonly routeBy: InputRouteBy & ValidRouteBy<InputRouteBy>;
    } & ExactSourceKeys<
        InputKeys,
        LeasedInputShape<
          Descriptors,
          Client,
          Method,
          ValidRouteBy<InputRouteBy>,
          RequestOutput,
          Row,
          RouteRow,
          RequestFactory,
          Mapping
        >
      > &
      (IsAny<RequestFactory> extends true ? never : unknown) &
      (IsAny<Mapping> extends true ? never : unknown) &
      (true extends ForbiddenFactoryOutput<Row> ? never : unknown),
    retry?: SourceRetryPolicy<GrpcAdapterFailure, RetryServices>,
    ..._invalid: PropertyKey extends InputKeys
      ? [invalidInput: never]
      : IsAny<InputClient> extends true
        ? [invalidClient: never]
        : IsAny<InputMethod> extends true
          ? [invalidMethod: never]
          : []
  ) => SourceDefinition<
    typeof GrpcSourceAdapter,
    "leased",
    GrpcLeasedDefinitionOptions<string, Row>,
    ValidRouteBy<InputRouteBy>,
    RetryServices,
    Row
  > &
    GrpcSourceClientMetadata<Client>;
};

export class GrpcSourceConfigurationError extends TypeError {
  override readonly name = "GrpcSourceConfigurationError";
}

const captureExactNonEmptyUniqueStringArray = (
  value: unknown,
): readonly [string, ...ReadonlyArray<string>] | undefined => {
  const values = exactArrayValues(value);
  if (values === undefined) {
    return undefined;
  }
  const [first, ...rest] = values;
  const stringRest = rest.filter((entry): entry is string => typeof entry === "string");
  if (
    typeof first !== "string" ||
    first.length === 0 ||
    stringRest.length !== rest.length ||
    stringRest.some((entry) => entry.length === 0) ||
    new Set(values).size !== values.length
  ) {
    return undefined;
  }
  return Object.freeze([first, ...stringRest] as const);
};

const isGrpcMessageDescriptor = (value: unknown): boolean =>
  typeof value === "object" && value !== null && Reflect.get(value, "kind") === "message";

const isGrpcMethodDescriptor = (value: unknown, service: object): boolean =>
  typeof value === "object" &&
  value !== null &&
  Reflect.get(value, "kind") === "rpc" &&
  Reflect.get(value, "parent") === service &&
  typeof Reflect.get(value, "name") === "string" &&
  Reflect.get(value, "name") !== "" &&
  typeof Reflect.get(value, "localName") === "string" &&
  Reflect.get(value, "localName") !== "" &&
  ["unary", "server_streaming", "client_streaming", "bidi_streaming"].includes(
    String(Reflect.get(value, "methodKind")),
  ) &&
  isGrpcMessageDescriptor(Reflect.get(value, "input")) &&
  isGrpcMessageDescriptor(Reflect.get(value, "output")) &&
  typeof Reflect.get(value, "toString") === "function";

export const isGrpcServiceDescriptor = (value: unknown): value is DescService => {
  if (
    typeof value !== "object" ||
    value === null ||
    Reflect.get(value, "kind") !== "service" ||
    typeof Reflect.get(value, "name") !== "string" ||
    Reflect.get(value, "name") === "" ||
    typeof Reflect.get(value, "typeName") !== "string" ||
    Reflect.get(value, "typeName") === "" ||
    typeof Reflect.get(value, "toString") !== "function"
  ) {
    return false;
  }
  const file = Reflect.get(value, "file");
  const methods = Reflect.get(value, "methods");
  const methodRecord = Reflect.get(value, "method");
  if (
    typeof file !== "object" ||
    file === null ||
    Reflect.get(file, "kind") !== "file" ||
    !Array.isArray(Reflect.get(file, "services")) ||
    !Reflect.get(file, "services").includes(value) ||
    !Array.isArray(methods) ||
    typeof methodRecord !== "object" ||
    methodRecord === null ||
    Reflect.ownKeys(methodRecord).length !== methods.length
  ) {
    return false;
  }
  return methods.every((method) => {
    if (!isGrpcMethodDescriptor(method, value)) {
      return false;
    }
    const localName = Reflect.get(method, "localName");
    return (
      typeof localName === "string" &&
      Object.hasOwn(methodRecord, localName) &&
      Reflect.get(methodRecord, localName) === method
    );
  });
};

const requireDescriptors = (value: unknown): GrpcDescriptorRecord => {
  try {
    const entries = exactDataEntries(value);
    if (entries === undefined || entries.length === 0) {
      throw new GrpcSourceConfigurationError(
        "grpc.topicSources requires one non-empty exact generated service descriptor record.",
      );
    }
    const captured: Record<string, DescService> = {};
    for (const [key, descriptor] of entries) {
      if (key.length === 0 || !isGrpcServiceDescriptor(descriptor)) {
        throw new GrpcSourceConfigurationError(
          "grpc.topicSources accepts non-empty logical names mapped only to generated service descriptors.",
        );
      }
      Object.defineProperty(captured, key, {
        enumerable: true,
        value: descriptor,
      });
    }
    return Object.freeze(captured);
  } catch (cause) {
    if (cause instanceof GrpcSourceConfigurationError) {
      throw cause;
    }
    throw new GrpcSourceConfigurationError(
      "grpc.topicSources requires one non-empty exact generated service descriptor record.",
    );
  }
};

const isServerStreamingMethod = (
  value: unknown,
): value is DescMethodServerStreaming<DescMessage, DescMessage> =>
  typeof value === "object" &&
  value !== null &&
  Reflect.get(value, "methodKind") === "server_streaming";

export const selectedGrpcMethod = (
  service: DescService,
  method: string,
): DescMethodServerStreaming<DescMessage, DescMessage> | undefined => {
  const selected = Reflect.get(service.method, method);
  return isServerStreamingMethod(selected) ? selected : undefined;
};

type CheckedMaterializedInput = {
  readonly client: string;
  readonly map: (...arguments_: Array<never>) => unknown;
  readonly method: string;
  readonly request: (...arguments_: Array<never>) => unknown;
  readonly routeBy: undefined;
  readonly service: DescService;
};

type CheckedLeasedInput = Omit<CheckedMaterializedInput, "routeBy"> & {
  readonly routeBy: readonly [string, ...ReadonlyArray<string>];
};

function requireCommonInput(
  descriptors: GrpcDescriptorRecord,
  input: unknown,
  lifecycle: "materialized",
): CheckedMaterializedInput;
function requireCommonInput(
  descriptors: GrpcDescriptorRecord,
  input: unknown,
  lifecycle: "leased",
): CheckedLeasedInput;
function requireCommonInput(
  descriptors: GrpcDescriptorRecord,
  input: unknown,
  lifecycle: "materialized" | "leased",
): CheckedMaterializedInput | CheckedLeasedInput {
  const keys =
    lifecycle === "materialized"
      ? ["client", "method", "request", "map"]
      : ["client", "method", "routeBy", "request", "map"];
  const entries = exactDataEntries(input);
  if (
    entries === undefined ||
    entries.length !== keys.length ||
    entries.some(([key]) => !keys.includes(key))
  ) {
    throw new GrpcSourceConfigurationError(
      `gRPC ${lifecycle} Source Definition requires exactly: ${keys.join(", ")}.`,
    );
  }
  const values = new Map(entries);
  const client = values.get("client");
  const method = values.get("method");
  const request = values.get("request");
  const map = values.get("map");
  const routeBy =
    lifecycle === "leased"
      ? captureExactNonEmptyUniqueStringArray(values.get("routeBy"))
      : undefined;
  const service =
    typeof client === "string" && Object.hasOwn(descriptors, client)
      ? descriptors[client]
      : undefined;
  if (
    typeof client !== "string" ||
    service === undefined ||
    typeof method !== "string" ||
    selectedGrpcMethod(service, method) === undefined ||
    typeof request !== "function" ||
    typeof map !== "function"
  ) {
    throw new GrpcSourceConfigurationError(
      `gRPC ${lifecycle} Source Definition requires a known client, server-streaming method, request factory, and Mapping.`,
    );
  }
  const checked = {
    client,
    method,
    request: (...arguments_: Array<never>) => Reflect.apply(request, undefined, arguments_),
    map: (...arguments_: Array<never>) => Reflect.apply(map, undefined, arguments_),
    service,
  };
  if (lifecycle === "leased") {
    if (routeBy === undefined) {
      throw new GrpcSourceConfigurationError(
        "gRPC Leased Source Definition requires exact non-empty unique Route Fields.",
      );
    }
    return {
      ...checked,
      routeBy,
    };
  }
  return {
    ...checked,
    routeBy: undefined,
  };
}

export type GrpcHelper = {
  readonly topicSources: <const Descriptors extends GrpcDescriptorRecord>(
    descriptors: ExactGrpcDescriptorRecord<Descriptors>,
  ) => GrpcTopicSources<Descriptors>;
};

const makeMaterializedDefinition = <
  const Client extends string,
  Mapping extends (...arguments_: Array<never>) => object,
  RetryServices,
>(
  _input: {
    readonly client: Client;
    readonly map: Mapping;
  },
  checked: CheckedMaterializedInput,
  retry: SourceRetryPolicy<GrpcAdapterFailure, RetryServices> | undefined,
): SourceDefinition<
  typeof GrpcSourceAdapter,
  "materialized",
  GrpcMaterializedDefinitionOptions<string, MappingRow<Mapping>>,
  readonly [],
  RetryServices,
  MappingRow<Mapping>
> &
  GrpcSourceClientMetadata<Client> =>
  GrpcSourceAdapter.materializedSource<
    MappingRow<Mapping>,
    GrpcMaterializedDefinitionOptions<string, MappingRow<Mapping>>,
    RetryServices
  >(
    {
      client: checked.client,
      method: checked.method,
      request: (): unknown => Reflect.apply(checked.request, undefined, []),
      mapValue: (value: unknown): MappingRow<Mapping> =>
        Reflect.apply(checked.map, undefined, [{ value }]),
      service: () => checked.service,
    },
    retry,
  );

function capturedRouteBy<const RouteBy extends readonly [string, ...ReadonlyArray<string>]>(
  input: { readonly routeBy: RouteBy },
  routeBy: readonly [string, ...ReadonlyArray<string>],
): RouteBy;
function capturedRouteBy(
  _input: { readonly routeBy: readonly [string, ...ReadonlyArray<string>] },
  routeBy: readonly [string, ...ReadonlyArray<string>],
): readonly [string, ...ReadonlyArray<string>] {
  return routeBy;
}

const makeLeasedDefinition = <
  const Client extends string,
  const RouteBy extends readonly [string, ...ReadonlyArray<string>],
  Mapping extends (...arguments_: Array<never>) => object,
  RetryServices,
>(
  _input: {
    readonly client: Client;
    readonly map: Mapping;
    readonly routeBy: RouteBy;
  },
  checked: CheckedLeasedInput,
  retry: SourceRetryPolicy<GrpcAdapterFailure, RetryServices> | undefined,
): SourceDefinition<
  typeof GrpcSourceAdapter,
  "leased",
  GrpcLeasedDefinitionOptions<string, MappingRow<Mapping>>,
  RouteBy,
  RetryServices,
  MappingRow<Mapping>
> &
  GrpcSourceClientMetadata<Client> =>
  GrpcSourceAdapter.leasedSource<
    RouteBy,
    MappingRow<Mapping>,
    GrpcLeasedDefinitionOptions<string, MappingRow<Mapping>>,
    RetryServices
  >(
    capturedRouteBy(_input, checked.routeBy),
    {
      client: checked.client,
      method: checked.method,
      request: (route: Readonly<Record<string, unknown>>): unknown =>
        Reflect.apply(checked.request, undefined, [route]),
      mapValue: (value: unknown, route: Readonly<Record<string, unknown>>): MappingRow<Mapping> =>
        Reflect.apply(checked.map, undefined, [{ value, route }]),
      service: () => checked.service,
    },
    retry,
  );

export const grpc: GrpcHelper = {
  topicSources: (descriptors) => {
    const captured = requireDescriptors(descriptors);
    const materialized: GrpcTopicSources<typeof descriptors>["materialized"] = (
      input,
      retry,
      ..._invalid
    ) => {
      try {
        const checked = requireCommonInput(captured, input, "materialized");
        return makeMaterializedDefinition(input, checked, retry);
      } catch (cause) {
        if (cause instanceof GrpcSourceConfigurationError) {
          throw cause;
        }
        throw new GrpcSourceConfigurationError(
          "gRPC materialized Source Definition requires exact data-only configuration.",
        );
      }
    };
    const leased: GrpcTopicSources<typeof descriptors>["leased"] = (input, retry, ..._invalid) => {
      try {
        const checked = requireCommonInput(captured, input, "leased");
        return makeLeasedDefinition(input, checked, retry);
      } catch (cause) {
        if (cause instanceof GrpcSourceConfigurationError) {
          throw cause;
        }
        throw new GrpcSourceConfigurationError(
          "gRPC Leased Source Definition requires exact non-empty unique Route Fields.",
        );
      }
    };
    return Object.freeze({
      materialized,
      leased,
    });
  },
};

Object.freeze(grpc);

export const isGrpcSourceDefinitionOptions = (
  value: unknown,
): value is GrpcMaterializedDefinitionOptions | GrpcLeasedDefinitionOptions =>
  captureGrpcSourceDefinitionOptions(value) !== undefined;

export const captureGrpcSourceDefinitionOptions = (
  value: unknown,
): GrpcMaterializedDefinitionOptions | GrpcLeasedDefinitionOptions | undefined => {
  const expected = ["client", "mapValue", "method", "request", "service"];
  const entries = exactDataEntries(value);
  if (
    entries === undefined ||
    entries.length !== expected.length ||
    entries.some(([key]) => !expected.includes(key))
  ) {
    return undefined;
  }
  const fields = new Map(entries);
  const client = fields.get("client");
  const mapValue = fields.get("mapValue");
  const method = fields.get("method");
  const request = fields.get("request");
  const service = fields.get("service");
  if (
    typeof client !== "string" ||
    typeof mapValue !== "function" ||
    typeof method !== "string" ||
    typeof request !== "function" ||
    typeof service !== "function"
  ) {
    return undefined;
  }
  return Object.freeze({
    client,
    mapValue: (...arguments_: ReadonlyArray<unknown>) =>
      Reflect.apply(mapValue, undefined, arguments_),
    method,
    request: (...arguments_: ReadonlyArray<unknown>) =>
      Reflect.apply(request, undefined, arguments_),
    service: () => Reflect.apply(service, undefined, []),
  });
};
