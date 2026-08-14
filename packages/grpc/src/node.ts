import { definedFields } from "./optional-fields";
import type { DescService } from "@bufbuild/protobuf";
import { createClient, type Interceptor } from "@connectrpc/connect";
import {
  createGrpcTransport,
  Http2SessionManager,
  type GrpcTransportOptions,
  type Http2SessionOptions,
} from "@connectrpc/connect-node";
import { Config, Effect, Layer, Option, Schema } from "effect";
import { Buffer } from "node:buffer";
import { exactArrayValues, exactDataEntries, type DataEntry } from "./exact-shape";
import {
  GrpcSourceAdapter,
  type GrpcAdapterFailure,
  type GrpcSourceDefinitionClient,
} from "./model";
import type { SourceDefinitionAdapter } from "effect-view-server/source-adapter";
import {
  grpcBindingPlan,
  grpcServerLayerFromBindingPlan,
  type GrpcRuntimeClient,
  type GrpcRuntimeClients,
  type GrpcServerViewServer,
} from "./server-internal";

type GrpcSourceClient<Source> =
  SourceDefinitionAdapter<Source> extends typeof GrpcSourceAdapter
    ? GrpcSourceDefinitionClient<Source>
    : never;

export type GrpcNodeClientNames<ViewServer> = ViewServer extends {
  readonly topics: infer Topics extends Readonly<Record<string, unknown>>;
}
  ? {
      readonly [Topic in keyof Topics]: Topics[Topic] extends {
        readonly source: infer Source;
      }
        ? GrpcSourceClient<Source>
        : never;
    }[keyof Topics]
  : never;

export type GrpcNodeClientOptions = {
  readonly baseUrl: string;
  readonly interceptors?: ReadonlyArray<Interceptor>;
  readonly transport?: Omit<GrpcTransportOptions, "baseUrl" | "interceptors">;
};

export type GrpcNodeOptions<ViewServer> = [GrpcNodeClientNames<ViewServer>] extends [never]
  ? never
  : {
      readonly [Client in GrpcNodeClientNames<ViewServer>]: GrpcNodeClientOptions;
    };

type GrpcNodeConfigOptions<ViewServer> = Config.Wrap<GrpcNodeOptions<ViewServer>>;

type IsAny<Value> = 0 extends 1 & Value ? true : false;

type ContainsAny<Value, Seen = never> =
  IsAny<Value> extends true
    ? true
    : Value extends Config.Config<infer ConfigValue>
      ? ContainsAny<ConfigValue, Seen | Value>
      : Value extends (...arguments_: ReadonlyArray<never>) => unknown
        ? false
        : Value extends Seen
          ? false
          : Value extends ReadonlyArray<infer Item>
            ? ContainsAny<Item, Seen | Value>
            : Value extends object
              ? true extends {
                  readonly [Key in keyof Value]-?: ContainsAny<Value[Key], Seen | Value>;
                }[keyof Value]
                ? true
                : false
              : false;

type ClientOptionsAreExact<Candidate> =
  true extends ContainsAny<Candidate>
    ? false
    : Candidate extends GrpcNodeClientOptions
      ? Exclude<keyof Candidate, keyof GrpcNodeClientOptions> extends never
        ? true
        : false
      : false;

type ConfigClientOptionsAreExact<Candidate> =
  true extends ContainsAny<Candidate>
    ? false
    : Candidate extends Config.Config<infer Resolved>
      ? ClientOptionsAreExact<Resolved>
      : Candidate extends Config.Wrap<GrpcNodeClientOptions>
        ? Exclude<keyof Candidate, keyof GrpcNodeClientOptions> extends never
          ? true
          : false
        : false;

type InvalidResolvedClients<ViewServer, Candidate> = {
  readonly [Client in keyof Candidate]: Client extends keyof GrpcNodeOptions<ViewServer>
    ? ClientOptionsAreExact<Candidate[Client]> extends true
      ? never
      : Client
    : Client;
}[keyof Candidate];

type NodeOptionsAreExact<ViewServer, Candidate> =
  IsAny<Candidate> extends true
    ? false
    : Candidate extends GrpcNodeOptions<ViewServer>
      ? Exclude<keyof Candidate, keyof GrpcNodeOptions<ViewServer>> extends never
        ? InvalidResolvedClients<ViewServer, Candidate> extends never
          ? true
          : false
        : false
      : false;

type ExactNodeOptions<ViewServer, Candidate> =
  NodeOptionsAreExact<ViewServer, Candidate> extends true ? Candidate : never;

type InvalidConfigClients<ViewServer, Candidate> = {
  readonly [Client in keyof Candidate]: Client extends keyof GrpcNodeOptions<ViewServer>
    ? ConfigClientOptionsAreExact<Candidate[Client]> extends true
      ? never
      : Client
    : Client;
}[keyof Candidate];

type ConfigNodeOptionsAreExact<ViewServer, Candidate> =
  IsAny<Candidate> extends true
    ? false
    : true extends ContainsAny<Candidate>
      ? false
      : Candidate extends Config.Config<infer Resolved>
        ? NodeOptionsAreExact<ViewServer, Resolved>
        : Candidate extends GrpcNodeConfigOptions<ViewServer>
          ? Exclude<keyof Candidate, keyof GrpcNodeOptions<ViewServer>> extends never
            ? InvalidConfigClients<ViewServer, Candidate> extends never
              ? true
              : false
            : false
          : false;

type ExactConfigNodeOptions<ViewServer, Candidate> =
  ConfigNodeOptionsAreExact<ViewServer, Candidate> extends true ? Candidate : never;

export class GrpcNodeConfigurationError extends TypeError {
  override readonly name = "GrpcNodeConfigurationError";
}

const configurationShapeError = (): GrpcNodeConfigurationError =>
  new GrpcNodeConfigurationError(
    "gRPC Node configuration must use exact enumerable string data fields and dense data arrays.",
  );

const snapshotDataEntries = (
  entries: ReadonlyArray<DataEntry>,
): Readonly<Record<string, unknown>> => {
  const snapshot: Record<string, unknown> = {};
  for (const [key, value] of entries) {
    Object.defineProperty(snapshot, key, {
      enumerable: true,
      value: snapshotPlainConfiguration(value),
    });
  }
  return Object.freeze(snapshot);
};

function snapshotPlainConfiguration<Value>(value: Value): Value;
function snapshotPlainConfiguration(value: unknown): unknown {
  if (value instanceof Uint8Array) {
    return Buffer.isBuffer(value) ? Buffer.from(value) : Uint8Array.from(value);
  }
  if (isCompressionResource(value) || isSessionManagerResource(value)) {
    return value;
  }
  if (Array.isArray(value)) {
    const values = exactArrayValues(value);
    if (values === undefined) {
      throw configurationShapeError();
    }
    return Object.freeze(values.map(snapshotPlainConfiguration));
  }
  if (typeof value !== "object" || value === null) {
    return value;
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    return value;
  }
  const entries = exactDataEntries(value);
  if (entries === undefined) {
    throw configurationShapeError();
  }
  return snapshotDataEntries(entries);
}

const snapshotTransport = (
  transport: NonNullable<GrpcNodeClientOptions["transport"]>,
): NonNullable<GrpcNodeClientOptions["transport"]> => snapshotPlainConfiguration(transport);

const nonNegativeFiniteNumber = (value: unknown): boolean =>
  typeof value === "number" && Number.isFinite(value) && value >= 0;

function resourcePropertyIs<Value extends object>(
  value: Value,
  key: string,
  predicate: <Candidate>(candidate: Candidate) => boolean,
): boolean {
  return predicate(Reflect.get(value, key));
}

function isCompressionResource(value: unknown): boolean {
  try {
    return (
      typeof value === "object" &&
      value !== null &&
      resourcePropertyIs(
        value,
        "name",
        (candidate) => typeof candidate === "string" && candidate !== "",
      ) &&
      resourcePropertyIs(value, "compress", (candidate) => typeof candidate === "function") &&
      resourcePropertyIs(value, "decompress", (candidate) => typeof candidate === "function")
    );
  } catch {
    return false;
  }
}

function isSessionManagerResource(value: unknown): boolean {
  try {
    return (
      typeof value === "object" &&
      value !== null &&
      resourcePropertyIs(
        value,
        "authority",
        (candidate) => typeof candidate === "string" && candidate !== "",
      ) &&
      resourcePropertyIs(value, "request", (candidate) => typeof candidate === "function") &&
      resourcePropertyIs(
        value,
        "notifyResponseByteRead",
        (candidate) => typeof candidate === "function",
      )
    );
  } catch {
    return false;
  }
}

const transportOptionValueIsValid = (key: string, value: unknown): boolean => {
  switch (key) {
    case "useBinaryFormat":
    case "pingIdleConnection":
      return typeof value === "boolean";
    case "compressMinBytes":
    case "defaultTimeoutMs":
    case "idleConnectionTimeoutMs":
    case "pingIntervalMs":
    case "pingTimeoutMs":
    case "readMaxBytes":
    case "writeMaxBytes":
      return nonNegativeFiniteNumber(value);
    case "acceptCompression": {
      const values = exactArrayValues(value);
      return values !== undefined && values.every(isCompressionResource);
    }
    case "sendCompression":
      return isCompressionResource(value);
    case "sessionManager":
      return isSessionManagerResource(value);
    case "binaryOptions":
    case "jsonOptions":
    case "nodeOptions":
      return exactDataEntries(value) !== undefined;
    default:
      return false;
  }
};

const transportOptionValueCanBeCaptured = (key: string, value: unknown): boolean => {
  switch (key) {
    case "acceptCompression": {
      const values = exactArrayValues(value);
      return (
        values !== undefined && values.every((entry) => typeof entry === "object" && entry !== null)
      );
    }
    case "sendCompression":
    case "sessionManager":
      return typeof value === "object" && value !== null;
    default:
      return transportOptionValueIsValid(key, value);
  }
};

const isGrpcTransportOptions = (
  value: Schema.Schema.Type<typeof Schema.Unknown>,
): value is NonNullable<GrpcNodeClientOptions["transport"]> => {
  const entries = exactDataEntries(value);
  return (
    entries !== undefined &&
    entries.every(([key, entry]) => transportOptionValueIsValid(key, entry))
  );
};

const isCapturableGrpcTransportOptions = (
  value: Schema.Schema.Type<typeof Schema.Unknown>,
): value is NonNullable<GrpcNodeClientOptions["transport"]> => {
  const entries = exactDataEntries(value);
  return (
    entries !== undefined &&
    entries.every(([key, entry]) => transportOptionValueCanBeCaptured(key, entry))
  );
};

const nodeConfigurationFailure = (
  client: string,
  message: string,
): Extract<GrpcAdapterFailure, { readonly _tag: "GrpcConfigurationFailure" }> => ({
  _tag: "GrpcConfigurationFailure",
  client,
  message,
  phase: "client-construction",
});

const captureOptions = (
  options: Readonly<Record<string, GrpcNodeClientOptions>>,
): Readonly<Record<string, GrpcNodeClientOptions>> => {
  const optionEntries = exactDataEntries(options);
  if (optionEntries === undefined || optionEntries.length === 0) {
    throw new GrpcNodeConfigurationError(
      "grpcNode.layer requires one non-empty exact logical-client map.",
    );
  }
  const snapshot: Record<string, GrpcNodeClientOptions> = {};
  for (const [key, entry] of optionEntries) {
    const entryValues = exactDataEntries(entry);
    if (
      key.length === 0 ||
      entryValues === undefined ||
      entryValues.some(([entryKey]) => !["baseUrl", "interceptors", "transport"].includes(entryKey))
    ) {
      throw new GrpcNodeConfigurationError(
        "Every gRPC logical client requires exact non-empty baseUrl and optional interceptors/transport.",
      );
    }
    const values = new Map(entryValues);
    const baseUrl = values.get("baseUrl");
    const interceptorInput = values.get("interceptors");
    const interceptors =
      interceptorInput === undefined ? undefined : exactArrayValues(interceptorInput);
    const transport = values.get("transport");
    if (
      typeof baseUrl !== "string" ||
      baseUrl.length === 0 ||
      (interceptors !== undefined &&
        !interceptors.every((interceptor) => typeof interceptor === "function")) ||
      (interceptorInput !== undefined && interceptors === undefined) ||
      (transport !== undefined && !isCapturableGrpcTransportOptions(transport))
    ) {
      throw new GrpcNodeConfigurationError(
        "Every gRPC logical client requires exact non-empty baseUrl and optional interceptors/transport.",
      );
    }
    Object.defineProperty(snapshot, key, {
      enumerable: true,
      value: Object.freeze({
        baseUrl,
        ...definedFields(interceptors, (interceptors) => ({
          interceptors: Object.freeze(
            interceptors.filter(
              (interceptor): interceptor is Interceptor => typeof interceptor === "function",
            ),
          ),
        })),
        ...definedFields(transport, (transport) => ({
          transport: snapshotTransport(transport),
        })),
      }),
    });
  }
  return Object.freeze(snapshot);
};

const snapshotOptions = (
  options: Readonly<Record<string, GrpcNodeClientOptions>>,
): Readonly<Record<string, GrpcNodeClientOptions>> => {
  try {
    return captureOptions(options);
  } catch (cause) {
    if (cause instanceof GrpcNodeConfigurationError) {
      throw cause;
    }
    throw configurationShapeError();
  }
};

const runtimeClient = (
  service: DescService,
  options: GrpcNodeClientOptions,
  ownedSessionManager: Http2SessionManager | undefined,
): GrpcRuntimeClient => {
  const transport = createGrpcTransport({
    ...options.transport,
    baseUrl: options.baseUrl,
    ...definedFields(options.interceptors, (interceptors) => ({
      interceptors: [...interceptors],
    })),
    ...definedFields(ownedSessionManager, (sessionManager) => ({ sessionManager })),
  });
  const client = createClient(service, transport);
  return Object.freeze({
    endpoints: Object.freeze([options.baseUrl]),
    service,
    invoke: <Request>(method: string, request: Request, signal: AbortSignal): unknown => {
      const selected = Reflect.get(client, method);
      return Reflect.apply(selected, client, [request, { signal }]);
    },
  });
};

const releaseOwnedSessionManager = Effect.fn("GrpcSourceAdapter.node.session.release")(
  (manager: Http2SessionManager) =>
    Effect.sync(() => manager.abort()).pipe(
      Effect.catchCause(() =>
        Effect.logWarning("gRPC Node HTTP/2 session manager finalization failed."),
      ),
    ),
);

const ownedSessionOptions = (
  transport: GrpcNodeClientOptions["transport"],
): Http2SessionOptions => {
  const options: Http2SessionOptions = {};
  if (transport?.idleConnectionTimeoutMs !== undefined) {
    options.idleConnectionTimeoutMs = transport.idleConnectionTimeoutMs;
  }
  if (transport?.pingIdleConnection !== undefined) {
    options.pingIdleConnection = transport.pingIdleConnection;
  }
  if (transport?.pingIntervalMs !== undefined) {
    options.pingIntervalMs = transport.pingIntervalMs;
  }
  if (transport?.pingTimeoutMs !== undefined) {
    options.pingTimeoutMs = transport.pingTimeoutMs;
  }
  return options;
};

const acquireRuntimeClient = Effect.fn("GrpcSourceAdapter.node.client.acquire")(function* (
  client: string,
  service: DescService,
  options: GrpcNodeClientOptions,
) {
  const ownedSessionManager =
    options.transport?.sessionManager === undefined
      ? yield* Effect.acquireRelease(
          Effect.try({
            try: () =>
              new Http2SessionManager(
                options.baseUrl,
                ownedSessionOptions(options.transport),
                options.transport?.nodeOptions,
              ),
            catch: () =>
              nodeConfigurationFailure(
                client,
                `Logical gRPC client ${client} transport construction failed.`,
              ),
          }),
          releaseOwnedSessionManager,
        )
      : undefined;
  return yield* Effect.try({
    try: () => runtimeClient(service, options, ownedSessionManager),
    catch: () =>
      nodeConfigurationFailure(
        client,
        `Logical gRPC client ${client} transport construction failed.`,
      ),
  });
});

const buildRuntimeClients = Effect.fn("GrpcSourceAdapter.node.clients.build")(function* (
  viewServer: GrpcServerViewServer,
  options: Readonly<Record<string, GrpcNodeClientOptions>>,
) {
  const bindingPlan = yield* grpcBindingPlan(viewServer);
  const services = bindingPlan.services;
  if (
    services.size !== Reflect.ownKeys(options).length ||
    Reflect.ownKeys(options).some((key) => typeof key !== "string" || !services.has(key))
  ) {
    return yield* Effect.fail(
      nodeConfigurationFailure(
        "",
        "The gRPC Node options must contain all and only referenced logical clients.",
      ),
    );
  }
  const clients: Record<string, GrpcRuntimeClient> = {};
  for (const [client, service] of services) {
    const optionsEntry = Option.getOrThrow(Option.fromUndefinedOr(options[client]));
    if (optionsEntry.transport !== undefined && !isGrpcTransportOptions(optionsEntry.transport)) {
      return yield* Effect.fail(
        nodeConfigurationFailure(
          client,
          `Logical gRPC client ${client} contains malformed transport resources.`,
        ),
      );
    }
    const built = yield* acquireRuntimeClient(client, service, optionsEntry);
    Object.defineProperty(clients, client, {
      enumerable: true,
      value: built,
    });
  }
  return {
    bindingPlan,
    clients: Object.freeze(clients) satisfies GrpcRuntimeClients,
  };
});

const layerFromSnapshot = <const ViewServer extends GrpcServerViewServer>(
  viewServer: ViewServer,
  captured: Readonly<Record<string, GrpcNodeClientOptions>>,
): Layer.Layer<
  import("effect").Context.Service.Identifier<typeof GrpcSourceAdapter.runtimeService>,
  GrpcAdapterFailure
> =>
  Layer.unwrap(
    buildRuntimeClients(viewServer, captured).pipe(
      Effect.map(({ bindingPlan, clients }) =>
        grpcServerLayerFromBindingPlan(bindingPlan, clients),
      ),
    ),
  );

const resolvedLayer = <const ViewServer extends GrpcServerViewServer>(
  viewServer: ViewServer,
  options: GrpcNodeOptions<ViewServer>,
): Layer.Layer<
  import("effect").Context.Service.Identifier<typeof GrpcSourceAdapter.runtimeService>,
  GrpcAdapterFailure
> => layerFromSnapshot(viewServer, snapshotOptions(options));

const snapshotConfigOptions = Effect.fn("GrpcSourceAdapter.node.config.snapshot")(function* (
  options: Readonly<Record<string, GrpcNodeClientOptions>>,
) {
  return yield* Effect.try({
    try: () => snapshotOptions(options),
    catch: () =>
      nodeConfigurationFailure(
        "",
        "The resolved gRPC Node configuration must contain exact non-empty client options.",
      ),
  });
});

const captureConfigWrappedValue = (value: unknown): unknown => {
  if (Config.isConfig(value)) {
    return value;
  }
  const entries = exactDataEntries(value);
  if (entries === undefined) {
    throw configurationShapeError();
  }
  const snapshot: Record<string, unknown> = {};
  for (const [key, entry] of entries) {
    Object.defineProperty(snapshot, key, {
      enumerable: true,
      value: captureConfigWrappedValue(entry),
    });
  }
  return Object.freeze(snapshot);
};

function captureConfigWrappedOptions(
  options: Config.Wrap<Readonly<Record<string, GrpcNodeClientOptions>>>,
): Config.Wrap<Readonly<Record<string, GrpcNodeClientOptions>>>;
function captureConfigWrappedOptions(options: unknown): unknown {
  if (Config.isConfig(options)) {
    return options;
  }
  const entries = exactDataEntries(options);
  if (entries === undefined || entries.length === 0) {
    throw configurationShapeError();
  }
  return captureConfigWrappedValue(options);
}

export const grpcNodeLayer = <
  const ViewServer extends GrpcServerViewServer,
  const Options extends GrpcNodeOptions<ViewServer>,
>(
  viewServer: ViewServer,
  options: ExactNodeOptions<ViewServer, Options>,
): Layer.Layer<
  import("effect").Context.Service.Identifier<typeof GrpcSourceAdapter.runtimeService>,
  GrpcAdapterFailure
> => resolvedLayer(viewServer, options);

export function grpcNodeLayerConfig<const ViewServer extends GrpcServerViewServer, const Options>(
  viewServer: ViewServer,
  options: Options,
  ..._invalid: IsAny<Options> extends true
    ? [invalidOptions: never]
    : [Options] extends [ExactConfigNodeOptions<ViewServer, NoInfer<Options>>]
      ? []
      : [invalidOptions: never]
): Layer.Layer<
  import("effect").Context.Service.Identifier<typeof GrpcSourceAdapter.runtimeService>,
  GrpcAdapterFailure | Config.ConfigError
>;
export function grpcNodeLayerConfig(
  viewServer: GrpcServerViewServer,
  options: Config.Wrap<Readonly<Record<string, GrpcNodeClientOptions>>>,
): Layer.Layer<
  import("effect").Context.Service.Identifier<typeof GrpcSourceAdapter.runtimeService>,
  GrpcAdapterFailure | Config.ConfigError
> {
  return Layer.unwrap(
    Effect.try({
      try: () => captureConfigWrappedOptions(options),
      catch: () =>
        nodeConfigurationFailure(
          "",
          "The gRPC Node Config input must contain exact enumerable string data fields.",
        ),
    }).pipe(
      Effect.flatMap((captured) => Config.unwrap(captured)),
      Effect.flatMap(snapshotConfigOptions),
      Effect.map((resolved) => layerFromSnapshot(viewServer, resolved)),
    ),
  );
}

export const grpcNode = Object.freeze({
  layer: grpcNodeLayer,
  layerConfig: grpcNodeLayerConfig,
});
