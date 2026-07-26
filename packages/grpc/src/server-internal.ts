import type { DescMessage, DescMethodServerStreaming, DescService } from "@bufbuild/protobuf";
import { ConnectError } from "@connectrpc/connect";
import { codeToString } from "@connectrpc/connect/protocol-connect";
import {
  SourceAdapterServer,
  type SourceAdapterServerLifecycle,
  type SourceAdapterServerView,
} from "@effect-view-server/source-adapter/server";
import type {
  SourceExecutionFailure,
  SourceLaneEvent,
  SourceLifecycleFactoryInput,
  SourceToolkit,
} from "@effect-view-server/source-adapter";
import {
  Cause,
  Chunk,
  Clock,
  Context,
  Effect,
  Exit,
  Layer,
  Option,
  Schedule,
  Scope,
  Stream,
} from "effect";
import {
  captureGrpcSourceDefinitionOptions,
  GrpcSourceAdapter,
  isGrpcServiceDescriptor,
  selectedGrpcMethod,
  type GrpcAdapterFailure,
  type GrpcLeasedDefinitionOptions,
  type GrpcLeasedMetrics,
  type GrpcMaterializedDefinitionOptions,
  type GrpcMaterializedMetrics,
  type GrpcRejectionLocation,
} from "./model";
import { validateAndSnapshotGrpcRequest } from "./request";

export type GrpcRuntimeClient = {
  readonly service: DescService;
  readonly invoke: (method: string, request: unknown, signal: AbortSignal) => unknown;
};

export type GrpcRuntimeClients = Readonly<Record<string, GrpcRuntimeClient>>;

export type GrpcServerViewServer = SourceAdapterServerView;

type GrpcDefinitionOptions = GrpcMaterializedDefinitionOptions | GrpcLeasedDefinitionOptions;

type MutableMetrics = {
  readonly logicalClient: string;
  readonly method: string;
  activeInvocations: bigint;
  acquisitionCount: bigint;
  reconnectCount: bigint;
  completionCount: bigint;
  streamFailureCount: bigint;
  messageCount: bigint;
  mappedMessageCount: bigint;
  rejectedMessageCount: bigint;
  cancellationCount: bigint;
  finalizationCount: bigint;
  finalizationFailureCount: bigint;
};

type RequestState =
  | {
      readonly _tag: "Uninitialized";
    }
  | {
      readonly _tag: "Failed";
      readonly failure: GrpcAdapterFailure;
    }
  | {
      readonly _tag: "Ready";
      readonly request: unknown;
    };

type LogicalState = {
  readonly metrics: MutableMetrics;
  request: RequestState;
};

type GrpcRuntimeRegistryValue = {
  readonly clients: ReadonlyMap<string, GrpcRuntimeClient>;
  readonly definitions: WeakMap<object, GrpcDefinitionOptions>;
  readonly logicalStates: WeakMap<object, LogicalState>;
  readonly methods: WeakMap<object, DescMethodServerStreaming<DescMessage, DescMessage>>;
};

class GrpcRuntimeRegistry extends Context.Service<GrpcRuntimeRegistry, GrpcRuntimeRegistryValue>()(
  "@effect-view-server/grpc/RuntimeRegistry",
) {}

const adapterFailure = (
  failure: GrpcAdapterFailure,
): SourceExecutionFailure<GrpcAdapterFailure> => ({
  _tag: "AdapterFailure",
  failure,
});

const configurationFailure = (
  client: string,
  message: string,
  phase: Extract<
    GrpcAdapterFailure,
    { readonly _tag: "GrpcConfigurationFailure" }
  >["phase"] = "configuration",
): GrpcAdapterFailure => ({
  _tag: "GrpcConfigurationFailure",
  client,
  message,
  phase,
});

const requestFailure = (
  definition: GrpcDefinitionOptions,
): Extract<GrpcAdapterFailure, { readonly _tag: "GrpcRequestConstructionFailure" }> => ({
  _tag: "GrpcRequestConstructionFailure",
  client: definition.client,
  method: definition.method,
  message: "The gRPC request factory threw or returned an invalid generated request-init object.",
});

const connectCode = (cause: unknown): string => {
  try {
    return codeToString(ConnectError.from(cause).code).toUpperCase();
  } catch {
    return "UNKNOWN";
  }
};

const invocationFailure = (
  definition: GrpcDefinitionOptions,
  cause: unknown,
): Extract<GrpcAdapterFailure, { readonly _tag: "GrpcInvocationFailure" }> => ({
  _tag: "GrpcInvocationFailure",
  client: definition.client,
  method: definition.method,
  code: connectCode(cause),
  message: "The selected gRPC server-streaming method could not be invoked.",
});

const streamFailure = (
  definition: GrpcDefinitionOptions,
  cause: unknown,
): Extract<GrpcAdapterFailure, { readonly _tag: "GrpcStreamFailure" }> => ({
  _tag: "GrpcStreamFailure",
  client: definition.client,
  method: definition.method,
  code: connectCode(cause),
  message: "The upstream gRPC response stream failed.",
});

const mappingFailure = (
  definition: GrpcDefinitionOptions,
): Extract<GrpcAdapterFailure, { readonly _tag: "GrpcMappingFailure" }> => ({
  _tag: "GrpcMappingFailure",
  client: definition.client,
  method: definition.method,
  message: "The synchronous gRPC Mapping callback threw.",
});

const cancellationFailure = (
  definition: GrpcDefinitionOptions,
): Extract<GrpcAdapterFailure, { readonly _tag: "GrpcCancellationFailure" }> => ({
  _tag: "GrpcCancellationFailure",
  client: definition.client,
  method: definition.method,
  message: "The upstream gRPC invocation rejected cancellation or iterator finalization.",
});

const initialMetrics = (definition: GrpcDefinitionOptions): MutableMetrics => ({
  logicalClient: definition.client,
  method: definition.method,
  activeInvocations: 0n,
  acquisitionCount: 0n,
  reconnectCount: 0n,
  completionCount: 0n,
  streamFailureCount: 0n,
  messageCount: 0n,
  mappedMessageCount: 0n,
  rejectedMessageCount: 0n,
  cancellationCount: 0n,
  finalizationCount: 0n,
  finalizationFailureCount: 0n,
});

const logicalState = (
  runtime: GrpcRuntimeRegistryValue,
  target: object,
  definition: GrpcDefinitionOptions,
): LogicalState => {
  const current = runtime.logicalStates.get(target);
  if (current !== undefined) {
    return current;
  }
  const created: LogicalState = {
    metrics: initialMetrics(definition),
    request: {
      _tag: "Uninitialized",
    },
  };
  runtime.logicalStates.set(target, created);
  return created;
};

const materializedMetrics = (metrics: MutableMetrics): GrpcMaterializedMetrics => ({
  logicalClient: metrics.logicalClient,
  method: metrics.method,
  activeInvocations: metrics.activeInvocations,
  acquisitionCount: metrics.acquisitionCount,
  reconnectCount: metrics.reconnectCount,
  completionCount: metrics.completionCount,
  streamFailureCount: metrics.streamFailureCount,
  messageCount: metrics.messageCount,
  mappedMessageCount: metrics.mappedMessageCount,
  rejectedMessageCount: metrics.rejectedMessageCount,
  cancellationCount: metrics.cancellationCount,
  finalizationCount: metrics.finalizationCount,
  finalizationFailureCount: metrics.finalizationFailureCount,
});

const leasedMetrics = (metrics: MutableMetrics): GrpcLeasedMetrics => ({
  ...materializedMetrics(metrics),
  activeFeeds: 1n,
});

const ensureRequest = Effect.fn("GrpcSourceAdapter.request.ensure")(function* (
  runtime: GrpcRuntimeRegistryValue,
  state: LogicalState,
  definition: GrpcDefinitionOptions,
  target: object,
) {
  if (state.request._tag === "Ready") {
    return state.request.request;
  }
  if (state.request._tag === "Failed") {
    return yield* Effect.fail(adapterFailure(state.request.failure));
  }
  const method = runtime.methods.get(definition);
  if (method === undefined) {
    return yield* Effect.fail(
      adapterFailure(
        configurationFailure(
          definition.client,
          "The validated gRPC method binding is unavailable for this Source Definition.",
        ),
      ),
    );
  }
  const result = yield* Effect.try({
    try: () => {
      const route =
        Reflect.get(target, "_tag") === "Leased" ? Reflect.get(target, "route") : undefined;
      const candidate =
        route === undefined
          ? Reflect.apply(definition.request, undefined, [])
          : Reflect.apply(definition.request, undefined, [route]);
      return validateAndSnapshotGrpcRequest(method.input, candidate);
    },
    catch: () => requestFailure(definition),
  }).pipe(
    Effect.matchEffect({
      onFailure: (failure) =>
        Effect.sync(() => {
          state.request = {
            _tag: "Failed",
            failure,
          };
          return failure;
        }).pipe(Effect.flatMap((failure) => Effect.fail(adapterFailure(failure)))),
      onSuccess: (request) =>
        Effect.sync(() => {
          state.request = {
            _tag: "Ready",
            request,
          };
          return request;
        }),
    }),
  );
  return result;
});

const isAsyncIterator = (value: unknown): value is AsyncIterator<unknown> =>
  typeof value === "object" && value !== null && typeof Reflect.get(value, "next") === "function";

const isAsyncIterable = (value: unknown): value is AsyncIterable<unknown> =>
  typeof value === "object" &&
  value !== null &&
  typeof Reflect.get(value, Symbol.asyncIterator) === "function";

const iteratorDone = (): IteratorReturnResult<undefined> => ({
  done: true,
  value: undefined,
});

export const makeOwnedGrpcIterable = (
  iterator: AsyncIterator<unknown>,
  controller: AbortController,
): {
  readonly iterable: AsyncIterable<unknown>;
  readonly finalize: () => Promise<
    | {
        readonly _tag: "Success";
      }
    | {
        readonly _tag: "Failure";
      }
  >;
} => {
  type FinalizationOutcome =
    | {
        readonly _tag: "Success";
      }
    | {
        readonly _tag: "Failure";
      };
  let finalization: Promise<FinalizationOutcome> | undefined;
  const finalize = (): Promise<FinalizationOutcome> => {
    if (finalization !== undefined) {
      return finalization;
    }
    finalization = Promise.resolve().then(async () => {
      try {
        controller.abort();
        const returnMethod = Reflect.get(iterator, "return");
        if (typeof returnMethod === "function") {
          await Reflect.apply(returnMethod, iterator, []);
        }
        return {
          _tag: "Success",
        };
      } catch {
        return {
          _tag: "Failure",
        };
      }
    });
    return finalization;
  };
  const owned: AsyncIterator<unknown> = {
    next: () => (finalization === undefined ? iterator.next() : Promise.resolve(iteratorDone())),
    return: () => finalize().then(iteratorDone),
  };
  return {
    iterable: {
      [Symbol.asyncIterator]: () => owned,
    },
    finalize,
  };
};

const rejectionLocation = (
  definition: GrpcDefinitionOptions,
  phase: GrpcRejectionLocation["phase"],
  streamItemIndex: bigint,
): GrpcRejectionLocation => ({
  client: definition.client,
  method: definition.method,
  phase,
  streamItemIndex,
});

const rejectMappedItem = <Row extends object, Services, Topic extends string>(
  toolkit: SourceToolkit<Row, GrpcAdapterFailure, GrpcRejectionLocation, Services, Topic>,
  definition: GrpcDefinitionOptions,
  state: LogicalState,
  streamItemIndex: bigint,
  failure: SourceExecutionFailure<GrpcAdapterFailure>,
  phase: GrpcRejectionLocation["phase"],
): Effect.Effect<
  SourceLaneEvent<Row, GrpcAdapterFailure, GrpcRejectionLocation, Services>,
  SourceExecutionFailure<GrpcAdapterFailure>
> =>
  Effect.sync(() => {
    state.metrics.rejectedMessageCount += 1n;
  }).pipe(
    Effect.andThen(Clock.currentTimeNanos),
    Effect.flatMap((rejectedAtNanos) =>
      toolkit.reject({
        failure,
        location: rejectionLocation(definition, phase, streamItemIndex),
        rejectedAtNanos,
      }),
    ),
  );

const mappedEvent = Effect.fnUntraced(function* <
  Row extends object,
  Services,
  Topic extends string,
>(
  toolkit: SourceToolkit<Row, GrpcAdapterFailure, GrpcRejectionLocation, Services, Topic>,
  definition: GrpcDefinitionOptions,
  route: Readonly<Record<string, unknown>> | undefined,
  state: LogicalState,
  value: unknown,
) {
  state.metrics.messageCount += 1n;
  const streamItemIndex = state.metrics.messageCount;
  const mapped = yield* Effect.try({
    try: () =>
      route === undefined
        ? Reflect.apply(definition.mapValue, undefined, [value])
        : Reflect.apply(definition.mapValue, undefined, [value, route]),
    catch: () => mappingFailure(definition),
  }).pipe(
    Effect.matchEffect({
      onFailure: (failure) =>
        rejectMappedItem(
          toolkit,
          definition,
          state,
          streamItemIndex,
          adapterFailure(failure),
          "mapping",
        ).pipe(
          Effect.map((event) => ({
            _tag: "Rejected" as const,
            event,
          })),
        ),
      onSuccess: (row) =>
        Effect.succeed({
          _tag: "Mapped" as const,
          row,
        }),
    }),
  );
  if (mapped._tag === "Rejected") {
    return mapped.event;
  }
  return yield* toolkit.upsert(mapped.row).pipe(
    Effect.matchEffect({
      onFailure: (failure) => {
        const phase =
          failure._tag === "RuntimeFailure" && failure.failure._tag === "InvalidFeedRoute"
            ? "route"
            : "row";
        return rejectMappedItem(toolkit, definition, state, streamItemIndex, failure, phase);
      },
      onSuccess: (upsert) =>
        Effect.sync(() => {
          state.metrics.mappedMessageCount += 1n;
        }).pipe(Effect.andThen(toolkit.delivery(Chunk.of(upsert)))),
    }),
  );
});

const acquireAttempt = Effect.fn("GrpcSourceAdapter.attempt.acquire")(function* <
  Lifecycle extends "materialized" | "leased",
  Row extends object,
  Route extends Readonly<Record<string, unknown>>,
  Topic extends string,
>(
  input: SourceLifecycleFactoryInput<
    Lifecycle extends "materialized"
      ? GrpcMaterializedDefinitionOptions<string, Row>
      : GrpcLeasedDefinitionOptions<string, Row>,
    Lifecycle,
    Route,
    Row,
    GrpcAdapterFailure,
    GrpcRejectionLocation,
    GrpcRuntimeRegistry,
    Topic
  >,
) {
  const runtime = yield* GrpcRuntimeRegistry;
  const definition = yield* Effect.fromOption(
    Option.fromUndefinedOr(runtime.definitions.get(input.definition)),
    () =>
      adapterFailure(
        configurationFailure(
          input.definition.client,
          "The captured gRPC Source Definition is unavailable during client lookup.",
          "client-lookup",
        ),
      ),
  );
  const target = input.target;
  const state = logicalState(runtime, target, definition);
  const request = yield* ensureRequest(runtime, state, definition, target);
  const client = yield* Effect.fromOption(
    Option.fromUndefinedOr(runtime.clients.get(definition.client)),
    () =>
      adapterFailure(
        configurationFailure(
          definition.client,
          "The configured gRPC runtime client is unavailable during client lookup.",
          "client-lookup",
        ),
      ),
  );
  const controller = yield* Effect.acquireRelease(
    Effect.sync(() => new AbortController()),
    (ownedController) =>
      Effect.sync(() => {
        ownedController.abort();
      }),
  );
  const iterable = yield* Effect.try({
    try: () => {
      const candidate = client.invoke(definition.method, request, controller.signal);
      return isAsyncIterable(candidate) ? candidate : undefined;
    },
    catch: (cause) => adapterFailure(invocationFailure(definition, cause)),
  }).pipe(
    Effect.flatMap((candidate) =>
      candidate === undefined
        ? Effect.fail(
            adapterFailure(
              invocationFailure(
                definition,
                new TypeError("The selected gRPC method did not return an AsyncIterable."),
              ),
            ),
          )
        : Effect.succeed(candidate),
    ),
  );
  const iterator = yield* Effect.try({
    try: () => {
      const candidate = iterable[Symbol.asyncIterator]();
      return isAsyncIterator(candidate) ? candidate : undefined;
    },
    catch: (cause) => adapterFailure(invocationFailure(definition, cause)),
  }).pipe(
    Effect.flatMap((candidate) =>
      candidate === undefined
        ? Effect.fail(
            adapterFailure(
              invocationFailure(
                definition,
                new TypeError("The gRPC response stream did not return an AsyncIterator."),
              ),
            ),
          )
        : Effect.succeed(candidate),
    ),
  );
  const owned = makeOwnedGrpcIterable(iterator, controller);
  state.metrics.acquisitionCount += 1n;
  state.metrics.reconnectCount = state.metrics.acquisitionCount - 1n;
  state.metrics.activeInvocations += 1n;
  let ended = false;
  let failed = false;
  const finalize = yield* Effect.cached(
    Effect.promise(owned.finalize).pipe(
      Effect.flatMap((outcome) =>
        outcome._tag === "Failure"
          ? Effect.sync(() => {
              state.metrics.finalizationFailureCount += 1n;
            }).pipe(
              Effect.andThen(
                Effect.logWarning("gRPC Source Attempt finalization failed.").pipe(
                  Effect.annotateLogs({
                    adapter: GrpcSourceAdapter.identity,
                    client: definition.client,
                    failure: cancellationFailure(definition),
                    lifecycle: input.target._tag,
                    method: definition.method,
                    topic: input.toolkit.topic,
                  }),
                ),
              ),
            )
          : Effect.void,
      ),
      Effect.ensuring(
        Effect.sync(() => {
          state.metrics.finalizationCount += 1n;
          state.metrics.activeInvocations -= 1n;
          if (!ended && !failed) {
            state.metrics.cancellationCount += 1n;
          }
        }),
      ),
    ),
  );
  yield* Scope.addFinalizer(yield* Effect.scope, finalize);
  const route = input.target._tag === "Leased" ? input.target.route : undefined;
  const events = Stream.fromAsyncIterable(owned.iterable, (cause) =>
    adapterFailure(streamFailure(definition, cause)),
  ).pipe(
    Stream.mapEffect((value) =>
      mappedEvent<Row, GrpcRuntimeRegistry, Topic>(input.toolkit, definition, route, state, value),
    ),
    Stream.onExit((exit) =>
      Effect.sync(() => {
        if (Exit.isSuccess(exit)) {
          ended = true;
          state.metrics.completionCount += 1n;
        } else if (!Cause.hasInterruptsOnly(exit.cause)) {
          failed = true;
          state.metrics.streamFailureCount += 1n;
        }
      }).pipe(Effect.andThen(finalize)),
    ),
  );
  return SourceAdapterServer.attempt<
    Row,
    GrpcAdapterFailure,
    GrpcRejectionLocation,
    GrpcRuntimeRegistry
  >([
    SourceAdapterServer.lane<Row, GrpcAdapterFailure, GrpcRejectionLocation, GrpcRuntimeRegistry>({
      id: "grpc",
      events,
    }),
  ]);
});

const acquireMaterialized: SourceAdapterServerLifecycle<
  GrpcAdapterFailure,
  NonNullable<typeof GrpcSourceAdapter.materialized>,
  "materialized",
  GrpcRuntimeRegistry
>["acquire"] = <
  const Topic extends string,
  Row extends object,
  Route extends Readonly<Record<string, unknown>>,
>(
  input: SourceLifecycleFactoryInput<
    GrpcMaterializedDefinitionOptions<string, Row>,
    "materialized",
    Route,
    Row,
    GrpcAdapterFailure,
    GrpcRejectionLocation,
    GrpcRuntimeRegistry,
    Topic
  >,
) => acquireAttempt<"materialized", Row, Route, Topic>(input);

const acquireLeased: SourceAdapterServerLifecycle<
  GrpcAdapterFailure,
  NonNullable<typeof GrpcSourceAdapter.leased>,
  "leased",
  GrpcRuntimeRegistry
>["acquire"] = <
  const Topic extends string,
  Row extends object,
  Route extends Readonly<Record<string, unknown>>,
>(
  input: SourceLifecycleFactoryInput<
    GrpcLeasedDefinitionOptions<string, Row>,
    "leased",
    Route,
    Row,
    GrpcAdapterFailure,
    GrpcRejectionLocation,
    GrpcRuntimeRegistry,
    Topic
  >,
) => acquireAttempt<"leased", Row, Route, Topic>(input);

const materializedLifecycle: SourceAdapterServerLifecycle<
  GrpcAdapterFailure,
  NonNullable<typeof GrpcSourceAdapter.materialized>,
  "materialized",
  GrpcRuntimeRegistry
> = {
  acquire: acquireMaterialized,
  metrics: (input) =>
    Effect.gen(function* () {
      const runtime = yield* GrpcRuntimeRegistry;
      const definition = input.definition;
      return materializedMetrics(logicalState(runtime, input.target, definition).metrics);
    }),
  retry: Schedule.spaced("1 second"),
};

const leasedLifecycle: SourceAdapterServerLifecycle<
  GrpcAdapterFailure,
  NonNullable<typeof GrpcSourceAdapter.leased>,
  "leased",
  GrpcRuntimeRegistry
> = {
  acquire: acquireLeased,
  metrics: (input) =>
    Effect.gen(function* () {
      const runtime = yield* GrpcRuntimeRegistry;
      const definition = input.definition;
      return leasedMetrics(logicalState(runtime, input.target, definition).metrics);
    }),
  retry: Schedule.spaced("1 second"),
};

const GrpcSourceServer = SourceAdapterServer.make<
  "grpc",
  "1",
  GrpcAdapterFailure,
  NonNullable<typeof GrpcSourceAdapter.materialized>,
  NonNullable<typeof GrpcSourceAdapter.leased>,
  GrpcRuntimeRegistry
>(GrpcSourceAdapter, {
  materialized: materializedLifecycle,
  leased: leasedLifecycle,
});

export type GrpcBindingPlan = {
  readonly definitions: WeakMap<object, GrpcDefinitionOptions>;
  readonly methods: WeakMap<object, DescMethodServerStreaming<DescMessage, DescMessage>>;
  readonly services: ReadonlyMap<string, DescService>;
};

export const grpcBindingPlan = Effect.fn("GrpcSourceAdapter.bindings.collect")(function* (
  viewServer: GrpcServerViewServer,
) {
  const bindings = new Map<string, DescService>();
  const capturedDefinitions = new WeakMap<object, GrpcDefinitionOptions>();
  const methods = new WeakMap<object, DescMethodServerStreaming<DescMessage, DescMessage>>();
  const definitions = yield* Effect.try({
    try: () =>
      SourceAdapterServer.definitions(viewServer, GrpcSourceAdapter).map(
        ({ topic, definition: source }) => {
          const options = source.options;
          return {
            captured: captureGrpcSourceDefinitionOptions(options),
            original: typeof options === "object" && options !== null ? options : undefined,
            topic,
          };
        },
      ),
    catch: () =>
      configurationFailure(
        "",
        "The View Server gRPC Source Definitions could not be inspected safely.",
      ),
  });
  for (const { captured: definition, original, topic } of definitions) {
    if (definition === undefined || original === undefined) {
      return yield* Effect.fail(
        configurationFailure(
          "",
          `Topic ${topic} contains invalid or tampered gRPC Source Definition options.`,
        ),
      );
    }
    const resolved = yield* Effect.try({
      try: () => {
        const service = definition.service();
        if (!isGrpcServiceDescriptor(service)) {
          return undefined;
        }
        const method = selectedGrpcMethod(service, definition.method);
        return method === undefined ? undefined : { method, service };
      },
      catch: () =>
        configurationFailure(
          definition.client,
          `Topic ${topic} could not resolve its generated gRPC service descriptor.`,
        ),
    });
    if (resolved === undefined) {
      return yield* Effect.fail(
        configurationFailure(
          definition.client,
          `Topic ${topic} must resolve a generated service and select one of its server-streaming methods.`,
        ),
      );
    }
    const { method, service } = resolved;
    const previous = bindings.get(definition.client);
    if (previous !== undefined && previous !== service) {
      return yield* Effect.fail(
        configurationFailure(
          definition.client,
          `Logical gRPC client ${definition.client} is bound to multiple generated services.`,
        ),
      );
    }
    bindings.set(definition.client, service);
    capturedDefinitions.set(original, definition);
    methods.set(definition, method);
  }
  if (bindings.size === 0) {
    return yield* Effect.fail(
      configurationFailure("", "The gRPC aggregate Layer requires at least one gRPC source."),
    );
  }
  return {
    definitions: capturedDefinitions,
    methods,
    services: bindings,
  } satisfies GrpcBindingPlan;
});

export const grpcServiceBindings = Effect.fn("GrpcSourceAdapter.bindings.services")(function* (
  viewServer: GrpcServerViewServer,
) {
  return (yield* grpcBindingPlan(viewServer)).services;
});

const makeRegistry = Effect.fn("GrpcSourceAdapter.registry.make")(function* (
  bindingPlan: GrpcBindingPlan,
  clients: GrpcRuntimeClients,
) {
  const bindings = bindingPlan.services;
  const captured = yield* Effect.try({
    try: () => {
      if (typeof clients !== "object" || clients === null || Array.isArray(clients)) {
        throw new TypeError("Invalid gRPC aggregate client map.");
      }
      const prototype = Object.getPrototypeOf(clients);
      if (prototype !== Object.prototype && prototype !== null) {
        throw new TypeError("Invalid gRPC aggregate client map.");
      }
      const entries = new Map<
        string,
        {
          readonly invoke: (...arguments_: ReadonlyArray<unknown>) => unknown;
          readonly service: unknown;
        }
      >();
      for (const key of Reflect.ownKeys(clients)) {
        if (typeof key !== "string") {
          throw new TypeError("Invalid gRPC aggregate client key.");
        }
        const descriptor = Object.getOwnPropertyDescriptor(clients, key);
        if (
          descriptor === undefined ||
          !descriptor.enumerable ||
          !("value" in descriptor) ||
          typeof descriptor.value !== "object" ||
          descriptor.value === null ||
          Array.isArray(descriptor.value)
        ) {
          throw new TypeError("Invalid gRPC aggregate client entry.");
        }
        const runtimeKeys = Reflect.ownKeys(descriptor.value);
        const serviceDescriptor = Object.getOwnPropertyDescriptor(descriptor.value, "service");
        const invokeDescriptor = Object.getOwnPropertyDescriptor(descriptor.value, "invoke");
        if (
          runtimeKeys.length !== 2 ||
          runtimeKeys.some((runtimeKey) => runtimeKey !== "service" && runtimeKey !== "invoke") ||
          serviceDescriptor === undefined ||
          !serviceDescriptor.enumerable ||
          !("value" in serviceDescriptor) ||
          invokeDescriptor === undefined ||
          !invokeDescriptor.enumerable ||
          !("value" in invokeDescriptor) ||
          typeof invokeDescriptor.value !== "function"
        ) {
          throw new TypeError("Invalid gRPC aggregate client entry.");
        }
        entries.set(key, {
          invoke: (...arguments_) =>
            Reflect.apply(invokeDescriptor.value, descriptor.value, arguments_),
          service: serviceDescriptor.value,
        });
      }
      return entries;
    },
    catch: () =>
      configurationFailure(
        "",
        "The gRPC aggregate client map must contain exact data-only runtime clients.",
        "client-construction",
      ),
  });
  if (captured.size !== bindings.size || [...captured.keys()].some((key) => !bindings.has(key))) {
    return yield* Effect.fail(
      configurationFailure(
        "",
        "The gRPC aggregate client map must contain all and only referenced logical clients.",
      ),
    );
  }
  const runtimeClients = new Map<string, GrpcRuntimeClient>();
  for (const [client, service] of bindings) {
    const runtime = captured.get(client);
    if (runtime === undefined || runtime.service !== service) {
      return yield* Effect.fail(
        configurationFailure(
          client,
          `Logical gRPC client ${client} is missing, malformed, or uses the wrong service.`,
          "client-construction",
        ),
      );
    }
    runtimeClients.set(
      client,
      Object.freeze({
        service,
        invoke: (method: string, request: unknown, signal: AbortSignal): unknown =>
          runtime.invoke(method, request, signal),
      }),
    );
  }
  return {
    clients: runtimeClients,
    definitions: bindingPlan.definitions,
    logicalStates: new WeakMap<object, LogicalState>(),
    methods: bindingPlan.methods,
  } satisfies GrpcRuntimeRegistryValue;
});

export const grpcServerLayerFromBindingPlan = (
  bindingPlan: GrpcBindingPlan,
  clients: GrpcRuntimeClients,
): Layer.Layer<
  Context.Service.Identifier<typeof GrpcSourceAdapter.runtimeService>,
  GrpcAdapterFailure
> => {
  const registry = Layer.effect(GrpcRuntimeRegistry)(makeRegistry(bindingPlan, clients));
  return GrpcSourceServer.pipe(Layer.provide(registry));
};

export const grpcServerLayer = (
  viewServer: GrpcServerViewServer,
  clients: GrpcRuntimeClients,
): Layer.Layer<
  Context.Service.Identifier<typeof GrpcSourceAdapter.runtimeService>,
  GrpcAdapterFailure
> => {
  return Layer.unwrap(
    grpcBindingPlan(viewServer).pipe(
      Effect.map((bindingPlan) => grpcServerLayerFromBindingPlan(bindingPlan, clients)),
    ),
  );
};

export const grpcServer = Object.freeze({
  layer: grpcServerLayer,
});
