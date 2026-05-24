import * as AtomReact from "@effect/atom-react";
import {
  createColumnLiveViewEngine,
  InvalidQueryError,
  InvalidRowError,
  InvalidTopicError,
  UnsupportedQueryError,
  type ColumnLiveViewEngine,
  type ColumnLiveViewEngineError,
  type ColumnLiveViewEngineEvent,
  type ColumnLiveViewEngineHealth,
  type DecodableTopicDefinitions,
} from "@view-server/column-live-view-engine";
import type {
  DeltaEvent,
  ExactPatch,
  ExactRawQuery,
  LiveQueryResult,
  LiveQueryRow,
  RawQuery,
  StatusEvent,
  TopicRow,
  ValidateLiveQuery,
  ViewServerConfig,
  ViewServerHealth,
  ViewServerInMemoryRuntime,
  ViewServerRuntimeError,
} from "@view-server/config";
import { Effect, Stream } from "effect";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import * as Atom from "effect/unstable/reactivity/Atom";
import * as AtomRef from "effect/unstable/reactivity/AtomRef";
import { createElement, useMemo, type ReactNode } from "react";

type ReactBindings<Topics extends DecodableTopicDefinitions> = {
  readonly ViewServerInMemoryProvider: (props: ViewServerInMemoryProviderProps) => ReactNode;
  readonly useLiveQuery: UseLiveQueryHook<Topics>;
  readonly useViewServerHealth: () => ViewServerHealth<Topics>;
  readonly useViewServerTestRuntime: () => ViewServerInMemoryRuntime<Topics>;
};

export type ViewServerInMemoryProviderProps = {
  readonly children: ReactNode;
  readonly subscriptionQueueCapacity?: number;
};

export type UseLiveQueryHook<Topics extends DecodableTopicDefinitions> = <
  Topic extends Extract<keyof Topics, string>,
  const Query extends RawQuery<TopicRow<Topics, Topic>>,
>(
  topic: Topic,
  query: Query &
    RawQuery<TopicRow<Topics, Topic>> &
    ExactRawQuery<TopicRow<Topics, Topic>, Query> &
    ValidateLiveQuery<Query>,
) => LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>;

type ProviderState<Topics extends DecodableTopicDefinitions> = {
  readonly engine: ColumnLiveViewEngine<Topics>;
  readonly runtime: ViewServerInMemoryRuntime<Topics>;
  readonly health: AtomRef.AtomRef<ViewServerHealth<Topics>>;
};

type ProviderInput = {
  readonly subscriptionQueueCapacity?: number;
};

type ClientState<Row> = {
  readonly rows: ReadonlyArray<Row>;
  readonly keys: ReadonlyArray<string>;
  readonly totalRows: number;
  readonly version: number;
  readonly status: LiveQueryResult<Row>["status"];
  readonly statusCode?: LiveQueryResult<Row>["statusCode"];
  readonly message?: string | undefined;
};

const initialClientState = <Row,>(): ClientState<Row> => ({
  rows: [],
  keys: [],
  totalRows: 0,
  version: 0,
  status: "loading",
});

const liveQueryResult = <Row,>(state: ClientState<Row>): LiveQueryResult<Row> => ({
  rows: state.rows,
  totalRows: state.totalRows,
  version: state.version,
  status: state.status,
  statusCode: state.statusCode,
  message: state.message,
});

const stableQueryValue = (value: unknown): unknown => {
  if (typeof value === "bigint") {
    return { $bigint: value.toString() };
  }
  if (Array.isArray(value)) {
    return value.map(stableQueryValue);
  }
  if (typeof value === "object" && value !== null) {
    return Object.fromEntries(
      Object.entries(value)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, entry]) => [key, stableQueryValue(entry)]),
    );
  }
  return value;
};

const stableQueryKey = (query: object): string => JSON.stringify(stableQueryValue(query));

const engineErrorToRuntimeError = (error: ColumnLiveViewEngineError): ViewServerRuntimeError => {
  if (error instanceof InvalidTopicError) {
    return {
      _tag: "ViewServerRuntimeError",
      code: "InvalidTopic",
      message: error.message,
      topic: error.topic,
    };
  }
  if (error instanceof InvalidRowError) {
    return {
      _tag: "ViewServerRuntimeError",
      code: "InvalidRow",
      message: error.message,
      topic: error.topic,
    };
  }
  if (error instanceof InvalidQueryError) {
    return {
      _tag: "ViewServerRuntimeError",
      code: "SnapshotStale",
      message: error.message,
      topic: error.topic,
    };
  }
  if (error instanceof UnsupportedQueryError) {
    return {
      _tag: "ViewServerRuntimeError",
      code: "SnapshotStale",
      message: error.message,
      topic: error.topic,
    };
  }
  return {
    _tag: "ViewServerRuntimeError",
    code: "RuntimeUnavailable",
    message: error.message,
  };
};

const applySnapshot = <Row,>(
  event: Extract<ColumnLiveViewEngineEvent<Row>, { readonly type: "snapshot" }>,
): ClientState<Row> => ({
  rows: event.rows,
  keys: event.keys,
  totalRows: event.totalRows,
  version: event.version,
  status: "ready",
  statusCode: "Ready",
});

const applyDeltaOperation = <Row,>(
  state: ClientState<Row>,
  operation: DeltaEvent<Row>["operations"][number],
): ClientState<Row> => {
  if (operation.type === "insert") {
    const nextRows = state.rows.slice();
    const nextKeys = state.keys.slice();
    nextRows.splice(operation.index, 0, operation.row);
    nextKeys.splice(operation.index, 0, operation.key);
    return { ...state, rows: nextRows, keys: nextKeys };
  }
  if (operation.type === "update") {
    const nextRows = state.rows.slice();
    const nextKeys = state.keys.slice();
    nextRows[operation.index] = operation.row;
    nextKeys[operation.index] = operation.key;
    return { ...state, rows: nextRows, keys: nextKeys };
  }
  if (operation.type === "move") {
    const nextRows = state.rows.slice();
    const nextKeys = state.keys.slice();
    nextRows.splice(operation.toIndex, 0, ...nextRows.splice(operation.fromIndex, 1));
    nextKeys.splice(operation.toIndex, 0, ...nextKeys.splice(operation.fromIndex, 1));
    return { ...state, rows: nextRows, keys: nextKeys };
  }
  const nextRows = state.rows.filter((_row, index) => state.keys[index] !== operation.key);
  const nextKeys = state.keys.filter((key) => key !== operation.key);
  return { ...state, rows: nextRows, keys: nextKeys };
};

const applyDelta = <Row,>(state: ClientState<Row>, event: DeltaEvent<Row>): ClientState<Row> => {
  let nextState = state;
  for (const operation of event.operations) {
    nextState = applyDeltaOperation(nextState, operation);
  }
  return {
    rows: nextState.rows,
    keys: nextState.keys,
    totalRows: event.totalRows,
    version: event.toVersion,
    status: "ready",
    statusCode: "Ready",
  };
};

const applyStatus = <Row,>(state: ClientState<Row>, event: StatusEvent): ClientState<Row> => ({
  ...state,
  status: event.status,
  statusCode: event.code,
  message: event.message,
});

const applyEvent = <Row,>(
  state: ClientState<Row>,
  event: ColumnLiveViewEngineEvent<Row>,
): ClientState<Row> => {
  if (event.type === "snapshot") {
    return applySnapshot(event);
  }
  if (event.type === "delta") {
    return applyDelta(state, event);
  }
  return applyStatus(state, event);
};

const healthFromEngine = <Topics extends DecodableTopicDefinitions>(
  engineHealth: ColumnLiveViewEngineHealth<Topics>,
): ViewServerHealth<Topics> => {
  return {
    status: engineHealth.status,
    version: engineHealth.version,
    uptimeMs: 0,
    engine: { topics: engineHealth.topics },
    transport: {
      activeClients: 1,
      activeStreams: engineHealth.activeSubscriptions,
      activeSubscriptions: engineHealth.activeSubscriptions,
      messagesPerSecond: 0,
      bytesPerSecond: 0,
      queuedMessages: engineHealth.queuedEvents,
      queuedBytes: 0,
      droppedClients: 0,
      backpressureEvents: engineHealth.backpressureEvents,
      reconnects: 0,
      lastError: null,
    },
  };
};

const readHealth = <Topics extends DecodableTopicDefinitions>(
  engine: ColumnLiveViewEngine<Topics>,
  health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
) =>
  engine.health().pipe(
    Effect.map(healthFromEngine),
    Effect.tap((value) => Effect.sync(() => health.set(value))),
  );

const refreshHealth = <Topics extends DecodableTopicDefinitions>(
  engine: ColumnLiveViewEngine<Topics>,
  health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
) => readHealth(engine, health).pipe(Effect.asVoid);

export const createViewServerReact = <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
): ReactBindings<Topics> => {
  const makeRuntime = (
    engine: ColumnLiveViewEngine<Topics>,
    health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
  ): ViewServerInMemoryRuntime<Topics> => ({
    publish: (topic, row) =>
      engine
        .publish(topic, row)
        .pipe(
          Effect.andThen(refreshHealth(engine, health)),
          Effect.mapError(engineErrorToRuntimeError),
        ),
    publishMany: (topic, rows) =>
      engine
        .publishMany(topic, rows)
        .pipe(
          Effect.andThen(refreshHealth(engine, health)),
          Effect.mapError(engineErrorToRuntimeError),
        ),
    patch: (topic, key, patch) =>
      engine
        .patch(topic, key, patch as ExactPatch<TopicRow<Topics, typeof topic>, typeof patch>)
        .pipe(
          Effect.andThen(refreshHealth(engine, health)),
          Effect.mapError(engineErrorToRuntimeError),
        ),
    delete: (topic, key) =>
      engine
        .delete(topic, key)
        .pipe(
          Effect.andThen(refreshHealth(engine, health)),
          Effect.mapError(engineErrorToRuntimeError),
        ),
    snapshot: (topic, query) =>
      engine
        .snapshot(topic, query as ExactRawQuery<TopicRow<Topics, typeof topic>, typeof query>)
        .pipe(Effect.mapError(engineErrorToRuntimeError)),
    health: () => readHealth(engine, health).pipe(Effect.mapError(engineErrorToRuntimeError)),
    reset: () =>
      engine
        .reset()
        .pipe(
          Effect.andThen(refreshHealth(engine, health)),
          Effect.mapError(engineErrorToRuntimeError),
        ),
  });

  const makeProviderState = (input: ProviderInput): ProviderState<Topics> => {
    const engineConfig =
      input.subscriptionQueueCapacity === undefined
        ? { topics: config.topics }
        : {
            topics: config.topics,
            subscriptionQueueCapacity: input.subscriptionQueueCapacity,
          };
    const engine = Effect.runSync(createColumnLiveViewEngine<Topics>(engineConfig));
    const initialHealth = Effect.runSync(engine.health().pipe(Effect.map(healthFromEngine)));
    const health = AtomRef.make(initialHealth);
    const runtime = makeRuntime(engine, health);
    return {
      engine,
      runtime,
      health,
    };
  };

  const ProviderAtom = AtomReact.make((input: ProviderInput) =>
    Atom.make((get) => {
      const providerState = makeProviderState(input);
      get.addFinalizer(() => {
        Effect.runFork(providerState.engine.close());
      });
      return providerState;
    }),
  );

  const useProviderState = (): ProviderState<Topics> => AtomReact.useAtomValue(ProviderAtom.use());

  function ViewServerInMemoryProvider(props: ViewServerInMemoryProviderProps): ReactNode {
    const { children, ...input } = props;
    return createElement(
      AtomReact.RegistryProvider,
      { defaultIdleTTL: 0 },
      createElement(ProviderAtom.Provider, { value: input }, children),
    );
  }

  const useLiveQuery: UseLiveQueryHook<Topics> = (topic, query) => {
    const providerState = useProviderState();
    type Row = LiveQueryRow<TopicRow<Topics, typeof topic>, typeof query>;
    const queryKey = stableQueryKey(query);
    const liveAtom = useMemo(
      () =>
        Atom.make(
          Stream.scoped(
            Stream.unwrap(
              Effect.gen(function* () {
                const subscription = yield* providerState.engine.subscribe(
                  topic,
                  query as ExactRawQuery<TopicRow<Topics, typeof topic>, typeof query>,
                );
                yield* refreshHealth(providerState.engine, providerState.health);
                return subscription.events.pipe(
                  Stream.scan(initialClientState<Row>(), applyEvent),
                  Stream.ensuring(
                    subscription
                      .close()
                      .pipe(
                        Effect.andThen(refreshHealth(providerState.engine, providerState.health)),
                      ),
                  ),
                );
              }),
            ),
          ),
        ),
      [providerState, topic, queryKey],
    );
    const result = AtomReact.useAtomValue(liveAtom);
    return liveQueryResult(AsyncResult.getOrElse(result, initialClientState<Row>));
  };

  const useViewServerHealth = (): ViewServerHealth<Topics> => {
    const providerState = useProviderState();
    return AtomReact.useAtomRef(providerState.health);
  };

  const useViewServerTestRuntime = (): ViewServerInMemoryRuntime<Topics> =>
    useProviderState().runtime;

  return {
    ViewServerInMemoryProvider,
    useLiveQuery,
    useViewServerHealth,
    useViewServerTestRuntime,
  };
};
