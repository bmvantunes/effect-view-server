import * as AtomReact from "@effect/atom-react";
import {
  ignoreLoggedTypedFailuresPreserveNonTypedFailures,
  snapshotViewServerQuery,
} from "@effect-view-server/effect-utils";
import {
  applyEvent,
  initialClientState,
  liveQueryResultFromAsyncResult,
  stableQueryKey,
  stableQueryKeyForRowSchema,
  type ClientState,
  type ViewServerLiveClient,
  type ViewServerLiveSubscription,
} from "@effect-view-server/client";
import {
  makeViewServerClient,
  type ViewServerClientOptions,
} from "@effect-view-server/client/remote";
import type {
  ExactLiveQueryInputForTopic,
  GrpcRuntimeClients,
  GroupedQuery,
  LiveQuery,
  LiveQueryResult,
  LiveQueryRow,
  RawQuery,
  RuntimeRegions,
  TopicDefinitions,
  TopicRouteBy,
  TopicRow,
  ViewServerConfig,
  ViewServerHealthConnectionStatus,
  ViewServerHealthDetails,
  ViewServerHealthSummary,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@effect-view-server/config";
import { Effect, Result, Stream } from "effect";
import * as Atom from "effect/unstable/reactivity/Atom";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import {
  createContext,
  useCallback,
  useContext,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { ViewServerReactClientProvider, ViewServerReactConfig } from "./internal";
import {
  liveGridChromeFromResult,
  liveGridIdleChrome,
  liveGridInvalidWindowChrome,
  liveGridOnChangeFromExact,
  liveGridOnChangeToQuery,
  liveGridOnScrollRequiresActiveQueryMessage,
  liveGridQueryIdentityKey,
  liveGridScrollQuery,
  decideLiveGridActivation,
  projectLiveGridSinkIfPresent,
  ownLiveGridOnChangeForPending,
  resolveLiveGridOwnedQuery,
  validateLiveGridWindow,
  type LiveGridDatasourceForTopic,
  type LiveGridDatasourceParams,
  type LiveGridOnChange,
  type UseLiveGridResultForTopic,
} from "./live-grid";

export type {
  ExactLiveGridOnChangeInputForTopic,
  LiveGridChrome,
  LiveGridDatasourceForTopic,
  LiveGridDatasourceParams,
  LiveGridGroupedOnChange,
  LiveGridOnChange,
  LiveGridOnChangeStateCandidate,
  LiveGridRawOnChange,
  UseLiveGridResultForTopic,
} from "./live-grid";

export type ViewServerReactBindings<
  Topics extends TopicDefinitions,
  Regions extends RuntimeRegions = RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
> = {
  readonly [ViewServerReactConfig]: ViewServerConfig<Topics, Regions, GrpcClients>;
  readonly [ViewServerReactClientProvider]: (
    props: ViewServerClientProviderProps<Topics>,
  ) => ReactNode;
  readonly useLiveQuery: UseLiveQueryHook<Topics>;
  readonly useLiveGrid: UseLiveGridHook<Topics>;
  readonly useViewServerHealth: () => ViewServerHealthDetails<Extract<keyof Topics, string>>;
  readonly useViewServerHealthSummary: () => ViewServerHealthSummary<Topics>;
  readonly ViewServerProvider: (props: ViewServerProviderProps) => ReactNode;
};

type ViewServerClientProviderProps<Topics extends TopicDefinitions> = {
  readonly client: ViewServerLiveClient<Topics>;
  readonly children?: ReactNode;
};

export type ViewServerProviderProps = ViewServerClientOptions & {
  readonly children?: ReactNode;
};

const ignoreSubscriptionCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring React subscription close failure.",
);

export type UseLiveQueryHook<Topics extends TopicDefinitions> = <
  Topic extends Extract<keyof Topics, string>,
  const Query extends
    | RawQuery<TopicRow<Topics, NoInfer<Topic>>>
    | GroupedQuery<TopicRow<Topics, NoInfer<Topic>>>,
>(
  topic: Topic,
  query: ExactLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
) => LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>;

/** Live grid v1 targets non-leased topics; leased routes need a follow-up routeBy API. */
type LiveGridTopicName<Topics extends TopicDefinitions> = {
  [Topic in Extract<keyof Topics, string>]: [TopicRouteBy<Topics, Topic>] extends [never]
    ? Topic
    : never;
}[Extract<keyof Topics, string>];

export type UseLiveGridHook<Topics extends TopicDefinitions> = <
  Topic extends LiveGridTopicName<Topics>,
>(
  topic: Topic,
) => UseLiveGridResultForTopic<Topics, Topic>;

type LiveGridActiveQuery<Row> = {
  readonly key: string;
  readonly query: LiveQuery<Row>;
  readonly firstRow: number;
};

type LiveGridControllerState<Row> = {
  sink: LiveGridDatasourceParams | null;
  pending: LiveGridOnChange<Row> | null;
  session: number;
  /** True while clearLiveGridSink is invoking the external sink (re-entrancy guard). */
  clearing: boolean;
};

const idleLiveGridSubscription = <Row,>(): Effect.Effect<ViewServerLiveSubscription<Row>> =>
  Effect.succeed({
    events: Stream.never,
    close: () => Effect.void,
  });

/**
 * APPROVED SEAM (PR #395 / useLiveGrid): session-owned LiveQuery cannot re-prove
 * ExactLiveQueryInputForTopic at subscribe time (const inference only exists at onChange).
 * Projected/grouped result rows are intentionally erased to `object` at the sink.
 * This is not `as any` / `as unknown` / `as never` — only the public Exact-at-call-site
 * subscribe signature is adapted to the session-owned LiveQuery + object-row path.
 * onChange remains exact-typed; runtime engine validation still applies.
 */
const subscribeLiveGridQuery = <
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Row extends TopicRow<Topics, Topic>,
>(
  client: ViewServerLiveClient<Topics>,
  topic: Topic,
  query: LiveQuery<Row>,
): Effect.Effect<
  ViewServerLiveSubscription<object>,
  ViewServerRuntimeError | ViewServerTransportError
> => {
  type SessionOwnedSubscribe = (
    topicName: Topic,
    liveQuery: LiveQuery<Row>,
  ) => Effect.Effect<
    ViewServerLiveSubscription<object>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  const subscribe = client.subscribe as SessionOwnedSubscribe;
  return subscribe(topic, query);
};

export const createViewServerReact = <
  const Topics extends TopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
): ViewServerReactBindings<Topics, Regions, GrpcClients> => {
  const ClientContext = createContext<ViewServerLiveClient<Topics> | null>(null);
  const RemoteClientAtom = AtomReact.make((options: ViewServerClientOptions) =>
    Atom.make((get) =>
      Effect.gen(function* () {
        const services = yield* Effect.context();
        const client = yield* makeViewServerClient(config, options);
        get.addFinalizer(() => {
          Effect.runForkWith(services)(client.close);
        });
        return client;
      }),
    ),
  );

  const useClient = (): ViewServerLiveClient<Topics> => {
    const client = useContext(ClientContext);
    if (client === null) {
      throw new Error("ViewServerProvider is missing a client.");
    }
    return client;
  };

  function ViewServerClientProvider(props: ViewServerClientProviderProps<Topics>): ReactNode {
    return (
      <AtomReact.RegistryProvider>
        <ClientContext.Provider value={props.client}>{props.children}</ClientContext.Provider>
      </AtomReact.RegistryProvider>
    );
  }

  function RemoteClientBoundary(props: { readonly children?: ReactNode }): ReactNode {
    const result = AtomReact.useAtomValue(RemoteClientAtom.use());
    if (AsyncResult.isSuccess(result)) {
      return <ClientContext.Provider value={result.value}>{props.children}</ClientContext.Provider>;
    }
    if (AsyncResult.isFailure(result)) {
      throw new Error(String(result.cause));
    }
    return null;
  }

  function ViewServerProvider(props: ViewServerProviderProps): ReactNode {
    const options = {
      url: props.url,
      ...(props.subscriptionBufferSize === undefined
        ? {}
        : { subscriptionBufferSize: props.subscriptionBufferSize }),
    } satisfies ViewServerClientOptions;
    const providerKey = [props.url, String(props.subscriptionBufferSize ?? "")].join(":");
    return (
      <AtomReact.RegistryProvider>
        <RemoteClientAtom.Provider key={providerKey} value={options}>
          <RemoteClientBoundary>{props.children}</RemoteClientBoundary>
        </RemoteClientAtom.Provider>
      </AtomReact.RegistryProvider>
    );
  }

  const useSubscription = <Row,>(
    subscriptionKey: string,
    subscribe: () => Effect.Effect<ViewServerLiveSubscription<Row>, unknown>,
    onState?: (state: ClientState<Row>) => void,
  ): LiveQueryResult<Row> => {
    const liveAtom = useMemo(
      () =>
        Atom.make(
          Stream.scoped(
            Stream.unwrap(
              Effect.gen(function* () {
                const subscription = yield* subscribe();
                const scanned = subscription.events.pipe(
                  Stream.scan(initialClientState<Row>(), applyEvent),
                );
                const projected =
                  onState === undefined
                    ? scanned
                    : scanned.pipe(
                        Stream.tap((state) =>
                          Effect.sync(() => {
                            onState(state);
                          }),
                        ),
                      );
                return projected.pipe(
                  Stream.ensuring(subscription.close().pipe(ignoreSubscriptionCloseFailure)),
                );
              }),
            ),
          ),
        ),
      [subscriptionKey],
    );
    const result = AtomReact.useAtomValue(liveAtom);
    return liveQueryResultFromAsyncResult<Row>(result);
  };

  function useLiveQuery<
    Topic extends Extract<keyof Topics, string>,
    const Query extends
      | RawQuery<TopicRow<Topics, NoInfer<Topic>>>
      | GroupedQuery<TopicRow<Topics, NoInfer<Topic>>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
  ): LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>> {
    const client = useClient();
    const topicDefinition = config.topics[topic];
    const queryIdentity = useMemo(() => {
      // A query reference owns one hook snapshot; changing the query requires a new reference.
      const capturedQuery = Result.try(() => snapshotViewServerQuery(query));
      const ownedQuery = Result.isSuccess(capturedQuery) ? capturedQuery.success : query;
      const key =
        topicDefinition === undefined
          ? stableQueryKey(ownedQuery)
          : stableQueryKeyForRowSchema(ownedQuery, topicDefinition.schema);
      return { key, query: ownedQuery };
    }, [query, topicDefinition]);
    return useSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>(
      `${client.health.key}:query:${topic}:${queryIdentity.key}`,
      () => client.subscribe<Topic, Query>(topic, queryIdentity.query),
    );
  }

  function useLiveGrid<Topic extends LiveGridTopicName<Topics>>(
    topic: Topic,
  ): UseLiveGridResultForTopic<Topics, Topic> {
    type Row = TopicRow<Topics, Topic>;
    const client = useClient();
    const topicDefinition = config.topics[topic];
    const controllerRef = useRef<LiveGridControllerState<Row>>({
      sink: null,
      pending: null,
      session: 0,
      clearing: false,
    });
    const [active, setActive] = useState<LiveGridActiveQuery<Row> | null>(null);
    const activeRef = useRef<LiveGridActiveQuery<Row> | null>(null);
    const [windowError, setWindowError] = useState<string | null>(null);

    const clearLiveGridSink = useCallback(() => {
      const controller = controllerRef.current;
      const sink = controller.sink;
      // Nested clear (setRowCount → onChange → activate → clear) must not recurse.
      if (sink === null || controller.clearing) {
        return;
      }
      controller.clearing = true;
      try {
        sink.setRowCount(0, true);
        // setRowCount may re-enter destroy and null the sink; never write setRowData on a dead sink.
        if (controller.sink !== sink) {
          return;
        }
        sink.setRowData({});
      } finally {
        controller.clearing = false;
      }
    }, []);

    // Topic identity change (rare; grids usually remount): commit-phase reset so we can clear
    // the external sink and so concurrent-render interruption cannot corrupt the active session.
    const boundTopicRef = useRef(topic);
    useLayoutEffect(() => {
      if (boundTopicRef.current === topic) {
        return;
      }
      boundTopicRef.current = topic;
      // Clear first: a one-shot setRowCount re-entry into onChange is suppressed while clearing.
      // Finalize session/ref reset afterward so a nested activation cannot stick as activeRef.
      clearLiveGridSink();
      controllerRef.current.session += 1;
      controllerRef.current.pending = null;
      activeRef.current = null;
      setActive(null);
      setWindowError(null);
    }, [topic, clearLiveGridSink]);

    const applyInvalidWindow = useCallback(
      (message: string) => {
        const invalidSession = controllerRef.current.session + 1;
        controllerRef.current.session = invalidSession;
        clearLiveGridSink();
        if (controllerRef.current.session !== invalidSession) {
          return;
        }
        setWindowError(message);
        activeRef.current = null;
        setActive(null);
      },
      [clearLiveGridSink],
    );

    const activateMappedQuery = useCallback(
      (mapped: { readonly firstRow: number; readonly query: LiveQuery<Row> }) => {
        // Fail closed on snapshot failure (same as pre-init): never subscribe an unowned query.
        const owned = resolveLiveGridOwnedQuery(mapped.query, (query) =>
          snapshotViewServerQuery(query),
        );
        if (owned._tag === "SnapshotFailed") {
          applyInvalidWindow(owned.message);
          return;
        }
        const ownedQuery = owned.query;
        const key = liveGridQueryIdentityKey(
          ownedQuery,
          topicDefinition?.schema,
          stableQueryKey,
          stableQueryKeyForRowSchema,
        );
        const decision = decideLiveGridActivation({
          query: ownedQuery,
          current: activeRef.current,
          key,
          firstRow: mapped.firstRow,
        });
        // Deduplicate identical activation (same query identity + firstRow).
        if (decision._tag === "Unchanged") {
          return;
        }
        const activationSession = controllerRef.current.session + 1;
        controllerRef.current.session = activationSession;
        // Drop previous viewport rows before the replacement subscription delivers data.
        clearLiveGridSink();
        // setRowCount(0) may re-enter destroy/onChange and advance the session further.
        if (controllerRef.current.session !== activationSession) {
          return;
        }
        setWindowError(null);
        const nextActive = {
          key,
          query: ownedQuery,
          firstRow: mapped.firstRow,
        };
        activeRef.current = nextActive;
        setActive(nextActive);
      },
      [applyInvalidWindow, clearLiveGridSink, topicDefinition],
    );

    const applyChange = useCallback(
      (state: LiveGridOnChange<Row>) => {
        const mapped = liveGridOnChangeToQuery(state);
        if (mapped._tag === "InvalidWindow") {
          applyInvalidWindow(mapped.message);
          return;
        }
        activateMappedQuery(mapped);
      },
      [activateMappedQuery, applyInvalidWindow],
    );

    const applyScroll = useCallback(
      (firstRow: number, lastRow: number) => {
        const current = activeRef.current;
        if (current === null) {
          applyInvalidWindow(liveGridOnScrollRequiresActiveQueryMessage);
          return;
        }
        const mapped = liveGridScrollQuery(current.query, firstRow, lastRow);
        if (mapped._tag === "InvalidWindow") {
          applyInvalidWindow(mapped.message);
          return;
        }
        activateMappedQuery(mapped);
      },
      [activateMappedQuery, applyInvalidWindow],
    );

    const datasource = useMemo((): LiveGridDatasourceForTopic<Topics, Topic> => {
      const controller = controllerRef.current;
      return {
        init: (params) => {
          controller.sink = params;
          if (controller.pending !== null) {
            const pending = controller.pending;
            controller.pending = null;
            applyChange(pending);
          }
        },
        onChange: (state) => {
          // Exact input is State & refinements where State extends LiveGridOnChange at the
          // call site; liveGridOnChangeFromExact is the typed runtime boundary (no cast).
          // Re-entry from setRowCount during clear is allowed; nested clear is a no-op
          // (controller.clearing) so activation cannot recurse into stack overflow.
          const change = liveGridOnChangeFromExact(state);
          if (controller.sink === null) {
            const mapped = liveGridOnChangeToQuery(change);
            if (mapped._tag === "InvalidWindow") {
              // Full-state replacement: an invalid last change supersedes any buffered valid one.
              controller.pending = null;
              applyChange(change);
              return;
            }
            // Own the change at submission so later caller mutation cannot alter the buffer.
            const ownedPending = ownLiveGridOnChangeForPending(change, snapshotViewServerQuery);
            if (ownedPending._tag === "SnapshotFailed") {
              controller.pending = null;
              setWindowError(ownedPending.message);
              activeRef.current = null;
              setActive(null);
              controller.session += 1;
              return;
            }
            // Successful full-state buffer replaces any prior invalid-window chrome.
            setWindowError(null);
            controller.pending = ownedPending.state;
            return;
          }
          applyChange(change);
        },
        onScroll: (firstRow, lastRow) => {
          if (controller.sink === null) {
            if (controller.pending === null) {
              applyInvalidWindow(liveGridOnScrollRequiresActiveQueryMessage);
              return;
            }
            // Pre-init: re-window the buffered full-state onChange (no active query yet).
            const window = validateLiveGridWindow(firstRow, lastRow);
            if (window._tag === "Invalid") {
              controller.pending = null;
              applyInvalidWindow(window.message);
              return;
            }
            controller.pending = {
              ...controller.pending,
              firstRow: window.firstRow,
              lastRow: window.lastRow,
            };
            return;
          }
          applyScroll(firstRow, lastRow);
        },
        destroy: () => {
          controller.sink = null;
          controller.pending = null;
          controller.session += 1;
          setWindowError(null);
          activeRef.current = null;
          setActive(null);
        },
      };
    }, [applyChange, applyInvalidWindow, applyScroll]);

    const subscriptionSession = controllerRef.current.session;
    const subscriptionKey =
      active === null
        ? `${client.health.key}:grid:${topic}:idle:${subscriptionSession}`
        : `${client.health.key}:grid:${topic}:${active.key}:${subscriptionSession}`;

    const activeQuery = active?.query;
    const subscriptionFirstRow = active?.firstRow ?? 0;
    const result = useSubscription<object>(
      subscriptionKey,
      () => {
        if (activeQuery === undefined) {
          return idleLiveGridSubscription<object>();
        }
        return subscribeLiveGridQuery(client, topic, activeQuery);
      },
      activeQuery === undefined
        ? undefined
        : (state) => {
            projectLiveGridSinkIfPresent(controllerRef.current.sink, subscriptionFirstRow, state, {
              getActiveSession: () => controllerRef.current.session,
              subscriptionSession,
            });
          },
    );

    if (windowError !== null) {
      return {
        datasource,
        ...liveGridInvalidWindowChrome(windowError),
      };
    }
    if (active === null) {
      return {
        datasource,
        ...liveGridIdleChrome(),
      };
    }
    return {
      datasource,
      ...liveGridChromeFromResult(result),
    };
  }

  const connectionStatusFromLiveQueryStatus = (
    status: LiveQueryResult<unknown>["status"],
  ): ViewServerHealthConnectionStatus => {
    if (status === "loading") {
      return "connecting";
    }
    if (status === "ready" || status === "stale") {
      return "connected";
    }
    return "disconnected";
  };

  const emptySummary = (
    connectionStatus: ViewServerHealthConnectionStatus,
  ): ViewServerHealthSummary<Topics> => ({
    status: connectionStatus === "connected" ? "starting" : connectionStatus,
    runtimeStatus: "starting",
    connectionStatus,
    unhealthyTopics: [],
    updatedAtNanos: 0n,
    maxKafkaLag: null,
  });

  const summaryFromRow = (
    row: ViewServerHealthSummaryRow<Topics>,
    connectionStatus: ViewServerHealthConnectionStatus,
  ): ViewServerHealthSummary<Topics> => ({
    status: connectionStatus === "connected" ? row.runtimeStatus : connectionStatus,
    runtimeStatus: row.runtimeStatus,
    connectionStatus,
    unhealthyTopics: row.unhealthyTopics,
    updatedAtNanos: row.updatedAtNanos,
    maxKafkaLag: row.maxKafkaLag,
  });

  const useViewServerHealthSummary = (): ViewServerHealthSummary<Topics> => {
    const client = useClient();
    const result = useSubscription<ViewServerHealthSummaryRow<Topics>>(
      `${client.health.key}:health-summary`,
      client.subscribeHealthSummary,
    );
    const connectionStatus = connectionStatusFromLiveQueryStatus(result.status);
    const row = result.rows[0];
    return row === undefined
      ? emptySummary(connectionStatus)
      : summaryFromRow(row, connectionStatus);
  };

  const useViewServerHealth = (): ViewServerHealthDetails<Extract<keyof Topics, string>> => {
    const client = useClient();
    const summary = useViewServerHealthSummary();
    const result = useSubscription<ViewServerHealthTopicRow<Extract<keyof Topics, string>>>(
      `${client.health.key}:health`,
      client.subscribeHealth,
    );
    const detailConnectionStatus = connectionStatusFromLiveQueryStatus(result.status);
    const connectionStatus =
      summary.connectionStatus === "connected" ? detailConnectionStatus : summary.connectionStatus;
    const status = connectionStatus === "connected" ? summary.runtimeStatus : connectionStatus;
    const statusCode =
      status !== "ready" && result.statusCode === "Ready" ? undefined : result.statusCode;
    return {
      ...result,
      runtimeStatus: summary.runtimeStatus,
      status,
      statusCode,
      connectionStatus,
    };
  };

  return {
    [ViewServerReactConfig]: config,
    [ViewServerReactClientProvider]: ViewServerClientProvider,
    useLiveQuery,
    useLiveGrid,
    useViewServerHealth,
    useViewServerHealthSummary,
    ViewServerProvider,
  };
};
