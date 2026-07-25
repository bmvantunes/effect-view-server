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
} from "@effect-view-server/config";
import { Effect, Result, Stream } from "effect";
import * as Atom from "effect/unstable/reactivity/Atom";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import {
  createContext,
  useCallback,
  useContext,
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
  liveGridQueryIdentityKey,
  decideLiveGridActivation,
  projectLiveGridSinkIfPresent,
  liveGridOwnedQueryOrFallback,
  resolveLiveGridOwnedQuery,
  type LiveGridDatasourceForTopic,
  type LiveGridDatasourceParams,
  type LiveGridOnChange,
  type UseLiveGridResultForTopic,
} from "./live-grid";

export type {
  ExactLiveGridOnChangeInputForTopic,
  LiveGridDatasource,
  LiveGridDatasourceForTopic,
  LiveGridDatasourceParams,
  LiveGridGroupedOnChange,
  LiveGridOnChange,
  LiveGridQueryCandidate,
  LiveGridRawOnChange,
  UseLiveGridResult,
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
};

const idleLiveGridSubscription = <Row,>(): Effect.Effect<ViewServerLiveSubscription<Row>> =>
  Effect.succeed({
    events: Stream.never,
    close: () => Effect.void,
  });

/**
 * Live grid stores a runtime Live Query after onChange. ExactLiveQueryInputForTopic cannot be
 * re-proven without const inference at the original call site, so this is the smallest subscribe
 * seam. onChange inputs remain typed; runtime engine validation still applies.
 */
const subscribeLiveGridQuery = <
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Row extends TopicRow<Topics, Topic>,
>(
  client: ViewServerLiveClient<Topics>,
  topic: Topic,
  query: LiveQuery<Row>,
): Effect.Effect<ViewServerLiveSubscription<object>> => {
  // Projected/grouped result rows are not full TopicRow values; the sink types rows as object.
  const subscribe = client.subscribe as (
    topicName: Topic,
    liveQuery: LiveQuery<Row>,
  ) => Effect.Effect<ViewServerLiveSubscription<object>>;
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
    });
    const [active, setActive] = useState<LiveGridActiveQuery<Row> | null>(null);
    const activeRef = useRef<LiveGridActiveQuery<Row> | null>(null);
    const [windowError, setWindowError] = useState<string | null>(null);

    const clearLiveGridSink = useCallback(() => {
      const sink = controllerRef.current.sink;
      if (sink === null) {
        return;
      }
      sink.setRowCount(0, true);
      sink.setRowData({});
    }, []);

    const applyChange = useCallback(
      (state: LiveGridOnChange<Row>) => {
        const mapped = liveGridOnChangeToQuery(state);
        if (mapped._tag === "InvalidWindow") {
          setWindowError(mapped.message);
          activeRef.current = null;
          setActive(null);
          controllerRef.current.session += 1;
          clearLiveGridSink();
          return;
        }
        // Match useLiveQuery: prefer a frozen snapshot, fall back to the mapped query on failure.
        const ownedQuery = liveGridOwnedQueryOrFallback(
          resolveLiveGridOwnedQuery(mapped.query, (query) => snapshotViewServerQuery(query)),
          mapped.query,
        );
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
        // Deduplicate identical full-state onChange (same query identity + firstRow).
        if (decision._tag === "Unchanged") {
          return;
        }
        setWindowError(null);
        controllerRef.current.session += 1;
        const nextActive = {
          key,
          query: ownedQuery,
          firstRow: mapped.firstRow,
        };
        activeRef.current = nextActive;
        setActive(nextActive);
      },
      [clearLiveGridSink, topicDefinition],
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
          const change = liveGridOnChangeFromExact(state);
          if (controller.sink === null) {
            const mapped = liveGridOnChangeToQuery(change);
            if (mapped._tag === "InvalidWindow") {
              applyChange(change);
              return;
            }
            controller.pending = change;
            return;
          }
          applyChange(change);
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
    }, [applyChange]);

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
              activeSession: controllerRef.current.session,
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
