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
  type ViewServerLiveClient,
  type ViewServerLiveSubscription,
  type ViewServerSourceHealthInputForTopic,
  type ViewServerSourceHealthResultForTopic,
  type ViewServerSourceHealthSubscription,
  type ViewServerSourceOwnedTopic,
} from "@effect-view-server/client";
import {
  makeViewServerClient,
  type ViewServerClientOptions,
} from "@effect-view-server/client/remote";
import type {
  ExactLiveQueryInputForTopic,
  GroupedQuery,
  LiveQueryResult,
  LiveQueryRow,
  RawQuery,
  TopicDefinitions,
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
import { Cause, Effect, Result, Stream } from "effect";
import * as Atom from "effect/unstable/reactivity/Atom";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import { createContext, useContext, useInsertionEffect, useMemo, type ReactNode } from "react";
import { ViewServerReactClientProvider, ViewServerReactConfig } from "./internal";
import {
  makeLiveQueryViewport,
  makeLiveQueryViewportAtom,
  makeLiveQueryViewportBinding,
  type UseLiveQueryViewportHook,
  type UseLiveQueryViewportResult,
} from "./live-query-viewport";

export type {
  LiveQueryViewport,
  LiveQueryViewportGeneration,
  LiveQueryViewportGroupedQuery,
  LiveQueryViewportQuery,
  LiveQueryViewportRawQuery,
  LiveQueryViewportSink,
  LiveQueryViewportWindow,
  UseLiveQueryViewportHook,
  UseLiveQueryViewportResult,
} from "./live-query-viewport";

export type ViewServerReactBindings<Topics extends TopicDefinitions> = {
  readonly [ViewServerReactConfig]: ViewServerConfig<Topics>;
  readonly [ViewServerReactClientProvider]: (
    props: ViewServerClientProviderProps<Topics>,
  ) => ReactNode;
  readonly useLiveQuery: UseLiveQueryHook<Topics>;
  readonly useLiveQueryViewport: UseLiveQueryViewportHook<Topics>;
  readonly useSourceHealth: UseSourceHealthHook<Topics>;
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

export type UseSourceHealthResult<Health> = AsyncResult.AsyncResult<
  Health,
  ViewServerRuntimeError | ViewServerTransportError | Cause.NoSuchElementError
>;

export type UseSourceHealthHook<Topics extends TopicDefinitions> = <
  const Input extends {
    readonly topic: ViewServerSourceOwnedTopic<Topics>;
  },
>(
  input: ViewServerSourceHealthInputForTopic<Topics, Input["topic"], Input>,
) => UseSourceHealthResult<ViewServerSourceHealthResultForTopic<Topics, Input["topic"]>>;

export const createViewServerReact = <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
): ViewServerReactBindings<Topics> => {
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
  type SourceHealthResult = {
    readonly [Topic in ViewServerSourceOwnedTopic<Topics>]: ViewServerSourceHealthResultForTopic<
      Topics,
      Topic
    >;
  }[ViewServerSourceOwnedTopic<Topics>];
  type SourceHealthAtomEntry<Health extends SourceHealthResult = SourceHealthResult> = {
    readonly atom: Atom.Atom<UseSourceHealthResult<Health>>;
  };
  const sourceHealthEntries = new WeakMap<
    object,
    WeakMap<ViewServerLiveClient<Topics>, Map<string, SourceHealthAtomEntry>>
  >();

  function sourceHealthEntry<Health extends SourceHealthResult>(
    registry: object,
    client: ViewServerLiveClient<Topics>,
    key: string,
    subscribe: () => Effect.Effect<
      ViewServerSourceHealthSubscription<Health>,
      ViewServerRuntimeError | ViewServerTransportError
    >,
  ): SourceHealthAtomEntry<Health>;
  function sourceHealthEntry(
    registry: object,
    client: ViewServerLiveClient<Topics>,
    key: string,
    subscribe: () => Effect.Effect<
      ViewServerSourceHealthSubscription<SourceHealthResult>,
      ViewServerRuntimeError | ViewServerTransportError
    >,
  ): SourceHealthAtomEntry {
    let clientEntries = sourceHealthEntries.get(registry);
    if (clientEntries === undefined) {
      clientEntries = new WeakMap();
      sourceHealthEntries.set(registry, clientEntries);
    }
    let entries = clientEntries.get(client);
    if (entries === undefined) {
      entries = new Map();
      clientEntries.set(client, entries);
    }
    const existing = entries.get(key);
    if (existing !== undefined) {
      return existing;
    }
    const evict = () => {
      entries.delete(key);
    };
    const entry = {
      atom: Atom.make((get) => {
        get.addFinalizer(evict);
        return Stream.scoped(
          Stream.unwrap(
            Effect.gen(function* () {
              const subscription = yield* subscribe();
              return subscription.events.pipe(
                Stream.ensuring(subscription.close().pipe(ignoreSubscriptionCloseFailure)),
              );
            }),
          ),
        );
      }),
    };
    entries.set(key, entry);
    return entry;
  }

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
    subscriptionOwner: object,
    subscribe: () => Effect.Effect<ViewServerLiveSubscription<Row>, unknown>,
  ): LiveQueryResult<Row> => {
    const liveAtom = useMemo(
      () =>
        Atom.make(
          Stream.scoped(
            Stream.unwrap(
              Effect.gen(function* () {
                const subscription = yield* subscribe();
                return subscription.events.pipe(
                  Stream.scan(initialClientState<Row>(), applyEvent),
                  Stream.ensuring(subscription.close().pipe(ignoreSubscriptionCloseFailure)),
                );
              }),
            ),
          ),
        ),
      [subscriptionKey, subscriptionOwner],
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
      client,
      () => client.subscribe<Topic, Query>(topic, queryIdentity.query),
    );
  }

  function useSourceHealth<
    const Input extends {
      readonly topic: ViewServerSourceOwnedTopic<Topics>;
    },
  >(
    input: ViewServerSourceHealthInputForTopic<Topics, Input["topic"], Input>,
  ): UseSourceHealthResult<ViewServerSourceHealthResultForTopic<Topics, Input["topic"]>> {
    const client = useClient();
    const registry = useContext(AtomReact.RegistryContext);
    const inputIdentity = useMemo(() => {
      const capturedInput = snapshotViewServerQuery(input);
      const key = stableQueryKey(capturedInput);
      return { input: capturedInput, key };
    }, [input]);
    const entry = sourceHealthEntry(
      registry,
      client,
      `${inputIdentity.input.topic}:${inputIdentity.key}`,
      () => client.subscribeSourceHealth(inputIdentity.input),
    );
    return AtomReact.useAtomValue(entry.atom);
  }

  function useLiveQueryViewport<Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
  ): UseLiveQueryViewportResult<Topics, Topic> {
    const client = useClient();
    // Topic identity owns the public facade. Client changes replace the installed
    // controller below without invalidating viewport references held by the grid.
    const binding = useMemo(() => makeLiveQueryViewportBinding<Topics, Topic>(), [topic]);
    const viewportState = useMemo(() => makeLiveQueryViewportAtom(), [client, topic]);
    const [result, publish] = AtomReact.useAtom(viewportState.atom);
    const entry = useMemo(() => {
      const viewport = makeLiveQueryViewport({
        client,
        config,
        topic,
        publish: (command) => {
          publish(viewportState.prepare(command));
        },
      });
      return { viewport, deactivate: viewport.deactivate };
    }, [client, publish, topic, viewportState]);
    useInsertionEffect(() => {
      binding.install(entry);
      return () => {
        binding.uninstall(entry);
      };
    }, [binding, entry]);
    const chrome = viewportState.read(result);
    return {
      viewport: binding.viewport,
      totalRows: chrome.totalRows,
      version: chrome.version,
      status: chrome.status,
      statusCode: chrome.statusCode,
      message: chrome.message,
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
  });

  const useViewServerHealthSummary = (): ViewServerHealthSummary<Topics> => {
    const client = useClient();
    const result = useSubscription<ViewServerHealthSummaryRow<Topics>>(
      `${client.health.key}:health-summary`,
      client,
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
      client,
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
    useLiveQueryViewport,
    useSourceHealth,
    useViewServerHealth,
    useViewServerHealthSummary,
    ViewServerProvider,
  };
};
