import * as AtomReact from "@effect/atom-react";
import {
  applyEvent,
  initialClientState,
  liveQueryResultFromAsyncResult,
  stableQueryKey,
  type ViewServerLiveClient,
} from "@view-server/client";
import { makeViewServerClient, type ViewServerClientOptions } from "@view-server/client/remote";
import type {
  ExactRawQuery,
  LiveQueryResult,
  LiveQueryRow,
  TopicDefinitions,
  TopicRow,
  ValidateLiveQuery,
  ViewServerConfig,
  ViewServerHealth,
} from "@view-server/config";
import { Duration, Effect, Stream } from "effect";
import * as Atom from "effect/unstable/reactivity/Atom";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import { createContext, useContext, useMemo, type ReactNode } from "react";
import { ViewServerReactClientProvider, ViewServerReactConfig } from "./internal";

export type ViewServerReactBindings<Topics extends TopicDefinitions> = {
  readonly [ViewServerReactConfig]: ViewServerConfig<Topics>;
  readonly [ViewServerReactClientProvider]: (
    props: ViewServerClientProviderProps<Topics>,
  ) => ReactNode;
  readonly useLiveQuery: UseLiveQueryHook<Topics>;
  readonly useViewServerHealth: () => ViewServerHealth<Topics>;
  readonly ViewServerProvider: (props: ViewServerProviderProps) => ReactNode;
};

type ViewServerClientProviderProps<Topics extends TopicDefinitions> = {
  readonly client: ViewServerLiveClient<Topics>;
  readonly children?: ReactNode;
};

export type ViewServerProviderProps = ViewServerClientOptions & {
  readonly children?: ReactNode;
};

export type UseLiveQueryHook<Topics extends TopicDefinitions> = <
  Topic extends Extract<keyof Topics, string>,
  const Query extends { readonly select: ReadonlyArray<unknown> },
>(
  topic: Topic,
  query: Query & ExactRawQuery<TopicRow<Topics, Topic>, Query> & ValidateLiveQuery<Query>,
) => LiveQueryResult<
  LiveQueryRow<
    TopicRow<Topics, Topic>,
    Query & ExactRawQuery<TopicRow<Topics, Topic>, Query> & ValidateLiveQuery<Query>
  >
>;

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

  const providerKeyFromHealthPollInterval = (
    interval: ViewServerProviderProps["healthPollInterval"],
  ): string => {
    if (interval === undefined) {
      return "default";
    }
    if (interval === false) {
      return "false";
    }
    return Duration.fromInputUnsafe(interval).toString();
  };

  function ViewServerProvider(props: ViewServerProviderProps): ReactNode {
    const options = {
      url: props.url,
      ...(props.subscriptionBufferSize === undefined
        ? {}
        : { subscriptionBufferSize: props.subscriptionBufferSize }),
      ...(props.healthPollInterval === undefined
        ? {}
        : { healthPollInterval: props.healthPollInterval }),
    } satisfies ViewServerClientOptions;
    const providerKey = [
      props.url,
      String(props.subscriptionBufferSize ?? ""),
      providerKeyFromHealthPollInterval(props.healthPollInterval),
    ].join(":");
    return (
      <AtomReact.RegistryProvider>
        <RemoteClientAtom.Provider key={providerKey} value={options}>
          <RemoteClientBoundary>{props.children}</RemoteClientBoundary>
        </RemoteClientAtom.Provider>
      </AtomReact.RegistryProvider>
    );
  }

  const useLiveQuery: UseLiveQueryHook<Topics> = (topic, query) => {
    const client = useClient();
    type Row = LiveQueryRow<TopicRow<Topics, typeof topic>, typeof query>;
    const queryKey = stableQueryKey(query);
    const liveAtom = useMemo(
      () =>
        Atom.make(
          Stream.scoped(
            Stream.unwrap(
              Effect.gen(function* () {
                const subscription = yield* client.subscribe(topic, query);
                return subscription.events.pipe(
                  Stream.scan(initialClientState<Row>(), applyEvent),
                  Stream.ensuring(subscription.close().pipe(Effect.ignore)),
                );
              }),
            ),
          ),
        ),
      [client, topic, queryKey],
    );
    const result = AtomReact.useAtomValue(liveAtom);
    return liveQueryResultFromAsyncResult<Row>(result);
  };

  const useViewServerHealth = (): ViewServerHealth<Topics> => {
    const client = useClient();
    return AtomReact.useAtomRef(client.health);
  };

  return {
    [ViewServerReactConfig]: config,
    [ViewServerReactClientProvider]: ViewServerClientProvider,
    useLiveQuery,
    useViewServerHealth,
    ViewServerProvider,
  };
};
