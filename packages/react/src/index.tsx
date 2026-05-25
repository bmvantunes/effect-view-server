import * as AtomReact from "@effect/atom-react";
import type { DecodableTopicDefinitions } from "@view-server/column-live-view-engine";
import type {
  ExactRawQuery,
  LiveQueryResult,
  LiveQueryRow,
  TopicRow,
  ValidateLiveQuery,
  ViewServerConfig,
  ViewServerHealth,
  ViewServerInMemoryRuntime,
} from "@view-server/config";
import { Cause, Effect, Stream } from "effect";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import * as Atom from "effect/unstable/reactivity/Atom";
import { createElement, useMemo, type ReactNode } from "react";
import { makeProviderState } from "./in-memory-runtime";
import { applyEvent, initialClientState, liveQueryResult } from "./live-query-state";
import type { ViewServerReactClient } from "./react-client";
import { stableQueryKey } from "./query-key";

type ReactBindings<Topics extends DecodableTopicDefinitions> = {
  readonly useLiveQuery: UseLiveQueryHook<Topics>;
  readonly useViewServerHealth: () => ViewServerHealth<Topics>;
  readonly createInMemoryViewServer: (
    options?: ViewServerInMemoryOptions,
  ) => ViewServerInMemoryInstance<Topics>;
};

export type ViewServerInMemoryProviderProps = {
  readonly children?: ReactNode;
};

export type ViewServerInMemoryOptions = {
  readonly subscriptionQueueCapacity?: number;
};

export type ViewServerInMemoryInstance<Topics extends DecodableTopicDefinitions> = {
  readonly ViewServerInMemoryProvider: (props: ViewServerInMemoryProviderProps) => ReactNode;
  readonly client: ViewServerInMemoryRuntime<Topics>;
};

export type UseLiveQueryHook<Topics extends DecodableTopicDefinitions> = <
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

export const createViewServerReact = <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
): ReactBindings<Topics> => {
  const ProviderAtom = AtomReact.make((client: ViewServerReactClient<Topics>) =>
    Atom.make((get) => {
      get.addFinalizer(() => {
        Effect.runFork(client.close);
      });
      return client;
    }),
  );

  const useClient = (): ViewServerReactClient<Topics> => AtomReact.useAtomValue(ProviderAtom.use());

  const createInMemoryViewServer = (
    options: ViewServerInMemoryOptions = {},
  ): ViewServerInMemoryInstance<Topics> => {
    const providerState = Effect.runSync(makeProviderState(config, options));

    function ViewServerInMemoryProvider(props: ViewServerInMemoryProviderProps): ReactNode {
      return createElement(
        AtomReact.RegistryProvider,
        { defaultIdleTTL: 0 },
        createElement(ProviderAtom.Provider, { value: providerState.reactClient }, props.children),
      );
    }

    return {
      ViewServerInMemoryProvider,
      client: providerState.runtime,
    };
  };

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
    const emptyState = () => initialClientState<Row>();
    if (AsyncResult.isFailure(result)) {
      const defect = Cause.squash(result.cause);
      return {
        ...liveQueryResult(emptyState()),
        status: "error",
        statusCode: "TransportError",
        message: String(defect),
      };
    }
    return liveQueryResult(AsyncResult.getOrElse(result, emptyState));
  };

  const useViewServerHealth = (): ViewServerHealth<Topics> => {
    const client = useClient();
    return AtomReact.useAtomRef(client.health);
  };

  return {
    useLiveQuery,
    useViewServerHealth,
    createInMemoryViewServer,
  };
};
