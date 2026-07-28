import {
  type ViewServerInMemoryTopicDefinitions,
  type ViewServerInMemoryOptions,
  type ViewServerSourceRequirements,
} from "@effect-view-server/in-memory";
import {
  createInMemoryViewServerTesting,
  makeInMemoryViewServerTesting,
  type ViewServerInMemoryTestingInstance,
} from "@effect-view-server/in-memory/testing";
import type { ViewServerRuntimeError } from "@effect-view-server/config";
import * as AtomReact from "@effect/atom-react";
import { Effect } from "effect";
import * as Atom from "effect/unstable/reactivity/Atom";
import type { ReactNode } from "react";
import type { ViewServerReactBindings } from "./index";
import { ViewServerReactClientProvider, ViewServerReactConfig } from "./internal";

export type { ViewServerInMemoryOptions } from "@effect-view-server/in-memory";

export type ViewServerInMemoryProviderProps = {
  readonly children?: ReactNode;
};

type ViewServerInMemoryReactTopicDefinitions = ViewServerInMemoryTopicDefinitions;

type SynchronousInMemoryReactBindings<Topics extends ViewServerInMemoryReactTopicDefinitions> =
  ViewServerReactBindings<Topics> &
    ([ViewServerSourceRequirements<NoInfer<Topics>>] extends [never] ? unknown : never);

export type ViewServerInMemoryReactInstance<
  Topics extends ViewServerInMemoryReactTopicDefinitions,
> = {
  readonly ViewServerInMemoryProvider: (props: ViewServerInMemoryProviderProps) => ReactNode;
  readonly client: ViewServerInMemoryTestingInstance<Topics>["client"];
  readonly close: Effect.Effect<void>;
};

const InMemoryLifetimeAtom = AtomReact.make((close: Effect.Effect<void>) =>
  Atom.make((get) => {
    get.addFinalizer(() => {
      Effect.runFork(close);
    });
    return null;
  }),
);

const toInMemoryViewServerReactInstance = <
  const Topics extends ViewServerInMemoryReactTopicDefinitions,
>(
  react: ViewServerReactBindings<Topics>,
  inMemory: ViewServerInMemoryTestingInstance<Topics>,
): ViewServerInMemoryReactInstance<Topics> => {
  const ViewServerClientProvider = react[ViewServerReactClientProvider];

  function InMemoryLifetimeMount(): null {
    AtomReact.useAtomMount(InMemoryLifetimeAtom.use());
    return null;
  }

  function ViewServerInMemoryProvider(props: ViewServerInMemoryProviderProps): ReactNode {
    return (
      <InMemoryLifetimeAtom.Provider value={inMemory.close}>
        <ViewServerClientProvider client={inMemory.liveClient}>
          <InMemoryLifetimeMount />
          {props.children}
        </ViewServerClientProvider>
      </InMemoryLifetimeAtom.Provider>
    );
  }

  return {
    ViewServerInMemoryProvider,
    client: inMemory.client,
    close: inMemory.close,
  };
};

export const makeInMemoryViewServerReact: <
  const Topics extends ViewServerInMemoryReactTopicDefinitions,
>(
  react: ViewServerReactBindings<Topics>,
  options?: ViewServerInMemoryOptions<Topics>,
) => Effect.Effect<
  ViewServerInMemoryReactInstance<Topics>,
  ViewServerRuntimeError,
  ViewServerSourceRequirements<Topics>
> = Effect.fn("ViewServerReact.testing.make")(function* <
  const Topics extends ViewServerInMemoryReactTopicDefinitions,
>(react: ViewServerReactBindings<Topics>, options: ViewServerInMemoryOptions<Topics> = {}) {
  const inMemory = yield* makeInMemoryViewServerTesting(react[ViewServerReactConfig], options);
  return toInMemoryViewServerReactInstance(react, inMemory);
});

export const createInMemoryViewServerReact = <
  const Topics extends ViewServerInMemoryReactTopicDefinitions,
>(
  react: SynchronousInMemoryReactBindings<Topics>,
  options: ViewServerInMemoryOptions<Topics> = {},
): ViewServerInMemoryReactInstance<Topics> =>
  toInMemoryViewServerReactInstance(
    react,
    createInMemoryViewServerTesting(react[ViewServerReactConfig], options),
  );
