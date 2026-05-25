import type { DecodableTopicDefinitions } from "@view-server/column-live-view-engine";
import type {
  DeltaEvent,
  ExactRawQuery,
  LiveQueryRow,
  SnapshotEvent,
  StatusEvent,
  TopicRow,
  ValidateLiveQuery,
  ViewServerHealth,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@view-server/config";
import type { Effect, Stream } from "effect";
import type * as AtomRef from "effect/unstable/reactivity/AtomRef";

export type ViewServerReactSubscription<Row> = {
  readonly events: Stream.Stream<SnapshotEvent<Row> | DeltaEvent<Row> | StatusEvent>;
  readonly close: () => Effect.Effect<void, ViewServerTransportError>;
};

export type ViewServerReactClient<Topics extends DecodableTopicDefinitions> = {
  readonly subscribe: <
    Topic extends Extract<keyof Topics, string>,
    const Query extends { readonly select: ReadonlyArray<unknown> },
  >(
    topic: Topic,
    query: Query & ExactRawQuery<TopicRow<Topics, Topic>, Query> & ValidateLiveQuery<Query>,
  ) => Effect.Effect<
    ViewServerReactSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  readonly health: AtomRef.AtomRef<ViewServerHealth<Topics>>;
  readonly close: Effect.Effect<void>;
};
