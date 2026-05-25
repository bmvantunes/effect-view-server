import {
  createColumnLiveViewEngine,
  InvalidQueryError,
  InvalidRowError,
  InvalidTopicError,
  UnsupportedQueryError,
  type ColumnLiveViewEngine,
  type ColumnLiveViewEngineError,
  type ColumnLiveViewEngineHealth,
  type DecodableTopicDefinitions,
} from "@view-server/column-live-view-engine";
import type {
  ExactPatch,
  ExactRawQuery,
  TopicRow,
  ViewServerConfig,
  ViewServerHealth,
  ViewServerInMemoryRuntime,
  ViewServerRuntimeError,
} from "@view-server/config";
import { Effect } from "effect";
import * as AtomRef from "effect/unstable/reactivity/AtomRef";

export type ProviderState<Topics extends DecodableTopicDefinitions> = {
  readonly engine: ColumnLiveViewEngine<Topics>;
  readonly runtime: ViewServerInMemoryRuntime<Topics>;
  readonly health: AtomRef.AtomRef<ViewServerHealth<Topics>>;
};

export type ProviderInput = {
  readonly subscriptionQueueCapacity?: number;
};

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

export const readHealth = <Topics extends DecodableTopicDefinitions>(
  engine: ColumnLiveViewEngine<Topics>,
  health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
) =>
  engine.health().pipe(
    Effect.map(healthFromEngine),
    Effect.tap((value) => Effect.sync(() => health.set(value))),
  );

export const refreshHealth = <Topics extends DecodableTopicDefinitions>(
  engine: ColumnLiveViewEngine<Topics>,
  health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
) => readHealth(engine, health).pipe(Effect.asVoid);

const makeRuntime = <Topics extends DecodableTopicDefinitions>(
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

export const makeProviderState = <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ProviderInput,
): ProviderState<Topics> => {
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
