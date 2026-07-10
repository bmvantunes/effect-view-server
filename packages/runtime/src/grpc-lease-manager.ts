import type {
  ExactLiveQueryInputForTopic,
  GrpcRuntimeClients,
  GroupedQuery,
  LiveQueryRow,
  RawQuery,
  RowSchema,
  StatusEvent,
  TopicRow,
  ViewServerTopicConfig,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@effect-view-server/config";
import { validateLiveQuerySourceRoute } from "@effect-view-server/config";
import {
  ignoreLoggedTypedFailuresPreserveNonTypedFailures,
  runAllFinalizers,
} from "@effect-view-server/effect-utils";
import type {
  ViewServerLiveEvent,
  ViewServerRuntimeLiveClient,
  ViewServerLiveSubscription,
} from "@effect-view-server/client";
import {
  Cause,
  Clock,
  Deferred,
  Effect,
  Exit,
  Option,
  Schema,
  Scope,
  Semaphore,
  Stream,
} from "effect";
import * as BigDecimal from "effect/BigDecimal";
import type { ViewServerGrpcHealthLedger } from "./grpc-health";
import {
  callLeasedGrpcSourceAcquire,
  callLeasedGrpcSourceRelease,
  callLeasedGrpcSourceRequest,
  makeGrpcSourceInput,
  makeDefaultGrpcClient,
  makeViewServerGrpcSourceError,
  ViewServerGrpcIngressError,
  type ViewServerGrpcClientFactory,
  type ViewServerGrpcRuntimeCallable,
  type ViewServerGrpcSourceInput,
} from "./grpc-source-lifecycle";
import type { ResolvedViewServerGrpcRuntimeOptions } from "./runtime-options";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";
import {
  makeSourceOwnershipPolicy,
  type ViewServerRuntimeCoreInternalClient,
  type ViewServerRuntimeCoreInternalLiveClient,
  type ViewServerRuntimeCoreTerminalObserver,
} from "@effect-view-server/runtime-core/internal";

type ViewServerGrpcHealthRefreshRequest = Effect.Effect<void>;

type RuntimeLeasedFeedDefinition = {
  readonly lifecycle: "leased";
  readonly topic: string;
  readonly client: string;
  readonly routeBy: ReadonlyArray<string>;
  readonly request: ViewServerGrpcRuntimeCallable;
  readonly acquire: ViewServerGrpcRuntimeCallable;
  readonly release?: ViewServerGrpcRuntimeCallable;
  readonly map: ViewServerGrpcRuntimeCallable;
};

const isRuntimeLeasedFeed = (feed: {
  readonly lifecycle: string;
  readonly routeBy?: ReadonlyArray<string>;
}): feed is RuntimeLeasedFeedDefinition =>
  feed.lifecycle === "leased" && feed.routeBy !== undefined;

type RuntimeTopicDefinition = {
  readonly schema: RowSchema & Schema.Codec<object, unknown, never, unknown>;
  readonly key: string;
};

type CanonicalRouteValue =
  | BigDecimal.BigDecimal
  | bigint
  | string
  | number
  | boolean
  | null
  | ReadonlyArray<CanonicalRouteValue>
  | { readonly [key: string]: CanonicalRouteValue };

type LeasedFeedRoute = Readonly<Record<string, CanonicalRouteValue>>;

type LeasedFeedRuntimeInput = ViewServerGrpcSourceInput<LeasedFeedRoute>;

type UpstreamLeaseTerminal = {
  readonly _tag: "Upstream";
  readonly message: string;
};

type EngineLeaseTerminal = {
  readonly _tag: "Engine";
  readonly ready: Deferred.Deferred<void>;
  readonly status: StatusEvent;
};

type RuntimeLeaseTerminal = {
  readonly _tag: "Runtime";
};

type ClosedLeaseTerminal = {
  readonly _tag: "Closed";
};

type LeaseTerminal =
  | UpstreamLeaseTerminal
  | EngineLeaseTerminal
  | RuntimeLeaseTerminal
  | ClosedLeaseTerminal;

type LeaseTerminalRegistration = {
  readonly observer: ViewServerRuntimeCoreTerminalObserver;
  readonly queryId: Deferred.Deferred<string>;
};

const closedLeaseTerminal: ClosedLeaseTerminal = {
  _tag: "Closed",
};

type LeaseRowOwner = {
  readonly feedName: string;
  readonly feed: RuntimeLeasedFeedDefinition;
  readonly internalToPublicKeys: Map<string, string>;
};

type ActiveLease = LeaseRowOwner & {
  readonly feedKey: string;
  readonly route: LeasedFeedRoute;
  readonly scope: Scope.Scope;
  readonly publicToInternalKeys: Map<string, string>;
  readonly cleanupRows: Effect.Effect<void, ViewServerGrpcIngressError, never>;
  readonly terminalSignals: Set<Deferred.Deferred<LeaseTerminal>>;
  readonly subscriptions: Set<ActiveLeaseSubscription>;
  subscribers: number;
  acceptingSubscribers: boolean;
};

type GroupedKeyTranslations = {
  readonly query: Pick<GroupedQuery<object>, "groupBy">;
  readonly byInternalKey: Map<string, string>;
};

type ActiveLeaseSubscription = {
  readonly close: () => Effect.Effect<void, ViewServerTransportError, never>;
};

type AcquiredLease = {
  readonly lease: ActiveLease;
  readonly terminalSignal: Deferred.Deferred<LeaseTerminal>;
};

export type ViewServerGrpcLeaseManager<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
  readonly close: Effect.Effect<void>;
};

/** @internal Package-local test probe; not exported from @effect-view-server/runtime. */
type ViewServerGrpcLeaseManagerTestProbe = {
  readonly onGroupedKeyTranslationsCreated: (translations: ReadonlyMap<string, string>) => void;
};

const grpcMessageBatchSize = 256;
const grpcMessageBatchFlushInterval = "2 millis";

const isRuntimeMutationEffect = (value: unknown): value is Effect.Effect<unknown, unknown, never> =>
  Effect.isEffect(value);

const ignoreGrpcFeedReleaseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring leased gRPC feed release failure.",
);
const ignoreGrpcHealthRefreshFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring leased gRPC health refresh failure.",
);
const ignoreLeasedSubscriptionCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring leased gRPC subscription close failure.",
);
const ignoreLeasedReleaseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring leased gRPC release failure.",
);

const runtimeError = (input: {
  readonly code: Extract<
    ViewServerRuntimeError,
    { readonly _tag: "ViewServerRuntimeError" }
  >["code"];
  readonly topic: string;
  readonly message: string;
}): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: input.code,
  topic: input.topic,
  message: input.message,
});

const grpcLeaseError = (input: {
  readonly message: string;
  readonly cause: unknown;
  readonly phase: NonNullable<ViewServerGrpcIngressError["phase"]>;
  readonly feedName: string;
  readonly topic: string;
}) =>
  makeViewServerGrpcSourceError({
    message: input.message,
    cause: input.cause,
    phase: input.phase,
    feedName: input.feedName,
    topic: input.topic,
  });

const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isPlainRouteRecord = (value: unknown): value is Readonly<Record<string, unknown>> => {
  if (!isRecord(value)) {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return (
    (prototype === null || prototype === Object.prototype) &&
    Object.getOwnPropertySymbols(value).length === 0 &&
    Object.getOwnPropertyNames(value).length === Object.keys(value).length
  );
};

const isCanonicalRouteValue = (value: unknown): value is CanonicalRouteValue => {
  if (BigDecimal.isBigDecimal(value)) {
    return true;
  }
  if (
    typeof value === "bigint" ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    value === null
  ) {
    return true;
  }
  if (typeof value === "number") {
    return Number.isFinite(value);
  }
  if (Array.isArray(value)) {
    return value.every(isCanonicalRouteValue);
  }
  if (isPlainRouteRecord(value)) {
    return Object.keys(value).every((key) => isCanonicalRouteValue(value[key]));
  }
  return false;
};

const exactEqValue = (value: unknown): Option.Option<unknown> => {
  if (!isRecord(value) || Object.keys(value).length !== 1 || !Object.hasOwn(value, "eq")) {
    return Option.none();
  }
  return Option.some(value["eq"]);
};

const extractRoute = Effect.fn("ViewServerRuntime.grpc.leased.route.extract")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Topic extends Extract<keyof Topics, string>,
>(
  config: ViewServerTopicConfig<Topics>,
  topic: Topic,
  feed: RuntimeLeasedFeedDefinition,
  query: unknown,
) {
  const routeError = validateLiveQuerySourceRoute(config.topics, topic, query);
  if (routeError !== undefined) {
    return yield* Effect.fail(
      runtimeError({
        code: "InvalidQuery",
        topic,
        message: routeError,
      }),
    );
  }
  if (!isRecord(query) || !isRecord(query["where"])) {
    return yield* Effect.fail(
      runtimeError({
        code: "InvalidQuery",
        topic,
        message: `Leased topic ${topic} requires exact equality filters for route fields: ${feed.routeBy.join(", ")}.`,
      }),
    );
  }
  const topicDefinition = yield* topicDefinitionFor(config, topic, feed.topic);
  const route: Record<string, CanonicalRouteValue> = Object.create(null);
  for (const field of feed.routeBy) {
    const value = exactEqValue(query["where"][field]);
    if (Option.isNone(value)) {
      return yield* Effect.fail(
        runtimeError({
          code: "InvalidQuery",
          topic,
          message: `Leased topic ${topic} route field ${field} must use an exact eq filter.`,
        }),
      );
    }
    const fieldSchema = topicDefinition.schema.fields[field];
    if (fieldSchema === undefined) {
      return yield* Effect.fail(
        runtimeError({
          code: "InvalidQuery",
          topic,
          message: `Leased topic ${topic} route field ${field} is not in the topic schema.`,
        }),
      );
    }
    yield* Schema.encodeUnknownEffect(fieldSchema)(value.value).pipe(
      Effect.mapError(() =>
        runtimeError({
          code: "InvalidQuery",
          topic,
          message: `Leased topic ${topic} route field ${field} value does not match the topic schema.`,
        }),
      ),
      Effect.asVoid,
    );
    const routeValue = value.value;
    if (!isCanonicalRouteValue(routeValue)) {
      return yield* Effect.fail(
        runtimeError({
          code: "InvalidQuery",
          topic,
          message: `Leased topic ${topic} route field ${field} value cannot be used as a stable leased gRPC route key.`,
        }),
      );
    }
    route[field] = routeValue;
  }
  return route;
});

const encodeFrame = (tag: string, payload: string): string => `${tag}:${payload.length}:${payload}`;

const isCanonicalRouteArray = (
  value: CanonicalRouteValue,
): value is ReadonlyArray<CanonicalRouteValue> => Array.isArray(value);

const encodeRouteRecord = (value: Readonly<Record<string, CanonicalRouteValue>>): string => {
  const entries: Array<string> = [];
  for (const [key, routeValue] of Object.entries(value).sort(([left], [right]) =>
    left.localeCompare(right),
  )) {
    const encodedValue = encodeRouteValue(routeValue);
    const encodedKey = JSON.stringify(key);
    entries.push(`${encodedKey.length}:${encodedKey}${encodedValue.length}:${encodedValue}`);
  }
  return entries.join("");
};

const encodeRouteValue = (value: CanonicalRouteValue): string => {
  if (BigDecimal.isBigDecimal(value)) {
    return encodeFrame("bigDecimal", BigDecimal.format(BigDecimal.normalize(value)));
  }
  if (typeof value === "bigint") {
    return encodeFrame("bigint", value.toString());
  }
  if (typeof value === "string") {
    return encodeFrame("string", value);
  }
  if (typeof value === "number") {
    return encodeFrame("number", Object.is(value, -0) ? "-0" : value.toString());
  }
  if (typeof value === "boolean") {
    return encodeFrame("boolean", value ? "true" : "false");
  }
  if (value === null) {
    return encodeFrame("null", "null");
  }
  if (isCanonicalRouteArray(value)) {
    const entries: Array<string> = [];
    for (const entry of value) {
      const encodedEntry = encodeRouteValue(entry);
      entries.push(`${encodedEntry.length}:${encodedEntry}`);
    }
    return encodeFrame("array", entries.join(""));
  }
  return encodeFrame("object", encodeRouteRecord(value));
};

const routeFeedKey = Effect.fn("ViewServerRuntime.grpc.leased.route.feedKey")(function* <
  Topic extends string,
>(topic: Topic, feedName: string, feed: RuntimeLeasedFeedDefinition, route: LeasedFeedRoute) {
  const parts: Array<string> = [];
  for (const field of feed.routeBy) {
    const routeValue = route[field];
    if (routeValue === undefined) {
      return yield* grpcLeaseError({
        message: `Leased gRPC route is missing configured field ${field}`,
        cause: route,
        phase: "request",
        feedName,
        topic,
      });
    }
    const encodedValue = encodeRouteValue(routeValue);
    parts.push(`${encodeURIComponent(field)}=${encodeURIComponent(encodedValue)}`);
  }
  return `${topic}/${feedName}/leased/${parts.join("&")}`;
});

const internalRowKey = (feedKey: string, publicKey: string): string =>
  `${feedKey}/row/${publicKey}`;

const callFeedRequest = (
  feedName: string,
  feed: RuntimeLeasedFeedDefinition,
  route: LeasedFeedRoute,
) => callLeasedGrpcSourceRequest(feedName, feed, route);

const callFeedAcquire = (
  feedName: string,
  feed: RuntimeLeasedFeedDefinition,
  input: LeasedFeedRuntimeInput,
) => callLeasedGrpcSourceAcquire(feedName, feed, input);

const callFeedRelease = (
  feedName: string,
  feed: RuntimeLeasedFeedDefinition,
  input: LeasedFeedRuntimeInput,
) => callLeasedGrpcSourceRelease(feedName, feed, input);

const topicDefinitionFor = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  topic: string,
  feedName: string,
): Effect.Effect<RuntimeTopicDefinition, ViewServerGrpcIngressError> =>
  Effect.suspend(() => {
    const topicDefinition = config.topics[topic];
    if (topicDefinition !== undefined) {
      return Effect.succeed(topicDefinition);
    }
    return grpcLeaseError({
      message: `gRPC leased feed ${feedName} references unknown topic ${topic}`,
      cause: topic,
      phase: "configuration",
      feedName,
      topic,
    });
  });

const isRuntimeTopic = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  topic: string,
): topic is Extract<keyof Topics, string> => Object.hasOwn(config.topics, topic);

const runtimeTopicFor = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  topic: string,
  feedName: string,
): Effect.Effect<Extract<keyof Topics, string>, ViewServerGrpcIngressError> =>
  Effect.suspend(() => {
    if (isRuntimeTopic(config, topic)) {
      return Effect.succeed(topic);
    }
    return grpcLeaseError({
      message: `gRPC leased feed ${feedName} references unknown topic ${topic}`,
      cause: topic,
      phase: "configuration",
      feedName,
      topic,
    });
  });

const mapLeasedValue = Effect.fn("ViewServerRuntime.grpc.leased.map")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  config: ViewServerTopicConfig<Topics>,
  feedName: string,
  feed: RuntimeLeasedFeedDefinition,
  route: LeasedFeedRoute,
  value: unknown,
) {
  const topicDefinition = yield* topicDefinitionFor(config, feed.topic, feedName);
  const row = yield* Effect.try({
    try: () =>
      Reflect.apply(feed.map, undefined, [
        {
          value,
          route,
          schema: topicDefinition.schema,
        },
      ]),
    catch: (cause) =>
      grpcLeaseError({
        message: `gRPC leased feed mapping failed for ${feedName}`,
        cause,
        phase: "mapping",
        feedName,
        topic: feed.topic,
      }),
  });
  const decoded = yield* Schema.decodeUnknownEffect(topicDefinition.schema)(row).pipe(
    Effect.mapError((cause) =>
      grpcLeaseError({
        message: `gRPC leased feed mapping produced an invalid row for ${feedName}`,
        cause,
        phase: "mapping",
        feedName,
        topic: feed.topic,
      }),
    ),
  );
  return decoded;
});

const rowKey = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  topic: string,
  feedName: string,
  row: object,
): Effect.Effect<string, ViewServerGrpcIngressError> =>
  Effect.gen(function* () {
    const topicDefinition = yield* topicDefinitionFor(config, topic, feedName);
    const keyField: string = topicDefinition.key;
    const key = Reflect.get(row, keyField);
    if (typeof key === "string") {
      return key;
    }
    return yield* grpcLeaseError({
      message: `gRPC leased feed row key ${keyField} for ${topic} is not a string`,
      cause: key,
      phase: "mapping",
      feedName,
      topic,
    });
  });

const topicKeyField = Effect.fn("ViewServerRuntime.grpc.leased.topicKeyField")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(config: ViewServerTopicConfig<Topics>, topic: string, feedName: string) {
  const topicDefinition = yield* topicDefinitionFor(config, topic, feedName);
  return topicDefinition.key;
});

type LeasedRowWithStorageKey = {
  readonly storageKey: string;
  readonly row: object;
};

const internalizeLeasedRow = Effect.fn("ViewServerRuntime.grpc.leased.row.internalize")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(config: ViewServerTopicConfig<Topics>, lease: ActiveLease, row: object) {
  const publicKey = yield* rowKey(config, lease.feed.topic, lease.feedName, row);
  const internalKey = internalRowKey(lease.feedKey, publicKey);
  lease.publicToInternalKeys.set(publicKey, internalKey);
  lease.internalToPublicKeys.set(internalKey, publicKey);
  return {
    storageKey: internalKey,
    row,
  } satisfies LeasedRowWithStorageKey;
});

const validateLeasedRowRoute = Effect.fn("ViewServerRuntime.grpc.leased.row.validateRoute")(
  function* (lease: ActiveLease, route: LeasedFeedRoute, row: object) {
    for (const field of lease.feed.routeBy) {
      const routeValue = route[field];
      const rowValue = Reflect.get(row, field);
      if (
        routeValue === undefined ||
        !isCanonicalRouteValue(rowValue) ||
        encodeRouteValue(rowValue) !== encodeRouteValue(routeValue)
      ) {
        return yield* grpcLeaseError({
          message: `gRPC leased feed ${lease.feedName} mapped row field ${field} outside the acquired route.`,
          cause: {
            field,
            rowValue,
            routeValue,
          },
          phase: "mapping",
          feedName: lease.feedName,
          topic: lease.feed.topic,
        });
      }
    }
    return row;
  },
);

const publicKeyForInternalKey = (lease: ActiveLease, key: string): string =>
  lease.internalToPublicKeys.get(key) ?? key;

const externalizeLeasedRow = <Row extends object>(
  lease: ActiveLease,
  keyField: string,
  row: Row,
): Row => {
  const rowKeyValue = Reflect.get(row, keyField);
  if (typeof rowKeyValue !== "string") {
    return row;
  }
  const cloned = Object.assign({}, row);
  Reflect.set(cloned, keyField, publicKeyForInternalKey(lease, rowKeyValue));
  return cloned;
};

const isGroupedRuntimeQuery = (query: unknown): query is Pick<GroupedQuery<object>, "groupBy"> =>
  isRecord(query) && Array.isArray(query["groupBy"]);

const groupedKeyEncodingErrorPrefix =
  "Leased gRPC grouped key value cannot be encoded as a stable public key";

const stableGroupedKeyValueTokenString = (value: unknown): string | undefined => {
  if (BigDecimal.isBigDecimal(value)) {
    return `["bigDecimal",${JSON.stringify(BigDecimal.format(BigDecimal.normalize(value)))}]`;
  }
  if (value === null) {
    return `["null"]`;
  }
  if (typeof value === "bigint") {
    return `["bigint",${JSON.stringify(value.toString())}]`;
  }
  if (typeof value === "number") {
    return `["number",${JSON.stringify(Object.is(value, -0) ? "-0" : String(value))}]`;
  }
  if (typeof value === "string") {
    return `["string",${JSON.stringify(value)}]`;
  }
  if (typeof value === "boolean") {
    return `["boolean",${value ? "true" : "false"}]`;
  }
  if (value === undefined) {
    return `["undefined"]`;
  }
  if (isCanonicalRouteValue(value)) {
    return `["canonical",${JSON.stringify(encodeRouteValue(value))}]`;
  }
  return undefined;
};

const stableGroupedKeyFieldTokenString = (field: string, value: unknown): string | undefined => {
  const valueToken = stableGroupedKeyValueTokenString(value);
  if (valueToken === undefined) {
    return undefined;
  }
  return `["array",[["string",${JSON.stringify(field)}],${valueToken}]]`;
};

const groupedKeyFromExternalizedRow = (
  query: Pick<GroupedQuery<object>, "groupBy">,
  row: object,
): string | undefined => {
  const tokens: Array<string> = [];
  for (const field of query.groupBy) {
    const token = stableGroupedKeyFieldTokenString(field, Reflect.get(row, field));
    if (token === undefined) {
      return undefined;
    }
    tokens.push(token);
  }
  return `["array",[${tokens.join(",")}]]`;
};

const groupedKeyEncodingErrorStatus = (lease: ActiveLease, queryId: string): StatusEvent => ({
  type: "status",
  topic: lease.feed.topic,
  queryId,
  status: "error",
  code: "RuntimeUnavailable",
  message: groupedKeyEncodingErrorPrefix,
});

const isGroupedKeyEncodingErrorStatus = (event: ViewServerLiveEvent<unknown>): boolean =>
  event.type === "status" &&
  event.status === "error" &&
  event.code === "RuntimeUnavailable" &&
  event.message?.startsWith(groupedKeyEncodingErrorPrefix) === true;

const isTerminalStatusEvent = (event: ViewServerLiveEvent<unknown>): event is StatusEvent =>
  event.type === "status" && (event.status === "closed" || event.status === "error");

const makeLeaseTerminalRegistration = Effect.fn(
  "ViewServerRuntime.grpc.leased.terminalRegistration.make",
)(function* (terminalSignal: Deferred.Deferred<LeaseTerminal>) {
  const ready = yield* Deferred.make<void>();
  const queryId = yield* Deferred.make<string>();
  const observer: ViewServerRuntimeCoreTerminalObserver = {
    onQueryRegistered: (registeredQueryId) =>
      Deferred.succeed(queryId, registeredQueryId).pipe(Effect.asVoid),
    onTerminalOccurrence: (status) =>
      Deferred.succeed(terminalSignal, {
        _tag: "Engine",
        ready,
        status,
      }).pipe(Effect.asVoid),
    onTerminalReady: () => Deferred.succeed(ready, undefined).pipe(Effect.asVoid),
  };
  return {
    observer,
    queryId,
  } satisfies LeaseTerminalRegistration;
});

const internalizeLeasedQuery = <Query extends Readonly<Record<string, unknown>>>(
  query: Query,
): Query => {
  const currentWhere: Record<string, unknown> = Object(query["where"]);
  const where: Record<string, unknown> = { ...currentWhere };
  return {
    ...query,
    where,
  };
};

const notifyLeaseSubscribers = Effect.fn("ViewServerRuntime.grpc.leased.subscribers.notify")(
  function* (lease: ActiveLease, message: string) {
    const terminal: UpstreamLeaseTerminal = {
      _tag: "Upstream",
      message,
    };
    yield* Effect.forEach(lease.terminalSignals, (signal) => Deferred.succeed(signal, terminal), {
      discard: true,
    });
  },
);

const resetLeaseRowCount = Effect.fn("ViewServerRuntime.grpc.leased.health.rowCount.reset")(
  function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
    requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
    health: ViewServerGrpcHealthLedger<Topics>,
    lease: ActiveLease,
  ) {
    const nowMillis = yield* Clock.currentTimeMillis;
    yield* health.rowsPublished(lease.feedKey, {
      messages: 0,
      rows: 0,
      rowCount: 0,
      nowMillis,
    });
    yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
  },
);

const externalizeLeasedEvent = <Row extends object>(
  lease: ActiveLease,
  keyField: string,
  groupedKeyTranslations: GroupedKeyTranslations | undefined,
  event: ViewServerLiveEvent<Row>,
): ViewServerLiveEvent<Row> => {
  if (event.type === "snapshot") {
    const rows = event.rows.map((row) => externalizeLeasedRow(lease, keyField, row));
    const keys: Array<string> = [];
    if (groupedKeyTranslations === undefined) {
      for (const key of event.keys) {
        keys.push(publicKeyForInternalKey(lease, key));
      }
    } else {
      const nextTranslations = new Map<string, string>();
      for (const [index, internalKey] of event.keys.entries()) {
        const row = Object(rows[index]);
        const publicKey = groupedKeyFromExternalizedRow(groupedKeyTranslations.query, row);
        if (publicKey === undefined) {
          return groupedKeyEncodingErrorStatus(lease, event.queryId);
        }
        nextTranslations.set(internalKey, publicKey);
        keys.push(publicKey);
      }
      groupedKeyTranslations.byInternalKey.clear();
      for (const [internalKey, publicKey] of nextTranslations) {
        groupedKeyTranslations.byInternalKey.set(internalKey, publicKey);
      }
    }
    return {
      ...event,
      keys,
      rows,
    };
  }
  if (event.type === "delta") {
    const operations: Array<(typeof event.operations)[number]> = [];
    if (groupedKeyTranslations === undefined) {
      for (const operation of event.operations) {
        if (operation.type === "move" || operation.type === "remove") {
          operations.push({
            ...operation,
            key: publicKeyForInternalKey(lease, operation.key),
          });
          continue;
        }
        const row = externalizeLeasedRow(lease, keyField, operation.row);
        operations.push({
          ...operation,
          key: publicKeyForInternalKey(lease, operation.key),
          row,
        });
      }
      return {
        ...event,
        operations,
      };
    }
    const pendingTranslations = new Map<string, string | undefined>();
    const publicGroupedKeyForInternalKey = (key: string): string | undefined =>
      pendingTranslations.has(key)
        ? pendingTranslations.get(key)
        : groupedKeyTranslations.byInternalKey.get(key);
    for (const operation of event.operations) {
      if (operation.type === "move" || operation.type === "remove") {
        const publicGroupedKey = publicGroupedKeyForInternalKey(operation.key);
        if (publicGroupedKey === undefined) {
          return groupedKeyEncodingErrorStatus(lease, event.queryId);
        }
        operations.push({
          ...operation,
          key: publicGroupedKey,
        });
        if (operation.type === "remove") {
          pendingTranslations.set(operation.key, undefined);
        }
        continue;
      }
      const row = externalizeLeasedRow(lease, keyField, operation.row);
      const publicGroupedKey = groupedKeyFromExternalizedRow(groupedKeyTranslations.query, row);
      if (publicGroupedKey === undefined) {
        return groupedKeyEncodingErrorStatus(lease, event.queryId);
      }
      operations.push({
        ...operation,
        key: publicGroupedKey,
        row,
      });
      pendingTranslations.set(operation.key, publicGroupedKey);
    }
    for (const [internalKey, publicKey] of pendingTranslations) {
      if (publicKey === undefined) {
        groupedKeyTranslations.byInternalKey.delete(internalKey);
      } else {
        groupedKeyTranslations.byInternalKey.set(internalKey, publicKey);
      }
    }
    return {
      ...event,
      operations,
    };
  }
  return event;
};

const callRuntimePublishMany = Effect.fn(
  "ViewServerRuntime.grpc.leased.runtime.publishManyDecoded",
)(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Topic extends Extract<keyof Topics, string>,
>(
  runtimeClient: ViewServerRuntimeCoreInternalClient<Topics>,
  topic: Topic,
  rows: ReadonlyArray<LeasedRowWithStorageKey>,
  feedName: string,
) {
  const effect = runtimeClient.publishManyDecodedRowsWithStorageKeys(topic, rows);
  if (!isRuntimeMutationEffect(effect)) {
    return yield* grpcLeaseError({
      message: `Runtime publishManyDecodedRowsWithStorageKeys did not return an Effect for leased gRPC feed ${feedName}`,
      cause: effect,
      phase: "publish",
      feedName,
      topic,
    });
  }
  yield* effect.pipe(
    Effect.asVoid,
    Effect.mapError((cause) =>
      grpcLeaseError({
        message: `gRPC leased feed publish failed for ${feedName}`,
        cause,
        phase: "publish",
        feedName,
        topic,
      }),
    ),
  );
});

const callRuntimeDelete = Effect.fn("ViewServerRuntime.grpc.leased.runtime.delete")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Topic extends Extract<keyof Topics, string>,
>(
  runtimeClient: ViewServerRuntimeCoreInternalClient<Topics>,
  topic: Topic,
  key: string,
  feedName: string,
) {
  const effect = runtimeClient.delete(topic, key);
  if (!isRuntimeMutationEffect(effect)) {
    return yield* grpcLeaseError({
      message: `Runtime delete did not return an Effect for leased gRPC feed ${feedName}`,
      cause: effect,
      phase: "release",
      feedName,
      topic,
    });
  }
  yield* effect.pipe(
    Effect.asVoid,
    Effect.mapError((cause) =>
      grpcLeaseError({
        message: `gRPC leased feed row cleanup failed for ${feedName}`,
        cause,
        phase: "release",
        feedName,
        topic,
      }),
    ),
  );
});

const publishLeasedBatch = Effect.fn("ViewServerRuntime.grpc.leased.publishBatch")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  config: ViewServerTopicConfig<Topics>,
  runtimeClient: ViewServerRuntimeCoreInternalClient<Topics>,
  requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
  health: ViewServerGrpcHealthLedger<Topics>,
  lease: ActiveLease,
  route: LeasedFeedRoute,
  values: ReadonlyArray<unknown>,
) {
  const rows = yield* Effect.forEach(values, (value) =>
    mapLeasedValue(config, lease.feedName, lease.feed, route, value).pipe(
      Effect.flatMap((row) => validateLeasedRowRoute(lease, route, row)),
      Effect.tapError((error) =>
        Clock.currentTimeMillis.pipe(
          Effect.flatMap((nowMillis) =>
            health.mappingFailed(lease.feedKey, {
              message: error.message,
              nowMillis,
            }),
          ),
        ),
      ),
    ),
  );
  const internalRows = yield* Effect.forEach(rows, (row) =>
    internalizeLeasedRow(config, lease, row),
  );
  const topic = yield* runtimeTopicFor(config, lease.feed.topic, lease.feedName);
  yield* callRuntimePublishMany(runtimeClient, topic, internalRows, lease.feedName).pipe(
    Effect.tapError((error) =>
      Clock.currentTimeMillis.pipe(
        Effect.flatMap((nowMillis) =>
          health.publishFailed(lease.feedKey, {
            message: error.message,
            nowMillis,
          }),
        ),
      ),
    ),
  );
  const nowMillis = yield* Clock.currentTimeMillis;
  yield* health.rowsPublished(lease.feedKey, {
    messages: values.length,
    rows: rows.length,
    rowCount: lease.publicToInternalKeys.size,
    nowMillis,
  });
  yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
});

const internalFeedFailureMessage = (feedName: string, cause: Cause.Cause<unknown>): string =>
  `gRPC leased feed ${feedName} failed: ${Cause.pretty(cause)}`;

const startLeaseStream = Effect.fn("ViewServerRuntime.grpc.leased.stream.start")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  config: ViewServerTopicConfig<Topics>,
  runtimeClient: ViewServerRuntimeCoreInternalClient<Topics>,
  requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
  health: ViewServerGrpcHealthLedger<Topics>,
  lease: ActiveLease,
  lock: Semaphore.Semaphore,
  route: LeasedFeedRoute,
  input: LeasedFeedRuntimeInput,
) {
  const releaseResources = (yield* Effect.cached(
    ignoreGrpcFeedReleaseFailure(callFeedRelease(lease.feedName, lease.feed, input)).pipe(
      Effect.withSpan("ViewServerRuntime.grpc.leased.resources.release"),
    ),
  )).pipe(Effect.uninterruptible);
  yield* Scope.addFinalizer(lease.scope, releaseResources);
  const stream = yield* callFeedAcquire(lease.feedName, lease.feed, input);
  const degradeInactiveLease = (input: {
    readonly publicMessage: string;
    readonly healthMessage: string;
  }) =>
    lock.withPermit(
      Effect.gen(function* () {
        lease.acceptingSubscribers = false;
        const cleanupRows = Effect.gen(function* () {
          const cleanupExit = yield* lease.cleanupRows.pipe(Effect.exit);
          if (Exit.isSuccess(cleanupExit)) {
            yield* resetLeaseRowCount(requestHealthRefresh, health, lease);
            return;
          }
          yield* ignoreLeasedReleaseFailure(Effect.failCause(cleanupExit.cause));
          yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
        });
        yield* runAllFinalizers([
          releaseResources,
          health.feedDegraded(lease.feedKey, input.healthMessage),
          health.clientDegraded(lease.feed.client, input.healthMessage),
          cleanupRows,
          notifyLeaseSubscribers(lease, input.publicMessage),
        ]);
      }),
    );
  const runFeed = stream.pipe(
    Stream.mapError((cause) =>
      grpcLeaseError({
        message: `gRPC leased feed stream failed for ${lease.feedName}`,
        cause,
        phase: "stream",
        feedName: lease.feedName,
        topic: lease.feed.topic,
      }),
    ),
    Stream.groupedWithin(grpcMessageBatchSize, grpcMessageBatchFlushInterval),
    Stream.runForEach((values) =>
      publishLeasedBatch(config, runtimeClient, requestHealthRefresh, health, lease, route, values),
    ),
    Effect.exit,
    Effect.flatMap((exit) => {
      if (Exit.isSuccess(exit)) {
        return degradeInactiveLease({
          publicMessage: "gRPC leased upstream completed unexpectedly.",
          healthMessage: `gRPC leased feed ${lease.feedName} completed unexpectedly.`,
        });
      }
      if (Cause.hasInterruptsOnly(exit.cause)) {
        return Effect.when(
          degradeInactiveLease({
            publicMessage: "gRPC leased upstream interrupted unexpectedly.",
            healthMessage: `gRPC leased feed ${lease.feedName} interrupted unexpectedly.`,
          }),
          Effect.sync(() => lease.acceptingSubscribers),
        ).pipe(Effect.asVoid);
      }
      return degradeInactiveLease({
        publicMessage: "gRPC leased upstream failed.",
        healthMessage: internalFeedFailureMessage(lease.feedName, exit.cause),
      });
    }),
  );
  yield* runFeed.pipe(Effect.forkIn(lease.scope, { startImmediately: true }));
});

const closeLeaseRows = Effect.fn("ViewServerRuntime.grpc.leased.rows.close")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  config: ViewServerTopicConfig<Topics>,
  runtimeClient: ViewServerRuntimeCoreInternalClient<Topics>,
  lease: LeaseRowOwner,
) {
  const topic = yield* runtimeTopicFor(config, lease.feed.topic, lease.feedName);
  yield* Effect.forEach(
    lease.internalToPublicKeys.keys(),
    (key) => callRuntimeDelete(runtimeClient, topic, key, lease.feedName),
    {
      discard: true,
    },
  );
});

const leasedFeedsByTopic = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
): Map<string, readonly [string, RuntimeLeasedFeedDefinition]> => {
  const feeds = new Map<string, readonly [string, RuntimeLeasedFeedDefinition]>();
  for (const [feedName, feed] of Object.entries(options.feeds)) {
    if (isRuntimeLeasedFeed(feed)) {
      feeds.set(feed.topic, [feedName, feed]);
    }
  }
  return feeds;
};

const normalizeAcquireLeaseError =
  (topic: string) =>
  (error: ViewServerRuntimeError | ViewServerGrpcIngressError): ViewServerRuntimeError => {
    if (error instanceof ViewServerGrpcIngressError) {
      return runtimeError({
        code: "RuntimeUnavailable",
        topic,
        message: error.message,
      });
    }
    return error;
  };

const normalizeAcquireLeaseCause =
  (topic: string) =>
  (
    cause: Cause.Cause<ViewServerRuntimeError | ViewServerGrpcIngressError>,
  ): Cause.Cause<ViewServerRuntimeError> =>
    Cause.map(cause, normalizeAcquireLeaseError(topic));

export const makeViewServerGrpcLeaseManager = Effect.fn(
  "ViewServerRuntime.grpc.leased.makeManager",
)(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerTopicConfig<Topics>,
  runtimeClient: ViewServerRuntimeCoreInternalClient<Topics>,
  liveClient: ViewServerRuntimeLiveClient<Topics>,
  internalLiveClient: ViewServerRuntimeCoreInternalLiveClient<Topics>,
  requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
  options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
  health: ViewServerGrpcHealthLedger<Topics>,
  makeClient: ViewServerGrpcClientFactory = makeDefaultGrpcClient,
  testProbe?: ViewServerGrpcLeaseManagerTestProbe,
) {
  const leases = new Map<string, ActiveLease>();
  const feedsByTopic = leasedFeedsByTopic(options);
  const sourceOwnership = makeSourceOwnershipPolicy(config);
  const lock = yield* Semaphore.make(1);
  const subscriptionScope = yield* Scope.make("parallel");
  let closed = false;

  const acquireLease = Effect.fn("ViewServerRuntime.grpc.leased.acquireLease")(function* <
    const Topic extends Extract<keyof Topics, string>,
  >(topic: Topic, query: unknown) {
    const configuredFeed = feedsByTopic.get(topic);
    if (configuredFeed === undefined) {
      if (sourceOwnership.isGrpcLeasedTopic(topic)) {
        return yield* Effect.fail(
          runtimeError({
            code: "RuntimeUnavailable",
            topic,
            message: `Leased gRPC topic ${topic} has no configured leased feed.`,
          }),
        );
      }
      return Option.none<AcquiredLease>();
    }
    const [feedName, feed] = configuredFeed;
    const route = yield* extractRoute(config, topic, feed, query);
    const feedKey = yield* routeFeedKey(topic, feedName, feed, route).pipe(
      Effect.mapError((error) =>
        runtimeError({
          code: "RuntimeUnavailable",
          topic,
          message: error.message,
        }),
      ),
    );
    if (closed) {
      return yield* Effect.fail(
        runtimeError({
          code: "RuntimeUnavailable",
          topic,
          message: "gRPC leased feed manager is closed.",
        }),
      );
    }
    const existing = leases.get(feedKey);
    const terminalSignal = yield* Deferred.make<LeaseTerminal>();
    if (existing !== undefined) {
      return yield* Effect.uninterruptible(
        Effect.gen(function* () {
          if (!existing.acceptingSubscribers) {
            yield* Deferred.succeed(terminalSignal, closedLeaseTerminal);
            return yield* Effect.fail(
              runtimeError({
                code: "RuntimeUnavailable",
                topic,
                message:
                  "gRPC leased upstream is not accepting new subscribers after completion or failure.",
              }),
            );
          }
          existing.subscribers += 1;
          existing.terminalSignals.add(terminalSignal);
          yield* health.subscriberAdded(feedKey);
          return Option.some({
            lease: existing,
            terminalSignal,
          });
        }),
      );
    }
    const clientDefinition = options.clients[feed.client];
    if (clientDefinition === undefined) {
      return yield* Effect.fail(
        runtimeError({
          code: "RuntimeUnavailable",
          topic,
          message: `gRPC leased feed ${feedName} references missing client: ${feed.client}`,
        }),
      );
    }
    const baseUrl = options.clientBaseUrls[feed.client];
    if (baseUrl === undefined) {
      return yield* Effect.fail(
        runtimeError({
          code: "RuntimeUnavailable",
          topic,
          message: `gRPC leased feed ${feedName} references unresolved client URL: ${feed.client}`,
        }),
      );
    }
    const grpcClient = yield* Effect.try({
      try: () => makeClient(clientDefinition, baseUrl),
      catch: (cause) =>
        grpcLeaseError({
          message: `gRPC leased client creation failed for ${feedName}`,
          cause,
          phase: "client",
          feedName,
          topic,
        }),
    }).pipe(
      Effect.mapError((error) =>
        runtimeError({
          code: "RuntimeUnavailable",
          topic,
          message: error.message,
        }),
      ),
    );
    const request = yield* callFeedRequest(feedName, feed, route).pipe(
      Effect.mapError((error) =>
        runtimeError({
          code: "RuntimeUnavailable",
          topic,
          message: error.message,
        }),
      ),
    );
    const scope = yield* Scope.make("parallel");
    const rowOwner: LeaseRowOwner = {
      feedName,
      feed,
      internalToPublicKeys: new Map<string, string>(),
    };
    const cleanupRows = (yield* Effect.cached(
      closeLeaseRows(config, runtimeClient, rowOwner),
    )).pipe(Effect.uninterruptible);
    const lease: ActiveLease = {
      ...rowOwner,
      feedKey,
      route,
      scope,
      publicToInternalKeys: new Map<string, string>(),
      cleanupRows,
      terminalSignals: new Set<Deferred.Deferred<LeaseTerminal>>([terminalSignal]),
      subscriptions: new Set<ActiveLeaseSubscription>(),
      subscribers: 1,
      acceptingSubscribers: true,
    };
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        leases.set(feedKey, lease);
        const input: LeasedFeedRuntimeInput = makeGrpcSourceInput(grpcClient, request, route);
        const startedAt = yield* Clock.currentTimeMillis;
        yield* health.clientConnected(feed.client, startedAt);
        yield* health.leasedFeedStarting({
          feedName,
          feedKey,
          topic,
          clientName: feed.client,
        });
        yield* health.subscriberAdded(feedKey);
        yield* health.feedReady(feedKey);
        yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
        const startExit = yield* restore(
          startLeaseStream(
            config,
            runtimeClient,
            requestHealthRefresh,
            health,
            lease,
            lock,
            route,
            input,
          ),
        ).pipe(Effect.exit);
        if (Exit.isFailure(startExit)) {
          const cleanupExit = yield* runAllFinalizers([
            Scope.close(scope, Exit.void),
            health.clientDegraded(
              feed.client,
              `gRPC leased feed ${feedName} failed to start: ${String(startExit.cause)}`,
            ),
            Deferred.succeed(terminalSignal, closedLeaseTerminal),
            health.leasedFeedRemoved(feedKey),
            Effect.sync(() => leases.delete(feedKey)),
            ignoreGrpcHealthRefreshFailure(requestHealthRefresh),
          ]).pipe(Effect.exit);
          const cause = Exit.isFailure(cleanupExit)
            ? Cause.combine(startExit.cause, cleanupExit.cause)
            : startExit.cause;
          return yield* Effect.failCause(normalizeAcquireLeaseCause(topic)(cause));
        }
        return Option.some({
          lease,
          terminalSignal,
        });
      }),
    );
  });

  const releaseLeaseUnderPermit: (
    lease: ActiveLease,
  ) => Effect.Effect<Option.Option<ActiveLease>, never, never> = Effect.fn(
    "ViewServerRuntime.grpc.leased.releaseLeaseUnderPermit",
  )(function* (lease: ActiveLease) {
    const current = leases.get(lease.feedKey);
    if (current === undefined) {
      return Option.none();
    }
    current.subscribers -= 1;
    yield* health.subscriberRemoved(lease.feedKey);
    if (current.subscribers > 0) {
      yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
      return Option.none();
    }
    if (!current.acceptingSubscribers) {
      return Option.some(current);
    }
    current.acceptingSubscribers = false;
    yield* health.feedStopping(lease.feedKey);
    yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
    return Option.some(current);
  });

  const cleanupReleasedLease: (lease: ActiveLease) => Effect.Effect<void, never, never> = Effect.fn(
    "ViewServerRuntime.grpc.leased.releaseLease.cleanup",
  )(function* (lease: ActiveLease) {
    const cleanupRowsAndHealth = Effect.gen(function* () {
      const cleanupExit = yield* lease.cleanupRows.pipe(Effect.exit);
      if (Exit.isFailure(cleanupExit)) {
        yield* ignoreLeasedReleaseFailure(Effect.failCause(cleanupExit.cause));
        yield* health.feedDegraded(
          lease.feedKey,
          `gRPC leased feed row cleanup failed for ${lease.feedName}`,
        );
        yield* health.clientDegraded(
          lease.feed.client,
          `gRPC leased feed row cleanup failed for ${lease.feedName}`,
        );
        yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
        return;
      }
      yield* resetLeaseRowCount(requestHealthRefresh, health, lease);
      yield* lock.withPermit(
        Effect.gen(function* () {
          leases.delete(lease.feedKey);
          yield* health.leasedFeedRemoved(lease.feedKey);
        }),
      );
      yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
    });
    yield* runAllFinalizers([Scope.close(lease.scope, Exit.void), cleanupRowsAndHealth]);
  });

  const releaseLease: (lease: ActiveLease) => Effect.Effect<void, never, never> = Effect.fn(
    "ViewServerRuntime.grpc.leased.releaseLease",
  )(function* (lease: ActiveLease) {
    const releasedLease = yield* lock.withPermit(releaseLeaseUnderPermit(lease));
    if (Option.isSome(releasedLease)) {
      yield* cleanupReleasedLease(releasedLease.value);
    }
  });

  const withLeaseClose = <Row extends object>(input: {
    readonly subscription: ViewServerLiveSubscription<Row>;
    readonly lease: ActiveLease;
    readonly keyField: string;
    readonly query: unknown;
    readonly terminalSignal: Deferred.Deferred<LeaseTerminal>;
    readonly terminalRegistration: LeaseTerminalRegistration;
  }): Effect.Effect<ViewServerLiveSubscription<Row>, never, never> =>
    Effect.gen(function* () {
      const groupedKeyTranslations: GroupedKeyTranslations | undefined = isGroupedRuntimeQuery(
        input.query,
      )
        ? {
            query: input.query,
            byInternalKey: new Map<string, string>(),
          }
        : undefined;
      if (groupedKeyTranslations !== undefined) {
        testProbe?.onGroupedKeyTranslationsCreated(groupedKeyTranslations.byInternalKey);
      }
      function close(): Effect.Effect<void, never, never> {
        return closeEffect;
      }
      const subscriptionOwner: ActiveLeaseSubscription = { close };
      const closeEffect = (yield* Effect.cached(
        Effect.gen(function* () {
          input.lease.terminalSignals.delete(input.terminalSignal);
          yield* Deferred.succeed(input.terminalSignal, closedLeaseTerminal);
          yield* runAllFinalizers([
            input.subscription.close().pipe(ignoreLeasedSubscriptionCloseFailure),
            releaseLease(input.lease),
            Effect.sync(() => input.lease.subscriptions.delete(subscriptionOwner)),
            Effect.sync(() => {
              groupedKeyTranslations?.byInternalKey.clear();
            }),
          ]);
        }).pipe(Effect.withSpan("ViewServerRuntime.grpc.leased.subscription.close")),
      )).pipe(Effect.uninterruptible);
      const runtimeTerminal: RuntimeLeaseTerminal = {
        _tag: "Runtime",
      };
      const claimRuntimeTerminal = (terminal: RuntimeLeaseTerminal) =>
        Effect.gen(function* () {
          yield* Deferred.succeed(input.terminalSignal, terminal);
          return (yield* Deferred.await(input.terminalSignal)) === terminal;
        });
      const runtimeEvents = input.subscription.events.pipe(
        Stream.map((event) =>
          externalizeLeasedEvent(input.lease, input.keyField, groupedKeyTranslations, event),
        ),
        Stream.filterEffect((event) => {
          if (isGroupedKeyEncodingErrorStatus(event)) {
            return claimRuntimeTerminal(runtimeTerminal);
          }
          if (isTerminalStatusEvent(event)) {
            return Effect.succeed(false);
          }
          return Effect.succeed(true);
        }),
        Stream.takeUntil(isGroupedKeyEncodingErrorStatus),
      );
      const terminalStatusEvents = Stream.fromEffect(Deferred.await(input.terminalSignal)).pipe(
        Stream.flatMap((terminal) => {
          if (terminal._tag === "Engine") {
            return Stream.succeed(terminal.status);
          }
          if (terminal._tag === "Upstream") {
            return Stream.fromEffect(Deferred.await(input.terminalRegistration.queryId)).pipe(
              Stream.map(
                (queryId): StatusEvent => ({
                  type: "status",
                  topic: input.lease.feed.topic,
                  queryId,
                  status: "error",
                  code: "RuntimeUnavailable",
                  message: terminal.message,
                }),
              ),
            );
          }
          return Stream.empty;
        }),
      );
      const wrappedSubscription: ViewServerLiveSubscription<Row> = {
        events: runtimeEvents.pipe(
          Stream.concat(terminalStatusEvents),
          Stream.takeUntil(isTerminalStatusEvent),
          Stream.ensuring(close()),
        ),
        close: () => close(),
      };
      const closeAfterTerminal = Deferred.await(input.terminalSignal).pipe(
        Effect.flatMap((terminal) => {
          if (terminal._tag === "Engine") {
            return Deferred.await(terminal.ready).pipe(Effect.andThen(close()));
          }
          return terminal._tag === "Runtime" || terminal._tag === "Upstream"
            ? close()
            : Effect.void;
        }),
      );
      const registered = yield* lock.withPermit(
        Effect.gen(function* () {
          const terminalAlreadyClaimed = yield* Deferred.isDone(input.terminalSignal);
          if (closed || !input.lease.acceptingSubscribers || terminalAlreadyClaimed) {
            return false;
          }
          input.lease.subscriptions.add(subscriptionOwner);
          yield* closeAfterTerminal.pipe(
            Effect.forkIn(subscriptionScope, { startImmediately: true }),
          );
          return true;
        }),
      );
      if (!registered) {
        const terminal = yield* Deferred.await(input.terminalSignal);
        if (terminal._tag === "Engine") {
          yield* Deferred.await(terminal.ready);
        }
        yield* close();
      }
      return wrappedSubscription;
    });

  const releaseAcquiredLeaseUnderPermit = (
    acquired: AcquiredLease,
  ): Effect.Effect<Option.Option<ActiveLease>, never, never> =>
    Effect.gen(function* () {
      acquired.lease.terminalSignals.delete(acquired.terminalSignal);
      yield* Deferred.succeed(acquired.terminalSignal, closedLeaseTerminal);
      return yield* releaseLeaseUnderPermit(acquired.lease);
    });
  const releaseAcquiredLease = (acquired: AcquiredLease): Effect.Effect<void, never, never> =>
    Effect.gen(function* () {
      const releasedLease = yield* lock.withPermit(releaseAcquiredLeaseUnderPermit(acquired));
      if (Option.isSome(releasedLease)) {
        yield* cleanupReleasedLease(releasedLease.value);
      }
    });

  function subscribe<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  ): Effect.Effect<
    ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  function subscribe<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  ): Effect.Effect<
    ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError | ViewServerTransportError
  > {
    return Effect.gen(function* () {
      const lease = yield* lock
        .withPermit(acquireLease(topic, query))
        .pipe(
          Effect.catchCause((cause) => Effect.failCause(normalizeAcquireLeaseCause(topic)(cause))),
        );
      if (Option.isNone(lease)) {
        return yield* internalLiveClient.subscribeInternal<Topic, Query>(topic, query);
      }
      const acquired = lease.value;
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const keyField = yield* topicKeyField(config, topic, acquired.lease.feedName).pipe(
            Effect.mapError((error) =>
              runtimeError({
                code: "RuntimeUnavailable",
                topic,
                message: error.message,
              }),
            ),
          );
          const internalQuery = internalizeLeasedQuery(query);
          const terminalRegistration = yield* makeLeaseTerminalRegistration(
            acquired.terminalSignal,
          );
          const subscription = yield* restore(
            internalLiveClient.subscribeObservedInternal<Topic, Query>(
              topic,
              internalQuery,
              terminalRegistration.observer,
            ),
          );
          return yield* withLeaseClose({
            subscription,
            lease: acquired.lease,
            keyField,
            query,
            terminalSignal: acquired.terminalSignal,
            terminalRegistration,
          });
        }),
      ).pipe(Effect.onError(() => releaseAcquiredLease(acquired)));
    });
  }

  const subscribeRuntime: ViewServerRuntimeLiveClient<Topics>["subscribeRuntime"] = (
    topic,
    query,
  ) =>
    Effect.gen(function* () {
      const lease = yield* lock
        .withPermit(acquireLease(topic, query))
        .pipe(
          Effect.catchCause((cause) => Effect.failCause(normalizeAcquireLeaseCause(topic)(cause))),
        );
      if (Option.isNone(lease)) {
        return yield* internalLiveClient.subscribeRuntimeInternal(topic, query);
      }
      const acquired = lease.value;
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const keyField = yield* topicKeyField(config, topic, acquired.lease.feedName).pipe(
            Effect.mapError((error) =>
              runtimeError({
                code: "RuntimeUnavailable",
                topic,
                message: error.message,
              }),
            ),
          );
          const internalQuery = internalizeLeasedQuery(query);
          const terminalRegistration = yield* makeLeaseTerminalRegistration(
            acquired.terminalSignal,
          );
          const subscription = yield* restore(
            internalLiveClient.subscribeRuntimeObservedInternal(
              topic,
              internalQuery,
              terminalRegistration.observer,
            ),
          );
          return yield* withLeaseClose({
            subscription,
            lease: acquired.lease,
            keyField,
            query,
            terminalSignal: acquired.terminalSignal,
            terminalRegistration,
          });
        }),
      ).pipe(Effect.onError(() => releaseAcquiredLease(acquired)));
    });

  const snapshot: ViewServerRuntimeClient<Topics>["snapshot"] = (topic, query) =>
    sourceOwnership
      .requirePublicReadAllowed(topic, "managedRuntime")
      .pipe(Effect.flatMap(() => runtimeClient.snapshot(topic, query)));

  const publish: ViewServerRuntimeClient<Topics>["publish"] = (topic, row) =>
    sourceOwnership
      .requirePublicMutationAllowed(topic, "managedRuntime")
      .pipe(Effect.flatMap(() => runtimeClient.publish(topic, row)));
  const publishMany: ViewServerRuntimeClient<Topics>["publishMany"] = (topic, rows) =>
    sourceOwnership
      .requirePublicMutationAllowed(topic, "managedRuntime")
      .pipe(Effect.flatMap(() => runtimeClient.publishMany(topic, rows)));
  const patch: ViewServerRuntimeClient<Topics>["patch"] = (topic, key, patchValue) =>
    sourceOwnership
      .requirePublicMutationAllowed(topic, "managedRuntime")
      .pipe(Effect.flatMap(() => runtimeClient.patch(topic, key, patchValue)));
  const deleteRow: ViewServerRuntimeClient<Topics>["delete"] = (topic, key) =>
    sourceOwnership
      .requirePublicMutationAllowed(topic, "managedRuntime")
      .pipe(Effect.flatMap(() => runtimeClient.delete(topic, key)));

  const reset: ViewServerRuntimeClient<Topics>["reset"] = () =>
    sourceOwnership
      .requirePublicResetAllowed("managedRuntime")
      .pipe(Effect.flatMap(() => runtimeClient.reset()));

  const client: ViewServerRuntimeClient<Topics> = {
    publish,
    publishMany,
    patch,
    delete: deleteRow,
    snapshot,
    health: runtimeClient.health,
    reset,
  };

  const close = (yield* Effect.cached(
    Effect.gen(function* () {
      const activeLeases = yield* lock.withPermit(
        Effect.sync(() => {
          closed = true;
          const currentLeases = Array.from(leases.values());
          leases.clear();
          for (const lease of currentLeases) {
            lease.acceptingSubscribers = false;
          }
          return currentLeases;
        }),
      );
      yield* runAllFinalizers([
        runAllFinalizers(
          activeLeases.map((lease) =>
            runAllFinalizers([
              Scope.close(lease.scope, Exit.void),
              runAllFinalizers(
                Array.from(lease.terminalSignals, (signal) =>
                  Deferred.succeed(signal, closedLeaseTerminal),
                ),
              ),
              Effect.sync(() => lease.terminalSignals.clear()),
              runAllFinalizers(
                Array.from(lease.subscriptions, (subscription) =>
                  subscription.close().pipe(ignoreLeasedSubscriptionCloseFailure),
                ),
              ),
              Effect.sync(() => lease.subscriptions.clear()),
              lease.cleanupRows.pipe(ignoreLeasedReleaseFailure),
              health.leasedFeedRemoved(lease.feedKey),
            ]),
          ),
        ),
        Scope.close(subscriptionScope, Exit.void),
        ignoreGrpcHealthRefreshFailure(requestHealthRefresh),
      ]);
    }).pipe(Effect.withSpan("ViewServerRuntime.grpc.leased.close")),
  )).pipe(Effect.uninterruptible);

  return {
    client,
    liveClient: {
      close: liveClient.close.pipe(Effect.ensuring(close)),
      health: liveClient.health,
      subscribe,
      subscribeRuntime,
      subscribeHealth: liveClient.subscribeHealth,
      subscribeHealthSummary: liveClient.subscribeHealthSummary,
    },
    close,
  };
});
