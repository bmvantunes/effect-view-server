import type { LiveQueryRow, TopicRow } from "@view-server/config";
import { Effect } from "effect";
import { UnsupportedQueryError } from "./engine-errors";
import type { DecodableTopicDefinitions } from "./engine-contract";
import { makeLiveSubscription } from "./live-subscription";
import {
  evaluateCompiledRawQuery,
  prepareRawQuery,
  type CompiledRawQuery,
} from "./raw-query-compiler";
import { liveQueryResult, type QueryEvaluation } from "./query-result";
import type { TopicStore } from "./topic-store";

type RowObject = object;

export type ExecutableQuery<StoreRow extends RowObject, ResultRow extends RowObject> = {
  readonly kind: "raw";
  readonly compiled: CompiledRawQuery<StoreRow, ResultRow>;
};

export const isGroupedQuery = (query: unknown): boolean =>
  typeof query === "object" &&
  query !== null &&
  !Array.isArray(query) &&
  ("groupBy" in query || "aggregates" in query);

const unsupportedGroupedQuery = (topic: string) =>
  new UnsupportedQueryError({
    topic,
    message: "Grouped aggregate queries are not implemented in this slice.",
  });

export const prepareExecutableQuery = Effect.fn("ColumnLiveViewEngine.queryExecution.prepare")(
  function* <StoreRow extends RowObject, ResultRow extends RowObject>(
    topic: string,
    store: TopicStore<StoreRow>,
    query: unknown,
  ) {
    if (isGroupedQuery(query)) {
      return yield* unsupportedGroupedQuery(topic);
    }
    const compiled = yield* prepareRawQuery<StoreRow, ResultRow>(
      topic,
      store.rawQueryMetadata,
      query,
    );
    return {
      kind: "raw",
      compiled,
    } satisfies ExecutableQuery<StoreRow, ResultRow>;
  },
);

export const evaluateExecutableQuery = <StoreRow extends RowObject, ResultRow extends RowObject>(
  store: TopicStore<StoreRow>,
  executable: ExecutableQuery<StoreRow, ResultRow>,
): QueryEvaluation<ResultRow> => evaluateCompiledRawQuery(store, executable.compiled);

export const snapshotExecutableQuery = Effect.fn("ColumnLiveViewEngine.queryExecution.snapshot")(
  function* <
    Topics extends DecodableTopicDefinitions,
    Topic extends Extract<keyof Topics, string>,
    const Query,
  >(topic: Topic, store: TopicStore<TopicRow<Topics, Topic>>, query: Query) {
    type StoreRow = TopicRow<Topics, Topic>;
    type ResultRow = LiveQueryRow<StoreRow, Query>;
    const executable = yield* prepareExecutableQuery<StoreRow, ResultRow>(topic, store, query);
    return liveQueryResult(evaluateExecutableQuery(store, executable));
  },
);

export const subscribeExecutableQuery = Effect.fn("ColumnLiveViewEngine.queryExecution.subscribe")(
  function* <
    Topics extends DecodableTopicDefinitions,
    Topic extends Extract<keyof Topics, string>,
    const Query,
  >(
    topic: Topic,
    store: TopicStore<TopicRow<Topics, Topic>>,
    query: Query,
    input: {
      readonly queryId: string;
      readonly queueCapacity: number;
    },
  ) {
    type StoreRow = TopicRow<Topics, Topic>;
    type ResultRow = LiveQueryRow<StoreRow, Query>;
    const executable = yield* prepareExecutableQuery<StoreRow, ResultRow>(topic, store, query);
    return yield* makeLiveSubscription({
      store,
      queryId: input.queryId,
      compiled: executable.compiled,
      queueCapacity: input.queueCapacity,
    });
  },
);
