import { Effect } from "effect";
import { activeStoreRawQueryExecutionCount } from "./active-query";
import { collectTopicStoreHealthView, type TopicStoreHealthState } from "./topic-store-health";
import {
  TopicStore,
  topicStoreRawQueryMetadata,
  topicStoreReadModel,
  topicStoreState,
} from "./topic-store-state";

export { TopicStore, topicStoreRawQueryMetadata, topicStoreReadModel };
export {
  deleteTopicStoreRow,
  patchTopicStoreRow,
  publishTopicStoreRow,
  publishTopicStoreRows,
} from "./topic-store-mutation";
export { closeTopicStoreSubscriptions, resetTopicStore } from "./topic-store-lifecycle";
export {
  acquireTopicStoreSubscription,
  closeBackpressuredTopicStoreSubscription,
  closeTopicStoreSubscription,
  registerTopicStoreSubscription,
  trackTopicStoreSubscriptionQueueDepth,
} from "./topic-store-subscription";
export {
  acquireTopicStoreMaterializedQueryExecution,
  acquireTopicStoreRawQueryExecution,
  evaluateTopicStoreGroupedQuery,
  evaluateTopicStoreRawQuery,
  prepareTopicStoreGroupedQuery,
  prepareTopicStoreRawQuery,
  releaseTopicStoreMaterializedQueryExecution,
  releaseTopicStoreRawQueryExecution,
} from "./topic-store-query";
export type { TopicStoreSubscriptionPermit } from "./topic-store-state";

const topicStoreHealthState = (store: TopicStore, activeViews: number): TopicStoreHealthState => {
  const state = topicStoreState(store);
  return {
    activeViews,
    healthLedger: state.healthLedger,
    subscribers: state.subscribers,
    topic: store.topic,
  };
};

export const collectTopicStoreHealth = Effect.fn("ColumnLiveViewEngine.topicStore.health")(
  function* (store: TopicStore, closed: boolean) {
    const activeViews = yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store));
    return yield* collectTopicStoreHealthView(topicStoreHealthState(store, activeViews), closed);
  },
);
