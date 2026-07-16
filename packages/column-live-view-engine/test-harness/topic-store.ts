import { Effect } from "effect";
import type { LiveTopicSubscriber } from "../src/topic-subscriber";
import {
  acquireTopicStoreSubscription,
  registerTopicStoreSubscription,
  TopicStore,
} from "../src/topic-store";
import { topicStoreQueryResources } from "../src/topic-store-state";

export const topicStoreTestQueryInterface = (store: TopicStore) =>
  topicStoreQueryResources(store).queryInterface;

export const topicStoreTestQueryMetadata = (store: TopicStore) =>
  topicStoreQueryResources(store).metadata;

export const registerTestTopicStoreSubscriber = (
  store: TopicStore,
  subscriber: LiveTopicSubscriber,
): Effect.Effect<void> =>
  acquireTopicStoreSubscription(store, (permit, markAcquired) =>
    Effect.gen(function* () {
      const subscription = {
        close: () => Effect.void,
      };
      yield* registerTopicStoreSubscription(permit, subscriber);
      yield* markAcquired(subscription);
      return subscription;
    }),
  ).pipe(Effect.asVoid);
