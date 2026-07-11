import { Effect } from "effect";
import type { LiveTopicSubscriber } from "../src/topic-subscriber";
import {
  acquireTopicStoreSubscription,
  registerTopicStoreSubscription,
  TopicStore,
} from "../src/topic-store";

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
