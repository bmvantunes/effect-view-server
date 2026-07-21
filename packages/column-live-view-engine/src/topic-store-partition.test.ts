import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { InvalidRowError } from "./index";
import type { LiveTopicSubscriber } from "./topic-subscriber";
import {
  publishTopicStoreDecodedRowsWithStorageKeys,
  publishTopicStoreRow,
  resetTopicStore,
  TopicStore,
} from "./topic-store";
import {
  registerTestTopicStoreSubscriber,
  topicStoreTestQueryInterface,
} from "../test-harness/topic-store";
import { Order, order } from "../test-harness/public-engine";

const invalidRow = (topic: string, message: string) => InvalidRowError.make({ topic, message });

const makeSubscriber = (
  queryId: string,
  notifications: Array<string>,
  partitionKey?: string,
): LiveTopicSubscriber => ({
  topic: "orders",
  queryId,
  ...(partitionKey === undefined ? {} : { partitionKey }),
  notify: () =>
    Effect.sync(() => {
      notifications.push(queryId);
    }),
  queuedEvents: Effect.succeed(0),
  end: Effect.void,
  closeWithStatus: () => Effect.void,
  maxQueueDepth: 0,
  backpressureEvents: 0,
  closed: false,
});

describe("Topic Store mutation partitions", () => {
  it.effect("records targeted changes only in the matching sparse partition journal", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      const query = topicStoreTestQueryInterface(store);
      query.retainChanges("partition:a");
      query.retainChanges("partition:b");

      yield* publishTopicStoreDecodedRowsWithStorageKeys(
        store,
        [{ storageKey: "a", row: order("a", "open", 10, 1, "a") }],
        invalidRow,
        "partition:a",
      );

      expect(query.changesSince(0, "partition:a")).toStrictEqual([
        {
          changes: [
            {
              key: "a",
              previous: undefined,
              next: order("a", "open", 10, 1, "a"),
            },
          ],
          version: 1,
        },
      ]);
      expect(query.changesSince(0, "partition:b")).toStrictEqual([]);

      yield* publishTopicStoreRow(store, order("shared", "open", 20, 2), invalidRow);

      expect(query.changesSince(1, "partition:a")).toStrictEqual([
        {
          changes: [
            {
              key: "shared",
              previous: undefined,
              next: order("shared", "open", 20, 2),
            },
          ],
          version: 2,
        },
      ]);
      expect(query.changesSince(0, "partition:b")).toStrictEqual([
        {
          changes: [
            {
              key: "shared",
              previous: undefined,
              next: order("shared", "open", 20, 2),
            },
          ],
          version: 2,
        },
      ]);

      yield* resetTopicStore(store);

      query.releaseChanges("partition:a");
      query.releaseChanges("partition:b");
      expect(query.changesSince(0, "partition:a")).toBeUndefined();
      expect(query.changesSince(0, "partition:b")).toBeUndefined();
    }),
  );

  it.effect("notifies only the targeted partition and unpartitioned subscribers", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      const notifications: Array<string> = [];
      yield* registerTestTopicStoreSubscriber(
        store,
        makeSubscriber("partition-a", notifications, "partition:a"),
      );
      yield* registerTestTopicStoreSubscriber(
        store,
        makeSubscriber("partition-b", notifications, "partition:b"),
      );
      yield* registerTestTopicStoreSubscriber(
        store,
        makeSubscriber("unpartitioned", notifications),
      );

      yield* publishTopicStoreDecodedRowsWithStorageKeys(
        store,
        [{ storageKey: "a", row: order("a", "open", 10, 1, "a") }],
        invalidRow,
        "partition:a",
      );
      expect(notifications).toStrictEqual(["unpartitioned", "partition-a"]);

      notifications.length = 0;
      yield* publishTopicStoreDecodedRowsWithStorageKeys(
        store,
        [{ storageKey: "b", row: order("b", "open", 20, 2, "b") }],
        invalidRow,
        "partition:b",
      );
      expect(notifications).toStrictEqual(["unpartitioned", "partition-b"]);

      notifications.length = 0;
      yield* publishTopicStoreRow(store, order("all", "open", 30, 3), invalidRow);
      expect(notifications).toStrictEqual(["partition-a", "partition-b", "unpartitioned"]);

      notifications.length = 0;
      yield* publishTopicStoreDecodedRowsWithStorageKeys(
        store,
        [{ storageKey: "a", row: order("a", "open", 10, 1, "a") }],
        invalidRow,
        "partition:a",
      );
      expect(notifications).toStrictEqual([]);
    }),
  );
});
