export type TopicHealthSubscription = {
  queuedEvents: number;
  maxQueueDepth: number;
  backpressureEvents: number;
};

type TopicHealthTotals = {
  activeSubscriptions: number;
  queuedEvents: number;
  maxQueueDepth: number;
  backpressureEvents: number;
  rowCount: number;
  version: number;
  lastMutationAt: number | null;
  mutationsPerSecond: number;
  rowsPerSecond: number;
  pendingMutationBatches: number;
};

type TopicHealthLedger = {
  readonly beginMutationBatch: () => void;
  readonly endMutationBatch: () => void;
  readonly recordMutation: (input: {
    readonly version: number;
    readonly rowCount: number;
    readonly rowsChanged: number;
    readonly occurredAt: number;
  }) => void;
  readonly openSubscription: (subscription: LiveTopicSubscriber) => void;
  readonly closeSubscription: (subscription: LiveTopicSubscriber) => void;
  readonly updateQueueDepth: (subscription: LiveTopicSubscriber, queueDepth: number) => void;
  readonly markBackpressure: (subscription: LiveTopicSubscriber) => void;
  readonly reset: () => void;
  readonly snapshot: (now: number) => TopicHealthTotals;
};

type MutationRateBucket = {
  occurredAt: number;
  mutations: number;
  rowsChanged: number;
};

const mutationRateWindowMillis = 1_000;
const maxMutationRateBuckets = mutationRateWindowMillis + 1;

export const createTopicHealthLedger = (): TopicHealthLedger => {
  const subscriptions = new Map<LiveTopicSubscriber, TopicHealthSubscription>();
  const mutationRateBuckets = Array.from(
    { length: maxMutationRateBuckets },
    (): MutationRateBucket | undefined => undefined,
  );
  let activeSubscriptions = 0;
  let queuedEvents = 0;
  let maxQueueDepth = 0;
  let backpressureEvents = 0;
  let rowCount = 0;
  let version = 0;
  let lastMutationAt: number | null = null;
  let pendingMutationBatches = 0;

  const ensureSubscription = (
    subscription: LiveTopicSubscriber,
  ): TopicHealthSubscription | undefined => subscriptions.get(subscription);

  const mutationRateBucketIndex = (occurredAt: number): number => {
    return Math.abs(occurredAt % maxMutationRateBuckets);
  };

  const beginMutationBatch = (): void => {
    pendingMutationBatches += 1;
  };

  const endMutationBatch = (): void => {
    pendingMutationBatches = Math.max(0, pendingMutationBatches - 1);
  };

  const recordMutation = (input: {
    readonly version: number;
    readonly rowCount: number;
    readonly rowsChanged: number;
    readonly occurredAt: number;
  }): void => {
    const occurredAt = Math.trunc(input.occurredAt);
    rowCount = input.rowCount;
    version = input.version;
    lastMutationAt = occurredAt;
    const bucketIndex = mutationRateBucketIndex(occurredAt);
    const existingBucket = mutationRateBuckets[bucketIndex];
    if (existingBucket !== undefined && existingBucket.occurredAt === occurredAt) {
      existingBucket.mutations += 1;
      existingBucket.rowsChanged += Math.max(0, input.rowsChanged);
      return;
    }
    mutationRateBuckets[bucketIndex] = {
      occurredAt,
      mutations: 1,
      rowsChanged: Math.max(0, input.rowsChanged),
    };
  };

  const openSubscription = (subscription: LiveTopicSubscriber): void => {
    subscriptions.set(subscription, {
      queuedEvents: 0,
      maxQueueDepth: 0,
      backpressureEvents: 0,
    });
    activeSubscriptions += 1;
  };

  const closeSubscription = (subscription: LiveTopicSubscriber): void => {
    const tracked = ensureSubscription(subscription);
    if (tracked === undefined) {
      return;
    }
    subscriptions.delete(subscription);
    activeSubscriptions -= 1;
    queuedEvents = Math.max(0, queuedEvents - tracked.queuedEvents);
  };

  const updateQueueDepth = (subscription: LiveTopicSubscriber, nextDepth: number): void => {
    const tracked = ensureSubscription(subscription);
    if (tracked === undefined) {
      return;
    }
    queuedEvents -= tracked.queuedEvents;
    queuedEvents += nextDepth;

    tracked.queuedEvents = nextDepth;
    if (nextDepth > tracked.maxQueueDepth) {
      tracked.maxQueueDepth = nextDepth;
      maxQueueDepth = Math.max(maxQueueDepth, nextDepth);
    }
  };

  const markBackpressure = (subscription: LiveTopicSubscriber): void => {
    const tracked = ensureSubscription(subscription);
    if (tracked === undefined) {
      return;
    }
    tracked.backpressureEvents += 1;
    backpressureEvents += 1;
  };

  const reset = (): void => {
    subscriptions.clear();
    mutationRateBuckets.fill(undefined);
    activeSubscriptions = 0;
    queuedEvents = 0;
    maxQueueDepth = 0;
    backpressureEvents = 0;
    rowCount = 0;
    version = 0;
    lastMutationAt = null;
    pendingMutationBatches = 0;
  };

  const snapshot = (now: number): TopicHealthTotals => {
    const occurredBefore = Math.trunc(now) - mutationRateWindowMillis;
    const occurredAfter = Math.trunc(now);
    let mutationsPerSecond = 0;
    let rowsPerSecond = 0;
    for (const mutationRateBucket of mutationRateBuckets) {
      if (
        mutationRateBucket === undefined ||
        mutationRateBucket.occurredAt < occurredBefore ||
        mutationRateBucket.occurredAt > occurredAfter
      ) {
        continue;
      }
      mutationsPerSecond += mutationRateBucket.mutations;
      rowsPerSecond += mutationRateBucket.rowsChanged;
    }
    return {
      activeSubscriptions,
      queuedEvents: Math.max(0, queuedEvents),
      maxQueueDepth,
      backpressureEvents,
      rowCount,
      version,
      lastMutationAt,
      mutationsPerSecond,
      rowsPerSecond,
      pendingMutationBatches,
    };
  };

  return {
    beginMutationBatch,
    endMutationBatch,
    recordMutation,
    openSubscription: (subscription: LiveTopicSubscriber): void => {
      if (subscriptions.has(subscription)) {
        return;
      }
      openSubscription(subscription);
    },
    closeSubscription: (subscription: LiveTopicSubscriber): void => {
      closeSubscription(subscription);
    },
    updateQueueDepth: (subscription: LiveTopicSubscriber, queueDepth: number): void => {
      updateQueueDepth(subscription, queueDepth);
    },
    markBackpressure: (subscription: LiveTopicSubscriber): void => {
      markBackpressure(subscription);
    },
    reset,
    snapshot,
  };
};
import type { LiveTopicSubscriber } from "./topic-subscriber";
