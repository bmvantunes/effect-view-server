import { describe, expect, it } from "@effect/vitest";
import { createTopicHealthLedger } from "./topic-health-ledger";

describe("column-live-view-engine topic health ledger", () => {
  it("tracks and guards subscription lifecycle totals", () => {
    const ledger = createTopicHealthLedger();

    const opened = { id: "opened" };
    const unknown = { id: "unknown" };

    expect(ledger.snapshot(0).activeSubscriptions).toBe(0);

    ledger.openSubscription(opened);
    ledger.openSubscription(opened);
    expect(ledger.snapshot(0)).toStrictEqual({
      activeSubscriptions: 1,
      queuedEvents: 0,
      maxQueueDepth: 0,
      backpressureEvents: 0,
      rowCount: 0,
      version: 0,
      lastMutationAt: null,
      mutationsPerSecond: 0,
      rowsPerSecond: 0,
      pendingMutationBatches: 0,
    });

    ledger.updateQueueDepth(unknown, 3);
    expect(ledger.snapshot(0).queuedEvents).toBe(0);

    ledger.markBackpressure(unknown);
    expect(ledger.snapshot(0).backpressureEvents).toBe(0);

    ledger.updateQueueDepth(opened, 2);
    ledger.markBackpressure(opened);

    expect(ledger.snapshot(0)).toStrictEqual({
      activeSubscriptions: 1,
      queuedEvents: 2,
      maxQueueDepth: 2,
      backpressureEvents: 1,
      rowCount: 0,
      version: 0,
      lastMutationAt: null,
      mutationsPerSecond: 0,
      rowsPerSecond: 0,
      pendingMutationBatches: 0,
    });

    ledger.updateQueueDepth(opened, 5);
    expect(ledger.snapshot(0)).toStrictEqual({
      activeSubscriptions: 1,
      queuedEvents: 5,
      maxQueueDepth: 5,
      backpressureEvents: 1,
      rowCount: 0,
      version: 0,
      lastMutationAt: null,
      mutationsPerSecond: 0,
      rowsPerSecond: 0,
      pendingMutationBatches: 0,
    });

    ledger.closeSubscription(opened);
    expect(ledger.snapshot(0)).toStrictEqual({
      activeSubscriptions: 0,
      queuedEvents: 0,
      maxQueueDepth: 5,
      backpressureEvents: 1,
      rowCount: 0,
      version: 0,
      lastMutationAt: null,
      mutationsPerSecond: 0,
      rowsPerSecond: 0,
      pendingMutationBatches: 0,
    });

    ledger.closeSubscription(opened);
    expect(ledger.snapshot(0)).toStrictEqual({
      activeSubscriptions: 0,
      queuedEvents: 0,
      maxQueueDepth: 5,
      backpressureEvents: 1,
      rowCount: 0,
      version: 0,
      lastMutationAt: null,
      mutationsPerSecond: 0,
      rowsPerSecond: 0,
      pendingMutationBatches: 0,
    });

    ledger.reset();
    expect(ledger.snapshot(0)).toStrictEqual({
      activeSubscriptions: 0,
      queuedEvents: 0,
      maxQueueDepth: 0,
      backpressureEvents: 0,
      rowCount: 0,
      version: 0,
      lastMutationAt: null,
      mutationsPerSecond: 0,
      rowsPerSecond: 0,
      pendingMutationBatches: 0,
    });
  });

  it("tracks mutation rates and pending batches", () => {
    const ledger = createTopicHealthLedger();

    ledger.beginMutationBatch();
    ledger.beginMutationBatch();
    expect(ledger.snapshot(1_000).pendingMutationBatches).toBe(2);

    ledger.recordMutation({
      version: 1,
      rowCount: 2,
      rowsChanged: 2,
      occurredAt: 1_000,
    });
    ledger.recordMutation({
      version: 2,
      rowCount: 3,
      rowsChanged: 1,
      occurredAt: 1_500,
    });
    ledger.recordMutation({
      version: 3,
      rowCount: 4,
      rowsChanged: 4,
      occurredAt: 1_500,
    });
    ledger.endMutationBatch();

    expect(ledger.snapshot(1_500)).toStrictEqual({
      activeSubscriptions: 0,
      queuedEvents: 0,
      maxQueueDepth: 0,
      backpressureEvents: 0,
      rowCount: 4,
      version: 3,
      lastMutationAt: 1_500,
      mutationsPerSecond: 3,
      rowsPerSecond: 7,
      pendingMutationBatches: 1,
    });

    expect(ledger.snapshot(2_501)).toStrictEqual({
      activeSubscriptions: 0,
      queuedEvents: 0,
      maxQueueDepth: 0,
      backpressureEvents: 0,
      rowCount: 4,
      version: 3,
      lastMutationAt: 1_500,
      mutationsPerSecond: 0,
      rowsPerSecond: 0,
      pendingMutationBatches: 1,
    });

    ledger.recordMutation({
      version: 4,
      rowCount: 1,
      rowsChanged: -10,
      occurredAt: 2_501,
    });
    ledger.endMutationBatch();
    ledger.endMutationBatch();

    expect(ledger.snapshot(2_501)).toStrictEqual({
      activeSubscriptions: 0,
      queuedEvents: 0,
      maxQueueDepth: 0,
      backpressureEvents: 0,
      rowCount: 1,
      version: 4,
      lastMutationAt: 2_501,
      mutationsPerSecond: 1,
      rowsPerSecond: 0,
      pendingMutationBatches: 0,
    });
  });

  it("bounds sparse mutation rate buckets without health reads", () => {
    const ledger = createTopicHealthLedger();

    for (let mutationIndex = 0; mutationIndex < 1_200; mutationIndex += 1) {
      ledger.recordMutation({
        version: mutationIndex + 1,
        rowCount: mutationIndex + 1,
        rowsChanged: 1,
        occurredAt: mutationIndex * 2,
      });
    }

    expect(ledger.snapshot(2_398)).toStrictEqual({
      activeSubscriptions: 0,
      queuedEvents: 0,
      maxQueueDepth: 0,
      backpressureEvents: 0,
      rowCount: 1_200,
      version: 1_200,
      lastMutationAt: 2_398,
      mutationsPerSecond: 501,
      rowsPerSecond: 501,
      pendingMutationBatches: 0,
    });
  });

  it("ignores future buckets when the clock moves backward", () => {
    const ledger = createTopicHealthLedger();

    ledger.recordMutation({
      version: 1,
      rowCount: 1,
      rowsChanged: 1,
      occurredAt: 5_000,
    });
    ledger.recordMutation({
      version: 2,
      rowCount: 2,
      rowsChanged: 1,
      occurredAt: 4_000,
    });

    expect(ledger.snapshot(4_000)).toStrictEqual({
      activeSubscriptions: 0,
      queuedEvents: 0,
      maxQueueDepth: 0,
      backpressureEvents: 0,
      rowCount: 2,
      version: 2,
      lastMutationAt: 4_000,
      mutationsPerSecond: 1,
      rowsPerSecond: 1,
      pendingMutationBatches: 0,
    });
  });
});
