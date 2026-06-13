import type { TopicRowChange, TopicRowChangeBatch } from "./row-scan";

type RowObject = object;

const maxRowChangeJournalEntries = 65_536;
const maxRowChangeJournalVersions = 1_024;

export type TopicRowChangeJournalLimits = {
  readonly maxEntries?: number;
  readonly maxVersions?: number;
};

const positiveSafeIntegerOrDefault = (value: number | undefined, fallback: number): number => {
  if (value === undefined) {
    return fallback;
  }
  return Number.isSafeInteger(value) && value > 0 ? value : fallback;
};

export class TopicRowChangeJournal<Row extends RowObject> {
  private pendingChanges: Array<TopicRowChange<Row>> = [];
  private pendingChangesOverflowed = false;
  private readonly batches: Array<TopicRowChangeBatch<Row>> = [];
  private changeCount = 0;
  private invalidBeforeVersion = 0;
  private refs = 0;
  private readonly maxEntries: number;
  private readonly maxVersions: number;

  constructor(limits?: TopicRowChangeJournalLimits) {
    this.maxEntries = positiveSafeIntegerOrDefault(limits?.maxEntries, maxRowChangeJournalEntries);
    this.maxVersions = positiveSafeIntegerOrDefault(
      limits?.maxVersions,
      maxRowChangeJournalVersions,
    );
  }

  changesSince(
    version: number,
    currentVersion: number,
  ): ReadonlyArray<TopicRowChangeBatch<Row>> | undefined {
    if (version === currentVersion) {
      return [];
    }
    if (this.refs === 0 || version < 0 || version > currentVersion) {
      return undefined;
    }
    if (version < this.invalidBeforeVersion) {
      return undefined;
    }
    if (this.batches.length === 0) {
      return undefined;
    }
    const firstBatch = this.batches[0]!;
    if (version < firstBatch.version - 1) {
      return undefined;
    }
    const batches: Array<TopicRowChangeBatch<Row>> = [];
    for (const batch of this.batches) {
      if (batch.version > version) {
        batches.push(batch);
      }
    }
    return batches;
  }

  clear(currentVersion: number): void {
    this.pendingChanges = [];
    this.pendingChangesOverflowed = false;
    this.batches.length = 0;
    this.changeCount = 0;
    this.invalidBeforeVersion = currentVersion;
  }

  commit(currentVersion: number): void {
    if (this.refs === 0) {
      return;
    }
    if (this.pendingChangesOverflowed) {
      this.pendingChanges = [];
      this.pendingChangesOverflowed = false;
      return;
    }
    const changes = this.pendingChanges;
    this.batches.push({
      changes,
      version: currentVersion,
    });
    this.pendingChanges = [];
    this.changeCount += changes.length;
    this.trim(currentVersion);
  }

  record(change: TopicRowChange<Row>, currentVersion: number): void {
    if (this.refs === 0 || this.pendingChangesOverflowed) {
      return;
    }
    if (this.pendingChanges.length + 1 > this.maxEntries) {
      this.invalidate(currentVersion + 1);
      this.pendingChangesOverflowed = true;
      return;
    }
    this.pendingChanges.push(change);
  }

  release(currentVersion: number): void {
    this.refs = Math.max(0, this.refs - 1);
    if (this.refs === 0) {
      this.clear(currentVersion);
    }
  }

  retain(currentVersion: number): void {
    this.refs += 1;
    if (this.refs === 1) {
      this.clear(currentVersion);
    }
  }

  private invalidate(invalidBeforeVersion: number): void {
    this.clear(invalidBeforeVersion);
    this.invalidBeforeVersion = invalidBeforeVersion;
  }

  private trim(currentVersion: number): void {
    while (this.batches.length > this.maxVersions) {
      const removed = this.batches.shift()!;
      this.changeCount -= removed.changes.length;
    }
    if (this.changeCount > this.maxEntries) {
      this.invalidate(currentVersion);
    }
  }
}
