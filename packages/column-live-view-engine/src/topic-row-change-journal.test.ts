import { describe, expect, it } from "@effect/vitest";
import { TopicRowChangeJournal } from "./topic-row-change-journal";

const row = (id: string): object => ({ id });

describe("column-live-view-engine topic row change journal", () => {
  it("normalizes invalid retained limit overrides", () => {
    const journal = new TopicRowChangeJournal<object>({
      maxEntries: Number.NaN,
      maxVersions: 0,
    });

    journal.retain(0);
    journal.record({ key: "first", previous: undefined, next: row("first") }, 0);
    journal.commit(1);

    expect(journal.changesSince(0, 1)).toStrictEqual([
      {
        changes: [{ key: "first", previous: undefined, next: row("first") }],
        version: 1,
      },
    ]);
  });

  it("uses positive retained limit overrides", () => {
    const journal = new TopicRowChangeJournal<object>({
      maxEntries: 1,
      maxVersions: 1,
    });

    journal.retain(0);
    journal.record({ key: "first", previous: undefined, next: row("first") }, 0);
    journal.record({ key: "second", previous: undefined, next: row("second") }, 0);
    journal.commit(1);
    expect(journal.changesSince(0, 1)).toBeUndefined();

    journal.record({ key: "third", previous: undefined, next: row("third") }, 1);
    journal.commit(2);
    journal.record({ key: "fourth", previous: undefined, next: row("fourth") }, 2);
    journal.commit(3);

    expect(journal.changesSince(1, 3)).toBeUndefined();
    expect(journal.changesSince(2, 3)).toStrictEqual([
      {
        changes: [{ key: "fourth", previous: undefined, next: row("fourth") }],
        version: 3,
      },
    ]);
  });

  it("uses default retained limits and reports missing history after clear", () => {
    const journal = new TopicRowChangeJournal<object>();

    journal.retain(0);
    expect(journal.changesSince(0, 1)).toBeUndefined();

    journal.clear(1);

    expect(journal.changesSince(0, 1)).toBeUndefined();
  });

  it("retains sparse partition history across unrelated global versions", () => {
    const journal = new TopicRowChangeJournal<object>(undefined, true);

    journal.retain(0);

    expect(journal.changesSince(0, 2)).toStrictEqual([]);

    journal.record({ key: "owned", previous: undefined, next: row("owned") }, 2);
    journal.commit(3);

    expect(journal.changesSince(0, 3)).toStrictEqual([
      {
        changes: [{ key: "owned", previous: undefined, next: row("owned") }],
        version: 3,
      },
    ]);
  });

  it("invalidates trimmed sparse partition history without requiring contiguous versions", () => {
    const journal = new TopicRowChangeJournal<object>({ maxVersions: 1 }, true);

    journal.retain(0);
    journal.record({ key: "first", previous: undefined, next: row("first") }, 0);
    journal.commit(1);
    journal.record({ key: "third", previous: undefined, next: row("third") }, 2);
    journal.commit(3);

    expect(journal.changesSince(0, 3)).toBeUndefined();
    expect(journal.changesSince(1, 3)).toStrictEqual([
      {
        changes: [{ key: "third", previous: undefined, next: row("third") }],
        version: 3,
      },
    ]);
  });

  it("keeps retained sparse history until its final consumer releases it", () => {
    const journal = new TopicRowChangeJournal<object>(undefined, true);

    journal.retain(0);
    journal.retain(0);
    journal.record({ key: "owned", previous: undefined, next: row("owned") }, 0);
    journal.commit(1);

    expect(journal.release(1)).toBe(false);
    expect(journal.changesSince(0, 1)).toStrictEqual([
      {
        changes: [{ key: "owned", previous: undefined, next: row("owned") }],
        version: 1,
      },
    ]);
    expect(journal.release(1)).toBe(true);
    expect(journal.changesSince(0, 1)).toBeUndefined();
  });
});
