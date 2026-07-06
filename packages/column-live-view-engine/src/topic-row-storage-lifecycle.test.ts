import { describe, expect, it } from "@effect/vitest";
import type { TopicRowEntry } from "./row-scan";
import { deleteCompactingTopicRowSlot } from "./topic-row-storage-lifecycle";

type Row = {
  readonly id: string;
};

type TestColumn = {
  readonly values: Array<string>;
  copySlot(targetSlot: number, sourceSlot: number): void;
  pop(): void;
};

const row = (id: string): Row => ({ id });

const entries = (...ids: ReadonlyArray<string>): Array<TopicRowEntry<Row>> =>
  ids.map((id) => ({
    key: id,
    row: row(id),
  }));

const keySlots = (...ids: ReadonlyArray<string>): Map<string, number> =>
  new Map(ids.map((id, slot) => [id, slot]));

const testColumn = (
  values: ReadonlyArray<string>,
  events: Array<string>,
  name: string,
): TestColumn => ({
  values: [...values],
  copySlot(targetSlot, sourceSlot) {
    events.push(`${name}:copy:${targetSlot}:${sourceSlot}`);
    this.values[targetSlot] = this.values[sourceSlot]!;
  },
  pop() {
    events.push(`${name}:pop`);
    this.values.pop();
  },
});

const testColumnMap = (...columns: ReadonlyArray<TestColumn>): Map<string, TestColumn> =>
  new Map(columns.map((column, index) => [`column-${index}`, column]));

describe("column-live-view-engine topic row storage lifecycle", () => {
  it("keeps storage unchanged for unknown delete keys", () => {
    const events: Array<string> = [];
    const slots = entries("a", "b");
    const keyToSlot = keySlots("a", "b");
    const column = testColumn(["A", "B"], events, "column");

    expect(
      deleteCompactingTopicRowSlot(
        {
          addSlotToScalarIndexes: (slot) => events.push(`addScalar:${slot}`),
          columns: () => testColumnMap(column).values(),
          insertSlotIntoOrderedIndexes: (slot) => events.push(`insertOrdered:${slot}`),
          keyToSlot,
          removeSlotFromOrderedIndexes: (slot) => events.push(`removeOrdered:${slot}`),
          removeSlotFromScalarIndexes: (slot) => events.push(`removeScalar:${slot}`),
          slots,
        },
        "missing",
      ),
    ).toBeUndefined();

    expect({
      columnValues: column.values,
      events,
      keyToSlot,
      slots,
    }).toStrictEqual({
      columnValues: ["A", "B"],
      events: [],
      keyToSlot: keySlots("a", "b"),
      slots: entries("a", "b"),
    });
  });

  it("deletes the last row without moving another slot", () => {
    const events: Array<string> = [];
    const slots = entries("a", "b");
    const keyToSlot = keySlots("a", "b");
    const column = testColumn(["A", "B"], events, "column");

    expect(
      deleteCompactingTopicRowSlot(
        {
          addSlotToScalarIndexes: (slot) => events.push(`addScalar:${slot}`),
          columns: () => testColumnMap(column).values(),
          insertSlotIntoOrderedIndexes: (slot) => events.push(`insertOrdered:${slot}`),
          keyToSlot,
          removeSlotFromOrderedIndexes: (slot) => events.push(`removeOrdered:${slot}`),
          removeSlotFromScalarIndexes: (slot) => events.push(`removeScalar:${slot}`),
          slots,
        },
        "b",
      ),
    ).toStrictEqual({
      moved: undefined,
      previous: row("b"),
    });

    expect({
      columnValues: column.values,
      events,
      keyToSlot,
      slots,
    }).toStrictEqual({
      columnValues: ["A"],
      events: ["removeScalar:1", "removeOrdered:1", "column:pop"],
      keyToSlot: keySlots("a"),
      slots: entries("a"),
    });
  });

  it("deletes a middle row by compacting the final slot and repairing indexes", () => {
    const events: Array<string> = [];
    const slots = entries("a", "b", "c");
    const keyToSlot = keySlots("a", "b", "c");
    const column = testColumn(["A", "B", "C"], events, "column");

    expect(
      deleteCompactingTopicRowSlot(
        {
          addSlotToScalarIndexes: (slot) => events.push(`addScalar:${slot}`),
          columns: () => testColumnMap(column).values(),
          insertSlotIntoOrderedIndexes: (slot) => events.push(`insertOrdered:${slot}`),
          keyToSlot,
          removeSlotFromOrderedIndexes: (slot) => events.push(`removeOrdered:${slot}`),
          removeSlotFromScalarIndexes: (slot) => events.push(`removeScalar:${slot}`),
          slots,
        },
        "b",
      ),
    ).toStrictEqual({
      moved: {
        fromSlot: 2,
        key: "c",
        toSlot: 1,
      },
      previous: row("b"),
    });

    expect({
      columnValues: column.values,
      events,
      keyToSlot,
      slots,
    }).toStrictEqual({
      columnValues: ["A", "C"],
      events: [
        "removeScalar:1",
        "removeOrdered:1",
        "removeScalar:2",
        "removeOrdered:2",
        "column:copy:1:2",
        "addScalar:1",
        "insertOrdered:1",
        "column:pop",
      ],
      keyToSlot: keySlots("a", "c"),
      slots: entries("a", "c"),
    });
  });
});
