import type { TopicRowEntry } from "./row-scan";

type RowObject = object;

export type TopicRowStorageLifecycleColumn = {
  copySlot(targetSlot: number, sourceSlot: number): void;
  pop(): void;
};

export type TopicRowStorageLifecycle<Row extends RowObject> = {
  readonly addSlotToScalarIndexes: (slot: number) => void;
  readonly columns: () => Iterable<TopicRowStorageLifecycleColumn>;
  readonly insertSlotIntoOrderedIndexes: (slot: number) => void;
  readonly keyToSlot: Map<string, number>;
  readonly removeSlotFromOrderedIndexes: (slot: number) => void;
  readonly removeSlotFromScalarIndexes: (slot: number) => void;
  readonly slots: Array<TopicRowEntry<Row>>;
};

export type CompactingTopicRowDelete<Row extends RowObject> = {
  readonly moved:
    | {
        readonly fromSlot: number;
        readonly key: string;
        readonly toSlot: number;
      }
    | undefined;
  readonly previous: Row;
};

export const deleteCompactingTopicRowSlot = <Row extends RowObject>(
  lifecycle: TopicRowStorageLifecycle<Row>,
  key: string,
): CompactingTopicRowDelete<Row> | undefined => {
  const slot = lifecycle.keyToSlot.get(key);
  if (slot === undefined) {
    return undefined;
  }

  const lastSlot = lifecycle.slots.length - 1;
  const lastEntry = lifecycle.slots[lastSlot]!;
  const previous = lifecycle.slots[slot]!.row;
  lifecycle.removeSlotFromScalarIndexes(slot);
  lifecycle.removeSlotFromOrderedIndexes(slot);
  lifecycle.keyToSlot.delete(key);

  if (slot === lastSlot) {
    lifecycle.slots.pop();
    for (const column of lifecycle.columns()) {
      column.pop();
    }
    return {
      moved: undefined,
      previous,
    };
  }

  lifecycle.removeSlotFromScalarIndexes(lastSlot);
  lifecycle.removeSlotFromOrderedIndexes(lastSlot);
  lifecycle.slots[slot] = lastEntry;
  lifecycle.keyToSlot.set(lastEntry.key, slot);
  for (const column of lifecycle.columns()) {
    column.copySlot(slot, lastSlot);
  }
  lifecycle.addSlotToScalarIndexes(slot);
  lifecycle.insertSlotIntoOrderedIndexes(slot);
  lifecycle.slots.pop();
  for (const column of lifecycle.columns()) {
    column.pop();
  }
  return {
    moved: {
      fromSlot: lastSlot,
      key: lastEntry.key,
      toSlot: slot,
    },
    previous,
  };
};
