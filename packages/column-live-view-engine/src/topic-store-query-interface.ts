import type { TopicRawWindowScan } from "./raw-window-scan";
import type { TopicRowScan, TopicRowVisitor } from "./row-scan";

type RowObject = object;

export type TopicStoreQueryInterface<Row extends RowObject = RowObject> = TopicRowScan<Row> &
  TopicRawWindowScan<Row> & {
    readonly releaseChanges: (partitionKey?: string) => void;
    readonly retainChanges: (partitionKey?: string) => void;
    readonly scanRowsByStorageKeys: (
      storageKeys: Iterable<string>,
      visitor: TopicRowVisitor<Row>,
    ) => void;
    readonly topic: string;
  };
