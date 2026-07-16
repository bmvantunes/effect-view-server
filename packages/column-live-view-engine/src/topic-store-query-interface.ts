import type { TopicRawWindowScan } from "./raw-window-scan";
import type { TopicRowScan } from "./row-scan";

type RowObject = object;

export type TopicStoreQueryInterface<Row extends RowObject = RowObject> = TopicRowScan<Row> &
  TopicRawWindowScan<Row> & {
    readonly releaseChanges: () => void;
    readonly retainChanges: () => void;
    readonly topic: string;
  };
