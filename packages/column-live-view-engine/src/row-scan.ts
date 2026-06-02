type RowObject = object;

export type TopicRowVisitor<Row extends RowObject> = (key: string, row: Row) => void;

export type TopicRowScan<Row extends RowObject> = {
  readonly scanRows: (visitor: TopicRowVisitor<Row>) => void;
  readonly version: () => number;
};
