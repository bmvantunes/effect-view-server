type RowObject = object;

export type TopicRowEntry<Row extends RowObject> = {
  readonly key: string;
  readonly row: Row;
};

export type TopicRowVisitor<Row extends RowObject> = (key: string, row: Row) => false | void;

export type TopicRowChange<Row extends RowObject> = {
  readonly key: string;
  readonly next: Row | undefined;
  readonly previous: Row | undefined;
};

export type TopicRowChangeBatch<Row extends RowObject> = {
  readonly changes: ReadonlyArray<TopicRowChange<Row>>;
  readonly version: number;
};

export type TopicRowScan<Row extends RowObject> = {
  readonly changesSince: (version: number) => ReadonlyArray<TopicRowChangeBatch<Row>> | undefined;
  readonly scanRows: (visitor: TopicRowVisitor<Row>) => void;
  readonly version: () => number;
};
