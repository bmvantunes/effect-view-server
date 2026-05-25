import type { FieldKey } from "./query-core";
import type { RejectExtraKeys } from "./query-exact";
import type { ExactWhere, Where } from "./query-filter";
import type { ExactRawOrderBy, OrderBy } from "./query-sort";

export type RawQuery<Row> = {
  readonly select: readonly [FieldKey<Row>, ...Array<FieldKey<Row>>];
  readonly where?: Where<Row>;
  readonly orderBy?: ReadonlyArray<OrderBy<Row>>;
  readonly offset?: number;
  readonly limit?: number;
};

export type ExactRawQuery<Row, Query> = Query &
  RejectExtraKeys<Query, RawQuery<Row>> & {
    readonly groupBy?: never;
    readonly aggregates?: never;
  } & ExactWhere<Row, Query> &
  ExactRawOrderBy<Row, Query>;

export type ExactPatch<Row, Patch> = Patch & RejectExtraKeys<Patch, Partial<Row>>;

export type PickRawFields<Row, Query> = Query extends {
  readonly select: ReadonlyArray<infer Field>;
}
  ? Pick<Row, Extract<Field, keyof Row>>
  : never;
