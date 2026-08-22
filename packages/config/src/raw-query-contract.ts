import type { CompleteRawSelectWitnessRow, FieldKey, PickTupleFields } from "./query-core";
import type { ExactQueryWindow, RejectArrayExtraKeys, RejectExtraKeys } from "./query-exact";
import type { ExactWhere, Where } from "./query-filter";
import type { ExactRawOrderBy, OrderBy } from "./query-sort";

export type RawSelect<Row> = ReadonlyArray<FieldKey<Row>>;

type RejectBroadSelect<Select> =
  Select extends ReadonlyArray<unknown>
    ? number extends Select["length"]
      ? never
      : unknown
    : never;

type RejectEmptySelect<Select> = Select extends readonly [] ? never : unknown;

type ExactRawSelectField<Row, Field> = [Field] extends [FieldKey<Row>] ? Field : never;

type ExactRawSelectFields<Row, Select> =
  Select extends ReadonlyArray<unknown>
    ? {
        readonly [Index in keyof Select]: ExactRawSelectField<Row, Select[Index]>;
      }
    : never;

type CompleteRawSelectWitnessKey = "__effect-view-server/LiveQueryViewportCompleteRawSelect@v1";

type ExactCompleteRawSelectFields<Row, Select extends ReadonlyArray<unknown>> = [
  Select[number],
] extends [FieldKey<Row>]
  ? unknown
  : never;

type IsExact<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;

type ExactRawSelect<Row, Query> = Query extends {
  readonly select: infer Select;
}
  ? Select extends ReadonlyArray<unknown>
    ? IsExact<CompleteRawSelectWitnessRow<Select>, Row> extends true
      ? {
          readonly select: Select &
            RejectArrayExtraKeys<Select, CompleteRawSelectWitnessKey> &
            ExactCompleteRawSelectFields<Row, Select>;
        }
      : {
          readonly select: Select &
            RejectArrayExtraKeys<Select> &
            RejectBroadSelect<Select> &
            RejectEmptySelect<Select> &
            ExactRawSelectFields<Row, Select>;
        }
    : { readonly select: never }
  : {
      readonly select: RawSelect<Row>;
    };

export type RawQuery<Row> = {
  readonly select: RawSelect<Row>;
  readonly where?: Where<Row>;
  readonly orderBy?: ReadonlyArray<OrderBy<Row>>;
  readonly offset?: number;
  readonly limit?: number;
};

type ExactRawQueryMember<Row, Query> = Query &
  RejectExtraKeys<Query, RawQuery<Row>> & {
    readonly groupBy?: never;
    readonly aggregates?: never;
  } & ExactRawSelect<Row, Query> &
  ExactWhere<Row, Query> &
  ExactRawOrderBy<Row, Query> &
  ExactQueryWindow<Query>;

type InvalidExactRawQueryMember<Row, Query> = Query extends unknown
  ? Query extends ExactRawQueryMember<Row, Query>
    ? never
    : Query
  : never;

type ExactRawQueryMembers<Row, Query> = Query extends unknown
  ? ExactRawQueryMember<Row, Query>
  : never;

export type ExactRawQuery<Row, Query> = [InvalidExactRawQueryMember<Row, Query>] extends [never]
  ? ExactRawQueryMembers<Row, Query>
  : never;

export type ExactPatch<Row, Patch> = Patch & RejectExtraKeys<Patch, Partial<Row>>;

export type PickRawFields<Row, Query> = Query extends {
  readonly select: infer Select extends ReadonlyArray<unknown>;
}
  ? IsExact<CompleteRawSelectWitnessRow<Select>, Row> extends true
    ? Row
    : PickTupleFields<Row, Select>
  : never;
