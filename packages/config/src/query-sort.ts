import type { FieldKey, SortDirection } from "./query-core";
import type { PresentPropertyValue, RejectArrayExtraKeys, RejectExtraKeys } from "./query-exact";

export type OrderByField<Field extends string> = {
  readonly field: Field;
  readonly aggregate?: never;
  readonly direction: SortDirection;
};

export type OrderBy<Row> = OrderByField<FieldKey<Row>>;

export type AggregateOrderByField<Alias extends string = string> = {
  readonly aggregate: Alias;
  readonly field?: never;
  readonly direction: SortDirection;
};

export type GroupedOrderBy<Row> = OrderByField<FieldKey<Row>> | AggregateOrderByField;

type ExactOrderByEntry<Entry, Field extends string> = Entry &
  RejectExtraKeys<Entry, OrderByField<Field>> &
  (Entry extends { readonly field: infer QueryField }
    ? { readonly field: QueryField & Field }
    : { readonly field: Field }) &
  (Entry extends { readonly aggregate: unknown } ? { readonly aggregate: never } : unknown) &
  (Entry extends { readonly direction: infer QueryDirection }
    ? { readonly direction: QueryDirection & SortDirection }
    : { readonly direction: SortDirection });

export type ExactRawOrderBy<Row, Query> = "orderBy" extends keyof Query
  ? PresentPropertyValue<Query, "orderBy"> extends infer Candidate
    ? Candidate extends ReadonlyArray<infer Entry>
      ? {
          readonly orderBy?: Candidate &
            RejectArrayExtraKeys<Candidate> &
            ReadonlyArray<ExactOrderByEntry<Entry, FieldKey<Row>>>;
        }
      : { readonly orderBy?: never }
    : never
  : unknown;

export type ExactGroupedOrderByEntry<Entry, Field extends string, Alias extends string> =
  | (Entry &
      RejectExtraKeys<Entry, OrderByField<Field>> &
      (Entry extends { readonly field: infer QueryField }
        ? { readonly field: QueryField & Field }
        : { readonly field: Field }) &
      (Entry extends { readonly aggregate: unknown } ? { readonly aggregate: never } : unknown) &
      (Entry extends { readonly direction: infer QueryDirection }
        ? { readonly direction: QueryDirection & SortDirection }
        : { readonly direction: SortDirection }))
  | (Entry &
      RejectExtraKeys<Entry, AggregateOrderByField<Alias>> &
      (Entry extends { readonly aggregate: infer QueryAggregate }
        ? { readonly aggregate: QueryAggregate & Alias }
        : { readonly aggregate: Alias }) &
      (Entry extends { readonly field: unknown } ? { readonly field: never } : unknown) &
      (Entry extends { readonly direction: infer QueryDirection }
        ? { readonly direction: QueryDirection & SortDirection }
        : { readonly direction: SortDirection }));
