import type {
  AggregateAliasesFromAggregates,
  AggregateResultValue,
  Aggregates,
  AverageAggregate,
  ComparableAggregate,
  CountAggregate,
  CountDistinctAggregate,
  SumAggregate,
} from "./query-aggregate";
import type { FieldKey, PickTupleFields, Simplify } from "./query-core";
import type { RejectExtraKeys } from "./query-exact";
import type { ExactWhere, Where } from "./query-filter";
import type { ExactGroupedOrderByEntry, GroupedOrderBy } from "./query-sort";

export type GroupedQuery<Row> = {
  readonly groupBy: readonly [FieldKey<Row>, ...Array<FieldKey<Row>>];
  readonly aggregates: Aggregates<Row>;
  readonly select?: never;
  readonly where?: Where<Row>;
  readonly orderBy?: ReadonlyArray<GroupedOrderBy<Row>>;
  readonly offset?: number;
  readonly limit?: number;
};

type NonEmptyFieldTuple<Row, Tuple> = Tuple extends readonly [unknown, ...Array<unknown>]
  ? { readonly [Index in keyof Tuple]: Tuple[Index] & FieldKey<Row> }
  : never;

type ExactAggregate<Row, Candidate> = Candidate extends {
  readonly aggFunc: "count";
}
  ? Candidate & CountAggregate & RejectExtraKeys<Candidate, CountAggregate>
  : Candidate extends { readonly aggFunc: "countDistinct" }
    ? Candidate &
        CountDistinctAggregate<Row> &
        RejectExtraKeys<Candidate, CountDistinctAggregate<Row>>
    : Candidate extends { readonly aggFunc: "sum" }
      ? Candidate & SumAggregate<Row> & RejectExtraKeys<Candidate, SumAggregate<Row>>
      : Candidate extends { readonly aggFunc: "avg" }
        ? Candidate & AverageAggregate<Row> & RejectExtraKeys<Candidate, AverageAggregate<Row>>
        : Candidate extends { readonly aggFunc: "min" | "max" }
          ? Candidate &
              ComparableAggregate<Row> &
              RejectExtraKeys<Candidate, ComparableAggregate<Row>>
          : never;

type ExactAggregates<Row, Candidate> = {
  readonly [Alias in keyof Candidate]: ExactAggregate<Row, Candidate[Alias]>;
};

type GroupedOrderByField<Row, GroupBy> = Extract<
  GroupBy extends ReadonlyArray<infer Field> ? Field : never,
  FieldKey<Row>
>;

type ExactGroupedOrderBy<Row, Query> = Query extends {
  readonly orderBy: ReadonlyArray<infer Entry>;
  readonly groupBy: infer GroupBy;
  readonly aggregates: infer Aggregates;
}
  ? {
      readonly orderBy: ReadonlyArray<
        ExactGroupedOrderByEntry<
          Entry,
          GroupedOrderByField<Row, GroupBy>,
          AggregateAliasesFromAggregates<Aggregates>
        >
      >;
    }
  : unknown;

export type ExactGroupedQuery<Row, Query> = Query &
  RejectExtraKeys<Query, GroupedQuery<Row>> & {
    readonly select?: never;
  } & ExactWhere<Row, Query> &
  ExactGroupedOrderBy<Row, Query> &
  (Query extends {
    readonly groupBy: infer GroupBy;
    readonly aggregates: infer Aggregates;
  }
    ? {
        readonly groupBy: NonEmptyFieldTuple<Row, GroupBy>;
        readonly aggregates: ExactAggregates<Row, Aggregates>;
      }
    : {
        readonly groupBy: readonly [FieldKey<Row>, ...Array<FieldKey<Row>>];
        readonly aggregates: Aggregates<Row>;
      });

type GroupedAggregateResultFields<Row, AggregateSet> = AggregateSet extends unknown
  ? {
      readonly [Alias in keyof AggregateSet]: AggregateResultValue<Row, AggregateSet[Alias]>;
    }
  : never;

export type GroupedResult<Row, Query> = Query extends {
  readonly groupBy: infer GroupBy extends ReadonlyArray<unknown>;
  readonly aggregates: infer Aggs;
}
  ? Simplify<PickTupleFields<Row, GroupBy> & GroupedAggregateResultFields<Row, Aggs>>
  : never;
