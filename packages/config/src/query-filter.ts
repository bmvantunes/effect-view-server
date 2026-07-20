import type * as BigDecimal from "effect/BigDecimal";
import type { ValidateExactArray } from "./query-exact";

export type FilterableScalar = string | number | bigint | boolean | BigDecimal.BigDecimal | null;

type IsAny<Value> = 0 extends 1 & Value ? true : false;
type Defined<Value> = Exclude<Value, undefined>;
type ScalarBranches<Value> =
  IsAny<Value> extends true ? never : Extract<Defined<Value>, FilterableScalar>;
type StringBranches<Value> = IsAny<Value> extends true ? never : Extract<Defined<Value>, string>;
type NumberBranches<Value> = IsAny<Value> extends true ? never : Extract<Defined<Value>, number>;
type BigIntBranches<Value> = IsAny<Value> extends true ? never : Extract<Defined<Value>, bigint>;
type BigDecimalBranches<Value> =
  IsAny<Value> extends true ? never : Extract<Defined<Value>, BigDecimal.BigDecimal>;

type IsRouteFieldValue<Value> =
  IsAny<Value> extends true
    ? false
    : [Defined<Value>] extends [never]
      ? false
      : [Defined<Value>] extends [FilterableScalar]
        ? true
        : false;

export type RouteFieldKey<Row> = Extract<
  {
    readonly [Key in keyof Row]-?: IsRouteFieldValue<Row[Key]> extends true ? Key : never;
  }[keyof Row],
  string
>;

export type RouteFieldValue<Row, Field extends RouteFieldKey<Row>> = Defined<Row[Field]>;

type TraversableObject<Value> =
  IsAny<Value> extends true
    ? never
    : Value extends FilterableScalar
      ? never
      : Value extends ReadonlyArray<unknown>
        ? never
        : Value extends ReadonlyMap<unknown, unknown> | ReadonlySet<unknown>
          ? never
          : Value extends (...args: ReadonlyArray<never>) => unknown
            ? never
            : Value extends object
              ? string extends keyof Value
                ? never
                : Value
              : never;

type StringKey<Value> = Value extends unknown ? Extract<keyof Value, string> : never;
type WithoutReservedDot<Key> = Key extends string
  ? Key extends `${string}.${string}`
    ? never
    : Key
  : never;
type FilterableObjectKey<Value> = WithoutReservedDot<StringKey<Value>>;
type ValueAtKey<Value, Key extends string> = Value extends unknown
  ? Key extends keyof Value
    ? Value[Key]
    : undefined
  : never;

type IsExactly<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;

type SeenContains<Seen extends ReadonlyArray<unknown>, Candidate> = Seen extends readonly [
  infer Head,
  ...infer Tail,
]
  ? IsExactly<Head, Candidate> extends true
    ? true
    : SeenContains<Tail, Candidate>
  : false;

type NestedFilterableFieldPath<Key extends string, Nested, Seen extends ReadonlyArray<unknown>> = [
  Nested,
] extends [never]
  ? never
  : SeenContains<Seen, Nested> extends true
    ? never
    : `${Key}.${FilterableFieldPathForBranch<Nested, readonly [...Seen, Nested]>}`;

type FilterableFieldPathForBranch<Value, Seen extends ReadonlyArray<unknown>> =
  Value extends TraversableObject<Value>
    ? {
        readonly [Key in FilterableObjectKey<Value>]:
          | (ScalarBranches<ValueAtKey<Value, Key>> extends never ? never : Key)
          | NestedFilterableFieldPath<
              Key,
              TraversableObject<Defined<ValueAtKey<Value, Key>>>,
              readonly [...Seen, Value]
            >;
      }[FilterableObjectKey<Value>]
    : never;

export type FilterableFieldPath<Row> = Extract<
  FilterableFieldPathForBranch<TraversableObject<Row>, readonly []>,
  string
>;

type ValueAtPathBranch<Value, Path extends string> = Path extends `${infer Head}.${infer Tail}`
  ? Value extends unknown
    ? Head extends keyof Value
      ? ValueAtPathBranch<Value[Head], Tail>
      : undefined
    : never
  : Value extends unknown
    ? Path extends keyof Value
      ? Value[Path]
      : undefined
    : never;

export type FilterableFieldValue<Row, Path extends FilterableFieldPath<Row>> = ValueAtPathBranch<
  Row,
  Path
>;

export type TextMatchingOptions = {
  readonly caseSensitive?: boolean;
  readonly accentSensitive?: boolean;
};

type TextMatchingFor<Value> = StringBranches<Value> extends never ? {} : TextMatchingOptions;

export type EqualsCondition<Field extends string, Value> = {
  readonly field: Field;
  readonly type: "equals";
  readonly filter: ScalarBranches<Value>;
} & TextMatchingFor<Value>;

export type NotEqualCondition<Field extends string, Value> = {
  readonly field: Field;
  readonly type: "notEqual";
  readonly filter: ScalarBranches<Value>;
} & TextMatchingFor<Value>;

export type InCondition<Field extends string, Value> = {
  readonly field: Field;
  readonly type: "in";
  readonly filter: ReadonlyArray<ScalarBranches<Value>>;
} & TextMatchingFor<Value>;

type TextSearchConditionType = "contains" | "notContains" | "startsWith" | "endsWith";

type TextSearchConditionForType<
  Field extends string,
  Type extends TextSearchConditionType,
> = Type extends unknown
  ? {
      readonly field: Field;
      readonly type: Type;
      readonly filter: string;
    } & TextMatchingOptions
  : never;

export type TextSearchCondition<Field extends string> = TextSearchConditionForType<
  Field,
  TextSearchConditionType
>;

type NumericDomain<Value> =
  | (NumberBranches<Value> extends never ? never : number)
  | (BigIntBranches<Value> extends never ? never : bigint)
  | (BigDecimalBranches<Value> extends never ? never : BigDecimal.BigDecimal);

type NumericComparisonConditionType =
  | "greaterThan"
  | "greaterThanOrEqual"
  | "lessThan"
  | "lessThanOrEqual";

type NumericComparisonConditionForType<
  Field extends string,
  Domain,
  Type extends NumericComparisonConditionType,
> = Type extends unknown
  ? { readonly field: Field; readonly type: Type; readonly filter: Domain }
  : never;

type NumericComparisonConditionForDomain<Field extends string, Domain> = Domain extends unknown
  ? NumericComparisonConditionForType<Field, Domain, NumericComparisonConditionType>
  : never;

export type NumericComparisonCondition<
  Field extends string,
  Value,
> = NumericComparisonConditionForDomain<Field, NumericDomain<Value>>;

type InRangeConditionForDomain<Field extends string, Domain> = Domain extends unknown
  ? {
      readonly field: Field;
      readonly type: "inRange";
      readonly filter: Domain;
      readonly filterTo: Domain;
    }
  : never;

export type InRangeCondition<Field extends string, Value> = InRangeConditionForDomain<
  Field,
  NumericDomain<Value>
>;

type BlankConditionForType<
  Field extends string,
  Type extends "blank" | "notBlank",
> = Type extends unknown ? { readonly field: Field; readonly type: Type } : never;

export type BlankCondition<Field extends string> = BlankConditionForType<
  Field,
  "blank" | "notBlank"
>;

export type FieldConditionForPath<
  Row,
  Path extends FilterableFieldPath<Row>,
  Value = FilterableFieldValue<Row, Path>,
> =
  | EqualsCondition<Path, Value>
  | NotEqualCondition<Path, Value>
  | InCondition<Path, Value>
  | (StringBranches<Value> extends never ? never : TextSearchCondition<Path>)
  | (NumericDomain<Value> extends never ? never : NumericComparisonCondition<Path, Value>)
  | (NumericDomain<Value> extends never ? never : InRangeCondition<Path, Value>)
  | BlankCondition<Path>;

export type FieldCondition<Row> = {
  readonly [Path in FilterableFieldPath<Row>]: FieldConditionForPath<Row, Path>;
}[FilterableFieldPath<Row>];

export type FilterGroup<Row> = {
  readonly type: "AND" | "OR";
  readonly conditions: ReadonlyArray<FilterExpression<Row>>;
};

export type NegationExpression<Row> = {
  readonly type: "NOT";
  readonly condition: FilterExpression<Row>;
};

type FilterExpressionMember<Row> = FieldCondition<Row> | FilterGroup<Row> | NegationExpression<Row>;

declare const filterExpressionType: unique symbol;

type FilterExpressionTypeMarker = {
  readonly [filterExpressionType]?: true;
};

type KeysOfUnion<Value> = Value extends unknown ? keyof Value : never;

type StrictUnionMember<Member, AllMembers> = Member extends unknown
  ? Member & {
      readonly [Key in Exclude<KeysOfUnion<AllMembers>, keyof Member>]?: never;
    }
  : never;

export type FilterExpression<Row> = StrictUnionMember<
  FilterExpressionMember<Row>,
  FilterExpressionMember<Row>
> &
  FilterExpressionTypeMarker;

export type Where<Row> = ReadonlyArray<FilterExpression<Row>>;

type RejectExpressionExtraKeys<Candidate, Shape> = {
  readonly [Key in Exclude<keyof Candidate, keyof Shape>]: never;
};

type FilterExpressionKey =
  | keyof TextMatchingOptions
  | keyof InRangeCondition<string, number>
  | keyof FilterGroup<never>
  | keyof NegationExpression<never>
  | typeof filterExpressionType;

type StrictFilterExpressionShape<Shape> = Shape extends unknown
  ? Shape & {
      readonly [Key in Exclude<FilterExpressionKey, keyof Shape>]?: never;
    }
  : never;

type FilterGroupCandidateShape = StrictFilterExpressionShape<{
  readonly type: "AND" | "OR";
  readonly conditions: ReadonlyArray<unknown>;
}>;

type NegationCandidateShape = StrictFilterExpressionShape<{
  readonly type: "NOT";
  readonly condition: unknown;
}>;

type ExactFieldConditionFilter<Candidate> = Candidate extends {
  readonly type: "in";
  readonly filter: infer Filter;
}
  ? [Filter] extends [ReadonlyArray<unknown>]
    ? ValidateExactArray<Filter> extends never
      ? { readonly filter: never }
      : { readonly filter: Filter }
    : { readonly filter: never }
  : unknown;

type ExactFieldCondition<Row, Candidate> = Candidate extends {
  readonly field: infer Field;
  readonly type: infer Type;
}
  ? Field extends FilterableFieldPath<Row>
    ? StrictFilterExpressionShape<
        Extract<FieldConditionForPath<Row, Field>, { readonly field: Field; readonly type: Type }>
      > extends infer Shape
      ? [Shape] extends [never]
        ? never
        : Candidate &
            Shape &
            RejectExpressionExtraKeys<Candidate, Shape> &
            ExactFieldConditionFilter<Candidate>
      : never
    : never
  : never;

type ExactShallowFilterExpression<Row, Candidate> = Candidate extends FilterGroupCandidateShape
  ? Candidate["conditions"] extends infer Conditions extends ReadonlyArray<unknown>
    ? Candidate &
        RejectExpressionExtraKeys<Candidate, FilterGroupCandidateShape> & {
          readonly conditions: Conditions & ValidateExactArray<Conditions>;
        }
    : never
  : Candidate extends NegationCandidateShape
    ? Candidate & RejectExpressionExtraKeys<Candidate, NegationCandidateShape>
    : ExactFieldCondition<Row, Candidate>;

type FilterExpressionChildren<Candidate> = Candidate extends FilterGroupCandidateShape
  ? Candidate["conditions"] extends ReadonlyArray<unknown>
    ? Candidate["conditions"]
    : readonly []
  : Candidate extends NegationCandidateShape
    ? readonly [Candidate["condition"]]
    : readonly [];

type InvalidShallowFilterExpressionMember<Row, Candidate> = Candidate extends unknown
  ? [Candidate] extends [ExactShallowFilterExpression<Row, Candidate>]
    ? never
    : Candidate
  : never;

type ValidateShallowFilterExpression<Row, Candidate> = [Candidate] extends [never]
  ? unknown
  : [InvalidShallowFilterExpressionMember<Row, Candidate>] extends [never]
    ? unknown
    : never;

type IsUnion<Value, All = Value> = Value extends unknown
  ? [All] extends [Value]
    ? false
    : true
  : never;

type CanonicalFilterExpressionExtraMember<Candidate> = Candidate extends unknown
  ? Exclude<keyof Candidate, FilterExpressionKey> extends never
    ? never
    : Candidate
  : never;

type IsCanonicalFilterExpression<Row, Candidate> = [
  CanonicalFilterExpressionExtraMember<Candidate>,
] extends [never]
  ? typeof filterExpressionType extends keyof Candidate
    ? [Candidate] extends [FilterExpression<Row>]
      ? true
      : false
    : false
  : false;

type InvalidFilterExpressionChildrenMember<
  Row,
  Children,
  Rest extends ReadonlyArray<unknown>,
  Seen extends ReadonlyArray<unknown>,
> =
  Children extends ReadonlyArray<unknown>
    ? Children extends readonly [unknown, ...ReadonlyArray<unknown>]
      ? ValidateFilterExpressionWorklist<Row, readonly [...Rest, ...Children], Seen> extends never
        ? Children
        : never
      : number extends Children["length"]
        ? ValidateWidenedFilterExpression<Row, Children[number], Seen> extends never
          ? Children
          : ValidateFilterExpressionWorklist<Row, Rest, Seen> extends never
            ? Children
            : never
        : ValidateWidenedFilterExpression<
              Row,
              Exclude<Children[number], undefined>,
              Seen
            > extends never
          ? Children
          : ValidateFilterExpressionWorklist<Row, Rest, Seen> extends never
            ? Children
            : never
    : Children;

type ValidateFilterExpressionChildren<
  Row,
  Children extends ReadonlyArray<unknown>,
  Rest extends ReadonlyArray<unknown>,
  Seen extends ReadonlyArray<unknown>,
> = [InvalidFilterExpressionChildrenMember<Row, Children, Rest, Seen>] extends [never]
  ? unknown
  : never;

type ValidateWidenedFilterExpressionMember<Row, Candidate, Seen extends ReadonlyArray<unknown>> =
  SeenContains<Seen, Candidate> extends true
    ? unknown
    : ValidateShallowFilterExpression<Row, Candidate> extends never
      ? never
      : FilterExpressionChildren<Candidate> extends infer Children extends ReadonlyArray<unknown>
        ? ValidateFilterExpressionChildren<
            Row,
            Children,
            readonly [],
            readonly [...Seen, Candidate]
          >
        : never;

type InvalidWidenedFilterExpressionMember<
  Row,
  Candidate,
  Seen extends ReadonlyArray<unknown>,
> = Candidate extends unknown
  ? ValidateWidenedFilterExpressionMember<Row, Candidate, Seen> extends never
    ? Candidate
    : never
  : never;

type ValidateWidenedFilterExpression<
  Row,
  Candidate,
  Seen extends ReadonlyArray<unknown> = readonly [],
> = [Candidate] extends [never]
  ? unknown
  : IsCanonicalFilterExpression<Row, Candidate> extends true
    ? unknown
    : [InvalidWidenedFilterExpressionMember<Row, Candidate, Seen>] extends [never]
      ? unknown
      : never;

type ValidateFilterExpressionWorklist<
  Row,
  Pending extends ReadonlyArray<unknown>,
  Seen extends ReadonlyArray<unknown> = readonly [],
> = Pending extends readonly [infer Current, ...infer Rest]
  ? SeenContains<Seen, Current> extends true
    ? ValidateFilterExpressionWorklist<Row, Rest, Seen>
    : IsCanonicalFilterExpression<Row, Current> extends true
      ? ValidateFilterExpressionWorklist<Row, Rest, Seen>
      : IsUnion<Current> extends true
        ? ValidateWidenedFilterExpression<Row, Current, Seen> extends never
          ? never
          : ValidateFilterExpressionWorklist<Row, Rest, Seen>
        : ValidateShallowFilterExpression<Row, Current> extends never
          ? never
          : FilterExpressionChildren<Current> extends infer Children extends ReadonlyArray<unknown>
            ? IsUnion<Children> extends false
              ? Children extends readonly []
                ? ValidateFilterExpressionWorklist<Row, Rest, Seen>
                : Children extends readonly [unknown, ...ReadonlyArray<unknown>]
                  ? ValidateFilterExpressionWorklist<Row, readonly [...Rest, ...Children], Seen>
                  : ValidateFilterExpressionChildren<Row, Children, Rest, Seen>
              : ValidateWidenedFilterExpression<Row, Current, Seen> extends never
                ? never
                : ValidateFilterExpressionWorklist<Row, Rest, Seen>
            : never
  : number extends Pending["length"]
    ? ValidateWidenedFilterExpression<Row, Pending[number], Seen>
    : ValidateWidenedFilterExpression<Row, Exclude<Pending[number], undefined>, Seen>;

type InvalidWhereArrayMember<Row, QueryWhere> =
  QueryWhere extends ReadonlyArray<unknown>
    ? ValidateFilterExpressionWorklist<Row, QueryWhere> extends never
      ? QueryWhere
      : never
    : QueryWhere;

type ValidateWhereArray<Row, QueryWhere extends ReadonlyArray<unknown>> = [
  InvalidWhereArrayMember<Row, QueryWhere>,
] extends [never]
  ? ValidateExactArray<QueryWhere>
  : never;

type CanonicalWhereArrayMember<Row, Candidate> =
  Candidate extends ReadonlyArray<unknown>
    ? number extends Candidate["length"]
      ? IsCanonicalFilterExpression<Row, Candidate[number]> extends true
        ? ValidateExactArray<Candidate> extends never
          ? false
          : true
        : false
      : false
    : false;

type IsCanonicalWhereArray<Row, Candidate> = [CanonicalWhereArrayMember<Row, Candidate>] extends [
  true,
]
  ? true
  : false;

type InvalidWhereQueryMember<Row, Query> = Query extends unknown
  ? Query extends { readonly where: infer QueryWhere }
    ? [QueryWhere] extends [ReadonlyArray<unknown>]
      ? IsCanonicalWhereArray<Row, QueryWhere> extends true
        ? never
        : ValidateWhereArray<Row, QueryWhere> extends never
          ? Query
          : never
      : Query
    : never
  : never;

type ExactWhereMembers<Query> = Query extends unknown
  ? Query extends { readonly where: infer QueryWhere }
    ? { readonly where: QueryWhere }
    : unknown
  : never;

export type ExactWhere<Row, Query> = [InvalidWhereQueryMember<Row, Query>] extends [never]
  ? ExactWhereMembers<Query>
  : never;
