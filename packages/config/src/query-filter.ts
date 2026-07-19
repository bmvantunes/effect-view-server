import type * as BigDecimal from "effect/BigDecimal";

export type FilterableScalar = string | number | bigint | boolean | BigDecimal.BigDecimal | null;

type Defined<Value> = Exclude<Value, undefined>;
type ScalarBranches<Value> = Extract<Defined<Value>, FilterableScalar>;
type StringBranches<Value> = Extract<Defined<Value>, string>;
type NumberBranches<Value> = Extract<Defined<Value>, number>;
type BigIntBranches<Value> = Extract<Defined<Value>, bigint>;
type BigDecimalBranches<Value> = Extract<Defined<Value>, BigDecimal.BigDecimal>;

type IsRouteFieldValue<Value> = [Defined<Value>] extends [never]
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

type TraversableObject<Value> = Value extends FilterableScalar
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
type ValueAtKey<Value, Key extends string> = Value extends unknown
  ? Key extends keyof Value
    ? Value[Key]
    : undefined
  : never;

type FilterableFieldPathForBranch<Value, Seen> =
  Value extends TraversableObject<Value>
    ? {
        readonly [Key in StringKey<Value>]:
          | (ScalarBranches<ValueAtKey<Value, Key>> extends never ? never : Key)
          | (TraversableObject<Defined<ValueAtKey<Value, Key>>> extends infer Nested
              ? [Nested] extends [never]
                ? never
                : Extract<Nested, Seen> extends never
                  ? `${Key}.${FilterableFieldPathForBranch<Nested, Seen | Value>}`
                  : never
              : never);
      }[StringKey<Value>]
    : never;

export type FilterableFieldPath<Row> = Extract<
  FilterableFieldPathForBranch<TraversableObject<Row>, Row>,
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

type KeysOfUnion<Value> = Value extends unknown ? keyof Value : never;

type StrictUnionMember<Member, AllMembers> = Member extends unknown
  ? Member & {
      readonly [Key in Exclude<KeysOfUnion<AllMembers>, keyof Member>]?: never;
    }
  : never;

export type FilterExpression<Row> = StrictUnionMember<
  FilterExpressionMember<Row>,
  FilterExpressionMember<Row>
>;

export type Where<Row> = ReadonlyArray<FilterExpression<Row>>;

type RejectExpressionExtraKeys<Candidate, Shape> = {
  readonly [Key in Exclude<keyof Candidate, keyof Shape>]: never;
};

type ExactFieldCondition<Row, Candidate> = Candidate extends {
  readonly field: infer Field;
  readonly type: infer Type;
}
  ? Field extends FilterableFieldPath<Row>
    ? Extract<
        FilterExpression<Row>,
        { readonly field: Field; readonly type: Type }
      > extends infer Shape
      ? [Shape] extends [never]
        ? never
        : Candidate & Shape & RejectExpressionExtraKeys<Candidate, Shape>
      : never
    : never
  : never;

type ExactFilterExpression<Row, Candidate> = Candidate extends {
  readonly type: "AND" | "OR";
  readonly conditions: infer Conditions;
}
  ? Conditions extends ReadonlyArray<unknown>
    ? Candidate &
        Extract<FilterExpression<Row>, { readonly type: "AND" | "OR" }> &
        RejectExpressionExtraKeys<
          Candidate,
          Extract<FilterExpression<Row>, { readonly type: "AND" | "OR" }>
        > & {
          readonly conditions: {
            readonly [Index in keyof Conditions]: ExactFilterExpression<Row, Conditions[Index]>;
          };
        }
    : never
  : Candidate extends { readonly type: "NOT"; readonly condition: infer Condition }
    ? Candidate &
        Extract<FilterExpression<Row>, { readonly type: "NOT" }> &
        RejectExpressionExtraKeys<
          Candidate,
          Extract<FilterExpression<Row>, { readonly type: "NOT" }>
        > & {
          readonly condition: ExactFilterExpression<Row, Condition>;
        }
    : ExactFieldCondition<Row, Candidate>;

export type ExactWhere<Row, Query> = Query extends { readonly where: infer QueryWhere }
  ? QueryWhere extends ReadonlyArray<unknown>
    ? {
        readonly where: QueryWhere & {
          readonly [Index in keyof QueryWhere]: ExactFilterExpression<Row, QueryWhere[Index]>;
        };
      }
    : { readonly where: never }
  : unknown;
