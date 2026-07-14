import type { Schema } from "effect";
import type * as BigDecimal from "effect/BigDecimal";
import type { TopicSourceDefinition } from "./source-contract";

export type TopicName = string;
export type SortDirection = "asc" | "desc";

export type SchemaType<S> = Schema.Schema.Type<S>;
export type RowSchema = Schema.Codec<object, unknown, never, never> & {
  readonly fields: Readonly<Record<string, Schema.Codec<unknown, unknown, never, never>>>;
};
export type RowFromSchema<S extends RowSchema> = Schema.Struct.Type<S["fields"]>;

export type StringFieldKey<Row> = Extract<
  {
    readonly [Key in keyof Row]-?: Row[Key] extends string ? Key : never;
  }[keyof Row],
  string
>;

type NumericValue = number | bigint | BigDecimal.BigDecimal;
type IsNumericFieldValue<Value> = [Value] extends [never]
  ? false
  : undefined extends Value
    ? false
    : [Value] extends [NumericValue]
      ? true
      : false;

export type NumericFieldKey<Row> = Extract<
  {
    readonly [Key in keyof Row]-?: IsNumericFieldValue<Row[Key]> extends true ? Key : never;
  }[keyof Row],
  string
>;

export type FieldKey<Row> = Extract<keyof Row, string>;

export type TopicDefinition<S extends RowSchema, Key extends string> = {
  readonly schema: S;
  readonly key: Key;
  readonly kafkaSource?: object | undefined;
  readonly grpcSource?: TopicSourceDefinition | undefined;
  readonly source?: never;
};

export type TopicDefinitions = Record<
  string,
  {
    readonly schema: RowSchema;
    readonly key: string;
    readonly kafkaSource?: object | undefined;
    readonly grpcSource?: TopicSourceDefinition | undefined;
    readonly source?: never;
  }
>;

export type TopicSchema<Topics, Topic extends keyof Topics> = Topics[Topic] extends {
  readonly schema: infer S extends RowSchema;
}
  ? S
  : never;

export type TopicRow<Topics, Topic extends keyof Topics> = Topics[Topic] extends {
  readonly schema: infer S extends RowSchema;
}
  ? RowFromSchema<S>
  : never;

export type Simplify<T> = { readonly [Key in keyof T]: T[Key] };

type TupleIndexKeys<Tuple extends ReadonlyArray<unknown>> = Exclude<
  keyof Tuple,
  keyof ReadonlyArray<unknown>
>;

type TupleMemberRequiresElement<Tuple extends ReadonlyArray<unknown>, Element> = true extends {
  readonly [Index in TupleIndexKeys<Tuple>]: [Tuple[Index]] extends [Element] ? true : false;
}[TupleIndexKeys<Tuple>]
  ? true
  : false;

type TupleMembersRequireElement<Tuple extends ReadonlyArray<unknown>, Element> =
  Tuple extends ReadonlyArray<unknown> ? TupleMemberRequiresElement<Tuple, Element> : never;

type TupleRequiredElements<
  Tuple extends ReadonlyArray<unknown>,
  Elements = Tuple[number],
> = Elements extends unknown
  ? false extends TupleMembersRequireElement<Tuple, Elements>
    ? never
    : Elements
  : never;

export type PickTupleFields<Row, Tuple extends ReadonlyArray<unknown>> = Simplify<
  Pick<Row, Extract<TupleRequiredElements<Tuple>, keyof Row>> &
    Partial<Pick<Row, Extract<Exclude<Tuple[number], TupleRequiredElements<Tuple>>, keyof Row>>>
>;
