export type RejectExtraKeys<Candidate, Shape> = {
  readonly [Key in Exclude<keyof Candidate, keyof Shape>]: never;
};

export type PresentPropertyValue<Candidate, Key extends PropertyKey> = Key extends keyof Candidate
  ? Required<Pick<Candidate, Key>>[Key]
  : never;

type ExactQueryWindowField<Query, Field extends "offset" | "limit"> = Field extends keyof Query
  ? [PresentPropertyValue<Query, Field>] extends [number]
    ? unknown
    : { readonly [Key in Field]: never }
  : unknown;

export type ExactQueryWindow<Query> = ExactQueryWindowField<Query, "offset"> &
  ExactQueryWindowField<Query, "limit">;

type TupleIndexKeys<
  Candidate extends ReadonlyArray<unknown>,
  Indices extends ReadonlyArray<unknown> = readonly [],
  Keys extends string = never,
> = Candidate extends readonly [unknown, ...infer Tail]
  ? TupleIndexKeys<Tail, readonly [...Indices, unknown], Keys | `${Indices["length"]}`>
  : Keys;

type ArrayPrototypeKeys<Candidate> =
  Candidate extends Array<unknown> ? keyof Array<unknown> : keyof ReadonlyArray<unknown>;

type IsExactly<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;

type MutableArrayPrototypeShape<Candidate extends Array<unknown>> = Omit<
  Array<Candidate[number]>,
  "copyWithin" | "fill" | "sort"
> & {
  copyWithin(target: number, start: number, end?: number): Candidate;
  fill(value: Candidate[number], start?: number, end?: number): Candidate;
  sort(compareFn?: (left: Candidate[number], right: Candidate[number]) => number): Candidate;
};

type ArrayPrototypeShape<Candidate extends ReadonlyArray<unknown>> =
  Candidate extends Array<unknown>
    ? MutableArrayPrototypeShape<Candidate>
    : ReadonlyArray<Candidate[number]>;

type ComparableArrayPrototypeKey<Candidate extends ReadonlyArray<unknown>> = Exclude<
  keyof ArrayPrototypeShape<Candidate>,
  number | "length"
>;

type OverriddenArrayPrototypeKey<Candidate extends ReadonlyArray<unknown>> = {
  readonly [Key in ComparableArrayPrototypeKey<Candidate>]: Key extends keyof Candidate
    ? IsExactly<Candidate[Key], ArrayPrototypeShape<Candidate>[Key]> extends true
      ? never
      : Key
    : never;
}[ComparableArrayPrototypeKey<Candidate>];

type InvalidArrayLengthKey<Candidate extends ReadonlyArray<unknown>> =
  number extends Candidate["length"]
    ? never
    : Candidate extends readonly [] | readonly [unknown, ...ReadonlyArray<unknown>]
      ? never
      : "length";

export type RejectArrayExtraKeys<Candidate extends ReadonlyArray<unknown>> = {
  readonly [Key in
    | Exclude<keyof Candidate, ArrayPrototypeKeys<Candidate> | TupleIndexKeys<Candidate>>
    | OverriddenArrayPrototypeKey<Candidate>
    | InvalidArrayLengthKey<Candidate>]: never;
};

type InvalidArrayMember<Candidate extends ReadonlyArray<unknown>> = Candidate extends unknown
  ? Candidate extends Candidate & RejectArrayExtraKeys<Candidate>
    ? never
    : Candidate
  : never;

export type ValidateExactArray<Candidate extends ReadonlyArray<unknown>> = [
  InvalidArrayMember<Candidate>,
] extends [never]
  ? unknown
  : never;
