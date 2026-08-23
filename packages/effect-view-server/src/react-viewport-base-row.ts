type IsAny<Value> = 0 extends 1 & Value ? true : false;

type IsUnknown<Value> = IsAny<Value> extends true ? false : unknown extends Value ? true : false;

type IsExact<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;

type ExactSafeRow<Input, Output> =
  IsAny<Input> extends true
    ? never
    : IsUnknown<Input> extends true
      ? never
      : IsAny<Output> extends true
        ? never
        : IsUnknown<Output> extends true
          ? never
          : IsExact<Input, Output> extends true
            ? Input
            : never;

type ExactSafeWitness<Witness> = [Witness] extends [(_row: infer Input) => infer Output]
  ? IsExact<Witness, (_row: Input) => Output> extends true
    ? ExactSafeRow<Input, Output>
    : never
  : never;

type LiveQueryViewportBaseRowMember<Viewport> =
  "__effect-view-server/LiveQueryViewportBaseRow@v1" extends Exclude<
    keyof Viewport,
    "__effect-view-server/LiveQueryViewportBaseRow@v1"
  >
    ? never
    : "__effect-view-server/LiveQueryViewportBaseRow@v1" extends keyof Viewport
      ? ExactSafeWitness<
          Exclude<Viewport["__effect-view-server/LiveQueryViewportBaseRow@v1"], undefined>
        >
      : never;

type LiveQueryViewportBaseRowMembers<Viewport> = Viewport extends unknown
  ? LiveQueryViewportBaseRowMember<Viewport>
  : never;

type LiveQueryViewportBaseRowMemberValidity<Viewport> = Viewport extends unknown
  ? [LiveQueryViewportBaseRowMember<Viewport>] extends [never]
    ? false
    : true
  : never;

type LiveQueryViewportBaseRowMemberUniformity<Viewport, Rows> = Viewport extends unknown
  ? IsExact<LiveQueryViewportBaseRowMember<Viewport>, Rows>
  : never;

export type LiveQueryViewportBaseRow<Viewport> =
  IsAny<Viewport> extends true
    ? never
    : IsUnknown<Viewport> extends true
      ? never
      : false extends LiveQueryViewportBaseRowMemberValidity<Viewport>
        ? never
        : false extends LiveQueryViewportBaseRowMemberUniformity<
              Viewport,
              LiveQueryViewportBaseRowMembers<Viewport>
            >
          ? never
          : LiveQueryViewportBaseRowMembers<Viewport>;

type LiveQueryViewportCompleteRawSelectMember<Viewport> =
  "__effect-view-server/LiveQueryViewportCompleteRawSelect@v1" extends Exclude<
    keyof Viewport,
    "__effect-view-server/LiveQueryViewportCompleteRawSelect@v1"
  >
    ? never
    : "__effect-view-server/LiveQueryViewportCompleteRawSelect@v1" extends keyof Viewport
      ? Exclude<Viewport["__effect-view-server/LiveQueryViewportCompleteRawSelect@v1"], undefined>
      : never;

type LiveQueryViewportCompleteRawSelectMembers<Viewport> = Viewport extends unknown
  ? LiveQueryViewportCompleteRawSelectMember<Viewport>
  : never;

type IsSafeCompleteRawSelect<Select> =
  IsAny<Select> extends true
    ? false
    : IsUnknown<Select> extends true
      ? false
      : [Select] extends [readonly [string, ...ReadonlyArray<string>]]
        ? true
        : false;

type LiveQueryViewportCompleteRawSelectMemberValidity<Viewport> = Viewport extends unknown
  ? IsSafeCompleteRawSelect<LiveQueryViewportCompleteRawSelectMember<Viewport>>
  : never;

type LiveQueryViewportCompleteRawSelectMemberUniformity<Viewport, Select> = Viewport extends unknown
  ? IsSafeCompleteRawSelect<LiveQueryViewportCompleteRawSelectMember<Viewport>> extends true
    ? IsExact<LiveQueryViewportCompleteRawSelectMember<Viewport>, Select>
    : false
  : never;

export type LiveQueryViewportCompleteRawSelect<Viewport> =
  IsAny<Viewport> extends true
    ? never
    : IsUnknown<Viewport> extends true
      ? never
      : [LiveQueryViewportBaseRow<Viewport>] extends [never]
        ? never
        : false extends LiveQueryViewportCompleteRawSelectMemberValidity<Viewport>
          ? never
          : false extends LiveQueryViewportCompleteRawSelectMemberUniformity<
                Viewport,
                LiveQueryViewportCompleteRawSelectMembers<Viewport>
              >
            ? never
            : LiveQueryViewportCompleteRawSelectMembers<Viewport>;

type LiveQueryViewportInvariantWitnessMember<Viewport, Key extends PropertyKey> =
  Key extends Exclude<keyof Viewport, Key>
    ? never
    : Key extends keyof Viewport
      ? ExactSafeWitness<Exclude<Viewport[Key], undefined>>
      : never;

type LiveQueryViewportInvariantWitnessMembers<
  Viewport,
  Key extends PropertyKey,
> = Viewport extends unknown ? LiveQueryViewportInvariantWitnessMember<Viewport, Key> : never;

type LiveQueryViewportInvariantWitnessMemberValidity<
  Viewport,
  Key extends PropertyKey,
> = Viewport extends unknown
  ? [LiveQueryViewportInvariantWitnessMember<Viewport, Key>] extends [never]
    ? false
    : true
  : never;

type LiveQueryViewportInvariantWitnessMemberUniformity<
  Viewport,
  Key extends PropertyKey,
  Members,
> = Viewport extends unknown
  ? IsExact<LiveQueryViewportInvariantWitnessMember<Viewport, Key>, Members>
  : never;

type LiveQueryViewportInvariantWitness<Viewport, Key extends PropertyKey> =
  IsAny<Viewport> extends true
    ? never
    : IsUnknown<Viewport> extends true
      ? never
      : false extends LiveQueryViewportInvariantWitnessMemberValidity<Viewport, Key>
        ? never
        : false extends LiveQueryViewportInvariantWitnessMemberUniformity<
              Viewport,
              Key,
              LiveQueryViewportInvariantWitnessMembers<Viewport, Key>
            >
          ? never
          : LiveQueryViewportInvariantWitnessMembers<Viewport, Key>;

/** Exact source-owned Feed Route values for a Viewport Source, or `never` when materialized. */
export type LiveQueryViewportRouteBy<Viewport> = Exclude<
  [LiveQueryViewportBaseRow<Viewport>] extends [never]
    ? never
    : LiveQueryViewportInvariantWitness<
        Viewport,
        "__effect-view-server/LiveQueryViewportRouteBy@v1"
      >,
  undefined
>;

/** Exact source-owned Filter Expressions for a Viewport Source's complete Topic Row. */
export type LiveQueryViewportWhere<Viewport> = [LiveQueryViewportBaseRow<Viewport>] extends [never]
  ? never
  : LiveQueryViewportInvariantWitness<Viewport, "__effect-view-server/LiveQueryViewportWhere@v1">;
