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

type LiveQueryViewportCompleteRawField<Row> = Extract<keyof Row, string> & {
  readonly "__effect-view-server/LiveQueryViewportCompleteRawSelect@v1": (_row: Row) => Row;
};

export type LiveQueryViewportCompleteRawSelect<Viewport> =
  LiveQueryViewportBaseRow<Viewport> extends infer Row
    ? [Row] extends [never]
      ? never
      : readonly [
          LiveQueryViewportCompleteRawField<Row>,
          ...Array<LiveQueryViewportCompleteRawField<Row>>,
        ]
    : never;
