import { describe, expectTypeOf, it } from "@effect/vitest";
import { Schema } from "effect";
import type * as BigDecimal from "effect/BigDecimal";
import type { ExactWhere, FilterableFieldPath, FilterableFieldValue, Where } from "./query-filter";

const AnyFieldRow = Schema.Struct({
  id: Schema.String,
  value: Schema.Any,
});

type FilterRow = {
  readonly id: string;
  readonly age: number;
  readonly quantity: bigint;
  readonly numericUnion: number | bigint;
  readonly price: BigDecimal.BigDecimal;
  readonly active: boolean;
  readonly optionalAge?: number;
  readonly profile?: {
    readonly country: string;
    readonly address: {
      readonly city: string;
    };
  };
  readonly mixed: string | { readonly code: string };
  readonly tags: ReadonlyArray<string>;
  readonly dynamic: Readonly<Record<string, string>>;
};

describe("query filter types", () => {
  it("derives scalar paths and their values", () => {
    expectTypeOf<FilterableFieldPath<FilterRow>>().toEqualTypeOf<
      | "id"
      | "age"
      | "quantity"
      | "numericUnion"
      | "price"
      | "active"
      | "optionalAge"
      | "profile.country"
      | "profile.address.city"
      | "mixed"
      | "mixed.code"
    >();
    expectTypeOf<FilterableFieldValue<FilterRow, "profile.country">>().toEqualTypeOf<
      string | undefined
    >();
    expectTypeOf<FilterableFieldValue<FilterRow, "mixed">>().toEqualTypeOf<
      string | { readonly code: string }
    >();
    expectTypeOf<FilterableFieldPath<typeof AnyFieldRow.Type>>().toEqualTypeOf<"id">();
  });

  it("accepts recursive exact expressions without const assertions", () => {
    const where = [
      { field: "id", type: "contains", filter: "résumé" },
      { field: "optionalAge", type: "greaterThan", filter: 18 },
      { field: "numericUnion", type: "greaterThan", filter: 1 },
      { field: "numericUnion", type: "greaterThan", filter: 1n },
      { field: "numericUnion", type: "inRange", filter: 1, filterTo: 2 },
      { field: "numericUnion", type: "inRange", filter: 1n, filterTo: 2n },
      {
        type: "OR",
        conditions: [
          { field: "profile.country", type: "equals", filter: "PT" },
          { field: "mixed.code", type: "startsWith", filter: "A" },
        ],
      },
      {
        type: "NOT",
        condition: { field: "active", type: "equals", filter: false },
      },
    ] satisfies Where<FilterRow>;

    expectTypeOf(where).toMatchTypeOf<Where<FilterRow>>();
    const exact: ExactWhere<FilterRow, { readonly where: typeof where }> = { where };
    expectTypeOf(exact.where).toMatchTypeOf<Where<FilterRow>>();
  });

  it("rejects invalid paths, operators, operands, and surplus properties", () => {
    const legacyObjectWhere = {
      where: {
        id: { type: "contains", filter: "a" },
      },
    };
    // @ts-expect-error where is always an array; legacy field-keyed objects are rejected.
    const _legacyObjectWhere: ExactWhere<FilterRow, typeof legacyObjectWhere> = legacyObjectWhere;

    const _collectionPath = [
      // @ts-expect-error collections are not filterable scalar paths.
      { field: "tags", type: "equals", filter: [] },
    ] satisfies Where<FilterRow>;

    const _dynamicPath = [
      // @ts-expect-error dynamic record keys are not statically named paths.
      { field: "dynamic.country", type: "equals", filter: "PT" },
    ] satisfies Where<FilterRow>;

    const _anyFieldPath = [
      // @ts-expect-error Schema.Any does not promise a runtime-filterable scalar domain.
      { field: "value", type: "contains", filter: "PT" },
    ] satisfies Where<typeof AnyFieldRow.Type>;

    const _numericText = [
      // @ts-expect-error text operators do not apply to numeric fields.
      { field: "age", type: "contains", filter: "2" },
    ] satisfies Where<FilterRow>;

    const _mixedNumeric = [
      // @ts-expect-error number and bigint comparison operands never mix.
      { field: "quantity", type: "greaterThan", filter: 1 },
    ] satisfies Where<FilterRow>;

    const _mixedRange = [
      // @ts-expect-error range bounds must use one numeric kind.
      { field: "age", type: "inRange", filter: 1, filterTo: 2n },
    ] satisfies Where<FilterRow>;

    const surplus = {
      where: [{ field: "id", type: "equals", filter: "a", unexpected: true }],
    } satisfies {
      readonly where: readonly [
        {
          readonly field: "id";
          readonly type: "equals";
          readonly filter: "a";
          readonly unexpected: true;
        },
      ];
    };
    // @ts-expect-error exact field conditions reject surplus properties.
    const _surplus: ExactWhere<FilterRow, typeof surplus> = surplus;

    const optionalUndefinedSurplus: {
      readonly where: readonly [
        {
          readonly field: "id";
          readonly type: "equals";
          readonly filter: "a";
          readonly unexpected?: undefined;
        },
      ];
    } = {
      where: [
        {
          field: "id",
          type: "equals",
          filter: "a",
          unexpected: undefined,
        },
      ],
    };
    // @ts-expect-error present optional-undefined surplus properties are still extra properties.
    const _optionalUndefinedSurplus: ExactWhere<FilterRow, typeof optionalUndefinedSurplus> =
      optionalUndefinedSurplus;

    const leafWithGroupKey: {
      readonly where: readonly [
        {
          readonly field: "id";
          readonly type: "equals";
          readonly filter: "a";
          readonly conditions?: undefined;
        },
      ];
    } = {
      where: [{ field: "id", type: "equals", filter: "a", conditions: undefined }],
    };
    // @ts-expect-error group-only keys are forbidden on field conditions, even when optional.
    const _leafWithGroupKey: ExactWhere<FilterRow, typeof leafWithGroupKey> = leafWithGroupKey;

    const groupWithLeafKey: {
      readonly where: readonly [
        {
          readonly type: "OR";
          readonly conditions: readonly [];
          readonly field?: undefined;
        },
      ];
    } = {
      where: [{ type: "OR", conditions: [], field: undefined }],
    };
    // @ts-expect-error leaf-only keys are forbidden on groups, even when optional.
    const _groupWithLeafKey: ExactWhere<FilterRow, typeof groupWithLeafKey> = groupWithLeafKey;

    const notWithLeafKey: {
      readonly where: readonly [
        {
          readonly type: "NOT";
          readonly condition: {
            readonly field: "active";
            readonly type: "equals";
            readonly filter: false;
          };
          readonly filter?: undefined;
        },
      ];
    } = {
      where: [
        {
          type: "NOT",
          condition: { field: "active", type: "equals", filter: false },
          filter: undefined,
        },
      ],
    };
    // @ts-expect-error leaf-only keys are forbidden on NOT, even when optional.
    const _notWithLeafKey: ExactWhere<FilterRow, typeof notWithLeafKey> = notWithLeafKey;

    const explicitUndefined = {
      where: [{ field: "id", type: "equals", filter: "a", caseSensitive: undefined }],
    } satisfies {
      readonly where: readonly [
        {
          readonly field: "id";
          readonly type: "equals";
          readonly filter: "a";
          readonly caseSensitive: undefined;
        },
      ];
    };
    // @ts-expect-error optional query properties must be omitted rather than undefined.
    const _explicitUndefined: ExactWhere<FilterRow, typeof explicitUndefined> = explicitUndefined;
  });
});
