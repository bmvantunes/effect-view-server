import { describe, expectTypeOf, it } from "@effect/vitest";
import { Schema } from "effect";
import type * as BigDecimal from "effect/BigDecimal";
import type {
  ExactWhere,
  FilterableFieldPath,
  FilterableFieldValue,
  FilterExpression,
  Where,
} from "./query-filter";
import { viewSchema } from "./view-schema";

const AnyFieldRow = Schema.Struct({
  id: Schema.String,
  value: Schema.Any,
});

class OpaqueBoundaryProfile extends Schema.Class<OpaqueBoundaryProfile>("OpaqueBoundaryProfile")({
  nickname: Schema.String,
}) {}

const OpaqueEffectFieldRow = Schema.Struct({
  id: Schema.String,
  profile: Schema.Struct({ country: Schema.String }),
  profileClass: OpaqueBoundaryProfile,
  optionalProfile: viewSchema.Option(Schema.Struct({ country: Schema.String })),
  profiles: viewSchema.Chunk(Schema.Struct({ country: Schema.String })),
  profilesById: viewSchema.HashMap(Schema.String, Schema.Struct({ country: Schema.String })),
  profileSet: viewSchema.HashSet(Schema.Struct({ country: Schema.String })),
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

type RecursiveNode = { readonly child: RecursiveNode | null };

type DottedRootRow = {
  readonly "profile.country": string;
};

type DottedNestedRow = {
  readonly profile: {
    readonly country: string;
    readonly "country.code": string;
  };
};

type FiniteOptionalNestedRow = {
  readonly profile: {
    readonly country: string;
    readonly details?: {
      readonly country: string;
    };
  };
};

type RecursiveUnionNode =
  | {
      readonly kind: "a";
      readonly a: string;
      readonly next?: RecursiveUnionNode;
    }
  | {
      readonly kind: "b";
      readonly b: string;
      readonly next?: RecursiveUnionNode;
    };

type RepeatTuple<
  Value,
  Size extends number,
  Values extends ReadonlyArray<unknown> = readonly [],
> = Values["length"] extends Size ? Values : RepeatTuple<Value, Size, readonly [...Values, Value]>;

type HundredIdConditions = RepeatTuple<
  {
    readonly field: "id";
    readonly type: "equals";
    readonly filter: "bulk";
  },
  100
>;

type InvalidNumericTextCondition = {
  readonly field: "age";
  readonly type: "contains";
  readonly filter: "invalid";
};

type NestedNotExpression<
  Depth extends number,
  Levels extends ReadonlyArray<unknown> = readonly [],
> = Levels["length"] extends Depth
  ? InvalidNumericTextCondition
  : {
      readonly type: "NOT";
      readonly condition: NestedNotExpression<Depth, readonly [...Levels, unknown]>;
    };

type GeneratedInvalidNestedQuery<Depth extends number> = {
  readonly where: readonly [NestedNotExpression<Depth>];
};

type ValidNestedNotExpression<
  Depth extends number,
  Levels extends ReadonlyArray<unknown> = readonly [],
> = Levels["length"] extends Depth
  ? { readonly field: "id"; readonly type: "equals"; readonly filter: "valid" }
  : {
      readonly type: "NOT";
      readonly condition: ValidNestedNotExpression<Depth, readonly [...Levels, unknown]>;
    };

type GeneratedValidNestedQuery<Depth extends number> = {
  readonly where: readonly [ValidNestedNotExpression<Depth>];
};

type ValidIdCondition = {
  readonly field: "id";
  readonly type: "equals";
  readonly filter: "valid";
};

type QueryUnionWithInvalidWhere =
  | { readonly where: readonly [ValidIdCondition] }
  | {
      readonly where: readonly [ValidIdCondition & { readonly unexpected: true }];
    };

type WidenedInvalidOperatorGroup = {
  readonly type: "AND";
  readonly conditions: ReadonlyArray<InvalidNumericTextCondition>;
};

type WidenedSurplusGroup = {
  readonly type: "OR";
  readonly conditions: ReadonlyArray<ValidIdCondition & { readonly unexpected: true }>;
};

type RecursiveValidGroup = {
  readonly type: "AND";
  readonly conditions: ReadonlyArray<ValidIdCondition | RecursiveValidGroup>;
};

type RecursiveValidNotUnion =
  | ValidIdCondition
  | { readonly type: "NOT"; readonly condition: RecursiveValidNotUnion };

type RecursiveOptionalGroup = {
  readonly type: "AND";
  readonly conditions: readonly [] | readonly [RecursiveOptionalGroup];
};

type VariadicSurplusWhere = readonly [
  ValidIdCondition,
  ...Array<ValidIdCondition & { readonly unexpected: true }>,
];

type WidenedInvalidUnionWhere = readonly [ValidIdCondition | WidenedSurplusGroup];

type GroupWithInvalidConditionsUnion = {
  readonly type: "AND";
  readonly conditions:
    | readonly []
    | ReadonlyArray<ValidIdCondition & { readonly unexpected: true }>;
};

type OptionalSurplusWhere = readonly [(ValidIdCondition & { readonly unexpected: true })?];

type DecoratedConditionsUnionGroup = {
  readonly type: "AND";
  readonly conditions:
    | readonly [ValidIdCondition]
    | (readonly [ValidIdCondition] & { readonly metadata: true });
};

type DecoratedInFilterUnion =
  | readonly ["valid"]
  | (readonly ["valid"] & { readonly metadata: true });

type CanonicalExpressionWithOptionalSurplus = FilterExpression<FilterRow> & {
  readonly unexpected?: true;
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
    expectTypeOf<
      FilterableFieldPath<{ readonly recursive: RecursiveNode }>
    >().toEqualTypeOf<"recursive.child">();
    expectTypeOf<FilterableFieldPath<DottedRootRow>>().toEqualTypeOf<never>();
    expectTypeOf<FilterableFieldPath<DottedNestedRow>>().toEqualTypeOf<"profile.country">();
    expectTypeOf<FilterableFieldPath<FiniteOptionalNestedRow>>().toEqualTypeOf<
      "profile.country" | "profile.details.country"
    >();
    expectTypeOf<FilterableFieldPath<{ readonly node: RecursiveUnionNode }>>().toEqualTypeOf<
      "node.kind" | "node.a" | "node.b"
    >();
    expectTypeOf<FilterableFieldPath<typeof OpaqueEffectFieldRow.Type>>().toEqualTypeOf<
      "id" | "profile.country" | "profileClass.nickname"
    >();
  });

  it("treats admitted Effect values as opaque filter fields", () => {
    const supported = [
      { field: "id", type: "equals", filter: "profile-1" },
      { field: "profile.country", type: "equals", filter: "PT" },
      { field: "profileClass.nickname", type: "contains", filter: "admin" },
    ] satisfies Where<typeof OpaqueEffectFieldRow.Type>;

    expectTypeOf(supported).toExtend<Where<typeof OpaqueEffectFieldRow.Type>>();

    const _optionImplementationField = [
      // @ts-expect-error admitted Option values are opaque runtime declarations.
      { field: "optionalProfile.value.country", type: "equals", filter: "PT" },
    ] satisfies Where<typeof OpaqueEffectFieldRow.Type>;
    const _chunkImplementationField = [
      // @ts-expect-error admitted Chunk values do not expose their length as a filter field.
      { field: "profiles.length", type: "equals", filter: 1 },
    ] satisfies Where<typeof OpaqueEffectFieldRow.Type>;
    const _hashMapImplementationField = [
      {
        // @ts-expect-error admitted HashMap values do not expose implementation markers.
        field: "profilesById.~effect/collections/HashMap",
        type: "equals",
        filter: "~effect/collections/HashMap",
      },
    ] satisfies Where<typeof OpaqueEffectFieldRow.Type>;
    const _hashSetImplementationField = [
      {
        // @ts-expect-error admitted HashSet values do not expose implementation markers.
        field: "profileSet.~effect/collections/HashSet",
        type: "equals",
        filter: "~effect/collections/HashSet",
      },
    ] satisfies Where<typeof OpaqueEffectFieldRow.Type>;
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
    expectTypeOf<{ readonly where: HundredIdConditions }>().toMatchTypeOf<
      ExactWhere<FilterRow, { readonly where: HundredIdConditions }>
    >();
    expectTypeOf<ExactWhere<FilterRow, GeneratedInvalidNestedQuery<5>>>().toBeNever();
    expectTypeOf<ExactWhere<FilterRow, GeneratedInvalidNestedQuery<100>>>().toBeNever();
    expectTypeOf<ExactWhere<FilterRow, QueryUnionWithInvalidWhere>>().toBeNever();
    expectTypeOf<GeneratedValidNestedQuery<100>>().toMatchTypeOf<
      ExactWhere<FilterRow, GeneratedValidNestedQuery<100>>
    >();
    expectTypeOf<{ readonly where: ReadonlyArray<ValidIdCondition> }>().toMatchTypeOf<
      ExactWhere<FilterRow, { readonly where: ReadonlyArray<ValidIdCondition> }>
    >();
    expectTypeOf<{ readonly where: Where<FilterRow> }>().toMatchTypeOf<
      ExactWhere<FilterRow, { readonly where: Where<FilterRow> }>
    >();
    expectTypeOf<{
      readonly where: readonly [FilterExpression<FilterRow>];
    }>().toMatchTypeOf<
      ExactWhere<FilterRow, { readonly where: readonly [FilterExpression<FilterRow>] }>
    >();
    expectTypeOf<{ readonly where: ReadonlyArray<never> }>().toMatchTypeOf<
      ExactWhere<FilterRow, { readonly where: ReadonlyArray<never> }>
    >();
    expectTypeOf<{ readonly where: readonly [RecursiveValidGroup] }>().toMatchTypeOf<
      ExactWhere<FilterRow, { readonly where: readonly [RecursiveValidGroup] }>
    >();
    expectTypeOf<{ readonly where: readonly [RecursiveValidNotUnion] }>().toMatchTypeOf<
      ExactWhere<FilterRow, { readonly where: readonly [RecursiveValidNotUnion] }>
    >();
    expectTypeOf<{ readonly where: readonly [RecursiveOptionalGroup] }>().toMatchTypeOf<
      ExactWhere<FilterRow, { readonly where: readonly [RecursiveOptionalGroup] }>
    >();
  });

  it("allows text modifiers only when equality and membership fields support strings", () => {
    type ModifierRow = {
      readonly text: string;
      readonly mixed: string | number;
      readonly numeric: number;
    };

    const textModifiers = [
      {
        field: "text",
        type: "equals",
        filter: "Résumé",
        caseSensitive: true,
        accentSensitive: true,
      },
      {
        field: "text",
        type: "in",
        filter: ["Résumé"],
        caseSensitive: true,
        accentSensitive: true,
      },
      {
        field: "mixed",
        type: "equals",
        filter: 1,
        caseSensitive: true,
        accentSensitive: true,
      },
      {
        field: "mixed",
        type: "in",
        filter: [1],
        caseSensitive: true,
        accentSensitive: true,
      },
    ] satisfies Where<ModifierRow>;

    expectTypeOf(textModifiers).toMatchTypeOf<Where<ModifierRow>>();

    const _numericEqualsModifier = [
      // @ts-expect-error numeric-only equality fields reject text modifiers.
      { field: "numeric", type: "equals", filter: 1, caseSensitive: true },
    ] satisfies Where<ModifierRow>;
    const _numericInModifier = [
      // @ts-expect-error numeric-only membership fields reject text modifiers.
      { field: "numeric", type: "in", filter: [1], accentSensitive: true },
    ] satisfies Where<ModifierRow>;
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

    const _recursivePathBeyondRuntimeMetadata = [
      // @ts-expect-error recursive paths stop where the runtime schema traversal detects its cycle.
      { field: "recursive.child.child", type: "blank" },
    ] satisfies Where<{ readonly recursive: RecursiveNode }>;

    const _recursiveUnionPathBeyondRuntimeMetadata = [
      // @ts-expect-error a repeated recursive union stops at the runtime Suspend boundary.
      { field: "node.next.a", type: "equals", filter: "a" },
    ] satisfies Where<{ readonly node: RecursiveUnionNode }>;

    const _dottedRootPath = [
      // @ts-expect-error dots are reserved for path segments, never literal property names.
      { field: "profile.country", type: "equals", filter: "PT" },
    ] satisfies Where<DottedRootRow>;

    const _dottedNestedPath = [
      // @ts-expect-error dotted nested keys are excluded even when their apparent path is unique.
      { field: "profile.country.code", type: "equals", filter: "PT" },
    ] satisfies Where<DottedNestedRow>;

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

    const nestedGroupSurplus = {
      where: [
        {
          type: "OR",
          conditions: [{ field: "id", type: "equals", filter: "a", unexpected: true }],
        },
      ],
    } satisfies {
      readonly where: readonly [
        {
          readonly type: "OR";
          readonly conditions: readonly [
            {
              readonly field: "id";
              readonly type: "equals";
              readonly filter: "a";
              readonly unexpected: true;
            },
          ];
        },
      ];
    };
    // @ts-expect-error recursive group conditions reject surplus leaf properties.
    const _nestedGroupSurplus: ExactWhere<FilterRow, typeof nestedGroupSurplus> =
      nestedGroupSurplus;

    expectTypeOf<
      ExactWhere<FilterRow, { readonly where: readonly [WidenedSurplusGroup] }>
    >().toBeNever();
    expectTypeOf<
      ExactWhere<FilterRow, { readonly where: readonly [WidenedInvalidOperatorGroup] }>
    >().toBeNever();
    expectTypeOf<ExactWhere<FilterRow, { readonly where: VariadicSurplusWhere }>>().toBeNever();
    expectTypeOf<ExactWhere<FilterRow, { readonly where: WidenedInvalidUnionWhere }>>().toBeNever();
    expectTypeOf<
      ExactWhere<FilterRow, { readonly where: readonly [GroupWithInvalidConditionsUnion] }>
    >().toBeNever();
    expectTypeOf<ExactWhere<FilterRow, { readonly where: OptionalSurplusWhere }>>().toBeNever();
    expectTypeOf<
      ExactWhere<FilterRow, { readonly where: readonly [ValidIdCondition?] }>
    >().toBeNever();
    expectTypeOf<
      ExactWhere<FilterRow, { readonly where: readonly [DecoratedConditionsUnionGroup] }>
    >().toBeNever();
    expectTypeOf<
      ExactWhere<
        FilterRow,
        {
          readonly where: readonly [
            {
              readonly field: "id";
              readonly type: "in";
              readonly filter: DecoratedInFilterUnion;
            },
          ];
        }
      >
    >().toBeNever();
    expectTypeOf<
      ExactWhere<
        FilterRow,
        { readonly where: ReadonlyArray<CanonicalExpressionWithOptionalSurplus> }
      >
    >().toBeNever();
    expectTypeOf<
      ExactWhere<
        FilterRow,
        {
          readonly where: readonly [
            {
              readonly type: "OR";
              readonly conditions: readonly [
                ValidIdCondition,
                ...Array<ValidIdCondition | WidenedSurplusGroup>,
              ];
            },
          ];
        }
      >
    >().toBeNever();

    const nestedInvalidNot = {
      where: [
        {
          type: "NOT",
          condition: { field: "age", type: "contains", filter: "2" },
        },
      ],
    } satisfies {
      readonly where: readonly [
        {
          readonly type: "NOT";
          readonly condition: {
            readonly field: "age";
            readonly type: "contains";
            readonly filter: "2";
          };
        },
      ];
    };
    // @ts-expect-error recursive NOT conditions enforce field-specific operators.
    const _nestedInvalidNot: ExactWhere<FilterRow, typeof nestedInvalidNot> = nestedInvalidNot;

    const decoratedNestedConditions = Object.assign(
      [{ field: "id", type: "equals", filter: "a" }] satisfies Array<{
        readonly field: "id";
        readonly type: "equals";
        readonly filter: "a";
      }>,
      { metadata: true },
    );
    const nestedDecoratedGroup = {
      where: [{ type: "OR", conditions: decoratedNestedConditions }],
    } satisfies {
      readonly where: readonly [
        { readonly type: "OR"; readonly conditions: typeof decoratedNestedConditions },
      ];
    };
    // @ts-expect-error recursive group conditions reject array-level decorations.
    const _nestedDecoratedGroup: ExactWhere<FilterRow, typeof nestedDecoratedGroup> =
      nestedDecoratedGroup;

    const expressionDecoration = {
      field: "id",
      type: "equals",
      filter: "decoration",
    } as const;
    const decoratedConditionsWithExpression = Object.assign(
      [{ field: "id", type: "equals", filter: "a" }] satisfies Array<{
        readonly field: "id";
        readonly type: "equals";
        readonly filter: "a";
      }>,
      { metadata: expressionDecoration },
    );
    const nestedExpressionDecoratedGroup = {
      where: [{ type: "OR", conditions: decoratedConditionsWithExpression }],
    } satisfies {
      readonly where: readonly [
        {
          readonly type: "OR";
          readonly conditions: typeof decoratedConditionsWithExpression;
        },
      ];
    };
    // @ts-expect-error group condition arrays reject expression-valued string decorations.
    const _nestedExpressionDecoratedGroup: ExactWhere<
      FilterRow,
      typeof nestedExpressionDecoratedGroup
    > = nestedExpressionDecoratedGroup;

    const decorationSymbol = Symbol("where-decoration");
    const decoratedTopLevelWhere = Object.assign(
      [{ field: "id", type: "equals", filter: "a" }] satisfies Array<{
        readonly field: "id";
        readonly type: "equals";
        readonly filter: "a";
      }>,
      { [decorationSymbol]: expressionDecoration },
    );
    const topLevelExpressionDecoratedQuery = {
      where: decoratedTopLevelWhere,
    } satisfies { readonly where: typeof decoratedTopLevelWhere };
    // @ts-expect-error top-level where rejects expression-valued symbol decorations.
    const _topLevelExpressionDecoratedQuery: ExactWhere<
      FilterRow,
      typeof topLevelExpressionDecoratedQuery
    > = topLevelExpressionDecoratedQuery;

    const mutableWhereSource = [{ field: "id", type: "equals", filter: "a" }] satisfies Array<{
      readonly field: "id";
      readonly type: "equals";
      readonly filter: "a";
    }>;
    const mutablePushDecoratedWhere = Object.assign(mutableWhereSource, {
      push: mutableWhereSource.push,
    });
    const mutablePushDecoratedQuery = {
      where: mutablePushDecoratedWhere,
    } satisfies { readonly where: typeof mutablePushDecoratedWhere };
    // @ts-expect-error mutable where arrays reject own mutable-array method decorations.
    const _mutablePushDecoratedQuery: ExactWhere<FilterRow, typeof mutablePushDecoratedQuery> =
      mutablePushDecoratedQuery;

    const decoratedInFilter = Object.assign(["a"], { metadata: true });
    const decoratedInQuery = {
      where: [{ field: "id", type: "in", filter: decoratedInFilter }],
    } satisfies {
      readonly where: readonly [
        {
          readonly field: "id";
          readonly type: "in";
          readonly filter: typeof decoratedInFilter;
        },
      ];
    };
    // @ts-expect-error nested membership arrays reject array-level decorations.
    const _decoratedInQuery: ExactWhere<FilterRow, typeof decoratedInQuery> = decoratedInQuery;

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
