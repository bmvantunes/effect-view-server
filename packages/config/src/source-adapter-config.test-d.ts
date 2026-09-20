import { describe, expectTypeOf, it } from "@effect/vitest";
import {
  SourceAdapter,
  type SourceDefinitionOptionsFamily,
  type SourceDefinitionRouteFields,
  type SourceDefinitionRow,
  type SourceHealthForDefinition,
} from "@effect-view-server/source-adapter";
import { Schema } from "effect";
import {
  defineViewServerConfig,
  ViewServerId,
  type DefineViewServerConfigInput,
  type ExactLiveQueryInputForTopic,
  type FilterableScalar,
  type TopicRow,
  type ViewServerConfig,
  type ViewServerHealth,
  type ViewServerConfigTopicInputShape,
  type ViewServerSourceHealth,
} from "./index";

const Failure = Schema.TaggedStruct("ConfigTypeSourceFailure", {
  message: Schema.String,
});
const Declaration = {
  metrics: Schema.Struct({ connected: Schema.Boolean }),
  rejectionLocation: Schema.Struct({ offset: Schema.BigInt }),
  definitionOptions: SourceAdapter.definitionOptions<{
    readonly stream: string;
  }>(),
};
const adapter = SourceAdapter.make({
  identity: { name: "config-type-source" },
  failure: Failure,
  materialized: Declaration,
  leased: Declaration,
});
const Row = Schema.Struct({
  id: ViewServerId,
  region: Schema.String,
  shard: Schema.BigInt,
});
const LeftUnionSchema = Schema.Struct({
  id: ViewServerId,
  left: Schema.String,
});
const RightUnionSchema = Schema.Struct({
  id: ViewServerId,
  right: Schema.Number,
});
declare const exclusiveUnionSchema: typeof LeftUnionSchema | typeof RightUnionSchema;
const NumberIdSchema = Schema.Struct({ id: Schema.Number });
declare const mixedCanonicalIdSchema: typeof Row | typeof NumberIdSchema;
type MissingFieldRow = {
  readonly id: string;
  readonly region: string;
};
type ExtraFieldRow = typeof Row.Type & {
  readonly extra: boolean;
};
type DifferentFieldTypeRow = {
  readonly id: string;
  readonly region: string;
  readonly shard: number;
};
declare const extraFieldInitial: ExtraFieldRow;
declare enum GeneratedStatus {
  Pending = 0,
  Complete = 1,
}
type GeneratedEnumRow = {
  readonly id: string;
  readonly status: GeneratedStatus;
};
type LiteralRegionRow = {
  readonly id: string;
  readonly region: "eu" | "us";
};
type MissingIdRow = {
  readonly region: string;
};
type NarrowIdRow = {
  readonly id: `user:${string}`;
  readonly region: string;
};
type RequiredUndefinedNoteRow = {
  readonly id: string;
  readonly note: string | undefined;
};
type ExclusiveUnionRow =
  | { readonly id: string; readonly left: string }
  | { readonly id: string; readonly right: number };
declare const exclusiveUnionInitial: ExclusiveUnionRow;
type PartiallySharedUnionRow =
  | typeof LeftUnionSchema.Type
  | { readonly id: string; readonly other: boolean };
declare const partiallySharedUnionInitial: PartiallySharedUnionRow;
const VariantA = Schema.Struct({
  id: ViewServerId,
  kind: Schema.Literal("a"),
  left: Schema.String,
});
const VariantB = Schema.Struct({
  id: ViewServerId,
  kind: Schema.Literal("b"),
  right: Schema.Number,
});
declare const discriminatedUnionSchema: typeof VariantA | typeof VariantB;
type MismatchedDiscriminatedUnionRow =
  | { readonly id: string; readonly kind: "a"; readonly left: number }
  | { readonly id: string; readonly kind: "b"; readonly right: string };
declare const mismatchedDiscriminatedUnionInitial: MismatchedDiscriminatedUnionRow;
type MismatchedDiscriminatedUnionWithExtraVariantRow =
  | MismatchedDiscriminatedUnionRow
  | { readonly id: string; readonly kind: "c"; readonly tail: boolean };
declare const mismatchedDiscriminatedUnionWithExtraVariantInitial: MismatchedDiscriminatedUnionWithExtraVariantRow;
const RepeatedRegionVariantA = Schema.Struct({
  id: ViewServerId,
  kind: Schema.Literal("a"),
  region: Schema.Literal("shared"),
  left: Schema.String,
});
const RepeatedRegionVariantB = Schema.Struct({
  id: ViewServerId,
  kind: Schema.Literal("b"),
  region: Schema.Literal("shared"),
  right: Schema.Number,
});
const RepeatedRegionVariantC = Schema.Struct({
  id: ViewServerId,
  kind: Schema.Literal("c"),
  region: Schema.Literal("other"),
  tail: Schema.Boolean,
});
declare const repeatedRegionUnionSchema:
  | typeof RepeatedRegionVariantA
  | typeof RepeatedRegionVariantB
  | typeof RepeatedRegionVariantC;
type MismatchedRepeatedRegionUnionRow =
  | {
      readonly id: string;
      readonly kind: "a";
      readonly region: "shared";
      readonly left: number;
    }
  | {
      readonly id: string;
      readonly kind: "b";
      readonly region: "shared";
      readonly right: string;
    }
  | {
      readonly id: string;
      readonly kind: "c";
      readonly region: "other";
      readonly tail: string;
    };
declare const mismatchedRepeatedRegionUnionInitial: MismatchedRepeatedRegionUnionRow;
const ConflictingDiscriminatorVariantA = Schema.Struct({
  id: ViewServerId,
  kind: Schema.Literal("a"),
  code: Schema.Literal("x"),
  left: Schema.String,
});
const ConflictingDiscriminatorVariantB = Schema.Struct({
  id: ViewServerId,
  kind: Schema.Literal("b"),
  code: Schema.Literal("y"),
  right: Schema.Number,
});
declare const conflictingDiscriminatorUnionSchema:
  | typeof ConflictingDiscriminatorVariantA
  | typeof ConflictingDiscriminatorVariantB;
type MismatchedConflictingDiscriminatorUnionRow =
  | {
      readonly id: string;
      readonly kind: "a";
      readonly code: "y";
      readonly left: number;
    }
  | {
      readonly id: string;
      readonly kind: "b";
      readonly code: "x";
      readonly right: string;
    };
declare const mismatchedConflictingDiscriminatorUnionInitial: MismatchedConflictingDiscriminatorUnionRow;
const NonMatchingPreferredVariantA = Schema.TaggedStruct("expected-a", {
  id: ViewServerId,
  kind: Schema.Literal("a"),
  left: Schema.String,
});
const NonMatchingPreferredVariantB = Schema.TaggedStruct("expected-b", {
  id: ViewServerId,
  kind: Schema.Literal("b"),
  right: Schema.Number,
});
declare const nonMatchingPreferredUnionSchema:
  | typeof NonMatchingPreferredVariantA
  | typeof NonMatchingPreferredVariantB;
type MismatchedNonMatchingPreferredUnionRow =
  | {
      readonly id: string;
      readonly _tag: "received-a";
      readonly kind: "a";
      readonly left: number;
    }
  | {
      readonly id: string;
      readonly _tag: "received-b";
      readonly kind: "b";
      readonly right: string;
    };
declare const mismatchedNonMatchingPreferredUnionInitial: MismatchedNonMatchingPreferredUnionRow;
declare const usePartialRouteSchema: boolean;
type OptionalUndefinedNoteRow = {
  readonly id: string;
  readonly note?: string | undefined;
};
declare const indexedStringRow: Record<string, string>;
declare const forgedNever: never;
declare const optionalMalformedSourceTopic: {
  readonly schema: typeof Row;
  readonly source?: {};
};
type InputFromPublicTopicConstraint<Topics extends ViewServerConfigTopicInputShape> =
  DefineViewServerConfigInput<Topics>;
type MappedDefinitionOptions<SourceRow extends object> = {
  readonly stream: string;
  readonly initial: SourceRow;
};
interface MappedDefinitionOptionsFamily extends SourceDefinitionOptionsFamily {
  readonly type: MappedDefinitionOptions<this["Row"]>;
}
const mappedAdapter = SourceAdapter.make({
  identity: { name: "config-mapped-type-source" },
  failure: Failure,
  materialized: {
    ...Declaration,
    definitionOptions: SourceAdapter.definitionOptionsFamily<MappedDefinitionOptionsFamily>(),
  },
  leased: undefined,
});
const mappedSource = <SourceRow extends object>(stream: string, initial: SourceRow) =>
  mappedAdapter.materializedSource<SourceRow>({ stream, initial });

const config = defineViewServerConfig({
  topics: {
    all: {
      schema: Row,
      source: adapter.materializedSource({ stream: "all" }),
    },
    routed: {
      schema: Row,
      source: adapter.leasedSource(["region", "shard"], { stream: "routed" }),
    },
  },
});
const sourceFreeConfig = defineViewServerConfig({
  topics: {
    manual: {
      schema: Row,
    },
  },
});
declare const alternateValidTopics:
  | { readonly orders: { readonly schema: typeof Row } }
  | { readonly trades: { readonly schema: typeof Row } };
const alternateValidConfig = defineViewServerConfig({ topics: alternateValidTopics });
declare const validOrMalformedSupersetTopics:
  | {
      readonly good: { readonly schema: typeof Row };
      readonly bad: null;
    }
  | { readonly good: { readonly schema: typeof Row } };
declare const validOrCallableSameKeyTopics:
  | {
      readonly orders: (() => void) & { readonly schema: typeof Row };
    }
  | { readonly orders: { readonly schema: typeof Row } };
declare const validOrCallableTopicValue: {
  readonly orders:
    | ((() => void) & { readonly schema: typeof Row })
    | { readonly schema: typeof Row };
};
declare const widenedInvalidTopics: Record<string, { readonly schema: typeof NumberIdSchema }>;
declare const widenedValidTopics: Record<string, { readonly schema: typeof Row }>;
declare const useLeasedSource: boolean;
declare const useRegionRoute: boolean;
declare const useExtraFieldSource: boolean;
const mixedLifecycleConfig = defineViewServerConfig({
  topics: {
    mixed: {
      schema: Row,
      source: useLeasedSource
        ? adapter.leasedSource(["region", "shard"], { stream: "mixed-leased" })
        : adapter.materializedSource({ stream: "mixed-materialized" }),
    },
  },
});
const mixedLeasedRoutesConfig = defineViewServerConfig({
  topics: {
    mixedRoutes: {
      schema: Row,
      source: useRegionRoute
        ? adapter.leasedSource(["region"], { stream: "mixed-region" })
        : adapter.leasedSource(["shard"], { stream: "mixed-shard" }),
    },
  },
});
const mappedConfig = defineViewServerConfig({
  topics: {
    mapped: {
      schema: Row,
      source: mappedSource("mapped", {
        id: "initial",
        region: "eu",
        shard: 1n,
      }),
    },
  },
});
const NestedRow = Schema.Struct({
  id: ViewServerId,
  metadata: Schema.Struct({
    region: Schema.String,
    tags: Schema.Array(
      Schema.Struct({
        name: Schema.String,
      }),
    ),
  }),
});
const nestedMappedConfig = defineViewServerConfig({
  topics: {
    nested: {
      schema: NestedRow,
      source: mappedSource("nested", {
        id: "initial",
        metadata: {
          region: "eu",
          tags: [{ name: "primary" }],
        },
      }),
    },
  },
});
type MaterializedHealth = SourceHealthForDefinition<
  typeof config.topics.all.source,
  TopicRow<typeof config.topics, "all">
>;
type LeasedHealth = SourceHealthForDefinition<
  typeof config.topics.routed.source,
  TopicRow<typeof config.topics, "routed">
>;
type MixedDefinition = typeof mixedLifecycleConfig.topics.mixed.source;
type MixedMaterializedHealth = SourceHealthForDefinition<
  Extract<MixedDefinition, { readonly lifecycle: "materialized" }>,
  TopicRow<typeof mixedLifecycleConfig.topics, "mixed">
>;
type MixedLeasedHealth = SourceHealthForDefinition<
  Extract<MixedDefinition, { readonly lifecycle: "leased" }>,
  TopicRow<typeof mixedLifecycleConfig.topics, "mixed">
>;
declare const materializedHealth: MaterializedHealth;
declare const leasedHealth: LeasedHealth;
declare const sourceFreeHealth: ViewServerHealth<typeof sourceFreeConfig.topics>;

describe("Source Adapter config type contracts", () => {
  it("infers canonical ids, exact routes, and source definitions without as const", () => {
    expectTypeOf<typeof ViewServerId.Type>().toEqualTypeOf<string>();
    expectTypeOf<typeof ViewServerId.Encoded>().toEqualTypeOf<string>();
    expectTypeOf(config.topics.all.schema.fields.id).toEqualTypeOf<typeof ViewServerId>();
    expectTypeOf<
      InputFromPublicTopicConstraint<typeof config.topics>["topics"]["all"]["schema"]
    >().toEqualTypeOf<typeof Row>();
    expectTypeOf(sourceFreeConfig.topics.manual.schema.fields.id).toEqualTypeOf<
      typeof ViewServerId
    >();
    expectTypeOf(alternateValidConfig.topics).toEqualTypeOf<typeof alternateValidTopics>();
    defineViewServerConfig({
      // @ts-expect-error Every union registry branch must be valid independently.
      topics: validOrMalformedSupersetTopics,
    });
    expectTypeOf<ViewServerConfig<typeof validOrCallableSameKeyTopics>>().toEqualTypeOf<never>();
    defineViewServerConfig({
      // @ts-expect-error Same-key union registry branches validate independently.
      topics: validOrCallableSameKeyTopics,
    });
    expectTypeOf<ViewServerConfig<typeof validOrCallableTopicValue>>().toEqualTypeOf<never>();
    defineViewServerConfig({
      topics: {
        // @ts-expect-error Every union member of one topic value validates independently.
        orders: validOrCallableTopicValue.orders,
      },
    });
    expectTypeOf<ViewServerConfig<typeof widenedValidTopics>>().toEqualTypeOf<never>();
    defineViewServerConfig({
      // @ts-expect-error Widened registries cannot exclude reserved system topic names.
      topics: widenedValidTopics,
    });
    type WidenedInvalidInput = DefineViewServerConfigInput<typeof widenedInvalidTopics>;
    expectTypeOf<ViewServerConfig<typeof widenedInvalidTopics>>().toEqualTypeOf<never>();
    expectTypeOf<WidenedInvalidInput["topics"][string]["__viewServerConfigError"]>().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: string;
      readonly reason: "topic schema must define id as ViewServerId";
      readonly details: {
        readonly field: "id";
        readonly expected: typeof ViewServerId;
        readonly received: typeof NumberIdSchema.fields.id;
      };
    }>();
    defineViewServerConfig({
      // @ts-expect-error Widened registries still validate their topic value type.
      topics: widenedInvalidTopics,
    });
    // @ts-expect-error Topic configuration never exposes a configurable key.
    void config.topics.all.key;
    // @ts-expect-error Source-free Topic configuration never exposes a configurable key.
    void sourceFreeConfig.topics.manual.key;
    expectTypeOf(mappedConfig.topics.mapped.source.lifecycle).toEqualTypeOf<"materialized">();
    expectTypeOf(mappedConfig.topics.mapped.source.options.stream).toEqualTypeOf<string>();
    expectTypeOf(mappedConfig.topics.mapped.source.options.initial.id).toEqualTypeOf<string>();
    expectTypeOf(mappedConfig.topics.mapped.source.options.initial.region).toEqualTypeOf<string>();
    expectTypeOf(mappedConfig.topics.mapped.source.options.initial.shard).toEqualTypeOf<bigint>();
    expectTypeOf<
      SourceDefinitionRow<typeof mappedConfig.topics.mapped.source>["id"]
    >().toEqualTypeOf<string>();
    expectTypeOf<
      SourceDefinitionRow<typeof mappedConfig.topics.mapped.source>["region"]
    >().toEqualTypeOf<string>();
    expectTypeOf<
      SourceDefinitionRow<typeof mappedConfig.topics.mapped.source>["shard"]
    >().toEqualTypeOf<bigint>();
    expectTypeOf<
      SourceDefinitionRouteFields<typeof mappedConfig.topics.mapped.source>
    >().toEqualTypeOf<readonly []>();
    expectTypeOf(nestedMappedConfig.topics.nested.source.lifecycle).toEqualTypeOf<"materialized">();
    expectTypeOf(
      nestedMappedConfig.topics.nested.source.options.initial.metadata.region,
    ).toEqualTypeOf<string>();
    expectTypeOf(
      nestedMappedConfig.topics.nested.source.options.initial.metadata.tags[0]?.name,
    ).toEqualTypeOf<string | undefined>();
    expectTypeOf<
      SourceDefinitionRow<typeof nestedMappedConfig.topics.nested.source>["id"]
    >().toEqualTypeOf<string>();
    expectTypeOf<
      SourceDefinitionRow<typeof nestedMappedConfig.topics.nested.source>["metadata"]["region"]
    >().toEqualTypeOf<string>();
    expectTypeOf<
      SourceDefinitionRow<
        typeof nestedMappedConfig.topics.nested.source
      >["metadata"]["tags"][number]["name"]
    >().toEqualTypeOf<string>();
    expectTypeOf<
      SourceDefinitionRouteFields<typeof nestedMappedConfig.topics.nested.source>
    >().toEqualTypeOf<readonly []>();
    expectTypeOf(config.topics.routed.source.routeBy).toEqualTypeOf<readonly ["region", "shard"]>();
    expectTypeOf<ViewServerHealth<typeof config.topics>["sources"]["all"]>().toEqualTypeOf<
      MaterializedHealth | undefined
    >();
    expectTypeOf<ViewServerHealth<typeof config.topics>["sources"]["routed"]>().toEqualTypeOf<
      ReadonlyArray<LeasedHealth>
    >();
    expectTypeOf<
      NonNullable<ViewServerHealth<typeof config.topics>["sources"]["all"]>["metrics"]["adapter"]
    >().toEqualTypeOf<{ readonly connected: boolean }>();
    expectTypeOf<
      ViewServerHealth<typeof mixedLifecycleConfig.topics>["sources"]["mixed"]
    >().toEqualTypeOf<MixedMaterializedHealth | ReadonlyArray<MixedLeasedHealth> | undefined>();
    expectTypeOf<
      keyof ViewServerHealth<typeof sourceFreeConfig.topics>["sources"]
    >().toEqualTypeOf<never>();

    const validSourceHealth: ViewServerSourceHealth<typeof config.topics> = {
      all: materializedHealth,
      routed: [leasedHealth],
    };
    expectTypeOf(validSourceHealth.routed).toEqualTypeOf<ReadonlyArray<LeasedHealth>>();

    const pendingMaterializedHealth: ViewServerSourceHealth<typeof config.topics> = {
      routed: [leasedHealth],
    };
    expectTypeOf(pendingMaterializedHealth.all).toEqualTypeOf<MaterializedHealth | undefined>();
    expectTypeOf(pendingMaterializedHealth.routed).toEqualTypeOf<ReadonlyArray<LeasedHealth>>();

    const invalidLeasedHealth: ViewServerSourceHealth<typeof config.topics> = {
      all: materializedHealth,
      // @ts-expect-error Leased aggregate health is the exact active-health array.
      routed: leasedHealth,
    };
    expectTypeOf(invalidLeasedHealth.all).toEqualTypeOf<MaterializedHealth | undefined>();

    // @ts-expect-error Source-free Topics have no canonical aggregate Source Health key.
    void sourceFreeHealth.sources.manual;

    const valid: ExactLiveQueryInputForTopic<
      typeof config.topics,
      "routed",
      {
        readonly select: readonly ["id"];
        readonly routeBy: {
          readonly region: string;
          readonly shard: bigint;
        };
      }
    > = {
      select: ["id"],
      routeBy: {
        region: "eu",
        shard: 7n,
      },
    };
    expectTypeOf(valid.routeBy.shard).toEqualTypeOf<bigint>();

    const mixedLeasedQuery: ExactLiveQueryInputForTopic<
      typeof mixedLifecycleConfig.topics,
      "mixed",
      {
        readonly select: readonly ["id"];
        readonly routeBy: {
          readonly region: string;
          readonly shard: bigint;
        };
      }
    > = {
      select: ["id"],
      routeBy: {
        region: "eu",
        shard: 7n,
      },
    };
    expectTypeOf(mixedLeasedQuery.routeBy.shard).toEqualTypeOf<bigint>();

    const mixedMaterializedQuery: ExactLiveQueryInputForTopic<
      typeof mixedLifecycleConfig.topics,
      "mixed",
      { readonly select: readonly ["id"] }
    > = {
      select: ["id"],
    };
    expectTypeOf(mixedMaterializedQuery).not.toBeAny();

    // @ts-expect-error A conditional Source still rejects routes outside its leased contract.
    const mixedInvalidRoute: ExactLiveQueryInputForTopic<
      typeof mixedLifecycleConfig.topics,
      "mixed",
      {
        readonly select: readonly ["id"];
        readonly routeBy: {
          readonly region: string;
        };
      }
    > = {
      select: ["id"],
      routeBy: {
        region: "eu",
      },
    };
    expectTypeOf(mixedInvalidRoute).not.toBeAny();

    const mixedRegionRoute: ExactLiveQueryInputForTopic<
      typeof mixedLeasedRoutesConfig.topics,
      "mixedRoutes",
      {
        readonly select: readonly ["id"];
        readonly routeBy: {
          readonly region: string;
        };
      }
    > = {
      select: ["id"],
      routeBy: {
        region: "eu",
      },
    };
    expectTypeOf(mixedRegionRoute.routeBy.region).toEqualTypeOf<string>();

    const mixedShardRoute: ExactLiveQueryInputForTopic<
      typeof mixedLeasedRoutesConfig.topics,
      "mixedRoutes",
      {
        readonly select: readonly ["id"];
        readonly routeBy: {
          readonly shard: bigint;
        };
      }
    > = {
      select: ["id"],
      routeBy: {
        shard: 7n,
      },
    };
    expectTypeOf(mixedShardRoute.routeBy.shard).toEqualTypeOf<bigint>();

    // @ts-expect-error Conditional leased routes accept one exact branch, never their union.
    const mixedCombinedRoute: ExactLiveQueryInputForTopic<
      typeof mixedLeasedRoutesConfig.topics,
      "mixedRoutes",
      {
        readonly select: readonly ["id"];
        readonly routeBy: {
          readonly region: string;
          readonly shard: bigint;
        };
      }
    > = {
      select: ["id"],
      routeBy: {
        region: "eu",
        shard: 7n,
      },
    };
    expectTypeOf(mixedCombinedRoute).not.toBeAny();

    // @ts-expect-error Every branch of a conditional leased Source requires its route.
    const mixedLeasedMissingRoute: ExactLiveQueryInputForTopic<
      typeof mixedLeasedRoutesConfig.topics,
      "mixedRoutes",
      { readonly select: readonly ["id"] }
    > = {
      select: ["id"],
    };
    expectTypeOf(mixedLeasedMissingRoute).not.toBeAny();
  });

  it("rejects keys, invalid routes, and source-owner conflicts", () => {
    defineViewServerConfig({
      topics: {
        keyed: {
          schema: Row,
          // @ts-expect-error Every Topic rejects the removed configurable key.
          key: "id",
          source: adapter.materializedSource({ stream: "all" }),
        },
      },
    });

    defineViewServerConfig({
      topics: {
        invalidRoute: {
          schema: Row,
          // @ts-expect-error Reports topic "invalidRoute" and field "missing".
          source: adapter.leasedSource(["missing"], { stream: "routed" }),
        },
      },
    });

    defineViewServerConfig({
      topics: {
        conflicting: {
          schema: Row,
          source: adapter.materializedSource({ stream: "all" }),
          // @ts-expect-error Legacy source owners are removed.
          grpcSource: {},
        },
      },
    });

    defineViewServerConfig({
      topics: {
        conflictingKafka: {
          schema: Row,
          source: adapter.materializedSource({ stream: "all" }),
          // @ts-expect-error Legacy Kafka source owners are removed.
          kafkaSource: {},
        },
      },
    });

    defineViewServerConfig({
      topics: {
        // @ts-expect-error Reports topic "missingId" and the missing canonical "id" field.
        missingId: {
          schema: Schema.Struct({ region: Schema.String }),
          source: adapter.materializedSource({ stream: "all" }),
        },
      },
    });

    defineViewServerConfig({
      topics: {
        // @ts-expect-error canonical Source-owned ids may not be optional.
        optionalId: {
          schema: Schema.Struct({
            id: Schema.optionalKey(Schema.String),
            region: Schema.String,
          }),
          source: adapter.materializedSource({ stream: "all" }),
        },
      },
    });

    defineViewServerConfig({
      topics: {
        // @ts-expect-error canonical Source-owned ids may not be numbers.
        numberId: {
          schema: Schema.Struct({ id: Schema.Number, region: Schema.String }),
          source: adapter.materializedSource({ stream: "all" }),
        },
      },
    });

    defineViewServerConfig({
      topics: {
        // @ts-expect-error canonical Source-owned ids may not be branded.
        brandedId: {
          schema: Schema.Struct({
            id: ViewServerId.pipe(Schema.brand("SourceId")),
            region: Schema.String,
          }),
          source: adapter.materializedSource({ stream: "all" }),
        },
      },
    });

    defineViewServerConfig({
      topics: {
        // @ts-expect-error canonical ids must use the nominal ViewServerId schema.
        plainStringId: {
          schema: Schema.Struct({ id: Schema.String, region: Schema.String }),
          source: adapter.materializedSource({ stream: "all" }),
        },
      },
    });

    defineViewServerConfig({
      topics: {
        // @ts-expect-error canonical Source-owned ids may not be transformations.
        transformedId: {
          schema: Schema.Struct({ id: Schema.Trim, region: Schema.String }),
          source: adapter.materializedSource({ stream: "all" }),
        },
      },
    });

    defineViewServerConfig({
      topics: {
        // @ts-expect-error canonical Source-owned ids may not be refinements.
        refinedId: {
          schema: Schema.Struct({ id: Schema.NonEmptyString, region: Schema.String }),
          source: adapter.materializedSource({ stream: "all" }),
        },
      },
    });

    defineViewServerConfig({
      topics: {
        // @ts-expect-error Source-free Topics also require the canonical id.
        missingManualId: {
          schema: Schema.Struct({ region: Schema.String }),
        },
      },
    });

    defineViewServerConfig({
      topics: {
        missingMaterializedField: {
          schema: Row,
          // @ts-expect-error Reports topic "missingMaterializedField" and missing field "shard".
          source: adapter.materializedSource<MissingFieldRow>({ stream: "all" }),
        },
      },
    });

    const unsafeMaterializedSource = adapter.materializedSource<any>({ stream: "all" });
    type UnsafeMaterializedInput = DefineViewServerConfigInput<{
      readonly unsafeMaterializedRow: {
        readonly schema: typeof Row;
        readonly source: typeof unsafeMaterializedSource;
      };
    }>;
    expectTypeOf<
      UnsafeMaterializedInput["topics"]["unsafeMaterializedRow"]["source"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "unsafeMaterializedRow";
      readonly reason: "source row type must not be any or unknown";
      readonly details: { readonly received: "any" };
    }>();
    defineViewServerConfig({
      topics: {
        unsafeMaterializedRow: {
          schema: Row,
          // @ts-expect-error Reports the unsafe row on topic "unsafeMaterializedRow".
          source: unsafeMaterializedSource,
        },
      },
    });

    defineViewServerConfig({
      topics: {
        extraMaterializedField: {
          schema: Row,
          // @ts-expect-error Reports topic "extraMaterializedField" and unexpected field "extra".
          source: adapter.materializedSource<ExtraFieldRow>({ stream: "all" }),
        },
      },
    });

    defineViewServerConfig({
      topics: {
        differentMaterializedFieldType: {
          schema: Row,
          // @ts-expect-error Reports topic "differentMaterializedFieldType" and field "shard".
          source: adapter.materializedSource<DifferentFieldTypeRow>({ stream: "all" }),
        },
      },
    });

    defineViewServerConfig({
      topics: {
        missingLeasedField: {
          schema: Row,
          // @ts-expect-error Reports topic "missingLeasedField" and missing field "shard".
          source: adapter.leasedSource<readonly ["id"], MissingFieldRow>(["id"], {
            stream: "routed",
          }),
        },
      },
    });

    defineViewServerConfig({
      topics: {
        unsafeLeasedRow: {
          schema: Row,
          // @ts-expect-error Reports the unsafe row on topic "unsafeLeasedRow".
          source: adapter.leasedSource<readonly ["id"], any>(["id"], {
            stream: "routed",
          }),
        },
      },
    });

    defineViewServerConfig({
      topics: {
        extraLeasedField: {
          schema: Row,
          // @ts-expect-error Reports topic "extraLeasedField" and unexpected field "extra".
          source: adapter.leasedSource<readonly ["id"], ExtraFieldRow>(["id"], {
            stream: "routed",
          }),
        },
      },
    });

    defineViewServerConfig({
      topics: {
        differentLeasedFieldType: {
          schema: Row,
          // @ts-expect-error Reports topic "differentLeasedFieldType" and field "shard".
          source: adapter.leasedSource<readonly ["id"], DifferentFieldTypeRow>(["id"], {
            stream: "routed",
          }),
        },
      },
    });
  });

  it("reports the topic and differing field for source row mismatches", () => {
    const NumberStatusRow = Schema.Struct({
      id: ViewServerId,
      status: Schema.Number,
    });
    const enumStatusSource = mappedSource<GeneratedEnumRow>("enum-status", {
      id: "event-1",
      status: GeneratedStatus.Pending,
    });
    type EnumStatusInput = DefineViewServerConfigInput<{
      readonly enumStatus: {
        readonly schema: typeof NumberStatusRow;
        readonly source: typeof enumStatusSource;
      };
    }>;
    expectTypeOf<
      EnumStatusInput["topics"]["enumStatus"]["source"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "enumStatus";
      readonly reason: "source row does not match topic schema row";
      readonly details: {
        readonly field: "status";
        readonly expected: number;
        readonly received: GeneratedStatus;
      };
    }>();
    defineViewServerConfig({
      topics: {
        enumStatus: {
          schema: NumberStatusRow,
          // @ts-expect-error Reports topic "enumStatus", field "status", expected number, and received GeneratedStatus.
          source: enumStatusSource,
        },
      },
    });

    const StringRegionRow = Schema.Struct({
      id: ViewServerId,
      region: Schema.String,
    });
    const literalRegionSource = mappedSource<LiteralRegionRow>("literal-region", {
      id: "event-1",
      region: "eu",
    });
    type LiteralRegionInput = DefineViewServerConfigInput<{
      readonly literalRegion: {
        readonly schema: typeof StringRegionRow;
        readonly source: typeof literalRegionSource;
      };
    }>;
    expectTypeOf<
      LiteralRegionInput["topics"]["literalRegion"]["source"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "literalRegion";
      readonly reason: "source row does not match topic schema row";
      readonly details: {
        readonly field: "region";
        readonly expected: string;
        readonly received: "eu" | "us";
      };
    }>();
    defineViewServerConfig({
      topics: {
        literalRegion: {
          schema: StringRegionRow,
          // @ts-expect-error Reports topic "literalRegion", field "region", expected string, and the received literal union.
          source: literalRegionSource,
        },
      },
    });

    const missingIdSource = mappedSource<MissingIdRow>("missing-id", { region: "eu" });
    type MissingIdInput = DefineViewServerConfigInput<{
      readonly missingId: {
        readonly schema: typeof StringRegionRow;
        readonly source: typeof missingIdSource;
      };
    }>;
    expectTypeOf<
      MissingIdInput["topics"]["missingId"]["source"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "missingId";
      readonly reason: "source row does not match topic schema row";
      readonly details: {
        readonly field: "id";
        readonly expected: string;
        readonly received: "missing";
        readonly receivedPresent: false;
      };
    }>();
    defineViewServerConfig({
      topics: {
        missingId: {
          schema: StringRegionRow,
          // @ts-expect-error Reports topic "missingId" and the missing source row field "id".
          source: missingIdSource,
        },
      },
    });

    const narrowIdSource = mappedSource<NarrowIdRow>("narrow-id", {
      id: "user:1",
      region: "eu",
    });
    type NarrowIdInput = DefineViewServerConfigInput<{
      readonly narrowId: {
        readonly schema: typeof StringRegionRow;
        readonly source: typeof narrowIdSource;
      };
    }>;
    expectTypeOf<
      NarrowIdInput["topics"]["narrowId"]["source"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "narrowId";
      readonly reason: "source row does not match topic schema row";
      readonly details: {
        readonly field: "id";
        readonly expected: string;
        readonly received: `user:${string}`;
      };
    }>();
    defineViewServerConfig({
      topics: {
        narrowId: {
          schema: StringRegionRow,
          // @ts-expect-error Reports topic "narrowId", field "id", expected string, and the received template-literal type.
          source: narrowIdSource,
        },
      },
    });
  });

  it("preserves diagnostic details for optional, union, route, and malformed inputs", () => {
    const OptionalNoteRow = Schema.Struct({
      id: ViewServerId,
      note: Schema.optionalKey(Schema.String),
    });
    const requiredUndefinedNoteSource = mappedSource<RequiredUndefinedNoteRow>("optional-note", {
      id: "event-1",
      note: undefined,
    });
    type OptionalNoteInput = DefineViewServerConfigInput<{
      readonly optionalNote: {
        readonly schema: typeof OptionalNoteRow;
        readonly source: typeof requiredUndefinedNoteSource;
      };
    }>;
    expectTypeOf<
      OptionalNoteInput["topics"]["optionalNote"]["source"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "optionalNote";
      readonly reason: "source row does not match topic schema row";
      readonly details: {
        readonly field: "note";
        readonly expected: string;
        readonly received: string | undefined;
        readonly expectedOptional: true;
        readonly receivedOptional: false;
      };
    }>();
    defineViewServerConfig({
      topics: {
        optionalNote: {
          schema: OptionalNoteRow,
          // @ts-expect-error Required `undefined` differs from an optional property.
          source: requiredUndefinedNoteSource,
        },
      },
    });

    const IdOnlyRow = Schema.Struct({ id: ViewServerId });
    const exclusiveUnionSource = mappedSource<ExclusiveUnionRow>(
      "exclusive-union",
      exclusiveUnionInitial,
    );
    type ExclusiveUnionInput = DefineViewServerConfigInput<{
      readonly exclusiveUnion: {
        readonly schema: typeof IdOnlyRow;
        readonly source: typeof exclusiveUnionSource;
      };
    }>;
    expectTypeOf<
      ExclusiveUnionInput["topics"]["exclusiveUnion"]["source"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "exclusiveUnion";
      readonly reason: "source row does not match topic schema row";
      readonly details:
        | {
            readonly field: "left";
            readonly expected: "absent";
            readonly expectedPresent: false;
            readonly received: string;
          }
        | {
            readonly field: "right";
            readonly expected: "absent";
            readonly expectedPresent: false;
            readonly received: number;
          };
    }>();
    defineViewServerConfig({
      topics: {
        exclusiveUnion: {
          schema: IdOnlyRow,
          // @ts-expect-error Union-exclusive fields remain visible in the diagnostic.
          source: exclusiveUnionSource,
        },
      },
    });

    const idOnlySource = mappedSource("union-schema", { id: "id" });
    type UnionSchemaInput = DefineViewServerConfigInput<{
      readonly unionSchema: {
        readonly schema: typeof exclusiveUnionSchema;
        readonly source: typeof idOnlySource;
      };
    }>;
    expectTypeOf<
      UnionSchemaInput["topics"]["unionSchema"]["source"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "unionSchema";
      readonly reason: "source row does not match topic schema row";
      readonly details:
        | {
            readonly field: "left";
            readonly expected: string;
            readonly received: "missing";
            readonly receivedPresent: false;
          }
        | {
            readonly field: "right";
            readonly expected: number;
            readonly received: "missing";
            readonly receivedPresent: false;
          };
    }>();
    defineViewServerConfig({
      topics: {
        unionSchema: {
          schema: exclusiveUnionSchema,
          // @ts-expect-error Schema-union fields remain visible in the diagnostic.
          source: idOnlySource,
        },
      },
    });

    const partiallySharedUnionSource = mappedSource<PartiallySharedUnionRow>(
      "partially-shared-union",
      partiallySharedUnionInitial,
    );
    type PartiallySharedUnionInput = DefineViewServerConfigInput<{
      readonly partiallySharedUnion: {
        readonly schema: typeof exclusiveUnionSchema;
        readonly source: typeof partiallySharedUnionSource;
      };
    }>;
    expectTypeOf<
      PartiallySharedUnionInput["topics"]["partiallySharedUnion"]["source"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "partiallySharedUnion";
      readonly reason: "source row does not match topic schema row";
      readonly details:
        | {
            readonly field: "right";
            readonly expected: number;
            readonly received: "missing";
            readonly receivedPresent: false;
          }
        | {
            readonly field: "other";
            readonly expected: "absent";
            readonly expectedPresent: false;
            readonly received: boolean;
          };
    }>();
    defineViewServerConfig({
      topics: {
        partiallySharedUnion: {
          schema: exclusiveUnionSchema,
          // @ts-expect-error Shared union members do not pollute mismatch details.
          source: partiallySharedUnionSource,
        },
      },
    });

    const mismatchedDiscriminatedUnionSource = mappedSource<MismatchedDiscriminatedUnionRow>(
      "mismatched-discriminated-union",
      mismatchedDiscriminatedUnionInitial,
    );
    type MismatchedDiscriminatedUnionInput = DefineViewServerConfigInput<{
      readonly mismatched: {
        readonly schema: typeof discriminatedUnionSchema;
        readonly source: typeof mismatchedDiscriminatedUnionSource;
      };
    }>;
    expectTypeOf<
      MismatchedDiscriminatedUnionInput["topics"]["mismatched"]["source"]["__viewServerConfigError"]["details"]
    >().toEqualTypeOf<
      | { readonly field: "left"; readonly expected: string; readonly received: number }
      | { readonly field: "right"; readonly expected: number; readonly received: string }
    >();
    defineViewServerConfig({
      topics: {
        mismatched: {
          schema: discriminatedUnionSchema,
          // @ts-expect-error Discriminated union diagnostics correlate matching variants.
          source: mismatchedDiscriminatedUnionSource,
        },
      },
    });

    const mismatchedDiscriminatedUnionWithExtraVariantSource =
      mappedSource<MismatchedDiscriminatedUnionWithExtraVariantRow>(
        "mismatched-discriminated-union-with-extra-variant",
        mismatchedDiscriminatedUnionWithExtraVariantInitial,
      );
    type MismatchedDiscriminatedUnionWithExtraVariantInput = DefineViewServerConfigInput<{
      readonly mismatchedWithExtraVariant: {
        readonly schema: typeof discriminatedUnionSchema;
        readonly source: typeof mismatchedDiscriminatedUnionWithExtraVariantSource;
      };
    }>;
    expectTypeOf<
      MismatchedDiscriminatedUnionWithExtraVariantInput["topics"]["mismatchedWithExtraVariant"]["source"]["__viewServerConfigError"]["details"]
    >().toEqualTypeOf<
      | { readonly field: "left"; readonly expected: string; readonly received: number }
      | { readonly field: "right"; readonly expected: number; readonly received: string }
      | {
          readonly field: "kind";
          readonly expected: "a" | "b";
          readonly received: "c";
        }
    >();
    defineViewServerConfig({
      topics: {
        mismatchedWithExtraVariant: {
          schema: discriminatedUnionSchema,
          // @ts-expect-error Shared discriminator values correlate before reporting an extra variant.
          source: mismatchedDiscriminatedUnionWithExtraVariantSource,
        },
      },
    });

    const exactRowSource = mappedSource<typeof Row.Type>("exact-row-union-member", {
      id: "event-1",
      region: "eu",
      shard: 1n,
    });
    const extraFieldSource = mappedSource<ExtraFieldRow>(
      "extra-field-union-member",
      extraFieldInitial,
    );
    const exactOrExtraFieldSource = useExtraFieldSource ? extraFieldSource : exactRowSource;
    type ExactOrExtraFieldSourceTopics = {
      readonly mixedSource: {
        readonly schema: typeof Row;
        readonly source: typeof exactOrExtraFieldSource;
      };
    };
    expectTypeOf<ViewServerConfig<ExactOrExtraFieldSourceTopics>>().toEqualTypeOf<never>();
    defineViewServerConfig({
      // @ts-expect-error Every source-union member must match the schema row exactly.
      topics: {
        mixedSource: {
          schema: Row,
          source: exactOrExtraFieldSource,
        },
      },
    });
    const exactOrUndefinedSource = useExtraFieldSource ? exactRowSource : undefined;
    const exactOrUndefinedConfig = defineViewServerConfig({
      topics: {
        optionalSourceValue: {
          schema: Row,
          source: exactOrUndefinedSource,
        },
      },
    });
    expectTypeOf(exactOrUndefinedConfig.topics.optionalSourceValue.source).toEqualTypeOf<
      typeof exactRowSource | undefined
    >();
    const optionalExactOrExtraSourceTopic: {
      readonly schema: typeof Row;
      readonly source?: typeof exactOrExtraFieldSource;
    } = { schema: Row, source: exactOrExtraFieldSource };
    defineViewServerConfig({
      // @ts-expect-error Optional source unions still validate every present member.
      topics: { optionalMixedSource: optionalExactOrExtraSourceTopic },
    });

    const mismatchedRepeatedRegionUnionSource = mappedSource<MismatchedRepeatedRegionUnionRow>(
      "mismatched-repeated-region-union",
      mismatchedRepeatedRegionUnionInitial,
    );
    type MismatchedRepeatedRegionUnionInput = DefineViewServerConfigInput<{
      readonly mismatchedRepeatedRegion: {
        readonly schema: typeof repeatedRegionUnionSchema;
        readonly source: typeof mismatchedRepeatedRegionUnionSource;
      };
    }>;
    expectTypeOf<
      MismatchedRepeatedRegionUnionInput["topics"]["mismatchedRepeatedRegion"]["source"]["__viewServerConfigError"]["details"]
    >().toEqualTypeOf<
      | { readonly field: "left"; readonly expected: string; readonly received: number }
      | { readonly field: "right"; readonly expected: number; readonly received: string }
      | { readonly field: "tail"; readonly expected: boolean; readonly received: string }
    >();
    defineViewServerConfig({
      topics: {
        mismatchedRepeatedRegion: {
          schema: repeatedRegionUnionSchema,
          // @ts-expect-error Only unique discriminants correlate union variants.
          source: mismatchedRepeatedRegionUnionSource,
        },
      },
    });

    const mismatchedConflictingDiscriminatorUnionSource =
      mappedSource<MismatchedConflictingDiscriminatorUnionRow>(
        "mismatched-conflicting-discriminator-union",
        mismatchedConflictingDiscriminatorUnionInitial,
      );
    type MismatchedConflictingDiscriminatorUnionInput = DefineViewServerConfigInput<{
      readonly mismatchedConflictingDiscriminator: {
        readonly schema: typeof conflictingDiscriminatorUnionSchema;
        readonly source: typeof mismatchedConflictingDiscriminatorUnionSource;
      };
    }>;
    expectTypeOf<
      MismatchedConflictingDiscriminatorUnionInput["topics"]["mismatchedConflictingDiscriminator"]["source"]["__viewServerConfigError"]["details"]
    >().toEqualTypeOf<
      | { readonly field: "code"; readonly expected: "x"; readonly received: "y" }
      | { readonly field: "left"; readonly expected: string; readonly received: number }
      | { readonly field: "code"; readonly expected: "y"; readonly received: "x" }
      | { readonly field: "right"; readonly expected: number; readonly received: string }
    >();
    defineViewServerConfig({
      topics: {
        mismatchedConflictingDiscriminator: {
          schema: conflictingDiscriminatorUnionSchema,
          // @ts-expect-error A single preferred discriminator correlates every union member.
          source: mismatchedConflictingDiscriminatorUnionSource,
        },
      },
    });

    const mismatchedNonMatchingPreferredUnionSource =
      mappedSource<MismatchedNonMatchingPreferredUnionRow>(
        "mismatched-non-matching-preferred-union",
        mismatchedNonMatchingPreferredUnionInitial,
      );
    type MismatchedNonMatchingPreferredUnionInput = DefineViewServerConfigInput<{
      readonly mismatchedNonMatchingPreferred: {
        readonly schema: typeof nonMatchingPreferredUnionSchema;
        readonly source: typeof mismatchedNonMatchingPreferredUnionSource;
      };
    }>;
    expectTypeOf<
      MismatchedNonMatchingPreferredUnionInput["topics"]["mismatchedNonMatchingPreferred"]["source"]["__viewServerConfigError"]["details"]
    >().toEqualTypeOf<
      | {
          readonly field: "_tag";
          readonly expected: "expected-a";
          readonly received: "received-a";
        }
      | { readonly field: "left"; readonly expected: string; readonly received: number }
      | {
          readonly field: "_tag";
          readonly expected: "expected-b";
          readonly received: "received-b";
        }
      | { readonly field: "right"; readonly expected: number; readonly received: string }
    >();
    defineViewServerConfig({
      topics: {
        mismatchedNonMatchingPreferred: {
          schema: nonMatchingPreferredUnionSchema,
          // @ts-expect-error Correlation skips unique fields without matching values.
          source: mismatchedNonMatchingPreferredUnionSource,
        },
      },
    });

    const OptionalUndefinedNoteSchema = Schema.Struct({
      id: ViewServerId,
      note: Schema.optionalKey(Schema.String),
    });
    const optionalUndefinedNoteSource = mappedSource<OptionalUndefinedNoteRow>(
      "optional-undefined-note",
      { id: "event-1" },
    );
    type OptionalUndefinedNoteInput = DefineViewServerConfigInput<{
      readonly optionalUndefinedNote: {
        readonly schema: typeof OptionalUndefinedNoteSchema;
        readonly source: typeof optionalUndefinedNoteSource;
      };
    }>;
    expectTypeOf<
      OptionalUndefinedNoteInput["topics"]["optionalUndefinedNote"]["source"]["__viewServerConfigError"]["details"]
    >().toEqualTypeOf<{
      readonly field: "note";
      readonly expected: string;
      readonly received: string | undefined;
    }>();
    defineViewServerConfig({
      topics: {
        optionalUndefinedNote: {
          schema: OptionalUndefinedNoteSchema,
          // @ts-expect-error Explicit undefined differs from an absent optional property.
          source: optionalUndefinedNoteSource,
        },
      },
    });

    const indexedStringSource = mappedSource<Record<string, string>>(
      "indexed-string",
      indexedStringRow,
    );
    type IndexedStringInput = DefineViewServerConfigInput<{
      readonly indexed: {
        readonly schema: typeof Row;
        readonly source: typeof indexedStringSource;
      };
    }>;
    type IndexedStringDetails =
      IndexedStringInput["topics"]["indexed"]["source"]["__viewServerConfigError"]["details"];
    expectTypeOf<IndexedStringDetails>().toEqualTypeOf<
      | {
          readonly field: "shard";
          readonly expected: bigint;
          readonly received: string;
        }
      | {
          readonly field: string;
          readonly expected: "absent";
          readonly expectedPresent: false;
          readonly received: string;
        }
    >();
    defineViewServerConfig({
      topics: {
        indexed: {
          schema: Row,
          // @ts-expect-error Indexed source rows preserve concrete schema-field diagnostics.
          source: indexedStringSource,
        },
      },
    });

    const callableDefinition = Object.assign(() => undefined, { schema: Row });
    type CallableDefinitionInput = DefineViewServerConfigInput<{
      readonly callable: typeof callableDefinition;
    }>;
    expectTypeOf<
      CallableDefinitionInput["topics"]["callable"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "callable";
      readonly reason: "topic definition must not be a function value";
      readonly details: { readonly received: typeof callableDefinition };
    }>();
    defineViewServerConfig({
      topics: {
        // @ts-expect-error Callable topic definitions are rejected with a localized diagnostic.
        callable: callableDefinition,
      },
    });

    class ConstructibleDefinition {
      static readonly schema = Row;
    }
    type ConstructibleDefinitionInput = DefineViewServerConfigInput<{
      readonly constructible: typeof ConstructibleDefinition;
    }>;
    expectTypeOf<
      ConstructibleDefinitionInput["topics"]["constructible"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "constructible";
      readonly reason: "topic definition must not be a function value";
      readonly details: { readonly received: typeof ConstructibleDefinition };
    }>();
    defineViewServerConfig({
      topics: {
        // @ts-expect-error Constructible topic definitions are rejected with a localized diagnostic.
        constructible: ConstructibleDefinition,
      },
    });

    class PrivateConstructibleDefinition {
      static readonly schema = Row;

      private constructor() {}
    }
    type PrivateConstructibleDefinitionInput = DefineViewServerConfigInput<{
      readonly privateConstructible: typeof PrivateConstructibleDefinition;
    }>;
    expectTypeOf<
      PrivateConstructibleDefinitionInput["topics"]["privateConstructible"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "privateConstructible";
      readonly reason: "topic definition must not be a function value";
      readonly details: { readonly received: typeof PrivateConstructibleDefinition };
    }>();
    defineViewServerConfig({
      topics: {
        // @ts-expect-error Classes with non-public constructors are rejected as function values.
        privateConstructible: PrivateConstructibleDefinition,
      },
    });

    const structuralFunctionLookalike = {
      schema: Row,
      apply: 0,
      bind: 0,
      call: 0,
      prototype: 0,
    };
    type StructuralFunctionLookalikeInput = DefineViewServerConfigInput<{
      readonly structuralFunctionLookalike: typeof structuralFunctionLookalike;
    }>;
    expectTypeOf<
      StructuralFunctionLookalikeInput["topics"]["structuralFunctionLookalike"]["apply"]
    >().toEqualTypeOf<never>();
    expectTypeOf<
      // @ts-expect-error Plain objects with function-like property names use exact-key diagnostics.
      StructuralFunctionLookalikeInput["topics"]["structuralFunctionLookalike"]["__viewServerConfigError"]
    >();
    defineViewServerConfig({
      topics: {
        // @ts-expect-error Plain objects retain their unsupported-property diagnostic.
        structuralFunctionLookalike,
      },
    });

    const LiteralMissingRow = Schema.Struct({
      id: ViewServerId,
      marker: Schema.Literal("missing"),
    });
    const missingLiteralFieldSource = mappedSource("missing-literal-field", { id: "id" });
    type MissingLiteralFieldInput = DefineViewServerConfigInput<{
      readonly missingLiteralField: {
        readonly schema: typeof LiteralMissingRow;
        readonly source: typeof missingLiteralFieldSource;
      };
    }>;
    expectTypeOf<
      MissingLiteralFieldInput["topics"]["missingLiteralField"]["source"]["__viewServerConfigError"]["details"]
    >().toEqualTypeOf<{
      readonly field: "marker";
      readonly expected: "missing";
      readonly received: "missing";
      readonly receivedPresent: false;
    }>();
    defineViewServerConfig({
      topics: {
        missingLiteralField: {
          schema: LiteralMissingRow,
          // @ts-expect-error Presence metadata disambiguates a missing field from its literal type.
          source: missingLiteralFieldSource,
        },
      },
    });

    const unexpectedAbsentLiteralSource = mappedSource<{
      readonly id: string;
      readonly marker: "absent";
    }>("unexpected-absent-literal", { id: "id", marker: "absent" });
    type UnexpectedAbsentLiteralInput = DefineViewServerConfigInput<{
      readonly unexpectedAbsentLiteral: {
        readonly schema: typeof IdOnlyRow;
        readonly source: typeof unexpectedAbsentLiteralSource;
      };
    }>;
    expectTypeOf<
      UnexpectedAbsentLiteralInput["topics"]["unexpectedAbsentLiteral"]["source"]["__viewServerConfigError"]["details"]
    >().toEqualTypeOf<{
      readonly field: "marker";
      readonly expected: "absent";
      readonly expectedPresent: false;
      readonly received: "absent";
    }>();
    defineViewServerConfig({
      topics: {
        unexpectedAbsentLiteral: {
          schema: IdOnlyRow,
          // @ts-expect-error Presence metadata disambiguates an unexpected field from its literal type.
          source: unexpectedAbsentLiteralSource,
        },
      },
    });

    const neverSource = adapter.materializedSource<never>({ stream: "never" });
    type NeverSourceInput = DefineViewServerConfigInput<{
      readonly neverRow: { readonly schema: typeof Row; readonly source: typeof neverSource };
    }>;
    expectTypeOf<
      NeverSourceInput["topics"]["neverRow"]["source"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "neverRow";
      readonly reason: "source row type must not be never";
      readonly details: { readonly received: never };
    }>();
    defineViewServerConfig({
      topics: {
        neverRow: {
          schema: Row,
          // @ts-expect-error `never` is rejected separately from any/unknown.
          source: neverSource,
        },
      },
    });

    const nestedRouteSource = adapter.leasedSource(["metadata", "missing"], {
      stream: "nested-route",
    });
    type NestedRouteInput = DefineViewServerConfigInput<{
      readonly nestedRoute: {
        readonly schema: typeof NestedRow;
        readonly source: typeof nestedRouteSource;
      };
    }>;
    expectTypeOf<
      NestedRouteInput["topics"]["nestedRoute"]["source"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "nestedRoute";
      readonly reason: "leased source routeBy field is not a scalar topic row field";
      readonly details:
        | {
            readonly field: "metadata";
            readonly expected: FilterableScalar;
            readonly received: typeof NestedRow.Type.metadata;
          }
        | {
            readonly field: "missing";
            readonly expected: FilterableScalar;
            readonly received: "missing";
            readonly receivedPresent: false;
          };
    }>();
    defineViewServerConfig({
      topics: {
        nestedRoute: {
          schema: NestedRow,
          // @ts-expect-error Route diagnostics report expected and received field types.
          source: nestedRouteSource,
        },
      },
    });

    const RegionRouteRow = Schema.Struct({ id: ViewServerId, region: Schema.String });
    const ShardRouteRow = Schema.Struct({ id: ViewServerId, shard: Schema.String });
    const partialRouteUnionSchema = usePartialRouteSchema ? RegionRouteRow : ShardRouteRow;
    const partialUnionRouteSource = adapter.leasedSource(["region"], {
      stream: "partial-union-route",
    });
    type PartialUnionRouteInput = DefineViewServerConfigInput<{
      readonly partialUnionRoute: {
        readonly schema: typeof partialRouteUnionSchema;
        readonly source: typeof partialUnionRouteSource;
      };
    }>;
    expectTypeOf<
      PartialUnionRouteInput["topics"]["partialUnionRoute"]["source"]["__viewServerConfigError"]["details"]
    >().toEqualTypeOf<
      | {
          readonly field: "region";
          readonly expected: FilterableScalar;
          readonly received: string;
        }
      | {
          readonly field: "region";
          readonly expected: FilterableScalar;
          readonly received: "missing";
          readonly receivedPresent: false;
        }
    >();
    defineViewServerConfig({
      topics: {
        partialUnionRoute: {
          schema: partialRouteUnionSchema,
          // @ts-expect-error Union route diagnostics retain present and missing member details.
          source: partialUnionRouteSource,
        },
      },
    });

    type MalformedSourceInput = DefineViewServerConfigInput<{
      readonly malformedSource: { readonly schema: typeof Row; readonly source: {} };
    }>;
    expectTypeOf<
      MalformedSourceInput["topics"]["malformedSource"]["source"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "malformedSource";
      readonly reason: "source must be created by SourceAdapter.make(...)";
      readonly details: { readonly received: {} };
    }>();
    defineViewServerConfig({
      topics: {
        malformedSource: {
          schema: Row,
          // @ts-expect-error Structural sources receive the configured diagnostic.
          source: {},
        },
      },
    });

    type PrimitiveSourceInput = DefineViewServerConfigInput<{
      readonly primitiveSource: { readonly schema: typeof Row; readonly source: "kafka" };
    }>;
    expectTypeOf<
      PrimitiveSourceInput["topics"]["primitiveSource"]["source"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "primitiveSource";
      readonly reason: "source must be created by SourceAdapter.make(...)";
      readonly details: { readonly received: "kafka" };
    }>();
    defineViewServerConfig({
      topics: {
        primitiveSource: {
          schema: Row,
          // @ts-expect-error Primitive sources receive the configured diagnostic.
          source: "kafka",
        },
      },
    });

    type NullSourceInput = DefineViewServerConfigInput<{
      readonly nullSource: { readonly schema: typeof Row; readonly source: null };
    }>;
    expectTypeOf<
      NullSourceInput["topics"]["nullSource"]["source"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "nullSource";
      readonly reason: "source must be created by SourceAdapter.make(...)";
      readonly details: { readonly received: null };
    }>();
    const nullSourceTopic = { schema: Row, source: null };
    defineViewServerConfig({
      topics: {
        // @ts-expect-error Null sources receive the configured diagnostic.
        nullSource: nullSourceTopic,
      },
    });

    type OptionalMalformedSourceInput = DefineViewServerConfigInput<{
      readonly optionalMalformedSource: typeof optionalMalformedSourceTopic;
    }>;
    expectTypeOf<
      NonNullable<
        OptionalMalformedSourceInput["topics"]["optionalMalformedSource"]["source"]
      >["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "optionalMalformedSource";
      readonly reason: "source must be created by SourceAdapter.make(...)";
      readonly details: { readonly received: {} };
    }>();
    defineViewServerConfig({
      topics: {
        // @ts-expect-error Optional source properties validate their present value.
        optionalMalformedSource: optionalMalformedSourceTopic,
      },
    });

    const explicitUndefinedSourceConfig = defineViewServerConfig({
      topics: {
        explicitUndefinedSource: { schema: Row, source: undefined },
      },
    });
    expectTypeOf(
      explicitUndefinedSourceConfig.topics.explicitUndefinedSource.source,
    ).toEqualTypeOf<undefined>();

    type MalformedSchemaInput = DefineViewServerConfigInput<{
      readonly malformedSchema: { readonly schema: {} };
    }>;
    expectTypeOf<
      MalformedSchemaInput["topics"]["malformedSchema"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "malformedSchema";
      readonly reason: "topic schema must expose concrete struct fields";
      readonly details: { readonly received: { readonly schema: {} } };
    }>();
    defineViewServerConfig({
      topics: {
        // @ts-expect-error Malformed schemas receive the configured diagnostic.
        malformedSchema: { schema: {} },
      },
    });

    type PrimitiveSchemaInput = DefineViewServerConfigInput<{
      readonly primitiveSchema: { readonly schema: "row" };
    }>;
    expectTypeOf<
      PrimitiveSchemaInput["topics"]["primitiveSchema"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "primitiveSchema";
      readonly reason: "topic schema must expose concrete struct fields";
      readonly details: { readonly received: { readonly schema: "row" } };
    }>();
    defineViewServerConfig({
      topics: {
        // @ts-expect-error Primitive schemas receive the configured diagnostic.
        primitiveSchema: { schema: "row" },
      },
    });

    type NullSchemaInput = DefineViewServerConfigInput<{
      readonly nullSchema: { readonly schema: null };
    }>;
    expectTypeOf<
      NullSchemaInput["topics"]["nullSchema"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "nullSchema";
      readonly reason: "topic schema must expose concrete struct fields";
      readonly details: { readonly received: { readonly schema: null } };
    }>();
    defineViewServerConfig({
      topics: {
        // @ts-expect-error Null schemas receive the configured diagnostic.
        nullSchema: { schema: null },
      },
    });

    type MissingSchemaInput = DefineViewServerConfigInput<{
      readonly missingSchema: {};
    }>;
    expectTypeOf<
      MissingSchemaInput["topics"]["missingSchema"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "missingSchema";
      readonly reason: "topic schema must expose concrete struct fields";
      readonly details: { readonly received: {} };
    }>();
    defineViewServerConfig({
      topics: {
        // @ts-expect-error Missing schemas receive the configured diagnostic.
        missingSchema: {},
      },
    });

    type MixedCanonicalIdInput = DefineViewServerConfigInput<{
      readonly mixedCanonicalId: { readonly schema: typeof mixedCanonicalIdSchema };
    }>;
    expectTypeOf<
      MixedCanonicalIdInput["topics"]["mixedCanonicalId"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "mixedCanonicalId";
      readonly reason: "topic schema must define id as ViewServerId";
      readonly details: {
        readonly field: "id";
        readonly expected: typeof ViewServerId;
        readonly received: typeof Schema.Number;
      };
    }>();
    defineViewServerConfig({
      topics: {
        // @ts-expect-error Canonical-ID diagnostics exclude already-valid union members.
        mixedCanonicalId: { schema: mixedCanonicalIdSchema },
      },
    });

    type PrimitiveTopicInput = DefineViewServerConfigInput<{
      readonly primitiveTopic: "topic";
    }>;
    expectTypeOf<
      PrimitiveTopicInput["topics"]["primitiveTopic"]["__viewServerConfigError"]
    >().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "primitiveTopic";
      readonly reason: "topic schema must expose concrete struct fields";
      readonly details: { readonly received: "topic" };
    }>();
    defineViewServerConfig({
      topics: {
        // @ts-expect-error Primitive topics receive the configured diagnostic.
        primitiveTopic: "topic",
      },
    });

    type NullTopicInput = DefineViewServerConfigInput<{
      readonly nullTopic: null;
    }>;
    expectTypeOf<NullTopicInput["topics"]["nullTopic"]["__viewServerConfigError"]>().toEqualTypeOf<{
      readonly __invalid: never;
      readonly topic: "nullTopic";
      readonly reason: "topic schema must expose concrete struct fields";
      readonly details: { readonly received: null };
    }>();
    defineViewServerConfig({
      topics: {
        // @ts-expect-error Null topics receive the configured diagnostic.
        nullTopic: null,
      },
    });

    defineViewServerConfig({
      topics: {
        // @ts-expect-error The diagnostic marker cannot be forged without its private symbol.
        forgedDiagnostic: {
          schema: Schema.Struct({ id: Schema.String }),
          __viewServerConfigError: {
            __invalid: forgedNever,
            topic: "forgedDiagnostic",
            reason: "topic schema must define id as ViewServerId",
            details: { field: "id", expected: ViewServerId, received: Schema.String },
          },
        },
      },
    });
  });
});
