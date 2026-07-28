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
  type ExactLiveQueryInputForTopic,
  type TopicRow,
  type ViewServerHealth,
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
declare const useLeasedSource: boolean;
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
    expectTypeOf(sourceFreeConfig.topics.manual.schema.fields.id).toEqualTypeOf<
      typeof ViewServerId
    >();
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
    expectTypeOf<
      ViewServerHealth<typeof config.topics>["sources"]["all"]
    >().toEqualTypeOf<MaterializedHealth>();
    expectTypeOf<ViewServerHealth<typeof config.topics>["sources"]["routed"]>().toEqualTypeOf<
      ReadonlyArray<LeasedHealth>
    >();
    expectTypeOf<
      ViewServerHealth<typeof config.topics>["sources"]["all"]["metrics"]["adapter"]
    >().toEqualTypeOf<{ readonly connected: boolean }>();
    expectTypeOf<
      ViewServerHealth<typeof mixedLifecycleConfig.topics>["sources"]["mixed"]
    >().toEqualTypeOf<MixedMaterializedHealth | ReadonlyArray<MixedLeasedHealth>>();
    expectTypeOf<
      keyof ViewServerHealth<typeof sourceFreeConfig.topics>["sources"]
    >().toEqualTypeOf<never>();

    const validSourceHealth: ViewServerSourceHealth<typeof config.topics> = {
      all: materializedHealth,
      routed: [leasedHealth],
    };
    expectTypeOf(validSourceHealth.routed).toEqualTypeOf<ReadonlyArray<LeasedHealth>>();

    // @ts-expect-error Materialized Source Health is mandatory and singular.
    const missingMaterializedHealth: ViewServerSourceHealth<typeof config.topics> = {
      routed: [leasedHealth],
    };
    expectTypeOf(missingMaterializedHealth.routed).toEqualTypeOf<ReadonlyArray<LeasedHealth>>();

    const invalidLeasedHealth: ViewServerSourceHealth<typeof config.topics> = {
      all: materializedHealth,
      // @ts-expect-error Leased aggregate health is the exact active-health array.
      routed: leasedHealth,
    };
    expectTypeOf(invalidLeasedHealth.all).toEqualTypeOf<MaterializedHealth>();

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
  });

  it("rejects keys, invalid routes, and source-owner conflicts", () => {
    // @ts-expect-error Every Topic rejects the removed configurable key.
    defineViewServerConfig({
      topics: {
        keyed: {
          schema: Row,
          key: "id",
          source: adapter.materializedSource({ stream: "all" }),
        },
      },
    });

    // @ts-expect-error Leased Source routes must be row scalar fields.
    defineViewServerConfig({
      topics: {
        invalidRoute: {
          schema: Row,
          source: adapter.leasedSource(["missing"], { stream: "routed" }),
        },
      },
    });

    // @ts-expect-error Legacy source owners are removed.
    defineViewServerConfig({
      topics: {
        conflicting: {
          schema: Row,
          source: adapter.materializedSource({ stream: "all" }),
          grpcSource: {},
        },
      },
    });

    // @ts-expect-error Legacy Kafka source owners are removed.
    defineViewServerConfig({
      topics: {
        conflictingKafka: {
          schema: Row,
          source: adapter.materializedSource({ stream: "all" }),
          kafkaSource: {},
        },
      },
    });

    // @ts-expect-error canonical Source-owned rows require an id field.
    defineViewServerConfig({
      topics: {
        missingId: {
          schema: Schema.Struct({ region: Schema.String }),
          source: adapter.materializedSource({ stream: "all" }),
        },
      },
    });

    // @ts-expect-error canonical Source-owned ids may not be optional.
    defineViewServerConfig({
      topics: {
        optionalId: {
          schema: Schema.Struct({
            id: Schema.optionalKey(Schema.String),
            region: Schema.String,
          }),
          source: adapter.materializedSource({ stream: "all" }),
        },
      },
    });

    // @ts-expect-error canonical Source-owned ids may not be numbers.
    defineViewServerConfig({
      topics: {
        numberId: {
          schema: Schema.Struct({ id: Schema.Number, region: Schema.String }),
          source: adapter.materializedSource({ stream: "all" }),
        },
      },
    });

    // @ts-expect-error canonical Source-owned ids may not be branded.
    defineViewServerConfig({
      topics: {
        brandedId: {
          schema: Schema.Struct({
            id: ViewServerId.pipe(Schema.brand("SourceId")),
            region: Schema.String,
          }),
          source: adapter.materializedSource({ stream: "all" }),
        },
      },
    });

    // @ts-expect-error canonical ids must use the nominal ViewServerId schema.
    defineViewServerConfig({
      topics: {
        plainStringId: {
          schema: Schema.Struct({ id: Schema.String, region: Schema.String }),
          source: adapter.materializedSource({ stream: "all" }),
        },
      },
    });

    // @ts-expect-error canonical Source-owned ids may not be transformations.
    defineViewServerConfig({
      topics: {
        transformedId: {
          schema: Schema.Struct({ id: Schema.Trim, region: Schema.String }),
          source: adapter.materializedSource({ stream: "all" }),
        },
      },
    });

    // @ts-expect-error canonical Source-owned ids may not be refinements.
    defineViewServerConfig({
      topics: {
        refinedId: {
          schema: Schema.Struct({ id: Schema.NonEmptyString, region: Schema.String }),
          source: adapter.materializedSource({ stream: "all" }),
        },
      },
    });

    // @ts-expect-error Source-free Topics also require the canonical id.
    defineViewServerConfig({
      topics: {
        missingManualId: {
          schema: Schema.Struct({ region: Schema.String }),
        },
      },
    });

    // @ts-expect-error a bound Materialized Source row may not omit Topic Row fields.
    defineViewServerConfig({
      topics: {
        missingMaterializedField: {
          schema: Row,
          source: adapter.materializedSource<MissingFieldRow>({ stream: "all" }),
        },
      },
    });

    // @ts-expect-error an any-valued Materialized Source row cannot bind to a Topic.
    defineViewServerConfig({
      topics: {
        unsafeMaterializedRow: {
          schema: Row,
          source: adapter.materializedSource<any>({ stream: "all" }),
        },
      },
    });

    // @ts-expect-error a bound Materialized Source row may not add Topic Row fields.
    defineViewServerConfig({
      topics: {
        extraMaterializedField: {
          schema: Row,
          source: adapter.materializedSource<ExtraFieldRow>({ stream: "all" }),
        },
      },
    });

    // @ts-expect-error a bound Materialized Source row must preserve Topic Row field types.
    defineViewServerConfig({
      topics: {
        differentMaterializedFieldType: {
          schema: Row,
          source: adapter.materializedSource<DifferentFieldTypeRow>({ stream: "all" }),
        },
      },
    });

    // @ts-expect-error a bound Leased Source row may not omit Topic Row fields.
    defineViewServerConfig({
      topics: {
        missingLeasedField: {
          schema: Row,
          source: adapter.leasedSource<readonly ["id"], MissingFieldRow>(["id"], {
            stream: "routed",
          }),
        },
      },
    });

    // @ts-expect-error an any-valued Leased Source row cannot bind to a Topic.
    defineViewServerConfig({
      topics: {
        unsafeLeasedRow: {
          schema: Row,
          source: adapter.leasedSource<readonly ["id"], any>(["id"], {
            stream: "routed",
          }),
        },
      },
    });

    // @ts-expect-error a bound Leased Source row may not add Topic Row fields.
    defineViewServerConfig({
      topics: {
        extraLeasedField: {
          schema: Row,
          source: adapter.leasedSource<readonly ["id"], ExtraFieldRow>(["id"], {
            stream: "routed",
          }),
        },
      },
    });

    // @ts-expect-error a bound Leased Source row must preserve Topic Row field types.
    defineViewServerConfig({
      topics: {
        differentLeasedFieldType: {
          schema: Row,
          source: adapter.leasedSource<readonly ["id"], DifferentFieldTypeRow>(["id"], {
            stream: "routed",
          }),
        },
      },
    });
  });
});
