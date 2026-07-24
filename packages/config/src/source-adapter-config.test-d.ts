import { describe, expectTypeOf, it } from "@effect/vitest";
import {
  SourceAdapter,
  type SourceDefinitionOptionsFamily,
} from "@effect-view-server/source-adapter";
import { Schema } from "effect";
import { defineViewServerConfig, type ExactLiveQueryInputForTopic } from "./index";
import { grpcSourceMarkers } from "./internal";

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
  id: Schema.String,
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
  id: Schema.String,
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

describe("Source Adapter config type contracts", () => {
  it("infers canonical ids, exact routes, and source definitions without as const", () => {
    expectTypeOf(config.topics.all.key).toEqualTypeOf<"id">();
    expectTypeOf(config.topics.routed.key).toEqualTypeOf<"id">();
    expectTypeOf(mappedConfig.topics.mapped.key).toEqualTypeOf<"id">();
    expectTypeOf(nestedMappedConfig.topics.nested.key).toEqualTypeOf<"id">();
    expectTypeOf(config.topics.routed.source.routeBy).toEqualTypeOf<readonly ["region", "shard"]>();

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
    // @ts-expect-error Source-owned topics cannot declare key.
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

    defineViewServerConfig({
      topics: {
        // @ts-expect-error Source-owned topics cannot declare legacy owners.
        conflicting: {
          schema: Row,
          source: adapter.materializedSource({ stream: "all" }),
          grpcSource: grpcSourceMarkers.materialized(),
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
            id: Schema.String.pipe(Schema.brand("SourceId")),
            region: Schema.String,
          }),
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
