import { describe, expectTypeOf, it } from "@effect/vitest";
import { ViewServerId, defineViewServerConfig, viewSchema } from "effect-view-server/config";
import type { LiveQueryResult, RowFromSchema } from "effect-view-server/config";
import { createViewServerReact } from "effect-view-server/react";
import { createInMemoryViewServerReact } from "effect-view-server/react/testing";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { createViewServerWebSocketServer } from "effect-view-server/server";
import { createInMemoryViewServer } from "effect-view-server/in-memory";
import {
  decodeKafkaCodec,
  kafka as kafkaSourceAdapter,
  type KafkaCodecValue as KafkaSourceCodecValue,
  type KafkaCompactionMappingInput,
} from "effect-view-server/kafka/contract";
import { Schema } from "effect";
import type * as EffectOption from "effect/Option";
import type { BigDecimal } from "effect/BigDecimal";
import type { ViewServerLiveClient } from "effect-view-server/client";
import {
  compareWireSafeBigDecimalComparisonMetadata,
  compareTrustedWireSafeBigDecimal,
  compareWireSafeBigDecimal,
  inspectWireSafeBigDecimal,
  isWireSafeBigDecimal,
  trustedWireSafeBigDecimalComparisonMetadata,
  type WireSafeBigDecimal,
  type WireSafeBigDecimalComparisonMetadata,
  type WireSafeBigDecimalInspection,
  wireSafeBigDecimalComparisonMetadata,
  wireSafeBigDecimalSemanticKey,
} from "effect-view-server/value-semantics";
import * as PublicValueSemantics from "effect-view-server/value-semantics";
import {
  SourceFixture,
  registerSourceAdapterConformance,
  registerSourceAdapterPackageConformance,
  sourceAdapterConformanceDefinitionIsLinked,
  type SourceAdapterConformanceOptions,
  type SourceAdapterPackageConformanceOptions,
} from "effect-view-server/source-adapter/testing";

const Order = Schema.Struct({
  id: ViewServerId,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed"]),
  price: Schema.Number,
  region: Schema.String,
});

declare const publicBigDecimal: BigDecimal;
declare const publicBigDecimalComparisonMetadata: WireSafeBigDecimalComparisonMetadata;

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
    },
  },
});

const react = createViewServerReact(viewServer);
const kafkaOrderCodec = kafkaSourceAdapter.json(() => Schema.toCodecJson(Order));
const IncomingKafkaOrder = Schema.Struct({
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed"]),
  price: Schema.Number,
  region: Schema.String,
});
const kafkaSourceValue = kafkaSourceAdapter.json(() => Schema.toCodecJson(IncomingKafkaOrder));
const kafkaSourceViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: kafkaSourceAdapter.source({
        cleanupPolicy: "delete",
        retentionPolicy: "match-kafka-retention",
        topic: "orders-source",
        regions: ["eu"],
        key: kafkaSourceAdapter.string(),
        value: kafkaSourceValue,
        localRowKey: ({ key }) => key,
        map: ({ value }) => ({
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: value.region,
        }),
        startFrom: "earliest",
      }),
    },
  },
});
const compactedKafkaSourceViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: kafkaSourceAdapter.source({
        cleanupPolicy: "compact-and-delete",
        retentionPolicy: "5 minutes",
        topic: "compacted-orders-source",
        regions: ["eu"],
        key: kafkaSourceAdapter.compactionKey.string(),
        value: kafkaSourceValue,
        map: ({ key, value, region, metadata }) => {
          expectTypeOf(key).toEqualTypeOf<string>();
          expectTypeOf(region).toEqualTypeOf<"eu">();
          expectTypeOf(metadata.partition).toEqualTypeOf<number>();
          return {
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: value.region,
          };
        },
        startFrom: "earliest",
      }),
    },
  },
});
const invalidFacadeCompactionIdentity = {
  cleanupPolicy: "compact" as const,
  retentionPolicy: "match-kafka-retention" as const,
  topic: "invalid-facade-compaction-identity",
  regions: ["eu"] satisfies readonly ["eu"],
  key: kafkaSourceAdapter.compactionKey.string(),
  value: kafkaSourceValue,
  map: (
    input: KafkaCompactionMappingInput<
      "eu",
      ReturnType<typeof kafkaSourceAdapter.compactionKey.string>,
      typeof kafkaSourceValue
    >,
  ) => ({
    id: `application:${input.key}`,
    customerId: input.value.customerId,
    status: input.value.status,
    price: input.value.price,
    region: input.value.region,
  }),
  startFrom: "earliest" as const,
};
// @ts-expect-error the public facade preserves SDK-owned compaction identity.
kafkaSourceAdapter.source(invalidFacadeCompactionIdentity);

class PublicProfile extends Schema.Class<PublicProfile>("PublicProfile")({
  id: ViewServerId,
  score: Schema.NumberFromString,
  backup: viewSchema.Option(Schema.String),
  nickname: Schema.optionalKey(Schema.String),
}) {
  upperId(): string {
    return this.id.toUpperCase();
  }
}
viewSchema.admitClass(PublicProfile);

const publicProfileViewServer = defineViewServerConfig({
  topics: {
    profiles: {
      schema: PublicProfile,
    },
  },
});
const publicProfileReact = createViewServerReact(publicProfileViewServer);
const publicProfileInMemory = createInMemoryViewServer(publicProfileViewServer);

describe("public effect-view-server subpath type contracts", () => {
  it("rejects bare root package imports", () => {
    // @ts-expect-error effect-view-server intentionally has no root export.
    expectTypeOf<typeof import("effect-view-server")>().not.toBeAny();
  });

  it("exposes public package subpaths", () => {
    expectTypeOf(defineViewServerConfig).not.toBeAny();
    expectTypeOf(createViewServerReact).not.toBeAny();
    expectTypeOf(createInMemoryViewServerReact).not.toBeAny();
    expectTypeOf(runViewServerRuntime).not.toBeAny();
    expectTypeOf(createViewServerWebSocketServer).not.toBeAny();
    expectTypeOf(createInMemoryViewServer).not.toBeAny();
    expectTypeOf(decodeKafkaCodec).not.toBeAny();
    expectTypeOf(viewSchema).not.toBeAny();
    expectTypeOf<typeof PublicProfile.Type>().toEqualTypeOf<PublicProfile>();
    expectTypeOf<typeof PublicProfile.Encoded>().toEqualTypeOf<{
      readonly id: string;
      readonly score: string;
      readonly backup: EffectOption.Option<string>;
      readonly nickname?: string;
    }>();
    expectTypeOf<RowFromSchema<typeof PublicProfile>>().toEqualTypeOf<{
      readonly id: string;
      readonly score: number;
      readonly backup: EffectOption.Option<string>;
      readonly nickname?: string;
    }>();
    expectTypeOf(publicProfileViewServer.topics.profiles.schema).toEqualTypeOf<
      typeof PublicProfile
    >();
    expectTypeOf<KafkaSourceCodecValue<typeof kafkaOrderCodec>>().toEqualTypeOf<
      typeof Order.Type
    >();
    expectTypeOf<KafkaSourceCodecValue<typeof kafkaSourceValue>>().toEqualTypeOf<
      typeof IncomingKafkaOrder.Type
    >();
    expectTypeOf(kafkaSourceValue).not.toHaveProperty("schema");
    expectTypeOf(kafkaSourceViewServer.topics.orders.source).not.toBeAny();
    expectTypeOf(
      compactedKafkaSourceViewServer.topics.orders.source.options.cleanupPolicy,
    ).toEqualTypeOf<"compact-and-delete">();
    expectTypeOf(compactedKafkaSourceViewServer.topics.orders.source.options).not.toHaveProperty(
      "localRowKey",
    );
    expectTypeOf(kafkaOrderCodec).not.toHaveProperty("schema");
    expectTypeOf<ViewServerLiveClient<typeof viewServer.topics>>().not.toBeAny();
    expectTypeOf(SourceFixture).not.toBeAny();
    expectTypeOf(registerSourceAdapterConformance).not.toBeAny();
    expectTypeOf(registerSourceAdapterPackageConformance).not.toBeAny();
    expectTypeOf(sourceAdapterConformanceDefinitionIsLinked).not.toBeAny();
    expectTypeOf<SourceAdapterConformanceOptions>().not.toBeAny();
    expectTypeOf<SourceAdapterPackageConformanceOptions>().not.toBeAny();
    expectTypeOf(inspectWireSafeBigDecimal).toEqualTypeOf<
      (value: unknown) => WireSafeBigDecimalInspection
    >();
    expectTypeOf<WireSafeBigDecimalInspection>().toEqualTypeOf<
      | { readonly _tag: "NotBigDecimal" }
      | { readonly _tag: "UnsafeBigDecimal" }
      | { readonly _tag: "ReflectionFailure" }
      | {
          readonly _tag: "Success";
          readonly source: WireSafeBigDecimal;
          readonly coefficient: bigint;
          readonly scale: number;
          readonly semanticKey: string;
        }
    >();
    expectTypeOf(isWireSafeBigDecimal).toEqualTypeOf<
      (value: unknown) => value is WireSafeBigDecimal
    >();
    expectTypeOf<BigDecimal>().toMatchTypeOf<WireSafeBigDecimal>();
    expectTypeOf<WireSafeBigDecimal>().not.toMatchTypeOf<BigDecimal>();
    expectTypeOf(compareWireSafeBigDecimal).toEqualTypeOf<
      (left: unknown, right: unknown) => number | undefined
    >();
    expectTypeOf(compareTrustedWireSafeBigDecimal).toEqualTypeOf<
      (left: BigDecimal, right: BigDecimal) => number | undefined
    >();
    expectTypeOf(wireSafeBigDecimalComparisonMetadata).toEqualTypeOf<
      (value: unknown) => WireSafeBigDecimalComparisonMetadata | undefined
    >();
    expectTypeOf(trustedWireSafeBigDecimalComparisonMetadata).toEqualTypeOf<
      (value: BigDecimal) => WireSafeBigDecimalComparisonMetadata | undefined
    >();
    expectTypeOf(compareWireSafeBigDecimalComparisonMetadata).toEqualTypeOf<
      (
        left: WireSafeBigDecimalComparisonMetadata,
        right: WireSafeBigDecimalComparisonMetadata,
      ) => number | undefined
    >();
    expectTypeOf(wireSafeBigDecimalSemanticKey).toEqualTypeOf<
      (value: unknown) => string | undefined
    >();
    expectTypeOf(PublicValueSemantics).not.toHaveProperty("isTrustedWireSafeBigDecimal");
    expectTypeOf(PublicValueSemantics).not.toHaveProperty("trustedWireSafeBigDecimalSemanticKey");
    // @ts-expect-error trusted comparison requires admitted BigDecimal operands.
    compareTrustedWireSafeBigDecimal({}, publicBigDecimal);
    // @ts-expect-error trusted comparison requires admitted BigDecimal operands.
    compareTrustedWireSafeBigDecimal(publicBigDecimal, 1n);
    compareWireSafeBigDecimalComparisonMetadata(
      // @ts-expect-error reusable comparison accepts only opaque View Server metadata.
      publicBigDecimal,
      publicBigDecimalComparisonMetadata,
    );
    // @ts-expect-error opaque comparison metadata cannot be constructed structurally.
    compareWireSafeBigDecimalComparisonMetadata({}, publicBigDecimalComparisonMetadata);
  });

  it("preserves query result inference through public subpaths", () => {
    const result = react.useLiveQuery("orders", {
      select: ["id", "price"],
      where: [{ field: "status", type: "equals", filter: "open" }],
      orderBy: [{ field: "price", direction: "desc" }],
      limit: 10,
    });

    expectTypeOf(result).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly price: number;
      }>
    >();
    const viewport = react.useLiveQueryViewport("orders");
    viewport.viewport.replace({
      window: { firstRow: 0, lastRow: 19 },
      query: {
        select: ["id", "price"],
        where: [],
        orderBy: [{ field: "price", direction: "desc" }],
      },
      sink: {
        setRowCount: (count) => {
          expectTypeOf(count).toBeNumber();
        },
        setRowData: (rows, rowKeys) => {
          expectTypeOf(rows[0]).toEqualTypeOf<
            { readonly id: string; readonly price: number } | undefined
          >();
          expectTypeOf(rowKeys[0]).toEqualTypeOf<string | undefined>();
        },
      },
    });
    expectTypeOf(viewport).not.toHaveProperty("rows");

    const profileResult = publicProfileReact.useLiveQuery("profiles", {
      select: ["id", "score"],
      where: [{ field: "score", type: "greaterThanOrEqual", filter: 10 }],
      orderBy: [{ field: "score", direction: "desc" }],
    });
    expectTypeOf(profileResult).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly score: number;
      }>
    >();
  });

  it("rejects invalid query and config contracts through public subpaths", () => {
    // @ts-expect-error viewport topics must exist in the configured topic map.
    react.useLiveQueryViewport("missing");

    // @ts-expect-error raw queries must explicitly select columns.
    react.useLiveQuery("orders", { where: [{ field: "status", type: "equals", filter: "open" }] });

    const unknownProjectedFieldQuery = {
      select: ["missing"],
    } satisfies {
      readonly select: readonly ["missing"];
    };
    // @ts-expect-error projected fields must be topic field names.
    react.useLiveQuery("orders", unknownProjectedFieldQuery);

    const encodedProfileScoreQuery = {
      select: ["id"],
      where: [{ field: "score", type: "greaterThanOrEqual", filter: "10" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        {
          readonly field: "score";
          readonly type: "greaterThanOrEqual";
          readonly filter: "10";
        },
      ];
    };
    // @ts-expect-error transformed query operands must use the decoded number type.
    publicProfileReact.useLiveQuery("profiles", encodedProfileScoreQuery);

    const stringRangeFilterQuery = {
      select: ["id"],
      where: [{ field: "status", type: "greaterThanOrEqual", filter: "open" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        {
          readonly field: "status";
          readonly type: "greaterThanOrEqual";
          readonly filter: "open";
        },
      ];
    };
    // @ts-expect-error string fields do not accept range predicates.
    react.useLiveQuery("orders", stringRangeFilterQuery);

    // @ts-expect-error configurable Topic keys were removed; exact id is implicit.
    defineViewServerConfig({
      topics: {
        invalidOrders: {
          schema: Order,
          key: "id",
        },
      },
    });

    // @ts-expect-error configurable Topic keys were removed for Schema.Class Topics too.
    defineViewServerConfig({
      topics: {
        invalidProfiles: {
          schema: PublicProfile,
          key: "id",
        },
      },
    });

    // @ts-expect-error Kafka JSON codecs require a zero-argument Schema JSON codec factory.
    kafkaSourceAdapter.json(Order);

    const missingCleanupPolicySource = {
      retentionPolicy: "match-kafka-retention",
      topic: "orders-source",
      regions: ["eu"] as const,
      key: kafkaSourceAdapter.string(),
      value: kafkaSourceValue,
      localRowKey: ({ key }: { readonly key: string }) => key,
      map: ({ value }: { readonly value: typeof Order.Type }) => ({
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: value.region,
      }),
      startFrom: "earliest" as const,
    };
    // @ts-expect-error cleanupPolicy is mandatory through the publishable Kafka subpath.
    kafkaSourceAdapter.source(missingCleanupPolicySource);

    const missingRetentionPolicySource = {
      cleanupPolicy: "delete",
      topic: "orders-source",
      regions: ["eu"] as const,
      key: kafkaSourceAdapter.string(),
      value: kafkaSourceValue,
      localRowKey: ({ key }: { readonly key: string }) => key,
      map: ({ value }: { readonly value: typeof Order.Type }) => ({
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: value.region,
      }),
      startFrom: "earliest" as const,
    };
    // @ts-expect-error retentionPolicy is mandatory through the publishable Kafka subpath.
    kafkaSourceAdapter.source(missingRetentionPolicySource);

    // @ts-expect-error migrated Kafka Mapping must return every Topic Row field except id.
    defineViewServerConfig({
      topics: {
        missingKafkaMappingField: {
          schema: Order,
          source: kafkaSourceAdapter.source({
            cleanupPolicy: "delete",
            retentionPolicy: "match-kafka-retention",
            topic: "orders-source",
            regions: ["eu"],
            key: kafkaSourceAdapter.string(),
            value: kafkaSourceValue,
            localRowKey: ({ key }) => key,
            map: ({ value }) => ({
              customerId: value.customerId,
              status: value.status,
              price: value.price,
            }),
            startFrom: "earliest",
          }),
        },
      },
    });

    // @ts-expect-error migrated Kafka Mapping cannot return fields outside the Topic Row.
    defineViewServerConfig({
      topics: {
        extraKafkaMappingField: {
          schema: Order,
          source: kafkaSourceAdapter.source({
            cleanupPolicy: "delete",
            retentionPolicy: "match-kafka-retention",
            topic: "orders-source",
            regions: ["eu"],
            key: kafkaSourceAdapter.string(),
            value: kafkaSourceValue,
            localRowKey: ({ key }) => key,
            map: ({ value }) => ({
              customerId: value.customerId,
              status: value.status,
              price: value.price,
              region: value.region,
              venue: "XNAS",
            }),
            startFrom: "earliest",
          }),
        },
      },
    });

    // @ts-expect-error migrated Kafka Mapping field types must match the Topic Row.
    defineViewServerConfig({
      topics: {
        invalidKafkaMappingField: {
          schema: Order,
          source: kafkaSourceAdapter.source({
            cleanupPolicy: "delete",
            retentionPolicy: "match-kafka-retention",
            topic: "orders-source",
            regions: ["eu"],
            key: kafkaSourceAdapter.string(),
            value: kafkaSourceValue,
            localRowKey: ({ key }) => key,
            map: ({ value }) => ({
              customerId: value.customerId,
              status: value.status,
              price: String(value.price),
              region: value.region,
            }),
            startFrom: "earliest",
          }),
        },
      },
    });

    const methodSelectQuery = {
      select: ["upperId"],
    } satisfies { readonly select: readonly ["upperId"] };
    // @ts-expect-error Schema.Class methods are not Topic Row select fields.
    publicProfileReact.useLiveQuery("profiles", methodSelectQuery);

    const methodWhereQuery = {
      select: ["id"],
      where: [{ field: "upperId", type: "equals", filter: () => "PROFILE" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        {
          readonly field: "upperId";
          readonly type: "equals";
          readonly filter: () => string;
        },
      ];
    };
    // @ts-expect-error Schema.Class methods are not Topic Row where fields.
    publicProfileReact.useLiveQuery("profiles", methodWhereQuery);

    const methodOrderQuery = {
      select: ["id"],
      orderBy: [{ field: "upperId", direction: "asc" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly orderBy: readonly [{ readonly field: "upperId"; readonly direction: "asc" }];
    };
    // @ts-expect-error Schema.Class methods are not Topic Row orderBy fields.
    publicProfileReact.useLiveQuery("profiles", methodOrderQuery);

    const methodGroupQuery = {
      groupBy: ["upperId"],
      aggregates: { rowCount: { aggFunc: "count" } },
    } satisfies {
      readonly groupBy: readonly ["upperId"];
      readonly aggregates: { readonly rowCount: { readonly aggFunc: "count" } };
    };
    // @ts-expect-error Schema.Class methods are not Topic Row groupBy fields.
    publicProfileReact.useLiveQuery("profiles", methodGroupQuery);

    const methodAggregateQuery = {
      groupBy: ["id"],
      aggregates: {
        distinctMethods: { aggFunc: "countDistinct", field: "upperId" },
      },
    } satisfies {
      readonly groupBy: readonly ["id"];
      readonly aggregates: {
        readonly distinctMethods: {
          readonly aggFunc: "countDistinct";
          readonly field: "upperId";
        };
      };
    };
    // @ts-expect-error Schema.Class methods are not Topic Row aggregate fields.
    publicProfileReact.useLiveQuery("profiles", methodAggregateQuery);

    const methodPatch = { upperId: () => "PROFILE" };
    const invalidMethodPatch = publicProfileInMemory.client.patch(
      "profiles",
      "profile-1",
      // @ts-expect-error Schema.Class methods are not Topic Row patch fields.
      methodPatch,
    );
    expectTypeOf(invalidMethodPatch).not.toBeAny();
  });

  it("preserves public in-memory client and React testing provider types", () => {
    const runtime = createInMemoryViewServer(viewServer);
    const testReact = createInMemoryViewServerReact(react);

    expectTypeOf(runtime.client.publish).parameter(0).toEqualTypeOf<"orders">();
    expectTypeOf(runtime.client.publish).parameter(1).toEqualTypeOf<typeof Order.Type>();
    expectTypeOf(testReact.client.publish).parameter(0).toEqualTypeOf<"orders">();
    expectTypeOf(testReact.ViewServerInMemoryProvider).not.toBeAny();
  });
});
