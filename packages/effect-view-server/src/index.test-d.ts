import { describe, expectTypeOf, it } from "@effect/vitest";
import type * as PublicConfig from "./config";

// @ts-expect-error row-only complete-projection authority is not a public facade export.
export type _NoPublicCompleteSelect = PublicConfig.LiveQueryViewportCompleteRawSelectForRow;
import type { DescMessage } from "@bufbuild/protobuf";
import { EmptySchema, TimestampSchema, type Timestamp } from "@bufbuild/protobuf/wkt";
import { ViewServerId, defineViewServerConfig, viewSchema } from "effect-view-server/config";
import type { FalseExpression, LiveQueryResult, RowFromSchema } from "effect-view-server/config";
import {
  createViewServerReact,
  type LiveQueryViewport,
  type LiveQueryViewportBaseRow,
  type LiveQueryViewportSemanticKey,
} from "effect-view-server/react";
import type {
  LiveQueryViewportBaseRow as PureLiveQueryViewportBaseRow,
  LiveQueryViewportCompleteRawSelect,
} from "effect-view-server/react/viewport-base-row";
import { createInMemoryViewServerReact } from "effect-view-server/react/testing";
import { runViewServerRuntime } from "effect-view-server/runtime";
import type {
  RuntimeDependency,
  RuntimeDependencyIssue,
  RuntimeHeartbeat,
  ViewServerRuntimeReportingOptions,
} from "effect-view-server/runtime";
import { createViewServerWebSocketServer } from "effect-view-server/server";
import { createInMemoryViewServer } from "effect-view-server/in-memory";
import {
  decodeKafkaCodec,
  kafka as kafkaSourceAdapter,
  KafkaSourceAdapter,
  type KafkaCodecValue as KafkaSourceCodecValue,
  type KafkaCompactionMappingInput,
} from "effect-view-server/kafka/contract";
import {
  layer as kafkaNodeLayer,
  layerConfig as kafkaNodeLayerConfig,
  type KafkaBrokerContractValidationFailure,
  type KafkaSchemaRegistryContractValidationFailure,
} from "effect-view-server/kafka/node";
import { Config, Context, Effect, Layer, Schema } from "effect";
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

const RegistryOrder = Schema.Struct({
  id: ViewServerId,
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

const Position = Schema.Struct({
  id: ViewServerId,
  quantity: Schema.Number,
});

const positionViewServer = defineViewServerConfig({
  topics: {
    positions: {
      schema: Position,
    },
  },
});

const optionalOrderViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Schema.Struct({
        id: ViewServerId,
        customerId: Schema.String,
        status: Schema.Literals(["open", "closed"]),
        price: Schema.Number,
        region: Schema.String,
        note: Schema.optionalKey(Schema.String),
      }),
    },
  },
});

class PublicReportingCallbackDependency extends Context.Service<
  PublicReportingCallbackDependency,
  { readonly report: () => void }
>()("effect-view-server/type-test/PublicReportingCallbackDependency") {}

const react = createViewServerReact(viewServer);
const kafkaOrderCodec = kafkaSourceAdapter.json(() => Schema.toCodecJson(Order));
const IncomingKafkaOrder = Schema.Struct({
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed"]),
  price: Schema.Number,
  region: Schema.String,
});
const kafkaSourceValue = kafkaSourceAdapter.json(() => Schema.toCodecJson(IncomingKafkaOrder));
const kafkaRegistryValue = kafkaSourceAdapter.schemaRegistry.protobuf(TimestampSchema);
declare const widenedKafkaDescriptor: DescMessage;
declare const generatedKafkaDescriptorUnion: typeof EmptySchema | typeof TimestampSchema;
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
const registryKafkaSourceViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: RegistryOrder,
      source: kafkaSourceAdapter.source({
        cleanupPolicy: "delete",
        retentionPolicy: "Infinity",
        topic: "registry-orders-source",
        regions: ["eu"],
        key: kafkaSourceAdapter.string(),
        value: kafkaRegistryValue,
        localRowKey: ({ key }) => key,
        map: ({ value, region }) => ({
          price: value.nanos,
          region: String(region),
        }),
        startFrom: "earliest",
      }),
    },
  },
});
const registryKafkaLayer = kafkaNodeLayer(registryKafkaSourceViewServer, {
  consumerGroupPrefix: "public-facade",
  regions: {
    eu: {
      bootstrapServers: "eu:9092",
      schemaRegistry: { url: "https://registry.example.com" },
    },
  },
});
const registryKafkaConfigLayer = kafkaNodeLayerConfig(registryKafkaSourceViewServer, {
  consumerGroupPrefix: Config.succeed("public-facade"),
  regions: {
    eu: {
      bootstrapServers: Config.succeed("eu:9092"),
      schemaRegistry: {
        url: Config.succeed("https://registry.example.com"),
        monitorInterval: Config.succeed("30 seconds"),
      },
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
    expectTypeOf<RuntimeHeartbeat>().toEqualTypeOf<{
      readonly status:
        | "Starting"
        | "Ready"
        | "Degraded"
        | "WaitingToRetry"
        | "Reacquiring"
        | "Exhausted"
        | "Stopping";
      readonly problems: ReadonlyArray<"self" | "dependency">;
    }>();
    expectTypeOf<RuntimeDependency>().toEqualTypeOf<{
      readonly dependency: string;
      readonly target: string;
      readonly endpoints: ReadonlyArray<string>;
      readonly status:
        | "Starting"
        | "Ready"
        | "Degraded"
        | "WaitingToRetry"
        | "Reacquiring"
        | "Exhausted"
        | "Stopping"
        | "Inactive";
      readonly issues: ReadonlyArray<RuntimeDependencyIssue>;
    }>();
    expectTypeOf<RuntimeDependencyIssue["source"]>().toEqualTypeOf<string>();
    expectTypeOf<RuntimeDependencyIssue["code"]>().toEqualTypeOf<string>();
    expectTypeOf<RuntimeDependencyIssue["message"]>().toEqualTypeOf<string>();
    expectTypeOf<RuntimeDependencyIssue["attributes"]>().toEqualTypeOf<
      ReadonlyArray<{
        readonly name: string;
        readonly value: string;
      }>
    >();
    expectTypeOf<
      Parameters<ViewServerRuntimeReportingOptions["onHeartbeat"]>[0]
    >().toEqualTypeOf<RuntimeHeartbeat>();
    expectTypeOf<
      Parameters<ViewServerRuntimeReportingOptions["onDependenciesUpdate"]>[0]
    >().toEqualTypeOf<ReadonlyArray<RuntimeDependency>>();
    const runtimeWithReporting = runViewServerRuntime(viewServer, {
      reporting: {
        heartbeatInterval: "5 seconds",
        dependenciesInterval: "30 seconds",
        onHeartbeat: () => Effect.void,
        onDependenciesUpdate: () => Effect.void,
      },
    });
    expectTypeOf<Effect.Services<typeof runtimeWithReporting>>().toEqualTypeOf<never>();

    const missingReportingCallback = runViewServerRuntime(viewServer, {
      // @ts-expect-error public reporting requires both callbacks.
      reporting: {
        heartbeatInterval: "5 seconds",
        dependenciesInterval: "30 seconds",
        onHeartbeat: () => Effect.void,
      },
    });
    const invalidReportingCallback = runViewServerRuntime(viewServer, {
      reporting: {
        heartbeatInterval: "5 seconds",
        dependenciesInterval: "30 seconds",
        // @ts-expect-error public reporting callbacks must return Effects.
        onHeartbeat: () => undefined,
        onDependenciesUpdate: () => Effect.void,
      },
    });
    const extraReportingKey = runViewServerRuntime(viewServer, {
      reporting: {
        heartbeatInterval: "5 seconds",
        dependenciesInterval: "30 seconds",
        onHeartbeat: () => Effect.void,
        onDependenciesUpdate: () => Effect.void,
        // @ts-expect-error public reporting rejects extra keys.
        interval: "1 second",
      },
    });
    // These deliberately invalid Effect environments are the contracts under test.
    // @effect-diagnostics missingEffectContext:off
    const serviceHeartbeatCallback = runViewServerRuntime(viewServer, {
      reporting: {
        heartbeatInterval: "5 seconds",
        dependenciesInterval: "30 seconds",
        // @ts-expect-error public heartbeat callbacks must be closed Effects.
        onHeartbeat: () => PublicReportingCallbackDependency.pipe(Effect.asVoid),
        onDependenciesUpdate: () => Effect.void,
      },
    });
    const serviceDependenciesCallback = runViewServerRuntime(viewServer, {
      reporting: {
        heartbeatInterval: "5 seconds",
        dependenciesInterval: "30 seconds",
        onHeartbeat: () => Effect.void,
        // @ts-expect-error public dependency callbacks must be closed Effects.
        onDependenciesUpdate: () => PublicReportingCallbackDependency.pipe(Effect.asVoid),
      },
    });
    // @effect-diagnostics missingEffectContext:on
    expectTypeOf(missingReportingCallback).not.toBeAny();
    expectTypeOf(invalidReportingCallback).not.toBeAny();
    expectTypeOf(extraReportingKey).not.toBeAny();
    expectTypeOf(serviceHeartbeatCallback).not.toBeAny();
    expectTypeOf(serviceDependenciesCallback).not.toBeAny();
    expectTypeOf(createViewServerWebSocketServer).not.toBeAny();
    expectTypeOf(createInMemoryViewServer).not.toBeAny();
    expectTypeOf(decodeKafkaCodec).not.toBeAny();
    expectTypeOf(viewSchema).not.toBeAny();
    expectTypeOf<FalseExpression>().toEqualTypeOf<{ readonly type: "FALSE" }>();
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
    expectTypeOf<KafkaSourceCodecValue<typeof kafkaRegistryValue>>().toEqualTypeOf<Timestamp>();
    expectTypeOf(registryKafkaLayer).toEqualTypeOf<
      Layer.Layer<
        Context.Service.Identifier<typeof KafkaSourceAdapter.runtimeService>,
        KafkaBrokerContractValidationFailure | KafkaSchemaRegistryContractValidationFailure
      >
    >();
    expectTypeOf(registryKafkaConfigLayer).toEqualTypeOf<
      Layer.Layer<
        Context.Service.Identifier<typeof KafkaSourceAdapter.runtimeService>,
        | Config.ConfigError
        | KafkaBrokerContractValidationFailure
        | KafkaSchemaRegistryContractValidationFailure
      >
    >();
    // @ts-expect-error the public facade rejects widened dynamic descriptors.
    kafkaSourceAdapter.schemaRegistry.protobuf(widenedKafkaDescriptor);
    // @ts-expect-error the public facade rejects unions of generated descriptors.
    kafkaSourceAdapter.schemaRegistry.protobuf(generatedKafkaDescriptorUnion);
    kafkaNodeLayer(registryKafkaSourceViewServer, {
      consumerGroupPrefix: "public-facade",
      regions: {
        // @ts-expect-error a public Registry-backed Source requires its Region Registry resource.
        eu: { bootstrapServers: "eu:9092" },
      },
    });
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
    expectTypeOf<LiveQueryViewportBaseRow<typeof viewport.viewport>>().toEqualTypeOf<
      typeof Order.Type
    >();
    expectTypeOf(
      viewport.viewport.semanticKey({ select: ["id"], where: [], orderBy: [] }),
    ).toEqualTypeOf<LiveQueryViewportSemanticKey>();
    expectTypeOf<PureLiveQueryViewportBaseRow<typeof viewport.viewport>>().toEqualTypeOf<
      LiveQueryViewportBaseRow<typeof viewport.viewport>
    >();
    expectTypeOf(viewport.completeRawSelect).toEqualTypeOf<
      LiveQueryViewportCompleteRawSelect<typeof viewport.viewport>
    >();
    expectTypeOf<LiveQueryViewportCompleteRawSelect<any>>().toBeNever();
    expectTypeOf<LiveQueryViewportCompleteRawSelect<unknown>>().toBeNever();
    expectTypeOf<
      LiveQueryViewportCompleteRawSelect<{
        readonly "__effect-view-server/LiveQueryViewportBaseRow@v1"?: (
          row: typeof Order.Type,
        ) => typeof Order.Type;
        readonly "__effect-view-server/LiveQueryViewportCompleteRawSelect@v1"?: any;
      }>
    >().toBeNever();
    expectTypeOf<
      LiveQueryViewportCompleteRawSelect<{
        readonly "__effect-view-server/LiveQueryViewportBaseRow@v1"?: (
          row: typeof Order.Type,
        ) => typeof Order.Type;
        readonly "__effect-view-server/LiveQueryViewportCompleteRawSelect@v1"?: unknown;
      }>
    >().toBeNever();
    expectTypeOf<
      LiveQueryViewportCompleteRawSelect<{
        readonly "__effect-view-server/LiveQueryViewportBaseRow@v1"?: (
          row: typeof Order.Type,
        ) => typeof Order.Type;
        readonly "__effect-view-server/LiveQueryViewportCompleteRawSelect@v1"?: readonly string[];
      }>
    >().toBeNever();
    expectTypeOf<
      LiveQueryViewportCompleteRawSelect<
        Readonly<Record<string, (_row: typeof Order.Type) => typeof Order.Type>>
      >
    >().toBeNever();
    expectTypeOf<
      LiveQueryViewportBaseRow<
        | (typeof viewport.viewport & { readonly source: "left" })
        | (typeof viewport.viewport & { readonly source: "right" })
      >
    >().toEqualTypeOf<typeof Order.Type>();
    expectTypeOf<LiveQueryViewportBaseRow<any>>().toBeNever();
    expectTypeOf<LiveQueryViewportBaseRow<unknown>>().toBeNever();
    expectTypeOf<LiveQueryViewportBaseRow<LiveQueryViewport<any, string>>>().toBeNever();
    expectTypeOf<
      LiveQueryViewportBaseRow<
        Readonly<Record<string, (_row: typeof Order.Type) => typeof Order.Type>>
      >
    >().toBeNever();
    expectTypeOf<
      LiveQueryViewportBaseRow<
        Readonly<
          Record<`__effect-view-server/${string}`, (_row: typeof Order.Type) => typeof Order.Type>
        >
      >
    >().toBeNever();
    expectTypeOf<
      LiveQueryViewportBaseRow<
        | LiveQueryViewport<typeof viewServer.topics, "orders">
        | Readonly<Record<string, (_row: typeof Order.Type) => typeof Order.Type>>
      >
    >().toBeNever();
    expectTypeOf<
      LiveQueryViewportBaseRow<
        | LiveQueryViewport<typeof viewServer.topics, "orders">
        | LiveQueryViewport<typeof positionViewServer.topics, "positions">
      >
    >().toBeNever();
    expectTypeOf<
      LiveQueryViewportBaseRow<
        LiveQueryViewport<typeof viewServer.topics, "orders"> &
          LiveQueryViewport<typeof positionViewServer.topics, "positions">
      >
    >().toBeNever();
    expectTypeOf<
      LiveQueryViewportBaseRow<
        | LiveQueryViewport<typeof viewServer.topics, "orders">
        | LiveQueryViewport<typeof optionalOrderViewServer.topics, "orders">
      >
    >().toBeNever();
    expectTypeOf<
      LiveQueryViewportBaseRow<{
        readonly "__effect-view-server/LiveQueryViewportBaseRow@v1"?: (_row: {
          readonly id: string;
        }) => { readonly id: string; readonly note?: string };
      }>
    >().toBeNever();
    expectTypeOf<
      LiveQueryViewportBaseRow<{
        readonly "__effect-view-server/LiveQueryViewportBaseRow@v1"?:
          | ((_row: typeof Order.Type) => typeof Order.Type)
          | 0;
      }>
    >().toBeNever();
    expectTypeOf<
      LiveQueryViewportBaseRow<{
        readonly "__effect-view-server/LiveQueryViewportBaseRow@v1"?:
          | ((_row: typeof Order.Type) => typeof Order.Type)
          | ((_row: typeof Position.Type) => typeof Position.Type);
      }>
    >().toBeNever();
    expectTypeOf<
      LiveQueryViewportBaseRow<{
        readonly "__effect-view-server/LiveQueryViewportBaseRow@v1"?: (
          arg: typeof Order.Type,
        ) => typeof Order.Type;
      }>
    >().toEqualTypeOf<typeof Order.Type>();
    expectTypeOf<
      LiveQueryViewportBaseRow<{
        readonly "__effect-view-server/LiveQueryViewportBaseRow@v1"?: ((
          _row: typeof Order.Type,
        ) => typeof Order.Type) &
          ((_row: typeof Position.Type) => typeof Position.Type);
      }>
    >().toBeNever();
    expectTypeOf<
      LiveQueryViewportBaseRow<{
        readonly replace: (...args: ReadonlyArray<never>) => unknown;
        readonly destroy: () => void;
      }>
    >().toBeNever();
    const rawGeneration = viewport.viewport.replace({
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
    rawGeneration.setWindow({ firstRow: 20, lastRow: 39 });
    viewport.viewport.replace({
      window: { firstRow: 0, lastRow: 19 },
      query: {
        select: viewport.completeRawSelect,
        where: [],
        orderBy: [{ field: "price", direction: "desc" }],
      },
      sink: {
        setRowCount: () => undefined,
        setRowData: (rows) => {
          expectTypeOf(rows[0]).toEqualTypeOf<typeof Order.Type | undefined>();
        },
      },
    });
    viewport.viewport.replace({
      window: { firstRow: 0, lastRow: 19 },
      query: {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        where: [],
        orderBy: [{ aggregate: "rowCount", direction: "desc" }],
      },
      sink: {
        setRowCount: () => undefined,
        setRowData: (rows) => {
          expectTypeOf(rows[0]).toEqualTypeOf<
            { readonly status: "open" | "closed"; readonly rowCount: bigint } | undefined
          >();
        },
      },
    });
    expectTypeOf<LiveQueryViewportBaseRow<typeof viewport.viewport>>().toEqualTypeOf<
      typeof Order.Type
    >();
    const requireOrdersViewport = (
      _viewport: LiveQueryViewport<typeof viewServer.topics, "orders">,
    ): void => undefined;
    requireOrdersViewport(viewport.viewport);
    // @ts-expect-error the emitted viewport witness rejects a different base Topic row.
    requireOrdersViewport(publicProfileReact.useLiveQueryViewport("profiles").viewport);
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

  it("preserves source-native match-none predicates through public subpaths", () => {
    const rawResult = react.useLiveQuery("orders", {
      select: ["id"],
      where: [{ type: "FALSE" }],
      orderBy: [{ field: "id", direction: "asc" }],
    });
    expectTypeOf(rawResult).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
      }>
    >();

    const groupedResult = react.useLiveQuery("orders", {
      groupBy: ["status"],
      aggregates: { rowCount: { aggFunc: "count" } },
      where: [{ type: "FALSE" }],
      orderBy: [{ aggregate: "rowCount", direction: "desc" }],
    });
    expectTypeOf(groupedResult).toEqualTypeOf<
      LiveQueryResult<{
        readonly status: "open" | "closed";
        readonly rowCount: bigint;
      }>
    >();

    const viewport = react.useLiveQueryViewport("orders");
    viewport.viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: {
        select: ["id"],
        where: [{ type: "FALSE" }],
        orderBy: [],
      },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    viewport.viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        where: [{ type: "FALSE" }],
        orderBy: [{ aggregate: "rowCount", direction: "desc" }],
      },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });

    // @ts-expect-error source-native FALSE expressions reject extra keys.
    react.useLiveQuery("orders", {
      select: ["id"],
      where: [{ type: "FALSE", conditions: [] }],
      orderBy: [],
    });
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
