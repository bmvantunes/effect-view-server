import { describe, expectTypeOf, it } from "@effect/vitest";
import type { DescMessage } from "@bufbuild/protobuf";
import {
  SourceAdapter,
  type SourceDefinitionRetryServices,
  type SourceHealthForDefinition,
  type SourceTermination,
} from "effect-view-server/source-adapter";
import { Context, Effect, Option, Schedule, Schema } from "effect";
import type * as KafkaPublicContract from "@effect-view-server/kafka/contract";
import {
  KafkaSourceAdapter,
  kafka,
  type KafkaAdapterFailure,
  type KafkaCapturedStartPosition,
  type KafkaCodec,
  type KafkaCodecDecodeInput,
  type KafkaCodecFailure,
  type KafkaCodecValue,
  type KafkaCompactionKeyCodecDecodeInput,
  type KafkaCompactionMappingInput,
  type KafkaCompactionSourceDefinition,
  type KafkaDeleteMappingInput,
  type KafkaMaterializedMetrics,
  type KafkaMaterializedRegionMetrics,
  type KafkaDeleteSourceDefinition,
  type KafkaLocalRowKeyInput,
  type KafkaSourceRetryPolicy,
} from "@effect-view-server/kafka/contract";

const JsonValueRow = Schema.Struct({
  price: Schema.Number,
});
const canonicalJsonValueCodec = Schema.toCodecJson(JsonValueRow);
const key = kafka.string();
const value = kafka.json(() => canonicalJsonValueCodec);
// @ts-expect-error cleanup-ambiguous Kafka source input is not a public contract.
type RemovedKafkaSourceInput = KafkaPublicContract.KafkaSourceInput<
  readonly ["eu"],
  "delete",
  typeof key,
  typeof value,
  (input: KafkaLocalRowKeyInput<"eu", typeof key, typeof value>) => string,
  (input: KafkaDeleteMappingInput<"eu", typeof key, typeof value>) => object
>;
// @ts-expect-error delete-only compatibility Mapping alias is not a public contract.
type RemovedKafkaMappingInput = KafkaPublicContract.KafkaMappingInput<
  "eu",
  typeof key,
  typeof value
>;
expectTypeOf<RemovedKafkaSourceInput>();
expectTypeOf<RemovedKafkaMappingInput>();
const decodedDeleteRowId = kafka.decodeRowId("eu:0:kYWJj", "delete");
const decodedCompactionRowId = kafka.decodeRowId("eu:0:kYWJj", "compact");
expectTypeOf(decodedDeleteRowId._tag).toEqualTypeOf<"Delete">();
expectTypeOf(decodedCompactionRowId._tag).toEqualTypeOf<"Compaction">();
// @ts-expect-error row-ID decoding requires the Topic cleanup policy to disambiguate identities.
kafka.decodeRowId("eu:0:kYWJj");
// @ts-expect-error row-ID decoding rejects cleanup policies outside the Kafka contract.
kafka.decodeRowId("eu:0:kYWJj", "archive");
const source = kafka.source({
  cleanupPolicy: "delete",
  retentionPolicy: "Infinity",
  topic: "orders-source",
  regions: ["eu", "us"],
  key,
  value,
  localRowKey: ({ key, value, region }) => {
    expectTypeOf(key).toEqualTypeOf<string>();
    expectTypeOf(value).toEqualTypeOf<{ readonly price: number }>();
    expectTypeOf(region).toEqualTypeOf<"eu" | "us">();
    return key;
  },
  map: ({ key, value, region, localRowKey, metadata }) => {
    expectTypeOf(key).toEqualTypeOf<string>();
    expectTypeOf(value).toEqualTypeOf<{ readonly price: number }>();
    expectTypeOf(region).toEqualTypeOf<"eu" | "us">();
    expectTypeOf(localRowKey).toEqualTypeOf<string>();
    expectTypeOf(metadata.offset).toEqualTypeOf<bigint>();
    return {
      price: value.price,
      region,
    };
  },
  startFrom: "earliest",
});
const extraRowSource = kafka.source({
  cleanupPolicy: "delete",
  retentionPolicy: "Infinity",
  topic: "orders-source",
  regions: ["eu", "us"],
  key,
  value,
  localRowKey: ({ key }) => key,
  map: ({ value, region }) => ({
    price: value.price,
    region,
    extra: true,
  }),
  startFrom: "earliest",
});
const conditionalRowSource = Math.random() > 0.5 ? source : extraRowSource;
// @ts-expect-error captured Kafka definitions preserve every conditional mapped Row branch.
const invalidConditionalRowSource: typeof source = conditionalRowSource;
const retrySource = kafka.source(
  {
    cleanupPolicy: "delete",
    retentionPolicy: "Infinity",
    topic: "orders-source",
    regions: ["eu"],
    key,
    value,
    localRowKey: ({ key }) => key,
    map: ({ value }) => ({ price: value.price }),
    startFrom: "earliest",
  },
  Schedule.identity<SourceTermination<KafkaAdapterFailure<"eu">>>(),
);
const committedSource = kafka.source({
  cleanupPolicy: "delete",
  retentionPolicy: "Infinity",
  topic: "orders-source",
  regions: ["eu"],
  key,
  value,
  localRowKey: ({ key }) => key,
  map: ({ value }) => ({ price: value.price }),
  startFrom: {
    mode: "committed",
    consumerGroupId: "orders-rebuild",
    fallback: "earliest",
  },
});
const timestampSource = kafka.source({
  cleanupPolicy: "delete",
  retentionPolicy: "Infinity",
  topic: "orders-source",
  regions: ["eu"],
  key,
  value,
  localRowKey: ({ key }) => key,
  map: ({ value }) => ({ price: value.price }),
  startFrom: {
    mode: "timestamp",
    atNanos: 1n,
    fallback: "latest",
  },
});
const durationSource = kafka.source({
  cleanupPolicy: "delete",
  retentionPolicy: "Infinity",
  topic: "orders-source",
  regions: ["eu"],
  key,
  value,
  localRowKey: ({ key }) => key,
  map: ({ value }) => ({ price: value.price }),
  startFrom: {
    mode: "durationAgo",
    duration: "5 minutes",
    fallback: "fail",
  },
});
const custom = kafka.codec({
  name: "custom",
  decode: (
    input,
  ): Effect.Effect<{ readonly decodedBytes: number }, { readonly _tag: "CustomCodecFailure" }> => {
    expectTypeOf(input).toEqualTypeOf<KafkaCodecDecodeInput>();
    return input.bytes.length > 0
      ? Effect.succeed({ decodedBytes: input.bytes.length })
      : Effect.fail({
          _tag: "CustomCodecFailure" as const,
        });
  },
});
const compactionKey = kafka.compactionKey.string();
const customCompactionKey = kafka.compactionKey.codec({
  name: "custom-compaction-key",
  decode: (input) => {
    expectTypeOf(input).toEqualTypeOf<KafkaCompactionKeyCodecDecodeInput>();
    // @ts-expect-error compaction identity decoding cannot observe Kafka metadata.
    void input.metadata;
    // @ts-expect-error compaction identity decoding cannot observe a Region.
    void input.region;
    return Effect.succeed(input.bytes.length);
  },
});
const compactSource = kafka.source({
  cleanupPolicy: "compact",
  retentionPolicy: "match-kafka-retention",
  topic: "compacted-orders-source",
  regions: ["eu", "us"],
  key: compactionKey,
  value,
  map: ({ key, value, region, metadata }) => {
    expectTypeOf(key).toEqualTypeOf<string>();
    expectTypeOf(value).toEqualTypeOf<{ readonly price: number }>();
    expectTypeOf(region).toEqualTypeOf<"eu" | "us">();
    expectTypeOf(metadata.partition).toEqualTypeOf<number>();
    return {
      price: value.price,
      region,
    };
  },
  startFrom: "earliest",
});
const compactAndDeleteSource = kafka.source({
  cleanupPolicy: "compact-and-delete",
  retentionPolicy: "5 minutes",
  topic: "retained-compacted-orders-source",
  regions: ["eu"],
  key: kafka.compactionKey.bytes(),
  value,
  map: ({ value }) => ({ price: value.price }),
  startFrom: "latest",
});

declare const unsafeAny: any;
declare const unsafeUnknown: unknown;
declare const unsafeNever: never;
declare const unsafeAnySuccessEffect: Effect.Effect<any, { readonly _tag: "TypedFailure" }>;
declare const unsafeUnknownSuccessEffect: Effect.Effect<unknown, { readonly _tag: "TypedFailure" }>;
declare const unsafeAnyFailureEffect: Effect.Effect<string, any>;
declare const unsafeUnknownFailureEffect: Effect.Effect<string, unknown>;
declare const protobufDescriptor: DescMessage;
declare const protobufDescriptorOrString: DescMessage | string;
declare const kafkaRuntimeService: Context.Service.Identifier<
  typeof KafkaSourceAdapter.runtimeService
>;

class DecodeService extends Context.Service<DecodeService, true>()("KafkaDecodeService") {}
class EncodeService extends Context.Service<EncodeService, true>()("KafkaEncodeService") {}

const decodeServiceNumber = Schema.Number.pipe(
  Schema.middlewareDecoding((effect) =>
    Effect.gen(function* () {
      yield* DecodeService;
      return yield* effect;
    }),
  ),
);
const encodeServiceNumber = Schema.Number.pipe(
  Schema.middlewareEncoding((effect) =>
    Effect.gen(function* () {
      yield* EncodeService;
      return yield* effect;
    }),
  ),
);
const DecodeServiceRow = Schema.Struct({ price: decodeServiceNumber });
const EncodeServiceRow = Schema.Struct({ price: encodeServiceNumber });

describe("Kafka Source Adapter type contract", () => {
  it("preserves codec, region, mapping, and nominal definition inference", () => {
    expectTypeOf(canonicalJsonValueCodec.schema).toEqualTypeOf<typeof JsonValueRow>();
    expectTypeOf<KafkaCodecValue<typeof key>>().toEqualTypeOf<string>();
    expectTypeOf<KafkaCodecValue<typeof value>>().toEqualTypeOf<{
      readonly price: number;
    }>();
    expectTypeOf<KafkaCodecValue<typeof custom>>().toEqualTypeOf<{
      readonly decodedBytes: number;
    }>();
    expectTypeOf<KafkaCodecValue<typeof customCompactionKey>>().toEqualTypeOf<number>();
    // @ts-expect-error compaction key codecs are intentionally distinct from ordinary codecs.
    const ordinaryCodec: KafkaCodec<unknown, unknown> = compactionKey;
    expectTypeOf(ordinaryCodec).not.toBeAny();
    expectTypeOf(value).not.toHaveProperty("schema");
    expectTypeOf<KafkaCodecFailure<typeof value>>().toEqualTypeOf<{
      readonly _tag: "KafkaCodecError";
      readonly message: string;
    }>();
    type CustomCodecFailure = KafkaCodecFailure<typeof custom>;
    expectTypeOf<
      Extract<CustomCodecFailure, { readonly _tag: "KafkaCodecError" }>
    >().toEqualTypeOf<{
      readonly _tag: "KafkaCodecError";
      readonly message: string;
    }>();
    expectTypeOf<
      Extract<CustomCodecFailure, { readonly _tag: "CustomCodecFailure" }>
    >().toEqualTypeOf<{
      readonly _tag: "CustomCodecFailure";
    }>();
    expectTypeOf<
      Exclude<CustomCodecFailure, { readonly _tag: "KafkaCodecError" | "CustomCodecFailure" }>
    >().toEqualTypeOf<never>();
    expectTypeOf(KafkaSourceAdapter).not.toBeAny();
    expectTypeOf(source.adapter).toEqualTypeOf<typeof KafkaSourceAdapter>();
    expectTypeOf(source.lifecycle).toEqualTypeOf<"materialized">();
    expectTypeOf(source.options.regions).toEqualTypeOf<readonly ["eu", "us"]>();
    expectTypeOf(invalidConditionalRowSource).not.toBeAny();
    expectTypeOf(source).toExtend<
      KafkaDeleteSourceDefinition<readonly ["eu", "us"], typeof key, typeof value>
    >();
    expectTypeOf(compactSource).toExtend<
      KafkaCompactionSourceDefinition<
        readonly ["eu", "us"],
        "compact",
        typeof compactionKey,
        typeof value
      >
    >();
    expectTypeOf(
      compactAndDeleteSource.options.cleanupPolicy,
    ).toEqualTypeOf<"compact-and-delete">();
    expectTypeOf(compactSource.options).not.toHaveProperty("localRowKey");
    expectTypeOf<SourceDefinitionRetryServices<typeof source>>().toEqualTypeOf<never>();
    expectTypeOf<SourceDefinitionRetryServices<typeof retrySource>>().toEqualTypeOf<never>();
    type RetryPolicy = Extract<typeof retrySource.retry, { readonly _tag: "Override" }>["policy"];
    expectTypeOf<Schedule.Input<RetryPolicy>>().toEqualTypeOf<
      SourceTermination<KafkaAdapterFailure<"eu">>
    >();
    expectTypeOf(committedSource.options.startFrom).toEqualTypeOf<KafkaCapturedStartPosition>();
    expectTypeOf(timestampSource.options.startFrom).toEqualTypeOf<KafkaCapturedStartPosition>();
    expectTypeOf(durationSource.options.startFrom).toEqualTypeOf<KafkaCapturedStartPosition>();

    type Health = SourceHealthForDefinition<
      typeof source,
      {
        readonly id: string;
        readonly price: number;
        readonly region: "eu" | "us";
      }
    >;
    type RetryFailure = Extract<
      Extract<Health["status"], { readonly _tag: "WaitingToRetry" }>["termination"],
      { readonly _tag: "Failed" }
    >["failure"];
    type AdapterFailure = Extract<RetryFailure, { readonly _tag: "AdapterFailure" }>["failure"];
    expectTypeOf<Extract<AdapterFailure, { readonly region: string }>["region"]>().toEqualTypeOf<
      "eu" | "us"
    >();
    expectTypeOf<
      Extract<
        Extract<
          Extract<Health["status"], { readonly _tag: "Degraded" }>["reasons"][number],
          { readonly _tag: "SourceItemRejection" }
        >["latestRejection"]["location"]["region"],
        string
      >
    >().toEqualTypeOf<"eu" | "us">();
    expectTypeOf<Health["metrics"]["adapter"]["regions"][number]["region"]>().toEqualTypeOf<
      "eu" | "us"
    >();
    expectTypeOf<KafkaMaterializedMetrics<readonly ["eu", "us"]>["regions"]>().toEqualTypeOf<
      readonly [KafkaMaterializedRegionMetrics<"eu">, KafkaMaterializedRegionMetrics<"us">]
    >();
    expectTypeOf<Health["metrics"]["adapter"]["regions"]>().toEqualTypeOf<
      readonly [KafkaMaterializedRegionMetrics<"eu">, KafkaMaterializedRegionMetrics<"us">]
    >();
    expectTypeOf<Health["metrics"]["adapter"]["regions"][0]["region"]>().toEqualTypeOf<"eu">();
    expectTypeOf<Health["metrics"]["adapter"]["regions"][1]["region"]>().toEqualTypeOf<"us">();
    expectTypeOf<
      Health["metrics"]["adapter"]["regions"][number]["retention"]["lastSweepRetryableFailures"]
    >().toEqualTypeOf<number>();
    type RetentionMetrics = Health["metrics"]["adapter"]["regions"][number]["retention"];
    // @ts-expect-error the ambiguous due-row name is not part of the public retention metrics.
    expectTypeOf<RetentionMetrics["dueBacklog"]>().not.toBeAny();
    expectTypeOf<
      Health["metrics"]["adapter"]["regions"][number]["retention"]["expirationRetryFailures"]
    >().toEqualTypeOf<bigint>();
    expectTypeOf<
      NonNullable<
        Health["metrics"]["adapter"]["regions"][number]["retention"]["latestExpirationFailure"]
      >["region"]
    >().toEqualTypeOf<"eu" | "us">();
    expectTypeOf<
      Health["metrics"]["adapter"]["regions"][number]["retention"]["lastSweepAtNanos"]
    >().toEqualTypeOf<bigint | null>();
    expectTypeOf<
      Health["metrics"]["adapter"]["regions"][number]["retention"]["lastSweepDurationNanos"]
    >().toEqualTypeOf<bigint | null>();
    expectTypeOf<
      Health["metrics"]["adapter"]["regions"][number]["retention"]["sweepIntervalNanos"]
    >().toEqualTypeOf<bigint>();
    // @ts-expect-error materialized Kafka health always has one metric per configured Region.
    const emptyRegionMetrics: Health["metrics"]["adapter"]["regions"] = [];
    expectTypeOf(emptyRegionMetrics).not.toBeAny();
    expectTypeOf<Health["metrics"]["runtime"]["lanes"][number]["id"]>().toEqualTypeOf<
      "eu" | "us"
    >();
  });

  it("rejects invalid source shapes and async or unsafe mappings", () => {
    // @ts-expect-error retry policy aliases remain nominal Effect Schedules.
    const invalidRetryPolicy: KafkaSourceRetryPolicy = 123;
    expectTypeOf(invalidRetryPolicy).toEqualTypeOf<KafkaSourceRetryPolicy>();
    // @ts-expect-error retry policies with service environments remain nominal Schedules.
    const invalidServiceRetryPolicy: KafkaSourceRetryPolicy<"eu", unknown> = 123;
    expectTypeOf(invalidServiceRetryPolicy).toEqualTypeOf<KafkaSourceRetryPolicy<"eu", unknown>>();

    // @ts-expect-error raw Row Schemas are not canonical JSON codec factories.
    kafka.json(JsonValueRow);
    // @ts-expect-error direct canonical codecs are not lazy factories.
    kafka.json(canonicalJsonValueCodec);
    const structuralJsonCodec: Schema.Codec<typeof JsonValueRow.Type, Schema.Json, never, never> =
      canonicalJsonValueCodec;
    // @ts-expect-error structural JSON codecs lack the canonical schema witness.
    kafka.json(() => structuralJsonCodec);
    // @ts-expect-error non-JSON encoded codecs are not canonical JSON codecs.
    kafka.json(() => Schema.toCodecStringTree(JsonValueRow));
    const argumentFactory = (_schema: typeof JsonValueRow) => canonicalJsonValueCodec;
    // @ts-expect-error JSON codec factories must take zero arguments.
    kafka.json(argumentFactory);
    // @ts-expect-error JSON Row Schemas cannot require decoding services.
    kafka.json(() => Schema.toCodecJson(DecodeServiceRow));
    // @ts-expect-error JSON Row Schemas cannot require encoding services.
    kafka.json(() => Schema.toCodecJson(EncodeServiceRow));
    // @ts-expect-error the JSON factory itself cannot be any.
    kafka.json(unsafeAny);
    // @ts-expect-error JSON factory results cannot be any.
    kafka.json(() => unsafeAny);
    // @ts-expect-error JSON factory results cannot be unknown.
    kafka.json(() => unsafeUnknown);
    // @ts-expect-error JSON factory results cannot be never.
    kafka.json(() => unsafeNever);
    // @ts-expect-error JSON codecs must decode View Server row objects.
    kafka.json(() => Schema.toCodecJson(Schema.String));
    // @ts-expect-error protobuf descriptors cannot be any.
    kafka.protobuf(unsafeAny);
    // @ts-expect-error protobuf descriptor unions cannot contain non-descriptor members.
    kafka.protobuf(protobufDescriptorOrString);
    // @ts-expect-error custom codec definitions cannot be any.
    kafka.codec(unsafeAny);
    const compactionCodecWithAnyName = {
      name: unsafeAny,
      decode: () => Effect.succeed("decoded"),
    };
    // @ts-expect-error compaction codec descriptor variables reject any-typed names.
    kafka.compactionKey.codec(compactionCodecWithAnyName);
    const validCompactionCodecDescriptor = {
      name: "valid-compaction-codec" as const,
      decode: () => Effect.succeed("decoded" as const),
    };
    const alternateValidCompactionCodecDescriptor = {
      name: "alternate-valid-compaction-codec" as const,
      decode: () => Effect.succeed(1 as const),
    };
    const validCompactionCodecUnion =
      Math.random() > 0.5
        ? validCompactionCodecDescriptor
        : alternateValidCompactionCodecDescriptor;
    const validCompactionUnionCodec = kafka.compactionKey.codec(validCompactionCodecUnion);
    expectTypeOf<KafkaCodecValue<typeof validCompactionUnionCodec>>().toEqualTypeOf<
      "decoded" | 1
    >();
    const compactionCodecUnionWithAnyName =
      Math.random() > 0.5 ? validCompactionCodecDescriptor : compactionCodecWithAnyName;
    // @ts-expect-error every compaction codec descriptor union member must have a typed name.
    kafka.compactionKey.codec(compactionCodecUnionWithAnyName);
    const compactionCodecWithExtra = {
      name: "valid-compaction-codec" as const,
      decode: () => Effect.succeed(1 as const),
      extra: true,
    };
    const compactionCodecUnionWithExtra =
      Math.random() > 0.5 ? validCompactionCodecDescriptor : compactionCodecWithExtra;
    // @ts-expect-error every compaction codec descriptor union member must be exact.
    kafka.compactionKey.codec(compactionCodecUnionWithExtra);
    kafka.codec({
      name: "non-effect",
      // @ts-expect-error custom decoders must return an Effect.
      decode: (input) => input.bytes,
    });
    // @ts-expect-error custom decoder success values cannot be any.
    kafka.codec({
      name: "any-success",
      decode: () => unsafeAnySuccessEffect,
    });
    // @ts-expect-error custom decoder success values cannot be unknown.
    kafka.codec({
      name: "unknown-success",
      decode: () => unsafeUnknownSuccessEffect,
    });
    // @ts-expect-error custom decoder failures cannot be any.
    kafka.codec({
      name: "any-failure",
      decode: () => unsafeAnyFailureEffect,
    });
    // @ts-expect-error custom decoder failures cannot be unknown.
    kafka.codec({
      name: "unknown-failure",
      decode: () => unsafeUnknownFailureEffect,
    });
    const decoratedCodec = {
      name: "decorated",
      decode: () => Effect.succeed("value"),
      extra: true,
    };
    // @ts-expect-error custom codec definitions are exact even through variables.
    kafka.codec(decoratedCodec);
    expectTypeOf(kafka.protobuf(protobufDescriptor)).toExtend<{
      readonly format: "protobuf";
    }>();

    const validDeleteSourceOptions = {
      cleanupPolicy: "delete" as const,
      retentionPolicy: "Infinity" as const,
      topic: "orders-source",
      regions: ["eu"] as const,
      key,
      value,
      localRowKey: ({ key }: { readonly key: string }) => key,
      map: ({ value }: { readonly value: { readonly price: number } }) => ({
        price: value.price,
      }),
      startFrom: "earliest" as const,
    };
    kafka.source({
      ...validDeleteSourceOptions,
      localRowKey: (input) => {
        expectTypeOf(input.key).toEqualTypeOf<string>();
        expectTypeOf(input.value).toEqualTypeOf<{ readonly price: number }>();
        expectTypeOf(input.region).toEqualTypeOf<"eu">();
        // @ts-expect-error delete-only localRowKey cannot observe Kafka metadata.
        void input.metadata;
        // @ts-expect-error delete-only localRowKey cannot observe a partition.
        void input.partition;
        // @ts-expect-error delete-only localRowKey cannot observe an offset.
        void input.offset;
        return input.key;
      },
    });
    const missingCleanupPolicy = {
      retentionPolicy: "Infinity",
      topic: "orders-source",
      regions: ["eu"] as const,
      key,
      value,
      localRowKey: ({ key }: { readonly key: string }) => key,
      map: ({ value }: { readonly value: { readonly price: number } }) => ({
        price: value.price,
      }),
      startFrom: "earliest" as const,
    };
    // @ts-expect-error cleanupPolicy is mandatory for every Kafka source.
    kafka.source(missingCleanupPolicy);

    const unknownCleanupPolicy = {
      ...validDeleteSourceOptions,
      cleanupPolicy: "archive" as const,
    };
    // @ts-expect-error cleanupPolicy accepts only delete, compact, and compact-and-delete.
    kafka.source(unknownCleanupPolicy);

    const missingRetentionPolicy = {
      cleanupPolicy: "delete",
      topic: "orders-source",
      regions: ["eu"] as const,
      key,
      value,
      localRowKey: ({ key }: { readonly key: string }) => key,
      map: ({ value }: { readonly value: { readonly price: number } }) => ({
        price: value.price,
      }),
      startFrom: "earliest" as const,
    };
    // @ts-expect-error retentionPolicy is mandatory for every Kafka source.
    kafka.source(missingRetentionPolicy);

    const invalidRetentionPolicy = {
      ...validDeleteSourceOptions,
      retentionPolicy: true,
    };
    // @ts-expect-error retentionPolicy accepts only the documented policies and Effect Duration inputs.
    kafka.source(invalidRetentionPolicy);

    const retentionPolicyWithExtraField = {
      ...validDeleteSourceOptions,
      retentionPolicy: {
        minutes: 5,
        garbage: 1,
      },
    };
    // @ts-expect-error structured retentionPolicy inputs are exact through variables.
    kafka.source(retentionPolicyWithExtraField);

    const retentionPolicyWithAnyField = {
      ...validDeleteSourceOptions,
      retentionPolicy: {
        minutes: unsafeAny,
      },
    };
    // @ts-expect-error structured retentionPolicy fields cannot be any.
    kafka.source(retentionPolicyWithAnyField);

    const anyCleanupPolicy = {
      ...validDeleteSourceOptions,
      cleanupPolicy: unsafeAny,
    };
    // @ts-expect-error cleanupPolicy cannot be any.
    kafka.source(anyCleanupPolicy);

    const compactionRowIdInput = {
      region: "eu",
      partition: 0,
      serializedKeyBytes: new Uint8Array([1]),
    };
    expectTypeOf(kafka.compactionRowId(compactionRowIdInput)).toEqualTypeOf<string>();
    const compactionRowIdWithExtraField = {
      ...compactionRowIdInput,
      localRowKey: "application-owned",
    };
    // @ts-expect-error compaction row identity accepts exactly the canonical Kafka coordinates.
    kafka.compactionRowId(compactionRowIdWithExtraField);
    const compactionRowIdWithAnyPartition = {
      ...compactionRowIdInput,
      partition: unsafeAny,
    };
    // @ts-expect-error compaction row identity fields cannot be any.
    kafka.compactionRowId(compactionRowIdWithAnyPartition);

    const deleteRowIdInput = {
      region: "eu",
      partition: 0,
      localRowKey: "order-1",
    };
    expectTypeOf(kafka.deleteRowId(deleteRowIdInput)).toEqualTypeOf<string>();
    const deleteRowIdWithExtraField = {
      ...deleteRowIdInput,
      serializedKeyBytes: new Uint8Array([1]),
    };
    // @ts-expect-error delete row identity accepts exactly its documented decoded inputs.
    kafka.deleteRowId(deleteRowIdWithExtraField);
    const deleteRowIdWithAnyKey = {
      ...deleteRowIdInput,
      localRowKey: unsafeAny,
    };
    // @ts-expect-error delete row identity fields cannot be any.
    kafka.deleteRowId(deleteRowIdWithAnyKey);

    const canonicalRowIdInput = {
      cleanupPolicy: "compact" as const,
      ...compactionRowIdInput,
    };
    expectTypeOf(kafka.rowId(canonicalRowIdInput)).toEqualTypeOf<string>();
    const canonicalRowIdWithExtraField = {
      ...canonicalRowIdInput,
      localRowKey: "application-owned",
    };
    // @ts-expect-error canonical row identity rejects policy-incompatible surplus fields.
    kafka.rowId(canonicalRowIdWithExtraField);
    const canonicalRowIdWithAnyCleanup = {
      ...canonicalRowIdInput,
      cleanupPolicy: unsafeAny,
    };
    // @ts-expect-error canonical row identity cleanupPolicy cannot be any.
    kafka.rowId(canonicalRowIdWithAnyCleanup);

    const compactionWithOrdinaryKey = {
      cleanupPolicy: "compact" as const,
      retentionPolicy: "Infinity" as const,
      topic: "orders-source",
      regions: ["eu"] as const,
      key,
      value,
      map: ({ value }: { readonly value: { readonly price: number } }) => ({
        price: value.price,
      }),
      startFrom: "earliest" as const,
    };
    // @ts-expect-error compaction-capable sources require a metadata-free compaction key codec.
    kafka.source(compactionWithOrdinaryKey);

    const compactionWithoutKey = {
      cleanupPolicy: "compact" as const,
      retentionPolicy: "Infinity" as const,
      topic: "orders-source",
      regions: ["eu"] as const,
      value,
      map: ({ value }: { readonly value: { readonly price: number } }) => ({
        price: value.price,
      }),
      startFrom: "earliest" as const,
    };
    // @ts-expect-error compaction-capable sources require an explicit compaction key codec.
    kafka.source(compactionWithoutKey);

    const deleteWithCompactionKey = {
      ...validDeleteSourceOptions,
      key: compactionKey,
    };
    // @ts-expect-error delete-only sources require a metadata-aware ordinary key codec.
    kafka.source(deleteWithCompactionKey);

    const compactionWithLocalRowKey = {
      cleanupPolicy: "compact-and-delete",
      retentionPolicy: "Infinity",
      topic: "orders-source",
      regions: ["eu"] satisfies readonly ["eu"],
      key: compactionKey,
      value,
      map: (input: KafkaCompactionMappingInput<"eu", typeof compactionKey, typeof value>) => ({
        price: input.value.price,
      }),
      startFrom: "earliest",
      localRowKey: ({ key }: { readonly key: string }) => key,
    };
    // @ts-expect-error compaction-capable identity is canonical and cannot accept localRowKey.
    kafka.source(compactionWithLocalRowKey);

    const deleteWithRemovedRowKey = {
      ...validDeleteSourceOptions,
      rowKey: ({ key }: { readonly key: string }) => key,
    };
    // @ts-expect-error rowKey was removed; delete-only sources use localRowKey.
    kafka.source(deleteWithRemovedRowKey);

    kafka.source({
      cleanupPolicy: "compact",
      retentionPolicy: "match-kafka-retention",
      topic: "private-identity-inputs",
      regions: ["eu"],
      key: compactionKey,
      value,
      map: (input) => {
        // @ts-expect-error canonical identity is injected after Mapping and is never observable.
        void input.id;
        // @ts-expect-error exact serialized Kafka key bytes are private identity material.
        void input.serializedKeyBytes;
        // @ts-expect-error compaction-capable Mapping has no delete-only Local Row Key.
        void input.localRowKey;
        return { price: input.value.price };
      },
      startFrom: "earliest",
    });

    const compactionMapReturnsId = {
      cleanupPolicy: "compact" as const,
      retentionPolicy: "match-kafka-retention" as const,
      topic: "mapper-owned-identity",
      regions: ["eu"] satisfies readonly ["eu"],
      key: compactionKey,
      value,
      map: (input: KafkaCompactionMappingInput<"eu", typeof compactionKey, typeof value>) => ({
        id: "application-owned",
        price: input.value.price,
      }),
      startFrom: "earliest" as const,
    };
    // @ts-expect-error compaction Mapping cannot return or replace canonical identity.
    kafka.source(compactionMapReturnsId);

    const anyKeySource = {
      ...validDeleteSourceOptions,
      key: unsafeAny,
    };
    // @ts-expect-error Kafka key codecs cannot be any.
    kafka.source(anyKeySource);

    const anyValueSource = {
      ...validDeleteSourceOptions,
      value: unsafeAny,
      map: () => ({ price: 1 }),
    };
    // @ts-expect-error Kafka value codecs cannot be any.
    kafka.source(anyValueSource);

    const sourceWithInlineExtra = {
      ...validDeleteSourceOptions,
      extra: true,
    };
    // @ts-expect-error Kafka Source options are exact.
    kafka.source(sourceWithInlineExtra);

    const emptyRegionsSource = {
      ...validDeleteSourceOptions,
      regions: [],
    };
    // @ts-expect-error Kafka regions must be a non-empty tuple.
    kafka.source(emptyRegionsSource);

    const anyRegionMemberSource = {
      ...validDeleteSourceOptions,
      regions: ["eu", unsafeAny],
    };
    // @ts-expect-error every Kafka Region tuple member must reject any.
    kafka.source(anyRegionMemberSource);

    // @ts-expect-error Kafka definitions must be constructed through kafka.source.
    KafkaSourceAdapter.materializedSource({
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      localRowKey: () => 1,
      map: async () => ({ price: 1 }),
      startFrom: "earliest",
    });

    // @ts-expect-error The public descriptor cannot be passed to the delegated SDK builder.
    SourceAdapter.materializedSource(KafkaSourceAdapter, {
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      localRowKey: () => 1,
      map: async () => ({ price: 1 }),
      startFrom: "earliest",
    });

    // @ts-expect-error Runtime service lookup must not recover the hidden SDK builder.
    kafkaRuntimeService.adapter.materializedSource({
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      localRowKey: () => 1,
      map: async () => ({ price: 1 }),
      startFrom: "earliest",
    });

    const anyLocalRowKeySource = {
      ...validDeleteSourceOptions,
      localRowKey: () => unsafeAny,
    };
    // @ts-expect-error localRowKey cannot erase its result to any.
    kafka.source(anyLocalRowKeySource);

    const neverLocalRowKeySource = {
      ...validDeleteSourceOptions,
      localRowKey: () => unsafeNever,
    };
    // @ts-expect-error localRowKey cannot return never.
    kafka.source(neverLocalRowKeySource);

    const numericLocalRowKeySource = {
      ...validDeleteSourceOptions,
      localRowKey: () => 1,
    };
    // @ts-expect-error localRowKey must return a string.
    kafka.source(numericLocalRowKeySource);

    const effectMappingSource = {
      ...validDeleteSourceOptions,
      map: () => Effect.succeed({ price: 1 }),
    };
    // @ts-expect-error Mapping cannot return Effect.
    kafka.source(effectMappingSource);

    const promiseMappingSource = {
      ...validDeleteSourceOptions,
      map: async () => ({ price: 1 }),
    };
    // @ts-expect-error Mapping cannot return Promise.
    kafka.source(promiseMappingSource);

    const optionMappingSource = {
      ...validDeleteSourceOptions,
      map: () => Option.some({ price: 1 }),
    };
    // @ts-expect-error Mapping cannot return Option.
    kafka.source(optionMappingSource);

    const undefinedMappingSource = {
      ...validDeleteSourceOptions,
      map: () => undefined,
    };
    // @ts-expect-error Mapping cannot return undefined.
    kafka.source(undefinedMappingSource);

    const idMappingSource = {
      ...validDeleteSourceOptions,
      map: () => ({ id: "owned", price: 1 }),
    };
    // @ts-expect-error Mapping cannot return id; the Adapter owns it.
    kafka.source(idMappingSource);

    const anyMappingSource = {
      ...validDeleteSourceOptions,
      map: () => unsafeAny,
    };
    // @ts-expect-error Mapping cannot erase its result to any.
    kafka.source(anyMappingSource);

    const neverMappingSource = {
      ...validDeleteSourceOptions,
      map: () => unsafeNever,
    };
    // @ts-expect-error Mapping cannot return never.
    kafka.source(neverMappingSource);

    const anyTopicSource = {
      ...validDeleteSourceOptions,
      topic: unsafeAny,
    };
    // @ts-expect-error Kafka source Topics cannot be any.
    kafka.source(anyTopicSource);

    const anyRegionsSource = {
      ...validDeleteSourceOptions,
      regions: unsafeAny,
    };
    // @ts-expect-error Kafka source Regions cannot be any.
    kafka.source(anyRegionsSource);

    const anyStartSource = {
      ...validDeleteSourceOptions,
      startFrom: unsafeAny,
    };
    // @ts-expect-error Kafka Start Position cannot be any.
    kafka.source(anyStartSource);

    const timestampWithExtra = {
      mode: "timestamp" as const,
      atNanos: 1n,
      fallback: "earliest" as const,
      extra: true,
    };
    const extraTimestampSource = {
      ...validDeleteSourceOptions,
      startFrom: timestampWithExtra,
    };
    // @ts-expect-error Start Position branches are exact through variables.
    kafka.source(extraTimestampSource);

    const numericCommittedGroupSource = {
      ...validDeleteSourceOptions,
      startFrom: {
        mode: "committed" as const,
        consumerGroupId: 1,
        fallback: "earliest" as const,
      },
    };
    // @ts-expect-error committed starts require a string consumer group ID.
    kafka.source(numericCommittedGroupSource);

    const numericTimestampSource = {
      ...validDeleteSourceOptions,
      startFrom: {
        mode: "timestamp" as const,
        atNanos: 1,
        fallback: "latest" as const,
      },
    };
    // @ts-expect-error timestamp starts require epoch nanoseconds as bigint.
    kafka.source(numericTimestampSource);

    const invalidDurationFallbackSource = {
      ...validDeleteSourceOptions,
      startFrom: {
        mode: "durationAgo" as const,
        duration: "5 minutes",
        fallback: "middle",
      },
    };
    // @ts-expect-error structured starts require an exact fallback.
    kafka.source(invalidDurationFallbackSource);

    const missingCommittedGroupSource = {
      ...validDeleteSourceOptions,
      startFrom: {
        mode: "committed" as const,
        fallback: "fail" as const,
      },
    };
    // @ts-expect-error committed starts require consumerGroupId.
    kafka.source(missingCommittedGroupSource);

    const sourceWithExtra = {
      ...validDeleteSourceOptions,
      extra: true,
    };
    // @ts-expect-error Source options are exact through variables.
    kafka.source(sourceWithExtra);

    // @ts-expect-error retry policies cannot be any.
    kafka.source(validDeleteSourceOptions, unsafeAny);
    // @ts-expect-error retry overrides must be Source Retry Policy schedules.
    kafka.source(validDeleteSourceOptions, 123);
    const wrongRegionRetry =
      Schedule.identity<
        import("effect-view-server/source-adapter").SourceTermination<KafkaAdapterFailure<"apac">>
      >();
    // @ts-expect-error retry inputs must use this definition's exact Region failure.
    kafka.source(validDeleteSourceOptions, wrongRegionRetry);
  });
});
