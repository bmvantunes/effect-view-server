import { describe, expectTypeOf, it } from "@effect/vitest";
import type { DescMessage } from "@bufbuild/protobuf";
import {
  SourceAdapter,
  type SourceDefinitionRetryServices,
  type SourceHealthForDefinition,
  type SourceTermination,
} from "effect-view-server/source-adapter";
import { Context, Effect, Option, Schedule, Schema } from "effect";
import {
  KafkaSourceAdapter,
  kafka,
  type KafkaAdapterFailure,
  type KafkaCapturedStartPosition,
  type KafkaCodecDecodeInput,
  type KafkaCodecFailure,
  type KafkaCodecValue,
  type KafkaMaterializedMetrics,
  type KafkaRegionMetrics,
  type KafkaSourceDefinition,
  type KafkaSourceRetryPolicy,
} from "@effect-view-server/kafka/contract";

const JsonValueRow = Schema.Struct({
  price: Schema.Number,
});
const canonicalJsonValueCodec = Schema.toCodecJson(JsonValueRow);
const key = kafka.string();
const value = kafka.json(() => canonicalJsonValueCodec);
const source = kafka.source({
  topic: "orders-source",
  regions: ["eu", "us"],
  key,
  value,
  localRowKey: ({ key, region, metadata }) => {
    expectTypeOf(key).toEqualTypeOf<string>();
    expectTypeOf(region).toEqualTypeOf<"eu" | "us">();
    expectTypeOf(metadata.sourceRegion).toEqualTypeOf<"eu" | "us">();
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
    expectTypeOf(source.adapter).toEqualTypeOf<typeof KafkaSourceAdapter>();
    expectTypeOf(source.lifecycle).toEqualTypeOf<"materialized">();
    expectTypeOf(source.options.regions).toEqualTypeOf<readonly ["eu", "us"]>();
    expectTypeOf(invalidConditionalRowSource).not.toBeAny();
    expectTypeOf(source).toExtend<
      KafkaSourceDefinition<readonly ["eu", "us"], typeof key, typeof value>
    >();
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
        Health["status"],
        { readonly _tag: "Degraded" }
      >["latestRejection"]["location"]["region"]
    >().toEqualTypeOf<"eu" | "us">();
    expectTypeOf<Health["metrics"]["adapter"]["regions"][number]["region"]>().toEqualTypeOf<
      "eu" | "us"
    >();
    expectTypeOf<KafkaMaterializedMetrics<readonly ["eu", "us"]>["regions"]>().toEqualTypeOf<
      readonly [KafkaRegionMetrics<"eu">, KafkaRegionMetrics<"us">]
    >();
    expectTypeOf<Health["metrics"]["adapter"]["regions"]>().toEqualTypeOf<
      readonly [KafkaRegionMetrics<"eu">, KafkaRegionMetrics<"us">]
    >();
    expectTypeOf<Health["metrics"]["adapter"]["regions"][0]["region"]>().toEqualTypeOf<"eu">();
    expectTypeOf<Health["metrics"]["adapter"]["regions"][1]["region"]>().toEqualTypeOf<"us">();
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

    kafka.source({
      topic: "orders-source",
      regions: ["eu"],
      // @ts-expect-error Kafka key codecs cannot be any.
      key: unsafeAny,
      value,
      localRowKey: ({ key }) => String(key),
      map: ({ value }) => ({ price: value.price }),
      startFrom: "earliest",
    });

    kafka.source({
      topic: "orders-source",
      regions: ["eu"],
      key,
      // @ts-expect-error Kafka value codecs cannot be any.
      value: unsafeAny,
      localRowKey: ({ key }) => key,
      map: () => ({ price: 1 }),
      startFrom: "earliest",
    });

    kafka.source({
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      localRowKey: ({ key }: { readonly key: string }) => key,
      map: ({ value }) => ({ price: value.price }),
      startFrom: "latest",
      // @ts-expect-error Kafka Source options are exact.
      extra: true,
    });

    kafka.source({
      topic: "orders-source",
      // @ts-expect-error Kafka regions must be a non-empty tuple.
      regions: [],
      key,
      value,
      localRowKey: ({ key }: { readonly key: string }) => key,
      map: ({ value }) => ({ price: value.price }),
      startFrom: "earliest",
    });

    kafka.source({
      topic: "orders-source",
      // @ts-expect-error every Kafka Region tuple member must reject any.
      regions: ["eu", unsafeAny],
      key,
      value,
      localRowKey: ({ key }) => key,
      map: ({ value }) => ({ price: value.price }),
      startFrom: "earliest",
    });

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

    kafka.source({
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      // @ts-expect-error localRowKey cannot erase its result to any.
      localRowKey: () => unsafeAny,
      map: ({ value }) => ({ price: value.price }),
      startFrom: "earliest",
    });

    kafka.source({
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      // @ts-expect-error localRowKey cannot return never.
      localRowKey: () => unsafeNever,
      map: ({ value }) => ({ price: value.price }),
      startFrom: "earliest",
    });

    kafka.source({
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      // @ts-expect-error localRowKey must return a string.
      localRowKey: () => 1,
      map: ({ value }) => ({ price: value.price }),
      startFrom: "earliest",
    });

    kafka.source({
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      localRowKey: ({ key }) => key,
      // @ts-expect-error Mapping cannot return Effect.
      map: ({ value }) => Effect.succeed({ price: value.price }),
      startFrom: "earliest",
    });

    kafka.source({
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      localRowKey: ({ key }) => key,
      // @ts-expect-error Mapping cannot return Promise.
      map: async ({ value }) => ({ price: value.price }),
      startFrom: "earliest",
    });

    kafka.source({
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      localRowKey: ({ key }) => key,
      // @ts-expect-error Mapping cannot return Option.
      map: ({ value }) => Option.some({ price: value.price }),
      startFrom: "earliest",
    });

    kafka.source({
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      localRowKey: ({ key }) => key,
      // @ts-expect-error Mapping cannot return undefined.
      map: () => undefined,
      startFrom: "earliest",
    });

    kafka.source({
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      localRowKey: ({ key }: { readonly key: string }) => key,
      // @ts-expect-error Mapping cannot return id; the Adapter owns it.
      map: ({ value }) => ({ id: "owned", price: value.price }),
      startFrom: "earliest",
    });

    kafka.source({
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      localRowKey: ({ key }: { readonly key: string }) => key,
      // @ts-expect-error Mapping cannot erase its result to any.
      map: () => unsafeAny,
      startFrom: "earliest",
    });

    kafka.source({
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      localRowKey: ({ key }) => key,
      // @ts-expect-error Mapping cannot return never.
      map: () => unsafeNever,
      startFrom: "earliest",
    });

    kafka.source({
      // @ts-expect-error Kafka source Topics cannot be any.
      topic: unsafeAny,
      regions: ["eu"],
      key,
      value,
      localRowKey: ({ key }) => key,
      map: ({ value }) => ({ price: value.price }),
      startFrom: "earliest",
    });

    kafka.source({
      topic: "orders-source",
      // @ts-expect-error Kafka source Regions cannot be any.
      regions: unsafeAny,
      key,
      value,
      localRowKey: ({ key }) => key,
      map: ({ value }) => ({ price: value.price }),
      startFrom: "earliest",
    });

    kafka.source({
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      localRowKey: ({ key }) => key,
      map: ({ value }) => ({ price: value.price }),
      // @ts-expect-error Kafka Start Position cannot be any.
      startFrom: unsafeAny,
    });

    const timestampWithExtra = {
      mode: "timestamp" as const,
      atNanos: 1n,
      fallback: "earliest" as const,
      extra: true,
    };
    kafka.source({
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      localRowKey: ({ key }) => key,
      map: ({ value }) => ({ price: value.price }),
      // @ts-expect-error Start Position branches are exact through variables.
      startFrom: timestampWithExtra,
    });

    kafka.source({
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      localRowKey: ({ key }) => key,
      map: ({ value }) => ({ price: value.price }),
      startFrom: {
        mode: "committed",
        // @ts-expect-error committed starts require a string consumer group ID.
        consumerGroupId: 1,
        fallback: "earliest",
      },
    });

    kafka.source({
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      localRowKey: ({ key }) => key,
      map: ({ value }) => ({ price: value.price }),
      startFrom: {
        mode: "timestamp",
        // @ts-expect-error timestamp starts require epoch nanoseconds as bigint.
        atNanos: 1,
        fallback: "latest",
      },
    });

    kafka.source({
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      localRowKey: ({ key }) => key,
      map: ({ value }) => ({ price: value.price }),
      startFrom: {
        mode: "durationAgo",
        duration: "5 minutes",
        // @ts-expect-error structured starts require an exact fallback.
        fallback: "middle",
      },
    });

    kafka.source({
      topic: "orders-source",
      regions: ["eu"],
      key,
      value,
      localRowKey: ({ key }) => key,
      map: ({ value }) => ({ price: value.price }),
      // @ts-expect-error committed starts require consumerGroupId.
      startFrom: {
        mode: "committed",
        fallback: "fail",
      },
    });

    const sourceWithExtra = {
      topic: "orders-source",
      regions: ["eu"] as const,
      key,
      value,
      localRowKey: ({ key }: { readonly key: string }) => key,
      map: ({ value }: { readonly value: { readonly price: number } }) => ({
        price: value.price,
      }),
      startFrom: "earliest" as const,
      extra: true,
    };
    // @ts-expect-error Source options are exact through variables.
    kafka.source(sourceWithExtra);

    // @ts-expect-error retry policies cannot be any.
    kafka.source(
      {
        topic: "orders-source",
        regions: ["eu"],
        key,
        value,
        localRowKey: ({ key }) => key,
        map: ({ value }) => ({ price: value.price }),
        startFrom: "earliest",
      },
      unsafeAny,
    );

    kafka.source(
      {
        topic: "orders-source",
        regions: ["eu"],
        key,
        value,
        localRowKey: ({ key }) => key,
        map: ({ value }) => ({ price: value.price }),
        startFrom: "earliest",
      },
      // @ts-expect-error retry overrides must be Source Retry Policy schedules.
      123,
    );

    kafka.source(
      {
        topic: "orders-source",
        regions: ["eu"],
        key,
        value,
        localRowKey: ({ key }) => key,
        map: ({ value }) => ({ price: value.price }),
        startFrom: "earliest",
      },
      // @ts-expect-error retry inputs must use this definition's exact Region failure.
      Schedule.identity<
        import("effect-view-server/source-adapter").SourceTermination<KafkaAdapterFailure<"apac">>
      >(),
    );
  });
});
