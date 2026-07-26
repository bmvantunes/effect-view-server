import { fromBinary } from "@bufbuild/protobuf";
import type { DescMessage, MessageShape } from "@bufbuild/protobuf";
import { Duration, Effect, Option, Result, Schema } from "effect";
import type { Schedule } from "effect";
import {
  SourceAdapter,
  type SourceAdapterHandle,
  type SourceDefinition,
  type SourceLifecycleDeclaration,
  type SourceRetryPolicy,
} from "effect-view-server/source-adapter";

const KafkaCodecTypeId: unique symbol = Symbol("@effect-view-server/kafka/KafkaCodec");
const KafkaCodecDecodeTypeId: unique symbol = Symbol("@effect-view-server/kafka/KafkaCodecDecode");

type IsAny<Value> = 0 extends 1 & Value ? true : false;

type IsUnknown<Value> =
  IsAny<Value> extends true
    ? false
    : unknown extends Value
      ? [Value] extends [unknown]
        ? true
        : false
      : false;

type IsNever<Value> = [Value] extends [never] ? true : false;

type RejectAny<Value> =
  IsAny<Value> extends true
    ? {
        readonly __kafkaValueCannotBeAny: never;
      }
    : unknown;

type RejectUnknown<Value> =
  IsUnknown<Value> extends true
    ? {
        readonly __kafkaValueCannotBeUnknown: never;
      }
    : unknown;

type RejectExtraKeys<Candidate, Shape> = {
  readonly [Key in Exclude<keyof Candidate, keyof Shape>]: never;
};

type KafkaRowSchema = Schema.Codec<object, unknown, never, never> & {
  readonly fields: Readonly<Record<string, Schema.Codec<unknown, unknown, never, never>>>;
};

const isKafkaRowSchema = (value: unknown): value is KafkaRowSchema =>
  Result.try(() => {
    if (!Schema.isSchema(value)) {
      return false;
    }
    const fields = Reflect.get(value, "fields");
    if (typeof fields !== "object" || fields === null || Array.isArray(fields)) {
      return false;
    }
    return Reflect.ownKeys(fields).every(
      (key) => typeof key === "string" && Schema.isSchema(Reflect.get(fields, key)),
    );
  }).pipe(
    Result.match({
      onFailure: () => false,
      onSuccess: (isRowSchema) => isRowSchema,
    }),
  );

export type KafkaNonEmptyReadonlyArray<Value> = readonly [Value, ...ReadonlyArray<Value>];

export type KafkaMessageMetadata<Region extends string = string> = {
  readonly sourceTopic: string;
  readonly sourceRegion: Region;
  readonly partition: number;
  readonly offset: bigint;
  readonly timestampNanos: bigint;
  readonly headers: Readonly<Record<string, Uint8Array | ReadonlyArray<Uint8Array>>>;
};

export type KafkaCodecDecodeInput<Region extends string = string> = {
  readonly bytes: Uint8Array;
  readonly metadata: KafkaMessageMetadata<Region>;
};

export type KafkaCodec<Value, Error = never> = {
  readonly [KafkaCodecTypeId]: () => KafkaCodec<Value, Error>;
  readonly [KafkaCodecDecodeTypeId]: (input: KafkaCodecDecodeInput) => Effect.Effect<Value, Error>;
  readonly format: string;
};

export type KafkaBytesCodec = KafkaCodec<Uint8Array> & {
  readonly format: "bytes";
};

export type KafkaStringCodec = KafkaCodec<string> & {
  readonly format: "string";
};

export type KafkaJsonCodec<SourceSchema extends KafkaRowSchema = KafkaRowSchema> = KafkaCodec<
  SourceSchema["Type"],
  KafkaCodecError
> & {
  readonly format: "json";
};

export type KafkaProtobufCodec<Descriptor extends DescMessage = DescMessage> = KafkaCodec<
  MessageShape<Descriptor>,
  KafkaCodecError
> & {
  readonly descriptor: Descriptor;
  readonly format: "protobuf";
};

export type KafkaCustomCodec<Value, Error> = KafkaCodec<Value, Error> & {
  readonly format: "custom";
  readonly name: string;
};

export type KafkaCodecValue<Codec> = Codec extends KafkaCodec<infer Value, unknown> ? Value : never;

export type KafkaCodecFailure<Codec> =
  Codec extends KafkaCodec<unknown, infer Error> ? Error : never;

export const KafkaCodecError = Schema.TaggedStruct("KafkaCodecError", {
  message: Schema.String,
});
export type KafkaCodecError = typeof KafkaCodecError.Type;

const codecError = (message: string): KafkaCodecError => ({
  _tag: "KafkaCodecError",
  message,
});

const makeCodec = <Value, Error, const Format extends string>(
  format: Format,
  decode: (input: KafkaCodecDecodeInput) => Effect.Effect<Value, Error>,
): KafkaCodec<Value, Error> & { readonly format: Format } => {
  const codec: KafkaCodec<Value, Error> & { readonly format: Format } = {
    [KafkaCodecTypeId]: () => codec,
    [KafkaCodecDecodeTypeId]: decode,
    format,
  };
  return SourceAdapter.executable(codec);
};

export const isKafkaCodec = (value: unknown): value is KafkaCodec<unknown, unknown> => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const brand = Result.try(() => Reflect.get(value, KafkaCodecTypeId));
  const decode = Result.try(() => Reflect.get(value, KafkaCodecDecodeTypeId));
  return (
    Result.isSuccess(brand) &&
    typeof brand.success === "function" &&
    Result.isSuccess(decode) &&
    typeof decode.success === "function" &&
    Result.try(() => Reflect.apply(brand.success, undefined, [])).pipe(
      Result.match({
        onFailure: () => false,
        onSuccess: (branded) => branded === value,
      }),
    )
  );
};

export const decodeKafkaCodec = <Value, Error>(
  codec: KafkaCodec<Value, Error>,
  input: KafkaCodecDecodeInput,
): Effect.Effect<Value, Error> => codec[KafkaCodecDecodeTypeId](input);

const textDecoder = new TextDecoder();

const bytesCodec = (): KafkaBytesCodec =>
  makeCodec("bytes", (input) => Effect.succeed(input.bytes));

const stringCodec = (): KafkaStringCodec =>
  makeCodec("string", (input) => Effect.succeed(textDecoder.decode(input.bytes)));

type KafkaJsonFactory = () => { readonly schema: KafkaRowSchema };

type KafkaJsonFactorySourceSchema<Factory extends KafkaJsonFactory> = ReturnType<Factory>["schema"];

type SupportedKafkaJsonFactory<Factory extends KafkaJsonFactory> =
  IsAny<Factory> extends true
    ? never
    : IsAny<ReturnType<Factory>> extends true
      ? never
      : IsAny<KafkaJsonFactorySourceSchema<Factory>> extends true
        ? never
        : [ReturnType<Factory>] extends [never]
          ? never
          : Parameters<Factory> extends []
            ? ReturnType<Factory> extends Schema.toCodecJson<KafkaJsonFactorySourceSchema<Factory>>
              ? unknown
              : never
            : never;

type KafkaJsonDecoderState<SourceSchema extends KafkaRowSchema> =
  | {
      readonly _tag: "Ready";
      readonly decode: (input: unknown) => Effect.Effect<SourceSchema["Type"], Schema.SchemaError>;
    }
  | {
      readonly _tag: "Failed";
      readonly error: KafkaCodecError;
    };

function jsonCodec<const Factory extends KafkaJsonFactory>(
  factory: (() => Schema.toCodecJson<KafkaJsonFactorySourceSchema<Factory>>) &
    Factory &
    SupportedKafkaJsonFactory<Factory>,
): KafkaJsonCodec<KafkaJsonFactorySourceSchema<Factory>>;
function jsonCodec<const SourceSchema extends KafkaRowSchema>(
  factory: () => Schema.toCodecJson<SourceSchema>,
): KafkaJsonCodec<SourceSchema> {
  if (typeof factory !== "function") {
    throw new KafkaSourceConfigurationError(
      "Kafka JSON codec requires a factory returning a JSON-compatible Schema.",
    );
  }
  const codec = Result.try(factory);
  const decoder = ((): KafkaJsonDecoderState<SourceSchema> => {
    if (Result.isFailure(codec)) {
      return {
        _tag: "Failed",
        error: codecError("Kafka JSON schema is not JSON-compatible"),
      };
    }
    const schema = codec.success;
    const canonicalWitness = Result.try(
      () =>
        Schema.isSchema(schema) &&
        Object.hasOwn(schema, "schema") &&
        isKafkaRowSchema(schema.schema),
    );
    if (Result.isFailure(canonicalWitness) || !canonicalWitness.success) {
      throw new KafkaSourceConfigurationError(
        "Kafka JSON codec requires a factory returning a JSON-compatible Schema.",
      );
    }
    return {
      _tag: "Ready",
      decode: Schema.decodeUnknownEffect(schema),
    };
  })();
  return makeCodec<SourceSchema["Type"], KafkaCodecError, "json">("json", (input) => {
    if (decoder._tag === "Failed") {
      return Effect.fail(decoder.error);
    }
    return Effect.try({
      try: (): unknown => JSON.parse(textDecoder.decode(input.bytes)),
      catch: () => codecError("Kafka JSON payload is not valid JSON."),
    }).pipe(
      Effect.flatMap((value) =>
        decoder
          .decode(value)
          .pipe(
            Effect.mapError(() => codecError("Kafka JSON payload does not satisfy its Schema.")),
          ),
      ),
    );
  });
}

type KafkaProtobufAdditionalArguments<Descriptor> =
  IsAny<Descriptor> extends true
    ? readonly [never]
    : IsUnknown<Descriptor> extends true
      ? readonly [never]
      : IsNever<Descriptor> extends true
        ? readonly [never]
        : Descriptor extends DescMessage
          ? readonly []
          : readonly [never];

function protobufCodec<const Descriptor>(
  descriptor: Descriptor,
  ..._unsupported: KafkaProtobufAdditionalArguments<NoInfer<Descriptor>>
): KafkaProtobufCodec<Extract<Descriptor, DescMessage>>;
function protobufCodec(descriptor: DescMessage): KafkaProtobufCodec<DescMessage> {
  const codec = makeCodec<MessageShape<DescMessage>, KafkaCodecError, "protobuf">(
    "protobuf",
    (input) =>
      Effect.try({
        try: () => fromBinary(descriptor, input.bytes),
        catch: () => codecError("Kafka protobuf payload could not be decoded."),
      }),
  );
  const protobuf: KafkaProtobufCodec<DescMessage> = {
    [KafkaCodecTypeId]: () => protobuf,
    [KafkaCodecDecodeTypeId]: (input) => decodeKafkaCodec(codec, input),
    format: "protobuf",
    descriptor,
  };
  return SourceAdapter.executable(protobuf);
}

type KafkaCustomDecodeResult = Effect.Effect<unknown, unknown>;

type KafkaCustomDecodeValue<DecodeResult> =
  DecodeResult extends Effect.Effect<infer Value, unknown> ? Value : never;

type KafkaCustomDecodeFailure<DecodeResult> =
  DecodeResult extends Effect.Effect<unknown, infer Error> ? Error : never;

type KafkaCustomCodecShape = {
  readonly name: string;
  readonly decode: (input: KafkaCodecDecodeInput) => KafkaCustomDecodeResult;
};

type KafkaCustomCodecAdditionalArguments<Definition> =
  IsAny<Definition> extends true ? readonly [never] : readonly [];

function customCodec<
  const Definition extends {
    readonly name: string;
    readonly decode: (...arguments_: ReadonlyArray<never>) => unknown;
  },
>(
  definition: Definition & {
    readonly decode: (input: KafkaCodecDecodeInput) => KafkaCustomDecodeResult;
  } & RejectAny<KafkaCustomDecodeValue<NoInfer<ReturnType<Definition["decode"]>>>> &
    RejectUnknown<KafkaCustomDecodeValue<NoInfer<ReturnType<Definition["decode"]>>>> &
    RejectAny<KafkaCustomDecodeFailure<NoInfer<ReturnType<Definition["decode"]>>>> &
    RejectUnknown<KafkaCustomDecodeFailure<NoInfer<ReturnType<Definition["decode"]>>>> &
    RejectExtraKeys<Definition, KafkaCustomCodecShape>,
  ..._unsupported: KafkaCustomCodecAdditionalArguments<NoInfer<Definition>>
): KafkaCustomCodec<
  KafkaCustomDecodeValue<ReturnType<Definition["decode"]>>,
  KafkaCustomDecodeFailure<ReturnType<Definition["decode"]>>
>;
function customCodec(definition: KafkaCustomCodecShape): KafkaCustomCodec<unknown, unknown> {
  if (
    typeof definition !== "object" ||
    definition === null ||
    Object.keys(definition).length !== 2 ||
    !Object.hasOwn(definition, "name") ||
    !Object.hasOwn(definition, "decode") ||
    typeof definition.name !== "string" ||
    definition.name.length === 0 ||
    typeof definition.decode !== "function"
  ) {
    throw new KafkaSourceConfigurationError(
      "Kafka custom codec requires exactly a non-empty name and decode function.",
    );
  }
  const codec: KafkaCustomCodec<unknown, unknown> = {
    [KafkaCodecTypeId]: () => codec,
    [KafkaCodecDecodeTypeId]: definition.decode,
    format: "custom",
    name: definition.name,
  };
  return SourceAdapter.executable(codec);
}

export type KafkaStartFallback = "earliest" | "latest" | "fail";

export type KafkaStartPosition =
  | "earliest"
  | "latest"
  | {
      readonly mode: "committed";
      readonly consumerGroupId: string;
      readonly fallback: KafkaStartFallback;
    }
  | {
      readonly mode: "timestamp";
      readonly atNanos: bigint;
      readonly fallback: KafkaStartFallback;
    }
  | {
      readonly mode: "durationAgo";
      readonly duration: Duration.Input;
      readonly fallback: KafkaStartFallback;
    };

export type KafkaCapturedStartPosition =
  | "earliest"
  | "latest"
  | {
      readonly mode: "committed";
      readonly consumerGroupId: string;
      readonly fallback: KafkaStartFallback;
    }
  | {
      readonly mode: "timestamp";
      readonly atNanos: bigint;
      readonly fallback: KafkaStartFallback;
    }
  | {
      readonly mode: "durationAgo";
      readonly durationNanos: bigint;
      readonly fallback: KafkaStartFallback;
    };

export type KafkaResolvedStartPosition =
  | {
      readonly mode: "earliest";
    }
  | {
      readonly mode: "latest";
    }
  | {
      readonly mode: "committed";
      readonly consumerGroupId: string;
      readonly fallback: KafkaStartFallback;
    }
  | {
      readonly mode: "timestamp";
      readonly atNanos: bigint;
      readonly atMillis: bigint;
      readonly fallback: KafkaStartFallback;
    }
  | {
      readonly mode: "durationAgo";
      readonly durationNanos: bigint;
      readonly resolvedAtNanos: bigint;
      readonly atNanos: bigint;
      readonly atMillis: bigint;
      readonly fallback: KafkaStartFallback;
    };

const KafkaStartFallbackSchema = Schema.Literals(["earliest", "latest", "fail"]);

export const KafkaResolvedStartPositionSchema: Schema.Codec<KafkaResolvedStartPosition> =
  Schema.Union([
    Schema.Struct({
      mode: Schema.Literal("earliest"),
    }),
    Schema.Struct({
      mode: Schema.Literal("latest"),
    }),
    Schema.Struct({
      mode: Schema.Literal("committed"),
      consumerGroupId: Schema.NonEmptyString,
      fallback: KafkaStartFallbackSchema,
    }),
    Schema.Struct({
      mode: Schema.Literal("timestamp"),
      atNanos: Schema.BigInt,
      atMillis: Schema.BigInt,
      fallback: KafkaStartFallbackSchema,
    }),
    Schema.Struct({
      mode: Schema.Literal("durationAgo"),
      durationNanos: Schema.BigInt,
      resolvedAtNanos: Schema.BigInt,
      atNanos: Schema.BigInt,
      atMillis: Schema.BigInt,
      fallback: KafkaStartFallbackSchema,
    }),
  ]);

export type KafkaStartResolution =
  | {
      readonly _tag: "Pending";
    }
  | {
      readonly _tag: "Resolved";
      readonly position: KafkaResolvedStartPosition;
    };

export const KafkaStartResolutionSchema: Schema.Codec<KafkaStartResolution> = Schema.Union([
  Schema.TaggedStruct("Pending", {}),
  Schema.TaggedStruct("Resolved", {
    position: KafkaResolvedStartPositionSchema,
  }),
]);

export type KafkaRejectionPhase =
  | "keyDecode"
  | "valueDecode"
  | "localRowKey"
  | "canonicalId"
  | "mapping"
  | "topicSchema";

export const KafkaRejectionPhaseSchema = Schema.Literals([
  "keyDecode",
  "valueDecode",
  "localRowKey",
  "canonicalId",
  "mapping",
  "topicSchema",
]);

export const KafkaSourceRejectionLocation = Schema.Struct({
  region: Schema.NonEmptyString,
  topic: Schema.NonEmptyString,
  partition: Schema.Int,
  offset: Schema.BigInt,
  phase: KafkaRejectionPhaseSchema,
  message: Schema.String,
});
export type KafkaSourceRejectionLocation = typeof KafkaSourceRejectionLocation.Type;

export const KafkaAdapterFailure = Schema.Union([
  Schema.TaggedStruct("KafkaConfigurationFailure", {
    message: Schema.String,
  }),
  Schema.TaggedStruct("KafkaAcquisitionFailure", {
    region: Schema.NonEmptyString,
    topic: Schema.NonEmptyString,
    message: Schema.String,
  }),
  Schema.TaggedStruct("KafkaConsumeFailure", {
    region: Schema.NonEmptyString,
    topic: Schema.NonEmptyString,
    message: Schema.String,
  }),
  Schema.TaggedStruct("KafkaDecodeFailure", {
    region: Schema.NonEmptyString,
    topic: Schema.NonEmptyString,
    message: Schema.String,
  }),
  Schema.TaggedStruct("KafkaMappingFailure", {
    region: Schema.NonEmptyString,
    topic: Schema.NonEmptyString,
    message: Schema.String,
  }),
  Schema.TaggedStruct("KafkaCommitFailure", {
    region: Schema.NonEmptyString,
    topic: Schema.NonEmptyString,
    message: Schema.String,
  }),
  Schema.TaggedStruct("KafkaReleaseFailure", {
    region: Schema.NonEmptyString,
    topic: Schema.NonEmptyString,
    message: Schema.String,
  }),
]);
export type KafkaAdapterFailure = typeof KafkaAdapterFailure.Type;

export const KafkaPartitionMetrics = Schema.Struct({
  partition: Schema.Int,
  offset: Schema.BigInt,
  lag: Schema.BigInt,
});
export type KafkaPartitionMetrics = typeof KafkaPartitionMetrics.Type;

export const KafkaRegionMetrics = Schema.Struct({
  region: Schema.NonEmptyString,
  assignments: Schema.Array(KafkaPartitionMetrics),
  commits: Schema.BigInt,
  commitFailures: Schema.BigInt,
  decoded: Schema.BigInt,
  decodeFailures: Schema.BigInt,
  mapped: Schema.BigInt,
  mappingFailures: Schema.BigInt,
  rejections: Schema.BigInt,
  reconnects: Schema.BigInt,
  rebalances: Schema.BigInt,
  closes: Schema.BigInt,
  closeFailures: Schema.BigInt,
});
export type KafkaRegionMetrics = typeof KafkaRegionMetrics.Type;

export const KafkaMaterializedMetrics = Schema.Struct({
  activeGroupId: Schema.NonEmptyString,
  start: KafkaStartResolutionSchema,
  regions: Schema.Array(KafkaRegionMetrics),
});
export type KafkaMaterializedMetrics = typeof KafkaMaterializedMetrics.Type;

export type KafkaLocalRowKeyInput<
  Region extends string,
  KeyCodec extends KafkaCodec<unknown, unknown>,
> = {
  readonly key: KafkaCodecValue<KeyCodec>;
  readonly region: Region;
  readonly metadata: KafkaMessageMetadata<Region>;
};

export type KafkaMappingInput<
  Region extends string,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
> = KafkaLocalRowKeyInput<Region, KeyCodec> & {
  readonly value: KafkaCodecValue<ValueCodec>;
  readonly localRowKey: string;
};

type KafkaRuntimeDefinitionOptions = {
  readonly topic: string;
  readonly regions: KafkaNonEmptyReadonlyArray<string>;
  readonly key: KafkaCodec<unknown, unknown>;
  readonly value: KafkaCodec<unknown, unknown>;
  readonly localRowKey: (input: never) => unknown;
  readonly map: (input: never) => unknown;
  readonly startFrom: KafkaCapturedStartPosition;
};

type KafkaMaterializedLifecycle = SourceLifecycleDeclaration<
  KafkaMaterializedMetrics,
  KafkaSourceRejectionLocation,
  KafkaRuntimeDefinitionOptions
>;

export const KafkaSourceAdapter: SourceAdapterHandle<
  "kafka",
  "1",
  KafkaAdapterFailure,
  KafkaMaterializedLifecycle,
  undefined
> = SourceAdapter.make({
  identity: {
    name: "kafka",
    version: "1",
  },
  failure: KafkaAdapterFailure,
  materialized: {
    metrics: KafkaMaterializedMetrics,
    rejectionLocation: KafkaSourceRejectionLocation,
    definitionOptions: SourceAdapter.definitionOptions<KafkaRuntimeDefinitionOptions>(),
  },
  leased: undefined,
});

export type KafkaSourceInput<
  Regions extends KafkaNonEmptyReadonlyArray<string>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  LocalRowKey extends (input: KafkaLocalRowKeyInput<Regions[number], KeyCodec>) => string,
  Mapping extends (input: KafkaMappingInput<Regions[number], KeyCodec, ValueCodec>) => object,
> = {
  readonly topic: string;
  readonly regions: Regions;
  readonly key: KeyCodec;
  readonly value: ValueCodec;
  readonly localRowKey: LocalRowKey;
  readonly map: Mapping;
  readonly startFrom: KafkaStartPosition;
};

type MappingResult<Mapping> = Mapping extends (...arguments_: ReadonlyArray<never>) => infer Result
  ? Result
  : never;

type KafkaTopicRow<Mapping> = Readonly<{
  id: string;
}> &
  MappingResult<Mapping>;

type IsSafeKafkaCodec<Codec> =
  IsAny<Codec> extends true
    ? false
    : IsUnknown<Codec> extends true
      ? false
      : IsNever<Codec> extends true
        ? false
        : Codec extends KafkaCodec<unknown, unknown>
          ? IsAny<KafkaCodecValue<Codec>> extends true
            ? false
            : IsUnknown<KafkaCodecValue<Codec>> extends true
              ? false
              : IsNever<KafkaCodecValue<Codec>> extends true
                ? false
                : IsAny<KafkaCodecFailure<Codec>> extends true
                  ? false
                  : IsUnknown<KafkaCodecFailure<Codec>> extends true
                    ? false
                    : true
          : false;

type IsSafeLocalRowKeyResult<Result> =
  IsAny<Result> extends true
    ? false
    : IsUnknown<Result> extends true
      ? false
      : IsNever<Result> extends true
        ? false
        : Result extends string
          ? true
          : false;

type IsSafeMappedRow<Result> =
  IsAny<Result> extends true
    ? false
    : IsUnknown<Result> extends true
      ? false
      : IsNever<Result> extends true
        ? false
        : Result extends object
          ? Result extends
              | Effect.Effect<unknown, unknown, unknown>
              | PromiseLike<unknown>
              | Option.Option<unknown>
            ? false
            : "id" extends keyof Result
              ? false
              : undefined extends Result
                ? false
                : true
          : false;

type HasExactKeys<Candidate, Shape> =
  Exclude<keyof Candidate, keyof Shape> extends never ? true : false;

type IsSafeKafkaStartPosition<Start> =
  IsAny<Start> extends true
    ? false
    : IsUnknown<Start> extends true
      ? false
      : IsNever<Start> extends true
        ? false
        : Start extends "earliest" | "latest"
          ? true
          : Start extends {
                readonly mode: "committed";
                readonly consumerGroupId: string;
                readonly fallback: KafkaStartFallback;
              }
            ? IsAny<Start["consumerGroupId"]> extends true
              ? false
              : IsAny<Start["fallback"]> extends true
                ? false
                : HasExactKeys<
                    Start,
                    {
                      readonly mode: "committed";
                      readonly consumerGroupId: string;
                      readonly fallback: KafkaStartFallback;
                    }
                  >
            : Start extends {
                  readonly mode: "timestamp";
                  readonly atNanos: bigint;
                  readonly fallback: KafkaStartFallback;
                }
              ? IsAny<Start["atNanos"]> extends true
                ? false
                : IsAny<Start["fallback"]> extends true
                  ? false
                  : HasExactKeys<
                      Start,
                      {
                        readonly mode: "timestamp";
                        readonly atNanos: bigint;
                        readonly fallback: KafkaStartFallback;
                      }
                    >
              : Start extends {
                    readonly mode: "durationAgo";
                    readonly duration: Duration.Input;
                    readonly fallback: KafkaStartFallback;
                  }
                ? IsAny<Start["duration"]> extends true
                  ? false
                  : IsAny<Start["fallback"]> extends true
                    ? false
                    : HasExactKeys<
                        Start,
                        {
                          readonly mode: "durationAgo";
                          readonly duration: Duration.Input;
                          readonly fallback: KafkaStartFallback;
                        }
                      >
                : false;

type KafkaNotAny<Value> = IsAny<Value> extends true ? never : unknown;

type KafkaSourceField<Input, Key extends PropertyKey> =
  Input extends Readonly<Record<Key, infer Value>> ? Value : never;

type RejectAnySourceField<Input, Key extends PropertyKey> =
  Input extends Readonly<Record<Key, infer Value>>
    ? IsAny<Value> extends true
      ? { readonly [Field in Key]: never }
      : unknown
    : unknown;

type RejectUnsafeSourceRegions<Input> = Input extends { readonly regions: infer Regions }
  ? IsAny<Regions> extends true
    ? { readonly regions: never }
    : Regions extends readonly [infer Region, ...ReadonlyArray<unknown>]
      ? IsAny<Region> extends true
        ? { readonly regions: never }
        : unknown
      : { readonly regions: never }
  : unknown;

type RejectUnsafeSourceCodec<Input, Key extends "key" | "value"> =
  Input extends Readonly<Record<Key, infer Codec>>
    ? IsSafeKafkaCodec<Codec> extends true
      ? unknown
      : { readonly [Field in Key]: never }
    : unknown;

type RejectUnsafeLocalRowKey<Input> = Input extends {
  readonly localRowKey: (...arguments_: ReadonlyArray<never>) => infer Result;
}
  ? IsSafeLocalRowKeyResult<Result> extends true
    ? unknown
    : { readonly localRowKey: never }
  : unknown;

type RejectUnsafeMapping<Input> = Input extends {
  readonly map: (...arguments_: ReadonlyArray<never>) => infer Result;
}
  ? IsSafeMappedRow<Result> extends true
    ? unknown
    : { readonly map: never }
  : unknown;

type RejectUnsafeStart<Input> = Input extends { readonly startFrom: infer Start }
  ? [IsSafeKafkaStartPosition<Start>] extends [true]
    ? unknown
    : { readonly startFrom: never }
  : unknown;

type KafkaSourceInputGuards<Input, Shape> = KafkaNotAny<Input> &
  RejectExtraKeys<Input, Shape> &
  RejectAnySourceField<Input, "topic"> &
  RejectUnsafeSourceRegions<Input> &
  RejectUnsafeSourceCodec<Input, "key"> &
  RejectUnsafeSourceCodec<Input, "value"> &
  RejectUnsafeLocalRowKey<Input> &
  RejectUnsafeMapping<Input> &
  RejectUnsafeStart<Input>;

type KafkaSourceRetryAdditionalArguments<Retry> =
  IsAny<Retry> extends true ? readonly [never] : readonly [];

type KafkaSourceRetryServices<Retry> = Schedule.Env<Exclude<Retry, undefined>>;

type CapturedRegions<Input> = Extract<
  KafkaSourceField<Input, "regions">,
  KafkaNonEmptyReadonlyArray<string>
>;

type CapturedKey<Input> = Extract<KafkaSourceField<Input, "key">, KafkaCodec<unknown, unknown>>;

type CapturedValue<Input> = Extract<KafkaSourceField<Input, "value">, KafkaCodec<unknown, unknown>>;

type CapturedLocalRowKey<Input> = Extract<
  KafkaSourceField<Input, "localRowKey">,
  KafkaSourceLocalRowKey<CapturedRegions<Input>, CapturedKey<Input>, string>
>;

type CapturedMapping<Input> = Extract<
  KafkaSourceField<Input, "map">,
  KafkaSourceMapping<CapturedRegions<Input>, CapturedKey<Input>, CapturedValue<Input>, object>
>;

type CapturedDefinition<Input, Services> = KafkaSourceDefinition<
  CapturedRegions<Input>,
  CapturedKey<Input>,
  CapturedValue<Input>,
  CapturedLocalRowKey<Input>,
  CapturedMapping<Input>,
  Services
>;

type KafkaSourceDefinitionOptions<
  Regions extends KafkaNonEmptyReadonlyArray<string>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  LocalRowKey extends (input: KafkaLocalRowKeyInput<Regions[number], KeyCodec>) => string,
  Mapping extends (input: KafkaMappingInput<Regions[number], KeyCodec, ValueCodec>) => object,
> = {
  readonly topic: string;
  readonly regions: Regions;
  readonly key: KeyCodec;
  readonly value: ValueCodec;
  readonly localRowKey: LocalRowKey;
  readonly map: Mapping;
  readonly startFrom: KafkaCapturedStartPosition;
};

export type KafkaSourceDefinition<
  Regions extends KafkaNonEmptyReadonlyArray<string> = KafkaNonEmptyReadonlyArray<string>,
  KeyCodec extends KafkaCodec<unknown, unknown> = KafkaCodec<unknown, unknown>,
  ValueCodec extends KafkaCodec<unknown, unknown> = KafkaCodec<unknown, unknown>,
  LocalRowKey extends (input: KafkaLocalRowKeyInput<Regions[number], KeyCodec>) => string = (
    input: KafkaLocalRowKeyInput<Regions[number], KeyCodec>,
  ) => string,
  Mapping extends (input: KafkaMappingInput<Regions[number], KeyCodec, ValueCodec>) => object = (
    input: KafkaMappingInput<Regions[number], KeyCodec, ValueCodec>,
  ) => object,
  RetryServices = never,
> = SourceDefinition<
  typeof KafkaSourceAdapter,
  "materialized",
  KafkaSourceDefinitionOptions<Regions, KeyCodec, ValueCodec, LocalRowKey, Mapping>,
  readonly [],
  RetryServices,
  KafkaTopicRow<Mapping>
>;

export class KafkaSourceConfigurationError extends Error {
  override readonly name = "KafkaSourceConfigurationError";
}

const exactOwnDataKeys = (value: object, expected: ReadonlyArray<string>): boolean => {
  const keys = Result.try(() => Reflect.ownKeys(value));
  return (
    Result.isSuccess(keys) &&
    keys.success.length === expected.length &&
    keys.success.every((key) => typeof key === "string" && expected.includes(key)) &&
    expected.every((key) => {
      const descriptor = Result.try(() => Object.getOwnPropertyDescriptor(value, key));
      return (
        Result.isSuccess(descriptor) &&
        descriptor.success !== undefined &&
        descriptor.success.enumerable === true &&
        "value" in descriptor.success
      );
    })
  );
};

const validateFallback = (fallback: unknown): fallback is KafkaStartFallback =>
  fallback === "earliest" || fallback === "latest" || fallback === "fail";

const validateConsumerGroupId = (groupId: unknown): groupId is string =>
  typeof groupId === "string" && groupId.length > 0 && /^\S+$/u.test(groupId);

const captureStartPosition = (start: KafkaStartPosition): KafkaCapturedStartPosition => {
  if (start === "earliest" || start === "latest") {
    return start;
  }
  if (typeof start !== "object" || start === null) {
    throw new KafkaSourceConfigurationError(
      "Kafka startFrom must be earliest, latest, committed, timestamp, or durationAgo.",
    );
  }
  if (start.mode === "committed") {
    if (
      !exactOwnDataKeys(start, ["mode", "consumerGroupId", "fallback"]) ||
      !validateConsumerGroupId(start.consumerGroupId) ||
      !validateFallback(start.fallback)
    ) {
      throw new KafkaSourceConfigurationError(
        "Kafka committed startFrom requires exactly a non-empty whitespace-free consumerGroupId and fallback.",
      );
    }
    return Object.freeze({
      mode: start.mode,
      consumerGroupId: start.consumerGroupId,
      fallback: start.fallback,
    });
  }
  if (start.mode === "timestamp") {
    if (
      !exactOwnDataKeys(start, ["mode", "atNanos", "fallback"]) ||
      typeof start.atNanos !== "bigint" ||
      start.atNanos < 0n ||
      !validateFallback(start.fallback)
    ) {
      throw new KafkaSourceConfigurationError(
        "Kafka timestamp startFrom requires exactly non-negative atNanos and fallback.",
      );
    }
    return Object.freeze({
      mode: start.mode,
      atNanos: start.atNanos,
      fallback: start.fallback,
    });
  }
  if (start.mode === "durationAgo") {
    if (
      !exactOwnDataKeys(start, ["mode", "duration", "fallback"]) ||
      !validateFallback(start.fallback)
    ) {
      throw new KafkaSourceConfigurationError(
        "Kafka durationAgo startFrom requires exactly duration and fallback.",
      );
    }
    const duration = Duration.fromInput(start.duration);
    const nanos = Option.flatMap(duration, Duration.toNanos);
    if (Option.isNone(nanos) || nanos.value < 0n) {
      throw new KafkaSourceConfigurationError("Kafka durationAgo must be finite and non-negative.");
    }
    return Object.freeze({
      mode: start.mode,
      durationNanos: nanos.value,
      fallback: start.fallback,
    });
  }
  throw new KafkaSourceConfigurationError("Kafka startFrom contains an unsupported mode.");
};

const validateRegion = (region: unknown): region is string =>
  typeof region === "string" && region.length > 0 && !region.includes(":");

type KafkaCodecCandidate<Codec> = Extract<Codec, KafkaCodec<unknown, unknown>>;

type KafkaSourceLocalRowKey<
  Regions extends KafkaNonEmptyReadonlyArray<string>,
  KeyCodec,
  Result,
> = (input: KafkaLocalRowKeyInput<Regions[number], KafkaCodecCandidate<KeyCodec>>) => Result;

type KafkaSourceMapping<
  Regions extends KafkaNonEmptyReadonlyArray<string>,
  KeyCodec,
  ValueCodec,
  Result,
> = (
  input: KafkaMappingInput<
    Regions[number],
    KafkaCodecCandidate<KeyCodec>,
    KafkaCodecCandidate<ValueCodec>
  >,
) => Result;

type KafkaSourceCandidate<
  Topic extends string,
  Regions extends KafkaNonEmptyReadonlyArray<string>,
  KeyCodec,
  ValueCodec,
  LocalRowKey,
  Mapping,
  StartFrom extends KafkaStartPosition,
> = {
  readonly topic: Topic;
  readonly regions: Regions;
  readonly key: KeyCodec;
  readonly value: ValueCodec;
  readonly localRowKey: LocalRowKey;
  readonly map: Mapping;
  readonly startFrom: StartFrom;
};

function makeKafkaSource<
  const Topic extends string,
  const Regions extends KafkaNonEmptyReadonlyArray<string>,
  const KeyCodec,
  const ValueCodec,
  const LocalRowKey extends KafkaSourceLocalRowKey<Regions, NoInfer<KeyCodec>, string>,
  const Mapping extends KafkaSourceMapping<Regions, NoInfer<KeyCodec>, NoInfer<ValueCodec>, object>,
  const StartFrom extends KafkaStartPosition,
  const Input,
>(
  input: KafkaSourceCandidate<
    Topic,
    Regions,
    KeyCodec,
    ValueCodec,
    LocalRowKey,
    Mapping,
    StartFrom
  > &
    Input &
    KafkaSourceInputGuards<
      NoInfer<Input>,
      KafkaSourceCandidate<Topic, Regions, KeyCodec, ValueCodec, LocalRowKey, Mapping, StartFrom>
    >,
): CapturedDefinition<Input, never>;
function makeKafkaSource<
  const Topic extends string,
  const Regions extends KafkaNonEmptyReadonlyArray<string>,
  const KeyCodec,
  const ValueCodec,
  const LocalRowKey extends KafkaSourceLocalRowKey<Regions, NoInfer<KeyCodec>, string>,
  const Mapping extends KafkaSourceMapping<Regions, NoInfer<KeyCodec>, NoInfer<ValueCodec>, object>,
  const StartFrom extends KafkaStartPosition,
  const Input,
  const Retry,
>(
  input: KafkaSourceCandidate<
    Topic,
    Regions,
    KeyCodec,
    ValueCodec,
    LocalRowKey,
    Mapping,
    StartFrom
  > &
    Input &
    KafkaSourceInputGuards<
      NoInfer<Input>,
      KafkaSourceCandidate<Topic, Regions, KeyCodec, ValueCodec, LocalRowKey, Mapping, StartFrom>
    >,
  retry: Retry & (KafkaSourceRetryAdditionalArguments<Retry> extends readonly [] ? unknown : never),
): CapturedDefinition<Input, KafkaSourceRetryServices<Retry>>;
function makeKafkaSource(
  input: {
    readonly topic: string;
    readonly regions: KafkaNonEmptyReadonlyArray<string>;
    readonly key: unknown;
    readonly value: unknown;
    readonly localRowKey: (input: never) => unknown;
    readonly map: (input: never) => unknown;
    readonly startFrom: KafkaStartPosition;
  },
  retry?: SourceRetryPolicy<KafkaAdapterFailure, never>,
): unknown {
  if (
    typeof input !== "object" ||
    input === null ||
    !exactOwnDataKeys(input, [
      "topic",
      "regions",
      "key",
      "value",
      "localRowKey",
      "map",
      "startFrom",
    ])
  ) {
    throw new KafkaSourceConfigurationError(
      "Kafka source requires exactly topic, regions, key, value, localRowKey, map, and startFrom.",
    );
  }
  if (typeof input.topic !== "string" || input.topic.length === 0) {
    throw new KafkaSourceConfigurationError("Kafka source topic must be non-empty.");
  }
  if (
    !Array.isArray(input.regions) ||
    input.regions.length === 0 ||
    !input.regions.every(validateRegion) ||
    new Set(input.regions).size !== input.regions.length
  ) {
    throw new KafkaSourceConfigurationError(
      "Kafka source regions must be non-empty, unique, and cannot contain ':'.",
    );
  }
  if (!isKafkaCodec(input.key) || !isKafkaCodec(input.value)) {
    throw new KafkaSourceConfigurationError("Kafka source key and value must be Kafka codecs.");
  }
  if (typeof input.localRowKey !== "function" || typeof input.map !== "function") {
    throw new KafkaSourceConfigurationError(
      "Kafka source localRowKey and map must be synchronous functions.",
    );
  }
  const options = {
    topic: input.topic,
    regions: input.regions,
    key: input.key,
    value: input.value,
    localRowKey: input.localRowKey,
    map: input.map,
    startFrom: captureStartPosition(input.startFrom),
  };
  return KafkaSourceAdapter.materializedSource(options, retry);
}

export const kafkaRowId = (input: {
  readonly region: string;
  readonly localRowKey: string;
}): string => {
  if (!exactOwnDataKeys(input, ["region", "localRowKey"])) {
    throw new KafkaSourceConfigurationError("Kafka rowId requires exactly region and localRowKey.");
  }
  if (!validateRegion(input.region)) {
    throw new KafkaSourceConfigurationError(
      "Kafka rowId region must be non-empty and cannot contain ':'.",
    );
  }
  if (typeof input.localRowKey !== "string" || input.localRowKey.length === 0) {
    throw new KafkaSourceConfigurationError("Kafka rowId localRowKey must be a non-empty string.");
  }
  return `${input.region}:${input.localRowKey}`;
};

export type KafkaDecodedRowId = {
  readonly region: string;
  readonly localRowKey: string;
};

export const decodeKafkaRowId = (id: string): KafkaDecodedRowId => {
  if (typeof id !== "string") {
    throw new KafkaSourceConfigurationError("Kafka row ID must be a string.");
  }
  const separator = id.indexOf(":");
  if (separator <= 0 || separator === id.length - 1) {
    throw new KafkaSourceConfigurationError(
      "Kafka row ID must contain non-empty region and local key components.",
    );
  }
  const region = id.slice(0, separator);
  const localRowKey = id.slice(separator + 1);
  return Object.freeze({
    region,
    localRowKey,
  });
};

const encodeGroupChunk = (value: string): string =>
  encodeURIComponent(value).replace(
    /[!'()*]/gu,
    (character) => `%${character.charCodeAt(0).toString(16).toUpperCase()}`,
  );

const encodeGroupComponent = (value: string): string => {
  for (let index = 0; index < value.length; index += 1) {
    const codeUnit = value.charCodeAt(index);
    if (codeUnit >= 0xd800 && codeUnit <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        index += 1;
        continue;
      }
    } else if (codeUnit < 0xdc00 || codeUnit > 0xdfff) {
      continue;
    }
    throw new KafkaSourceConfigurationError(
      "Kafka consumer group prefix and View Server Topic must contain well-formed Unicode.",
    );
  }
  return encodeGroupChunk(value);
};

export const kafkaConsumerGroupId = (consumerGroupPrefix: string, topic: string): string => {
  if (
    typeof consumerGroupPrefix !== "string" ||
    consumerGroupPrefix.length === 0 ||
    typeof topic !== "string" ||
    topic.length === 0
  ) {
    throw new KafkaSourceConfigurationError(
      "Kafka consumer group prefix and View Server Topic must be non-empty.",
    );
  }
  return `${encodeGroupComponent(consumerGroupPrefix)}:${encodeGroupComponent(topic)}`;
};

export const kafka = Object.freeze({
  bytes: bytesCodec,
  string: stringCodec,
  json: jsonCodec,
  protobuf: protobufCodec,
  codec: customCodec,
  source: makeKafkaSource,
  rowId: kafkaRowId,
  decodeRowId: decodeKafkaRowId,
  consumerGroupId: kafkaConsumerGroupId,
});

export type KafkaMaterializedLifecycleDeclaration = KafkaMaterializedLifecycle;

export type KafkaSourceRetryPolicy<Services = never> = Schedule.Schedule<
  unknown,
  import("effect-view-server/source-adapter").SourceTermination<KafkaAdapterFailure>,
  never,
  Services
>;
