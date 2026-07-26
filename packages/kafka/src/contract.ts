import { create, createFileRegistry, fromBinary, toBinary } from "@bufbuild/protobuf";
import type { DescFile, DescMessage, MessageShape } from "@bufbuild/protobuf";
import { FileDescriptorSetSchema } from "@bufbuild/protobuf/wkt";
import { Duration, Effect, Option, Result, Schedule, Schema } from "effect";
import {
  SourceAdapter,
  type SourceDefinition,
  type SourceLifecycleDeclaration,
  type SourceRetryPolicy,
  type SourceTermination,
} from "effect-view-server/source-adapter";

const KafkaCodecTypeId: unique symbol = Symbol("@effect-view-server/kafka/KafkaCodec");
const KafkaCodecDecodeTypeId: unique symbol = Symbol("@effect-view-server/kafka/KafkaCodecDecode");
declare const KafkaCapturedDefinitionRowTypeId: unique symbol;

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

type KafkaCodecLike = {
  readonly [KafkaCodecDecodeTypeId]: (
    input: KafkaCodecDecodeInput,
  ) => Effect.Effect<unknown, unknown>;
};

export type KafkaCodecValue<Codec extends KafkaCodecLike> = Effect.Success<
  ReturnType<Codec[typeof KafkaCodecDecodeTypeId]>
>;

export type KafkaCodecFailure<Codec extends KafkaCodecLike> = Effect.Error<
  ReturnType<Codec[typeof KafkaCodecDecodeTypeId]>
>;

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
const jsonTextDecoder = new TextDecoder("utf-8", { fatal: true });
const textEncoder = new TextEncoder();
const kafkaConsumerGroupIdMaxBytes = 32_767;

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
      try: (): unknown => JSON.parse(jsonTextDecoder.decode(input.bytes)),
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
        : [Descriptor] extends [DescMessage]
          ? readonly []
          : readonly [never];

const freezeDescriptorGraph = (value: unknown, visited = new WeakSet<object>()): void => {
  if (
    (typeof value !== "object" && typeof value !== "function") ||
    value === null ||
    ArrayBuffer.isView(value) ||
    visited.has(value)
  ) {
    return;
  }
  visited.add(value);
  for (const key of Reflect.ownKeys(value)) {
    freezeDescriptorGraph(Reflect.get(value, key), visited);
  }
  Object.freeze(value);
};

const captureProtobufDescriptor = (descriptor: DescMessage): DescMessage => {
  const captured = Result.try(() => {
    const typeName = descriptor.typeName;
    const rootFile = descriptor.file;
    const files: Array<DescFile> = [];
    const visited = new Set<DescFile>();
    const visit = (file: DescFile): void => {
      if (visited.has(file)) {
        return;
      }
      visited.add(file);
      for (const dependency of file.dependencies) {
        visit(dependency);
      }
      files.push(file);
    };
    visit(rootFile);
    const descriptorSet = create(FileDescriptorSetSchema, {
      file: files.map((file) => file.proto),
    });
    const clonedDescriptorSet = fromBinary(
      FileDescriptorSetSchema,
      toBinary(FileDescriptorSetSchema, descriptorSet),
    );
    const clone = createFileRegistry(clonedDescriptorSet).getMessage(typeName);
    if (clone === undefined) {
      throw new Error("message descriptor is missing from its file");
    }
    freezeDescriptorGraph(clone);
    return clone;
  });
  if (Result.isFailure(captured)) {
    throw new KafkaSourceConfigurationError(
      "Kafka protobuf codec requires a valid message descriptor.",
    );
  }
  return captured.success;
};

function protobufCodec<const Descriptor>(
  descriptor: Descriptor,
  ..._unsupported: KafkaProtobufAdditionalArguments<NoInfer<Descriptor>>
): KafkaProtobufCodec<Extract<Descriptor, DescMessage>>;
function protobufCodec(descriptor: DescMessage): KafkaProtobufCodec<DescMessage> {
  const capturedDescriptor = captureProtobufDescriptor(descriptor);
  const codec = makeCodec<MessageShape<DescMessage>, KafkaCodecError, "protobuf">(
    "protobuf",
    (input) =>
      Effect.try({
        try: () => fromBinary(capturedDescriptor, input.bytes),
        catch: () => codecError("Kafka protobuf payload could not be decoded."),
      }),
  );
  const protobuf: KafkaProtobufCodec<DescMessage> = {
    [KafkaCodecTypeId]: () => protobuf,
    [KafkaCodecDecodeTypeId]: (input) => decodeKafkaCodec(codec, input),
    format: "protobuf",
    descriptor: capturedDescriptor,
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

const isKafkaCustomDecode = (value: unknown): value is KafkaCustomCodecShape["decode"] =>
  typeof value === "function";

type KafkaCustomCodecAdditionalArguments<Definition> =
  IsAny<Definition> extends true ? readonly [never] : readonly [];

const captureCustomCodecDefinition = (definition: unknown): KafkaCustomCodecShape | undefined => {
  if (typeof definition !== "object" || definition === null) {
    return undefined;
  }
  const captured = Result.try(() => {
    const keys = Reflect.ownKeys(definition);
    if (keys.length !== 2 || keys.some((key) => key !== "name" && key !== "decode")) {
      return undefined;
    }
    const nameDescriptor = Object.getOwnPropertyDescriptor(definition, "name");
    const decodeDescriptor = Object.getOwnPropertyDescriptor(definition, "decode");
    if (
      nameDescriptor === undefined ||
      decodeDescriptor === undefined ||
      nameDescriptor.enumerable !== true ||
      decodeDescriptor.enumerable !== true ||
      !("value" in nameDescriptor) ||
      !("value" in decodeDescriptor)
    ) {
      return undefined;
    }
    const name: unknown = nameDescriptor.value;
    const decode: unknown = decodeDescriptor.value;
    if (typeof name !== "string" || name.length === 0 || !isKafkaCustomDecode(decode)) {
      return undefined;
    }
    return Object.freeze({
      name,
      decode,
    });
  });
  return Result.isSuccess(captured) ? captured.success : undefined;
};

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
  KafkaCustomDecodeValue<NoInfer<ReturnType<Definition["decode"]>>>,
  KafkaCodecError | KafkaCustomDecodeFailure<NoInfer<ReturnType<Definition["decode"]>>>
>;
function customCodec(definition: KafkaCustomCodecShape): KafkaCustomCodec<unknown, unknown> {
  const captured = captureCustomCodecDefinition(definition);
  if (captured === undefined) {
    throw new KafkaSourceConfigurationError(
      "Kafka custom codec requires exactly a non-empty name and decode function.",
    );
  }
  const decode = captured.decode;
  const codec: KafkaCustomCodec<unknown, unknown> = {
    [KafkaCodecTypeId]: () => codec,
    [KafkaCodecDecodeTypeId]: (input) =>
      Effect.try({
        try: () => decode(input),
        catch: () => codecError("Kafka custom codec threw synchronously."),
      }).pipe(Effect.flatten),
    format: "custom",
    name: captured.name,
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

const KafkaPartition = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(2_147_483_647),
);
const KafkaNonNegativeBigInt = Schema.BigInt.check(Schema.isGreaterThanOrEqualToBigInt(0n));

export const KafkaSourceRejectionLocation = Schema.Struct({
  region: Schema.NonEmptyString,
  topic: Schema.NonEmptyString,
  partition: KafkaPartition,
  offset: KafkaNonNegativeBigInt,
  phase: KafkaRejectionPhaseSchema,
  message: Schema.String,
});
export type KafkaSourceRejectionLocation<Region extends string = string> = Omit<
  typeof KafkaSourceRejectionLocation.Type,
  "region"
> & {
  readonly region: Region;
};

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
type KafkaAdapterFailureWithRegion<Value, Region extends string> = Value extends {
  readonly region: string;
}
  ? Omit<Value, "region"> & {
      readonly region: Region;
    }
  : Value;

export type KafkaAdapterFailure<Region extends string = string> = KafkaAdapterFailureWithRegion<
  typeof KafkaAdapterFailure.Type,
  Region
>;

export const KafkaPartitionMetrics = Schema.Struct({
  partition: KafkaPartition,
  offset: KafkaNonNegativeBigInt,
  lag: KafkaNonNegativeBigInt,
});
export type KafkaPartitionMetrics = typeof KafkaPartitionMetrics.Type;

export const KafkaRegionMetrics = Schema.Struct({
  region: Schema.NonEmptyString,
  assignments: Schema.Array(KafkaPartitionMetrics),
  commits: KafkaNonNegativeBigInt,
  commitFailures: KafkaNonNegativeBigInt,
  decoded: KafkaNonNegativeBigInt,
  decodeFailures: KafkaNonNegativeBigInt,
  mapped: KafkaNonNegativeBigInt,
  mappingFailures: KafkaNonNegativeBigInt,
  rejections: KafkaNonNegativeBigInt,
  reconnects: KafkaNonNegativeBigInt,
  rebalances: KafkaNonNegativeBigInt,
  closes: KafkaNonNegativeBigInt,
  closeFailures: KafkaNonNegativeBigInt,
});
export type KafkaRegionMetrics<Region extends string = string> = Omit<
  typeof KafkaRegionMetrics.Type,
  "region"
> & {
  readonly region: Region;
};

export const KafkaMaterializedMetrics = Schema.Struct({
  activeGroupId: Schema.NonEmptyString,
  start: KafkaStartResolutionSchema,
  regions: Schema.NonEmptyArray(KafkaRegionMetrics),
});
type KafkaRegionMetricsForRemainingRegions<Regions extends ReadonlyArray<string>> =
  number extends Regions["length"]
    ? ReadonlyArray<KafkaRegionMetrics<Regions[number]>>
    : Regions extends readonly [
          infer First extends string,
          ...infer Remaining extends ReadonlyArray<string>,
        ]
      ? readonly [KafkaRegionMetrics<First>, ...KafkaRegionMetricsForRemainingRegions<Remaining>]
      : readonly [];

type KafkaRegionMetricsForRegions<Regions extends KafkaNonEmptyReadonlyArray<string>> =
  Regions extends readonly [
    infer First extends string,
    ...infer Remaining extends ReadonlyArray<string>,
  ]
    ? readonly [KafkaRegionMetrics<First>, ...KafkaRegionMetricsForRemainingRegions<Remaining>]
    : never;
export type KafkaMaterializedMetrics<
  Regions extends KafkaNonEmptyReadonlyArray<string> = KafkaNonEmptyReadonlyArray<string>,
> = Omit<typeof KafkaMaterializedMetrics.Type, "regions"> & {
  readonly regions: KafkaRegionMetricsForRegions<Regions>;
};

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

const KafkaSourceAdapterHandle = SourceAdapter.make<
  "kafka",
  "1",
  KafkaAdapterFailure,
  KafkaMaterializedLifecycle,
  undefined
>({
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

export const KafkaSourceAdapter = SourceAdapter.descriptor(KafkaSourceAdapterHandle);

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
    : Regions extends readonly [unknown, ...ReadonlyArray<unknown>]
      ? IsAny<Regions[number]> extends true
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

type KafkaSourceRetryServices<Retry> = Schedule.Env<Retry>;

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
> & {
  readonly [KafkaCapturedDefinitionRowTypeId]?: (
    _row: KafkaTopicRow<CapturedMapping<Input>>,
  ) => KafkaTopicRow<CapturedMapping<Input>>;
};

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
  KafkaTopicRow<Mapping>,
  KafkaAdapterFailure<Regions[number]>,
  KafkaMaterializedMetrics<Regions>,
  KafkaSourceRejectionLocation<Regions[number]>,
  Regions[number]
>;

export class KafkaSourceConfigurationError extends Error {
  override readonly name = "KafkaSourceConfigurationError";
}

const captureOwnDataValues = (value: unknown): ReadonlyMap<PropertyKey, unknown> | undefined =>
  Result.try(() => {
    if (typeof value !== "object" || value === null) {
      return undefined;
    }
    const captured = new Map<PropertyKey, unknown>();
    for (const key of Reflect.ownKeys(value)) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (descriptor === undefined || descriptor.enumerable !== true || !("value" in descriptor)) {
        return undefined;
      }
      captured.set(key, descriptor.value);
    }
    return captured;
  }).pipe(
    Result.match({
      onFailure: () => undefined,
      onSuccess: (captured) => captured,
    }),
  );

const captureExactOwnDataValues = (
  value: unknown,
  expected: ReadonlyArray<string>,
): ReadonlyMap<PropertyKey, unknown> | undefined => {
  const captured = captureOwnDataValues(value);
  return captured !== undefined &&
    captured.size === expected.length &&
    expected.every((key) => captured.has(key))
    ? captured
    : undefined;
};

const validateFallback = (fallback: unknown): fallback is KafkaStartFallback =>
  fallback === "earliest" || fallback === "latest" || fallback === "fail";

const validateConsumerGroupId = (groupId: unknown): groupId is string =>
  typeof groupId === "string" &&
  groupId.length > 0 &&
  /^\S+$/u.test(groupId) &&
  textEncoder.encode(groupId).byteLength <= kafkaConsumerGroupIdMaxBytes;

type KafkaDurationString = Extract<Duration.Input, string>;

const isDurationString = (value: unknown): value is KafkaDurationString =>
  typeof value === "string" &&
  (value === "Infinity" ||
    value === "-Infinity" ||
    /^-?\d+(?:\.\d+)?\s+(?:nanos?|micros?|millis?|seconds?|minutes?|hours?|days?|weeks?)$/u.test(
      value,
    ));

const isFiniteNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value);

const durationFromUnknown = (input: unknown): Option.Option<Duration.Duration> =>
  Result.try(() => {
    if (typeof input === "number") {
      return isFiniteNumber(input) ? Duration.fromInput(input) : Option.none();
    }
    if (typeof input === "bigint" || isDurationString(input) || Duration.isDuration(input)) {
      return Duration.fromInput(input);
    }
    if (Array.isArray(input)) {
      const length = Option.getOrThrow(
        Option.fromUndefinedOr(Object.getOwnPropertyDescriptor(input, "length")),
      ).value;
      const seconds = Object.getOwnPropertyDescriptor(input, "0");
      const nanos = Object.getOwnPropertyDescriptor(input, "1");
      if (
        length !== 2 ||
        seconds === undefined ||
        nanos === undefined ||
        seconds.enumerable !== true ||
        nanos.enumerable !== true ||
        !("value" in seconds) ||
        !("value" in nanos) ||
        !isFiniteNumber(seconds.value) ||
        !isFiniteNumber(nanos.value)
      ) {
        return Option.none();
      }
      return Duration.fromInput([seconds.value, nanos.value]);
    }
    const captured = captureOwnDataValues(input);
    if (captured === undefined) {
      return Option.none();
    }
    const allowed = [
      "weeks",
      "days",
      "hours",
      "minutes",
      "seconds",
      "milliseconds",
      "microseconds",
      "nanoseconds",
    ] as const;
    if (
      !Array.from(captured.keys()).every(
        (key) =>
          typeof key === "string" &&
          allowed.some((allowedKey) => allowedKey === key) &&
          (captured.get(key) === undefined || isFiniteNumber(captured.get(key))),
      )
    ) {
      return Option.none();
    }
    const component = (name: (typeof allowed)[number]): number | undefined => {
      const value = captured.get(name);
      return typeof value === "number" ? value : undefined;
    };
    return Duration.fromInput({
      weeks: component("weeks"),
      days: component("days"),
      hours: component("hours"),
      minutes: component("minutes"),
      seconds: component("seconds"),
      milliseconds: component("milliseconds"),
      microseconds: component("microseconds"),
      nanoseconds: component("nanoseconds"),
    });
  }).pipe(
    Result.match({
      onFailure: () => Option.none(),
      onSuccess: (duration) => duration,
    }),
  );

const captureStartPosition = (start: unknown): KafkaCapturedStartPosition => {
  if (start === "earliest" || start === "latest") {
    return start;
  }
  const captured = captureOwnDataValues(start);
  if (captured === undefined) {
    throw new KafkaSourceConfigurationError(
      "Kafka startFrom must be earliest, latest, committed, timestamp, or durationAgo.",
    );
  }
  const mode = captured.get("mode");
  if (mode === "committed") {
    const consumerGroupId = captured.get("consumerGroupId");
    const fallback = captured.get("fallback");
    if (
      captured.size !== 3 ||
      !validateConsumerGroupId(consumerGroupId) ||
      !validateFallback(fallback)
    ) {
      throw new KafkaSourceConfigurationError(
        "Kafka committed startFrom requires exactly a non-empty whitespace-free consumerGroupId no longer than 32767 UTF-8 bytes and fallback.",
      );
    }
    return Object.freeze({
      mode,
      consumerGroupId,
      fallback,
    });
  }
  if (mode === "timestamp") {
    const atNanos = captured.get("atNanos");
    const fallback = captured.get("fallback");
    if (
      captured.size !== 3 ||
      typeof atNanos !== "bigint" ||
      atNanos < 0n ||
      !validateFallback(fallback)
    ) {
      throw new KafkaSourceConfigurationError(
        "Kafka timestamp startFrom requires exactly non-negative atNanos and fallback.",
      );
    }
    return Object.freeze({
      mode,
      atNanos,
      fallback,
    });
  }
  if (mode === "durationAgo") {
    const fallback = captured.get("fallback");
    if (captured.size !== 3 || !captured.has("duration") || !validateFallback(fallback)) {
      throw new KafkaSourceConfigurationError(
        "Kafka durationAgo startFrom requires exactly duration and fallback.",
      );
    }
    const duration = durationFromUnknown(captured.get("duration"));
    const nanos = Option.flatMap(duration, Duration.toNanos);
    if (Option.isNone(nanos) || nanos.value < 0n) {
      throw new KafkaSourceConfigurationError("Kafka durationAgo must be finite and non-negative.");
    }
    return Object.freeze({
      mode,
      durationNanos: nanos.value,
      fallback,
    });
  }
  throw new KafkaSourceConfigurationError("Kafka startFrom contains an unsupported mode.");
};

const validateRegion = (region: unknown): region is string =>
  typeof region === "string" && region.length > 0 && !region.includes(":");

const isKafkaRuntimeCallback = (value: unknown): value is (input: never) => unknown =>
  typeof value === "function";

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

type KafkaRegionsWithoutAny<Regions extends KafkaNonEmptyReadonlyArray<string>> = {
  readonly [Index in keyof Regions]: IsAny<Regions[Index]> extends true ? never : Regions[Index];
};

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
  readonly regions: Regions & KafkaRegionsWithoutAny<NoInfer<Regions>>;
  readonly key: KeyCodec;
  readonly value: ValueCodec;
  readonly localRowKey: LocalRowKey;
  readonly map: Mapping;
  readonly startFrom: StartFrom;
};

const isKafkaSourceRetryPolicy = (
  value: unknown,
): value is SourceRetryPolicy<KafkaAdapterFailure, unknown> =>
  Result.try(() => Schedule.isSchedule(value)).pipe(
    Result.match({
      onFailure: () => false,
      onSuccess: (isSchedule) => isSchedule,
    }),
  );

type KafkaSourceApi = {
  <
    const Topic extends string,
    const Regions extends KafkaNonEmptyReadonlyArray<string>,
    const KeyCodec,
    const ValueCodec,
    const LocalRowKey extends KafkaSourceLocalRowKey<Regions, NoInfer<KeyCodec>, string>,
    const Mapping extends KafkaSourceMapping<
      Regions,
      NoInfer<KeyCodec>,
      NoInfer<ValueCodec>,
      object
    >,
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
  <
    const Topic extends string,
    const Regions extends KafkaNonEmptyReadonlyArray<string>,
    const KeyCodec,
    const ValueCodec,
    const LocalRowKey extends KafkaSourceLocalRowKey<Regions, NoInfer<KeyCodec>, string>,
    const Mapping extends KafkaSourceMapping<
      Regions,
      NoInfer<KeyCodec>,
      NoInfer<ValueCodec>,
      object
    >,
    const StartFrom extends KafkaStartPosition,
    const Input,
    const Retry extends SourceRetryPolicy<KafkaAdapterFailure<Regions[number]>, unknown>,
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
    retry: Retry,
    ..._unsupported: KafkaSourceRetryAdditionalArguments<NoInfer<Retry>>
  ): CapturedDefinition<Input, KafkaSourceRetryServices<Retry>>;
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
  const Retry extends SourceRetryPolicy<KafkaAdapterFailure<Regions[number]>, unknown>,
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
  retry: Retry,
  ..._unsupported: KafkaSourceRetryAdditionalArguments<NoInfer<Retry>>
): CapturedDefinition<Input, KafkaSourceRetryServices<Retry>>;
function makeKafkaSource(
  input: unknown,
  retry?: unknown,
  ..._unsupported: ReadonlyArray<unknown>
): unknown {
  const captured = captureExactOwnDataValues(input, [
    "topic",
    "regions",
    "key",
    "value",
    "localRowKey",
    "map",
    "startFrom",
  ]);
  if (captured === undefined) {
    throw new KafkaSourceConfigurationError(
      "Kafka source requires exactly topic, regions, key, value, localRowKey, map, and startFrom.",
    );
  }
  const topic = captured.get("topic");
  const regionInput = captured.get("regions");
  const key = captured.get("key");
  const value = captured.get("value");
  const localRowKey = captured.get("localRowKey");
  const map = captured.get("map");
  const startFrom = captured.get("startFrom");
  if (typeof topic !== "string" || topic.length === 0) {
    throw new KafkaSourceConfigurationError("Kafka source topic must be non-empty.");
  }
  const regions = Result.try(() => {
    if (!Array.isArray(regionInput)) {
      return undefined;
    }
    const length = Option.getOrThrow(
      Option.fromUndefinedOr(Object.getOwnPropertyDescriptor(regionInput, "length")),
    ).value;
    if (length <= 0) {
      return undefined;
    }
    const values: Array<string> = [];
    for (let index = 0; index < length; index += 1) {
      const descriptor = Object.getOwnPropertyDescriptor(regionInput, String(index));
      if (
        descriptor === undefined ||
        descriptor.enumerable !== true ||
        !("value" in descriptor) ||
        !validateRegion(descriptor.value)
      ) {
        return undefined;
      }
      values.push(descriptor.value);
    }
    const [first, ...rest] = values;
    return Object.freeze([
      Option.getOrThrow(Option.fromUndefinedOr(first)),
      ...rest,
    ]) satisfies KafkaNonEmptyReadonlyArray<string>;
  }).pipe(
    Result.match({
      onFailure: () => undefined,
      onSuccess: (value) => value,
    }),
  );
  if (regions === undefined || new Set(regions).size !== regions.length) {
    throw new KafkaSourceConfigurationError(
      "Kafka source regions must be non-empty, unique, and cannot contain ':'.",
    );
  }
  if (!isKafkaCodec(key) || !isKafkaCodec(value)) {
    throw new KafkaSourceConfigurationError("Kafka source key and value must be Kafka codecs.");
  }
  if (!isKafkaRuntimeCallback(localRowKey) || !isKafkaRuntimeCallback(map)) {
    throw new KafkaSourceConfigurationError(
      "Kafka source localRowKey and map must be synchronous functions.",
    );
  }
  if (retry !== undefined && !isKafkaSourceRetryPolicy(retry)) {
    throw new KafkaSourceConfigurationError(
      "Kafka source retry override must be an Effect Schedule.",
    );
  }
  const options = {
    topic,
    regions,
    key,
    value,
    localRowKey,
    map,
    startFrom: captureStartPosition(startFrom),
  };
  return retry === undefined
    ? SourceAdapter.materializedSource(KafkaSourceAdapterHandle, options)
    : SourceAdapter.materializedSource(KafkaSourceAdapterHandle, options, retry);
}

export const kafkaRowId = (input: {
  readonly region: string;
  readonly localRowKey: string;
}): string => {
  const captured = captureExactOwnDataValues(input, ["region", "localRowKey"]);
  if (captured === undefined) {
    throw new KafkaSourceConfigurationError("Kafka rowId requires exactly region and localRowKey.");
  }
  const region = captured.get("region");
  const localRowKey = captured.get("localRowKey");
  if (!validateRegion(region)) {
    throw new KafkaSourceConfigurationError(
      "Kafka rowId region must be non-empty and cannot contain ':'.",
    );
  }
  if (typeof localRowKey !== "string" || localRowKey.length === 0) {
    throw new KafkaSourceConfigurationError("Kafka rowId localRowKey must be a non-empty string.");
  }
  return `${region}:${localRowKey}`;
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
  const groupId = `${encodeGroupComponent(consumerGroupPrefix)}:${encodeGroupComponent(topic)}`;
  if (groupId.length > kafkaConsumerGroupIdMaxBytes) {
    throw new KafkaSourceConfigurationError(
      "Kafka derived consumer group ID exceeds the 32767-byte Kafka protocol limit.",
    );
  }
  return groupId;
};

type KafkaContractApi = {
  readonly bytes: typeof bytesCodec;
  readonly codec: typeof customCodec;
  readonly consumerGroupId: typeof kafkaConsumerGroupId;
  readonly decodeRowId: typeof decodeKafkaRowId;
  readonly json: typeof jsonCodec;
  readonly protobuf: typeof protobufCodec;
  readonly rowId: typeof kafkaRowId;
  readonly source: KafkaSourceApi;
  readonly string: typeof stringCodec;
};

export const kafka: KafkaContractApi = Object.freeze({
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

export type KafkaSourceRetryPolicy<
  Region extends string = string,
  Services = never,
> = Schedule.Schedule<unknown, SourceTermination<KafkaAdapterFailure<Region>>, never, Services>;
