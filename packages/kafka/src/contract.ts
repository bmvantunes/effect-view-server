import { create, createFileRegistry, fromBinary, toBinary } from "@bufbuild/protobuf";
import type { DescFile, DescMessage, MessageShape } from "@bufbuild/protobuf";
import type { GenMessage } from "@bufbuild/protobuf/codegenv2";
import { FileDescriptorSetSchema } from "@bufbuild/protobuf/wkt";
import { Duration, Effect, Option, Result, Schedule, Schema } from "effect";
import type {
  SourceAdapterDescriptor,
  SourceDefinition,
  SourceLifecycleDeclaration,
  SourceRetryPolicy,
  SourceTermination,
} from "effect-view-server/source-adapter";
import { SourceAdapter } from "effect-view-server/source-adapter";

const KafkaCodecTypeId: unique symbol = Symbol("@effect-view-server/kafka/KafkaCodec");
const KafkaCodecDecodeTypeId: unique symbol = Symbol("@effect-view-server/kafka/KafkaCodecDecode");
const KafkaCompactionKeyCodecTypeId: unique symbol = Symbol(
  "@effect-view-server/kafka/KafkaCompactionKeyCodec",
);
const KafkaCompactionKeyCodecDecodeTypeId: unique symbol = Symbol(
  "@effect-view-server/kafka/KafkaCompactionKeyCodecDecode",
);
const KafkaSchemaRegistryRequirementTypeId: unique symbol = Symbol(
  "@effect-view-server/kafka/KafkaSchemaRegistryRequirement",
);
const KafkaDirectDecodeTypeId: unique symbol = Symbol(
  "@effect-view-server/kafka/KafkaDirectDecode",
);
const KafkaSchemaRegistryProtobufCodecTypeId: unique symbol = Symbol(
  "@effect-view-server/kafka/KafkaSchemaRegistryProtobufCodec",
);
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

type IsUnion<Value, Whole = Value> = Value extends unknown
  ? [Whole] extends [Value]
    ? false
    : true
  : false;

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

type KafkaCandidateKeys<Candidate> = Candidate extends unknown ? keyof Candidate : never;

type RejectExtraKeys<Candidate, Shape> = {
  readonly [Key in Exclude<KafkaCandidateKeys<Candidate>, keyof Shape>]: never;
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

export type KafkaCompactionKeyCodecDecodeInput = {
  readonly bytes: Uint8Array;
};

export type KafkaCodec<Value, Error = never, RequiresSchemaRegistry extends boolean = boolean> = {
  readonly [KafkaCodecTypeId]: () => KafkaCodec<Value, Error, RequiresSchemaRegistry>;
  readonly [KafkaCodecDecodeTypeId]: (input: KafkaCodecDecodeInput) => Effect.Effect<Value, Error>;
  readonly [KafkaSchemaRegistryRequirementTypeId]: RequiresSchemaRegistry;
  readonly format: string;
};

export type KafkaCompactionKeyCodec<
  Value,
  Error = never,
  RequiresSchemaRegistry extends boolean = boolean,
> = {
  readonly [KafkaCompactionKeyCodecTypeId]: () => KafkaCompactionKeyCodec<
    Value,
    Error,
    RequiresSchemaRegistry
  >;
  readonly [KafkaCompactionKeyCodecDecodeTypeId]: (
    input: KafkaCompactionKeyCodecDecodeInput,
  ) => Effect.Effect<Value, Error>;
  readonly [KafkaSchemaRegistryRequirementTypeId]: RequiresSchemaRegistry;
  readonly format: string;
};

type KafkaDirectCodec<Value, Error> = KafkaCodec<Value, Error, false> & {
  readonly [KafkaDirectDecodeTypeId]: true;
};

type KafkaDirectCompactionKeyCodec<Value, Error> = KafkaCompactionKeyCodec<Value, Error, false> & {
  readonly [KafkaDirectDecodeTypeId]: true;
};

export type KafkaBytesCodec = KafkaDirectCodec<Uint8Array, never> & {
  readonly format: "bytes";
};

export type KafkaStringCodec = KafkaDirectCodec<string, never> & {
  readonly format: "string";
};

export type KafkaJsonCodec<SourceSchema extends KafkaRowSchema = KafkaRowSchema> = KafkaDirectCodec<
  SourceSchema["Type"],
  KafkaCodecError
> & {
  readonly format: "json";
};

export type KafkaProtobufCodec<Descriptor extends DescMessage = DescMessage> = KafkaDirectCodec<
  MessageShape<Descriptor>,
  KafkaCodecError
> & {
  readonly descriptor: Descriptor;
  readonly format: "protobuf";
};

export type KafkaSchemaRegistryProtobufCodec<Descriptor extends DescMessage = DescMessage> =
  KafkaCodec<MessageShape<Descriptor>, KafkaCodecError, true> &
    KafkaCompactionKeyCodec<MessageShape<Descriptor>, KafkaCodecError, true> & {
      readonly [KafkaSchemaRegistryProtobufCodecTypeId]: () => KafkaSchemaRegistryProtobufCodec<Descriptor>;
      readonly descriptor: Descriptor;
      readonly format: "schema-registry-protobuf";
    };

export type KafkaCodecSchemaRegistryRequirement<Codec> =
  Codec extends KafkaCodec<unknown, unknown, infer RequiresSchemaRegistry>
    ? RequiresSchemaRegistry
    : Codec extends KafkaCompactionKeyCodec<unknown, unknown, infer RequiresSchemaRegistry>
      ? RequiresSchemaRegistry
      : false;

export const isKafkaSchemaRegistryProtobufCodec = (
  value: unknown,
): value is KafkaSchemaRegistryProtobufCodec => {
  if (!isKafkaCodec(value) || !isKafkaCompactionKeyCodec(value)) {
    return false;
  }
  const brand = Result.try(() => Reflect.get(value, KafkaSchemaRegistryProtobufCodecTypeId));
  return (
    Result.isSuccess(brand) &&
    typeof brand.success === "function" &&
    Result.try(() => Reflect.apply(brand.success, undefined, [])).pipe(
      Result.match({
        onFailure: () => false,
        onSuccess: (branded) => branded === value,
      }),
    )
  );
};

const isKafkaRuntimeCodec = (
  value: unknown,
): value is KafkaDirectCodec<unknown, unknown> | KafkaSchemaRegistryProtobufCodec<DescMessage> =>
  isKafkaSchemaRegistryProtobufCodec(value) ||
  (isKafkaCodec(value) &&
    value[KafkaSchemaRegistryRequirementTypeId] === false &&
    Result.try(() => Reflect.get(value, KafkaDirectDecodeTypeId)).pipe(
      Result.match({
        onFailure: () => false,
        onSuccess: (directDecode) => directDecode === true,
      }),
    ));

const isKafkaRuntimeCompactionKeyCodec = (
  value: unknown,
): value is
  | KafkaDirectCompactionKeyCodec<unknown, unknown>
  | KafkaSchemaRegistryProtobufCodec<DescMessage> =>
  isKafkaSchemaRegistryProtobufCodec(value) ||
  (isKafkaCompactionKeyCodec(value) &&
    value[KafkaSchemaRegistryRequirementTypeId] === false &&
    Result.try(() => Reflect.get(value, KafkaDirectDecodeTypeId)).pipe(
      Result.match({
        onFailure: () => false,
        onSuccess: (directDecode) => directDecode === true,
      }),
    ));

export type KafkaCustomCodec<Value, Error> = KafkaDirectCodec<Value, Error> & {
  readonly format: "custom";
  readonly name: string;
};

type KafkaCodecLike = {
  readonly [KafkaCodecDecodeTypeId]: (
    input: KafkaCodecDecodeInput,
  ) => Effect.Effect<unknown, unknown>;
};

type KafkaCompactionKeyCodecLike = {
  readonly [KafkaCompactionKeyCodecDecodeTypeId]: (
    input: KafkaCompactionKeyCodecDecodeInput,
  ) => Effect.Effect<unknown, unknown>;
};

type KafkaAnyCodecLike = KafkaCodecLike | KafkaCompactionKeyCodecLike;

export type KafkaCodecValue<Codec extends KafkaAnyCodecLike> = Codec extends KafkaCodecLike
  ? Effect.Success<ReturnType<Codec[typeof KafkaCodecDecodeTypeId]>>
  : Codec extends KafkaCompactionKeyCodecLike
    ? Effect.Success<ReturnType<Codec[typeof KafkaCompactionKeyCodecDecodeTypeId]>>
    : never;

export type KafkaCodecFailure<Codec extends KafkaAnyCodecLike> = Codec extends KafkaCodecLike
  ? Effect.Error<ReturnType<Codec[typeof KafkaCodecDecodeTypeId]>>
  : Codec extends KafkaCompactionKeyCodecLike
    ? Effect.Error<ReturnType<Codec[typeof KafkaCompactionKeyCodecDecodeTypeId]>>
    : never;

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
): KafkaDirectCodec<Value, Error> & { readonly format: Format } => {
  const codec: KafkaDirectCodec<Value, Error> & { readonly format: Format } = {
    [KafkaCodecTypeId]: () => codec,
    [KafkaCodecDecodeTypeId]: decode,
    [KafkaDirectDecodeTypeId]: true,
    [KafkaSchemaRegistryRequirementTypeId]: false,
    format,
  };
  return SourceAdapter.executable(codec);
};

export const isKafkaCodec = (value: unknown): value is KafkaCodec<unknown, unknown, boolean> => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const brand = Result.try(() => Reflect.get(value, KafkaCodecTypeId));
  const decode = Result.try(() => Reflect.get(value, KafkaCodecDecodeTypeId));
  const requirement = Result.try(() => Reflect.get(value, KafkaSchemaRegistryRequirementTypeId));
  return (
    Result.isSuccess(brand) &&
    typeof brand.success === "function" &&
    Result.isSuccess(decode) &&
    typeof decode.success === "function" &&
    Result.isSuccess(requirement) &&
    typeof requirement.success === "boolean" &&
    Result.try(() => Reflect.apply(brand.success, undefined, [])).pipe(
      Result.match({
        onFailure: () => false,
        onSuccess: (branded) => branded === value,
      }),
    )
  );
};

export const decodeKafkaCodec = <Value, Error>(
  codec: KafkaDirectCodec<Value, Error>,
  input: KafkaCodecDecodeInput,
): Effect.Effect<Value, Error> => codec[KafkaCodecDecodeTypeId](input);

const makeCompactionKeyCodec = <Value, Error, const Format extends string>(
  format: Format,
  decode: (input: KafkaCompactionKeyCodecDecodeInput) => Effect.Effect<Value, Error>,
): KafkaDirectCompactionKeyCodec<Value, Error> & { readonly format: Format } => {
  const codec: KafkaDirectCompactionKeyCodec<Value, Error> & { readonly format: Format } = {
    [KafkaCompactionKeyCodecTypeId]: () => codec,
    [KafkaCompactionKeyCodecDecodeTypeId]: decode,
    [KafkaDirectDecodeTypeId]: true,
    [KafkaSchemaRegistryRequirementTypeId]: false,
    format,
  };
  return SourceAdapter.executable(codec);
};

export const isKafkaCompactionKeyCodec = (
  value: unknown,
): value is KafkaCompactionKeyCodec<unknown, unknown, boolean> => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const brand = Result.try(() => Reflect.get(value, KafkaCompactionKeyCodecTypeId));
  const decode = Result.try(() => Reflect.get(value, KafkaCompactionKeyCodecDecodeTypeId));
  const requirement = Result.try(() => Reflect.get(value, KafkaSchemaRegistryRequirementTypeId));
  return (
    Result.isSuccess(brand) &&
    typeof brand.success === "function" &&
    Result.isSuccess(decode) &&
    typeof decode.success === "function" &&
    Result.isSuccess(requirement) &&
    typeof requirement.success === "boolean" &&
    Result.try(() => Reflect.apply(brand.success, undefined, [])).pipe(
      Result.match({
        onFailure: () => false,
        onSuccess: (branded) => branded === value,
      }),
    )
  );
};

export const decodeKafkaCompactionKeyCodec = <Value, Error>(
  codec: KafkaDirectCompactionKeyCodec<Value, Error>,
  input: KafkaCompactionKeyCodecDecodeInput,
): Effect.Effect<Value, Error> => codec[KafkaCompactionKeyCodecDecodeTypeId](input);

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

type KafkaSchemaRegistryGeneratedDescriptor<Descriptor> =
  true extends IsUnion<Descriptor>
    ? {
        readonly __kafkaSchemaRegistryRequiresOneGeneratedDescriptor: never;
      }
    : [Descriptor] extends [DescMessage]
      ? true extends IsUnion<Descriptor["typeName"]>
        ? {
            readonly __kafkaSchemaRegistryRequiresOneGeneratedDescriptor: never;
          }
        : string extends Descriptor["typeName"]
          ? {
              readonly __kafkaSchemaRegistryRequiresGeneratedDescriptor: never;
            }
          : [Descriptor] extends [GenMessage<MessageShape<Descriptor>>]
            ? unknown
            : {
                readonly __kafkaSchemaRegistryRequiresGeneratedDescriptor: never;
              }
      : {
          readonly __kafkaSchemaRegistryRequiresGeneratedDescriptor: never;
        };

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
    [KafkaDirectDecodeTypeId]: true,
    [KafkaSchemaRegistryRequirementTypeId]: false,
    format: "protobuf",
    descriptor: capturedDescriptor,
  };
  return SourceAdapter.executable(protobuf);
}

function schemaRegistryProtobufCodec<const Descriptor>(
  descriptor: Descriptor & KafkaSchemaRegistryGeneratedDescriptor<NoInfer<Descriptor>>,
  ..._unsupported: KafkaProtobufAdditionalArguments<NoInfer<Descriptor>>
): KafkaSchemaRegistryProtobufCodec<Extract<Descriptor, DescMessage>>;
function schemaRegistryProtobufCodec(
  descriptor: DescMessage,
): KafkaSchemaRegistryProtobufCodec<DescMessage> {
  const capturedDescriptor = captureProtobufDescriptor(descriptor);
  const decode = (): Effect.Effect<MessageShape<DescMessage>, KafkaCodecError> =>
    Effect.fail(
      codecError("Schema Registry contract validation is required before decoding Kafka protobuf."),
    );
  const codec: KafkaSchemaRegistryProtobufCodec<DescMessage> = {
    [KafkaCodecTypeId]: () => codec,
    [KafkaCodecDecodeTypeId]: () => decode(),
    [KafkaCompactionKeyCodecTypeId]: () => codec,
    [KafkaCompactionKeyCodecDecodeTypeId]: () => decode(),
    [KafkaSchemaRegistryRequirementTypeId]: true,
    [KafkaSchemaRegistryProtobufCodecTypeId]: () => codec,
    descriptor: capturedDescriptor,
    format: "schema-registry-protobuf",
  };
  return SourceAdapter.executable(codec);
}

export const KafkaSchemaRegistry = Object.freeze({
  protobuf: schemaRegistryProtobufCodec,
});

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
    [KafkaDirectDecodeTypeId]: true,
    [KafkaSchemaRegistryRequirementTypeId]: false,
    format: "custom",
    name: captured.name,
  };
  return SourceAdapter.executable(codec);
}

export type KafkaCompactionBytesCodec = KafkaDirectCompactionKeyCodec<Uint8Array, never> & {
  readonly format: "bytes";
};

export type KafkaCompactionStringCodec = KafkaDirectCompactionKeyCodec<string, never> & {
  readonly format: "string";
};

export type KafkaCompactionJsonCodec<SourceSchema extends KafkaRowSchema = KafkaRowSchema> =
  KafkaDirectCompactionKeyCodec<SourceSchema["Type"], KafkaCodecError> & {
    readonly format: "json";
  };

export type KafkaCompactionProtobufCodec<Descriptor extends DescMessage = DescMessage> =
  KafkaDirectCompactionKeyCodec<MessageShape<Descriptor>, KafkaCodecError> & {
    readonly descriptor: Descriptor;
    readonly format: "protobuf";
  };

export type KafkaCompactionCustomCodec<Value, Error> = KafkaDirectCompactionKeyCodec<
  Value,
  Error
> & {
  readonly format: "custom";
  readonly name: string;
};

type KafkaCompactionCustomCodecShape = {
  readonly name: string;
  readonly decode: (input: KafkaCompactionKeyCodecDecodeInput) => KafkaCustomDecodeResult;
};

type KafkaCompactionCustomCodecUnionKeys<Definition> = Definition extends unknown
  ? keyof Definition
  : never;

type RejectKafkaCompactionCustomCodecExtraKeys<Definition> = {
  readonly [Key in Exclude<
    KafkaCompactionCustomCodecUnionKeys<Definition>,
    keyof KafkaCompactionCustomCodecShape
  >]: never;
};

const compactionBytesCodec = (): KafkaCompactionBytesCodec =>
  makeCompactionKeyCodec("bytes", (input) => Effect.succeed(Uint8Array.from(input.bytes)));

const compactionStringCodec = (): KafkaCompactionStringCodec =>
  makeCompactionKeyCodec("string", (input) => Effect.succeed(textDecoder.decode(input.bytes)));

function compactionJsonCodec<const Factory extends KafkaJsonFactory>(
  factory: (() => Schema.toCodecJson<KafkaJsonFactorySourceSchema<Factory>>) &
    Factory &
    SupportedKafkaJsonFactory<Factory>,
): KafkaCompactionJsonCodec<KafkaJsonFactorySourceSchema<Factory>>;
function compactionJsonCodec<const SourceSchema extends KafkaRowSchema>(
  factory: () => Schema.toCodecJson<SourceSchema>,
): KafkaCompactionJsonCodec<SourceSchema> {
  if (typeof factory !== "function") {
    throw new KafkaSourceConfigurationError(
      "Kafka compaction key JSON codec requires a factory returning a JSON-compatible Schema.",
    );
  }
  const codec = Result.try(factory);
  // Compaction keys define canonical row identity, so an unusable key Schema must
  // reject the Source Definition before startup rather than reject every record later.
  if (
    Result.isFailure(codec) ||
    !Schema.isSchema(codec.success) ||
    !Object.hasOwn(codec.success, "schema") ||
    !isKafkaRowSchema(codec.success.schema)
  ) {
    throw new KafkaSourceConfigurationError(
      "Kafka compaction key JSON codec requires a factory returning a JSON-compatible Schema.",
    );
  }
  const decode = Schema.decodeUnknownEffect(codec.success);
  return makeCompactionKeyCodec<SourceSchema["Type"], KafkaCodecError, "json">("json", (input) =>
    Effect.try({
      try: (): unknown => JSON.parse(jsonTextDecoder.decode(input.bytes)),
      catch: () => codecError("Kafka compaction key JSON payload is not valid JSON."),
    }).pipe(
      Effect.flatMap((value) =>
        decode(value).pipe(
          Effect.mapError(() =>
            codecError("Kafka compaction key JSON payload does not satisfy its Schema."),
          ),
        ),
      ),
    ),
  );
}

function compactionProtobufCodec<const Descriptor>(
  descriptor: Descriptor,
  ..._unsupported: KafkaProtobufAdditionalArguments<NoInfer<Descriptor>>
): KafkaCompactionProtobufCodec<Extract<Descriptor, DescMessage>>;
function compactionProtobufCodec(
  descriptor: DescMessage,
): KafkaCompactionProtobufCodec<DescMessage> {
  const ordinary = protobufCodec(descriptor);
  const codec: KafkaCompactionProtobufCodec<DescMessage> = {
    [KafkaCompactionKeyCodecTypeId]: () => codec,
    [KafkaCompactionKeyCodecDecodeTypeId]: (input) =>
      decodeKafkaCodec(ordinary, {
        bytes: input.bytes,
        metadata: {
          sourceTopic: "",
          sourceRegion: "",
          partition: 0,
          offset: 0n,
          timestampNanos: 0n,
          headers: {},
        },
      }),
    [KafkaDirectDecodeTypeId]: true,
    [KafkaSchemaRegistryRequirementTypeId]: false,
    descriptor: ordinary.descriptor,
    format: "protobuf",
  };
  return SourceAdapter.executable(codec);
}

function compactionCustomCodec<
  const Name,
  const Definition extends {
    readonly name: Name;
    readonly decode: (...arguments_: ReadonlyArray<never>) => unknown;
  },
>(
  definition: Definition & {
    readonly name: Name & string;
    readonly decode: (input: KafkaCompactionKeyCodecDecodeInput) => KafkaCustomDecodeResult;
  } & RejectAny<NoInfer<Name>> &
    RejectAny<KafkaCustomDecodeValue<NoInfer<ReturnType<Definition["decode"]>>>> &
    RejectUnknown<KafkaCustomDecodeValue<NoInfer<ReturnType<Definition["decode"]>>>> &
    RejectAny<KafkaCustomDecodeFailure<NoInfer<ReturnType<Definition["decode"]>>>> &
    RejectUnknown<KafkaCustomDecodeFailure<NoInfer<ReturnType<Definition["decode"]>>>> &
    RejectKafkaCompactionCustomCodecExtraKeys<Definition>,
  ..._unsupported: KafkaCustomCodecAdditionalArguments<NoInfer<Definition>>
): KafkaCompactionCustomCodec<
  KafkaCustomDecodeValue<NoInfer<ReturnType<Definition["decode"]>>>,
  KafkaCodecError | KafkaCustomDecodeFailure<NoInfer<ReturnType<Definition["decode"]>>>
>;
function compactionCustomCodec(
  definition: KafkaCompactionCustomCodecShape,
): KafkaCompactionCustomCodec<unknown, unknown> {
  const captured = captureCustomCodecDefinition(definition);
  if (captured === undefined) {
    throw new KafkaSourceConfigurationError(
      "Kafka compaction key custom codec requires exactly a non-empty name and decode function.",
    );
  }
  const decode = captured.decode;
  const applyDecode = (input: KafkaCompactionKeyCodecDecodeInput): KafkaCustomDecodeResult =>
    Reflect.apply(decode, undefined, [input]);
  const codec: KafkaCompactionCustomCodec<unknown, unknown> = {
    [KafkaCompactionKeyCodecTypeId]: () => codec,
    [KafkaCompactionKeyCodecDecodeTypeId]: (input) =>
      Effect.try({
        try: () => applyDecode(input),
        catch: () => codecError("Kafka compaction key custom codec threw synchronously."),
      }).pipe(Effect.flatten),
    [KafkaDirectDecodeTypeId]: true,
    [KafkaSchemaRegistryRequirementTypeId]: false,
    format: "custom",
    name: captured.name,
  };
  return SourceAdapter.executable(codec);
}

export const KafkaCompactionKey = Object.freeze({
  bytes: compactionBytesCodec,
  codec: compactionCustomCodec,
  json: compactionJsonCodec,
  protobuf: compactionProtobufCodec,
  string: compactionStringCodec,
});

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
  | "nullValue"
  | "localRowKey"
  | "canonicalId"
  | "mapping"
  | "topicSchema";

export const KafkaRejectionPhaseSchema = Schema.Literals([
  "keyDecode",
  "valueDecode",
  "nullValue",
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
  Schema.TaggedStruct("KafkaSchemaRegistryUnavailable", {
    region: Schema.NonEmptyString,
    topic: Schema.NonEmptyString,
    subject: Schema.NonEmptyString,
    side: Schema.Literals(["key", "value"]),
    schemaId: Schema.NullOr(Schema.Int),
    message: Schema.String,
  }),
  Schema.TaggedStruct("KafkaSchemaRegistryPolicyMismatch", {
    region: Schema.NonEmptyString,
    topic: Schema.NonEmptyString,
    subject: Schema.NonEmptyString,
    side: Schema.Literals(["key", "value"]),
    schemaId: Schema.NullOr(Schema.Int),
    message: Schema.String,
  }),
  Schema.TaggedStruct("KafkaSchemaRegistrySchemaMismatch", {
    region: Schema.NonEmptyString,
    topic: Schema.NonEmptyString,
    subject: Schema.NonEmptyString,
    side: Schema.Literals(["key", "value"]),
    schemaId: Schema.NullOr(Schema.Int),
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

export const KafkaCleanupPolicySchema = Schema.Literals([
  "delete",
  "compact",
  "compact-and-delete",
]);

export const KafkaCapturedRetentionPolicySchema = Schema.Union([
  Schema.TaggedStruct("MatchKafkaRetention", {}),
  Schema.TaggedStruct("Forever", {}),
  Schema.TaggedStruct("Finite", {
    durationNanos: KafkaNonNegativeBigInt.check(Schema.isGreaterThanBigInt(0n)),
  }),
]);

export const KafkaResolvedRetentionSchema = Schema.Union([
  Schema.TaggedStruct("Forever", {}),
  Schema.TaggedStruct("Finite", {
    durationNanos: KafkaNonNegativeBigInt,
  }),
]);

export const KafkaExpirationFailure = Schema.Struct({
  region: Schema.NonEmptyString,
  topic: Schema.NonEmptyString,
  id: Schema.NonEmptyString,
  generation: KafkaNonNegativeBigInt.check(Schema.isGreaterThanBigInt(0n)),
  failedAtNanos: KafkaNonNegativeBigInt,
  message: Schema.Literal("Kafka retention expiration Delete failed."),
});
export type KafkaExpirationFailure<Region extends string = string> = Omit<
  typeof KafkaExpirationFailure.Type,
  "region"
> & {
  readonly region: Region;
};

export const KafkaRetentionMetrics = Schema.Struct({
  declaredCleanupPolicy: KafkaCleanupPolicySchema,
  observedCleanupPolicy: KafkaCleanupPolicySchema,
  configuredRetention: KafkaCapturedRetentionPolicySchema,
  resolvedRetention: KafkaResolvedRetentionSchema,
  trackedRows: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  lastSweepRetryableFailures: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  expiredRows: KafkaNonNegativeBigInt,
  authoritativeExpiredDeletes: KafkaNonNegativeBigInt,
  failedWorkBacklog: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  expirationRetryFailures: KafkaNonNegativeBigInt,
  latestExpirationFailure: Schema.NullOr(KafkaExpirationFailure),
  lastSweepAtNanos: Schema.NullOr(KafkaNonNegativeBigInt),
  lastSweepDurationNanos: Schema.NullOr(KafkaNonNegativeBigInt),
  sweepIntervalNanos: KafkaNonNegativeBigInt.check(Schema.isGreaterThanBigInt(0n)),
});
export type KafkaRetentionMetrics<Region extends string = string> = Omit<
  typeof KafkaRetentionMetrics.Type,
  "latestExpirationFailure"
> & {
  readonly latestExpirationFailure: KafkaExpirationFailure<Region> | null;
};

export const KafkaMaterializedRegionMetrics = Schema.Struct({
  ...KafkaRegionMetrics.fields,
  retention: KafkaRetentionMetrics,
});
export type KafkaMaterializedRegionMetrics<Region extends string = string> = Omit<
  typeof KafkaMaterializedRegionMetrics.Type,
  "region" | "retention"
> & {
  readonly region: Region;
  readonly retention: KafkaRetentionMetrics<Region>;
};

export const KafkaMaterializedMetrics = Schema.Struct({
  activeGroupId: Schema.NonEmptyString,
  start: KafkaStartResolutionSchema,
  regions: Schema.NonEmptyArray(KafkaMaterializedRegionMetrics),
});
type KafkaRegionMetricsForRemainingRegions<Regions extends ReadonlyArray<string>> =
  number extends Regions["length"]
    ? ReadonlyArray<KafkaMaterializedRegionMetrics<Regions[number]>>
    : Regions extends readonly [
          infer First extends string,
          ...infer Remaining extends ReadonlyArray<string>,
        ]
      ? readonly [
          KafkaMaterializedRegionMetrics<First>,
          ...KafkaRegionMetricsForRemainingRegions<Remaining>,
        ]
      : readonly [];

type KafkaRegionMetricsForRegions<Regions extends KafkaNonEmptyReadonlyArray<string>> =
  Regions extends readonly [
    infer First extends string,
    ...infer Remaining extends ReadonlyArray<string>,
  ]
    ? readonly [
        KafkaMaterializedRegionMetrics<First>,
        ...KafkaRegionMetricsForRemainingRegions<Remaining>,
      ]
    : never;
export type KafkaMaterializedMetrics<
  Regions extends KafkaNonEmptyReadonlyArray<string> = KafkaNonEmptyReadonlyArray<string>,
> = Omit<typeof KafkaMaterializedMetrics.Type, "regions"> & {
  readonly regions: KafkaRegionMetricsForRegions<Regions>;
};

export type KafkaCleanupPolicy = "delete" | "compact" | "compact-and-delete";

type KafkaDurationString = Extract<Duration.Input, string>;
type KafkaDurationComponents = {
  readonly [Key in keyof Duration.DurationObject]-?: Readonly<
    Required<Pick<Duration.DurationObject, Key>> & Partial<Omit<Duration.DurationObject, Key>>
  >;
}[keyof Duration.DurationObject];

type KafkaDurationComponentsContainAny<Value> = true extends {
  readonly [Key in keyof Duration.DurationObject]-?: Key extends keyof Value
    ? IsAny<Value[Key]>
    : false;
}[keyof Duration.DurationObject]
  ? true
  : false;

type KafkaDurationTupleContainsAny<Value> = Value extends readonly [number, number]
  ? IsAny<Value[0]> extends true
    ? true
    : IsAny<Value[1]>
  : false;

type IsSafeKafkaRetentionPolicy<Value> =
  IsAny<Value> extends true
    ? false
    : Value extends readonly [number, number]
      ? KafkaDurationTupleContainsAny<Value> extends true
        ? false
        : true
      : Value extends KafkaDurationComponents
        ? KafkaDurationComponentsContainAny<Value> extends true
          ? false
          : HasExactKeys<Value, Duration.DurationObject>
        : Value extends KafkaRetentionPolicy
          ? true
          : false;

export type KafkaRetentionPolicy =
  | "match-kafka-retention"
  | Duration.Duration
  | number
  | bigint
  | readonly [seconds: number, nanos: number]
  | KafkaDurationString
  | KafkaDurationComponents;

export type KafkaCapturedRetentionPolicy =
  | {
      readonly _tag: "MatchKafkaRetention";
    }
  | {
      readonly _tag: "Forever";
    }
  | {
      readonly _tag: "Finite";
      readonly durationNanos: bigint;
    };

export type KafkaResolvedRetention =
  | {
      readonly _tag: "Forever";
    }
  | {
      readonly _tag: "Finite";
      readonly durationNanos: bigint;
    };

export type KafkaLocalRowKeyInput<
  Region extends string,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
> = {
  readonly key: KafkaCodecValue<KeyCodec>;
  readonly value: KafkaCodecValue<ValueCodec>;
  readonly region: Region;
};

export type KafkaDeleteMappingInput<
  Region extends string,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
> = {
  readonly key: KafkaCodecValue<KeyCodec>;
  readonly value: KafkaCodecValue<ValueCodec>;
  readonly region: Region;
  readonly localRowKey: string;
  readonly metadata: KafkaMessageMetadata<Region>;
};

export type KafkaCompactionMappingInput<
  Region extends string,
  KeyCodec extends KafkaCompactionKeyCodec<unknown, unknown>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
> = {
  readonly key: KafkaCodecValue<KeyCodec>;
  readonly value: KafkaCodecValue<ValueCodec>;
  readonly region: Region;
  readonly metadata: KafkaMessageMetadata<Region>;
};

export type KafkaRuntimeDeleteDefinitionOptions = {
  readonly cleanupPolicy: "delete";
  readonly retentionPolicy: KafkaCapturedRetentionPolicy;
  readonly topic: string;
  readonly regions: KafkaNonEmptyReadonlyArray<string>;
  readonly key: KafkaDirectCodec<unknown, unknown> | KafkaSchemaRegistryProtobufCodec<DescMessage>;
  readonly value:
    | KafkaDirectCodec<unknown, unknown>
    | KafkaSchemaRegistryProtobufCodec<DescMessage>;
  readonly localRowKey: (input: never) => unknown;
  readonly map: (input: never) => unknown;
  readonly startFrom: KafkaCapturedStartPosition;
};

export type KafkaRuntimeCompactionDefinitionOptions = {
  readonly cleanupPolicy: "compact" | "compact-and-delete";
  readonly retentionPolicy: KafkaCapturedRetentionPolicy;
  readonly topic: string;
  readonly regions: KafkaNonEmptyReadonlyArray<string>;
  readonly key:
    | KafkaDirectCompactionKeyCodec<unknown, unknown>
    | KafkaSchemaRegistryProtobufCodec<DescMessage>;
  readonly value:
    | KafkaDirectCodec<unknown, unknown>
    | KafkaSchemaRegistryProtobufCodec<DescMessage>;
  readonly localRowKey?: never;
  readonly map: (input: never) => unknown;
  readonly startFrom: KafkaCapturedStartPosition;
};

export type KafkaRuntimeDefinitionOptions =
  | KafkaRuntimeDeleteDefinitionOptions
  | KafkaRuntimeCompactionDefinitionOptions;

type KafkaMaterializedLifecycle = SourceLifecycleDeclaration<
  KafkaMaterializedMetrics,
  KafkaSourceRejectionLocation,
  KafkaRuntimeDefinitionOptions
> & {
  readonly applicationState: "required";
};

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
    applicationState: "required",
    metrics: KafkaMaterializedMetrics,
    rejectionLocation: KafkaSourceRejectionLocation,
    definitionOptions: SourceAdapter.definitionOptions<KafkaRuntimeDefinitionOptions>(),
  },
  leased: undefined,
});

export const KafkaSourceAdapter: SourceAdapterDescriptor<
  "kafka",
  "1",
  KafkaAdapterFailure,
  KafkaMaterializedLifecycle,
  undefined
> = SourceAdapter.descriptor(KafkaSourceAdapterHandle);

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

type RejectUnsafeRetentionPolicy<Input> = Input extends {
  readonly retentionPolicy: infer RetentionPolicy;
}
  ? [IsSafeKafkaRetentionPolicy<RetentionPolicy>] extends [true]
    ? unknown
    : { readonly retentionPolicy: never }
  : unknown;

type KafkaSourceInputGuards<Input, Shape> = KafkaNotAny<Input> &
  RejectExtraKeys<Input, Shape> &
  RejectAnySourceField<Input, "cleanupPolicy"> &
  RejectAnySourceField<Input, "topic"> &
  RejectUnsafeSourceRegions<Input> &
  RejectUnsafeSourceCodec<Input, "key"> &
  RejectUnsafeSourceCodec<Input, "value"> &
  RejectUnsafeLocalRowKey<Input> &
  RejectUnsafeMapping<Input> &
  RejectUnsafeStart<Input> &
  RejectUnsafeRetentionPolicy<Input>;

type KafkaSourceRetryAdditionalArguments<Retry> =
  IsAny<Retry> extends true ? readonly [never] : readonly [];

type KafkaSourceRetryServices<Retry> = Schedule.Env<Retry>;

type KafkaSourceDefinitionOptions<
  Regions extends KafkaNonEmptyReadonlyArray<string>,
  CleanupPolicy extends KafkaCleanupPolicy,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  LocalRowKey extends (
    input: KafkaLocalRowKeyInput<Regions[number], KeyCodec, ValueCodec>,
  ) => string,
  Mapping extends (input: KafkaDeleteMappingInput<Regions[number], KeyCodec, ValueCodec>) => object,
> = {
  readonly cleanupPolicy: CleanupPolicy;
  readonly retentionPolicy: KafkaCapturedRetentionPolicy;
  readonly topic: string;
  readonly regions: Regions;
  readonly key: KeyCodec;
  readonly value: ValueCodec;
  readonly localRowKey: LocalRowKey;
  readonly map: Mapping;
  readonly startFrom: KafkaCapturedStartPosition;
};

export type KafkaDeleteSourceDefinition<
  Regions extends KafkaNonEmptyReadonlyArray<string> = KafkaNonEmptyReadonlyArray<string>,
  KeyCodec extends KafkaCodec<unknown, unknown> = KafkaCodec<unknown, unknown>,
  ValueCodec extends KafkaCodec<unknown, unknown> = KafkaCodec<unknown, unknown>,
  LocalRowKey extends (
    input: KafkaLocalRowKeyInput<Regions[number], KeyCodec, ValueCodec>,
  ) => string = (input: KafkaLocalRowKeyInput<Regions[number], KeyCodec, ValueCodec>) => string,
  Mapping extends (
    input: KafkaDeleteMappingInput<Regions[number], KeyCodec, ValueCodec>,
  ) => object = (input: KafkaDeleteMappingInput<Regions[number], KeyCodec, ValueCodec>) => object,
  RetryServices = never,
> = SourceDefinition<
  typeof KafkaSourceAdapter,
  "materialized",
  KafkaSourceDefinitionOptions<Regions, "delete", KeyCodec, ValueCodec, LocalRowKey, Mapping>,
  readonly [],
  RetryServices,
  KafkaTopicRow<Mapping>,
  KafkaAdapterFailure<Regions[number]>,
  KafkaMaterializedMetrics<Regions>,
  KafkaSourceRejectionLocation<Regions[number]>,
  Regions[number]
>;

type KafkaCompactionSourceDefinitionOptions<
  Regions extends KafkaNonEmptyReadonlyArray<string>,
  CleanupPolicy extends "compact" | "compact-and-delete",
  KeyCodec extends KafkaCompactionKeyCodec<unknown, unknown>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  Mapping extends (
    input: KafkaCompactionMappingInput<Regions[number], KeyCodec, ValueCodec>,
  ) => object,
> = {
  readonly cleanupPolicy: CleanupPolicy;
  readonly retentionPolicy: KafkaCapturedRetentionPolicy;
  readonly topic: string;
  readonly regions: Regions;
  readonly key: KeyCodec;
  readonly value: ValueCodec;
  readonly map: Mapping;
  readonly startFrom: KafkaCapturedStartPosition;
};

export type KafkaCompactionSourceDefinition<
  Regions extends KafkaNonEmptyReadonlyArray<string> = KafkaNonEmptyReadonlyArray<string>,
  CleanupPolicy extends "compact" | "compact-and-delete" = "compact" | "compact-and-delete",
  KeyCodec extends KafkaCompactionKeyCodec<unknown, unknown> = KafkaCompactionKeyCodec<
    unknown,
    unknown
  >,
  ValueCodec extends KafkaCodec<unknown, unknown> = KafkaCodec<unknown, unknown>,
  Mapping extends (
    input: KafkaCompactionMappingInput<Regions[number], KeyCodec, ValueCodec>,
  ) => object = (
    input: KafkaCompactionMappingInput<Regions[number], KeyCodec, ValueCodec>,
  ) => object,
  RetryServices = never,
> = SourceDefinition<
  typeof KafkaSourceAdapter,
  "materialized",
  KafkaCompactionSourceDefinitionOptions<Regions, CleanupPolicy, KeyCodec, ValueCodec, Mapping>,
  readonly [],
  RetryServices,
  KafkaTopicRow<Mapping>,
  KafkaAdapterFailure<Regions[number]>,
  KafkaMaterializedMetrics<Regions>,
  KafkaSourceRejectionLocation<Regions[number]>,
  Regions[number]
>;

export type KafkaSourceDefinition<
  Regions extends KafkaNonEmptyReadonlyArray<string> = KafkaNonEmptyReadonlyArray<string>,
  RetryServices = never,
> =
  | KafkaDeleteSourceDefinition<
      Regions,
      KafkaCodec<unknown, unknown>,
      KafkaCodec<unknown, unknown>,
      (
        input: KafkaLocalRowKeyInput<
          Regions[number],
          KafkaCodec<unknown, unknown>,
          KafkaCodec<unknown, unknown>
        >,
      ) => string,
      (
        input: KafkaDeleteMappingInput<
          Regions[number],
          KafkaCodec<unknown, unknown>,
          KafkaCodec<unknown, unknown>
        >,
      ) => object,
      RetryServices
    >
  | KafkaCompactionSourceDefinition<
      Regions,
      "compact" | "compact-and-delete",
      KafkaCompactionKeyCodec<unknown, unknown>,
      KafkaCodec<unknown, unknown>,
      (
        input: KafkaCompactionMappingInput<
          Regions[number],
          KafkaCompactionKeyCodec<unknown, unknown>,
          KafkaCodec<unknown, unknown>
        >,
      ) => object,
      RetryServices
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

const isDurationString = (value: unknown): value is KafkaDurationString =>
  typeof value === "string" &&
  (value === "Infinity" ||
    value === "-Infinity" ||
    /^-?\d+(?:\.\d+)?\s+(?:nanos?|micros?|millis?|seconds?|minutes?|hours?|days?|weeks?)$/u.test(
      value,
    ));

const isFiniteNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value);

export const decodeKafkaDurationInput = (input: unknown): Option.Option<Duration.Duration> =>
  Result.try(() => {
    if (typeof input === "number") {
      return isFiniteNumber(input) || input === Number.POSITIVE_INFINITY
        ? Duration.fromInput(input)
        : Option.none();
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
    const duration = decodeKafkaDurationInput(captured.get("duration"));
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

type KafkaCompactionKeyCodecCandidate<Codec> = Extract<
  Codec,
  KafkaCompactionKeyCodec<unknown, unknown>
>;

type KafkaSourceLocalRowKey<
  Regions extends KafkaNonEmptyReadonlyArray<string>,
  KeyCodec,
  ValueCodec,
  Result,
> = (
  input: KafkaLocalRowKeyInput<
    Regions[number],
    KafkaCodecCandidate<KeyCodec>,
    KafkaCodecCandidate<ValueCodec>
  >,
) => Result;

type KafkaDeleteSourceMapping<
  Regions extends KafkaNonEmptyReadonlyArray<string>,
  KeyCodec,
  ValueCodec,
  Result,
> = (
  input: KafkaDeleteMappingInput<
    Regions[number],
    KafkaCodecCandidate<KeyCodec>,
    KafkaCodecCandidate<ValueCodec>
  >,
) => Result;

type KafkaCompactionSourceMapping<
  Regions extends KafkaNonEmptyReadonlyArray<string>,
  KeyCodec,
  ValueCodec,
  Result,
> = (
  input: KafkaCompactionMappingInput<
    Regions[number],
    KafkaCompactionKeyCodecCandidate<KeyCodec>,
    KafkaCodecCandidate<ValueCodec>
  >,
) => Result;

type KafkaRegionsWithoutAny<Regions extends KafkaNonEmptyReadonlyArray<string>> = {
  readonly [Index in keyof Regions]: IsAny<Regions[Index]> extends true ? never : Regions[Index];
};

type KafkaDeleteSourceCandidate<
  Topic extends string,
  Regions extends KafkaNonEmptyReadonlyArray<string>,
  KeyCodec,
  ValueCodec,
  LocalRowKey,
  Mapping,
  StartFrom extends KafkaStartPosition,
  RetentionPolicy,
> = {
  readonly cleanupPolicy: "delete";
  readonly retentionPolicy: IsAny<RetentionPolicy> extends true
    ? never
    : RetentionPolicy extends KafkaRetentionPolicy
      ? RetentionPolicy
      : never;
  readonly topic: Topic;
  readonly regions: Regions & KafkaRegionsWithoutAny<NoInfer<Regions>>;
  readonly key: KeyCodec;
  readonly value: ValueCodec;
  readonly localRowKey: LocalRowKey;
  readonly map: Mapping;
  readonly startFrom: StartFrom;
};

type KafkaCompactionSourceCandidate<
  Topic extends string,
  Regions extends KafkaNonEmptyReadonlyArray<string>,
  CleanupPolicy extends "compact" | "compact-and-delete",
  KeyCodec,
  ValueCodec,
  Mapping,
  StartFrom extends KafkaStartPosition,
  RetentionPolicy,
> = {
  readonly cleanupPolicy: CleanupPolicy;
  readonly retentionPolicy: IsAny<RetentionPolicy> extends true
    ? never
    : RetentionPolicy extends KafkaRetentionPolicy
      ? RetentionPolicy
      : never;
  readonly topic: Topic;
  readonly regions: Regions & KafkaRegionsWithoutAny<NoInfer<Regions>>;
  readonly key: KeyCodec;
  readonly value: ValueCodec;
  readonly localRowKey?: never;
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

type CapturedDeleteDefinition<
  Regions extends KafkaNonEmptyReadonlyArray<string>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  LocalRowKey extends (
    input: KafkaLocalRowKeyInput<Regions[number], KeyCodec, ValueCodec>,
  ) => string,
  Mapping extends (input: KafkaDeleteMappingInput<Regions[number], KeyCodec, ValueCodec>) => object,
  Services,
> = KafkaDeleteSourceDefinition<Regions, KeyCodec, ValueCodec, LocalRowKey, Mapping, Services> & {
  readonly [KafkaCapturedDefinitionRowTypeId]?: (
    _row: KafkaTopicRow<Mapping>,
  ) => KafkaTopicRow<Mapping>;
};

type CapturedCompactionDefinition<
  Regions extends KafkaNonEmptyReadonlyArray<string>,
  CleanupPolicy extends "compact" | "compact-and-delete",
  KeyCodec extends KafkaCompactionKeyCodec<unknown, unknown>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  Mapping extends (
    input: KafkaCompactionMappingInput<Regions[number], KeyCodec, ValueCodec>,
  ) => object,
  Services,
> = KafkaCompactionSourceDefinition<
  Regions,
  CleanupPolicy,
  KeyCodec,
  ValueCodec,
  Mapping,
  Services
> & {
  readonly [KafkaCapturedDefinitionRowTypeId]?: (
    _row: KafkaTopicRow<Mapping>,
  ) => KafkaTopicRow<Mapping>;
};

type IsSafeKafkaCompactionKeyCodec<Codec> =
  IsAny<Codec> extends true
    ? false
    : IsUnknown<Codec> extends true
      ? false
      : IsNever<Codec> extends true
        ? false
        : Codec extends KafkaCompactionKeyCodec<unknown, unknown>
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

type RejectUnsafeCompactionKeyCodec<Input> =
  Input extends Readonly<Record<"key", infer Codec>>
    ? IsSafeKafkaCompactionKeyCodec<Codec> extends true
      ? unknown
      : { readonly key: never }
    : unknown;

type KafkaCompactionSourceInputGuards<Input, Shape> = KafkaNotAny<Input> &
  RejectExtraKeys<Input, Shape> &
  RejectAnySourceField<Input, "cleanupPolicy"> &
  RejectAnySourceField<Input, "topic"> &
  RejectUnsafeSourceRegions<Input> &
  RejectUnsafeCompactionKeyCodec<Input> &
  RejectUnsafeSourceCodec<Input, "value"> &
  RejectUnsafeMapping<Input> &
  RejectUnsafeStart<Input> &
  RejectUnsafeRetentionPolicy<Input>;

type KafkaSourceApi = {
  <
    const Topic extends string,
    const Regions extends KafkaNonEmptyReadonlyArray<string>,
    const KeyCodec extends KafkaCodec<unknown, unknown>,
    const ValueCodec extends KafkaCodec<unknown, unknown>,
    const LocalRowKey extends KafkaSourceLocalRowKey<
      Regions,
      NoInfer<KeyCodec>,
      NoInfer<ValueCodec>,
      string
    >,
    const Mapping extends KafkaDeleteSourceMapping<
      Regions,
      NoInfer<KeyCodec>,
      NoInfer<ValueCodec>,
      object
    >,
    const StartFrom extends KafkaStartPosition,
    const RetentionPolicy extends KafkaRetentionPolicy,
    const Input,
  >(
    input: KafkaDeleteSourceCandidate<
      Topic,
      Regions,
      KeyCodec,
      ValueCodec,
      LocalRowKey,
      Mapping,
      StartFrom,
      RetentionPolicy
    > &
      Input &
      KafkaSourceInputGuards<
        NoInfer<Input>,
        KafkaDeleteSourceCandidate<
          Topic,
          Regions,
          KeyCodec,
          ValueCodec,
          LocalRowKey,
          Mapping,
          StartFrom,
          RetentionPolicy
        >
      >,
  ): CapturedDeleteDefinition<
    Regions,
    KafkaCodecCandidate<KeyCodec>,
    KafkaCodecCandidate<ValueCodec>,
    LocalRowKey,
    Mapping,
    never
  >;
  <
    const Topic extends string,
    const Regions extends KafkaNonEmptyReadonlyArray<string>,
    const CleanupPolicy extends "compact" | "compact-and-delete",
    const KeyCodec extends KafkaCompactionKeyCodec<unknown, unknown>,
    const ValueCodec extends KafkaCodec<unknown, unknown>,
    const Mapping extends KafkaCompactionSourceMapping<
      Regions,
      NoInfer<KeyCodec>,
      NoInfer<ValueCodec>,
      object
    >,
    const StartFrom extends KafkaStartPosition,
    const RetentionPolicy extends KafkaRetentionPolicy,
    const Input,
  >(
    input: KafkaCompactionSourceCandidate<
      Topic,
      Regions,
      CleanupPolicy,
      KeyCodec,
      ValueCodec,
      Mapping,
      StartFrom,
      RetentionPolicy
    > &
      Input &
      KafkaCompactionSourceInputGuards<
        NoInfer<Input>,
        KafkaCompactionSourceCandidate<
          Topic,
          Regions,
          CleanupPolicy,
          KeyCodec,
          ValueCodec,
          Mapping,
          StartFrom,
          RetentionPolicy
        >
      >,
  ): CapturedCompactionDefinition<
    Regions,
    CleanupPolicy,
    KafkaCompactionKeyCodecCandidate<KeyCodec>,
    KafkaCodecCandidate<ValueCodec>,
    Mapping,
    never
  >;
  <
    const Topic extends string,
    const Regions extends KafkaNonEmptyReadonlyArray<string>,
    const KeyCodec extends KafkaCodec<unknown, unknown>,
    const ValueCodec extends KafkaCodec<unknown, unknown>,
    const LocalRowKey extends KafkaSourceLocalRowKey<
      Regions,
      NoInfer<KeyCodec>,
      NoInfer<ValueCodec>,
      string
    >,
    const Mapping extends KafkaDeleteSourceMapping<
      Regions,
      NoInfer<KeyCodec>,
      NoInfer<ValueCodec>,
      object
    >,
    const StartFrom extends KafkaStartPosition,
    const RetentionPolicy extends KafkaRetentionPolicy,
    const Input,
    const Retry extends SourceRetryPolicy<KafkaAdapterFailure<Regions[number]>, unknown>,
  >(
    input: KafkaDeleteSourceCandidate<
      Topic,
      Regions,
      KeyCodec,
      ValueCodec,
      LocalRowKey,
      Mapping,
      StartFrom,
      RetentionPolicy
    > &
      Input &
      KafkaSourceInputGuards<
        NoInfer<Input>,
        KafkaDeleteSourceCandidate<
          Topic,
          Regions,
          KeyCodec,
          ValueCodec,
          LocalRowKey,
          Mapping,
          StartFrom,
          RetentionPolicy
        >
      >,
    retry: Retry,
    ..._unsupported: KafkaSourceRetryAdditionalArguments<NoInfer<Retry>>
  ): CapturedDeleteDefinition<
    Regions,
    KafkaCodecCandidate<KeyCodec>,
    KafkaCodecCandidate<ValueCodec>,
    LocalRowKey,
    Mapping,
    KafkaSourceRetryServices<Retry>
  >;
  <
    const Topic extends string,
    const Regions extends KafkaNonEmptyReadonlyArray<string>,
    const CleanupPolicy extends "compact" | "compact-and-delete",
    const KeyCodec extends KafkaCompactionKeyCodec<unknown, unknown>,
    const ValueCodec extends KafkaCodec<unknown, unknown>,
    const Mapping extends KafkaCompactionSourceMapping<
      Regions,
      NoInfer<KeyCodec>,
      NoInfer<ValueCodec>,
      object
    >,
    const StartFrom extends KafkaStartPosition,
    const RetentionPolicy extends KafkaRetentionPolicy,
    const Input,
    const Retry extends SourceRetryPolicy<KafkaAdapterFailure<Regions[number]>, unknown>,
  >(
    input: KafkaCompactionSourceCandidate<
      Topic,
      Regions,
      CleanupPolicy,
      KeyCodec,
      ValueCodec,
      Mapping,
      StartFrom,
      RetentionPolicy
    > &
      Input &
      KafkaCompactionSourceInputGuards<
        NoInfer<Input>,
        KafkaCompactionSourceCandidate<
          Topic,
          Regions,
          CleanupPolicy,
          KeyCodec,
          ValueCodec,
          Mapping,
          StartFrom,
          RetentionPolicy
        >
      >,
    retry: Retry,
    ..._unsupported: KafkaSourceRetryAdditionalArguments<NoInfer<Retry>>
  ): CapturedCompactionDefinition<
    Regions,
    CleanupPolicy,
    KafkaCompactionKeyCodecCandidate<KeyCodec>,
    KafkaCodecCandidate<ValueCodec>,
    Mapping,
    KafkaSourceRetryServices<Retry>
  >;
};

const captureRetentionPolicy = (input: unknown): KafkaCapturedRetentionPolicy => {
  if (input === "match-kafka-retention") {
    return Object.freeze({ _tag: "MatchKafkaRetention" });
  }
  const duration = decodeKafkaDurationInput(input);
  if (Option.isNone(duration)) {
    throw new KafkaSourceConfigurationError(
      "Kafka retentionPolicy must be match-kafka-retention, a positive Effect Duration, or positive infinity.",
    );
  }
  const captured = Duration.match(duration.value, {
    onMillis: (millis) =>
      millis > 0
        ? {
            _tag: "Finite" as const,
            durationNanos: BigInt(millis) * 1_000_000n,
          }
        : undefined,
    onNanos: (nanos) =>
      nanos > 0n
        ? {
            _tag: "Finite" as const,
            durationNanos: nanos,
          }
        : undefined,
    onInfinity: () => ({ _tag: "Forever" as const }),
    onNegativeInfinity: () => undefined,
  });
  if (captured === undefined) {
    throw new KafkaSourceConfigurationError(
      "Kafka explicit retentionPolicy must be positive or positive infinity.",
    );
  }
  return Object.freeze(captured);
};

function makeKafkaSource<
  const Topic extends string,
  const Regions extends KafkaNonEmptyReadonlyArray<string>,
  const KeyCodec extends KafkaCodec<unknown, unknown>,
  const ValueCodec extends KafkaCodec<unknown, unknown>,
  const LocalRowKey extends KafkaSourceLocalRowKey<
    Regions,
    NoInfer<KeyCodec>,
    NoInfer<ValueCodec>,
    string
  >,
  const Mapping extends KafkaDeleteSourceMapping<
    Regions,
    NoInfer<KeyCodec>,
    NoInfer<ValueCodec>,
    object
  >,
  const StartFrom extends KafkaStartPosition,
  const RetentionPolicy extends KafkaRetentionPolicy,
  const Input,
>(
  input: KafkaDeleteSourceCandidate<
    Topic,
    Regions,
    KeyCodec,
    ValueCodec,
    LocalRowKey,
    Mapping,
    StartFrom,
    RetentionPolicy
  > &
    Input &
    KafkaSourceInputGuards<
      NoInfer<Input>,
      KafkaDeleteSourceCandidate<
        Topic,
        Regions,
        KeyCodec,
        ValueCodec,
        LocalRowKey,
        Mapping,
        StartFrom,
        RetentionPolicy
      >
    >,
): CapturedDeleteDefinition<
  Regions,
  KafkaCodecCandidate<KeyCodec>,
  KafkaCodecCandidate<ValueCodec>,
  LocalRowKey,
  Mapping,
  never
>;
function makeKafkaSource<
  const Topic extends string,
  const Regions extends KafkaNonEmptyReadonlyArray<string>,
  const KeyCodec extends KafkaCodec<unknown, unknown>,
  const ValueCodec extends KafkaCodec<unknown, unknown>,
  const LocalRowKey extends KafkaSourceLocalRowKey<
    Regions,
    NoInfer<KeyCodec>,
    NoInfer<ValueCodec>,
    string
  >,
  const Mapping extends KafkaDeleteSourceMapping<
    Regions,
    NoInfer<KeyCodec>,
    NoInfer<ValueCodec>,
    object
  >,
  const StartFrom extends KafkaStartPosition,
  const RetentionPolicy extends KafkaRetentionPolicy,
  const Input,
  const Retry extends SourceRetryPolicy<KafkaAdapterFailure<Regions[number]>, unknown>,
>(
  input: KafkaDeleteSourceCandidate<
    Topic,
    Regions,
    KeyCodec,
    ValueCodec,
    LocalRowKey,
    Mapping,
    StartFrom,
    RetentionPolicy
  > &
    Input &
    KafkaSourceInputGuards<
      NoInfer<Input>,
      KafkaDeleteSourceCandidate<
        Topic,
        Regions,
        KeyCodec,
        ValueCodec,
        LocalRowKey,
        Mapping,
        StartFrom,
        RetentionPolicy
      >
    >,
  retry: Retry,
  ..._unsupported: KafkaSourceRetryAdditionalArguments<NoInfer<Retry>>
): CapturedDeleteDefinition<
  Regions,
  KafkaCodecCandidate<KeyCodec>,
  KafkaCodecCandidate<ValueCodec>,
  LocalRowKey,
  Mapping,
  KafkaSourceRetryServices<Retry>
>;
function makeKafkaSource<
  const Topic extends string,
  const Regions extends KafkaNonEmptyReadonlyArray<string>,
  const CleanupPolicy extends "compact" | "compact-and-delete",
  const KeyCodec extends KafkaCompactionKeyCodec<unknown, unknown>,
  const ValueCodec extends KafkaCodec<unknown, unknown>,
  const Mapping extends KafkaCompactionSourceMapping<
    Regions,
    NoInfer<KeyCodec>,
    NoInfer<ValueCodec>,
    object
  >,
  const StartFrom extends KafkaStartPosition,
  const RetentionPolicy extends KafkaRetentionPolicy,
  const Input,
>(
  input: KafkaCompactionSourceCandidate<
    Topic,
    Regions,
    CleanupPolicy,
    KeyCodec,
    ValueCodec,
    Mapping,
    StartFrom,
    RetentionPolicy
  > &
    Input &
    KafkaCompactionSourceInputGuards<
      NoInfer<Input>,
      KafkaCompactionSourceCandidate<
        Topic,
        Regions,
        CleanupPolicy,
        KeyCodec,
        ValueCodec,
        Mapping,
        StartFrom,
        RetentionPolicy
      >
    >,
): CapturedCompactionDefinition<
  Regions,
  CleanupPolicy,
  KafkaCompactionKeyCodecCandidate<KeyCodec>,
  KafkaCodecCandidate<ValueCodec>,
  Mapping,
  never
>;
function makeKafkaSource<
  const Topic extends string,
  const Regions extends KafkaNonEmptyReadonlyArray<string>,
  const CleanupPolicy extends "compact" | "compact-and-delete",
  const KeyCodec extends KafkaCompactionKeyCodec<unknown, unknown>,
  const ValueCodec extends KafkaCodec<unknown, unknown>,
  const Mapping extends KafkaCompactionSourceMapping<
    Regions,
    NoInfer<KeyCodec>,
    NoInfer<ValueCodec>,
    object
  >,
  const StartFrom extends KafkaStartPosition,
  const RetentionPolicy extends KafkaRetentionPolicy,
  const Input,
  const Retry extends SourceRetryPolicy<KafkaAdapterFailure<Regions[number]>, unknown>,
>(
  input: KafkaCompactionSourceCandidate<
    Topic,
    Regions,
    CleanupPolicy,
    KeyCodec,
    ValueCodec,
    Mapping,
    StartFrom,
    RetentionPolicy
  > &
    Input &
    KafkaCompactionSourceInputGuards<
      NoInfer<Input>,
      KafkaCompactionSourceCandidate<
        Topic,
        Regions,
        CleanupPolicy,
        KeyCodec,
        ValueCodec,
        Mapping,
        StartFrom,
        RetentionPolicy
      >
    >,
  retry: Retry,
  ..._unsupported: KafkaSourceRetryAdditionalArguments<NoInfer<Retry>>
): CapturedCompactionDefinition<
  Regions,
  CleanupPolicy,
  KafkaCompactionKeyCodecCandidate<KeyCodec>,
  KafkaCodecCandidate<ValueCodec>,
  Mapping,
  KafkaSourceRetryServices<Retry>
>;

function makeKafkaSource(
  input: unknown,
  retry?: unknown,
  ..._unsupported: ReadonlyArray<unknown>
): unknown {
  const envelope = captureOwnDataValues(input);
  const cleanupPolicy = envelope?.get("cleanupPolicy");
  const expectedKeys =
    cleanupPolicy === "delete"
      ? [
          "cleanupPolicy",
          "retentionPolicy",
          "topic",
          "regions",
          "key",
          "value",
          "localRowKey",
          "map",
          "startFrom",
        ]
      : [
          "cleanupPolicy",
          "retentionPolicy",
          "topic",
          "regions",
          "key",
          "value",
          "map",
          "startFrom",
        ];
  const captured = captureExactOwnDataValues(input, expectedKeys);
  if (captured === undefined) {
    throw new KafkaSourceConfigurationError(
      "Kafka source requires exactly its cleanup-policy-specific fields, including cleanupPolicy and retentionPolicy.",
    );
  }
  const topic = captured.get("topic");
  const regionInput = captured.get("regions");
  const key = captured.get("key");
  const value = captured.get("value");
  const localRowKey = captured.get("localRowKey");
  const map = captured.get("map");
  const startFrom = captured.get("startFrom");
  const retentionPolicy = captureRetentionPolicy(captured.get("retentionPolicy"));
  if (
    cleanupPolicy !== "delete" &&
    cleanupPolicy !== "compact" &&
    cleanupPolicy !== "compact-and-delete"
  ) {
    throw new KafkaSourceConfigurationError(
      "Kafka cleanupPolicy must be delete, compact, or compact-and-delete.",
    );
  }
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
  if (!isKafkaRuntimeCodec(value)) {
    throw new KafkaSourceConfigurationError("Kafka source value must be a Kafka codec.");
  }
  if (!isKafkaRuntimeCallback(map)) {
    throw new KafkaSourceConfigurationError(
      cleanupPolicy === "delete"
        ? "Delete-only Kafka source localRowKey and map must be synchronous functions."
        : "Compaction-capable Kafka source map must be a synchronous function.",
    );
  }
  if (retry !== undefined && !isKafkaSourceRetryPolicy(retry)) {
    throw new KafkaSourceConfigurationError(
      "Kafka source retry override must be an Effect Schedule.",
    );
  }
  const capturedStart = captureStartPosition(startFrom);
  if (cleanupPolicy === "delete") {
    if (!isKafkaRuntimeCodec(key)) {
      throw new KafkaSourceConfigurationError(
        "Delete-only Kafka source key must be a Kafka codec.",
      );
    }
    if (!isKafkaRuntimeCallback(localRowKey)) {
      throw new KafkaSourceConfigurationError(
        "Delete-only Kafka source localRowKey and map must be synchronous functions.",
      );
    }
    const options: KafkaRuntimeDeleteDefinitionOptions = {
      cleanupPolicy,
      retentionPolicy,
      topic,
      regions,
      key,
      value,
      localRowKey,
      map,
      startFrom: capturedStart,
    };
    return retry === undefined
      ? SourceAdapter.materializedSource(KafkaSourceAdapterHandle, options)
      : SourceAdapter.materializedSource(KafkaSourceAdapterHandle, options, retry);
  }
  if (!isKafkaRuntimeCompactionKeyCodec(key)) {
    throw new KafkaSourceConfigurationError(
      "Compaction-capable Kafka source key must be a metadata-free Kafka Compaction Key codec.",
    );
  }
  const options: KafkaRuntimeCompactionDefinitionOptions = {
    cleanupPolicy,
    retentionPolicy,
    topic,
    regions,
    key,
    value,
    map,
    startFrom: capturedStart,
  };
  return retry === undefined
    ? SourceAdapter.materializedSource(KafkaSourceAdapterHandle, options)
    : SourceAdapter.materializedSource(KafkaSourceAdapterHandle, options, retry);
}

const validatePartition = (partition: unknown): partition is number =>
  typeof partition === "number" &&
  Number.isSafeInteger(partition) &&
  partition >= 0 &&
  partition <= 2_147_483_647;

export type KafkaDeleteRowIdInput = {
  readonly region: string;
  readonly partition: number;
  readonly localRowKey: string;
};

type KafkaDeleteRowIdInputGuards<Input> = KafkaNotAny<Input> &
  RejectExtraKeys<Input, KafkaDeleteRowIdInput> &
  RejectAnySourceField<Input, "region"> &
  RejectAnySourceField<Input, "partition"> &
  RejectAnySourceField<Input, "localRowKey">;

const makeKafkaDeleteRowId = (
  region: unknown,
  partition: unknown,
  localRowKey: unknown,
): string => {
  if (!validateRegion(region)) {
    throw new KafkaSourceConfigurationError(
      "Kafka delete rowId region must be non-empty and cannot contain ':'.",
    );
  }
  if (!validatePartition(partition)) {
    throw new KafkaSourceConfigurationError(
      "Kafka delete rowId partition must be a non-negative Kafka partition.",
    );
  }
  if (typeof localRowKey !== "string" || localRowKey.length === 0) {
    throw new KafkaSourceConfigurationError(
      "Kafka delete rowId localRowKey must be a non-empty string.",
    );
  }
  return `${region}:${partition}:${localRowKey}`;
};

export const kafkaDeleteRowId = <const Input extends KafkaDeleteRowIdInput>(
  input: Input & KafkaDeleteRowIdInputGuards<NoInfer<Input>>,
): string => {
  const captured = captureExactOwnDataValues(input, ["region", "partition", "localRowKey"]);
  if (captured === undefined) {
    throw new KafkaSourceConfigurationError(
      "Kafka delete rowId requires exactly region, partition, and localRowKey.",
    );
  }
  const region = captured.get("region");
  const partition = captured.get("partition");
  const localRowKey = captured.get("localRowKey");
  return makeKafkaDeleteRowId(region, partition, localRowKey);
};

export type KafkaDecodedDeleteRowId = {
  readonly _tag: "Delete";
  readonly region: string;
  readonly partition: number;
  readonly localRowKey: string;
};

export const decodeKafkaDeleteRowId = (id: string): KafkaDecodedDeleteRowId => {
  if (typeof id !== "string") {
    throw new KafkaSourceConfigurationError("Kafka delete row ID must be a string.");
  }
  const regionSeparator = id.indexOf(":");
  const partitionSeparator = id.indexOf(":", regionSeparator + 1);
  if (
    regionSeparator <= 0 ||
    partitionSeparator <= regionSeparator + 1 ||
    partitionSeparator === id.length - 1
  ) {
    throw new KafkaSourceConfigurationError(
      "Kafka delete row ID must contain region, partition, and local key components.",
    );
  }
  const region = id.slice(0, regionSeparator);
  const partitionText = id.slice(regionSeparator + 1, partitionSeparator);
  const partition = Number(partitionText);
  const localRowKey = id.slice(partitionSeparator + 1);
  if (
    !validateRegion(region) ||
    !validatePartition(partition) ||
    partitionText !== String(partition)
  ) {
    throw new KafkaSourceConfigurationError(
      "Kafka delete row ID contains an invalid region or partition.",
    );
  }
  return Object.freeze({
    _tag: "Delete",
    region,
    partition,
    localRowKey,
  });
};

const base64UrlAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

const encodeBase64Url = (bytes: Uint8Array): string => {
  let encoded = "";
  const values = bytes.values();
  while (true) {
    const first = values.next();
    if (first.done) {
      break;
    }
    const second = values.next();
    const third = values.next();
    const value =
      (first.value << 16) |
      ((second.done ? 0 : second.value) << 8) |
      (third.done ? 0 : third.value);
    encoded += base64UrlAlphabet[(value >>> 18) & 63];
    encoded += base64UrlAlphabet[(value >>> 12) & 63];
    if (!second.done) {
      encoded += base64UrlAlphabet[(value >>> 6) & 63];
    }
    if (!third.done) {
      encoded += base64UrlAlphabet[value & 63];
    }
  }
  return encoded;
};

const decodeBase64Url = (encoded: string): Uint8Array => {
  if (!/^[A-Za-z0-9_-]*$/u.test(encoded) || encoded.length % 4 === 1) {
    throw new KafkaSourceConfigurationError(
      "Kafka compact row ID contains invalid serialized key bytes.",
    );
  }
  const bytes: Array<number> = [];
  for (let index = 0; index < encoded.length; index += 4) {
    const first = base64UrlAlphabet.indexOf(encoded.charAt(index));
    const second = base64UrlAlphabet.indexOf(encoded.charAt(index + 1));
    const third =
      index + 2 < encoded.length ? base64UrlAlphabet.indexOf(encoded.charAt(index + 2)) : 0;
    const fourth =
      index + 3 < encoded.length ? base64UrlAlphabet.indexOf(encoded.charAt(index + 3)) : 0;
    const value = (first << 18) | (second << 12) | (third << 6) | fourth;
    bytes.push((value >>> 16) & 255);
    if (index + 2 < encoded.length) {
      bytes.push((value >>> 8) & 255);
    }
    if (index + 3 < encoded.length) {
      bytes.push(value & 255);
    }
  }
  const decoded = Uint8Array.from(bytes);
  if (encodeBase64Url(decoded) !== encoded) {
    throw new KafkaSourceConfigurationError(
      "Kafka compact row ID contains non-canonical serialized key bytes.",
    );
  }
  return decoded;
};

export type KafkaCompactionRowIdInput = {
  readonly region: string;
  readonly partition: number;
  readonly serializedKeyBytes: Uint8Array;
};

type KafkaCompactionRowIdInputGuards<Input> = KafkaNotAny<Input> &
  RejectExtraKeys<Input, KafkaCompactionRowIdInput> &
  RejectAnySourceField<Input, "region"> &
  RejectAnySourceField<Input, "partition"> &
  RejectAnySourceField<Input, "serializedKeyBytes">;

const makeKafkaCompactionRowId = (
  region: unknown,
  partition: unknown,
  serializedKeyBytes: unknown,
): string => {
  if (!validateRegion(region) || !validatePartition(partition)) {
    throw new KafkaSourceConfigurationError(
      "Kafka compact rowId requires a valid region and Kafka partition.",
    );
  }
  if (!(serializedKeyBytes instanceof Uint8Array)) {
    throw new KafkaSourceConfigurationError(
      "Kafka compact rowId serializedKeyBytes must be a Uint8Array.",
    );
  }
  return `${region}:${partition}:k${encodeBase64Url(serializedKeyBytes)}`;
};

export const kafkaCompactionRowId = <const Input extends KafkaCompactionRowIdInput>(
  input: Input & KafkaCompactionRowIdInputGuards<NoInfer<Input>>,
): string => {
  const captured = captureExactOwnDataValues(input, ["region", "partition", "serializedKeyBytes"]);
  if (captured === undefined) {
    throw new KafkaSourceConfigurationError(
      "Kafka compact rowId requires exactly region, partition, and serializedKeyBytes.",
    );
  }
  const region = captured.get("region");
  const partition = captured.get("partition");
  const serializedKeyBytes = captured.get("serializedKeyBytes");
  return makeKafkaCompactionRowId(region, partition, serializedKeyBytes);
};

export type KafkaDecodedCompactionRowId = {
  readonly _tag: "Compaction";
  readonly region: string;
  readonly partition: number;
  readonly serializedKeyBytes: Uint8Array;
};

export const decodeKafkaCompactionRowId = (id: string): KafkaDecodedCompactionRowId => {
  if (typeof id !== "string") {
    throw new KafkaSourceConfigurationError("Kafka compact row ID must be a string.");
  }
  const regionSeparator = id.indexOf(":");
  const partitionSeparator = id.indexOf(":", regionSeparator + 1);
  if (
    regionSeparator <= 0 ||
    partitionSeparator <= regionSeparator + 1 ||
    id[partitionSeparator + 1] !== "k"
  ) {
    throw new KafkaSourceConfigurationError(
      "Kafka compact row ID must contain region, partition, and serialized key components.",
    );
  }
  const region = id.slice(0, regionSeparator);
  const partitionText = id.slice(regionSeparator + 1, partitionSeparator);
  const partition = Number(partitionText);
  if (
    !validateRegion(region) ||
    !validatePartition(partition) ||
    partitionText !== String(partition)
  ) {
    throw new KafkaSourceConfigurationError(
      "Kafka compact row ID contains an invalid region or partition.",
    );
  }
  return Object.freeze({
    _tag: "Compaction",
    region,
    partition,
    serializedKeyBytes: decodeBase64Url(id.slice(partitionSeparator + 2)),
  });
};

export type KafkaRowIdInput =
  | ({ readonly cleanupPolicy: "delete" } & KafkaDeleteRowIdInput)
  | ({
      readonly cleanupPolicy: "compact" | "compact-and-delete";
    } & KafkaCompactionRowIdInput);

type KafkaDeleteRowIdWithPolicy = Extract<KafkaRowIdInput, { readonly cleanupPolicy: "delete" }>;
type KafkaCompactionRowIdWithPolicy = Extract<
  KafkaRowIdInput,
  { readonly cleanupPolicy: "compact" | "compact-and-delete" }
>;

type KafkaRowIdInputGuards<Input> = KafkaNotAny<Input> &
  RejectAnySourceField<Input, "cleanupPolicy"> &
  (Input extends KafkaDeleteRowIdWithPolicy
    ? RejectExtraKeys<Input, KafkaDeleteRowIdWithPolicy> &
        RejectAnySourceField<Input, "region"> &
        RejectAnySourceField<Input, "partition"> &
        RejectAnySourceField<Input, "localRowKey">
    : Input extends KafkaCompactionRowIdWithPolicy
      ? RejectExtraKeys<Input, KafkaCompactionRowIdWithPolicy> &
          RejectAnySourceField<Input, "region"> &
          RejectAnySourceField<Input, "partition"> &
          RejectAnySourceField<Input, "serializedKeyBytes">
      : never);

export function kafkaRowId<const Input extends KafkaDeleteRowIdWithPolicy>(
  input: Input & KafkaRowIdInputGuards<NoInfer<Input>>,
): string;
export function kafkaRowId<const Input extends KafkaCompactionRowIdWithPolicy>(
  input: Input & KafkaRowIdInputGuards<NoInfer<Input>>,
): string;
export function kafkaRowId(input: KafkaRowIdInput): string {
  const captured = captureOwnDataValues(input);
  if (captured === undefined) {
    throw new KafkaSourceConfigurationError("Kafka rowId input must be an object.");
  }
  const cleanupPolicy = captured.get("cleanupPolicy");
  if (cleanupPolicy === "delete") {
    const expected = ["cleanupPolicy", "region", "partition", "localRowKey"];
    if (captured.size !== expected.length || !expected.every((key) => captured.has(key))) {
      throw new KafkaSourceConfigurationError(
        "Kafka delete rowId requires exactly cleanupPolicy, region, partition, and localRowKey.",
      );
    }
    return makeKafkaDeleteRowId(
      captured.get("region"),
      captured.get("partition"),
      captured.get("localRowKey"),
    );
  }
  if (cleanupPolicy === "compact" || cleanupPolicy === "compact-and-delete") {
    const expected = ["cleanupPolicy", "region", "partition", "serializedKeyBytes"];
    if (captured.size !== expected.length || !expected.every((key) => captured.has(key))) {
      throw new KafkaSourceConfigurationError(
        "Kafka compact rowId requires exactly cleanupPolicy, region, partition, and serializedKeyBytes.",
      );
    }
    return makeKafkaCompactionRowId(
      captured.get("region"),
      captured.get("partition"),
      captured.get("serializedKeyBytes"),
    );
  }
  throw new KafkaSourceConfigurationError(
    "Kafka rowId cleanupPolicy must be delete, compact, or compact-and-delete.",
  );
}

export type KafkaDecodedRowId = KafkaDecodedDeleteRowId | KafkaDecodedCompactionRowId;

type KafkaDecodedRowIdForPolicy<Policy> =
  IsAny<Policy> extends true
    ? KafkaDecodedRowId
    : Policy extends "delete"
      ? KafkaDecodedDeleteRowId
      : Policy extends "compact" | "compact-and-delete"
        ? KafkaDecodedCompactionRowId
        : KafkaDecodedRowId;

export function decodeKafkaRowId<const Policy extends KafkaCleanupPolicy>(
  id: string,
  cleanupPolicy: Policy & RejectAny<NoInfer<Policy>>,
): KafkaDecodedRowIdForPolicy<Policy>;
export function decodeKafkaRowId(id: string, cleanupPolicy: unknown): KafkaDecodedRowId {
  if (cleanupPolicy === "delete") {
    return decodeKafkaDeleteRowId(id);
  }
  if (cleanupPolicy === "compact" || cleanupPolicy === "compact-and-delete") {
    return decodeKafkaCompactionRowId(id);
  }
  throw new KafkaSourceConfigurationError(
    "Kafka rowId cleanupPolicy must be delete, compact, or compact-and-delete.",
  );
}

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
  readonly compactionKey: typeof KafkaCompactionKey;
  readonly consumerGroupId: typeof kafkaConsumerGroupId;
  readonly decodeCompactionRowId: typeof decodeKafkaCompactionRowId;
  readonly decodeDeleteRowId: typeof decodeKafkaDeleteRowId;
  readonly decodeRowId: typeof decodeKafkaRowId;
  readonly deleteRowId: typeof kafkaDeleteRowId;
  readonly compactionRowId: typeof kafkaCompactionRowId;
  readonly json: typeof jsonCodec;
  readonly protobuf: typeof protobufCodec;
  readonly schemaRegistry: typeof KafkaSchemaRegistry;
  readonly rowId: typeof kafkaRowId;
  readonly source: KafkaSourceApi;
  readonly string: typeof stringCodec;
};

export const kafka: KafkaContractApi = Object.freeze({
  bytes: bytesCodec,
  string: stringCodec,
  json: jsonCodec,
  protobuf: protobufCodec,
  schemaRegistry: KafkaSchemaRegistry,
  codec: customCodec,
  compactionKey: KafkaCompactionKey,
  source: makeKafkaSource,
  rowId: kafkaRowId,
  deleteRowId: kafkaDeleteRowId,
  compactionRowId: kafkaCompactionRowId,
  decodeRowId: decodeKafkaRowId,
  decodeDeleteRowId: decodeKafkaDeleteRowId,
  decodeCompactionRowId: decodeKafkaCompactionRowId,
  consumerGroupId: kafkaConsumerGroupId,
});

export type KafkaMaterializedLifecycleDeclaration = KafkaMaterializedLifecycle;

export type KafkaSourceRetryPolicy<
  Region extends string = string,
  Services = never,
> = Schedule.Schedule<unknown, SourceTermination<KafkaAdapterFailure<Region>>, never, Services>;
