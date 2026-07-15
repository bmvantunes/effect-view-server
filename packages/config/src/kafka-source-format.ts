import type { Effect } from "effect";

import type { KafkaCodec, KafkaCodecDecodeInput, KafkaSourceCodec } from "./kafka-contract";

export type KafkaSourceFormat = KafkaSourceCodec["format"];

export type KafkaResolvedSourceFormat<A, E> = {
  readonly decode: (input: KafkaCodecDecodeInput) => Effect.Effect<A, E>;
};

type KafkaSourceFormatDecoder<A, E> = (input: KafkaCodecDecodeInput) => Effect.Effect<A, E>;

const isNonNullObject = (value: unknown): value is object =>
  typeof value === "object" && value !== null;

const requireCodecOption = (codec: object, format: KafkaSourceFormat, option: string): unknown => {
  if (!Object.hasOwn(codec, option)) {
    throw new Error(`Kafka ${format} codec requires ${option}.`);
  }
  return Reflect.get(codec, option);
};

const validateCodecOptions = (
  codec: object,
  format: KafkaSourceFormat,
  allowedOptions: ReadonlyArray<string>,
): void => {
  for (const option of Object.getOwnPropertyNames(codec)) {
    if (!allowedOptions.includes(option)) {
      throw new Error(`Kafka ${format} codec cannot declare ${option}.`);
    }
  }
};

const validateOptionFreeCodec = (codec: object, format: "bytes" | "string" | "json"): void => {
  validateCodecOptions(codec, format, ["format"]);
};

const validateProtobufCodec = (codec: object): void => {
  validateCodecOptions(codec, "protobuf", ["format", "descriptor"]);
  const descriptor = requireCodecOption(codec, "protobuf", "descriptor");
  if (
    !isNonNullObject(descriptor) ||
    Reflect.get(descriptor, "kind") !== "message" ||
    typeof Reflect.get(descriptor, "typeName") !== "string" ||
    typeof Reflect.get(descriptor, "name") !== "string" ||
    !isNonNullObject(Reflect.get(descriptor, "file")) ||
    !Array.isArray(Reflect.get(descriptor, "fields")) ||
    !isNonNullObject(Reflect.get(descriptor, "field")) ||
    !Array.isArray(Reflect.get(descriptor, "members"))
  ) {
    throw new Error("Kafka protobuf codec descriptor must be a message descriptor.");
  }
};

const validateCustomCodec = (codec: object): void => {
  validateCodecOptions(codec, "custom", ["format", "name", "decode"]);
  const name = requireCodecOption(codec, "custom", "name");
  if (typeof name !== "string") {
    throw new Error("Kafka custom codec name must be a string.");
  }
  const decode = requireCodecOption(codec, "custom", "decode");
  if (typeof decode !== "function") {
    throw new Error("Kafka custom codec decode must be a function.");
  }
};

const makeResolvedSourceFormat = <A, E>(
  decode: KafkaSourceFormatDecoder<A, E>,
): KafkaResolvedSourceFormat<A, E> => ({
  decode,
});

export const resolveKafkaSourceFormat = <A, E>(
  codec: KafkaCodec<A, E>,
  decode: KafkaSourceFormatDecoder<A, E>,
): KafkaResolvedSourceFormat<A, E> => {
  switch (codec.format) {
    case "bytes":
      validateOptionFreeCodec(codec, "bytes");
      return makeResolvedSourceFormat(decode);
    case "string":
      validateOptionFreeCodec(codec, "string");
      return makeResolvedSourceFormat(decode);
    case "json":
      validateOptionFreeCodec(codec, "json");
      return makeResolvedSourceFormat(decode);
    case "protobuf":
      validateProtobufCodec(codec);
      return makeResolvedSourceFormat(decode);
    case "custom":
      validateCustomCodec(codec);
      return makeResolvedSourceFormat(decode);
    default:
      throw new Error(`Unsupported Kafka source codec format: ${codec.format}.`);
  }
};
