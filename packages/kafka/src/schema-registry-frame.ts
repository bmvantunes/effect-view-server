export type KafkaSchemaRegistryProtobufFrame = {
  readonly _tag: "KafkaSchemaRegistryProtobufFrame";
  readonly schemaId: number;
  readonly messageIndexes: readonly [number, ...ReadonlyArray<number>];
  readonly payload: Uint8Array;
};

export type KafkaSchemaRegistryFrameParseFailure = {
  readonly _tag: "KafkaSchemaRegistryFrameParseFailure";
  readonly schemaId: number | null;
  readonly message: string;
};

export type KafkaSchemaRegistryFrameParseResult =
  | KafkaSchemaRegistryProtobufFrame
  | KafkaSchemaRegistryFrameParseFailure;

type SignedVarint = {
  readonly _tag: "SignedVarint";
  readonly value: number;
  readonly nextOffset: number;
};

const defaultMessageIndexes: readonly [number] = Object.freeze([0]);

const frameFailure = (
  message: string,
  schemaId: number | null = null,
): KafkaSchemaRegistryFrameParseFailure => ({
  _tag: "KafkaSchemaRegistryFrameParseFailure",
  schemaId,
  message: `Confluent Schema Registry Protobuf frame ${message}`,
});

const readSignedVarint = (
  bytes: Uint8Array,
  offset: number,
  schemaId: number,
): SignedVarint | KafkaSchemaRegistryFrameParseFailure => {
  let raw = 0;
  let multiplier = 1;
  for (let index = 0; index < 5; index += 1) {
    const nextOffset = offset + index;
    const byte = bytes[nextOffset];
    if (byte === undefined) {
      return frameFailure("contains a truncated message-index varint.", schemaId);
    }
    raw += (byte & 0x7f) * multiplier;
    if ((byte & 0x80) === 0) {
      if (raw > 0xff_ff_ff_ff) {
        return frameFailure("contains an overflowing message-index varint.", schemaId);
      }
      return {
        _tag: "SignedVarint",
        value: raw % 2 === 0 ? raw / 2 : -(raw + 1) / 2,
        nextOffset: nextOffset + 1,
      };
    }
    multiplier *= 128;
  }
  return frameFailure("contains a message-index varint longer than five bytes.", schemaId);
};

export const parseKafkaSchemaRegistryProtobufFrame = (
  bytes: Uint8Array,
): KafkaSchemaRegistryFrameParseResult => {
  if (bytes.byteLength < 5) {
    return frameFailure("is shorter than its six-byte minimum prefix.");
  }
  if (bytes[0] !== 0) {
    return frameFailure(`uses unsupported payload-prefix version ${String(bytes[0])}.`);
  }

  let schemaId = 0;
  for (const byte of bytes.subarray(1, 5)) {
    schemaId = schemaId * 0x100 + byte;
  }
  if (schemaId <= 0 || schemaId > 0x7f_ff_ff_ff) {
    return frameFailure("contains an invalid schema ID.");
  }
  if (bytes.byteLength < 6) {
    return frameFailure("is shorter than its six-byte minimum prefix.", schemaId);
  }
  const size = readSignedVarint(bytes, 5, schemaId);
  if (size._tag === "KafkaSchemaRegistryFrameParseFailure") {
    return size;
  }
  if (size.value === 0) {
    return {
      _tag: "KafkaSchemaRegistryProtobufFrame",
      schemaId,
      messageIndexes: defaultMessageIndexes,
      payload: bytes.subarray(size.nextOffset),
    };
  }
  if (size.value < 0) {
    return frameFailure("contains a negative message-index count.", schemaId);
  }
  if (size.value > bytes.byteLength - size.nextOffset) {
    return frameFailure("declares more message indexes than the payload contains.", schemaId);
  }

  let firstMessageIndex = 0;
  const remainingMessageIndexes: Array<number> = [];
  let offset = size.nextOffset;
  for (let index = 0; index < size.value; index += 1) {
    const messageIndex = readSignedVarint(bytes, offset, schemaId);
    if (messageIndex._tag === "KafkaSchemaRegistryFrameParseFailure") {
      return messageIndex;
    }
    if (messageIndex.value < 0) {
      return frameFailure("contains a negative message index.", schemaId);
    }
    if (index === 0) {
      firstMessageIndex = messageIndex.value;
    } else {
      remainingMessageIndexes.push(messageIndex.value);
    }
    offset = messageIndex.nextOffset;
  }
  return {
    _tag: "KafkaSchemaRegistryProtobufFrame",
    schemaId,
    messageIndexes: [firstMessageIndex, ...remainingMessageIndexes],
    payload: bytes.subarray(offset),
  };
};
