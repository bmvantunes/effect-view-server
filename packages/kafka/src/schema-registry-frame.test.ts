import { describe, expect, it } from "@effect/vitest";
import { parseKafkaSchemaRegistryProtobufFrame } from "./schema-registry-frame";

describe("Confluent Schema Registry Protobuf framing", () => {
  it("parses multiple message indexes and preserves the payload", () => {
    expect(
      parseKafkaSchemaRegistryProtobufFrame(Uint8Array.from([0, 0, 0, 0, 42, 4, 2, 4, 0xaa])),
    ).toStrictEqual({
      _tag: "KafkaSchemaRegistryProtobufFrame",
      schemaId: 42,
      messageIndexes: [1, 2],
      payload: Uint8Array.from([0xaa]),
    });
  });

  it("reports every malformed frame shape at the parser boundary", () => {
    const malformedFrames = [
      Uint8Array.from([]),
      Uint8Array.from([0, 0, 0, 0, 99]),
      Uint8Array.from([1, 0, 0, 0, 42, 0]),
      Uint8Array.from([0, 0, 0, 0, 42, 0x80]),
      Uint8Array.from([0, 0, 0, 0, 42, 2]),
      Uint8Array.from([0, 0, 0, 0, 42, 4, 1, 0]),
      Uint8Array.from([0, 0, 0, 0, 0, 0]),
      Uint8Array.from([0, 0x80, 0, 0, 0, 0]),
      Uint8Array.from([0, 0, 0, 0, 42, 0xff, 0xff, 0xff, 0xff, 0x1f]),
      Uint8Array.from([0, 0, 0, 0, 42, 0x80, 0x80, 0x80, 0x80, 0x80]),
      Uint8Array.from([0, 0, 0, 0, 42, 1]),
      Uint8Array.from([0, 0, 0, 0, 42, 6, 0]),
      Uint8Array.from([0, 0, 0, 0, 42, 2, 0x80]),
      Uint8Array.from([0, 0, 0, 0, 42, 2, 0xff, 0xff, 0xff, 0xff, 0x1f]),
    ];

    expect(malformedFrames.map(parseKafkaSchemaRegistryProtobufFrame)).toStrictEqual([
      {
        _tag: "KafkaSchemaRegistryFrameParseFailure",
        schemaId: null,
        message:
          "Confluent Schema Registry Protobuf frame is shorter than its six-byte minimum prefix.",
      },
      {
        _tag: "KafkaSchemaRegistryFrameParseFailure",
        schemaId: 99,
        message:
          "Confluent Schema Registry Protobuf frame is shorter than its six-byte minimum prefix.",
      },
      {
        _tag: "KafkaSchemaRegistryFrameParseFailure",
        schemaId: null,
        message:
          "Confluent Schema Registry Protobuf frame uses unsupported payload-prefix version 1.",
      },
      {
        _tag: "KafkaSchemaRegistryFrameParseFailure",
        schemaId: 42,
        message:
          "Confluent Schema Registry Protobuf frame contains a truncated message-index varint.",
      },
      {
        _tag: "KafkaSchemaRegistryFrameParseFailure",
        schemaId: 42,
        message:
          "Confluent Schema Registry Protobuf frame declares more message indexes than the payload contains.",
      },
      {
        _tag: "KafkaSchemaRegistryFrameParseFailure",
        schemaId: 42,
        message: "Confluent Schema Registry Protobuf frame contains a negative message index.",
      },
      {
        _tag: "KafkaSchemaRegistryFrameParseFailure",
        schemaId: null,
        message: "Confluent Schema Registry Protobuf frame contains an invalid schema ID.",
      },
      {
        _tag: "KafkaSchemaRegistryFrameParseFailure",
        schemaId: null,
        message: "Confluent Schema Registry Protobuf frame contains an invalid schema ID.",
      },
      {
        _tag: "KafkaSchemaRegistryFrameParseFailure",
        schemaId: 42,
        message:
          "Confluent Schema Registry Protobuf frame contains an overflowing message-index varint.",
      },
      {
        _tag: "KafkaSchemaRegistryFrameParseFailure",
        schemaId: 42,
        message:
          "Confluent Schema Registry Protobuf frame contains a message-index varint longer than five bytes.",
      },
      {
        _tag: "KafkaSchemaRegistryFrameParseFailure",
        schemaId: 42,
        message:
          "Confluent Schema Registry Protobuf frame contains a negative message-index count.",
      },
      {
        _tag: "KafkaSchemaRegistryFrameParseFailure",
        schemaId: 42,
        message:
          "Confluent Schema Registry Protobuf frame declares more message indexes than the payload contains.",
      },
      {
        _tag: "KafkaSchemaRegistryFrameParseFailure",
        schemaId: 42,
        message:
          "Confluent Schema Registry Protobuf frame contains a truncated message-index varint.",
      },
      {
        _tag: "KafkaSchemaRegistryFrameParseFailure",
        schemaId: 42,
        message:
          "Confluent Schema Registry Protobuf frame contains an overflowing message-index varint.",
      },
    ]);
  });
});
