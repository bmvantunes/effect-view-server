import { create, toBinary } from "@bufbuild/protobuf";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { decodeKafkaCodec, defineViewServerConfig, kafka } from "./index";
import type { KafkaCodec } from "./index";
import { resolveKafkaSourceFormat } from "./kafka-source-format";
import { decodeKafkaTopicMessage, makeKafkaSourceTopicsForConfig } from "./internal";

import { kafkaRegions, kafkaTestMetadata, textEncoder } from "../test-harness/kafka";
import { ordersService, ordersValueSchema } from "../test-harness/protobuf";

const textDecoder = new TextDecoder();

const resolveTestSourceFormat = <A, E>(codec: KafkaCodec<A, E>) =>
  resolveKafkaSourceFormat(codec, (input) => decodeKafkaCodec(codec, input));

const JsonValue = Schema.Struct({
  payload: Schema.String,
});

const ResolvedRow = Schema.Struct({
  id: Schema.String,
  payload: Schema.String,
  region: Schema.Literals(["usa", "london"]),
});

describe("Kafka source format resolution", () => {
  it.effect("normalizes every Kafka source codec through one decoder contract", () =>
    Effect.gen(function* () {
      const bytes = resolveTestSourceFormat(kafka.bytes());
      const string = resolveTestSourceFormat(kafka.string());
      const json = resolveTestSourceFormat(kafka.json(() => Schema.toCodecJson(JsonValue)));
      const protobuf = resolveTestSourceFormat(kafka.protobuf(ordersValueSchema));
      const custom = resolveTestSourceFormat(
        kafka.codec({
          name: "utf8-text",
          decode: ({ bytes: input }) => Effect.succeed(textDecoder.decode(input)),
        }),
      );
      const metadata = kafkaTestMetadata("usa");

      expect(
        yield* bytes.decode({
          bytes: new Uint8Array([1, 2, 3]),
          metadata,
        }),
      ).toStrictEqual(new Uint8Array([1, 2, 3]));
      expect(
        yield* string.decode({
          bytes: textEncoder.encode("string-value"),
          metadata,
        }),
      ).toBe("string-value");
      expect(
        yield* json.decode({
          bytes: textEncoder.encode('{"payload":"json-value"}'),
          metadata,
        }),
      ).toStrictEqual({ payload: "json-value" });
      const expectedProtobufValue = create(ordersValueSchema, {
        customerId: "protobuf-value",
        status: "open",
        price: 42,
        updatedAt: 100,
      });
      const protobufValue = yield* protobuf.decode({
        bytes: toBinary(ordersValueSchema, expectedProtobufValue),
        metadata,
      });
      expect(protobufValue).toStrictEqual(expectedProtobufValue);
      expect(
        yield* custom.decode({
          bytes: textEncoder.encode("custom-value"),
          metadata,
        }),
      ).toBe("custom-value");
      expect(
        [bytes, string, json, protobuf, custom].map((resolved) =>
          Object.getOwnPropertyNames(resolved),
        ),
      ).toStrictEqual([["decode"], ["decode"], ["decode"], ["decode"], ["decode"]]);
    }),
  );

  it("rejects unsupported formats and invalid format option combinations", () => {
    const unsupported = kafka.string();
    Object.defineProperty(unsupported, "format", { value: "xml" });
    expect(() => resolveTestSourceFormat(unsupported)).toThrow(
      "Unsupported Kafka source codec format: xml.",
    );

    const stringWithEncoding = kafka.string();
    Object.defineProperty(stringWithEncoding, "encoding", { value: "utf8" });
    expect(() => resolveTestSourceFormat(stringWithEncoding)).toThrow(
      "Kafka string codec cannot declare encoding.",
    );
    const invalidSourceConfig = defineViewServerConfig({
      kafka: kafkaRegions,
      topics: {
        invalid: {
          schema: ResolvedRow,
          key: "id",
          kafkaSource: kafka.source({
            topic: "invalid-source",
            regions: ["usa"],
            value: stringWithEncoding,
            rowKey: ({ key }) => key,
            map: ({ value, region }) => ({ payload: value, region }),
          }),
        },
      },
    });
    expect(() => makeKafkaSourceTopicsForConfig(invalidSourceConfig)).toThrow(
      "Kafka string codec cannot declare encoding.",
    );

    const protobufWithoutDescriptor = kafka.protobuf(ordersValueSchema);
    Reflect.deleteProperty(protobufWithoutDescriptor, "descriptor");
    expect(() => resolveTestSourceFormat(protobufWithoutDescriptor)).toThrow(
      "Kafka protobuf codec requires descriptor.",
    );

    const protobufWithInvalidDescriptor = kafka.protobuf(ordersValueSchema);
    Object.defineProperty(protobufWithInvalidDescriptor, "descriptor", { value: null });
    expect(() => resolveTestSourceFormat(protobufWithInvalidDescriptor)).toThrow(
      "Kafka protobuf codec descriptor must be a message descriptor.",
    );

    const protobufWithPlainObjectDescriptor = kafka.protobuf(ordersValueSchema);
    Object.defineProperty(protobufWithPlainObjectDescriptor, "descriptor", { value: {} });
    expect(() => resolveTestSourceFormat(protobufWithPlainObjectDescriptor)).toThrow(
      "Kafka protobuf codec descriptor must be a message descriptor.",
    );

    const protobufWithServiceDescriptor = kafka.protobuf(ordersValueSchema);
    Object.defineProperty(protobufWithServiceDescriptor, "descriptor", { value: ordersService });
    expect(() => resolveTestSourceFormat(protobufWithServiceDescriptor)).toThrow(
      "Kafka protobuf codec descriptor must be a message descriptor.",
    );

    const protobufWithNullDescriptorMembers = kafka.protobuf(ordersValueSchema);
    Object.defineProperty(protobufWithNullDescriptorMembers, "descriptor", {
      value: {
        kind: "message",
        typeName: "example.Invalid",
        name: "Invalid",
        file: null,
        fields: [],
        field: null,
        members: [],
      },
    });
    expect(() => resolveTestSourceFormat(protobufWithNullDescriptorMembers)).toThrow(
      "Kafka protobuf codec descriptor must be a message descriptor.",
    );

    const protobufWithFraming = kafka.protobuf(ordersValueSchema);
    Object.defineProperty(protobufWithFraming, "framing", { value: "delimited" });
    expect(() => resolveTestSourceFormat(protobufWithFraming)).toThrow(
      "Kafka protobuf codec cannot declare framing.",
    );

    const customWithDescriptor = kafka.codec({
      name: "custom-with-descriptor",
      decode: () => Effect.succeed("value"),
    });
    Object.defineProperty(customWithDescriptor, "descriptor", { value: ordersValueSchema });
    expect(() => resolveTestSourceFormat(customWithDescriptor)).toThrow(
      "Kafka custom codec cannot declare descriptor.",
    );

    const customWithoutName = kafka.codec({
      name: "custom-without-name",
      decode: () => Effect.succeed("value"),
    });
    Reflect.deleteProperty(customWithoutName, "name");
    expect(() => resolveTestSourceFormat(customWithoutName)).toThrow(
      "Kafka custom codec requires name.",
    );

    const customWithInvalidName = kafka.codec({
      name: "custom-with-invalid-name",
      decode: () => Effect.succeed("value"),
    });
    Object.defineProperty(customWithInvalidName, "name", { value: 1 });
    expect(() => resolveTestSourceFormat(customWithInvalidName)).toThrow(
      "Kafka custom codec name must be a string.",
    );

    const customWithoutDecode = kafka.codec({
      name: "custom-without-decode",
      decode: () => Effect.succeed("value"),
    });
    Reflect.deleteProperty(customWithoutDecode, "decode");
    expect(() => resolveTestSourceFormat(customWithoutDecode)).toThrow(
      "Kafka custom codec requires decode.",
    );

    const customWithInvalidDecode = kafka.codec({
      name: "custom-with-invalid-decode",
      decode: () => Effect.succeed("value"),
    });
    Object.defineProperty(customWithInvalidDecode, "decode", { value: "invalid" });
    expect(() => resolveTestSourceFormat(customWithInvalidDecode)).toThrow(
      "Kafka custom codec decode must be a function.",
    );
  });

  it.effect("resolves a source declaration into a decoder-only schema-valid row contract", () =>
    Effect.gen(function* () {
      const config = defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          resolved: {
            schema: ResolvedRow,
            key: "id",
            kafkaSource: kafka.source({
              topic: "resolved-source",
              regions: ["usa"],
              value: kafka.string(),
              rowKey: ({ key }) => key,
              map: ({ value, region }) => ({ payload: value, region }),
            }),
          },
        },
      });
      const source = makeKafkaSourceTopicsForConfig(config)[0]!;

      expect(Object.getOwnPropertyNames(source).sort()).toStrictEqual([
        "decode",
        "regions",
        "topic",
        "viewServerTopic",
      ]);
      expect(
        yield* decodeKafkaTopicMessage(source, {
          keyBytes: textEncoder.encode("resolved-1"),
          valueBytes: textEncoder.encode("resolved-value"),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
          rowKeyField: "id",
          schema: ResolvedRow,
          viewServerTopic: "resolved",
        }),
      ).toStrictEqual({
        row: {
          id: "resolved-1",
          payload: "resolved-value",
          region: "usa",
        },
        rowKey: "resolved-1",
        viewServerTopic: "resolved",
      });
    }),
  );
});
