import { create, toBinary } from "@bufbuild/protobuf";
import { fileDesc, messageDesc } from "@bufbuild/protobuf/codegenv2";
import type { Message } from "@bufbuild/protobuf";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { describe, expect, it } from "@effect/vitest";
import { Duration, Effect, Option, Schema } from "effect";
import {
  KafkaSourceConfigurationError,
  decodeKafkaCodec,
  decodeKafkaRowId,
  isKafkaCodec,
  kafka,
  kafkaConsumerGroupId,
  kafkaRowId,
} from "./contract";

const encoder = new TextEncoder();
const jsonFactoryFailureRow = Schema.Struct({
  value: Schema.String,
});

const metadata = {
  sourceTopic: "orders-source",
  sourceRegion: "eu",
  partition: 2,
  offset: 7n,
  timestampNanos: 9n,
  headers: {},
} as const;

type ContractMessage = Message<"kafka.contract.Value"> & {
  readonly label: string;
};

const base64FromBytes = (bytes: Uint8Array): string =>
  globalThis.btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(""));

const contractFileBytes = toBinary(
  FileDescriptorProtoSchema,
  create(FileDescriptorProtoSchema, {
    name: "kafka/contract.proto",
    package: "kafka.contract",
    syntax: "proto3",
    messageType: [
      {
        name: "Value",
        field: [
          {
            name: "label",
            number: 1,
            type: FieldDescriptorProto_Type.STRING,
          },
        ],
      },
    ],
  }),
);
const makeContractMessage = () =>
  messageDesc<ContractMessage>(fileDesc(base64FromBytes(contractFileBytes)), 0);
const contractMessage = makeContractMessage();

const validSourceInput = () => ({
  topic: "orders-source",
  regions: ["eu", "us"] as const,
  key: kafka.string(),
  value: kafka.json(() =>
    Schema.toCodecJson(
      Schema.Struct({
        price: Schema.Number,
      }),
    ),
  ),
  localRowKey: ({ key }: { readonly key: string }) => key,
  map: ({ value }: { readonly value: { readonly price: number } }) => ({
    price: value.price,
  }),
  startFrom: "earliest" as const,
});

describe("Kafka Source Adapter contract", () => {
  it.effect("decodes bytes, strings, JSON, protobuf, and custom codecs", () =>
    Effect.gen(function* () {
      const bytes = Uint8Array.from([1, 2, 3]);
      const bytesResult = yield* decodeKafkaCodec(kafka.bytes(), {
        bytes,
        metadata,
      });
      const stringResult = yield* decodeKafkaCodec(kafka.string(), {
        bytes: encoder.encode("hello"),
        metadata,
      });
      let jsonFactoryCalls = 0;
      const jsonCodec = kafka.json(() => {
        jsonFactoryCalls += 1;
        return Schema.toCodecJson(
          Schema.Struct({
            price: Schema.Number,
          }),
        );
      });
      const jsonResult = yield* decodeKafkaCodec(jsonCodec, {
        bytes: encoder.encode('{"price":42}'),
        metadata,
      });
      const secondJsonResult = yield* decodeKafkaCodec(jsonCodec, {
        bytes: encoder.encode('{"price":43}'),
        metadata,
      });
      const protobufResult = yield* decodeKafkaCodec(kafka.protobuf(contractMessage), {
        bytes: toBinary(contractMessage, create(contractMessage, { label: "decoded" })),
        metadata,
      });
      const customResult = yield* decodeKafkaCodec(
        kafka.codec({
          name: "length",
          decode: ({ bytes }) =>
            Effect.succeed({
              length: bytes.byteLength,
            }),
        }),
        {
          bytes,
          metadata,
        },
      );
      const synchronousCustomFailure = yield* decodeKafkaCodec(
        kafka.codec({
          name: "throwing",
          decode: () => {
            throw new Error("private decoder failure");
          },
        }),
        {
          bytes,
          metadata,
        },
      ).pipe(Effect.flip);

      expect(bytesResult).toBe(bytes);
      expect(stringResult).toBe("hello");
      expect(jsonResult).toStrictEqual({ price: 42 });
      expect(secondJsonResult).toStrictEqual({ price: 43 });
      expect(jsonFactoryCalls).toBe(1);
      expect(protobufResult.label).toBe("decoded");
      expect(customResult).toStrictEqual({ length: 3 });
      expect(synchronousCustomFailure).toStrictEqual({
        _tag: "KafkaCodecError",
        message: "Kafka custom codec threw synchronously.",
      });
    }),
  );

  it.effect("snapshots custom decoders before caller-owned definitions can mutate", () =>
    Effect.gen(function* () {
      const definition = {
        name: "snapshot",
        decode: () => Effect.succeed("before"),
      };
      const codec = kafka.codec(definition);
      definition.decode = () => Effect.succeed("after");

      const decoded = yield* decodeKafkaCodec(codec, {
        bytes: encoder.encode("ignored"),
        metadata,
      });

      expect(decoded).toBe("before");
    }),
  );

  it.effect("captures an immutable protobuf descriptor before decoding", () =>
    Effect.gen(function* () {
      const callerDescriptor = makeContractMessage();
      callerDescriptor.file.dependencies.push(callerDescriptor.file);
      const codec = kafka.protobuf(callerDescriptor);
      const payload = toBinary(contractMessage, create(contractMessage, { label: "captured" }));
      callerDescriptor.fields.splice(0);
      const decoded = yield* decodeKafkaCodec(codec, {
        bytes: payload,
        metadata,
      });

      expect({
        callerFields: callerDescriptor.fields,
        capturedIsIndependent: codec.descriptor === callerDescriptor,
        descriptorFrozen: Object.isFrozen(codec.descriptor),
        fieldsFrozen: Object.isFrozen(codec.descriptor.fields),
        mutationAccepted: Reflect.deleteProperty(codec.descriptor.fields, "0"),
        decoded,
      }).toStrictEqual({
        callerFields: [],
        capturedIsIndependent: false,
        descriptorFrozen: true,
        fieldsFrozen: true,
        mutationAccepted: false,
        decoded: {
          $typeName: "kafka.contract.Value",
          label: "captured",
        },
      });
    }),
  );

  it("rejects malformed and hostile protobuf descriptors during construction", () => {
    expect(() => Reflect.apply(kafka.protobuf, undefined, [{}])).toThrowError(
      "Kafka protobuf codec requires a valid message descriptor.",
    );
    const hostileDescriptor = new Proxy(
      {},
      {
        get: () => {
          throw new Error("hostile descriptor");
        },
      },
    );
    expect(() => Reflect.apply(kafka.protobuf, undefined, [hostileDescriptor])).toThrowError(
      "Kafka protobuf codec requires a valid message descriptor.",
    );
    const missingDescriptor = Object.create(contractMessage);
    Object.defineProperty(missingDescriptor, "typeName", {
      enumerable: true,
      value: "kafka.contract.Missing",
    });
    expect(() => Reflect.apply(kafka.protobuf, undefined, [missingDescriptor])).toThrowError(
      "Kafka protobuf codec requires a valid message descriptor.",
    );
  });

  it("accepts nominal protobuf codecs in Source Definitions", () => {
    const source = kafka.source({
      topic: "protobuf-orders",
      regions: ["local"],
      key: kafka.protobuf(contractMessage),
      value: kafka.protobuf(contractMessage),
      localRowKey: ({ key }) => key.label,
      map: ({ value }) => ({ label: value.label }),
      startFrom: "earliest",
    });

    expect(source.options.key.format).toBe("protobuf");
    expect(source.options.value.format).toBe("protobuf");
  });

  it.effect("reports exact safe codec failures", () =>
    Effect.gen(function* () {
      const invalidJson = yield* decodeKafkaCodec(
        kafka.json(() =>
          Schema.toCodecJson(
            Schema.Struct({
              price: Schema.Number,
            }),
          ),
        ),
        {
          bytes: encoder.encode("{"),
          metadata,
        },
      ).pipe(Effect.flip);
      const invalidUtf8 = yield* decodeKafkaCodec(
        kafka.json(() =>
          Schema.toCodecJson(
            Schema.Struct({
              price: Schema.String,
            }),
          ),
        ),
        {
          bytes: Uint8Array.from([...encoder.encode('{"price":"'), 0xff, ...encoder.encode('"}')]),
          metadata,
        },
      ).pipe(Effect.flip);
      const invalidShape = yield* decodeKafkaCodec(
        kafka.json(() =>
          Schema.toCodecJson(
            Schema.Struct({
              price: Schema.Number,
            }),
          ),
        ),
        {
          bytes: encoder.encode('{"price":"wrong"}'),
          metadata,
        },
      ).pipe(Effect.flip);
      const invalidProtobuf = yield* decodeKafkaCodec(kafka.protobuf(contractMessage), {
        bytes: Uint8Array.from([255]),
        metadata,
      }).pipe(Effect.flip);

      expect(invalidJson).toStrictEqual({
        _tag: "KafkaCodecError",
        message: "Kafka JSON payload is not valid JSON.",
      });
      expect(invalidUtf8).toStrictEqual({
        _tag: "KafkaCodecError",
        message: "Kafka JSON payload is not valid JSON.",
      });
      expect(invalidShape).toStrictEqual({
        _tag: "KafkaCodecError",
        message: "Kafka JSON payload does not satisfy its Schema.",
      });
      expect(invalidProtobuf).toStrictEqual({
        _tag: "KafkaCodecError",
        message: "Kafka protobuf payload could not be decoded.",
      });
    }),
  );

  it.effect("retains a synchronous JSON codec factory failure in the typed decode channel", () =>
    Effect.gen(function* () {
      let factoryCalls = 0;
      const codec = kafka.json((): Schema.toCodecJson<typeof jsonFactoryFailureRow> => {
        factoryCalls += 1;
        throw new Error("invalid JSON Schema factory");
      });

      const first = yield* decodeKafkaCodec(codec, {
        bytes: encoder.encode('"ignored"'),
        metadata,
      }).pipe(Effect.flip);
      const second = yield* decodeKafkaCodec(codec, {
        bytes: encoder.encode('"ignored again"'),
        metadata,
      }).pipe(Effect.flip);

      expect(factoryCalls).toBe(1);
      expect(first).toStrictEqual({
        _tag: "KafkaCodecError",
        message: "Kafka JSON schema is not JSON-compatible",
      });
      expect(second).toStrictEqual({
        _tag: "KafkaCodecError",
        message: "Kafka JSON schema is not JSON-compatible",
      });
    }),
  );

  it("recognizes only nominal codecs and validates custom definitions", () => {
    const codec = kafka.string();
    const custom = kafka.codec({
      name: "custom",
      decode: () => Effect.succeed("decoded"),
    });
    const codecBrand = Option.getOrThrow(
      Option.fromUndefinedOr(
        Reflect.ownKeys(codec).find(
          (key) =>
            typeof key === "symbol" && key.description === "@effect-view-server/kafka/KafkaCodec",
        ),
      ),
    );
    const codecDecode = Option.getOrThrow(
      Option.fromUndefinedOr(
        Reflect.ownKeys(codec).find(
          (key) =>
            typeof key === "symbol" &&
            key.description === "@effect-view-server/kafka/KafkaCodecDecode",
        ),
      ),
    );
    const throwingBrand = {
      [codecBrand]: () => {
        throw new Error("hostile brand");
      },
      [codecDecode]: () => Effect.void,
      format: "hostile",
    };
    const hostileBrand = Object.create(null);
    Object.defineProperty(hostileBrand, Symbol(), {
      enumerable: true,
      get() {
        throw new Error("hostile");
      },
    });
    const hostileCustomDefinition = new Proxy(
      {},
      {
        ownKeys() {
          throw new Error("hostile custom codec");
        },
      },
    );
    let accessorReads = 0;
    const accessorDefinition = Object.create(null);
    Object.defineProperties(accessorDefinition, {
      name: {
        enumerable: true,
        get() {
          accessorReads += 1;
          return "accessor";
        },
      },
      decode: {
        enumerable: true,
        get() {
          accessorReads += 1;
          return () => Effect.succeed("decoded");
        },
      },
    });

    expect(isKafkaCodec(codec)).toBe(true);
    expect(isKafkaCodec(custom)).toBe(true);
    expect(isKafkaCodec(null)).toBe(false);
    expect(isKafkaCodec({ format: "string" })).toBe(false);
    expect(isKafkaCodec(hostileBrand)).toBe(false);
    expect(isKafkaCodec(throwingBrand)).toBe(false);
    expect(() => Reflect.apply(kafka.codec, undefined, [null])).toThrow(
      KafkaSourceConfigurationError,
    );
    expect(() => Reflect.apply(kafka.codec, undefined, [42])).toThrow(
      KafkaSourceConfigurationError,
    );
    expect(() => Reflect.apply(kafka.codec, undefined, [hostileCustomDefinition])).toThrow(
      KafkaSourceConfigurationError,
    );
    expect(() =>
      Reflect.apply(kafka.codec, undefined, [
        {
          name: "",
          decode: () => Effect.void,
        },
      ]),
    ).toThrow(KafkaSourceConfigurationError);
    expect(() => Reflect.apply(kafka.codec, undefined, [accessorDefinition])).toThrow(
      KafkaSourceConfigurationError,
    );
    expect(accessorReads).toBe(0);
    expect(() => Reflect.apply(kafka.json, undefined, [() => ({})])).toThrowError(
      "Kafka JSON codec requires a factory returning a JSON-compatible Schema.",
    );
    expect(() => Reflect.apply(kafka.json, undefined, [42])).toThrowError(
      "Kafka JSON codec requires a factory returning a JSON-compatible Schema.",
    );
    expect(() => Reflect.apply(kafka.json, undefined, [() => Schema.String])).toThrowError(
      "Kafka JSON codec requires a factory returning a JSON-compatible Schema.",
    );
    expect(() =>
      Reflect.apply(kafka.json, undefined, [() => Schema.toCodecJson(Schema.String)]),
    ).toThrowError("Kafka JSON codec requires a factory returning a JSON-compatible Schema.");
    const invalidCanonicalWitness = Object.create(Schema.String);
    Object.defineProperty(invalidCanonicalWitness, "schema", {
      value: {},
    });
    expect(() =>
      Reflect.apply(kafka.json, undefined, [() => invalidCanonicalWitness]),
    ).toThrowError("Kafka JSON codec requires a factory returning a JSON-compatible Schema.");
    const hostileCanonicalWitness = new Proxy(
      {},
      {
        has() {
          throw new Error("hostile canonical witness");
        },
      },
    );
    expect(() =>
      Reflect.apply(kafka.json, undefined, [() => hostileCanonicalWitness]),
    ).toThrowError("Kafka JSON codec requires a factory returning a JSON-compatible Schema.");
    const invalidRowFields = Object.create(Schema.String);
    Object.defineProperty(invalidRowFields, "fields", {
      value: { value: {} },
    });
    const invalidRowCodec = Object.create(Schema.toCodecJson(Schema.String));
    Object.defineProperty(invalidRowCodec, "schema", {
      value: invalidRowFields,
    });
    expect(() => Reflect.apply(kafka.json, undefined, [() => invalidRowCodec])).toThrowError(
      "Kafka JSON codec requires a factory returning a JSON-compatible Schema.",
    );
    const symbolicRowFields = Object.create(Schema.String);
    Object.defineProperty(symbolicRowFields, "fields", {
      value: { [Symbol("field")]: Schema.String },
    });
    const symbolicRowCodec = Object.create(Schema.toCodecJson(Schema.String));
    Object.defineProperty(symbolicRowCodec, "schema", {
      value: symbolicRowFields,
    });
    expect(() => Reflect.apply(kafka.json, undefined, [() => symbolicRowCodec])).toThrowError(
      "Kafka JSON codec requires a factory returning a JSON-compatible Schema.",
    );
    const hostileRowFields = Object.create(Schema.String);
    Object.defineProperty(hostileRowFields, "fields", {
      value: new Proxy(
        {},
        {
          ownKeys() {
            throw new Error("hostile row fields");
          },
        },
      ),
    });
    const hostileRowCodec = Object.create(Schema.toCodecJson(Schema.String));
    Object.defineProperty(hostileRowCodec, "schema", {
      value: hostileRowFields,
    });
    expect(() => Reflect.apply(kafka.json, undefined, [() => hostileRowCodec])).toThrowError(
      "Kafka JSON codec requires a factory returning a JSON-compatible Schema.",
    );
    expect(() =>
      Reflect.apply(kafka.codec, undefined, [
        {
          name: "invalid",
          decode: true,
        },
      ]),
    ).toThrow(KafkaSourceConfigurationError);
    expect(() =>
      Reflect.apply(kafka.codec, undefined, [
        {
          name: "decorated",
          decode: () => Effect.void,
          extra: true,
        },
      ]),
    ).toThrow(KafkaSourceConfigurationError);
  });

  it("constructs and decodes canonical region-qualified row IDs", () => {
    const id = kafkaRowId({
      region: "eu",
      localRowKey: "desk:order:1",
    });

    expect(id).toBe("eu:desk:order:1");
    expect(decodeKafkaRowId(id)).toStrictEqual({
      region: "eu",
      localRowKey: "desk:order:1",
    });
    expect(() => kafkaRowId({ region: "bad:region", localRowKey: "a" })).toThrow(
      KafkaSourceConfigurationError,
    );
    expect(() => kafkaRowId({ region: "eu", localRowKey: "" })).toThrow(
      KafkaSourceConfigurationError,
    );
    expect(() =>
      Reflect.apply(kafkaRowId, undefined, [{ region: "eu", localRowKey: "a", extra: true }]),
    ).toThrow(KafkaSourceConfigurationError);
    expect(() => decodeKafkaRowId("missing")).toThrow(KafkaSourceConfigurationError);
    expect(() => decodeKafkaRowId(":missing")).toThrow(KafkaSourceConfigurationError);
    expect(() => decodeKafkaRowId("missing:")).toThrow(KafkaSourceConfigurationError);
    expect(() => Reflect.apply(decodeKafkaRowId, undefined, [1])).toThrow(
      KafkaSourceConfigurationError,
    );
  });

  it("derives unambiguous uppercase UTF-8 consumer group IDs", () => {
    expect(kafkaConsumerGroupId("my-view-server", "orders")).toBe("my-view-server:orders");
    expect(kafkaConsumerGroupId("replica:1", "orders/é!*")).toBe(
      "replica%3A1:orders%2F%C3%A9%21%2A",
    );
    expect(kafkaConsumerGroupId("a:b", "c")).not.toBe(kafkaConsumerGroupId("a", "b:c"));
    expect(kafkaConsumerGroupId("😀", "orders")).toBe("%F0%9F%98%80:orders");
    expect(() => kafkaConsumerGroupId("\ud800", "orders")).toThrow(KafkaSourceConfigurationError);
    expect(() => kafkaConsumerGroupId("replica", "\udc00")).toThrow(KafkaSourceConfigurationError);
    expect(() => kafkaConsumerGroupId("", "orders")).toThrow(KafkaSourceConfigurationError);
    expect(() => kafkaConsumerGroupId("replica", "")).toThrow(KafkaSourceConfigurationError);
    expect(kafkaConsumerGroupId("a".repeat(32_765), "b")).toHaveLength(32_767);
    expect(() => kafkaConsumerGroupId("a".repeat(32_766), "b")).toThrow(
      "Kafka derived consumer group ID exceeds the 32767-byte Kafka protocol limit.",
    );
    expect(() => kafkaConsumerGroupId("😀".repeat(2_731), "b")).toThrow(
      KafkaSourceConfigurationError,
    );
  });

  it("captures exact start policies and freezes source definitions", () => {
    const earliest = kafka.source(validSourceInput());
    const latest = kafka.source({
      ...validSourceInput(),
      startFrom: "latest",
    });
    const committed = kafka.source({
      ...validSourceInput(),
      startFrom: {
        mode: "committed",
        consumerGroupId: "seed",
        fallback: "earliest",
      },
    });
    const timestamp = kafka.source({
      ...validSourceInput(),
      startFrom: {
        mode: "timestamp",
        atNanos: 1_234_567n,
        fallback: "latest",
      },
    });
    const durationAgo = kafka.source({
      ...validSourceInput(),
      startFrom: {
        mode: "durationAgo",
        duration: "5 minutes",
        fallback: "fail",
      },
    });
    const durationTuple = kafka.source({
      ...validSourceInput(),
      startFrom: {
        mode: "durationAgo",
        duration: [1, 2],
        fallback: "latest",
      },
    });
    const durationObject = kafka.source({
      ...validSourceInput(),
      startFrom: {
        mode: "durationAgo",
        duration: {
          weeks: 1,
          days: 1,
          hours: 1,
          minutes: 1,
          seconds: 1,
          milliseconds: 1,
          microseconds: 1,
          nanoseconds: 1,
        },
        fallback: "earliest",
      },
    });
    const partialDurationObject = kafka.source({
      ...validSourceInput(),
      startFrom: {
        mode: "durationAgo",
        duration: {
          minutes: 1,
        },
        fallback: "earliest",
      },
    });
    const nominalDuration = kafka.source({
      ...validSourceInput(),
      startFrom: {
        mode: "durationAgo",
        duration: Duration.seconds(1),
        fallback: "latest",
      },
    });
    const bigintDuration = kafka.source({
      ...validSourceInput(),
      startFrom: {
        mode: "durationAgo",
        duration: 1_000_000_000n,
        fallback: "latest",
      },
    });

    expect(earliest.options.startFrom).toBe("earliest");
    expect(latest.options.startFrom).toBe("latest");
    expect(committed.options.startFrom).toStrictEqual({
      mode: "committed",
      consumerGroupId: "seed",
      fallback: "earliest",
    });
    expect(timestamp.options.startFrom).toStrictEqual({
      mode: "timestamp",
      atNanos: 1_234_567n,
      fallback: "latest",
    });
    expect(durationAgo.options.startFrom).toStrictEqual({
      mode: "durationAgo",
      durationNanos: 300_000_000_000n,
      fallback: "fail",
    });
    expect([
      durationTuple.options.startFrom,
      durationObject.options.startFrom,
      partialDurationObject.options.startFrom,
      nominalDuration.options.startFrom,
      bigintDuration.options.startFrom,
    ]).toStrictEqual([
      {
        mode: "durationAgo",
        durationNanos: 1_000_000_002n,
        fallback: "latest",
      },
      {
        mode: "durationAgo",
        durationNanos: 694_861_001_001_001n,
        fallback: "earliest",
      },
      {
        mode: "durationAgo",
        durationNanos: 60_000_000_000n,
        fallback: "earliest",
      },
      {
        mode: "durationAgo",
        durationNanos: 1_000_000_000n,
        fallback: "latest",
      },
      {
        mode: "durationAgo",
        durationNanos: 1_000_000_000n,
        fallback: "latest",
      },
    ]);
    expect(Object.isFrozen(durationAgo)).toBe(true);
    expect(Object.isFrozen(durationAgo.options.startFrom)).toBe(true);
  });

  it("captures hostile source, start, Region, and row-ID descriptors exactly once", () => {
    let lateReads = 0;
    const startFrom = new Proxy(
      {
        mode: "timestamp",
        atNanos: 1_234_567n,
        fallback: "latest",
      } as const,
      {
        get: () => {
          lateReads += 1;
          return "invalid";
        },
      },
    );
    const regions = new Proxy(["eu", "us"] as const, {
      get: () => {
        lateReads += 1;
        return "invalid";
      },
    });
    const sourceInput = new Proxy(
      {
        ...validSourceInput(),
        regions,
        startFrom,
      },
      {
        get: () => {
          lateReads += 1;
          return "invalid";
        },
      },
    );
    const source = kafka.source(sourceInput);
    const rowId = kafkaRowId(
      new Proxy(
        {
          region: "eu",
          localRowKey: "order:1",
        },
        {
          get: () => {
            lateReads += 1;
            return "invalid";
          },
        },
      ),
    );

    expect({
      lateReads,
      regions: source.options.regions,
      rowId,
      startFrom: source.options.startFrom,
    }).toStrictEqual({
      lateReads: 0,
      regions: ["eu", "us"],
      rowId: "eu:order:1",
      startFrom: {
        mode: "timestamp",
        atNanos: 1_234_567n,
        fallback: "latest",
      },
    });
  });

  it("normalizes hostile source and start descriptors without invoking accessors", () => {
    let accessorReads = 0;
    const accessorInput = {
      ...validSourceInput(),
    };
    Object.defineProperty(accessorInput, "topic", {
      enumerable: true,
      get: () => {
        accessorReads += 1;
        throw new Error("source getter escaped");
      },
    });
    const hostileSource = new Proxy(validSourceInput(), {
      getOwnPropertyDescriptor: () => {
        throw new Error("source descriptor escaped");
      },
    });
    const hostileStart = new Proxy(
      {
        mode: "timestamp",
        atNanos: 1n,
        fallback: "latest",
      } as const,
      {
        getOwnPropertyDescriptor: () => {
          throw new Error("start descriptor escaped");
        },
      },
    );
    const hostileRowId = new Proxy(
      {
        region: "eu",
        localRowKey: "order:1",
      },
      {
        getOwnPropertyDescriptor: () => {
          throw new Error("row ID descriptor escaped");
        },
      },
    );

    expect(() => kafka.source(accessorInput)).toThrowError(
      "Kafka source requires exactly topic, regions, key, value, localRowKey, map, and startFrom.",
    );
    expect(accessorReads).toBe(0);
    expect(() => kafka.source(hostileSource)).toThrowError(
      "Kafka source requires exactly topic, regions, key, value, localRowKey, map, and startFrom.",
    );
    expect(() =>
      kafka.source({
        ...validSourceInput(),
        startFrom: hostileStart,
      }),
    ).toThrowError(
      "Kafka startFrom must be earliest, latest, committed, timestamp, or durationAgo.",
    );
    expect(() => kafkaRowId(hostileRowId)).toThrowError(
      "Kafka rowId requires exactly region and localRowKey.",
    );
  });

  it("rejects malformed source declarations and start policies", () => {
    const durationInput = (duration: unknown) => ({
      ...validSourceInput(),
      startFrom: {
        mode: "durationAgo",
        duration,
        fallback: "latest",
      },
    });
    const sparseDurationSeconds: Array<number> = [];
    sparseDurationSeconds.length = 2;
    sparseDurationSeconds[1] = 1;
    const sparseDurationNanos = [1];
    sparseDurationNanos.length = 2;
    const accessorDurationSeconds = [1, 2];
    Object.defineProperty(accessorDurationSeconds, "0", {
      enumerable: true,
      get: () => 1,
    });
    const accessorDurationNanos = [1, 2];
    Object.defineProperty(accessorDurationNanos, "1", {
      enumerable: true,
      get: () => 2,
    });
    const hiddenDurationSeconds = [1, 2];
    Object.defineProperty(hiddenDurationSeconds, "0", {
      enumerable: false,
      value: 1,
    });
    const hiddenDurationNanos = [1, 2];
    Object.defineProperty(hiddenDurationNanos, "1", {
      enumerable: false,
      value: 2,
    });
    const hostileDurationTuple = new Proxy([1, 2], {
      getOwnPropertyDescriptor: () => {
        throw new Error("duration descriptor escaped");
      },
    });
    const accessorDurationObject = Object.create(null);
    Object.defineProperty(accessorDurationObject, "minutes", {
      enumerable: true,
      get: () => 1,
    });
    const symbolicDurationObject = {
      [Symbol("minutes")]: 1,
    };
    const sparseRegions: Array<string> = [];
    sparseRegions.length = 1;
    const accessorRegions = ["eu"];
    Object.defineProperty(accessorRegions, "0", {
      enumerable: true,
      get: () => "eu",
    });
    const hiddenRegions = ["eu"];
    Object.defineProperty(hiddenRegions, "0", {
      enumerable: false,
      value: "eu",
    });
    const hostileRegions = new Proxy(["eu"], {
      getOwnPropertyDescriptor: () => {
        throw new Error("Region descriptor escaped");
      },
    });
    const invalidInputs: ReadonlyArray<object> = [
      {},
      { ...validSourceInput(), topic: "" },
      { ...validSourceInput(), regions: {} },
      { ...validSourceInput(), regions: [] },
      { ...validSourceInput(), regions: ["eu", "eu"] },
      { ...validSourceInput(), regions: ["bad:region"] },
      { ...validSourceInput(), regions: sparseRegions },
      { ...validSourceInput(), regions: accessorRegions },
      { ...validSourceInput(), regions: hiddenRegions },
      { ...validSourceInput(), regions: hostileRegions },
      { ...validSourceInput(), key: {} },
      { ...validSourceInput(), value: {} },
      { ...validSourceInput(), localRowKey: true },
      { ...validSourceInput(), map: true },
      { ...validSourceInput(), extra: true },
      { ...validSourceInput(), startFrom: 1 },
      {
        ...validSourceInput(),
        startFrom: {
          mode: "committed",
          consumerGroupId: "",
          fallback: "earliest",
        },
      },
      {
        ...validSourceInput(),
        startFrom: {
          mode: "committed",
          consumerGroupId: "invalid group",
          fallback: "earliest",
        },
      },
      {
        ...validSourceInput(),
        startFrom: {
          mode: "committed",
          consumerGroupId: "a".repeat(32_768),
          fallback: "earliest",
        },
      },
      {
        ...validSourceInput(),
        startFrom: {
          mode: "committed",
          consumerGroupId: "seed",
          fallback: "invalid",
        },
      },
      {
        ...validSourceInput(),
        startFrom: {
          mode: "timestamp",
          atNanos: -1n,
          fallback: "fail",
        },
      },
      {
        ...validSourceInput(),
        startFrom: {
          mode: "durationAgo",
          duration: Number.POSITIVE_INFINITY,
          fallback: "latest",
        },
      },
      {
        ...validSourceInput(),
        startFrom: {
          mode: "durationAgo",
          duration: "1 minute",
          fallback: "latest",
          extra: true,
        },
      },
      durationInput([]),
      durationInput(sparseDurationSeconds),
      durationInput(sparseDurationNanos),
      durationInput(accessorDurationSeconds),
      durationInput(accessorDurationNanos),
      durationInput(hiddenDurationSeconds),
      durationInput(hiddenDurationNanos),
      durationInput(["seconds", 1]),
      durationInput([1, "nanos"]),
      durationInput(hostileDurationTuple),
      durationInput(accessorDurationObject),
      durationInput(symbolicDurationObject),
      durationInput({ unexpected: 1 }),
      durationInput({ minutes: "one" }),
      durationInput("Infinity"),
      durationInput("-Infinity"),
      {
        ...validSourceInput(),
        startFrom: {
          mode: "unsupported",
        },
      },
    ];

    for (const input of invalidInputs) {
      expect(() => Reflect.apply(kafka.source, undefined, [input])).toThrow(
        KafkaSourceConfigurationError,
      );
    }
    expect(() => Reflect.apply(kafka.source, undefined, [validSourceInput(), 123])).toThrowError(
      "Kafka source retry override must be an Effect Schedule.",
    );
    const hostileRetry = new Proxy(
      {},
      {
        has: () => {
          throw new Error("hostile retry");
        },
      },
    );
    expect(() =>
      Reflect.apply(kafka.source, undefined, [validSourceInput(), hostileRetry]),
    ).toThrowError("Kafka source retry override must be an Effect Schedule.");
  });
});
