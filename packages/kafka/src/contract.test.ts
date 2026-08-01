import { create, toBinary } from "@bufbuild/protobuf";
import { fileDesc, messageDesc } from "@bufbuild/protobuf/codegenv2";
import type { Message } from "@bufbuild/protobuf";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { describe, expect, it } from "@effect/vitest";
import { SourceAdapter } from "effect-view-server/source-adapter";
import { Duration, Effect, Option, Schedule, Schema } from "effect";
import {
  KafkaMaterializedMetrics,
  KafkaSourceAdapter,
  KafkaSourceRejectionLocation,
  KafkaSourceConfigurationError,
  decodeKafkaCompactionKeyCodec,
  decodeKafkaCompactionRowId,
  decodeKafkaCodec,
  decodeKafkaDeleteRowId,
  decodeKafkaRowId,
  isKafkaCompactionKeyCodec,
  isKafkaCodec,
  kafka,
  kafkaCompactionRowId,
  kafkaConsumerGroupId,
  kafkaDeleteRowId,
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
  cleanupPolicy: "delete" as const,
  retentionPolicy: "Infinity" as const,
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

const validRegionMetrics = () => ({
  region: "eu",
  assignments: [],
  commits: 0n,
  commitFailures: 0n,
  decoded: 0n,
  decodeFailures: 0n,
  mapped: 0n,
  mappingFailures: 0n,
  rejections: 0n,
  reconnects: 0n,
  rebalances: 0n,
  closes: 0n,
  closeFailures: 0n,
  retention: {
    declaredCleanupPolicy: "delete",
    observedCleanupPolicy: "delete",
    configuredRetention: { _tag: "Forever" },
    resolvedRetention: { _tag: "Forever" },
    trackedRows: 0,
    lastSweepRetryableFailures: 0,
    expiredRows: 0n,
    authoritativeExpiredDeletes: 0n,
    failedWorkBacklog: 0,
    expirationRetryFailures: 0n,
    latestExpirationFailure: null,
    lastSweepAtNanos: null,
    lastSweepDurationNanos: null,
    sweepIntervalNanos: 900_000_000_000n,
  },
});

describe("Kafka Source Adapter contract", () => {
  it("exports a frozen nominal descriptor without generic lifecycle builders", () => {
    const source = kafka.source(validSourceInput());
    const symbolValues = Reflect.ownKeys(KafkaSourceAdapter)
      .filter((key): key is symbol => typeof key === "symbol")
      .map((key) => Reflect.get(KafkaSourceAdapter, key));

    expect(source.adapter).toBe(KafkaSourceAdapter);
    expect(Object.keys(KafkaSourceAdapter)).toStrictEqual([
      "identity",
      "failureSchema",
      "materialized",
      "leased",
      "runtimeService",
      "failure",
    ]);
    expect(Reflect.get(KafkaSourceAdapter, "materializedSource")).toBeUndefined();
    expect(Reflect.get(KafkaSourceAdapter, "leasedSource")).toBeUndefined();
    expect(symbolValues).toHaveLength(1);
    expect(typeof symbolValues[0]).toBe("symbol");
    expect(Object.isFrozen(KafkaSourceAdapter)).toBe(true);
    expect(() =>
      Reflect.apply(SourceAdapter.materializedSource, undefined, [
        KafkaSourceAdapter,
        validSourceInput(),
      ]),
    ).toThrow("delegated construction requires a nominal Source Adapter handle");
  });

  it.effect("requires at least one Region metric in materialized health", () =>
    Effect.gen(function* () {
      const region = validRegionMetrics();
      const input = {
        activeGroupId: "replica:orders",
        start: { _tag: "Pending" },
        regions: [region],
      };
      const valid = yield* Schema.decodeUnknownEffect(KafkaMaterializedMetrics)(input);
      const empty = yield* Schema.decodeUnknownEffect(KafkaMaterializedMetrics)({
        ...input,
        regions: [],
      }).pipe(Effect.flip);

      expect(valid).toStrictEqual(input);
      expect(Schema.isSchemaError(empty)).toBe(true);
    }),
  );

  it.effect.each([
    {
      label: "negative partitions",
      region: {
        ...validRegionMetrics(),
        assignments: [{ partition: -1, offset: 0n, lag: 0n }],
      },
    },
    {
      label: "partitions beyond Kafka's signed 32-bit bound",
      region: {
        ...validRegionMetrics(),
        assignments: [{ partition: 2_147_483_648, offset: 0n, lag: 0n }],
      },
    },
    {
      label: "negative offsets",
      region: {
        ...validRegionMetrics(),
        assignments: [{ partition: 0, offset: -1n, lag: 0n }],
      },
    },
    {
      label: "negative lag",
      region: {
        ...validRegionMetrics(),
        assignments: [{ partition: 0, offset: 0n, lag: -1n }],
      },
    },
    {
      label: "negative commits",
      region: {
        ...validRegionMetrics(),
        commits: -1n,
      },
    },
    {
      label: "negative commit failures",
      region: {
        ...validRegionMetrics(),
        commitFailures: -1n,
      },
    },
    {
      label: "negative decoded count",
      region: {
        ...validRegionMetrics(),
        decoded: -1n,
      },
    },
    {
      label: "negative decode failures",
      region: {
        ...validRegionMetrics(),
        decodeFailures: -1n,
      },
    },
    {
      label: "negative mapped count",
      region: {
        ...validRegionMetrics(),
        mapped: -1n,
      },
    },
    {
      label: "negative mapping failures",
      region: {
        ...validRegionMetrics(),
        mappingFailures: -1n,
      },
    },
    {
      label: "negative rejections",
      region: {
        ...validRegionMetrics(),
        rejections: -1n,
      },
    },
    {
      label: "negative reconnects",
      region: {
        ...validRegionMetrics(),
        reconnects: -1n,
      },
    },
    {
      label: "negative rebalances",
      region: {
        ...validRegionMetrics(),
        rebalances: -1n,
      },
    },
    {
      label: "negative closes",
      region: {
        ...validRegionMetrics(),
        closes: -1n,
      },
    },
    {
      label: "negative close failures",
      region: {
        ...validRegionMetrics(),
        closeFailures: -1n,
      },
    },
  ])("rejects $label in materialized health", ({ region }) =>
    Schema.decodeUnknownEffect(KafkaMaterializedMetrics)({
      activeGroupId: "replica:orders",
      start: { _tag: "Pending" },
      regions: [region],
    }).pipe(
      Effect.flip,
      Effect.map((failure) => {
        expect(Schema.isSchemaError(failure)).toBe(true);
      }),
    ),
  );

  it.effect("enforces non-negative Kafka rejection coordinates", () =>
    Effect.gen(function* () {
      const location = {
        region: "eu",
        topic: "orders-source",
        partition: 0,
        offset: 0n,
        phase: "keyDecode",
        message: "rejected",
      } as const;
      const valid = yield* Schema.decodeUnknownEffect(KafkaSourceRejectionLocation)(location);
      const negativePartition = yield* Schema.decodeUnknownEffect(KafkaSourceRejectionLocation)({
        ...location,
        partition: -1,
      }).pipe(Effect.flip);
      const negativeOffset = yield* Schema.decodeUnknownEffect(KafkaSourceRejectionLocation)({
        ...location,
        offset: -1n,
      }).pipe(Effect.flip);

      expect(valid).toStrictEqual(location);
      expect([
        Schema.isSchemaError(negativePartition),
        Schema.isSchemaError(negativeOffset),
      ]).toStrictEqual([true, true]);
    }),
  );

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
      cleanupPolicy: "delete",
      retentionPolicy: "Infinity",
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

  it.effect("decodes every metadata-free compaction key codec with exact safe failures", () =>
    Effect.gen(function* () {
      const sourceBytes = Uint8Array.from([1, 2, 3]);
      const bytesCodec = kafka.compactionKey.bytes();
      const decodedBytes = yield* decodeKafkaCompactionKeyCodec(bytesCodec, {
        bytes: sourceBytes,
      });
      sourceBytes[0] = 9;
      const decodedString = yield* decodeKafkaCompactionKeyCodec(kafka.compactionKey.string(), {
        bytes: encoder.encode("desk"),
      });
      const jsonCodec = kafka.compactionKey.json(() =>
        Schema.toCodecJson(
          Schema.Struct({
            tenant: Schema.String,
          }),
        ),
      );
      const decodedJson = yield* decodeKafkaCompactionKeyCodec(jsonCodec, {
        bytes: encoder.encode('{"tenant":"north"}'),
      });
      const invalidJson = yield* decodeKafkaCompactionKeyCodec(jsonCodec, {
        bytes: encoder.encode("{"),
      }).pipe(Effect.flip);
      const invalidJsonShape = yield* decodeKafkaCompactionKeyCodec(jsonCodec, {
        bytes: encoder.encode('{"tenant":1}'),
      }).pipe(Effect.flip);
      const protobufCodec = kafka.compactionKey.protobuf(contractMessage);
      const decodedProtobuf = yield* decodeKafkaCompactionKeyCodec(protobufCodec, {
        bytes: toBinary(contractMessage, create(contractMessage, { label: "north" })),
      });
      const invalidProtobuf = yield* decodeKafkaCompactionKeyCodec(protobufCodec, {
        bytes: Uint8Array.from([255]),
      }).pipe(Effect.flip);
      const customFailure = {
        _tag: "CustomFailure",
        message: "no key",
      } as const;
      const custom = kafka.compactionKey.codec({
        name: "tenant-key",
        decode: ({ bytes }) =>
          bytes.length === 0 ? Effect.fail(customFailure) : Effect.succeed(bytes[0]),
      });
      const customValue = yield* decodeKafkaCompactionKeyCodec(custom, {
        bytes: Uint8Array.from([7]),
      });
      const customTypedFailure = yield* decodeKafkaCompactionKeyCodec(custom, {
        bytes: Uint8Array.from([]),
      }).pipe(Effect.flip);
      const throwingCustom = kafka.compactionKey.codec({
        name: "throwing",
        decode: (_input) => {
          throw new Error("escaped");
        },
      });
      const synchronousFailure = yield* decodeKafkaCompactionKeyCodec(throwingCustom, {
        bytes: Uint8Array.from([1]),
      }).pipe(Effect.flip);

      expect({
        bytes: [...decodedBytes],
        customTypedFailure,
        customValue,
        decodedJson,
        decodedProtobuf: decodedProtobuf.label,
        decodedString,
        invalidJson,
        invalidJsonShape,
        invalidProtobuf,
        synchronousFailure,
      }).toStrictEqual({
        bytes: [1, 2, 3],
        customTypedFailure,
        customValue: 7,
        decodedJson: { tenant: "north" },
        decodedProtobuf: "north",
        decodedString: "desk",
        invalidJson: {
          _tag: "KafkaCodecError",
          message: "Kafka compaction key JSON payload is not valid JSON.",
        },
        invalidJsonShape: {
          _tag: "KafkaCodecError",
          message: "Kafka compaction key JSON payload does not satisfy its Schema.",
        },
        invalidProtobuf: {
          _tag: "KafkaCodecError",
          message: "Kafka protobuf payload could not be decoded.",
        },
        synchronousFailure: {
          _tag: "KafkaCodecError",
          message: "Kafka compaction key custom codec threw synchronously.",
        },
      });
    }),
  );

  it("recognizes and validates only nominal metadata-free compaction key codecs", () => {
    const codec = kafka.compactionKey.string();
    const brand = Option.getOrThrow(
      Option.fromUndefinedOr(
        Reflect.ownKeys(codec).find(
          (key) =>
            typeof key === "symbol" &&
            key.description === "@effect-view-server/kafka/KafkaCompactionKeyCodec",
        ),
      ),
    );
    const decode = Option.getOrThrow(
      Option.fromUndefinedOr(
        Reflect.ownKeys(codec).find(
          (key) =>
            typeof key === "symbol" &&
            key.description === "@effect-view-server/kafka/KafkaCompactionKeyCodecDecode",
        ),
      ),
    );
    const throwingBrand = {
      [brand]: () => {
        throw new Error("hostile");
      },
      [decode]: () => Effect.void,
    };
    const throwingLookup = new Proxy(
      {},
      {
        get() {
          throw new Error("hostile");
        },
      },
    );

    expect(isKafkaCompactionKeyCodec(codec)).toBe(true);
    expect(isKafkaCompactionKeyCodec(kafka.compactionKey.protobuf(contractMessage))).toBe(true);
    expect(
      isKafkaCompactionKeyCodec(
        kafka.compactionKey.codec({
          name: "custom",
          decode: (_input) => Effect.succeed("decoded"),
        }),
      ),
    ).toBe(true);
    expect(isKafkaCompactionKeyCodec(null)).toBe(false);
    expect(isKafkaCompactionKeyCodec({})).toBe(false);
    expect(isKafkaCompactionKeyCodec(throwingBrand)).toBe(false);
    expect(isKafkaCompactionKeyCodec(throwingLookup)).toBe(false);
    expect(() => Reflect.apply(kafka.compactionKey.json, undefined, [42])).toThrow(
      KafkaSourceConfigurationError,
    );
    expect(() =>
      Reflect.apply(kafka.compactionKey.json, undefined, [
        () => {
          throw new Error("hostile");
        },
      ]),
    ).toThrow(KafkaSourceConfigurationError);
    expect(() => Reflect.apply(kafka.compactionKey.json, undefined, [() => Schema.String])).toThrow(
      KafkaSourceConfigurationError,
    );
    expect(() => Reflect.apply(kafka.compactionKey.protobuf, undefined, [{}])).toThrow(
      KafkaSourceConfigurationError,
    );
    expect(() => Reflect.apply(kafka.compactionKey.codec, undefined, [null])).toThrow(
      KafkaSourceConfigurationError,
    );
    expect(() =>
      Reflect.apply(kafka.compactionKey.codec, undefined, [
        {
          name: "",
          decode: () => Effect.void,
        },
      ]),
    ).toThrow(KafkaSourceConfigurationError);
    expect(() =>
      Reflect.apply(kafka.compactionKey.codec, undefined, [
        {
          name: "decorated",
          decode: () => Effect.void,
          extra: true,
        },
      ]),
    ).toThrow(KafkaSourceConfigurationError);
  });

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
      cleanupPolicy: "delete",
      region: "eu",
      partition: 2,
      localRowKey: "desk:order:1",
    });

    expect(id).toBe("eu:2:desk:order:1");
    expect(decodeKafkaRowId(id, "delete")).toStrictEqual({
      _tag: "Delete",
      region: "eu",
      partition: 2,
      localRowKey: "desk:order:1",
    });
    expect(() =>
      kafkaRowId({
        cleanupPolicy: "delete",
        region: "bad:region",
        partition: 0,
        localRowKey: "a",
      }),
    ).toThrow(KafkaSourceConfigurationError);
    expect(() =>
      kafkaRowId({
        cleanupPolicy: "delete",
        region: "eu",
        partition: 0,
        localRowKey: "",
      }),
    ).toThrow(KafkaSourceConfigurationError);
    expect(() =>
      Reflect.apply(kafkaRowId, undefined, [
        {
          cleanupPolicy: "delete",
          region: "eu",
          partition: 0,
          localRowKey: "a",
          extra: true,
        },
      ]),
    ).toThrow(KafkaSourceConfigurationError);
    expect(() => decodeKafkaRowId("missing", "delete")).toThrow(KafkaSourceConfigurationError);
    expect(() => decodeKafkaRowId(":missing", "delete")).toThrow(KafkaSourceConfigurationError);
    expect(() => decodeKafkaRowId("missing:", "delete")).toThrow(KafkaSourceConfigurationError);
    expect(() => Reflect.apply(decodeKafkaRowId, undefined, [1, "delete"])).toThrow(
      KafkaSourceConfigurationError,
    );
    expect(() => Reflect.apply(decodeKafkaRowId, undefined, [id, "archive"])).toThrow(
      "Kafka rowId cleanupPolicy must be delete, compact, or compact-and-delete.",
    );
  });

  it("constructs and rejects exact delete and compaction row-ID forms", () => {
    const deleteId = kafkaDeleteRowId({
      region: "eu",
      partition: 2,
      localRowKey: "desk:1",
    });
    const compactIds = [
      Uint8Array.from([]),
      Uint8Array.from([1]),
      Uint8Array.from([1, 2]),
      Uint8Array.from([1, 2, 3]),
      Uint8Array.from([1, 2, 3, 4]),
    ].map((serializedKeyBytes) =>
      kafkaCompactionRowId({
        region: "eu",
        partition: 2,
        serializedKeyBytes,
      }),
    );

    expect(deleteId).toBe("eu:2:desk:1");
    expect(decodeKafkaDeleteRowId(deleteId)).toStrictEqual({
      _tag: "Delete",
      region: "eu",
      partition: 2,
      localRowKey: "desk:1",
    });
    expect(
      compactIds.map((id) => {
        const decoded = decodeKafkaCompactionRowId(id);
        return {
          id,
          key: [...decoded.serializedKeyBytes],
          partition: decoded.partition,
          region: decoded.region,
          tag: decoded._tag,
        };
      }),
    ).toStrictEqual([
      { id: "eu:2:k", key: [], partition: 2, region: "eu", tag: "Compaction" },
      { id: "eu:2:kAQ", key: [1], partition: 2, region: "eu", tag: "Compaction" },
      { id: "eu:2:kAQI", key: [1, 2], partition: 2, region: "eu", tag: "Compaction" },
      { id: "eu:2:kAQID", key: [1, 2, 3], partition: 2, region: "eu", tag: "Compaction" },
      { id: "eu:2:kAQIDBA", key: [1, 2, 3, 4], partition: 2, region: "eu", tag: "Compaction" },
    ]);
    expect(
      kafkaRowId({
        cleanupPolicy: "compact-and-delete",
        region: "us",
        partition: 3,
        serializedKeyBytes: Uint8Array.from([255]),
      }),
    ).toBe("us:3:k_w");
    expect(decodeKafkaRowId("us:3:k_w", "compact-and-delete")).toStrictEqual({
      _tag: "Compaction",
      region: "us",
      partition: 3,
      serializedKeyBytes: Uint8Array.from([255]),
    });
    const collidingId = kafkaDeleteRowId({
      region: "eu",
      partition: 0,
      localRowKey: "kYWJj",
    });
    expect({
      compact: decodeKafkaRowId(collidingId, "compact"),
      delete: decodeKafkaRowId(collidingId, "delete"),
    }).toStrictEqual({
      compact: {
        _tag: "Compaction",
        region: "eu",
        partition: 0,
        serializedKeyBytes: Uint8Array.from([97, 98, 99]),
      },
      delete: {
        _tag: "Delete",
        region: "eu",
        partition: 0,
        localRowKey: "kYWJj",
      },
    });

    const invalidDeleteInputs: ReadonlyArray<unknown> = [
      null,
      { region: "eu", partition: 0, localRowKey: "a", extra: true },
      { region: "", partition: 0, localRowKey: "a" },
      { region: "eu", partition: -1, localRowKey: "a" },
      { region: "eu", partition: 0, localRowKey: "" },
    ];
    for (const input of invalidDeleteInputs) {
      expect(() => Reflect.apply(kafkaDeleteRowId, undefined, [input])).toThrow(
        KafkaSourceConfigurationError,
      );
    }
    const invalidCompactInputs: ReadonlyArray<unknown> = [
      null,
      { region: "eu", partition: 0, serializedKeyBytes: Uint8Array.from([]), extra: true },
      { region: "", partition: 0, serializedKeyBytes: Uint8Array.from([]) },
      { region: "eu", partition: -1, serializedKeyBytes: Uint8Array.from([]) },
      { region: "eu", partition: 0, serializedKeyBytes: [] },
    ];
    for (const input of invalidCompactInputs) {
      expect(() => Reflect.apply(kafkaCompactionRowId, undefined, [input])).toThrow(
        KafkaSourceConfigurationError,
      );
    }
    for (const id of [
      1,
      "",
      "eu:2",
      "eu::kAA",
      "eu:-1:kAA",
      "eu:01:kAA",
      "eu:+1:kAA",
      "eu:1e0:kAA",
      "eu: 1:kAA",
      "eu:-0:kAA",
      "eu:0:kA",
      "eu:0:k$",
      "eu:0:kAB",
    ]) {
      expect(() => Reflect.apply(decodeKafkaCompactionRowId, undefined, [id])).toThrow(
        KafkaSourceConfigurationError,
      );
    }
    for (const id of [
      1,
      "",
      "eu:2",
      "eu::a",
      "eu:-1:a",
      "eu:01:a",
      "eu:+1:a",
      "eu:1e0:a",
      "eu: 1:a",
      "eu:-0:a",
    ]) {
      expect(() => Reflect.apply(decodeKafkaDeleteRowId, undefined, [id])).toThrow(
        KafkaSourceConfigurationError,
      );
    }
    for (const input of [
      null,
      { cleanupPolicy: "delete", region: "eu", partition: -1, localRowKey: "a" },
      { cleanupPolicy: "delete", region: "eu", partition: 0, localRowKey: "" },
      {
        cleanupPolicy: "compact",
        region: "eu",
        partition: 0,
        serializedKeyBytes: [],
      },
      {
        cleanupPolicy: "compact",
        region: "eu",
        partition: 0,
        serializedKeyBytes: Uint8Array.from([]),
        extra: true,
      },
      {
        cleanupPolicy: "compact",
        region: "",
        partition: 0,
        serializedKeyBytes: Uint8Array.from([]),
      },
      { cleanupPolicy: "unknown" },
    ]) {
      expect(() => Reflect.apply(kafkaRowId, undefined, [input])).toThrow(
        KafkaSourceConfigurationError,
      );
    }
  });

  it("captures mandatory cleanup and retention policies for every source shape", () => {
    const compactionInput = {
      cleanupPolicy: "compact" as const,
      retentionPolicy: "match-kafka-retention" as const,
      topic: "compacted-orders",
      regions: ["eu"] as const,
      key: kafka.compactionKey.string(),
      value: kafka.string(),
      map: ({ key, value }: { readonly key: string; readonly value: string }) => ({
        key,
        value,
      }),
      startFrom: "earliest" as const,
    };
    const matched = kafka.source(compactionInput);
    const finiteMillis = kafka.source({
      ...validSourceInput(),
      retentionPolicy: Duration.millis(1),
    });
    const finiteNanos = kafka.source({
      ...validSourceInput(),
      retentionPolicy: Duration.nanos(1n),
    });
    const fractionalMillis = kafka.source({
      ...validSourceInput(),
      retentionPolicy: Duration.millis(1.000_001),
    });
    const halfMillis = kafka.source({
      ...validSourceInput(),
      retentionPolicy: Duration.millis(0.5),
    });
    const maximumSafeMillis = kafka.source({
      ...validSourceInput(),
      retentionPolicy: Duration.millis(Number.MAX_SAFE_INTEGER),
    });
    const maximumFiniteMillis = kafka.source({
      ...validSourceInput(),
      retentionPolicy: Duration.millis(Number.MAX_VALUE),
    });
    const forever = kafka.source(validSourceInput());
    const retriedDelete = kafka.source(validSourceInput(), Schedule.recurs(1));
    const retried = kafka.source(
      {
        ...compactionInput,
        cleanupPolicy: "compact-and-delete",
      },
      Schedule.recurs(1),
    );

    expect({
      finiteMillis: finiteMillis.options.retentionPolicy,
      finiteNanos: finiteNanos.options.retentionPolicy,
      fractionalMillis: fractionalMillis.options.retentionPolicy,
      halfMillis: halfMillis.options.retentionPolicy,
      maximumSafeMillis: maximumSafeMillis.options.retentionPolicy,
      maximumFiniteMillis: maximumFiniteMillis.options.retentionPolicy,
      forever: forever.options.retentionPolicy,
      matched: matched.options.retentionPolicy,
      retriedDeleteCleanup: retriedDelete.options.cleanupPolicy,
      retriedCleanup: retried.options.cleanupPolicy,
    }).toStrictEqual({
      finiteMillis: { _tag: "Finite", durationNanos: 1_000_000n },
      finiteNanos: { _tag: "Finite", durationNanos: 1n },
      fractionalMillis: { _tag: "Finite", durationNanos: 1_000_001n },
      halfMillis: { _tag: "Finite", durationNanos: 500_000n },
      maximumSafeMillis: {
        _tag: "Finite",
        durationNanos: BigInt(Number.MAX_SAFE_INTEGER) * 1_000_000n,
      },
      maximumFiniteMillis: {
        _tag: "Finite",
        durationNanos: BigInt(Number.MAX_VALUE) * 1_000_000n,
      },
      forever: { _tag: "Forever" },
      matched: { _tag: "MatchKafkaRetention" },
      retriedDeleteCleanup: "delete",
      retriedCleanup: "compact-and-delete",
    });

    for (const retentionPolicy of [null, {}, -1, -1n, 0, 0n, "-Infinity"]) {
      expect(() =>
        Reflect.apply(kafka.source, undefined, [
          {
            ...validSourceInput(),
            retentionPolicy,
          },
        ]),
      ).toThrow(KafkaSourceConfigurationError);
    }
    expect(() =>
      kafka.source({
        ...validSourceInput(),
        retentionPolicy: Duration.millis(0.000_000_1),
      }),
    ).toThrow(KafkaSourceConfigurationError);
    expect(() =>
      Reflect.apply(kafka.source, undefined, [
        {
          ...compactionInput,
          cleanupPolicy: "invalid",
        },
      ]),
    ).toThrow(KafkaSourceConfigurationError);
    expect(() =>
      Reflect.apply(kafka.source, undefined, [
        {
          ...compactionInput,
          key: kafka.string(),
        },
      ]),
    ).toThrow(KafkaSourceConfigurationError);
    expect(() =>
      Reflect.apply(kafka.source, undefined, [
        {
          ...compactionInput,
          map: true,
        },
      ]),
    ).toThrow(KafkaSourceConfigurationError);
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
          cleanupPolicy: "delete",
          region: "eu",
          partition: 0,
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
      rowId: "eu:0:order:1",
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
        cleanupPolicy: "delete",
        region: "eu",
        partition: 0,
        localRowKey: "order:1",
      } as const,
      {
        getOwnPropertyDescriptor: () => {
          throw new Error("row ID descriptor escaped");
        },
      },
    );

    expect(() => kafka.source(accessorInput)).toThrowError(
      "Kafka source requires exactly its cleanup-policy-specific fields, including cleanupPolicy and retentionPolicy.",
    );
    expect(accessorReads).toBe(0);
    expect(() => kafka.source(hostileSource)).toThrowError(
      "Kafka source requires exactly its cleanup-policy-specific fields, including cleanupPolicy and retentionPolicy.",
    );
    expect(() =>
      kafka.source({
        ...validSourceInput(),
        startFrom: hostileStart,
      }),
    ).toThrowError(
      "Kafka startFrom must be earliest, latest, committed, timestamp, or durationAgo.",
    );
    expect(() => kafkaRowId(hostileRowId)).toThrowError("Kafka rowId input must be an object.");
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
      durationInput(Number.NaN),
      durationInput([Number.NaN, 0]),
      durationInput([0, Number.NaN]),
      durationInput({ seconds: Number.NaN }),
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
