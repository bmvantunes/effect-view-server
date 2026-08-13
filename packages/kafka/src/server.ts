import {
  Duration,
  Effect,
  Fiber,
  Layer,
  Option,
  Result,
  Schedule,
  Scope,
  Semaphore,
  Stream,
} from "effect";
import type {
  SourceApplicationExit,
  SourceExecutionFailure,
  SourceToolkit,
} from "effect-view-server/source-adapter";
import { SourceAdapterServer } from "effect-view-server/source-adapter/server";
import {
  isKafkaResolvedBrokerContract,
  kafkaBrokerContractKey,
  type KafkaResolvedBrokerContract,
  snapshotKafkaResolvedBrokerContract,
} from "./broker-contract";
import {
  KafkaSourceAdapter,
  KafkaSourceConfigurationError,
  decodeKafkaCompactionKeyCodec,
  decodeKafkaCodec,
  isKafkaSchemaRegistryProtobufCodec,
  kafkaConsumerGroupId,
  kafkaRowId,
  type KafkaAdapterFailure,
  type KafkaCapturedStartPosition,
  type KafkaCodec,
  type KafkaCodecError,
  type KafkaMaterializedMetrics,
  type KafkaMessageMetadata,
  type KafkaRegionMetrics,
  type KafkaRejectionPhase,
  type KafkaResolvedStartPosition,
  type KafkaSchemaRegistryProtobufCodec,
  type KafkaStartResolution,
  type KafkaSourceRejectionLocation,
} from "./contract";
import type {
  KafkaSchemaRegistryBindingInput,
  KafkaSchemaRegistryRecordValidationInput,
  KafkaSchemaRegistryRuntime,
  KafkaServerSchemaRegistry,
} from "./schema-registry-runtime";
import { decodeKafkaSchemaRegistryProtobufPayload } from "./schema-registry-protobuf-codec";
import {
  completeKafkaDelivery,
  configurationFailure,
  currentEpochNanos,
  makeKafkaApplicationStateRegistration,
  resolveKafkaContracts,
  resolveStart,
  type KafkaApplicationState,
  type KafkaRetentionCommand,
  type KafkaRuntimeDefinition,
} from "./server-internal";

export const KafkaSourceAdapterServer: typeof KafkaSourceAdapter = KafkaSourceAdapter;

export type KafkaServerRecord<Region extends string = string> = {
  readonly key: Uint8Array | null;
  readonly value: Uint8Array | null;
  readonly metadata: KafkaMessageMetadata<Region>;
  readonly settlement: (
    applicationExit: SourceApplicationExit,
  ) => Effect.Effect<void, KafkaAdapterFailure<Region>>;
};

export type KafkaServerRegionAcquireInput<Region extends string = string> = {
  readonly activeGroupId: string;
  readonly lifetimeScope: Scope.Scope;
  readonly region: Region;
  readonly sourceTopic: string;
  readonly start: KafkaResolvedStartPosition;
  readonly viewServerTopic: string;
};

export type KafkaServerRegionMetricsInput<Region extends string = string> = {
  readonly activeGroupId: string;
  readonly lifetimeScope: Scope.Scope;
  readonly region: Region;
  readonly sourceTopic: string;
  readonly viewServerTopic: string;
};

export type KafkaServerRegionConsumer<Region extends string = string> = {
  readonly records: Stream.Stream<KafkaServerRecord<Region>, KafkaAdapterFailure<Region>>;
  readonly recordDecoded: Effect.Effect<void>;
  readonly recordDecodeFailure: Effect.Effect<void>;
  readonly recordMapped: Effect.Effect<void>;
  readonly recordMappingFailure: Effect.Effect<void>;
  readonly recordRejection: Effect.Effect<void>;
};

export type KafkaServerRegion<Region extends string = string> = {
  readonly endpoints?: ReadonlyArray<string>;
  readonly acquire: (
    input: KafkaServerRegionAcquireInput<Region>,
  ) => Effect.Effect<KafkaServerRegionConsumer<Region>, KafkaAdapterFailure<Region>, Scope.Scope>;
  readonly metrics: (
    input: KafkaServerRegionMetricsInput<Region>,
  ) => Effect.Effect<KafkaRegionMetrics<Region>>;
};

export type KafkaServerLayerOptions = {
  readonly consumerGroupPrefix: string;
  readonly regions: ReadonlyMap<string, KafkaServerRegion>;
  readonly schemaRegistries?: ReadonlyMap<string, KafkaServerSchemaRegistry>;
  readonly brokerContracts: ReadonlyArray<KafkaResolvedBrokerContract>;
  readonly retentionSweepIntervalNanos: bigint;
};

const bindRegionFailure = <const Region extends string>(
  region: Region,
  sourceTopic: string,
  failure: KafkaAdapterFailure,
): KafkaAdapterFailure<Region> =>
  Result.try((): KafkaAdapterFailure<Region> => {
    const tag = failure._tag;
    const message = failure.message;
    if (typeof message !== "string") {
      return configurationFailure<Region>(
        `Kafka Region ${JSON.stringify(region)} returned an invalid failure.`,
      );
    }
    if (tag === "KafkaConfigurationFailure") {
      return {
        _tag: tag,
        message,
      };
    }
    if (
      tag !== "KafkaAcquisitionFailure" &&
      tag !== "KafkaConsumeFailure" &&
      tag !== "KafkaDecodeFailure" &&
      tag !== "KafkaMappingFailure" &&
      tag !== "KafkaCommitFailure" &&
      tag !== "KafkaReleaseFailure" &&
      tag !== "KafkaSchemaRegistryUnavailable" &&
      tag !== "KafkaSchemaRegistryPolicyMismatch" &&
      tag !== "KafkaSchemaRegistrySchemaMismatch"
    ) {
      return configurationFailure<Region>(
        `Kafka Region ${JSON.stringify(region)} returned an invalid failure.`,
      );
    }
    const failureRegion = failure.region;
    const topic = failure.topic;
    if (failureRegion !== region) {
      return configurationFailure<Region>(
        `Kafka Region ${JSON.stringify(region)} returned a failure for ${JSON.stringify(failureRegion)}.`,
      );
    }
    if (topic !== sourceTopic) {
      return configurationFailure<Region>(
        `Kafka Region ${JSON.stringify(region)} returned a failure for source Topic ${JSON.stringify(topic)}.`,
      );
    }
    if (
      tag === "KafkaSchemaRegistryUnavailable" ||
      tag === "KafkaSchemaRegistryPolicyMismatch" ||
      tag === "KafkaSchemaRegistrySchemaMismatch"
    ) {
      const subject = failure.subject;
      const side = failure.side;
      const schemaId = failure.schemaId;
      if (
        typeof subject !== "string" ||
        subject.length === 0 ||
        (side !== "key" && side !== "value") ||
        (schemaId !== null && (typeof schemaId !== "number" || !Number.isSafeInteger(schemaId)))
      ) {
        return configurationFailure<Region>(
          `Kafka Region ${JSON.stringify(region)} returned an invalid failure.`,
        );
      }
      return {
        _tag: tag,
        region,
        topic: sourceTopic,
        subject,
        side,
        schemaId,
        message,
      };
    }
    return { _tag: tag, region, topic: sourceTopic, message };
  }).pipe(
    Result.match({
      onFailure: () =>
        configurationFailure<Region>(
          `Kafka Region ${JSON.stringify(region)} returned an invalid failure.`,
        ),
      onSuccess: (bound) => bound,
    }),
  );

const bindRegionMetrics = <const Region extends string>(
  region: Region,
  metrics: KafkaRegionMetrics,
): KafkaRegionMetrics<Region> =>
  Result.try(
    (): KafkaRegionMetrics<Region> => ({
      ...metrics,
      region,
    }),
  ).pipe(
    Result.match({
      onFailure: () => emptyMetrics(region),
      onSuccess: (bound) => bound,
    }),
  );

const decodeFailure = (region: string, topic: string, message: string): KafkaAdapterFailure => ({
  _tag: "KafkaDecodeFailure",
  region,
  topic,
  message,
});

const mappingFailure = (region: string, topic: string, message: string): KafkaAdapterFailure => ({
  _tag: "KafkaMappingFailure",
  region,
  topic,
  message,
});

const unexpectedConsumerCompletion = (region: string, topic: string): KafkaAdapterFailure => ({
  _tag: "KafkaConsumeFailure",
  region,
  topic,
  message: `Kafka Region ${JSON.stringify(region)} consumer stream completed unexpectedly.`,
});

const adapterExecutionFailure = (
  failure: KafkaAdapterFailure,
): SourceExecutionFailure<KafkaAdapterFailure> => ({
  _tag: "AdapterFailure",
  failure,
});

const recordLocation = (
  metadata: KafkaMessageMetadata,
  phase: KafkaRejectionPhase,
  message: string,
): KafkaSourceRejectionLocation => ({
  region: metadata.sourceRegion,
  topic: metadata.sourceTopic,
  partition: metadata.partition,
  offset: metadata.offset,
  phase,
  message,
});

const invalidRecordMetadataMessage = (region: string): string =>
  `Kafka Region ${JSON.stringify(region)} returned invalid record metadata.`;

const invalidRecordMetadata = (region: string): never => {
  throw new KafkaSourceConfigurationError(invalidRecordMetadataMessage(region));
};

const snapshotHeaders = (
  headers: KafkaMessageMetadata["headers"],
  region: string,
): KafkaMessageMetadata["headers"] => {
  if (typeof headers !== "object" || headers === null || Array.isArray(headers)) {
    return invalidRecordMetadata(region);
  }
  const prototype = Object.getPrototypeOf(headers);
  if (prototype !== Object.prototype && prototype !== null) {
    return invalidRecordMetadata(region);
  }
  const snapshot: Record<string, Uint8Array | ReadonlyArray<Uint8Array>> = Object.create(null);
  for (const name of Reflect.ownKeys(headers)) {
    if (typeof name !== "string") {
      return invalidRecordMetadata(region);
    }
    const descriptor = Object.getOwnPropertyDescriptor(headers, name);
    if (descriptor === undefined || descriptor.enumerable !== true || !("value" in descriptor)) {
      return invalidRecordMetadata(region);
    }
    const value = descriptor.value;
    if (value instanceof Uint8Array) {
      snapshot[name] = Uint8Array.from(value);
      continue;
    }
    if (!Array.isArray(value)) {
      return invalidRecordMetadata(region);
    }
    const repeated: Array<Uint8Array> = [];
    for (const entry of value) {
      if (!(entry instanceof Uint8Array)) {
        return invalidRecordMetadata(region);
      }
      repeated.push(Uint8Array.from(entry));
    }
    snapshot[name] = Object.freeze(repeated);
  }
  return Object.freeze(snapshot);
};

const snapshotMetadata = <const Region extends string>(
  metadata: KafkaMessageMetadata,
  region: Region,
  sourceTopic: string,
): KafkaMessageMetadata<Region> => {
  const sourceRegion = metadata.sourceRegion;
  if (sourceRegion !== region) {
    throw new KafkaSourceConfigurationError(
      `Kafka Region ${JSON.stringify(region)} returned record metadata for ${JSON.stringify(sourceRegion)}.`,
    );
  }
  const recordSourceTopic = metadata.sourceTopic;
  const partition = metadata.partition;
  const offset = metadata.offset;
  const timestampNanos = metadata.timestampNanos;
  if (
    recordSourceTopic !== sourceTopic ||
    typeof partition !== "number" ||
    !Number.isSafeInteger(partition) ||
    partition < 0 ||
    partition > 2_147_483_647 ||
    typeof offset !== "bigint" ||
    offset < 0n ||
    typeof timestampNanos !== "bigint" ||
    timestampNanos < 0n
  ) {
    return invalidRecordMetadata(region);
  }
  const headers = snapshotHeaders(metadata.headers, region);
  return Object.freeze({
    sourceTopic,
    sourceRegion: region,
    partition,
    offset,
    timestampNanos,
    headers,
  });
};

const invalidRecordMessage = (region: string): string =>
  `Kafka Region ${JSON.stringify(region)} returned an invalid record.`;

const snapshotPayload = (value: unknown, region: string): Uint8Array | null => {
  if (value === null) {
    return null;
  }
  if (!(value instanceof Uint8Array)) {
    throw new KafkaSourceConfigurationError(invalidRecordMessage(region));
  }
  return Uint8Array.from(value);
};

const bindRegionRecordFailure = <const Region extends string>(
  region: Region,
  cause: unknown,
): KafkaAdapterFailure<Region> =>
  configurationFailure<Region>(
    Result.try(() => {
      if (!(cause instanceof KafkaSourceConfigurationError)) {
        return invalidRecordMessage(region);
      }
      const message = cause.message;
      return typeof message === "string" ? message : invalidRecordMessage(region);
    }).pipe(
      Result.match({
        onFailure: () => invalidRecordMessage(region),
        onSuccess: (message) => message,
      }),
    ),
  );

const bindRegionRecord = <const Region extends string>(
  region: Region,
  sourceTopic: string,
  record: KafkaServerRecord,
): Effect.Effect<KafkaServerRecord<Region>, KafkaAdapterFailure<Region>> =>
  Effect.try({
    try: (): KafkaServerRecord<Region> => {
      const key = snapshotPayload(record.key, region);
      const value = snapshotPayload(record.value, region);
      const settlement = record.settlement;
      if (typeof settlement !== "function") {
        throw new KafkaSourceConfigurationError(invalidRecordMessage(region));
      }
      const metadata = snapshotMetadata(record.metadata, region, sourceTopic);
      return {
        key,
        value,
        metadata,
        settlement: (applicationExit) => {
          const candidate = settlement(applicationExit);
          return Effect.isEffect(candidate)
            ? candidate.pipe(
                Effect.mapError((failure) => bindRegionFailure(region, sourceTopic, failure)),
              )
            : Effect.fail(configurationFailure<Region>(invalidRecordMessage(region)));
        },
      };
    },
    catch: (cause) => bindRegionRecordFailure(region, cause),
  });

const codecRejectionMessage = (
  role: "key" | "value",
  codec:
    | KafkaCodec<unknown, unknown>
    | import("./contract").KafkaCompactionKeyCodec<unknown, unknown>,
): string => {
  const identity = Result.try(() => ({
    format: Reflect.get(codec, "format"),
    name: Reflect.get(codec, "name"),
  }));
  return Result.isSuccess(identity) &&
    identity.success.format === "custom" &&
    typeof identity.success.name === "string" &&
    identity.success.name.length > 0
    ? `Kafka ${role} codec ${JSON.stringify(identity.success.name)} rejected the record.`
    : `Kafka ${role} codec rejected the record.`;
};

const captureCompleteMappedRow = (id: string, value: unknown): Option.Option<object> =>
  Result.try(() => {
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      return Option.none<object>();
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      return Option.none<object>();
    }
    const row = { id };
    for (const key of Reflect.ownKeys(value)) {
      if (typeof key !== "string" || key === "id") {
        return Option.none<object>();
      }
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (descriptor === undefined || descriptor.enumerable !== true || !("value" in descriptor)) {
        return Option.none<object>();
      }
      Object.defineProperty(row, key, {
        configurable: true,
        enumerable: true,
        value: descriptor.value,
        writable: true,
      });
    }
    return Option.some<object>(row);
  }).pipe(
    Result.match({
      onFailure: () => Option.none<object>(),
      onSuccess: (row) => row,
    }),
  );

const fromCallback = <Input extends object>(
  callback: (input: never) => unknown,
  input: Input,
  failure: KafkaAdapterFailure,
): Effect.Effect<unknown, KafkaAdapterFailure> =>
  Effect.try({
    try: () => Reflect.apply(callback, undefined, [input]),
    catch: () => failure,
  });

const rejection = Effect.fn("KafkaSourceAdapter.record.reject")(function* <
  Row extends object,
  Topic extends string,
>(
  toolkit: SourceToolkit<Row, KafkaAdapterFailure, KafkaSourceRejectionLocation, never, Topic>,
  metadata: KafkaMessageMetadata,
  settlement: KafkaServerRecord["settlement"],
  phase: KafkaRejectionPhase,
  failure: SourceExecutionFailure<KafkaAdapterFailure>,
  message: string,
) {
  return yield* toolkit.reject({
    failure,
    location: recordLocation(metadata, phase, message),
    rejectedAtNanos: yield* currentEpochNanos().pipe(Effect.mapError(adapterExecutionFailure)),
    settlement,
  });
});

type Processed<Value> =
  | {
      readonly _tag: "Value";
      readonly value: Value;
    }
  | {
      readonly _tag: "Rejected";
      readonly event: import("effect-view-server/source-adapter").SourceItemRejection<
        KafkaAdapterFailure,
        KafkaSourceRejectionLocation
      >;
    };

const processedValue = <Value>(value: Value): Processed<Value> => ({
  _tag: "Value",
  value,
});

const processedRejection = (
  event: import("effect-view-server/source-adapter").SourceItemRejection<
    KafkaAdapterFailure,
    KafkaSourceRejectionLocation
  >,
): Processed<never> => ({
  _tag: "Rejected",
  event,
});

const effectFailure = <Value>(
  effect: Effect.Effect<Value, unknown>,
  onFailure: () => Effect.Effect<
    import("effect-view-server/source-adapter").SourceItemRejection<
      KafkaAdapterFailure,
      KafkaSourceRejectionLocation
    >,
    SourceExecutionFailure<KafkaAdapterFailure>
  >,
): Effect.Effect<Processed<Value>, SourceExecutionFailure<KafkaAdapterFailure>> =>
  Effect.matchEffect(effect, {
    onFailure: () => onFailure().pipe(Effect.map(processedRejection)),
    onSuccess: (value) => Effect.succeed(processedValue(value)),
  });

type KafkaSchemaRegistryCodecs = {
  readonly key: KafkaSchemaRegistryProtobufCodec | undefined;
  readonly value: KafkaSchemaRegistryProtobufCodec | undefined;
};

const schemaRegistryCodecs = (definition: KafkaRuntimeDefinition): KafkaSchemaRegistryCodecs =>
  Object.freeze({
    key: isKafkaSchemaRegistryProtobufCodec(definition.key) ? definition.key : undefined,
    value: isKafkaSchemaRegistryProtobufCodec(definition.value) ? definition.value : undefined,
  });

const schemaRegistrySides = (codecs: KafkaSchemaRegistryCodecs): ReadonlyArray<"key" | "value"> =>
  Object.freeze([
    ...(codecs.key === undefined ? [] : (["key"] as const)),
    ...(codecs.value === undefined ? [] : (["value"] as const)),
  ]);

const validateSchemaRegistryRecord = (
  registry: KafkaSchemaRegistryRuntime,
  input: KafkaSchemaRegistryRecordValidationInput,
) => registry.validateRecord(input).pipe(Effect.mapError(adapterExecutionFailure));

const decodeSchemaRegistryProtobufPayload = (
  codec: KafkaSchemaRegistryProtobufCodec,
  payload: Uint8Array,
): Effect.Effect<unknown, KafkaCodecError> =>
  decodeKafkaSchemaRegistryProtobufPayload(codec.descriptor, payload);

const recordEvent = Effect.fn("KafkaSourceAdapter.record.event")(function* <
  Row extends object,
  Topic extends string,
>(
  definition: KafkaRuntimeDefinition,
  toolkit: SourceToolkit<Row, KafkaAdapterFailure, KafkaSourceRejectionLocation, never, Topic>,
  regionConsumer: KafkaServerRegionConsumer,
  metricInput: KafkaServerRegionMetricsInput,
  record: KafkaServerRecord,
  schemaRegistry: KafkaSchemaRegistryRuntime | undefined,
  registryCodecs: KafkaSchemaRegistryCodecs,
  registrySides: ReadonlyArray<"key" | "value">,
  lifetime: {
    readonly applicationState: KafkaApplicationState<Topic>;
    readonly contracts: ReadonlyMap<string, KafkaResolvedBrokerContract>;
  },
) {
  const region = metricInput.region;
  const sourceTopic = metricInput.sourceTopic;
  const metadata = record.metadata;
  const retention = Option.getOrThrow(
    Option.fromUndefinedOr(lifetime.contracts.get(region)?.resolvedRetention),
  );
  const rejectDecode = (
    phase: Extract<KafkaRejectionPhase, "keyDecode" | "valueDecode" | "nullValue">,
    message: string,
  ) =>
    regionConsumer.recordDecodeFailure.pipe(
      Effect.andThen(regionConsumer.recordRejection),
      Effect.andThen(
        rejection(
          toolkit,
          metadata,
          record.settlement,
          phase,
          adapterExecutionFailure(decodeFailure(region, sourceTopic, message)),
          message,
        ),
      ),
    );
  const rejectMapping = (
    phase: Exclude<KafkaRejectionPhase, "keyDecode" | "valueDecode">,
    message: string,
    failure: SourceExecutionFailure<KafkaAdapterFailure> = adapterExecutionFailure(
      mappingFailure(region, sourceTopic, message),
    ),
  ) =>
    regionConsumer.recordMappingFailure.pipe(
      Effect.andThen(regionConsumer.recordRejection),
      Effect.andThen(rejection(toolkit, metadata, record.settlement, phase, failure, message)),
    );
  const keyedDelivery = (
    id: string,
    mutation: import("effect-view-server/source-adapter").SourceMutation<Row>,
    command: KafkaRetentionCommand,
  ) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const prepared = yield* restore(lifetime.applicationState.prepare(command));
        const delivery = yield* Effect.exit(
          toolkit.delivery(mutation, record.settlement, prepared.transition),
        );
        return yield* completeKafkaDelivery(delivery, prepared);
      }),
    );

  if (record.key === null) {
    return yield* rejectDecode("keyDecode", "Kafka record key is required.");
  }
  const registryPayloads =
    registryCodecs.key === undefined && registryCodecs.value === undefined
      ? undefined
      : yield* validateSchemaRegistryRecord(
          Option.getOrThrow(Option.fromUndefinedOr(schemaRegistry)),
          {
            viewServerTopic: toolkit.topic,
            sourceTopic,
            sides: registrySides,
            key: registryCodecs.key === undefined ? null : record.key,
            value: registryCodecs.value === undefined ? null : record.value,
          },
        );
  const registryKeyPayload = registryPayloads?.key;
  // Compaction identity owns an immutable snapshot that application codecs can never mutate.
  const serializedKeyBytes =
    definition.cleanupPolicy === "delete" ? undefined : Uint8Array.from(record.key);
  const registryValuePayload = registryPayloads?.value;
  const processedRegistryKey =
    registryCodecs.key === undefined
      ? undefined
      : yield* effectFailure(
          decodeSchemaRegistryProtobufPayload(
            registryCodecs.key,
            Option.getOrThrow(Option.fromUndefinedOr(registryKeyPayload)),
          ),
          () => rejectDecode("keyDecode", codecRejectionMessage("key", definition.key)),
        );
  if (processedRegistryKey?._tag === "Rejected") {
    return processedRegistryKey.event;
  }
  if (record.value === null) {
    if (definition.cleanupPolicy === "delete") {
      return yield* rejectDecode(
        "nullValue",
        "Delete-only Kafka source records require a non-null value.",
      );
    }
    yield* regionConsumer.recordDecoded;
    const id = kafkaRowId({
      cleanupPolicy: definition.cleanupPolicy,
      region,
      partition: metadata.partition,
      serializedKeyBytes: Option.getOrThrow(Option.fromUndefinedOr(serializedKeyBytes)),
    });
    const mutation = yield* toolkit.delete(id);
    return yield* keyedDelivery(id, mutation, {
      _tag: "AppliedDelete",
      id,
      region,
      authoritativeExpired: false,
    });
  }
  const processedKey = yield* effectFailure(
    isKafkaSchemaRegistryProtobufCodec(definition.key)
      ? Effect.succeed(Option.getOrThrow(Option.fromUndefinedOr(processedRegistryKey)).value)
      : definition.cleanupPolicy === "delete"
        ? decodeKafkaCodec(definition.key, {
            bytes: record.key,
            metadata,
          })
        : decodeKafkaCompactionKeyCodec(definition.key, {
            bytes: record.key,
          }),
    () => rejectDecode("keyDecode", codecRejectionMessage("key", definition.key)),
  );
  if (processedKey._tag === "Rejected") {
    return processedKey.event;
  }
  const key = processedKey.value;
  const processedValueResult = yield* effectFailure(
    isKafkaSchemaRegistryProtobufCodec(definition.value)
      ? decodeSchemaRegistryProtobufPayload(
          definition.value,
          Option.getOrThrow(Option.fromUndefinedOr(registryValuePayload)),
        )
      : decodeKafkaCodec(definition.value, {
          bytes: record.value,
          metadata,
        }),
    () => rejectDecode("valueDecode", codecRejectionMessage("value", definition.value)),
  );
  if (processedValueResult._tag === "Rejected") {
    return processedValueResult.event;
  }
  const value = processedValueResult.value;
  yield* regionConsumer.recordDecoded;
  const processedLocalRowKey =
    definition.cleanupPolicy === "delete"
      ? yield* fromCallback(
          definition.localRowKey,
          {
            key,
            value,
            region,
          },
          mappingFailure(region, sourceTopic, "Kafka Local Row Key threw."),
        ).pipe(
          Effect.flatMap((candidate) =>
            typeof candidate === "string" && candidate.length > 0
              ? Effect.succeed(candidate)
              : Effect.fail(
                  mappingFailure(
                    region,
                    sourceTopic,
                    "Kafka Local Row Key must return a non-empty string.",
                  ),
                ),
          ),
          Effect.matchEffect({
            onFailure: () =>
              rejectMapping("localRowKey", "Kafka Local Row Key could not be constructed.").pipe(
                Effect.map(processedRejection),
              ),
            onSuccess: (candidate) => Effect.succeed(processedValue(candidate)),
          }),
        )
      : processedValue(undefined);
  if (processedLocalRowKey._tag === "Rejected") {
    return processedLocalRowKey.event;
  }
  const localRowKey = processedLocalRowKey.value;
  const processedId = yield* Effect.try({
    try: () =>
      definition.cleanupPolicy === "delete"
        ? kafkaRowId({
            cleanupPolicy: "delete",
            region,
            partition: metadata.partition,
            localRowKey: Option.getOrThrow(Option.fromNullishOr(localRowKey)),
          })
        : kafkaRowId({
            cleanupPolicy: definition.cleanupPolicy,
            region,
            partition: metadata.partition,
            serializedKeyBytes: Option.getOrThrow(Option.fromUndefinedOr(serializedKeyBytes)),
          }),
    catch: () => mappingFailure(region, sourceTopic, "Kafka canonical row ID failed."),
  }).pipe(
    Effect.matchEffect({
      onFailure: () =>
        rejectMapping("canonicalId", "Kafka canonical row ID could not be constructed.").pipe(
          Effect.map(processedRejection),
        ),
      onSuccess: (candidate) => Effect.succeed(processedValue(candidate)),
    }),
  );
  if (processedId._tag === "Rejected") {
    return processedId.event;
  }
  const id = processedId.value;
  const processedMapped = yield* fromCallback(
    definition.map,
    definition.cleanupPolicy === "delete"
      ? {
          key,
          value,
          region,
          localRowKey,
          metadata,
        }
      : {
          key,
          value,
          region,
          metadata,
        },
    mappingFailure(region, sourceTopic, "Kafka Mapping threw."),
  ).pipe(
    Effect.flatMap((candidate) =>
      captureCompleteMappedRow(id, candidate).pipe(
        Option.match({
          onNone: () =>
            Effect.fail(
              mappingFailure(
                region,
                sourceTopic,
                "Kafka Mapping must return a plain exact non-ID row.",
              ),
            ),
          onSome: Effect.succeed,
        }),
      ),
    ),
    Effect.matchEffect({
      onFailure: () =>
        rejectMapping("mapping", "Kafka Mapping rejected the record.").pipe(
          Effect.map(processedRejection),
        ),
      onSuccess: (candidate) => Effect.succeed(processedValue(candidate)),
    }),
  );
  if (processedMapped._tag === "Rejected") {
    return processedMapped.event;
  }
  const processedMutation = yield* toolkit.decodeUpsert(processedMapped.value).pipe(
    Effect.matchEffect({
      onFailure: (failure) =>
        rejectMapping(
          "topicSchema",
          "Kafka mapped row does not satisfy the Topic Schema.",
          failure,
        ).pipe(Effect.map(processedRejection)),
      onSuccess: (mutation) => Effect.succeed(processedValue(mutation)),
    }),
  );
  if (processedMutation._tag === "Rejected") {
    return processedMutation.event;
  }
  yield* regionConsumer.recordMapped;
  const deadlineNanos =
    retention._tag === "Forever" ? null : metadata.timestampNanos + retention.durationNanos;
  if (deadlineNanos !== null) {
    const nowNanos = yield* currentEpochNanos().pipe(Effect.mapError(adapterExecutionFailure));
    if (deadlineNanos <= nowNanos) {
      const mutation = yield* toolkit.delete(id);
      return yield* keyedDelivery(id, mutation, {
        _tag: "AppliedDelete",
        id,
        region,
        authoritativeExpired: true,
      });
    }
  }
  return yield* keyedDelivery(id, processedMutation.value, {
    _tag: "AppliedUpsert",
    id,
    region,
    deadlineNanos,
  });
});

const emptyMetrics = <const Region extends string>(region: Region): KafkaRegionMetrics<Region> => ({
  region,
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
});

export const makeKafkaServerLayer = (
  options: KafkaServerLayerOptions,
): Layer.Layer<
  import("effect").Context.Service.Identifier<typeof KafkaSourceAdapter.runtimeService>
> => {
  if (
    typeof options.retentionSweepIntervalNanos !== "bigint" ||
    options.retentionSweepIntervalNanos <= 0n
  ) {
    throw new KafkaSourceConfigurationError(
      "Kafka retention sweep interval must be a positive bigint.",
    );
  }
  // A one-byte Topic validates the prefix itself against the strictest possible
  // lower bound; every configured Topic is validated exactly below.
  kafkaConsumerGroupId(options.consumerGroupPrefix, "x");
  const brokerContracts = new Map<string, KafkaResolvedBrokerContract>();
  for (const suppliedContract of options.brokerContracts) {
    if (!isKafkaResolvedBrokerContract(suppliedContract)) {
      throw new KafkaSourceConfigurationError(
        "Kafka broker contract must be a complete validated broker-resolution result.",
      );
    }
    const contract = snapshotKafkaResolvedBrokerContract(suppliedContract);
    kafkaConsumerGroupId(options.consumerGroupPrefix, contract.viewServerTopic);
    const key = kafkaBrokerContractKey(contract.viewServerTopic, contract.region);
    if (brokerContracts.has(key)) {
      throw new KafkaSourceConfigurationError(
        `Kafka broker contract for Topic ${contract.viewServerTopic} Region ${contract.region} is duplicated.`,
      );
    }
    brokerContracts.set(key, contract);
  }
  const consumerGroupPrefix = options.consumerGroupPrefix;
  const regions = new Map(options.regions);
  const schemaRegistries = new Map(options.schemaRegistries ?? []);
  const retentionSweepIntervalNanos = options.retentionSweepIntervalNanos;
  return Layer.unwrap(
    Effect.sync(() => {
      type KafkaLifetimeState = {
        start: KafkaResolvedStartPosition | undefined;
        readonly contracts: ReadonlyMap<string, KafkaResolvedBrokerContract>;
      };
      const lifetimes = new Map<Scope.Scope, KafkaLifetimeState>();
      const lifetimeStateLock = Semaphore.makeUnsafe(1);
      const resolvedContracts = (
        viewServerTopic: string,
        definition: KafkaRuntimeDefinition,
      ): ReadonlyArray<KafkaResolvedBrokerContract> =>
        resolveKafkaContracts(viewServerTopic, definition, brokerContracts);
      const applicationStateRegistration = makeKafkaApplicationStateRegistration(
        resolvedContracts,
        retentionSweepIntervalNanos,
      );
      const lifetimeState = Effect.fn("KafkaSourceAdapter.lifetime.state")(function* (
        lifetimeScope: Scope.Scope,
        viewServerTopic: string,
        definition: KafkaRuntimeDefinition,
      ) {
        return yield* Effect.uninterruptible(
          lifetimeStateLock.withPermit(
            Effect.gen(function* () {
              const existing = lifetimes.get(lifetimeScope);
              if (existing !== undefined) {
                return existing;
              }
              if (schemaRegistrySides(schemaRegistryCodecs(definition)).length > 0) {
                yield* Effect.forEach(
                  definition.regions,
                  (region) => {
                    const schemaRegistry = schemaRegistries.get(region);
                    return schemaRegistry === undefined
                      ? Effect.void
                      : schemaRegistry.retain(lifetimeScope);
                  },
                  { discard: true },
                );
              }
              const contracts = new Map(
                resolvedContracts(viewServerTopic, definition).map((contract) => [
                  contract.region,
                  contract,
                ]),
              );
              const state: KafkaLifetimeState = {
                start: undefined,
                contracts,
              };
              lifetimes.set(lifetimeScope, state);
              yield* Scope.addFinalizer(
                lifetimeScope,
                Effect.sync(() => {
                  lifetimes.delete(lifetimeScope);
                }),
              );
              return state;
            }),
          ),
        );
      });
      const resolveBindingStart = (state: KafkaLifetimeState, start: KafkaCapturedStartPosition) =>
        Effect.uninterruptible(
          Effect.gen(function* () {
            if (state.start !== undefined) {
              return state.start;
            }
            const resolved = yield* resolveStart(start);
            state.start = resolved;
            return resolved;
          }),
        );
      return SourceAdapterServer.make(KafkaSourceAdapter, {
        reporting: {
          dependencies: (input) => {
            const registrySides = schemaRegistrySides(schemaRegistryCodecs(input.definition));
            return Effect.succeed(
              input.definition.regions.flatMap((region) => [
                {
                  dependency: "kafka",
                  target: region,
                  endpoints: regions.get(region)?.endpoints ?? [],
                },
                ...(registrySides.length === 0
                  ? []
                  : [
                      {
                        dependency: "schema-registry",
                        target: region,
                        endpoints: schemaRegistries.get(region)?.endpoints ?? [],
                      },
                    ]),
              ]),
            );
          },
          classifyFailure: (failure) => {
            if (
              failure._tag === "KafkaConfigurationFailure" ||
              failure._tag === "KafkaMappingFailure"
            ) {
              return { problem: "self" };
            }
            if (
              failure._tag === "KafkaSchemaRegistryUnavailable" ||
              failure._tag === "KafkaSchemaRegistryPolicyMismatch" ||
              failure._tag === "KafkaSchemaRegistrySchemaMismatch"
            ) {
              const attributes = [
                { name: "subject", value: failure.subject },
                { name: "side", value: failure.side },
              ];
              if (failure.schemaId !== null) {
                attributes.push({ name: "schemaId", value: String(failure.schemaId) });
              }
              return {
                problem: "dependency",
                targets: [{ dependency: "schema-registry", target: failure.region }],
                issue: {
                  code: failure._tag,
                  message: failure.message,
                  attributes,
                },
              };
            }
            return {
              problem: "dependency",
              targets: [{ dependency: "kafka", target: failure.region }],
            };
          },
        },
        materialized: {
          applicationState: applicationStateRegistration,
          initialLaneIds: (input) => input.definition.regions,
          acquire: (input) =>
            Effect.gen(function* () {
              const definition = input.definition;
              const state = yield* lifetimeState(
                input.lifetimeScope,
                input.toolkit.topic,
                definition,
              );
              const applicationState = applicationStateRegistration.forLifetime(
                input.lifetimeScope,
                input.toolkit.topic,
              );
              const activeGroupId = yield* Effect.try({
                try: () => kafkaConsumerGroupId(consumerGroupPrefix, input.toolkit.topic),
                catch: (cause) =>
                  adapterExecutionFailure(
                    configurationFailure(
                      cause instanceof KafkaSourceConfigurationError
                        ? cause.message
                        : "Kafka consumer group ID could not be constructed.",
                    ),
                  ),
              });
              const start = yield* resolveBindingStart(state, definition.startFrom).pipe(
                Effect.mapError(adapterExecutionFailure),
              );
              const acquired = yield* Effect.forEach(definition.regions, (region) => {
                const regionRuntime = regions.get(region);
                if (regionRuntime === undefined) {
                  return Effect.fail(
                    adapterExecutionFailure(
                      configurationFailure(
                        `Kafka Region ${region} is not provided by the aggregate Layer.`,
                      ),
                    ),
                  );
                }
                const metricInput: KafkaServerRegionMetricsInput<typeof region> = {
                  activeGroupId,
                  lifetimeScope: input.lifetimeScope,
                  region,
                  sourceTopic: definition.topic,
                  viewServerTopic: input.toolkit.topic,
                };
                const schemaRegistry = schemaRegistries.get(region);
                const registryCodecs = schemaRegistryCodecs(definition);
                const registrySides = schemaRegistrySides(registryCodecs);
                const registryBinding: KafkaSchemaRegistryBindingInput = {
                  viewServerTopic: input.toolkit.topic,
                  sourceTopic: definition.topic,
                  sides: registrySides,
                };
                const acquireRegion = regionRuntime
                  .acquire({
                    ...metricInput,
                    start,
                  })
                  .pipe(
                    Effect.mapError((failure) =>
                      adapterExecutionFailure(bindRegionFailure(region, definition.topic, failure)),
                    ),
                    Effect.map((consumer) => {
                      const records = consumer.records.pipe(
                        Stream.rechunk(1),
                        Stream.mapError((failure) =>
                          adapterExecutionFailure(
                            bindRegionFailure(region, definition.topic, failure),
                          ),
                        ),
                        Stream.mapEffect((record) =>
                          bindRegionRecord(region, definition.topic, record).pipe(
                            Effect.mapError(adapterExecutionFailure),
                            Effect.flatMap((boundRecord) =>
                              recordEvent(
                                definition,
                                input.toolkit,
                                consumer,
                                metricInput,
                                boundRecord,
                                schemaRegistry,
                                registryCodecs,
                                registrySides,
                                {
                                  applicationState,
                                  contracts: state.contracts,
                                },
                              ),
                            ),
                          ),
                        ),
                        Stream.concat(
                          Stream.fail(
                            adapterExecutionFailure(
                              unexpectedConsumerCompletion(region, definition.topic),
                            ),
                          ),
                        ),
                      );
                      const events =
                        registrySides.length === 0 || schemaRegistry === undefined
                          ? records
                          : Stream.transformPull(records, (pull, scope) =>
                              Effect.map(
                                Effect.forkIn(
                                  schemaRegistry
                                    .failures(registryBinding)
                                    .pipe(
                                      Stream.mapError(adapterExecutionFailure),
                                      Stream.runDrain,
                                      Effect.andThen(Effect.never),
                                    ),
                                  scope,
                                ),
                                (failureFiber) => Effect.raceFirst(pull, Fiber.join(failureFiber)),
                              ),
                            );
                      return SourceAdapterServer.lane({ id: region, events });
                    }),
                  );
                if (registrySides.length === 0) {
                  return acquireRegion;
                }
                const firstRegistrySide = Option.getOrThrow(
                  Option.fromUndefinedOr(registrySides[0]),
                );
                if (schemaRegistry === undefined) {
                  return Effect.fail(
                    adapterExecutionFailure({
                      _tag: "KafkaSchemaRegistryUnavailable",
                      region,
                      topic: definition.topic,
                      subject: `${definition.topic}-${firstRegistrySide}`,
                      side: firstRegistrySide,
                      schemaId: null,
                      message: `Kafka Region ${JSON.stringify(region)} has no Schema Registry resource.`,
                    }),
                  );
                }
                return schemaRegistry
                  .guard(registryBinding)
                  .pipe(Effect.mapError(adapterExecutionFailure), Effect.andThen(acquireRegion));
              });
              const [first, ...rest] = acquired;
              return SourceAdapterServer.attempt([first, ...rest]);
            }),
          metrics: (input): Effect.Effect<KafkaMaterializedMetrics> =>
            Effect.gen(function* () {
              const state = yield* lifetimeState(
                input.lifetimeScope,
                input.topic,
                input.definition,
              );
              const applicationState = applicationStateRegistration.forLifetime(
                input.lifetimeScope,
                input.topic,
              );
              const activeGroupId = Result.match(
                Result.try(() => kafkaConsumerGroupId(consumerGroupPrefix, input.topic)),
                {
                  onFailure: () => "invalid-kafka-consumer-group",
                  onSuccess: (value) => value,
                },
              );
              const retentionMetricsByRegion = applicationState.metrics();
              const regionMetrics = Effect.fn("KafkaSourceAdapter.region.metrics")(function* (
                region: string,
              ) {
                const regionRuntime = regions.get(region);
                const transport =
                  regionRuntime === undefined
                    ? emptyMetrics(region)
                    : yield* regionRuntime
                        .metrics({
                          activeGroupId,
                          lifetimeScope: input.lifetimeScope,
                          region,
                          sourceTopic: input.definition.topic,
                          viewServerTopic: input.topic,
                        })
                        .pipe(Effect.map((metrics) => bindRegionMetrics(region, metrics)));
                const retention = Option.getOrThrow(
                  Option.fromUndefinedOr(retentionMetricsByRegion.get(region)),
                );
                return {
                  ...transport,
                  retention,
                };
              });
              const [firstRegion, ...remainingRegions] = input.definition.regions;
              const firstMetrics = yield* regionMetrics(firstRegion);
              const remainingMetrics = yield* Effect.forEach(remainingRegions, regionMetrics);
              const start: KafkaStartResolution =
                state.start === undefined
                  ? {
                      _tag: "Pending",
                    }
                  : {
                      _tag: "Resolved",
                      position: state.start,
                    };
              return {
                activeGroupId,
                start,
                regions: [firstMetrics, ...remainingMetrics],
              };
            }),
          retry: Schedule.min([
            Schedule.exponential("500 millis").pipe(
              Schedule.jittered,
              Schedule.modifyDelay(({ duration }) =>
                Effect.succeed(Duration.millis(Math.ceil(Duration.toMillis(duration)))),
              ),
            ),
            Schedule.spaced("30 seconds"),
          ]),
        },
      });
    }),
  );
};

export const kafkaServer: {
  readonly adapter: typeof KafkaSourceAdapterServer;
  readonly layer: typeof makeKafkaServerLayer;
} = Object.freeze({
  adapter: KafkaSourceAdapterServer,
  layer: makeKafkaServerLayer,
});
