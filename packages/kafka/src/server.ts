import { Chunk, Clock, Effect, Layer, Option, Result, Schedule, Scope, Stream } from "effect";
import type {
  SourceApplicationExit,
  SourceExecutionFailure,
  SourceToolkit,
} from "effect-view-server/source-adapter";
import { SourceAdapterServer } from "effect-view-server/source-adapter/server";
import {
  KafkaSourceAdapter,
  KafkaSourceConfigurationError,
  decodeKafkaCodec,
  kafkaConsumerGroupId,
  kafkaRowId,
  type KafkaAdapterFailure,
  type KafkaCapturedStartPosition,
  type KafkaCodec,
  type KafkaMaterializedMetrics,
  type KafkaMessageMetadata,
  type KafkaRegionMetrics,
  type KafkaRejectionPhase,
  type KafkaResolvedStartPosition,
  type KafkaStartResolution,
  type KafkaSourceRejectionLocation,
} from "./contract";

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
};

type KafkaRuntimeDefinition = {
  readonly topic: string;
  readonly regions: readonly [string, ...ReadonlyArray<string>];
  readonly key: KafkaCodec<unknown, unknown>;
  readonly value: KafkaCodec<unknown, unknown>;
  readonly localRowKey: (input: never) => unknown;
  readonly map: (input: never) => unknown;
  readonly startFrom: KafkaCapturedStartPosition;
};

const configurationFailure = <Region extends string = string>(
  message: string,
): KafkaAdapterFailure<Region> => ({
  _tag: "KafkaConfigurationFailure",
  message,
});

const bindRegionFailure = <const Region extends string>(
  region: Region,
  failure: KafkaAdapterFailure,
): KafkaAdapterFailure<Region> =>
  Result.try((): KafkaAdapterFailure<Region> => {
    const tag = failure._tag;
    const message = failure.message;
    if (tag === "KafkaConfigurationFailure") {
      return {
        _tag: tag,
        message,
      };
    }
    const failureRegion = failure.region;
    const topic = failure.topic;
    if (failureRegion !== region) {
      return configurationFailure<Region>(
        `Kafka Region ${JSON.stringify(region)} returned a failure for ${JSON.stringify(failureRegion)}.`,
      );
    }
    return {
      _tag: tag,
      region,
      topic,
      message,
    };
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

const adapterExecutionFailure = (
  failure: KafkaAdapterFailure,
): SourceExecutionFailure<KafkaAdapterFailure> => ({
  _tag: "AdapterFailure",
  failure,
});

const resolveStart = Effect.fn("KafkaSourceAdapter.start.resolve")(function* (
  start: KafkaCapturedStartPosition,
): Effect.fn.Return<KafkaResolvedStartPosition> {
  if (start === "earliest") {
    return {
      mode: "earliest",
    };
  }
  if (start === "latest") {
    return {
      mode: "latest",
    };
  }
  if (start.mode === "committed") {
    return {
      mode: start.mode,
      consumerGroupId: start.consumerGroupId,
      fallback: start.fallback,
    };
  }
  if (start.mode === "timestamp") {
    return {
      mode: start.mode,
      atNanos: start.atNanos,
      atMillis: nanosToKafkaMillis(start.atNanos),
      fallback: start.fallback,
    };
  }
  const resolvedAtNanos = yield* Clock.currentTimeNanos;
  const atNanos =
    resolvedAtNanos > start.durationNanos ? resolvedAtNanos - start.durationNanos : 0n;
  return {
    mode: start.mode,
    durationNanos: start.durationNanos,
    resolvedAtNanos,
    atNanos,
    atMillis: nanosToKafkaMillis(atNanos),
    fallback: start.fallback,
  };
});

const nanosToKafkaMillis = (nanos: bigint): bigint =>
  nanos === 0n ? 0n : (nanos + 999_999n) / 1_000_000n;

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
      const key = record.key;
      const value = record.value;
      const settlement = record.settlement;
      const metadata = snapshotMetadata(record.metadata, region, sourceTopic);
      return {
        key,
        value,
        metadata,
        settlement: (applicationExit) =>
          settlement(applicationExit).pipe(
            Effect.mapError((failure) => bindRegionFailure(region, failure)),
          ),
      };
    },
    catch: (cause) => bindRegionRecordFailure(region, cause),
  });

const codecRejectionMessage = (
  role: "key" | "value",
  codec: KafkaCodec<unknown, unknown>,
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

const fromCallback = (
  callback: (input: never) => unknown,
  input: object,
  failure: KafkaAdapterFailure,
): Effect.Effect<unknown, KafkaAdapterFailure> =>
  Effect.try({
    try: () => Reflect.apply(callback, undefined, [input]),
    catch: () => failure,
  });

const rejection = Effect.fn("KafkaSourceAdapter.record.reject")(function* <Row extends object>(
  toolkit: SourceToolkit<Row, KafkaAdapterFailure, KafkaSourceRejectionLocation>,
  metadata: KafkaMessageMetadata,
  settlement: KafkaServerRecord["settlement"],
  phase: KafkaRejectionPhase,
  failure: SourceExecutionFailure<KafkaAdapterFailure>,
  message: string,
) {
  return yield* toolkit.reject({
    failure,
    location: recordLocation(metadata, phase, message),
    rejectedAtNanos: yield* Clock.currentTimeNanos,
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

const recordEvent = Effect.fn("KafkaSourceAdapter.record.event")(function* <Row extends object>(
  definition: KafkaRuntimeDefinition,
  toolkit: SourceToolkit<Row, KafkaAdapterFailure, KafkaSourceRejectionLocation>,
  regionConsumer: KafkaServerRegionConsumer,
  metricInput: KafkaServerRegionMetricsInput,
  record: KafkaServerRecord,
) {
  const region = metricInput.region;
  const sourceTopic = metricInput.sourceTopic;
  const metadata = record.metadata;
  const rejectDecode = (
    phase: Extract<KafkaRejectionPhase, "keyDecode" | "valueDecode">,
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

  if (record.key === null) {
    return yield* rejectDecode("keyDecode", "Kafka record key is required.");
  }
  const processedKey = yield* effectFailure(
    decodeKafkaCodec(definition.key, {
      bytes: record.key,
      metadata,
    }),
    () => rejectDecode("keyDecode", codecRejectionMessage("key", definition.key)),
  );
  if (processedKey._tag === "Rejected") {
    return processedKey.event;
  }
  const key = processedKey.value;
  if (record.value === null) {
    yield* regionConsumer.recordDecoded;
  }
  const processedLocalRowKey = yield* fromCallback(
    definition.localRowKey,
    {
      key,
      region,
      metadata,
    },
    mappingFailure(region, sourceTopic, "Kafka Local Row Key threw."),
  ).pipe(
    Effect.flatMap((value) =>
      typeof value === "string" && value.length > 0
        ? Effect.succeed(value)
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
      onSuccess: (value) => Effect.succeed(processedValue(value)),
    }),
  );
  if (processedLocalRowKey._tag === "Rejected") {
    return processedLocalRowKey.event;
  }
  const localRowKey = processedLocalRowKey.value;
  const processedId = yield* Effect.try({
    try: () => kafkaRowId({ region, localRowKey }),
    catch: () => mappingFailure(region, sourceTopic, "Kafka canonical row ID failed."),
  }).pipe(
    Effect.matchEffect({
      onFailure: () =>
        rejectMapping("canonicalId", "Kafka canonical row ID could not be constructed.").pipe(
          Effect.map(processedRejection),
        ),
      onSuccess: (value) => Effect.succeed(processedValue(value)),
    }),
  );
  if (processedId._tag === "Rejected") {
    return processedId.event;
  }
  const id = processedId.value;
  if (record.value === null) {
    const mutation = yield* toolkit.delete(id);
    return yield* toolkit.delivery(Chunk.of(mutation), record.settlement);
  }
  const processedValueResult = yield* effectFailure(
    decodeKafkaCodec(definition.value, {
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
  const processedMapped = yield* fromCallback(
    definition.map,
    {
      key,
      value,
      region,
      localRowKey,
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
  return yield* toolkit.delivery(Chunk.of(processedMutation.value), record.settlement);
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
> =>
  Layer.unwrap(
    Effect.sync(() => {
      type KafkaLifetimeState = {
        start: KafkaResolvedStartPosition | undefined;
      };
      const lifetimes = new Map<Scope.Scope, KafkaLifetimeState>();
      const lifetimeState = Effect.fn("KafkaSourceAdapter.lifetime.state")(function* (
        lifetimeScope: Scope.Scope,
      ) {
        return yield* Effect.uninterruptible(
          Effect.gen(function* () {
            const existing = lifetimes.get(lifetimeScope);
            if (existing !== undefined) {
              return existing;
            }
            const state: KafkaLifetimeState = {
              start: undefined,
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
        );
      });
      const resolveBindingStart = (
        state: KafkaLifetimeState,
        start: KafkaCapturedStartPosition,
      ) => {
        if (state.start !== undefined) {
          return Effect.succeed(state.start);
        }
        return resolveStart(start).pipe(
          Effect.tap((resolved) =>
            Effect.sync(() => {
              state.start = resolved;
            }),
          ),
        );
      };
      return SourceAdapterServer.make(KafkaSourceAdapter, {
        materialized: {
          acquire: (input) =>
            Effect.gen(function* () {
              const definition = input.definition;
              const state = yield* lifetimeState(input.lifetimeScope);
              const activeGroupId = yield* Effect.try({
                try: () => kafkaConsumerGroupId(options.consumerGroupPrefix, input.toolkit.topic),
                catch: (cause) =>
                  adapterExecutionFailure(
                    configurationFailure(
                      cause instanceof KafkaSourceConfigurationError
                        ? cause.message
                        : "Kafka consumer group ID could not be constructed.",
                    ),
                  ),
              });
              const start = yield* resolveBindingStart(state, definition.startFrom);
              const acquired = yield* Effect.forEach(definition.regions, (region) => {
                const regionRuntime = options.regions.get(region);
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
                return regionRuntime
                  .acquire({
                    ...metricInput,
                    start,
                  })
                  .pipe(
                    Effect.mapError((failure) =>
                      adapterExecutionFailure(bindRegionFailure(region, failure)),
                    ),
                    Effect.map((consumer) =>
                      SourceAdapterServer.lane({
                        id: region,
                        events: consumer.records.pipe(
                          Stream.mapError((failure) =>
                            adapterExecutionFailure(bindRegionFailure(region, failure)),
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
                                ),
                              ),
                            ),
                          ),
                        ),
                      }),
                    ),
                  );
              });
              const [first, ...rest] = acquired;
              return SourceAdapterServer.attempt([first, ...rest]);
            }),
          metrics: (input): Effect.Effect<KafkaMaterializedMetrics> =>
            Effect.gen(function* () {
              const state = yield* lifetimeState(input.lifetimeScope);
              const activeGroupId = Result.match(
                Result.try(() => kafkaConsumerGroupId(options.consumerGroupPrefix, input.topic)),
                {
                  onFailure: () => "invalid-kafka-consumer-group",
                  onSuccess: (value) => value,
                },
              );
              return yield* Effect.forEach(input.definition.regions, (region) => {
                const regionRuntime = options.regions.get(region);
                return regionRuntime === undefined
                  ? Effect.succeed(emptyMetrics(region))
                  : regionRuntime
                      .metrics({
                        activeGroupId,
                        lifetimeScope: input.lifetimeScope,
                        region,
                        sourceTopic: input.definition.topic,
                        viewServerTopic: input.topic,
                      })
                      .pipe(Effect.map((metrics) => bindRegionMetrics(region, metrics)));
              }).pipe(
                Effect.map((regions) => {
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
                    regions,
                  };
                }),
              );
            }),
          retry: Schedule.min([
            Schedule.exponential("500 millis").pipe(Schedule.jittered),
            Schedule.spaced("30 seconds"),
          ]),
        },
      });
    }),
  );

export const kafkaServer: {
  readonly adapter: typeof KafkaSourceAdapterServer;
  readonly layer: typeof makeKafkaServerLayer;
} = Object.freeze({
  adapter: KafkaSourceAdapterServer,
  layer: makeKafkaServerLayer,
});
