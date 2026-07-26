import { Chunk, Clock, Effect, Layer, Result, Schedule, Scope, Stream } from "effect";
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
  type KafkaSourceRejectionLocation,
} from "./contract";

export const KafkaSourceAdapterServer: typeof KafkaSourceAdapter = KafkaSourceAdapter;

export type KafkaServerRecord = {
  readonly key: Uint8Array | null;
  readonly value: Uint8Array | null;
  readonly metadata: KafkaMessageMetadata;
  readonly settlement: (
    applicationExit: SourceApplicationExit,
  ) => Effect.Effect<void, KafkaAdapterFailure>;
};

export type KafkaServerRegionAcquireInput = {
  readonly activeGroupId: string;
  readonly region: string;
  readonly sourceTopic: string;
  readonly start: KafkaResolvedStartPosition;
  readonly viewServerTopic: string;
};

export type KafkaServerRegionMetricsInput = {
  readonly activeGroupId: string;
  readonly region: string;
  readonly sourceTopic: string;
  readonly viewServerTopic: string;
};

export type KafkaServerRegionConsumer = {
  readonly records: Stream.Stream<KafkaServerRecord, KafkaAdapterFailure>;
  readonly recordDecoded: Effect.Effect<void>;
  readonly recordDecodeFailure: Effect.Effect<void>;
  readonly recordMapped: Effect.Effect<void>;
  readonly recordMappingFailure: Effect.Effect<void>;
  readonly recordRejection: Effect.Effect<void>;
};

export type KafkaServerRegion = {
  readonly acquire: (
    input: KafkaServerRegionAcquireInput,
  ) => Effect.Effect<KafkaServerRegionConsumer, KafkaAdapterFailure, Scope.Scope>;
  readonly metrics: (input: KafkaServerRegionMetricsInput) => Effect.Effect<KafkaRegionMetrics>;
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

const configurationFailure = (message: string): KafkaAdapterFailure => ({
  _tag: "KafkaConfigurationFailure",
  message,
});

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
  record: KafkaServerRecord,
  phase: KafkaRejectionPhase,
  message: string,
): KafkaSourceRejectionLocation => ({
  region: record.metadata.sourceRegion,
  topic: record.metadata.sourceTopic,
  partition: record.metadata.partition,
  offset: record.metadata.offset,
  phase,
  message,
});

const isPlainMappedRow = (value: unknown): value is object => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }
  const inspected = Result.try(() => {
    if (Object.hasOwn(value, "id")) {
      return false;
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      return false;
    }
    return Reflect.ownKeys(value).every((key) => {
      if (typeof key !== "string") {
        return false;
      }
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      return descriptor !== undefined && descriptor.enumerable === true && "value" in descriptor;
    });
  });
  return Result.isSuccess(inspected) && inspected.success;
};

const fromCallback = (
  callback: (input: never) => unknown,
  input: object,
  failure: KafkaAdapterFailure,
): Effect.Effect<unknown, KafkaAdapterFailure> =>
  Effect.try({
    try: () => Reflect.apply(callback, undefined, [input]),
    catch: () => failure,
  });

const makeCompleteRow = (id: string, fields: object): Effect.Effect<object, KafkaAdapterFailure> =>
  Effect.try({
    try: () => Object.assign({ id }, fields),
    catch: () =>
      mappingFailure("unknown", "unknown", "Kafka Mapping result could not be materialized."),
  });

const rejection = Effect.fn("KafkaSourceAdapter.record.reject")(function* <Row extends object>(
  toolkit: SourceToolkit<Row, KafkaAdapterFailure, KafkaSourceRejectionLocation>,
  record: KafkaServerRecord,
  phase: KafkaRejectionPhase,
  failure: SourceExecutionFailure<KafkaAdapterFailure>,
  message: string,
) {
  return yield* toolkit.reject({
    failure,
    location: recordLocation(record, phase, message),
    rejectedAtNanos: yield* Clock.currentTimeNanos,
    settlement: record.settlement,
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
  const rejectDecode = (
    phase: Extract<KafkaRejectionPhase, "keyDecode" | "valueDecode">,
    message: string,
  ) =>
    regionConsumer.recordDecodeFailure.pipe(
      Effect.andThen(regionConsumer.recordRejection),
      Effect.andThen(
        rejection(
          toolkit,
          record,
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
      Effect.andThen(rejection(toolkit, record, phase, failure, message)),
    );

  if (record.key === null) {
    return yield* rejectDecode("keyDecode", "Kafka record key is required.");
  }
  const processedKey = yield* effectFailure(
    decodeKafkaCodec(definition.key, {
      bytes: record.key,
      metadata: record.metadata,
    }),
    () => rejectDecode("keyDecode", "Kafka key codec rejected the record."),
  );
  if (processedKey._tag === "Rejected") {
    return processedKey.event;
  }
  const key = processedKey.value;
  yield* regionConsumer.recordDecoded;
  const processedLocalRowKey = yield* fromCallback(
    definition.localRowKey,
    {
      key,
      region,
      metadata: record.metadata,
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
      metadata: record.metadata,
    }),
    () => rejectDecode("valueDecode", "Kafka value codec rejected the record."),
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
      metadata: record.metadata,
    },
    mappingFailure(region, sourceTopic, "Kafka Mapping threw."),
  ).pipe(
    Effect.flatMap((candidate) =>
      isPlainMappedRow(candidate)
        ? Effect.succeed(candidate)
        : Effect.fail(
            mappingFailure(
              region,
              sourceTopic,
              "Kafka Mapping must return a plain exact non-ID row.",
            ),
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
  const mapped = processedMapped.value;
  const processedComplete = yield* makeCompleteRow(id, mapped).pipe(
    Effect.mapError(() =>
      mappingFailure(region, sourceTopic, "Kafka Mapping result could not be materialized."),
    ),
    Effect.matchEffect({
      onFailure: () =>
        rejectMapping("mapping", "Kafka Mapping rejected the record.").pipe(
          Effect.map(processedRejection),
        ),
      onSuccess: (candidate) => Effect.succeed(processedValue(candidate)),
    }),
  );
  if (processedComplete._tag === "Rejected") {
    return processedComplete.event;
  }
  const processedMutation = yield* toolkit.decodeUpsert(processedComplete.value).pipe(
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

const emptyMetrics = (region: string): KafkaRegionMetrics => ({
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
      const resolvedStarts = new Map<string, KafkaResolvedStartPosition>();
      const resolveBindingStart = (topic: string, start: KafkaCapturedStartPosition) => {
        const current = resolvedStarts.get(topic);
        if (current !== undefined) {
          return Effect.succeed(current);
        }
        return resolveStart(start).pipe(
          Effect.tap((resolved) =>
            Effect.sync(() => {
              resolvedStarts.set(topic, resolved);
            }),
          ),
        );
      };
      return SourceAdapterServer.make(KafkaSourceAdapter, {
        materialized: {
          acquire: (input) =>
            Effect.gen(function* () {
              const definition = input.definition;
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
              const start = yield* resolveBindingStart(input.toolkit.topic, definition.startFrom);
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
                const metricInput: KafkaServerRegionMetricsInput = {
                  activeGroupId,
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
                    Effect.mapError(adapterExecutionFailure),
                    Effect.map((consumer) =>
                      SourceAdapterServer.lane({
                        id: region,
                        events: consumer.records.pipe(
                          Stream.mapError(adapterExecutionFailure),
                          Stream.mapEffect((record) =>
                            recordEvent(definition, input.toolkit, consumer, metricInput, record),
                          ),
                        ),
                      }),
                    ),
                  );
              });
              const [first, ...rest] = acquired;
              return SourceAdapterServer.attempt([first, ...rest]);
            }),
          metrics: (input): Effect.Effect<KafkaMaterializedMetrics> => {
            const activeGroupId = Result.match(
              Result.try(() => kafkaConsumerGroupId(options.consumerGroupPrefix, input.topic)),
              {
                onFailure: () => "invalid-kafka-consumer-group",
                onSuccess: (value) => value,
              },
            );
            const start = resolvedStarts.get(input.topic);
            return Effect.forEach(input.definition.regions, (region) => {
              const regionRuntime = options.regions.get(region);
              return regionRuntime === undefined
                ? Effect.succeed(emptyMetrics(region))
                : regionRuntime.metrics({
                    activeGroupId,
                    region,
                    sourceTopic: input.definition.topic,
                    viewServerTopic: input.topic,
                  });
            }).pipe(
              Effect.map((regions) => ({
                activeGroupId,
                start:
                  start === undefined
                    ? {
                        _tag: "Pending",
                      }
                    : {
                        _tag: "Resolved",
                        position: start,
                      },
                regions,
              })),
            );
          },
          retry: Schedule.spaced("1 second"),
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
