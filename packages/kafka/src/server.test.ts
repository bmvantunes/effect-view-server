import { describe, expect, it, vi } from "@effect/vitest";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { makeViewServerRuntimeCore } from "@effect-view-server/runtime-core";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import type { SourceApplicationExit } from "@effect-view-server/source-adapter";
import { SourceAdapterServer } from "@effect-view-server/source-adapter/server";
import {
  makeSourceMaintenanceOperation,
  resolveSourceApplicationStateRegistration,
  resolveSourceMaintenanceOperation,
} from "@effect-view-server/source-adapter/internal";
import {
  Cause,
  Clock,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Queue,
  Result,
  Schedule,
  Schema,
  Scope,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { kafkaBrokerContractKey, resolveKafkaRetention } from "./broker-contract";
import {
  kafka,
  type KafkaAdapterFailure,
  KafkaSourceAdapter,
  type KafkaMessageMetadata,
  type KafkaRegionMetrics,
  type KafkaStartPosition,
  KafkaSourceConfigurationError,
} from "./contract";
import * as kafkaContract from "./contract";
import {
  makeKafkaServerLayer,
  type KafkaServerRecord,
  type KafkaServerRegion,
  type KafkaServerRegionAcquireInput,
} from "./server";
import * as kafkaServerInternals from "./server-internal";

const Order = Schema.Struct({
  id: ViewServerId,
  price: Schema.Number,
  region: Schema.String,
});

const kafkaTestLifetimeIdentity = Object.freeze({});

const bytes = (value: string): Uint8Array => new TextEncoder().encode(value);

const metadata = (
  region: string,
  offset: bigint,
  headers: KafkaMessageMetadata["headers"] = {},
  partition = 0,
  timestampNanos = offset * 1_000_000n,
): KafkaMessageMetadata => ({
  sourceTopic: "source-orders",
  sourceRegion: region,
  partition,
  offset,
  timestampNanos,
  headers,
});

const commitFailure = (region: string): KafkaAdapterFailure => ({
  _tag: "KafkaCommitFailure",
  region,
  topic: "source-orders",
  message: "commit failed",
});

const acquisitionFailure = (region: string): KafkaAdapterFailure => ({
  _tag: "KafkaAcquisitionFailure",
  region,
  topic: "source-orders",
  message: "acquisition failed",
});

type FakeRegion = {
  readonly runtime: KafkaServerRegion;
  readonly acquisitions: Array<KafkaServerRegionAcquireInput>;
  readonly commits: Array<bigint>;
  readonly failNextAcquisition: () => void;
  readonly offer: (input: {
    readonly key: string | null;
    readonly value: string | null;
    readonly offset: bigint;
    readonly commitFailure?: KafkaAdapterFailure;
    readonly headers?: KafkaMessageMetadata["headers"];
    readonly partition?: number;
    readonly timestampNanos?: bigint;
  }) => Effect.Effect<void>;
  readonly offerRecord: (record: KafkaServerRecord) => Effect.Effect<void>;
  readonly failStream: (failure: KafkaAdapterFailure) => Effect.Effect<void>;
  readonly counts: () => {
    readonly acquisitions: number;
    readonly finalizations: number;
  };
  readonly awaitAcquisitions: (count: number) => Effect.Effect<void>;
};

const awaitCondition = (
  predicate: () => boolean,
  diagnostic = "unspecified",
): Effect.Effect<void> =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 100_000; attempt += 1) {
      if (predicate()) {
        return;
      }
      yield* Effect.yieldNow;
    }
    return yield* Effect.die(
      new Error(`Kafka server test condition was not satisfied: ${diagnostic}.`),
    );
  });

const awaitEffectCondition = <Error>(
  label: string,
  predicate: () => Effect.Effect<boolean, Error>,
): Effect.Effect<void, Error> =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 100_000; attempt += 1) {
      if (yield* predicate()) {
        return;
      }
      yield* Effect.yieldNow;
    }
    return yield* Effect.die(
      new Error(`Kafka server Effect condition ${label} was not satisfied.`),
    );
  });

const makeFakeRegion = (
  region: string,
  acquisitionOrder: Array<string>,
): Effect.Effect<FakeRegion> =>
  Effect.sync(() => {
    let active: Queue.Queue<KafkaServerRecord, KafkaAdapterFailure> | undefined;
    let finalizations = 0;
    let shouldFailAcquisition = false;
    const acquisitions: Array<KafkaServerRegionAcquireInput> = [];
    const commits: Array<bigint> = [];
    const metrics: {
      -readonly [Key in keyof KafkaRegionMetrics]: KafkaRegionMetrics[Key];
    } = {
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
    };
    const updateMetric = (
      key: "decoded" | "decodeFailures" | "mapped" | "mappingFailures" | "rejections",
    ) =>
      Effect.sync(() => {
        metrics[key] += 1n;
      });
    const runtime: KafkaServerRegion = {
      acquire: (input) =>
        Effect.gen(function* () {
          acquisitionOrder.push(`${region}:${acquisitions.length + 1}`);
          if (shouldFailAcquisition) {
            shouldFailAcquisition = false;
            return yield* Effect.fail(acquisitionFailure(region));
          }
          acquisitions.push(input);
          const queue = yield* Queue.unbounded<KafkaServerRecord, KafkaAdapterFailure>();
          active = queue;
          yield* Scope.addFinalizer(
            yield* Effect.scope,
            Effect.sync(() => {
              finalizations += 1;
              metrics.closes += 1n;
              if (active === queue) {
                active = undefined;
              }
            }).pipe(Effect.andThen(Queue.shutdown(queue))),
          );
          return {
            records: Stream.fromQueue(queue),
            recordDecoded: updateMetric("decoded"),
            recordDecodeFailure: updateMetric("decodeFailures"),
            recordMapped: updateMetric("mapped"),
            recordMappingFailure: updateMetric("mappingFailures"),
            recordRejection: updateMetric("rejections"),
          };
        }),
      metrics: () => Effect.sync(() => ({ ...metrics })),
    };
    return {
      runtime,
      acquisitions,
      commits,
      failNextAcquisition: () => {
        shouldFailAcquisition = true;
      },
      offer: (input) =>
        Effect.gen(function* () {
          const queue = active;
          if (queue === undefined) {
            return yield* Effect.die(new Error(`Kafka fake Region ${region} is not active.`));
          }
          const commit =
            input.commitFailure === undefined
              ? Effect.sync(() => {
                  commits.push(input.offset);
                  metrics.commits += 1n;
                })
              : Effect.sync(() => {
                  metrics.commitFailures += 1n;
                }).pipe(Effect.andThen(Effect.fail(input.commitFailure)));
          yield* Queue.offer(queue, {
            key: input.key === null ? null : bytes(input.key),
            value: input.value === null ? null : bytes(input.value),
            metadata: metadata(
              region,
              input.offset,
              input.headers,
              input.partition,
              input.timestampNanos,
            ),
            settlement: (applicationExit) =>
              Exit.isSuccess(applicationExit) ? commit : Effect.void,
          });
        }),
      offerRecord: (record) =>
        Effect.gen(function* () {
          const queue = active;
          if (queue === undefined) {
            return yield* Effect.die(new Error(`Kafka fake Region ${region} is not active.`));
          }
          yield* Queue.offer(queue, record);
        }),
      failStream: (failure) =>
        Effect.gen(function* () {
          const queue = active;
          if (queue === undefined) {
            return yield* Effect.die(new Error(`Kafka fake Region ${region} is not active.`));
          }
          yield* Queue.fail(queue, failure);
        }),
      counts: () => ({
        acquisitions: acquisitions.length,
        finalizations,
      }),
      awaitAcquisitions: (count) =>
        awaitCondition(
          () => acquisitions.length >= count,
          `${region} acquisition ${count}; observed ${acquisitions.length}`,
        ),
    };
  });

const makeSource = (startFrom: KafkaStartPosition) =>
  kafka.source({
    cleanupPolicy: "delete",
    retentionPolicy: "Infinity",
    topic: "source-orders",
    regions: ["eu", "us"],
    key: kafka.string(),
    value: kafka.json(() => Schema.toCodecJson(Schema.Struct({ price: Schema.Number }))),
    localRowKey: ({ key }) => key,
    map: ({ value, region }) => ({
      price: value.price,
      region: String(region),
    }),
    startFrom,
  });

const foreverRetentionMetrics = () => ({
  declaredCleanupPolicy: "delete" as const,
  observedCleanupPolicy: "delete" as const,
  configuredRetention: { _tag: "Forever" as const },
  resolvedRetention: { _tag: "Forever" as const },
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
});

const retentionMetricsFixture = (
  overrides: Partial<kafkaContract.KafkaRetentionMetrics> = {},
): kafkaContract.KafkaRetentionMetrics => ({
  declaredCleanupPolicy: "delete",
  observedCleanupPolicy: "delete",
  configuredRetention: {
    _tag: "Finite",
    durationNanos: 5_000_000_000n,
  },
  resolvedRetention: {
    _tag: "Finite",
    durationNanos: 5_000_000_000n,
  },
  trackedRows: 0,
  lastSweepRetryableFailures: 0,
  expiredRows: 0n,
  authoritativeExpiredDeletes: 0n,
  failedWorkBacklog: 0,
  expirationRetryFailures: 0n,
  latestExpirationFailure: null,
  lastSweepAtNanos: null,
  lastSweepDurationNanos: null,
  sweepIntervalNanos: 1_000n,
  ...overrides,
});

const finiteBrokerContract = () =>
  ({
    viewServerTopic: "orders",
    sourceTopic: "source-orders",
    region: "eu",
    cleanupPolicy: "delete",
    retentionPolicy: {
      _tag: "Finite",
      durationNanos: 5_000_000_000n,
    },
    observedCleanupPolicy: "delete",
    observedRetentionMs: 5_000n,
    resolvedRetention: {
      _tag: "Finite",
      durationNanos: 5_000_000_000n,
    },
  }) as const;

const foreverBrokerContracts = (
  regions: ReadonlyArray<string>,
  cleanupPolicy: "delete" | "compact" | "compact-and-delete" = "delete",
) =>
  regions.map((region) => ({
    viewServerTopic: "orders",
    sourceTopic: "source-orders",
    region,
    cleanupPolicy,
    retentionPolicy: { _tag: "Forever" as const },
    observedCleanupPolicy: cleanupPolicy,
    observedRetentionMs: -1n,
    resolvedRetention: { _tag: "Forever" as const },
  }));

type FaultLocalRowKeyInput = {
  readonly key: string;
};

function faultLocalRowKey(input: FaultLocalRowKeyInput): string;
function faultLocalRowKey(input: FaultLocalRowKeyInput) {
  if (input.key === "local-throw") {
    throw new Error("local row key failed");
  }
  return input.key === "local-empty" ? "" : input.key;
}

type FaultMapInput = {
  readonly key: string;
  readonly value: {
    readonly price: number;
  };
  readonly region: "eu";
};

type FaultMappedRow = {
  readonly price: number;
  readonly region: string;
};

function faultMap(input: FaultMapInput): FaultMappedRow;
function faultMap(input: FaultMapInput) {
  const row = {
    price: input.value.price,
    region: String(input.region),
  };
  if (input.key === "map-throw") {
    throw new Error("mapping failed");
  }
  if (input.key === "map-array") {
    return [row];
  }
  if (input.key === "map-id") {
    return { ...row, id: "adapter-owned" };
  }
  if (input.key === "map-proto") {
    return new Map([["price", row.price]]);
  }
  if (input.key === "map-prototype-throw") {
    return new Proxy(row, {
      getPrototypeOf: () => {
        throw new Error("prototype failed");
      },
    });
  }
  if (input.key === "map-symbol") {
    return {
      ...row,
      [Symbol("unexpected")]: true,
    };
  }
  if (input.key === "map-accessor") {
    return Object.defineProperty({ region: row.region }, "price", {
      enumerable: true,
      get: () => row.price,
    });
  }
  if (input.key === "map-descriptor-throw") {
    return new Proxy(row, {
      getOwnPropertyDescriptor: () => {
        throw new Error("descriptor failed");
      },
    });
  }
  if (input.key === "good") {
    let ownKeyReads = 0;
    return new Proxy(row, {
      ownKeys: (target) => {
        ownKeyReads += 1;
        return ownKeyReads > 1 ? [...Reflect.ownKeys(target), "id"] : Reflect.ownKeys(target);
      },
      getOwnPropertyDescriptor: (target, property) =>
        property === "id" && ownKeyReads > 1
          ? {
              configurable: true,
              enumerable: true,
              value: "adapter-owned",
              writable: true,
            }
          : Reflect.getOwnPropertyDescriptor(target, property),
      get: (target, property, receiver) =>
        property === "id" && ownKeyReads > 1
          ? "adapter-owned"
          : Reflect.get(target, property, receiver),
    });
  }
  if (input.key === "schema") {
    return {
      price: "wrong",
      region: row.region,
    };
  }
  return row;
}

type CompactionFaultMapInput = {
  readonly value: {
    readonly price: number;
  };
  readonly region: string;
};

function compactionFaultMap(input: CompactionFaultMapInput): FaultMappedRow;
function compactionFaultMap(input: CompactionFaultMapInput) {
  const row = {
    price: input.value.price,
    region: String(input.region),
  };
  if (input.value.price === 90) {
    return {
      ...row,
      id: "application-cannot-replace-canonical-id",
    };
  }
  if (input.value.price === 91) {
    return new Proxy(row, {
      ownKeys: () => {
        throw new Error("hostile compaction Mapping ownKeys");
      },
    });
  }
  return row;
}

const makeFaultSource = (keyDecodeStarted?: Deferred.Deferred<void>) => {
  const decoder = new TextDecoder();
  return kafka.source({
    cleanupPolicy: "delete",
    retentionPolicy: "Infinity",
    topic: "source-orders",
    regions: ["eu"],
    key: kafka.codec({
      name: "fault-key",
      decode: ({ bytes }) => {
        const key = decoder.decode(bytes);
        if (key === "key-fail") {
          return Effect.fail({
            _tag: "KafkaCodecError" as const,
            message: "key failed",
          });
        }
        if (key === "key-never") {
          return keyDecodeStarted === undefined
            ? Effect.never
            : Deferred.succeed(keyDecodeStarted, undefined).pipe(Effect.andThen(Effect.never));
        }
        if (key === "key-die") {
          return Effect.die(new Error("key decoder defect"));
        }
        return Effect.succeed(key);
      },
    }),
    value: kafka.json(() => Schema.toCodecJson(Schema.Struct({ price: Schema.Number }))),
    localRowKey: faultLocalRowKey,
    map: faultMap,
    startFrom: "earliest",
  });
};

describe("Kafka Source Adapter Server", () => {
  it.effect("reports region endpoints and classifies dependency provenance", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeRegion("tokyo", []);
      const layer = makeKafkaServerLayer({
        brokerContracts: [],
        retentionSweepIntervalNanos: 900_000_000_000n,
        consumerGroupPrefix: "reporting",
        regions: new Map([
          [
            "tokyo",
            {
              ...fake.runtime,
              endpoints: ["b-1.kafka-tky.com", "b-2.kafka-tky.com"],
            },
          ],
        ]),
      });
      const context = yield* Effect.scoped(Layer.build(layer));
      const service = Context.getUnsafe(context, KafkaSourceAdapter.runtimeService);
      const reporting = Option.getOrThrow(Option.fromNullishOr(service.reporting));

      expect(
        yield* reporting.dependencies({
          topic: "orders",
          lifecycle: "materialized",
          definition: {
            ...makeSource("earliest").options,
            regions: ["tokyo"],
          },
        }),
      ).toStrictEqual([
        {
          target: "tokyo",
          endpoints: ["b-1.kafka-tky.com", "b-2.kafka-tky.com"],
        },
      ]);
      expect(
        [
          { _tag: "KafkaConfigurationFailure", message: "invalid config" } as const,
          {
            _tag: "KafkaMappingFailure",
            region: "tokyo",
            topic: "orders",
            message: "mapping failed",
          } as const,
        ].map(reporting.classifyFailure),
      ).toStrictEqual([{ problem: "self" }, { problem: "self" }]);
      expect(
        (
          [
            "KafkaAcquisitionFailure",
            "KafkaConsumeFailure",
            "KafkaDecodeFailure",
            "KafkaCommitFailure",
            "KafkaReleaseFailure",
          ] as const
        ).map((_tag) =>
          reporting.classifyFailure({
            _tag,
            region: "tokyo",
            topic: "orders",
            message: "dependency failed",
          }),
        ),
      ).toStrictEqual([
        { problem: "dependency", targets: ["tokyo"] },
        { problem: "dependency", targets: ["tokyo"] },
        { problem: "dependency", targets: ["tokyo"] },
        { problem: "dependency", targets: ["tokyo"] },
        { problem: "dependency", targets: ["tokyo"] },
      ]);
    }),
  );

  it.effect("enforces epoch time, start, and broker fallback invariants", () =>
    Effect.gen(function* () {
      expect(kafkaServerInternals.epochNanosFromWallMillis(1_234)).toBe(1_234_000_000n);
      for (const invalid of [-1, Number.NaN, Number.POSITIVE_INFINITY]) {
        expect(() => kafkaServerInternals.epochNanosFromWallMillis(invalid)).toThrow(
          "Effect Clock returned an invalid wall-clock millisecond value.",
        );
      }
      const invalidClock: Clock.Clock = {
        currentTimeMillisUnsafe: () => -1,
        currentTimeMillis: Effect.succeed(-1),
        currentTimeNanosUnsafe: () => 0n,
        currentTimeNanos: Effect.succeed(0n),
        sleep: () => Effect.void,
      };
      const clockFailure = yield* kafkaServerInternals
        .currentEpochNanos()
        .pipe(Effect.provideService(Clock.Clock, invalidClock), Effect.flip);
      const committed = yield* kafkaServerInternals.resolveStart({
        mode: "committed",
        consumerGroupId: "seed",
        fallback: "latest",
      });

      const forever = kafka.source({
        cleanupPolicy: "delete",
        retentionPolicy: "Infinity",
        topic: "source-orders",
        regions: ["eu"],
        key: kafka.string(),
        value: kafka.string(),
        localRowKey: ({ key }) => key,
        map: ({ value }) => ({ value }),
        startFrom: "earliest",
      }).options;
      const finite = kafka.source({
        cleanupPolicy: "delete",
        retentionPolicy: "5 seconds",
        topic: "source-orders",
        regions: ["eu"],
        key: kafka.string(),
        value: kafka.string(),
        localRowKey: ({ key }) => key,
        map: ({ value }) => ({ value }),
        startFrom: "earliest",
      }).options;
      const matched = kafka.source({
        cleanupPolicy: "delete",
        retentionPolicy: "match-kafka-retention",
        topic: "source-orders",
        regions: ["eu"],
        key: kafka.string(),
        value: kafka.string(),
        localRowKey: ({ key }) => key,
        map: ({ value }) => ({ value }),
        startFrom: "earliest",
      }).options;

      expect(clockFailure).toStrictEqual({
        _tag: "KafkaConfigurationFailure",
        message: "Effect Clock returned an invalid wall-clock millisecond value.",
      });
      expect(committed).toStrictEqual({
        mode: "committed",
        consumerGroupId: "seed",
        fallback: "latest",
      });
      const foreverContract = foreverBrokerContracts(["eu"])[0];
      const finiteContract = finiteBrokerContract();
      expect(
        kafkaServerInternals.resolveKafkaContracts(
          "orders",
          forever,
          new Map([
            [
              kafkaBrokerContractKey("orders", "eu"),
              Option.getOrThrow(Option.fromUndefinedOr(foreverContract)),
            ],
          ]),
        ),
      ).toStrictEqual([
        {
          viewServerTopic: "orders",
          sourceTopic: "source-orders",
          region: "eu",
          cleanupPolicy: "delete",
          retentionPolicy: { _tag: "Forever" },
          observedCleanupPolicy: "delete",
          observedRetentionMs: -1n,
          resolvedRetention: { _tag: "Forever" },
        },
      ]);
      expect(
        kafkaServerInternals.resolveKafkaContracts(
          "orders",
          finite,
          new Map([[kafkaBrokerContractKey("orders", "eu"), finiteContract]]),
        )[0]?.resolvedRetention,
      ).toStrictEqual({
        _tag: "Finite",
        durationNanos: 5_000_000_000n,
      });
      expect(() =>
        kafkaServerInternals.resolveKafkaContracts("orders", matched, new Map()),
      ).toThrow("Kafka broker contract for Topic orders Region eu is unavailable.");
      expect(() =>
        kafkaServerInternals.resolveKafkaContracts(
          "orders",
          finite,
          new Map([
            [
              kafkaBrokerContractKey("orders", "eu"),
              {
                ...finiteContract,
                sourceTopic: "other-source-orders",
              },
            ],
          ]),
        ),
      ).toThrow(
        "Kafka broker contract for Topic orders Region eu does not match its Source Definition.",
      );
      const multiRegion = kafka.source({
        cleanupPolicy: "delete",
        retentionPolicy: "match-kafka-retention",
        topic: "source-orders",
        regions: ["eu", "us"],
        key: kafka.string(),
        value: kafka.string(),
        localRowKey: ({ key }) => key,
        map: ({ value }) => ({ value }),
        startFrom: "earliest",
      }).options;
      expect(() =>
        kafkaServerInternals.resolveKafkaContracts(
          "orders",
          multiRegion,
          new Map([
            [
              kafkaBrokerContractKey("orders", "eu"),
              {
                ...finiteContract,
                sourceTopic: "other-source-orders",
              },
            ],
          ]),
        ),
      ).toThrow(
        "Kafka broker contract for Topic orders Region eu does not match its Source Definition. Kafka broker contract for Topic orders Region us is unavailable.",
      );
      expect(kafkaServerInternals.isKafkaRuntimeDefinition(forever)).toBe(true);
      expect(kafkaServerInternals.isKafkaRuntimeDefinition(null)).toBe(false);
      expect(kafkaServerInternals.isKafkaRuntimeDefinition({ cleanupPolicy: "delete" })).toBe(
        false,
      );
      expect(
        [
          { ...forever, topic: "" },
          { ...forever, regions: [] },
          { ...forever, retentionPolicy: null },
          { ...forever, retentionPolicy: { _tag: "Unknown" } },
          { ...forever, retentionPolicy: { _tag: "Finite", durationNanos: 0n } },
          Object.defineProperty({ ...forever }, "retentionPolicy", {
            enumerable: true,
            get: () => {
              throw new Error("hostile retention policy");
            },
          }),
        ].map(kafkaServerInternals.isKafkaRuntimeDefinition),
      ).toStrictEqual([false, false, false, false, false, false]);
      const duplicateContract = finiteBrokerContract();
      expect(() =>
        makeKafkaServerLayer({
          brokerContracts: [duplicateContract, duplicateContract],
          retentionSweepIntervalNanos: 1_000_000_000n,
          consumerGroupPrefix: "replica",
          regions: new Map(),
        }),
      ).toThrow("Kafka broker contract for Topic orders Region eu is duplicated.");
      const invalidResolvedContracts = [
        {
          ...duplicateContract,
          observedRetentionMs: -2n,
          resolvedRetention: { _tag: "Finite" as const, durationNanos: -2_000_000n },
        },
        {
          ...duplicateContract,
          observedCleanupPolicy: "compact" as const,
        },
        {
          ...duplicateContract,
          resolvedRetention: { _tag: "Finite" as const, durationNanos: 1n },
        },
      ];
      for (const brokerContract of invalidResolvedContracts) {
        expect(() =>
          makeKafkaServerLayer({
            brokerContracts: [brokerContract],
            retentionSweepIntervalNanos: 1_000_000_000n,
            consumerGroupPrefix: "replica",
            regions: new Map(),
          }),
        ).toThrow("Kafka broker contract must be a complete validated broker-resolution result.");
      }
      expect(() =>
        makeKafkaServerLayer({
          brokerContracts: [],
          retentionSweepIntervalNanos: 0n,
          consumerGroupPrefix: "replica",
          regions: new Map(),
        }),
      ).toThrow("Kafka retention sweep interval must be a positive bigint.");
      expect(() =>
        makeKafkaServerLayer({
          brokerContracts: [],
          retentionSweepIntervalNanos: 1_000_000_000n,
          consumerGroupPrefix: "",
          regions: new Map(),
        }),
      ).toThrow(KafkaSourceConfigurationError);
      expect(() =>
        makeKafkaServerLayer({
          brokerContracts: [
            {
              ...duplicateContract,
              viewServerTopic: "b",
            },
          ],
          retentionSweepIntervalNanos: 1_000_000_000n,
          consumerGroupPrefix: "a".repeat(32_765),
          regions: new Map(),
        }),
      ).not.toThrow();
      expect(() =>
        makeKafkaServerLayer({
          brokerContracts: [
            {
              ...duplicateContract,
              viewServerTopic: "b".repeat(32_766),
            },
          ],
          retentionSweepIntervalNanos: 1_000_000_000n,
          consumerGroupPrefix: "a",
          regions: new Map(),
        }),
      ).toThrow("Kafka derived consumer group ID exceeds the 32767-byte Kafka protocol limit.");
      const staticallyValidatedLayer = makeKafkaServerLayer({
        brokerContracts: [duplicateContract],
        retentionSweepIntervalNanos: 1_000_000_000n,
        consumerGroupPrefix: "replica",
        regions: new Map(),
      });
      yield* Effect.scoped(Layer.build(staticallyValidatedLayer));
    }),
  );

  it.effect(
    "releases keyed leases on interruption, duplicate release, and ownership transfer",
    () =>
      Effect.gen(function* () {
        const registry = kafkaServerInternals.makeKafkaKeyLeaseRegistry();
        const first = yield* registry.acquire("row");
        const waiting = yield* registry
          .acquire("row")
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Effect.yieldNow;
        expect(waiting.pollUnsafe()).toBeUndefined();
        const unrelated = yield* registry.acquire("other-row");
        yield* unrelated.release;
        yield* Fiber.interrupt(waiting);
        yield* first.release;
        yield* first.release;
        const next = yield* registry.acquire("row");
        yield* next.release;

        const queueHolder = yield* registry.acquire("queue-shapes");
        const queuedHead = yield* registry
          .acquire("queue-shapes")
          .pipe(Effect.forkChild({ startImmediately: true }));
        const queuedMiddle = yield* registry
          .acquire("queue-shapes")
          .pipe(Effect.forkChild({ startImmediately: true }));
        const queuedTail = yield* registry
          .acquire("queue-shapes")
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Effect.yieldNow;
        yield* Fiber.interrupt(queuedMiddle);
        yield* Fiber.interrupt(queuedTail);
        yield* Fiber.interrupt(queuedHead);
        yield* queueHolder.release;
        expect(registry.users("queue-shapes")).toBe(0);
        const afterQueueCancellation = yield* registry.acquire("queue-shapes");
        yield* afterQueueCancellation.release;

        const fifoOrder: Array<string> = [];
        const fifoHolder = yield* registry.acquire("fifo");
        const fifoFirst = yield* registry.acquire("fifo").pipe(
          Effect.tap(() => Effect.sync(() => fifoOrder.push("first"))),
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Effect.yieldNow;
        const fifoSecond = yield* registry.acquire("fifo").pipe(
          Effect.tap(() => Effect.sync(() => fifoOrder.push("second"))),
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Effect.yieldNow;
        const fifoThird = yield* registry.acquire("fifo").pipe(
          Effect.tap(() => Effect.sync(() => fifoOrder.push("third"))),
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Effect.yieldNow;
        yield* fifoHolder.release;
        const fifoFirstLease = yield* Fiber.join(fifoFirst);
        expect({
          order: fifoOrder,
          secondPending: fifoSecond.pollUnsafe() === undefined,
          thirdPending: fifoThird.pollUnsafe() === undefined,
        }).toStrictEqual({
          order: ["first"],
          secondPending: true,
          thirdPending: true,
        });
        yield* fifoFirstLease.release;
        const fifoSecondLease = yield* Fiber.join(fifoSecond);
        expect({
          order: fifoOrder,
          thirdPending: fifoThird.pollUnsafe() === undefined,
        }).toStrictEqual({
          order: ["first", "second"],
          thirdPending: true,
        });
        yield* fifoSecondLease.release;
        const fifoThirdLease = yield* Fiber.join(fifoThird);
        expect(fifoOrder).toStrictEqual(["first", "second", "third"]);
        yield* fifoThirdLease.release;
        expect(registry.users("fifo")).toBe(0);

        const held = yield* registry.acquire("contended");
        const contender = yield* registry
          .acquire("contended")
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Effect.yieldNow;
        yield* held.release;
        const acquiredContender = yield* Fiber.join(contender);
        yield* acquiredContender.release;

        const ownershipGranted = yield* Deferred.make<void>();
        const releaseGrantedWaiter = yield* Deferred.make<void>();
        const handoffRegistry = kafkaServerInternals.makeKafkaKeyLeaseRegistry(
          Deferred.succeed(ownershipGranted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseGrantedWaiter)),
          ),
        );
        const postGrantHolder = yield* handoffRegistry.acquire("post-grant-interruption");
        const postGrantWaiter = yield* handoffRegistry
          .acquire("post-grant-interruption")
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Effect.yieldNow;
        postGrantHolder.releaseNow();
        yield* Deferred.await(ownershipGranted);
        yield* Fiber.interrupt(postGrantWaiter);
        const postGrantExit = yield* Fiber.await(postGrantWaiter);
        expect({
          failed: Exit.isFailure(postGrantExit),
          interrupted:
            Exit.isFailure(postGrantExit) && Cause.hasInterruptsOnly(postGrantExit.cause),
          users: handoffRegistry.users("post-grant-interruption"),
        }).toStrictEqual({
          failed: true,
          interrupted: true,
          users: 0,
        });

        const waiterToInterrupt: {
          current?: {
            interruptUnsafe(): void;
          };
        } = {};
        const transferRegistry = kafkaServerInternals.makeKafkaKeyLeaseRegistry(Effect.void, () => {
          waiterToInterrupt.current?.interruptUnsafe();
        });
        const transferHolder = yield* transferRegistry.acquire("transfer-interruption");
        const transferWaiter = yield* transferRegistry
          .acquire("transfer-interruption")
          .pipe(Effect.forkChild({ startImmediately: true }));
        waiterToInterrupt.current = transferWaiter;
        yield* Effect.yieldNow;
        transferHolder.releaseNow();
        const transferExit = yield* Fiber.await(transferWaiter);
        expect({
          interrupted: Exit.isFailure(transferExit) && Cause.hasInterruptsOnly(transferExit.cause),
          users: transferRegistry.users("transfer-interruption"),
        }).toStrictEqual({
          interrupted: true,
          users: 0,
        });
        const afterTransferInterruption = yield* transferRegistry.acquire("transfer-interruption");
        yield* afterTransferInterruption.release;

        let failureReleaseCount = 0;
        const failedDelivery = yield* kafkaServerInternals
          .completeKafkaDelivery(Exit.fail("delivery failed"), {
            release: Effect.sync(() => {
              failureReleaseCount += 1;
            }),
          })
          .pipe(Effect.exit);
        expect({
          failed: Exit.isFailure(failedDelivery),
          failureReleaseCount,
          missingLeaseUsers: registry.users("missing"),
        }).toStrictEqual({
          failed: true,
          failureReleaseCount: 1,
          missingLeaseUsers: 0,
        });
      }),
  );

  it.effect(
    "rechunks one upstream batch so same-key deliveries, rejection, tombstone, and reinsertion converge",
    () =>
      Effect.gen(function* () {
        const settlements: Array<{ readonly offset: bigint; readonly succeeded: boolean }> = [];
        const record = (offset: bigint, value: string | null): KafkaServerRecord => ({
          key: bytes("same"),
          value: value === null ? null : bytes(value),
          metadata: metadata("eu", offset),
          settlement: (exit) =>
            Effect.sync(() => {
              settlements.push({
                offset,
                succeeded: Exit.isSuccess(exit),
              });
            }),
        });
        const records = [
          record(1n, JSON.stringify({ price: 1 })),
          record(2n, "{invalid-json"),
          ...Array.from({ length: 30 }, (_, index) =>
            record(BigInt(index + 3), JSON.stringify({ price: index + 3 })),
          ),
          record(33n, null),
          record(34n, JSON.stringify({ price: 34 })),
        ];
        const region: KafkaServerRegion = {
          acquire: () =>
            Effect.succeed({
              records: Stream.fromIterable(records).pipe(Stream.concat(Stream.never)),
              recordDecoded: Effect.void,
              recordDecodeFailure: Effect.void,
              recordMapped: Effect.void,
              recordMappingFailure: Effect.void,
              recordRejection: Effect.void,
            }),
          metrics: () =>
            Effect.succeed({
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
            }),
        };
        const config = defineViewServerConfig({
          topics: {
            orders: {
              schema: Order,
              source: kafka.source({
                cleanupPolicy: "compact-and-delete",
                retentionPolicy: "5 seconds",
                topic: "source-orders",
                regions: ["eu"],
                key: kafka.compactionKey.string(),
                value: kafka.json(() =>
                  Schema.toCodecJson(Schema.Struct({ price: Schema.Number })),
                ),
                map: ({ value, region }) => ({
                  price: value.price,
                  region: String(region),
                }),
                startFrom: "earliest",
              }),
            },
          },
        });
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(
            makeKafkaServerLayer({
              brokerContracts: [
                {
                  ...finiteBrokerContract(),
                  cleanupPolicy: "compact-and-delete",
                  observedCleanupPolicy: "compact-and-delete",
                },
              ],
              retentionSweepIntervalNanos: 900_000_000_000n,
              consumerGroupPrefix: "replica",
              regions: new Map([["eu", region]]),
            }),
          ),
        );

        yield* awaitCondition(() => settlements.length === records.length);
        expect(settlements).toStrictEqual(
          records.map((entry) => ({
            offset: entry.metadata.offset,
            succeeded: true,
          })),
        );
        expect(
          yield* runtime.client.snapshot("orders", {
            select: ["id", "price", "region"],
          }),
        ).toStrictEqual({
          rows: [{ id: "eu:0:kc2FtZQ", price: 34, region: "eu" }],
          totalRows: 1,
          version: 33,
          status: "ready",
          statusCode: "Ready",
        });

        yield* runtime.close;
      }),
  );

  it.effect(
    "closes an actual Source Attempt blocked on a keyed transition with no delivery, settlement, or fatal signal",
    () =>
      Effect.gen(function* () {
        let retentionState:
          | ReturnType<typeof kafkaServerInternals.makeKafkaRetentionState>
          | undefined;
        const applicationState = SourceAdapterServer.applicationState({
          sweepIntervalNanos: 900_000_000_000n,
          initialState: () => {
            const state = kafkaServerInternals.makeKafkaRetentionState("orders", [
              finiteBrokerContract(),
            ]);
            retentionState = state;
            return state;
          },
          reduce: kafkaServerInternals.reduceKafkaRetentionState,
          cancelledMaintenanceWorkIds: (state, command) =>
            command._tag === "AppliedUpsert" || command._tag === "AppliedDelete"
              ? kafkaServerInternals.retentionWorkForId(state, command.id)
              : [],
          acquireTransition: (state, command) =>
            kafkaServerInternals
              .acquireKafkaTransitionLease(state, command)
              .pipe(Effect.map((lease) => lease.releaseNow)),
          metrics: (state) =>
            new Map([["eu", kafkaServerInternals.retentionMetrics(state, "eu", 900_000_000_000n)]]),
          runDueSweep: kafkaServerInternals.runKafkaDueSweep,
        });
        const releaseDelivery = yield* Deferred.make<void>();
        const settlements: Array<SourceApplicationExit> = [];
        let acquisitions = 0;
        let finalizations = 0;
        let deliveries = 0;
        const layer = SourceAdapterServer.make(KafkaSourceAdapter, {
          materialized: {
            applicationState,
            initialLaneIds: () => ["eu"],
            acquire: (input) =>
              Effect.gen(function* () {
                acquisitions += 1;
                yield* Scope.addFinalizer(
                  yield* Effect.scope,
                  Effect.sync(() => {
                    finalizations += 1;
                  }),
                );
                const mutation = yield* input.toolkit.decodeUpsert({
                  id: "delivery-row",
                  price: 1,
                  region: "eu",
                });
                const state = applicationState.forLifetime(
                  input.lifetimeScope,
                  input.toolkit.topic,
                );
                const event = Deferred.await(releaseDelivery).pipe(
                  Effect.andThen(
                    state.prepare({
                      _tag: "AppliedUpsert",
                      id: "delivery-row",
                      region: "eu",
                      deadlineNanos: 10n,
                    }),
                  ),
                  Effect.flatMap((prepared) =>
                    input.toolkit.delivery(
                      mutation,
                      (applicationExit) =>
                        Effect.sync(() => {
                          settlements.push(applicationExit);
                        }),
                      prepared.transition,
                    ),
                  ),
                  Effect.tap(() =>
                    Effect.sync(() => {
                      deliveries += 1;
                    }),
                  ),
                );
                return SourceAdapterServer.attempt([
                  SourceAdapterServer.lane({
                    id: "eu",
                    events: Stream.fromEffect(event).pipe(Stream.concat(Stream.never)),
                  }),
                ]);
              }),
            metrics: (input): Effect.Effect<kafkaContract.KafkaMaterializedMetrics> => {
              const retention = Option.getOrThrow(
                Option.fromUndefinedOr(
                  applicationState
                    .forLifetime(input.lifetimeScope, input.topic)
                    .metrics()
                    .get("eu"),
                ),
              );
              return Effect.succeed({
                activeGroupId: "blocked-attempt",
                start: { _tag: "Pending" },
                regions: [
                  {
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
                    retention,
                  },
                ],
              });
            },
            retry: Schedule.forever,
          },
        });
        const config = defineViewServerConfig({
          topics: {
            orders: {
              schema: Order,
              source: kafka.source({
                cleanupPolicy: "delete",
                retentionPolicy: "5 seconds",
                topic: "source-orders",
                regions: ["eu"],
                key: kafka.string(),
                value: kafka.json(() =>
                  Schema.toCodecJson(Schema.Struct({ price: Schema.Number })),
                ),
                localRowKey: ({ key }) => key,
                map: ({ value, region }) => ({
                  price: value.price,
                  region: String(region),
                }),
                startFrom: "earliest",
              }),
            },
          },
        });
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(Effect.provide(layer));
        yield* awaitCondition(() => acquisitions === 1);
        const state = Option.getOrThrow(Option.fromUndefinedOr(retentionState));
        const holder = yield* kafkaServerInternals.acquireKafkaRetentionKeyLease(
          state,
          "delivery-row",
        );
        const fatal = yield* runtime.fatal.pipe(Effect.exit, Effect.forkChild());

        yield* Deferred.succeed(releaseDelivery, undefined);
        yield* awaitCondition(
          () => kafkaServerInternals.kafkaRetentionKeyLeaseUsers(state, "delivery-row") === 2,
        );
        yield* runtime.close;

        expect({
          acquisitions,
          deliveries,
          finalizations,
          settlements,
          fatalCompleted: fatal.pollUnsafe() !== undefined,
          users: kafkaServerInternals.kafkaRetentionKeyLeaseUsers(state, "delivery-row"),
        }).toStrictEqual({
          acquisitions: 1,
          deliveries: 0,
          finalizations: 1,
          settlements: [],
          fatalCompleted: false,
          users: 1,
        });
        yield* Fiber.interrupt(fatal);
        yield* holder.release;
        const probe = yield* kafkaServerInternals.acquireKafkaRetentionKeyLease(
          state,
          "delivery-row",
        );
        yield* probe.release;
      }),
  );

  it.effect(
    "cancels a blocked retention sweep with no maintenance effects and permits reacquisition",
    () =>
      Effect.gen(function* () {
        let sweepState = kafkaServerInternals.makeKafkaRetentionState("orders", [
          finiteBrokerContract(),
        ]);
        sweepState = kafkaServerInternals.reduceKafkaRetentionState(sweepState, {
          _tag: "AppliedUpsert",
          id: "sweep-row",
          region: "eu",
          deadlineNanos: 10n,
        });
        const sweepHolder = yield* kafkaServerInternals.acquireKafkaRetentionKeyLease(
          sweepState,
          "sweep-row",
        );
        const sweepScope = yield* Scope.make();
        let maintenanceOperations = 0;
        let maintenanceExecutions = 0;
        let maintenanceUpdates = 0;
        let failedWorkTransitions = 0;
        const sweepWaiter = yield* kafkaServerInternals
          .runKafkaDueSweep({
            epochNowNanos: 10n,
            state: sweepState,
            update: () => {
              maintenanceUpdates += 1;
            },
            operation: () => {
              maintenanceOperations += 1;
              return makeSourceMaintenanceOperation({
                topic: "orders",
                id: "sweep-row",
                workId: "sweep-row:1",
                lifetimeIdentity: kafkaTestLifetimeIdentity,
                isCurrent: () => true,
                onSuccess: () => {
                  failedWorkTransitions += 1;
                },
                onFailure: () => {
                  failedWorkTransitions += 1;
                },
                onStale: () => {
                  failedWorkTransitions += 1;
                },
              });
            },
            execute: () => {
              maintenanceExecutions += 1;
              return Effect.succeed({
                _tag: "Applied",
                exit: Exit.void,
              });
            },
          })
          .pipe(Effect.forkIn(sweepScope, { startImmediately: true }));
        yield* Effect.yieldNow;
        expect(sweepWaiter.pollUnsafe()).toBeUndefined();
        yield* Scope.close(sweepScope, Exit.void);
        const sweepExit = yield* Fiber.await(sweepWaiter);
        expect({
          interrupted: Exit.isFailure(sweepExit) && Cause.hasInterruptsOnly(sweepExit.cause),
          maintenanceOperations,
          maintenanceExecutions,
          maintenanceUpdates,
          failedWorkTransitions,
          users: kafkaServerInternals.kafkaRetentionKeyLeaseUsers(sweepState, "sweep-row"),
        }).toStrictEqual({
          interrupted: true,
          maintenanceOperations: 0,
          maintenanceExecutions: 0,
          maintenanceUpdates: 0,
          failedWorkTransitions: 0,
          users: 1,
        });
        yield* sweepHolder.release;
        const sweepProbe = yield* kafkaServerInternals.acquireKafkaRetentionKeyLease(
          sweepState,
          "sweep-row",
        );
        yield* sweepProbe.release;
      }),
  );

  it.effect("rechecks a lifetime generation after a due candidate waits for its keyed lease", () =>
    Effect.gen(function* () {
      let state = kafkaServerInternals.makeKafkaRetentionState("orders", [finiteBrokerContract()]);
      state = kafkaServerInternals.reduceKafkaRetentionState(state, {
        _tag: "AppliedUpsert",
        id: "row",
        region: "eu",
        deadlineNanos: 10n,
      });
      const firstGeneration = state.deadlines.get("row")?.generation;
      const ingestionLease = yield* kafkaServerInternals.acquireKafkaRetentionKeyLease(
        state,
        "row",
      );
      const deleted: Array<string> = [];
      const sweep = yield* kafkaServerInternals
        .runKafkaDueSweep({
          epochNowNanos: 10n,
          state,
          update: (command) => {
            state = kafkaServerInternals.reduceKafkaRetentionState(state, command);
          },
          operation: (input) =>
            makeSourceMaintenanceOperation({
              topic: state.topic,
              id: input.id,
              workId: input.workId,
              lifetimeIdentity: kafkaTestLifetimeIdentity,
              isCurrent: () => input.isCurrent(state),
              onSuccess: () => {
                state = kafkaServerInternals.reduceKafkaRetentionState(state, input.onSuccess);
              },
              onFailure: (exit) => {
                state = kafkaServerInternals.reduceKafkaRetentionState(
                  state,
                  input.onFailure(exit),
                );
              },
              onStale: () => {
                state = kafkaServerInternals.reduceKafkaRetentionState(state, input.onStale);
              },
            }),
          execute: (operation) =>
            Effect.sync(() => {
              const resolved = Option.getOrThrow(
                Option.fromUndefinedOr(resolveSourceMaintenanceOperation(operation)),
              );
              if (!resolved.isCurrent()) {
                resolved.onStale();
                return { _tag: "Stale" };
              }
              deleted.push(operation.id);
              resolved.onSuccess();
              return {
                _tag: "Applied",
                exit: Exit.void,
              };
            }),
        })
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* awaitCondition(
        () => kafkaServerInternals.kafkaRetentionKeyLeaseUsers(state, "row") === 2,
      );
      state = kafkaServerInternals.reduceKafkaRetentionState(state, {
        _tag: "AppliedDelete",
        id: "row",
        region: "eu",
        authoritativeExpired: false,
      });
      state = kafkaServerInternals.reduceKafkaRetentionState(state, {
        _tag: "AppliedUpsert",
        id: "row",
        region: "eu",
        deadlineNanos: 20n,
      });
      const secondGeneration = state.deadlines.get("row")?.generation;
      yield* ingestionLease.release;
      const outcome = yield* Fiber.join(sweep);

      expect({
        deleted,
        firstGeneration,
        outcome,
        retainedDeadline: state.deadlines.get("row"),
        secondGeneration,
      }).toStrictEqual({
        deleted: [],
        firstGeneration: 1n,
        outcome: { retryableFailuresByRegion: new Map() },
        retainedDeadline: {
          id: "row",
          region: "eu",
          deadlineNanos: 20n,
          generation: 2n,
        },
        secondGeneration: 2n,
      });
    }),
  );

  it.effect(
    "rejects retention state snapshots detached from their lifetime runtime resources",
    () =>
      Effect.gen(function* () {
        const state = kafkaServerInternals.makeKafkaRetentionState("orders", [
          finiteBrokerContract(),
        ]);
        const detachedState = { ...state };
        const command = {
          _tag: "AppliedDelete" as const,
          id: "row",
          region: "eu",
          authoritativeExpired: false,
        };

        expect(() =>
          kafkaServerInternals.reduceKafkaRetentionState(detachedState, command),
        ).toThrow(
          new kafkaContract.KafkaSourceConfigurationError(
            "Kafka retention state lost its lifetime generation sequence.",
          ),
        );
        expect(kafkaServerInternals.kafkaRetentionKeyLeaseUsers(detachedState, "row")).toBe(0);
        const leaseExit = yield* Effect.scoped(
          kafkaServerInternals.acquireKafkaRetentionKeyLease(detachedState, "row"),
        ).pipe(Effect.exit);
        const transitionExit = yield* Effect.scoped(
          kafkaServerInternals.acquireKafkaTransitionLease(detachedState, command),
        ).pipe(Effect.exit);
        const sweepExit = yield* kafkaServerInternals
          .runKafkaDueSweep({
            epochNowNanos: 10n,
            state: detachedState,
            update: () => undefined,
            operation: () => {
              throw new Error("detached sweep must fail before constructing work");
            },
            execute: () => Effect.die("detached sweep must fail before executing work"),
          })
          .pipe(Effect.exit);
        const defect = <Value>(exit: Exit.Exit<Value, never>): unknown =>
          Exit.isFailure(exit) ? Result.getOrThrow(Cause.findDefect(exit.cause)) : undefined;
        expect([defect(leaseExit), defect(transitionExit), defect(sweepExit)]).toStrictEqual([
          new kafkaContract.KafkaSourceConfigurationError(
            "Kafka retention state lost its lifetime generation sequence.",
          ),
          new kafkaContract.KafkaSourceConfigurationError(
            "Kafka retention state lost its lifetime generation sequence.",
          ),
          new kafkaContract.KafkaSourceConfigurationError(
            "Kafka retention state lost its lifetime generation sequence.",
          ),
        ]);
      }),
  );

  it("returns deeply immutable retention snapshots without retroactive state changes", () => {
    const initial = kafkaServerInternals.makeKafkaRetentionState("orders", [
      finiteBrokerContract(),
    ]);
    const afterUpsert = kafkaServerInternals.reduceKafkaRetentionState(initial, {
      _tag: "AppliedUpsert",
      id: "row",
      region: "eu",
      deadlineNanos: 10n,
    });
    const candidate = Option.getOrThrow(Option.fromUndefinedOr(afterUpsert.deadlines.get("row")));
    const afterFailure = kafkaServerInternals.reduceKafkaRetentionState(afterUpsert, {
      _tag: "ExpirationFailed",
      candidate,
      workId: kafkaServerInternals.retentionWorkId(candidate),
      failure: {
        region: "eu",
        topic: "orders",
        id: "row",
        generation: candidate.generation,
        failedAtNanos: 10n,
        message: "Kafka retention expiration Delete failed.",
      },
    });
    const afterSweep = kafkaServerInternals.reduceKafkaRetentionState(afterFailure, {
      _tag: "SweepCompleted",
      completedAtNanos: 10n,
      durationNanos: 2n,
      retryableFailuresByRegion: new Map([["eu", 1]]),
    });
    const afterDelete = kafkaServerInternals.reduceKafkaRetentionState(afterSweep, {
      _tag: "AppliedDelete",
      id: "row",
      region: "eu",
      authoritativeExpired: false,
    });
    const detachedDeadlines = afterUpsert.deadlines.set("other", {
      id: "other",
      region: "eu",
      deadlineNanos: 20n,
      generation: 99n,
    });
    const detachedFailedWork = Option.getOrThrow(
      Option.fromUndefinedOr(afterUpsert.regions.get("eu")),
    ).failedWork.add("detached");

    expect({
      frozen: {
        deadline: Object.isFrozen(candidate),
        index: Object.isFrozen(afterUpsert.deadlineIndex),
        initial: Object.isFrozen(initial),
        region: Object.isFrozen(afterUpsert.regions.get("eu")),
        upsert: Object.isFrozen(afterUpsert),
      },
      initial: {
        deadlines: initial.deadlines.size,
        metrics: kafkaServerInternals.retentionMetrics(initial, "eu", 1_000n),
        nextGeneration: initial.nextGeneration,
      },
      upsert: {
        deadline: afterUpsert.deadlines.get("row"),
        deadlines: afterUpsert.deadlines.size,
        detachedDeadlineCount: detachedDeadlines.size,
        detachedFailedWorkCount: detachedFailedWork.size,
        failedWorkCount: afterUpsert.regions.get("eu")?.failedWork.size,
        metrics: kafkaServerInternals.retentionMetrics(afterUpsert, "eu", 1_000n),
        nextGeneration: afterUpsert.nextGeneration,
      },
      failure: kafkaServerInternals.retentionMetrics(afterFailure, "eu", 1_000n),
      sweep: kafkaServerInternals.retentionMetrics(afterSweep, "eu", 1_000n),
      deleted: {
        deadlines: afterDelete.deadlines.size,
        nextGeneration: afterDelete.nextGeneration,
      },
    }).toStrictEqual({
      frozen: {
        deadline: true,
        index: true,
        initial: true,
        region: true,
        upsert: true,
      },
      initial: {
        deadlines: 0,
        metrics: retentionMetricsFixture({
          trackedRows: 0,
        }),
        nextGeneration: 0n,
      },
      upsert: {
        deadline: {
          id: "row",
          region: "eu",
          deadlineNanos: 10n,
          generation: 1n,
        },
        deadlines: 1,
        detachedDeadlineCount: 2,
        detachedFailedWorkCount: 1,
        failedWorkCount: 0,
        metrics: retentionMetricsFixture({
          trackedRows: 1,
        }),
        nextGeneration: 1n,
      },
      failure: retentionMetricsFixture({
        trackedRows: 1,
        failedWorkBacklog: 1,
        expirationRetryFailures: 1n,
        latestExpirationFailure: {
          region: "eu",
          topic: "orders",
          id: "row",
          generation: 1n,
          failedAtNanos: 10n,
          message: "Kafka retention expiration Delete failed.",
        },
      }),
      sweep: retentionMetricsFixture({
        trackedRows: 1,
        lastSweepRetryableFailures: 1,
        failedWorkBacklog: 1,
        expirationRetryFailures: 1n,
        latestExpirationFailure: {
          region: "eu",
          topic: "orders",
          id: "row",
          generation: 1n,
          failedAtNanos: 10n,
          message: "Kafka retention expiration Delete failed.",
        },
        lastSweepAtNanos: 10n,
        lastSweepDurationNanos: 2n,
        sweepIntervalNanos: 1_000n,
      }),
      deleted: {
        deadlines: 0,
        nextGeneration: 1n,
      },
    });
  });

  it("keeps the ordered retention index proportional to live deadlines under write churn", () => {
    const directDeadline = {
      id: "direct",
      region: "eu",
      deadlineNanos: 1n,
      generation: 1n,
    };
    const directIndex = new kafkaServerInternals.KafkaRetentionDeadlineIndex()
      .set(directDeadline)
      .set({
        ...directDeadline,
        generation: 2n,
      })
      .remove({
        id: "absent",
        region: "eu",
        deadlineNanos: 2n,
        generation: 1n,
      });
    let state = kafkaServerInternals.makeKafkaRetentionState("orders", [finiteBrokerContract()]);
    for (let generation = 1; generation <= 2_000; generation += 1) {
      state = kafkaServerInternals.reduceKafkaRetentionState(state, {
        _tag: "AppliedUpsert",
        id: "hot-row",
        region: "eu",
        deadlineNanos: BigInt(3_000 - generation),
      });
      expect(state.deadlineIndex.size).toBe(state.deadlines.size);
    }
    for (let row = 0; row < 128; row += 1) {
      state = kafkaServerInternals.reduceKafkaRetentionState(state, {
        _tag: "AppliedUpsert",
        id: `row-${String(row).padStart(3, "0")}`,
        region: "eu",
        deadlineNanos: BigInt((row * 37) % 131),
      });
    }
    for (let row = 0; row < 128; row += 3) {
      state = kafkaServerInternals.reduceKafkaRetentionState(state, {
        _tag: "AppliedDelete",
        id: `row-${String(row).padStart(3, "0")}`,
        region: "eu",
        authoritativeExpired: false,
      });
    }
    const ordered = kafkaServerInternals.retentionDeadlineOrder(state);

    expect({
      deadlineCount: state.deadlines.size,
      indexCount: state.deadlineIndex.size,
      orderedCount: ordered.length,
      direct: [...directIndex],
      retainedHotRow: ordered.filter(({ id }) => id === "hot-row"),
    }).toStrictEqual({
      deadlineCount: 86,
      indexCount: 86,
      orderedCount: 86,
      direct: [
        {
          id: "direct",
          region: "eu",
          deadlineNanos: 1n,
          generation: 2n,
        },
      ],
      retainedHotRow: [
        {
          id: "hot-row",
          region: "eu",
          deadlineNanos: 1_000n,
          generation: 2_000n,
        },
      ],
    });
  });

  it.effect(
    "aborts an inactive due sweep without changing prior failed-work or sweep metrics",
    () =>
      Effect.gen(function* () {
        let state = kafkaServerInternals.makeKafkaRetentionState("orders", [
          finiteBrokerContract(),
        ]);
        state = kafkaServerInternals.reduceKafkaRetentionState(state, {
          _tag: "AppliedUpsert",
          id: "failed-row",
          region: "eu",
          deadlineNanos: 10n,
        });
        const candidate = Option.getOrThrow(
          Option.fromUndefinedOr(state.deadlines.get("failed-row")),
        );
        state = kafkaServerInternals.reduceKafkaRetentionState(state, {
          _tag: "ExpirationFailed",
          candidate,
          workId: kafkaServerInternals.retentionWorkId(candidate),
          failure: {
            region: "eu",
            topic: "orders",
            id: "failed-row",
            generation: candidate.generation,
            failedAtNanos: 10n,
            message: "Kafka retention expiration Delete failed.",
          },
        });
        state = kafkaServerInternals.reduceKafkaRetentionState(state, {
          _tag: "SweepCompleted",
          completedAtNanos: 10n,
          durationNanos: 3n,
          retryableFailuresByRegion: new Map([["eu", 1]]),
        });
        const before = kafkaServerInternals.retentionMetrics(state, "eu", 1_000n);
        const snapshot = state;
        const executed: Array<string> = [];
        const outcome = yield* kafkaServerInternals.runKafkaDueSweep({
          epochNowNanos: 20n,
          state,
          update: (command) => {
            state = kafkaServerInternals.reduceKafkaRetentionState(state, command);
          },
          operation: (input) =>
            makeSourceMaintenanceOperation({
              topic: state.topic,
              id: input.id,
              workId: input.workId,
              lifetimeIdentity: kafkaTestLifetimeIdentity,
              isCurrent: () => input.isCurrent(state),
              onSuccess: () => {
                state = kafkaServerInternals.reduceKafkaRetentionState(state, input.onSuccess);
              },
              onFailure: (exit) => {
                state = kafkaServerInternals.reduceKafkaRetentionState(
                  state,
                  input.onFailure(exit),
                );
              },
              onStale: () => {
                state = kafkaServerInternals.reduceKafkaRetentionState(state, input.onStale);
              },
            }),
          execute: (operation) =>
            Effect.sync(() => {
              executed.push(operation.id);
              return {
                _tag: "Inactive",
              };
            }),
        });

        expect({
          executed,
          metrics: kafkaServerInternals.retentionMetrics(state, "eu", 1_000n),
          outcome,
          retainedSnapshot: state === snapshot,
        }).toStrictEqual({
          executed: ["failed-row"],
          metrics: before,
          outcome: { retryableFailuresByRegion: new Map() },
          retainedSnapshot: true,
        });
      }),
  );

  it("tracks failed expiration work by exact identity across retries and replacement", () => {
    let state = kafkaServerInternals.makeKafkaRetentionState("orders", [finiteBrokerContract()]);
    for (const id of ["first", "second"]) {
      state = kafkaServerInternals.reduceKafkaRetentionState(state, {
        _tag: "AppliedUpsert",
        id,
        region: "eu",
        deadlineNanos: 10n,
      });
    }
    const first = Option.getOrThrow(Option.fromUndefinedOr(state.deadlines.get("first")));
    const second = Option.getOrThrow(Option.fromUndefinedOr(state.deadlines.get("second")));
    const fail = (
      current: kafkaServerInternals.KafkaRetentionState,
      candidate: kafkaServerInternals.KafkaRetentionDeadline,
      failedAtNanos: bigint,
    ) =>
      kafkaServerInternals.reduceKafkaRetentionState(current, {
        _tag: "ExpirationFailed",
        candidate,
        workId: kafkaServerInternals.retentionWorkId(candidate),
        failure: {
          region: "eu",
          topic: "orders",
          id: candidate.id,
          generation: candidate.generation,
          failedAtNanos,
          message: "Kafka retention expiration Delete failed.",
        },
      });
    const observations: Array<readonly [number, bigint]> = [];
    for (const failedAtNanos of [11n, 12n, 13n]) {
      state = fail(state, first, failedAtNanos);
      const metrics = kafkaServerInternals.retentionMetrics(state, "eu", 1_000n);
      observations.push([metrics.failedWorkBacklog, metrics.expirationRetryFailures]);
    }
    state = fail(state, second, 14n);
    const afterDistinct = kafkaServerInternals.retentionMetrics(state, "eu", 1_000n);
    state = kafkaServerInternals.reduceKafkaRetentionState(state, {
      _tag: "ExpirationSucceeded",
      candidate: first,
      workId: kafkaServerInternals.retentionWorkId(first),
    });
    const afterIndependentClear = kafkaServerInternals.retentionMetrics(state, "eu", 1_000n);
    state = kafkaServerInternals.reduceKafkaRetentionState(state, {
      _tag: "AppliedUpsert",
      id: "second",
      region: "eu",
      deadlineNanos: 20n,
    });
    const replacement = Option.getOrThrow(Option.fromUndefinedOr(state.deadlines.get("second")));
    const afterReplacement = kafkaServerInternals.retentionMetrics(state, "eu", 1_000n);
    state = fail(state, replacement, 15n);
    const afterReplacementFailure = kafkaServerInternals.retentionMetrics(state, "eu", 1_000n);

    expect({
      afterDistinct: [afterDistinct.failedWorkBacklog, afterDistinct.expirationRetryFailures],
      afterIndependentClear: [
        afterIndependentClear.failedWorkBacklog,
        afterIndependentClear.expirationRetryFailures,
      ],
      afterReplacement: [
        afterReplacement.failedWorkBacklog,
        afterReplacement.expirationRetryFailures,
      ],
      afterReplacementFailure: {
        backlog: afterReplacementFailure.failedWorkBacklog,
        latest: afterReplacementFailure.latestExpirationFailure,
        retries: afterReplacementFailure.expirationRetryFailures,
      },
      observations,
      replacementGeneration: replacement.generation,
    }).toStrictEqual({
      afterDistinct: [2, 4n],
      afterIndependentClear: [1, 4n],
      afterReplacement: [0, 4n],
      afterReplacementFailure: {
        backlog: 1,
        latest: {
          region: "eu",
          topic: "orders",
          id: "second",
          generation: 3n,
          failedAtNanos: 15n,
          message: "Kafka retention expiration Delete failed.",
        },
        retries: 5n,
      },
      observations: [
        [1, 1n],
        [1, 2n],
        [1, 3n],
      ],
      replacementGeneration: 3n,
    });
  });

  it.effect("keeps every due generation retryable when a sweep is interrupted mid-batch", () =>
    Effect.gen(function* () {
      let state = kafkaServerInternals.makeKafkaRetentionState("orders", [finiteBrokerContract()]);
      for (const id of ["a", "b", "c"]) {
        state = kafkaServerInternals.reduceKafkaRetentionState(state, {
          _tag: "AppliedUpsert",
          id,
          region: "eu",
          deadlineNanos: 10n,
        });
      }
      const makeOperation: Parameters<
        typeof kafkaServerInternals.runKafkaDueSweep
      >[0]["operation"] = (input) =>
        makeSourceMaintenanceOperation({
          topic: state.topic,
          id: input.id,
          workId: input.workId,
          lifetimeIdentity: kafkaTestLifetimeIdentity,
          isCurrent: () => input.isCurrent(state),
          onSuccess: () => {
            state = kafkaServerInternals.reduceKafkaRetentionState(state, input.onSuccess);
          },
          onFailure: (exit) => {
            state = kafkaServerInternals.reduceKafkaRetentionState(state, input.onFailure(exit));
          },
          onStale: () => {
            state = kafkaServerInternals.reduceKafkaRetentionState(state, input.onStale);
          },
        });
      const executionStarted = yield* Deferred.make<void>();
      const executionGate = yield* Deferred.make<void>();
      const stateBeforeSweep = state;
      const interruptedSweep = yield* kafkaServerInternals
        .runKafkaDueSweep({
          epochNowNanos: 10n,
          state,
          update: (command) => {
            state = kafkaServerInternals.reduceKafkaRetentionState(state, command);
          },
          operation: makeOperation,
          execute: () =>
            Deferred.succeed(executionStarted, undefined).pipe(
              Effect.andThen(Deferred.await(executionGate)),
              Effect.as({ _tag: "Applied", exit: Exit.void }),
            ),
        })
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* Deferred.await(executionStarted);
      expect(state).toBe(stateBeforeSweep);
      expect(kafkaServerInternals.retentionDeadlineOrder(state).map(({ id }) => id)).toStrictEqual([
        "a",
        "b",
        "c",
      ]);
      yield* Fiber.interrupt(interruptedSweep);

      expect({
        deadlines: state.deadlines.size,
        heapIds: kafkaServerInternals.retentionDeadlineOrder(state).map(({ id }) => id),
      }).toStrictEqual({
        deadlines: 3,
        heapIds: ["a", "b", "c"],
      });

      const retried: Array<string> = [];
      yield* kafkaServerInternals.runKafkaDueSweep({
        epochNowNanos: 10n,
        state,
        update: (command) => {
          state = kafkaServerInternals.reduceKafkaRetentionState(state, command);
        },
        operation: makeOperation,
        execute: (operation) =>
          Effect.sync(() => {
            retried.push(operation.id);
            const internal = Option.getOrThrow(
              Option.fromUndefinedOr(resolveSourceMaintenanceOperation(operation)),
            );
            internal.onSuccess();
            return { _tag: "Applied", exit: Exit.void };
          }),
      });

      expect({
        deadlines: state.deadlines.size,
        heapSize: kafkaServerInternals.retentionDeadlineOrder(state).length,
        retried,
      }).toStrictEqual({
        deadlines: 0,
        heapSize: 0,
        retried: ["a", "b", "c"],
      });
    }),
  );

  it.effect(
    "keeps failed expiration retryable and protects reinserted rows with lifetime generations",
    () =>
      Effect.gen(function* () {
        let state = kafkaServerInternals.makeKafkaRetentionState("orders", [
          finiteBrokerContract(),
        ]);
        state = kafkaServerInternals.reduceKafkaRetentionState(state, {
          _tag: "AppliedUpsert",
          id: "row",
          region: "eu",
          deadlineNanos: 10n,
        });
        const firstGeneration = state.deadlines.get("row")?.generation;
        const makeOperation = (
          input: Parameters<
            Parameters<typeof kafkaServerInternals.runKafkaDueSweep>[0]["operation"]
          >[0],
        ) =>
          makeSourceMaintenanceOperation({
            topic: state.topic,
            id: input.id,
            workId: input.workId,
            lifetimeIdentity: kafkaTestLifetimeIdentity,
            isCurrent: () => input.isCurrent(state),
            onSuccess: () => {
              state = kafkaServerInternals.reduceKafkaRetentionState(state, input.onSuccess);
            },
            onFailure: (exit) => {
              state = kafkaServerInternals.reduceKafkaRetentionState(state, input.onFailure(exit));
            },
            onStale: () => {
              state = kafkaServerInternals.reduceKafkaRetentionState(state, input.onStale);
            },
          });
        const failureExit = Exit.fail({
          _tag: "InvalidSourceDelivery" as const,
          message: "Injected expiration Delete failure.",
        });
        const failureSweep = yield* kafkaServerInternals.runKafkaDueSweep({
          epochNowNanos: 10n,
          state,
          update: (command) => {
            state = kafkaServerInternals.reduceKafkaRetentionState(state, command);
          },
          operation: makeOperation,
          execute: (operation) =>
            Effect.sync(() => {
              const resolved = Option.getOrThrow(
                Option.fromUndefinedOr(resolveSourceMaintenanceOperation(operation)),
              );
              resolved.onFailure(failureExit);
              return {
                _tag: "Applied",
                exit: failureExit,
              };
            }),
        });
        const failedMetrics = kafkaServerInternals.retentionMetrics(state, "eu", 1_000_000_000n);

        const staleSweep = yield* kafkaServerInternals.runKafkaDueSweep({
          epochNowNanos: 10n,
          state,
          update: (command) => {
            state = kafkaServerInternals.reduceKafkaRetentionState(state, command);
          },
          operation: makeOperation,
          execute: (operation) =>
            Effect.sync(() => {
              state = kafkaServerInternals.reduceKafkaRetentionState(state, {
                _tag: "AppliedUpsert",
                id: "row",
                region: "eu",
                deadlineNanos: 20n,
              });
              const resolved = Option.getOrThrow(
                Option.fromUndefinedOr(resolveSourceMaintenanceOperation(operation)),
              );
              expect(resolved.isCurrent()).toBe(false);
              resolved.onStale();
              return {
                _tag: "Stale",
              };
            }),
        });
        const secondGeneration = state.deadlines.get("row")?.generation;
        const successSweep = yield* kafkaServerInternals.runKafkaDueSweep({
          epochNowNanos: 20n,
          state,
          update: (command) => {
            state = kafkaServerInternals.reduceKafkaRetentionState(state, command);
          },
          operation: makeOperation,
          execute: (operation) =>
            Effect.sync(() => {
              const resolved = Option.getOrThrow(
                Option.fromUndefinedOr(resolveSourceMaintenanceOperation(operation)),
              );
              expect(resolved.isCurrent()).toBe(true);
              resolved.onSuccess();
              return {
                _tag: "Applied",
                exit: Exit.succeed(undefined),
              };
            }),
        });
        let sortedState = kafkaServerInternals.makeKafkaRetentionState("orders", [
          finiteBrokerContract(),
        ]);
        for (const [id, deadlineNanos] of [
          ["b", 5n],
          ["a", 5n],
          ["later", 6n],
          ["first", 4n],
        ] as const) {
          sortedState = kafkaServerInternals.reduceKafkaRetentionState(sortedState, {
            _tag: "AppliedUpsert",
            id,
            region: "eu",
            deadlineNanos,
          });
        }
        const sortedIds: Array<string> = [];
        const sortedSweep = yield* kafkaServerInternals.runKafkaDueSweep({
          epochNowNanos: 6n,
          state: sortedState,
          update: (command) => {
            sortedState = kafkaServerInternals.reduceKafkaRetentionState(sortedState, command);
          },
          operation: (input) =>
            makeSourceMaintenanceOperation({
              topic: sortedState.topic,
              id: input.id,
              workId: input.workId,
              lifetimeIdentity: kafkaTestLifetimeIdentity,
              isCurrent: () => input.isCurrent(sortedState),
              onSuccess: () => {
                sortedState = kafkaServerInternals.reduceKafkaRetentionState(
                  sortedState,
                  input.onSuccess,
                );
              },
              onFailure: (exit) => {
                sortedState = kafkaServerInternals.reduceKafkaRetentionState(
                  sortedState,
                  input.onFailure(exit),
                );
              },
              onStale: () => {
                sortedState = kafkaServerInternals.reduceKafkaRetentionState(
                  sortedState,
                  input.onStale,
                );
              },
            }),
          execute: (operation) =>
            Effect.sync(() => {
              sortedIds.push(operation.id);
              const resolved = Option.getOrThrow(
                Option.fromUndefinedOr(resolveSourceMaintenanceOperation(operation)),
              );
              resolved.onSuccess();
              return {
                _tag: "Applied",
                exit: Exit.succeed(undefined),
              };
            }),
        });

        expect({
          failedMetrics,
          failureSweep,
          firstGeneration,
          secondGeneration,
          staleSweep,
          sortedIds,
          sortedSweep,
          successSweep,
          tracked: state.deadlines.size,
        }).toStrictEqual({
          failedMetrics: {
            declaredCleanupPolicy: "delete",
            observedCleanupPolicy: "delete",
            configuredRetention: {
              _tag: "Finite",
              durationNanos: 5_000_000_000n,
            },
            resolvedRetention: {
              _tag: "Finite",
              durationNanos: 5_000_000_000n,
            },
            trackedRows: 1,
            lastSweepRetryableFailures: 1,
            expiredRows: 0n,
            authoritativeExpiredDeletes: 0n,
            failedWorkBacklog: 1,
            expirationRetryFailures: 1n,
            latestExpirationFailure: {
              region: "eu",
              topic: "orders",
              id: "row",
              generation: 1n,
              failedAtNanos: 10n,
              message: "Kafka retention expiration Delete failed.",
            },
            lastSweepAtNanos: 0n,
            lastSweepDurationNanos: 0n,
            sweepIntervalNanos: 1_000_000_000n,
          },
          failureSweep: { retryableFailuresByRegion: new Map([["eu", 1]]) },
          firstGeneration: 1n,
          secondGeneration: 2n,
          staleSweep: { retryableFailuresByRegion: new Map() },
          sortedIds: ["first", "a", "b", "later"],
          sortedSweep: { retryableFailuresByRegion: new Map() },
          successSweep: { retryableFailuresByRegion: new Map() },
          tracked: 0,
        });
      }),
  );

  it.effect("defers and cancels deadlines created after a due sweep begins", () =>
    Effect.gen(function* () {
      let state = kafkaServerInternals.makeKafkaRetentionState("orders", [finiteBrokerContract()]);
      for (let index = 0; index < 257; index += 1) {
        state = kafkaServerInternals.reduceKafkaRetentionState(state, {
          _tag: "AppliedUpsert",
          id: `old-${String(index).padStart(3, "0")}`,
          region: "eu",
          deadlineNanos: 2n,
        });
      }
      let insertedFuture = false;
      const outcome = yield* kafkaServerInternals.runKafkaDueSweep({
        epochNowNanos: 2n,
        state,
        update: (command) => {
          state = kafkaServerInternals.reduceKafkaRetentionState(state, command);
        },
        operation: (input) =>
          makeSourceMaintenanceOperation({
            topic: state.topic,
            id: input.id,
            workId: input.workId,
            lifetimeIdentity: kafkaTestLifetimeIdentity,
            isCurrent: () => input.isCurrent(state),
            onSuccess: () => {
              state = kafkaServerInternals.reduceKafkaRetentionState(state, input.onSuccess);
            },
            onFailure: (exit) => {
              state = kafkaServerInternals.reduceKafkaRetentionState(state, input.onFailure(exit));
            },
            onStale: () => {
              state = kafkaServerInternals.reduceKafkaRetentionState(state, input.onStale);
            },
          }),
        execute: (operation) =>
          Effect.sync(() => {
            if (!insertedFuture) {
              insertedFuture = true;
              state = kafkaServerInternals.reduceKafkaRetentionState(state, {
                _tag: "AppliedUpsert",
                id: "future",
                region: "eu",
                deadlineNanos: 1n,
              });
            }
            if (operation.id === "old-256") {
              state = kafkaServerInternals.reduceKafkaRetentionState(state, {
                _tag: "AppliedDelete",
                id: "future",
                region: "eu",
                authoritativeExpired: false,
              });
            }
            const resolved = Option.getOrThrow(
              Option.fromUndefinedOr(resolveSourceMaintenanceOperation(operation)),
            );
            resolved.onSuccess();
            return {
              _tag: "Applied",
              exit: Exit.void,
            };
          }),
      });

      expect({
        deadlines: state.deadlines.size,
        insertedFuture,
        outcome,
      }).toStrictEqual({
        deadlines: 0,
        insertedFuture: true,
        outcome: { retryableFailuresByRegion: new Map() },
      });
    }),
  );

  it.effect("expires deadlines in chronological order after arbitrary indexed deletions", () =>
    Effect.gen(function* () {
      const expire = Effect.fn("KafkaSourceAdapter.test.retention.expire")(function* (
        state: kafkaServerInternals.KafkaRetentionState,
      ) {
        const expired: Array<string> = [];
        yield* kafkaServerInternals.runKafkaDueSweep({
          epochNowNanos: 100n,
          state,
          update: (command) => {
            state = kafkaServerInternals.reduceKafkaRetentionState(state, command);
          },
          operation: (input) =>
            makeSourceMaintenanceOperation({
              topic: state.topic,
              id: input.id,
              workId: input.workId,
              lifetimeIdentity: kafkaTestLifetimeIdentity,
              isCurrent: () => input.isCurrent(state),
              onSuccess: () => {
                state = kafkaServerInternals.reduceKafkaRetentionState(state, input.onSuccess);
              },
              onFailure: (exit) => {
                state = kafkaServerInternals.reduceKafkaRetentionState(
                  state,
                  input.onFailure(exit),
                );
              },
              onStale: () => {
                state = kafkaServerInternals.reduceKafkaRetentionState(state, input.onStale);
              },
            }),
          execute: (operation) =>
            Effect.sync(() => {
              expired.push(operation.id);
              Option.getOrThrow(
                Option.fromUndefinedOr(resolveSourceMaintenanceOperation(operation)),
              ).onSuccess();
              return {
                _tag: "Applied",
                exit: Exit.void,
              };
            }),
        });
        return expired;
      });
      const install = (
        state: kafkaServerInternals.KafkaRetentionState,
        entries: ReadonlyArray<readonly [string, bigint]>,
      ): kafkaServerInternals.KafkaRetentionState => {
        let installed = state;
        for (const [id, deadlineNanos] of entries) {
          installed = kafkaServerInternals.reduceKafkaRetentionState(installed, {
            _tag: "AppliedUpsert",
            id,
            region: "eu",
            deadlineNanos,
          });
        }
        return installed;
      };
      const remove = (state: kafkaServerInternals.KafkaRetentionState, id: string) =>
        kafkaServerInternals.reduceKafkaRetentionState(state, {
          _tag: "AppliedDelete",
          id,
          region: "eu",
          authoritativeExpired: false,
        });

      let siftDown = kafkaServerInternals.makeKafkaRetentionState("orders", [
        finiteBrokerContract(),
      ]);
      siftDown = install(siftDown, [
        ["root", 1n],
        ["left", 3n],
        ["right", 2n],
        ["leaf-a", 4n],
        ["leaf-b", 5n],
      ]);
      siftDown = remove(siftDown, "root");

      let siftUp = kafkaServerInternals.makeKafkaRetentionState("orders", [finiteBrokerContract()]);
      siftUp = install(siftUp, [
        ["root", 1n],
        ["left", 10n],
        ["right", 2n],
        ["left-a", 11n],
        ["removed", 12n],
        ["right-a", 3n],
        ["last", 4n],
      ]);
      siftUp = remove(siftUp, "removed");

      expect({
        afterRootDeletion: yield* expire(siftDown),
        afterInteriorDeletion: yield* expire(siftUp),
      }).toStrictEqual({
        afterRootDeletion: ["right", "left", "leaf-a", "leaf-b"],
        afterInteriorDeletion: ["root", "right", "right-a", "last", "left", "left-a"],
      });
    }),
  );

  it.effect(
    "uses epoch wall time for eligibility and monotonic time only for sweep duration",
    () => {
      let wallMillis = 2_000;
      let monotonicNanos = 100n;
      const clock: Clock.Clock = {
        currentTimeMillisUnsafe: () => wallMillis,
        currentTimeMillis: Effect.sync(() => wallMillis),
        currentTimeNanosUnsafe: () => monotonicNanos,
        currentTimeNanos: Effect.sync(() => {
          const current = monotonicNanos;
          monotonicNanos += 50n;
          return current;
        }),
        sleep: () => Effect.void,
      };
      return Effect.gen(function* () {
        let state = kafkaServerInternals.makeKafkaRetentionState("orders", [
          finiteBrokerContract(),
        ]);
        state = kafkaServerInternals.reduceKafkaRetentionState(state, {
          _tag: "AppliedUpsert",
          id: "epoch-row",
          region: "eu",
          deadlineNanos: 1_500_000_000n,
        });
        const deleted: Array<string> = [];
        const sweep = (epochNowNanos: bigint) =>
          kafkaServerInternals.runKafkaDueSweep({
            epochNowNanos,
            state,
            update: (command) => {
              state = kafkaServerInternals.reduceKafkaRetentionState(state, command);
            },
            operation: (input) =>
              makeSourceMaintenanceOperation({
                topic: state.topic,
                id: input.id,
                workId: input.workId,
                lifetimeIdentity: kafkaTestLifetimeIdentity,
                isCurrent: () => input.isCurrent(state),
                onSuccess: () => {
                  state = kafkaServerInternals.reduceKafkaRetentionState(state, input.onSuccess);
                },
                onFailure: (exit) => {
                  state = kafkaServerInternals.reduceKafkaRetentionState(
                    state,
                    input.onFailure(exit),
                  );
                },
                onStale: () => {
                  state = kafkaServerInternals.reduceKafkaRetentionState(state, input.onStale);
                },
              }),
            execute: (operation) =>
              Effect.sync(() => {
                deleted.push(operation.id);
                wallMillis += 500;
                const resolved = Option.getOrThrow(
                  Option.fromUndefinedOr(resolveSourceMaintenanceOperation(operation)),
                );
                resolved.onSuccess();
                return {
                  _tag: "Applied" as const,
                  exit: Exit.void,
                };
              }),
          });
        const epochNowNanos = yield* kafkaServerInternals.currentEpochNanos();
        yield* sweep(epochNowNanos);

        expect({
          deleted,
          epochNowNanos,
          metrics: kafkaServerInternals.retentionMetrics(state, "eu", 1_000n),
        }).toStrictEqual({
          deleted: ["epoch-row"],
          epochNowNanos: 2_000_000_000n,
          metrics: {
            declaredCleanupPolicy: "delete",
            observedCleanupPolicy: "delete",
            configuredRetention: {
              _tag: "Finite",
              durationNanos: 5_000_000_000n,
            },
            resolvedRetention: {
              _tag: "Finite",
              durationNanos: 5_000_000_000n,
            },
            trackedRows: 0,
            lastSweepRetryableFailures: 0,
            expiredRows: 1n,
            authoritativeExpiredDeletes: 0n,
            failedWorkBacklog: 0,
            expirationRetryFailures: 0n,
            latestExpirationFailure: null,
            lastSweepAtNanos: 2_500_000_000n,
            lastSweepDurationNanos: 50n,
            sweepIntervalNanos: 1_000n,
          },
        });

        state = kafkaServerInternals.reduceKafkaRetentionState(state, {
          _tag: "AppliedUpsert",
          id: "future-row",
          region: "eu",
          deadlineNanos: 5_000_000_000n,
        });
        monotonicNanos = 1_000_000_000_000n;
        const unchangedWallEpoch = yield* kafkaServerInternals.currentEpochNanos();
        yield* sweep(unchangedWallEpoch);
        expect({
          deleted,
          retained: state.deadlines.get("future-row"),
          sweepDuration: kafkaServerInternals.retentionMetrics(state, "eu", 1_000n)
            .lastSweepDurationNanos,
          unchangedWallEpoch,
        }).toStrictEqual({
          deleted: ["epoch-row"],
          retained: {
            id: "future-row",
            region: "eu",
            deadlineNanos: 5_000_000_000n,
            generation: 2n,
          },
          sweepDuration: 50n,
          unchangedWallEpoch: 2_500_000_000n,
        });

        wallMillis = 5_000;
        const advancedWallEpoch = yield* kafkaServerInternals.currentEpochNanos();
        yield* sweep(advancedWallEpoch);
        expect({
          advancedWallEpoch,
          deleted,
          retained: state.deadlines.get("future-row"),
        }).toStrictEqual({
          advancedWallEpoch: 5_000_000_000n,
          deleted: ["epoch-row", "future-row"],
          retained: undefined,
        });
      }).pipe(Effect.provideService(Clock.Clock, clock));
    },
  );

  it.effect("covers retention reducer transitions and registration validation exactly", () =>
    Effect.gen(function* () {
      let state = kafkaServerInternals.makeKafkaRetentionState("orders", [finiteBrokerContract()]);
      expect(kafkaServerInternals.retentionWorkForId(state, "missing")).toStrictEqual([]);
      state = kafkaServerInternals.reduceKafkaRetentionState(state, {
        _tag: "AppliedUpsert",
        id: "forever",
        region: "eu",
        deadlineNanos: null,
      });
      state = kafkaServerInternals.reduceKafkaRetentionState(state, {
        _tag: "AppliedUpsert",
        id: "delete",
        region: "eu",
        deadlineNanos: 10n,
      });
      const deleteDeadline = Option.getOrThrow(
        Option.fromUndefinedOr(state.deadlines.get("delete")),
      );
      expect(kafkaServerInternals.retentionWorkId(deleteDeadline)).toBe("delete\u00001");
      expect(kafkaServerInternals.retentionWorkForId(state, "delete")).toStrictEqual([
        "delete\u00001",
      ]);
      state = kafkaServerInternals.reduceKafkaRetentionState(state, {
        _tag: "AppliedDelete",
        id: "delete",
        region: "eu",
        authoritativeExpired: true,
      });
      state = kafkaServerInternals.reduceKafkaRetentionState(state, {
        _tag: "AppliedDelete",
        id: "missing",
        region: "missing",
        authoritativeExpired: true,
      });
      state = kafkaServerInternals.reduceKafkaRetentionState(state, {
        _tag: "SweepCompleted",
        completedAtNanos: 100n,
        durationNanos: 5n,
        retryableFailuresByRegion: new Map(),
      });
      const missingCandidate = {
        id: "missing",
        region: "missing",
        deadlineNanos: 1n,
        generation: 99n,
      };
      state = kafkaServerInternals.reduceKafkaRetentionState(state, {
        _tag: "ExpirationFailed",
        candidate: missingCandidate,
        workId: "missing\u000099",
        failure: {
          region: "missing",
          topic: "orders",
          id: "missing",
          generation: 99n,
          failedAtNanos: 100n,
          message: "Kafka retention expiration Delete failed.",
        },
      });
      state = kafkaServerInternals.reduceKafkaRetentionState(state, {
        _tag: "ExpirationStale",
        candidate: missingCandidate,
        workId: "missing\u000099",
      });
      state = kafkaServerInternals.reduceKafkaRetentionState(state, {
        _tag: "ExpirationSucceeded",
        candidate: missingCandidate,
        workId: "missing\u000099",
      });
      state = kafkaServerInternals.reduceKafkaRetentionState(state, {
        _tag: "AppliedUpsert",
        id: "missing-region",
        region: "missing",
        deadlineNanos: 200n,
      });
      const metricsWithOtherRegion = kafkaServerInternals.retentionMetrics(
        state,
        "eu",
        1_000_000_000n,
      );
      const currentMissingRegion = Option.getOrThrow(
        Option.fromUndefinedOr(state.deadlines.get("missing-region")),
      );
      state = kafkaServerInternals.reduceKafkaRetentionState(state, {
        _tag: "ExpirationSucceeded",
        candidate: currentMissingRegion,
        workId: kafkaServerInternals.retentionWorkId(currentMissingRegion),
      });

      const registration = kafkaServerInternals.makeKafkaApplicationStateRegistration(
        () => [finiteBrokerContract()],
        1_000_000_000n,
      );
      const registrationInternal = Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationStateRegistration(registration)),
      );
      const lifetimeScope = yield* Scope.make();
      expect(() =>
        Reflect.apply(registrationInternal.bind, registrationInternal, [
          {
            topic: "orders",
            definition: {},
            target: {},
            lifetimeScope,
          },
        ]),
      ).toThrow("Kafka Application State received an invalid Source Definition.");
      yield* Scope.close(lifetimeScope, Exit.succeed(undefined));

      const validLifetimeScope = yield* Scope.make();
      Reflect.apply(registrationInternal.bind, registrationInternal, [
        {
          topic: "orders",
          definition: makeSource("earliest").options,
          target: { _tag: "Materialized" },
          lifetimeScope: validLifetimeScope,
        },
      ]);
      const module = registration.forLifetime(validLifetimeScope, "orders");
      const invalidTransitionCause = yield* module
        .prepare({
          _tag: "SweepCompleted",
          completedAtNanos: 1n,
          durationNanos: 1n,
          retryableFailuresByRegion: new Map(),
        })
        .pipe(Effect.provideService(Scope.Scope, validLifetimeScope), Effect.sandbox, Effect.flip);
      expect(Cause.pretty(invalidTransitionCause)).toContain(
        "Kafka delivery transition requires an applied Upsert or Delete command.",
      );
      registrationInternal.unbind(validLifetimeScope);
      yield* Scope.close(validLifetimeScope, Exit.succeed(undefined));

      expect({
        metricsWithOtherRegion,
        metrics: kafkaServerInternals.retentionMetrics(state, "eu", 1_000_000_000n),
      }).toStrictEqual({
        metricsWithOtherRegion: {
          declaredCleanupPolicy: "delete",
          observedCleanupPolicy: "delete",
          configuredRetention: {
            _tag: "Finite",
            durationNanos: 5_000_000_000n,
          },
          resolvedRetention: {
            _tag: "Finite",
            durationNanos: 5_000_000_000n,
          },
          trackedRows: 0,
          lastSweepRetryableFailures: 0,
          expiredRows: 0n,
          authoritativeExpiredDeletes: 1n,
          failedWorkBacklog: 0,
          expirationRetryFailures: 0n,
          latestExpirationFailure: null,
          lastSweepAtNanos: 100n,
          lastSweepDurationNanos: 5n,
          sweepIntervalNanos: 1_000_000_000n,
        },
        metrics: {
          declaredCleanupPolicy: "delete",
          observedCleanupPolicy: "delete",
          configuredRetention: {
            _tag: "Finite",
            durationNanos: 5_000_000_000n,
          },
          resolvedRetention: {
            _tag: "Finite",
            durationNanos: 5_000_000_000n,
          },
          trackedRows: 0,
          lastSweepRetryableFailures: 0,
          expiredRows: 0n,
          authoritativeExpiredDeletes: 1n,
          failedWorkBacklog: 0,
          expirationRetryFailures: 0n,
          latestExpirationFailure: null,
          lastSweepAtNanos: 100n,
          lastSweepDurationNanos: 5n,
          sweepIntervalNanos: 1_000_000_000n,
        },
      });
    }),
  );

  it.effect("publishes exact configured Region lanes while acquisition is pending and ready", () =>
    Effect.gen(function* () {
      const acquisitionOrder: Array<string> = [];
      const eu = yield* makeFakeRegion("eu", acquisitionOrder);
      const us = yield* makeFakeRegion("us", acquisitionOrder);
      const continueAcquisition = yield* Deferred.make<void>();
      const pendingEu: KafkaServerRegion = {
        acquire: (input) =>
          Deferred.await(continueAcquisition).pipe(Effect.andThen(eu.runtime.acquire(input))),
        metrics: eu.runtime.metrics,
      };
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source: makeSource("earliest"),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(
          makeKafkaServerLayer({
            brokerContracts: foreverBrokerContracts(["eu", "us"]),
            retentionSweepIntervalNanos: 900_000_000_000n,
            consumerGroupPrefix: "replica",
            regions: new Map([
              ["eu", pendingEu],
              ["us", us.runtime],
            ]),
          }),
        ),
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
      const starting = Option.getOrThrow(
        yield* diagnostics.events.pipe(
          Stream.filter((health) => health.status._tag === "Starting"),
          Stream.take(1),
          Stream.runHead,
        ),
      );
      expect(starting.metrics.runtime.lanes).toStrictEqual([
        { id: "eu", buffer: { _tag: "Unbuffered" } },
        { id: "us", buffer: { _tag: "Unbuffered" } },
      ]);
      expect(starting.metrics.adapter.start).toStrictEqual({ _tag: "Pending" });

      yield* Deferred.succeed(continueAcquisition, undefined);
      const ready = Option.getOrThrow(
        yield* diagnostics.events.pipe(
          Stream.filter((health) => health.status._tag === "Ready"),
          Stream.take(1),
          Stream.runHead,
        ),
      );
      expect(ready.metrics.runtime.lanes).toStrictEqual([
        { id: "eu", buffer: { _tag: "Unbuffered" } },
        { id: "us", buffer: { _tag: "Unbuffered" } },
      ]);
      expect(ready.metrics.adapter.start).toStrictEqual({ _tag: "Pending" });
      yield* TestClock.adjust("1 second");
      const refreshed = Option.getOrThrow(
        yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
      );
      expect(refreshed.status._tag).toBe("Ready");
      expect(refreshed.metrics.runtime.lanes).toStrictEqual([
        { id: "eu", buffer: { _tag: "Unbuffered" } },
        { id: "us", buffer: { _tag: "Unbuffered" } },
      ]);
      expect(refreshed.metrics.adapter.start).toStrictEqual({
        _tag: "Resolved",
        position: { mode: "earliest" },
      });
      yield* diagnostics.close();
      yield* runtime.close;
    }),
  );

  it.effect(
    "runs delete-only materialization across Regions and settles poison and null records",
    () =>
      Effect.gen(function* () {
        const acquisitionOrder: Array<string> = [];
        const eu = yield* makeFakeRegion("eu", acquisitionOrder);
        const us = yield* makeFakeRegion("us", acquisitionOrder);
        const config = defineViewServerConfig({
          topics: {
            orders: {
              schema: Order,
              source: makeSource("earliest"),
            },
          },
        });
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(
            makeKafkaServerLayer({
              brokerContracts: foreverBrokerContracts(["eu", "us"]),
              retentionSweepIntervalNanos: 900_000_000_000n,
              consumerGroupPrefix: "replica / primary",
              regions: new Map([
                ["eu", eu.runtime],
                ["us", us.runtime],
              ]),
            }),
          ),
        );
        yield* eu.awaitAcquisitions(1);
        yield* us.awaitAcquisitions(1);

        yield* eu.offer({
          key: "shared",
          value: JSON.stringify({ price: 10 }),
          offset: 1n,
        });
        yield* us.offer({
          key: "shared",
          value: JSON.stringify({ price: 20 }),
          offset: 1n,
        });
        yield* eu.offer({
          key: "poison",
          value: "{",
          offset: 2n,
        });
        yield* eu.offer({
          key: "second",
          value: JSON.stringify({ price: 30 }),
          offset: 3n,
        });
        yield* us.offer({
          key: "shared",
          value: null,
          offset: 2n,
        });
        yield* awaitCondition(() => eu.commits.length === 3 && us.commits.length === 2);

        const snapshot = yield* runtime.client.snapshot("orders", {
          select: ["id", "price", "region"],
          orderBy: [{ field: "id", direction: "asc" }],
        });
        expect(snapshot).toStrictEqual({
          rows: [
            { id: "eu:0:second", price: 30, region: "eu" },
            { id: "eu:0:shared", price: 10, region: "eu" },
            { id: "us:0:shared", price: 20, region: "us" },
          ],
          totalRows: 3,
          version: 3,
          status: "ready",
          statusCode: "Ready",
        });
        expect({
          acquisitionOrder,
          euCommits: eu.commits,
          usCommits: us.commits,
        }).toStrictEqual({
          acquisitionOrder: ["eu:1", "us:1"],
          euCommits: [1n, 2n, 3n],
          usCommits: [1n, 2n],
        });

        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
        const health = Option.getOrThrow(
          yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
        );
        expect(health.status._tag).toBe("Degraded");
        const degraded = Option.getOrThrow(
          Option.liftPredicate(health.status, (status) => status._tag === "Degraded"),
        );
        const rejection = Option.getOrThrow(
          Option.liftPredicate(
            degraded.reasons[0],
            (reason) => reason._tag === "SourceItemRejection",
          ),
        );
        expect(rejection.latestRejection.location).toStrictEqual({
          region: "us",
          topic: "source-orders",
          partition: 0,
          offset: 2n,
          phase: "nullValue",
          message: "Delete-only Kafka source records require a non-null value.",
        });
        yield* TestClock.adjust("1 second");
        const refreshedHealth = Option.getOrThrow(
          yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
        );
        expect(refreshedHealth.metrics.adapter).toStrictEqual({
          activeGroupId: "replica%20%2F%20primary:orders",
          start: {
            _tag: "Resolved",
            position: { mode: "earliest" },
          },
          regions: [
            {
              region: "eu",
              assignments: [],
              commits: 3n,
              commitFailures: 0n,
              decoded: 2n,
              decodeFailures: 1n,
              mapped: 2n,
              mappingFailures: 0n,
              rejections: 1n,
              reconnects: 0n,
              rebalances: 0n,
              closes: 0n,
              closeFailures: 0n,
              retention: foreverRetentionMetrics(),
            },
            {
              region: "us",
              assignments: [],
              commits: 2n,
              commitFailures: 0n,
              decoded: 1n,
              decodeFailures: 1n,
              mapped: 1n,
              mappingFailures: 0n,
              rejections: 1n,
              reconnects: 0n,
              rebalances: 0n,
              closes: 0n,
              closeFailures: 0n,
              retention: foreverRetentionMetrics(),
            },
          ],
        });

        yield* diagnostics.close();
        yield* runtime.close;
        expect({
          eu: eu.counts(),
          us: us.counts(),
        }).toStrictEqual({
          eu: { acquisitions: 1, finalizations: 1 },
          us: { acquisitions: 1, finalizations: 1 },
        });
      }),
  );

  it.effect(
    "uses exact serialized key bytes for compaction identity and applies tombstones as keyed Deletes",
    () =>
      Effect.gen(function* () {
        const acquisitionOrder: Array<string> = [];
        const eu = yield* makeFakeRegion("eu", acquisitionOrder);
        const config = defineViewServerConfig({
          topics: {
            orders: {
              schema: Order,
              source: kafka.source({
                cleanupPolicy: "compact-and-delete",
                retentionPolicy: "5 seconds",
                topic: "source-orders",
                regions: ["eu"],
                key: kafka.compactionKey.codec({
                  name: "same-logical-key",
                  decode: ({ bytes }) =>
                    bytes[0] === 33
                      ? Effect.fail({ _tag: "UndecodableKey" } as const)
                      : Effect.succeed("logical-key"),
                }),
                value: kafka.json(() =>
                  Schema.toCodecJson(Schema.Struct({ price: Schema.Number })),
                ),
                map: compactionFaultMap,
                startFrom: "earliest",
              }),
            },
          },
        });
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(
            makeKafkaServerLayer({
              brokerContracts: [
                {
                  ...finiteBrokerContract(),
                  cleanupPolicy: "compact-and-delete",
                  observedCleanupPolicy: "compact-and-delete",
                },
              ],
              retentionSweepIntervalNanos: 1_000_000_000n,
              consumerGroupPrefix: "replica",
              regions: new Map([["eu", eu.runtime]]),
            }),
          ),
        );
        yield* eu.awaitAcquisitions(1);
        yield* eu.offer({
          key: "same",
          value: JSON.stringify({ price: 1 }),
          offset: 1n,
        });
        yield* eu.offer({
          key: "same",
          value: JSON.stringify({ price: 2 }),
          offset: 2n,
        });
        yield* eu.offer({
          key: "same",
          value: JSON.stringify({ price: 3 }),
          offset: 3n,
          partition: 1,
        });
        yield* eu.offer({
          key: "left",
          value: JSON.stringify({ price: 4 }),
          offset: 4n,
        });
        yield* eu.offer({
          key: "right",
          value: JSON.stringify({ price: 5 }),
          offset: 5n,
          timestampNanos: 10_000_000_000n,
        });
        yield* awaitCondition(() => eu.commits.length === 5);
        expect(
          yield* runtime.client.snapshot("orders", {
            select: ["id", "price", "region"],
            orderBy: [{ field: "id", direction: "asc" }],
          }),
        ).toStrictEqual({
          rows: [
            { id: "eu:0:kbGVmdA", price: 4, region: "eu" },
            { id: "eu:0:kc2FtZQ", price: 2, region: "eu" },
            { id: "eu:0:kcmlnaHQ", price: 5, region: "eu" },
            { id: "eu:1:kc2FtZQ", price: 3, region: "eu" },
          ],
          totalRows: 4,
          version: 5,
          status: "ready",
          statusCode: "Ready",
        });

        yield* eu.offer({
          key: "mapper-id",
          value: JSON.stringify({ price: 90 }),
          offset: 6n,
        });
        yield* eu.offer({
          key: "mapper-proxy",
          value: JSON.stringify({ price: 91 }),
          offset: 7n,
        });
        yield* awaitCondition(() => eu.commits.length === 7);
        expect(
          yield* runtime.client.snapshot("orders", {
            select: ["id", "price", "region"],
            orderBy: [{ field: "id", direction: "asc" }],
          }),
        ).toStrictEqual({
          rows: [
            { id: "eu:0:kbGVmdA", price: 4, region: "eu" },
            { id: "eu:0:kc2FtZQ", price: 2, region: "eu" },
            { id: "eu:0:kcmlnaHQ", price: 5, region: "eu" },
            { id: "eu:1:kc2FtZQ", price: 3, region: "eu" },
          ],
          totalRows: 4,
          version: 5,
          status: "ready",
          statusCode: "Ready",
        });

        yield* eu.offer({
          key: "left",
          value: null,
          offset: 8n,
        });
        yield* eu.offer({
          key: "same",
          value: null,
          offset: 9n,
        });
        yield* eu.offer({
          key: null,
          value: null,
          offset: 10n,
        });
        yield* eu.offer({
          key: "!",
          value: null,
          offset: 11n,
        });
        yield* awaitCondition(() => eu.commits.length === 11);
        expect(
          yield* runtime.client.snapshot("orders", {
            select: ["id", "price", "region"],
            orderBy: [{ field: "id", direction: "asc" }],
          }),
        ).toStrictEqual({
          rows: [
            { id: "eu:0:kcmlnaHQ", price: 5, region: "eu" },
            { id: "eu:1:kc2FtZQ", price: 3, region: "eu" },
          ],
          totalRows: 2,
          version: 7,
          status: "ready",
          statusCode: "Ready",
        });
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
        const health = Option.getOrThrow(
          yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
        );
        const degraded = Option.getOrThrow(
          Option.liftPredicate(health.status, (status) => status._tag === "Degraded"),
        );
        expect(
          Option.getOrThrow(
            Option.liftPredicate(
              degraded.reasons[0],
              (reason) => reason._tag === "SourceItemRejection",
            ),
          ).latestRejection.location,
        ).toStrictEqual({
          region: "eu",
          topic: "source-orders",
          partition: 0,
          offset: 10n,
          phase: "keyDecode",
          message: "Kafka record key is required.",
        });

        yield* TestClock.adjust("6 seconds");
        expect(
          yield* runtime.client.snapshot("orders", {
            select: ["id", "price", "region"],
            orderBy: [{ field: "id", direction: "asc" }],
          }),
        ).toStrictEqual({
          rows: [{ id: "eu:0:kcmlnaHQ", price: 5, region: "eu" }],
          totalRows: 1,
          version: 8,
          status: "ready",
          statusCode: "Ready",
        });
        yield* TestClock.adjust("10 seconds");
        expect(
          yield* runtime.client.snapshot("orders", {
            select: ["id", "price", "region"],
            orderBy: [{ field: "id", direction: "asc" }],
          }),
        ).toStrictEqual({
          rows: [],
          totalRows: 0,
          version: 9,
          status: "ready",
          statusCode: "Ready",
        });
        yield* diagnostics.close();
        yield* runtime.close;
      }),
  );

  it.effect("applies compaction tombstones without retention state when retention is forever", () =>
    Effect.gen(function* () {
      const acquisitionOrder: Array<string> = [];
      const eu = yield* makeFakeRegion("eu", acquisitionOrder);
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source: kafka.source({
              cleanupPolicy: "compact",
              retentionPolicy: "Infinity",
              topic: "source-orders",
              regions: ["eu"],
              key: kafka.compactionKey.string(),
              value: kafka.json(() => Schema.toCodecJson(Schema.Struct({ price: Schema.Number }))),
              map: ({ value, region }) => ({
                price: value.price,
                region: String(region),
              }),
              startFrom: "earliest",
            }),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(
          makeKafkaServerLayer({
            brokerContracts: foreverBrokerContracts(["eu"], "compact"),
            retentionSweepIntervalNanos: 1_000_000_000n,
            consumerGroupPrefix: "replica",
            regions: new Map([["eu", eu.runtime]]),
          }),
        ),
      );
      yield* eu.awaitAcquisitions(1);
      yield* eu.offer({
        key: "forever-key",
        value: JSON.stringify({ price: 42 }),
        offset: 1n,
      });
      yield* eu.offer({
        key: "forever-key",
        value: null,
        offset: 2n,
      });
      yield* awaitCondition(() => eu.commits.length === 2);

      expect(
        yield* runtime.client.snapshot("orders", {
          select: ["id", "price", "region"],
        }),
      ).toStrictEqual({
        rows: [],
        totalRows: 0,
        version: 2,
        status: "ready",
        statusCode: "Ready",
      });
      yield* runtime.close;
    }),
  );

  it.effect(
    "resolves matched zero against epoch record time while sweep cadence remains monotonic",
    () =>
      Effect.gen(function* () {
        const monotonicClock = yield* TestClock.testClockWith(Effect.succeed);
        let wallMillis = 2_000;
        const dualClock: TestClock.TestClock = {
          currentTimeMillisUnsafe: () => wallMillis,
          currentTimeMillis: Effect.sync(() => wallMillis),
          currentTimeNanosUnsafe: () => monotonicClock.currentTimeNanosUnsafe(),
          currentTimeNanos: monotonicClock.currentTimeNanos,
          sleep: (duration) => monotonicClock.sleep(duration),
          adjust: (duration) => monotonicClock.adjust(duration),
          setTime: (timestamp) => monotonicClock.setTime(timestamp),
          withLive: (effect) => monotonicClock.withLive(effect),
        };
        return yield* Effect.gen(function* () {
          const acquisitionOrder: Array<string> = [];
          const eu = yield* makeFakeRegion("eu", acquisitionOrder);
          const config = defineViewServerConfig({
            topics: {
              orders: {
                schema: Order,
                source: kafka.source({
                  cleanupPolicy: "delete",
                  retentionPolicy: "match-kafka-retention",
                  topic: "source-orders",
                  regions: ["eu"],
                  key: kafka.string(),
                  value: kafka.json(() =>
                    Schema.toCodecJson(Schema.Struct({ price: Schema.Number })),
                  ),
                  localRowKey: ({ key }) => key,
                  map: ({ value, region }) => ({
                    price: value.price,
                    region: String(region),
                  }),
                  startFrom: "earliest",
                }),
              },
            },
          });
          const resolvedRetention = resolveKafkaRetention(
            "delete",
            { _tag: "MatchKafkaRetention" },
            0n,
          );
          const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
            Effect.provide(
              makeKafkaServerLayer({
                brokerContracts: [
                  {
                    viewServerTopic: "orders",
                    sourceTopic: "source-orders",
                    region: "eu",
                    cleanupPolicy: "delete",
                    retentionPolicy: {
                      _tag: "MatchKafkaRetention",
                    },
                    observedCleanupPolicy: "delete",
                    observedRetentionMs: 0n,
                    resolvedRetention,
                  },
                ],
                retentionSweepIntervalNanos: 1_000_000_000n,
                consumerGroupPrefix: "replica",
                regions: new Map([["eu", eu.runtime]]),
              }),
            ),
          );
          yield* eu.awaitAcquisitions(1);
          yield* eu.offer({
            key: "past",
            value: JSON.stringify({ price: 1 }),
            offset: 1n,
            timestampNanos: 1_000_000_000n,
          });
          yield* eu.offer({
            key: "current",
            value: JSON.stringify({ price: 2 }),
            offset: 2n,
            timestampNanos: 2_000_000_000n,
          });
          yield* eu.offer({
            key: "future",
            value: JSON.stringify({ price: 3 }),
            offset: 3n,
            timestampNanos: 3_000_000_000n,
          });
          yield* awaitCondition(() => eu.commits.length === 3);

          expect({
            resolvedRetention,
            snapshot: yield* runtime.client.snapshot("orders", {
              select: ["id", "price"],
            }),
          }).toStrictEqual({
            resolvedRetention: {
              _tag: "Finite",
              durationNanos: 0n,
            },
            snapshot: {
              rows: [{ id: "eu:0:future", price: 3 }],
              totalRows: 1,
              version: 1,
              status: "ready",
              statusCode: "Ready",
            },
          });

          yield* TestClock.adjust("1 second");
          expect(
            yield* runtime.client.snapshot("orders", {
              select: ["id", "price"],
            }),
          ).toStrictEqual({
            rows: [{ id: "eu:0:future", price: 3 }],
            totalRows: 1,
            version: 1,
            status: "ready",
            statusCode: "Ready",
          });

          wallMillis = 3_000;
          expect(
            yield* runtime.client.snapshot("orders", {
              select: ["id", "price"],
            }),
          ).toStrictEqual({
            rows: [{ id: "eu:0:future", price: 3 }],
            totalRows: 1,
            version: 1,
            status: "ready",
            statusCode: "Ready",
          });
          yield* TestClock.adjust("1 second");
          yield* awaitEffectCondition("matched-zero future expiration", () =>
            runtime.client
              .snapshot("orders", {
                select: ["id"],
              })
              .pipe(Effect.map((snapshot) => snapshot.totalRows === 0)),
          );

          const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({
            topic: "orders",
          });
          const health = Option.getOrThrow(
            yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
          );
          const retention = Option.getOrThrow(
            Option.fromNullishOr(health.metrics.adapter.regions[0]?.retention),
          );
          const { lastSweepAtNanos, lastSweepDurationNanos, ...stableRetention } = retention;
          expect({
            snapshot: yield* runtime.client.snapshot("orders", {
              select: ["id"],
            }),
            stableRetention,
          }).toStrictEqual({
            snapshot: {
              rows: [],
              totalRows: 0,
              version: 2,
              status: "ready",
              statusCode: "Ready",
            },
            stableRetention: {
              declaredCleanupPolicy: "delete",
              observedCleanupPolicy: "delete",
              configuredRetention: {
                _tag: "MatchKafkaRetention",
              },
              resolvedRetention: {
                _tag: "Finite",
                durationNanos: 0n,
              },
              trackedRows: 0,
              lastSweepRetryableFailures: 0,
              expiredRows: 1n,
              authoritativeExpiredDeletes: 2n,
              failedWorkBacklog: 0,
              expirationRetryFailures: 0n,
              latestExpirationFailure: null,
              sweepIntervalNanos: 1_000_000_000n,
            },
          });
          expect(lastSweepAtNanos).toBe(3_000_000_000n);
          expect(lastSweepDurationNanos).not.toBeNull();
          expect(lastSweepDurationNanos ?? -1n).toBeGreaterThanOrEqual(0n);
          yield* diagnostics.close();
          yield* runtime.close;
        }).pipe(Effect.provideService(Clock.Clock, dualClock));
      }),
  );

  it.effect(
    "expires finite rows from Kafka record time, refreshes deadlines, and deletes already-expired records",
    () =>
      Effect.gen(function* () {
        const acquisitionOrder: Array<string> = [];
        const eu = yield* makeFakeRegion("eu", acquisitionOrder);
        const config = defineViewServerConfig({
          topics: {
            orders: {
              schema: Order,
              source: kafka.source({
                cleanupPolicy: "delete",
                retentionPolicy: "5 seconds",
                topic: "source-orders",
                regions: ["eu"],
                key: kafka.string(),
                value: kafka.json(() =>
                  Schema.toCodecJson(Schema.Struct({ price: Schema.Number })),
                ),
                localRowKey: ({ key }) => key,
                map: ({ value, region }) => ({
                  price: value.price,
                  region: String(region),
                }),
                startFrom: "earliest",
              }),
            },
          },
        });
        const contract = {
          viewServerTopic: "orders",
          sourceTopic: "source-orders",
          region: "eu",
          cleanupPolicy: "delete" as const,
          retentionPolicy: {
            _tag: "Finite" as const,
            durationNanos: 5_000_000_000n,
          },
          observedCleanupPolicy: "delete" as const,
          observedRetentionMs: 5_000n,
          resolvedRetention: {
            _tag: "Finite" as const,
            durationNanos: 5_000_000_000n,
          },
        };
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(
            makeKafkaServerLayer({
              brokerContracts: [contract],
              retentionSweepIntervalNanos: 1_000_000_000n,
              consumerGroupPrefix: "replica",
              regions: new Map([["eu", eu.runtime]]),
            }),
          ),
        );
        contract.sourceTopic = "mutated-after-layer-acquisition";
        contract.observedRetentionMs = 50_000n;
        contract.retentionPolicy.durationNanos = 50_000_000_000n;
        contract.resolvedRetention.durationNanos = 50_000_000_000n;
        yield* eu.awaitAcquisitions(1);
        yield* eu.offer({
          key: "expiring",
          value: JSON.stringify({ price: 1 }),
          offset: 1n,
          timestampNanos: 0n,
        });
        yield* awaitCondition(() => eu.commits.length === 1);
        expect(
          yield* runtime.client.snapshot("orders", {
            select: ["id", "price"],
          }),
        ).toStrictEqual({
          rows: [{ id: "eu:0:expiring", price: 1 }],
          totalRows: 1,
          version: 1,
          status: "ready",
          statusCode: "Ready",
        });

        yield* TestClock.adjust("4 seconds");
        expect(
          yield* runtime.client.snapshot("orders", {
            select: ["id"],
          }),
        ).toStrictEqual({
          rows: [{ id: "eu:0:expiring" }],
          totalRows: 1,
          version: 1,
          status: "ready",
          statusCode: "Ready",
        });
        yield* TestClock.adjust("1 second");
        yield* awaitEffectCondition("first expiration", () =>
          runtime.client
            .snapshot("orders", {
              select: ["id"],
            })
            .pipe(Effect.map((snapshot) => snapshot.totalRows === 0)),
        );
        expect(
          yield* runtime.client.snapshot("orders", {
            select: ["id"],
          }),
        ).toStrictEqual({
          rows: [],
          totalRows: 0,
          version: 2,
          status: "ready",
          statusCode: "Ready",
        });

        yield* eu.offer({
          key: "already-expired",
          value: JSON.stringify({ price: 2 }),
          offset: 2n,
          timestampNanos: 0n,
        });
        yield* awaitCondition(() => eu.commits.length === 2);
        expect(
          yield* runtime.client.snapshot("orders", {
            select: ["id"],
          }),
        ).toStrictEqual({
          rows: [],
          totalRows: 0,
          version: 2,
          status: "ready",
          statusCode: "Ready",
        });

        yield* eu.offer({
          key: "refreshed",
          value: JSON.stringify({ price: 3 }),
          offset: 3n,
          timestampNanos: 10_000_000_000n,
        });
        yield* eu.offer({
          key: "refreshed",
          value: JSON.stringify({ price: 4 }),
          offset: 4n,
          timestampNanos: 20_000_000_000n,
        });
        yield* awaitCondition(() => eu.commits.length === 4);
        yield* TestClock.adjust("19 seconds");
        expect(
          yield* runtime.client.snapshot("orders", {
            select: ["id", "price"],
          }),
        ).toStrictEqual({
          rows: [{ id: "eu:0:refreshed", price: 4 }],
          totalRows: 1,
          version: 4,
          status: "ready",
          statusCode: "Ready",
        });
        yield* TestClock.adjust("2 seconds");
        yield* awaitEffectCondition("refreshed expiration", () =>
          runtime.client
            .snapshot("orders", {
              select: ["id"],
            })
            .pipe(Effect.map((snapshot) => snapshot.totalRows === 0)),
        );
        yield* TestClock.adjust("1 second");
        expect(
          yield* runtime.client.snapshot("orders", {
            select: ["id"],
          }),
        ).toStrictEqual({
          rows: [],
          totalRows: 0,
          version: 5,
          status: "ready",
          statusCode: "Ready",
        });

        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
        const health = Option.getOrThrow(
          yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
        );
        const retention = Option.getOrThrow(
          Option.fromNullishOr(health.metrics.adapter.regions[0]?.retention),
        );
        const { lastSweepAtNanos, lastSweepDurationNanos, ...stableRetention } = retention;
        expect(stableRetention).toStrictEqual({
          declaredCleanupPolicy: "delete",
          observedCleanupPolicy: "delete",
          configuredRetention: {
            _tag: "Finite",
            durationNanos: 5_000_000_000n,
          },
          resolvedRetention: {
            _tag: "Finite",
            durationNanos: 5_000_000_000n,
          },
          trackedRows: 0,
          lastSweepRetryableFailures: 0,
          expiredRows: 2n,
          authoritativeExpiredDeletes: 1n,
          failedWorkBacklog: 0,
          expirationRetryFailures: 0n,
          latestExpirationFailure: null,
          sweepIntervalNanos: 1_000_000_000n,
        });
        expect(lastSweepAtNanos).not.toBeNull();
        expect(lastSweepAtNanos ?? 0n).toBeGreaterThanOrEqual(25_000_000_000n);
        expect(lastSweepDurationNanos).not.toBeNull();
        expect(lastSweepDurationNanos ?? -1n).toBeGreaterThanOrEqual(0n);
        yield* diagnostics.close();
        yield* runtime.close;
      }),
  );

  it.effect("resolves duration starts once and reacquires every Region after commit failure", () =>
    Effect.gen(function* () {
      const acquisitionOrder: Array<string> = [];
      const eu = yield* makeFakeRegion("eu", acquisitionOrder);
      const us = yield* makeFakeRegion("us", acquisitionOrder);
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source: makeSource({
              mode: "durationAgo",
              duration: "1 minute",
              fallback: "earliest",
            }),
          },
        },
      });
      yield* TestClock.adjust("2 minutes");
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(
          makeKafkaServerLayer({
            brokerContracts: foreverBrokerContracts(["eu", "us"]),
            retentionSweepIntervalNanos: 900_000_000_000n,
            consumerGroupPrefix: "replica",
            regions: new Map([
              ["eu", eu.runtime],
              ["us", us.runtime],
            ]),
          }),
        ),
      );
      yield* eu.awaitAcquisitions(1);
      yield* us.awaitAcquisitions(1);
      const retryDiagnostics = yield* runtime.liveClient.subscribeSourceHealth({
        topic: "orders",
      });
      const initialAcquisition = Option.getOrThrow(Option.fromUndefinedOr(eu.acquisitions[0]));
      const initialStart = initialAcquisition.start;
      expect(initialStart).toStrictEqual({
        mode: "durationAgo",
        durationNanos: 60_000_000_000n,
        resolvedAtNanos: 120_000_000_000n,
        atNanos: 60_000_000_000n,
        atMillis: 60_000n,
        fallback: "earliest",
      });

      yield* eu.offer({
        key: "retry",
        value: JSON.stringify({ price: 1 }),
        offset: 1n,
        commitFailure: commitFailure("eu"),
      });
      yield* awaitCondition(() => eu.counts().finalizations === 1, "retry finalization");
      yield* retryDiagnostics.events.pipe(
        Stream.filter((health) => health.status._tag === "WaitingToRetry"),
        Stream.take(1),
        Stream.runDrain,
      );
      yield* TestClock.withLive(Effect.sleep("1 millis"));
      yield* TestClock.adjust("1 second");
      yield* eu.awaitAcquisitions(2);
      yield* us.awaitAcquisitions(2);

      expect([
        eu.acquisitions[0]?.start,
        us.acquisitions[0]?.start,
        eu.acquisitions[1]?.start,
        us.acquisitions[1]?.start,
      ]).toStrictEqual([initialStart, initialStart, initialStart, initialStart]);
      const lifetimeScope = initialAcquisition.lifetimeScope;
      expect([
        eu.acquisitions[0]?.lifetimeScope,
        us.acquisitions[0]?.lifetimeScope,
        eu.acquisitions[1]?.lifetimeScope,
        us.acquisitions[1]?.lifetimeScope,
      ]).toStrictEqual([lifetimeScope, lifetimeScope, lifetimeScope, lifetimeScope]);
      expect([
        eu.acquisitions[0]?.activeGroupId,
        us.acquisitions[0]?.activeGroupId,
        eu.acquisitions[1]?.activeGroupId,
        us.acquisitions[1]?.activeGroupId,
      ]).toStrictEqual(["replica:orders", "replica:orders", "replica:orders", "replica:orders"]);
      expect(acquisitionOrder).toStrictEqual(["eu:1", "us:1", "eu:2", "us:2"]);
      expect({
        eu: eu.counts(),
        us: us.counts(),
      }).toStrictEqual({
        eu: { acquisitions: 2, finalizations: 1 },
        us: { acquisitions: 2, finalizations: 1 },
      });

      yield* eu.offer({
        key: "retry",
        value: JSON.stringify({ price: 2 }),
        offset: 2n,
      });
      yield* awaitCondition(() => eu.commits.length === 1, "post-retry commit");
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "price", "region"],
      });
      expect(snapshot).toStrictEqual({
        rows: [{ id: "eu:0:retry", price: 2, region: "eu" }],
        totalRows: 1,
        version: 2,
        status: "ready",
        statusCode: "Ready",
      });

      yield* retryDiagnostics.close();
      yield* runtime.close;
    }),
  );

  it.effect("freezes callback metadata and names custom codecs in safe rejections", () =>
    Effect.gen(function* () {
      const acquisitionOrder: Array<string> = [];
      const eu = yield* makeFakeRegion("eu", acquisitionOrder);
      const decoder = new TextDecoder();
      const namedCodecFailure = {
        _tag: "NamedCodecFailure",
        message: "private decoder detail",
      } as const;
      let goodMetadata: KafkaMessageMetadata | undefined;
      let originalKeyPayload: Uint8Array | undefined;
      let originalValuePayload: Uint8Array | undefined;
      let keyPayloadDetached = false;
      let valuePayloadDetached = false;
      const NamedValue = Schema.Struct({
        price: Schema.Number,
      });
      const source = kafka.source({
        cleanupPolicy: "delete",
        retentionPolicy: "Infinity",
        topic: "source-orders",
        regions: ["eu"],
        key: kafka.codec({
          name: "key\ncodec",
          decode: ({ bytes: input, metadata: inputMetadata }) => {
            if (originalKeyPayload !== undefined) {
              keyPayloadDetached = input !== originalKeyPayload;
            }
            const key = decoder.decode(input);
            if (key === "key-fail") {
              return Reflect.set(inputMetadata, "sourceRegion", "")
                ? Effect.die(new Error("Kafka callback metadata was mutable."))
                : Effect.fail(namedCodecFailure);
            }
            if (key === "key-throw") {
              throw new Error("private synchronous decoder failure");
            }
            if (key === "good") {
              goodMetadata = inputMetadata;
            }
            return Effect.succeed(key);
          },
        }),
        value: kafka.codec({
          name: "value-codec",
          decode: ({ bytes: input }) => {
            if (originalValuePayload !== undefined) {
              valuePayloadDetached = input !== originalValuePayload;
            }
            const value = decoder.decode(input);
            if (value === "value-fail") {
              return Effect.fail(namedCodecFailure);
            }
            return Effect.try({
              try: (): unknown => JSON.parse(value),
              catch: () => namedCodecFailure,
            }).pipe(
              Effect.flatMap(Schema.decodeUnknownEffect(NamedValue)),
              Effect.mapError(() => namedCodecFailure),
            );
          },
        }),
        localRowKey: ({ key }) => key,
        map: ({ value, region }) => ({
          price: value.price,
          region: String(region),
        }),
        startFrom: "earliest",
      });
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source,
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(
          makeKafkaServerLayer({
            brokerContracts: foreverBrokerContracts(["eu", "us"]),
            retentionSweepIntervalNanos: 900_000_000_000n,
            consumerGroupPrefix: "replica",
            regions: new Map([["eu", eu.runtime]]),
          }),
        ),
      );
      yield* eu.awaitAcquisitions(1);
      const latestRejection = Effect.fn("KafkaSourceAdapter.test.namedRejection")(function* () {
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
        const health = Option.getOrThrow(
          yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
        );
        yield* diagnostics.close();
        const degraded = Option.getOrThrow(
          Option.liftPredicate(health.status, (status) => status._tag === "Degraded"),
        );
        return Option.getOrThrow(
          Option.liftPredicate(
            degraded.reasons[0],
            (reason) => reason._tag === "SourceItemRejection",
          ),
        ).latestRejection;
      });

      yield* eu.offer({
        key: "key-fail",
        value: JSON.stringify({ price: 1 }),
        offset: 1n,
      });
      yield* awaitCondition(() => eu.commits.length === 1);
      expect(yield* latestRejection()).toStrictEqual({
        failure: {
          _tag: "AdapterFailure",
          failure: {
            _tag: "KafkaDecodeFailure",
            region: "eu",
            topic: "source-orders",
            message: 'Kafka key codec "key\\ncodec" rejected the record.',
          },
        },
        location: {
          region: "eu",
          topic: "source-orders",
          partition: 0,
          offset: 1n,
          phase: "keyDecode",
          message: 'Kafka key codec "key\\ncodec" rejected the record.',
        },
        rejectedAtNanos: 0n,
      });

      yield* eu.offer({
        key: "key-throw",
        value: JSON.stringify({ price: 2 }),
        offset: 2n,
      });
      yield* awaitCondition(() => eu.commits.length === 2);
      expect(yield* latestRejection()).toStrictEqual({
        failure: {
          _tag: "AdapterFailure",
          failure: {
            _tag: "KafkaDecodeFailure",
            region: "eu",
            topic: "source-orders",
            message: 'Kafka key codec "key\\ncodec" rejected the record.',
          },
        },
        location: {
          region: "eu",
          topic: "source-orders",
          partition: 0,
          offset: 2n,
          phase: "keyDecode",
          message: 'Kafka key codec "key\\ncodec" rejected the record.',
        },
        rejectedAtNanos: 0n,
      });

      yield* eu.offer({
        key: "value-fail",
        value: "value-fail",
        offset: 3n,
      });
      yield* awaitCondition(() => eu.commits.length === 3);
      expect(yield* latestRejection()).toStrictEqual({
        failure: {
          _tag: "AdapterFailure",
          failure: {
            _tag: "KafkaDecodeFailure",
            region: "eu",
            topic: "source-orders",
            message: 'Kafka value codec "value-codec" rejected the record.',
          },
        },
        location: {
          region: "eu",
          topic: "source-orders",
          partition: 0,
          offset: 3n,
          phase: "valueDecode",
          message: 'Kafka value codec "value-codec" rejected the record.',
        },
        rejectedAtNanos: 0n,
      });

      const singleHeader = bytes("single");
      const repeatedHeaders = [bytes("first"), bytes("second")];
      const goodHeaders: Record<string, Uint8Array | ReadonlyArray<Uint8Array>> =
        Object.create(null);
      goodHeaders["single"] = singleHeader;
      goodHeaders["repeated"] = repeatedHeaders;
      let headerReads = 0;
      const observedHeaders = new Proxy(goodHeaders, {
        get: (target, property, receiver) => {
          headerReads += 1;
          return Reflect.get(target, property, receiver);
        },
      });
      yield* eu.offer({
        key: "good",
        value: JSON.stringify({ price: 3 }),
        offset: 4n,
        headers: observedHeaders,
      });
      yield* awaitCondition(() => eu.commits.length === 4);
      const capturedMetadata = Option.getOrThrow(Option.fromUndefinedOr(goodMetadata));
      expect({
        metadataFrozen: Object.isFrozen(capturedMetadata),
        headersFrozen: Object.isFrozen(capturedMetadata.headers),
        repeatedFrozen: Object.isFrozen(capturedMetadata.headers["repeated"]),
        scalarEnvelope: {
          sourceTopic: capturedMetadata.sourceTopic,
          sourceRegion: capturedMetadata.sourceRegion,
          partition: capturedMetadata.partition,
          offset: capturedMetadata.offset,
          timestampNanos: capturedMetadata.timestampNanos,
        },
        entries: Object.entries(capturedMetadata.headers),
        singleDetached: capturedMetadata.headers["single"] !== singleHeader,
        repeatedDetached:
          capturedMetadata.headers["repeated"]?.[0] !== repeatedHeaders[0] &&
          capturedMetadata.headers["repeated"]?.[1] !== repeatedHeaders[1],
        headerReads,
      }).toStrictEqual({
        metadataFrozen: true,
        headersFrozen: true,
        repeatedFrozen: true,
        scalarEnvelope: {
          sourceTopic: "source-orders",
          sourceRegion: "eu",
          partition: 0,
          offset: 4n,
          timestampNanos: 4_000_000n,
        },
        entries: [
          ["single", bytes("single")],
          ["repeated", [bytes("first"), bytes("second")]],
        ],
        singleDetached: true,
        repeatedDetached: true,
        headerReads: 0,
      });
      expect(
        yield* runtime.client.snapshot("orders", {
          select: ["id", "price", "region"],
        }),
      ).toStrictEqual({
        rows: [{ id: "eu:0:good", price: 3, region: "eu" }],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });

      originalKeyPayload = bytes("detached");
      originalValuePayload = bytes(JSON.stringify({ price: 4 }));
      const mutableRecord: KafkaServerRecord = {
        key: originalKeyPayload,
        value: originalValuePayload,
        metadata: metadata("eu", 5n),
        settlement: () =>
          Effect.sync(() => {
            eu.commits.push(5n);
          }),
      };
      yield* eu.offerRecord(
        new Proxy(mutableRecord, {
          get: (target, property, receiver) => {
            if (property === "value") {
              originalKeyPayload?.fill(0);
            }
            if (property === "metadata") {
              originalValuePayload?.fill(0);
            }
            return Reflect.get(target, property, receiver);
          },
        }),
      );
      yield* awaitCondition(() => eu.commits.length === 5);
      expect({ keyPayloadDetached, valuePayloadDetached }).toStrictEqual({
        keyPayloadDetached: true,
        valuePayloadDetached: true,
      });
      expect(
        yield* runtime.client.snapshot("orders", {
          select: ["id", "price", "region"],
          orderBy: [{ field: "id", direction: "asc" }],
        }),
      ).toStrictEqual({
        rows: [
          { id: "eu:0:detached", price: 4, region: "eu" },
          { id: "eu:0:good", price: 3, region: "eu" },
        ],
        totalRows: 2,
        version: 2,
        status: "ready",
        statusCode: "Ready",
      });
      const metrics = yield* eu.runtime.metrics({
        activeGroupId: "replica:orders",
        lifetimeScope: Option.getOrThrow(Option.fromUndefinedOr(eu.acquisitions[0])).lifetimeScope,
        region: "eu",
        sourceTopic: "source-orders",
        viewServerTopic: "orders",
      });
      expect({
        commits: eu.commits,
        decoded: metrics.decoded,
        decodeFailures: metrics.decodeFailures,
        rejections: metrics.rejections,
      }).toStrictEqual({
        commits: [1n, 2n, 3n, 4n, 5n],
        decoded: 2n,
        decodeFailures: 3n,
        rejections: 3n,
      });
      yield* runtime.close;
    }),
  );

  it.effect("ceil-rounds timestamp boundaries and resolves duration afresh after restart", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const acquisitionOrder: Array<string> = [];
        const eu = yield* makeFakeRegion("eu", acquisitionOrder);
        const us = yield* makeFakeRegion("us", acquisitionOrder);
        const timestampConfig = defineViewServerConfig({
          topics: {
            orders: {
              schema: Order,
              source: makeSource({
                mode: "timestamp",
                atNanos: 1_000_001n,
                fallback: "latest",
              }),
            },
          },
        });
        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* Effect.acquireRelease(
              makeViewServerRuntimeCore(timestampConfig, {}).pipe(
                Effect.provide(
                  makeKafkaServerLayer({
                    brokerContracts: foreverBrokerContracts(["eu", "us"]),
                    retentionSweepIntervalNanos: 900_000_000_000n,
                    consumerGroupPrefix: "replica",
                    regions: new Map([
                      ["eu", eu.runtime],
                      ["us", us.runtime],
                    ]),
                  }),
                ),
              ),
              (runtime) => runtime.close,
            );
            yield* eu.awaitAcquisitions(1);
            expect(eu.acquisitions[0]?.start).toStrictEqual({
              mode: "timestamp",
              atNanos: 1_000_001n,
              atMillis: 2n,
              fallback: "latest",
            });
          }),
        );

        const durationConfig = defineViewServerConfig({
          topics: {
            orders: {
              schema: Order,
              source: makeSource({
                mode: "durationAgo",
                duration: "1 second",
                fallback: "earliest",
              }),
            },
          },
        });
        const durationContext = yield* Layer.build(
          makeKafkaServerLayer({
            brokerContracts: foreverBrokerContracts(["eu", "us"]),
            retentionSweepIntervalNanos: 900_000_000_000n,
            consumerGroupPrefix: "replica",
            regions: new Map([
              ["eu", eu.runtime],
              ["us", us.runtime],
            ]),
          }),
        );
        yield* TestClock.adjust("10 seconds");
        const firstDuration = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* Effect.acquireRelease(
              makeViewServerRuntimeCore(durationConfig, {}).pipe(
                Effect.provideContext(durationContext),
              ),
              (runtime) => runtime.close,
            );
            yield* eu.awaitAcquisitions(2);
            return eu.acquisitions[1]?.start;
          }),
        );

        yield* TestClock.adjust("1 second");
        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* Effect.acquireRelease(
              makeViewServerRuntimeCore(durationConfig, {}).pipe(
                Effect.provideContext(durationContext),
              ),
              (runtime) => runtime.close,
            );
            yield* eu.awaitAcquisitions(3);
            expect({
              first: firstDuration,
              second: eu.acquisitions[2]?.start,
            }).toStrictEqual({
              first: {
                mode: "durationAgo",
                durationNanos: 1_000_000_000n,
                resolvedAtNanos: 10_000_000_000n,
                atNanos: 9_000_000_000n,
                atMillis: 9_000n,
                fallback: "earliest",
              },
              second: {
                mode: "durationAgo",
                durationNanos: 1_000_000_000n,
                resolvedAtNanos: 11_000_000_000n,
                atNanos: 10_000_000_000n,
                atMillis: 10_000n,
                fallback: "earliest",
              },
            });
            expect(eu.acquisitions[1]?.lifetimeScope).not.toBe(eu.acquisitions[2]?.lifetimeScope);
          }),
        );
      }),
    ),
  );

  it.effect(
    "owns delete-only null rejection settlement throws, failures, and shutdown interruption",
    () =>
      Effect.gen(function* () {
        const sourceOptions = {
          cleanupPolicy: "delete",
          retentionPolicy: "Infinity",
          topic: "source-orders",
          regions: ["eu"],
          key: kafka.string(),
          value: kafka.json(() => Schema.toCodecJson(Schema.Struct({ price: Schema.Number }))),
          localRowKey: ({ key }: { readonly key: string }) => key,
          map: ({
            value,
            region,
          }: {
            readonly value: { readonly price: number };
            readonly region: string;
          }) => ({
            price: value.price,
            region,
          }),
          startFrom: "earliest",
        } as const;
        const layerFor = (region: KafkaServerRegion, group: string) =>
          makeKafkaServerLayer({
            brokerContracts: foreverBrokerContracts(["eu", "us"]),
            retentionSweepIntervalNanos: 900_000_000_000n,
            consumerGroupPrefix: group,
            regions: new Map([["eu", region]]),
          });
        let throwingScheduleSteps = 0;
        let throwingCallbacks = 0;
        const throwingRecordingExits: Array<SourceApplicationExit> = [];
        const throwingOrder: Array<string> = [];
        const throwingRegion = yield* makeFakeRegion("eu", throwingOrder);
        const throwingConfig = defineViewServerConfig({
          topics: {
            orders: {
              schema: Order,
              source: kafka.source(
                sourceOptions,
                Schedule.recurs(0).pipe(
                  Schedule.tap(() =>
                    Effect.sync(() => {
                      throwingScheduleSteps += 1;
                    }),
                  ),
                ),
              ),
            },
          },
        });
        const throwingRuntime = yield* makeViewServerRuntimeCoreInternal(throwingConfig, {}).pipe(
          Effect.provide(layerFor(throwingRegion.runtime, "null-throw")),
        );
        const throwingFatal = yield* throwingRuntime.fatal.pipe(Effect.forkChild);
        yield* throwingRegion.awaitAcquisitions(1);
        yield* throwingRegion.offerRecord({
          key: bytes("null-throw"),
          value: null,
          metadata: metadata("eu", 1n),
          settlement: (recordingExit) => {
            throwingCallbacks += 1;
            throwingRecordingExits.push(recordingExit);
            throw new Error("injected rejection settlement callback defect");
          },
        });
        yield* awaitCondition(() => throwingRegion.counts().finalizations === 1);
        const throwingDiagnostics = yield* throwingRuntime.liveClient.subscribeSourceHealth({
          topic: "orders",
        });
        const throwingHealth = Option.getOrThrow(
          yield* throwingDiagnostics.events.pipe(
            Stream.filter((health) => health.status._tag === "Exhausted"),
            Stream.take(1),
            Stream.runHead,
          ),
        );
        yield* throwingDiagnostics.close();
        yield* Fiber.interrupt(throwingFatal);
        const throwingFatalExit = yield* Fiber.await(throwingFatal);
        expect({
          acquisitions: throwingRegion.counts().acquisitions,
          callbacks: throwingCallbacks,
          commits: throwingRegion.commits,
          recordingExits: throwingRecordingExits,
          fatalInterruptedOnly:
            Exit.isFailure(throwingFatalExit) && Cause.hasInterruptsOnly(throwingFatalExit.cause),
          finalizations: throwingRegion.counts().finalizations,
          rejected: throwingHealth.metrics.runtime.rejectedItemCount,
          scheduleSteps: throwingScheduleSteps,
          status: throwingHealth.status,
        }).toStrictEqual({
          acquisitions: 1,
          callbacks: 1,
          commits: [],
          recordingExits: [Exit.void],
          fatalInterruptedOnly: true,
          finalizations: 1,
          rejected: 1n,
          scheduleSteps: 0,
          status: {
            _tag: "Exhausted",
            exhaustion: {
              _tag: "RetryExhausted",
              lastTermination: {
                _tag: "Failed",
                failure: {
                  _tag: "RuntimeFailure",
                  failure: {
                    _tag: "InvalidSourceSettlement",
                    message: "Source Settlement callback threw before returning an Effect",
                  },
                },
              },
            },
            exhaustedAtNanos: 0n,
          },
        });
        yield* throwingRuntime.close;

        let failingScheduleSteps = 0;
        let failingCallbacks = 0;
        const failingRecordingExits: Array<SourceApplicationExit> = [];
        const failingOrder: Array<string> = [];
        const failingRegion = yield* makeFakeRegion("eu", failingOrder);
        const failingConfig = defineViewServerConfig({
          topics: {
            orders: {
              schema: Order,
              source: kafka.source(
                sourceOptions,
                Schedule.recurs(0).pipe(
                  Schedule.tap(() =>
                    Effect.sync(() => {
                      failingScheduleSteps += 1;
                    }),
                  ),
                ),
              ),
            },
          },
        });
        const failingRuntime = yield* makeViewServerRuntimeCoreInternal(failingConfig, {}).pipe(
          Effect.provide(layerFor(failingRegion.runtime, "null-failure")),
        );
        const failingFatal = yield* failingRuntime.fatal.pipe(Effect.forkChild);
        yield* failingRegion.awaitAcquisitions(1);
        yield* failingRegion.offerRecord({
          key: bytes("null-failure"),
          value: null,
          metadata: metadata("eu", 2n),
          settlement: (recordingExit) => {
            failingCallbacks += 1;
            failingRecordingExits.push(recordingExit);
            return Effect.fail(commitFailure("eu"));
          },
        });
        yield* awaitCondition(() => failingRegion.counts().finalizations === 1);
        const failingDiagnostics = yield* failingRuntime.liveClient.subscribeSourceHealth({
          topic: "orders",
        });
        const failingHealth = Option.getOrThrow(
          yield* failingDiagnostics.events.pipe(
            Stream.filter((health) => health.status._tag === "Exhausted"),
            Stream.take(1),
            Stream.runHead,
          ),
        );
        yield* failingDiagnostics.close();
        yield* Fiber.interrupt(failingFatal);
        const failingFatalExit = yield* Fiber.await(failingFatal);
        expect({
          acquisitions: failingRegion.counts().acquisitions,
          callbacks: failingCallbacks,
          commits: failingRegion.commits,
          recordingExits: failingRecordingExits,
          fatalInterruptedOnly:
            Exit.isFailure(failingFatalExit) && Cause.hasInterruptsOnly(failingFatalExit.cause),
          finalizations: failingRegion.counts().finalizations,
          rejected: failingHealth.metrics.runtime.rejectedItemCount,
          scheduleSteps: failingScheduleSteps,
          status: failingHealth.status,
        }).toStrictEqual({
          acquisitions: 1,
          callbacks: 1,
          commits: [],
          recordingExits: [Exit.void],
          fatalInterruptedOnly: true,
          finalizations: 1,
          rejected: 1n,
          scheduleSteps: 0,
          status: {
            _tag: "Exhausted",
            exhaustion: {
              _tag: "RetryExhausted",
              lastTermination: {
                _tag: "Failed",
                failure: {
                  _tag: "AdapterFailure",
                  failure: {
                    _tag: "KafkaCommitFailure",
                    region: "eu",
                    topic: "source-orders",
                    message: "commit failed",
                  },
                },
              },
            },
            exhaustedAtNanos: 0n,
          },
        });
        yield* failingRuntime.close;

        let blockedScheduleSteps = 0;
        let blockedCallbacks = 0;
        const blockedRecordingExits: Array<SourceApplicationExit> = [];
        const blockedStarted = yield* Deferred.make<void>();
        const blockedFinalized = yield* Deferred.make<void>();
        const blockedOrder: Array<string> = [];
        const blockedRegion = yield* makeFakeRegion("eu", blockedOrder);
        const blockedConfig = defineViewServerConfig({
          topics: {
            orders: {
              schema: Order,
              source: kafka.source(
                sourceOptions,
                Schedule.recurs(1).pipe(
                  Schedule.tap(() =>
                    Effect.sync(() => {
                      blockedScheduleSteps += 1;
                    }),
                  ),
                ),
              ),
            },
          },
        });
        const blockedRuntime = yield* makeViewServerRuntimeCoreInternal(blockedConfig, {}).pipe(
          Effect.provide(layerFor(blockedRegion.runtime, "null-blocked")),
        );
        const blockedFatal = yield* blockedRuntime.fatal.pipe(Effect.forkChild);
        const blockedDiagnostics = yield* blockedRuntime.liveClient.subscribeSourceHealth({
          topic: "orders",
        });
        yield* blockedRegion.awaitAcquisitions(1);
        yield* blockedRegion.offerRecord({
          key: bytes("null-blocked"),
          value: null,
          metadata: metadata("eu", 3n),
          settlement: (recordingExit) => {
            blockedCallbacks += 1;
            blockedRecordingExits.push(recordingExit);
            return Deferred.succeed(blockedStarted, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(Deferred.succeed(blockedFinalized, undefined).pipe(Effect.asVoid)),
            );
          },
        });
        yield* Deferred.await(blockedStarted);
        const degraded = Option.getOrThrow(
          yield* blockedDiagnostics.events.pipe(
            Stream.filter((health) => health.status._tag === "Degraded"),
            Stream.take(1),
            Stream.runHead,
          ),
        );
        const blockedClose = yield* blockedRuntime.close.pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Deferred.await(blockedFinalized);
        yield* Fiber.join(blockedClose);
        yield* Fiber.interrupt(blockedFatal);
        const blockedFatalExit = yield* Fiber.await(blockedFatal);
        expect({
          acquisitions: blockedRegion.counts().acquisitions,
          callbacks: blockedCallbacks,
          commits: blockedRegion.commits,
          recordingExits: blockedRecordingExits,
          fatalInterruptedOnly:
            Exit.isFailure(blockedFatalExit) && Cause.hasInterruptsOnly(blockedFatalExit.cause),
          finalizations: blockedRegion.counts().finalizations,
          rejected: degraded.metrics.runtime.rejectedItemCount,
          scheduleSteps: blockedScheduleSteps,
          statusTag: degraded.status._tag,
        }).toStrictEqual({
          acquisitions: 1,
          callbacks: 1,
          commits: [],
          recordingExits: [Exit.void],
          fatalInterruptedOnly: true,
          finalizations: 1,
          rejected: 1n,
          scheduleSteps: 0,
          statusTag: "Degraded",
        });
        yield* blockedDiagnostics.close();
      }),
  );

  it.effect("replays a rejected record when its settlement commit fails", () =>
    Effect.gen(function* () {
      const acquisitionOrder: Array<string> = [];
      const eu = yield* makeFakeRegion("eu", acquisitionOrder);
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source: kafka.source({
              cleanupPolicy: "delete",
              retentionPolicy: "Infinity",
              topic: "source-orders",
              regions: ["eu"],
              key: kafka.string(),
              value: kafka.json(() => Schema.toCodecJson(Schema.Struct({ price: Schema.Number }))),
              localRowKey: ({ key }) => key,
              map: ({ value, region }) => ({
                price: value.price,
                region: String(region),
              }),
              startFrom: "earliest",
            }),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(
          makeKafkaServerLayer({
            brokerContracts: foreverBrokerContracts(["eu", "us"]),
            retentionSweepIntervalNanos: 900_000_000_000n,
            consumerGroupPrefix: "replica",
            regions: new Map([["eu", eu.runtime]]),
          }),
        ),
      );
      yield* eu.awaitAcquisitions(1);
      const retryDiagnostics = yield* runtime.liveClient.subscribeSourceHealth({
        topic: "orders",
      });
      yield* eu.offer({
        key: "poison",
        value: "{",
        offset: 1n,
        commitFailure: commitFailure("eu"),
      });
      yield* awaitCondition(() => eu.counts().finalizations === 1);
      yield* retryDiagnostics.events.pipe(
        Stream.filter((health) => health.status._tag === "WaitingToRetry"),
        Stream.take(1),
        Stream.runDrain,
      );
      yield* TestClock.withLive(Effect.sleep("1 millis"));
      yield* TestClock.adjust("1 second");
      yield* eu.awaitAcquisitions(2);
      expect(eu.commits).toStrictEqual([]);

      yield* eu.offer({
        key: "poison",
        value: "{",
        offset: 1n,
      });
      yield* eu.offer({
        key: "after",
        value: JSON.stringify({ price: 2 }),
        offset: 2n,
      });
      yield* awaitCondition(() => eu.commits.length === 2);
      expect(eu.commits).toStrictEqual([1n, 2n]);
      expect(
        yield* runtime.client.snapshot("orders", {
          select: ["id", "price", "region"],
        }),
      ).toStrictEqual({
        rows: [{ id: "eu:0:after", price: 2, region: "eu" }],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      yield* retryDiagnostics.close();
      yield* runtime.close;
    }),
  );

  it.effect("reconstructs state after lifetime loss from an authoritative earliest replay", () =>
    Effect.gen(function* () {
      const acquisitionOrder: Array<string> = [];
      const eu = yield* makeFakeRegion("eu", acquisitionOrder);
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source: kafka.source({
              cleanupPolicy: "delete",
              retentionPolicy: "Infinity",
              topic: "source-orders",
              regions: ["eu"],
              key: kafka.string(),
              value: kafka.json(() => Schema.toCodecJson(Schema.Struct({ price: Schema.Number }))),
              localRowKey: ({ key }) => key,
              map: ({ value, region }) => ({
                price: value.price,
                region: String(region),
              }),
              startFrom: "earliest",
            }),
          },
        },
      });
      const context = yield* Layer.build(
        makeKafkaServerLayer({
          brokerContracts: foreverBrokerContracts(["eu"]),
          retentionSweepIntervalNanos: 900_000_000_000n,
          consumerGroupPrefix: "rebuild",
          regions: new Map([["eu", eu.runtime]]),
        }),
      );
      const expectedSnapshot = {
        rows: [{ id: "eu:0:authoritative", price: 41, region: "eu" }],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      } as const;

      const first = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provideContext(context),
      );
      yield* eu.awaitAcquisitions(1);
      yield* eu.offer({
        key: "authoritative",
        value: JSON.stringify({ price: 41 }),
        offset: 1n,
      });
      yield* awaitCondition(() => eu.commits.length === 1);
      expect(
        yield* first.client.snapshot("orders", {
          select: ["id", "price", "region"],
        }),
      ).toStrictEqual(expectedSnapshot);
      yield* first.close;

      const reconstructed = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provideContext(context),
      );
      yield* eu.awaitAcquisitions(2);
      expect(eu.acquisitions[1]?.start).toStrictEqual({ mode: "earliest" });
      yield* eu.offer({
        key: "authoritative",
        value: JSON.stringify({ price: 41 }),
        offset: 1n,
      });
      yield* awaitCondition(() => eu.commits.length === 2);
      expect(
        yield* reconstructed.client.snapshot("orders", {
          select: ["id", "price", "region"],
        }),
      ).toStrictEqual(expectedSnapshot);
      expect(eu.commits).toStrictEqual([1n, 1n]);
      yield* reconstructed.close;
    }),
  );

  it.effect(
    "leaves every transition-defective settlement mode uncommitted and reconstructs from authoritative replay",
    () =>
      Effect.gen(function* () {
        const settlementModes = ["success", "throw", "failure", "blocked"] as const;
        yield* Effect.forEach(
          settlementModes,
          (settlementMode) =>
            Effect.gen(function* () {
              const transitionDefect = new Error(
                `injected ${settlementMode} Kafka retention transition defect`,
              );
              const acquisitionOrder: Array<string> = [];
              const eu = yield* makeFakeRegion("eu", acquisitionOrder);
              const applicationExits: Array<SourceApplicationExit> = [];
              let settlementCallbacks = 0;
              let returnedSettlementFinalizations = 0;
              let returnedSettlementStarts = 0;
              const config = defineViewServerConfig({
                topics: {
                  orders: {
                    schema: Order,
                    source: kafka.source({
                      cleanupPolicy: "delete",
                      retentionPolicy: "5 seconds",
                      topic: "source-orders",
                      regions: ["eu"],
                      key: kafka.string(),
                      value: kafka.json(() =>
                        Schema.toCodecJson(Schema.Struct({ price: Schema.Number })),
                      ),
                      localRowKey: ({ key }) => key,
                      map: ({ value, region }) => ({
                        price: value.price,
                        region: String(region),
                      }),
                      startFrom: "earliest",
                    }),
                  },
                },
              });
              const layer = makeKafkaServerLayer({
                brokerContracts: [finiteBrokerContract()],
                retentionSweepIntervalNanos: 900_000_000_000n,
                consumerGroupPrefix: `transition-replay-${settlementMode}`,
                regions: new Map([["eu", eu.runtime]]),
              });
              const defective = yield* Effect.acquireUseRelease(
                Effect.sync(() =>
                  vi
                    .spyOn(kafkaServerInternals.KafkaRetentionDeadlineIndex.prototype, "set")
                    .mockImplementationOnce(() => {
                      throw transitionDefect;
                    }),
                ),
                () =>
                  Effect.gen(function* () {
                    const runtime = yield* makeViewServerRuntimeCoreInternal(config, {}).pipe(
                      Effect.provide(layer),
                    );
                    yield* eu.awaitAcquisitions(1);
                    yield* eu.offerRecord({
                      key: bytes("authoritative"),
                      value: bytes(JSON.stringify({ price: 41 })),
                      metadata: metadata("eu", 1n, {}, 0, 0n),
                      settlement: (applicationExit) => {
                        settlementCallbacks += 1;
                        applicationExits.push(applicationExit);
                        if (settlementMode === "throw") {
                          throw new Error("injected Kafka settlement callback defect");
                        }
                        const started = Effect.sync(() => {
                          returnedSettlementStarts += 1;
                        });
                        const returned =
                          settlementMode === "failure"
                            ? started.pipe(Effect.andThen(Effect.fail(commitFailure("eu"))))
                            : settlementMode === "blocked"
                              ? started.pipe(Effect.andThen(Effect.never))
                              : started;
                        return returned.pipe(
                          Effect.ensuring(
                            Effect.sync(() => {
                              returnedSettlementFinalizations += 1;
                            }),
                          ),
                        );
                      },
                    });
                    const fatalExit = yield* Effect.exit(runtime.fatal);
                    const appliedBeforeFatal = yield* runtime.client.snapshot("orders", {
                      select: ["id", "price", "region"],
                    });
                    yield* runtime.close;
                    return {
                      appliedBeforeFatal,
                      fatalExit,
                    };
                  }),
                (spy) =>
                  Effect.sync(() => {
                    spy.mockRestore();
                  }),
              );
              const defectiveCause = Option.getOrThrow(Exit.getCause(defective.fatalExit));
              const applicationCause = Option.getOrThrow(
                Exit.getCause(Option.getOrThrow(Option.fromUndefinedOr(applicationExits[0]))),
              );
              expect({
                applicationDefect: Result.getOrThrow(Cause.findDefect(applicationCause)),
                applicationExitCount: applicationExits.length,
                appliedBeforeFatal: defective.appliedBeforeFatal,
                commits: eu.commits,
                defect: Result.getOrThrow(Cause.findDefect(defectiveCause)),
                failure: Option.getOrThrow(Cause.findErrorOption(defectiveCause)),
                finalizations: eu.counts().finalizations,
                returnedSettlementFinalizations,
                returnedSettlementStarts,
                settlementCallbacks,
              }).toStrictEqual({
                applicationDefect: transitionDefect,
                applicationExitCount: 1,
                appliedBeforeFatal: {
                  rows: [{ id: "eu:0:authoritative", price: 41, region: "eu" }],
                  totalRows: 1,
                  version: 1,
                  status: "ready",
                  statusCode: "Ready",
                },
                commits: [],
                defect: transitionDefect,
                failure: {
                  _tag: "ViewServerRuntimeError",
                  code: "RuntimeUnavailable",
                  topic: "orders",
                  message: "Source application transition failed and stopped the complete runtime.",
                },
                finalizations: 1,
                returnedSettlementFinalizations: settlementMode === "throw" ? 0 : 1,
                returnedSettlementStarts: settlementMode === "throw" ? 0 : 1,
                settlementCallbacks: 1,
              });

              const reconstructed = yield* makeViewServerRuntimeCoreInternal(config, {}).pipe(
                Effect.provide(layer),
              );
              yield* eu.awaitAcquisitions(2);
              expect(eu.acquisitions[1]?.start).toStrictEqual({ mode: "earliest" });
              yield* eu.offer({
                key: "authoritative",
                value: JSON.stringify({ price: 41 }),
                offset: 1n,
                timestampNanos: 0n,
              });
              yield* awaitCondition(() => eu.commits.length === 1);
              expect({
                commits: eu.commits,
                reconstructed: yield* reconstructed.client.snapshot("orders", {
                  select: ["id", "price", "region"],
                }),
              }).toStrictEqual({
                commits: [1n],
                reconstructed: {
                  rows: [{ id: "eu:0:authoritative", price: 41, region: "eu" }],
                  totalRows: 1,
                  version: 1,
                  status: "ready",
                  statusCode: "Ready",
                },
              });
              yield* reconstructed.close;
            }),
          { discard: true },
        );
      }),
  );

  it.effect("releases earlier Regions when a later Region acquisition fails", () =>
    Effect.gen(function* () {
      const acquisitionOrder: Array<string> = [];
      const eu = yield* makeFakeRegion("eu", acquisitionOrder);
      const us = yield* makeFakeRegion("us", acquisitionOrder);
      us.failNextAcquisition();
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source: makeSource("latest"),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(
          makeKafkaServerLayer({
            brokerContracts: foreverBrokerContracts(["eu", "us"]),
            retentionSweepIntervalNanos: 900_000_000_000n,
            consumerGroupPrefix: "replica",
            regions: new Map([
              ["eu", eu.runtime],
              ["us", us.runtime],
            ]),
          }),
        ),
      );
      const retryDiagnostics = yield* runtime.liveClient.subscribeSourceHealth({
        topic: "orders",
      });
      yield* awaitCondition(() => eu.counts().finalizations === 1);
      yield* retryDiagnostics.events.pipe(
        Stream.filter((health) => health.status._tag === "WaitingToRetry"),
        Stream.take(1),
        Stream.runDrain,
      );
      yield* TestClock.withLive(Effect.sleep("1 millis"));
      yield* TestClock.adjust("1 second");
      yield* eu.awaitAcquisitions(2);
      yield* us.awaitAcquisitions(1);
      expect({
        acquisitionOrder,
        eu: eu.counts(),
        us: us.counts(),
      }).toStrictEqual({
        acquisitionOrder: ["eu:1", "us:1", "eu:2", "us:1"],
        eu: { acquisitions: 2, finalizations: 1 },
        us: { acquisitions: 1, finalizations: 0 },
      });

      yield* retryDiagnostics.close();
      yield* runtime.close;
    }),
  );

  it.effect("settles item-local callback and shape faults, then continues the lane", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const acquisitionOrder: Array<string> = [];
        const eu = yield* makeFakeRegion("eu", acquisitionOrder);
        const keyDecodeStarted = yield* Deferred.make<void>();
        yield* Effect.acquireRelease(
          Effect.sync(() =>
            vi.spyOn(kafkaContract, "kafkaRowId").mockImplementationOnce(() => {
              throw new Error("canonical row ID failed");
            }),
          ),
          (spy) =>
            Effect.sync(() => {
              spy.mockRestore();
            }),
        );
        const config = defineViewServerConfig({
          topics: {
            orders: {
              schema: Order,
              source: makeFaultSource(keyDecodeStarted),
            },
          },
        });
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(
            makeKafkaServerLayer({
              brokerContracts: foreverBrokerContracts(["eu", "us"]),
              retentionSweepIntervalNanos: 900_000_000_000n,
              consumerGroupPrefix: "replica",
              regions: new Map([["eu", eu.runtime]]),
            }),
          ),
        );
        yield* eu.awaitAcquisitions(1);
        const currentRejection = Effect.fn("KafkaSourceAdapter.test.rejection.current")(
          function* () {
            const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({
              topic: "orders",
            });
            const health = Option.getOrThrow(
              yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
            );
            yield* diagnostics.close();
            const degraded = Option.getOrThrow(
              Option.liftPredicate(health.status, (status) => status._tag === "Degraded"),
            );
            return Option.getOrThrow(
              Option.liftPredicate(
                degraded.reasons[0],
                (reason) => reason._tag === "SourceItemRejection",
              ),
            ).latestRejection;
          },
        );
        const poisonCases = [
          {
            key: null,
            value: JSON.stringify({ price: 1 }),
            phase: "keyDecode",
            message: "Kafka record key is required.",
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "KafkaDecodeFailure",
                region: "eu",
                topic: "source-orders",
                message: "Kafka record key is required.",
              },
            },
            decoded: 0n,
            decodeFailures: 1n,
            mappingFailures: 0n,
          },
          {
            key: "key-fail",
            value: JSON.stringify({ price: 2 }),
            phase: "keyDecode",
            message: 'Kafka key codec "fault-key" rejected the record.',
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "KafkaDecodeFailure",
                region: "eu",
                topic: "source-orders",
                message: 'Kafka key codec "fault-key" rejected the record.',
              },
            },
            decoded: 0n,
            decodeFailures: 2n,
            mappingFailures: 0n,
          },
          {
            key: "value-fail",
            value: "{",
            phase: "valueDecode",
            message: "Kafka value codec rejected the record.",
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "KafkaDecodeFailure",
                region: "eu",
                topic: "source-orders",
                message: "Kafka value codec rejected the record.",
              },
            },
            decoded: 0n,
            decodeFailures: 3n,
            mappingFailures: 0n,
          },
          {
            key: "local-throw",
            value: JSON.stringify({ price: 4 }),
            phase: "localRowKey",
            message: "Kafka Local Row Key could not be constructed.",
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "KafkaMappingFailure",
                region: "eu",
                topic: "source-orders",
                message: "Kafka Local Row Key could not be constructed.",
              },
            },
            decoded: 1n,
            decodeFailures: 3n,
            mappingFailures: 1n,
          },
          {
            key: "local-empty",
            value: JSON.stringify({ price: 5 }),
            phase: "localRowKey",
            message: "Kafka Local Row Key could not be constructed.",
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "KafkaMappingFailure",
                region: "eu",
                topic: "source-orders",
                message: "Kafka Local Row Key could not be constructed.",
              },
            },
            decoded: 2n,
            decodeFailures: 3n,
            mappingFailures: 2n,
          },
          {
            key: "canonical-fail",
            value: JSON.stringify({ price: 6 }),
            phase: "canonicalId",
            message: "Kafka canonical row ID could not be constructed.",
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "KafkaMappingFailure",
                region: "eu",
                topic: "source-orders",
                message: "Kafka canonical row ID could not be constructed.",
              },
            },
            decoded: 3n,
            decodeFailures: 3n,
            mappingFailures: 3n,
          },
          {
            key: "map-throw",
            value: JSON.stringify({ price: 7 }),
            phase: "mapping",
            message: "Kafka Mapping rejected the record.",
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "KafkaMappingFailure",
                region: "eu",
                topic: "source-orders",
                message: "Kafka Mapping rejected the record.",
              },
            },
            decoded: 4n,
            decodeFailures: 3n,
            mappingFailures: 4n,
          },
          {
            key: "map-array",
            value: JSON.stringify({ price: 8 }),
            phase: "mapping",
            message: "Kafka Mapping rejected the record.",
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "KafkaMappingFailure",
                region: "eu",
                topic: "source-orders",
                message: "Kafka Mapping rejected the record.",
              },
            },
            decoded: 5n,
            decodeFailures: 3n,
            mappingFailures: 5n,
          },
          {
            key: "map-id",
            value: JSON.stringify({ price: 9 }),
            phase: "mapping",
            message: "Kafka Mapping rejected the record.",
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "KafkaMappingFailure",
                region: "eu",
                topic: "source-orders",
                message: "Kafka Mapping rejected the record.",
              },
            },
            decoded: 6n,
            decodeFailures: 3n,
            mappingFailures: 6n,
          },
          {
            key: "map-proto",
            value: JSON.stringify({ price: 10 }),
            phase: "mapping",
            message: "Kafka Mapping rejected the record.",
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "KafkaMappingFailure",
                region: "eu",
                topic: "source-orders",
                message: "Kafka Mapping rejected the record.",
              },
            },
            decoded: 7n,
            decodeFailures: 3n,
            mappingFailures: 7n,
          },
          {
            key: "map-prototype-throw",
            value: JSON.stringify({ price: 11 }),
            phase: "mapping",
            message: "Kafka Mapping rejected the record.",
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "KafkaMappingFailure",
                region: "eu",
                topic: "source-orders",
                message: "Kafka Mapping rejected the record.",
              },
            },
            decoded: 8n,
            decodeFailures: 3n,
            mappingFailures: 8n,
          },
          {
            key: "map-symbol",
            value: JSON.stringify({ price: 12 }),
            phase: "mapping",
            message: "Kafka Mapping rejected the record.",
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "KafkaMappingFailure",
                region: "eu",
                topic: "source-orders",
                message: "Kafka Mapping rejected the record.",
              },
            },
            decoded: 9n,
            decodeFailures: 3n,
            mappingFailures: 9n,
          },
          {
            key: "map-accessor",
            value: JSON.stringify({ price: 13 }),
            phase: "mapping",
            message: "Kafka Mapping rejected the record.",
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "KafkaMappingFailure",
                region: "eu",
                topic: "source-orders",
                message: "Kafka Mapping rejected the record.",
              },
            },
            decoded: 10n,
            decodeFailures: 3n,
            mappingFailures: 10n,
          },
          {
            key: "map-descriptor-throw",
            value: JSON.stringify({ price: 14 }),
            phase: "mapping",
            message: "Kafka Mapping rejected the record.",
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "KafkaMappingFailure",
                region: "eu",
                topic: "source-orders",
                message: "Kafka Mapping rejected the record.",
              },
            },
            decoded: 11n,
            decodeFailures: 3n,
            mappingFailures: 11n,
          },
          {
            key: "schema",
            value: JSON.stringify({ price: 16 }),
            phase: "topicSchema",
            message: "Kafka mapped row does not satisfy the Topic Schema.",
            failure: {
              _tag: "RuntimeFailure",
              failure: {
                _tag: "InvalidTopicRow",
                topic: "orders",
                message: "Source Upsert does not satisfy Topic orders Schema.",
              },
            },
            decoded: 12n,
            decodeFailures: 3n,
            mappingFailures: 12n,
          },
        ] as const;
        yield* Effect.forEach(
          poisonCases,
          (poison, index) =>
            Effect.gen(function* () {
              const offset = BigInt(index + 1);
              yield* eu.offer({
                key: poison.key,
                value: poison.value,
                offset,
              });
              yield* awaitCondition(() => eu.commits.length === index + 1);
              expect(yield* currentRejection()).toStrictEqual({
                failure: poison.failure,
                location: {
                  region: "eu",
                  topic: "source-orders",
                  partition: 0,
                  offset,
                  phase: poison.phase,
                  message: poison.message,
                },
                rejectedAtNanos: 0n,
              });
              const metrics = yield* eu.runtime.metrics({
                activeGroupId: "replica:orders",
                lifetimeScope: Option.getOrThrow(Option.fromUndefinedOr(eu.acquisitions[0]))
                  .lifetimeScope,
                region: "eu",
                sourceTopic: "source-orders",
                viewServerTopic: "orders",
              });
              expect(metrics.decoded).toBe(poison.decoded);
              expect(metrics.decodeFailures).toBe(poison.decodeFailures);
              expect(metrics.mapped).toBe(0n);
              expect(metrics.mappingFailures).toBe(poison.mappingFailures);
              expect(metrics.rejections).toBe(offset);
            }),
          { discard: true },
        );
        yield* eu.offer({
          key: "good",
          value: JSON.stringify({ price: 17 }),
          offset: 16n,
        });
        yield* awaitCondition(() => eu.commits.length === 16);
        expect({
          commits: eu.commits,
          counts: eu.counts(),
        }).toStrictEqual({
          commits: [1n, 2n, 3n, 4n, 5n, 6n, 7n, 8n, 9n, 10n, 11n, 12n, 13n, 14n, 15n, 16n],
          counts: {
            acquisitions: 1,
            finalizations: 0,
          },
        });
        const snapshot = yield* runtime.client.snapshot("orders", {
          select: ["id", "price", "region"],
        });
        expect(snapshot).toStrictEqual({
          rows: [{ id: "eu:0:good", price: 17, region: "eu" }],
          totalRows: 1,
          version: 1,
          status: "ready",
          statusCode: "Ready",
        });
        yield* eu.offer({
          key: "key-never",
          value: JSON.stringify({ price: 18 }),
          offset: 17n,
        });
        yield* Deferred.await(keyDecodeStarted);
        yield* runtime.close;
        expect(eu.counts().finalizations).toBe(1);
      }),
    ),
  );

  it.effect("propagates decoder defects without committing or recording a rejection", () =>
    Effect.gen(function* () {
      const acquisitionOrder: Array<string> = [];
      const eu = yield* makeFakeRegion("eu", acquisitionOrder);
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source: makeFaultSource(),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(
          makeKafkaServerLayer({
            brokerContracts: foreverBrokerContracts(["eu", "us"]),
            retentionSweepIntervalNanos: 900_000_000_000n,
            consumerGroupPrefix: "replica",
            regions: new Map([["eu", eu.runtime]]),
          }),
        ),
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
      yield* eu.awaitAcquisitions(1);
      yield* eu.offer({
        key: "key-die",
        value: JSON.stringify({ price: 1 }),
        offset: 1n,
      });
      yield* awaitCondition(() => eu.counts().finalizations === 1);
      yield* TestClock.adjust("1 second");
      const health = Option.getOrThrow(
        yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
      );
      expect({
        commits: eu.commits,
        rejected: health.metrics.runtime.rejectedItemCount,
        adapterRejections: health.metrics.adapter.regions[0]?.rejections,
      }).toStrictEqual({
        commits: [],
        rejected: 0n,
        adapterRejections: 0n,
      });
      yield* diagnostics.close();
      yield* runtime.close;
    }),
  );

  it.effect(
    "rejects invalid group input at construction and keeps runtime group failures typed",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const acquisitionOrder: Array<string> = [];
          const eu = yield* makeFakeRegion("eu", acquisitionOrder);
          const source = kafka.source(
            {
              cleanupPolicy: "delete",
              retentionPolicy: "Infinity",
              topic: "source-orders",
              regions: ["eu"],
              key: kafka.string(),
              value: kafka.json(() => Schema.toCodecJson(Schema.Struct({ price: Schema.Number }))),
              localRowKey: ({ key }) => key,
              map: ({ value, region }) => ({
                price: value.price,
                region: String(region),
              }),
              startFrom: "earliest",
            },
            Schedule.recurs(0),
          );
          const config = defineViewServerConfig({
            topics: {
              orders: {
                schema: Order,
                source,
              },
            },
          });
          expect(() =>
            makeKafkaServerLayer({
              brokerContracts: foreverBrokerContracts(["eu", "us"]),
              retentionSweepIntervalNanos: 900_000_000_000n,
              consumerGroupPrefix: "\ud800",
              regions: new Map([["eu", eu.runtime]]),
            }),
          ).toThrow(
            "Kafka consumer group prefix and View Server Topic must contain well-formed Unicode.",
          );
          expect(acquisitionOrder).toStrictEqual([]);

          const fallbackAcquisitionOrder: Array<string> = [];
          const fallbackRegion = yield* makeFakeRegion("eu", fallbackAcquisitionOrder);
          const fallbackLayer = makeKafkaServerLayer({
            brokerContracts: foreverBrokerContracts(["eu", "us"]),
            retentionSweepIntervalNanos: 900_000_000_000n,
            consumerGroupPrefix: "replica",
            regions: new Map([["eu", fallbackRegion.runtime]]),
          });
          const domainFailureAcquisitionOrder: Array<string> = [];
          const domainFailureRegion = yield* makeFakeRegion("eu", domainFailureAcquisitionOrder);
          const domainFailureLayer = makeKafkaServerLayer({
            brokerContracts: foreverBrokerContracts(["eu", "us"]),
            retentionSweepIntervalNanos: 900_000_000_000n,
            consumerGroupPrefix: "replica",
            regions: new Map([["eu", domainFailureRegion.runtime]]),
          });
          const groupIdSpy = yield* Effect.acquireRelease(
            Effect.sync(() =>
              vi.spyOn(kafkaContract, "kafkaConsumerGroupId").mockImplementation(() => {
                throw new Error("unexpected group construction failure");
              }),
            ),
            (spy) =>
              Effect.sync(() => {
                spy.mockRestore();
              }),
          );
          const fallbackRuntime = yield* makeViewServerRuntimeCore(config, {}).pipe(
            Effect.provide(fallbackLayer),
          );
          const fallbackDiagnostics = yield* fallbackRuntime.liveClient.subscribeSourceHealth({
            topic: "orders",
          });
          const fallbackExhausted = Option.getOrThrow(
            yield* fallbackDiagnostics.events.pipe(
              Stream.filter((health) => health.status._tag === "Exhausted"),
              Stream.take(1),
              Stream.runHead,
            ),
          );
          expect(fallbackExhausted.status).toStrictEqual({
            _tag: "Exhausted",
            exhaustion: {
              _tag: "RetryExhausted",
              lastTermination: {
                _tag: "Failed",
                failure: {
                  _tag: "AdapterFailure",
                  failure: {
                    _tag: "KafkaConfigurationFailure",
                    message: "Kafka consumer group ID could not be constructed.",
                  },
                },
              },
            },
            exhaustedAtNanos: 0n,
          });
          expect(fallbackAcquisitionOrder).toStrictEqual([]);
          yield* fallbackDiagnostics.close();
          yield* fallbackRuntime.close;

          groupIdSpy.mockImplementation(() => {
            throw new KafkaSourceConfigurationError("runtime group configuration failure");
          });
          const domainFailureRuntime = yield* makeViewServerRuntimeCore(config, {}).pipe(
            Effect.provide(domainFailureLayer),
          );
          const domainFailureDiagnostics =
            yield* domainFailureRuntime.liveClient.subscribeSourceHealth({
              topic: "orders",
            });
          const domainFailureExhausted = Option.getOrThrow(
            yield* domainFailureDiagnostics.events.pipe(
              Stream.filter((health) => health.status._tag === "Exhausted"),
              Stream.take(1),
              Stream.runHead,
            ),
          );
          expect(domainFailureExhausted.status).toStrictEqual({
            _tag: "Exhausted",
            exhaustion: {
              _tag: "RetryExhausted",
              lastTermination: {
                _tag: "Failed",
                failure: {
                  _tag: "AdapterFailure",
                  failure: {
                    _tag: "KafkaConfigurationFailure",
                    message: "runtime group configuration failure",
                  },
                },
              },
            },
            exhaustedAtNanos: 0n,
          });
          expect(domainFailureAcquisitionOrder).toStrictEqual([]);
          yield* domainFailureDiagnostics.close();
          yield* domainFailureRuntime.close;
        }),
      ),
  );

  it.effect("binds hostile Region failures and record metadata to the requested Region", () =>
    Effect.gen(function* () {
      const source = kafka.source(
        {
          cleanupPolicy: "delete",
          retentionPolicy: "Infinity",
          topic: "source-orders",
          regions: ["eu"],
          key: kafka.string(),
          value: kafka.json(() => Schema.toCodecJson(Schema.Struct({ price: Schema.Number }))),
          localRowKey: ({ key }) => key,
          map: ({ value, region }) => ({
            price: value.price,
            region: String(region),
          }),
          startFrom: "earliest",
        },
        Schedule.recurs(0),
      );
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source,
          },
        },
      });
      const metrics: KafkaRegionMetrics<"apac"> = {
        region: "apac",
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
      };
      const consumer = (
        records: Stream.Stream<KafkaServerRecord, KafkaAdapterFailure>,
      ): import("./server").KafkaServerRegionConsumer => ({
        records,
        recordDecoded: Effect.void,
        recordDecodeFailure: Effect.void,
        recordMapped: Effect.void,
        recordMappingFailure: Effect.void,
        recordRejection: Effect.void,
      });
      let invalidMetadataCallbackCount = 0;
      let invalidMetadataSettlementCount = 0;
      const invalidMetadataConsumer = (
        candidate: KafkaMessageMetadata,
      ): import("./server").KafkaServerRegionConsumer => {
        const recordCallback = Effect.sync(() => {
          invalidMetadataCallbackCount += 1;
        });
        return {
          records: Stream.make({
            key: bytes("metadata"),
            value: bytes(JSON.stringify({ price: 1 })),
            metadata: candidate,
            settlement: () =>
              Effect.sync(() => {
                invalidMetadataSettlementCount += 1;
              }),
          }),
          recordDecoded: recordCallback,
          recordDecodeFailure: recordCallback,
          recordMapped: recordCallback,
          recordMappingFailure: recordCallback,
          recordRejection: recordCallback,
        };
      };
      const invalidMetadataRegion = (candidate: KafkaMessageMetadata): KafkaServerRegion => ({
        acquire: () => Effect.succeed(invalidMetadataConsumer(candidate)),
        metrics: () => Effect.succeed(metrics),
      });
      const validRecord = (): KafkaServerRecord => ({
        key: bytes("record"),
        value: bytes(JSON.stringify({ price: 1 })),
        metadata: metadata("eu", 1n),
        settlement: () => Effect.void,
      });
      const recordWithValue = (
        property: keyof KafkaServerRecord,
        value: unknown,
      ): KafkaServerRecord =>
        new Proxy(validRecord(), {
          get: (target, current, receiver) =>
            current === property ? value : Reflect.get(target, current, receiver),
        });
      const invalidRecordRegion = (record: KafkaServerRecord): KafkaServerRegion => ({
        acquire: () => Effect.succeed(consumer(Stream.make(record))),
        metrics: () => Effect.succeed(metrics),
      });
      const metadataWithValue = (
        property: keyof KafkaMessageMetadata,
        value: unknown,
      ): KafkaMessageMetadata =>
        new Proxy(metadata("eu", 1n), {
          get: (target, current, receiver) =>
            current === property ? value : Reflect.get(target, current, receiver),
        });
      const expectConfigurationExhaustion = Effect.fn(
        "KafkaSourceAdapter.test.configurationExhaustion",
      )(function* (regionRuntime: KafkaServerRegion, message: string) {
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(
            makeKafkaServerLayer({
              brokerContracts: foreverBrokerContracts(["eu", "us"]),
              retentionSweepIntervalNanos: 900_000_000_000n,
              consumerGroupPrefix: "replica",
              regions: new Map([["eu", regionRuntime]]),
            }),
          ),
        );
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
        const exhausted = Option.getOrThrow(
          yield* diagnostics.events.pipe(
            Stream.filter((health) => health.status._tag === "Exhausted"),
            Stream.take(1),
            Stream.runHead,
          ),
        );
        expect(exhausted.status).toStrictEqual({
          _tag: "Exhausted",
          exhaustion: {
            _tag: "RetryExhausted",
            lastTermination: {
              _tag: "Failed",
              failure: {
                _tag: "AdapterFailure",
                failure: {
                  _tag: "KafkaConfigurationFailure",
                  message,
                },
              },
            },
          },
          exhaustedAtNanos: 0n,
        });
        yield* diagnostics.close();
        yield* runtime.close;
      });
      const expectInvalidSettlementExhaustion = Effect.fn(
        "KafkaSourceAdapter.test.invalidSettlementExhaustion",
      )(function* (regionRuntime: KafkaServerRegion) {
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(
            makeKafkaServerLayer({
              brokerContracts: foreverBrokerContracts(["eu", "us"]),
              retentionSweepIntervalNanos: 900_000_000_000n,
              consumerGroupPrefix: "replica",
              regions: new Map([["eu", regionRuntime]]),
            }),
          ),
        );
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
        const exhausted = Option.getOrThrow(
          yield* diagnostics.events.pipe(
            Stream.filter((health) => health.status._tag === "Exhausted"),
            Stream.take(1),
            Stream.runHead,
          ),
        );
        expect(exhausted.status).toStrictEqual({
          _tag: "Exhausted",
          exhaustion: {
            _tag: "RetryExhausted",
            lastTermination: {
              _tag: "Failed",
              failure: {
                _tag: "RuntimeFailure",
                failure: {
                  _tag: "InvalidSourceSettlement",
                  message: "Source Settlement callback threw before returning an Effect",
                },
              },
            },
          },
          exhaustedAtNanos: 0n,
        });
        yield* diagnostics.close();
        yield* runtime.close;
      });
      const wrongRegionFailure = acquisitionFailure("apac");
      const wrongTopicFailure = new Proxy(acquisitionFailure("eu"), {
        get: (target, property, receiver) =>
          property === "topic" ? "other-source" : Reflect.get(target, property, receiver),
      });
      const emptyTopicFailure = new Proxy(acquisitionFailure("eu"), {
        get: (target, property, receiver) =>
          property === "topic" ? "" : Reflect.get(target, property, receiver),
      });
      const nonStringMessageFailure = new Proxy(acquisitionFailure("eu"), {
        get: (target, property, receiver) =>
          property === "message" ? {} : Reflect.get(target, property, receiver),
      });
      const unknownTagFailure = new Proxy(acquisitionFailure("eu"), {
        get: (target, property, receiver) =>
          property === "_tag" ? "KafkaUnknownFailure" : Reflect.get(target, property, receiver),
      });
      const hostileFailure = () =>
        new Proxy(acquisitionFailure("eu"), {
          get: () => {
            throw new Error("failure read failed");
          },
        });
      const configurationFailureRegion: KafkaServerRegion = {
        acquire: () =>
          Effect.fail({
            _tag: "KafkaConfigurationFailure",
            message: "driver configuration failed",
          }),
        metrics: () => Effect.succeed(metrics),
      };
      const acquireFailureRegion: KafkaServerRegion = {
        acquire: () => Effect.fail(wrongRegionFailure),
        metrics: () => Effect.succeed(metrics),
      };
      const streamFailureRegion: KafkaServerRegion = {
        acquire: () => Effect.succeed(consumer(Stream.fail(wrongRegionFailure))),
        metrics: () => Effect.succeed(metrics),
      };
      const wrongTopicFailureRegion: KafkaServerRegion = {
        acquire: () => Effect.fail(wrongTopicFailure),
        metrics: () => Effect.succeed(metrics),
      };
      const emptyTopicFailureRegion: KafkaServerRegion = {
        acquire: () => Effect.fail(emptyTopicFailure),
        metrics: () => Effect.succeed(metrics),
      };
      const nonStringMessageFailureRegion: KafkaServerRegion = {
        acquire: () => Effect.fail(nonStringMessageFailure),
        metrics: () => Effect.succeed(metrics),
      };
      const unknownTagFailureRegion: KafkaServerRegion = {
        acquire: () => Effect.fail(unknownTagFailure),
        metrics: () => Effect.succeed(metrics),
      };
      const hostileAcquireFailureRegion: KafkaServerRegion = {
        acquire: () => Effect.fail(hostileFailure()),
        metrics: () => Effect.succeed(metrics),
      };
      const hostileStreamFailureRegion: KafkaServerRegion = {
        acquire: () => Effect.succeed(consumer(Stream.fail(hostileFailure()))),
        metrics: () => Effect.succeed(metrics),
      };
      const metadataFailureRegion: KafkaServerRegion = {
        acquire: () =>
          Effect.succeed(
            consumer(
              Stream.make({
                key: bytes("metadata"),
                value: bytes(JSON.stringify({ price: 1 })),
                metadata: metadata("apac", 1n),
                settlement: () => Effect.void,
              }),
            ),
          ),
        metrics: () => Effect.succeed(metrics),
      };
      const wrongTopicMetadataRegion = invalidMetadataRegion(
        metadataWithValue("sourceTopic", "other-source"),
      );
      const negativePartitionMetadataRegion = invalidMetadataRegion(
        metadataWithValue("partition", -1),
      );
      const fractionalPartitionMetadataRegion = invalidMetadataRegion(
        metadataWithValue("partition", 0.5),
      );
      const oversizedPartitionMetadataRegion = invalidMetadataRegion(
        metadataWithValue("partition", 2_147_483_648),
      );
      const nonNumberPartitionMetadataRegion = invalidMetadataRegion(
        metadataWithValue("partition", "0"),
      );
      const negativeOffsetMetadataRegion = invalidMetadataRegion(metadataWithValue("offset", -1n));
      const nonBigIntOffsetMetadataRegion = invalidMetadataRegion(metadataWithValue("offset", 0));
      const negativeTimestampMetadataRegion = invalidMetadataRegion(
        metadataWithValue("timestampNanos", -1n),
      );
      const nonBigIntTimestampMetadataRegion = invalidMetadataRegion(
        metadataWithValue("timestampNanos", 0),
      );
      const primitiveHeadersMetadataRegion = invalidMetadataRegion(
        metadataWithValue("headers", "invalid"),
      );
      const nullHeadersMetadataRegion = invalidMetadataRegion(metadataWithValue("headers", null));
      const arrayHeadersMetadataRegion = invalidMetadataRegion(metadataWithValue("headers", []));
      const nonPlainHeadersMetadataRegion = invalidMetadataRegion(
        metadataWithValue("headers", new Map()),
      );
      const symbolHeaders = {};
      Reflect.set(symbolHeaders, Symbol("invalid"), bytes("invalid"));
      const symbolHeadersMetadataRegion = invalidMetadataRegion(
        metadataWithValue("headers", symbolHeaders),
      );
      const ghostHeaders = new Proxy(
        {},
        {
          ownKeys: () => ["ghost"],
        },
      );
      const ghostHeadersMetadataRegion = invalidMetadataRegion(
        metadataWithValue("headers", ghostHeaders),
      );
      const hiddenHeaders = {};
      Object.defineProperty(hiddenHeaders, "hidden", {
        value: bytes("hidden"),
      });
      const hiddenHeadersMetadataRegion = invalidMetadataRegion(
        metadataWithValue("headers", hiddenHeaders),
      );
      const accessorHeaders = {};
      Object.defineProperty(accessorHeaders, "accessor", {
        enumerable: true,
        get: () => bytes("accessor"),
      });
      const accessorHeadersMetadataRegion = invalidMetadataRegion(
        metadataWithValue("headers", accessorHeaders),
      );
      const invalidHeaderValueMetadataRegion = invalidMetadataRegion(
        metadataWithValue("headers", { invalid: 1 }),
      );
      const invalidRepeatedHeaderMetadataRegion = invalidMetadataRegion(
        metadataWithValue("headers", { invalid: [bytes("valid"), 1] }),
      );
      const hostilePrototypeCause = new Proxy(
        {},
        {
          getPrototypeOf: () => {
            throw new Error("hostile metadata cause prototype inspected");
          },
        },
      );
      const hostilePrototypeMetadataRegion = invalidMetadataRegion(
        new Proxy(metadata("eu", 1n), {
          get: () => {
            throw hostilePrototypeCause;
          },
        }),
      );
      const hostileMessageCause = new Proxy(
        new kafkaContract.KafkaSourceConfigurationError("invalid"),
        {
          get: (target, property, receiver) =>
            property === "message" ? {} : Reflect.get(target, property, receiver),
        },
      );
      const hostileMessageMetadataRegion = invalidMetadataRegion(
        new Proxy(metadata("eu", 1n), {
          get: () => {
            throw hostileMessageCause;
          },
        }),
      );
      const hostileRecord = new Proxy(
        {
          key: bytes("hostile"),
          value: bytes(JSON.stringify({ price: 1 })),
          metadata: metadata("eu", 1n),
          settlement: () => Effect.void,
        },
        {
          get: () => {
            throw new Error("record read failed");
          },
        },
      );
      const hostileRecordRegion: KafkaServerRegion = {
        acquire: () => Effect.succeed(consumer(Stream.make(hostileRecord))),
        metrics: () => Effect.succeed(metrics),
      };
      const invalidKeyRegion = invalidRecordRegion(recordWithValue("key", "invalid"));
      const invalidValueRegion = invalidRecordRegion(recordWithValue("value", "invalid"));
      const invalidSettlementRegion = invalidRecordRegion(recordWithValue("settlement", "invalid"));
      const throwingSettlementRegion = invalidRecordRegion(
        recordWithValue("settlement", () => {
          throw new Error("settlement invocation failed");
        }),
      );
      const nonEffectSettlementRegion = invalidRecordRegion(
        recordWithValue("settlement", () => "invalid"),
      );
      const hostileSettlementResultRegion = invalidRecordRegion(
        recordWithValue(
          "settlement",
          () =>
            new Proxy(
              {},
              {
                has: () => {
                  throw new Error("settlement result inspection failed");
                },
              },
            ),
        ),
      );
      const settlementFailureRegion: KafkaServerRegion = {
        acquire: () =>
          Effect.succeed(
            consumer(
              Stream.make({
                key: bytes("settlement"),
                value: bytes(JSON.stringify({ price: 1 })),
                metadata: metadata("eu", 1n),
                settlement: () => Effect.fail(commitFailure("apac")),
              }),
            ),
          ),
        metrics: () => Effect.succeed(metrics),
      };
      const hostileSettlementFailureRegion: KafkaServerRegion = {
        acquire: () =>
          Effect.succeed(
            consumer(
              Stream.make({
                key: bytes("hostile-settlement"),
                value: bytes(JSON.stringify({ price: 1 })),
                metadata: metadata("eu", 1n),
                settlement: () => Effect.fail(hostileFailure()),
              }),
            ),
          ),
        metrics: () => Effect.succeed(metrics),
      };

      yield* expectConfigurationExhaustion(
        configurationFailureRegion,
        "driver configuration failed",
      );
      yield* expectConfigurationExhaustion(
        acquireFailureRegion,
        'Kafka Region "eu" returned a failure for "apac".',
      );
      yield* expectConfigurationExhaustion(
        streamFailureRegion,
        'Kafka Region "eu" returned a failure for "apac".',
      );
      yield* expectConfigurationExhaustion(
        wrongTopicFailureRegion,
        'Kafka Region "eu" returned a failure for source Topic "other-source".',
      );
      yield* expectConfigurationExhaustion(
        emptyTopicFailureRegion,
        'Kafka Region "eu" returned a failure for source Topic "".',
      );
      yield* expectConfigurationExhaustion(
        nonStringMessageFailureRegion,
        'Kafka Region "eu" returned an invalid failure.',
      );
      yield* expectConfigurationExhaustion(
        unknownTagFailureRegion,
        'Kafka Region "eu" returned an invalid failure.',
      );
      yield* expectConfigurationExhaustion(
        hostileAcquireFailureRegion,
        'Kafka Region "eu" returned an invalid failure.',
      );
      yield* expectConfigurationExhaustion(
        hostileStreamFailureRegion,
        'Kafka Region "eu" returned an invalid failure.',
      );
      yield* expectConfigurationExhaustion(
        metadataFailureRegion,
        'Kafka Region "eu" returned record metadata for "apac".',
      );
      yield* expectConfigurationExhaustion(
        wrongTopicMetadataRegion,
        'Kafka Region "eu" returned invalid record metadata.',
      );
      yield* expectConfigurationExhaustion(
        negativePartitionMetadataRegion,
        'Kafka Region "eu" returned invalid record metadata.',
      );
      yield* expectConfigurationExhaustion(
        fractionalPartitionMetadataRegion,
        'Kafka Region "eu" returned invalid record metadata.',
      );
      yield* expectConfigurationExhaustion(
        oversizedPartitionMetadataRegion,
        'Kafka Region "eu" returned invalid record metadata.',
      );
      yield* expectConfigurationExhaustion(
        nonNumberPartitionMetadataRegion,
        'Kafka Region "eu" returned invalid record metadata.',
      );
      yield* expectConfigurationExhaustion(
        negativeOffsetMetadataRegion,
        'Kafka Region "eu" returned invalid record metadata.',
      );
      yield* expectConfigurationExhaustion(
        nonBigIntOffsetMetadataRegion,
        'Kafka Region "eu" returned invalid record metadata.',
      );
      yield* expectConfigurationExhaustion(
        negativeTimestampMetadataRegion,
        'Kafka Region "eu" returned invalid record metadata.',
      );
      yield* expectConfigurationExhaustion(
        nonBigIntTimestampMetadataRegion,
        'Kafka Region "eu" returned invalid record metadata.',
      );
      yield* expectConfigurationExhaustion(
        primitiveHeadersMetadataRegion,
        'Kafka Region "eu" returned invalid record metadata.',
      );
      yield* expectConfigurationExhaustion(
        nullHeadersMetadataRegion,
        'Kafka Region "eu" returned invalid record metadata.',
      );
      yield* expectConfigurationExhaustion(
        arrayHeadersMetadataRegion,
        'Kafka Region "eu" returned invalid record metadata.',
      );
      yield* expectConfigurationExhaustion(
        nonPlainHeadersMetadataRegion,
        'Kafka Region "eu" returned invalid record metadata.',
      );
      yield* expectConfigurationExhaustion(
        symbolHeadersMetadataRegion,
        'Kafka Region "eu" returned invalid record metadata.',
      );
      yield* expectConfigurationExhaustion(
        ghostHeadersMetadataRegion,
        'Kafka Region "eu" returned invalid record metadata.',
      );
      yield* expectConfigurationExhaustion(
        hiddenHeadersMetadataRegion,
        'Kafka Region "eu" returned invalid record metadata.',
      );
      yield* expectConfigurationExhaustion(
        accessorHeadersMetadataRegion,
        'Kafka Region "eu" returned invalid record metadata.',
      );
      yield* expectConfigurationExhaustion(
        invalidHeaderValueMetadataRegion,
        'Kafka Region "eu" returned invalid record metadata.',
      );
      yield* expectConfigurationExhaustion(
        invalidRepeatedHeaderMetadataRegion,
        'Kafka Region "eu" returned invalid record metadata.',
      );
      yield* expectConfigurationExhaustion(
        hostilePrototypeMetadataRegion,
        'Kafka Region "eu" returned an invalid record.',
      );
      yield* expectConfigurationExhaustion(
        hostileMessageMetadataRegion,
        'Kafka Region "eu" returned an invalid record.',
      );
      yield* expectConfigurationExhaustion(
        hostileRecordRegion,
        'Kafka Region "eu" returned an invalid record.',
      );
      yield* expectConfigurationExhaustion(
        invalidKeyRegion,
        'Kafka Region "eu" returned an invalid record.',
      );
      yield* expectConfigurationExhaustion(
        invalidValueRegion,
        'Kafka Region "eu" returned an invalid record.',
      );
      yield* expectConfigurationExhaustion(
        invalidSettlementRegion,
        'Kafka Region "eu" returned an invalid record.',
      );
      yield* expectInvalidSettlementExhaustion(throwingSettlementRegion);
      yield* expectConfigurationExhaustion(
        nonEffectSettlementRegion,
        'Kafka Region "eu" returned an invalid record.',
      );
      yield* expectInvalidSettlementExhaustion(hostileSettlementResultRegion);
      yield* expectConfigurationExhaustion(
        settlementFailureRegion,
        'Kafka Region "eu" returned a failure for "apac".',
      );
      yield* expectConfigurationExhaustion(
        hostileSettlementFailureRegion,
        'Kafka Region "eu" returned an invalid failure.',
      );
      expect({
        invalidMetadataCallbackCount,
        invalidMetadataSettlementCount,
      }).toStrictEqual({
        invalidMetadataCallbackCount: 0,
        invalidMetadataSettlementCount: 0,
      });
    }),
  );

  it.effect("binds hostile Region metrics to the requested Region", () =>
    Effect.gen(function* () {
      const source = kafka.source({
        cleanupPolicy: "delete",
        retentionPolicy: "Infinity",
        topic: "source-orders",
        regions: ["eu"],
        key: kafka.string(),
        value: kafka.json(() => Schema.toCodecJson(Schema.Struct({ price: Schema.Number }))),
        localRowKey: ({ key }) => key,
        map: ({ value, region }) => ({
          price: value.price,
          region: String(region),
        }),
        startFrom: "earliest",
      });
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source,
          },
        },
      });
      const hostileMetrics: KafkaRegionMetrics = {
        region: "apac",
        assignments: [],
        commits: 1n,
        commitFailures: 2n,
        decoded: 3n,
        decodeFailures: 4n,
        mapped: 5n,
        mappingFailures: 6n,
        rejections: 7n,
        reconnects: 8n,
        rebalances: 9n,
        closes: 10n,
        closeFailures: 11n,
      };
      const regionRuntime: KafkaServerRegion = {
        acquire: () =>
          Effect.succeed({
            records: Stream.never,
            recordDecoded: Effect.void,
            recordDecodeFailure: Effect.void,
            recordMapped: Effect.void,
            recordMappingFailure: Effect.void,
            recordRejection: Effect.void,
          }),
        metrics: () => Effect.succeed(hostileMetrics),
      };
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(
          makeKafkaServerLayer({
            brokerContracts: foreverBrokerContracts(["eu", "us"]),
            retentionSweepIntervalNanos: 900_000_000_000n,
            consumerGroupPrefix: "replica",
            regions: new Map([["eu", regionRuntime]]),
          }),
        ),
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
      yield* TestClock.adjust("1 second");
      const health = Option.getOrThrow(
        yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
      );
      expect(health.metrics.adapter.regions).toStrictEqual([
        {
          ...hostileMetrics,
          region: "eu",
          retention: foreverRetentionMetrics(),
        },
      ]);
      yield* diagnostics.close();
      yield* runtime.close;

      const throwingMetrics = new Proxy(hostileMetrics, {
        ownKeys: () => {
          throw new Error("metrics materialization failed");
        },
      });
      const fallbackRegionRuntime: KafkaServerRegion = {
        ...regionRuntime,
        metrics: () => Effect.succeed(throwingMetrics),
      };
      const fallbackRuntime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(
          makeKafkaServerLayer({
            brokerContracts: foreverBrokerContracts(["eu", "us"]),
            retentionSweepIntervalNanos: 900_000_000_000n,
            consumerGroupPrefix: "replica",
            regions: new Map([["eu", fallbackRegionRuntime]]),
          }),
        ),
      );
      const fallbackDiagnostics = yield* fallbackRuntime.liveClient.subscribeSourceHealth({
        topic: "orders",
      });
      yield* TestClock.adjust("1 second");
      const fallbackHealth = Option.getOrThrow(
        yield* fallbackDiagnostics.events.pipe(Stream.take(1), Stream.runHead),
      );
      expect(fallbackHealth.metrics.adapter.regions).toStrictEqual([
        {
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
          retention: foreverRetentionMetrics(),
        },
      ]);
      yield* fallbackDiagnostics.close();
      yield* fallbackRuntime.close;
    }),
  );

  it.effect("reports missing aggregate Regions with exact empty local metrics", () =>
    Effect.gen(function* () {
      const source = kafka.source(
        {
          cleanupPolicy: "delete",
          retentionPolicy: "Infinity",
          topic: "source-orders",
          regions: ["missing"],
          key: kafka.string(),
          value: kafka.json(() => Schema.toCodecJson(Schema.Struct({ price: Schema.Number }))),
          localRowKey: ({ key }) => key,
          map: ({ value, region }) => ({
            price: value.price,
            region: String(region),
          }),
          startFrom: {
            mode: "durationAgo",
            duration: "1 minute",
            fallback: "latest",
          },
        },
        Schedule.recurs(0),
      );
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source,
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(
          makeKafkaServerLayer({
            brokerContracts: foreverBrokerContracts(["missing"]),
            retentionSweepIntervalNanos: 900_000_000_000n,
            consumerGroupPrefix: "replica",
            regions: new Map(),
          }),
        ),
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
      const exhausted = Option.getOrThrow(
        yield* diagnostics.events.pipe(
          Stream.filter((health) => health.status._tag === "Exhausted"),
          Stream.take(1),
          Stream.runHead,
        ),
      );
      expect(exhausted.status._tag).toBe("Exhausted");
      yield* TestClock.adjust("1 second");
      const refreshed = Option.getOrThrow(
        yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
      );
      expect(refreshed.metrics.adapter).toStrictEqual({
        activeGroupId: "replica:orders",
        start: {
          _tag: "Resolved",
          position: {
            mode: "durationAgo",
            durationNanos: 60_000_000_000n,
            resolvedAtNanos: 0n,
            atNanos: 0n,
            atMillis: 0n,
            fallback: "latest",
          },
        },
        regions: [
          {
            region: "missing",
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
            retention: foreverRetentionMetrics(),
          },
        ],
      });

      yield* diagnostics.close();
      yield* runtime.close;
    }),
  );
});
