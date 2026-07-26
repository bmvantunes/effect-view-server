import { describe, expect, it, vi } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { makeViewServerRuntimeCore } from "@effect-view-server/runtime-core";
import {
  Deferred,
  Effect,
  Exit,
  Layer,
  Option,
  Queue,
  Schedule,
  Schema,
  Scope,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import {
  kafka,
  type KafkaAdapterFailure,
  type KafkaMessageMetadata,
  type KafkaRegionMetrics,
  type KafkaStartPosition,
} from "./contract";
import * as kafkaContract from "./contract";
import {
  makeKafkaServerLayer,
  type KafkaServerRecord,
  type KafkaServerRegion,
  type KafkaServerRegionAcquireInput,
} from "./server";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
  region: Schema.String,
});

const bytes = (value: string): Uint8Array => new TextEncoder().encode(value);

const metadata = (
  region: string,
  offset: bigint,
  headers: KafkaMessageMetadata["headers"] = {},
): KafkaMessageMetadata => ({
  sourceTopic: "source-orders",
  sourceRegion: region,
  partition: 0,
  offset,
  timestampNanos: offset * 1_000_000n,
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
  }) => Effect.Effect<void>;
  readonly failStream: (failure: KafkaAdapterFailure) => Effect.Effect<void>;
  readonly counts: () => {
    readonly acquisitions: number;
    readonly finalizations: number;
  };
  readonly awaitAcquisitions: (count: number) => Effect.Effect<void>;
};

const awaitCondition = (predicate: () => boolean): Effect.Effect<void> =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 100_000; attempt += 1) {
      if (predicate()) {
        return;
      }
      yield* Effect.yieldNow;
    }
    return yield* Effect.die(new Error("Kafka server test condition was not satisfied."));
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
            metadata: metadata(region, input.offset, input.headers),
            settlement: (applicationExit) =>
              Exit.isSuccess(applicationExit) ? commit : Effect.void,
          });
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
      awaitAcquisitions: (count) => awaitCondition(() => acquisitions.length >= count),
    };
  });

const makeSource = (startFrom: KafkaStartPosition) =>
  kafka.source({
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

const makeFaultSource = (keyDecodeStarted?: Deferred.Deferred<void>) => {
  const decoder = new TextDecoder();
  return kafka.source({
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
  it.effect(
    "runs the complete materialized slice across Regions, commits poison items, and applies tombstones",
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
            { id: "eu:second", price: 30, region: "eu" },
            { id: "eu:shared", price: 10, region: "eu" },
          ],
          totalRows: 2,
          version: 4,
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

        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("orders");
        const health = Option.getOrThrow(
          yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
        );
        expect(health.status._tag).toBe("Degraded");
        const degraded = Option.getOrThrow(
          Option.liftPredicate(health.status, (status) => status._tag === "Degraded"),
        );
        expect(degraded.latestRejection.location).toStrictEqual({
          region: "eu",
          topic: "source-orders",
          partition: 0,
          offset: 2n,
          phase: "valueDecode",
          message: "Kafka value codec rejected the record.",
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
            },
            {
              region: "us",
              assignments: [],
              commits: 2n,
              commitFailures: 0n,
              decoded: 2n,
              decodeFailures: 0n,
              mapped: 1n,
              mappingFailures: 0n,
              rejections: 0n,
              reconnects: 0n,
              rebalances: 0n,
              closes: 0n,
              closeFailures: 0n,
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
      yield* awaitCondition(() => eu.counts().finalizations === 1);
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
      yield* awaitCondition(() => eu.commits.length === 1);
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "price", "region"],
      });
      expect(snapshot).toStrictEqual({
        rows: [{ id: "eu:retry", price: 2, region: "eu" }],
        totalRows: 1,
        version: 2,
        status: "ready",
        statusCode: "Ready",
      });

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
      const NamedValue = Schema.Struct({
        price: Schema.Number,
      });
      const source = kafka.source({
        topic: "source-orders",
        regions: ["eu"],
        key: kafka.codec({
          name: "key\ncodec",
          decode: ({ bytes: input, metadata: inputMetadata }) => {
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
            consumerGroupPrefix: "replica",
            regions: new Map([["eu", eu.runtime]]),
          }),
        ),
      );
      yield* eu.awaitAcquisitions(1);
      const latestRejection = Effect.fn("KafkaSourceAdapter.test.namedRejection")(function* () {
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("orders");
        const health = Option.getOrThrow(
          yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
        );
        yield* diagnostics.close();
        return Option.getOrThrow(
          Option.liftPredicate(health.status, (status) => status._tag === "Degraded"),
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
        rows: [{ id: "eu:good", price: 3, region: "eu" }],
        totalRows: 1,
        version: 1,
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
        commits: [1n, 2n, 3n, 4n],
        decoded: 1n,
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

  it.effect("replays a rejected record when its settlement commit fails", () =>
    Effect.gen(function* () {
      const acquisitionOrder: Array<string> = [];
      const eu = yield* makeFakeRegion("eu", acquisitionOrder);
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source: kafka.source({
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
            consumerGroupPrefix: "replica",
            regions: new Map([["eu", eu.runtime]]),
          }),
        ),
      );
      yield* eu.awaitAcquisitions(1);
      yield* eu.offer({
        key: "poison",
        value: "{",
        offset: 1n,
        commitFailure: commitFailure("eu"),
      });
      yield* awaitCondition(() => eu.counts().finalizations === 1);
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
        rows: [{ id: "eu:after", price: 2, region: "eu" }],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      yield* runtime.close;
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
            consumerGroupPrefix: "replica",
            regions: new Map([
              ["eu", eu.runtime],
              ["us", us.runtime],
            ]),
          }),
        ),
      );
      yield* awaitCondition(() => eu.counts().finalizations === 1);
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

      yield* runtime.close;
    }),
  );

  it.effect("settles item-local callback and shape faults, then continues the lane", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const acquisitionOrder: Array<string> = [];
        const eu = yield* makeFakeRegion("eu", acquisitionOrder);
        const keyDecodeStarted = yield* Deferred.make<void>();
        const originalKafkaRowId = kafkaContract.kafkaRowId;
        yield* Effect.acquireRelease(
          Effect.sync(() =>
            vi.spyOn(kafkaContract, "kafkaRowId").mockImplementation((input) => {
              if (input.localRowKey === "canonical-fail") {
                throw new Error("canonical row ID failed");
              }
              return originalKafkaRowId(input);
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
              consumerGroupPrefix: "replica",
              regions: new Map([["eu", eu.runtime]]),
            }),
          ),
        );
        yield* eu.awaitAcquisitions(1);
        const currentRejection = Effect.fn("KafkaSourceAdapter.test.rejection.current")(
          function* () {
            const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("orders");
            const health = Option.getOrThrow(
              yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
            );
            yield* diagnostics.close();
            return Option.getOrThrow(
              Option.liftPredicate(health.status, (status) => status._tag === "Degraded"),
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
            decoded: 0n,
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
            decoded: 0n,
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
            decoded: 0n,
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
            decoded: 1n,
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
            decoded: 2n,
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
            decoded: 3n,
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
            decoded: 4n,
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
            decoded: 5n,
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
            decoded: 6n,
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
            decoded: 7n,
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
            decoded: 8n,
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
            decoded: 9n,
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
          rows: [{ id: "eu:good", price: 17, region: "eu" }],
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
            consumerGroupPrefix: "replica",
            regions: new Map([["eu", eu.runtime]]),
          }),
        ),
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("orders");
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

  it.effect("reports invalid group input as a typed exhaustion while metrics remain total", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const acquisitionOrder: Array<string> = [];
        const eu = yield* makeFakeRegion("eu", acquisitionOrder);
        const source = kafka.source(
          {
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
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(
            makeKafkaServerLayer({
              consumerGroupPrefix: "\ud800",
              regions: new Map([["eu", eu.runtime]]),
            }),
          ),
        );
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("orders");
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
                  message:
                    "Kafka consumer group prefix and View Server Topic must contain well-formed Unicode.",
                },
              },
            },
          },
          exhaustedAtNanos: 0n,
        });
        yield* TestClock.adjust("1 second");
        const refreshed = Option.getOrThrow(
          yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
        );
        expect({
          acquisitions: acquisitionOrder,
          activeGroupId: refreshed.metrics.adapter.activeGroupId,
        }).toStrictEqual({
          acquisitions: [],
          activeGroupId: "invalid-kafka-consumer-group",
        });

        yield* diagnostics.close();
        yield* runtime.close;

        yield* Effect.acquireRelease(
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
        const fallbackAcquisitionOrder: Array<string> = [];
        const fallbackRegion = yield* makeFakeRegion("eu", fallbackAcquisitionOrder);
        const fallbackRuntime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(
            makeKafkaServerLayer({
              consumerGroupPrefix: "replica",
              regions: new Map([["eu", fallbackRegion.runtime]]),
            }),
          ),
        );
        const fallbackDiagnostics =
          yield* fallbackRuntime.liveClient.subscribeSourceHealth("orders");
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
          exhaustedAtNanos: 1_000_000_000n,
        });
        expect(fallbackAcquisitionOrder).toStrictEqual([]);
        yield* fallbackDiagnostics.close();
        yield* fallbackRuntime.close;
      }),
    ),
  );

  it.effect("binds hostile Region failures and record metadata to the requested Region", () =>
    Effect.gen(function* () {
      const source = kafka.source(
        {
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
              consumerGroupPrefix: "replica",
              regions: new Map([["eu", regionRuntime]]),
            }),
          ),
        );
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("orders");
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
      const wrongRegionFailure = acquisitionFailure("apac");
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
            consumerGroupPrefix: "replica",
            regions: new Map([["eu", regionRuntime]]),
          }),
        ),
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("orders");
      yield* TestClock.adjust("1 second");
      const health = Option.getOrThrow(
        yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
      );
      expect(health.metrics.adapter.regions).toStrictEqual([
        {
          ...hostileMetrics,
          region: "eu",
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
            consumerGroupPrefix: "replica",
            regions: new Map([["eu", fallbackRegionRuntime]]),
          }),
        ),
      );
      const fallbackDiagnostics = yield* fallbackRuntime.liveClient.subscribeSourceHealth("orders");
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
            consumerGroupPrefix: "replica",
            regions: new Map(),
          }),
        ),
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("orders");
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
          },
        ],
      });

      yield* diagnostics.close();
      yield* runtime.close;
    }),
  );
});
