import { describe, expect, it } from "@effect/vitest";
import {
  ViewServerId,
  defineViewServerConfig,
  type ViewServerHealth,
} from "@effect-view-server/config";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { Effect, Schema, SchemaGetter } from "effect";
import { make as makeBigDecimal } from "effect/BigDecimal";
import { viewServerDecodeHealth, viewServerEncodeHealth } from "./protocol-health-codec";
import { topicHealth } from "../test-harness/protocol";

const Failure = Schema.TaggedStruct("AggregateHealthFailure", {
  message: Schema.String,
  offset: Schema.BigInt,
});

const Metrics = Schema.Struct({
  observed: Schema.BigInt,
  watermark: Schema.BigDecimal,
});

const RejectionLocation = Schema.Struct({
  partition: Schema.Number,
  offset: Schema.BigInt,
});

const adapter = SourceAdapter.make({
  identity: {
    name: "aggregate-health-fixture",
    version: "1",
  },
  failure: Failure,
  materialized: {
    metrics: Metrics,
    rejectionLocation: RejectionLocation,
    definitionOptions: SourceAdapter.definitionOptions<{ readonly label: string }>(),
  },
  leased: {
    metrics: Metrics,
    rejectionLocation: RejectionLocation,
    definitionOptions: SourceAdapter.definitionOptions<{ readonly label: string }>(),
  },
});

const NotJsonSafeMetric = Schema.String.pipe(
  Schema.encodeTo(Schema.Any, {
    decode: SchemaGetter.transform((value) => (typeof value === "string" ? value : "decoded")),
    encode: SchemaGetter.transform(() => Symbol("not-json-safe")),
  }),
);
const notJsonSafeAdapter = SourceAdapter.make({
  identity: {
    name: "not-json-safe-health",
  },
  failure: Failure,
  materialized: {
    metrics: Schema.Struct({
      value: NotJsonSafeMetric,
    }),
    rejectionLocation: RejectionLocation,
    definitionOptions: SourceAdapter.definitionOptions<{ readonly label: string }>(),
  },
  leased: undefined,
});

const Row = Schema.Struct({
  id: ViewServerId,
  region: Schema.String,
  shard: Schema.BigInt,
});

const config = defineViewServerConfig({
  topics: {
    manual: {
      schema: Row,
    },
    materialized: {
      schema: Row,
      source: adapter.materializedSource({ label: "materialized" }),
    },
    leased: {
      schema: Row,
      source: adapter.leasedSource(["region", "shard"], { label: "leased" }),
    },
  },
});
const notJsonSafeConfig = defineViewServerConfig({
  topics: {
    materialized: {
      schema: Row,
      source: notJsonSafeAdapter.materializedSource({ label: "not-json-safe" }),
    },
  },
});

const runtimeMetrics = {
  startedAtNanos: 1n,
  lastAttemptStartedAtNanos: 2n,
  lastDeliveryAtNanos: 3n,
  lastRejectionAtNanos: 4n,
  lastAppliedMutationAtNanos: 5n,
  lastTerminationAtNanos: null,
  currentAttempt: 2n,
  retryCount: 1n,
  receivedDeliveryCount: 3n,
  rejectedItemCount: 1n,
  attemptedMutationCount: 4n,
  appliedUpsertCount: 2n,
  appliedDeleteCount: 1n,
  failedMutationCount: 1n,
  completedSettlementCount: 3n,
  failedSettlementCount: 1n,
  retainedRowCount: 2,
  lanes: [
    {
      id: "aggregate-health",
      buffer: {
        _tag: "Bounded",
        capacity: 16,
        depth: 2,
        highWaterMark: 4,
        overflowCount: 1n,
      },
    },
  ],
} as const;

const materializedHealth = {
  adapter: {
    name: "aggregate-health-fixture",
    version: "1",
  },
  target: {
    _tag: "Materialized",
  },
  status: {
    _tag: "Degraded",
    attempt: 2n,
    degradedAtNanos: 6n,
    latestRejection: {
      failure: {
        _tag: "AdapterFailure",
        failure: {
          _tag: "AggregateHealthFailure",
          message: "invalid item",
          offset: 7n,
        },
      },
      location: {
        partition: 0,
        offset: 7n,
      },
      rejectedAtNanos: 8n,
    },
  },
  metrics: {
    runtime: runtimeMetrics,
    adapter: {
      observed: 9n,
      watermark: makeBigDecimal(123n, 2),
    },
  },
  sampledAtNanos: 10n,
} as const;

const leasedHealth = {
  adapter: {
    name: "aggregate-health-fixture",
    version: "1",
  },
  target: {
    _tag: "Leased",
    route: {
      region: "eu",
      shard: 11n,
    },
  },
  status: {
    _tag: "Ready",
    attempt: 2n,
    readyAtNanos: 12n,
  },
  metrics: {
    runtime: {
      ...runtimeMetrics,
      lastRejectionAtNanos: null,
      rejectedItemCount: 0n,
    },
    adapter: {
      observed: 13n,
      watermark: makeBigDecimal(456n, 2),
    },
  },
  sampledAtNanos: 14n,
} as const;

const aggregateHealth = {
  status: "degraded",
  version: 1,
  uptimeMs: 15,
  engine: {
    topics: {
      manual: topicHealth,
      materialized: {
        ...topicHealth,
        status: "degraded",
      },
      leased: topicHealth,
    },
  },
  sources: {
    materialized: materializedHealth,
    leased: [leasedHealth],
  },
  transport: {
    activeClients: 1,
    activeStreams: 2,
    activeSubscriptions: 3,
    messagesPerSecond: 4,
    bytesPerSecond: 5,
    queuedMessages: 6,
    queuedBytes: 7,
    droppedClients: 8,
    backpressureEvents: 9,
    reconnects: 10,
    lastError: null,
  },
} as const satisfies ViewServerHealth<typeof config.topics>;

const isJsonArray = (candidate: Schema.Json): candidate is Schema.JsonArray =>
  Array.isArray(candidate);

const requireJsonObject = (candidate: Schema.Json): Schema.JsonObject | undefined =>
  typeof candidate === "object" && candidate !== null && !isJsonArray(candidate)
    ? candidate
    : undefined;

function typedAggregateHealth(candidate: unknown): ViewServerHealth<typeof config.topics>;
function typedAggregateHealth(candidate: unknown): unknown {
  return candidate;
}

function typedConfig(candidate: unknown): typeof config;
function typedConfig(candidate: unknown): unknown {
  return candidate;
}

describe("Aggregate Source Health wire contract", () => {
  it.effect("round-trips exact materialized and active leased health as canonical JSON", () =>
    Effect.gen(function* () {
      const encoded = yield* viewServerEncodeHealth(config, aggregateHealth);
      const decoded = yield* viewServerDecodeHealth(config, encoded);

      expect(yield* viewServerEncodeHealth(config, decoded)).toStrictEqual(encoded);
      expect(JSON.stringify(encoded)).toContain('"currentAttempt":"2"');
      expect(JSON.stringify(encoded)).toContain('"observed":"13"');
      expect(Object.keys(encoded.sources)).toStrictEqual(["materialized", "leased"]);
      expect(Object.hasOwn(encoded.sources, "manual")).toBe(false);
    }),
  );

  it.effect("round-trips an empty active set for a leased source", () =>
    Effect.gen(function* () {
      const health = {
        ...aggregateHealth,
        sources: {
          ...aggregateHealth.sources,
          leased: [],
        },
      };
      const encoded = yield* viewServerEncodeHealth(config, health);
      const decoded = yield* viewServerDecodeHealth(config, encoded);

      expect(yield* viewServerEncodeHealth(config, decoded)).toStrictEqual(encoded);
      expect(encoded.sources["leased"]).toStrictEqual([]);
    }),
  );

  it.effect("rejects missing, unknown, and source-free aggregate source keys", () =>
    Effect.gen(function* () {
      const encoded = yield* viewServerEncodeHealth(config, aggregateHealth);
      const missing = yield* Effect.flip(
        viewServerDecodeHealth(config, {
          ...encoded,
          sources: {
            leased: encoded.sources["leased"] ?? null,
          },
        }),
      );
      const unknown = yield* Effect.flip(
        viewServerDecodeHealth(config, {
          ...encoded,
          sources: {
            ...encoded.sources,
            unknown: {},
          },
        }),
      );
      const sourceFree = yield* Effect.flip(
        viewServerDecodeHealth(config, {
          ...encoded,
          sources: {
            ...encoded.sources,
            manual: {},
          },
        }),
      );

      expect([missing.message, unknown.message, sourceFree.message]).toStrictEqual([
        "Health payload is missing source topic: materialized",
        "Health payload references unknown or source-free source topic: unknown",
        "Health payload references unknown or source-free source topic: manual",
      ]);
    }),
  );

  it.effect("rejects aggregate lifecycle cardinality and configured adapter identity drift", () =>
    Effect.gen(function* () {
      const encoded = yield* viewServerEncodeHealth(config, aggregateHealth);
      const materialized = requireJsonObject(encoded.sources["materialized"] ?? null);
      if (materialized === undefined) {
        return yield* Effect.die("Expected encoded materialized Source Health.");
      }
      const leasedCardinality = yield* Effect.flip(
        viewServerDecodeHealth(config, {
          ...encoded,
          sources: {
            ...encoded.sources,
            leased: encoded.sources["materialized"] ?? null,
          },
        }),
      );
      const materializedCardinality = yield* Effect.flip(
        viewServerDecodeHealth(config, {
          ...encoded,
          sources: {
            ...encoded.sources,
            materialized: [],
          },
        }),
      );
      const identity = yield* Effect.flip(
        viewServerDecodeHealth(config, {
          ...encoded,
          sources: {
            ...encoded.sources,
            materialized: {
              ...materialized,
              adapter: {
                name: "different-adapter",
                version: "1",
              },
            },
          },
        }),
      );

      expect(leasedCardinality.message).toBe(
        "Leased aggregate Source Health for leased must be an array.",
      );
      expect(materializedCardinality.message).toContain(
        "Source Health adapter identity must match aggregate-health-fixture.",
      );
      expect(identity.message).toBe(
        "Source Health adapter identity must match aggregate-health-fixture.",
      );
    }),
  );

  it.effect("rejects extra leased route fields and invalid exact adapter metrics", () =>
    Effect.gen(function* () {
      const encoded = yield* viewServerEncodeHealth(config, aggregateHealth);
      const leasedValues = encoded.sources["leased"];
      if (!Array.isArray(leasedValues)) {
        return yield* Effect.die("Expected encoded leased Source Health array.");
      }
      const first = requireJsonObject(leasedValues[0] ?? null);
      const target = requireJsonObject(first?.["target"] ?? null);
      const route = requireJsonObject(target?.["route"] ?? null);
      const materialized = requireJsonObject(encoded.sources["materialized"] ?? null);
      const metrics = requireJsonObject(materialized?.["metrics"] ?? null);
      const adapterMetrics = requireJsonObject(metrics?.["adapter"] ?? null);
      if (
        first === undefined ||
        target === undefined ||
        route === undefined ||
        materialized === undefined ||
        metrics === undefined ||
        adapterMetrics === undefined
      ) {
        return yield* Effect.die("Expected encoded aggregate Source Health object fields.");
      }

      const routeFailure = yield* Effect.flip(
        viewServerDecodeHealth(config, {
          ...encoded,
          sources: {
            ...encoded.sources,
            leased: [
              {
                ...first,
                target: {
                  ...target,
                  route: {
                    ...route,
                    extra: "not-a-route-field",
                  },
                },
              },
            ],
          },
        }),
      );
      const metricsFailure = yield* Effect.flip(
        viewServerDecodeHealth(config, {
          ...encoded,
          sources: {
            ...encoded.sources,
            materialized: {
              ...materialized,
              metrics: {
                ...metrics,
                adapter: {
                  ...adapterMetrics,
                  observed: 9,
                },
              },
            },
          },
        }),
      );

      expect(routeFailure.message).toBe(
        "Leased Source routeBy must contain all and only: region, shard.",
      );
      expect(metricsFailure.message).toMatch(/Invalid aggregate Source Health value/);
    }),
  );

  it.effect("validates aggregate Source Health before server-side encoding", () =>
    Effect.gen(function* () {
      const invalidHealth = {
        ...aggregateHealth,
        sources: {
          ...aggregateHealth.sources,
          materialized: {
            ...materializedHealth,
            adapter: {
              name: "different-adapter",
              version: "1",
            },
          },
        },
      };
      const failure = yield* Effect.flip(viewServerEncodeHealth(config, invalidHealth));

      expect(failure.message).toBe(
        "Source Health adapter identity must match aggregate-health-fixture.",
      );
    }),
  );

  it.effect("rejects hostile source records and invalid leased runtime values while encoding", () =>
    Effect.gen(function* () {
      const hostileSources = new Proxy(
        {},
        {
          ownKeys: () => {
            throw new Error("hostile ownKeys");
          },
        },
      );
      const hostileKeys = yield* Effect.flip(
        viewServerEncodeHealth(
          config,
          typedAggregateHealth({
            ...aggregateHealth,
            sources: hostileSources,
          }),
        ),
      );
      const invalidSourcesObject = yield* Effect.flip(
        viewServerEncodeHealth(
          config,
          typedAggregateHealth({
            ...aggregateHealth,
            sources: null,
          }),
        ),
      );
      const symbolicSourceKey = Symbol("source");
      const symbolicKeys = yield* Effect.flip(
        viewServerEncodeHealth(
          config,
          typedAggregateHealth({
            ...aggregateHealth,
            sources: {
              ...aggregateHealth.sources,
              [symbolicSourceKey]: materializedHealth,
            },
          }),
        ),
      );
      const throwingValue = Object.defineProperty(
        {
          leased: aggregateHealth.sources.leased,
        },
        "materialized",
        {
          enumerable: true,
          get: () => {
            throw new Error("hostile source value");
          },
        },
      );
      const hostileValue = yield* Effect.flip(
        viewServerEncodeHealth(
          config,
          typedAggregateHealth({
            ...aggregateHealth,
            sources: throwingValue,
          }),
        ),
      );
      const leasedCardinality = yield* Effect.flip(
        viewServerEncodeHealth(
          config,
          typedAggregateHealth({
            ...aggregateHealth,
            sources: {
              ...aggregateHealth.sources,
              leased: leasedHealth,
            },
          }),
        ),
      );
      const leasedIdentity = yield* Effect.flip(
        viewServerEncodeHealth(
          config,
          typedAggregateHealth({
            ...aggregateHealth,
            sources: {
              ...aggregateHealth.sources,
              leased: [
                {
                  ...leasedHealth,
                  adapter: {
                    name: "different-adapter",
                    version: "1",
                  },
                },
              ],
            },
          }),
        ),
      );

      expect(hostileKeys.message).toBe("Health payload sources must contain string topic keys.");
      expect(invalidSourcesObject.message).toBe(
        "Health payload sources must be a topic-keyed object.",
      );
      expect(symbolicKeys.message).toBe("Health payload sources must contain string topic keys.");
      expect(hostileValue.message).toBe(
        "Source Health adapter identity must match aggregate-health-fixture.",
      );
      expect(leasedCardinality.message).toBe(
        "Leased aggregate Source Health for leased must be an array.",
      );
      expect(leasedIdentity.message).toBe(
        "Source Health adapter identity must match aggregate-health-fixture.",
      );
    }),
  );

  it.effect("maps aggregate contract compilation failures to health errors", () =>
    Effect.gen(function* () {
      const encoded = yield* viewServerEncodeHealth(config, aggregateHealth);
      const malformedConfig = typedConfig({
        ...config,
        topics: {
          ...config.topics,
          leased: {
            ...config.topics.leased,
            schema: Schema.Struct({
              id: ViewServerId,
            }),
          },
        },
      });

      const failure = yield* Effect.flip(viewServerDecodeHealth(malformedConfig, encoded));
      const encodeFailure = yield* Effect.flip(
        viewServerEncodeHealth(malformedConfig, aggregateHealth),
      );

      expect(failure.message).toBe("Topic leased Source route field region is missing.");
      expect(encodeFailure.message).toBe("Topic leased Source route field region is missing.");
    }),
  );

  it.effect("rejects adapter health metrics that cannot be materialized as JSON", () =>
    Effect.gen(function* () {
      const health = {
        ...aggregateHealth,
        engine: {
          topics: {
            materialized: topicHealth,
          },
        },
        sources: {
          materialized: {
            ...materializedHealth,
            adapter: notJsonSafeAdapter.identity,
            metrics: {
              ...materializedHealth.metrics,
              adapter: {
                value: "not-json-safe",
              },
            },
          },
        },
      } satisfies ViewServerHealth<typeof notJsonSafeConfig.topics>;

      const failure = yield* Effect.flip(viewServerEncodeHealth(notJsonSafeConfig, health));

      expect(failure.message).toMatch(/^Aggregate Source Health is not wire-safe:/);
    }),
  );
});
