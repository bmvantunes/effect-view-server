import { beforeEach, describe, expect, expectTypeOf, it } from "@effect/vitest";
// Vitest's mock transform requires this API to come directly from "vitest";
// the @effect/vitest re-export cannot be hoisted.
import { vi } from "vitest";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { makeViewServerRuntimeCore } from "@effect-view-server/runtime-core";
import type {
  SourceApplicationExit,
  SourceRuntimeFailure,
} from "effect-view-server/source-adapter";
import { runViewServerRuntime } from "@effect-view-server/runtime";
import { Buffer } from "node:buffer";
import {
  Cause,
  Config,
  Effect,
  Exit,
  Fiber,
  Layer as EffectLayer,
  Logger,
  Option,
  References,
  Schedule,
  Schema,
  Scope,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import {
  KafkaSourceAdapter,
  KafkaSourceConfigurationError,
  kafka,
  type KafkaSourceRetryPolicy,
  type KafkaStartPosition,
} from "./contract";
import { layer, layerConfig, type KafkaBrokerContractValidationFailure } from "./node";
import { kafkaNodeInternals } from "./node-internal";

const platformatic = vi.hoisted(() => {
  type ConsumerOptions = {
    readonly groupId: string;
    readonly bootstrapBrokers: ReadonlyArray<string>;
    readonly clientId: string;
    readonly tls?: {
      readonly ca?: string | Uint8Array | ReadonlyArray<string | Uint8Array>;
      readonly cert?: string | Uint8Array | ReadonlyArray<string | Uint8Array>;
      readonly key?: string | Uint8Array | ReadonlyArray<string | Uint8Array>;
      readonly rejectUnauthorized?: boolean;
      readonly servername?: string;
    };
  };
  type AdminOptions = {
    readonly bootstrapBrokers: ReadonlyArray<string>;
    readonly clientId: string;
  };
  type DescribeConfigsInput = {
    readonly resources: ReadonlyArray<{
      readonly resourceType: number;
      readonly resourceName: string;
      readonly configurationKeys?: ReadonlyArray<string>;
    }>;
  };
  type OffsetInput = {
    readonly topics: ReadonlyArray<string>;
    readonly timestamp: bigint;
  };
  type CommittedInput = {
    readonly topics: ReadonlyArray<{
      readonly topic: string;
      readonly partitions: ReadonlyArray<number>;
    }>;
  };
  type ConsumeInput = {
    readonly autocommit: boolean;
    readonly topics: ReadonlyArray<string>;
    readonly mode: string;
    readonly offsets: ReadonlyArray<{
      readonly topic: string;
      readonly partition: number;
      readonly offset: bigint;
    }>;
  };
  type FakeMessage = {
    readonly key: Buffer | null | undefined;
    readonly value: Buffer | null | undefined;
    readonly headers: ReadonlyMap<Buffer, Buffer>;
    readonly topic: string;
    readonly partition: number;
    readonly timestamp: bigint;
    readonly offset: bigint;
    readonly metadata: Readonly<Record<string, never>>;
    readonly commit: () => Promise<void>;
    readonly toJSON: () => Readonly<Record<string, never>>;
  };
  type Next =
    | {
        readonly done: false;
        readonly value: FakeMessage;
      }
    | {
        readonly done: true;
        readonly value: undefined;
      };
  type EventPayload = {
    readonly assignments?: ReadonlyArray<{
      readonly topic: string;
      readonly partitions: ReadonlyArray<number>;
    }>;
  };
  type LagPayload = Map<string, ReadonlyArray<bigint>>;
  type EventHandler = (payload?: EventPayload | LagPayload) => void;

  class ControlledStream implements AsyncIterable<FakeMessage> {
    readonly queued: Array<FakeMessage> = [];
    readonly waiters: Array<{
      readonly resolve: (next: Next) => void;
      readonly reject: (error: Error) => void;
    }> = [];
    closed = false;
    failClose = false;
    failure: Error | undefined;

    push(message: FakeMessage): void {
      const waiter = this.waiters.shift();
      if (waiter === undefined) {
        this.queued.push(message);
      } else {
        waiter.resolve({ done: false, value: message });
      }
    }

    finish(): void {
      this.closed = true;
      for (const waiter of this.waiters.splice(0)) {
        waiter.resolve({ done: true, value: undefined });
      }
    }

    async close(): Promise<void> {
      await Promise.resolve();
      if (this.failClose) {
        this.failClose = false;
        throw new Error("stream close failed");
      }
      this.finish();
    }

    fail(error: Error): void {
      this.failure = error;
      for (const waiter of this.waiters.splice(0)) {
        waiter.reject(error);
      }
    }

    [Symbol.asyncIterator](): AsyncIterator<FakeMessage, undefined> {
      if (state.failNextIterator) {
        state.failNextIterator = false;
        throw new Error("iterator acquisition failed");
      }
      return {
        next: () => {
          const message = this.queued.shift();
          if (message !== undefined) {
            return Promise.resolve({
              done: false,
              value: message,
            });
          }
          if (this.closed) {
            return Promise.resolve({
              done: true,
              value: undefined,
            });
          }
          if (this.failure !== undefined) {
            return Promise.reject(this.failure);
          }
          return new Promise<Next>((resolve, reject) => {
            this.waiters.push({ resolve, reject });
          });
        },
        return: () => {
          this.finish();
          return Promise.resolve({
            done: true,
            value: undefined,
          });
        },
      };
    }
  }

  type State = {
    readonly admins: Array<Admin>;
    readonly describeConfigCalls: Array<{
      readonly clientId: string;
      readonly input: DescribeConfigsInput;
    }>;
    readonly brokerConfigs: Map<
      string,
      {
        readonly cleanupPolicy: string;
        readonly retentionMs: string;
      }
    >;
    readonly describeConfigResponses: Array<unknown>;
    readonly consumers: Array<Consumer>;
    readonly offsetCalls: Array<{
      readonly groupId: string;
      readonly input: OffsetInput;
    }>;
    readonly committedCalls: Array<{
      readonly groupId: string;
      readonly input: CommittedInput;
    }>;
    readonly consumeCalls: Array<{
      readonly groupId: string;
      readonly input: ConsumeInput;
    }>;
    readonly streams: Array<ControlledStream>;
    readonly offsetsByTimestamp: Map<bigint, ReadonlyArray<bigint>>;
    readonly committedByGroup: Map<string, ReadonlyArray<bigint>>;
    failNextConstruction: boolean;
    failNextAdminConstruction: boolean;
    failNextAdminClose: boolean;
    failNextDescribeConfigs: boolean;
    blockNextDescribeConfigs: boolean;
    failNextListOffsets: boolean;
    failNextListCommitted: boolean;
    failNextConsume: boolean;
    failNextConsumerClose: boolean;
    failNextIterator: boolean;
    failNextListenerRegistration: boolean;
    failNextListenerRemoval: boolean;
    failNextStreamClose: boolean;
    failNextStartLagMonitoring: boolean;
    failNextStopLagMonitoring: boolean;
  };
  const state: State = {
    admins: [],
    describeConfigCalls: [],
    brokerConfigs: new Map(),
    describeConfigResponses: [],
    consumers: [],
    offsetCalls: [],
    committedCalls: [],
    consumeCalls: [],
    streams: [],
    offsetsByTimestamp: new Map(),
    committedByGroup: new Map(),
    failNextConstruction: false,
    failNextAdminConstruction: false,
    failNextAdminClose: false,
    failNextDescribeConfigs: false,
    blockNextDescribeConfigs: false,
    failNextListOffsets: false,
    failNextListCommitted: false,
    failNextConsume: false,
    failNextConsumerClose: false,
    failNextIterator: false,
    failNextListenerRegistration: false,
    failNextListenerRemoval: false,
    failNextStreamClose: false,
    failNextStartLagMonitoring: false,
    failNextStopLagMonitoring: false,
  };

  class Admin {
    readonly options: AdminOptions;
    closed = false;

    constructor(options: AdminOptions) {
      if (state.failNextAdminConstruction) {
        state.failNextAdminConstruction = false;
        throw new Error("admin construction failed");
      }
      this.options = options;
      state.admins.push(this);
    }

    describeConfigs(input: DescribeConfigsInput): Promise<unknown> {
      state.describeConfigCalls.push({
        clientId: this.options.clientId,
        input,
      });
      if (state.failNextDescribeConfigs) {
        state.failNextDescribeConfigs = false;
        return Promise.reject(new Error("describe configs failed"));
      }
      if (state.blockNextDescribeConfigs) {
        state.blockNextDescribeConfigs = false;
        return new Promise(() => undefined);
      }
      const configuredResponse = state.describeConfigResponses.shift();
      if (configuredResponse !== undefined) {
        return Promise.resolve(configuredResponse);
      }
      return Promise.resolve(
        input.resources.map((resource) => {
          const configured = state.brokerConfigs.get(resource.resourceName) ?? {
            cleanupPolicy: "delete",
            retentionMs: "-1",
          };
          return {
            resourceType: 2,
            resourceName: resource.resourceName,
            configs: [
              {
                name: "cleanup.policy",
                value: configured.cleanupPolicy,
              },
              {
                name: "retention.ms",
                value: configured.retentionMs,
              },
            ],
          };
        }),
      );
    }

    close(): Promise<void> {
      this.closed = true;
      if (state.failNextAdminClose) {
        state.failNextAdminClose = false;
        return Promise.reject(new Error("admin close failed"));
      }
      return Promise.resolve();
    }
  }

  class Consumer {
    readonly options: ConsumerOptions;
    readonly handlers = new Map<string, Set<EventHandler>>();
    assignments: ReadonlyArray<{
      readonly topic: string;
      readonly partitions: ReadonlyArray<number>;
    }> | null = null;
    closed = false;
    lagMonitoring = false;

    constructor(options: ConsumerOptions) {
      if (state.failNextConstruction) {
        state.failNextConstruction = false;
        throw new Error("construction failed");
      }
      this.options = options;
      state.consumers.push(this);
    }

    listOffsets(input: OffsetInput): Promise<Map<string, Array<bigint>>> {
      if (state.failNextListOffsets) {
        state.failNextListOffsets = false;
        return Promise.reject(new Error("list offsets failed"));
      }
      state.offsetCalls.push({
        groupId: this.options.groupId,
        input,
      });
      return Promise.resolve(
        new Map([
          [input.topics[0] ?? "", [...(state.offsetsByTimestamp.get(input.timestamp) ?? [])]],
        ]),
      );
    }

    listCommittedOffsets(input: CommittedInput): Promise<Map<string, Array<bigint>>> {
      if (state.failNextListCommitted) {
        state.failNextListCommitted = false;
        return Promise.reject(new Error("list committed offsets failed"));
      }
      state.committedCalls.push({
        groupId: this.options.groupId,
        input,
      });
      return Promise.resolve(
        new Map([
          [
            input.topics[0]?.topic ?? "",
            [...(state.committedByGroup.get(this.options.groupId) ?? [])],
          ],
        ]),
      );
    }

    consume(input: ConsumeInput): Promise<ControlledStream> {
      if (state.failNextConsume) {
        state.failNextConsume = false;
        return Promise.reject(new Error("consume failed"));
      }
      state.consumeCalls.push({
        groupId: this.options.groupId,
        input,
      });
      const stream = new ControlledStream();
      stream.failClose = state.failNextStreamClose;
      state.failNextStreamClose = false;
      state.streams.push(stream);
      return Promise.resolve(stream);
    }

    on(event: string, handler: EventHandler): void {
      const handlers = this.handlers.get(event) ?? new Set<EventHandler>();
      handlers.add(handler);
      this.handlers.set(event, handlers);
      if (state.failNextListenerRegistration) {
        state.failNextListenerRegistration = false;
        throw new Error("listener registration failed");
      }
    }

    off(event: string, handler: EventHandler): void {
      this.handlers.get(event)?.delete(handler);
      if (state.failNextListenerRemoval) {
        state.failNextListenerRemoval = false;
        throw new Error("listener removal failed");
      }
    }

    emit(event: string, payload?: EventPayload): void {
      for (const handler of this.handlers.get(event) ?? []) {
        handler(payload);
      }
    }

    emitLag(payload: LagPayload): void {
      for (const handler of this.handlers.get("consumer:lag") ?? []) {
        handler(payload);
      }
    }

    startLagMonitoring(): void {
      if (state.failNextStartLagMonitoring) {
        state.failNextStartLagMonitoring = false;
        throw new Error("lag monitoring start failed");
      }
      this.lagMonitoring = true;
    }

    stopLagMonitoring(): void {
      if (state.failNextStopLagMonitoring) {
        state.failNextStopLagMonitoring = false;
        throw new Error("lag monitoring stop failed");
      }
      this.lagMonitoring = false;
    }

    close(): Promise<void> {
      this.closed = true;
      if (state.failNextConsumerClose) {
        state.failNextConsumerClose = false;
        return Promise.reject(new Error("consumer close failed"));
      }
      return Promise.resolve();
    }
  }

  const reset = (): void => {
    state.admins.splice(0);
    state.describeConfigCalls.splice(0);
    state.brokerConfigs.clear();
    state.describeConfigResponses.splice(0);
    state.consumers.splice(0);
    state.offsetCalls.splice(0);
    state.committedCalls.splice(0);
    state.consumeCalls.splice(0);
    state.streams.splice(0);
    state.offsetsByTimestamp.clear();
    state.committedByGroup.clear();
    state.failNextConstruction = false;
    state.failNextAdminConstruction = false;
    state.failNextAdminClose = false;
    state.failNextDescribeConfigs = false;
    state.blockNextDescribeConfigs = false;
    state.failNextListOffsets = false;
    state.failNextListCommitted = false;
    state.failNextConsume = false;
    state.failNextConsumerClose = false;
    state.failNextIterator = false;
    state.failNextListenerRegistration = false;
    state.failNextListenerRemoval = false;
    state.failNextStreamClose = false;
    state.failNextStartLagMonitoring = false;
    state.failNextStopLagMonitoring = false;
  };

  return {
    Admin,
    Consumer,
    reset,
    state,
  };
});

vi.mock("@platformatic/kafka", () => ({
  Admin: platformatic.Admin,
  ConfigResourceTypes: {
    TOPIC: 2,
  },
  Consumer: platformatic.Consumer,
}));

const Order = Schema.Struct({
  id: ViewServerId,
  price: Schema.Number,
  region: Schema.String,
});

const makeConfig = (startFrom: KafkaStartPosition, retry?: KafkaSourceRetryPolicy<"eu">) => {
  const sourceOptions = {
    cleanupPolicy: "delete" as const,
    retentionPolicy: "Infinity" as const,
    topic: "source-orders",
    regions: ["eu"] satisfies readonly ["eu"],
    key: kafka.string(),
    value: kafka.json(() =>
      Schema.toCodecJson(
        Schema.Struct({
          price: Schema.Number,
        }),
      ),
    ),
    localRowKey: ({ key }: { readonly key: string }) => key,
    map: ({
      key,
      value,
      region,
    }: {
      readonly key: string;
      readonly value: { readonly price: number };
      readonly region: "eu";
    }) => {
      if (key === "mapping-failure") {
        throw new Error("mapping failed");
      }
      return {
        price: value.price,
        region: String(region),
      };
    },
    startFrom,
  };
  const source =
    retry === undefined ? kafka.source(sourceOptions) : kafka.source(sourceOptions, retry);
  return defineViewServerConfig({
    topics: {
      orders: {
        schema: Order,
        source,
      },
    },
  });
};

const makeBatchedBrokerConfig = () =>
  defineViewServerConfig({
    topics: {
      inventory: {
        schema: Order,
        source: kafka.source({
          cleanupPolicy: "delete",
          retentionPolicy: "match-kafka-retention",
          topic: "source-inventory",
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
      orders: {
        schema: Order,
        source: kafka.source({
          cleanupPolicy: "delete",
          retentionPolicy: "match-kafka-retention",
          topic: "source-orders",
          regions: ["eu", "us"],
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

const makeConfigWithMalformedTopic = () => {
  const config = makeConfig("earliest");
  const topics = {
    orders: config.topics.orders,
  };
  Reflect.deleteProperty(topics, "orders");
  Object.defineProperty(topics, "\udfff", {
    enumerable: true,
    value: config.topics.orders,
  });
  return {
    ...config,
    topics,
  };
};

const makeConfigWithNonKafkaDefinitions = () => {
  const config = makeConfig("earliest");
  const topics = {
    orders: config.topics.orders,
  };
  Object.defineProperties(topics, {
    nullDefinition: {
      enumerable: true,
      value: null,
    },
    objectDefinition: {
      enumerable: true,
      value: {},
    },
    primitiveDefinition: {
      enumerable: true,
      value: "not-a-topic-definition",
    },
  });
  return {
    ...config,
    topics,
  };
};

const awaitCondition = (predicate: () => boolean): Effect.Effect<void> =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 100_000; attempt += 1) {
      if (predicate()) {
        return;
      }
      yield* Effect.yieldNow;
    }
    return yield* Effect.die(new Error("Kafka Node test condition was not satisfied."));
  });

const message = (input: {
  readonly groupId: string;
  readonly key: string | null | undefined;
  readonly price: number | null | undefined;
  readonly offset: bigint;
  readonly failCommit?: boolean;
  readonly headers?: ReadonlyMap<Buffer, Buffer>;
}) => ({
  key: input.key === null || input.key === undefined ? input.key : Buffer.from(input.key),
  value:
    input.price === null || input.price === undefined
      ? input.price
      : Buffer.from(JSON.stringify({ price: input.price })),
  headers: input.headers ?? new Map<Buffer, Buffer>(),
  topic: "source-orders",
  partition: 0,
  timestamp: 123n,
  offset: input.offset,
  metadata: {},
  commit: () => {
    if (input.failCommit === true) {
      return Promise.reject(new Error("commit failed"));
    }
    platformatic.state.committedByGroup.set(input.groupId, [input.offset + 1n]);
    return Promise.resolve();
  },
  toJSON: () => ({}),
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

beforeEach(() => {
  platformatic.reset();
});

describe("Kafka Node Adapter", () => {
  it.effect(
    "crashes composed production runtime startup before every listener and consumer on batched broker violations",
    () =>
      Effect.gen(function* () {
        platformatic.state.brokerConfigs.set("source-inventory", {
          cleanupPolicy: "compact",
          retentionMs: "60000",
        });
        platformatic.state.brokerConfigs.set("source-orders", {
          cleanupPolicy: "delete",
          retentionMs: "invalid",
        });
        const config = makeBatchedBrokerConfig();
        const listenerMessages: Array<string> = [];
        const logger = Logger.make<unknown, void>((options) => {
          const messages = Array.isArray(options.message) ? options.message : [];
          for (const message of messages) {
            if (typeof message === "string" && message.includes("listening at")) {
              listenerMessages.push(message);
            }
          }
        });
        const startup = runViewServerRuntime(config, {
          host: "127.0.0.1",
          tcpPublishHost: "127.0.0.1",
          tcpPublishPort: 0,
          websocketPort: 0,
        }).pipe(
          Effect.provide(
            EffectLayer.mergeAll(
              layer(config, {
                consumerGroupPrefix: "replica",
                regions: {
                  eu: { bootstrapServers: "eu:9092" },
                  us: { bootstrapServers: "us:9092" },
                },
              }),
              Logger.layer([logger]),
              EffectLayer.succeed(References.MinimumLogLevel, "Trace"),
            ),
          ),
        );
        type StartupError = Effect.Error<typeof startup>;
        expectTypeOf<
          Extract<StartupError, { readonly _tag: "KafkaBrokerContractValidationFailure" }>
        >().toEqualTypeOf<KafkaBrokerContractValidationFailure>();

        const failure = yield* Effect.flip(startup);

        expect(failure).toStrictEqual({
          _tag: "KafkaBrokerContractValidationFailure",
          message: "Kafka broker cleanup and retention validation failed before runtime startup.",
          issues: [
            {
              _tag: "CleanupPolicyMismatch",
              region: "eu",
              topic: "source-inventory",
              declared: "delete",
              observed: "compact",
            },
            {
              _tag: "InvalidRetentionMs",
              region: "eu",
              topic: "source-orders",
            },
            {
              _tag: "InvalidRetentionMs",
              region: "us",
              topic: "source-orders",
            },
          ],
        });
        expect({
          admins: platformatic.state.admins.map((admin) => admin.closed),
          consumers: platformatic.state.consumers.length,
          describeCalls: platformatic.state.describeConfigCalls.length,
          listenerMessages,
        }).toStrictEqual({
          admins: [true, true],
          consumers: 0,
          describeCalls: 2,
          listenerMessages: [],
        });
      }),
  );

  it.effect("batches broker contract discovery once per Region before consumer startup", () =>
    Effect.gen(function* () {
      platformatic.state.brokerConfigs.set("source-inventory", {
        cleanupPolicy: " delete ",
        retentionMs: "60000",
      });
      platformatic.state.brokerConfigs.set("source-orders", {
        cleanupPolicy: "delete",
        retentionMs: "120000",
      });
      const config = makeBatchedBrokerConfig();
      yield* Effect.scoped(
        EffectLayer.build(
          layer(config, {
            consumerGroupPrefix: "replica",
            retentionSweepInterval: "2 minutes",
            regions: {
              eu: { bootstrapServers: "eu:9092" },
              us: { bootstrapServers: "us:9092" },
            },
          }),
        ),
      );

      expect({
        admins: platformatic.state.admins.map((admin) => ({
          brokers: admin.options.bootstrapBrokers,
          clientId: admin.options.clientId,
          closed: admin.closed,
        })),
        calls: platformatic.state.describeConfigCalls,
        consumers: platformatic.state.consumers.length,
      }).toStrictEqual({
        admins: [
          {
            brokers: ["eu:9092"],
            clientId: "effect-view-server-eu-broker-validation",
            closed: true,
          },
          {
            brokers: ["us:9092"],
            clientId: "effect-view-server-us-broker-validation",
            closed: true,
          },
        ],
        calls: [
          {
            clientId: "effect-view-server-eu-broker-validation",
            input: {
              resources: [
                {
                  resourceType: 2,
                  resourceName: "source-inventory",
                  configurationKeys: ["cleanup.policy", "retention.ms"],
                },
                {
                  resourceType: 2,
                  resourceName: "source-orders",
                  configurationKeys: ["cleanup.policy", "retention.ms"],
                },
              ],
            },
          },
          {
            clientId: "effect-view-server-us-broker-validation",
            input: {
              resources: [
                {
                  resourceType: 2,
                  resourceName: "source-orders",
                  configurationKeys: ["cleanup.policy", "retention.ms"],
                },
              ],
            },
          },
        ],
        consumers: 0,
      });
    }),
  );

  it.effect("turns hostile Admin responses into accumulated typed startup failures", () =>
    Effect.gen(function* () {
      const hostileResource = Object.create(null);
      Object.defineProperty(hostileResource, "resourceType", {
        enumerable: true,
        get: () => {
          throw new Error("hostile resource getter");
        },
      });
      const hostileConfig = Object.create(null);
      Object.defineProperty(hostileConfig, "name", {
        enumerable: true,
        get: () => {
          throw new Error("hostile config getter");
        },
      });
      const hostileResponses: ReadonlyArray<unknown> = [
        { malformed: "non-array" },
        [hostileResource],
        [
          {
            resourceType: 2,
            resourceName: "source-orders",
            configs: [hostileConfig],
          },
        ],
      ];
      const config = makeBatchedBrokerConfig();
      for (const response of hostileResponses) {
        platformatic.reset();
        platformatic.state.describeConfigResponses.push(response);
        const failure = yield* Effect.scoped(
          EffectLayer.build(
            layer(config, {
              consumerGroupPrefix: "replica",
              regions: {
                eu: { bootstrapServers: "eu:9092" },
                us: { bootstrapServers: "us:9092" },
              },
            }),
          ),
        ).pipe(Effect.flip);

        expect(failure).toStrictEqual({
          _tag: "KafkaBrokerContractValidationFailure",
          message: "Kafka broker cleanup and retention validation failed before runtime startup.",
          issues: [
            {
              _tag: "MalformedBrokerConfiguration",
              region: "eu",
              topic: "source-inventory",
              configuration: "response",
            },
            {
              _tag: "MalformedBrokerConfiguration",
              region: "eu",
              topic: "source-orders",
              configuration: "response",
            },
          ],
        });
        expect(platformatic.state.consumers).toStrictEqual([]);
      }
    }),
  );

  it.effect(
    "fails Layer acquisition safely before any consumer for unavailable broker config",
    () =>
      Effect.gen(function* () {
        const config = makeConfig("earliest");
        platformatic.state.failNextDescribeConfigs = true;
        platformatic.state.failNextAdminClose = true;
        const failure = yield* Effect.scoped(
          EffectLayer.build(
            layer(config, {
              consumerGroupPrefix: "replica",
              regions: {
                eu: { bootstrapServers: "eu:9092" },
              },
            }),
          ),
        ).pipe(Effect.flip);

        expect(failure).toStrictEqual({
          _tag: "KafkaBrokerContractValidationFailure",
          message: "Kafka broker cleanup and retention validation failed before runtime startup.",
          issues: [
            {
              _tag: "BrokerConfigurationUnavailable",
              region: "eu",
              topic: "source-orders",
            },
          ],
        });
        expect({
          admins: platformatic.state.admins.map((admin) => admin.closed),
          consumers: platformatic.state.consumers.length,
        }).toStrictEqual({
          admins: [true],
          consumers: 0,
        });
      }),
  );

  it.effect("finalizes an in-flight Admin discovery when startup validation is interrupted", () =>
    Effect.gen(function* () {
      platformatic.state.blockNextDescribeConfigs = true;
      const config = makeConfig("earliest");
      const acquisition = yield* Effect.scoped(
        EffectLayer.build(
          layer(config, {
            consumerGroupPrefix: "replica",
            regions: {
              eu: { bootstrapServers: "eu:9092" },
            },
          }),
        ),
      ).pipe(Effect.forkChild({ startImmediately: true }));

      yield* awaitCondition(() => platformatic.state.describeConfigCalls.length === 1);
      yield* Fiber.interrupt(acquisition);
      const interrupted = yield* Fiber.await(acquisition);

      expect(Exit.isFailure(interrupted) && Cause.hasInterruptsOnly(interrupted.cause)).toBe(true);
      expect({
        admins: platformatic.state.admins.map((admin) => admin.closed),
        consumers: platformatic.state.consumers.length,
      }).toStrictEqual({
        admins: [true],
        consumers: 0,
      });
    }),
  );

  it.effect("fails Layer acquisition for construction, cleanup, and retention violations", () =>
    Effect.gen(function* () {
      const config = makeConfig("earliest");
      const acquire = () =>
        Effect.scoped(
          EffectLayer.build(
            layer(config, {
              consumerGroupPrefix: "replica",
              regions: {
                eu: { bootstrapServers: "eu:9092" },
              },
            }),
          ),
        ).pipe(Effect.flip);

      platformatic.state.failNextAdminConstruction = true;
      expect(yield* acquire()).toStrictEqual({
        _tag: "KafkaBrokerContractValidationFailure",
        message: "Kafka broker cleanup and retention validation failed before runtime startup.",
        issues: [
          {
            _tag: "BrokerConfigurationUnavailable",
            region: "eu",
            topic: "source-orders",
          },
        ],
      });
      expect(platformatic.state.consumers).toStrictEqual([]);

      platformatic.reset();
      platformatic.state.brokerConfigs.set("source-orders", {
        cleanupPolicy: "compact",
        retentionMs: "-1",
      });
      expect(yield* acquire()).toStrictEqual({
        _tag: "KafkaBrokerContractValidationFailure",
        message: "Kafka broker cleanup and retention validation failed before runtime startup.",
        issues: [
          {
            _tag: "CleanupPolicyMismatch",
            region: "eu",
            topic: "source-orders",
            declared: "delete",
            observed: "compact",
          },
        ],
      });
      expect(platformatic.state.consumers).toStrictEqual([]);

      platformatic.reset();
      platformatic.state.brokerConfigs.set("source-orders", {
        cleanupPolicy: "delete",
        retentionMs: "invalid",
      });
      expect(yield* acquire()).toStrictEqual({
        _tag: "KafkaBrokerContractValidationFailure",
        message: "Kafka broker cleanup and retention validation failed before runtime startup.",
        issues: [
          {
            _tag: "InvalidRetentionMs",
            region: "eu",
            topic: "source-orders",
          },
        ],
      });
      expect(platformatic.state.consumers).toStrictEqual([]);

      platformatic.reset();
      platformatic.state.brokerConfigs.set("source-orders", {
        cleanupPolicy: "delete,unknown",
        retentionMs: "-1",
      });
      expect(yield* acquire()).toStrictEqual({
        _tag: "KafkaBrokerContractValidationFailure",
        message: "Kafka broker cleanup and retention validation failed before runtime startup.",
        issues: [
          {
            _tag: "MalformedBrokerConfiguration",
            region: "eu",
            topic: "source-orders",
            configuration: "cleanup.policy",
          },
        ],
      });
      expect(platformatic.state.consumers).toStrictEqual([]);
    }),
  );

  it.effect("records mapping rejection metrics on the bound consumer state", () =>
    Effect.gen(function* () {
      platformatic.state.offsetsByTimestamp.set(-1n, [100n]);
      platformatic.state.offsetsByTimestamp.set(-2n, [0n]);
      platformatic.state.committedByGroup.set("replica:orders", []);
      const config = makeConfig("earliest");
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(
          layer(config, {
            consumerGroupPrefix: "replica",
            regions: {
              eu: { bootstrapServers: "one:9092" },
            },
          }),
        ),
      );
      yield* awaitCondition(() => platformatic.state.streams.length === 1);
      platformatic.state.streams[0]?.push(
        message({
          groupId: "replica:orders",
          key: "mapping-failure",
          price: 1,
          offset: 0n,
        }),
      );
      yield* awaitCondition(
        () => platformatic.state.committedByGroup.get("replica:orders")?.[0] === 1n,
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
      yield* TestClock.adjust("1 second");
      const health = Option.getOrThrow(
        yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
      );
      expect({
        mappingFailures: health.metrics.adapter.regions[0]?.mappingFailures,
        rejections: health.metrics.adapter.regions[0]?.rejections,
      }).toStrictEqual({
        mappingFailures: 1n,
        rejections: 1n,
      });

      yield* diagnostics.close();
      yield* runtime.close;
    }),
  );

  it.effect(
    "freezes the initial timestamp offsets and resumes retries from active-group commits",
    () =>
      Effect.gen(function* () {
        platformatic.state.offsetsByTimestamp.set(-1n, [100n, 200n]);
        platformatic.state.offsetsByTimestamp.set(5n, [10n, 20n]);
        platformatic.state.committedByGroup.set("replica:orders", [77n]);
        const config = makeConfig({
          mode: "timestamp",
          atNanos: 5_000_000n,
          fallback: "latest",
        });
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(
            layer(config, {
              consumerGroupPrefix: "replica",
              regions: {
                eu: {
                  bootstrapServers: ["one:9092", "two:9092"],
                  clientId: "kafka-node-test",
                },
              },
            }),
          ),
        );
        yield* awaitCondition(() => platformatic.state.streams.length === 1);
        const retryDiagnostics = yield* runtime.liveClient.subscribeSourceHealth({
          topic: "orders",
        });
        expect({
          groupId: platformatic.state.consumers[0]?.options.groupId,
          brokers: platformatic.state.consumers[0]?.options.bootstrapBrokers,
          offsetCalls: platformatic.state.offsetCalls.map(({ input }) => input.timestamp),
          consumeOffsets: platformatic.state.consumeCalls[0]?.input.offsets,
        }).toStrictEqual({
          groupId: "replica:orders",
          brokers: ["one:9092", "two:9092"],
          offsetCalls: [-1n, 5n],
          consumeOffsets: [
            {
              topic: "source-orders",
              partition: 0,
              offset: 10n,
            },
            {
              topic: "source-orders",
              partition: 1,
              offset: 20n,
            },
          ],
        });

        platformatic.state.streams[0]?.push(
          message({
            groupId: "replica:orders",
            key: "one",
            price: 1,
            offset: 10n,
          }),
        );
        platformatic.state.streams[0]?.push(
          message({
            groupId: "replica:orders",
            key: "two",
            price: 2,
            offset: 11n,
            failCommit: true,
          }),
        );
        yield* awaitCondition(
          () => platformatic.state.committedByGroup.get("replica:orders")?.[0] === 11n,
        );
        yield* awaitCondition(() => platformatic.state.streams[0]?.closed === true);
        yield* retryDiagnostics.events.pipe(
          Stream.filter((health) => health.status._tag === "WaitingToRetry"),
          Stream.take(1),
          Stream.runDrain,
        );
        yield* TestClock.withLive(Effect.sleep("1 millis"));
        yield* TestClock.adjust("1 second");
        yield* awaitCondition(() => platformatic.state.streams.length === 2);
        yield* retryDiagnostics.close();

        expect({
          offsetCalls: platformatic.state.offsetCalls.map(({ input }) => input.timestamp),
          consumeOffsets: platformatic.state.consumeCalls.map(({ input }) => input.offsets),
          priorClosed: platformatic.state.consumers[0]?.closed,
        }).toStrictEqual({
          offsetCalls: [-1n, 5n],
          consumeOffsets: [
            [
              {
                topic: "source-orders",
                partition: 0,
                offset: 10n,
              },
              {
                topic: "source-orders",
                partition: 1,
                offset: 20n,
              },
            ],
            [
              {
                topic: "source-orders",
                partition: 0,
                offset: 11n,
              },
              {
                topic: "source-orders",
                partition: 1,
                offset: 20n,
              },
            ],
          ],
          priorClosed: true,
        });

        platformatic.state.streams[1]?.push(
          message({
            groupId: "replica:orders",
            key: "two",
            price: 3,
            offset: 11n,
            headers: new Map([
              [Buffer.from("trace"), Buffer.from("first")],
              [Buffer.from("trace"), Buffer.from("second")],
            ]),
          }),
        );
        yield* awaitCondition(
          () => platformatic.state.committedByGroup.get("replica:orders")?.[0] === 12n,
        );
        platformatic.state.consumers[1]?.emit("consumer:group:join", {
          assignments: [{ topic: "source-orders", partitions: [0] }],
        });
        const snapshot = yield* runtime.client.snapshot("orders", {
          select: ["id", "price", "region"],
          orderBy: [{ field: "id", direction: "asc" }],
        });
        expect(snapshot).toStrictEqual({
          rows: [
            { id: "eu:0:one", price: 1, region: "eu" },
            { id: "eu:0:two", price: 3, region: "eu" },
          ],
          totalRows: 2,
          version: 3,
          status: "ready",
          statusCode: "Ready",
        });
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
        yield* TestClock.adjust("1 second");
        const health = Option.getOrThrow(
          yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
        );
        expect(health.metrics.adapter.regions[0]).toStrictEqual({
          region: "eu",
          assignments: [{ partition: 0, offset: 12n, lag: 88n }],
          commits: 2n,
          commitFailures: 1n,
          decoded: 3n,
          decodeFailures: 0n,
          mapped: 3n,
          mappingFailures: 0n,
          rejections: 0n,
          reconnects: 1n,
          rebalances: 0n,
          closes: 1n,
          closeFailures: 0n,
          retention: foreverRetentionMetrics(),
        });
        yield* diagnostics.close();

        yield* runtime.close;
        expect({
          streams: platformatic.state.streams.map((stream) => stream.closed),
          consumers: platformatic.state.consumers.map((consumer) => consumer.closed),
        }).toStrictEqual({
          streams: [true, true],
          consumers: [true, true],
        });
      }),
  );

  it.effect(
    "reuses explicit offsets when the first acquisition fails before current-lifetime progress",
    () =>
      Effect.gen(function* () {
        platformatic.state.offsetsByTimestamp.set(-1n, [100n]);
        platformatic.state.offsetsByTimestamp.set(-2n, [0n]);
        platformatic.state.committedByGroup.set("replica:orders", [50n]);
        platformatic.state.failNextConsume = true;
        const config = makeConfig("earliest", Schedule.recurs(1));
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(
            layer(config, {
              consumerGroupPrefix: "replica",
              regions: {
                eu: { bootstrapServers: "one:9092" },
              },
            }),
          ),
        );
        yield* awaitCondition(() => platformatic.state.streams.length === 1);

        expect({
          consumers: platformatic.state.consumers.length,
          committedCalls: platformatic.state.committedCalls,
          offsets: platformatic.state.consumeCalls[0]?.input.offsets,
        }).toStrictEqual({
          consumers: 2,
          committedCalls: [],
          offsets: [
            {
              topic: "source-orders",
              partition: 0,
              offset: 0n,
            },
          ],
        });

        platformatic.state.streams[0]?.push(
          message({
            groupId: "replica:orders",
            key: "rebuilt",
            price: 1,
            offset: 0n,
          }),
        );
        yield* awaitCondition(
          () => platformatic.state.committedByGroup.get("replica:orders")?.[0] === 1n,
        );
        expect(
          yield* runtime.client.snapshot("orders", {
            select: ["id", "price", "region"],
          }),
        ).toStrictEqual({
          rows: [{ id: "eu:0:rebuilt", price: 1, region: "eu" }],
          totalRows: 1,
          version: 1,
          status: "ready",
          statusCode: "Ready",
        });
        yield* runtime.close;
      }),
  );

  it.effect(
    "falls back to frozen explicit offsets when a current-lifetime commit disappears before retry",
    () =>
      Effect.gen(function* () {
        platformatic.state.offsetsByTimestamp.set(-1n, [100n]);
        platformatic.state.offsetsByTimestamp.set(-2n, [0n]);
        platformatic.state.committedByGroup.set("replica:orders", [50n]);
        const config = makeConfig("earliest", Schedule.recurs(1));
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(
            layer(config, {
              consumerGroupPrefix: "replica",
              regions: {
                eu: { bootstrapServers: "one:9092" },
              },
            }),
          ),
        );
        yield* awaitCondition(() => platformatic.state.streams.length === 1);
        platformatic.state.streams[0]?.push(
          message({
            groupId: "replica:orders",
            key: "committed",
            price: 1,
            offset: 0n,
          }),
        );
        yield* awaitCondition(
          () => platformatic.state.committedByGroup.get("replica:orders")?.[0] === 1n,
        );
        platformatic.state.streams[0]?.push(
          message({
            groupId: "replica:orders",
            key: "retry",
            price: 2,
            offset: 1n,
            failCommit: true,
          }),
        );
        yield* awaitCondition(() => platformatic.state.streams[0]?.closed === true);
        platformatic.state.committedByGroup.set("replica:orders", []);
        yield* TestClock.adjust("1 second");
        yield* awaitCondition(() => platformatic.state.streams.length === 2);

        expect(platformatic.state.consumeCalls.map(({ input }) => input.offsets)).toStrictEqual([
          [
            {
              topic: "source-orders",
              partition: 0,
              offset: 0n,
            },
          ],
          [
            {
              topic: "source-orders",
              partition: 0,
              offset: 0n,
            },
          ],
        ]);
        yield* runtime.close;
      }),
  );

  it.effect(
    "resolves committed seed offsets with per-partition fallback and closes the resolver",
    () =>
      Effect.gen(function* () {
        platformatic.state.offsetsByTimestamp.set(-1n, [100n, 200n]);
        platformatic.state.offsetsByTimestamp.set(-2n, [0n, 5n]);
        platformatic.state.committedByGroup.set("seed-consumer", [50n, -1n]);
        platformatic.state.committedByGroup.set("replica:orders", [-1n, -1n]);
        const config = makeConfig({
          mode: "committed",
          consumerGroupId: "seed-consumer",
          fallback: "earliest",
        });
        const certificate = Uint8Array.from([1, 2, 3]);
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(
            layer(config, {
              consumerGroupPrefix: "replica",
              regions: {
                eu: {
                  bootstrapServers: "one:9092, two:9092",
                  connectTimeout: 1,
                  requestTimeout: 2,
                  timeout: 3,
                  retries: false,
                  metadataMaxAge: 4,
                  sasl: {
                    mechanism: "PLAIN",
                    username: "user",
                    password: "secret",
                  },
                  tls: {
                    ca: certificate,
                    serverName: "kafka.internal",
                    rejectUnauthorized: true,
                  },
                },
              },
            }),
          ),
        );
        certificate[0] = 9;
        yield* awaitCondition(() => platformatic.state.streams.length === 1);

        expect({
          groups: platformatic.state.consumers.map(({ options }) => options.groupId),
          resolverClosed: platformatic.state.consumers[1]?.closed,
          consumeOffsets: platformatic.state.consumeCalls[0]?.input.offsets,
          tls: platformatic.state.consumers[0]?.options.tls,
        }).toStrictEqual({
          groups: ["replica:orders", "seed-consumer"],
          resolverClosed: true,
          consumeOffsets: [
            {
              topic: "source-orders",
              partition: 0,
              offset: 50n,
            },
            {
              topic: "source-orders",
              partition: 1,
              offset: 5n,
            },
          ],
          tls: {
            ca: Buffer.from([1, 2, 3]),
            servername: "kafka.internal",
            rejectUnauthorized: true,
          },
        });

        const active = platformatic.state.consumers[0];
        active?.emit("consumer:group:join", {
          assignments: [
            {
              topic: "source-orders",
              partitions: [0, 1],
            },
          ],
        });
        active?.emit("consumer:group:rebalance");
        active?.emitLag(new Map([["source-orders", [40n, 7n]]]));
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
        yield* TestClock.adjust("1 second");
        const rebalancing = Option.getOrThrow(
          yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
        );
        expect(rebalancing.metrics.adapter.regions[0]?.assignments).toStrictEqual([]);

        active?.emit("consumer:group:join", {
          assignments: [
            {
              topic: "source-orders",
              partitions: [0, 1],
            },
          ],
        });
        active?.emitLag(new Map([["source-orders", [40n, 7n]]]));
        yield* TestClock.adjust("1 second");
        const rejoined = Option.getOrThrow(
          yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
        );
        expect(rejoined.metrics.adapter.regions[0]).toStrictEqual({
          region: "eu",
          assignments: [
            { partition: 0, offset: 50n, lag: 40n },
            { partition: 1, offset: 5n, lag: 7n },
          ],
          commits: 0n,
          commitFailures: 0n,
          decoded: 0n,
          decodeFailures: 0n,
          mapped: 0n,
          mappingFailures: 0n,
          rejections: 0n,
          reconnects: 0n,
          rebalances: 1n,
          closes: 1n,
          closeFailures: 0n,
          retention: foreverRetentionMetrics(),
        });

        yield* diagnostics.close();
        yield* runtime.close;
        expect(active?.lagMonitoring).toBe(false);
      }),
  );

  it.effect("reapplies explicit starts and resets metrics for a new lifetime on one Layer", () =>
    Effect.scoped(
      Effect.gen(function* () {
        platformatic.state.offsetsByTimestamp.set(-1n, [100n]);
        platformatic.state.offsetsByTimestamp.set(-2n, [0n]);
        platformatic.state.committedByGroup.set("replica:orders", [50n]);
        const config = makeConfig("earliest");
        const context = yield* EffectLayer.build(
          layer(config, {
            consumerGroupPrefix: "replica",
            regions: {
              eu: { bootstrapServers: "one:9092" },
            },
          }),
        );

        const firstRuntime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provideContext(context),
        );
        yield* awaitCondition(() => platformatic.state.streams.length === 1);
        expect(platformatic.state.consumeCalls[0]?.input.offsets).toStrictEqual([
          {
            topic: "source-orders",
            partition: 0,
            offset: 0n,
          },
        ]);
        platformatic.state.streams[0]?.push(
          message({
            groupId: "replica:orders",
            key: "first",
            price: 1,
            offset: 0n,
          }),
        );
        yield* awaitCondition(
          () => platformatic.state.committedByGroup.get("replica:orders")?.[0] === 1n,
        );
        yield* firstRuntime.close;

        const secondRuntime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provideContext(context),
        );
        yield* awaitCondition(() => platformatic.state.streams.length === 2);
        expect(platformatic.state.consumeCalls.map(({ input }) => input.offsets)).toStrictEqual([
          [
            {
              topic: "source-orders",
              partition: 0,
              offset: 0n,
            },
          ],
          [
            {
              topic: "source-orders",
              partition: 0,
              offset: 0n,
            },
          ],
        ]);
        const diagnostics = yield* secondRuntime.liveClient.subscribeSourceHealth({
          topic: "orders",
        });
        yield* TestClock.adjust("1 second");
        const health = Option.getOrThrow(
          yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
        );
        expect({
          commits: health.metrics.adapter.regions[0]?.commits,
          reconnects: health.metrics.adapter.regions[0]?.reconnects,
          closes: health.metrics.adapter.regions[0]?.closes,
        }).toStrictEqual({
          commits: 0n,
          reconnects: 0n,
          closes: 0n,
        });
        yield* diagnostics.close();
        yield* secondRuntime.close;
      }),
    ),
  );

  it.effect("honors earliest and latest starts and translates nullish Kafka fields", () =>
    Effect.gen(function* () {
      platformatic.state.offsetsByTimestamp.set(-1n, [100n]);
      platformatic.state.offsetsByTimestamp.set(-2n, [0n]);
      platformatic.state.committedByGroup.set("replica:orders", []);
      const earliestConfig = makeConfig("earliest");
      const earliestRuntime = yield* makeViewServerRuntimeCore(earliestConfig, {}).pipe(
        Effect.provide(
          layer(earliestConfig, {
            consumerGroupPrefix: "replica",
            regions: {
              eu: { bootstrapServers: "one:9092" },
            },
          }),
        ),
      );
      yield* awaitCondition(() => platformatic.state.streams.length === 1);
      expect(platformatic.state.consumeCalls[0]?.input.offsets).toStrictEqual([
        {
          topic: "source-orders",
          partition: 0,
          offset: 0n,
        },
      ]);

      const active = Option.getOrThrow(Option.fromUndefinedOr(platformatic.state.consumers[0]));
      active.assignments = [{ topic: "source-orders", partitions: [7] }];
      active.emit("consumer:group:join", {});
      active.assignments = null;
      active.emit("consumer:group:join", {});
      platformatic.state.streams[0]?.push(
        message({
          groupId: "replica:orders",
          key: null,
          price: 1,
          offset: 0n,
        }),
      );
      platformatic.state.streams[0]?.push(
        message({
          groupId: "replica:orders",
          key: "missing",
          price: undefined,
          offset: 1n,
        }),
      );
      yield* awaitCondition(
        () => platformatic.state.committedByGroup.get("replica:orders")?.[0] === 2n,
      );
      yield* earliestRuntime.close;

      platformatic.reset();
      platformatic.state.offsetsByTimestamp.set(-1n, [100n]);
      platformatic.state.committedByGroup.set("replica:orders", []);
      const latestConfig = makeConfig("latest");
      const latestRuntime = yield* makeViewServerRuntimeCore(latestConfig, {}).pipe(
        Effect.provide(
          layer(latestConfig, {
            consumerGroupPrefix: "replica",
            regions: {
              eu: { bootstrapServers: "one:9092" },
            },
          }),
        ),
      );
      yield* awaitCondition(() => platformatic.state.streams.length === 1);
      expect(platformatic.state.consumeCalls[0]?.input.offsets).toStrictEqual([
        {
          topic: "source-orders",
          partition: 0,
          offset: 100n,
        },
      ]);
      yield* latestRuntime.close;
    }),
  );

  it.effect(
    "applies latest and fail timestamp fallbacks and rejects incomplete offset vectors",
    () =>
      Effect.gen(function* () {
        platformatic.state.offsetsByTimestamp.set(-1n, [100n, 200n]);
        platformatic.state.offsetsByTimestamp.set(5n, [-1n, -1n]);
        platformatic.state.committedByGroup.set("replica:orders", []);
        const fallbackConfig = makeConfig({
          mode: "timestamp",
          atNanos: 5_000_000n,
          fallback: "latest",
        });
        const fallbackRuntime = yield* makeViewServerRuntimeCore(fallbackConfig, {}).pipe(
          Effect.provide(
            layer(fallbackConfig, {
              consumerGroupPrefix: "replica",
              regions: {
                eu: { bootstrapServers: "one:9092" },
              },
            }),
          ),
        );
        yield* awaitCondition(() => platformatic.state.streams.length === 1);
        expect(platformatic.state.consumeCalls[0]?.input.offsets).toStrictEqual([
          {
            topic: "source-orders",
            partition: 0,
            offset: 100n,
          },
          {
            topic: "source-orders",
            partition: 1,
            offset: 200n,
          },
        ]);
        yield* fallbackRuntime.close;

        platformatic.reset();
        platformatic.state.offsetsByTimestamp.set(-1n, [100n]);
        platformatic.state.offsetsByTimestamp.set(5n, [-1n, -1n]);
        platformatic.state.committedByGroup.set("replica:orders", []);
        const incompleteConfig = makeConfig(
          {
            mode: "timestamp",
            atNanos: 5_000_000n,
            fallback: "latest",
          },
          Schedule.recurs(0),
        );
        const incompleteRuntime = yield* makeViewServerRuntimeCore(incompleteConfig, {}).pipe(
          Effect.provide(
            layer(incompleteConfig, {
              consumerGroupPrefix: "replica",
              regions: {
                eu: { bootstrapServers: "one:9092" },
              },
            }),
          ),
        );
        yield* awaitCondition(() => platformatic.state.consumers[0]?.closed === true);
        yield* incompleteRuntime.close;

        platformatic.reset();
        platformatic.state.offsetsByTimestamp.set(-1n, [100n]);
        platformatic.state.offsetsByTimestamp.set(5n, [-1n]);
        platformatic.state.committedByGroup.set("replica:orders", []);
        const failedFallbackConfig = makeConfig(
          {
            mode: "timestamp",
            atNanos: 5_000_000n,
            fallback: "fail",
          },
          Schedule.recurs(0),
        );
        const failedFallbackRuntime = yield* makeViewServerRuntimeCore(
          failedFallbackConfig,
          {},
        ).pipe(
          Effect.provide(
            layer(failedFallbackConfig, {
              consumerGroupPrefix: "replica",
              regions: {
                eu: { bootstrapServers: "one:9092" },
              },
            }),
          ),
        );
        yield* awaitCondition(() => platformatic.state.consumers[0]?.closed === true);
        yield* failedFallbackRuntime.close;
      }),
  );

  it.effect(
    "clears closed-attempt assignments while waiting to retry and after acquisition exhaustion",
    () =>
      Effect.gen(function* () {
        platformatic.state.offsetsByTimestamp.set(-1n, [10n]);
        platformatic.state.committedByGroup.set("replica:orders", []);
        const waitingConfig = makeConfig("latest", Schedule.spaced("10 seconds"));
        const waitingRuntime = yield* makeViewServerRuntimeCore(waitingConfig, {}).pipe(
          Effect.provide(
            layer(waitingConfig, {
              consumerGroupPrefix: "replica",
              regions: {
                eu: { bootstrapServers: "one:9092" },
              },
            }),
          ),
        );
        yield* awaitCondition(() => platformatic.state.streams.length === 1);
        const waitingDiagnostics = yield* waitingRuntime.liveClient.subscribeSourceHealth({
          topic: "orders",
        });
        yield* TestClock.adjust("1 second");
        const active = Option.getOrThrow(
          yield* waitingDiagnostics.events.pipe(
            Stream.filter(
              (health) =>
                (health.status._tag === "Ready" || health.status._tag === "Degraded") &&
                health.metrics.adapter.regions[0]?.assignments.length === 1,
            ),
            Stream.take(1),
            Stream.runHead,
          ),
        );
        expect(active.metrics.adapter.regions[0]?.assignments).toStrictEqual([
          {
            partition: 0,
            offset: 10n,
            lag: 0n,
          },
        ]);

        platformatic.state.streams[0]?.fail(new Error("stream failed"));
        const waiting = Option.getOrThrow(
          yield* waitingDiagnostics.events.pipe(
            Stream.filter((health) => health.status._tag === "WaitingToRetry"),
            Stream.take(1),
            Stream.runHead,
          ),
        );
        expect(waiting.status._tag).toBe("WaitingToRetry");
        yield* TestClock.adjust("1 second");
        const clearedWaiting = Option.getOrThrow(
          yield* waitingDiagnostics.events.pipe(
            Stream.filter(
              (health) =>
                health.status._tag === "WaitingToRetry" &&
                health.metrics.adapter.regions[0]?.assignments.length === 0,
            ),
            Stream.take(1),
            Stream.runHead,
          ),
        );
        expect(clearedWaiting.metrics.adapter.regions[0]?.assignments).toStrictEqual([]);
        yield* waitingDiagnostics.close();
        yield* waitingRuntime.close;

        platformatic.reset();
        platformatic.state.offsetsByTimestamp.set(-1n, [10n]);
        platformatic.state.committedByGroup.set("replica:orders", []);
        platformatic.state.failNextStartLagMonitoring = true;
        const exhaustedConfig = makeConfig("latest", Schedule.recurs(0));
        const exhaustedRuntime = yield* makeViewServerRuntimeCore(exhaustedConfig, {}).pipe(
          Effect.provide(
            layer(exhaustedConfig, {
              consumerGroupPrefix: "replica",
              regions: {
                eu: { bootstrapServers: "one:9092" },
              },
            }),
          ),
        );
        const exhaustedDiagnostics = yield* exhaustedRuntime.liveClient.subscribeSourceHealth({
          topic: "orders",
        });
        const exhausted = Option.getOrThrow(
          yield* exhaustedDiagnostics.events.pipe(
            Stream.filter((health) => health.status._tag === "Exhausted"),
            Stream.take(1),
            Stream.runHead,
          ),
        );
        expect(exhausted.status._tag).toBe("Exhausted");
        yield* TestClock.adjust("1 second");
        const clearedExhausted = Option.getOrThrow(
          yield* exhaustedDiagnostics.events.pipe(
            Stream.filter(
              (health) =>
                health.status._tag === "Exhausted" &&
                health.metrics.adapter.regions[0]?.assignments.length === 0,
            ),
            Stream.take(1),
            Stream.runHead,
          ),
        );
        expect(clearedExhausted.metrics.adapter.regions[0]?.assignments).toStrictEqual([]);
        yield* exhaustedDiagnostics.close();
        yield* exhaustedRuntime.close;
      }),
  );

  it.effect("maps iterator acquisition defects and cleans the failed attempt before retry", () =>
    Effect.gen(function* () {
      platformatic.state.offsetsByTimestamp.set(-1n, [10n]);
      platformatic.state.committedByGroup.set("replica:orders", []);
      platformatic.state.failNextIterator = true;
      const config = makeConfig("latest", Schedule.spaced("10 seconds"));
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(
          layer(config, {
            consumerGroupPrefix: "replica",
            regions: {
              eu: { bootstrapServers: "one:9092" },
            },
          }),
        ),
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
      const waiting = Option.getOrThrow(
        yield* diagnostics.events.pipe(
          Stream.filter((health) => health.status._tag === "WaitingToRetry"),
          Stream.take(1),
          Stream.runHead,
        ),
      );
      expect(waiting.status).toStrictEqual({
        _tag: "WaitingToRetry",
        nextAttempt: 2n,
        termination: {
          _tag: "Failed",
          failure: {
            _tag: "AdapterFailure",
            failure: {
              _tag: "KafkaConsumeFailure",
              region: "eu",
              topic: "source-orders",
              message: "Kafka Region consumer stream failed.",
            },
          },
        },
        retryAtNanos: 10_000_000_000n,
      });
      expect({
        consumerClosed: platformatic.state.consumers[0]?.closed,
        handlersRemoved: [...(platformatic.state.consumers[0]?.handlers.values() ?? [])].every(
          (handlers) => handlers.size === 0,
        ),
        iteratorFailureConsumed: platformatic.state.failNextIterator,
        streamClosed: platformatic.state.streams[0]?.closed,
      }).toStrictEqual({
        consumerClosed: true,
        handlersRemoved: true,
        iteratorFailureConsumed: false,
        streamClosed: true,
      });
      yield* TestClock.adjust("10 seconds");
      yield* awaitCondition(() => platformatic.state.streams.length === 2);
      yield* diagnostics.close();
      yield* runtime.close;
    }),
  );

  it.effect("surfaces malformed UTF-8 header names as typed consume failures", () =>
    Effect.gen(function* () {
      platformatic.state.offsetsByTimestamp.set(-1n, [10n]);
      platformatic.state.committedByGroup.set("replica:orders", []);
      const config = makeConfig("latest", Schedule.recurs(0));
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(
          layer(config, {
            consumerGroupPrefix: "replica",
            regions: {
              eu: { bootstrapServers: "one:9092" },
            },
          }),
        ),
      );
      yield* awaitCondition(() => platformatic.state.streams.length === 1);
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
      platformatic.state.streams[0]?.push(
        message({
          groupId: "replica:orders",
          key: "malformed-header",
          price: 1,
          offset: 0n,
          headers: new Map([[Buffer.from([0xff]), Buffer.from("value")]]),
        }),
      );
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
                _tag: "KafkaConsumeFailure",
                region: "eu",
                topic: "source-orders",
                message: "Kafka Region consumer stream failed.",
              },
            },
          },
        },
        exhaustedAtNanos: 0n,
      });
      expect(platformatic.state.committedByGroup.get("replica:orders")).toStrictEqual([]);
      yield* diagnostics.close();
      yield* runtime.close;
    }),
  );

  it.effect(
    "closes consumers on every driver acquisition failure and surfaces stream failures",
    () =>
      Effect.gen(function* () {
        const flags: ReadonlyArray<
          | "failNextConstruction"
          | "failNextListOffsets"
          | "failNextListCommitted"
          | "failNextConsume"
          | "failNextIterator"
          | "failNextListenerRegistration"
          | "failNextStartLagMonitoring"
        > = [
          "failNextConstruction",
          "failNextListOffsets",
          "failNextListCommitted",
          "failNextConsume",
          "failNextIterator",
          "failNextListenerRegistration",
          "failNextStartLagMonitoring",
        ];
        for (const flag of flags) {
          platformatic.reset();
          platformatic.state.offsetsByTimestamp.set(-1n, [10n]);
          platformatic.state.committedByGroup.set("replica:orders", []);
          platformatic.state[flag] = true;
          const config = makeConfig(
            flag === "failNextListCommitted"
              ? {
                  mode: "committed",
                  consumerGroupId: "seed",
                  fallback: "latest",
                }
              : "latest",
            Schedule.recurs(0),
          );
          const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
            Effect.provide(
              layer(config, {
                consumerGroupPrefix: "replica",
                regions: {
                  eu: { bootstrapServers: "one:9092" },
                },
              }),
            ),
          );
          yield* awaitCondition(() => platformatic.state[flag] === false);
          yield* runtime.close;
          expect(
            platformatic.state.consumers.every((consumer) =>
              [...consumer.handlers.values()].every((handlers) => handlers.size === 0),
            ),
          ).toBe(true);
        }

        platformatic.reset();
        platformatic.state.offsetsByTimestamp.set(-1n, [10n]);
        platformatic.state.committedByGroup.set("replica:orders", []);
        const streamConfig = makeConfig("latest", Schedule.recurs(0));
        const streamRuntime = yield* makeViewServerRuntimeCore(streamConfig, {}).pipe(
          Effect.provide(
            layer(streamConfig, {
              consumerGroupPrefix: "replica",
              regions: {
                eu: { bootstrapServers: "one:9092" },
              },
            }),
          ),
        );
        yield* awaitCondition(() => platformatic.state.streams.length === 1);
        platformatic.state.streams[0]?.fail(new Error("stream failed"));
        yield* awaitCondition(() => platformatic.state.consumers[0]?.closed === true);
        yield* streamRuntime.close;

        platformatic.reset();
        platformatic.state.offsetsByTimestamp.set(-1n, [10n]);
        platformatic.state.committedByGroup.set("replica:orders", []);
        platformatic.state.failNextStreamClose = true;
        platformatic.state.failNextConsumerClose = true;
        platformatic.state.failNextListenerRemoval = true;
        platformatic.state.failNextStopLagMonitoring = true;
        const closeConfig = makeConfig("latest");
        const closeRuntime = yield* makeViewServerRuntimeCore(closeConfig, {}).pipe(
          Effect.provide(
            layer(closeConfig, {
              consumerGroupPrefix: "replica",
              regions: {
                eu: { bootstrapServers: "one:9092" },
              },
            }),
          ),
        );
        yield* awaitCondition(() => platformatic.state.streams.length === 1);
        yield* closeRuntime.close;
        expect({
          streamFailureConsumed: platformatic.state.failNextStreamClose,
          consumerFailureConsumed: platformatic.state.failNextConsumerClose,
          listenerFailureConsumed: platformatic.state.failNextListenerRemoval,
          lagMonitoringFailureConsumed: platformatic.state.failNextStopLagMonitoring,
        }).toStrictEqual({
          streamFailureConsumed: false,
          consumerFailureConsumed: false,
          listenerFailureConsumed: false,
          lagMonitoringFailureConsumed: false,
        });
      }),
  );

  it.effect("builds the aggregate Layer from Config exactly once", () =>
    Effect.gen(function* () {
      const config = makeConfig("earliest");
      yield* Effect.scoped(
        EffectLayer.build(
          layerConfig(config, {
            consumerGroupPrefix: Config.succeed("replica"),
            regions: {
              eu: {
                bootstrapServers: Config.succeed("one:9092"),
              },
            },
          }),
        ),
      );
      expect(platformatic.state.consumers).toStrictEqual([]);
    }),
  );

  it.effect("reports resolved Config validation on the typed Config failure channel", () =>
    Effect.gen(function* () {
      const config = makeConfig("earliest");
      const failure = yield* Effect.flip(
        Effect.scoped(
          EffectLayer.build(
            layerConfig(config, {
              consumerGroupPrefix: Config.succeed(""),
              regions: {
                eu: {
                  bootstrapServers: Config.succeed("one:9092"),
                },
              },
            }),
          ),
        ),
      );
      expect(failure).toBeInstanceOf(Config.ConfigError);
      expect(failure.message).toContain(
        "Kafka Node Layer requires exactly consumerGroupPrefix and regions.",
      );
    }),
  );

  it.effect.each([
    {
      label: "unexpected value coercion",
      makeCause: () =>
        Object.defineProperty({}, "toString", {
          get: () => {
            throw new Error("hostile cause inspected");
          },
        }),
    },
    {
      label: "unexpected value prototypes",
      makeCause: () =>
        new Proxy(
          {},
          {
            getPrototypeOf: () => {
              throw new Error("hostile cause prototype inspected");
            },
          },
        ),
    },
    {
      label: "hostile configuration error messages",
      makeCause: () =>
        new Proxy(new KafkaSourceConfigurationError("invalid"), {
          get: (target, property, receiver) => {
            if (property === "message") {
              throw new Error("hostile cause message inspected");
            }
            return Reflect.get(target, property, receiver);
          },
        }),
    },
    {
      label: "non-string configuration error messages",
      makeCause: () =>
        new Proxy(new KafkaSourceConfigurationError("invalid"), {
          get: (target, property, receiver) =>
            property === "message" ? {} : Reflect.get(target, property, receiver),
        }),
    },
  ])("does not inspect $label during Config validation", ({ makeCause }) =>
    Effect.gen(function* () {
      const config = makeConfig("earliest");
      const hostileCause = makeCause();
      const hostileConfig = new Proxy(config, {
        get: (target, property, receiver) => {
          if (property === "topics") {
            throw hostileCause;
          }
          return Reflect.get(target, property, receiver);
        },
      });
      const failure = yield* Effect.flip(
        Effect.scoped(
          EffectLayer.build(
            layerConfig(hostileConfig, {
              consumerGroupPrefix: Config.succeed("replica"),
              regions: {
                eu: {
                  bootstrapServers: Config.succeed("one:9092"),
                },
              },
            }),
          ),
        ),
      );
      expect(failure).toBeInstanceOf(Config.ConfigError);
      expect(failure.message).toContain("Kafka Node Layer configuration validation failed.");
    }),
  );

  it("validates every deterministic consumer group during pure Layer construction", () => {
    const config = makeConfig("earliest");
    expect(() =>
      layer(config, {
        consumerGroupPrefix: "\ud800",
        regions: {
          eu: { bootstrapServers: "one:9092" },
        },
      }),
    ).toThrowError(KafkaSourceConfigurationError);

    const malformedTopicConfig = makeConfigWithMalformedTopic();
    expect(() =>
      layer(malformedTopicConfig, {
        consumerGroupPrefix: "replica",
        regions: {
          eu: { bootstrapServers: "one:9092" },
        },
      }),
    ).toThrowError(KafkaSourceConfigurationError);

    expect(() =>
      layer(config, {
        consumerGroupPrefix: "a".repeat(32_761),
        regions: {
          eu: { bootstrapServers: "one:9092" },
        },
      }),
    ).toThrowError("Kafka derived consumer group ID exceeds the 32767-byte Kafka protocol limit.");
    expect(platformatic.state.consumers).toStrictEqual([]);
  });

  it("ignores non-Kafka definitions while validating consumer groups", () => {
    const config = makeConfigWithNonKafkaDefinitions();
    expect(() =>
      layer(config, {
        consumerGroupPrefix: "replica",
        regions: {
          eu: { bootstrapServers: "one:9092" },
        },
      }),
    ).not.toThrow();
  });

  it.effect("preserves malformed group components as typed Config Layer failures", () =>
    Effect.gen(function* () {
      const malformedTopicConfig = makeConfigWithMalformedTopic();
      const prefixFailure = yield* Effect.flip(
        Effect.scoped(
          EffectLayer.build(
            layerConfig(makeConfig("earliest"), {
              consumerGroupPrefix: Config.succeed("\ud800"),
              regions: {
                eu: {
                  bootstrapServers: Config.succeed("one:9092"),
                },
              },
            }),
          ),
        ),
      );
      const topicFailure = yield* Effect.flip(
        Effect.scoped(
          EffectLayer.build(
            layerConfig(malformedTopicConfig, {
              consumerGroupPrefix: Config.succeed("replica"),
              regions: {
                eu: {
                  bootstrapServers: Config.succeed("one:9092"),
                },
              },
            }),
          ),
        ),
      );
      expect(
        [prefixFailure, topicFailure].every((failure) => failure instanceof Config.ConfigError),
      ).toBe(true);
    }),
  );

  it.effect("retains close-failure metrics when initial offset resolution fails", () =>
    Effect.gen(function* () {
      platformatic.state.offsetsByTimestamp.set(-1n, [10n]);
      platformatic.state.failNextListOffsets = true;
      platformatic.state.failNextConsumerClose = true;
      const config = makeConfig("latest", Schedule.recurs(0));
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(
          layer(config, {
            consumerGroupPrefix: "replica",
            regions: {
              eu: { bootstrapServers: "one:9092" },
            },
          }),
        ),
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
      yield* awaitCondition(
        () =>
          platformatic.state.failNextListOffsets === false &&
          platformatic.state.failNextConsumerClose === false,
      );
      yield* TestClock.adjust("1 second");
      const health = Option.getOrThrow(
        yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
      );
      expect(health.metrics.adapter.regions[0]).toStrictEqual({
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
        closeFailures: 1n,
        retention: foreverRetentionMetrics(),
      });
      yield* diagnostics.close();
      yield* runtime.close;
    }),
  );

  it("rejects missing, extra, and malformed aggregate Region options", () => {
    const config = makeConfig("earliest");
    expect(() =>
      Reflect.apply(layer, undefined, [
        config,
        {
          consumerGroupPrefix: "replica",
          regions: {},
        },
      ]),
    ).toThrowError("Kafka Node Layer regions must contain all and only referenced Regions.");
    expect(() =>
      Reflect.apply(layer, undefined, [
        config,
        {
          consumerGroupPrefix: "replica",
          regions: {
            eu: { bootstrapServers: "one:9092" },
            us: { bootstrapServers: "two:9092" },
          },
        },
      ]),
    ).toThrowError("Kafka Node Layer regions must contain all and only referenced Regions.");
    expect(() =>
      Reflect.apply(layer, undefined, [
        config,
        {
          consumerGroupPrefix: "replica",
          regions: {
            eu: {
              bootstrapServers: "",
            },
          },
        },
      ]),
    ).toThrowError("Kafka Region bootstrapServers must contain only non-empty brokers.");
  });

  it("validates and snapshots every Node option boundary", () => {
    const bytesInput = Uint8Array.from([1, 2]);
    const copiedBytes = kafkaNodeInternals.copyTlsValue(bytesInput);
    const copiedList = kafkaNodeInternals.copyTlsValue(["certificate", bytesInput]);
    expect({
      brokers: kafkaNodeInternals.bootstrapServers(["one:9092", "two:9092"]),
      stringTls: kafkaNodeInternals.copyTlsValue("certificate"),
      copiedBytes,
      copiedList,
      finite: [
        kafkaNodeInternals.finiteNonNegative(undefined),
        kafkaNodeInternals.finiteNonNegative(0),
        kafkaNodeInternals.finiteNonNegative(-1),
        kafkaNodeInternals.finiteNonNegative(Number.POSITIVE_INFINITY),
      ],
      oauth: kafkaNodeInternals.snapshotSasl({
        mechanism: "OAUTHBEARER",
        token: "token",
      }),
    }).toStrictEqual({
      brokers: ["one:9092", "two:9092"],
      stringTls: "certificate",
      copiedBytes: Uint8Array.from([1, 2]),
      copiedList: ["certificate", Uint8Array.from([1, 2])],
      finite: [true, true, false, false],
      oauth: {
        mechanism: "OAUTHBEARER",
        token: "token",
      },
    });
    expect(copiedBytes === bytesInput).toBe(false);
    expect(copiedList[1] === bytesInput).toBe(false);

    const tls = kafkaNodeInternals.snapshotTls({
      ca: "ca",
      cert: Uint8Array.from([3]),
      key: ["key", Uint8Array.from([4])],
      rejectUnauthorized: false,
      serverName: "broker.internal",
    });
    expect({
      tls,
      nodeString: kafkaNodeInternals.nodeTlsValue("certificate"),
      nodeBytes: kafkaNodeInternals.nodeTlsValue(Uint8Array.from([5])),
      nodeList: kafkaNodeInternals.nodeTlsValue(["key", Uint8Array.from([6])]),
      nodeEmpty: kafkaNodeInternals.nodeTlsOptions({}),
      nodeTls: kafkaNodeInternals.nodeTlsOptions(tls),
    }).toStrictEqual({
      tls: {
        ca: "ca",
        cert: Uint8Array.from([3]),
        key: ["key", Uint8Array.from([4])],
        rejectUnauthorized: false,
        serverName: "broker.internal",
      },
      nodeString: "certificate",
      nodeBytes: Buffer.from([5]),
      nodeList: ["key", Buffer.from([6])],
      nodeEmpty: {},
      nodeTls: {
        ca: "ca",
        cert: Buffer.from([3]),
        key: ["key", Buffer.from([4])],
        rejectUnauthorized: false,
        servername: "broker.internal",
      },
    });

    expect(
      kafkaNodeInternals.snapshotRegionOptions({
        bootstrapServers: "one:9092",
        clientId: "client",
        connectTimeout: 1,
        requestTimeout: 2,
        timeout: 3,
        retries: 4,
        metadataMaxAge: 5,
        sasl: {
          mechanism: "SCRAM-SHA-256",
          username: "user",
          password: "password",
        },
        tls: {},
      }),
    ).toStrictEqual({
      bootstrapServers: ["one:9092"],
      clientId: "client",
      connectTimeout: 1,
      requestTimeout: 2,
      timeout: 3,
      retries: 4,
      metadataMaxAge: 5,
      sasl: {
        mechanism: "SCRAM-SHA-256",
        username: "user",
        password: "password",
      },
      tls: {},
    });

    const malformedRegions: ReadonlyArray<object> = [
      { bootstrapServers: "one:9092", connectTimeout: -1 },
      { bootstrapServers: "one:9092", requestTimeout: -1 },
      { bootstrapServers: "one:9092", timeout: -1 },
      { bootstrapServers: "one:9092", metadataMaxAge: -1 },
      { bootstrapServers: "one:9092", retries: 1.5 },
      { bootstrapServers: "one:9092", clientId: "" },
      { bootstrapServers: "one:9092", extra: true },
    ];
    for (const malformed of malformedRegions) {
      expect(() =>
        Reflect.apply(kafkaNodeInternals.snapshotRegionOptions, undefined, [malformed]),
      ).toThrowError("Kafka Region options are invalid.");
    }
    for (const malformed of [null, []]) {
      expect(() =>
        Reflect.apply(kafkaNodeInternals.snapshotRegionOptions, undefined, [malformed]),
      ).toThrowError("Kafka Region options are invalid.");
    }
    expect(() =>
      Reflect.apply(kafkaNodeInternals.snapshotSasl, undefined, [{ mechanism: "invalid" }]),
    ).toThrowError("Kafka Region sasl options are invalid.");
    for (const malformed of [
      null,
      [],
      { mechanism: "GSSAPI" },
      { mechanism: "PLAIN" },
      { mechanism: "PLAIN", username: 1 },
      { mechanism: "PLAIN", password: false },
      { mechanism: "PLAIN", username: "user", password: "secret", token: "extra" },
      { mechanism: "OAUTHBEARER" },
      { mechanism: "OAUTHBEARER", token: 1 },
      { mechanism: "OAUTHBEARER", token: "token", username: "extra" },
    ]) {
      expect(() =>
        Reflect.apply(kafkaNodeInternals.snapshotSasl, undefined, [malformed]),
      ).toThrowError("Kafka Region sasl options are invalid.");
    }
    expect(() =>
      Reflect.apply(kafkaNodeInternals.snapshotTls, undefined, [{ extra: true }]),
    ).toThrowError("Kafka Region tls options are invalid.");
    for (const malformed of [
      null,
      [],
      { ca: [true] },
      { cert: 1 },
      { key: {} },
      { rejectUnauthorized: "yes" },
      { serverName: 1 },
    ]) {
      expect(() =>
        Reflect.apply(kafkaNodeInternals.snapshotTls, undefined, [malformed]),
      ).toThrowError("Kafka Region tls options are invalid.");
    }
    for (const malformed of [null, {}, [], ["one:9092", ""], ["one:9092", 1]]) {
      expect(() =>
        Reflect.apply(kafkaNodeInternals.bootstrapServers, undefined, [malformed]),
      ).toThrowError("Kafka Region bootstrapServers must contain only non-empty brokers.");
    }
    const sparseBootstrap = Array<string>(3);
    sparseBootstrap[0] = "one:9092";
    sparseBootstrap[2] = "three:9092";
    let bootstrapAccessorReads = 0;
    const accessorBootstrap: Array<string> = [];
    Object.defineProperty(accessorBootstrap, "0", {
      configurable: true,
      enumerable: true,
      get: () => {
        bootstrapAccessorReads += 1;
        return "one:9092";
      },
    });
    const hostileBootstrap = new Proxy(["one:9092"], {
      ownKeys: () => {
        throw new Error("bootstrap keys failed");
      },
    });
    for (const malformed of [sparseBootstrap, accessorBootstrap, hostileBootstrap]) {
      expect(() =>
        Reflect.apply(kafkaNodeInternals.bootstrapServers, undefined, [malformed]),
      ).toThrowError("Kafka Region bootstrapServers must contain only non-empty brokers.");
    }
    expect(bootstrapAccessorReads).toBe(0);

    const sparseTls = Array<string | Uint8Array>(1);
    let tlsAccessorReads = 0;
    const accessorTls: Array<string | Uint8Array> = [];
    Object.defineProperty(accessorTls, "0", {
      configurable: true,
      enumerable: true,
      get: () => {
        tlsAccessorReads += 1;
        return "certificate";
      },
    });
    const hostileTls = new Proxy(["certificate"], {
      getOwnPropertyDescriptor: () => {
        throw new Error("TLS descriptor failed");
      },
    });
    for (const malformed of [sparseTls, accessorTls, hostileTls]) {
      expect(() =>
        kafkaNodeInternals.snapshotTls({
          ca: malformed,
        }),
      ).toThrowError("Kafka Region tls options are invalid.");
    }
    expect(tlsAccessorReads).toBe(0);

    const symbolOptions = {
      bootstrapServers: "one:9092",
      [Symbol("extra")]: true,
    };
    const accessorOptions = Object.defineProperty({}, "bootstrapServers", {
      enumerable: true,
      get: () => "one:9092",
    });
    expect(() => kafkaNodeInternals.ownDataKeys(symbolOptions)).toThrowError(
      "Kafka Node options must contain enumerable string data fields.",
    );
    expect(() => kafkaNodeInternals.ownDataKeys(accessorOptions)).toThrowError(
      "Kafka Node options must contain enumerable string data fields.",
    );
  });

  it("validates discovered Kafka bindings and snapshots pure diagnostics", () => {
    const validConfig = makeConfig("earliest");
    let propertyReads = 0;
    const regionOptions = new Proxy(
      { bootstrapServers: "one:9092" },
      {
        get: () => {
          propertyReads += 1;
          throw new Error("region option property was read");
        },
      },
    );
    const regions = new Proxy(
      { eu: regionOptions },
      {
        get: () => {
          propertyReads += 1;
          throw new Error("regions property was read");
        },
      },
    );
    const options = new Proxy(
      {
        consumerGroupPrefix: "replica",
        regions,
      },
      {
        get: () => {
          propertyReads += 1;
          throw new Error("top-level option property was read");
        },
      },
    );
    expect(
      Reflect.apply(kafkaNodeInternals.snapshotLayerOptions, undefined, [validConfig, options]),
    ).toStrictEqual({
      consumerGroupPrefix: "replica",
      regions: new Map([["eu", { bootstrapServers: ["one:9092"] }]]),
      retentionSweepIntervalNanos: 900_000_000_000n,
    });
    expect(propertyReads).toBe(0);
    const hostileOptions = new Proxy(
      {
        consumerGroupPrefix: "replica",
        regions: { eu: { bootstrapServers: "one:9092" } },
      },
      {
        ownKeys: () => {
          throw new Error("hostile ownKeys");
        },
      },
    );
    expect(() =>
      Reflect.apply(kafkaNodeInternals.snapshotLayerOptions, undefined, [
        validConfig,
        hostileOptions,
      ]),
    ).toThrowError("Kafka Node Layer requires exactly consumerGroupPrefix and regions.");
    const hostileRegion = new Proxy(
      { bootstrapServers: "one:9092" },
      {
        getOwnPropertyDescriptor: () => {
          throw new Error("hostile descriptor");
        },
      },
    );
    expect(() =>
      Reflect.apply(kafkaNodeInternals.snapshotLayerOptions, undefined, [
        validConfig,
        {
          consumerGroupPrefix: "replica",
          regions: { eu: hostileRegion },
        },
      ]),
    ).toThrowError("Kafka Region options are invalid.");
    expect(kafkaNodeInternals.kafkaSourceRegions(validConfig)).toStrictEqual(new Set(["eu"]));
    expect(() =>
      Reflect.apply(kafkaNodeInternals.kafkaSourceRegions, undefined, [null]),
    ).toThrowError("Kafka Node Layer requires a View Server Config.");
    expect(() =>
      Reflect.apply(kafkaNodeInternals.kafkaSourceRegions, undefined, [{ topics: { plain: {} } }]),
    ).toThrowError("Kafka Node Layer requires at least one Kafka Source Definition.");
    expect(() =>
      Reflect.apply(kafkaNodeInternals.kafkaSourceRegions, undefined, [
        {
          topics: {
            malformed: {
              source: {
                adapter: KafkaSourceAdapter,
                options: { regions: [] },
              },
            },
          },
        },
      ]),
    ).toThrowError("Kafka source for Topic malformed contains invalid Regions.");
    expect(() =>
      Reflect.apply(kafkaNodeInternals.kafkaSourceRegions, undefined, [
        {
          topics: {
            malformed: {
              source: {
                adapter: KafkaSourceAdapter,
                options: null,
              },
            },
          },
        },
      ]),
    ).toThrowError("Kafka source for Topic malformed contains invalid Regions.");
    expect(() =>
      Reflect.apply(kafkaNodeInternals.snapshotLayerOptions, undefined, [
        validConfig,
        {
          consumerGroupPrefix: "replica",
          regions: { eu: null },
        },
      ]),
    ).toThrowError("Kafka Node Region eu options are invalid.");
    expect(() =>
      Reflect.apply(kafkaNodeInternals.snapshotLayerOptions, undefined, [
        validConfig,
        { consumerGroupPrefix: "replica" },
      ]),
    ).toThrowError("Kafka Node Layer requires exactly consumerGroupPrefix and regions.");
    for (const malformed of [null, []]) {
      expect(() =>
        Reflect.apply(kafkaNodeInternals.snapshotLayerOptions, undefined, [validConfig, malformed]),
      ).toThrowError("Kafka Node Layer requires exactly consumerGroupPrefix and regions.");
    }

    expect([
      kafkaNodeInternals.capturedRetentionPolicy({ _tag: "MatchKafkaRetention" }),
      kafkaNodeInternals.capturedRetentionPolicy({ _tag: "Forever" }),
      kafkaNodeInternals.capturedRetentionPolicy({
        _tag: "Finite",
        durationNanos: 1n,
      }),
    ]).toStrictEqual([
      { _tag: "MatchKafkaRetention" },
      { _tag: "Forever" },
      { _tag: "Finite", durationNanos: 1n },
    ]);
    for (const malformed of [null, {}, { _tag: "Finite", durationNanos: 0n }]) {
      expect(() =>
        Reflect.apply(kafkaNodeInternals.capturedRetentionPolicy, undefined, [malformed]),
      ).toThrowError("Kafka source contains an invalid retention policy.");
    }
    expect(kafkaNodeInternals.retentionSweepIntervalNanos(undefined)).toBe(900_000_000_000n);
    expect(kafkaNodeInternals.retentionSweepIntervalNanos("2 seconds")).toBe(2_000_000_000n);
    for (const malformed of [0, "Infinity", {}]) {
      expect(() =>
        Reflect.apply(kafkaNodeInternals.retentionSweepIntervalNanos, undefined, [malformed]),
      ).toThrowError(
        "Kafka Node Layer retentionSweepInterval must be a positive finite Effect Duration.",
      );
    }

    const declarations = kafkaNodeInternals.kafkaBrokerDeclarations(makeBatchedBrokerConfig());
    expect(declarations).toStrictEqual([
      {
        cleanupPolicy: "delete",
        region: "eu",
        retentionPolicy: { _tag: "MatchKafkaRetention" },
        sourceTopic: "source-inventory",
        viewServerTopic: "inventory",
      },
      {
        cleanupPolicy: "delete",
        region: "eu",
        retentionPolicy: { _tag: "MatchKafkaRetention" },
        sourceTopic: "source-orders",
        viewServerTopic: "orders",
      },
      {
        cleanupPolicy: "delete",
        region: "us",
        retentionPolicy: { _tag: "MatchKafkaRetention" },
        sourceTopic: "source-orders",
        viewServerTopic: "orders",
      },
    ]);
    expect(
      kafkaNodeInternals.kafkaBrokerDeclarations(makeConfigWithNonKafkaDefinitions()),
    ).toHaveLength(1);
    const malformedBrokerOptions: ReadonlyArray<unknown> = [
      null,
      {},
      {
        topic: "",
        cleanupPolicy: "delete",
        regions: ["eu"],
        retentionPolicy: { _tag: "Forever" },
      },
      {
        topic: "source",
        cleanupPolicy: "invalid",
        regions: ["eu"],
        retentionPolicy: { _tag: "Forever" },
      },
      {
        topic: "source",
        cleanupPolicy: "delete",
        regions: [],
        retentionPolicy: { _tag: "Forever" },
      },
      {
        topic: "source",
        cleanupPolicy: "delete",
        regions: [""],
        retentionPolicy: { _tag: "Forever" },
      },
    ];
    for (const options of malformedBrokerOptions) {
      expect(() =>
        Reflect.apply(kafkaNodeInternals.kafkaBrokerDeclarations, undefined, [
          {
            topics: {
              malformed: {
                source: {
                  adapter: KafkaSourceAdapter,
                  options,
                },
              },
            },
          },
        ]),
      ).toThrow(KafkaSourceConfigurationError);
    }

    const validBrokerConfigs = [
      { name: "cleanup.policy", value: "delete" },
      { name: "retention.ms", value: "-1" },
    ];
    expect(kafkaNodeInternals.configValue(validBrokerConfigs, "cleanup.policy")).toBe("delete");
    expect(kafkaNodeInternals.configValue([], "cleanup.policy")).toBeUndefined();
    expect(
      kafkaNodeInternals.configValue(
        [
          { name: "cleanup.policy", value: "delete" },
          { name: "cleanup.policy", value: "compact" },
        ],
        "cleanup.policy",
      ),
    ).toBeUndefined();
    expect(
      kafkaNodeInternals.configValue([{ name: "cleanup.policy", value: 1 }], "cleanup.policy"),
    ).toBeUndefined();
    const hostileConfig = Object.create(null);
    Object.defineProperty(hostileConfig, "name", {
      enumerable: true,
      get: () => {
        throw new Error("hostile config getter");
      },
    });
    expect(kafkaNodeInternals.configValue([hostileConfig], "cleanup.policy")).toBeUndefined();
    const validBrokerResource = {
      resourceType: 2,
      resourceName: "source-orders",
      configs: validBrokerConfigs,
    };
    expect(kafkaNodeInternals.brokerConfigResource(validBrokerResource)).toStrictEqual({
      resourceName: "source-orders",
      cleanupPolicy: "delete",
      retentionMs: "-1",
    });
    for (const malformed of [
      null,
      { ...validBrokerResource, resourceType: 1 },
      { ...validBrokerResource, resourceName: "" },
      { ...validBrokerResource, configs: null },
    ]) {
      expect(
        Reflect.apply(kafkaNodeInternals.brokerConfigResource, undefined, [malformed]),
      ).toBeUndefined();
    }
    const hostileBrokerResource = Object.create(null);
    Object.defineProperty(hostileBrokerResource, "resourceType", {
      enumerable: true,
      get: () => {
        throw new Error("hostile resource getter");
      },
    });
    expect(kafkaNodeInternals.brokerConfigResource(hostileBrokerResource)).toBeUndefined();
    const malformedRetentionResource = {
      ...validBrokerResource,
      configs: [{ name: "cleanup.policy", value: "delete" }],
    };
    const malformedCleanupResource = {
      ...validBrokerResource,
      configs: [{ name: "retention.ms", value: "-1" }],
    };
    expect(kafkaNodeInternals.brokerConfigResource(malformedRetentionResource)).toStrictEqual({
      resourceName: "source-orders",
      malformedConfiguration: "retention.ms",
    });
    expect(kafkaNodeInternals.brokerConfigResource(malformedCleanupResource)).toStrictEqual({
      resourceName: "source-orders",
      malformedConfiguration: "cleanup.policy",
    });
    expect(
      kafkaNodeInternals.snapshotAdminResponse([
        validBrokerResource,
        malformedRetentionResource,
        { resourceType: 1, resourceName: "ignored", configs: [] },
      ]),
    ).toStrictEqual([
      {
        resourceName: "source-orders",
        cleanupPolicy: "delete",
        retentionMs: "-1",
      },
      {
        resourceName: "source-orders",
        malformedConfiguration: "retention.ms",
      },
    ]);
    expect(kafkaNodeInternals.snapshotAdminResponse({ malformed: "non-array" })).toBeUndefined();
    expect(kafkaNodeInternals.snapshotAdminResponse([hostileBrokerResource])).toBeUndefined();
    expect(
      kafkaNodeInternals.snapshotAdminResponse([
        {
          resourceType: 2,
          resourceName: "source-orders",
          configs: [hostileConfig],
        },
      ]),
    ).toBeUndefined();

    const metrics = kafkaNodeInternals.emptyMutableMetrics();
    metrics.assignments.set(0, {
      partition: 0,
      offset: 10n,
      lag: 2n,
    });
    metrics.activePartitions.add(0);
    metrics.commits = 1n;
    expect(kafkaNodeInternals.snapshotMetrics("eu", metrics)).toStrictEqual({
      region: "eu",
      assignments: [{ partition: 0, offset: 10n, lag: 2n }],
      commits: 1n,
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
    kafkaNodeInternals.updateLag(metrics, "source-orders", new Map([["source-orders", [-1n]]]));
    kafkaNodeInternals.updateLag(metrics, "source-orders", new Map());
    metrics.activePartitions.add(1);
    kafkaNodeInternals.updateLag(metrics, "source-orders", new Map([["source-orders", [-1n, 1n]]]));
    expect(metrics.assignments.get(0)?.lag).toBe(2n);
    expect(kafkaNodeInternals.snapshotMetrics("eu", metrics).assignments).toStrictEqual([
      { partition: 0, offset: 10n, lag: 2n },
    ]);

    const assignedFromFallbacks = kafkaNodeInternals.emptyMutableMetrics();
    kafkaNodeInternals.updateAssignmentOffsets(assignedFromFallbacks, [], [], [2]);
    expect(
      kafkaNodeInternals.snapshotMetrics("eu", assignedFromFallbacks).assignments,
    ).toStrictEqual([{ partition: 2, offset: 0n, lag: 0n }]);
    const assignedFromKnownOffsets = kafkaNodeInternals.emptyMutableMetrics();
    kafkaNodeInternals.updateAssignmentOffsets(
      assignedFromKnownOffsets,
      [{ topic: "source-orders", partition: 0, offset: 5n }],
      [10n],
      [0],
    );
    expect(
      kafkaNodeInternals.snapshotMetrics("eu", assignedFromKnownOffsets).assignments,
    ).toStrictEqual([{ partition: 0, offset: 5n, lag: 5n }]);
    kafkaNodeInternals.resetAttemptAssignments(
      assignedFromKnownOffsets,
      [{ topic: "source-orders", partition: 2, offset: 4n }],
      [],
    );
    expect(
      kafkaNodeInternals.snapshotMetrics("eu", assignedFromKnownOffsets).assignments,
    ).toStrictEqual([{ partition: 2, offset: 4n, lag: 0n }]);

    const commitBranches = kafkaNodeInternals.emptyMutableMetrics();
    const initial = {
      offsets: [],
      latestOffsets: [],
    };
    kafkaNodeInternals.updateCommit(commitBranches, initial, 3, 5n);
    kafkaNodeInternals.updateCommit(commitBranches, initial, 3, 4n);
    const committed = Option.getOrThrow(Option.fromUndefinedOr(commitBranches.assignments.get(3)));
    committed.lag = 10n;
    kafkaNodeInternals.updateCommit(commitBranches, initial, 3, 6n);
    committed.lag = 1n;
    kafkaNodeInternals.updateCommit(commitBranches, initial, 3, 8n);
    expect(kafkaNodeInternals.snapshotMetrics("eu", commitBranches).assignments).toStrictEqual([
      { partition: 3, offset: 8n, lag: 0n },
    ]);
    const initialCommitWithLag = kafkaNodeInternals.emptyMutableMetrics();
    kafkaNodeInternals.updateCommit(
      initialCommitWithLag,
      {
        offsets: [],
        latestOffsets: [10n],
      },
      0,
      5n,
    );
    expect(
      kafkaNodeInternals.snapshotMetrics("eu", initialCommitWithLag).assignments,
    ).toStrictEqual([{ partition: 0, offset: 5n, lag: 5n }]);

    const rebalanced = kafkaNodeInternals.emptyMutableMetrics();
    const rebalancedInitial = {
      offsets: [
        { topic: "source-orders", partition: 0, offset: 5n },
        { topic: "source-orders", partition: 1, offset: 20n },
      ],
      latestOffsets: [15n, 30n],
    };
    kafkaNodeInternals.resetAttemptAssignments(
      rebalanced,
      rebalancedInitial.offsets,
      rebalancedInitial.latestOffsets,
    );
    kafkaNodeInternals.updateCommit(rebalanced, rebalancedInitial, 0, 7n);
    kafkaNodeInternals.updateAssignmentOffsets(
      rebalanced,
      rebalancedInitial.offsets,
      rebalancedInitial.latestOffsets,
      [1],
    );
    expect(kafkaNodeInternals.snapshotMetrics("eu", rebalanced).assignments).toStrictEqual([
      { partition: 1, offset: 20n, lag: 10n },
    ]);
    kafkaNodeInternals.updateAssignmentOffsets(
      rebalanced,
      rebalancedInitial.offsets,
      rebalancedInitial.latestOffsets,
      [0, 1],
    );
    expect(kafkaNodeInternals.snapshotMetrics("eu", rebalanced).assignments).toStrictEqual([
      { partition: 0, offset: 7n, lag: 8n },
      { partition: 1, offset: 20n, lag: 10n },
    ]);
    kafkaNodeInternals.resetAttemptAssignments(
      rebalanced,
      [
        { topic: "source-orders", partition: 0, offset: 11n },
        { topic: "source-orders", partition: 1, offset: 25n },
      ],
      rebalancedInitial.latestOffsets,
    );
    expect(kafkaNodeInternals.snapshotMetrics("eu", rebalanced).assignments).toStrictEqual([
      { partition: 0, offset: 11n, lag: 4n },
      { partition: 1, offset: 25n, lag: 5n },
    ]);

    const traceOne = Buffer.from("one");
    const traceTwo = Buffer.from("two");
    const traceThree = Buffer.from("three");
    const single = Buffer.from("value");
    const headers = kafkaNodeInternals.headersFromMessage(
      new Map([
        [Buffer.from("trace"), traceOne],
        [Buffer.from("trace"), traceTwo],
        [Buffer.from("trace"), traceThree],
        [Buffer.from("single"), single],
      ]),
    );
    expect({
      prototype: Object.getPrototypeOf(headers),
      entries: Object.entries(headers),
    }).toStrictEqual({
      prototype: null,
      entries: [
        ["trace", [Buffer.from("one"), Buffer.from("two"), Buffer.from("three")]],
        ["single", Buffer.from("value")],
      ],
    });
    expect(headers["trace"]?.[0]).toBe(traceOne);
    expect(headers["trace"]?.[1]).toBe(traceTwo);
    expect(headers["trace"]?.[2]).toBe(traceThree);
    expect(headers["single"]).toBe(single);

    const regionInput = {
      activeGroupId: "replica:orders",
      lifetimeScope: Scope.makeUnsafe(),
      region: "eu",
      sourceTopic: "source-orders",
      start: { mode: "earliest" as const },
      viewServerTopic: "orders",
    };
    expect({
      acquisition: kafkaNodeInternals.acquisitionFailure(regionInput),
      consume: kafkaNodeInternals.consumeFailure(regionInput),
      commit: kafkaNodeInternals.commitFailure(regionInput),
      release: kafkaNodeInternals.releaseFailure(regionInput),
      key: kafkaNodeInternals.bindingKey(regionInput),
      absent: kafkaNodeInternals.offsetsForTopic(new Map(), "source-orders"),
      offsets: kafkaNodeInternals.offsetList("source-orders", [1n, 2n]),
    }).toStrictEqual({
      acquisition: {
        _tag: "KafkaAcquisitionFailure",
        region: "eu",
        topic: "source-orders",
        message: "Kafka Region consumer acquisition failed.",
      },
      consume: {
        _tag: "KafkaConsumeFailure",
        region: "eu",
        topic: "source-orders",
        message: "Kafka Region consumer stream failed.",
      },
      commit: {
        _tag: "KafkaCommitFailure",
        region: "eu",
        topic: "source-orders",
        message: "Kafka record offset commit failed.",
      },
      release: {
        _tag: "KafkaReleaseFailure",
        region: "eu",
        topic: "source-orders",
        message: "Kafka Region consumer release failed.",
      },
      key: JSON.stringify(["replica:orders", "eu", "source-orders"]),
      absent: [],
      offsets: [
        {
          topic: "source-orders",
          partition: 0,
          offset: 1n,
        },
        {
          topic: "source-orders",
          partition: 1,
          offset: 2n,
        },
      ],
    });
  });

  it.effect("commits only successful application exits", () =>
    Effect.gen(function* () {
      const committed: Array<string> = [];
      const settlement = kafkaNodeInternals.settleCommittedRecord(
        Effect.sync(() => {
          committed.push("committed");
        }),
      );
      const runtimeFailure = {
        _tag: "InvalidTopicRow",
        message: "invalid row",
        topic: "orders",
      } satisfies SourceRuntimeFailure;
      const exits: ReadonlyArray<SourceApplicationExit> = [
        Exit.void,
        Exit.fail(runtimeFailure),
        Exit.die(new Error("application defect")),
        Exit.interrupt(),
      ];
      for (const applicationExit of exits) {
        yield* settlement(applicationExit);
      }
      expect(committed).toStrictEqual(["committed"]);
    }),
  );
});
