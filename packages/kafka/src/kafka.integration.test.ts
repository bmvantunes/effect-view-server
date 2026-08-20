import { create, toBinary } from "@bufbuild/protobuf";
import { describe, expect, it } from "@effect/vitest";
import { Admin, Producer } from "@platformatic/kafka";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { makeViewServerRuntimeCore } from "@effect-view-server/runtime-core";
import { Buffer } from "node:buffer";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  Clock,
  Data,
  Deferred,
  Duration,
  Effect,
  Fiber,
  Layer,
  Option,
  Schedule,
  Schema,
  Stream,
} from "effect";
import { kafka, type KafkaSourceRetryPolicy, type KafkaStartPosition } from "./contract";
import { kafkaNode } from "./node";
import { OrderKeySchema, OrderValueSchema } from "./test-fixtures/orders_pb";

const kafkaBootstrapServers =
  process.env["VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS"] ?? "localhost:9092";
const londonKafkaBootstrapServers =
  process.env["VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS"] ?? "localhost:9094";
const integrationEnabled = process.env["VIEW_SERVER_KAFKA_INTEGRATION"] === "1";
const integrationIt = (
  name: string,
  test: () => Effect.Effect<void, unknown>,
  timeout = 90_000,
): void => {
  if (integrationEnabled) {
    it.live(name, test, {
      timeout,
    });
  } else {
    it.skip(name, () => {});
  }
};
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
const poll = Schedule.addDelay(Schedule.recurs(400), () => Effect.succeed("25 millis"));

type BrokerMessage = {
  readonly topic: string;
  readonly key: Uint8Array | null;
  readonly value: Uint8Array | null;
  readonly partition?: number;
  readonly timestamp?: bigint;
};

const uniqueName = (prefix: string): string =>
  `view-server-${prefix}-${randomUUID().replaceAll("-", "")}`;

const createTopics = Effect.fn("KafkaSourceAdapter.integration.topics.create")(function* (
  bootstrapServers: string,
  topics: ReadonlyArray<string>,
  partitions = 1,
  configs: ReadonlyArray<{
    readonly name: string;
    readonly value: string;
  }> = [],
) {
  const admin = new Admin({
    bootstrapBrokers: [bootstrapServers],
    clientId: uniqueName("admin"),
  });
  yield* Effect.acquireUseRelease(
    Effect.succeed(admin),
    (current) =>
      Effect.promise(() =>
        current.createTopics({
          partitions,
          replicas: 1,
          topics: [...topics],
          configs: [...configs],
        }),
      ),
    (current) => Effect.promise(() => current.close()).pipe(Effect.ignore),
  );
});

const increaseTopicPartitionsAndSend = Effect.fn(
  "KafkaSourceAdapter.integration.topics.increasePartitionsAndSend",
)(function* (bootstrapServers: string, topic: string, partitions: number, message: BrokerMessage) {
  const admin = new Admin({
    bootstrapBrokers: [bootstrapServers],
    clientId: uniqueName("partition-admin"),
  });
  const producer = new Producer<Buffer | null, Buffer | null, Buffer, Buffer>({
    bootstrapBrokers: [bootstrapServers],
    clientId: uniqueName("partition-producer"),
  });
  yield* Effect.acquireUseRelease(
    Effect.succeed({ admin, producer }),
    (current) =>
      Effect.promise(async () => {
        await current.admin.createPartitions({
          topics: [{ name: topic, count: partitions }],
        });
        await current.producer.send({
          messages: [
            {
              topic: message.topic,
              key: message.key === null ? null : Buffer.from(message.key),
              value: message.value === null ? null : Buffer.from(message.value),
              ...(message.partition === undefined ? {} : { partition: message.partition }),
              ...(message.timestamp === undefined ? {} : { timestamp: message.timestamp }),
            },
          ],
        });
      }),
    (current) =>
      Effect.all([
        Effect.promise(() => current.admin.close()).pipe(Effect.ignore),
        Effect.promise(() => current.producer.close()).pipe(Effect.ignore),
      ]).pipe(Effect.asVoid),
  );
});

const send = Effect.fn("KafkaSourceAdapter.integration.messages.send")(function* (
  bootstrapServers: string,
  messages: ReadonlyArray<BrokerMessage>,
) {
  const producer = new Producer<Buffer | null, Buffer | null, Buffer, Buffer>({
    bootstrapBrokers: [bootstrapServers],
    clientId: uniqueName("producer"),
  });
  yield* Effect.acquireUseRelease(
    Effect.succeed(producer),
    (current) =>
      Effect.promise(() =>
        current.send({
          messages: messages.map((message) => ({
            topic: message.topic,
            key: message.key === null ? null : Buffer.from(message.key),
            value: message.value === null ? null : Buffer.from(message.value),
            ...(message.partition === undefined ? {} : { partition: message.partition }),
            ...(message.timestamp === undefined ? {} : { timestamp: message.timestamp }),
          })),
        }),
      ),
    (current) => Effect.promise(() => current.close()).pipe(Effect.ignore),
  );
});

const seedCommittedOffset = Effect.fn("KafkaSourceAdapter.integration.offsets.seed")(function* (
  bootstrapServers: string,
  groupId: string,
  topic: string,
  offset: bigint,
) {
  const admin = new Admin({
    bootstrapBrokers: [bootstrapServers],
    clientId: uniqueName("seed-admin"),
  });
  yield* Effect.acquireUseRelease(
    Effect.succeed(admin),
    (current) =>
      Effect.promise(() =>
        current.alterConsumerGroupOffsets({
          groupId,
          topics: [
            {
              name: topic,
              partitionOffsets: [{ partition: 0, offset }],
            },
          ],
        }),
      ),
    (current) => Effect.promise(() => current.close()).pipe(Effect.ignore),
  );
});

const groupMemberCount = Effect.fn("KafkaSourceAdapter.integration.groups.members")(function* (
  bootstrapServers: string,
  groupId: string,
) {
  const admin = new Admin({
    bootstrapBrokers: [bootstrapServers],
    clientId: uniqueName("group-admin"),
  });
  return yield* Effect.acquireUseRelease(
    Effect.succeed(admin),
    (current) =>
      Effect.promise(() => current.describeGroups({ groups: [groupId] })).pipe(
        Effect.map((groups) => groups.get(groupId)?.members.size ?? 0),
      ),
    (current) => Effect.promise(() => current.close()).pipe(Effect.ignore),
  );
});

const removeConsumerGroupMembersIfPresent = Effect.fn(
  "KafkaSourceAdapter.integration.groups.removeMembers",
)(function* (bootstrapServers: string, groupId: string) {
  const admin = new Admin({
    bootstrapBrokers: [bootstrapServers],
    clientId: uniqueName("group-removal-admin"),
  });
  yield* Effect.acquireUseRelease(
    Effect.succeed(admin),
    (current) =>
      Effect.promise(async () => {
        const groups = await current.describeGroups({ groups: [groupId] });
        const group = groups.get(groupId);
        if (group === undefined || group.members.size === 0) {
          return;
        }
        await current.removeMembersFromConsumerGroup({
          groupId,
          members: [...group.members.keys()].map((memberId) => ({ memberId })),
        });
      }),
    (current) => Effect.promise(() => current.close()).pipe(Effect.ignore),
  );
});

const composeFile = fileURLToPath(new URL("../../../compose.yaml", import.meta.url));
class DockerComposeTestError extends Data.TaggedError("KafkaDockerComposeTestError")<{
  readonly message: string;
}> {}

const runDockerCompose = Effect.fn("KafkaSourceAdapter.integration.docker.compose")(function* (
  arguments_: ReadonlyArray<string>,
) {
  return yield* Effect.tryPromise({
    try: () =>
      new Promise<void>((resolve, reject) => {
        execFile(
          "docker",
          ["compose", "-f", composeFile, ...arguments_],
          { cwd: fileURLToPath(new URL("../../..", import.meta.url)) },
          (error) => {
            if (error === null) {
              resolve();
            } else {
              reject(error);
            }
          },
        );
      }),
    catch: () =>
      new DockerComposeTestError({
        message: "Docker Compose command failed.",
      }),
  });
});

const withKafkaOutage = <Success, Failure, Requirements>(
  use: Effect.Effect<Success, Failure, Requirements>,
): Effect.Effect<Success, Failure | DockerComposeTestError, Requirements> =>
  Effect.acquireUseRelease(
    runDockerCompose(["pause", "kafka"]),
    () => use,
    () => runDockerCompose(["unpause", "kafka"]).pipe(Effect.ignore),
  );

const bytes = (value: string): Uint8Array => textEncoder.encode(value);

const JsonInput = Schema.Struct({
  customerId: Schema.String,
  price: Schema.Number,
});

const JsonOrder = Schema.Struct({
  id: ViewServerId,
  customerId: Schema.String,
  price: Schema.Number,
});

const ProtobufOrder = Schema.Struct({
  id: ViewServerId,
  customerId: Schema.String,
  price: Schema.Number,
});

const CustomOrder = Schema.Struct({
  id: ViewServerId,
  value: Schema.Number,
});

const customInteger = kafka.codec({
  name: "decimal-integer",
  decode: ({ bytes }) => Effect.sync(() => Number.parseInt(textDecoder.decode(bytes), 10)),
});

const jsonSource = (
  topic: string,
  startFrom: KafkaStartPosition,
  retry?: KafkaSourceRetryPolicy<"local">,
) => {
  const options = {
    cleanupPolicy: "delete" as const,
    retentionPolicy: "Infinity" as const,
    topic,
    regions: ["local"] satisfies readonly ["local"],
    key: kafka.string(),
    value: kafka.json(() => Schema.toCodecJson(JsonInput)),
    localRowKey: ({ key }: { readonly key: string }) => key,
    map: ({ value }: { readonly value: typeof JsonInput.Type }) => ({
      customerId: value.customerId,
      price: value.price,
    }),
    startFrom,
  };
  return retry === undefined ? kafka.source(options) : kafka.source(options, retry);
};

const outageCodecError = {
  _tag: "KafkaOutageCodecError",
  message: "Kafka outage test payload is invalid.",
} as const;

const decodeJsonInput = (payload: Uint8Array) =>
  Effect.try({
    try: (): unknown => JSON.parse(textDecoder.decode(payload)),
    catch: () => outageCodecError,
  }).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(JsonInput)),
    Effect.mapError(() => outageCodecError),
  );

describe("Kafka Source Adapter with real Apache Kafka", () => {
  integrationIt(
    "ingests JSON, protobuf, and custom codecs and settles poison and delete-only null records",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const jsonTopic = uniqueName("json");
          const protobufTopic = uniqueName("protobuf");
          const customTopic = uniqueName("custom");
          yield* createTopics(kafkaBootstrapServers, [jsonTopic, protobufTopic, customTopic]);

          const viewServer = defineViewServerConfig({
            topics: {
              jsonOrders: {
                schema: JsonOrder,
                source: kafka.source({
                  cleanupPolicy: "delete",
                  retentionPolicy: "Infinity",
                  topic: jsonTopic,
                  regions: ["local"],
                  key: kafka.string(),
                  value: kafka.json(() => Schema.toCodecJson(JsonInput)),
                  localRowKey: ({ key }) => key,
                  map: ({ value }) => ({
                    customerId: value.customerId,
                    price: value.price,
                  }),
                  startFrom: "earliest",
                }),
              },
              protobufOrders: {
                schema: ProtobufOrder,
                source: kafka.source({
                  cleanupPolicy: "delete",
                  retentionPolicy: "Infinity",
                  topic: protobufTopic,
                  regions: ["local"],
                  key: kafka.protobuf(OrderKeySchema),
                  value: kafka.protobuf(OrderValueSchema),
                  localRowKey: ({ key }) => key.orderId,
                  map: ({ value }) => ({
                    customerId: value.customerId,
                    price: value.price,
                  }),
                  startFrom: "earliest",
                }),
              },
              customOrders: {
                schema: CustomOrder,
                source: kafka.source({
                  cleanupPolicy: "delete",
                  retentionPolicy: "Infinity",
                  topic: customTopic,
                  regions: ["local"],
                  key: kafka.string(),
                  value: customInteger,
                  localRowKey: ({ key }) => key,
                  map: ({ value }) => ({ value }),
                  startFrom: "earliest",
                }),
              },
            },
          });
          const kafkaContext = yield* Layer.build(
            kafkaNode.layer(viewServer, {
              consumerGroupPrefix: uniqueName("codec-replica"),
              regions: {
                local: {
                  bootstrapServers: kafkaBootstrapServers,
                },
              },
            }),
          );
          const diagnostics = yield* Effect.acquireRelease(
            makeViewServerRuntimeCore(viewServer, {}).pipe(Effect.provideContext(kafkaContext)),
            (runtime) => runtime.close,
          );
          const jsonHealth = yield* diagnostics.liveClient.subscribeSourceHealth({
            topic: "jsonOrders",
          });

          yield* send(kafkaBootstrapServers, [
            {
              topic: jsonTopic,
              key: bytes("first"),
              value: bytes(
                JSON.stringify({
                  customerId: "first",
                  price: 1,
                }),
              ),
            },
            {
              topic: jsonTopic,
              key: bytes("poison"),
              value: bytes("{"),
            },
            {
              topic: jsonTopic,
              key: bytes("second"),
              value: bytes(
                JSON.stringify({
                  customerId: "second",
                  price: 2,
                }),
              ),
            },
            {
              topic: jsonTopic,
              key: bytes("first"),
              value: null,
            },
            {
              topic: protobufTopic,
              key: toBinary(
                OrderKeySchema,
                create(OrderKeySchema, {
                  orderId: "protobuf",
                }),
              ),
              value: toBinary(
                OrderValueSchema,
                create(OrderValueSchema, {
                  customerId: "protobuf",
                  price: 3,
                }),
              ),
            },
            {
              topic: customTopic,
              key: bytes("custom"),
              value: bytes("4"),
            },
          ]);

          const jsonSnapshot = yield* diagnostics.client
            .snapshot("jsonOrders", {
              select: ["id", "customerId", "price"],
              orderBy: [
                {
                  field: "id",
                  direction: "asc",
                },
              ],
              limit: 10,
            })
            .pipe(
              Effect.repeat({
                schedule: poll,
                until: (snapshot) =>
                  snapshot.totalRows === 2 && snapshot.rows[1]?.id === "local:0:second",
              }),
            );
          const protobufSnapshot = yield* diagnostics.client
            .snapshot("protobufOrders", {
              select: ["id", "customerId", "price"],
              limit: 10,
            })
            .pipe(
              Effect.repeat({
                schedule: poll,
                until: (snapshot) => snapshot.totalRows === 1,
              }),
            );
          const customSnapshot = yield* diagnostics.client
            .snapshot("customOrders", {
              select: ["id", "value"],
              limit: 10,
            })
            .pipe(
              Effect.repeat({
                schedule: poll,
                until: (snapshot) => snapshot.totalRows === 1,
              }),
            );
          const degraded = Option.getOrThrow(
            yield* jsonHealth.events.pipe(
              Stream.filter(
                (health) =>
                  health.status._tag === "Degraded" &&
                  health.metrics.runtime.rejectedItemCount === 2n &&
                  health.metrics.adapter.regions[0]?.commits === 4n,
              ),
              Stream.take(1),
              Stream.runHead,
              Effect.timeout("10 seconds"),
            ),
          );
          const degradedStatus = Option.getOrThrow(
            Option.liftPredicate(degraded.status, (status) => status._tag === "Degraded"),
          );

          expect({
            jsonSnapshot,
            protobufSnapshot,
            customSnapshot,
            rejection: Option.getOrThrow(
              Option.liftPredicate(
                degradedStatus.reasons[0],
                (reason) => reason._tag === "SourceItemRejection",
              ),
            ).latestRejection,
            runtimeMetrics: {
              rejected: degraded.metrics.runtime.rejectedItemCount,
              deletes: degraded.metrics.runtime.appliedDeleteCount,
            },
            assignments: degraded.metrics.adapter.regions[0]?.assignments,
          }).toStrictEqual({
            jsonSnapshot: {
              status: "ready",
              statusCode: "Ready",
              rows: [
                {
                  id: "local:0:first",
                  customerId: "first",
                  price: 1,
                },
                {
                  id: "local:0:second",
                  customerId: "second",
                  price: 2,
                },
              ],
              totalRows: 2,
              version: expect.any(Number),
            },
            protobufSnapshot: {
              status: "ready",
              statusCode: "Ready",
              rows: [
                {
                  id: "local:0:protobuf",
                  customerId: "protobuf",
                  price: 3,
                },
              ],
              totalRows: 1,
              version: expect.any(Number),
            },
            customSnapshot: {
              status: "ready",
              statusCode: "Ready",
              rows: [
                {
                  id: "local:0:custom",
                  value: 4,
                },
              ],
              totalRows: 1,
              version: expect.any(Number),
            },
            rejection: {
              failure: {
                _tag: "AdapterFailure",
                failure: {
                  _tag: "KafkaDecodeFailure",
                  region: "local",
                  topic: jsonTopic,
                  message: "Delete-only Kafka source records require a non-null value.",
                },
              },
              location: {
                region: "local",
                topic: jsonTopic,
                partition: 0,
                offset: 3n,
                phase: "nullValue",
                message: "Delete-only Kafka source records require a non-null value.",
              },
              rejectedAtNanos: expect.any(BigInt),
            },
            runtimeMetrics: {
              rejected: 2n,
              deletes: 0n,
            },
            assignments: [{ partition: 0, offset: 4n, lag: 0n }],
          });

          yield* jsonHealth.close();
        }),
      ),
  );

  integrationIt(
    "applies compact and compact-delete tombstones, grouped live state, and retention expiry",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const compactTopic = uniqueName("compact");
          const compactDeleteTopic = uniqueName("compact-delete");
          const retainedTopic = uniqueName("retained");
          yield* createTopics(kafkaBootstrapServers, [compactTopic], 1, [
            { name: "cleanup.policy", value: "compact" },
            { name: "retention.ms", value: "-1" },
          ]);
          yield* createTopics(kafkaBootstrapServers, [compactDeleteTopic], 1, [
            { name: "cleanup.policy", value: "compact,delete" },
            { name: "retention.ms", value: "-1" },
          ]);
          yield* createTopics(kafkaBootstrapServers, [retainedTopic], 1, [
            { name: "cleanup.policy", value: "delete" },
            { name: "retention.ms", value: "-1" },
          ]);

          const viewServer = defineViewServerConfig({
            topics: {
              compactOrders: {
                schema: JsonOrder,
                source: kafka.source({
                  cleanupPolicy: "compact",
                  retentionPolicy: "match-kafka-retention",
                  topic: compactTopic,
                  regions: ["local"],
                  key: kafka.compactionKey.string(),
                  value: kafka.json(() => Schema.toCodecJson(JsonInput)),
                  map: ({ value }) => ({
                    customerId: value.customerId,
                    price: value.price,
                  }),
                  startFrom: "earliest",
                }),
              },
              compactDeleteOrders: {
                schema: JsonOrder,
                source: kafka.source({
                  cleanupPolicy: "compact-and-delete",
                  retentionPolicy: "Infinity",
                  topic: compactDeleteTopic,
                  regions: ["local"],
                  key: kafka.compactionKey.string(),
                  value: kafka.json(() => Schema.toCodecJson(JsonInput)),
                  map: ({ value }) => ({
                    customerId: value.customerId,
                    price: value.price,
                  }),
                  startFrom: "earliest",
                }),
              },
              retainedOrders: {
                schema: JsonOrder,
                source: kafka.source({
                  cleanupPolicy: "delete",
                  retentionPolicy: Duration.seconds(1),
                  topic: retainedTopic,
                  regions: ["local"],
                  key: kafka.string(),
                  value: kafka.json(() => Schema.toCodecJson(JsonInput)),
                  localRowKey: ({ key }) => key,
                  map: ({ value }) => ({
                    customerId: value.customerId,
                    price: value.price,
                  }),
                  startFrom: "earliest",
                }),
              },
            },
          });
          const kafkaContext = yield* Layer.build(
            kafkaNode.layer(viewServer, {
              consumerGroupPrefix: uniqueName("retention-replica"),
              retentionSweepInterval: "25 millis",
              regions: {
                local: {
                  bootstrapServers: kafkaBootstrapServers,
                },
              },
            }),
          );
          const runtime = yield* Effect.acquireRelease(
            makeViewServerRuntimeCore(viewServer, {}).pipe(Effect.provideContext(kafkaContext)),
            (current) => current.close,
          );
          const compactLive = yield* runtime.liveClient.subscribe("compactOrders", {
            select: ["id", "customerId", "price"],
            orderBy: [{ field: "id", direction: "asc" }],
            limit: 10,
          });
          const retainedLive = yield* runtime.liveClient.subscribe("retainedOrders", {
            select: ["id", "customerId", "price"],
            limit: 10,
          });
          const retainedGroupedLive = yield* runtime.liveClient.subscribe("retainedOrders", {
            groupBy: ["customerId"],
            aggregates: {
              rowCount: { aggFunc: "count" },
            },
            orderBy: [{ field: "customerId", direction: "asc" }],
            limit: 10,
          });
          const compactEvents = yield* compactLive.events.pipe(
            Stream.filter((event) => event.type !== "status"),
            Stream.take(4),
            Stream.runCollect,
            Effect.forkChild({ startImmediately: true }),
          );
          const retainedGroupedEvents = yield* retainedGroupedLive.events.pipe(
            Stream.filter((event) => event.type !== "status"),
            Stream.take(3),
            Stream.runCollect,
            Effect.forkChild({ startImmediately: true }),
          );
          const retainedEvents = yield* retainedLive.events.pipe(
            Stream.filter((event) => event.type !== "status"),
            Stream.take(3),
            Stream.runCollect,
            Effect.forkChild({ startImmediately: true }),
          );

          yield* send(kafkaBootstrapServers, [
            {
              topic: compactTopic,
              key: bytes("same"),
              value: bytes(JSON.stringify({ customerId: "first", price: 1 })),
            },
            {
              topic: compactTopic,
              key: bytes("same"),
              value: bytes(JSON.stringify({ customerId: "updated", price: 2 })),
            },
            {
              topic: compactDeleteTopic,
              key: bytes("first"),
              value: bytes(JSON.stringify({ customerId: "first", price: 10 })),
            },
            {
              topic: compactDeleteTopic,
              key: bytes("second"),
              value: bytes(JSON.stringify({ customerId: "second", price: 20 })),
            },
            {
              topic: retainedTopic,
              key: bytes("expires"),
              value: bytes(JSON.stringify({ customerId: "expires", price: 30 })),
            },
          ]);
          const compactBeforeTombstone = yield* runtime.client
            .snapshot("compactOrders", {
              select: ["id", "customerId", "price"],
              limit: 10,
            })
            .pipe(
              Effect.repeat({
                schedule: poll,
                until: (snapshot) => snapshot.rows[0]?.price === 2,
              }),
            );
          const retainedBeforeExpiry = yield* runtime.client
            .snapshot("retainedOrders", {
              select: ["id", "customerId", "price"],
              limit: 10,
            })
            .pipe(
              Effect.repeat({
                schedule: poll,
                until: (snapshot) => snapshot.totalRows === 1,
              }),
            );

          yield* send(kafkaBootstrapServers, [
            {
              topic: compactTopic,
              key: bytes("same"),
              value: null,
            },
            {
              topic: compactDeleteTopic,
              key: bytes("first"),
              value: null,
            },
          ]);
          const compactAfterTombstone = yield* runtime.client
            .snapshot("compactOrders", {
              select: ["id"],
              limit: 10,
            })
            .pipe(
              Effect.repeat({
                schedule: poll,
                until: (snapshot) => snapshot.totalRows === 0,
              }),
            );
          const groupedAfterTombstone = yield* runtime.client
            .snapshot("compactDeleteOrders", {
              groupBy: ["customerId"],
              aggregates: {
                rowCount: { aggFunc: "count" },
              },
              orderBy: [{ field: "customerId", direction: "asc" }],
              limit: 10,
            })
            .pipe(
              Effect.repeat({
                schedule: poll,
                until: (snapshot) =>
                  snapshot.totalRows === 1 && snapshot.rows[0]?.customerId === "second",
              }),
            );
          const retentionHealth = yield* runtime.liveClient.subscribeSourceHealth({
            topic: "retainedOrders",
          });
          const expiredHealth = Option.getOrThrow(
            yield* retentionHealth.events.pipe(
              Stream.filter(
                (health) =>
                  health.metrics.adapter.regions[0]?.retention.expiredRows === 1n &&
                  health.metrics.adapter.regions[0].retention.trackedRows === 0,
              ),
              Stream.take(1),
              Stream.runHead,
              Effect.timeout("30 seconds"),
            ),
          );
          const compactEventTypes = (yield* Fiber.join(compactEvents)).map((event) => event.type);
          const retainedEventTypes = (yield* Fiber.join(retainedEvents)).map((event) => event.type);
          const retainedGroupedConvergence = yield* Fiber.join(retainedGroupedEvents);

          expect({
            compactBeforeTombstone,
            compactAfterTombstone,
            groupedAfterTombstone,
            retainedBeforeExpiry,
            retainedAfterExpiry: yield* runtime.client.snapshot("retainedOrders", {
              select: ["id"],
              limit: 10,
            }),
            compactEventTypes,
            retainedEventTypes,
            retainedGroupedConvergence,
            retention: expiredHealth.metrics.adapter.regions[0]?.retention,
          }).toStrictEqual({
            compactBeforeTombstone: {
              rows: [{ id: "local:0:kc2FtZQ", customerId: "updated", price: 2 }],
              totalRows: 1,
              version: expect.any(Number),
              status: "ready",
              statusCode: "Ready",
            },
            compactAfterTombstone: {
              rows: [],
              totalRows: 0,
              version: expect.any(Number),
              status: "ready",
              statusCode: "Ready",
            },
            groupedAfterTombstone: {
              rows: [{ customerId: "second", rowCount: 1n }],
              totalRows: 1,
              version: expect.any(Number),
              status: "ready",
              statusCode: "Ready",
            },
            retainedBeforeExpiry: {
              rows: [
                {
                  id: "local:0:expires",
                  customerId: "expires",
                  price: 30,
                },
              ],
              totalRows: 1,
              version: expect.any(Number),
              status: "ready",
              statusCode: "Ready",
            },
            retainedAfterExpiry: {
              rows: [],
              totalRows: 0,
              version: expect.any(Number),
              status: "ready",
              statusCode: "Ready",
            },
            compactEventTypes: ["snapshot", "delta", "delta", "delta"],
            retainedEventTypes: ["snapshot", "delta", "delta"],
            retainedGroupedConvergence: [
              {
                type: "snapshot",
                topic: "retainedOrders",
                queryId: expect.any(String),
                version: 0,
                keys: [],
                rows: [],
                totalRows: 0,
              },
              {
                type: "delta",
                topic: "retainedOrders",
                queryId: expect.any(String),
                fromVersion: 0,
                toVersion: 1,
                operations: [
                  {
                    type: "insert",
                    key: expect.any(String),
                    row: {
                      customerId: "expires",
                      rowCount: 1n,
                    },
                    index: 0,
                  },
                ],
                totalRows: 1,
              },
              {
                type: "delta",
                topic: "retainedOrders",
                queryId: expect.any(String),
                fromVersion: 1,
                toVersion: 2,
                operations: [
                  {
                    type: "remove",
                    key: expect.any(String),
                  },
                ],
                totalRows: 0,
              },
            ],
            retention: {
              declaredCleanupPolicy: "delete",
              observedCleanupPolicy: "delete",
              configuredRetention: {
                _tag: "Finite",
                durationNanos: 1_000_000_000n,
              },
              resolvedRetention: {
                _tag: "Finite",
                durationNanos: 1_000_000_000n,
              },
              trackedRows: 0,
              lastSweepRetryableFailures: 0,
              expiredRows: 1n,
              authoritativeExpiredDeletes: 0n,
              failedWorkBacklog: 0,
              expirationRetryFailures: 0n,
              latestExpirationFailure: null,
              lastSweepAtNanos: expect.any(BigInt),
              lastSweepDurationNanos: expect.any(BigInt),
              sweepIntervalNanos: 25_000_000n,
            },
          });

          yield* Effect.all(
            [
              compactLive.close(),
              retainedLive.close(),
              retainedGroupedLive.close(),
              retentionHealth.close(),
            ],
            {
              discard: true,
            },
          );
        }),
      ),
  );

  integrationIt("keeps identical local keys collision-safe across concurrent regions", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const topic = uniqueName("regional");
        yield* createTopics(kafkaBootstrapServers, [topic], 1, [
          { name: "cleanup.policy", value: "delete" },
          { name: "retention.ms", value: "2000" },
        ]);
        yield* createTopics(londonKafkaBootstrapServers, [topic], 1, [
          { name: "cleanup.policy", value: "delete" },
          { name: "retention.ms", value: "5000" },
        ]);
        const viewServer = defineViewServerConfig({
          topics: {
            orders: {
              schema: JsonOrder,
              source: kafka.source({
                cleanupPolicy: "delete",
                retentionPolicy: "match-kafka-retention",
                topic,
                regions: ["usa", "london"],
                key: kafka.string(),
                value: kafka.json(() => Schema.toCodecJson(JsonInput)),
                localRowKey: ({ key }) => key,
                map: ({ region, value }) => ({
                  customerId: `${region}:${value.customerId}`,
                  price: value.price,
                }),
                startFrom: "earliest",
              }),
            },
          },
        });
        const kafkaContext = yield* Layer.build(
          kafkaNode.layer(viewServer, {
            consumerGroupPrefix: uniqueName("regional-replica"),
            retentionSweepInterval: "25 millis",
            regions: {
              usa: {
                bootstrapServers: kafkaBootstrapServers,
              },
              london: {
                bootstrapServers: londonKafkaBootstrapServers,
              },
            },
          }),
        );
        const runtime = yield* Effect.acquireRelease(
          makeViewServerRuntimeCore(viewServer, {}).pipe(Effect.provideContext(kafkaContext)),
          (current) => current.close,
        );
        const live = yield* runtime.liveClient.subscribe("orders", {
          select: ["id", "customerId", "price"],
          orderBy: [{ field: "id", direction: "asc" }],
          limit: 10,
        });
        const initialSnapshotSeen = yield* Deferred.make<void>();
        const liveEventsFiber = yield* live.events.pipe(
          Stream.filter((event) => event.type !== "status"),
          Stream.tap((event) =>
            event.type === "snapshot"
              ? Deferred.succeed(initialSnapshotSeen, undefined).pipe(Effect.asVoid)
              : Effect.void,
          ),
          Stream.take(5),
          Stream.runCollect,
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Deferred.await(initialSnapshotSeen);
        const recordTimestamp = BigInt(yield* Clock.currentTimeMillis);
        yield* Effect.all(
          [
            send(londonKafkaBootstrapServers, [
              {
                topic,
                key: bytes("same"),
                value: bytes(
                  JSON.stringify({
                    customerId: "customer",
                    price: 2,
                  }),
                ),
                timestamp: recordTimestamp,
              },
            ]),
            send(kafkaBootstrapServers, [
              {
                topic,
                key: bytes("same"),
                value: bytes(
                  JSON.stringify({
                    customerId: "customer",
                    price: 1,
                  }),
                ),
                timestamp: recordTimestamp,
              },
            ]),
          ],
          { concurrency: "unbounded", discard: true },
        );
        const bothRegions = yield* runtime.client
          .snapshot("orders", {
            select: ["id", "customerId", "price"],
            orderBy: [{ field: "id", direction: "asc" }],
            limit: 10,
          })
          .pipe(
            Effect.repeat({
              schedule: poll,
              until: (current) => current.totalRows === 2,
            }),
          );
        const afterUsaExpiry = yield* runtime.client
          .snapshot("orders", {
            select: ["id", "customerId", "price"],
            orderBy: [{ field: "id", direction: "asc" }],
            limit: 10,
          })
          .pipe(
            Effect.repeat({
              schedule: poll,
              until: (current) =>
                current.totalRows === 1 && current.rows[0]?.id === "london:0:same",
            }),
          );
        const afterLondonExpiry = yield* runtime.client
          .snapshot("orders", {
            select: ["id", "customerId", "price"],
            orderBy: [{ field: "id", direction: "asc" }],
            limit: 10,
          })
          .pipe(
            Effect.repeat({
              schedule: poll,
              until: (current) => current.totalRows === 0,
            }),
          );
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
        const expiredHealth = Option.getOrThrow(
          yield* diagnostics.events.pipe(
            Stream.filter((health) =>
              health.metrics.adapter.regions.every(
                (region) =>
                  region.retention.expiredRows === 1n && region.retention.trackedRows === 0,
              ),
            ),
            Stream.take(1),
            Stream.runHead,
            Effect.timeout("30 seconds"),
          ),
        );
        const liveEvents = yield* Fiber.join(liveEventsFiber);
        const liveEventTypes = liveEvents.map((event) => event.type);
        const liveMutationKeys = liveEvents
          .slice(1)
          .flatMap((event) =>
            event.type === "delta"
              ? event.operations.map((operation) => `${operation.type}:${operation.key}`)
              : [],
          )
          .sort();
        const retentionByRegion = expiredHealth.metrics.adapter.regions.map((region) => {
          const { lastSweepAtNanos, lastSweepDurationNanos, ...retention } = region.retention;
          return {
            region: region.region,
            retention,
            sweepRecorded: lastSweepAtNanos !== null && lastSweepDurationNanos !== null,
          };
        });

        expect({
          afterLondonExpiry,
          afterUsaExpiry,
          bothRegions,
          liveEventTypes,
          liveMutationKeys,
          retentionByRegion,
        }).toStrictEqual({
          afterLondonExpiry: {
            status: "ready",
            statusCode: "Ready",
            rows: [],
            totalRows: 0,
            version: expect.any(Number),
          },
          afterUsaExpiry: {
            status: "ready",
            statusCode: "Ready",
            rows: [
              {
                id: "london:0:same",
                customerId: "london:customer",
                price: 2,
              },
            ],
            totalRows: 1,
            version: expect.any(Number),
          },
          bothRegions: {
            status: "ready",
            statusCode: "Ready",
            rows: [
              {
                id: "london:0:same",
                customerId: "london:customer",
                price: 2,
              },
              {
                id: "usa:0:same",
                customerId: "usa:customer",
                price: 1,
              },
            ],
            totalRows: 2,
            version: expect.any(Number),
          },
          liveEventTypes: ["snapshot", "delta", "delta", "delta", "delta"],
          liveMutationKeys: [
            "insert:london:0:same",
            "insert:usa:0:same",
            "remove:london:0:same",
            "remove:usa:0:same",
          ],
          retentionByRegion: [
            {
              region: "usa",
              retention: {
                declaredCleanupPolicy: "delete",
                observedCleanupPolicy: "delete",
                configuredRetention: {
                  _tag: "MatchKafkaRetention",
                },
                resolvedRetention: {
                  _tag: "Finite",
                  durationNanos: 2_000_000_000n,
                },
                trackedRows: 0,
                lastSweepRetryableFailures: 0,
                expiredRows: 1n,
                authoritativeExpiredDeletes: 0n,
                failedWorkBacklog: 0,
                expirationRetryFailures: 0n,
                latestExpirationFailure: null,
                sweepIntervalNanos: 25_000_000n,
              },
              sweepRecorded: true,
            },
            {
              region: "london",
              retention: {
                declaredCleanupPolicy: "delete",
                observedCleanupPolicy: "delete",
                configuredRetention: {
                  _tag: "MatchKafkaRetention",
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
                sweepIntervalNanos: 25_000_000n,
              },
              sweepRecorded: true,
            },
          ],
        });
        yield* live.close();
        yield* diagnostics.close();
      }),
    ),
  );

  integrationIt("honors earliest and latest against records that predate acquisition", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const earliestTopic = uniqueName("earliest");
        const latestTopic = uniqueName("latest");
        yield* createTopics(kafkaBootstrapServers, [earliestTopic, latestTopic]);
        yield* send(kafkaBootstrapServers, [
          {
            topic: earliestTopic,
            key: bytes("old"),
            value: bytes(
              JSON.stringify({
                customerId: "old",
                price: 1,
              }),
            ),
          },
          {
            topic: latestTopic,
            key: bytes("old"),
            value: bytes(
              JSON.stringify({
                customerId: "old",
                price: 1,
              }),
            ),
          },
        ]);
        const viewServer = defineViewServerConfig({
          topics: {
            earliestOrders: {
              schema: JsonOrder,
              source: kafka.source({
                cleanupPolicy: "delete",
                retentionPolicy: "Infinity",
                topic: earliestTopic,
                regions: ["local"],
                key: kafka.string(),
                value: kafka.json(() => Schema.toCodecJson(JsonInput)),
                localRowKey: ({ key }) => key,
                map: ({ value }) => ({
                  customerId: value.customerId,
                  price: value.price,
                }),
                startFrom: "earliest",
              }),
            },
            latestOrders: {
              schema: JsonOrder,
              source: kafka.source({
                cleanupPolicy: "delete",
                retentionPolicy: "Infinity",
                topic: latestTopic,
                regions: ["local"],
                key: kafka.string(),
                value: kafka.json(() => Schema.toCodecJson(JsonInput)),
                localRowKey: ({ key }) => key,
                map: ({ value }) => ({
                  customerId: value.customerId,
                  price: value.price,
                }),
                startFrom: "latest",
              }),
            },
          },
        });
        const kafkaContext = yield* Layer.build(
          kafkaNode.layer(viewServer, {
            consumerGroupPrefix: uniqueName("start-policy-replica"),
            regions: {
              local: {
                bootstrapServers: kafkaBootstrapServers,
              },
            },
          }),
        );
        const runtime = yield* Effect.acquireRelease(
          makeViewServerRuntimeCore(viewServer, {}).pipe(Effect.provideContext(kafkaContext)),
          (current) => current.close,
        );
        const earliestHealth = yield* runtime.liveClient.subscribeSourceHealth({
          topic: "earliestOrders",
        });
        const latestHealth = yield* runtime.liveClient.subscribeSourceHealth({
          topic: "latestOrders",
        });
        yield* Effect.all(
          [
            earliestHealth.events.pipe(
              Stream.filter((health) => health.status._tag === "Ready"),
              Stream.take(1),
              Stream.runDrain,
              Effect.timeout("10 seconds"),
            ),
            latestHealth.events.pipe(
              Stream.filter((health) => health.status._tag === "Ready"),
              Stream.take(1),
              Stream.runDrain,
              Effect.timeout("10 seconds"),
            ),
          ],
          { concurrency: "unbounded" },
        );
        yield* send(kafkaBootstrapServers, [
          {
            topic: earliestTopic,
            key: bytes("new"),
            value: bytes(
              JSON.stringify({
                customerId: "new",
                price: 2,
              }),
            ),
          },
          {
            topic: latestTopic,
            key: bytes("new"),
            value: bytes(
              JSON.stringify({
                customerId: "new",
                price: 2,
              }),
            ),
          },
        ]);
        const earliestSnapshot = yield* runtime.client
          .snapshot("earliestOrders", {
            select: ["id", "customerId", "price"],
            orderBy: [
              {
                field: "id",
                direction: "asc",
              },
            ],
            limit: 10,
          })
          .pipe(
            Effect.repeat({
              schedule: poll,
              until: (snapshot) => snapshot.totalRows === 2,
            }),
          );
        const latestSnapshot = yield* runtime.client
          .snapshot("latestOrders", {
            select: ["id", "customerId", "price"],
            limit: 10,
          })
          .pipe(
            Effect.repeat({
              schedule: poll,
              until: (snapshot) => snapshot.totalRows === 1,
            }),
          );
        expect({
          earliest: earliestSnapshot.rows,
          latest: latestSnapshot.rows,
        }).toStrictEqual({
          earliest: [
            {
              id: "local:0:new",
              customerId: "new",
              price: 2,
            },
            {
              id: "local:0:old",
              customerId: "old",
              price: 1,
            },
          ],
          latest: [
            {
              id: "local:0:new",
              customerId: "new",
              price: 2,
            },
          ],
        });
        yield* earliestHealth.close();
        yield* latestHealth.close();
      }),
    ),
  );

  integrationIt("covers committed, timestamp, and duration starts with every fallback", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const topics = {
          committedHit: uniqueName("committed-hit"),
          committedEarliest: uniqueName("committed-earliest"),
          committedLatest: uniqueName("committed-latest"),
          committedFail: uniqueName("committed-fail"),
          timestampHit: uniqueName("timestamp-hit"),
          timestampEarliest: uniqueName("timestamp-earliest"),
          timestampLatest: uniqueName("timestamp-latest"),
          timestampFail: uniqueName("timestamp-fail"),
          durationHit: uniqueName("duration-hit"),
          durationEarliest: uniqueName("duration-earliest"),
          durationLatest: uniqueName("duration-latest"),
          durationFail: uniqueName("duration-fail"),
        };
        yield* createTopics(kafkaBootstrapServers, Object.values(topics));
        const nowMillis = BigInt(yield* Clock.currentTimeMillis);
        const futureNanos = (nowMillis + 86_400_000n) * 1_000_000n;
        const jsonValue = (customerId: string, price: number) =>
          bytes(JSON.stringify({ customerId, price }));
        yield* send(kafkaBootstrapServers, [
          {
            topic: topics.committedHit,
            key: bytes("committed-old"),
            value: jsonValue("committed-old", 1),
          },
          {
            topic: topics.committedHit,
            key: bytes("committed-hit"),
            value: jsonValue("committed-hit", 2),
          },
          {
            topic: topics.committedEarliest,
            key: bytes("committed-earliest"),
            value: jsonValue("committed-earliest", 3),
          },
          {
            topic: topics.committedLatest,
            key: bytes("committed-latest-old"),
            value: jsonValue("committed-latest-old", 4),
          },
          {
            topic: topics.committedFail,
            key: bytes("committed-fail-old"),
            value: jsonValue("committed-fail-old", 5),
          },
          {
            topic: topics.timestampHit,
            key: bytes("timestamp-old"),
            value: jsonValue("timestamp-old", 6),
            timestamp: 1_000n,
          },
          {
            topic: topics.timestampHit,
            key: bytes("timestamp-hit"),
            value: jsonValue("timestamp-hit", 7),
            timestamp: 3_000n,
          },
          {
            topic: topics.timestampEarliest,
            key: bytes("timestamp-earliest"),
            value: jsonValue("timestamp-earliest", 8),
            timestamp: 1_000n,
          },
          {
            topic: topics.timestampLatest,
            key: bytes("timestamp-latest-old"),
            value: jsonValue("timestamp-latest-old", 9),
            timestamp: 1_000n,
          },
          {
            topic: topics.timestampFail,
            key: bytes("timestamp-fail-old"),
            value: jsonValue("timestamp-fail-old", 10),
            timestamp: 1_000n,
          },
          {
            topic: topics.durationHit,
            key: bytes("duration-old"),
            value: jsonValue("duration-old", 11),
            timestamp: nowMillis - 7_200_000n,
          },
          {
            topic: topics.durationHit,
            key: bytes("duration-hit"),
            value: jsonValue("duration-hit", 12),
            timestamp: nowMillis - 1_800_000n,
          },
          {
            topic: topics.durationEarliest,
            key: bytes("duration-earliest"),
            value: jsonValue("duration-earliest", 13),
            timestamp: nowMillis - 60_000n,
          },
          {
            topic: topics.durationLatest,
            key: bytes("duration-latest-old"),
            value: jsonValue("duration-latest-old", 14),
            timestamp: nowMillis - 60_000n,
          },
          {
            topic: topics.durationFail,
            key: bytes("duration-fail-old"),
            value: jsonValue("duration-fail-old", 15),
            timestamp: nowMillis - 60_000n,
          },
        ]);
        yield* seedCommittedOffset(
          kafkaBootstrapServers,
          "seed-committed-hit",
          topics.committedHit,
          1n,
        );

        const noRetry = Schedule.recurs(0);
        const viewServer = defineViewServerConfig({
          topics: {
            committedHit: {
              schema: JsonOrder,
              source: jsonSource(topics.committedHit, {
                mode: "committed",
                consumerGroupId: "seed-committed-hit",
                fallback: "fail",
              }),
            },
            committedEarliest: {
              schema: JsonOrder,
              source: jsonSource(topics.committedEarliest, {
                mode: "committed",
                consumerGroupId: uniqueName("missing-committed-earliest"),
                fallback: "earliest",
              }),
            },
            committedLatest: {
              schema: JsonOrder,
              source: jsonSource(topics.committedLatest, {
                mode: "committed",
                consumerGroupId: uniqueName("missing-committed-latest"),
                fallback: "latest",
              }),
            },
            committedFail: {
              schema: JsonOrder,
              source: jsonSource(
                topics.committedFail,
                {
                  mode: "committed",
                  consumerGroupId: uniqueName("missing-committed-fail"),
                  fallback: "fail",
                },
                noRetry,
              ),
            },
            timestampHit: {
              schema: JsonOrder,
              source: jsonSource(topics.timestampHit, {
                mode: "timestamp",
                atNanos: 2_000_000_000n,
                fallback: "fail",
              }),
            },
            timestampEarliest: {
              schema: JsonOrder,
              source: jsonSource(topics.timestampEarliest, {
                mode: "timestamp",
                atNanos: futureNanos,
                fallback: "earliest",
              }),
            },
            timestampLatest: {
              schema: JsonOrder,
              source: jsonSource(topics.timestampLatest, {
                mode: "timestamp",
                atNanos: futureNanos,
                fallback: "latest",
              }),
            },
            timestampFail: {
              schema: JsonOrder,
              source: jsonSource(
                topics.timestampFail,
                {
                  mode: "timestamp",
                  atNanos: futureNanos,
                  fallback: "fail",
                },
                noRetry,
              ),
            },
            durationHit: {
              schema: JsonOrder,
              source: jsonSource(topics.durationHit, {
                mode: "durationAgo",
                duration: "1 hour",
                fallback: "fail",
              }),
            },
            durationEarliest: {
              schema: JsonOrder,
              source: jsonSource(topics.durationEarliest, {
                mode: "durationAgo",
                duration: 0,
                fallback: "earliest",
              }),
            },
            durationLatest: {
              schema: JsonOrder,
              source: jsonSource(topics.durationLatest, {
                mode: "durationAgo",
                duration: 0,
                fallback: "latest",
              }),
            },
            durationFail: {
              schema: JsonOrder,
              source: jsonSource(
                topics.durationFail,
                {
                  mode: "durationAgo",
                  duration: 0,
                  fallback: "fail",
                },
                noRetry,
              ),
            },
          },
        });
        const consumerGroupPrefix = uniqueName("start-matrix");
        const kafkaContext = yield* Layer.build(
          kafkaNode.layer(viewServer, {
            consumerGroupPrefix,
            regions: {
              local: {
                bootstrapServers: kafkaBootstrapServers,
              },
            },
          }),
        );
        const runtime = yield* Effect.acquireRelease(
          makeViewServerRuntimeCore(viewServer, {}).pipe(Effect.provideContext(kafkaContext)),
          (current) => current.close,
        );
        const committedLatestHealth = yield* runtime.liveClient.subscribeSourceHealth({
          topic: "committedLatest",
        });
        const timestampLatestHealth = yield* runtime.liveClient.subscribeSourceHealth({
          topic: "timestampLatest",
        });
        const durationLatestHealth = yield* runtime.liveClient.subscribeSourceHealth({
          topic: "durationLatest",
        });
        const committedFailHealth = yield* runtime.liveClient.subscribeSourceHealth({
          topic: "committedFail",
        });
        const timestampFailHealth = yield* runtime.liveClient.subscribeSourceHealth({
          topic: "timestampFail",
        });
        const durationFailHealth = yield* runtime.liveClient.subscribeSourceHealth({
          topic: "durationFail",
        });
        yield* Effect.all(
          [
            committedLatestHealth.events,
            timestampLatestHealth.events,
            durationLatestHealth.events,
          ].map((events) =>
            events.pipe(
              Stream.filter((health) => health.status._tag === "Ready"),
              Stream.take(1),
              Stream.runDrain,
              Effect.timeout("20 seconds"),
            ),
          ),
          { concurrency: "unbounded" },
        );
        yield* Effect.all(
          [committedFailHealth.events, timestampFailHealth.events, durationFailHealth.events].map(
            (events) =>
              events.pipe(
                Stream.filter((health) => health.status._tag === "Exhausted"),
                Stream.take(1),
                Stream.runDrain,
                Effect.timeout("20 seconds"),
              ),
          ),
          { concurrency: "unbounded" },
        );
        yield* send(kafkaBootstrapServers, [
          {
            topic: topics.committedLatest,
            key: bytes("committed-latest"),
            value: jsonValue("committed-latest", 16),
          },
          {
            topic: topics.timestampLatest,
            key: bytes("timestamp-latest"),
            value: jsonValue("timestamp-latest", 17),
          },
          {
            topic: topics.durationLatest,
            key: bytes("duration-latest"),
            value: jsonValue("duration-latest", 18),
          },
        ]);
        const snapshots = yield* Effect.all(
          {
            committedHit: runtime.client.snapshot("committedHit", {
              select: ["id"],
              limit: 10,
            }),
            committedEarliest: runtime.client.snapshot("committedEarliest", {
              select: ["id"],
              limit: 10,
            }),
            committedLatest: runtime.client.snapshot("committedLatest", {
              select: ["id"],
              limit: 10,
            }),
            timestampHit: runtime.client.snapshot("timestampHit", {
              select: ["id"],
              limit: 10,
            }),
            timestampEarliest: runtime.client.snapshot("timestampEarliest", {
              select: ["id"],
              limit: 10,
            }),
            timestampLatest: runtime.client.snapshot("timestampLatest", {
              select: ["id"],
              limit: 10,
            }),
            durationHit: runtime.client.snapshot("durationHit", {
              select: ["id"],
              limit: 10,
            }),
            durationEarliest: runtime.client.snapshot("durationEarliest", {
              select: ["id"],
              limit: 10,
            }),
            durationLatest: runtime.client.snapshot("durationLatest", {
              select: ["id"],
              limit: 10,
            }),
          },
          { concurrency: "unbounded" },
        ).pipe(
          Effect.repeat({
            schedule: poll,
            until: (current) =>
              Object.values(current).every((snapshot) => snapshot.totalRows === 1),
          }),
        );
        expect(
          Object.fromEntries(
            Object.entries(snapshots).map(([name, snapshot]) => [name, snapshot.rows[0]?.id]),
          ),
        ).toStrictEqual({
          committedHit: "local:0:committed-hit",
          committedEarliest: "local:0:committed-earliest",
          committedLatest: "local:0:committed-latest",
          timestampHit: "local:0:timestamp-hit",
          timestampEarliest: "local:0:timestamp-earliest",
          timestampLatest: "local:0:timestamp-latest",
          durationHit: "local:0:duration-hit",
          durationEarliest: "local:0:duration-earliest",
          durationLatest: "local:0:duration-latest",
        });

        yield* Effect.all(
          [
            committedLatestHealth.close(),
            timestampLatestHealth.close(),
            durationLatestHealth.close(),
            committedFailHealth.close(),
            timestampFailHealth.close(),
            durationFailHealth.close(),
          ],
          { discard: true },
        );
      }),
    ),
  );

  integrationIt("resolves duration starts afresh for a new runtime lifetime", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const topic = uniqueName("duration-restart");
        const consumerGroupPrefix = uniqueName("duration-restart-replica");
        yield* createTopics(kafkaBootstrapServers, [topic]);
        const viewServer = defineViewServerConfig({
          topics: {
            orders: {
              schema: JsonOrder,
              source: jsonSource(topic, {
                mode: "durationAgo",
                duration: 0,
                fallback: "latest",
              }),
            },
          },
        });
        const kafkaContext = yield* Layer.build(
          kafkaNode.layer(viewServer, {
            consumerGroupPrefix,
            regions: {
              local: {
                bootstrapServers: kafkaBootstrapServers,
              },
            },
          }),
        );
        const readStart = Effect.fn("KafkaSourceAdapter.integration.duration.start")(function* () {
          const runtime = yield* Effect.acquireRelease(
            makeViewServerRuntimeCore(viewServer, {}).pipe(Effect.provideContext(kafkaContext)),
            (current) => current.close,
          );
          const health = yield* Effect.acquireRelease(
            runtime.liveClient.subscribeSourceHealth({ topic: "orders" }),
            (current) => current.close().pipe(Effect.orDie),
          );
          return Option.getOrThrow(
            yield* health.events.pipe(
              Stream.filter((snapshot) => snapshot.metrics.adapter.start._tag === "Resolved"),
              Stream.take(1),
              Stream.runHead,
              Effect.timeout("20 seconds"),
            ),
          );
        });
        const firstReady = yield* Effect.scoped(readStart());
        yield* Effect.sleep("10 millis");
        const secondReady = yield* Effect.scoped(readStart());
        const firstStart = firstReady.metrics.adapter.start;
        const secondStart = secondReady.metrics.adapter.start;
        const firstPosition = Option.getOrThrow(
          Option.liftPredicate(firstStart, (start) => start._tag === "Resolved"),
        ).position;
        const secondPosition = Option.getOrThrow(
          Option.liftPredicate(secondStart, (start) => start._tag === "Resolved"),
        ).position;
        const firstDuration = Option.getOrThrow(
          Option.liftPredicate(firstPosition, (position) => position.mode === "durationAgo"),
        );
        const secondDuration = Option.getOrThrow(
          Option.liftPredicate(secondPosition, (position) => position.mode === "durationAgo"),
        );
        expect(secondDuration.resolvedAtNanos > firstDuration.resolvedAtNanos).toBe(true);
      }),
    ),
  );

  integrationIt(
    "replays an unsettled delivery after a real commit failure and then continues",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const topic = uniqueName("delivery-outage");
          const consumerGroupPrefix = uniqueName("delivery-outage-replica");
          const decodeStarted = yield* Deferred.make<void>();
          const releaseDecode = yield* Deferred.make<void>();
          let decodeCount = 0;
          yield* createTopics(kafkaBootstrapServers, [topic]);
          const gatedJson = kafka.codec({
            name: "gated-json-delivery",
            decode: ({ bytes: payload }) =>
              Effect.sync(() => {
                decodeCount += 1;
              }).pipe(
                Effect.andThen(Deferred.succeed(decodeStarted, undefined)),
                Effect.andThen(Deferred.await(releaseDecode)),
                Effect.andThen(decodeJsonInput(payload)),
              ),
          });
          const viewServer = defineViewServerConfig({
            topics: {
              orders: {
                schema: JsonOrder,
                source: kafka.source({
                  cleanupPolicy: "delete",
                  retentionPolicy: "Infinity",
                  topic,
                  regions: ["local"],
                  key: kafka.string(),
                  value: gatedJson,
                  localRowKey: ({ key }) => key,
                  map: ({ value }) => ({
                    customerId: value.customerId,
                    price: value.price,
                  }),
                  startFrom: "earliest",
                }),
              },
            },
          });
          const kafkaContext = yield* Layer.build(
            kafkaNode.layer(viewServer, {
              consumerGroupPrefix,
              regions: {
                local: {
                  bootstrapServers: kafkaBootstrapServers,
                  connectTimeout: 1_000,
                  requestTimeout: 1_000,
                  timeout: 1_000,
                  retries: 0,
                },
              },
            }),
          );
          const runtime = yield* Effect.acquireRelease(
            makeViewServerRuntimeCore(viewServer, {}).pipe(Effect.provideContext(kafkaContext)),
            (current) => current.close,
          );
          const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
          yield* diagnostics.events.pipe(
            Stream.filter((health) => health.status._tag === "Ready"),
            Stream.take(1),
            Stream.runDrain,
            Effect.timeout("20 seconds"),
          );
          yield* send(kafkaBootstrapServers, [
            {
              topic,
              key: bytes("replayed"),
              value: bytes(JSON.stringify({ customerId: "replayed", price: 1 })),
            },
          ]);
          yield* Deferred.await(decodeStarted);
          yield* removeConsumerGroupMembersIfPresent(
            kafkaBootstrapServers,
            kafka.consumerGroupId(consumerGroupPrefix, "orders"),
          );
          yield* diagnostics.close();
          yield* Deferred.succeed(releaseDecode, undefined);
          expect(
            yield* Effect.sync(() => decodeCount).pipe(
              Effect.repeat({
                schedule: poll,
                until: (count) => count >= 2,
              }),
            ),
          ).toBeGreaterThanOrEqual(2);
          const replayed = yield* runtime.client
            .snapshot("orders", {
              select: ["id", "customerId", "price"],
              orderBy: [{ field: "id", direction: "asc" }],
              limit: 10,
            })
            .pipe(
              Effect.repeat({
                schedule: poll,
                until: (snapshot) => snapshot.totalRows === 1,
              }),
            );
          yield* send(kafkaBootstrapServers, [
            {
              topic,
              key: bytes("after"),
              value: bytes(JSON.stringify({ customerId: "after", price: 2 })),
            },
          ]);
          const continued = yield* runtime.client
            .snapshot("orders", {
              select: ["id", "customerId", "price"],
              orderBy: [{ field: "id", direction: "asc" }],
              limit: 10,
            })
            .pipe(
              Effect.repeat({
                schedule: poll,
                until: (snapshot) => snapshot.totalRows === 2,
              }),
            );
          expect({
            replayed: replayed.rows,
            continued: continued.rows,
          }).toStrictEqual({
            replayed: [{ id: "local:0:replayed", customerId: "replayed", price: 1 }],
            continued: [
              { id: "local:0:after", customerId: "after", price: 2 },
              { id: "local:0:replayed", customerId: "replayed", price: 1 },
            ],
          });
          const recoveredDiagnostics = yield* runtime.liveClient.subscribeSourceHealth({
            topic: "orders",
          });
          const recovered = Option.getOrThrow(
            yield* recoveredDiagnostics.events.pipe(
              Stream.filter(
                (health) =>
                  health.metrics.adapter.regions[0]?.commits === 2n &&
                  health.metrics.adapter.regions[0].commitFailures === 1n &&
                  health.metrics.adapter.regions[0].reconnects === 1n,
              ),
              Stream.take(1),
              Stream.runHead,
              Effect.timeout("20 seconds"),
            ),
          );
          expect(recovered.metrics.adapter.regions[0]?.commits).toBe(2n);
          expect(recovered.metrics.adapter.regions[0]?.commitFailures).toBe(1n);
          expect(recovered.metrics.adapter.regions[0]?.reconnects).toBe(1n);
          yield* recoveredDiagnostics.close();
        }),
      ),
  );

  integrationIt("replays a rejected record when its real commit fails", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const topic = uniqueName("rejection-outage");
        const consumerGroupPrefix = uniqueName("rejection-outage-replica");
        const decodeStarted = yield* Deferred.make<void>();
        const releaseDecode = yield* Deferred.make<void>();
        let poisonDecodeCount = 0;
        yield* createTopics(kafkaBootstrapServers, [topic]);
        const gatedJson = kafka.codec({
          name: "gated-json-rejection",
          decode: ({ bytes: payload }) => {
            const text = textDecoder.decode(payload);
            if (text === "poison") {
              return Effect.sync(() => {
                poisonDecodeCount += 1;
              }).pipe(
                Effect.andThen(Deferred.succeed(decodeStarted, undefined)),
                Effect.andThen(Deferred.await(releaseDecode)),
                Effect.andThen(Effect.fail(outageCodecError)),
              );
            }
            return decodeJsonInput(payload);
          },
        });
        const viewServer = defineViewServerConfig({
          topics: {
            orders: {
              schema: JsonOrder,
              source: kafka.source({
                cleanupPolicy: "delete",
                retentionPolicy: "Infinity",
                topic,
                regions: ["local"],
                key: kafka.string(),
                value: gatedJson,
                localRowKey: ({ key }) => key,
                map: ({ value }) => ({
                  customerId: value.customerId,
                  price: value.price,
                }),
                startFrom: "earliest",
              }),
            },
          },
        });
        const kafkaContext = yield* Layer.build(
          kafkaNode.layer(viewServer, {
            consumerGroupPrefix,
            regions: {
              local: {
                bootstrapServers: kafkaBootstrapServers,
                connectTimeout: 1_000,
                requestTimeout: 1_000,
                timeout: 1_000,
                retries: 0,
              },
            },
          }),
        );
        const runtime = yield* Effect.acquireRelease(
          makeViewServerRuntimeCore(viewServer, {}).pipe(Effect.provideContext(kafkaContext)),
          (current) => current.close,
        );
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
        yield* diagnostics.events.pipe(
          Stream.filter((health) => health.status._tag === "Ready"),
          Stream.take(1),
          Stream.runDrain,
          Effect.timeout("20 seconds"),
        );
        yield* send(kafkaBootstrapServers, [
          {
            topic,
            key: bytes("poison"),
            value: bytes("poison"),
          },
        ]);
        yield* Deferred.await(decodeStarted);
        yield* removeConsumerGroupMembersIfPresent(
          kafkaBootstrapServers,
          kafka.consumerGroupId(consumerGroupPrefix, "orders"),
        );
        yield* diagnostics.close();
        yield* Deferred.succeed(releaseDecode, undefined);
        expect(
          yield* Effect.sync(() => poisonDecodeCount).pipe(
            Effect.repeat({
              schedule: poll,
              until: (count) => count >= 2,
            }),
          ),
        ).toBeGreaterThanOrEqual(2);
        const replayDiagnostics = yield* runtime.liveClient.subscribeSourceHealth({
          topic: "orders",
        });
        const replayedRejection = Option.getOrThrow(
          yield* replayDiagnostics.events.pipe(
            Stream.filter(
              (health) =>
                health.status._tag === "Degraded" &&
                health.metrics.runtime.rejectedItemCount === 2n &&
                health.metrics.adapter.regions[0]?.commits === 1n &&
                health.metrics.adapter.regions[0].commitFailures === 1n &&
                health.metrics.adapter.regions[0].reconnects === 1n,
            ),
            Stream.take(1),
            Stream.runHead,
            Effect.timeout("20 seconds"),
          ),
        );
        yield* send(kafkaBootstrapServers, [
          {
            topic,
            key: bytes("after"),
            value: bytes(JSON.stringify({ customerId: "after", price: 2 })),
          },
        ]);
        const continued = yield* runtime.client
          .snapshot("orders", {
            select: ["id", "customerId", "price"],
            limit: 10,
          })
          .pipe(
            Effect.repeat({
              schedule: poll,
              until: (snapshot) => snapshot.totalRows === 1,
            }),
          );
        expect({
          rejection: Option.getOrThrow(
            Option.liftPredicate(
              Option.getOrThrow(
                Option.liftPredicate(
                  replayedRejection.status,
                  (status) => status._tag === "Degraded",
                ),
              ).reasons[0],
              (reason) => reason._tag === "SourceItemRejection",
            ),
          ).latestRejection.location,
          rows: continued.rows,
        }).toStrictEqual({
          rejection: {
            region: "local",
            topic,
            partition: 0,
            offset: 0n,
            phase: "valueDecode",
            message: 'Kafka value codec "gated-json-rejection" rejected the record.',
          },
          rows: [{ id: "local:0:after", customerId: "after", price: 2 }],
        });
        yield* replayDiagnostics.close();
      }),
    ),
  );

  integrationIt("reacquires its source attempt after a broker outage", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const topic = uniqueName("broker-outage");
        const consumerGroupPrefix = uniqueName("broker-outage-replica");
        yield* createTopics(kafkaBootstrapServers, [topic]);
        const viewServer = defineViewServerConfig({
          topics: {
            orders: {
              schema: JsonOrder,
              source: jsonSource(topic, "earliest"),
            },
          },
        });
        const kafkaContext = yield* Layer.build(
          kafkaNode.layer(viewServer, {
            consumerGroupPrefix,
            regions: {
              local: {
                bootstrapServers: kafkaBootstrapServers,
                connectTimeout: 1_000,
                requestTimeout: 1_000,
                timeout: 1_000,
                retries: 0,
              },
            },
          }),
        );
        const runtime = yield* Effect.acquireRelease(
          makeViewServerRuntimeCore(viewServer, {}).pipe(Effect.provideContext(kafkaContext)),
          (current) => current.close,
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            const initialDiagnostics = yield* Effect.acquireRelease(
              runtime.liveClient.subscribeSourceHealth({ topic: "orders" }),
              (subscription) => subscription.close().pipe(Effect.orDie),
            );
            yield* initialDiagnostics.events.pipe(
              Stream.filter((health) => health.status._tag === "Ready"),
              Stream.take(1),
              Stream.runDrain,
              Effect.timeout("20 seconds"),
            );
          }),
        );

        yield* Effect.scoped(
          Effect.gen(function* () {
            const outageDiagnostics = yield* Effect.acquireRelease(
              runtime.liveClient.subscribeSourceHealth({ topic: "orders" }),
              (subscription) => subscription.close().pipe(Effect.orDie),
            );
            yield* withKafkaOutage(
              outageDiagnostics.events.pipe(
                Stream.filter(
                  (health) =>
                    health.status._tag === "WaitingToRetry" || health.status._tag === "Reacquiring",
                ),
                Stream.take(1),
                Stream.runDrain,
                Effect.timeout("20 seconds"),
              ),
            );
          }),
        );
        yield* send(kafkaBootstrapServers, [
          {
            topic,
            key: bytes("recovered"),
            value: bytes(JSON.stringify({ customerId: "recovered", price: 1 })),
          },
        ]);
        const recovered = yield* runtime.client
          .snapshot("orders", {
            select: ["id", "customerId", "price"],
            limit: 10,
          })
          .pipe(
            Effect.repeat({
              schedule: poll,
              until: (snapshot) => snapshot.totalRows === 1,
            }),
          );
        const recoveredDiagnostics = yield* Effect.acquireRelease(
          runtime.liveClient.subscribeSourceHealth({ topic: "orders" }),
          (subscription) => subscription.close().pipe(Effect.orDie),
        );
        const recoveredHealth = Option.getOrThrow(
          yield* recoveredDiagnostics.events.pipe(
            Stream.filter(
              (health) =>
                health.metrics.adapter.regions[0]?.commits === 1n &&
                health.metrics.adapter.regions[0].reconnects >= 1n,
            ),
            Stream.take(1),
            Stream.runHead,
            Effect.timeout("20 seconds"),
          ),
        );
        expect(recovered.rows).toStrictEqual([
          { id: "local:0:recovered", customerId: "recovered", price: 1 },
        ]);
        expect(recoveredHealth.metrics.adapter.regions[0]?.reconnects).toBeGreaterThanOrEqual(1n);
      }),
    ),
  );

  integrationIt(
    "reacquires configured offsets when a partition is added at runtime",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const topic = uniqueName("runtime-partition");
          const consumerGroupPrefix = uniqueName("runtime-partition-replica");
          yield* createTopics(kafkaBootstrapServers, [topic]);
          const viewServer = defineViewServerConfig({
            topics: {
              orders: {
                schema: JsonOrder,
                source: jsonSource(topic, "earliest"),
              },
            },
          });
          const kafkaContext = yield* Layer.build(
            kafkaNode.layer(viewServer, {
              consumerGroupPrefix,
              regions: {
                local: {
                  bootstrapServers: kafkaBootstrapServers,
                  metadataMaxAge: 10_000,
                },
              },
            }),
          );
          const runtime = yield* Effect.acquireRelease(
            makeViewServerRuntimeCore(viewServer, {}).pipe(Effect.provideContext(kafkaContext)),
            (current) => current.close,
          );
          yield* Effect.scoped(
            Effect.gen(function* () {
              const initialDiagnostics = yield* Effect.acquireRelease(
                runtime.liveClient.subscribeSourceHealth({ topic: "orders" }),
                (current) => current.close().pipe(Effect.orDie),
              );
              yield* initialDiagnostics.events.pipe(
                Stream.filter((health) => health.status._tag === "Ready"),
                Stream.take(1),
                Stream.runDrain,
                Effect.timeout("20 seconds"),
              );
            }),
          );
          const growthDiagnostics = yield* Effect.acquireRelease(
            runtime.liveClient.subscribeSourceHealth({ topic: "orders" }),
            (current) => current.close().pipe(Effect.orDie),
          );

          yield* increaseTopicPartitionsAndSend(kafkaBootstrapServers, topic, 2, {
            topic,
            partition: 1,
            key: bytes("new-partition"),
            value: bytes(JSON.stringify({ customerId: "new-partition", price: 1 })),
          });
          yield* Effect.sleep("11 seconds");
          yield* removeConsumerGroupMembersIfPresent(
            kafkaBootstrapServers,
            kafka.consumerGroupId(consumerGroupPrefix, "orders"),
          );

          const growthFailure = Option.getOrThrow(
            yield* growthDiagnostics.events.pipe(
              Stream.filter(
                (health) =>
                  health.status._tag === "WaitingToRetry" &&
                  health.status.termination._tag === "Failed" &&
                  health.status.termination.failure._tag === "AdapterFailure" &&
                  health.status.termination.failure.failure._tag === "KafkaConsumeFailure" &&
                  health.status.termination.failure.failure.message ===
                    "Kafka Region discovered a new partition and is reacquiring configured start offsets.",
              ),
              Stream.take(1),
              Stream.runHead,
              Effect.timeout("20 seconds"),
            ),
          );
          const recovered = yield* runtime.client
            .snapshot("orders", {
              select: ["id", "customerId", "price"],
              limit: 10,
            })
            .pipe(
              Effect.repeat({
                schedule: poll,
                until: (snapshot) => snapshot.totalRows === 1,
              }),
            );
          expect(recovered.rows).toStrictEqual([
            {
              id: "local:1:new-partition",
              customerId: "new-partition",
              price: 1,
            },
          ]);
          const recoveredDiagnostics = yield* Effect.acquireRelease(
            runtime.liveClient.subscribeSourceHealth({ topic: "orders" }),
            (current) => current.close().pipe(Effect.orDie),
          );
          const recoveredHealth = Option.getOrThrow(
            yield* recoveredDiagnostics.events.pipe(
              Stream.filter(
                (health) =>
                  health.status._tag === "Ready" &&
                  health.metrics.adapter.regions[0]?.reconnects === 1n &&
                  health.metrics.adapter.regions[0].assignments.length === 2,
              ),
              Stream.take(1),
              Stream.runHead,
              Effect.timeout("20 seconds"),
            ),
          );

          expect({
            growthFailure: growthFailure.status,
            rows: recovered.rows,
            reconnects: recoveredHealth.metrics.adapter.regions[0]?.reconnects,
            assignments: recoveredHealth.metrics.adapter.regions[0]?.assignments,
          }).toStrictEqual({
            growthFailure: {
              _tag: "WaitingToRetry",
              nextAttempt: 2n,
              termination: {
                _tag: "Failed",
                failure: {
                  _tag: "AdapterFailure",
                  failure: {
                    _tag: "KafkaConsumeFailure",
                    region: "local",
                    topic,
                    message:
                      "Kafka Region discovered a new partition and is reacquiring configured start offsets.",
                  },
                },
              },
              retryAtNanos: expect.any(BigInt),
            },
            rows: [
              {
                id: "local:1:new-partition",
                customerId: "new-partition",
                price: 1,
              },
            ],
            reconnects: 1n,
            assignments: [
              { partition: 0, offset: 0n, lag: 0n },
              { partition: 1, offset: 1n, lag: 0n },
            ],
          });
        }),
      ),
    120_000,
  );

  integrationIt("reapplies explicit start for a restarted replica", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const topic = uniqueName("restart");
        const consumerGroupPrefix = uniqueName("restart-replica");
        yield* createTopics(kafkaBootstrapServers, [topic]);
        const viewServer = defineViewServerConfig({
          topics: {
            orders: {
              schema: JsonOrder,
              source: kafka.source({
                cleanupPolicy: "delete",
                retentionPolicy: "Infinity",
                topic,
                regions: ["local"],
                key: kafka.string(),
                value: kafka.json(() => Schema.toCodecJson(JsonInput)),
                localRowKey: ({ key }) => key,
                map: ({ value }) => ({
                  customerId: value.customerId,
                  price: value.price,
                }),
                startFrom: "earliest",
              }),
            },
          },
        });
        const kafkaContext = yield* Layer.build(
          kafkaNode.layer(viewServer, {
            consumerGroupPrefix,
            regions: {
              local: {
                bootstrapServers: kafkaBootstrapServers,
              },
            },
          }),
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            const runtime = yield* Effect.acquireRelease(
              makeViewServerRuntimeCore(viewServer, {}).pipe(Effect.provideContext(kafkaContext)),
              (current) => current.close,
            );
            const health = yield* Effect.acquireRelease(
              runtime.liveClient.subscribeSourceHealth({ topic: "orders" }),
              (current) => current.close().pipe(Effect.orDie),
            );
            yield* health.events.pipe(
              Stream.filter((snapshot) => snapshot.status._tag === "Ready"),
              Stream.take(1),
              Stream.runDrain,
              Effect.timeout("10 seconds"),
            );
            yield* send(kafkaBootstrapServers, [
              {
                topic,
                key: bytes("first"),
                value: bytes(
                  JSON.stringify({
                    customerId: "first",
                    price: 1,
                  }),
                ),
              },
            ]);
            yield* runtime.client
              .snapshot("orders", {
                select: ["id"],
                limit: 10,
              })
              .pipe(
                Effect.repeat({
                  schedule: poll,
                  until: (snapshot) => snapshot.totalRows === 1,
                }),
              );
            yield* health.events.pipe(
              Stream.filter((snapshot) => snapshot.metrics.adapter.regions[0]?.commits === 1n),
              Stream.take(1),
              Stream.runDrain,
              Effect.timeout("10 seconds"),
            );
          }),
        );
        expect(
          yield* groupMemberCount(
            kafkaBootstrapServers,
            kafka.consumerGroupId(consumerGroupPrefix, "orders"),
          ).pipe(
            Effect.repeat({
              schedule: poll,
              until: (members) => members === 0,
            }),
          ),
        ).toBe(0);

        yield* Effect.scoped(
          Effect.gen(function* () {
            const runtime = yield* Effect.acquireRelease(
              makeViewServerRuntimeCore(viewServer, {}).pipe(Effect.provideContext(kafkaContext)),
              (current) => current.close,
            );
            const health = yield* Effect.acquireRelease(
              runtime.liveClient.subscribeSourceHealth({ topic: "orders" }),
              (current) => current.close().pipe(Effect.orDie),
            );
            yield* health.events.pipe(
              Stream.filter((snapshot) => snapshot.status._tag === "Ready"),
              Stream.take(1),
              Stream.runDrain,
              Effect.timeout("10 seconds"),
            );
            yield* send(kafkaBootstrapServers, [
              {
                topic,
                key: bytes("second"),
                value: bytes(
                  JSON.stringify({
                    customerId: "second",
                    price: 2,
                  }),
                ),
              },
            ]);
            const restartedSnapshot = yield* runtime.client
              .snapshot("orders", {
                select: ["id", "customerId", "price"],
                orderBy: [{ field: "id", direction: "asc" }],
                limit: 10,
              })
              .pipe(
                Effect.repeat({
                  schedule: poll,
                  until: (snapshot) => snapshot.totalRows === 2,
                }),
              );
            expect(restartedSnapshot.rows).toStrictEqual([
              {
                id: "local:0:first",
                customerId: "first",
                price: 1,
              },
              {
                id: "local:0:second",
                customerId: "second",
                price: 2,
              },
            ]);
          }),
        );
        expect(
          yield* groupMemberCount(
            kafkaBootstrapServers,
            kafka.consumerGroupId(consumerGroupPrefix, "orders"),
          ).pipe(
            Effect.repeat({
              schedule: poll,
              until: (members) => members === 0,
            }),
          ),
        ).toBe(0);
      }),
    ),
  );
});
