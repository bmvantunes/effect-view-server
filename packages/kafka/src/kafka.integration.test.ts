import { create, toBinary } from "@bufbuild/protobuf";
import { describe, expect, it } from "@effect/vitest";
import { Admin, Producer } from "@platformatic/kafka";
import { defineViewServerConfig } from "@effect-view-server/config";
import { makeViewServerRuntimeCore } from "@effect-view-server/runtime-core";
import { Buffer } from "node:buffer";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { Clock, Data, Deferred, Effect, Layer, Option, Schedule, Schema, Stream } from "effect";
import { kafka, type KafkaSourceRetryPolicy, type KafkaStartPosition } from "./contract";
import { kafkaNode } from "./node";
import { OrderKeySchema, OrderValueSchema } from "./test-fixtures/orders_pb";

const kafkaBootstrapServers =
  process.env["VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS"] ?? "localhost:9092";
const londonKafkaBootstrapServers =
  process.env["VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS"] ?? "localhost:9094";
const integrationEnabled = process.env["VIEW_SERVER_KAFKA_INTEGRATION"] === "1";
const integrationIt = (name: string, test: () => Effect.Effect<void, unknown>): void => {
  if (integrationEnabled) {
    it.live(name, test, {
      timeout: 90_000,
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
        }),
      ),
    (current) => Effect.promise(() => current.close()).pipe(Effect.ignore),
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

const removeConsumerGroupMembers = Effect.fn("KafkaSourceAdapter.integration.groups.removeMembers")(
  function* (bootstrapServers: string, groupId: string) {
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
            throw new Error(`Kafka consumer group ${groupId} has no active members.`);
          }
          await current.removeMembersFromConsumerGroup({
            groupId,
            members: [...group.members.keys()].map((memberId) => ({ memberId })),
          });
        }),
      (current) => Effect.promise(() => current.close()).pipe(Effect.ignore),
    );
  },
);

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
  id: Schema.String,
  customerId: Schema.String,
  price: Schema.Number,
});

const ProtobufOrder = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  price: Schema.Number,
});

const CustomOrder = Schema.Struct({
  id: Schema.String,
  value: Schema.Number,
});

const customInteger = kafka.codec({
  name: "decimal-integer",
  decode: ({ bytes }) => Effect.sync(() => Number.parseInt(textDecoder.decode(bytes), 10)),
});

const jsonSource = (topic: string, startFrom: KafkaStartPosition, retry?: KafkaSourceRetryPolicy) =>
  kafka.source(
    {
      topic,
      regions: ["local"],
      key: kafka.string(),
      value: kafka.json(() => Schema.toCodecJson(JsonInput)),
      localRowKey: ({ key }) => key,
      map: ({ value }) => ({
        customerId: value.customerId,
        price: value.price,
      }),
      startFrom,
    },
    retry,
  );

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
    "ingests JSON, protobuf, and custom codecs, commits poison records, and applies tombstones",
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
          const jsonHealth = yield* diagnostics.liveClient.subscribeSourceHealth("jsonOrders");

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
                  snapshot.totalRows === 1 && snapshot.rows[0]?.id === "local:second",
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
                  health.metrics.runtime.rejectedItemCount === 1n &&
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
            rejection: degradedStatus.latestRejection,
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
                  id: "local:second",
                  customerId: "second",
                  price: 2,
                },
              ],
              totalRows: 1,
              version: expect.any(Number),
            },
            protobufSnapshot: {
              status: "ready",
              statusCode: "Ready",
              rows: [
                {
                  id: "local:protobuf",
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
                  id: "local:custom",
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
                  message: "Kafka value codec rejected the record.",
                },
              },
              location: {
                region: "local",
                topic: jsonTopic,
                partition: 0,
                offset: 1n,
                phase: "valueDecode",
                message: "Kafka value codec rejected the record.",
              },
              rejectedAtNanos: expect.any(BigInt),
            },
            runtimeMetrics: {
              rejected: 1n,
              deletes: 1n,
            },
            assignments: [{ partition: 0, offset: 4n, lag: 0n }],
          });

          yield* jsonHealth.close();
        }),
      ),
  );

  integrationIt("keeps identical local keys collision-safe across concurrent regions", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const topic = uniqueName("regional");
        yield* createTopics(kafkaBootstrapServers, [topic]);
        yield* createTopics(londonKafkaBootstrapServers, [topic]);
        const viewServer = defineViewServerConfig({
          topics: {
            orders: {
              schema: JsonOrder,
              source: kafka.source({
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
        yield* Effect.all(
          [
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
              },
            ]),
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
              },
            ]),
          ],
          { concurrency: "unbounded" },
        );
        const snapshot = yield* runtime.client
          .snapshot("orders", {
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
              until: (current) => current.totalRows === 2,
            }),
          );
        expect(snapshot).toStrictEqual({
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "london:same",
              customerId: "london:customer",
              price: 2,
            },
            {
              id: "usa:same",
              customerId: "usa:customer",
              price: 1,
            },
          ],
          totalRows: 2,
          version: expect.any(Number),
        });
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
        const earliestHealth = yield* runtime.liveClient.subscribeSourceHealth("earliestOrders");
        const latestHealth = yield* runtime.liveClient.subscribeSourceHealth("latestOrders");
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
              id: "local:new",
              customerId: "new",
              price: 2,
            },
            {
              id: "local:old",
              customerId: "old",
              price: 1,
            },
          ],
          latest: [
            {
              id: "local:new",
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
        const nowMillis = (yield* Clock.currentTimeNanos) / 1_000_000n;
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
        const committedLatestHealth =
          yield* runtime.liveClient.subscribeSourceHealth("committedLatest");
        const timestampLatestHealth =
          yield* runtime.liveClient.subscribeSourceHealth("timestampLatest");
        const durationLatestHealth =
          yield* runtime.liveClient.subscribeSourceHealth("durationLatest");
        const committedFailHealth =
          yield* runtime.liveClient.subscribeSourceHealth("committedFail");
        const timestampFailHealth =
          yield* runtime.liveClient.subscribeSourceHealth("timestampFail");
        const durationFailHealth = yield* runtime.liveClient.subscribeSourceHealth("durationFail");
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
          committedHit: "local:committed-hit",
          committedEarliest: "local:committed-earliest",
          committedLatest: "local:committed-latest",
          timestampHit: "local:timestamp-hit",
          timestampEarliest: "local:timestamp-earliest",
          timestampLatest: "local:timestamp-latest",
          durationHit: "local:duration-hit",
          durationEarliest: "local:duration-earliest",
          durationLatest: "local:duration-latest",
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
        const makeRuntime = Effect.fn("KafkaSourceAdapter.integration.duration.runtime")(
          function* () {
            const context = yield* Layer.build(
              kafkaNode.layer(viewServer, {
                consumerGroupPrefix,
                regions: {
                  local: {
                    bootstrapServers: kafkaBootstrapServers,
                  },
                },
              }),
            );
            return yield* makeViewServerRuntimeCore(viewServer, {}).pipe(
              Effect.provideContext(context),
            );
          },
        );
        const firstRuntime = yield* makeRuntime();
        const firstHealth = yield* firstRuntime.liveClient.subscribeSourceHealth("orders");
        const firstReady = Option.getOrThrow(
          yield* firstHealth.events.pipe(
            Stream.filter((health) => health.metrics.adapter.start._tag === "Resolved"),
            Stream.take(1),
            Stream.runHead,
            Effect.timeout("20 seconds"),
          ),
        );
        yield* firstHealth.close();
        yield* firstRuntime.close;
        yield* Effect.sleep("10 millis");

        const secondRuntime = yield* makeRuntime();
        const secondHealth = yield* secondRuntime.liveClient.subscribeSourceHealth("orders");
        const secondReady = Option.getOrThrow(
          yield* secondHealth.events.pipe(
            Stream.filter((health) => health.metrics.adapter.start._tag === "Resolved"),
            Stream.take(1),
            Stream.runHead,
            Effect.timeout("20 seconds"),
          ),
        );
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
        yield* secondHealth.close();
        yield* secondRuntime.close;
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
          const runtime = yield* makeViewServerRuntimeCore(viewServer, {}).pipe(
            Effect.provideContext(kafkaContext),
          );
          const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("orders");
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
          yield* removeConsumerGroupMembers(
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
            replayed: [{ id: "local:replayed", customerId: "replayed", price: 1 }],
            continued: [
              { id: "local:after", customerId: "after", price: 2 },
              { id: "local:replayed", customerId: "replayed", price: 1 },
            ],
          });
          const recoveredDiagnostics = yield* runtime.liveClient.subscribeSourceHealth("orders");
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
          yield* runtime.close;
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
        const runtime = yield* makeViewServerRuntimeCore(viewServer, {}).pipe(
          Effect.provideContext(kafkaContext),
        );
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("orders");
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
        yield* removeConsumerGroupMembers(
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
        const replayDiagnostics = yield* runtime.liveClient.subscribeSourceHealth("orders");
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
            Option.liftPredicate(replayedRejection.status, (status) => status._tag === "Degraded"),
          ).latestRejection.location,
          rows: continued.rows,
        }).toStrictEqual({
          rejection: {
            region: "local",
            topic,
            partition: 0,
            offset: 0n,
            phase: "valueDecode",
            message: "Kafka value codec rejected the record.",
          },
          rows: [{ id: "local:after", customerId: "after", price: 2 }],
        });
        yield* replayDiagnostics.close();
        yield* runtime.close;
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
        const runtime = yield* makeViewServerRuntimeCore(viewServer, {}).pipe(
          Effect.provideContext(kafkaContext),
        );
        const initialDiagnostics = yield* runtime.liveClient.subscribeSourceHealth("orders");
        yield* initialDiagnostics.events.pipe(
          Stream.filter((health) => health.status._tag === "Ready"),
          Stream.take(1),
          Stream.runDrain,
          Effect.timeout("20 seconds"),
        );
        yield* initialDiagnostics.close();

        yield* withKafkaOutage(Effect.sleep("10 seconds"));
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
        const recoveredDiagnostics = yield* runtime.liveClient.subscribeSourceHealth("orders");
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
          { id: "local:recovered", customerId: "recovered", price: 1 },
        ]);
        expect(recoveredHealth.metrics.adapter.regions[0]?.reconnects).toBeGreaterThanOrEqual(1n);
        yield* recoveredDiagnostics.close();
        yield* runtime.close;
      }),
    ),
  );

  integrationIt("resumes a restarted replica from its active-group commit", () =>
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
        const makeContext = () =>
          Layer.build(
            kafkaNode.layer(viewServer, {
              consumerGroupPrefix,
              regions: {
                local: {
                  bootstrapServers: kafkaBootstrapServers,
                },
              },
            }),
          );
        const firstContext = yield* makeContext();
        const firstRuntime = yield* makeViewServerRuntimeCore(viewServer, {}).pipe(
          Effect.provideContext(firstContext),
        );
        const firstHealth = yield* firstRuntime.liveClient.subscribeSourceHealth("orders");
        yield* firstHealth.events.pipe(
          Stream.filter((health) => health.status._tag === "Ready"),
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
        yield* firstRuntime.client
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
        yield* firstHealth.events.pipe(
          Stream.filter((health) => health.metrics.adapter.regions[0]?.commits === 1n),
          Stream.take(1),
          Stream.runDrain,
          Effect.timeout("10 seconds"),
        );
        yield* firstHealth.close();
        yield* firstRuntime.close;
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

        const secondContext = yield* makeContext();
        const secondRuntime = yield* makeViewServerRuntimeCore(viewServer, {}).pipe(
          Effect.provideContext(secondContext),
        );
        const secondHealth = yield* secondRuntime.liveClient.subscribeSourceHealth("orders");
        yield* secondHealth.events.pipe(
          Stream.filter((health) => health.status._tag === "Ready"),
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
        const restartedSnapshot = yield* secondRuntime.client
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
        expect(restartedSnapshot.rows).toStrictEqual([
          {
            id: "local:second",
            customerId: "second",
            price: 2,
          },
        ]);
        yield* secondHealth.close();
        yield* secondRuntime.close;
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
