import { Admin, Producer, stringSerializers } from "@platformatic/kafka";
import { Buffer } from "node:buffer";
import { Crypto, Effect, Schedule } from "effect";

import type { BinaryProducerMessage, ProducerMessage } from "./kafka-source-fixtures";

export const kafkaBootstrapServers =
  process.env["VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS"] ?? "localhost:9092";

export const londonKafkaBootstrapServers =
  process.env["VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS"] ?? "localhost:9094";

export const uniqueTopicName = Effect.fn("ViewServerRuntime.kafka.test.topicName")(function* (
  prefix: string,
) {
  const crypto = yield* Crypto.Crypto;
  const uuid = yield* crypto.randomUUIDv7;
  return `view-server-${prefix}-${uuid.replaceAll("-", "")}`;
});

export const uniqueGroupId = Effect.fn("ViewServerRuntime.kafka.test.groupId")(function* () {
  const crypto = yield* Crypto.Crypto;
  const uuid = yield* crypto.randomUUIDv7;
  return `view-server-test-${uuid.replaceAll("-", "")}`;
});

export const sendKafkaMessages = Effect.fn("ViewServerRuntime.kafka.test.produce")(function* (
  bootstrapServers: string,
  clientId: string,
  messages: ReadonlyArray<ProducerMessage>,
) {
  const producer = new Producer<string, string, string, string>({
    bootstrapBrokers: [bootstrapServers],
    clientId,
    serializers: stringSerializers,
  });

  return yield* Effect.acquireUseRelease(
    Effect.succeed(producer),
    (currentProducer) =>
      Effect.promise(() =>
        currentProducer.send({
          messages: [...messages],
        }),
      ),
    (currentProducer) => Effect.promise(() => currentProducer.close()).pipe(Effect.ignore),
  );
});

export const sendBinaryKafkaMessages = Effect.fn("ViewServerRuntime.kafka.test.produceBinary")(
  function* (
    bootstrapServers: string,
    clientId: string,
    messages: ReadonlyArray<BinaryProducerMessage>,
  ) {
    const producer = new Producer<Buffer, Buffer, Buffer, Buffer>({
      bootstrapBrokers: [bootstrapServers],
      clientId,
    });

    return yield* Effect.acquireUseRelease(
      Effect.succeed(producer),
      (currentProducer) =>
        Effect.promise(() =>
          currentProducer.send({
            messages: [...messages],
          }),
        ),
      (currentProducer) => Effect.promise(() => currentProducer.close()),
    );
  },
);

export const createKafkaTopics = Effect.fn("ViewServerRuntime.kafka.test.createTopics")(function* (
  bootstrapServers: string,
  topics: ReadonlyArray<string>,
) {
  const admin = new Admin({
    bootstrapBrokers: [bootstrapServers],
    clientId: "view-server-kafka-ingress-test-admin",
  });

  return yield* Effect.acquireUseRelease(
    Effect.succeed(admin),
    (currentAdmin) =>
      Effect.promise(() =>
        currentAdmin.createTopics({
          partitions: 1,
          replicas: 1,
          topics: [...topics],
        }),
      ),
    (currentAdmin) => Effect.promise(() => currentAdmin.close()).pipe(Effect.ignore),
  );
});

export const healthPollSchedule = Schedule.addDelay(Schedule.recurs(100), () =>
  Effect.succeed("25 millis"),
);

export const kafkaRestartPollSchedule = Schedule.addDelay(Schedule.recurs(400), () =>
  Effect.succeed("25 millis"),
);
