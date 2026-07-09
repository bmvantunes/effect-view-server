import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { Buffer } from "node:buffer";
import { Effect, Exit, Schema } from "effect";
import {
  publishAndCommitKafkaDecodedBatch,
  planKafkaDecodedPublishRuns,
  planKafkaUpsertPublishRuns,
  recordAndCommitKeylessKafkaMessage,
  type DecodedKafkaBatchMessage,
  type DecodedKafkaBatchTombstoneMessage,
  type DecodedKafkaBatchUpsertMessage,
  type KafkaDeliveryRuntimeClient,
  type KafkaConsumerMessage,
} from "./kafka-delivery-contract";
import { makeViewServerKafkaHealthLedger } from "./kafka-health";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

const Trade = Schema.Struct({
  id: Schema.String,
  quantity: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      key: "id",
      schema: Order,
    },
    trades: {
      key: "id",
      schema: Trade,
    },
  },
});

type Topics = typeof viewServer.topics;

const kafkaMessage = (input: {
  readonly commit?: KafkaConsumerMessage["commit"];
  readonly offset: bigint;
  readonly sourceTopic: string;
}): KafkaConsumerMessage => ({
  commit: input.commit ?? (() => undefined),
  headers: new Map(),
  key: Buffer.from(`key-${input.offset}`),
  metadata: {},
  offset: input.offset,
  partition: 0,
  timestamp: 0n,
  toJSON: () => ({
    headers: [],
    key: Buffer.from(`key-${input.offset}`),
    metadata: {},
    offset: String(input.offset),
    partition: 0,
    timestamp: "0",
    topic: input.sourceTopic,
    value: Buffer.from(`value-${input.offset}`),
  }),
  topic: input.sourceTopic,
  value: Buffer.from(`value-${input.offset}`),
});

const upsertMessage = (input: {
  readonly commit?: KafkaConsumerMessage["commit"];
  readonly offset: bigint;
  readonly row: object;
  readonly rowKey: string;
  readonly sourceTopic: string;
  readonly viewServerTopic: Extract<keyof Topics, string>;
}): DecodedKafkaBatchUpsertMessage<Topics> => ({
  decoded: {
    row: input.row,
    rowKey: input.rowKey,
    viewServerTopic: input.viewServerTopic,
  },
  message: kafkaMessage({
    ...(input.commit === undefined ? {} : { commit: input.commit }),
    offset: input.offset,
    sourceTopic: input.sourceTopic,
  }),
  messageBytes: 10,
  nowMillis: Number(input.offset),
  sourceTopic: input.sourceTopic,
});

const tombstoneMessage = (input: {
  readonly commit?: KafkaConsumerMessage["commit"];
  readonly offset: bigint;
  readonly rowKey: string;
  readonly sourceTopic: string;
  readonly viewServerTopic: Extract<keyof Topics, string>;
}): DecodedKafkaBatchTombstoneMessage<Topics> => ({
  decoded: {
    rowKey: input.rowKey,
    tombstone: true,
    viewServerTopic: input.viewServerTopic,
  },
  message: kafkaMessage({
    ...(input.commit === undefined ? {} : { commit: input.commit }),
    offset: input.offset,
    sourceTopic: input.sourceTopic,
  }),
  messageBytes: 10,
  nowMillis: Number(input.offset),
  sourceTopic: input.sourceTopic,
});

const makeDeliveryClient = (
  operations: Array<string>,
  options?: {
    readonly publishFails?: boolean;
    readonly publishFailsForTopic?: Extract<keyof Topics, string>;
  },
): KafkaDeliveryRuntimeClient<Topics> => ({
  delete: (topic, key) =>
    Effect.sync(() => {
      operations.push(`delete:${topic}:${key}`);
    }),
  publishManyDecodedRowsWithStorageKeys: (topic, rows) =>
    options?.publishFails === true || options?.publishFailsForTopic === topic
      ? Effect.sync(() => {
          operations.push(`publish-fail:${topic}:${rows.map((row) => row.storageKey).join(",")}`);
        }).pipe(Effect.andThen(Effect.die("publish-down")))
      : Effect.sync(() => {
          operations.push(`publish:${topic}:${rows.map((row) => row.storageKey).join(",")}`);
        }),
});

const makeKafkaHealthLedger = () =>
  makeViewServerKafkaHealthLedger<Topics>({
    regions: {
      local: "localhost:9092",
    },
    startFrom: {
      consumerGroupId: "view-server-delivery-contract-test",
      fallbackMode: "earliest",
      mode: "earliest",
    },
    topics: {
      "orders-source": {
        regions: ["local"],
        viewServerTopic: "orders",
      },
      "trades-source": {
        regions: ["local"],
        viewServerTopic: "trades",
      },
    },
  });

const healthRefresh = (operations: Array<string>) =>
  Effect.sync(() => {
    operations.push("health");
  });

describe("Kafka delivery contract", () => {
  it("plans contiguous upsert runs and tombstone runs without reordering messages", () => {
    const firstUpsert = upsertMessage({
      offset: 1n,
      row: { id: "a", price: 10 },
      rowKey: "a",
      sourceTopic: "orders-source",
      viewServerTopic: "orders",
    });
    const secondUpsert = upsertMessage({
      offset: 2n,
      row: { id: "b", price: 20 },
      rowKey: "b",
      sourceTopic: "orders-source",
      viewServerTopic: "orders",
    });
    const tombstone = tombstoneMessage({
      offset: 3n,
      rowKey: "a",
      sourceTopic: "orders-source",
      viewServerTopic: "orders",
    });
    const finalUpsert = upsertMessage({
      offset: 4n,
      row: { id: "c", price: 30 },
      rowKey: "c",
      sourceTopic: "orders-source",
      viewServerTopic: "orders",
    });
    const messages: ReadonlyArray<DecodedKafkaBatchMessage<Topics>> = [
      firstUpsert,
      secondUpsert,
      tombstone,
      finalUpsert,
    ];

    expect(planKafkaDecodedPublishRuns(messages)).toStrictEqual([
      {
        _tag: "Upserts",
        messages: [firstUpsert, secondUpsert],
      },
      {
        _tag: "Tombstone",
        message: tombstone,
      },
      {
        _tag: "Upserts",
        messages: [finalUpsert],
      },
    ]);
  });

  it("plans upsert publish runs by contiguous source topic and View Server topic", () => {
    const firstOrder = upsertMessage({
      offset: 1n,
      row: { id: "a", price: 10 },
      rowKey: "a",
      sourceTopic: "orders-source",
      viewServerTopic: "orders",
    });
    const secondOrder = upsertMessage({
      offset: 2n,
      row: { id: "b", price: 20 },
      rowKey: "b",
      sourceTopic: "orders-source",
      viewServerTopic: "orders",
    });
    const trade = upsertMessage({
      offset: 3n,
      row: { id: "t", quantity: 5 },
      rowKey: "t",
      sourceTopic: "trades-source",
      viewServerTopic: "trades",
    });
    const londonOrder = upsertMessage({
      offset: 4n,
      row: { id: "c", price: 30 },
      rowKey: "c",
      sourceTopic: "orders-london-source",
      viewServerTopic: "orders",
    });

    expect(planKafkaUpsertPublishRuns([firstOrder, secondOrder, trade, londonOrder])).toStrictEqual(
      [
        {
          rows: [
            {
              message: firstOrder,
              row: { id: "a", price: 10 },
              storageKey: "a",
            },
            {
              message: secondOrder,
              row: { id: "b", price: 20 },
              storageKey: "b",
            },
          ],
          sourceTopic: "orders-source",
          topic: "orders",
        },
        {
          rows: [
            {
              message: trade,
              row: { id: "t", quantity: 5 },
              storageKey: "t",
            },
          ],
          sourceTopic: "trades-source",
          topic: "trades",
        },
        {
          rows: [
            {
              message: londonOrder,
              row: { id: "c", price: 30 },
              storageKey: "c",
            },
          ],
          sourceTopic: "orders-london-source",
          topic: "orders",
        },
      ],
    );
  });

  it("keeps interleaved upsert topics in separate contiguous publish runs", () => {
    const firstOrder = upsertMessage({
      offset: 1n,
      row: { id: "a", price: 10 },
      rowKey: "a",
      sourceTopic: "orders-source",
      viewServerTopic: "orders",
    });
    const trade = upsertMessage({
      offset: 2n,
      row: { id: "t", quantity: 5 },
      rowKey: "t",
      sourceTopic: "trades-source",
      viewServerTopic: "trades",
    });
    const secondOrder = upsertMessage({
      offset: 3n,
      row: { id: "b", price: 20 },
      rowKey: "b",
      sourceTopic: "orders-source",
      viewServerTopic: "orders",
    });

    expect(planKafkaUpsertPublishRuns([firstOrder, trade, secondOrder])).toStrictEqual([
      {
        rows: [
          {
            message: firstOrder,
            row: { id: "a", price: 10 },
            storageKey: "a",
          },
        ],
        sourceTopic: "orders-source",
        topic: "orders",
      },
      {
        rows: [
          {
            message: trade,
            row: { id: "t", quantity: 5 },
            storageKey: "t",
          },
        ],
        sourceTopic: "trades-source",
        topic: "trades",
      },
      {
        rows: [
          {
            message: secondOrder,
            row: { id: "b", price: 20 },
            storageKey: "b",
          },
        ],
        sourceTopic: "orders-source",
        topic: "orders",
      },
    ]);
  });

  it.effect("publishes and commits decoded runs in Kafka order", () =>
    Effect.gen(function* () {
      const operations: Array<string> = [];
      const firstUpsert = upsertMessage({
        commit: () => {
          operations.push("commit:1");
        },
        offset: 1n,
        row: { id: "a", price: 10 },
        rowKey: "a",
        sourceTopic: "orders-source",
        viewServerTopic: "orders",
      });
      const secondUpsert = upsertMessage({
        commit: () => {
          operations.push("commit:2");
        },
        offset: 2n,
        row: { id: "b", price: 20 },
        rowKey: "b",
        sourceTopic: "orders-source",
        viewServerTopic: "orders",
      });
      const tombstone = tombstoneMessage({
        commit: () => {
          operations.push("commit:3");
        },
        offset: 3n,
        rowKey: "a",
        sourceTopic: "orders-source",
        viewServerTopic: "orders",
      });
      const finalUpsert = upsertMessage({
        commit: () => {
          operations.push("commit:4");
        },
        offset: 4n,
        row: { id: "c", price: 30 },
        rowKey: "c",
        sourceTopic: "orders-source",
        viewServerTopic: "orders",
      });

      yield* publishAndCommitKafkaDecodedBatch(
        makeDeliveryClient(operations),
        healthRefresh(operations),
        makeKafkaHealthLedger(),
        "local",
        [firstUpsert, secondUpsert, tombstone, finalUpsert],
      );

      expect(operations).toStrictEqual([
        "publish:orders:a,b",
        "commit:1",
        "commit:2",
        "health",
        "delete:orders:a",
        "commit:3",
        "health",
        "publish:orders:c",
        "commit:4",
        "health",
      ]);
    }),
  );

  it.effect("does not commit decoded messages after publish failure", () =>
    Effect.gen(function* () {
      const operations: Array<string> = [];
      const message = upsertMessage({
        commit: () => {
          operations.push("commit:1");
        },
        offset: 1n,
        row: { id: "a", price: 10 },
        rowKey: "a",
        sourceTopic: "orders-source",
        viewServerTopic: "orders",
      });

      const exit = yield* Effect.exit(
        publishAndCommitKafkaDecodedBatch(
          makeDeliveryClient(operations, { publishFails: true }),
          healthRefresh(operations),
          makeKafkaHealthLedger(),
          "local",
          [message],
        ),
      );

      expect(Exit.isFailure(exit)).toBe(true);
      expect(operations).toStrictEqual(["publish-fail:orders:a", "health"]);
    }),
  );

  it.effect("commits each successful compatible upsert run before publishing the next run", () =>
    Effect.gen(function* () {
      const operations: Array<string> = [];
      const ordersMessage = upsertMessage({
        commit: () => {
          operations.push("commit:orders");
        },
        offset: 1n,
        row: { id: "a", price: 10 },
        rowKey: "a",
        sourceTopic: "orders-source",
        viewServerTopic: "orders",
      });
      const tradesMessage = upsertMessage({
        commit: () => {
          operations.push("commit:trades");
        },
        offset: 2n,
        row: { id: "t1", quantity: 20 },
        rowKey: "t1",
        sourceTopic: "trades-source",
        viewServerTopic: "trades",
      });

      const exit = yield* Effect.exit(
        publishAndCommitKafkaDecodedBatch(
          makeDeliveryClient(operations, { publishFailsForTopic: "trades" }),
          healthRefresh(operations),
          makeKafkaHealthLedger(),
          "local",
          [ordersMessage, tradesMessage],
        ),
      );

      expect(Exit.isFailure(exit)).toBe(true);
      expect(operations).toStrictEqual([
        "publish:orders:a",
        "commit:orders",
        "health",
        "publish-fail:trades:t1",
        "health",
      ]);
    }),
  );

  it.effect("records and commits keyless source messages as skipped delivery failures", () =>
    Effect.gen(function* () {
      const operations: Array<string> = [];
      const message = kafkaMessage({
        commit: () => {
          operations.push("commit:5");
        },
        offset: 5n,
        sourceTopic: "orders-source",
      });

      yield* recordAndCommitKeylessKafkaMessage(
        healthRefresh(operations),
        makeKafkaHealthLedger(),
        "local",
        message,
      );

      expect(operations).toStrictEqual(["health", "commit:5", "health"]);
    }),
  );
});
