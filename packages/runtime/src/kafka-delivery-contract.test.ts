import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { Buffer } from "node:buffer";
import { Schema } from "effect";
import {
  planKafkaDecodedPublishRuns,
  planKafkaUpsertPublishRuns,
  type DecodedKafkaBatchMessage,
  type DecodedKafkaBatchTombstoneMessage,
  type DecodedKafkaBatchUpsertMessage,
  type KafkaConsumerMessage,
} from "./kafka-delivery-contract";

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
  readonly offset: bigint;
  readonly sourceTopic: string;
}): KafkaConsumerMessage => ({
  commit: () => undefined,
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
    offset: input.offset,
    sourceTopic: input.sourceTopic,
  }),
  messageBytes: 10,
  nowMillis: Number(input.offset),
  sourceTopic: input.sourceTopic,
});

const tombstoneMessage = (input: {
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
    offset: input.offset,
    sourceTopic: input.sourceTopic,
  }),
  messageBytes: 10,
  nowMillis: Number(input.offset),
  sourceTopic: input.sourceTopic,
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
});
