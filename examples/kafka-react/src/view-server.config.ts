import { ViewServerId, defineViewServerConfig } from "effect-view-server/config";
import { kafka } from "effect-view-server/kafka/contract";
import { createViewServerReact } from "effect-view-server/react";
import { Schema } from "effect";

export const Order = Schema.Struct({
  id: ViewServerId,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

export const KafkaOrder = Schema.Struct({
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  updatedAt: Schema.Number,
});

export const Trade = Schema.Struct({
  id: ViewServerId,
  symbol: Schema.String,
  side: Schema.Literals(["buy", "sell"]),
  quantity: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

export const KafkaTrade = Schema.Struct({
  symbol: Schema.String,
  side: Schema.Literals(["buy", "sell"]),
  quantity: Schema.Number,
  updatedAt: Schema.Number,
});

export const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: kafka.source({
        cleanupPolicy: "delete",
        retentionPolicy: "Infinity",
        topic: "view-server-example-orders-usa",
        regions: ["usa"],
        value: kafka.json(() => Schema.toCodecJson(KafkaOrder)),
        key: kafka.string(),
        localRowKey: ({ key }) => key,
        map: ({ value, region }) => ({
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: String(region),
          updatedAt: value.updatedAt,
        }),
        startFrom: "latest",
      }),
    },
    trades: {
      schema: Trade,
      source: kafka.source({
        cleanupPolicy: "delete",
        retentionPolicy: "Infinity",
        topic: "view-server-example-trades-london",
        regions: ["london"],
        value: kafka.json(() => Schema.toCodecJson(KafkaTrade)),
        key: kafka.string(),
        localRowKey: ({ key }) => key,
        map: ({ value, region }) => ({
          symbol: value.symbol,
          side: value.side,
          quantity: value.quantity,
          region: String(region),
          updatedAt: value.updatedAt,
        }),
        startFrom: "latest",
      }),
    },
  },
});

export const viewServerReact = createViewServerReact(viewServer);
export const {
  ViewServerProvider,
  useLiveQuery,
  useSourceHealth,
  useViewServerHealth,
  useViewServerHealthSummary,
} = viewServerReact;
