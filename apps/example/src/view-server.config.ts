import { ViewServerId, defineViewServerConfig } from "effect-view-server/config";
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

export const Trade = Schema.Struct({
  id: ViewServerId,
  symbol: Schema.String,
  quantity: Schema.BigInt,
  price: Schema.Number,
  region: Schema.String,
});

export const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
    },
    trades: {
      schema: Trade,
    },
  },
});

export const viewServerReact = createViewServerReact(viewServer);

export const { ViewServerProvider, useLiveQuery, useViewServerHealthSummary } = viewServerReact;
