import { defineViewServerConfig } from "@effect-view-server/config";
import { createViewServerReact } from "@effect-view-server/react";
import { Schema } from "effect";

export const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

export const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

export const viewServerReact = createViewServerReact(viewServer);
export const { ViewServerProvider, useLiveQuery, useViewServerHealthSummary } = viewServerReact;
