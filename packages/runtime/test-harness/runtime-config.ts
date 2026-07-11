import { ViewServerAuthError } from "@effect-view-server/server";
import { defineViewServerConfig } from "@effect-view-server/config";
import { Effect, Schema } from "effect";

export const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

export type OrderRow = typeof Order.Type;

export const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
});

export const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

export const order = (id: string, price: number): OrderRow => ({
  id,
  price,
});

export const bearerAuth = {
  validateRequest: (request: { readonly headers: Readonly<Record<string, string>> }) =>
    request.headers["authorization"] === "Bearer view-server-test"
      ? Effect.succeed({
          forwardedHeaders: {
            authorization: request.headers["authorization"],
          },
          id: "session-1",
          systemHeaders: {},
        })
      : Effect.fail(
          new ViewServerAuthError({
            message: "Missing or invalid authorization header.",
            status: 401,
          }),
        ),
};
