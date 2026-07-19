import { describe, expectTypeOf, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import type { ViewServerRuntimeError, ViewServerTransportError } from "@effect-view-server/config";
import type { Effect } from "effect";
import { Schema } from "effect";
import type { ViewServerLiveSubscription } from "./index";
import type { ViewServerRemoteClient } from "./remote";
import { makeViewServerClient } from "./remote";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  price: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

declare const client: ViewServerRemoteClient<typeof viewServer.topics>;

describe("remote client type contracts", () => {
  it("constructs a typed remote client", () => {
    const make = makeViewServerClient(viewServer, {
      url: "ws://127.0.0.1:8080/rpc",
    });

    expectTypeOf<Effect.Success<typeof make>>().toMatchTypeOf<
      ViewServerRemoteClient<typeof viewServer.topics>
    >();
    expectTypeOf<Effect.Error<typeof make>>().toEqualTypeOf<
      ViewServerRuntimeError | ViewServerTransportError
    >();
  });

  it("preserves read-only subscription topic and result typing", () => {
    const subscription = client.subscribe("orders", {
      select: ["id", "price"],
      where: [
        { field: "customerId", type: "startsWith", filter: "customer-" },
        { field: "price", type: "greaterThanOrEqual", filter: 10 },
      ],
      orderBy: [{ field: "price", direction: "desc" }],
    });

    expectTypeOf<Effect.Success<typeof subscription>>().toEqualTypeOf<
      ViewServerLiveSubscription<{
        readonly id: string;
        readonly price: number;
      }>
    >();
    expectTypeOf(client).not.toHaveProperty("publish");
    expectTypeOf(client).not.toHaveProperty("publishMany");
    expectTypeOf(client).not.toHaveProperty("reset");
  });

  it("rejects invalid remote subscription queries", () => {
    const invalidTopic = client.subscribe(
      // @ts-expect-error remote subscribe topics must exist in the config.
      "missing",
      {
        select: ["id"],
      },
    );

    const invalidSubscribe = client.subscribe("orders", {
      // @ts-expect-error remote subscribe select fields must exist on the topic row.
      select: ["missing"],
    });

    const invalidWhere = client.subscribe("orders", {
      select: ["id"],
      where: [
        {
          // @ts-expect-error remote subscribe where fields must exist on the topic row.
          field: "prcie",
          type: "equals",
          filter: 10,
        },
      ],
    });

    const invalidOperator = client.subscribe("orders", {
      select: ["id"],
      where: [
        // @ts-expect-error numeric fields do not support string operators.
        { field: "price", type: "startsWith", filter: "10" },
      ],
    });

    const invalidOrderBy = client.subscribe("orders", {
      select: ["id"],
      orderBy: [
        {
          // @ts-expect-error remote subscribe orderBy fields must exist on the topic row.
          field: "prcie",
          direction: "asc",
        },
      ],
    });

    expectTypeOf(invalidTopic).not.toBeAny();
    expectTypeOf(invalidSubscribe).not.toBeAny();
    expectTypeOf(invalidWhere).not.toBeAny();
    expectTypeOf(invalidOperator).not.toBeAny();
    expectTypeOf(invalidOrderBy).not.toBeAny();
  });
});
