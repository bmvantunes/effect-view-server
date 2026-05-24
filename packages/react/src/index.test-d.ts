import { describe, expectTypeOf, it } from "@effect/vitest";
import { defineViewServerConfig, type LiveQueryResult } from "@view-server/config";
import { Schema } from "effect";
import { createViewServerReact } from "./index";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

const { ViewServerInMemoryProvider, useLiveQuery, useViewServerHealth, useViewServerTestRuntime } =
  createViewServerReact(viewServer);

describe("React type contracts", () => {
  it("preserves selected row result types", () => {
    const selected = useLiveQuery("orders", {
      fields: ["id", "price"],
      orderBy: [{ field: "price", direction: "desc" }],
      limit: 5,
    });

    expectTypeOf(selected).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly price: number;
      }>
    >();
  });

  it("preserves full row result types", () => {
    const fullRows = useLiveQuery("orders", {
      where: {
        status: { eq: "open" },
        customerId: { startsWith: "customer-" },
        price: { gte: 10 },
      },
      orderBy: [{ field: "updatedAt", direction: "asc" }],
      limit: 10,
    });

    expectTypeOf(fullRows.rows[0]).toEqualTypeOf<
      | {
          readonly id: string;
          readonly customerId: string;
          readonly status: "open" | "closed" | "cancelled";
          readonly price: number;
          readonly region: string;
          readonly updatedAt: number;
        }
      | undefined
    >();
    expectTypeOf(fullRows.status).toEqualTypeOf<
      "loading" | "ready" | "stale" | "closed" | "error"
    >();
    expectTypeOf(fullRows.statusCode).toEqualTypeOf<
      | "Ready"
      | "SnapshotStale"
      | "SubscriptionClosed"
      | "TransportError"
      | "BackpressureExceeded"
      | undefined
    >();
  });

  it("rejects invalid raw query fields", () => {
    useLiveQuery("orders", {
      where: {
        // @ts-expect-error unknown where fields are rejected.
        prcie: 10,
      },
    });

    useLiveQuery("orders", {
      orderBy: [
        {
          // @ts-expect-error unknown orderBy fields are rejected.
          field: "prcie",
          direction: "asc",
        },
      ],
    });

    useLiveQuery("orders", {
      // @ts-expect-error unknown projected fields are rejected.
      fields: ["id", "prcie"],
    });
  });

  it("rejects invalid raw query operators", () => {
    useLiveQuery("orders", {
      where: {
        status: {
          // @ts-expect-error string fields do not support range filters.
          gte: "open",
        },
      },
    });

    useLiveQuery("orders", {
      where: {
        price: {
          // @ts-expect-error numeric fields do not support string filters.
          startsWith: "10",
        },
      },
    });
  });

  it("keeps health and test runtime keyed by configured topics", () => {
    const health = useViewServerHealth();
    type Runtime = ReturnType<typeof useViewServerTestRuntime>;

    expectTypeOf(health.engine.topics.orders.rowCount).toEqualTypeOf<number>();
    expectTypeOf<Parameters<Runtime["publish"]>>().toEqualTypeOf<
      [topic: "orders", row: typeof Order.Type]
    >();
  });

  it("rejects provider seed data", () => {
    void ViewServerInMemoryProvider({
      children: null,
      // @ts-expect-error setup data must go through runtime.publish or runtime.publishMany.
      seed: {},
    });
  });

  it("rejects grouped queries for the in-memory runtime slice", () => {
    const runtime = useViewServerTestRuntime();
    const groupedQuery = {
      groupBy: ["status"],
      aggregates: [{ type: "count", as: "count" }],
    };

    const invalidGroupedSnapshot =
      // @ts-expect-error grouped queries are not part of the raw in-memory runtime slice yet.
      runtime.snapshot("orders", groupedQuery);

    const invalidPatch = runtime.patch("orders", "order-1", {
      price: 10,
      // @ts-expect-error patches cannot contain fields outside the topic schema.
      prcie: 10,
    });

    expectTypeOf(invalidGroupedSnapshot).not.toBeAny();
    expectTypeOf(invalidPatch).not.toBeAny();
  });
});
