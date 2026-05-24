import { describe, expect, it } from "@effect/vitest";
import { render, waitFor, type RenderResult } from "@testing-library/react";
import { defineViewServerConfig, type ViewServerInMemoryRuntime } from "@view-server/config";
import { Effect, Schema } from "effect";
import { createViewServerReact } from "./index";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  quantity: Schema.BigInt,
  price: Schema.Number,
  region: Schema.String,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
    trades: {
      schema: Trade,
      key: "id",
    },
  },
});

const { ViewServerInMemoryProvider, useLiveQuery, useViewServerHealth, useViewServerTestRuntime } =
  createViewServerReact(viewServer);

type Topics = typeof viewServer.topics;
type OrderRow = typeof Order.Type;

const order = (id: string, price: number): OrderRow => ({
  id,
  customerId: `customer-${id}`,
  status: "open",
  price,
  region: "usa",
  updatedAt: price,
});

const waitForText = async (view: RenderResult, testId: string, expected: string) => {
  await waitFor(() => expect(view.getByTestId(testId).textContent).toBe(expected));
};

const getRuntime = (
  runtime: ViewServerInMemoryRuntime<Topics> | undefined,
): ViewServerInMemoryRuntime<Topics> => {
  expect(runtime).toBeDefined();
  return runtime as ViewServerInMemoryRuntime<Topics>;
};

describe("createViewServerReact", () => {
  it("streams runtime-published snapshots and live deltas in browser providers", async () => {
    let runtime: ViewServerInMemoryRuntime<Topics> | undefined;

    function RuntimeCapture() {
      runtime = useViewServerTestRuntime();
      return null;
    }
    function OrdersView() {
      const result = useLiveQuery("orders", {
        orderBy: [{ field: "price", direction: "asc" }],
        fields: ["id", "price"],
        limit: 10,
      });
      return (
        <output data-testid="orders">
          {result.rows.map((row) => `${row.id}:${row.price}`).join("|")}
        </output>
      );
    }
    function HealthView() {
      const health = useViewServerHealth();
      return <output data-testid="health">{health.engine.topics.orders.rowCount}</output>;
    }

    const view = render(
      <ViewServerInMemoryProvider>
        <RuntimeCapture />
        <OrdersView />
        <HealthView />
      </ViewServerInMemoryProvider>,
    );

    Effect.runSync(getRuntime(runtime).publishMany("orders", [order("b", 20), order("a", 10)]));

    await waitForText(view, "orders", "a:10|b:20");
    await waitForText(view, "health", "2");

    Effect.runSync(getRuntime(runtime).publish("orders", order("c", 5)));

    await waitForText(view, "orders", "c:5|a:10|b:20");
    await waitForText(view, "health", "3");
    view.unmount();
  });

  it("closes live subscriptions when browser components unmount", async () => {
    let runtime: ViewServerInMemoryRuntime<Topics> | undefined;

    function RuntimeCapture() {
      runtime = useViewServerTestRuntime();
      return null;
    }
    function OrdersView() {
      const result = useLiveQuery("orders", {
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return <output data-testid="orders">{result.rows.map((row) => row.id).join("|")}</output>;
    }

    const view = render(
      <ViewServerInMemoryProvider>
        <RuntimeCapture />
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );

    Effect.runSync(getRuntime(runtime).publish("orders", order("a", 10)));
    await waitForText(view, "orders", "a");

    view.rerender(
      <ViewServerInMemoryProvider>
        <RuntimeCapture />
      </ViewServerInMemoryProvider>,
    );

    await waitFor(() => {
      const health = Effect.runSync(getRuntime(runtime).health());
      expect(health.engine.topics.orders.activeSubscriptions).toBe(0);
    });
    view.unmount();
  });

  it("applies update, move, remove, patch, snapshot, and reset paths", async () => {
    let runtime: ViewServerInMemoryRuntime<Topics> | undefined;

    function RuntimeCapture() {
      runtime = useViewServerTestRuntime();
      return null;
    }
    function OrdersView() {
      const result = useLiveQuery("orders", {
        orderBy: [{ field: "price", direction: "asc" }],
        fields: ["id", "price"],
        limit: 10,
      });
      return (
        <output data-testid="orders">
          {result.rows.map((row) => `${row.id}:${row.price}`).join("|")}
        </output>
      );
    }

    const view = render(
      <ViewServerInMemoryProvider>
        <RuntimeCapture />
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );

    Effect.runSync(getRuntime(runtime).publishMany("orders", [order("a", 10), order("b", 20)]));
    await waitForText(view, "orders", "a:10|b:20");

    Effect.runSync(getRuntime(runtime).publish("orders", order("a", 30)));
    await waitForText(view, "orders", "b:20|a:30");

    Effect.runSync(getRuntime(runtime).patch("orders", "a", { price: 5 }));
    await waitForText(view, "orders", "a:5|b:20");

    Effect.runSync(getRuntime(runtime).delete("orders", "a"));
    await waitForText(view, "orders", "b:20");

    const snapshot = Effect.runSync(
      getRuntime(runtime).snapshot("orders", {
        fields: ["id", "price"],
        limit: 10,
      }),
    );
    expect(snapshot.rows).toEqual([{ id: "b", price: 20 }]);

    Effect.runSync(getRuntime(runtime).reset());
    expect(Effect.runSync(getRuntime(runtime).health()).engine.topics.orders.rowCount).toBe(0);
    view.unmount();
  });

  it("maps runtime errors", () => {
    let runtime: ViewServerInMemoryRuntime<Topics> | undefined;

    function RuntimeCapture() {
      runtime = useViewServerTestRuntime();
      return null;
    }

    const view = render(
      <ViewServerInMemoryProvider>
        <RuntimeCapture />
      </ViewServerInMemoryProvider>,
    );

    Effect.runSync(getRuntime(runtime).publish("orders", order("a", 10)));

    const invalidTopic = Effect.runSyncExit(
      // @ts-expect-error hostile runtime callers can still send unknown topics.
      getRuntime(runtime).publish("missing", order("b", 20)),
    );
    const invalidRow = Effect.runSyncExit(
      getRuntime(runtime).publish("orders", {
        id: "bad",
        customerId: "customer-bad",
        // @ts-expect-error hostile runtime callers can still send malformed rows.
        status: "unknown",
        price: 20,
        region: "usa",
        updatedAt: 20,
      }),
    );
    const groupedSnapshot = Effect.runSyncExit(
      getRuntime(runtime).snapshot("orders", {
        // @ts-expect-error grouped queries are rejected by the raw in-memory runtime slice.
        groupBy: ["status"],
        // @ts-expect-error grouped queries are rejected by the raw in-memory runtime slice.
        aggregates: [{ type: "count", as: "count" }],
      }),
    );
    const invalidQuery = Effect.runSyncExit(
      getRuntime(runtime).snapshot("orders", {
        // @ts-expect-error hostile runtime callers can still send unknown projected fields.
        fields: ["prcie"],
      }),
    );

    expect(invalidTopic._tag).toBe("Failure");
    expect(invalidRow._tag).toBe("Failure");
    expect(groupedSnapshot._tag).toBe("Failure");
    expect(invalidQuery._tag).toBe("Failure");
    view.unmount();
  });

  it("keeps query memoization safe for bigint query values", async () => {
    let runtime: ViewServerInMemoryRuntime<Topics> | undefined;

    function RuntimeCapture() {
      runtime = useViewServerTestRuntime();
      return null;
    }
    function TradesView() {
      const result = useLiveQuery("trades", {
        where: {
          quantity: { gte: 10n },
        },
        fields: ["id", "quantity"],
        limit: 10,
      });
      return (
        <output data-testid="trades">
          {result.rows.map((row) => `${row.id}:${row.quantity}`).join("|")}
        </output>
      );
    }

    const view = render(
      <ViewServerInMemoryProvider>
        <RuntimeCapture />
        <TradesView />
      </ViewServerInMemoryProvider>,
    );

    Effect.runSync(
      getRuntime(runtime).publishMany("trades", [
        { id: "a", symbol: "AAPL", quantity: 5n, price: 100, region: "usa" },
        { id: "b", symbol: "MSFT", quantity: 10n, price: 200, region: "usa" },
      ]),
    );

    await waitForText(view, "trades", "b:10");
    view.unmount();
  });

  it("surfaces runtime unavailable after provider disposal", async () => {
    let runtime: ViewServerInMemoryRuntime<Topics> | undefined;

    function RuntimeCapture() {
      runtime = useViewServerTestRuntime();
      return null;
    }

    const view = render(
      <ViewServerInMemoryProvider>
        <RuntimeCapture />
      </ViewServerInMemoryProvider>,
    );

    view.unmount();
    await waitFor(() => {
      const closedRuntime = Effect.runSyncExit(
        getRuntime(runtime).publish("orders", order("a", 10)),
      );
      expect(closedRuntime._tag).toBe("Failure");
    });
  });

  it("surfaces status events from bounded subscription queues", async () => {
    let runtime: ViewServerInMemoryRuntime<Topics> | undefined;

    function RuntimeCapture() {
      runtime = useViewServerTestRuntime();
      return null;
    }
    function OrdersView() {
      const result = useLiveQuery("orders", {
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output data-testid="orders">
          {result.status}:{result.statusCode}
        </output>
      );
    }

    const view = render(
      <ViewServerInMemoryProvider subscriptionQueueCapacity={1}>
        <RuntimeCapture />
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );

    Effect.runSync(getRuntime(runtime).publish("orders", order("a", 10)));
    await waitForText(view, "orders", "ready:Ready");

    for (let index = 0; index < 50; index += 1) {
      Effect.runSync(getRuntime(runtime).publish("orders", order(`burst-${index}`, index)));
    }

    expect(Effect.runSync(getRuntime(runtime).health()).transport.backpressureEvents).toBe(1);
    await waitForText(view, "orders", "closed:BackpressureExceeded");
    view.unmount();
  });
});
