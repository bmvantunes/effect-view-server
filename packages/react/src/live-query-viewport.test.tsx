import { describe, expect, it } from "@effect/vitest";
import type {
  ViewServerLiveClient,
  ViewServerLiveEvent,
  ViewServerLiveSubscription,
} from "@effect-view-server/client";
import {
  ViewServerId,
  defineViewServerConfig,
  type ExactLiveQueryInputForTopic,
  type GroupedQuery,
  type LiveQueryRow,
  type RawQuery,
  type TopicDefinitions,
  type TopicRow,
  type ViewServerRuntimeError,
  type ViewServerTransportError,
} from "@effect-view-server/config";
import { createInMemoryViewServer } from "@effect-view-server/in-memory";
import { Deferred, Effect, Queue, Schema, Stream } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import { StrictMode, useLayoutEffect } from "react";
import { renderToString } from "react-dom/server";
import { render } from "vitest-browser-react";
import { createViewServerReact } from "./index";
import { ViewServerReactClientProvider } from "./internal";
import type {
  LiveQueryViewport,
  LiveQueryViewportGeneration,
  LiveQueryViewportSink,
} from "./live-query-viewport";

const Order = Schema.Struct({
  id: ViewServerId,
  status: Schema.Literals(["open", "closed"]),
  price: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
    },
  },
});

const react = createViewServerReact(viewServer);
const { useLiveQuery, useLiveQueryViewport } = react;
const ViewServerClientProvider = react[ViewServerReactClientProvider];

const multiTopicViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
    },
    archivedOrders: {
      schema: Order,
    },
  },
});

const multiTopicReact = createViewServerReact(multiTopicViewServer);
const MultiTopicClientProvider = multiTopicReact[ViewServerReactClientProvider];

type QuerySubstrate<Topics_ extends TopicDefinitions> = (
  topic: Extract<keyof Topics_, string>,
  query: Readonly<Record<string, unknown>>,
) => Effect.Effect<
  ViewServerLiveSubscription<object>,
  ViewServerRuntimeError | ViewServerTransportError
>;

const adaptQuerySubstrate = <Topics_ extends TopicDefinitions>(
  substrate: QuerySubstrate<Topics_>,
): ViewServerLiveClient<Topics_>["subscribe"] => {
  function subscribe<
    Topic extends Extract<keyof Topics_, string>,
    const Query extends
      | RawQuery<TopicRow<Topics_, NoInfer<Topic>>>
      | GroupedQuery<TopicRow<Topics_, NoInfer<Topic>>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics_, NoInfer<Topic>, Query>,
  ): Effect.Effect<
    ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics_, Topic>, Query>>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  function subscribe(
    topic: Extract<keyof Topics_, string>,
    query: Readonly<Record<string, unknown>>,
  ): Effect.Effect<
    ViewServerLiveSubscription<object>,
    ViewServerRuntimeError | ViewServerTransportError
  > {
    return substrate(topic, query);
  }
  return subscribe;
};

type SelectedOrder = {
  readonly id: string;
  readonly price: number;
};

const makeGridModel = <Row,>() => {
  let rowCount = 0;
  let rows: { readonly [index: number]: Row } = {};
  const sink: LiveQueryViewportSink<Row> = {
    setRowCount: (count, keepRenderedRows) => {
      rowCount = count;
      if (keepRenderedRows !== true) {
        rows = {};
      }
    },
    setRowData: (changedRows) => {
      rows = { ...rows, ...changedRows };
    },
  };
  return {
    sink,
    rowCount: () => rowCount,
    rows: () => rows,
  };
};

describe("useLiveQueryViewport", () => {
  it("renders loading chrome during SSR without starting a subscription", async () => {
    const runtime = createInMemoryViewServer(viewServer);

    function ViewportChrome() {
      const result = useLiveQueryViewport("orders");
      return <output role="status">{`${result.status}:${result.totalRows}`}</output>;
    }

    expect(
      renderToString(
        <ViewServerClientProvider client={runtime.liveClient}>
          <ViewportChrome />
        </ViewServerClientProvider>,
      ),
    ).toBe('<output role="status">loading:0</output>');
    const health = await Effect.runPromise(runtime.client.health());
    expect(health.engine.topics.orders.activeSubscriptions).toBe(0);
    await Effect.runPromise(runtime.close);
  });

  it("accepts a descendant grid connection from its first layout effect", async () => {
    const runtime = createInMemoryViewServer(viewServer);
    const requests: Array<Queue.Queue<ViewServerLiveEvent<object>>> = [];
    const client = {
      ...runtime.liveClient,
      subscribe: adaptQuerySubstrate(() =>
        Effect.gen(function* () {
          const events = yield* Queue.unbounded<ViewServerLiveEvent<object>>();
          requests.push(events);
          return {
            events: Stream.fromQueue(events),
            close: () => Effect.void,
          };
        }),
      ),
    } satisfies ViewServerLiveClient<typeof viewServer.topics>;
    const grid = makeGridModel<{ readonly id: string }>();

    function GridAdapter(props: {
      readonly viewport: LiveQueryViewport<typeof viewServer.topics, "orders">;
    }) {
      useLayoutEffect(() => {
        const generation = props.viewport.replace({
          window: { firstRow: 0, lastRow: 9 },
          query: { select: ["id"], where: [], orderBy: [] },
          sink: grid.sink,
        });
        return generation.release;
      }, [props.viewport]);
      return null;
    }

    function ViewportOwner() {
      const result = useLiveQueryViewport("orders");
      return <GridAdapter viewport={result.viewport} />;
    }

    const view = await render(
      <ViewServerClientProvider client={client}>
        <ViewportOwner />
      </ViewServerClientProvider>,
    );
    await expect.poll(() => requests.length).toBe(1);
    await Effect.runPromise(
      Queue.offer(requests[0]!, {
        type: "snapshot",
        topic: "orders",
        queryId: "layout-grid",
        rows: [{ id: "connected" }],
        keys: ["connected"],
        totalRows: 1,
        version: 1,
      }),
    );
    await expect.poll(grid.rows).toStrictEqual({ 0: { id: "connected" } });

    await view.unmount();
    await Effect.runPromise(runtime.close);
  });

  it("loads absolute rows, scrolls through setWindow, and releases on unmount", async () => {
    const runtime = createInMemoryViewServer(viewServer);
    await Effect.runPromise(
      runtime.client.publishMany(
        "orders",
        Array.from({ length: 30 }, (_, index) => ({
          id: `order-${index}`,
          status: index % 2 === 0 ? ("open" as const) : ("closed" as const),
          price: index,
        })),
      ),
    );
    const grid = makeGridModel<SelectedOrder>();
    let generation: LiveQueryViewportGeneration | undefined;

    function ViewportView() {
      const result = useLiveQueryViewport("orders");
      return (
        <>
          <output role="status">
            {result.status}:{result.totalRows}:{result.version}
          </output>
          <button
            type="button"
            onClick={() => {
              generation = result.viewport.replace({
                window: { firstRow: 10, lastRow: 14 },
                query: {
                  select: ["id", "price"],
                  where: [],
                  orderBy: [{ field: "price", direction: "asc" }],
                },
                sink: grid.sink,
              });
            }}
          >
            load viewport
          </button>
          <button
            type="button"
            onClick={() => {
              generation?.setWindow({ firstRow: 20, lastRow: 24 });
            }}
          >
            scroll viewport
          </button>
        </>
      );
    }

    const view = await render(
      <StrictMode>
        <ViewServerClientProvider client={runtime.liveClient}>
          <ViewportView />
        </ViewServerClientProvider>
      </StrictMode>,
    );
    await view.getByRole("button", { name: "load viewport" }).click();
    await expect.poll(grid.rows).toStrictEqual({
      10: { id: "order-10", price: 10 },
      11: { id: "order-11", price: 11 },
      12: { id: "order-12", price: 12 },
      13: { id: "order-13", price: 13 },
      14: { id: "order-14", price: 14 },
    });
    expect(grid.rowCount()).toBe(30);
    await expect.element(view.getByText(/^ready:30:\d+$/)).toBeVisible();

    await view.getByRole("button", { name: "scroll viewport" }).click();
    await expect.poll(grid.rows).toStrictEqual({
      20: { id: "order-20", price: 20 },
      21: { id: "order-21", price: 21 },
      22: { id: "order-22", price: 22 },
      23: { id: "order-23", price: 23 },
      24: { id: "order-24", price: 24 },
    });
    expect(grid.rowCount()).toBe(30);
    await expect
      .poll(async () => {
        const health = await Effect.runPromise(runtime.client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(1);

    await view.unmount();
    await expect
      .poll(async () => {
        const health = await Effect.runPromise(runtime.client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(0);
    expect(generation).toBeDefined();
    generation!.setWindow({ firstRow: 0, lastRow: 4 });
    await expect
      .poll(async () => {
        const health = await Effect.runPromise(runtime.client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(0);
    expect(grid.rowCount()).toBe(0);
    await Effect.runPromise(runtime.close);
  });

  it("makes public destroy terminal, resets chrome, and closes the subscription", async () => {
    const runtime = createInMemoryViewServer(viewServer);
    await Effect.runPromise(
      runtime.client.publish("orders", {
        id: "destroyed-order",
        status: "open",
        price: 1,
      }),
    );
    const grid = makeGridModel<{ readonly id: string }>();
    const replacementGrid = makeGridModel<{ readonly id: string }>();
    let replaceDuringClear = false;

    function DestroyableViewport() {
      const result = useLiveQueryViewport("orders");
      const sink: LiveQueryViewportSink<{ readonly id: string }> = {
        setRowCount: (count, keepRenderedRows) => {
          grid.sink.setRowCount(count, keepRenderedRows);
          if (replaceDuringClear) {
            replaceDuringClear = false;
            result.viewport.replace({
              window: { firstRow: 10, lastRow: 19 },
              query: { select: ["id"], where: [], orderBy: [] },
              sink: replacementGrid.sink,
            });
          }
        },
        setRowData: grid.sink.setRowData,
      };
      return (
        <>
          <output role="status">
            {result.status}:{result.totalRows}:{result.version}
          </output>
          <button
            type="button"
            onClick={() => {
              result.viewport.replace({
                window: { firstRow: 0, lastRow: 9 },
                query: { select: ["id"], where: [], orderBy: [] },
                sink,
              });
            }}
          >
            load destroyable viewport
          </button>
          <button
            type="button"
            onClick={() => {
              replaceDuringClear = true;
              result.viewport.destroy();
              result.viewport.destroy();
              result.viewport.replace({
                window: { firstRow: 20, lastRow: 29 },
                query: { select: ["id"], where: [], orderBy: [] },
                sink: replacementGrid.sink,
              });
            }}
          >
            destroy viewport
          </button>
        </>
      );
    }

    const view = await render(
      <ViewServerClientProvider client={runtime.liveClient}>
        <DestroyableViewport />
      </ViewServerClientProvider>,
    );
    await view.getByRole("button", { name: "load destroyable viewport" }).click();
    await expect.element(view.getByText(/^ready:1:\d+$/)).toBeVisible();
    await expect
      .poll(async () => {
        const health = await Effect.runPromise(runtime.client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(1);

    await view.getByRole("button", { name: "destroy viewport" }).click();
    await expect.element(view.getByText("loading:0:0", { exact: true })).toBeVisible();
    await expect
      .poll(async () => {
        const health = await Effect.runPromise(runtime.client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(0);
    expect(grid.rowCount()).toBe(0);
    expect(grid.rows()).toStrictEqual({});
    expect(replacementGrid.rowCount()).toBe(0);
    expect(replacementGrid.rows()).toStrictEqual({});

    await view.unmount();
    await Effect.runPromise(runtime.close);
  });

  it("never lets an older useLiveQuery identity replace newer filter data", async () => {
    const runtime = createInMemoryViewServer(viewServer);
    const requests: Array<Queue.Queue<ViewServerLiveEvent<object>>> = [];
    const client = {
      ...runtime.liveClient,
      subscribe: adaptQuerySubstrate(() =>
        Effect.gen(function* () {
          const events = yield* Queue.unbounded<ViewServerLiveEvent<object>>();
          requests.push(events);
          return {
            events: Stream.fromQueue(events),
            close: () => Effect.void,
          };
        }),
      ),
    } satisfies ViewServerLiveClient<typeof viewServer.topics>;
    const openQuery = {
      select: ["id"],
      where: [{ field: "status", type: "equals", filter: "open" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        {
          readonly field: "status";
          readonly type: "equals";
          readonly filter: "open";
        },
      ];
    };
    const closedQuery = {
      select: ["id"],
      where: [{ field: "status", type: "equals", filter: "closed" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        {
          readonly field: "status";
          readonly type: "equals";
          readonly filter: "closed";
        },
      ];
    };

    function QueryView(props: {
      readonly query:
        | {
            readonly select: readonly ["id"];
            readonly where: typeof openQuery.where;
          }
        | {
            readonly select: readonly ["id"];
            readonly where: typeof closedQuery.where;
          };
    }) {
      const result = useLiveQuery("orders", props.query);
      return (
        <output role="status">
          {result.status}:{result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    const view = await render(
      <ViewServerClientProvider client={client}>
        <QueryView query={openQuery} />
      </ViewServerClientProvider>,
    );
    await expect.poll(() => requests.length).toBe(1);
    await Effect.runPromise(
      Queue.offer(requests[0]!, {
        type: "snapshot",
        topic: "orders",
        queryId: "open",
        rows: [{ id: "open-1" }],
        keys: ["open-1"],
        totalRows: 1,
        version: 1,
      }),
    );
    await expect.element(view.getByText("ready:open-1", { exact: true })).toBeVisible();

    await view.rerender(
      <ViewServerClientProvider client={client}>
        <QueryView query={closedQuery} />
      </ViewServerClientProvider>,
    );
    await expect.element(view.getByText("loading:", { exact: true })).toBeVisible();
    await expect.poll(() => requests.length).toBe(2);
    await Effect.runPromise(
      Queue.offer(requests[1]!, {
        type: "snapshot",
        topic: "orders",
        queryId: "closed",
        rows: [{ id: "closed-1" }],
        keys: ["closed-1"],
        totalRows: 1,
        version: 2,
      }),
    );
    await Effect.runPromise(
      Queue.offer(requests[0]!, {
        type: "delta",
        topic: "orders",
        queryId: "open",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "update",
            key: "open-1",
            row: { id: "open-delta-late" },
            index: 0,
          },
        ],
        totalRows: 1,
      }),
    );
    await Effect.runPromise(
      Queue.offer(requests[0]!, {
        type: "status",
        topic: "orders",
        queryId: "open",
        status: "error",
        code: "InvalidQuery",
        message: "obsolete status",
      }),
    );
    await expect.element(view.getByText("ready:closed-1", { exact: true })).toBeVisible();
    await Effect.runPromise(
      Queue.offer(requests[0]!, {
        type: "snapshot",
        topic: "orders",
        queryId: "open",
        rows: [{ id: "open-late" }],
        keys: ["open-late"],
        totalRows: 1,
        version: 3,
      }),
    );
    await expect.element(view.getByText("ready:closed-1", { exact: true })).toBeVisible();

    await view.unmount();
    await Effect.runPromise(runtime.close);
  });

  it("ignores an obsolete useLiveQuery acquisition failure after its replacement is ready", async () => {
    const runtime = createInMemoryViewServer(viewServer);
    const oldFailure = await Effect.runPromise(Deferred.make<void>());
    const currentEvents = await Effect.runPromise(Queue.unbounded<ViewServerLiveEvent<object>>());
    let subscriptionAttempt = 0;
    const client = {
      ...runtime.liveClient,
      subscribe: adaptQuerySubstrate(() => {
        subscriptionAttempt += 1;
        if (subscriptionAttempt === 1) {
          return Deferred.await(oldFailure).pipe(
            Effect.andThen(
              Effect.fail({
                _tag: "ViewServerRuntimeError" as const,
                code: "InvalidQuery" as const,
                message: "obsolete acquisition failure",
              }),
            ),
          );
        }
        return Effect.succeed({
          events: Stream.fromQueue(currentEvents),
          close: () => Effect.void,
        });
      }),
    } satisfies ViewServerLiveClient<typeof viewServer.topics>;
    const openQuery = {
      select: ["id"],
      where: [{ field: "status", type: "equals", filter: "open" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        {
          readonly field: "status";
          readonly type: "equals";
          readonly filter: "open";
        },
      ];
    };
    const closedQuery = {
      select: ["id"],
      where: [{ field: "status", type: "equals", filter: "closed" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        {
          readonly field: "status";
          readonly type: "equals";
          readonly filter: "closed";
        },
      ];
    };

    function QueryView(props: { readonly query: typeof openQuery | typeof closedQuery }) {
      const result = useLiveQuery("orders", props.query);
      return (
        <output role="status">
          {result.status}:{result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    const view = await render(
      <ViewServerClientProvider client={client}>
        <QueryView query={openQuery} />
      </ViewServerClientProvider>,
    );
    await expect.poll(() => subscriptionAttempt).toBe(1);
    await view.rerender(
      <ViewServerClientProvider client={client}>
        <QueryView query={closedQuery} />
      </ViewServerClientProvider>,
    );
    await expect.element(view.getByText("loading:", { exact: true })).toBeVisible();
    await expect.poll(() => subscriptionAttempt).toBe(2);
    await Effect.runPromise(
      Queue.offer(currentEvents, {
        type: "snapshot",
        topic: "orders",
        queryId: "closed",
        rows: [{ id: "closed-current" }],
        keys: ["closed-current"],
        totalRows: 1,
        version: 1,
      }),
    );
    await expect.element(view.getByText("ready:closed-current", { exact: true })).toBeVisible();
    await Effect.runPromise(Deferred.succeed(oldFailure, undefined));
    await expect.element(view.getByText("ready:closed-current", { exact: true })).toBeVisible();

    await view.unmount();
    await Effect.runPromise(runtime.close);
  });

  it("resets viewport chrome and rows while the newest request is pending", async () => {
    const runtime = createInMemoryViewServer(viewServer);
    const requests: Array<Queue.Queue<ViewServerLiveEvent<object>>> = [];
    const client = {
      ...runtime.liveClient,
      subscribe: adaptQuerySubstrate(() =>
        Effect.gen(function* () {
          const events = yield* Queue.unbounded<ViewServerLiveEvent<object>>();
          requests.push(events);
          return {
            events: Stream.fromQueue(events),
            close: () => Effect.void,
          };
        }),
      ),
    } satisfies ViewServerLiveClient<typeof viewServer.topics>;
    const grid = makeGridModel<SelectedOrder>();

    function ViewportSwitchView() {
      const result = useLiveQueryViewport("orders");
      const replace = (status: "open" | "closed") => {
        result.viewport.replace({
          window: { firstRow: 0, lastRow: 9 },
          query: {
            select: ["id", "price"],
            where: [{ field: "status", type: "equals", filter: status }],
            orderBy: [],
          },
          sink: grid.sink,
        });
      };
      return (
        <>
          <output role="status">
            {result.status}:{result.totalRows}:{result.version}
          </output>
          <button type="button" onClick={() => replace("open")}>
            request open
          </button>
          <button type="button" onClick={() => replace("closed")}>
            request closed
          </button>
        </>
      );
    }

    const view = await render(
      <ViewServerClientProvider client={client}>
        <ViewportSwitchView />
      </ViewServerClientProvider>,
    );
    await view.getByRole("button", { name: "request open" }).click();
    await expect.poll(() => requests.length).toBe(1);
    await Effect.runPromise(
      Queue.offer(requests[0]!, {
        type: "snapshot",
        topic: "orders",
        queryId: "open",
        rows: [{ id: "open-1", price: 1 }],
        keys: ["open-1"],
        totalRows: 1,
        version: 1,
      }),
    );
    await expect.element(view.getByText("ready:1:1", { exact: true })).toBeVisible();
    await expect.poll(grid.rows).toStrictEqual({ 0: { id: "open-1", price: 1 } });

    await view.getByRole("button", { name: "request closed" }).click();
    await expect.poll(() => requests.length).toBe(2);
    await expect.element(view.getByText("loading:0:0", { exact: true })).toBeVisible();
    expect(grid.rows()).toStrictEqual({});

    await Effect.runPromise(
      Queue.offer(requests[0]!, {
        type: "status",
        topic: "orders",
        queryId: "open",
        status: "error",
        code: "InvalidQuery",
        message: "late old error",
      }),
    );
    await expect.element(view.getByText("loading:0:0", { exact: true })).toBeVisible();
    await Effect.runPromise(
      Queue.offer(requests[1]!, {
        type: "snapshot",
        topic: "orders",
        queryId: "closed",
        rows: [{ id: "closed-1", price: 2 }],
        keys: ["closed-1"],
        totalRows: 1,
        version: 2,
      }),
    );
    await expect.element(view.getByText("ready:1:2", { exact: true })).toBeVisible();
    await expect.poll(grid.rows).toStrictEqual({ 0: { id: "closed-1", price: 2 } });

    await view.unmount();
    await Effect.runPromise(runtime.close);
  });

  it("maps the current acquisition failure into owned error chrome", async () => {
    const runtime = createInMemoryViewServer(viewServer);
    const client = {
      ...runtime.liveClient,
      subscribe: adaptQuerySubstrate(() =>
        Effect.fail({
          _tag: "ViewServerRuntimeError" as const,
          code: "InvalidQuery" as const,
          message: "rejected viewport",
        }),
      ),
    } satisfies ViewServerLiveClient<typeof viewServer.topics>;

    function FailingViewport() {
      const result = useLiveQueryViewport("orders");
      return (
        <>
          <output role="status">
            {result.status}:{result.statusCode ?? "none"}:{result.totalRows}
          </output>
          <button
            type="button"
            onClick={() => {
              result.viewport.replace({
                window: { firstRow: 0, lastRow: 9 },
                query: { select: ["id"], where: [], orderBy: [] },
                sink: { setRowCount: () => undefined, setRowData: () => undefined },
              });
            }}
          >
            load failing viewport
          </button>
        </>
      );
    }

    const view = await render(
      <ViewServerClientProvider client={client}>
        <FailingViewport />
      </ViewServerClientProvider>,
    );
    await view.getByRole("button", { name: "load failing viewport" }).click();
    await expect.element(view.getByText("error:InvalidQuery:0", { exact: true })).toBeVisible();

    await view.unmount();
    await Effect.runPromise(runtime.close);
  });

  it("finalizes topic ownership before a retained generation can restart it", async () => {
    const runtime = createInMemoryViewServer(multiTopicViewServer);
    const grid = makeGridModel<{ readonly id: string }>();
    let retainedGeneration: LiveQueryViewportGeneration | undefined;
    let retainedViewport:
      | LiveQueryViewport<typeof multiTopicViewServer.topics, "orders" | "archivedOrders">
      | undefined;

    function TopicViewport(props: { readonly topic: "orders" | "archivedOrders" }) {
      const result = multiTopicReact.useLiveQueryViewport(props.topic);
      return (
        <button
          type="button"
          onClick={() => {
            retainedViewport = result.viewport;
            retainedGeneration = result.viewport.replace({
              window: { firstRow: 0, lastRow: 9 },
              query: {
                select: ["id"],
                where: [],
                orderBy: [],
              },
              sink: grid.sink,
            });
          }}
        >
          load {props.topic}
        </button>
      );
    }

    const view = await render(
      <MultiTopicClientProvider client={runtime.liveClient}>
        <TopicViewport topic="orders" />
      </MultiTopicClientProvider>,
    );
    await view.getByRole("button", { name: "load orders" }).click();
    await expect
      .poll(async () => {
        const health = await Effect.runPromise(runtime.client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(1);

    await view.rerender(
      <MultiTopicClientProvider client={runtime.liveClient}>
        <TopicViewport topic="archivedOrders" />
      </MultiTopicClientProvider>,
    );
    await expect
      .poll(async () => {
        const health = await Effect.runPromise(runtime.client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(0);
    expect(retainedViewport).toBeDefined();
    expect(retainedGeneration).toBeDefined();
    retainedViewport!.replace({
      window: { firstRow: 20, lastRow: 29 },
      query: { select: ["id"], where: [], orderBy: [] },
      sink: grid.sink,
    });
    retainedGeneration!.setWindow({ firstRow: 10, lastRow: 19 });
    await expect
      .poll(async () => {
        const health = await Effect.runPromise(runtime.client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(0);
    await expect
      .poll(async () => {
        const health = await Effect.runPromise(runtime.client.health());
        return health.engine.topics.archivedOrders.activeSubscriptions;
      })
      .toBe(0);

    await view.getByRole("button", { name: "load archivedOrders" }).click();
    await expect
      .poll(async () => {
        const health = await Effect.runPromise(runtime.client.health());
        return health.engine.topics.archivedOrders.activeSubscriptions;
      })
      .toBe(1);
    await Effect.runPromise(runtime.liveClient.close);
    await expect
      .poll(async () => {
        const health = await Effect.runPromise(runtime.client.health());
        return health.engine.topics.archivedOrders.activeSubscriptions;
      })
      .toBe(0);

    await view.unmount();
    await Effect.runPromise(runtime.close);
  });

  it("switches useLiveQuery ownership when the provider client identity changes", async () => {
    const runtime = createInMemoryViewServer(viewServer);
    const oldRequests: Array<Queue.Queue<ViewServerLiveEvent<object>>> = [];
    const currentRequests: Array<Queue.Queue<ViewServerLiveEvent<object>>> = [];
    const makeClient = (
      requests: Array<Queue.Queue<ViewServerLiveEvent<object>>>,
    ): ViewServerLiveClient<typeof viewServer.topics> => ({
      ...runtime.liveClient,
      subscribe: adaptQuerySubstrate(() =>
        Effect.gen(function* () {
          const events = yield* Queue.unbounded<ViewServerLiveEvent<object>>();
          requests.push(events);
          return {
            events: Stream.fromQueue(events),
            close: () => Effect.void,
          };
        }),
      ),
    });
    const oldClient = makeClient(oldRequests);
    const currentClient = makeClient(currentRequests);

    function ClientQuery() {
      const result = useLiveQuery("orders", { select: ["id"] });
      return (
        <output role="status">
          {result.status}:{result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    const view = await render(
      <ViewServerClientProvider client={oldClient}>
        <ClientQuery />
      </ViewServerClientProvider>,
    );
    await expect.poll(() => oldRequests.length).toBe(1);
    await Effect.runPromise(
      Queue.offer(oldRequests[0]!, {
        type: "snapshot",
        topic: "orders",
        queryId: "old-client",
        rows: [{ id: "old-client" }],
        keys: ["old-client"],
        totalRows: 1,
        version: 1,
      }),
    );
    await expect.element(view.getByText("ready:old-client", { exact: true })).toBeVisible();

    await view.rerender(
      <ViewServerClientProvider client={currentClient}>
        <ClientQuery />
      </ViewServerClientProvider>,
    );
    await expect.element(view.getByText("loading:", { exact: true })).toBeVisible();
    await expect.poll(() => currentRequests.length).toBe(1);
    await Effect.runPromise(
      Queue.offer(currentRequests[0]!, {
        type: "snapshot",
        topic: "orders",
        queryId: "current-client",
        rows: [{ id: "current-client" }],
        keys: ["current-client"],
        totalRows: 1,
        version: 2,
      }),
    );
    await Effect.runPromise(
      Queue.offer(oldRequests[0]!, {
        type: "snapshot",
        topic: "orders",
        queryId: "old-client",
        rows: [{ id: "obsolete-client-late" }],
        keys: ["obsolete-client-late"],
        totalRows: 1,
        version: 3,
      }),
    );
    await expect.element(view.getByText("ready:current-client", { exact: true })).toBeVisible();

    await view.unmount();
    await Effect.runPromise(runtime.close);
  });

  it("switches viewport ownership immediately when the provider client identity changes", async () => {
    const runtime = createInMemoryViewServer(viewServer);
    const oldRequests: Array<Queue.Queue<ViewServerLiveEvent<object>>> = [];
    const currentRequests: Array<Queue.Queue<ViewServerLiveEvent<object>>> = [];
    const makeClient = (
      requests: Array<Queue.Queue<ViewServerLiveEvent<object>>>,
    ): ViewServerLiveClient<typeof viewServer.topics> => ({
      ...runtime.liveClient,
      subscribe: adaptQuerySubstrate(() =>
        Effect.gen(function* () {
          const events = yield* Queue.unbounded<ViewServerLiveEvent<object>>();
          requests.push(events);
          return {
            events: Stream.fromQueue(events),
            close: () => Effect.void,
          };
        }),
      ),
    });
    const oldClient = makeClient(oldRequests);
    const currentClient = makeClient(currentRequests);
    const grid = makeGridModel<{ readonly id: string }>();
    let retainedViewport: LiveQueryViewport<typeof viewServer.topics, "orders"> | undefined;

    function ClientViewport() {
      const result = useLiveQueryViewport("orders");
      return (
        <>
          <output role="status">
            {result.status}:{result.totalRows}:{result.version}
          </output>
          <button
            type="button"
            onClick={() => {
              retainedViewport = result.viewport;
              result.viewport.replace({
                window: { firstRow: 0, lastRow: 9 },
                query: { select: ["id"], where: [], orderBy: [] },
                sink: grid.sink,
              });
            }}
          >
            load client viewport
          </button>
        </>
      );
    }

    const view = await render(
      <ViewServerClientProvider client={oldClient}>
        <ClientViewport />
      </ViewServerClientProvider>,
    );
    await view.getByRole("button", { name: "load client viewport" }).click();
    await expect.poll(() => oldRequests.length).toBe(1);
    await Effect.runPromise(
      Queue.offer(oldRequests[0]!, {
        type: "snapshot",
        topic: "orders",
        queryId: "old-client",
        rows: [{ id: "old-client" }],
        keys: ["old-client"],
        totalRows: 1,
        version: 1,
      }),
    );
    await expect.poll(grid.rows).toStrictEqual({ 0: { id: "old-client" } });

    await view.rerender(
      <ViewServerClientProvider client={currentClient}>
        <ClientViewport />
      </ViewServerClientProvider>,
    );
    expect(retainedViewport).toBeDefined();
    retainedViewport!.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: { select: ["id"], where: [], orderBy: [] },
      sink: grid.sink,
    });
    await expect.poll(() => currentRequests.length).toBe(1);
    expect(grid.rows()).toStrictEqual({});
    await Effect.runPromise(
      Queue.offer(currentRequests[0]!, {
        type: "snapshot",
        topic: "orders",
        queryId: "current-client",
        rows: [{ id: "current-client" }],
        keys: ["current-client"],
        totalRows: 1,
        version: 2,
      }),
    );
    await expect.poll(grid.rows).toStrictEqual({ 0: { id: "current-client" } });

    await Effect.runPromise(
      Queue.offer(oldRequests[0]!, {
        type: "snapshot",
        topic: "orders",
        queryId: "old-client",
        rows: [{ id: "obsolete-client-late" }],
        keys: ["obsolete-client-late"],
        totalRows: 1,
        version: 3,
      }),
    );
    await expect.poll(grid.rows).toStrictEqual({ 0: { id: "current-client" } });

    await view.unmount();
    await Effect.runPromise(runtime.close);
  });

  it("streams grouped rows through the same viewport sink", async () => {
    const runtime = createInMemoryViewServer(viewServer);
    await Effect.runPromise(
      runtime.client.publishMany("orders", [
        { id: "open-1", status: "open", price: 10 },
        { id: "open-2", status: "open", price: 20 },
        { id: "closed-1", status: "closed", price: 5 },
      ]),
    );
    type GroupedRow = {
      readonly status: "open" | "closed";
      readonly rowCount: bigint;
      readonly totalPrice: BigDecimal.BigDecimal;
    };
    const grid = makeGridModel<GroupedRow>();

    function GroupedViewportView() {
      const result = useLiveQueryViewport("orders");
      return (
        <button
          type="button"
          onClick={() => {
            result.viewport.replace({
              window: { firstRow: 0, lastRow: 9 },
              query: {
                groupBy: ["status"],
                aggregates: {
                  rowCount: { aggFunc: "count" },
                  totalPrice: { aggFunc: "sum", field: "price" },
                },
                where: [],
                orderBy: [{ aggregate: "rowCount", direction: "desc" }],
              },
              sink: grid.sink,
            });
          }}
        >
          load grouped viewport
        </button>
      );
    }

    const view = await render(
      <ViewServerClientProvider client={runtime.liveClient}>
        <GroupedViewportView />
      </ViewServerClientProvider>,
    );
    await view.getByRole("button", { name: "load grouped viewport" }).click();
    await expect.poll(grid.rowCount).toBe(2);
    await expect.poll(() => grid.rows()[0]?.status).toBe("open");
    expect(grid.rows()[0]?.rowCount).toBe(2n);
    expect(BigDecimal.equals(grid.rows()[0]!.totalPrice, BigDecimal.make(30n, 0))).toBe(true);

    await Effect.runPromise(
      runtime.client.publish("orders", {
        id: "open-3",
        status: "open",
        price: 10,
      }),
    );
    await expect.poll(() => grid.rows()[0]?.rowCount).toBe(3n);
    expect(BigDecimal.equals(grid.rows()[0]!.totalPrice, BigDecimal.make(40n, 0))).toBe(true);

    await view.unmount();
    await Effect.runPromise(runtime.close);
  });
});
