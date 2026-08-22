import { describe, expectTypeOf, it } from "@effect/vitest";
import type { ViewServerLiveClient } from "@effect-view-server/client";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { Schema } from "effect";
import type * as BigDecimal from "effect/BigDecimal";
import { createViewServerReact } from "./index";
import {
  makeLiveQueryViewport,
  makeLiveQueryViewportBinding,
  type LiveQueryViewport,
} from "./live-query-viewport";

const Order = Schema.Struct({
  id: ViewServerId,
  status: Schema.Literals(["open", "closed"]),
  price: Schema.Number,
  region: Schema.String,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
    },
  },
});

const Position = Schema.Struct({
  id: ViewServerId,
  quantity: Schema.Number,
});

const positionViewServer = defineViewServerConfig({
  topics: {
    positions: {
      schema: Position,
    },
  },
});

const sourceAdapter = SourceAdapter.make({
  identity: { name: "viewport-type-source" },
  failure: Schema.Never,
  materialized: undefined,
  leased: {
    metrics: Schema.Struct({ observed: Schema.BigInt }),
    rejectionLocation: Schema.Struct({ offset: Schema.BigInt }),
    definitionOptions: SourceAdapter.definitionOptions<undefined>(),
  },
});
const leasedViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: sourceAdapter.leasedSource(["region"], undefined),
    },
  },
});

const react = createViewServerReact(viewServer);
const positionReact = createViewServerReact(positionViewServer);
const leasedReact = createViewServerReact(leasedViewServer);
declare const liveClient: ViewServerLiveClient<typeof viewServer.topics>;
const directViewport = makeLiveQueryViewport({
  client: liveClient,
  config: viewServer,
  topic: "orders",
  publish: () => undefined,
});

describe("Live Query Viewport type contracts", () => {
  it("keeps one invariant viewport shape independently of query results", () => {
    const result = react.useLiveQueryViewport("orders");
    const binding = makeLiveQueryViewportBinding<typeof viewServer.topics, "orders">();
    type Viewport = typeof result.viewport;

    expectTypeOf(directViewport).toMatchTypeOf<Viewport>();
    expectTypeOf(binding.viewport).toMatchTypeOf<Viewport>();

    const rawGeneration = result.viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: { select: ["id"], where: [], orderBy: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    rawGeneration.setWindow({ firstRow: 10, lastRow: 19 });
    result.viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        where: [],
        orderBy: [{ aggregate: "rowCount", direction: "desc" }],
      },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });

    expectTypeOf<typeof result.viewport>().toEqualTypeOf<Viewport>();
  });

  it("rejects mismatched base rows", () => {
    const orderViewport = react.useLiveQueryViewport("orders").viewport;
    const positionViewport = positionReact.useLiveQueryViewport("positions").viewport;
    const requireOrderViewport = (
      _viewport: LiveQueryViewport<typeof viewServer.topics, "orders">,
    ): void => undefined;

    requireOrderViewport(orderViewport);
    // @ts-expect-error the viewport base Topic row is invariant.
    requireOrderViewport(positionViewport);

    expectTypeOf(positionViewport).not.toEqualTypeOf(orderViewport);
  });

  it("binds the configured topic and exposes chrome without rows", () => {
    const result = react.useLiveQueryViewport("orders");

    expectTypeOf(result.totalRows).toBeNumber();
    expectTypeOf(result.version).toBeNumber();
    expectTypeOf(result.status).toEqualTypeOf<"loading" | "ready" | "stale" | "closed" | "error">();
    expectTypeOf(result).not.toHaveProperty("rows");

    // @ts-expect-error viewport topics are bound to configured topic names.
    react.useLiveQueryViewport("missing");
    expectTypeOf(react).not.toHaveProperty("useLiveGrid");
  });

  it("infers raw selected rows into the sink without as const", () => {
    const result = react.useLiveQueryViewport("orders");
    const generation = result.viewport.replace({
      window: { firstRow: 20, lastRow: 39 },
      query: {
        select: ["id", "price"],
        where: [{ field: "status", type: "equals", filter: "open" }],
        orderBy: [{ field: "price", direction: "desc" }],
      },
      sink: {
        setRowCount: (count, keepRenderedRows) => {
          expectTypeOf(count).toBeNumber();
          expectTypeOf(keepRenderedRows).toEqualTypeOf<boolean | undefined>();
        },
        setRowData: (rows, rowKeys) => {
          expectTypeOf(rows[20]).toEqualTypeOf<
            { readonly id: string; readonly price: number } | undefined
          >();
          expectTypeOf(rowKeys[20]).toEqualTypeOf<string | undefined>();
        },
      },
    });

    generation.setWindow({ firstRow: 40, lastRow: 59 });
    generation.release();
  });

  it("infers grouped result rows into the sink", () => {
    react.useLiveQueryViewport("orders").viewport.replace({
      window: { firstRow: 0, lastRow: 19 },
      query: {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          totalPrice: { aggFunc: "sum", field: "price" },
        },
        where: [],
        orderBy: [{ aggregate: "totalPrice", direction: "desc" }],
      },
      sink: {
        setRowCount: () => undefined,
        setRowData: (rows, rowKeys) => {
          expectTypeOf(rows[0]).toEqualTypeOf<
            | {
                readonly status: "open" | "closed";
                readonly rowCount: bigint;
                readonly totalPrice: BigDecimal.BigDecimal;
              }
            | undefined
          >();
          expectTypeOf(rowKeys[0]).toEqualTypeOf<string | undefined>();
        },
      },
    });
  });

  it("keeps one-argument row callbacks source-compatible", () => {
    react.useLiveQueryViewport("orders").viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: { select: ["id"], where: [], orderBy: [] },
      sink: {
        setRowCount: () => undefined,
        setRowData: (rows) => {
          expectTypeOf(rows[0]).toEqualTypeOf<{ readonly id: string } | undefined>();
        },
      },
    });
  });

  it("types captured viewport replacement inputs end to end", () => {
    directViewport.replaceCaptured({
      _tag: "Success",
      request: {
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id", "price"], where: [], orderBy: [] },
        sink: {
          setRowCount: () => undefined,
          setRowData: (rows) => {
            expectTypeOf(rows[0]).toEqualTypeOf<
              { readonly id: string; readonly price: number } | undefined
            >();
          },
        },
      },
    });

    directViewport.replaceCaptured({
      _tag: "Success",
      request: {
        window: { firstRow: 0, lastRow: 9 },
        // @ts-expect-error captured query selections must name configured fields.
        query: { select: ["missing"], where: [], orderBy: [] },
        sink: { setRowCount: () => undefined, setRowData: () => undefined },
      },
    });
    directViewport.replaceCaptured({
      _tag: "Success",
      request: {
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: { setRowCount: () => undefined, setRowData: () => undefined },
        // @ts-expect-error captured requests are exact.
        extra: true,
      },
    });
    directViewport.replaceCaptured({
      _tag: "Success",
      request: {
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: {
          setRowCount: () => undefined,
          // @ts-expect-error captured sinks must accept the selected row shape.
          setRowData: (_rows: { readonly [index: number]: { readonly id: number } }) => undefined,
        },
      },
    });
    directViewport.replaceCaptured({
      _tag: "Failure",
      request: {
        window: { firstRow: 0, lastRow: 9 },
        sink: { setRowCount: () => undefined, setRowData: () => undefined },
        // @ts-expect-error captured failures cannot carry a query.
        query: { select: ["id"], where: [], orderBy: [] },
      },
      failure: new Error("captured failure"),
    });
  });

  it("preserves leased route requirements", () => {
    leasedReact.useLiveQueryViewport("orders").viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: {
        routeBy: { region: "usa" },
        select: ["id"],
        where: [],
        orderBy: [],
      },
      sink: {
        setRowCount: () => undefined,
        setRowData: (rows) => {
          expectTypeOf(rows[0]).toEqualTypeOf<{ readonly id: string } | undefined>();
        },
      },
    });

    leasedReact.useLiveQueryViewport("orders").viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error leased viewport queries require their exact route.
      query: { select: ["id"], where: [], orderBy: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    leasedReact.useLiveQueryViewport("orders").viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error leased viewport route values must satisfy the configured route schema.
      query: { routeBy: { region: 1 }, select: ["id"], where: [], orderBy: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    leasedReact.useLiveQueryViewport("orders").viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error leased viewport routes are exact.
      query: {
        routeBy: { region: "usa", missing: "extra" },
        select: ["id"],
        where: [],
        orderBy: [],
      },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
  });

  it("rejects incomplete, invalid, and mixed query shapes", () => {
    const viewport = react.useLiveQueryViewport("orders").viewport;

    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error viewport raw queries require where.
      query: { select: ["id"], orderBy: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error viewport raw queries require orderBy.
      query: { select: ["id"], where: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error callers cannot supply the derived window fields.
      query: { select: ["id"], where: [], orderBy: [], offset: 0 },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error callers cannot supply a limit because the viewport derives it.
      query: { select: ["id"], where: [], orderBy: [], limit: 10 },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error raw queries require at least one selected field.
      query: { select: [], where: [], orderBy: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error selected fields must exist.
      query: { select: ["missing"], where: [], orderBy: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error where operators must be valid for the field.
      query: {
        select: ["id"],
        where: [{ field: "price", type: "contains", filter: "1" }],
        orderBy: [],
      },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error where fields must exist.
      query: {
        select: ["id"],
        where: [{ field: "missing", type: "equals", filter: "open" }],
        orderBy: [],
      },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error raw order fields must exist.
      query: {
        select: ["id"],
        where: [],
        orderBy: [{ field: "missing", direction: "asc" }],
      },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error grouped fields must exist.
      query: {
        groupBy: ["missing"],
        aggregates: { rowCount: { aggFunc: "count" } },
        where: [],
        orderBy: [],
      },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error grouped queries require aggregates.
      query: { groupBy: ["status"], where: [], orderBy: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error grouped viewport queries require where.
      query: {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        orderBy: [],
      },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error grouped viewport queries require orderBy.
      query: {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        where: [],
      },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error grouped viewport queries cannot supply the derived offset.
      query: {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        where: [],
        orderBy: [],
        offset: 0,
      },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error grouped viewport queries cannot supply the derived limit.
      query: {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        where: [],
        orderBy: [],
        limit: 10,
      },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error grouped ordering fields must be declared group fields.
      query: {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        where: [],
        orderBy: [{ field: "price", direction: "asc" }],
      },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error aggregate fields must exist and support the aggregate.
      query: {
        groupBy: ["status"],
        aggregates: { total: { aggFunc: "sum", field: "missing" } },
        where: [],
        orderBy: [],
      },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error grouped ordering must reference a declared group or aggregate alias.
      query: {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        where: [],
        orderBy: [{ aggregate: "missing", direction: "desc" }],
      },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error query inputs reject extra keys.
      query: { select: ["id"], where: [], orderBy: [], extra: true },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    // @ts-expect-error the sink is required.
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: { select: ["id"], where: [], orderBy: [] },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: { select: ["id"], where: [], orderBy: [] },
      sink: {
        setRowCount: () => undefined,
        // @ts-expect-error sink rows cannot require unselected fields.
        setRowData: (_rows: { readonly [index: number]: { readonly id: string; price: number } }) =>
          undefined,
      },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: { select: ["id"], where: [], orderBy: [] },
      sink: {
        setRowCount: () => undefined,
        // @ts-expect-error viewport row keys are always sparse string maps.
        setRowData: (_rows, _rowKeys: { readonly [index: number]: number }) => undefined,
      },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: { select: ["id"], where: [], orderBy: [] },
      sink: {
        setRowCount: () => undefined,
        // @ts-expect-error sink rows must preserve the selected field type.
        setRowData: (_rows: { readonly [index: number]: { readonly id: number } }) => undefined,
      },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: { select: ["id"], where: [], orderBy: [] },
      // @ts-expect-error sink rows cannot omit selected fields.
      sink: {
        setRowCount: () => undefined,
        setRowData: (_rows: { readonly [index: number]: {} }) => undefined,
      },
    });
    viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      // @ts-expect-error grouped queries cannot contain select.
      query: {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        select: ["id"],
        where: [],
        orderBy: [],
      },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
  });
});
