import { describe, expectTypeOf, it } from "@effect/vitest";
import { defineViewServerConfig, grpc, type GrpcRuntimeClients } from "@effect-view-server/config";
import type { Stream } from "effect";
import { Schema } from "effect";
import type * as BigDecimal from "effect/BigDecimal";
import { createViewServerReact } from "./index";

const Order = Schema.Struct({
  id: Schema.String,
  status: Schema.Literals(["open", "closed"]),
  price: Schema.Number,
  region: Schema.String,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

declare const grpcRuntimeClients: GrpcRuntimeClients;
declare const grpcRuntimeStream: Stream.Stream<unknown, unknown, never>;

const leasedSources = grpc.topicSources(grpcRuntimeClients);
const leasedViewServer = defineViewServerConfig({
  grpc: { clients: grpcRuntimeClients },
  topics: {
    orders: leasedSources.leased({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      routeBy: ["region"],
      request: (route) => route,
      acquire: () => grpcRuntimeStream,
      map: ({ route }) => ({
        id: "order-1",
        status: "open",
        price: 1,
        region: route.region,
      }),
    }),
  },
});

const react = createViewServerReact(viewServer);
const leasedReact = createViewServerReact(leasedViewServer);

describe("Live Query Viewport type contracts", () => {
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
        setRowData: (rows) => {
          expectTypeOf(rows[20]).toEqualTypeOf<
            { readonly id: string; readonly price: number } | undefined
          >();
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
        setRowData: (rows) => {
          expectTypeOf(rows[0]).toEqualTypeOf<
            | {
                readonly status: "open" | "closed";
                readonly rowCount: bigint;
                readonly totalPrice: BigDecimal.BigDecimal;
              }
            | undefined
          >();
        },
      },
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
