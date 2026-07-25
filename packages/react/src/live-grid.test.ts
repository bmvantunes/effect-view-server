import { describe, expect, it } from "@effect/vitest";
import {
  isLiveGridSessionCurrent,
  liveGridChromeFromResult,
  liveGridIdleChrome,
  liveGridInvalidWindowChrome,
  liveGridOnChangeToQuery,
  liveGridQueryIdentityKey,
  liveGridWindowSchemaErrorMessage,
  projectLiveGridSink,
  projectLiveGridSinkIfPresent,
  validateLiveGridWindow,
} from "./live-grid";

type OrderRow = {
  readonly id: string;
  readonly price: number;
  readonly status: string;
};

describe("live grid helpers", () => {
  it("accepts inclusive windows and derives limit", () => {
    expect(validateLiveGridWindow(0, 9)).toStrictEqual({
      _tag: "Valid",
      firstRow: 0,
      lastRow: 9,
      limit: 10,
    });
    expect(validateLiveGridWindow(5, 5)).toStrictEqual({
      _tag: "Valid",
      firstRow: 5,
      lastRow: 5,
      limit: 1,
    });
  });

  it("rejects invalid windows", () => {
    expect(validateLiveGridWindow(10, 9)).toStrictEqual({
      _tag: "Invalid",
      message: "Live grid window lastRow must be greater than or equal to firstRow.",
    });
    expect(validateLiveGridWindow(-1, 2)._tag).toBe("Invalid");
    expect(validateLiveGridWindow(1.5, 2)._tag).toBe("Invalid");
    expect(validateLiveGridWindow(Number.NaN, 2)._tag).toBe("Invalid");
    expect(liveGridWindowSchemaErrorMessage("SchemaError(bad window)")).toBe("bad window");
    expect(liveGridWindowSchemaErrorMessage("not a schema error")).toBe("not a schema error");
  });

  it("maps raw onChange to live query offset/limit", () => {
    const mapped = liveGridOnChangeToQuery<OrderRow>({
      mode: "raw",
      firstRow: 10,
      lastRow: 19,
      select: ["id", "price"],
      where: [],
      orderBy: [{ field: "price", direction: "asc" }],
    });
    expect(mapped).toStrictEqual({
      _tag: "Query",
      firstRow: 10,
      query: {
        select: ["id", "price"],
        where: [],
        orderBy: [{ field: "price", direction: "asc" }],
        offset: 10,
        limit: 10,
      },
    });
  });

  it("maps grouped onChange to live query offset/limit", () => {
    const mapped = liveGridOnChangeToQuery<OrderRow>({
      mode: "grouped",
      firstRow: 0,
      lastRow: 4,
      groupBy: ["status"],
      aggregates: {
        count: { aggFunc: "count" },
      },
      where: [],
      orderBy: [],
    });
    expect(mapped).toStrictEqual({
      _tag: "Query",
      firstRow: 0,
      query: {
        groupBy: ["status"],
        aggregates: {
          count: { aggFunc: "count" },
        },
        where: [],
        orderBy: [],
        offset: 0,
        limit: 5,
      },
    });
  });

  it("returns invalid window mapping without building a query", () => {
    const mapped = liveGridOnChangeToQuery<OrderRow>({
      mode: "raw",
      firstRow: 5,
      lastRow: 1,
      select: ["id"],
      where: [],
      orderBy: [],
    });
    expect(mapped._tag).toBe("InvalidWindow");
  });

  it("projects absolute index maps into the sink", () => {
    const counts: Array<{ count: number; keep?: boolean }> = [];
    const dataMaps: Array<Record<number, OrderRow>> = [];
    projectLiveGridSink(
      {
        setRowCount: (count, keepRenderedRows) => {
          counts.push(
            keepRenderedRows === undefined ? { count } : { count, keep: keepRenderedRows },
          );
        },
        setRowData: (rowData) => {
          dataMaps.push(rowData);
        },
      },
      10,
      {
        totalRows: 100,
        rows: [
          { id: "a", price: 1, status: "open" },
          { id: "b", price: 2, status: "open" },
        ],
      },
    );
    expect(counts).toStrictEqual([{ count: 100, keep: true }]);
    expect(dataMaps).toStrictEqual([
      {
        10: { id: "a", price: 1, status: "open" },
        11: { id: "b", price: 2, status: "open" },
      },
    ]);
    projectLiveGridSinkIfPresent(null, 0, {
      totalRows: 1,
      rows: [{ id: "x", price: 0, status: "open" }],
      status: "ready",
      version: 1,
    });
    expect(dataMaps).toHaveLength(1);
    projectLiveGridSinkIfPresent(
      {
        setRowCount: (count) => {
          counts.push({ count });
        },
        setRowData: (rowData) => {
          dataMaps.push(rowData);
        },
      },
      0,
      {
        totalRows: 0,
        rows: [],
        status: "loading",
        version: 0,
      },
    );
    expect(dataMaps).toHaveLength(1);
    expect(counts).toStrictEqual([{ count: 100, keep: true }]);
    projectLiveGridSinkIfPresent(
      {
        setRowCount: (count) => {
          counts.push({ count });
        },
        setRowData: (rowData) => {
          dataMaps.push(rowData);
        },
      },
      0,
      {
        totalRows: 5,
        rows: [{ id: "stale", price: 1, status: "open" }],
        status: "ready",
        version: 2,
      },
      { activeSession: 2, subscriptionSession: 1 },
    );
    expect(dataMaps).toHaveLength(1);
    expect(isLiveGridSessionCurrent(1, 1)).toBe(true);
    expect(isLiveGridSessionCurrent(2, 1)).toBe(false);
  });

  it("builds query identity keys with and without a row schema", () => {
    const query = {
      select: ["id"],
      where: [],
      orderBy: [],
      offset: 0,
      limit: 10,
    } as const;
    expect(
      liveGridQueryIdentityKey(
        query,
        undefined,
        () => "no-schema",
        () => "with-schema",
      ),
    ).toBe("no-schema");
    expect(
      liveGridQueryIdentityKey(
        query,
        { ast: "schema" },
        () => "no-schema",
        () => "with-schema",
      ),
    ).toBe("with-schema");
  });

  it("builds chrome helpers without a rows list", () => {
    expect(liveGridIdleChrome()).toStrictEqual({
      totalRows: 0,
      version: 0,
      status: "loading",
    });
    expect(liveGridInvalidWindowChrome("bad window")).toStrictEqual({
      totalRows: 0,
      version: 0,
      status: "error",
      statusCode: "InvalidQuery",
      message: "bad window",
    });
    expect(
      liveGridChromeFromResult({
        rows: [{ id: "a" }],
        totalRows: 3,
        version: 7,
        status: "ready",
        statusCode: "Ready",
      }),
    ).toStrictEqual({
      totalRows: 3,
      version: 7,
      status: "ready",
      statusCode: "Ready",
      message: undefined,
    });
  });
});
