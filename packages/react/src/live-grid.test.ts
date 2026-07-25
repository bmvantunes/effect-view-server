import { describe, expect, it } from "@effect/vitest";
import {
  isLiveGridSessionCurrent,
  liveGridChromeFromResult,
  liveGridIdleChrome,
  liveGridInvalidWindowChrome,
  liveGridOnChangeToQuery,
  liveGridOnScrollRequiresActiveQueryMessage,
  liveGridQueryIdentityKey,
  liveGridScrollQuery,
  liveGridWindowSchemaErrorMessage,
  ownLiveGridOnChangeForPending,
  projectLiveGridSink,
  projectLiveGridSinkIfPresent,
  decideLiveGridActivation,
  liveGridOwnedQueryOrFallback,
  resolveLiveGridOwnedQuery,
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
    expect(validateLiveGridWindow(0, Number.MAX_SAFE_INTEGER)).toStrictEqual({
      _tag: "Invalid",
      message: "Live grid window limit must be a safe integer.",
    });
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

  it("re-windows an active raw or grouped query for onScroll", () => {
    const raw = liveGridScrollQuery<OrderRow>(
      {
        select: ["id", "price"],
        where: [],
        orderBy: [{ field: "price", direction: "asc" }],
        offset: 0,
        limit: 10,
      },
      20,
      29,
    );
    expect(raw).toStrictEqual({
      _tag: "Query",
      firstRow: 20,
      query: {
        select: ["id", "price"],
        where: [],
        orderBy: [{ field: "price", direction: "asc" }],
        offset: 20,
        limit: 10,
      },
    });
    const grouped = liveGridScrollQuery<OrderRow>(
      {
        groupBy: ["status"],
        aggregates: { count: { aggFunc: "count" } },
        where: [],
        orderBy: [],
        offset: 0,
        limit: 5,
      },
      5,
      9,
    );
    expect(grouped).toStrictEqual({
      _tag: "Query",
      firstRow: 5,
      query: {
        groupBy: ["status"],
        aggregates: { count: { aggFunc: "count" } },
        where: [],
        orderBy: [],
        offset: 5,
        limit: 5,
      },
    });
    expect(liveGridScrollQuery<OrderRow>({ select: ["id"] }, 3, 1)._tag).toBe("InvalidWindow");
    expect(
      liveGridScrollQuery<OrderRow>(
        {
          select: ["id"],
          offset: 0,
          limit: 1,
        },
        2,
        2,
      ),
    ).toStrictEqual({
      _tag: "Query",
      firstRow: 2,
      query: {
        select: ["id"],
        offset: 2,
        limit: 1,
      },
    });
    expect(
      liveGridScrollQuery<OrderRow>(
        {
          groupBy: ["status"],
          aggregates: { count: { aggFunc: "count" } },
          offset: 0,
          limit: 2,
        },
        4,
        5,
      ),
    ).toStrictEqual({
      _tag: "Query",
      firstRow: 4,
      query: {
        groupBy: ["status"],
        aggregates: { count: { aggFunc: "count" } },
        offset: 4,
        limit: 2,
      },
    });
    expect(liveGridOnScrollRequiresActiveQueryMessage).toBe(
      "Live grid onScroll requires an active query from onChange.",
    );

    // High pagination windows (deep scroll) must map inclusive bounds to offset/limit.
    expect(
      liveGridScrollQuery<OrderRow>(
        {
          select: ["id", "price"],
          where: [],
          orderBy: [{ field: "price", direction: "asc" }],
          offset: 0,
          limit: 30,
        },
        450,
        480,
      ),
    ).toStrictEqual({
      _tag: "Query",
      firstRow: 450,
      query: {
        select: ["id", "price"],
        where: [],
        orderBy: [{ field: "price", direction: "asc" }],
        offset: 450,
        limit: 31,
      },
    });
    expect(
      liveGridOnChangeToQuery<OrderRow>({
        mode: "raw",
        firstRow: 900,
        lastRow: 929,
        select: ["id"],
        where: [],
        orderBy: [{ field: "price", direction: "desc" }],
      }),
    ).toStrictEqual({
      _tag: "Query",
      firstRow: 900,
      query: {
        select: ["id"],
        where: [],
        orderBy: [{ field: "price", direction: "desc" }],
        offset: 900,
        limit: 30,
      },
    });
    expect(validateLiveGridWindow(450, 480)).toStrictEqual({
      _tag: "Valid",
      firstRow: 450,
      lastRow: 480,
      limit: 31,
    });
    expect(validateLiveGridWindow(10_000, 10_049)).toStrictEqual({
      _tag: "Valid",
      firstRow: 10_000,
      lastRow: 10_049,
      limit: 50,
    });
  });

  it("projects absolute index maps into the sink", () => {
    const counts: Array<{ count: number; keep?: boolean }> = [];
    const dataMaps: Array<Record<number, object>> = [];
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
      450,
      {
        totalRows: 500,
        rows: [
          { id: "row-450", price: 450, status: "open" },
          { id: "row-451", price: 451, status: "open" },
          { id: "row-480", price: 480, status: "open" },
        ],
      },
    );
    expect(counts.at(-1)).toStrictEqual({ count: 500, keep: true });
    expect(dataMaps.at(-1)).toStrictEqual({
      450: { id: "row-450", price: 450, status: "open" },
      451: { id: "row-451", price: 451, status: "open" },
      452: { id: "row-480", price: 480, status: "open" },
    });
    const mapsAfterDeep = dataMaps.length;
    const countsAfterDeep = counts.length;
    projectLiveGridSinkIfPresent(null, 0, {
      totalRows: 1,
      rows: [{ id: "x", price: 0, status: "open" }],
      status: "ready",
      version: 1,
    });
    expect(dataMaps.length).toBe(mapsAfterDeep);
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
    expect(dataMaps.length).toBe(mapsAfterDeep);
    expect(counts.length).toBe(countsAfterDeep);
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
      {
        getActiveSession: () => 2,
        subscriptionSession: 1,
      },
    );
    expect(dataMaps.length).toBe(mapsAfterDeep);
    expect(isLiveGridSessionCurrent(1, 1)).toBe(true);
    expect(isLiveGridSessionCurrent(2, 1)).toBe(false);

    // setRowCount re-entrancy that bumps the session must not push setRowData.
    let session = 1;
    const reentrantData: Array<object> = [];
    projectLiveGridSink(
      {
        setRowCount: () => {
          session = 2;
        },
        setRowData: (rowData) => {
          reentrantData.push(rowData);
        },
      },
      0,
      {
        totalRows: 1,
        rows: [{ id: "stale" }],
      },
      () => session === 1,
    );
    expect(reentrantData).toStrictEqual([]);
  });

  it("surfaces snapshot ownership failures", () => {
    const query = {
      select: ["id"],
      where: [],
      orderBy: [],
      offset: 0,
      limit: 1,
    } as const;
    expect(resolveLiveGridOwnedQuery(query, (value) => value)).toStrictEqual({
      _tag: "Owned",
      query,
    });
    const failed = resolveLiveGridOwnedQuery(query, () => {
      throw new TypeError("Query input could not be snapshotted.");
    });
    expect(failed).toStrictEqual({
      _tag: "SnapshotFailed",
      message: "Query input could not be snapshotted.",
    });
    expect(liveGridOwnedQueryOrFallback(failed, query)).toBe(query);
    expect(liveGridOwnedQueryOrFallback({ _tag: "Owned", query }, query)).toBe(query);
    const pendingChange = {
      mode: "raw" as const,
      firstRow: 0,
      lastRow: 4,
      select: ["id"] as const,
      where: [] as const,
      orderBy: [] as const,
    };
    const snappedPending = {
      ...pendingChange,
      firstRow: 3,
    };
    expect(ownLiveGridOnChangeForPending(pendingChange, () => snappedPending)).toStrictEqual({
      _tag: "Owned",
      state: snappedPending,
    });
    expect(
      ownLiveGridOnChangeForPending(pendingChange, () => {
        throw new TypeError("cannot snapshot");
      }),
    ).toStrictEqual({
      _tag: "SnapshotFailed",
      message: "cannot snapshot",
    });
    expect(
      resolveLiveGridOwnedQuery(query, () => {
        throw "not-an-error";
      }),
    ).toStrictEqual({
      _tag: "SnapshotFailed",
      message: "Query input could not be snapshotted.",
    });
    expect(
      decideLiveGridActivation({
        query,
        current: { key: "k", firstRow: 0 },
        key: "k",
        firstRow: 0,
      }),
    ).toStrictEqual({ _tag: "Unchanged" });
    expect(
      decideLiveGridActivation({
        query,
        current: null,
        key: "k",
        firstRow: 2,
      }),
    ).toStrictEqual({
      _tag: "Activate",
      query,
      key: "k",
      firstRow: 2,
    });
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
