import type {
  Aggregates,
  GroupedOrderBy,
  GroupedQuery,
  LiveQuery,
  LiveQueryResult,
  OrderBy,
  RawQuery,
  Where,
} from "@effect-view-server/config";
import type { ClientState } from "@effect-view-server/client";
import { Result, Schema } from "effect";

export type LiveGridDatasourceParams = {
  readonly setRowCount: (count: number, keepRenderedRows?: boolean) => void;
  /**
   * Absolute index → projected result rows for the active query (selected fields or
   * grouped aggregates), not necessarily full topic rows.
   */
  readonly setRowData: (rowData: { readonly [index: number]: object }) => void;
};

export type LiveGridRawOnChange<Row> = {
  readonly mode: "raw";
  readonly firstRow: number;
  readonly lastRow: number;
  /** Non-empty select; empty projections are rejected. */
  readonly select: readonly [
    RawQuery<Row>["select"][number],
    ...Array<RawQuery<Row>["select"][number]>,
  ];
  readonly where: Where<Row>;
  readonly orderBy: ReadonlyArray<OrderBy<Row>>;
};

export type LiveGridGroupedOnChange<Row> = {
  readonly mode: "grouped";
  readonly firstRow: number;
  readonly lastRow: number;
  readonly groupBy: GroupedQuery<Row>["groupBy"];
  readonly aggregates: Aggregates<Row>;
  readonly where: Where<Row>;
  readonly orderBy: ReadonlyArray<GroupedOrderBy<Row>>;
};

export type LiveGridOnChange<Row> = LiveGridRawOnChange<Row> | LiveGridGroupedOnChange<Row>;

export type LiveGridDatasource<Row> = {
  readonly init: (params: LiveGridDatasourceParams) => void;
  readonly onChange: (state: LiveGridOnChange<Row>) => void;
  readonly destroy: () => void;
};

export type UseLiveGridResult<Row> = {
  readonly datasource: LiveGridDatasource<Row>;
  readonly totalRows: number;
  readonly version: number;
  readonly status: LiveQueryResult<Row>["status"];
  readonly statusCode?: LiveQueryResult<Row>["statusCode"];
  readonly message?: string | undefined;
};

export type LiveGridWindowValidation =
  | {
      readonly _tag: "Valid";
      readonly firstRow: number;
      readonly lastRow: number;
      readonly limit: number;
    }
  | { readonly _tag: "Invalid"; readonly message: string };

/** Non-negative safe integer row index (Schema.Int uses Number.isSafeInteger). */
const LiveGridWindowIndex = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

/**
 * Inclusive absolute window. lastRow must be >= firstRow so limit is always >= 1.
 */
export const LiveGridWindowSchema = Schema.Struct({
  firstRow: LiveGridWindowIndex,
  lastRow: LiveGridWindowIndex,
}).check(
  Schema.makeFilter((window) => {
    if (window.lastRow < window.firstRow) {
      return "Live grid window lastRow must be greater than or equal to firstRow.";
    }
    // lastRow - firstRow + 1 must stay a safe integer (avoid MAX_SAFE_INTEGER + 1).
    if (window.lastRow - window.firstRow >= Number.MAX_SAFE_INTEGER) {
      return "Live grid window limit must be a safe integer.";
    }
    return undefined;
  }),
);

const decodeLiveGridWindow = Schema.decodeUnknownResult(LiveGridWindowSchema);

export const liveGridWindowSchemaErrorMessage = (error: unknown): string => {
  const text = String(error);
  const schemaErrorPrefix = "SchemaError(";
  if (text.startsWith(schemaErrorPrefix) && text.endsWith(")")) {
    return text.slice(schemaErrorPrefix.length, -1);
  }
  return text;
};

export const validateLiveGridWindow = (
  firstRow: number,
  lastRow: number,
): LiveGridWindowValidation => {
  const decoded = decodeLiveGridWindow({ firstRow, lastRow });
  if (Result.isFailure(decoded)) {
    return {
      _tag: "Invalid",
      message: liveGridWindowSchemaErrorMessage(decoded.failure),
    };
  }
  return {
    _tag: "Valid",
    firstRow: decoded.success.firstRow,
    lastRow: decoded.success.lastRow,
    limit: decoded.success.lastRow - decoded.success.firstRow + 1,
  };
};

export type LiveGridMappedQuery<Row> =
  | {
      readonly _tag: "Query";
      readonly firstRow: number;
      readonly query: LiveQuery<Row>;
    }
  | {
      readonly _tag: "InvalidWindow";
      readonly message: string;
    };

export const liveGridOnChangeToQuery = <Row>(
  state: LiveGridOnChange<Row>,
): LiveGridMappedQuery<Row> => {
  const window = validateLiveGridWindow(state.firstRow, state.lastRow);
  if (window._tag === "Invalid") {
    return { _tag: "InvalidWindow", message: window.message };
  }
  if (state.mode === "raw") {
    const query: RawQuery<Row> = {
      select: state.select,
      where: state.where,
      orderBy: state.orderBy,
      offset: window.firstRow,
      limit: window.limit,
    };
    return { _tag: "Query", firstRow: window.firstRow, query };
  }
  const query: GroupedQuery<Row> = {
    groupBy: state.groupBy,
    aggregates: state.aggregates,
    where: state.where,
    orderBy: state.orderBy,
    offset: window.firstRow,
    limit: window.limit,
  };
  return { _tag: "Query", firstRow: window.firstRow, query };
};

export const projectLiveGridSink = (
  params: LiveGridDatasourceParams,
  firstRow: number,
  state: Pick<ClientState<object>, "rows" | "totalRows">,
): void => {
  params.setRowCount(state.totalRows, true);
  const rowData: { [index: number]: object } = {};
  for (let index = 0; index < state.rows.length; index += 1) {
    rowData[firstRow + index] = state.rows[index]!;
  }
  params.setRowData(rowData);
};

export const isLiveGridSessionCurrent = (
  activeSession: number,
  subscriptionSession: number,
): boolean => activeSession === subscriptionSession;

export const projectLiveGridSinkIfPresent = (
  params: LiveGridDatasourceParams | null,
  firstRow: number,
  state: Pick<ClientState<object>, "rows" | "totalRows" | "status" | "version">,
  options?: {
    readonly activeSession?: number;
    readonly subscriptionSession?: number;
  },
): void => {
  if (params === null) {
    return;
  }
  if (
    options !== undefined &&
    options.activeSession !== undefined &&
    options.subscriptionSession !== undefined &&
    !isLiveGridSessionCurrent(options.activeSession, options.subscriptionSession)
  ) {
    return;
  }
  // Stream.scan emits the loading seed before the first live event; never push that into the sink.
  if (state.status === "loading" && state.version === 0 && state.totalRows === 0) {
    return;
  }
  projectLiveGridSink(params, firstRow, state);
};

export const liveGridChromeFromResult = <Row>(
  result: LiveQueryResult<Row>,
): Omit<UseLiveGridResult<Row>, "datasource"> => ({
  totalRows: result.totalRows,
  version: result.version,
  status: result.status,
  statusCode: result.statusCode,
  message: result.message,
});

export const liveGridInvalidWindowChrome = <Row>(
  message: string,
): Omit<UseLiveGridResult<Row>, "datasource"> => ({
  totalRows: 0,
  version: 0,
  status: "error",
  statusCode: "InvalidQuery",
  message,
});

export const liveGridIdleChrome = <Row>(): Omit<UseLiveGridResult<Row>, "datasource"> => ({
  totalRows: 0,
  version: 0,
  status: "loading",
});

export const liveGridQueryIdentityKey = <Row, Schema>(
  query: LiveQuery<Row>,
  schema: Schema | undefined,
  stableKey: (query: LiveQuery<Row>) => string,
  stableKeyForSchema: (query: LiveQuery<Row>, schema: Schema) => string,
): string => (schema === undefined ? stableKey(query) : stableKeyForSchema(query, schema));

export type LiveGridOwnedQueryResolution<Query> =
  | { readonly _tag: "Owned"; readonly query: Query }
  | { readonly _tag: "SnapshotFailed"; readonly message: string };

/** Snapshot query ownership for identity; surface snapshot failures for typed chrome. */
export const resolveLiveGridOwnedQuery = <Query>(
  query: Query,
  snapshot: (query: Query) => Query,
): LiveGridOwnedQueryResolution<Query> => {
  const captured = Result.try(() => snapshot(query));
  if (Result.isSuccess(captured)) {
    return { _tag: "Owned", query: captured.success };
  }
  return {
    _tag: "SnapshotFailed",
    message:
      captured.failure instanceof Error
        ? captured.failure.message
        : "Query input could not be snapshotted.",
  };
};

export const liveGridOwnedQueryOrFallback = <Query>(
  owned: LiveGridOwnedQueryResolution<Query>,
  fallback: Query,
): Query => (owned._tag === "Owned" ? owned.query : fallback);

export type LiveGridActivationDecision<Query> =
  | { readonly _tag: "Unchanged" }
  | {
      readonly _tag: "Activate";
      readonly query: Query;
      readonly key: string;
      readonly firstRow: number;
    };

export const decideLiveGridActivation = <Query>(input: {
  readonly query: Query;
  readonly current: { readonly key: string; readonly firstRow: number } | null;
  readonly key: string;
  readonly firstRow: number;
}): LiveGridActivationDecision<Query> => {
  if (
    input.current !== null &&
    input.current.key === input.key &&
    input.current.firstRow === input.firstRow
  ) {
    return { _tag: "Unchanged" };
  }
  return {
    _tag: "Activate",
    query: input.query,
    key: input.key,
    firstRow: input.firstRow,
  };
};
