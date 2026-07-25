import type {
  Aggregates,
  ExactLiveQueryInputForTopic,
  GroupedOrderBy,
  GroupedQuery,
  LiveQuery,
  LiveQueryResult,
  OrderBy,
  RawQuery,
  TopicDefinitions,
  TopicRow,
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
  /** Non-empty select; empty projections are rejected by exact validation. */
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

/**
 * Full onChange payload shape used as the const-generic inference source.
 * Window (`firstRow`/`lastRow`) and `mode` are grid chrome; the query body is
 * exact-validated the same way as `useLiveQuery` (after stripping those keys).
 */
export type LiveGridOnChangeStateCandidate<Row> = LiveGridOnChange<Row>;

/** Query body only — never pass mode/window through ExactLiveQuery (extra keys → never). */
export type LiveGridQueryBodyFromOnChangeState<State> = Omit<
  State,
  "mode" | "firstRow" | "lastRow"
>;

/**
 * Exact onChange input. Structured like ExactLiveQueryInputForTopic (`State & NoInfer<…>`):
 * `State` is the inference source; exact query validation runs on the query body only
 * (mode/window stripped so RejectExtraKeys does not collapse valid calls to never).
 * Mode must match the body (`raw` without groupBy, `grouped` with groupBy).
 */
export type ExactLiveGridOnChangeInputForTopic<
  Topics extends TopicDefinitions,
  Topic extends keyof Topics,
  State,
> = State &
  NoInfer<
    {
      readonly mode: LiveGridQueryBodyFromOnChangeState<State> extends {
        readonly groupBy: unknown;
      }
        ? "grouped"
        : "raw";
    } & ExactLiveQueryInputForTopic<Topics, Topic, LiveGridQueryBodyFromOnChangeState<State>>
  >;

/**
 * Runtime boundary from exact onChange input to the structural LiveGridOnChange shape.
 * ExactLiveGridOnChangeInputForTopic is State & refinements with State extends LiveGridOnChange.
 */
export const liveGridOnChangeFromExact = <
  Topics extends TopicDefinitions,
  Topic extends keyof Topics,
  State extends LiveGridOnChange<TopicRow<Topics, Topic>>,
>(
  state: ExactLiveGridOnChangeInputForTopic<Topics, Topic, State>,
): LiveGridOnChange<TopicRow<Topics, Topic>> => state;

export type LiveGridDatasourceForTopic<
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
> = {
  readonly init: (params: LiveGridDatasourceParams) => void;
  /**
   * Full-state query + window replace. Every field is required (e.g. clear filters with
   * `where: []`, clear sort with `orderBy: []`). Prefer this when select/where/orderBy/group
   * change; include the new window (usually top of grid after filter/sort reset).
   */
  readonly onChange: <
    const State extends LiveGridOnChangeStateCandidate<TopicRow<Topics, NoInfer<Topic>>>,
  >(
    state: ExactLiveGridOnChangeInputForTopic<Topics, NoInfer<Topic>, State>,
  ) => void;
  /**
   * Window-only update for the active query. Prefer this for scroll/viewport moves: it keeps
   * the query plan identity (select/where/orderBy/groupBy) and only moves offset/limit, which
   * is the path reserved for server page-cache / seek optimizations. Do not use this for
   * filter/sort/column changes — call `onChange` with a full state (including the new window).
   */
  readonly onScroll: (firstRow: number, lastRow: number) => void;
  readonly destroy: () => void;
};

export type UseLiveGridResultForTopic<
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
> = {
  readonly datasource: LiveGridDatasourceForTopic<Topics, Topic>;
  readonly totalRows: number;
  readonly version: number;
  readonly status: LiveQueryResult<object>["status"];
  readonly statusCode?: LiveQueryResult<object>["statusCode"];
  readonly message?: string | undefined;
};

/** Chrome fields returned beside the datasource (no public rows list). */
export type LiveGridChrome = {
  readonly totalRows: number;
  readonly version: number;
  readonly status: LiveQueryResult<object>["status"];
  readonly statusCode?: LiveQueryResult<object>["statusCode"];
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

/**
 * Re-window an owned live query for `onScroll`. Keeps plan fields; only offset/limit move.
 */
export const liveGridScrollQuery = <Row>(
  query: LiveQuery<Row>,
  firstRow: number,
  lastRow: number,
): LiveGridMappedQuery<Row> => {
  const window = validateLiveGridWindow(firstRow, lastRow);
  if (window._tag === "Invalid") {
    return { _tag: "InvalidWindow", message: window.message };
  }
  if ("groupBy" in query) {
    const scrolled: GroupedQuery<Row> = {
      groupBy: query.groupBy,
      aggregates: query.aggregates,
      ...(query.where === undefined ? {} : { where: query.where }),
      ...(query.orderBy === undefined ? {} : { orderBy: query.orderBy }),
      offset: window.firstRow,
      limit: window.limit,
    };
    return { _tag: "Query", firstRow: window.firstRow, query: scrolled };
  }
  const scrolled: RawQuery<Row> = {
    select: query.select,
    ...(query.where === undefined ? {} : { where: query.where }),
    ...(query.orderBy === undefined ? {} : { orderBy: query.orderBy }),
    offset: window.firstRow,
    limit: window.limit,
  };
  return { _tag: "Query", firstRow: window.firstRow, query: scrolled };
};

export const liveGridOnScrollRequiresActiveQueryMessage =
  "Live grid onScroll requires an active query from onChange.";

export const projectLiveGridSink = (
  params: LiveGridDatasourceParams,
  firstRow: number,
  state: Pick<ClientState<object>, "rows" | "totalRows">,
  isSessionCurrent?: () => boolean,
): void => {
  params.setRowCount(state.totalRows, true);
  // setRowCount may re-enter datasource.onChange/destroy and bump the session.
  if (isSessionCurrent !== undefined && !isSessionCurrent()) {
    return;
  }
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
    /** Re-read on each check — must not capture a stale session snapshot. */
    readonly getActiveSession?: () => number;
    readonly subscriptionSession?: number;
  },
): void => {
  if (params === null) {
    return;
  }
  const isSessionCurrent = (): boolean => {
    if (
      options === undefined ||
      options.getActiveSession === undefined ||
      options.subscriptionSession === undefined
    ) {
      return true;
    }
    return isLiveGridSessionCurrent(options.getActiveSession(), options.subscriptionSession);
  };
  if (!isSessionCurrent()) {
    return;
  }
  // Stream.scan emits the loading seed before the first live event; never push that into the sink.
  if (state.status === "loading" && state.version === 0 && state.totalRows === 0) {
    return;
  }
  projectLiveGridSink(params, firstRow, state, isSessionCurrent);
};

export const liveGridChromeFromResult = <Row>(result: LiveQueryResult<Row>): LiveGridChrome => ({
  totalRows: result.totalRows,
  version: result.version,
  status: result.status,
  statusCode: result.statusCode,
  message: result.message,
});

export const liveGridInvalidWindowChrome = (message: string): LiveGridChrome => ({
  totalRows: 0,
  version: 0,
  status: "error",
  statusCode: "InvalidQuery",
  message,
});

export const liveGridIdleChrome = (): LiveGridChrome => ({
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

export type OwnLiveGridOnChangeForPendingResult<Row> =
  | { readonly _tag: "Owned"; readonly state: LiveGridOnChange<Row> }
  | { readonly _tag: "SnapshotFailed"; readonly message: string };

/**
 * Own a pre-init onChange payload so caller mutation cannot alter the buffered state.
 * Snapshot failure is surfaced as typed invalid-query chrome rather than buffering the
 * caller-owned reference.
 */
export const ownLiveGridOnChangeForPending = <Row>(
  state: LiveGridOnChange<Row>,
  snapshot: (state: LiveGridOnChange<Row>) => LiveGridOnChange<Row>,
): OwnLiveGridOnChangeForPendingResult<Row> => {
  const owned = resolveLiveGridOwnedQuery(state, snapshot);
  if (owned._tag === "Owned") {
    return { _tag: "Owned", state: owned.query };
  }
  return { _tag: "SnapshotFailed", message: owned.message };
};

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
