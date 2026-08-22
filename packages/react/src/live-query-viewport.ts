import type {
  ExactLiveQueryInputForTopic,
  GroupedOrderBy,
  GroupedQuery,
  LiveQueryResult,
  LiveQueryRow,
  OrderBy,
  RawQuery,
  TopicDefinitions,
  TopicRow,
  ViewServerRuntimeError,
  ViewServerTransportError,
  Where,
} from "@effect-view-server/config";
import {
  liveQueryFailureResult,
  makeIncrementalClientState,
  stableQueryKeyForRowSchema,
  type ClientState,
  type ClientStateChange,
  type ViewServerLiveClient,
} from "@effect-view-server/client";
import {
  ignoreLoggedTypedFailuresPreserveNonTypedFailures,
  snapshotViewServerQuery,
} from "@effect-view-server/effect-utils";
import { Effect, Result, Schema, Stream } from "effect";
import type * as Cause from "effect/Cause";
import * as Atom from "effect/unstable/reactivity/Atom";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";

type IsAny<Value> = 0 extends 1 & Value ? true : false;

type IsUnknown<Value> = IsAny<Value> extends true ? false : unknown extends Value ? true : false;

type LiveQueryViewportWitnessRow<Topics, Topic> =
  IsAny<Topics> extends true
    ? never
    : IsUnknown<Topics> extends true
      ? never
      : IsAny<Topic> extends true
        ? never
        : IsUnknown<Topic> extends true
          ? never
          : Topic extends keyof Topics
            ? TopicRow<Topics, Topic>
            : never;

export type LiveQueryViewportWindow = {
  readonly firstRow: number;
  readonly lastRow: number;
};

export type LiveQueryViewportSink<Row> = {
  readonly setRowCount: (count: number, keepRenderedRows?: boolean) => void;
  readonly setRowData: (
    rowsByIndex: { readonly [index: number]: Row },
    rowKeysByIndex: { readonly [index: number]: string },
  ) => void;
};

type LiveQueryViewportSinkRow<Row, Sink> = Sink extends {
  readonly setRowData: (...args: infer Args) => void;
}
  ? Args extends readonly []
    ? Row
    : Args[0] extends { readonly [index: number]: infer SinkRow }
      ? SinkRow
      : never
  : never;

type ExactLiveQueryViewportSink<Row, Sink> = [Row] extends [LiveQueryViewportSinkRow<Row, Sink>]
  ? [LiveQueryViewportSinkRow<Row, Sink>] extends [Row]
    ? unknown
    : never
  : never;

export type LiveQueryViewportRawQuery<Row> = RawQuery<Row> & {
  readonly where: Where<Row>;
  readonly orderBy: ReadonlyArray<OrderBy<Row>>;
  readonly offset?: never;
  readonly limit?: never;
};

export type LiveQueryViewportGroupedQuery<Row> = GroupedQuery<Row> & {
  readonly where: Where<Row>;
  readonly orderBy: ReadonlyArray<GroupedOrderBy<Row>>;
  readonly offset?: never;
  readonly limit?: never;
};

export type LiveQueryViewportQuery<Row> =
  | LiveQueryViewportRawQuery<Row>
  | LiveQueryViewportGroupedQuery<Row>;

export type LiveQueryViewportGeneration = {
  readonly setWindow: (window: LiveQueryViewportWindow) => void;
  readonly release: () => void;
};

type LiveQueryViewportRequest<
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Query extends LiveQueryViewportQuery<TopicRow<Topics, Topic>>,
  Sink extends LiveQueryViewportSink<LiveQueryRow<TopicRow<Topics, Topic>, NoInfer<Query>>>,
> = {
  readonly window: LiveQueryViewportWindow;
  readonly query: ExactLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>;
  readonly sink: Sink &
    ExactLiveQueryViewportSink<
      LiveQueryRow<TopicRow<Topics, Topic>, NoInfer<Query>>,
      NoInfer<Sink>
    >;
};

type LiveQueryViewportCapturedInput<
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Query extends LiveQueryViewportQuery<TopicRow<Topics, Topic>>,
  Sink extends LiveQueryViewportSink<LiveQueryRow<TopicRow<Topics, Topic>, NoInfer<Query>>>,
> =
  | {
      readonly _tag: "Success";
      readonly request: LiveQueryViewportRequest<Topics, Topic, Query, Sink>;
    }
  | {
      readonly _tag: "Failure";
      readonly request: {
        readonly window: LiveQueryViewportWindow;
        readonly sink: Sink &
          ExactLiveQueryViewportSink<
            LiveQueryRow<TopicRow<Topics, Topic>, NoInfer<Query>>,
            NoInfer<Sink>
          >;
      };
      readonly failure: unknown;
    };

export type LiveQueryViewportCapturedReplace<
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
> = <
  const Query extends LiveQueryViewportQuery<TopicRow<Topics, NoInfer<Topic>>>,
  const Sink extends LiveQueryViewportSink<LiveQueryRow<TopicRow<Topics, Topic>, NoInfer<Query>>>,
>(
  input: LiveQueryViewportCapturedInput<Topics, Topic, Query, Sink>,
) => LiveQueryViewportGeneration;

export type LiveQueryViewport<
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
> = {
  readonly "__effect-view-server/LiveQueryViewportBaseRow@v1"?: (
    _row: LiveQueryViewportWitnessRow<Topics, Topic>,
  ) => LiveQueryViewportWitnessRow<Topics, Topic>;
  readonly replace: <
    const Query extends LiveQueryViewportQuery<TopicRow<Topics, NoInfer<Topic>>>,
    const Sink extends LiveQueryViewportSink<LiveQueryRow<TopicRow<Topics, Topic>, NoInfer<Query>>>,
  >(
    request: LiveQueryViewportRequest<Topics, Topic, Query, Sink>,
  ) => LiveQueryViewportGeneration;
  readonly destroy: () => void;
};

export type UseLiveQueryViewportResult<
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
> = {
  readonly viewport: LiveQueryViewport<Topics, Topic>;
  readonly totalRows: number;
  readonly version: number;
  readonly status: LiveQueryResult<never>["status"];
  readonly statusCode?: LiveQueryResult<never>["statusCode"];
  readonly message?: string | undefined;
};

export type UseLiveQueryViewportHook<Topics extends TopicDefinitions> = <
  Topic extends Extract<keyof Topics, string>,
>(
  topic: Topic,
) => UseLiveQueryViewportResult<Topics, Topic>;

export type LiveQueryViewportChrome = Omit<LiveQueryResult<never>, "rows">;

type LiveQueryViewportCommand = {
  readonly owner: number;
  readonly stream: Stream.Stream<
    LiveQueryViewportChrome,
    ViewServerRuntimeError | ViewServerTransportError
  >;
};

type OwnedLiveQueryViewportChrome = {
  readonly owner: number;
  readonly chrome: LiveQueryViewportChrome;
};

const idleChrome = (): LiveQueryViewportChrome => ({
  totalRows: 0,
  version: 0,
  status: "loading",
});

const invalidWindowChrome = (message: string): LiveQueryViewportChrome => ({
  totalRows: 0,
  version: 0,
  status: "error",
  statusCode: "InvalidQuery",
  message,
});

const invalidQueryChrome = (message: string): LiveQueryViewportChrome => ({
  totalRows: 0,
  version: 0,
  status: "error",
  statusCode: "InvalidQuery",
  message,
});

export const liveQueryViewportFailureMessage = (failure: unknown): string => {
  if (
    typeof failure === "object" &&
    failure !== null &&
    "message" in failure &&
    typeof failure.message === "string"
  ) {
    return failure.message;
  }
  return String(failure);
};

const holdChrome = (chrome: LiveQueryViewportChrome): Stream.Stream<LiveQueryViewportChrome> =>
  Stream.concat(Stream.succeed(chrome), Stream.never);

export const liveQueryViewportChromeFromAsyncResult = (
  result: AsyncResult.AsyncResult<OwnedLiveQueryViewportChrome, unknown>,
  expectedOwner: number,
): LiveQueryViewportChrome => {
  if (AsyncResult.isSuccess(result) && result.value.owner === expectedOwner) {
    return result.value.chrome;
  }
  return idleChrome();
};

const failureChrome = (cause: Cause.Cause<unknown>) => {
  const failure = liveQueryFailureResult<never>(cause);
  return {
    totalRows: failure.totalRows,
    version: failure.version,
    status: failure.status,
    statusCode: failure.statusCode,
    message: failure.message,
  } satisfies LiveQueryViewportChrome;
};

export const makeLiveQueryViewportAtom = () => {
  let expectedOwner = 0;
  const atom = Atom.fn(
    (command: LiveQueryViewportCommand): Stream.Stream<OwnedLiveQueryViewportChrome> =>
      Stream.concat(
        Stream.succeed({
          owner: command.owner,
          chrome: idleChrome(),
        }),
        command.stream.pipe(
          Stream.map((chrome) => ({ owner: command.owner, chrome })),
          Stream.catchCause((cause) =>
            Stream.succeed({ owner: command.owner, chrome: failureChrome(cause) }),
          ),
        ),
      ),
    {
      initialValue: {
        owner: expectedOwner,
        chrome: idleChrome(),
      },
    },
  );
  return {
    atom,
    prepare: (command: LiveQueryViewportCommand): LiveQueryViewportCommand => {
      expectedOwner = command.owner;
      return command;
    },
    read: (
      result: AsyncResult.AsyncResult<OwnedLiveQueryViewportChrome, unknown>,
    ): LiveQueryViewportChrome => liveQueryViewportChromeFromAsyncResult(result, expectedOwner),
  };
};

export type LiveQueryViewportWindowValidation =
  | {
      readonly _tag: "Valid";
      readonly firstRow: number;
      readonly lastRow: number;
      readonly limit: number;
    }
  | {
      readonly _tag: "Invalid";
      readonly message: string;
    };

const LiveQueryViewportIndex = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

const LiveQueryViewportWindowSchema = Schema.Struct({
  firstRow: LiveQueryViewportIndex,
  lastRow: LiveQueryViewportIndex,
}).check(
  Schema.makeFilter((window) => {
    if (window.lastRow < window.firstRow) {
      return "Live Query Viewport lastRow must be greater than or equal to firstRow.";
    }
    if (window.lastRow - window.firstRow >= Number.MAX_SAFE_INTEGER) {
      return "Live Query Viewport limit must be a safe integer.";
    }
    return undefined;
  }),
);

const decodeLiveQueryViewportWindow = Schema.decodeUnknownResult(LiveQueryViewportWindowSchema);

export const validateLiveQueryViewportWindow = (
  window: LiveQueryViewportWindow,
): LiveQueryViewportWindowValidation => {
  const decoded = decodeLiveQueryViewportWindow(window);
  if (Result.isFailure(decoded)) {
    return {
      _tag: "Invalid",
      message: decoded.failure.message,
    };
  }
  return {
    _tag: "Valid",
    firstRow: decoded.success.firstRow,
    lastRow: decoded.success.lastRow,
    limit: decoded.success.lastRow - decoded.success.firstRow + 1,
  };
};

type WindowedQuery<Query> = Query & {
  readonly offset: number;
  readonly limit: number;
};

const queryWithWindow = <Row, Query extends LiveQueryViewportQuery<Row>>(
  query: Query,
  window: Extract<LiveQueryViewportWindowValidation, { readonly _tag: "Valid" }>,
): WindowedQuery<Query> => ({
  ...query,
  offset: window.firstRow,
  limit: window.limit,
});

const chromeFromClientState = <Row>(state: ClientState<Row>): LiveQueryViewportChrome => ({
  totalRows: state.totalRows,
  version: state.version,
  status: state.status,
  statusCode: state.statusCode,
  message: state.message,
});

type LiveQueryViewportData<Row> = {
  readonly rowsByIndex: { readonly [index: number]: Row };
  readonly rowKeysByIndex: { readonly [index: number]: string };
};

const viewportDataForRange = <Row>(
  firstRow: number,
  rows: ReadonlyArray<Row>,
  rowKeys: ReadonlyArray<string>,
  range: Exclude<ClientStateChange, { readonly _tag: "None" }>,
): LiveQueryViewportData<Row> => {
  const rowsByIndex: { [index: number]: Row } = {};
  const rowKeysByIndex: { [index: number]: string } = {};
  const start = range._tag === "All" ? 0 : range.start;
  const end = range._tag === "All" ? rows.length - 1 : range.end;
  for (let index = start; index <= end; index += 1) {
    const absoluteIndex = firstRow + index;
    rowsByIndex[absoluteIndex] = rows[index]!;
    rowKeysByIndex[absoluteIndex] = rowKeys[index]!;
  }
  return { rowsByIndex, rowKeysByIndex };
};

type LiveQueryViewportControllerInput<
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
> = {
  readonly client: ViewServerLiveClient<Topics>;
  readonly config: { readonly topics: Topics };
  readonly topic: Topic;
  readonly publish: (command: LiveQueryViewportCommand) => void;
};

type LiveQueryViewportBindingEntry<
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
> = {
  readonly viewport: LiveQueryViewport<Topics, Topic>;
  readonly replaceCaptured: LiveQueryViewportCapturedReplace<Topics, Topic>;
  readonly deactivate: () => void;
};

type LiveQueryViewportBindingOptions = {
  readonly deferDeactivation?: boolean;
};

type LiveQueryViewportBindingGeneration<
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
> = {
  readonly generation: LiveQueryViewportGeneration;
  readonly queueInstall: (entry: LiveQueryViewportBindingEntry<Topics, Topic>) => void;
  readonly cancelInstall: (entry: LiveQueryViewportBindingEntry<Topics, Topic>) => void;
  readonly flushInstall: () => void;
  readonly supersede: () => void;
};

const inactiveLiveQueryViewportGeneration: LiveQueryViewportGeneration = {
  setWindow: () => undefined,
  release: () => undefined,
};

const capturedInputFrom = <
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  const Query extends LiveQueryViewportQuery<TopicRow<Topics, Topic>>,
  const Sink extends LiveQueryViewportSink<LiveQueryRow<TopicRow<Topics, Topic>, NoInfer<Query>>>,
>(
  captured: Result.Result<ExactLiveQueryInputForTopic<Topics, Topic, Query>, unknown>,
  window: LiveQueryViewportWindow,
  sink: Sink &
    ExactLiveQueryViewportSink<
      LiveQueryRow<TopicRow<Topics, Topic>, NoInfer<Query>>,
      NoInfer<Sink>
    >,
): LiveQueryViewportCapturedInput<Topics, Topic, Query, Sink> =>
  Result.isSuccess(captured)
    ? { _tag: "Success", request: { window, query: captured.success, sink } }
    : { _tag: "Failure", request: { window, sink }, failure: captured.failure };

export type LiveQueryViewportBinding<
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
> = {
  readonly viewport: LiveQueryViewport<Topics, Topic>;
  readonly install: (entry: LiveQueryViewportBindingEntry<Topics, Topic>) => void;
  readonly uninstall: (entry: LiveQueryViewportBindingEntry<Topics, Topic>) => void;
  readonly flush: () => void;
};

export const makeLiveQueryViewportBinding = <
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  options: LiveQueryViewportBindingOptions = {},
): LiveQueryViewportBinding<Topics, Topic> => {
  let current: LiveQueryViewportBindingEntry<Topics, Topic> | undefined;
  let activeGeneration: LiveQueryViewportBindingGeneration<Topics, Topic> | undefined;
  let replaceInvocation = 0;
  let pendingDeactivations = new Set<LiveQueryViewportBindingEntry<Topics, Topic>>();
  let terminal = false;
  const deactivate = (entry: LiveQueryViewportBindingEntry<Topics, Topic>): void => {
    // React's insertion effect cannot synchronously notify a consumer sink.
    if (options.deferDeactivation === true) {
      pendingDeactivations.add(entry);
      return;
    }
    entry.deactivate();
  };
  const replace = <
    const Query extends LiveQueryViewportQuery<TopicRow<Topics, NoInfer<Topic>>>,
    const Sink extends LiveQueryViewportSink<LiveQueryRow<TopicRow<Topics, Topic>, NoInfer<Query>>>,
  >(
    request: LiveQueryViewportRequest<Topics, Topic, Query, Sink>,
  ): LiveQueryViewportGeneration => {
    const invocation = ++replaceInvocation;
    const entry = current;
    if (terminal || entry === undefined) {
      return inactiveLiveQueryViewportGeneration;
    }
    const ownsReplacement = (): boolean =>
      invocation === replaceInvocation && !terminal && current === entry;
    const capturedQuery = Result.try(() => snapshotViewServerQuery(request.query));
    if (!ownsReplacement()) {
      return inactiveLiveQueryViewportGeneration;
    }
    const sink = request.sink;
    let window = { ...request.window };
    if (!ownsReplacement()) {
      return inactiveLiveQueryViewportGeneration;
    }
    activeGeneration?.supersede();
    let installedEntry = entry;
    const install = (nextEntry: LiveQueryViewportBindingEntry<Topics, Topic>) =>
      nextEntry.replaceCaptured<Query, Sink>(
        capturedInputFrom<Topics, Topic, Query, Sink>(capturedQuery, window, sink),
      );
    let installedGeneration = install(entry);
    if (!ownsReplacement()) {
      return inactiveLiveQueryViewportGeneration;
    }
    let pendingEntry: LiveQueryViewportBindingEntry<Topics, Topic> | undefined;
    let active = true;
    const bindingGeneration: LiveQueryViewportBindingGeneration<Topics, Topic> = {
      generation: {
        setWindow: (nextWindow) => {
          if (!active) {
            return;
          }
          window = { ...nextWindow };
          installedGeneration.setWindow(window);
        },
        release: () => {
          if (!active) {
            return;
          }
          active = false;
          pendingEntry = undefined;
          activeGeneration = undefined;
          installedGeneration.release();
        },
      },
      queueInstall: (nextEntry) => {
        pendingEntry = installedEntry === nextEntry ? undefined : nextEntry;
      },
      cancelInstall: (removedEntry) => {
        if (pendingEntry === removedEntry) {
          pendingEntry = undefined;
        }
      },
      flushInstall: () => {
        if (pendingEntry === undefined) {
          return;
        }
        const nextEntry = pendingEntry;
        pendingEntry = undefined;
        const nextGeneration = install(nextEntry);
        if (!active) {
          nextGeneration.release();
          return;
        }
        installedEntry = nextEntry;
        installedGeneration = nextGeneration;
      },
      supersede: () => {
        active = false;
        pendingEntry = undefined;
      },
    };
    activeGeneration = bindingGeneration;
    return bindingGeneration.generation;
  };
  const viewport: LiveQueryViewport<Topics, Topic> = {
    replace,
    destroy: () => {
      if (terminal) {
        return;
      }
      terminal = true;
      replaceInvocation += 1;
      activeGeneration?.supersede();
      activeGeneration = undefined;
      const previous = current;
      current = undefined;
      previous?.viewport.destroy();
    },
  };
  return {
    viewport,
    install: (entry) => {
      if (terminal) {
        deactivate(entry);
        return;
      }
      pendingDeactivations.delete(entry);
      if (current === entry) {
        return;
      }
      const previous = current;
      current = entry;
      activeGeneration?.queueInstall(entry);
      if (previous !== undefined) {
        deactivate(previous);
      }
    },
    uninstall: (entry) => {
      if (current !== entry) {
        return;
      }
      current = undefined;
      activeGeneration?.cancelInstall(entry);
      deactivate(entry);
    },
    flush: () => {
      activeGeneration?.flushInstall();
      const deactivations = pendingDeactivations;
      pendingDeactivations = new Set();
      for (const entry of deactivations) {
        entry.deactivate();
      }
    },
  };
};

type ActiveRequest = {
  owner: number;
  readonly request: number;
  readonly criteriaKey: string;
  readonly firstRow: number;
  readonly lastRow: number;
  readonly latestTotalRows: { value: number };
  readonly sink: unknown;
  readonly cleanup: SinkCleanup;
  live: boolean;
};

type SinkCleanup = {
  readonly sink: unknown;
  readonly discard: () => void;
  readonly run: () => void;
};

const makeSinkCleanup = (sink: unknown, clear: () => void): SinkCleanup => {
  let pending = true;
  return {
    sink,
    discard: () => {
      pending = false;
    },
    run: () => {
      if (!pending) {
        return;
      }
      pending = false;
      clear();
    },
  };
};

const ignoreSubscriptionCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring React Live Query Viewport subscription close failure.",
);

export const makeLiveQueryViewport = <
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  input: LiveQueryViewportControllerInput<Topics, Topic>,
): LiveQueryViewport<Topics, Topic> & {
  readonly replaceCaptured: LiveQueryViewportCapturedReplace<Topics, Topic>;
  readonly deactivate: () => void;
} => {
  type Row = TopicRow<Topics, Topic>;
  let ownerCounter = 0;
  let requestCounter = 0;
  let replaceInvocation = 0;
  let mutationEpoch = 0;
  let active: ActiveRequest | undefined;
  let pendingCleanups: Array<SinkCleanup> = [];
  let latestPendingCleanupBySink = new Map<unknown, SinkCleanup>();
  let terminal = false;

  const isCurrent = (request: number): boolean => active?.request === request;

  const runSinkCleanups = (cleanups: ReadonlyArray<SinkCleanup>): void => {
    let firstFailure: unknown;
    let failed = false;
    for (const cleanup of cleanups) {
      const result = Result.try(() => {
        if (active !== undefined && Object.is(active.sink, cleanup.sink)) {
          cleanup.discard();
        } else {
          cleanup.run();
        }
      });
      if (!failed && Result.isFailure(result)) {
        failed = true;
        firstFailure = result.failure;
      }
    }
    if (failed) {
      throw firstFailure;
    }
  };

  const cleanupRequest = (request: ActiveRequest): void => {
    const cleanups = pendingCleanups;
    pendingCleanups = [];
    latestPendingCleanupBySink.get(request.sink)?.discard();
    latestPendingCleanupBySink = new Map();
    cleanups.push(request.cleanup);
    runSinkCleanups(cleanups);
  };

  const publishIdle = (owner: number): void => {
    input.publish({ owner, stream: Stream.succeed(idleChrome()) });
  };

  const clearSink = <SinkRow>(
    sink: LiveQueryViewportSink<SinkRow>,
    totalRows: number,
    request: number,
  ): boolean => {
    if (!isCurrent(request)) {
      return false;
    }
    sink.setRowCount(totalRows, false);
    return isCurrent(request);
  };

  const installActive = <SinkRow>(input_: {
    readonly owner: number;
    readonly criteriaKey: string;
    readonly window: LiveQueryViewportWindow;
    readonly latestTotalRows: { value: number };
    readonly sink: LiveQueryViewportSink<SinkRow>;
    readonly live: boolean;
  }): { readonly request: number; readonly activate: () => boolean } => {
    const request = ++requestCounter;
    const previous = active;
    if (previous !== undefined) {
      latestPendingCleanupBySink.get(previous.sink)?.discard();
      pendingCleanups.push(previous.cleanup);
      latestPendingCleanupBySink.set(previous.sink, previous.cleanup);
    }
    const current: ActiveRequest = {
      owner: input_.owner,
      request,
      criteriaKey: input_.criteriaKey,
      firstRow: input_.window.firstRow,
      lastRow: input_.window.lastRow,
      latestTotalRows: input_.latestTotalRows,
      sink: input_.sink,
      cleanup: makeSinkCleanup(input_.sink, () => input_.sink.setRowCount(0, false)),
      live: input_.live,
    };
    active = current;
    return {
      request,
      activate: () => {
        let activated = false;
        const predecessors = pendingCleanups;
        pendingCleanups = [];
        latestPendingCleanupBySink = new Map();
        const activation = Result.try(() => {
          runSinkCleanups(predecessors);
          if (!clearSink(input_.sink, input_.latestTotalRows.value, request)) {
            return false;
          }
          activated = true;
          return true;
        });
        if (activated) {
          return true;
        }
        if (isCurrent(request)) {
          active = undefined;
          mutationEpoch += 1;
        }
        const cleanup = Result.try(() => runSinkCleanups([current.cleanup]));
        if (Result.isFailure(activation)) {
          throw activation.failure;
        }
        if (Result.isFailure(cleanup)) {
          throw cleanup.failure;
        }
        return false;
      },
    };
  };

  const makeSubscriptionStream = <const Query extends LiveQueryViewportQuery<Row>>(
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
    sink: LiveQueryViewportSink<LiveQueryRow<Row, Query>>,
    window: Extract<LiveQueryViewportWindowValidation, { readonly _tag: "Valid" }>,
    request: number,
    updateTotalRows: (totalRows: number) => void,
  ): Stream.Stream<LiveQueryViewportChrome, ViewServerRuntimeError | ViewServerTransportError> => {
    const windowedQuery = queryWithWindow<Row, Query>(query, window);
    const projection = makeIncrementalClientState<LiveQueryRow<Row, Query>>();
    const acquireSubscription = Effect.fn("ViewServerReact.LiveQueryViewport.acquireSubscription")(
      function* () {
        const subscription = yield* input.client.subscribe<Topic, WindowedQuery<Query>>(
          input.topic,
          windowedQuery,
        );
        return subscription.events.pipe(
          Stream.filter(() => isCurrent(request)),
          Stream.map((event) => {
            const state = projection.apply(event);
            return {
              current: state.current,
              viewportData:
                state.change._tag === "None"
                  ? undefined
                  : viewportDataForRange(
                      window.firstRow,
                      state.current.rows,
                      state.current.keys,
                      state.change,
                    ),
            };
          }),
          Stream.tap((state) =>
            Effect.sync(() => {
              if (!isCurrent(request)) {
                return;
              }
              updateTotalRows(state.current.totalRows);
              if (state.current.status === "closed") {
                active!.live = false;
                sink.setRowCount(0, false);
                return;
              }
              sink.setRowCount(state.current.totalRows, true);
              if (state.viewportData === undefined) {
                return;
              }
              if (!isCurrent(request)) {
                return;
              }
              sink.setRowData(state.viewportData.rowsByIndex, state.viewportData.rowKeysByIndex);
            }),
          ),
          Stream.filter(() => isCurrent(request)),
          Stream.map((state) => chromeFromClientState(state.current)),
          Stream.ensuring(subscription.close().pipe(ignoreSubscriptionCloseFailure)),
        );
      },
    );
    return Stream.scoped(Stream.unwrap(acquireSubscription())).pipe(
      Stream.catchCause((cause) => (isCurrent(request) ? Stream.failCause(cause) : Stream.empty)),
      Stream.ensuring(
        Effect.sync(() => {
          const current = active;
          if (current !== undefined && current.request === request) {
            active = undefined;
            mutationEpoch += 1;
            cleanupRequest(current);
          }
        }),
      ),
    );
  };

  const start = <const Query extends LiveQueryViewportQuery<Row>>(input_: {
    readonly owner: number;
    readonly criteriaKey: string;
    readonly query: ExactLiveQueryInputForTopic<Topics, Topic, Query>;
    readonly sink: LiveQueryViewportSink<LiveQueryRow<Row, Query>>;
    readonly window: LiveQueryViewportWindowValidation;
    readonly latestTotalRows: { value: number };
  }): void => {
    const window = input_.window;
    const installedWindow =
      window._tag === "Valid"
        ? { firstRow: window.firstRow, lastRow: window.lastRow }
        : { firstRow: 0, lastRow: 0 };
    const installed = installActive({
      owner: input_.owner,
      criteriaKey: input_.criteriaKey,
      window: installedWindow,
      latestTotalRows: input_.latestTotalRows,
      sink: input_.sink,
      live: window._tag === "Valid",
    });
    if (window._tag === "Invalid") {
      input.publish({
        owner: installed.request,
        stream: Stream.unwrap(
          Effect.sync(() =>
            installed.activate() ? holdChrome(invalidWindowChrome(window.message)) : Stream.empty,
          ),
        ),
      });
      return;
    }
    input.publish({
      owner: installed.request,
      stream: Stream.unwrap(
        Effect.sync(() =>
          installed.activate()
            ? makeSubscriptionStream(
                input_.query,
                input_.sink,
                window,
                installed.request,
                (totalRows) => {
                  input_.latestTotalRows.value = totalRows;
                },
              )
            : Stream.empty,
        ),
      ),
    });
  };

  const installCaptured = <
    const Query extends LiveQueryViewportQuery<Row>,
    const Sink extends LiveQueryViewportSink<LiveQueryRow<Row, NoInfer<Query>>>,
  >(
    input_: LiveQueryViewportCapturedInput<Topics, Topic, Query, Sink>,
    invocation: number,
    epoch: number,
  ): LiveQueryViewportGeneration => {
    if (invocation !== replaceInvocation || epoch !== mutationEpoch || terminal) {
      return inactiveLiveQueryViewportGeneration;
    }
    if (input_._tag === "Failure") {
      const { request } = input_;
      const owner = ++ownerCounter;
      const installed = installActive({
        owner,
        criteriaKey: "",
        window: { firstRow: 0, lastRow: 0 },
        latestTotalRows: { value: 0 },
        sink: request.sink,
        live: false,
      });
      input.publish({
        owner: installed.request,
        stream: Stream.unwrap(
          Effect.sync(() =>
            installed.activate()
              ? holdChrome(invalidQueryChrome(liveQueryViewportFailureMessage(input_.failure)))
              : Stream.empty,
          ),
        ),
      });
      return {
        setWindow: () => undefined,
        release: () => {
          if (active?.owner === owner) {
            mutationEpoch += 1;
            const current = active;
            active = undefined;
            try {
              publishIdle(++requestCounter);
            } finally {
              cleanupRequest(current);
            }
          }
        },
      };
    }

    const { request } = input_;
    const query = request.query;
    const topicDefinition = input.config.topics[input.topic]!;
    const criteriaKey = stableQueryKeyForRowSchema(query, topicDefinition.schema);
    const window = validateLiveQueryViewportWindow(request.window);
    if (epoch !== mutationEpoch || terminal) {
      return inactiveLiveQueryViewportGeneration;
    }
    const current = active;
    if (
      current !== undefined &&
      current.live &&
      window._tag === "Valid" &&
      current.criteriaKey === criteriaKey &&
      current.firstRow === window.firstRow &&
      current.lastRow === window.lastRow &&
      Object.is(current.sink, request.sink)
    ) {
      const owner = ++ownerCounter;
      current.owner = owner;
      return makeGeneration(owner, criteriaKey, query, request.sink, current.latestTotalRows);
    }

    const owner = ++ownerCounter;
    const latestTotalRows = { value: 0 };
    start({
      owner,
      criteriaKey,
      query,
      sink: request.sink,
      window,
      latestTotalRows,
    });
    return makeGeneration(owner, criteriaKey, query, request.sink, latestTotalRows);
  };

  const replaceCaptured: LiveQueryViewportCapturedReplace<Topics, Topic> = (capturedInput) => {
    if (terminal) {
      return inactiveLiveQueryViewportGeneration;
    }
    const invocation = ++replaceInvocation;
    const epoch = ++mutationEpoch;
    return installCaptured(capturedInput, invocation, epoch);
  };

  const replace = <
    const Query extends LiveQueryViewportQuery<TopicRow<Topics, NoInfer<Topic>>>,
    const Sink extends LiveQueryViewportSink<LiveQueryRow<TopicRow<Topics, Topic>, NoInfer<Query>>>,
  >(
    request: LiveQueryViewportRequest<Topics, Topic, Query, Sink>,
  ): LiveQueryViewportGeneration => {
    if (terminal) {
      return inactiveLiveQueryViewportGeneration;
    }
    const invocation = ++replaceInvocation;
    const epoch = ++mutationEpoch;
    const captured = Result.try(() => snapshotViewServerQuery(request.query));
    return installCaptured<Query, Sink>(
      capturedInputFrom<Topics, Topic, Query, Sink>(captured, request.window, request.sink),
      invocation,
      epoch,
    );
  };

  function makeGeneration<const Query extends LiveQueryViewportQuery<Row>>(
    owner: number,
    criteriaKey: string,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
    sink: LiveQueryViewportSink<LiveQueryRow<Row, Query>>,
    latestTotalRows: { value: number },
  ): LiveQueryViewportGeneration {
    return {
      setWindow: (window) => {
        if (active?.owner !== owner) {
          return;
        }
        const epoch = ++mutationEpoch;
        const request = active.request;
        const validatedWindow = validateLiveQueryViewportWindow(window);
        if (
          epoch !== mutationEpoch ||
          active?.owner !== owner ||
          active.request !== request ||
          terminal
        ) {
          return;
        }
        if (
          active.live &&
          validatedWindow._tag === "Valid" &&
          active.firstRow === validatedWindow.firstRow &&
          active.lastRow === validatedWindow.lastRow
        ) {
          return;
        }
        start({
          owner,
          criteriaKey,
          query,
          sink,
          window: validatedWindow,
          latestTotalRows,
        });
      },
      release: () => {
        if (active?.owner !== owner) {
          return;
        }
        mutationEpoch += 1;
        const request = ++requestCounter;
        const current = active;
        active = undefined;
        try {
          publishIdle(request);
        } finally {
          cleanupRequest(current);
        }
      },
    };
  }

  const deactivate = (): void => {
    terminal = true;
    replaceInvocation += 1;
    mutationEpoch += 1;
    requestCounter += 1;
    const current = active;
    active = undefined;
    if (current !== undefined) {
      cleanupRequest(current);
    }
  };

  return {
    replace,
    replaceCaptured,
    destroy: () => {
      if (terminal) {
        return;
      }
      terminal = true;
      replaceInvocation += 1;
      mutationEpoch += 1;
      const request = ++requestCounter;
      const current = active;
      active = undefined;
      try {
        publishIdle(request);
      } finally {
        if (current !== undefined) {
          cleanupRequest(current);
        }
      }
    },
    deactivate,
  };
};
