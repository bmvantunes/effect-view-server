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

export type LiveQueryViewportWindow = {
  readonly firstRow: number;
  readonly lastRow: number;
};

export type LiveQueryViewportSink<Row> = {
  readonly setRowCount: (count: number, keepRenderedRows?: boolean) => void;
  readonly setRowData: (rows: { readonly [index: number]: Row }) => void;
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

export type LiveQueryViewport<
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
> = {
  readonly replace: <
    const Query extends LiveQueryViewportQuery<TopicRow<Topics, NoInfer<Topic>>>,
    const Sink extends LiveQueryViewportSink<LiveQueryRow<TopicRow<Topics, Topic>, NoInfer<Query>>>,
  >(request: {
    readonly window: LiveQueryViewportWindow;
    readonly query: ExactLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>;
    readonly sink: Sink &
      ExactLiveQueryViewportSink<
        LiveQueryRow<TopicRow<Topics, Topic>, NoInfer<Query>>,
        NoInfer<Sink>
      >;
  }) => LiveQueryViewportGeneration;
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
  readonly stream: Stream.Stream<LiveQueryViewportChrome, unknown>;
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

const rowDataForRange = <Row>(
  firstRow: number,
  rows: ReadonlyArray<Row>,
  range: Exclude<ClientStateChange, { readonly _tag: "None" }>,
): { readonly [index: number]: Row } => {
  const rowData: { [index: number]: Row } = {};
  const start = range._tag === "All" ? 0 : range.start;
  const end = range._tag === "All" ? rows.length - 1 : range.end;
  for (let index = start; index <= end; index += 1) {
    rowData[firstRow + index] = rows[index]!;
  }
  return rowData;
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
  readonly deactivate: () => void;
};

export type LiveQueryViewportBinding<
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
> = {
  readonly viewport: LiveQueryViewport<Topics, Topic>;
  readonly install: (entry: LiveQueryViewportBindingEntry<Topics, Topic>) => void;
  readonly uninstall: (entry: LiveQueryViewportBindingEntry<Topics, Topic>) => void;
};

export const makeLiveQueryViewportBinding = <
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(): LiveQueryViewportBinding<Topics, Topic> => {
  let current: LiveQueryViewportBindingEntry<Topics, Topic> | undefined;
  let terminal = false;
  const viewport: LiveQueryViewport<Topics, Topic> = {
    replace: (request) =>
      terminal
        ? {
            setWindow: () => undefined,
            release: () => undefined,
          }
        : (current?.viewport.replace(request) ?? {
            setWindow: () => undefined,
            release: () => undefined,
          }),
    destroy: () => {
      if (terminal) {
        return;
      }
      terminal = true;
      const previous = current;
      current = undefined;
      previous?.viewport.destroy();
    },
  };
  return {
    viewport,
    install: (entry) => {
      if (terminal) {
        entry.deactivate();
        return;
      }
      if (current === entry) {
        return;
      }
      current?.deactivate();
      current = entry;
    },
    uninstall: (entry) => {
      if (current !== entry) {
        return;
      }
      current = undefined;
      entry.deactivate();
    },
  };
};

type ActiveRequest = {
  readonly generation: number;
  readonly request: number;
  readonly criteriaKey: string;
  readonly firstRow: number;
  readonly lastRow: number;
  readonly latestTotalRows: { value: number };
  readonly sink: unknown;
  readonly clearSink: () => void;
  live: boolean;
};

const ignoreSubscriptionCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring React Live Query Viewport subscription close failure.",
);

export const makeLiveQueryViewport = <
  Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  input: LiveQueryViewportControllerInput<Topics, Topic>,
): LiveQueryViewport<Topics, Topic> & { readonly deactivate: () => void } => {
  type Row = TopicRow<Topics, Topic>;
  let generationCounter = 0;
  let requestCounter = 0;
  let replaceInvocation = 0;
  let active: ActiveRequest | undefined;
  let terminal = false;

  const isCurrent = (generation: number, request: number): boolean =>
    active?.generation === generation && active.request === request;

  const publishIdle = (owner: number): void => {
    input.publish({ owner, stream: Stream.succeed(idleChrome()) });
  };

  const clearSink = <SinkRow>(
    sink: LiveQueryViewportSink<SinkRow>,
    totalRows: number,
    generation: number,
    request: number,
  ): boolean => {
    sink.setRowCount(totalRows, false);
    return isCurrent(generation, request);
  };

  const makeSubscriptionStream = <const Query extends LiveQueryViewportQuery<Row>>(
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
    sink: LiveQueryViewportSink<LiveQueryRow<Row, Query>>,
    window: Extract<LiveQueryViewportWindowValidation, { readonly _tag: "Valid" }>,
    generation: number,
    request: number,
    updateTotalRows: (totalRows: number) => void,
  ): Stream.Stream<LiveQueryViewportChrome, unknown> => {
    const windowedQuery = queryWithWindow<Row, Query>(query, window);
    const projection = makeIncrementalClientState<LiveQueryRow<Row, Query>>();
    const acquireSubscription = Effect.fn("ViewServerReact.LiveQueryViewport.acquireSubscription")(
      function* () {
        const subscription = yield* input.client.subscribe<Topic, WindowedQuery<Query>>(
          input.topic,
          windowedQuery,
        );
        return subscription.events.pipe(
          Stream.filter(() => isCurrent(generation, request)),
          Stream.map((event) => {
            const state = projection.apply(event);
            return {
              current: state.current,
              rowData:
                state.change._tag === "None"
                  ? undefined
                  : rowDataForRange(window.firstRow, state.current.rows, state.change),
            };
          }),
          Stream.tap((state) =>
            Effect.sync(() => {
              if (!isCurrent(generation, request)) {
                return;
              }
              updateTotalRows(state.current.totalRows);
              if (state.current.status === "closed" || state.current.status === "error") {
                active!.live = false;
                sink.setRowCount(0, false);
                return;
              }
              sink.setRowCount(state.current.totalRows, true);
              if (state.rowData === undefined) {
                return;
              }
              if (!isCurrent(generation, request)) {
                return;
              }
              sink.setRowData(state.rowData);
            }),
          ),
          Stream.filter(() => isCurrent(generation, request)),
          Stream.map((state) => chromeFromClientState(state.current)),
          Stream.ensuring(subscription.close().pipe(ignoreSubscriptionCloseFailure)),
        );
      },
    );
    return Stream.scoped(Stream.unwrap(acquireSubscription())).pipe(
      Stream.catchCause((cause) =>
        isCurrent(generation, request) ? Stream.failCause(cause) : Stream.empty,
      ),
      Stream.ensuring(
        Effect.sync(() => {
          const current = active;
          if (
            current !== undefined &&
            current.generation === generation &&
            current.request === request
          ) {
            active = undefined;
            current.clearSink();
          }
        }),
      ),
    );
  };

  const start = <const Query extends LiveQueryViewportQuery<Row>>(input_: {
    readonly generation: number;
    readonly criteriaKey: string;
    readonly query: ExactLiveQueryInputForTopic<Topics, Topic, Query>;
    readonly sink: LiveQueryViewportSink<LiveQueryRow<Row, Query>>;
    readonly window: LiveQueryViewportWindow;
    readonly latestTotalRows: { value: number };
  }): void => {
    const request = ++requestCounter;
    const window = validateLiveQueryViewportWindow(input_.window);
    const previous = active;
    const nextActive: ActiveRequest = {
      generation: input_.generation,
      request,
      criteriaKey: input_.criteriaKey,
      firstRow: input_.window.firstRow,
      lastRow: input_.window.lastRow,
      latestTotalRows: input_.latestTotalRows,
      sink: input_.sink,
      clearSink: () => input_.sink.setRowCount(0, false),
      live: window._tag === "Valid",
    };
    active = nextActive;
    if (previous !== undefined && !Object.is(previous.sink, input_.sink)) {
      previous.clearSink();
    }
    if (!clearSink(input_.sink, input_.latestTotalRows.value, input_.generation, request)) {
      return;
    }
    if (window._tag === "Invalid") {
      input.publish({
        owner: request,
        stream: holdChrome(invalidWindowChrome(window.message)),
      });
      return;
    }
    input.publish({
      owner: request,
      stream: makeSubscriptionStream(
        input_.query,
        input_.sink,
        window,
        input_.generation,
        request,
        (totalRows) => {
          input_.latestTotalRows.value = totalRows;
        },
      ),
    });
  };

  const replace: LiveQueryViewport<Topics, Topic>["replace"] = (request) => {
    if (terminal) {
      return {
        setWindow: () => undefined,
        release: () => undefined,
      };
    }
    const invocation = ++replaceInvocation;
    const captured = Result.try(() => snapshotViewServerQuery(request.query));
    if (invocation !== replaceInvocation) {
      return {
        setWindow: () => undefined,
        release: () => undefined,
      };
    }
    if (Result.isFailure(captured)) {
      const generation = ++generationCounter;
      const requestId = ++requestCounter;
      const previous = active;
      const nextActive: ActiveRequest = {
        generation,
        request: requestId,
        criteriaKey: "",
        firstRow: request.window.firstRow,
        lastRow: request.window.lastRow,
        latestTotalRows: { value: 0 },
        sink: request.sink,
        clearSink: () => request.sink.setRowCount(0, false),
        live: false,
      };
      active = nextActive;
      if (previous !== undefined && !Object.is(previous.sink, request.sink)) {
        previous.clearSink();
      }
      if (!clearSink(request.sink, 0, generation, requestId)) {
        return {
          setWindow: () => undefined,
          release: () => undefined,
        };
      }
      input.publish({
        owner: requestId,
        stream: holdChrome(invalidWindowChrome(String(captured.failure))),
      });
      return {
        setWindow: () => undefined,
        release: () => {
          if (active?.generation === generation) {
            active = undefined;
            publishIdle(++requestCounter);
          }
        },
      };
    }

    const query = captured.success;
    const topicDefinition = input.config.topics[input.topic]!;
    const criteriaKey = stableQueryKeyForRowSchema(query, topicDefinition.schema);
    const current = active;
    if (
      current !== undefined &&
      current.live &&
      current.criteriaKey === criteriaKey &&
      current.firstRow === request.window.firstRow &&
      current.lastRow === request.window.lastRow &&
      Object.is(current.sink, request.sink)
    ) {
      return makeGeneration(
        current.generation,
        criteriaKey,
        query,
        request.sink,
        current.latestTotalRows,
      );
    }

    const generation = ++generationCounter;
    const latestTotalRows = { value: 0 };
    start({
      generation,
      criteriaKey,
      query,
      sink: request.sink,
      window: request.window,
      latestTotalRows,
    });
    return makeGeneration(generation, criteriaKey, query, request.sink, latestTotalRows);
  };

  function makeGeneration<const Query extends LiveQueryViewportQuery<Row>>(
    generation: number,
    criteriaKey: string,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
    sink: LiveQueryViewportSink<LiveQueryRow<Row, Query>>,
    latestTotalRows: { value: number },
  ): LiveQueryViewportGeneration {
    return {
      setWindow: (window) => {
        if (active?.generation !== generation) {
          return;
        }
        if (
          active.live &&
          active.firstRow === window.firstRow &&
          active.lastRow === window.lastRow
        ) {
          return;
        }
        start({
          generation,
          criteriaKey,
          query,
          sink,
          window,
          latestTotalRows,
        });
      },
      release: () => {
        if (active?.generation !== generation) {
          return;
        }
        const request = ++requestCounter;
        active = undefined;
        sink.setRowCount(0, false);
        if (active === undefined && request === requestCounter) {
          publishIdle(request);
        }
      },
    };
  }

  const deactivate = (): void => {
    terminal = true;
    replaceInvocation += 1;
    requestCounter += 1;
    const current = active;
    active = undefined;
    current?.clearSink();
  };

  return {
    replace,
    destroy: () => {
      if (terminal) {
        return;
      }
      terminal = true;
      replaceInvocation += 1;
      const request = ++requestCounter;
      const current = active;
      active = undefined;
      if (current !== undefined) {
        current.clearSink();
      }
      publishIdle(request);
    },
    deactivate,
  };
};
