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
  type DeltaOperation,
  type GroupedQuery,
  type LiveQueryRow,
  type RawQuery,
  type TopicDefinitions,
  type TopicRow,
  type ViewServerHealth,
  type ViewServerRuntimeError,
  type ViewServerTransportError,
} from "@effect-view-server/config";
import { createInMemoryViewServer } from "@effect-view-server/in-memory";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { BigDecimal, Deferred, Effect, Fiber, Option, Queue, Result, Schema, Stream } from "effect";
import { AtomRef } from "effect/unstable/reactivity";
import {
  liveQueryViewportChromeFromAsyncResult,
  liveQueryViewportFailureMessage,
  makeLiveQueryViewport,
  makeLiveQueryViewportBinding,
  validateLiveQueryViewportWindow,
  type LiveQueryViewport,
  type LiveQueryViewportChrome,
  type LiveQueryViewportCapturedReplace,
  type LiveQueryViewportGeneration,
  type LiveQueryViewportRawQuery,
  type LiveQueryViewportSink,
} from "./live-query-viewport";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";

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

type Topics = typeof viewServer.topics;

const unusedSemanticKey = () => {
  throw new Error("Semantic identity is unused by this viewport test double.");
};

const makeBindingEntry = (
  viewport: LiveQueryViewport<Topics, "orders">,
  deactivate: () => void = viewport.destroy,
) => {
  const replaceCaptured: LiveQueryViewportCapturedReplace<Topics, "orders"> = (input) =>
    input._tag === "Success"
      ? viewport.replace(input.request)
      : { setWindow: () => undefined, release: () => undefined };
  return { viewport, replaceCaptured, deactivate };
};

type ViewportChromeStream = Stream.Stream<
  LiveQueryViewportChrome,
  ViewServerRuntimeError | ViewServerTransportError
>;

const LeasedOrder = Schema.Struct({
  id: ViewServerId,
  region: Schema.String,
});

const sourceAdapter = SourceAdapter.make({
  identity: { name: "viewport-source" },
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
      schema: LeasedOrder,
      source: sourceAdapter.leasedSource(["region"], undefined),
    },
  },
});

type LeasedTopics = typeof leasedViewServer.topics;

const ExactRoutedOrder = Schema.Struct({
  id: ViewServerId,
  amount: Schema.BigDecimal,
  sequence: Schema.BigInt,
  tier: Schema.Literals(["gold", "silver"]),
  status: Schema.Literals(["open", "closed"]),
  price: Schema.Number,
});

const exactRoutedViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: ExactRoutedOrder,
      source: sourceAdapter.leasedSource(["amount", "sequence", "tier"], undefined),
    },
  },
});

type ExactRoutedTopics = typeof exactRoutedViewServer.topics;

type ManualRequest = {
  readonly query: Readonly<Record<string, unknown>>;
  readonly events: Queue.Queue<ViewServerLiveEvent<object>>;
  closes: number;
};

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

const makeManualClient = (
  base: ViewServerLiveClient<Topics>,
  requests: Array<ManualRequest>,
): ViewServerLiveClient<Topics> => ({
  ...base,
  subscribe: adaptQuerySubstrate((_, query) =>
    Effect.gen(function* () {
      const events = yield* Queue.unbounded<ViewServerLiveEvent<object>>();
      const request: ManualRequest = {
        query,
        events,
        closes: 0,
      };
      requests.push(request);
      return {
        events: Stream.fromQueue(events),
        close: () =>
          Effect.sync(() => {
            request.closes += 1;
          }),
      };
    }),
  ),
});

const snapshot = (id: string, price: number, version: number): ViewServerLiveEvent<object> => ({
  type: "snapshot",
  topic: "orders",
  queryId: "manual",
  rows: [{ id, price }],
  keys: [id],
  totalRows: 1,
  version,
});

const delta = (
  id: string,
  price: number,
  fromVersion: number,
  toVersion: number,
): ViewServerLiveEvent<object> => ({
  type: "delta",
  topic: "orders",
  queryId: "manual",
  operations: [{ type: "update", key: id, row: { id, price }, index: 0 }],
  totalRows: 1,
  fromVersion,
  toVersion,
});

const snapshotMany = (
  rows: ReadonlyArray<{ readonly id: string; readonly price: number }>,
  version: number,
): ViewServerLiveEvent<object> => ({
  type: "snapshot",
  topic: "orders",
  queryId: "manual",
  rows,
  keys: rows.map((row) => row.id),
  totalRows: rows.length,
  version,
});

const deltaMany = (
  operations: ReadonlyArray<DeltaOperation<object>>,
  totalRows: number,
  fromVersion: number,
  toVersion: number,
): ViewServerLiveEvent<object> => ({
  type: "delta",
  topic: "orders",
  queryId: "manual",
  operations,
  totalRows,
  fromVersion,
  toVersion,
});

const makeSink = <Row>() => {
  const rowCounts: Array<readonly [number, boolean | undefined]> = [];
  const rowData: Array<{ readonly [index: number]: Row }> = [];
  const rowKeys: Array<{ readonly [index: number]: string }> = [];
  const sink: LiveQueryViewportSink<Row> = {
    setRowCount: (count, keepRenderedRows) => {
      rowCounts.push([count, keepRenderedRows]);
    },
    setRowData: (rows, keys) => {
      rowData.push(rows);
      rowKeys.push(keys);
    },
  };
  return { rowCounts, rowData, rowKeys, sink };
};

const flush = Effect.yieldNow;

const makeSwitchingPublisher = () => {
  let current: Fiber.Fiber<void, ViewServerRuntimeError | ViewServerTransportError> | undefined;
  let publishCount = 0;
  return {
    publish: (command: { readonly stream: ViewportChromeStream }) => {
      publishCount += 1;
      if (current !== undefined) {
        Effect.runFork(Fiber.interrupt(current));
      }
      current = Effect.runFork(command.stream.pipe(Stream.runDrain));
    },
    current: () => current,
    publishCount: () => publishCount,
  };
};

describe("Live Query Viewport Module", () => {
  it("keeps the base-row witness declaration-only", () => {
    const binding = makeLiveQueryViewportBinding<Topics, "orders">();

    expect(Reflect.ownKeys(binding.viewport)).toStrictEqual(["semanticKey", "replace", "destroy"]);
  });

  it("keeps semantic identity stable across binding activation and deactivation", () => {
    const binding = makeLiveQueryViewportBinding<Topics, "orders">({ rowSchema: Order });
    const query = { select: ["id"] as const, where: [], orderBy: [] };
    const beforeActivation = binding.viewport.semanticKey(query);
    const viewport: LiveQueryViewport<Topics, "orders"> = {
      semanticKey: binding.viewport.semanticKey,
      replace: () => ({ setWindow: () => undefined, release: () => undefined }),
      destroy: () => undefined,
    };
    const entry = makeBindingEntry(viewport);

    binding.install(entry);
    const whileActive = binding.viewport.semanticKey({ ...query });
    binding.uninstall(entry);
    const whileInactive = binding.viewport.semanticKey({ ...query });

    expect(Object.is(beforeActivation, whileActive)).toBe(true);
    expect(Object.is(beforeActivation, whileInactive)).toBe(true);
  });

  it("delegates semantic identity when a binding does not own the row schema", () => {
    const binding = makeLiveQueryViewportBinding<Topics, "orders">();
    const query = { select: ["id"] as const, where: [], orderBy: [] };

    expect(() => binding.viewport.semanticKey(query)).toThrowError(
      "Live Query Viewport semantic identity is not installed.",
    );

    const owner = makeLiveQueryViewportBinding<Topics, "orders">({ rowSchema: Order });
    const viewport: LiveQueryViewport<Topics, "orders"> = {
      semanticKey: owner.viewport.semanticKey,
      replace: () => ({ setWindow: () => undefined, release: () => undefined }),
      destroy: () => undefined,
    };
    const entry = makeBindingEntry(viewport);
    binding.install(entry);

    expect(
      Object.is(binding.viewport.semanticKey(query), owner.viewport.semanticKey({ ...query })),
    ).toBe(true);
    binding.uninstall(entry);
  });

  it("validates inclusive absolute windows", () => {
    expect(validateLiveQueryViewportWindow({ firstRow: 10, lastRow: 19 })).toStrictEqual({
      _tag: "Valid",
      firstRow: 10,
      lastRow: 19,
      limit: 10,
    });
    expect(validateLiveQueryViewportWindow({ firstRow: -1, lastRow: 1 })).toStrictEqual({
      _tag: "Invalid",
      message: 'Expected a value greater than or equal to 0\n  at ["firstRow"]',
    });
    expect(validateLiveQueryViewportWindow({ firstRow: 2, lastRow: 1 })).toStrictEqual({
      _tag: "Invalid",
      message: "Live Query Viewport lastRow must be greater than or equal to firstRow.",
    });
    expect(
      validateLiveQueryViewportWindow({
        firstRow: 0,
        lastRow: Number.MAX_SAFE_INTEGER,
      }),
    ).toStrictEqual({
      _tag: "Invalid",
      message: "Live Query Viewport limit must be a safe integer.",
    });
    expect(validateLiveQueryViewportWindow({ firstRow: 0.5, lastRow: 1 })).toStrictEqual({
      _tag: "Invalid",
      message: 'Expected an integer\n  at ["firstRow"]',
    });
  });

  it("maps Atom results to chrome without exposing rows", () => {
    const ready: LiveQueryViewportChrome = {
      totalRows: 3,
      version: 2,
      status: "ready",
      statusCode: "Ready",
    };
    expect(
      liveQueryViewportChromeFromAsyncResult(AsyncResult.success({ owner: 1, chrome: ready }), 1),
    ).toStrictEqual(ready);
    expect(liveQueryViewportChromeFromAsyncResult(AsyncResult.initial(), 1)).toStrictEqual({
      totalRows: 0,
      version: 0,
      status: "loading",
    });
    expect(
      liveQueryViewportChromeFromAsyncResult(AsyncResult.success({ owner: 1, chrome: ready }), 2),
    ).toStrictEqual({
      totalRows: 0,
      version: 0,
      status: "loading",
    });
  });

  it("preserves useful messages for thrown query failures", () => {
    expect(liveQueryViewportFailureMessage(new Error("query exploded"))).toBe("query exploded");
    expect(liveQueryViewportFailureMessage("query exploded")).toBe("query exploded");
    expect(liveQueryViewportFailureMessage({ message: 123 })).toBe("[object Object]");
    expect(liveQueryViewportFailureMessage(null)).toBe("null");
  });

  it("keeps one stable viewport facade across committed controller replacements", () => {
    const binding = makeLiveQueryViewportBinding<Topics, "orders">();
    const generation = { setWindow: () => undefined, release: () => undefined };
    let oldDestroys = 0;
    let currentDestroys = 0;
    let currentReplaces = 0;
    const oldViewport = {
      semanticKey: unusedSemanticKey,
      replace: () => generation,
      destroy: () => {
        oldDestroys += 1;
      },
    } satisfies LiveQueryViewport<Topics, "orders">;
    const currentViewport = {
      semanticKey: unusedSemanticKey,
      replace: () => {
        currentReplaces += 1;
        return generation;
      },
      destroy: () => {
        currentDestroys += 1;
        binding.viewport.replace({
          window: { firstRow: 20, lastRow: 29 },
          query: { select: ["id"], where: [], orderBy: [] },
          sink: { setRowCount: () => undefined, setRowData: () => undefined },
        });
      },
    } satisfies LiveQueryViewport<Topics, "orders">;
    const oldEntry = makeBindingEntry(oldViewport);
    const currentEntry = makeBindingEntry(currentViewport);
    const facade = binding.viewport;
    const uninstalledGeneration = facade.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: { select: ["id"], where: [], orderBy: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    uninstalledGeneration.setWindow({ firstRow: 10, lastRow: 19 });
    uninstalledGeneration.release();

    binding.install(oldEntry);
    binding.install(oldEntry);
    binding.install(currentEntry);
    binding.uninstall(oldEntry);
    facade.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: { select: ["id"], where: [], orderBy: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });

    expect(binding.viewport).toBe(facade);
    expect(oldDestroys).toBe(1);
    expect(currentReplaces).toBe(1);
    binding.viewport.destroy();
    expect(currentReplaces).toBe(1);
    binding.install(currentEntry);
    binding.uninstall(currentEntry);
    binding.uninstall(currentEntry);
    const detachedGeneration = binding.viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: { select: ["id"], where: [], orderBy: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    detachedGeneration.setWindow({ firstRow: 10, lastRow: 19 });
    detachedGeneration.release();
    binding.viewport.destroy();
    expect(currentDestroys).toBe(2);
  });

  it("replays failed query snapshots on the replacement controller", () => {
    const replacements: Array<string> = [];
    const generation = { setWindow: () => undefined, release: () => undefined };
    const makeEntry = (label: string) => {
      const viewport = {
        semanticKey: unusedSemanticKey,
        replace: () => {
          replacements.push(`${label}:public`);
          return generation;
        },
        destroy: () => undefined,
      } satisfies LiveQueryViewport<Topics, "orders">;
      const replaceCaptured: LiveQueryViewportCapturedReplace<Topics, "orders"> = (input) => {
        replacements.push(label);
        expect(input._tag).toBe("Failure");
        return generation;
      };
      return {
        viewport,
        replaceCaptured,
        deactivate: viewport.destroy,
      };
    };
    const binding = makeLiveQueryViewportBinding<Topics, "orders">({
      deferDeactivation: true,
      rowSchema: Order,
    });
    binding.install(makeEntry("old"));
    const queryTarget = {
      select: ["id"],
      where: [],
      orderBy: [],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [];
      readonly orderBy: readonly [];
    };
    let failSnapshot = true;
    const query = new Proxy(queryTarget, {
      ownKeys: (target) => {
        if (failSnapshot) {
          failSnapshot = false;
          throw new Error("query snapshot failed");
        }
        return Reflect.ownKeys(target);
      },
    });

    binding.viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      query,
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    binding.install(makeEntry("current"));
    binding.flush();
    expect(replacements).toStrictEqual(["old", "current"]);
  });

  it.effect("installs captured query failures without rereading caller input", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      let published: ViewportChromeStream = Stream.never;
      const viewport = makeLiveQueryViewport({
        client: base.liveClient,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          published = command.stream;
        },
      });
      type CapturedQuery = {
        readonly select: readonly ["id"];
        readonly where: readonly [];
        readonly orderBy: readonly [];
      };
      const sink = { setRowCount: () => undefined, setRowData: () => undefined };
      viewport.replaceCaptured<CapturedQuery, typeof sink>({
        _tag: "Failure",
        request: {
          window: { firstRow: 0, lastRow: 9 },
          sink,
        },
        failure: new Error("captured query failure"),
      });

      expect(yield* published.pipe(Stream.runHead)).toStrictEqual(
        Option.some({
          totalRows: 0,
          version: 0,
          status: "error",
          statusCode: "InvalidQuery",
          message: "captured query failure",
        }),
      );
      viewport.destroy();
      const publishedAfterDestroy = published;
      let terminalRequestReads = 0;
      const terminalRequest = {
        window: { firstRow: 0, lastRow: 9 },
        get sink() {
          terminalRequestReads += 1;
          return sink;
        },
      };
      const terminalGeneration = viewport.replaceCaptured<CapturedQuery, typeof sink>({
        _tag: "Failure",
        request: terminalRequest,
        failure: new Error("terminal captured failure"),
      });
      terminalGeneration.setWindow({ firstRow: 10, lastRow: 19 });
      terminalGeneration.release();
      expect(terminalRequestReads).toBe(0);
      expect(published).toBe(publishedAfterDestroy);
      yield* base.close;
    }),
  );

  it.effect("passes captured queries through the production binding seam without rereading", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const viewport = makeLiveQueryViewport({
        client: base.liveClient,
        config: viewServer,
        topic: "orders",
        publish: () => undefined,
      });
      const binding = makeLiveQueryViewportBinding<Topics, "orders">();
      binding.install({
        viewport,
        replaceCaptured: viewport.replaceCaptured,
        deactivate: viewport.deactivate,
      });
      let queryReads = 0;
      const queryTarget = {
        select: ["id"],
        where: [],
        orderBy: [],
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: readonly [];
        readonly orderBy: readonly [];
      };
      const query = new Proxy(queryTarget, {
        ownKeys: (target) => {
          queryReads += 1;
          return Reflect.ownKeys(target);
        },
      });

      const generation = binding.viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query,
        sink: { setRowCount: () => undefined, setRowData: () => undefined },
      });

      expect(queryReads).toBe(1);
      generation.release();
      binding.viewport.destroy();
      yield* base.close;
    }),
  );

  it("preserves request snapshots across a deferred controller replacement", () => {
    let replayedRequest: unknown;
    const generation = { setWindow: () => undefined, release: () => undefined };
    const oldViewport = {
      semanticKey: unusedSemanticKey,
      replace: () => generation,
      destroy: () => undefined,
    } satisfies LiveQueryViewport<Topics, "orders">;
    const currentViewport = {
      semanticKey: unusedSemanticKey,
      replace: (request) => {
        replayedRequest = request;
        return generation;
      },
      destroy: () => undefined,
    } satisfies LiveQueryViewport<Topics, "orders">;
    const binding = makeLiveQueryViewportBinding<Topics, "orders">({
      deferDeactivation: true,
    });
    const where: Array<{
      readonly field: "status";
      readonly type: "equals";
      readonly filter: "closed";
    }> = [];
    const select: ["id"] = ["id"];
    const orderBy: [] = [];
    const query = {
      select,
      where,
      orderBy,
    } satisfies LiveQueryViewportRawQuery<TopicRow<Topics, "orders">>;
    const window = { firstRow: 0, lastRow: 9 };
    const sink = { setRowCount: () => undefined, setRowData: () => undefined };
    binding.install(makeBindingEntry(oldViewport));
    const active = binding.viewport.replace({
      window,
      query,
      sink,
    });
    window.firstRow = 90;
    where.push({ field: "status", type: "equals", filter: "closed" });
    const latestWindow = { firstRow: 10, lastRow: 19 };
    active.setWindow(latestWindow);
    latestWindow.firstRow = 80;
    binding.install(makeBindingEntry(currentViewport));
    binding.flush();

    expect(replayedRequest).toStrictEqual({
      window: { firstRow: 10, lastRow: 19 },
      query: {
        select: ["id"],
        where: [],
        orderBy: [],
      },
      sink,
    });
  });

  it("reads the caller query property once before owning its captured outcome", () => {
    const events: Array<string> = [];
    const binding = makeLiveQueryViewportBinding<Topics, "orders">({
      deferDeactivation: true,
    });
    const makeEntry = (label: string) => {
      const viewport: LiveQueryViewport<Topics, "orders"> = {
        semanticKey: unusedSemanticKey,
        replace: (request) => {
          events.push(`${label}:${request.window.firstRow}`);
          return { setWindow: () => undefined, release: () => undefined };
        },
        destroy: () => undefined,
      };
      return makeBindingEntry(viewport);
    };
    const query = {
      select: ["id"],
      where: [],
      orderBy: [],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [];
      readonly orderBy: readonly [];
    };
    let queryReads = 0;
    const request = {
      window: { firstRow: 0, lastRow: 9 },
      query,
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    };
    Object.defineProperty(request, "query", {
      enumerable: true,
      get: () => {
        queryReads += 1;
        if (queryReads === 2) {
          binding.viewport.replace({
            window: { firstRow: 20, lastRow: 29 },
            query,
            sink: request.sink,
          });
        }
        return query;
      },
    });
    binding.install(makeEntry("old"));
    binding.viewport.replace(request);
    binding.install(makeEntry("current"));
    binding.flush();

    expect(queryReads).toBe(1);
    expect(events).toStrictEqual(["old:0", "current:0"]);
  });

  it("keeps the active generation owned when caller field capture throws", () => {
    const events: Array<string> = [];
    const viewport: LiveQueryViewport<Topics, "orders"> = {
      semanticKey: unusedSemanticKey,
      replace: () => ({
        setWindow: (window) => {
          events.push(`window:${window.firstRow}`);
        },
        release: () => {
          events.push("release");
        },
      }),
      destroy: () => undefined,
    };
    const binding = makeLiveQueryViewportBinding<Topics, "orders">();
    binding.install(makeBindingEntry(viewport));
    const active = binding.viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: { select: ["id"], where: [], orderBy: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    const sinkRequest = {
      window: { firstRow: 10, lastRow: 19 },
      query: { select: ["id"], where: [], orderBy: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    } satisfies {
      readonly window: { readonly firstRow: number; readonly lastRow: number };
      readonly query: {
        readonly select: readonly ["id"];
        readonly where: readonly [];
        readonly orderBy: readonly [];
      };
      readonly sink: LiveQueryViewportSink<{ readonly id: string }>;
    };
    Object.defineProperty(sinkRequest, "sink", {
      enumerable: true,
      get: () => {
        throw new Error("sink capture failed");
      },
    });
    expect(() => binding.viewport.replace(sinkRequest)).toThrowError("sink capture failed");
    active.setWindow({ firstRow: 20, lastRow: 29 });

    const window = new Proxy(
      { firstRow: 30, lastRow: 39 },
      {
        get: () => {
          throw new Error("window capture failed");
        },
      },
    );
    expect(() =>
      binding.viewport.replace({
        window,
        query: { select: ["id"], where: [], orderBy: [] },
        sink: { setRowCount: () => undefined, setRowData: () => undefined },
      }),
    ).toThrowError("window capture failed");
    active.release();
    expect(events).toStrictEqual(["window:20", "release"]);
  });

  it("keeps switch-latest ownership through a reentrant release", () => {
    const events: Array<string> = [];
    const binding = makeLiveQueryViewportBinding<Topics, "orders">({
      deferDeactivation: true,
    });
    let reenterRelease = true;
    const makeEntry = (label: string) => {
      const viewport: LiveQueryViewport<Topics, "orders"> = {
        semanticKey: unusedSemanticKey,
        replace: (request) => {
          events.push(`${label}:replace:${request.window.firstRow}`);
          return {
            setWindow: () => undefined,
            release: () => {
              if (reenterRelease) {
                reenterRelease = false;
                binding.viewport.replace({
                  window: { firstRow: 20, lastRow: 29 },
                  query: { select: ["id"], where: [], orderBy: [] },
                  sink: { setRowCount: () => undefined, setRowData: () => undefined },
                });
              }
            },
          };
        },
        destroy: () => undefined,
      };
      return makeBindingEntry(viewport);
    };
    const oldEntry = makeEntry("old");
    const currentEntry = makeEntry("current");
    binding.install(oldEntry);
    const released = binding.viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: { select: ["id"], where: [], orderBy: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    released.release();
    binding.install(currentEntry);
    binding.flush();
    expect(events).toStrictEqual(["old:replace:0", "old:replace:20", "current:replace:20"]);
  });

  it("keeps switch-latest ownership through a reentrant query snapshot", () => {
    const events: Array<string> = [];
    const binding = makeLiveQueryViewportBinding<Topics, "orders">({
      deferDeactivation: true,
    });
    const makeEntry = (label: string) => {
      const viewport: LiveQueryViewport<Topics, "orders"> = {
        semanticKey: unusedSemanticKey,
        replace: (request) => {
          events.push(`${label}:replace:${request.window.firstRow}`);
          return { setWindow: () => undefined, release: () => undefined };
        },
        destroy: () => undefined,
      };
      return makeBindingEntry(viewport);
    };
    binding.install(makeEntry("current"));
    let reenterSnapshot = true;
    const queryTarget = {
      select: ["id"],
      where: [],
      orderBy: [],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [];
      readonly orderBy: readonly [];
    };
    const query = new Proxy(queryTarget, {
      ownKeys: (target) => {
        if (reenterSnapshot) {
          reenterSnapshot = false;
          binding.viewport.replace({
            window: { firstRow: 40, lastRow: 49 },
            query: { select: ["id"], where: [], orderBy: [] },
            sink: { setRowCount: () => undefined, setRowData: () => undefined },
          });
        }
        return Reflect.ownKeys(target);
      },
    });
    const superseded = binding.viewport.replace({
      window: { firstRow: 30, lastRow: 39 },
      query,
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    superseded.setWindow({ firstRow: 50, lastRow: 59 });
    superseded.release();
    const finalEntry = makeEntry("final");
    binding.install(finalEntry);
    binding.flush();
    expect(events).toStrictEqual(["current:replace:40", "final:replace:40"]);
  });

  it("keeps switch-latest ownership through a reentrant window snapshot", () => {
    const events: Array<string> = [];
    const binding = makeLiveQueryViewportBinding<Topics, "orders">({
      deferDeactivation: true,
    });
    const makeEntry = (label: string) => {
      const viewport: LiveQueryViewport<Topics, "orders"> = {
        semanticKey: unusedSemanticKey,
        replace: (request) => {
          events.push(`${label}:replace:${request.window.firstRow}`);
          return { setWindow: () => undefined, release: () => undefined };
        },
        destroy: () => undefined,
      };
      return makeBindingEntry(viewport);
    };
    binding.install(makeEntry("final"));
    let reenterWindow = true;
    const hostileWindow = new Proxy(
      { firstRow: 50, lastRow: 59 },
      {
        get: (target, property, receiver) => {
          if (property === "firstRow" && reenterWindow) {
            reenterWindow = false;
            binding.viewport.replace({
              window: { firstRow: 60, lastRow: 69 },
              query: { select: ["id"], where: [], orderBy: [] },
              sink: { setRowCount: () => undefined, setRowData: () => undefined },
            });
          }
          return Reflect.get(target, property, receiver);
        },
      },
    );
    const supersededWindow = binding.viewport.replace({
      window: hostileWindow,
      query: { select: ["id"], where: [], orderBy: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    supersededWindow.setWindow({ firstRow: 70, lastRow: 79 });
    supersededWindow.release();
    const windowFinalEntry = makeEntry("window-final");
    binding.install(windowFinalEntry);
    binding.flush();
    expect(events).toStrictEqual(["final:replace:60", "window-final:replace:60"]);
  });

  it("keeps nested replacements authoritative during controller installation", () => {
    const events: Array<string> = [];
    const binding = makeLiveQueryViewportBinding<Topics, "orders">({
      deferDeactivation: true,
    });
    let reenterInitialInstall = true;
    const initialViewport: LiveQueryViewport<Topics, "orders"> = {
      semanticKey: unusedSemanticKey,
      replace: (request) => {
        events.push(`initial:replace:${request.window.firstRow}`);
        if (reenterInitialInstall) {
          reenterInitialInstall = false;
          binding.viewport.replace({
            window: { firstRow: 10, lastRow: 19 },
            query: { select: ["id"], where: [], orderBy: [] },
            sink: { setRowCount: () => undefined, setRowData: () => undefined },
          });
        }
        return {
          setWindow: () => undefined,
          release: () => {
            events.push(`initial:release:${request.window.firstRow}`);
          },
        };
      },
      destroy: () => undefined,
    };
    binding.install(makeBindingEntry(initialViewport));
    const obsolete = binding.viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: { select: ["id"], where: [], orderBy: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    obsolete.setWindow({ firstRow: 90, lastRow: 99 });
    obsolete.release();

    let reenterReplay = true;
    const replayViewport: LiveQueryViewport<Topics, "orders"> = {
      semanticKey: unusedSemanticKey,
      replace: (request) => {
        events.push(`replay:replace:${request.window.firstRow}`);
        if (reenterReplay) {
          reenterReplay = false;
          binding.viewport.replace({
            window: { firstRow: 20, lastRow: 29 },
            query: { select: ["id"], where: [], orderBy: [] },
            sink: { setRowCount: () => undefined, setRowData: () => undefined },
          });
        }
        return {
          setWindow: () => undefined,
          release: () => {
            events.push(`replay:release:${request.window.firstRow}`);
          },
        };
      },
      destroy: () => undefined,
    };
    binding.install(makeBindingEntry(replayViewport));
    binding.flush();
    const finalViewport: LiveQueryViewport<Topics, "orders"> = {
      semanticKey: unusedSemanticKey,
      replace: (request) => {
        events.push(`final:replace:${request.window.firstRow}`);
        return { setWindow: () => undefined, release: () => undefined };
      },
      destroy: () => undefined,
    };
    binding.install(makeBindingEntry(finalViewport));
    binding.flush();

    expect(events).toStrictEqual([
      "initial:replace:0",
      "initial:replace:10",
      "replay:replace:10",
      "replay:replace:20",
      "replay:release:10",
      "final:replace:20",
    ]);
  });

  it("abandons a query snapshot when reentrant destruction loses ownership", () => {
    const generation = { setWindow: () => undefined, release: () => undefined };
    const makeViewport = (replace: LiveQueryViewport<Topics, "orders">["replace"]) => ({
      semanticKey: unusedSemanticKey,
      replace,
      destroy: () => undefined,
    });
    const select: ["id"] = ["id"];
    const where: [] = [];
    const orderBy: [] = [];
    const request = {
      window: { firstRow: 0, lastRow: 9 },
      query: { select, where, orderBy },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    };

    const snapshotDestroyBinding = makeLiveQueryViewportBinding<Topics, "orders">();
    let snapshotDestroyReplaces = 0;
    const snapshotDestroyViewport = makeViewport(() => {
      snapshotDestroyReplaces += 1;
      return generation;
    });
    snapshotDestroyBinding.install(makeBindingEntry(snapshotDestroyViewport));
    const destroyQuery = new Proxy(request.query, {
      ownKeys: (target) => {
        snapshotDestroyBinding.viewport.destroy();
        return Reflect.ownKeys(target);
      },
    });
    snapshotDestroyBinding.viewport.replace({ ...request, query: destroyQuery });
    expect(snapshotDestroyReplaces).toBe(0);
  });

  it("abandons a query snapshot when a reentrant controller switch loses ownership", () => {
    const generation = { setWindow: () => undefined, release: () => undefined };
    const makeViewport = (replace: LiveQueryViewport<Topics, "orders">["replace"]) => ({
      semanticKey: unusedSemanticKey,
      replace,
      destroy: () => undefined,
    });
    const select: ["id"] = ["id"];
    const where: [] = [];
    const orderBy: [] = [];
    const request = {
      window: { firstRow: 0, lastRow: 9 },
      query: { select, where, orderBy },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    };
    const snapshotSwitchBinding = makeLiveQueryViewportBinding<Topics, "orders">();
    let snapshotSwitchReplaces = 0;
    const snapshotOldViewport = makeViewport(() => {
      snapshotSwitchReplaces += 1;
      return generation;
    });
    const snapshotNewViewport = makeViewport(() => generation);
    const snapshotNewEntry = makeBindingEntry(snapshotNewViewport);
    snapshotSwitchBinding.install(makeBindingEntry(snapshotOldViewport));
    const switchQuery = new Proxy(request.query, {
      ownKeys: (target) => {
        snapshotSwitchBinding.install(snapshotNewEntry);
        return Reflect.ownKeys(target);
      },
    });
    snapshotSwitchBinding.viewport.replace({ ...request, query: switchQuery });
    expect(snapshotSwitchReplaces).toBe(0);
  });

  it("abandons an installed generation when reentrant destruction loses ownership", () => {
    const makeViewport = (replace: LiveQueryViewport<Topics, "orders">["replace"]) => ({
      semanticKey: unusedSemanticKey,
      replace,
      destroy: () => undefined,
    });
    const select: ["id"] = ["id"];
    const where: [] = [];
    const orderBy: [] = [];
    const request = {
      window: { firstRow: 0, lastRow: 9 },
      query: { select, where, orderBy },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    };
    const installDestroyBinding = makeLiveQueryViewportBinding<Topics, "orders">();
    const installDestroyEvents: Array<string> = [];
    const installDestroyViewport = makeViewport(() => {
      installDestroyBinding.viewport.destroy();
      return {
        setWindow: () => {
          installDestroyEvents.push("window");
        },
        release: () => {
          installDestroyEvents.push("release");
        },
      };
    });
    installDestroyBinding.install(makeBindingEntry(installDestroyViewport));
    const destroyedGeneration = installDestroyBinding.viewport.replace(request);
    destroyedGeneration.setWindow({ firstRow: 10, lastRow: 19 });
    destroyedGeneration.release();
    expect(installDestroyEvents).toStrictEqual([]);
  });

  it("abandons an installed generation when a reentrant controller switch loses ownership", () => {
    const generation = { setWindow: () => undefined, release: () => undefined };
    const makeViewport = (replace: LiveQueryViewport<Topics, "orders">["replace"]) => ({
      semanticKey: unusedSemanticKey,
      replace,
      destroy: () => undefined,
    });
    const select: ["id"] = ["id"];
    const where: [] = [];
    const orderBy: [] = [];
    const request = {
      window: { firstRow: 0, lastRow: 9 },
      query: { select, where, orderBy },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    };
    const installSwitchBinding = makeLiveQueryViewportBinding<Topics, "orders">();
    let installCurrentReplaces = 0;
    const installCurrentViewport = makeViewport(() => {
      installCurrentReplaces += 1;
      return generation;
    });
    const installCurrentEntry = makeBindingEntry(installCurrentViewport);
    const installSwitchEvents: Array<string> = [];
    const installOldViewport = makeViewport(() => {
      installSwitchBinding.install(installCurrentEntry);
      return {
        setWindow: () => {
          installSwitchEvents.push("window");
        },
        release: () => {
          installSwitchEvents.push("release");
        },
      };
    });
    installSwitchBinding.install(makeBindingEntry(installOldViewport));
    const switchedGeneration = installSwitchBinding.viewport.replace(request);
    switchedGeneration.setWindow({ firstRow: 10, lastRow: 19 });
    switchedGeneration.release();
    installSwitchBinding.viewport.replace(request);
    expect(installSwitchEvents).toStrictEqual([]);
    expect(installCurrentReplaces).toBe(1);
  });

  it("keeps one generation active across deferred controller replacements", () => {
    const events: Array<string> = [];
    const makeEntry = (label: string) => {
      const viewport: LiveQueryViewport<Topics, "orders"> = {
        semanticKey: unusedSemanticKey,
        replace: (request) => {
          events.push(`${label}:replace:${request.window.firstRow}-${request.window.lastRow}`);
          let released = false;
          return {
            setWindow: (window) => {
              if (!released) {
                events.push(`${label}:window:${window.firstRow}-${window.lastRow}`);
              }
            },
            release: () => {
              if (!released) {
                released = true;
                events.push(`${label}:release`);
              }
            },
          };
        },
        destroy: () => {
          events.push(`${label}:destroy`);
        },
      };
      return makeBindingEntry(viewport, () => {
        events.push(`${label}:deactivate`);
      });
    };
    const oldEntry = makeEntry("old");
    const currentEntry = makeEntry("current");
    const binding = makeLiveQueryViewportBinding<Topics, "orders">({
      deferDeactivation: true,
    });
    binding.install(oldEntry);
    const generation = binding.viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: { select: ["id"], where: [], orderBy: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    binding.install(currentEntry);
    expect(events).toStrictEqual(["old:replace:0-9"]);
    binding.flush();
    expect(events).toStrictEqual(["old:replace:0-9", "current:replace:0-9", "old:deactivate"]);

    generation.setWindow({ firstRow: 10, lastRow: 19 });
    expect(events.at(-1)).toBe("current:window:10-19");
    const replacement = binding.viewport.replace({
      window: { firstRow: 20, lastRow: 29 },
      query: { select: ["id"], where: [], orderBy: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    generation.setWindow({ firstRow: 30, lastRow: 39 });
    generation.release();
    expect(events.at(-1)).toBe("current:replace:20-29");
    replacement.release();
    replacement.release();
    replacement.setWindow({ firstRow: 40, lastRow: 49 });
    expect(events.at(-1)).toBe("current:release");

    const canceledBinding = makeLiveQueryViewportBinding<Topics, "orders">({
      deferDeactivation: true,
    });
    const canceledOldEntry = makeEntry("canceled-old");
    const canceledCurrentEntry = makeEntry("canceled-current");
    canceledBinding.install(canceledOldEntry);
    canceledBinding.viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: { select: ["id"], where: [], orderBy: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    canceledBinding.install(canceledCurrentEntry);
    canceledBinding.uninstall(canceledCurrentEntry);
    canceledBinding.flush();
    expect(events).not.toContain("canceled-current:replace:0-9");
    expect(events.slice(-2)).toStrictEqual([
      "canceled-old:deactivate",
      "canceled-current:deactivate",
    ]);

    const restoredBinding = makeLiveQueryViewportBinding<Topics, "orders">({
      deferDeactivation: true,
    });
    const restoredOldEntry = makeEntry("restored-old");
    const restoredCurrentEntry = makeEntry("restored-current");
    const restoredStart = events.length;
    restoredBinding.install(restoredOldEntry);
    restoredBinding.viewport.replace({
      window: { firstRow: 0, lastRow: 9 },
      query: { select: ["id"], where: [], orderBy: [] },
      sink: { setRowCount: () => undefined, setRowData: () => undefined },
    });
    restoredBinding.install(restoredCurrentEntry);
    restoredBinding.install(restoredOldEntry);
    restoredBinding.flush();
    expect(events.slice(restoredStart)).toStrictEqual([
      "restored-old:replace:0-9",
      "restored-current:deactivate",
    ]);
  });

  it.effect("projects snapshots and Deltas at absolute indexes", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const client = makeManualClient(base.liveClient, requests);
      let currentStream: ViewportChromeStream = Stream.never;
      const viewport = makeLiveQueryViewport({
        client,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          currentStream = command.stream;
        },
      });
      const projected = makeSink<{ readonly id: string; readonly price: number }>();

      const generation = viewport.replace({
        window: { firstRow: 20, lastRow: 29 },
        query: {
          select: ["id", "price"],
          where: [],
          orderBy: [{ field: "price", direction: "asc" }],
        },
        sink: projected.sink,
      });
      const chrome: Array<LiveQueryViewportChrome> = [];
      const fiber = yield* currentStream.pipe(
        Stream.tap((value) =>
          Effect.sync(() => {
            chrome.push(value);
          }),
        ),
        Stream.runDrain,
        Effect.forkChild,
      );
      yield* flush;
      expect(requests).toHaveLength(1);
      expect(requests[0]!.query).toStrictEqual({
        select: ["id", "price"],
        where: [],
        orderBy: [{ field: "price", direction: "asc" }],
        offset: 20,
        limit: 10,
      });

      yield* Queue.offer(requests[0]!.events, snapshot("a", 1, 1));
      yield* flush;
      yield* Queue.offer(requests[0]!.events, delta("a", 2, 1, 2));
      yield* flush;
      yield* Queue.offer(requests[0]!.events, deltaMany([], 2, 2, 3));
      yield* flush;

      expect(projected.rowData).toStrictEqual([
        { 20: { id: "a", price: 1 } },
        { 20: { id: "a", price: 2 } },
      ]);
      expect(projected.rowKeys).toStrictEqual([{ 20: "a" }, { 20: "a" }]);
      expect(projected.rowCounts).toStrictEqual([
        [0, false],
        [1, true],
        [1, true],
        [2, true],
      ]);
      expect(chrome).toStrictEqual([
        {
          totalRows: 1,
          version: 1,
          status: "ready",
          statusCode: "Ready",
          message: undefined,
        },
        {
          totalRows: 1,
          version: 2,
          status: "ready",
          statusCode: "Ready",
          message: undefined,
        },
        {
          totalRows: 2,
          version: 3,
          status: "ready",
          statusCode: "Ready",
          message: undefined,
        },
      ]);

      generation.release();
      yield* Fiber.interrupt(fiber);
      yield* base.close;
    }),
  );

  it.effect("preserves leased routing in the runtime subscription", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const leasedHealth = AtomRef.make<ViewServerHealth<LeasedTopics>>({
        ...base.liveClient.health.value,
        sources: { orders: [] },
      });
      const leasedClient = {
        subscribe: adaptQuerySubstrate<LeasedTopics>((_, query) =>
          Effect.gen(function* () {
            const events = yield* Queue.unbounded<ViewServerLiveEvent<object>>();
            requests.push({ query, events, closes: 0 });
            return {
              events: Stream.fromQueue(events),
              close: () => Effect.void,
            };
          }),
        ),
        subscribeHealthSummary: base.liveClient.subscribeHealthSummary,
        subscribeHealth: base.liveClient.subscribeHealth,
        subscribeSourceHealth: () => Effect.die("unused source health"),
        health: leasedHealth,
        close: Effect.void,
      } satisfies ViewServerLiveClient<LeasedTopics>;
      let currentStream: ViewportChromeStream = Stream.never;
      const viewport = makeLiveQueryViewport<LeasedTopics, "orders">({
        client: leasedClient,
        config: leasedViewServer,
        topic: "orders",
        publish: (command) => {
          currentStream = command.stream;
        },
      });
      const projected = makeSink<{ readonly id: string }>();
      viewport.replace({
        window: { firstRow: 10, lastRow: 19 },
        query: {
          routeBy: { region: "emea" },
          select: ["id"],
          where: [],
          orderBy: [],
        },
        sink: projected.sink,
      });
      const fiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;

      expect(requests[0]!.query).toStrictEqual({
        routeBy: { region: "emea" },
        select: ["id"],
        where: [],
        orderBy: [],
        offset: 10,
        limit: 10,
      });
      yield* Queue.offer(requests[0]!.events, {
        type: "snapshot",
        topic: "orders",
        queryId: "leased",
        rows: [{ id: "leased-order" }],
        keys: ["leased-order"],
        totalRows: 1,
        version: 1,
      });
      yield* flush;
      expect(projected.rowData).toStrictEqual([{ 10: { id: "leased-order" } }]);

      yield* Fiber.interrupt(fiber);
      viewport.destroy();
      yield* base.close;
    }),
  );

  it.effect("owns exact semantic query keys for the viewport lifetime", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const health = AtomRef.make<ViewServerHealth<ExactRoutedTopics>>({
        ...base.liveClient.health.value,
        sources: { orders: [] },
      });
      const client = {
        subscribe: adaptQuerySubstrate<ExactRoutedTopics>((_, query) =>
          Effect.gen(function* () {
            const events = yield* Queue.unbounded<ViewServerLiveEvent<object>>();
            requests.push({ query, events, closes: 0 });
            return { events: Stream.fromQueue(events), close: () => Effect.void };
          }),
        ),
        subscribeHealthSummary: base.liveClient.subscribeHealthSummary,
        subscribeHealth: base.liveClient.subscribeHealth,
        subscribeSourceHealth: () => Effect.die("unused source health"),
        health,
        close: Effect.void,
      } satisfies ViewServerLiveClient<ExactRoutedTopics>;
      let currentStream: ViewportChromeStream = Stream.never;
      const viewport = makeLiveQueryViewport<ExactRoutedTopics, "orders">({
        client,
        config: exactRoutedViewServer,
        topic: "orders",
        publish: (command) => {
          currentStream = command.stream;
        },
      });
      const query = () => ({
        routeBy: {
          amount: BigDecimal.make(150n, 2),
          sequence: 12n,
          tier: "gold" as const,
        },
        select: ["id"] as const,
        where: [],
        orderBy: [],
      });

      const firstKey = viewport.semanticKey(query());
      const secondKey = viewport.semanticKey(query());
      expect(Object.is(firstKey, secondKey)).toBe(true);
      expect(Object.isFrozen(firstKey)).toBe(true);
      expect(Object.keys(firstKey)).toStrictEqual([]);

      const distinctKeys = [
        viewport.semanticKey({
          ...query(),
          routeBy: { ...query().routeBy, amount: BigDecimal.make(15n, 1) },
        }),
        viewport.semanticKey({
          ...query(),
          routeBy: { ...query().routeBy, sequence: 13n },
        }),
        viewport.semanticKey({
          ...query(),
          routeBy: { ...query().routeBy, tier: "silver" },
        }),
        viewport.semanticKey({ ...query(), select: ["id", "price"] }),
        viewport.semanticKey({
          ...query(),
          where: [{ field: "status", type: "equals", filter: "open" }],
        }),
        viewport.semanticKey({
          ...query(),
          orderBy: [{ field: "price", direction: "asc" }],
        }),
        viewport.semanticKey({
          routeBy: query().routeBy,
          groupBy: ["status"],
          aggregates: { rowCount: { aggFunc: "count" } },
          where: [],
          orderBy: [],
        }),
        viewport.semanticKey({
          routeBy: query().routeBy,
          groupBy: ["status", "tier"],
          aggregates: { rowCount: { aggFunc: "count" } },
          where: [],
          orderBy: [],
        }),
        viewport.semanticKey({
          routeBy: query().routeBy,
          groupBy: ["status"],
          aggregates: { rowCount: { aggFunc: "count" } },
          where: [],
          orderBy: [{ aggregate: "rowCount", direction: "desc" }],
        }),
        viewport.semanticKey({
          routeBy: query().routeBy,
          groupBy: ["status"],
          aggregates: { totalPrice: { aggFunc: "sum", field: "price" } },
          where: [],
          orderBy: [],
        }),
      ];
      expect(new Set([firstKey, ...distinctKeys]).size).toBe(distinctKeys.length + 1);

      let queryReflectionCount = 0;
      const trackedQuery = new Proxy(query(), {
        ownKeys: (target) => {
          queryReflectionCount += 1;
          return Reflect.ownKeys(target);
        },
      });
      viewport.semanticKey(trackedQuery);
      const reflectionCountAfterIdentity = queryReflectionCount;
      const sink = makeSink<{ readonly id: string }>();
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: trackedQuery,
        sink: sink.sink,
      });
      const firstFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: query(),
        sink: sink.sink,
      });
      yield* flush;
      expect(requests).toHaveLength(1);
      expect(queryReflectionCount).toBe(reflectionCountAfterIdentity);

      const cyclicWhere: Array<unknown> = [];
      cyclicWhere.push(cyclicWhere);
      yield* Fiber.interrupt(firstFiber);
      const cyclicQuery = {
        ...query(),
        where: cyclicWhere,
      };
      // @ts-expect-error runtime admission rejects cyclic filters.
      const semanticFailure = Result.try(() => viewport.semanticKey(cyclicQuery));
      expect(Result.isFailure(semanticFailure)).toBe(true);
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        // @ts-expect-error runtime admission rejects cyclic filters.
        query: cyclicQuery,
        // @ts-expect-error invalid exact query makes the sink field uninhabitable.
        sink: sink.sink,
      });
      const invalidChrome = Option.getOrThrow(yield* currentStream.pipe(Stream.runHead));
      expect(invalidChrome).toStrictEqual({
        totalRows: 0,
        version: 0,
        status: "error",
        statusCode: "InvalidQuery",
        message: liveQueryViewportFailureMessage(
          Option.getOrThrow(Result.getFailure(semanticFailure)),
        ),
      });
      expect(requests).toHaveLength(1);

      const unsupportedQuery = {
        ...query(),
        where: [() => true],
      };
      // @ts-expect-error runtime admission rejects unsupported query values.
      const unsupportedSemanticFailure = Result.try(() => viewport.semanticKey(unsupportedQuery));
      expect(Result.isFailure(unsupportedSemanticFailure)).toBe(true);
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        // @ts-expect-error runtime admission rejects unsupported query values.
        query: unsupportedQuery,
        // @ts-expect-error invalid exact query makes the sink field uninhabitable.
        sink: sink.sink,
      });
      const unsupportedChrome = Option.getOrThrow(yield* currentStream.pipe(Stream.runHead));
      expect(unsupportedChrome).toStrictEqual({
        totalRows: 0,
        version: 0,
        status: "error",
        statusCode: "InvalidQuery",
        message: liveQueryViewportFailureMessage(
          Option.getOrThrow(Result.getFailure(unsupportedSemanticFailure)),
        ),
      });
      expect(requests).toHaveLength(1);

      const invalidFilterQuery = {
        ...query(),
        where: [{ field: "status" as const, type: "equals" as const, filter: "missing" }],
      };
      const invalidFilterSemanticFailure = Result.try(() =>
        Reflect.apply(viewport.semanticKey, viewport, [invalidFilterQuery]),
      );
      expect(Result.isFailure(invalidFilterSemanticFailure)).toBe(true);
      Reflect.apply(viewport.replace, viewport, [
        {
          window: { firstRow: 0, lastRow: 9 },
          query: invalidFilterQuery,
          sink: sink.sink,
        },
      ]);
      const invalidFilterChrome = Option.getOrThrow(yield* currentStream.pipe(Stream.runHead));
      expect(invalidFilterChrome).toStrictEqual({
        totalRows: 0,
        version: 0,
        status: "error",
        statusCode: "InvalidQuery",
        message: liveQueryViewportFailureMessage(
          Option.getOrThrow(Result.getFailure(invalidFilterSemanticFailure)),
        ),
      });
      expect(requests).toHaveLength(1);

      const invalidRouteQuery = {
        ...query(),
        routeBy: { ...query().routeBy, tier: "bronze" as const },
      };
      // @ts-expect-error runtime admission rejects route values outside the configured schema.
      const invalidRouteSemanticFailure = Result.try(() => viewport.semanticKey(invalidRouteQuery));
      expect(Result.isFailure(invalidRouteSemanticFailure)).toBe(true);
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        // @ts-expect-error runtime admission rejects route values outside the configured schema.
        query: invalidRouteQuery,
        sink: sink.sink,
      });
      const invalidRouteChrome = Option.getOrThrow(yield* currentStream.pipe(Stream.runHead));
      expect(invalidRouteChrome).toStrictEqual({
        totalRows: 0,
        version: 0,
        status: "error",
        statusCode: "InvalidQuery",
        message: liveQueryViewportFailureMessage(
          Option.getOrThrow(Result.getFailure(invalidRouteSemanticFailure)),
        ),
      });
      expect(requests).toHaveLength(1);

      viewport.destroy();
      yield* base.close;
    }),
  );

  it.effect("enforces switch-latest when an older request emits after its replacement", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const client = makeManualClient(base.liveClient, requests);
      let currentStream: ViewportChromeStream = Stream.never;
      const viewport = makeLiveQueryViewport({
        client,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          currentStream = command.stream;
        },
      });
      const projected = makeSink<{ readonly id: string; readonly price: number }>();

      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: {
          select: ["id", "price"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [],
        },
        sink: projected.sink,
      });
      const oldChrome: Array<LiveQueryViewportChrome> = [];
      const oldFiber = yield* currentStream.pipe(
        Stream.tap((chrome) =>
          Effect.sync(() => {
            oldChrome.push(chrome);
          }),
        ),
        Stream.runDrain,
        Effect.forkChild,
      );
      yield* flush;

      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: {
          select: ["id", "price"],
          where: [{ field: "status", type: "equals", filter: "closed" }],
          orderBy: [],
        },
        sink: projected.sink,
      });
      const newFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      expect(requests).toHaveLength(2);

      yield* Queue.offer(requests[1]!.events, snapshot("new", 2, 2));
      yield* flush;
      yield* Queue.offer(requests[0]!.events, snapshot("old-late", 1, 1));
      yield* Queue.offer(requests[0]!.events, delta("old-late", 3, 1, 2));
      yield* Queue.offer(requests[0]!.events, {
        type: "status",
        topic: "orders",
        queryId: "manual",
        status: "error",
        code: "InvalidQuery",
        message: "obsolete status",
      });
      yield* flush;

      expect(projected.rowData).toStrictEqual([{ 0: { id: "new", price: 2 } }]);
      expect(oldChrome).toStrictEqual([]);

      yield* Fiber.interrupt(oldFiber);
      expect(requests[0]!.closes).toBe(1);
      expect(requests[1]!.closes).toBe(0);
      yield* Fiber.interrupt(newFiber);
      expect(requests[1]!.closes).toBe(1);
      viewport.destroy();
      yield* base.close;
    }),
  );

  it.effect("uses setWindow as a generation-owned switch-latest hot path", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const client = makeManualClient(base.liveClient, requests);
      let currentStream: ViewportChromeStream = Stream.never;
      const viewport = makeLiveQueryViewport({
        client,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          currentStream = command.stream;
        },
      });
      const projected = makeSink<{ readonly id: string }>();
      const first = viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: projected.sink,
      });
      const oldFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;

      first.setWindow({ firstRow: 50, lastRow: 59 });
      const newFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      expect(requests[1]!.query).toStrictEqual({
        select: ["id"],
        where: [],
        orderBy: [],
        offset: 50,
        limit: 10,
      });

      yield* Queue.offer(requests[1]!.events, snapshot("new-window", 0, 2));
      yield* flush;
      yield* Queue.offer(requests[0]!.events, snapshot("old-window", 0, 1));
      yield* flush;
      expect(projected.rowData).toStrictEqual([{ 50: { id: "new-window", price: 0 } }]);

      const replacement = viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: {
          select: ["id"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [],
        },
        sink: projected.sink,
      });
      first.setWindow({ firstRow: 100, lastRow: 109 });
      expect(requests).toHaveLength(2);
      replacement.release();
      replacement.release();
      first.release();

      yield* Fiber.interrupt(oldFiber);
      yield* Fiber.interrupt(newFiber);
      yield* base.close;
    }),
  );

  it.effect("coalesces a large pre-activation scroll burst into the latest acquisition", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const client = makeManualClient(base.liveClient, requests);
      let currentStream: ViewportChromeStream = Stream.never;
      const viewport = makeLiveQueryViewport({
        client,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          currentStream = command.stream;
        },
      });
      const projected = makeSink<{ readonly id: string }>();
      const generation = viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: projected.sink,
      });

      for (let firstRow = 1; firstRow <= 10_000; firstRow += 1) {
        generation.setWindow({ firstRow, lastRow: firstRow + 9 });
      }
      expect(requests).toStrictEqual([]);
      expect(projected.rowCounts).toStrictEqual([]);

      const fiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      expect(requests).toHaveLength(1);
      expect(requests[0]!.query).toStrictEqual({
        select: ["id"],
        where: [],
        orderBy: [],
        offset: 10_000,
        limit: 10,
      });
      expect(projected.rowCounts).toStrictEqual([[0, false]]);

      generation.release();
      expect(projected.rowCounts).toStrictEqual([
        [0, false],
        [0, false],
      ]);
      yield* Fiber.interrupt(fiber);
      expect(requests[0]!.closes).toBe(1);
      yield* base.close;
    }),
  );

  it.effect("replaces filter, sort, selection, and query mode as distinct identities", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const client = makeManualClient(base.liveClient, requests);
      let currentStream: ViewportChromeStream = Stream.never;
      const viewport = makeLiveQueryViewport({
        client,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          currentStream = command.stream;
        },
      });
      const fibers: Array<Fiber.Fiber<void, unknown>> = [];
      const startCurrent = Effect.gen(function* () {
        const fiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
        fibers.push(fiber);
        yield* flush;
      });

      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: makeSink<{ readonly id: string }>().sink,
      });
      yield* startCurrent;
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: {
          select: ["id", "price"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "price", direction: "desc" }],
        },
        sink: makeSink<{ readonly id: string; readonly price: number }>().sink,
      });
      yield* startCurrent;
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: {
          groupBy: ["status"],
          aggregates: { rowCount: { aggFunc: "count" } },
          where: [],
          orderBy: [{ aggregate: "rowCount", direction: "desc" }],
        },
        sink: makeSink<{ readonly status: "open" | "closed"; readonly rowCount: bigint }>().sink,
      });
      yield* startCurrent;

      expect(requests.map(({ query }) => query)).toStrictEqual([
        { select: ["id"], where: [], orderBy: [], offset: 0, limit: 10 },
        {
          select: ["id", "price"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "price", direction: "desc" }],
          offset: 0,
          limit: 10,
        },
        {
          groupBy: ["status"],
          aggregates: { rowCount: { aggFunc: "count" } },
          where: [],
          orderBy: [{ aggregate: "rowCount", direction: "desc" }],
          offset: 0,
          limit: 10,
        },
      ]);

      for (const fiber of fibers) {
        yield* Fiber.interrupt(fiber);
      }
      viewport.destroy();
      yield* base.close;
    }),
  );

  it.effect("rejects invalid windows, retries closed identities, and retains error rows", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const client = makeManualClient(base.liveClient, requests);
      let currentStream: ViewportChromeStream = Stream.never;
      const viewport = makeLiveQueryViewport({
        client,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          currentStream = command.stream;
        },
      });
      const projected = makeSink<{ readonly id: string }>();
      viewport.replace({
        window: { firstRow: 2, lastRow: 1 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: projected.sink,
      });
      const invalidChrome = yield* currentStream.pipe(Stream.runHead);
      expect(invalidChrome).toStrictEqual(
        Option.some({
          totalRows: 0,
          version: 0,
          status: "error",
          statusCode: "InvalidQuery",
          message: "Live Query Viewport lastRow must be greater than or equal to firstRow.",
        }),
      );
      expect(requests).toHaveLength(0);

      const replaceValid = () =>
        viewport.replace({
          window: { firstRow: 0, lastRow: 9 },
          query: { select: ["id"], where: [], orderBy: [] },
          sink: projected.sink,
        });
      replaceValid();
      const firstFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      yield* Queue.offer(requests[0]!.events, {
        type: "status",
        topic: "orders",
        queryId: "manual",
        status: "closed",
        code: "SubscriptionClosed",
        message: "closed",
      });
      yield* flush;

      replaceValid();
      const retryFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      expect(requests).toHaveLength(2);
      yield* Queue.offer(requests[1]!.events, snapshot("retained", 7, 2));
      yield* flush;
      yield* Queue.offer(requests[1]!.events, {
        type: "status",
        topic: "orders",
        queryId: "manual",
        status: "error",
        code: "InvalidQuery",
        message: "terminal error",
      });
      yield* flush;
      expect(projected.rowData.at(-1)).toStrictEqual({ 0: { id: "retained", price: 7 } });
      expect(projected.rowCounts.at(-1)).toStrictEqual([1, true]);

      replaceValid();
      yield* flush;
      expect(requests).toHaveLength(2);
      yield* Queue.offer(requests[1]!.events, {
        type: "status",
        topic: "orders",
        queryId: "manual",
        status: "ready",
        code: "Ready",
      });
      yield* flush;
      expect(projected.rowData.at(-1)).toStrictEqual({ 0: { id: "retained", price: 7 } });
      expect(projected.rowCounts.at(-1)).toStrictEqual([1, true]);

      yield* Fiber.interrupt(firstFiber);
      yield* Fiber.interrupt(retryFiber);
      viewport.destroy();
      viewport.destroy();
      yield* base.close;
    }),
  );

  it.effect("does not overwrite a replacement started by a window accessor", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const client = makeManualClient(base.liveClient, requests);
      const published: Array<
        Stream.Stream<LiveQueryViewportChrome, ViewServerRuntimeError | ViewServerTransportError>
      > = [];
      const viewport = makeLiveQueryViewport({
        client,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          published.push(command.stream);
        },
      });
      const sink = makeSink<{ readonly id: string }>();
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: sink.sink,
      });
      const initialFiber = yield* published[0]!.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      const hostileWindow = { firstRow: 0, lastRow: 9 };
      let replaceOnRead = true;
      Object.defineProperty(hostileWindow, "firstRow", {
        enumerable: true,
        get: () => {
          if (replaceOnRead) {
            replaceOnRead = false;
            viewport.replace({
              window: { firstRow: 20, lastRow: 29 },
              query: {
                select: ["id"],
                where: [{ field: "status", type: "equals", filter: "closed" }],
                orderBy: [],
              },
              sink: sink.sink,
            });
          }
          return 0;
        },
      });

      const obsolete = viewport.replace({
        window: hostileWindow,
        query: { select: ["id"], where: [], orderBy: [] },
        sink: sink.sink,
      });
      expect(published).toHaveLength(2);
      const fiber = yield* published[1]!.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      expect(requests).toHaveLength(2);
      expect(requests[1]!.query).toStrictEqual({
        select: ["id"],
        where: [{ field: "status", type: "equals", filter: "closed" }],
        orderBy: [],
        offset: 20,
        limit: 10,
      });

      obsolete.setWindow({ firstRow: 30, lastRow: 39 });
      obsolete.release();
      expect(published).toHaveLength(2);
      viewport.destroy();
      yield* Fiber.interrupt(initialFiber);
      yield* Fiber.interrupt(fiber);
      yield* base.close;
    }),
  );

  it.effect("keeps a replacement installed by a setWindow accessor", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const published: Array<ViewportChromeStream> = [];
      const viewport = makeLiveQueryViewport({
        client: makeManualClient(base.liveClient, requests),
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          published.push(command.stream);
        },
      });
      const sink = makeSink<{ readonly id: string }>();
      const generation = viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: sink.sink,
      });
      const initialFiber = yield* published[0]!.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      const hostileWindow = { firstRow: 10, lastRow: 19 };
      Object.defineProperty(hostileWindow, "firstRow", {
        enumerable: true,
        get: () => {
          viewport.replace({
            window: { firstRow: 20, lastRow: 29 },
            query: {
              select: ["id"],
              where: [{ field: "status", type: "equals", filter: "open" }],
              orderBy: [],
            },
            sink: sink.sink,
          });
          return 10;
        },
      });

      generation.setWindow(hostileWindow);
      expect(published).toHaveLength(2);
      const successorFiber = yield* published[1]!.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      expect(requests).toHaveLength(2);
      expect(requests[1]!.query).toStrictEqual({
        select: ["id"],
        where: [{ field: "status", type: "equals", filter: "open" }],
        orderBy: [],
        offset: 20,
        limit: 10,
      });

      generation.setWindow({ firstRow: 30, lastRow: 39 });
      generation.release();
      expect(published).toHaveLength(2);
      viewport.destroy();
      yield* Fiber.interrupt(initialFiber);
      yield* Fiber.interrupt(successorFiber);
      yield* base.close;
    }),
  );

  it.effect("does not resurrect a generation released by a setWindow accessor", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const published: Array<ViewportChromeStream> = [];
      const viewport = makeLiveQueryViewport({
        client: makeManualClient(base.liveClient, requests),
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          published.push(command.stream);
        },
      });
      const generation = viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: makeSink<{ readonly id: string }>().sink,
      });
      const fiber = yield* published[0]!.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      const hostileWindow = { firstRow: 10, lastRow: 19 };
      Object.defineProperty(hostileWindow, "firstRow", {
        enumerable: true,
        get: () => {
          generation.release();
          return 10;
        },
      });

      generation.setWindow(hostileWindow);
      expect(requests).toHaveLength(1);
      expect(published).toHaveLength(2);
      expect(yield* published[1]!.pipe(Stream.runHead)).toStrictEqual(
        Option.some({
          totalRows: 0,
          version: 0,
          status: "loading",
        }),
      );
      generation.setWindow({ firstRow: 30, lastRow: 39 });
      generation.release();
      expect(published).toHaveLength(2);
      yield* Fiber.interrupt(fiber);
      viewport.destroy();
      yield* base.close;
    }),
  );

  it.effect("keeps identical replacement ownership transferred by a window accessor", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const published: Array<ViewportChromeStream> = [];
      const viewport = makeLiveQueryViewport({
        client: makeManualClient(base.liveClient, requests),
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          published.push(command.stream);
        },
      });
      const sink = makeSink<{ readonly id: string }>();
      const first = viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: sink.sink,
      });
      const initialFiber = yield* published[0]!.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      let successor: LiveQueryViewportGeneration | undefined;
      const hostileWindow = { firstRow: 0, lastRow: 9 };
      Object.defineProperty(hostileWindow, "firstRow", {
        enumerable: true,
        get: () => {
          successor = viewport.replace({
            window: { firstRow: 0, lastRow: 9 },
            query: { select: ["id"], where: [], orderBy: [] },
            sink: sink.sink,
          });
          return 0;
        },
      });

      const obsolete = viewport.replace({
        window: hostileWindow,
        query: { select: ["id"], where: [], orderBy: [] },
        sink: sink.sink,
      });
      expect(published).toHaveLength(1);
      successor!.setWindow({ firstRow: 20, lastRow: 29 });
      expect(published).toHaveLength(2);
      const successorFiber = yield* published[1]!.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      expect(requests).toHaveLength(2);
      expect(requests[1]!.query).toStrictEqual({
        select: ["id"],
        where: [],
        orderBy: [],
        offset: 20,
        limit: 10,
      });

      first.release();
      obsolete.setWindow({ firstRow: 30, lastRow: 39 });
      obsolete.release();
      expect(published).toHaveLength(2);
      successor!.release();
      expect(published).toHaveLength(3);
      yield* Fiber.interrupt(initialFiber);
      yield* Fiber.interrupt(successorFiber);
      viewport.destroy();
      yield* base.close;
    }),
  );

  it.effect("keeps invalid-query ownership installed by a window accessor", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const published: Array<ViewportChromeStream> = [];
      const viewport = makeLiveQueryViewport({
        client: makeManualClient(base.liveClient, requests),
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          published.push(command.stream);
        },
      });
      const sink = makeSink<{ readonly id: string }>();
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: sink.sink,
      });
      const initialFiber = yield* published[0]!.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      const hostileQuery = {
        select: ["id"],
        where: [],
        orderBy: [],
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: readonly [];
        readonly orderBy: readonly [];
      };
      Object.defineProperty(hostileQuery, "where", {
        enumerable: true,
        get: () => {
          throw new Error("hostile query");
        },
      });
      const hostileWindow = { firstRow: 10, lastRow: 19 };
      Object.defineProperty(hostileWindow, "firstRow", {
        enumerable: true,
        get: () => {
          viewport.replace({
            window: { firstRow: 20, lastRow: 29 },
            query: hostileQuery,
            sink: sink.sink,
          });
          return 10;
        },
      });

      const obsolete = viewport.replace({
        window: hostileWindow,
        query: { select: ["id"], where: [], orderBy: [] },
        sink: sink.sink,
      });
      expect(requests).toHaveLength(1);
      expect(published).toHaveLength(2);
      expect(yield* published[1]!.pipe(Stream.runHead)).toStrictEqual(
        Option.some({
          totalRows: 0,
          version: 0,
          status: "error",
          statusCode: "InvalidQuery",
          message: "Query input fields must be own enumerable data properties.",
        }),
      );
      obsolete.setWindow({ firstRow: 30, lastRow: 39 });
      obsolete.release();
      expect(published).toHaveLength(2);
      yield* Fiber.interrupt(initialFiber);
      viewport.destroy();
      yield* base.close;
    }),
  );

  it.effect("does not overwrite invalid-query release from a window accessor", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const published: Array<ViewportChromeStream> = [];
      const viewport = makeLiveQueryViewport({
        client: makeManualClient(base.liveClient, requests),
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          published.push(command.stream);
        },
      });
      const hostileQuery = {
        select: ["id"],
        where: [],
        orderBy: [],
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: readonly [];
        readonly orderBy: readonly [];
      };
      Object.defineProperty(hostileQuery, "where", {
        enumerable: true,
        get: () => {
          throw new Error("hostile query");
        },
      });
      const invalidGeneration = viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: hostileQuery,
        sink: makeSink<{ readonly id: string }>().sink,
      });
      const hostileWindow = { firstRow: 10, lastRow: 19 };
      Object.defineProperty(hostileWindow, "firstRow", {
        enumerable: true,
        get: () => {
          invalidGeneration.release();
          return 10;
        },
      });

      const obsolete = viewport.replace({
        window: hostileWindow,
        query: { select: ["id"], where: [], orderBy: [] },
        sink: makeSink<{ readonly id: string }>().sink,
      });
      expect(requests).toStrictEqual([]);
      expect(published).toHaveLength(2);
      expect(yield* published[1]!.pipe(Stream.runHead)).toStrictEqual(
        Option.some({
          totalRows: 0,
          version: 0,
          status: "loading",
        }),
      );
      obsolete.setWindow({ firstRow: 30, lastRow: 39 });
      obsolete.release();
      invalidGeneration.release();
      expect(published).toHaveLength(2);
      viewport.destroy();
      yield* base.close;
    }),
  );

  it.effect("does not start a request when a window accessor destroys the viewport", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const published: Array<
        Stream.Stream<LiveQueryViewportChrome, ViewServerRuntimeError | ViewServerTransportError>
      > = [];
      const viewport = makeLiveQueryViewport({
        client: makeManualClient(base.liveClient, requests),
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          published.push(command.stream);
        },
      });
      const hostileWindow = { firstRow: 0, lastRow: 9 };
      Object.defineProperty(hostileWindow, "firstRow", {
        enumerable: true,
        get: () => {
          viewport.destroy();
          return 0;
        },
      });

      const obsolete = viewport.replace({
        window: hostileWindow,
        query: { select: ["id"], where: [], orderBy: [] },
        sink: makeSink<{ readonly id: string }>().sink,
      });
      expect(requests).toStrictEqual([]);
      expect(published).toHaveLength(1);
      expect(yield* published[0]!.pipe(Stream.runHead)).toStrictEqual(
        Option.some({
          totalRows: 0,
          version: 0,
          status: "loading",
        }),
      );
      obsolete.setWindow({ firstRow: 30, lastRow: 39 });
      obsolete.release();
      expect(published).toHaveLength(1);
      yield* base.close;
    }),
  );

  it.effect("does not publish invalid-window chrome after sink clearing destroys ownership", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const published: Array<
        Stream.Stream<LiveQueryViewportChrome, ViewServerRuntimeError | ViewServerTransportError>
      > = [];
      const viewport = makeLiveQueryViewport({
        client: base.liveClient,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          published.push(command.stream);
        },
      });
      viewport.replace({
        window: { firstRow: 2, lastRow: 1 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: {
          setRowCount: () => {
            viewport.destroy();
          },
          setRowData: () => undefined,
        },
      });

      expect(yield* published[0]!.pipe(Stream.runHead)).toStrictEqual(Option.none());
      expect(published).toHaveLength(2);
      expect(yield* published[1]!.pipe(Stream.runHead)).toStrictEqual(
        Option.some({
          totalRows: 0,
          version: 0,
          status: "loading",
        }),
      );
      yield* base.close;
    }),
  );

  it.effect("projects every Delta operation shape and ignores non-row events", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const client = makeManualClient(base.liveClient, requests);
      let currentStream: ViewportChromeStream = Stream.never;
      const viewport = makeLiveQueryViewport({
        client,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          currentStream = command.stream;
        },
      });
      const projected = makeSink<{ readonly id: string; readonly price: number }>();
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id", "price"], where: [], orderBy: [] },
        sink: projected.sink,
      });
      const fiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      yield* Queue.offer(
        requests[0]!.events,
        snapshotMany(
          [
            { id: "a", price: 1 },
            { id: "b", price: 2 },
            { id: "c", price: 3 },
          ],
          1,
        ),
      );
      yield* Queue.offer(
        requests[0]!.events,
        deltaMany([{ type: "insert", key: "x", row: { id: "x", price: 4 }, index: 1 }], 4, 1, 2),
      );
      yield* Queue.offer(
        requests[0]!.events,
        deltaMany([{ type: "move", key: "c", fromIndex: 3, toIndex: 0 }], 4, 2, 3),
      );
      yield* Queue.offer(requests[0]!.events, deltaMany([{ type: "remove", key: "x" }], 3, 3, 4));
      yield* Queue.offer(requests[0]!.events, deltaMany([], 3, 4, 5));
      yield* Queue.offer(
        requests[0]!.events,
        deltaMany([{ type: "remove", key: "missing" }], 3, 5, 6),
      );
      yield* Queue.offer(requests[0]!.events, {
        type: "status",
        topic: "orders",
        queryId: "manual",
        status: "ready",
        code: "Ready",
      });
      yield* flush;

      expect(projected.rowData).toStrictEqual([
        {
          0: { id: "a", price: 1 },
          1: { id: "b", price: 2 },
          2: { id: "c", price: 3 },
        },
        {
          1: { id: "x", price: 4 },
          2: { id: "b", price: 2 },
          3: { id: "c", price: 3 },
        },
        {
          0: { id: "c", price: 3 },
          1: { id: "a", price: 1 },
          2: { id: "x", price: 4 },
          3: { id: "b", price: 2 },
        },
        { 2: { id: "b", price: 2 } },
      ]);
      expect(projected.rowKeys).toStrictEqual([
        { 0: "a", 1: "b", 2: "c" },
        { 1: "x", 2: "b", 3: "c" },
        { 0: "c", 1: "a", 2: "x", 3: "b" },
        { 2: "b" },
      ]);

      yield* Fiber.interrupt(fiber);
      viewport.destroy();
      yield* base.close;
    }),
  );

  it.effect("deduplicates a live identity and treats an unchanged window as a no-op", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const client = makeManualClient(base.liveClient, requests);
      let currentStream: ViewportChromeStream = Stream.never;
      let publishCount = 0;
      const viewport = makeLiveQueryViewport({
        client,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          publishCount += 1;
          currentStream = command.stream;
        },
      });
      const projected = makeSink<{ readonly id: string }>();
      const replace = () =>
        viewport.replace({
          window: { firstRow: 0, lastRow: 9 },
          query: { select: ["id"], where: [], orderBy: [] },
          sink: projected.sink,
        });
      const first = replace();
      const duplicate = replace();
      duplicate.setWindow({ firstRow: 0, lastRow: 9 });
      first.setWindow({ firstRow: 20, lastRow: 29 });
      first.release();
      expect(publishCount).toBe(1);

      const fiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      expect(requests).toHaveLength(1);
      yield* Queue.offer(requests[0]!.events, snapshot("still-current", 1, 1));
      yield* flush;
      expect(projected.rowData).toStrictEqual([{ 0: { id: "still-current", price: 1 } }]);

      duplicate.setWindow({ firstRow: 10, lastRow: 19 });
      expect(publishCount).toBe(2);
      const replacementFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      expect(requests).toHaveLength(2);
      duplicate.release();
      yield* Fiber.interrupt(fiber);
      yield* Fiber.interrupt(replacementFiber);
      yield* base.close;
    }),
  );

  it.effect("surfaces current subscription failures while suppressing obsolete failures", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const failure = {
        _tag: "ViewServerRuntimeError" as const,
        code: "InvalidQuery" as const,
        message: "subscription failed",
      };
      const failingClient = {
        ...base.liveClient,
        subscribe: (topic, query) =>
          base.liveClient.subscribe(topic, query).pipe(Effect.flatMap(() => Effect.fail(failure))),
      } satisfies ViewServerLiveClient<Topics>;
      let currentStream: ViewportChromeStream = Stream.never;
      const viewport = makeLiveQueryViewport({
        client: failingClient,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          currentStream = command.stream;
        },
      });
      const projected = makeSink<{ readonly id: string }>();
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: projected.sink,
      });

      const actualFailure = yield* currentStream.pipe(Stream.runDrain, Effect.flip);
      expect(actualFailure).toStrictEqual(failure);
      viewport.destroy();
      yield* base.close;
    }),
  );

  it.effect("clears ready rows after a current stream defect and permits an identical retry", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      let subscribeCount = 0;
      let closeCount = 0;
      const defectSignals: Array<Deferred.Deferred<void>> = [];
      const client = {
        ...base.liveClient,
        subscribe: adaptQuerySubstrate(() =>
          Effect.gen(function* () {
            subscribeCount += 1;
            const defectSignal = yield* Deferred.make<void>();
            defectSignals.push(defectSignal);
            return {
              events: Stream.concat(
                Stream.make(snapshot("ready-before-error", 1, 1)),
                Stream.fromEffect(
                  Deferred.await(defectSignal).pipe(
                    Effect.flatMap(() => Effect.die("stream defect")),
                  ),
                ),
              ),
              close: () =>
                Effect.sync(() => {
                  closeCount += 1;
                }),
            };
          }),
        ),
      } satisfies ViewServerLiveClient<Topics>;
      let currentStream: ViewportChromeStream = Stream.never;
      const viewport = makeLiveQueryViewport({
        client,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          currentStream = command.stream;
        },
      });
      const projected = makeSink<{ readonly id: string }>();
      const replace = () =>
        viewport.replace({
          window: { firstRow: 0, lastRow: 9 },
          query: { select: ["id"], where: [], orderBy: [] },
          sink: projected.sink,
        });

      replace();
      const firstFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      expect(projected.rowData).toStrictEqual([{ 0: { id: "ready-before-error", price: 1 } }]);
      yield* Deferred.succeed(defectSignals[0]!, undefined);
      const actualExit = yield* Fiber.await(firstFiber);
      expect(actualExit._tag).toBe("Failure");
      expect(projected.rowCounts.at(-1)).toStrictEqual([0, false]);
      expect(closeCount).toBe(1);

      replace();
      const retryFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      yield* Deferred.succeed(defectSignals[1]!, undefined);
      const retryExit = yield* Fiber.await(retryFiber);
      expect(retryExit._tag).toBe("Failure");
      expect(subscribeCount).toBe(2);
      expect(closeCount).toBe(2);
      viewport.destroy();
      yield* base.close;
    }),
  );

  it.effect("drops obsolete acquisition failures and reentrant event or sink writes", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const oldFailure = yield* Deferred.make<void>();
      const requests: Array<ManualRequest> = [];
      let subscriptionAttempt = 0;
      const client = {
        ...base.liveClient,
        subscribe: adaptQuerySubstrate((_, query) =>
          Effect.gen(function* () {
            subscriptionAttempt += 1;
            if (subscriptionAttempt === 1) {
              yield* Deferred.await(oldFailure);
              return yield* Effect.fail({
                _tag: "ViewServerRuntimeError" as const,
                code: "InvalidQuery" as const,
                message: "obsolete failure",
              });
            }
            const events = yield* Queue.unbounded<ViewServerLiveEvent<object>>();
            const request: ManualRequest = { query, events, closes: 0 };
            requests.push(request);
            return {
              events: Stream.fromQueue(events),
              close: () => Effect.void,
            };
          }),
        ),
      } satisfies ViewServerLiveClient<Topics>;
      let currentStream: ViewportChromeStream = Stream.never;
      const viewport = makeLiveQueryViewport({
        client,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          currentStream = command.stream;
        },
      });
      const firstSink = makeSink<{ readonly id: string }>();
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: firstSink.sink,
      });
      const oldFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;

      const secondSink = makeSink<{ readonly id: string }>();
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: {
          select: ["id"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [],
        },
        sink: secondSink.sink,
      });
      const currentFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      yield* Deferred.succeed(oldFailure, undefined);
      yield* flush;
      expect(requests).toHaveLength(1);
      const oldExit = yield* Fiber.await(oldFiber);
      expect(oldExit._tag).toBe("Success");

      const reentrantEvent = snapshot("reentrant-event", 1, 1);
      Object.defineProperty(reentrantEvent, "type", {
        enumerable: true,
        get: () => {
          viewport.replace({
            window: { firstRow: 10, lastRow: 19 },
            query: {
              select: ["id"],
              where: [{ field: "status", type: "equals", filter: "closed" }],
              orderBy: [],
            },
            sink: secondSink.sink,
          });
          return "snapshot";
        },
      });
      yield* Queue.offer(requests[0]!.events, reentrantEvent);
      yield* flush;
      expect(secondSink.rowData).toStrictEqual([]);

      const sinkReentryRequests: Array<ManualRequest> = [];
      const sinkReentryClient = makeManualClient(base.liveClient, sinkReentryRequests);
      let sinkReentryStream: ViewportChromeStream = Stream.never;
      const sinkReentryViewport = makeLiveQueryViewport({
        client: sinkReentryClient,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          sinkReentryStream = command.stream;
        },
      });
      const reentrantSink: LiveQueryViewportSink<{ readonly id: string }> = {
        setRowCount: (_, keepRenderedRows) => {
          if (keepRenderedRows === true) {
            sinkReentryViewport.replace({
              window: { firstRow: 10, lastRow: 19 },
              query: {
                select: ["id"],
                where: [{ field: "status", type: "equals", filter: "closed" }],
                orderBy: [],
              },
              sink: reentrantSink,
            });
          }
        },
        setRowData: () => undefined,
      };
      sinkReentryViewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: reentrantSink,
      });
      const sinkFiber = yield* sinkReentryStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      yield* Queue.offer(sinkReentryRequests[0]!.events, snapshot("sink-reentry", 1, 1));
      yield* flush;

      yield* Fiber.interrupt(oldFiber);
      yield* Fiber.interrupt(currentFiber);
      yield* Fiber.interrupt(sinkFiber);
      viewport.destroy();
      sinkReentryViewport.destroy();
      yield* base.close;
    }),
  );

  it.effect("reports hostile query access without starting a subscription", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const client = makeManualClient(base.liveClient, requests);
      const published: Array<
        Stream.Stream<LiveQueryViewportChrome, ViewServerRuntimeError | ViewServerTransportError>
      > = [];
      const viewport = makeLiveQueryViewport({
        client,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          published.push(command.stream);
        },
      });
      const hostile = {
        select: ["id"],
        where: [],
        orderBy: [],
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: readonly [];
        readonly orderBy: readonly [];
      };
      Object.defineProperty(hostile, "where", {
        enumerable: true,
        get: () => {
          throw new Error("hostile query");
        },
      });
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: { setRowCount: () => undefined, setRowData: () => undefined },
      });
      const hostileGeneration = viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: hostile,
        sink: { setRowCount: () => undefined, setRowData: () => undefined },
      });
      expect(yield* published[1]!.pipe(Stream.runHead)).toStrictEqual(
        Option.some({
          totalRows: 0,
          version: 0,
          status: "error",
          statusCode: "InvalidQuery",
          message: "Query input fields must be own enumerable data properties.",
        }),
      );
      hostileGeneration.setWindow({ firstRow: 10, lastRow: 19 });
      hostileGeneration.release();
      hostileGeneration.release();
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: hostile,
        sink: { setRowCount: () => undefined, setRowData: () => undefined },
      });
      viewport.destroy();

      expect(requests).toHaveLength(0);
      expect(published).toHaveLength(5);
      yield* base.close;
    }),
  );

  it.effect("makes destroy during invalid-query sink clearing terminal", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const client = makeManualClient(base.liveClient, requests);
      const published: Array<
        Stream.Stream<LiveQueryViewportChrome, ViewServerRuntimeError | ViewServerTransportError>
      > = [];
      const hostile = {
        select: ["id"],
        where: [],
        orderBy: [],
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: readonly [];
        readonly orderBy: readonly [];
      };
      Object.defineProperty(hostile, "where", {
        enumerable: true,
        get: () => {
          throw new Error("hostile query");
        },
      });
      const viewport = makeLiveQueryViewport({
        client,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          published.push(command.stream);
        },
      });
      let destroyInvalidOnClear = true;
      const invalidReentrantGeneration = viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: hostile,
        sink: {
          setRowCount: () => {
            if (destroyInvalidOnClear) {
              destroyInvalidOnClear = false;
              viewport.destroy();
            }
          },
          setRowData: () => undefined,
        },
      });
      invalidReentrantGeneration.setWindow({ firstRow: 10, lastRow: 19 });
      invalidReentrantGeneration.release();

      expect(requests).toHaveLength(0);
      expect(yield* published[0]!.pipe(Stream.runHead)).toStrictEqual(Option.none());
      expect(published).toHaveLength(3);
      expect(yield* published[1]!.pipe(Stream.runHead)).toStrictEqual(
        Option.some({
          totalRows: 0,
          version: 0,
          status: "loading",
        }),
      );
      expect(yield* published[2]!.pipe(Stream.runHead)).toStrictEqual(
        Option.some({
          totalRows: 0,
          version: 0,
          status: "loading",
        }),
      );
      yield* base.close;
    }),
  );

  it.effect("preserves a cleanup defect after activation is made obsolete reentrantly", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const published: Array<
        Stream.Stream<LiveQueryViewportChrome, ViewServerRuntimeError | ViewServerTransportError>
      > = [];
      const viewport = makeLiveQueryViewport({
        client: base.liveClient,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          published.push(command.stream);
        },
      });
      const successor = makeSink<{ readonly id: string }>();
      let clearCount = 0;
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: {
          setRowCount: () => {
            clearCount += 1;
            if (clearCount === 1) {
              viewport.replace({
                window: { firstRow: 10, lastRow: 19 },
                query: { select: ["id"], where: [], orderBy: [] },
                sink: successor.sink,
              });
              return;
            }
            throw new Error("cleanup defect");
          },
          setRowData: () => undefined,
        },
      });

      const obsoleteExit = yield* published[0]!.pipe(Stream.runHead, Effect.exit);
      expect(obsoleteExit._tag).toBe("Failure");
      expect(clearCount).toBe(2);
      expect(published).toHaveLength(2);

      viewport.destroy();
      expect(successor.rowCounts).toStrictEqual([[0, false]]);
      yield* base.close;
    }),
  );

  it.effect("makes destroy during query snapshotting terminal", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const client = makeManualClient(base.liveClient, requests);
      const published: Array<
        Stream.Stream<LiveQueryViewportChrome, ViewServerRuntimeError | ViewServerTransportError>
      > = [];
      const viewport = makeLiveQueryViewport({
        client,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          published.push(command.stream);
        },
      });
      const reentrantQueryTarget = {
        select: ["id"],
        where: [],
        orderBy: [],
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: readonly [];
        readonly orderBy: readonly [];
      };
      const reentrantQuery = new Proxy(reentrantQueryTarget, {
        ownKeys: (target) => {
          viewport.destroy();
          return Reflect.ownKeys(target);
        },
      });
      const obsoleteGeneration = viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: reentrantQuery,
        sink: { setRowCount: () => undefined, setRowData: () => undefined },
      });
      obsoleteGeneration.setWindow({ firstRow: 10, lastRow: 19 });
      obsoleteGeneration.release();

      expect(requests).toHaveLength(0);
      expect(published).toHaveLength(1);
      expect(yield* published[0]!.pipe(Stream.runHead)).toStrictEqual(
        Option.some({
          totalRows: 0,
          version: 0,
          status: "loading",
        }),
      );
      yield* base.close;
    }),
  );

  it.effect("makes destroy during a valid-query sink clear terminal", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const client = makeManualClient(base.liveClient, requests);
      const published: Array<
        Stream.Stream<LiveQueryViewportChrome, ViewServerRuntimeError | ViewServerTransportError>
      > = [];
      const viewport = makeLiveQueryViewport({
        client,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          published.push(command.stream);
        },
      });
      let destroyOnClear = true;
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: {
          setRowCount: () => {
            if (destroyOnClear) {
              destroyOnClear = false;
              viewport.destroy();
            }
          },
          setRowData: () => undefined,
        },
      });

      expect(requests).toHaveLength(0);
      expect(yield* published[0]!.pipe(Stream.runHead)).toStrictEqual(Option.none());
      expect(published).toHaveLength(2);
      expect(yield* published[1]!.pipe(Stream.runHead)).toStrictEqual(
        Option.some({
          totalRows: 0,
          version: 0,
          status: "loading",
        }),
      );
      yield* base.close;
    }),
  );

  it.effect("publishes cancellation before release cleanup installs a replacement", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const client = makeManualClient(base.liveClient, requests);
      let publishCount = 0;
      const viewport = makeLiveQueryViewport({
        client,
        config: viewServer,
        topic: "orders",
        publish: () => {
          publishCount += 1;
        },
      });
      let replaceDuringRelease = false;
      const sink = {
        setRowCount: () => {
          if (replaceDuringRelease) {
            replaceDuringRelease = false;
            viewport.replace({
              window: { firstRow: 10, lastRow: 19 },
              query: { select: ["id"], where: [], orderBy: [] },
              sink,
            });
          }
        },
        setRowData: () => undefined,
      };
      const generation = viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink,
      });
      replaceDuringRelease = true;
      generation.release();

      expect(publishCount).toBe(3);
      viewport.destroy();
      yield* base.close;
    }),
  );

  it.effect("cancels the subscription before release cleanup can throw", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const publisher = makeSwitchingPublisher();
      let throwOnClear = false;
      const rowData: Array<{ readonly [index: number]: { readonly id: string } }> = [];
      const viewport = makeLiveQueryViewport({
        client: makeManualClient(base.liveClient, requests),
        config: viewServer,
        topic: "orders",
        publish: publisher.publish,
      });
      const generation = viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: {
          setRowCount: () => {
            if (throwOnClear) {
              throw new Error("disposed release sink");
            }
          },
          setRowData: (rows) => {
            rowData.push(rows);
          },
        },
      });
      yield* flush;
      expect(requests).toHaveLength(1);

      throwOnClear = true;
      expect(() => generation.release()).toThrow("disposed release sink");
      yield* flush;
      expect(publisher.publishCount()).toBe(2);
      expect(requests[0]!.closes).toBe(1);
      yield* Queue.offer(requests[0]!.events, snapshot("late", 1, 1));
      yield* flush;
      expect(rowData).toStrictEqual([]);
      viewport.destroy();
      yield* base.close;
    }),
  );

  it.effect("cancels the subscription before terminal destroy cleanup can throw", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const publisher = makeSwitchingPublisher();
      let throwOnClear = false;
      const viewport = makeLiveQueryViewport({
        client: makeManualClient(base.liveClient, requests),
        config: viewServer,
        topic: "orders",
        publish: publisher.publish,
      });
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: {
          setRowCount: () => {
            if (throwOnClear) {
              throw new Error("disposed destroy sink");
            }
          },
          setRowData: () => undefined,
        },
      });
      yield* flush;
      expect(requests).toHaveLength(1);

      throwOnClear = true;
      expect(() => viewport.destroy()).toThrow("disposed destroy sink");
      yield* flush;
      expect(publisher.publishCount()).toBe(2);
      expect(requests[0]!.closes).toBe(1);
      viewport.replace({
        window: { firstRow: 10, lastRow: 19 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: { setRowCount: () => undefined, setRowData: () => undefined },
      });
      expect(publisher.publishCount()).toBe(2);
      yield* base.close;
    }),
  );

  it.effect("cancels the old subscription before previous-sink cleanup can throw", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const publisher = makeSwitchingPublisher();
      let throwOnClear = false;
      const viewport = makeLiveQueryViewport({
        client: makeManualClient(base.liveClient, requests),
        config: viewServer,
        topic: "orders",
        publish: publisher.publish,
      });
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: {
          setRowCount: () => {
            if (throwOnClear) {
              throw new Error("disposed previous sink");
            }
          },
          setRowData: () => undefined,
        },
      });
      yield* flush;
      expect(requests).toHaveLength(1);

      throwOnClear = true;
      viewport.replace({
        window: { firstRow: 10, lastRow: 19 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: { setRowCount: () => undefined, setRowData: () => undefined },
      });
      const failed = publisher.current();
      expect(failed).toBeDefined();
      expect((yield* Fiber.await(failed!))._tag).toBe("Failure");
      expect(requests[0]!.closes).toBe(1);
      expect(requests).toHaveLength(1);

      throwOnClear = false;
      viewport.replace({
        window: { firstRow: 20, lastRow: 29 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: { setRowCount: () => undefined, setRowData: () => undefined },
      });
      yield* flush;
      expect(requests).toHaveLength(2);
      viewport.destroy();
      yield* base.close;
    }),
  );

  it.effect("does not acquire after the successor sink initial clear throws", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const publisher = makeSwitchingPublisher();
      const viewport = makeLiveQueryViewport({
        client: makeManualClient(base.liveClient, requests),
        config: viewServer,
        topic: "orders",
        publish: publisher.publish,
      });
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: { setRowCount: () => undefined, setRowData: () => undefined },
      });
      yield* flush;
      expect(requests).toHaveLength(1);

      viewport.replace({
        window: { firstRow: 10, lastRow: 19 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: {
          setRowCount: () => {
            throw new Error("disposed successor sink");
          },
          setRowData: () => undefined,
        },
      });
      const failed = publisher.current();
      expect(failed).toBeDefined();
      expect((yield* Fiber.await(failed!))._tag).toBe("Failure");
      expect(requests[0]!.closes).toBe(1);
      expect(requests).toHaveLength(1);

      viewport.replace({
        window: { firstRow: 20, lastRow: 29 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: { setRowCount: () => undefined, setRowData: () => undefined },
      });
      yield* flush;
      expect(requests).toHaveLength(2);
      viewport.destroy();
      yield* base.close;
    }),
  );

  it.effect("clears a superseded sink and gives its replacement exclusive ownership", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const client = makeManualClient(base.liveClient, requests);
      const first = makeSink<{ readonly id: string }>();
      const second = makeSink<{ readonly id: string }>();
      let currentStream: ViewportChromeStream = Stream.never;
      const viewport = makeLiveQueryViewport({
        client,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          currentStream = command.stream;
        },
      });

      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: first.sink,
      });
      const firstFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      viewport.replace({
        window: { firstRow: 10, lastRow: 19 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: second.sink,
      });
      yield* Fiber.interrupt(firstFiber);
      const secondFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;

      expect(first.rowCounts).toStrictEqual([
        [0, false],
        [0, false],
      ]);
      expect(second.rowCounts).toStrictEqual([[0, false]]);
      viewport.destroy();
      yield* Fiber.interrupt(secondFiber);
      yield* base.close;
    }),
  );

  it.effect("clears both sinks when a replacement is released before activation", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const first = makeSink<{ readonly id: string }>();
      const second = makeSink<{ readonly id: string }>();
      let currentStream: ViewportChromeStream = Stream.never;
      const viewport = makeLiveQueryViewport({
        client: makeManualClient(base.liveClient, requests),
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          currentStream = command.stream;
        },
      });
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: first.sink,
      });
      const firstFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      yield* Queue.offer(requests[0]!.events, snapshot("rendered", 1, 4));
      yield* flush;

      const replacement = viewport.replace({
        window: { firstRow: 10, lastRow: 19 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: second.sink,
      });
      yield* Fiber.interrupt(firstFiber);
      replacement.release();
      yield* Queue.offer(requests[0]!.events, snapshot("late", 2, 5));
      yield* flush;

      expect(requests).toHaveLength(1);
      expect(requests[0]!.closes).toBe(1);
      expect(first.rowCounts).toStrictEqual([
        [0, false],
        [1, true],
        [0, false],
      ]);
      expect(first.rowData).toStrictEqual([{ 0: { id: "rendered", price: 1 } }]);
      expect(second.rowCounts).toStrictEqual([[0, false]]);
      expect(second.rowData).toStrictEqual([]);
      viewport.destroy();
      yield* base.close;
    }),
  );

  it.effect("clears both sinks when destroyed before replacement activation", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const first = makeSink<{ readonly id: string }>();
      const second = makeSink<{ readonly id: string }>();
      let currentStream: ViewportChromeStream = Stream.never;
      const viewport = makeLiveQueryViewport({
        client: makeManualClient(base.liveClient, requests),
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          currentStream = command.stream;
        },
      });
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: first.sink,
      });
      const firstFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      viewport.replace({
        window: { firstRow: 10, lastRow: 19 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: second.sink,
      });
      yield* Fiber.interrupt(firstFiber);
      viewport.destroy();

      expect(requests).toHaveLength(1);
      expect(requests[0]!.closes).toBe(1);
      expect(first.rowCounts).toStrictEqual([
        [0, false],
        [0, false],
      ]);
      expect(second.rowCounts).toStrictEqual([[0, false]]);
      yield* base.close;
    }),
  );

  it.effect("does not clear a cleanup-installed successor sharing the pending sink", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const successor = makeSink<{ readonly id: string }>();
      let installSuccessor = false;
      let currentStream: ViewportChromeStream = Stream.never;
      const viewport = makeLiveQueryViewport({
        client: makeManualClient(base.liveClient, requests),
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          currentStream = command.stream;
        },
      });
      const previousSink: LiveQueryViewportSink<{ readonly id: string }> = {
        setRowCount: (count) => {
          if (count === 0 && installSuccessor) {
            installSuccessor = false;
            viewport.replace({
              window: { firstRow: 20, lastRow: 29 },
              query: { select: ["id"], where: [], orderBy: [] },
              sink: successor.sink,
            });
          }
        },
        setRowData: () => undefined,
      };
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: previousSink,
      });
      const firstFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      const pending = viewport.replace({
        window: { firstRow: 10, lastRow: 19 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: successor.sink,
      });
      yield* Fiber.interrupt(firstFiber);
      installSuccessor = true;
      pending.release();
      expect(successor.rowCounts).toStrictEqual([]);

      const successorFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      expect(successor.rowCounts).toStrictEqual([[0, false]]);
      expect(requests).toHaveLength(2);
      viewport.destroy();
      yield* Fiber.interrupt(successorFiber);
      yield* base.close;
    }),
  );

  it.effect("does not write a superseded request after sink cleanup installs its successor", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      const requests: Array<ManualRequest> = [];
      const client = makeManualClient(base.liveClient, requests);
      const successor = makeSink<{ readonly id: string }>();
      let replaceDuringClear = false;
      let publishCount = 0;
      let currentStream: ViewportChromeStream = Stream.never;
      const previousRowCounts: Array<readonly [number, boolean | undefined]> = [];
      const viewport = makeLiveQueryViewport({
        client,
        config: viewServer,
        topic: "orders",
        publish: (command) => {
          publishCount += 1;
          currentStream = command.stream;
        },
      });
      const previous = {
        sink: {
          setRowCount: (count: number, keepRenderedRows?: boolean) => {
            previousRowCounts.push([count, keepRenderedRows]);
            if (replaceDuringClear) {
              replaceDuringClear = false;
              viewport.replace({
                window: { firstRow: 20, lastRow: 29 },
                query: { select: ["id"], where: [], orderBy: [] },
                sink: successor.sink,
              });
            }
          },
          setRowData: () => undefined,
        },
      };

      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: previous.sink,
      });
      const previousFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;
      replaceDuringClear = true;
      viewport.replace({
        window: { firstRow: 10, lastRow: 19 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: successor.sink,
      });
      yield* Fiber.interrupt(previousFiber);
      expect(yield* currentStream.pipe(Stream.runHead)).toStrictEqual(Option.none());
      const successorFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
      yield* flush;

      expect(previousRowCounts).toStrictEqual([
        [0, false],
        [0, false],
      ]);
      expect(successor.rowCounts).toStrictEqual([[0, false]]);
      expect(publishCount).toBe(3);
      expect(requests).toHaveLength(2);
      viewport.destroy();
      yield* Fiber.interrupt(successorFiber);
      yield* base.close;
    }),
  );

  it.effect(
    "commits controller replacements before late events and finalizers can touch the sink",
    () =>
      Effect.gen(function* () {
        const base = createInMemoryViewServer(viewServer);
        const oldRequests: Array<ManualRequest> = [];
        const currentRequests: Array<ManualRequest> = [];
        const binding = makeLiveQueryViewportBinding<Topics, "orders">();
        const sink = makeSink<{ readonly id: string }>();
        let oldStream: ViewportChromeStream = Stream.never;
        let currentStream: ViewportChromeStream = Stream.never;
        const oldViewport = makeLiveQueryViewport({
          client: makeManualClient(base.liveClient, oldRequests),
          config: viewServer,
          topic: "orders",
          publish: (command) => {
            oldStream = command.stream;
          },
        });
        const oldEntry = {
          viewport: oldViewport,
          replaceCaptured: oldViewport.replaceCaptured,
          deactivate: oldViewport.deactivate,
        };
        binding.install(oldEntry);
        binding.viewport.replace({
          window: { firstRow: 0, lastRow: 9 },
          query: { select: ["id"], where: [], orderBy: [] },
          sink: sink.sink,
        });
        const oldFiber = yield* oldStream.pipe(Stream.runDrain, Effect.forkChild);
        yield* flush;

        const currentViewport = makeLiveQueryViewport({
          client: makeManualClient(base.liveClient, currentRequests),
          config: viewServer,
          topic: "orders",
          publish: (command) => {
            currentStream = command.stream;
          },
        });
        const currentEntry = {
          viewport: currentViewport,
          replaceCaptured: currentViewport.replaceCaptured,
          deactivate: currentViewport.deactivate,
        };
        binding.install(currentEntry);
        const obsoleteGeneration = oldViewport.replace({
          window: { firstRow: 20, lastRow: 29 },
          query: { select: ["id"], where: [], orderBy: [] },
          sink: sink.sink,
        });
        obsoleteGeneration.setWindow({ firstRow: 30, lastRow: 39 });
        obsoleteGeneration.release();
        binding.viewport.replace({
          window: { firstRow: 10, lastRow: 19 },
          query: { select: ["id"], where: [], orderBy: [] },
          sink: sink.sink,
        });
        const currentFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
        yield* flush;
        yield* Queue.offer(currentRequests[0]!.events, snapshot("current", 2, 1));
        yield* flush;
        expect(sink.rowData.at(-1)).toStrictEqual({ 10: { id: "current", price: 2 } });

        yield* Queue.offer(oldRequests[0]!.events, snapshot("obsolete", 1, 1));
        binding.uninstall(oldEntry);
        const currentGeneration = binding.viewport.replace({
          window: { firstRow: 20, lastRow: 29 },
          query: { select: ["id"], where: [], orderBy: [] },
          sink: sink.sink,
        });
        currentGeneration.setWindow({ firstRow: 30, lastRow: 39 });
        const latestFiber = yield* currentStream.pipe(Stream.runDrain, Effect.forkChild);
        yield* flush;
        expect(currentRequests).toHaveLength(2);
        expect(oldRequests).toHaveLength(1);
        yield* Queue.offer(currentRequests[1]!.events, snapshot("latest", 3, 2));
        yield* flush;
        expect(sink.rowData.at(-1)).toStrictEqual({ 30: { id: "latest", price: 3 } });

        yield* Fiber.interrupt(oldFiber);
        yield* Fiber.interrupt(currentFiber);
        yield* Fiber.interrupt(latestFiber);
        binding.uninstall(currentEntry);
        yield* base.close;
      }),
  );

  it.effect("makes destroy terminal even when sink cleanup attempts to replace", () =>
    Effect.gen(function* () {
      const base = createInMemoryViewServer(viewServer);
      let publishCount = 0;
      const viewport = makeLiveQueryViewport({
        client: base.liveClient,
        config: viewServer,
        topic: "orders",
        publish: () => {
          publishCount += 1;
        },
      });
      let replaceDuringDestroy = false;
      const replacementSink = makeSink<{ readonly id: string }>();
      viewport.replace({
        window: { firstRow: 0, lastRow: 9 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: {
          setRowCount: () => {
            if (replaceDuringDestroy) {
              replaceDuringDestroy = false;
              viewport.replace({
                window: { firstRow: 10, lastRow: 19 },
                query: { select: ["id"], where: [], orderBy: [] },
                sink: replacementSink.sink,
              });
            }
          },
          setRowData: () => undefined,
        },
      });
      replaceDuringDestroy = true;
      viewport.destroy();
      const destroyedGeneration = viewport.replace({
        window: { firstRow: 20, lastRow: 29 },
        query: { select: ["id"], where: [], orderBy: [] },
        sink: replacementSink.sink,
      });
      destroyedGeneration.setWindow({ firstRow: 30, lastRow: 39 });
      destroyedGeneration.release();

      expect(publishCount).toBe(2);
      expect(replacementSink.rowCounts).toStrictEqual([]);
      viewport.destroy();
      expect(publishCount).toBe(2);
      yield* base.close;
    }),
  );
});
