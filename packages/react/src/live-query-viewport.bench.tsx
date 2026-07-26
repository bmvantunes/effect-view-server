// Import Vitest directly so @effect/vitest's eager Effect test runtime does not
// distort the allocation and latency profile this hot-path benchmark measures.
// This is a Chromium browser CPU/GC stress microbenchmark over a synthetic
// in-memory Queue. It is not network-shaped or production-like.
import { afterAll, bench, describe } from "vitest";
import type {
  ViewServerLiveClient,
  ViewServerLiveEvent,
  ViewServerLiveSubscription,
} from "@effect-view-server/client";
import {
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
import { Effect, Fiber, Queue, Schema, Stream } from "effect";
import { makeLiveQueryViewport, type LiveQueryViewportChrome } from "./live-query-viewport";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

type Topics = typeof viewServer.topics;

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

type ViewportBenchmark = {
  readonly update: () => Promise<void>;
  readonly replaceAll: () => Promise<void>;
  readonly moveAll: () => Promise<void>;
  readonly replaceTail: () => Promise<void>;
  readonly close: () => Promise<void>;
};

const waitForSinkWrite = (
  write: Promise<void>,
  label: string,
  timeoutMilliseconds = 10_000,
): Promise<void> =>
  new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`Timed out waiting for ${label} after ${timeoutMilliseconds}ms`));
    }, timeoutMilliseconds);
    write.then(
      () => {
        clearTimeout(timeout);
        resolve();
      },
      (error) => {
        clearTimeout(timeout);
        reject(error);
      },
    );
  });

const makeViewportBenchmark = async (rowCount: number): Promise<ViewportBenchmark> => {
  const runtime = createInMemoryViewServer(viewServer);
  const events = await Effect.runPromise(Queue.unbounded<ViewServerLiveEvent<object>>());
  const client = {
    ...runtime.liveClient,
    subscribe: adaptQuerySubstrate(() =>
      Effect.succeed({
        events: Stream.fromQueue(events),
        close: () => Effect.void,
      }),
    ),
  } satisfies ViewServerLiveClient<Topics>;
  let currentStream: Stream.Stream<LiveQueryViewportChrome, unknown> = Stream.never;
  let resolveWrite: (() => void) | undefined;
  const viewport = makeLiveQueryViewport({
    client,
    config: viewServer,
    topic: "orders",
    publish: (command) => {
      currentStream = command.stream;
    },
  });
  viewport.replace({
    window: { firstRow: 0, lastRow: rowCount - 1 },
    query: { select: ["id", "price"], where: [], orderBy: [] },
    sink: {
      setRowCount: () => undefined,
      setRowData: () => {
        resolveWrite?.();
      },
    },
  });
  const fiber = Effect.runFork(currentStream.pipe(Stream.runDrain));
  const rows = Array.from({ length: rowCount }, (_, index) => ({
    id: `row-${index}`,
    price: index,
  }));
  const snapshotWritten = new Promise<void>((resolve) => {
    resolveWrite = resolve;
  });
  await Effect.runPromise(
    Queue.offer(events, {
      type: "snapshot",
      topic: "orders",
      queryId: "viewport-benchmark",
      rows,
      keys: rows.map((row) => row.id),
      totalRows: rowCount,
      version: 1,
    }),
  );
  await waitForSinkWrite(snapshotWritten, `${rowCount}-row snapshot`);

  const index = Math.floor(rowCount / 2);
  const replacementRows = Array.from({ length: rowCount }, (_, rowIndex) => ({
    id: `replacement-${rowIndex}`,
    price: rowIndex,
  }));
  const replaceWithReplacement = [
    ...rows.map((row) => ({ type: "remove" as const, key: row.id })),
    ...replacementRows.map((row, rowIndex) => ({
      type: "insert" as const,
      key: row.id,
      row,
      index: rowIndex,
    })),
  ];
  const replaceWithOriginal = [
    ...replacementRows.map((row) => ({ type: "remove" as const, key: row.id })),
    ...rows.map((row, rowIndex) => ({
      type: "insert" as const,
      key: row.id,
      row,
      index: rowIndex,
    })),
  ];
  const moveAll = Array.from({ length: rowCount }, (_, moveIndex) => ({
    type: "move" as const,
    key: `row-${rowCount - moveIndex - 1}`,
    fromIndex: rowCount - 1,
    toIndex: 0,
  }));
  let replacementIsCurrent = false;
  let replacementTailIsCurrent = false;
  let version = 1;
  return {
    update: async () => {
      const nextVersion = version + 1;
      const written = new Promise<void>((resolve) => {
        resolveWrite = resolve;
      });
      await Effect.runPromise(
        Queue.offer(events, {
          type: "delta",
          topic: "orders",
          queryId: "viewport-benchmark",
          operations: [
            {
              type: "update",
              key: `row-${index}`,
              row: { id: `row-${index}`, price: nextVersion },
              index,
            },
          ],
          totalRows: rowCount,
          fromVersion: version,
          toVersion: nextVersion,
        }),
      );
      version = nextVersion;
      await waitForSinkWrite(written, `${rowCount}-row delta`);
    },
    replaceAll: async () => {
      const nextVersion = version + 1;
      const written = new Promise<void>((resolve) => {
        resolveWrite = resolve;
      });
      await Effect.runPromise(
        Queue.offer(events, {
          type: "delta",
          topic: "orders",
          queryId: "viewport-benchmark",
          operations: replacementIsCurrent ? replaceWithOriginal : replaceWithReplacement,
          totalRows: rowCount,
          fromVersion: version,
          toVersion: nextVersion,
        }),
      );
      replacementIsCurrent = !replacementIsCurrent;
      version = nextVersion;
      await waitForSinkWrite(written, `${rowCount}-row replacement delta`);
    },
    moveAll: async () => {
      const nextVersion = version + 1;
      const written = new Promise<void>((resolve) => {
        resolveWrite = resolve;
      });
      await Effect.runPromise(
        Queue.offer(events, {
          type: "delta",
          topic: "orders",
          queryId: "viewport-benchmark",
          operations: moveAll,
          totalRows: rowCount,
          fromVersion: version,
          toVersion: nextVersion,
        }),
      );
      version = nextVersion;
      await waitForSinkWrite(written, `${rowCount}-row move delta`);
    },
    replaceTail: async () => {
      const nextVersion = version + 1;
      const currentKey = replacementTailIsCurrent ? "replacement-tail" : `row-${rowCount - 1}`;
      const nextKey = replacementTailIsCurrent ? `row-${rowCount - 1}` : "replacement-tail";
      const written = new Promise<void>((resolve) => {
        resolveWrite = resolve;
      });
      await Effect.runPromise(
        Queue.offer(events, {
          type: "delta",
          topic: "orders",
          queryId: "viewport-benchmark",
          operations: [
            { type: "remove", key: currentKey },
            {
              type: "insert",
              key: nextKey,
              row: { id: nextKey, price: nextVersion },
              index: rowCount - 1,
            },
          ],
          totalRows: rowCount,
          fromVersion: version,
          toVersion: nextVersion,
        }),
      );
      replacementTailIsCurrent = !replacementTailIsCurrent;
      version = nextVersion;
      await waitForSinkWrite(written, `${rowCount}-row tail replacement delta`);
    },
    close: async () => {
      viewport.destroy();
      await Effect.runPromise(Fiber.interrupt(fiber));
      await Effect.runPromise(runtime.close);
    },
  };
};

const update100Rows = await makeViewportBenchmark(100);
const update10000Rows = await makeViewportBenchmark(10_000);
const replace1000Rows = await makeViewportBenchmark(1_000);
const replace10000Rows = await makeViewportBenchmark(10_000);
const move1000Rows = await makeViewportBenchmark(1_000);
const move10000Rows = await makeViewportBenchmark(10_000);
const replaceTail10000Rows = await makeViewportBenchmark(10_000);
const scrollSchedulingRuntime = createInMemoryViewServer(viewServer);

const schedulePreActivationScrollBurst = async (): Promise<void> => {
  let publishCount = 0;
  let clearCount = 0;
  const viewport = makeLiveQueryViewport({
    client: scrollSchedulingRuntime.liveClient,
    config: viewServer,
    topic: "orders",
    publish: () => {
      publishCount += 1;
    },
  });
  const generation = viewport.replace({
    window: { firstRow: 0, lastRow: 9 },
    query: { select: ["id"], where: [], orderBy: [] },
    sink: {
      setRowCount: () => {
        clearCount += 1;
      },
      setRowData: () => undefined,
    },
  });
  for (let firstRow = 1; firstRow <= 10_000; firstRow += 1) {
    generation.setWindow({ firstRow, lastRow: firstRow + 9 });
  }
  viewport.destroy();
  if (publishCount !== 10_002 || clearCount !== 1) {
    throw new Error("Pre-activation scroll burst did not retain only the latest request.");
  }
  await Promise.resolve();
};

afterAll(async () => {
  await update100Rows.close();
  await update10000Rows.close();
  await replace1000Rows.close();
  await replace10000Rows.close();
  await move1000Rows.close();
  await move10000Rows.close();
  await replaceTail10000Rows.close();
  await Effect.runPromise(scrollSchedulingRuntime.close);
});

describe("Live Query Viewport subscription-to-sink projection", () => {
  bench("single-row update in a 100-row viewport", () => update100Rows.update());
  bench("single-row update in a 10000-row viewport", () => update10000Rows.update());
  bench("full replacement in a 1000-row viewport", () => replace1000Rows.replaceAll());
  bench("full replacement in a 10000-row viewport", () => replace10000Rows.replaceAll());
  bench("full reorder in a 1000-row viewport", () => move1000Rows.moveAll());
  bench("full reorder in a 10000-row viewport", () => move10000Rows.moveAll());
  bench("tail replacement in a 10000-row viewport", () => replaceTail10000Rows.replaceTail());
  bench("schedule 10000 pre-activation scroll windows", () => schedulePreActivationScrollBurst(), {
    iterations: 5,
    time: 0,
    warmupIterations: 0,
    warmupTime: 0,
  });
});
