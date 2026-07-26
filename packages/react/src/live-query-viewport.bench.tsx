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
  readonly close: () => Promise<void>;
};

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
  await snapshotWritten;

  const index = Math.floor(rowCount / 2);
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
      await written;
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

afterAll(async () => {
  await update100Rows.close();
  await update10000Rows.close();
});

describe("Live Query Viewport subscription-to-sink projection", () => {
  bench("single-row update in a 100-row viewport", () => update100Rows.update());
  bench("single-row update in a 10000-row viewport", () => update10000Rows.update());
});
