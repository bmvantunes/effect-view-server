export const groupedWritePrimingAppendCase = "grouped write priming append";
export const groupedWritePrimingDeleteCase = "grouped write priming delete";
export const groupedWriteBenchmarkPostGcEventLoopTurns = 8;

export type GroupedWriteBenchmarkCleanupLedger = {
  readonly activeSubscriptions: number;
  readonly activeViews: number;
  readonly pendingMutationBatches: number;
  readonly queuedEvents: number;
};

export type GroupedWriteBenchmarkMemoryCheckpointSample<Memory> = {
  readonly cleanupLedger: GroupedWriteBenchmarkCleanupLedger;
  readonly eventLoopTurn: number;
  readonly memory: Memory;
};

export type GroupedWriteBenchmarkPrimingOperations = {
  readonly appendBatch: () => Promise<ReadonlyArray<string>>;
  readonly deleteRow: (key: string) => Promise<void>;
  readonly drainDelta: (caseName: string) => Promise<void>;
  readonly readRowCount: () => Promise<number>;
};

export const groupedWriteBenchmarkGarbageCollector = ({
  collectGarbage,
  explicitGc,
}: {
  readonly collectGarbage: (() => void) | undefined;
  readonly explicitGc: boolean;
}): (() => void) | undefined => {
  if (!explicitGc) {
    return undefined;
  }
  if (collectGarbage === undefined) {
    throw new Error(
      "Grouped write explicit GC requires Node to start with NODE_OPTIONS=--expose-gc.",
    );
  }
  return collectGarbage;
};

export const groupedWriteBenchmarkPostGcEventLoopTurnsFromEnv = (
  raw: string | undefined,
  explicitGc: boolean,
): number => {
  if (!explicitGc) {
    if (raw !== undefined) {
      throw new Error("VIEW_SERVER_ENGINE_BENCH_POST_GC_EVENT_LOOP_TURNS requires explicit GC.");
    }
    return 0;
  }
  if (raw === undefined) {
    throw new Error(
      "Grouped write explicit GC requires VIEW_SERVER_ENGINE_BENCH_POST_GC_EVENT_LOOP_TURNS.",
    );
  }
  const turns = Number(raw);
  if (!Number.isInteger(turns) || turns !== groupedWriteBenchmarkPostGcEventLoopTurns) {
    throw new Error(
      `VIEW_SERVER_ENGINE_BENCH_POST_GC_EVENT_LOOP_TURNS must be ${groupedWriteBenchmarkPostGcEventLoopTurns}.`,
    );
  }
  return turns;
};

export const settleAndCollectGroupedWriteBenchmarkMemoryCheckpoint = async <Memory>({
  capture,
  cleanupLedger,
  collectGarbage,
  postGcEventLoopTurns,
  settle,
}: {
  readonly capture: () => Memory;
  readonly cleanupLedger: GroupedWriteBenchmarkCleanupLedger;
  readonly collectGarbage: () => void;
  readonly postGcEventLoopTurns: number;
  readonly settle: () => Promise<void>;
}): Promise<{
  readonly endpoint: Memory;
  readonly samples: ReadonlyArray<GroupedWriteBenchmarkMemoryCheckpointSample<Memory>>;
}> => {
  if (
    cleanupLedger.activeSubscriptions !== 0 ||
    cleanupLedger.activeViews !== 0 ||
    cleanupLedger.pendingMutationBatches !== 0 ||
    cleanupLedger.queuedEvents !== 0
  ) {
    throw new Error("Grouped write benchmark cleanup ledger must be zero before memory sampling.");
  }
  await settle();
  collectGarbage();
  let endpoint = capture();
  const samples: Array<GroupedWriteBenchmarkMemoryCheckpointSample<Memory>> = [
    { cleanupLedger, eventLoopTurn: 0, memory: endpoint },
  ];
  for (let eventLoopTurn = 1; eventLoopTurn <= postGcEventLoopTurns; eventLoopTurn += 1) {
    await settle();
    endpoint = capture();
    samples.push({ cleanupLedger, eventLoopTurn, memory: endpoint });
  }
  return { endpoint, samples };
};

export const primeGroupedWriteBenchmark = async ({
  expectedRowCount,
  operations,
}: {
  readonly expectedRowCount: number;
  readonly operations: GroupedWriteBenchmarkPrimingOperations;
}): Promise<{
  readonly appendedRowCount: number;
  readonly deltaVersionCount: number;
  readonly restoredRowCount: number;
}> => {
  const appendedKeys = await operations.appendBatch();
  if (appendedKeys.length === 0) {
    throw new Error("Grouped write benchmark priming append must add at least one row.");
  }
  await operations.drainDelta(groupedWritePrimingAppendCase);

  for (const key of appendedKeys) {
    await operations.deleteRow(key);
    await operations.drainDelta(groupedWritePrimingDeleteCase);
  }

  const restoredRowCount = await operations.readRowCount();
  if (restoredRowCount !== expectedRowCount) {
    throw new Error(
      `Grouped write benchmark priming must restore ${expectedRowCount} rows but found ${restoredRowCount}.`,
    );
  }

  return {
    appendedRowCount: appendedKeys.length,
    deltaVersionCount: 1 + appendedKeys.length,
    restoredRowCount,
  };
};
