export const groupedWritePrimingAppendCase = "grouped write priming append";
export const groupedWritePrimingDeleteCase = "grouped write priming delete";

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

export const settleAndCollectGroupedWriteBenchmarkMemoryCheckpoint = async ({
  collectGarbage,
  settle,
}: {
  readonly collectGarbage: () => void;
  readonly settle: () => Promise<void>;
}): Promise<void> => {
  await settle();
  collectGarbage();
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
