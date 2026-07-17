export const groupedWritePrimingAppendCase = "grouped write priming append";
export const groupedWritePrimingDeleteCase = "grouped write priming delete";

export type GroupedWriteBenchmarkPrimingOperations = {
  readonly appendBatch: () => Promise<ReadonlyArray<string>>;
  readonly deleteRow: (key: string) => Promise<void>;
  readonly drainDelta: (caseName: string) => Promise<void>;
  readonly readRowCount: () => Promise<number>;
};

export const prepareGroupedWriteBenchmarkSetup = async ({
  captureMemoryBaseline,
  prepareMutationKeyIndexes,
  prime,
  settleMeasurementRuntime,
}: {
  readonly captureMemoryBaseline: () => void;
  readonly prepareMutationKeyIndexes: () => void;
  readonly prime: (() => Promise<void>) | undefined;
  readonly settleMeasurementRuntime: (() => void) | undefined;
}): Promise<void> => {
  prepareMutationKeyIndexes();
  if (settleMeasurementRuntime !== undefined) {
    settleMeasurementRuntime();
  }
  if (prime !== undefined) {
    await prime();
  }
  captureMemoryBaseline();
};

export const captureGroupedWriteBenchmarkAfterCleanup = async <Snapshot>({
  captureMemoryAfterBenchmark,
  releaseBenchmarkReferences,
  settleCleanupRuntime,
}: {
  readonly captureMemoryAfterBenchmark: () => Snapshot;
  readonly releaseBenchmarkReferences: () => Promise<void>;
  readonly settleCleanupRuntime: (() => void) | undefined;
}): Promise<Snapshot> => {
  await releaseBenchmarkReferences();
  if (settleCleanupRuntime !== undefined) {
    settleCleanupRuntime();
  }
  return captureMemoryAfterBenchmark();
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
