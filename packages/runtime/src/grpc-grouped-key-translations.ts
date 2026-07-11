import type { DeltaOperation } from "@effect-view-server/config";

export type GrpcGroupedKeyRetentionView = {
  readonly retainedEntryCount: () => number;
};

export type GrpcGroupedKeyRetentionObserver = (retention: GrpcGroupedKeyRetentionView) => void;

export type GrpcGroupedKeyTranslations<Row extends object> = {
  readonly translateSnapshot: (
    internalKeys: ReadonlyArray<string>,
    rows: ReadonlyArray<Row>,
  ) => ReadonlyArray<string> | undefined;
  readonly translateDelta: (
    operations: ReadonlyArray<DeltaOperation<Row>>,
  ) => ReadonlyArray<DeltaOperation<Row>> | undefined;
  readonly clear: () => void;
};

export const makeGrpcGroupedKeyTranslations = <Row extends object>(options: {
  readonly externalizeRow: (row: Row) => Row;
  readonly publicKeyFromRow: (row: Row) => string | undefined;
  readonly retentionObserver?: GrpcGroupedKeyRetentionObserver;
}): GrpcGroupedKeyTranslations<Row> => {
  const byInternalKey = new Map<string, string>();
  const pending = new Map<string, string | undefined>();
  const nextSnapshot = new Map<string, string>();

  if (options.retentionObserver !== undefined) {
    options.retentionObserver({
      retainedEntryCount: () => byInternalKey.size + pending.size + nextSnapshot.size,
    });
  }

  const publicKeyForInternalKey = (internalKey: string): string | undefined =>
    pending.has(internalKey) ? pending.get(internalKey) : byInternalKey.get(internalKey);

  const translateSnapshot = (
    internalKeys: ReadonlyArray<string>,
    rows: ReadonlyArray<Row>,
  ): ReadonlyArray<string> | undefined => {
    nextSnapshot.clear();
    const publicKeys: Array<string> = [];
    for (const [index, internalKey] of internalKeys.entries()) {
      const row = rows[index];
      if (row === undefined) {
        nextSnapshot.clear();
        return undefined;
      }
      const publicKey = options.publicKeyFromRow(row);
      if (publicKey === undefined) {
        nextSnapshot.clear();
        return undefined;
      }
      nextSnapshot.set(internalKey, publicKey);
      publicKeys.push(publicKey);
    }
    byInternalKey.clear();
    for (const [internalKey, publicKey] of nextSnapshot) {
      byInternalKey.set(internalKey, publicKey);
    }
    nextSnapshot.clear();
    return publicKeys;
  };

  const rollbackDelta = (): undefined => {
    pending.clear();
    return undefined;
  };

  const translateDelta = (
    operations: ReadonlyArray<DeltaOperation<Row>>,
  ): ReadonlyArray<DeltaOperation<Row>> | undefined => {
    pending.clear();
    const translated: Array<DeltaOperation<Row>> = [];
    for (const operation of operations) {
      if (operation.type === "move" || operation.type === "remove") {
        const publicKey = publicKeyForInternalKey(operation.key);
        if (publicKey === undefined) {
          return rollbackDelta();
        }
        translated.push({
          ...operation,
          key: publicKey,
        });
        if (operation.type === "remove") {
          pending.set(operation.key, undefined);
        }
        continue;
      }

      const row = options.externalizeRow(operation.row);
      const publicKey = options.publicKeyFromRow(row);
      if (publicKey === undefined) {
        return rollbackDelta();
      }
      pending.set(operation.key, publicKey);
      translated.push({
        ...operation,
        key: publicKey,
        row,
      });
    }

    for (const [internalKey, publicKey] of pending) {
      if (publicKey === undefined) {
        byInternalKey.delete(internalKey);
      } else {
        byInternalKey.set(internalKey, publicKey);
      }
    }
    pending.clear();
    return translated;
  };

  return {
    translateSnapshot,
    translateDelta,
    clear: () => {
      pending.clear();
      nextSnapshot.clear();
      byInternalKey.clear();
    },
  };
};
