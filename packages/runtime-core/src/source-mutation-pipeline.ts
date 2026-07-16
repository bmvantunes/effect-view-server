import type {
  ColumnLiveViewEngineError,
  DecodableTopicDefinitions,
} from "@effect-view-server/column-live-view-engine";
import type { ColumnLiveViewEngineInternal } from "@effect-view-server/column-live-view-engine/internal";
import type {
  ExactPatch,
  TopicRow,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
  ViewServerTopicConfig,
} from "@effect-view-server/config";
import type {
  ViewServerRuntimeDecodedMutation,
  ViewServerRuntimeDecodedMutationClient,
  viewServerRuntimeDecodedMutationTrust,
} from "@effect-view-server/config/internal";
import { Effect } from "effect";
import { engineErrorToRuntimeError } from "./runtime-error";
import { makeSourceOwnershipPolicy } from "./source-ownership-policy";

export type RuntimeCoreDecodedRowWithStorageKey = {
  readonly storageKey: string;
  readonly row: object;
};

export type ViewServerRuntimeCoreInternalMutations<Topics extends DecodableTopicDefinitions> = Pick<
  ViewServerRuntimeClient<Topics>,
  "delete" | "patch" | "publish" | "publishMany" | "reset"
> & {
  readonly patchDecodedFields: (
    topic: Extract<keyof Topics, string>,
    key: string,
    patch: object,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly publishManyDecodedRows: (
    topic: Extract<keyof Topics, string>,
    rows: ReadonlyArray<object>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly publishManyDecodedRowsWithStorageKeys: (
    topic: Extract<keyof Topics, string>,
    rows: ReadonlyArray<RuntimeCoreDecodedRowWithStorageKey>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly publishManyWithStorageKeys: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    rows: ReadonlyArray<{
      readonly storageKey: string;
      readonly row: TopicRow<Topics, Topic>;
    }>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
};

export type ViewServerRuntimeCoreCheckedMutations<Topics extends DecodableTopicDefinitions> = Pick<
  ViewServerRuntimeClient<Topics>,
  "delete" | "patch" | "publish" | "publishMany" | "reset"
>;

export type RuntimeCoreMutationPipeline<Topics extends DecodableTopicDefinitions> = {
  readonly internalMutations: ViewServerRuntimeCoreInternalMutations<Topics>;
  readonly checkedMutations: ViewServerRuntimeCoreCheckedMutations<Topics>;
  readonly decodedMutationClient: ViewServerRuntimeDecodedMutationClient<Topics>;
};

const applyEngineMutation = Effect.fn("ViewServerRuntimeCore.sourceMutation.apply")(function* (
  mutation: Effect.Effect<void, ColumnLiveViewEngineError>,
  requestHealthRefresh: Effect.Effect<void>,
) {
  yield* Effect.uninterruptible(mutation.pipe(Effect.tap(() => requestHealthRefresh))).pipe(
    Effect.mapError(engineErrorToRuntimeError),
  );
});

export const makeRuntimeCoreMutationPipeline = <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  engine: ColumnLiveViewEngineInternal<Topics>,
  requestHealthRefresh: Effect.Effect<void>,
): RuntimeCoreMutationPipeline<Topics> => {
  const sourceOwnership = makeSourceOwnershipPolicy(config);
  const publish = Effect.fn("ViewServerRuntimeCore.client.publish")(function* <
    Topic extends Extract<keyof Topics, string>,
  >(topic: Topic, row: TopicRow<Topics, Topic>) {
    yield* applyEngineMutation(engine.publish(topic, row), requestHealthRefresh);
  });
  const publishMany = Effect.fn("ViewServerRuntimeCore.client.publishMany")(function* <
    Topic extends Extract<keyof Topics, string>,
  >(topic: Topic, rows: ReadonlyArray<TopicRow<Topics, Topic>>) {
    yield* applyEngineMutation(engine.publishMany(topic, rows), requestHealthRefresh);
  });
  const publishManyDecodedRows = Effect.fn(
    "ViewServerRuntimeCore.sourceMutation.publishManyDecodedRows",
  )(function* (topic: Extract<keyof Topics, string>, rows: ReadonlyArray<object>) {
    yield* applyEngineMutation(engine.publishManyDecodedRows(topic, rows), requestHealthRefresh);
  });
  const publishManyDecodedRowsWithStorageKeys = Effect.fn(
    "ViewServerRuntimeCore.sourceMutation.publishManyDecodedRowsWithStorageKeys",
  )(function* (
    topic: Extract<keyof Topics, string>,
    rows: ReadonlyArray<RuntimeCoreDecodedRowWithStorageKey>,
  ) {
    yield* applyEngineMutation(
      engine.publishManyDecodedRowsWithStorageKeys(topic, rows),
      requestHealthRefresh,
    );
  });
  const publishManyWithStorageKeys = Effect.fn(
    "ViewServerRuntimeCore.sourceMutation.publishManyWithStorageKeys",
  )(function* <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    rows: ReadonlyArray<{
      readonly storageKey: string;
      readonly row: TopicRow<Topics, Topic>;
    }>,
  ) {
    yield* applyEngineMutation(
      engine.publishManyWithStorageKeys(topic, rows),
      requestHealthRefresh,
    );
  });
  const patchDecodedFields = Effect.fn("ViewServerRuntimeCore.sourceMutation.patchDecodedFields")(
    function* (topic: Extract<keyof Topics, string>, key: string, patch: object) {
      yield* applyEngineMutation(
        engine.patchDecodedFields(topic, key, patch),
        requestHealthRefresh,
      );
    },
  );
  const patch = Effect.fn("ViewServerRuntimeCore.client.patch")(function* <
    Topic extends Extract<keyof Topics, string>,
    const Patch,
  >(
    topic: Topic,
    key: string,
    patch: Patch & Partial<TopicRow<Topics, Topic>> & ExactPatch<TopicRow<Topics, Topic>, Patch>,
  ) {
    yield* applyEngineMutation(engine.patch(topic, key, patch), requestHealthRefresh);
  });
  const deleteRow = Effect.fn("ViewServerRuntimeCore.client.delete")(function* <
    Topic extends Extract<keyof Topics, string>,
  >(topic: Topic, key: string) {
    yield* applyEngineMutation(engine.delete(topic, key), requestHealthRefresh);
  });
  const reset = Effect.fn("ViewServerRuntimeCore.client.reset")(function* () {
    yield* applyEngineMutation(engine.reset(), requestHealthRefresh);
  });
  const internalMutations: ViewServerRuntimeCoreInternalMutations<Topics> = {
    publish,
    publishMany,
    publishManyDecodedRows,
    publishManyDecodedRowsWithStorageKeys,
    publishManyWithStorageKeys,
    patchDecodedFields,
    patch,
    delete: deleteRow,
    reset,
  };
  const decodedMutationClient: ViewServerRuntimeDecodedMutationClient<Topics> = {
    execute: Effect.fn("ViewServerRuntimeCore.sourceMutation.decoded.execute")(function* (
      mutation: ViewServerRuntimeDecodedMutation<Topics>,
      _trust?: typeof viewServerRuntimeDecodedMutationTrust,
    ) {
      yield* sourceOwnership.requirePublicMutationAllowed(mutation.topic, "runtimeCore");
      if (mutation._tag === "CheckMutationAllowed") {
        return;
      }
      if (mutation._tag === "PublishDecodedRows") {
        return yield* internalMutations.publishManyDecodedRows(mutation.topic, mutation.rows);
      }
      if (mutation._tag === "PatchDecodedFields") {
        return yield* internalMutations.patchDecodedFields(
          mutation.topic,
          mutation.key,
          mutation.patch,
        );
      }
      return yield* internalMutations.delete(mutation.topic, mutation.key);
    }),
  };
  const checkedMutations: ViewServerRuntimeCoreCheckedMutations<Topics> = {
    publish: (topic, row) =>
      sourceOwnership
        .requirePublicMutationAllowed(topic, "runtimeCore")
        .pipe(Effect.flatMap(() => internalMutations.publish(topic, row))),
    publishMany: (topic, rows) =>
      sourceOwnership
        .requirePublicMutationAllowed(topic, "runtimeCore")
        .pipe(Effect.flatMap(() => internalMutations.publishMany(topic, rows))),
    patch: (topic, key, patch) =>
      sourceOwnership
        .requirePublicMutationAllowed(topic, "runtimeCore")
        .pipe(Effect.flatMap(() => internalMutations.patch(topic, key, patch))),
    delete: (topic, key) =>
      sourceOwnership
        .requirePublicMutationAllowed(topic, "runtimeCore")
        .pipe(Effect.flatMap(() => internalMutations.delete(topic, key))),
    reset: () =>
      sourceOwnership
        .requirePublicResetAllowed("runtimeCore")
        .pipe(Effect.flatMap(() => internalMutations.reset())),
  };
  return {
    internalMutations,
    checkedMutations,
    decodedMutationClient,
  };
};
