import type { RowSchema } from "@effect-view-server/config";
import { validateDecodedRow } from "@effect-view-server/config/internal";
import { Effect } from "effect";
import { topicRowChangedFieldsFromRows, type TopicRowChangedFields } from "./row-scan";
import { fieldValue, isPlainRecord } from "./row-values";
import type { TopicRowValueSemantics } from "./topic-row-value-semantics";

type RowObject = object;

export type InvalidRowErrorFactory<Error> = (topic: string, message: string) => Error;

export type PreparedTopicRow = {
  readonly changedFields?: TopicRowChangedFields;
  readonly key: string;
  readonly row: object;
  readonly source: "patch" | "row";
};

export type TopicRowPreparationContext = {
  readonly fieldNames: ReadonlySet<string>;
  readonly keyField: string;
  readonly schema: RowSchema;
  readonly semantics: TopicRowValueSemantics;
  readonly topic: string;
};

const normalizedDecodedTopicRow = (
  context: TopicRowPreparationContext,
  decoded: RowObject,
): RowObject => {
  const cloned = context.semantics.materializeRow(decoded);
  for (const field of context.fieldNames) {
    if (!Object.hasOwn(cloned, field)) {
      Object.defineProperty(cloned, field, {
        configurable: true,
        enumerable: false,
        value: undefined,
        writable: true,
      });
    }
  }
  return cloned;
};

const normalizeDecodedTopicRow = Effect.fn("ColumnLiveViewEngine.topicRow.decoded.normalize")(
  function* <Error>(
    context: TopicRowPreparationContext,
    row: RowObject,
    invalidRow: InvalidRowErrorFactory<Error>,
  ) {
    return yield* Effect.try({
      try: () => normalizedDecodedTopicRow(context, row),
      catch: (cause) => invalidRow(context.topic, String(cause)),
    });
  },
);

const validateDecodedTopicRow = Effect.fn("ColumnLiveViewEngine.topicRow.decoded.validate")(
  function* <Error>(
    context: TopicRowPreparationContext,
    row: RowObject,
    invalidRow: InvalidRowErrorFactory<Error>,
  ) {
    const decoded = yield* validateDecodedRow(context.schema, row).pipe(
      Effect.mapError((cause) => invalidRow(context.topic, String(cause))),
    );
    return yield* normalizeDecodedTopicRow(context, decoded, invalidRow);
  },
);

const topicRowKey = Effect.fn("ColumnLiveViewEngine.topicRow.key")(function* <Error>(
  context: TopicRowPreparationContext,
  row: RowObject,
  invalidRow: InvalidRowErrorFactory<Error>,
) {
  const key = fieldValue(row, context.keyField);
  if (typeof key !== "string") {
    return yield* Effect.fail(
      invalidRow(context.topic, `Key field ${context.keyField} must decode to a string.`),
    );
  }
  return key;
});

const inspectTopicPatch = Effect.fn("ColumnLiveViewEngine.topicRow.patch.inspect")(function* <
  Error,
>(context: TopicRowPreparationContext, patch: unknown, invalidRow: InvalidRowErrorFactory<Error>) {
  const record = yield* Effect.try({
    try: () => (isPlainRecord(patch) ? patch : undefined),
    catch: () => invalidRow(context.topic, "Could not inspect patch object."),
  });
  if (record === undefined) {
    return yield* Effect.fail(invalidRow(context.topic, "Patch must be a plain object."));
  }
  const keys = yield* Effect.try({
    try: () => Reflect.ownKeys(record),
    catch: () => invalidRow(context.topic, "Could not inspect patch fields."),
  });
  const inspected: Record<string, unknown> = {};
  for (const key of keys) {
    if (typeof key !== "string" || !context.fieldNames.has(key)) {
      return yield* Effect.fail(
        invalidRow(context.topic, `Patch contains unknown field: ${String(key)}.`),
      );
    }
    const descriptor = yield* Effect.try({
      try: () => Object.getOwnPropertyDescriptor(record, key),
      catch: () => invalidRow(context.topic, `Could not inspect patch field: ${key}.`),
    });
    if (descriptor === undefined || !("value" in descriptor)) {
      return yield* Effect.fail(
        invalidRow(context.topic, `Patch field must be a data property: ${key}.`),
      );
    }
    if (descriptor.enumerable !== true) {
      return yield* Effect.fail(
        invalidRow(context.topic, `Patch field must be enumerable: ${key}.`),
      );
    }
    Object.defineProperty(inspected, key, {
      configurable: true,
      enumerable: true,
      value: descriptor.value,
      writable: true,
    });
  }
  return inspected;
});

export const prepareTopicRow = Effect.fn("ColumnLiveViewEngine.topicRow.prepare")(function* <
  Error,
  Row extends RowObject,
>(context: TopicRowPreparationContext, row: Row, invalidRow: InvalidRowErrorFactory<Error>) {
  const decoded = yield* validateDecodedTopicRow(context, row, invalidRow);
  const key = yield* topicRowKey(context, decoded, invalidRow);
  return {
    key,
    row: decoded,
    source: "row",
  } satisfies PreparedTopicRow;
});

export const prepareTopicRowWithStorageKey = Effect.fn(
  "ColumnLiveViewEngine.topicRow.prepareWithStorageKey",
)(function* <Error, Row extends RowObject>(
  context: TopicRowPreparationContext,
  row: Row,
  storageKey: string,
  invalidRow: InvalidRowErrorFactory<Error>,
) {
  const decoded = yield* validateDecodedTopicRow(context, row, invalidRow);
  yield* topicRowKey(context, decoded, invalidRow);
  return {
    key: storageKey,
    row: decoded,
    source: "row",
  } satisfies PreparedTopicRow;
});

export const prepareDecodedTopicRow = Effect.fn("ColumnLiveViewEngine.topicRow.decoded.prepare")(
  function* <Error, Row extends RowObject>(
    context: TopicRowPreparationContext,
    row: Row,
    invalidRow: InvalidRowErrorFactory<Error>,
  ) {
    const decoded = yield* normalizeDecodedTopicRow(context, row, invalidRow);
    const key = yield* topicRowKey(context, decoded, invalidRow);
    return {
      key,
      row: decoded,
      source: "row",
    } satisfies PreparedTopicRow;
  },
);

export const prepareDecodedTopicRowWithStorageKey = Effect.fn(
  "ColumnLiveViewEngine.topicRow.decoded.prepareWithStorageKey",
)(function* <Error, Row extends RowObject>(
  context: TopicRowPreparationContext,
  row: Row,
  storageKey: string,
  invalidRow: InvalidRowErrorFactory<Error>,
) {
  const decoded = yield* normalizeDecodedTopicRow(context, row, invalidRow);
  yield* topicRowKey(context, decoded, invalidRow);
  return {
    key: storageKey,
    row: decoded,
    source: "row",
  } satisfies PreparedTopicRow;
});

const preparePatchedTopicRow = Effect.fn("ColumnLiveViewEngine.topicRow.patch.preparePatched")(
  function* <Patch extends Partial<RowObject>, Error>(
    context: TopicRowPreparationContext,
    key: string,
    current: RowObject | undefined,
    patch: Patch,
    invalidRow: InvalidRowErrorFactory<Error>,
    preparePatchedRow: (row: RowObject) => Effect.Effect<RowObject, Error>,
  ) {
    const inspectedPatch = yield* inspectTopicPatch(context, patch, invalidRow);
    if (current === undefined) {
      return yield* Effect.fail(invalidRow(context.topic, `Cannot patch missing key: ${key}`));
    }
    const decoded = yield* preparePatchedRow({ ...current, ...inspectedPatch });
    const decodedKey = yield* topicRowKey(context, decoded, invalidRow);
    if (decodedKey !== key) {
      return yield* Effect.fail(invalidRow(context.topic, "Patch must not change the row key."));
    }
    const decodedFieldNames = new Set([...Object.keys(current), ...Object.keys(decoded)]);
    const topicRowChangedFields = topicRowChangedFieldsFromRows(
      current,
      decoded,
      decodedFieldNames,
      (field, left, right) => context.semantics.equivalentField(field, left, right),
    );
    if (topicRowChangedFields === undefined) {
      return {
        key,
        row: decoded,
        source: "patch",
      } satisfies PreparedTopicRow;
    }
    return {
      changedFields: topicRowChangedFields,
      key,
      row: decoded,
      source: "patch",
    } satisfies PreparedTopicRow;
  },
);

export const prepareTopicPatch = Effect.fn("ColumnLiveViewEngine.topicRow.patch.prepare")(
  function* <Patch extends Partial<RowObject>, Error>(
    context: TopicRowPreparationContext,
    key: string,
    current: RowObject | undefined,
    patch: Patch,
    invalidRow: InvalidRowErrorFactory<Error>,
  ) {
    return yield* preparePatchedTopicRow(context, key, current, patch, invalidRow, (row) =>
      validateDecodedTopicRow(context, row, invalidRow),
    );
  },
);

export const prepareDecodedTopicPatch = Effect.fn(
  "ColumnLiveViewEngine.topicRow.decodedPatch.prepare",
)(function* <Patch extends Partial<RowObject>, Error>(
  context: TopicRowPreparationContext,
  key: string,
  current: RowObject | undefined,
  patch: Patch,
  invalidRow: InvalidRowErrorFactory<Error>,
) {
  return yield* preparePatchedTopicRow(context, key, current, patch, invalidRow, (row) =>
    validateDecodedTopicRow(context, row, invalidRow),
  );
});
