import type { DeltaOperation, RowSchema } from "@effect-view-server/config";
import {
  compileGroupedKeyIdentity,
  makeSchemaJsonIdentity,
  type GroupedKeyIdentityField,
  type SchemaJsonIdentity,
} from "@effect-view-server/effect-utils";
import { Result, Schema } from "effect";

export class GrpcLeasedIdentityError extends Schema.TaggedErrorClass<GrpcLeasedIdentityError>()(
  "GrpcLeasedIdentityError",
  {
    kind: Schema.Literals(["Configuration", "Route", "RouteMismatch", "RowKey", "ResultKey"]),
    message: Schema.String,
    cause: Schema.Unknown,
  },
) {}

export type GrpcLeasedGroupedKeyRetentionView = {
  readonly retainedEntryCount: () => number;
};

export type GrpcLeasedGroupedKeyRetentionObserver = (
  retention: GrpcLeasedGroupedKeyRetentionView,
) => void;

export type GrpcLeasedResultKeyTranslation<Row extends object> = {
  readonly translateSnapshot: (
    internalKeys: ReadonlyArray<string>,
    rows: ReadonlyArray<Row>,
  ) => Result.Result<ReadonlyArray<string>, GrpcLeasedIdentityError>;
  readonly translateDelta: (
    operations: ReadonlyArray<DeltaOperation<Row>>,
  ) => Result.Result<ReadonlyArray<DeltaOperation<Row>>, GrpcLeasedIdentityError>;
  readonly clear: () => void;
};

export type GrpcLeasedInternalRowKey = {
  readonly storageKey: string;
};

export type GrpcLeasedIdentityLease = {
  readonly feedKey: string;
  readonly materializeRoute: () => Readonly<Record<string, unknown>>;
  readonly validateRowRoute: <Row extends object>(
    row: Row,
  ) => Result.Result<Row, GrpcLeasedIdentityError>;
  readonly internalizeRowKey: (
    row: object,
  ) => Result.Result<GrpcLeasedInternalRowKey, GrpcLeasedIdentityError>;
  readonly resultKeys: <Row extends object>(
    query: unknown,
    retentionObserver?: GrpcLeasedGroupedKeyRetentionObserver,
  ) => GrpcLeasedResultKeyTranslation<Row>;
};

export type GrpcLeasedIdentityContract = {
  readonly leaseFromQuery: (
    query: unknown,
  ) => Result.Result<GrpcLeasedIdentityLease, GrpcLeasedIdentityError>;
};

type CompiledRouteField = {
  readonly field: string;
  readonly identity: SchemaJsonIdentity;
};

type MaterializedRoute = {
  readonly canonicalKeys: ReadonlyArray<string>;
  readonly values: Readonly<Record<string, unknown>>;
};

const identityError = (
  kind: GrpcLeasedIdentityError["kind"],
  message: string,
  cause: unknown,
): GrpcLeasedIdentityError => new GrpcLeasedIdentityError({ kind, message, cause });

const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const exactEqValue = (value: unknown): Result.Result<unknown, undefined> =>
  isRecord(value) && Object.keys(value).length === 1 && Object.hasOwn(value, "eq")
    ? Result.succeed(value["eq"])
    : Result.fail(undefined);

const defineRouteField = (route: Record<string, unknown>, field: string, value: unknown): void => {
  Object.defineProperty(route, field, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
};

const materializeRoute = (
  topic: string,
  routeFields: ReadonlyArray<CompiledRouteField>,
  candidate: Readonly<Record<string, unknown>>,
): Result.Result<MaterializedRoute, GrpcLeasedIdentityError> => {
  const values: Record<string, unknown> = {};
  const canonicalKeys: Array<string> = [];
  for (const routeField of routeFields) {
    const roundTrip = Result.try(() => {
      const value = routeField.identity.materializeDecoded(candidate[routeField.field]);
      return {
        canonicalKey: routeField.identity.canonicalKey(value),
        value,
      };
    });
    if (Result.isFailure(roundTrip)) {
      return Result.fail(
        identityError(
          "Route",
          `Leased topic ${topic} route field ${routeField.field} value does not match the topic schema or cannot be used as a stable leased gRPC route key.`,
          roundTrip.failure,
        ),
      );
    }
    defineRouteField(values, routeField.field, roundTrip.success.value);
    canonicalKeys.push(roundTrip.success.canonicalKey);
  }
  return Result.succeed({ canonicalKeys, values });
};

const internalRowKey = (feedKey: string, publicKey: string): string =>
  JSON.stringify(["leased-row", feedKey, publicKey]);

const publicRowKey = (
  feedKey: string,
  storageKey: string,
): Result.Result<string, GrpcLeasedIdentityError> => {
  const decoded = Result.try((): unknown => JSON.parse(storageKey));
  if (
    Result.isFailure(decoded) ||
    !Array.isArray(decoded.success) ||
    decoded.success.length !== 3 ||
    decoded.success[0] !== "leased-row" ||
    decoded.success[1] !== feedKey ||
    typeof decoded.success[2] !== "string"
  ) {
    return Result.fail(
      identityError(
        "RowKey",
        "Leased gRPC internal Row Key does not belong to the acquired feed identity.",
        Result.isFailure(decoded) ? decoded.failure : storageKey,
      ),
    );
  }
  return Result.succeed(decoded.success[2]);
};

const mapInternalKeys = (
  feedKey: string,
  internalKeys: ReadonlyArray<string>,
): Result.Result<ReadonlyArray<string>, GrpcLeasedIdentityError> => {
  const publicKeys: Array<string> = [];
  for (const internalKey of internalKeys) {
    const translated = publicRowKey(feedKey, internalKey);
    if (Result.isFailure(translated)) {
      return Result.fail(translated.failure);
    }
    publicKeys.push(translated.success);
  }
  return Result.succeed(publicKeys);
};

const makeRawResultKeyTranslation = <Row extends object>(
  feedKey: string,
): GrpcLeasedResultKeyTranslation<Row> => ({
  translateSnapshot: (internalKeys) => mapInternalKeys(feedKey, internalKeys),
  translateDelta: (operations) => {
    const translated: Array<DeltaOperation<Row>> = [];
    for (const operation of operations) {
      const publicKey = publicRowKey(feedKey, operation.key);
      if (Result.isFailure(publicKey)) {
        return Result.fail(publicKey.failure);
      }
      translated.push({ ...operation, key: publicKey.success });
    }
    return Result.succeed(translated);
  },
  clear: () => undefined,
});

const resultKeyError = (cause: unknown): GrpcLeasedIdentityError =>
  identityError(
    "ResultKey",
    "Leased gRPC grouped key value cannot be encoded as a stable public key",
    cause,
  );

const makeFailedResultKeyTranslation = <Row extends object>(
  error: GrpcLeasedIdentityError,
): GrpcLeasedResultKeyTranslation<Row> => ({
  translateSnapshot: () => Result.fail(error),
  translateDelta: () => Result.fail(error),
  clear: () => undefined,
});

const compileGroupedPublicKey = (
  schema: RowSchema,
  groupBy: ReadonlyArray<unknown>,
): Result.Result<
  (row: object) => Result.Result<string, GrpcLeasedIdentityError>,
  GrpcLeasedIdentityError
> => {
  const fields: Array<GroupedKeyIdentityField> = [];
  for (const field of groupBy) {
    if (typeof field !== "string") {
      return Result.fail(resultKeyError(field));
    }
    const fieldSchema = schema.fields[field];
    if (fieldSchema === undefined) {
      return Result.fail(resultKeyError(field));
    }
    const identity = Result.try(() => makeSchemaJsonIdentity(fieldSchema));
    if (Result.isFailure(identity)) {
      return Result.fail(resultKeyError(identity.failure));
    }
    fields.push({ field, canonicalKey: identity.success.canonicalKey });
  }
  const compiled = compileGroupedKeyIdentity<object>(fields, "throw");
  const publicKeyFromRow = (row: object): Result.Result<string, GrpcLeasedIdentityError> => {
    const key = Result.try(() => compiled.key(row));
    return Result.isFailure(key)
      ? Result.fail(resultKeyError(key.failure))
      : Result.succeed(key.success);
  };
  return Result.succeed(publicKeyFromRow);
};

const makeGroupedResultKeyTranslation = <Row extends object>(options: {
  readonly publicKeyFromRow: (row: object) => Result.Result<string, GrpcLeasedIdentityError>;
  readonly retentionObserver?: GrpcLeasedGroupedKeyRetentionObserver;
}): GrpcLeasedResultKeyTranslation<Row> => {
  const byInternalKey = new Map<string, string>();
  const pending = new Map<string, string | undefined>();
  const nextSnapshot = new Map<string, string>();

  if (options.retentionObserver !== undefined) {
    options.retentionObserver({
      retainedEntryCount: () => byInternalKey.size + pending.size + nextSnapshot.size,
    });
  }

  const translatedKey = (internalKey: string): Result.Result<string, GrpcLeasedIdentityError> => {
    const publicKey = pending.has(internalKey)
      ? pending.get(internalKey)
      : byInternalKey.get(internalKey);
    return publicKey === undefined
      ? Result.fail(resultKeyError(internalKey))
      : Result.succeed(publicKey);
  };

  const translateSnapshot = (
    internalKeys: ReadonlyArray<string>,
    rows: ReadonlyArray<Row>,
  ): Result.Result<ReadonlyArray<string>, GrpcLeasedIdentityError> => {
    nextSnapshot.clear();
    const publicKeys: Array<string> = [];
    for (const [index, internalKey] of internalKeys.entries()) {
      const row = rows[index];
      if (row === undefined) {
        nextSnapshot.clear();
        return Result.fail(resultKeyError(internalKey));
      }
      const publicKey = options.publicKeyFromRow(row);
      if (Result.isFailure(publicKey)) {
        nextSnapshot.clear();
        return Result.fail(publicKey.failure);
      }
      nextSnapshot.set(internalKey, publicKey.success);
      publicKeys.push(publicKey.success);
    }
    byInternalKey.clear();
    for (const [internalKey, publicKey] of nextSnapshot) {
      byInternalKey.set(internalKey, publicKey);
    }
    nextSnapshot.clear();
    return Result.succeed(publicKeys);
  };

  const rollbackDelta = (
    error: GrpcLeasedIdentityError,
  ): Result.Result<never, GrpcLeasedIdentityError> => {
    pending.clear();
    return Result.fail(error);
  };

  const translateDelta = (
    operations: ReadonlyArray<DeltaOperation<Row>>,
  ): Result.Result<ReadonlyArray<DeltaOperation<Row>>, GrpcLeasedIdentityError> => {
    pending.clear();
    const translated: Array<DeltaOperation<Row>> = [];
    for (const operation of operations) {
      if (operation.type === "move" || operation.type === "remove") {
        const publicKey = translatedKey(operation.key);
        if (Result.isFailure(publicKey)) {
          return rollbackDelta(publicKey.failure);
        }
        translated.push({ ...operation, key: publicKey.success });
        if (operation.type === "remove") {
          pending.set(operation.key, undefined);
        }
        continue;
      }
      const publicKey = options.publicKeyFromRow(operation.row);
      if (Result.isFailure(publicKey)) {
        return rollbackDelta(publicKey.failure);
      }
      pending.set(operation.key, publicKey.success);
      translated.push({ ...operation, key: publicKey.success });
    }
    for (const [internalKey, publicKey] of pending) {
      if (publicKey === undefined) {
        byInternalKey.delete(internalKey);
      } else {
        byInternalKey.set(internalKey, publicKey);
      }
    }
    pending.clear();
    return Result.succeed(translated);
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

const groupedFields = (query: unknown): ReadonlyArray<unknown> | undefined =>
  isRecord(query) && Array.isArray(query["groupBy"]) ? query["groupBy"] : undefined;

const encodeIdentityComponent = (value: string): string => {
  const encoded = Result.try(() => encodeURIComponent(value));
  return Result.isSuccess(encoded)
    ? encoded.success
    : `json:${encodeURIComponent(String(JSON.stringify(value)))}`;
};

export const makeGrpcLeasedIdentityContract = (input: {
  readonly topic: string;
  readonly feedName: string;
  readonly routeBy: ReadonlyArray<string>;
  readonly schema: RowSchema;
  readonly keyField: string;
}): Result.Result<GrpcLeasedIdentityContract, GrpcLeasedIdentityError> => {
  const routeBy = Result.try(() => [...input.routeBy]);
  if (Result.isFailure(routeBy)) {
    return Result.fail(
      identityError(
        "Configuration",
        "Leased gRPC route fields could not be inspected.",
        routeBy.failure,
      ),
    );
  }
  if (routeBy.success.length === 0 || new Set(routeBy.success).size !== routeBy.success.length) {
    return Result.fail(
      identityError(
        "Configuration",
        `Leased topic ${input.topic} must configure distinct route fields.`,
        routeBy.success,
      ),
    );
  }
  const routeFields: Array<CompiledRouteField> = [];
  for (const field of routeBy.success) {
    const fieldSchema = input.schema.fields[field];
    if (fieldSchema === undefined) {
      return Result.fail(
        identityError(
          "Configuration",
          `Leased topic ${input.topic} route field ${field} is not in the topic schema.`,
          field,
        ),
      );
    }
    const identity = Result.try(() => makeSchemaJsonIdentity(fieldSchema));
    if (Result.isFailure(identity)) {
      return Result.fail(
        identityError(
          "Configuration",
          `Leased topic ${input.topic} route field ${field} has no canonical identity codec.`,
          identity.failure,
        ),
      );
    }
    routeFields.push({ field, identity: identity.success });
  }
  const frozenRouteFields = Object.freeze(routeFields);
  const encodedTopic = encodeIdentityComponent(input.topic);
  const encodedFeedName = encodeIdentityComponent(input.feedName);

  const leaseFromQuery = (
    query: unknown,
  ): Result.Result<GrpcLeasedIdentityLease, GrpcLeasedIdentityError> => {
    if (!isRecord(query) || !isRecord(query["where"])) {
      return Result.fail(
        identityError(
          "Route",
          `Leased topic ${input.topic} requires exact equality filters for route fields: ${routeBy.success.join(", ")}.`,
          query,
        ),
      );
    }
    const candidate: Record<string, unknown> = {};
    for (const routeField of frozenRouteFields) {
      const value = exactEqValue(query["where"][routeField.field]);
      if (Result.isFailure(value)) {
        return Result.fail(
          identityError(
            "Route",
            `Leased topic ${input.topic} route field ${routeField.field} must use an exact eq filter.`,
            query["where"][routeField.field],
          ),
        );
      }
      defineRouteField(candidate, routeField.field, value.success);
    }
    const materialized = materializeRoute(input.topic, frozenRouteFields, candidate);
    if (Result.isFailure(materialized)) {
      return Result.fail(materialized.failure);
    }
    const feedKey = `${encodedTopic}/${encodedFeedName}/leased/${frozenRouteFields
      .map(
        (routeField, index) =>
          `${encodeIdentityComponent(routeField.field)}=${encodeIdentityComponent(materialized.success.canonicalKeys[index]!)}`,
      )
      .join("&")}`;
    const storedRoute = materialized.success.values;

    const materializeStoredRoute = (): Readonly<Record<string, unknown>> => {
      const values: Record<string, unknown> = {};
      for (const routeField of frozenRouteFields) {
        defineRouteField(
          values,
          routeField.field,
          routeField.identity.materializeDecoded(storedRoute[routeField.field]),
        );
      }
      return values;
    };

    const validateRowRoute = <Row extends object>(
      row: Row,
    ): Result.Result<Row, GrpcLeasedIdentityError> => {
      for (const [index, routeField] of frozenRouteFields.entries()) {
        const rowKey = Result.try(() =>
          routeField.identity.canonicalKey(Reflect.get(row, routeField.field)),
        );
        if (
          Result.isFailure(rowKey) ||
          rowKey.success !== materialized.success.canonicalKeys[index]
        ) {
          return Result.fail(
            identityError(
              "RouteMismatch",
              `gRPC leased feed ${input.feedName} mapped row field ${routeField.field} outside the acquired route.`,
              Result.isFailure(rowKey) ? rowKey.failure : Reflect.get(row, routeField.field),
            ),
          );
        }
      }
      return Result.succeed(row);
    };

    const internalizeRowKey = (
      row: object,
    ): Result.Result<GrpcLeasedInternalRowKey, GrpcLeasedIdentityError> => {
      const publicKey = Result.try(() => Reflect.get(row, input.keyField));
      if (Result.isFailure(publicKey) || typeof publicKey.success !== "string") {
        return Result.fail(
          identityError(
            "RowKey",
            `gRPC leased feed row key ${input.keyField} for ${input.topic} is not a string`,
            Result.isFailure(publicKey) ? publicKey.failure : publicKey.success,
          ),
        );
      }
      return Result.succeed({
        storageKey: internalRowKey(feedKey, publicKey.success),
      });
    };

    const resultKeys = <Row extends object>(
      query: unknown,
      retentionObserver?: GrpcLeasedGroupedKeyRetentionObserver,
    ): GrpcLeasedResultKeyTranslation<Row> => {
      const groupBy = groupedFields(query);
      if (groupBy === undefined) {
        return makeRawResultKeyTranslation(feedKey);
      }
      const publicKey = compileGroupedPublicKey(input.schema, groupBy);
      return Result.isFailure(publicKey)
        ? makeFailedResultKeyTranslation(publicKey.failure)
        : makeGroupedResultKeyTranslation({
            publicKeyFromRow: publicKey.success,
            ...(retentionObserver === undefined ? {} : { retentionObserver }),
          });
    };

    return Result.succeed({
      feedKey,
      materializeRoute: materializeStoredRoute,
      validateRowRoute,
      internalizeRowKey,
      resultKeys,
    });
  };

  return Result.succeed({ leaseFromQuery });
};
