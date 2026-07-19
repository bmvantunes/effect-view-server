import type { DeltaOperation, RowSchema } from "@effect-view-server/config";
import type { ViewServerRuntimeCoreQueryPartition } from "@effect-view-server/runtime-core/internal";
import {
  compileGroupedKeyIdentity,
  isWireSafeBigDecimal,
  makeSchemaJsonIdentity,
  type GroupedKeyIdentityField,
} from "@effect-view-server/effect-utils";
import { Result, Schema } from "effect";
import { make as makeBigDecimal, type BigDecimal } from "effect/BigDecimal";

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
  readonly enginePartition: ViewServerRuntimeCoreQueryPartition;
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
  readonly is: (value: unknown) => boolean;
};

type MaterializedRoute = {
  readonly canonicalKeys: ReadonlyArray<string>;
  readonly values: ReadonlyArray<RouteScalar>;
};

const identityError = (
  kind: GrpcLeasedIdentityError["kind"],
  message: string,
  cause: unknown,
): GrpcLeasedIdentityError => new GrpcLeasedIdentityError({ kind, message, cause });

const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

type RouteScalar = null | string | number | bigint | boolean | BigDecimal;

const isRouteScalar = (value: unknown): value is RouteScalar =>
  value === null ||
  typeof value === "string" ||
  typeof value === "bigint" ||
  typeof value === "boolean" ||
  isWireSafeBigDecimal(value) ||
  (typeof value === "number" && Number.isFinite(value));

const copyRouteScalar = (value: RouteScalar): RouteScalar =>
  isWireSafeBigDecimal(value) ? makeBigDecimal(value.value, value.scale) : value;

const exactRouteScalarMatches = (actual: unknown, expected: RouteScalar): boolean => {
  if (isWireSafeBigDecimal(expected)) {
    return (
      isWireSafeBigDecimal(actual) &&
      actual.value === expected.value &&
      Object.is(actual.scale, expected.scale)
    );
  }
  return Object.is(actual, expected);
};

const exactRouteScalarKey = (value: RouteScalar): string => {
  if (isWireSafeBigDecimal(value)) {
    return JSON.stringify([
      "bigDecimal",
      value.value.toString(),
      Object.is(value.scale, -0) ? "-0" : String(value.scale),
    ]);
  }
  if (value === null) {
    return JSON.stringify(["null"]);
  }
  switch (typeof value) {
    case "string":
      return JSON.stringify(["string", value]);
    case "number":
      return JSON.stringify(["number", Object.is(value, -0) ? "-0" : String(value)]);
    case "bigint":
      return JSON.stringify(["bigint", value.toString()]);
    case "boolean":
      return JSON.stringify(["boolean", value]);
  }
};

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
  const values: Array<RouteScalar> = [];
  const canonicalKeys: Array<string> = [];
  for (const routeField of routeFields) {
    const owned = Result.try(() => {
      const candidateValue = candidate[routeField.field];
      if (!isRouteScalar(candidateValue) || !routeField.is(candidateValue)) {
        throw new TypeError("Route value does not satisfy its configured scalar schema.");
      }
      const value = copyRouteScalar(candidateValue);
      return {
        canonicalKey: exactRouteScalarKey(value),
        value,
      };
    });
    if (Result.isFailure(owned)) {
      return Result.fail(
        identityError(
          "Route",
          `Leased topic ${topic} route field ${routeField.field} value does not match the topic schema or cannot be used as a stable leased gRPC route key.`,
          owned.failure,
        ),
      );
    }
    values.push(owned.success.value);
    canonicalKeys.push(owned.success.canonicalKey);
  }
  return Result.succeed({ canonicalKeys, values });
};

const internalRowKey = (feedKey: string, publicKey: string): string =>
  JSON.stringify(["leased-row", feedKey, publicKey]);

const publicRowKey = (
  publicKeysByStorageKey: ReadonlyMap<string, string>,
  storageKey: string,
): Result.Result<string, GrpcLeasedIdentityError> => {
  const publicKey = publicKeysByStorageKey.get(storageKey);
  if (publicKey === undefined) {
    return Result.fail(
      identityError(
        "RowKey",
        "Leased gRPC internal Row Key does not belong to the acquired feed identity.",
        storageKey,
      ),
    );
  }
  return Result.succeed(publicKey);
};

const mapInternalKeys = (
  publicKeysByStorageKey: ReadonlyMap<string, string>,
  internalKeys: ReadonlyArray<string>,
): Result.Result<ReadonlyArray<string>, GrpcLeasedIdentityError> => {
  const publicKeys: Array<string> = [];
  for (const internalKey of internalKeys) {
    const translated = publicRowKey(publicKeysByStorageKey, internalKey);
    if (Result.isFailure(translated)) {
      return Result.fail(translated.failure);
    }
    publicKeys.push(translated.success);
  }
  return Result.succeed(publicKeys);
};

const makeRawResultKeyTranslation = <Row extends object>(
  publicKeysByStorageKey: ReadonlyMap<string, string>,
): GrpcLeasedResultKeyTranslation<Row> => ({
  translateSnapshot: (internalKeys) => mapInternalKeys(publicKeysByStorageKey, internalKeys),
  translateDelta: (operations) => {
    const translated: Array<DeltaOperation<Row>> = [];
    for (const operation of operations) {
      const publicKey = publicRowKey(publicKeysByStorageKey, operation.key);
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
    const fieldSchema = Result.try(() => schema.fields[field]);
    if (Result.isFailure(fieldSchema)) {
      return Result.fail(resultKeyError(fieldSchema.failure));
    }
    const schemaField = fieldSchema.success;
    if (schemaField === undefined) {
      return Result.fail(resultKeyError(field));
    }
    const identity = Result.try(() => makeSchemaJsonIdentity(schemaField));
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
    const fieldSchema = Result.try(() => input.schema.fields[field]);
    if (Result.isFailure(fieldSchema)) {
      return Result.fail(
        identityError(
          "Configuration",
          `Leased topic ${input.topic} route field ${field} could not be inspected in the topic schema.`,
          fieldSchema.failure,
        ),
      );
    }
    const schemaField = fieldSchema.success;
    if (schemaField === undefined) {
      return Result.fail(
        identityError(
          "Configuration",
          `Leased topic ${input.topic} route field ${field} is not in the topic schema.`,
          field,
        ),
      );
    }
    const fieldIs = Result.try(() => Schema.is(schemaField));
    if (Result.isFailure(fieldIs)) {
      return Result.fail(
        identityError(
          "Configuration",
          `Leased topic ${input.topic} route field ${field} has no usable scalar schema validator.`,
          fieldIs.failure,
        ),
      );
    }
    routeFields.push({ field, is: fieldIs.success });
  }
  const frozenRouteFields = Object.freeze(routeFields);
  const frozenRouteFieldNames = new Set(frozenRouteFields.map((routeField) => routeField.field));
  const encodedTopic = encodeIdentityComponent(input.topic);
  const encodedFeedName = encodeIdentityComponent(input.feedName);
  let leaseSequence = 0n;

  const leaseFromQuery = (
    query: unknown,
  ): Result.Result<GrpcLeasedIdentityLease, GrpcLeasedIdentityError> => {
    if (!isRecord(query) || !isRecord(query["routeBy"])) {
      return Result.fail(
        identityError(
          "Route",
          `Leased topic ${input.topic} requires routeBy fields: ${routeBy.success.join(", ")}.`,
          query,
        ),
      );
    }
    const queryRoute = query["routeBy"];
    const actualFields = Object.getOwnPropertyNames(queryRoute);
    if (
      Object.getOwnPropertySymbols(queryRoute).length > 0 ||
      actualFields.length !== frozenRouteFields.length ||
      actualFields.some((field) => !frozenRouteFieldNames.has(field))
    ) {
      return Result.fail(
        identityError(
          "Route",
          `Leased topic ${input.topic} routeBy must contain all and only: ${routeBy.success.join(", ")}.`,
          queryRoute,
        ),
      );
    }
    const candidate: Record<string, unknown> = {};
    for (const routeField of frozenRouteFields) {
      const descriptor = Object.getOwnPropertyDescriptor(queryRoute, routeField.field);
      if (
        descriptor === undefined ||
        !descriptor.enumerable ||
        !("value" in descriptor) ||
        !isRouteScalar(descriptor.value)
      ) {
        return Result.fail(
          identityError(
            "Route",
            `Leased topic ${input.topic} routeBy field ${routeField.field} must be an own scalar data value.`,
            queryRoute,
          ),
        );
      }
      defineRouteField(candidate, routeField.field, descriptor.value);
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
    leaseSequence += 1n;
    const enginePartitionKey = `${feedKey}/lease:${leaseSequence}`;
    const storedRoute = materialized.success.values;
    const publicKeysByStorageKey = new Map<string, string>();

    const materializeStoredRoute = (): Readonly<Record<string, unknown>> => {
      const values: Record<string, unknown> = {};
      for (const [index, routeField] of frozenRouteFields.entries()) {
        const value = storedRoute[index]!;
        defineRouteField(values, routeField.field, copyRouteScalar(value));
      }
      return values;
    };

    const validateRowRoute = <Row extends object>(
      row: Row,
    ): Result.Result<Row, GrpcLeasedIdentityError> => {
      for (const [index, routeField] of frozenRouteFields.entries()) {
        const descriptor = Result.try(() => Object.getOwnPropertyDescriptor(row, routeField.field));
        if (
          Result.isFailure(descriptor) ||
          descriptor.success === undefined ||
          descriptor.success.enumerable !== true ||
          !("value" in descriptor.success)
        ) {
          return Result.fail(
            identityError(
              "RouteMismatch",
              `gRPC leased feed ${input.feedName} mapped row field ${routeField.field} outside the acquired route.`,
              Result.isFailure(descriptor) ? descriptor.failure : descriptor.success,
            ),
          );
        }
        const rowValue = descriptor.success.value;
        if (!exactRouteScalarMatches(rowValue, storedRoute[index]!)) {
          return Result.fail(
            identityError(
              "RouteMismatch",
              `gRPC leased feed ${input.feedName} mapped row field ${routeField.field} outside the acquired route.`,
              rowValue,
            ),
          );
        }
      }
      return Result.succeed(row);
    };

    const matchesEnginePartition = (row: object, storageKey?: string): boolean => {
      if (storageKey !== undefined) {
        return publicKeysByStorageKey.has(storageKey);
      }
      try {
        for (const [index, routeField] of frozenRouteFields.entries()) {
          if (
            !Object.hasOwn(row, routeField.field) ||
            !exactRouteScalarMatches(Reflect.get(row, routeField.field), storedRoute[index]!)
          ) {
            return false;
          }
        }
        return true;
      } catch {
        return false;
      }
    };

    const enginePartition: ViewServerRuntimeCoreQueryPartition = Object.freeze({
      key: enginePartitionKey,
      matches: matchesEnginePartition,
      ownedStorageKeys: () => publicKeysByStorageKey.keys(),
    });

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
      const storageKey = internalRowKey(feedKey, publicKey.success);
      publicKeysByStorageKey.set(storageKey, publicKey.success);
      return Result.succeed({
        storageKey,
      });
    };

    const resultKeys = <Row extends object>(
      query: unknown,
      retentionObserver?: GrpcLeasedGroupedKeyRetentionObserver,
    ): GrpcLeasedResultKeyTranslation<Row> => {
      const groupBy = groupedFields(query);
      if (groupBy === undefined) {
        return makeRawResultKeyTranslation(publicKeysByStorageKey);
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
      enginePartition,
      materializeRoute: materializeStoredRoute,
      validateRowRoute,
      internalizeRowKey,
      resultKeys,
    });
  };

  return Result.succeed({ leaseFromQuery });
};
