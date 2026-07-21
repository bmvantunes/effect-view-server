import type { RowSchema, ViewServerRuntimeError } from "@effect-view-server/config";
import { inspectWireSafeBigDecimal } from "@effect-view-server/effect-utils";
import { Effect, Result, Schema } from "effect";
import { make as makeBigDecimal, type BigDecimal } from "effect/BigDecimal";
import {
  protocolRecordSnapshot,
  protocolSnapshotDataValue,
  protocolSnapshotHasExactDataKeys,
  type ProtocolRecordSnapshot,
} from "./protocol-structural-value";

type RouteScalar = null | string | number | bigint | boolean | BigDecimal;

const routeScalarTag = "$effect-view-server/route-scalar";

const invalidQuery = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidQuery",
  message,
  topic,
});

const ownDataValue = (snapshot: ProtocolRecordSnapshot, key: string): unknown =>
  protocolSnapshotDataValue(snapshot, key);

const hasExactKeys = (snapshot: ProtocolRecordSnapshot, keys: ReadonlyArray<string>): boolean =>
  protocolSnapshotHasExactDataKeys(snapshot, new Set(keys));

const routeScalarSnapshot = (value: unknown): RouteScalar | undefined => {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "bigint" ||
    typeof value === "boolean" ||
    (typeof value === "number" && Number.isFinite(value))
  ) {
    return value;
  }
  const decimal = inspectWireSafeBigDecimal(value);
  return decimal._tag === "Success"
    ? makeBigDecimal(decimal.coefficient, decimal.scale)
    : undefined;
};

const scalarSatisfiesSchema = (
  schema: Schema.Codec<unknown, unknown, never, never>,
  value: RouteScalar,
): boolean => {
  const result = Result.try(() => Schema.is(schema)(value));
  return Result.isSuccess(result) && result.success;
};

const encodeRouteScalar = (
  topic: string,
  field: string,
  schema: Schema.Codec<unknown, unknown, never, never>,
  value: unknown,
): Effect.Effect<Schema.Json, ViewServerRuntimeError> => {
  const candidate = routeScalarSnapshot(value);
  if (candidate === undefined || !scalarSatisfiesSchema(schema, candidate)) {
    return Effect.fail(
      invalidQuery(topic, `routeBy field ${field} does not satisfy its configured scalar schema`),
    );
  }
  if (candidate !== null && typeof candidate === "object") {
    return Effect.succeed({
      [routeScalarTag]: "bigDecimal",
      coefficient: candidate.value.toString(),
      scale: Object.is(candidate.scale, -0) ? "-0" : String(candidate.scale),
    });
  }
  if (typeof candidate === "bigint") {
    return Effect.succeed({ [routeScalarTag]: "bigint", value: candidate.toString() });
  }
  if (typeof candidate === "number" && Object.is(candidate, -0)) {
    return Effect.succeed({ [routeScalarTag]: "negativeZero" });
  }
  return Effect.succeed(candidate);
};

const parseInteger = (value: unknown): bigint | undefined => {
  if (typeof value !== "string" || !/^-?(?:0|[1-9]\d*)$/u.test(value)) {
    return undefined;
  }
  return BigInt(value);
};

const parseSafeInteger = (value: unknown): number | undefined => {
  if (typeof value !== "string" || !/^-?(?:0|[1-9]\d*)$/u.test(value)) {
    return undefined;
  }
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : undefined;
};

const decodeRouteEnvelope = (snapshot: ProtocolRecordSnapshot): RouteScalar | undefined => {
  const tag = ownDataValue(snapshot, routeScalarTag);
  if (tag === undefined) {
    return undefined;
  }
  if (tag === "negativeZero" && hasExactKeys(snapshot, [routeScalarTag])) {
    return -0;
  }
  if (tag === "bigint" && hasExactKeys(snapshot, [routeScalarTag, "value"])) {
    return parseInteger(ownDataValue(snapshot, "value"));
  }
  if (tag === "bigDecimal" && hasExactKeys(snapshot, [routeScalarTag, "coefficient", "scale"])) {
    const coefficient = parseInteger(ownDataValue(snapshot, "coefficient"));
    const scale = parseSafeInteger(ownDataValue(snapshot, "scale"));
    return coefficient !== undefined && scale !== undefined
      ? makeBigDecimal(coefficient, scale)
      : undefined;
  }
  return undefined;
};

const decodeRouteScalar = (
  topic: string,
  field: string,
  schema: Schema.Codec<unknown, unknown, never, never>,
  value: unknown,
): Effect.Effect<RouteScalar, ViewServerRuntimeError> => {
  const scalar = routeScalarSnapshot(value);
  const snapshot = scalar === undefined ? protocolRecordSnapshot(value) : undefined;
  const candidate =
    scalar !== undefined
      ? scalar
      : snapshot === undefined
        ? undefined
        : decodeRouteEnvelope(snapshot);
  return candidate !== undefined && scalarSatisfiesSchema(schema, candidate)
    ? Effect.succeed(candidate)
    : Effect.fail(
        invalidQuery(topic, `routeBy field ${field} does not satisfy its configured scalar schema`),
      );
};

const defineRouteField = <Value>(
  routeBy: Record<string, Value>,
  field: string,
  value: Value,
): void => {
  Object.defineProperty(routeBy, field, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
};

type RouteScalarTransform<Value> = (
  topic: string,
  field: string,
  schema: Schema.Codec<unknown, unknown, never, never>,
  value: unknown,
) => Effect.Effect<Value, ViewServerRuntimeError>;

const transformRouteBy = Effect.fn("ViewServerProtocol.routeBy.transform")(function* <Value>(
  topic: string,
  rowSchema: RowSchema,
  routeBy: unknown,
  transform: RouteScalarTransform<Value>,
) {
  if (routeBy === undefined) {
    return undefined;
  }
  const snapshot = protocolRecordSnapshot(routeBy);
  if (snapshot === undefined) {
    return yield* Effect.fail(invalidQuery(topic, "Query routeBy must be a plain object"));
  }
  const output: Record<string, Value> = {};
  for (const [field, fieldValue] of snapshot.entries) {
    const schema = Object.hasOwn(rowSchema.fields, field) ? rowSchema.fields[field] : undefined;
    if (schema === undefined) {
      return yield* Effect.fail(invalidQuery(topic, `Invalid routeBy field: ${field}`));
    }
    defineRouteField(output, field, yield* transform(topic, field, schema, fieldValue));
  }
  return Object.freeze(output);
});

export const encodeRouteBy = Effect.fn("ViewServerProtocol.routeBy.encode")(function* (
  topic: string,
  rowSchema: RowSchema,
  routeBy: unknown,
) {
  return yield* transformRouteBy(topic, rowSchema, routeBy, encodeRouteScalar);
});

export const decodeRouteBy = Effect.fn("ViewServerProtocol.routeBy.decode")(function* (
  topic: string,
  rowSchema: RowSchema,
  routeBy: unknown,
) {
  return yield* transformRouteBy(topic, rowSchema, routeBy, decodeRouteScalar);
});
