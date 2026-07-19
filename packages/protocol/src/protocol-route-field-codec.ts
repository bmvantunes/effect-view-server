import type { RowSchema, ViewServerRuntimeError } from "@effect-view-server/config";
import { isWireSafeBigDecimal } from "@effect-view-server/effect-utils";
import { Effect, Result, Schema } from "effect";
import { make as makeBigDecimal, type BigDecimal } from "effect/BigDecimal";

type RouteScalar = null | string | number | bigint | boolean | BigDecimal;

const routeScalarTag = "$effect-view-server/route-scalar";

const invalidQuery = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidQuery",
  message,
  topic,
});

const isPlainRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" &&
  value !== null &&
  !Array.isArray(value) &&
  Object.getPrototypeOf(value) === Object.prototype;

const ownDataValue = (
  value: Readonly<Record<string, unknown>>,
  key: string,
): { readonly found: boolean; readonly value: unknown } => {
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  return descriptor !== undefined && descriptor.enumerable && "value" in descriptor
    ? { found: true, value: descriptor.value }
    : { found: false, value: undefined };
};

const hasExactKeys = (
  value: Readonly<Record<string, unknown>>,
  keys: ReadonlyArray<string>,
): boolean =>
  Object.getOwnPropertySymbols(value).length === 0 &&
  Object.getOwnPropertyNames(value).length === keys.length &&
  keys.every((key) => ownDataValue(value, key).found);

const isRouteScalar = (value: unknown): value is RouteScalar =>
  value === null ||
  typeof value === "string" ||
  typeof value === "bigint" ||
  typeof value === "boolean" ||
  isWireSafeBigDecimal(value) ||
  (typeof value === "number" && Number.isFinite(value));

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
  if (!isRouteScalar(value) || !scalarSatisfiesSchema(schema, value)) {
    return Effect.fail(
      invalidQuery(topic, `routeBy field ${field} does not satisfy its configured scalar schema`),
    );
  }
  if (isWireSafeBigDecimal(value)) {
    return Effect.succeed({
      [routeScalarTag]: "bigDecimal",
      coefficient: value.value.toString(),
      scale: Object.is(value.scale, -0) ? "-0" : String(value.scale),
    });
  }
  if (typeof value === "bigint") {
    return Effect.succeed({ [routeScalarTag]: "bigint", value: value.toString() });
  }
  if (typeof value === "number" && Object.is(value, -0)) {
    return Effect.succeed({ [routeScalarTag]: "negativeZero" });
  }
  return Effect.succeed(value);
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

const decodeRouteEnvelope = (value: Readonly<Record<string, unknown>>): RouteScalar | undefined => {
  const tag = ownDataValue(value, routeScalarTag);
  if (!tag.found) {
    return undefined;
  }
  if (tag.value === "negativeZero" && hasExactKeys(value, [routeScalarTag])) {
    return -0;
  }
  if (tag.value === "bigint" && hasExactKeys(value, [routeScalarTag, "value"])) {
    return parseInteger(ownDataValue(value, "value").value);
  }
  if (tag.value === "bigDecimal" && hasExactKeys(value, [routeScalarTag, "coefficient", "scale"])) {
    const coefficient = parseInteger(ownDataValue(value, "coefficient").value);
    const scale = parseSafeInteger(ownDataValue(value, "scale").value);
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
  const candidate = isRouteScalar(value)
    ? value
    : isPlainRecord(value)
      ? decodeRouteEnvelope(value)
      : undefined;
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
  routeBy: Readonly<Record<string, unknown>> | undefined,
  transform: RouteScalarTransform<Value>,
) {
  if (routeBy === undefined) {
    return undefined;
  }
  if (!isPlainRecord(routeBy) || Object.getOwnPropertySymbols(routeBy).length > 0) {
    return yield* Effect.fail(invalidQuery(topic, "Query routeBy must be a plain object"));
  }
  const output: Record<string, Value> = {};
  for (const field of Object.getOwnPropertyNames(routeBy)) {
    const fieldValue = ownDataValue(routeBy, field);
    const schema = Object.hasOwn(rowSchema.fields, field) ? rowSchema.fields[field] : undefined;
    if (schema === undefined || !fieldValue.found) {
      return yield* Effect.fail(invalidQuery(topic, `Invalid routeBy field: ${field}`));
    }
    defineRouteField(output, field, yield* transform(topic, field, schema, fieldValue.value));
  }
  return Object.freeze(output);
});

export const encodeRouteBy = Effect.fn("ViewServerProtocol.routeBy.encode")(function* (
  topic: string,
  rowSchema: RowSchema,
  routeBy: Readonly<Record<string, unknown>> | undefined,
) {
  return yield* transformRouteBy(topic, rowSchema, routeBy, encodeRouteScalar);
});

export const decodeRouteBy = Effect.fn("ViewServerProtocol.routeBy.decode")(function* (
  topic: string,
  rowSchema: RowSchema,
  routeBy: Readonly<Record<string, unknown>> | undefined,
) {
  return yield* transformRouteBy(topic, rowSchema, routeBy, decodeRouteScalar);
});
