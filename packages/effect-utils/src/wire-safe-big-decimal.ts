import { isBigDecimal, type BigDecimal } from "effect/BigDecimal";
import type { Schema } from "effect";

type WireSafeBigDecimalInput = Schema.Schema.Type<typeof Schema.Unknown>;

export type WireSafeBigDecimal = {
  readonly "~effect/BigDecimal": "~effect/BigDecimal";
  readonly value: bigint;
  readonly scale: number;
};

export type WireSafeBigDecimalInspection =
  | { readonly _tag: "NotBigDecimal" }
  | { readonly _tag: "UnsafeBigDecimal" }
  | { readonly _tag: "ReflectionFailure" }
  | {
      readonly _tag: "Success";
      readonly source: WireSafeBigDecimal;
      readonly coefficient: bigint;
      readonly scale: number;
      readonly semanticKey: string;
    };

const wireSafeBigDecimalComparisonMetadataTypeId: unique symbol = Symbol(
  "effect-view-server/WireSafeBigDecimalComparisonMetadata",
);

/**
 * Opaque, reusable comparison evidence for one admitted wire-safe BigDecimal.
 *
 * Consumers may cache this token beside an owned value for the lifetime of the loaded module
 * instance. Tokens are process-local capabilities: do not clone, serialize, transfer, or persist
 * them. A token from another module instance is foreign. Its private representation remains the
 * View Server's responsibility so comparison semantics cannot drift into downstream packages.
 */
export type WireSafeBigDecimalComparisonMetadata = {
  readonly [wireSafeBigDecimalComparisonMetadataTypeId]: true;
};

type WireSafeBigDecimalComparisonParts = {
  readonly coefficient: bigint;
  readonly scale: number;
  readonly negative: boolean;
  readonly unsignedDigits: string;
  readonly magnitude: bigint;
};

const comparisonPartsByMetadata = new WeakMap<
  WireSafeBigDecimalComparisonMetadata,
  WireSafeBigDecimalComparisonParts
>();

const notBigDecimal: WireSafeBigDecimalInspection = { _tag: "NotBigDecimal" };
const unsafeBigDecimal: WireSafeBigDecimalInspection = { _tag: "UnsafeBigDecimal" };
const reflectionFailure: WireSafeBigDecimalInspection = { _tag: "ReflectionFailure" };
const bigDecimalTypeId = "~effect/BigDecimal";

const isBigInt = (value: unknown): value is bigint => typeof value === "bigint";
const isNumber = (value: unknown): value is number => typeof value === "number";

const hasBigDecimalPrototype = (value: WireSafeBigDecimalInput): value is WireSafeBigDecimal =>
  hasBigDecimalPrototypeBrand(value);

const isObjectInput = (value: WireSafeBigDecimalInput): value is object =>
  typeof value === "object" && value !== null;

const hasBigDecimalPrototypeBrand = (value: WireSafeBigDecimalInput): value is object => {
  if (!isObjectInput(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  if (typeof prototype !== "object" || prototype === null) {
    return false;
  }
  const descriptor = Object.getOwnPropertyDescriptor(prototype, bigDecimalTypeId);
  return descriptor !== undefined && "value" in descriptor && descriptor.value === bigDecimalTypeId;
};

const ownEnumerableDataValue = (value: WireSafeBigDecimal, key: string): unknown => {
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  return descriptor !== undefined && descriptor.enumerable && "value" in descriptor
    ? descriptor.value
    : undefined;
};

type CanonicalWireSafeBigDecimalParts = {
  readonly coefficient: bigint;
  readonly scale: number;
  readonly semanticKey: string;
};

const canonicalWireSafeBigDecimalParts = (
  coefficient: bigint,
  scale: number,
): CanonicalWireSafeBigDecimalParts | undefined => {
  if (!Number.isSafeInteger(scale)) {
    return undefined;
  }
  if (coefficient === 0n) {
    return { coefficient: 0n, scale: 0, semanticKey: '["0","0"]' };
  }

  const source = coefficient.toString();
  let end = source.length;
  while (source.charCodeAt(end - 1) === 48) {
    end -= 1;
  }
  const trailingZeroCount = source.length - end;
  const normalizedScale = scale - trailingZeroCount;
  if (!Number.isSafeInteger(normalizedScale)) {
    return undefined;
  }
  const normalizedCoefficient =
    trailingZeroCount === 0 ? coefficient : BigInt(source.slice(0, end));
  const canonicalScale = normalizedScale === 0 ? 0 : normalizedScale;

  // A non-negative safe scale can only reduce the finite coefficient exponent.
  if (canonicalScale < 0) {
    const coefficientWidth = end - (coefficient < 0n ? 1 : 0);
    const decimalTailLength = coefficientWidth - 1;
    const exponent = decimalTailLength - canonicalScale;
    if (!Number.isSafeInteger(exponent)) {
      return undefined;
    }
  }

  return {
    coefficient: normalizedCoefficient,
    scale: canonicalScale,
    semanticKey: JSON.stringify([normalizedCoefficient.toString(), String(canonicalScale)]),
  };
};

export const inspectWireSafeBigDecimal = (
  value: WireSafeBigDecimalInput,
): WireSafeBigDecimalInspection => {
  try {
    if (!hasBigDecimalPrototype(value)) {
      return notBigDecimal;
    }
    const coefficient = ownEnumerableDataValue(value, "value");
    const scale = ownEnumerableDataValue(value, "scale");
    if (typeof coefficient !== "bigint" || typeof scale !== "number") {
      return unsafeBigDecimal;
    }
    const canonical = canonicalWireSafeBigDecimalParts(coefficient, scale);
    return canonical === undefined
      ? unsafeBigDecimal
      : {
          _tag: "Success",
          source: value,
          coefficient,
          scale,
          semanticKey: canonical.semanticKey,
        };
  } catch {
    return reflectionFailure;
  }
};

export const isWireSafeBigDecimal = (
  value: WireSafeBigDecimalInput,
): value is WireSafeBigDecimal => {
  return inspectWireSafeBigDecimal(value)._tag === "Success";
};

export const wireSafeBigDecimalSemanticKey = (
  value: WireSafeBigDecimalInput,
): string | undefined => {
  const inspection = inspectWireSafeBigDecimal(value);
  return inspection._tag === "Success" ? inspection.semanticKey : undefined;
};

export const trustedWireSafeBigDecimalSemanticKey = (value: BigDecimal): string | undefined => {
  try {
    const coefficient = value.value;
    const scale = value.scale;
    return typeof coefficient === "bigint" && typeof scale === "number"
      ? canonicalWireSafeBigDecimalParts(coefficient, scale)?.semanticKey
      : undefined;
  } catch {
    return undefined;
  }
};

/** Allocation-free guard for values that already crossed a schema or query-ownership boundary. */
export const isTrustedWireSafeBigDecimal = (value: unknown): value is BigDecimal => {
  try {
    if (!isBigDecimal(value)) {
      return false;
    }
    const coefficient = value.value;
    const scale = value.scale;
    return (
      typeof coefficient === "bigint" && typeof scale === "number" && Number.isSafeInteger(scale)
    );
  } catch {
    return false;
  }
};

const compareUnsignedDecimalDigits = (left: string, right: string): number => {
  const width = Math.max(left.length, right.length);
  for (let index = 0; index < width; index += 1) {
    const leftDigit = index < left.length ? left.charCodeAt(index) : 48;
    const rightDigit = index < right.length ? right.charCodeAt(index) : 48;
    if (leftDigit !== rightDigit) {
      return leftDigit < rightDigit ? -1 : 1;
    }
  }
  return 0;
};

const makeWireSafeBigDecimalComparisonParts = (
  coefficient: bigint,
  scale: number,
): WireSafeBigDecimalComparisonParts => {
  const negative = coefficient < 0n;
  const unsignedDigits = (negative ? -coefficient : coefficient).toString();
  return {
    coefficient,
    scale,
    negative,
    unsignedDigits,
    magnitude: BigInt(unsignedDigits.length) - BigInt(scale),
  };
};

const makeComparisonMetadata = (
  parts: WireSafeBigDecimalComparisonParts,
): WireSafeBigDecimalComparisonMetadata => {
  const metadata: WireSafeBigDecimalComparisonMetadata = Object.freeze({
    [wireSafeBigDecimalComparisonMetadataTypeId]: true,
  });
  comparisonPartsByMetadata.set(metadata, parts);
  return metadata;
};

/** Builds opaque, cacheable comparison evidence after hostile-input admission. */
export const wireSafeBigDecimalComparisonMetadata = (
  value: WireSafeBigDecimalInput,
): WireSafeBigDecimalComparisonMetadata | undefined => {
  const inspection = inspectWireSafeBigDecimal(value);
  if (inspection._tag !== "Success") {
    return undefined;
  }
  const parts = makeWireSafeBigDecimalComparisonParts(inspection.coefficient, inspection.scale);
  return makeComparisonMetadata(parts);
};

/** Builds opaque, cacheable comparison evidence for an already-owned Effect BigDecimal. */
export const trustedWireSafeBigDecimalComparisonMetadata = (
  value: BigDecimal,
): WireSafeBigDecimalComparisonMetadata | undefined => {
  let coefficient: unknown;
  let scale: unknown;

  try {
    coefficient = value.value;
    scale = value.scale;
  } catch {
    return undefined;
  }

  if (
    !isBigInt(coefficient) ||
    !isNumber(scale) ||
    canonicalWireSafeBigDecimalParts(coefficient, scale) === undefined
  ) {
    return undefined;
  }

  return makeComparisonMetadata(makeWireSafeBigDecimalComparisonParts(coefficient, scale));
};

/**
 * Compares valid wire BigDecimals without materializing a power of ten.
 *
 * Effect's general-purpose comparator aligns scales with exponentiation. A wire-safe scale can be
 * any safe integer, so that implementation can still attempt an impossibly large allocation. This
 * comparator instead compares decimal magnitude and coefficient digits in O(coefficient digits).
 */
const compareWireSafeBigDecimalParts = (
  leftValue: bigint,
  leftScale: number,
  rightValue: bigint,
  rightScale: number,
): number => {
  return compareWireSafeBigDecimalScalars(leftValue, leftScale, rightValue, rightScale);
};

const compareWireSafeBigDecimalScalars = (
  leftCoefficient: bigint,
  leftScale: number,
  rightCoefficient: bigint,
  rightScale: number,
  preparedLeftDigits?: string,
  preparedLeftMagnitude?: bigint,
  preparedRightDigits?: string,
  preparedRightMagnitude?: bigint,
): number => {
  if (leftCoefficient === rightCoefficient && Object.is(leftScale, rightScale)) {
    return 0;
  }
  if (leftCoefficient === 0n) {
    return rightCoefficient === 0n ? 0 : rightCoefficient < 0n ? 1 : -1;
  }
  if (rightCoefficient === 0n) {
    return leftCoefficient < 0n ? -1 : 1;
  }
  const leftNegative = leftCoefficient < 0n;
  const rightNegative = rightCoefficient < 0n;
  if (leftNegative !== rightNegative) {
    return leftNegative ? -1 : 1;
  }
  const leftDigits =
    preparedLeftDigits ?? (leftNegative ? -leftCoefficient : leftCoefficient).toString();
  const rightDigits =
    preparedRightDigits ?? (rightNegative ? -rightCoefficient : rightCoefficient).toString();
  const leftMagnitude = preparedLeftMagnitude ?? BigInt(leftDigits.length) - BigInt(leftScale);
  const rightMagnitude = preparedRightMagnitude ?? BigInt(rightDigits.length) - BigInt(rightScale);
  const unsignedComparison =
    leftMagnitude === rightMagnitude
      ? compareUnsignedDecimalDigits(leftDigits, rightDigits)
      : leftMagnitude < rightMagnitude
        ? -1
        : 1;
  return leftNegative ? -unsignedComparison : unsignedComparison;
};

/**
 * Compares two reusable metadata tokens without rereading or re-stringifying either value.
 * Returns `undefined` when either token is foreign, forged, cloned, or otherwise not owned by this
 * loaded module instance.
 */
export const compareWireSafeBigDecimalComparisonMetadata = (
  left: WireSafeBigDecimalComparisonMetadata,
  right: WireSafeBigDecimalComparisonMetadata,
): number | undefined => {
  const leftParts = comparisonPartsByMetadata.get(left);
  const rightParts = comparisonPartsByMetadata.get(right);
  return leftParts === undefined || rightParts === undefined
    ? undefined
    : compareWireSafeBigDecimalScalars(
        leftParts.coefficient,
        leftParts.scale,
        rightParts.coefficient,
        rightParts.scale,
        leftParts.unsignedDigits,
        leftParts.magnitude,
        rightParts.unsignedDigits,
        rightParts.magnitude,
      );
};

/**
 * Comparator for values that already crossed a schema or query-ownership boundary. It avoids the
 * reflection and inspection-result allocations required at hostile-input boundaries.
 */
export const compareTrustedWireSafeBigDecimal = (
  left: BigDecimal,
  right: BigDecimal,
): number | undefined => {
  try {
    const leftValue = left.value;
    const leftScale = left.scale;
    const rightValue = right.value;
    const rightScale = right.scale;
    if (
      typeof leftValue !== "bigint" ||
      !Number.isSafeInteger(leftScale) ||
      typeof rightValue !== "bigint" ||
      !Number.isSafeInteger(rightScale)
    ) {
      return undefined;
    }
    return compareWireSafeBigDecimalParts(leftValue, leftScale, rightValue, rightScale);
  } catch {
    return undefined;
  }
};

export const compareWireSafeBigDecimal = (
  left: WireSafeBigDecimalInput,
  right: WireSafeBigDecimalInput,
): number | undefined => {
  const leftInspection = inspectWireSafeBigDecimal(left);
  if (leftInspection._tag !== "Success") {
    return undefined;
  }
  const rightInspection = Object.is(left, right)
    ? leftInspection
    : inspectWireSafeBigDecimal(right);
  return rightInspection._tag === "Success"
    ? compareWireSafeBigDecimalParts(
        leftInspection.coefficient,
        leftInspection.scale,
        rightInspection.coefficient,
        rightInspection.scale,
      )
    : undefined;
};
