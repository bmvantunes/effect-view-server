import { isBigDecimal, type BigDecimal } from "effect/BigDecimal";

export type WireSafeBigDecimalInspection =
  | { readonly _tag: "NotBigDecimal" }
  | { readonly _tag: "UnsafeBigDecimal" }
  | { readonly _tag: "ReflectionFailure" }
  | {
      readonly _tag: "Success";
      readonly source: BigDecimal;
      readonly coefficient: bigint;
      readonly scale: number;
      readonly semanticKey: string;
    };

const notBigDecimal: WireSafeBigDecimalInspection = { _tag: "NotBigDecimal" };
const unsafeBigDecimal: WireSafeBigDecimalInspection = { _tag: "UnsafeBigDecimal" };
const reflectionFailure: WireSafeBigDecimalInspection = { _tag: "ReflectionFailure" };
const bigDecimalTypeId = "~effect/BigDecimal";

const hasBigDecimalPrototype = (value: unknown): value is BigDecimal =>
  typeof value === "object" && value !== null && hasBigDecimalPrototypeBrand(value);

const hasBigDecimalPrototypeBrand = (value: object): boolean => {
  const prototype = Object.getPrototypeOf(value);
  if (typeof prototype !== "object" || prototype === null) {
    return false;
  }
  const descriptor = Object.getOwnPropertyDescriptor(prototype, bigDecimalTypeId);
  return descriptor !== undefined && "value" in descriptor && descriptor.value === bigDecimalTypeId;
};

const ownEnumerableDataValue = (value: object, key: string): unknown => {
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

  if (Math.abs(canonicalScale) >= 16) {
    const coefficientWidth =
      normalizedCoefficient < 0n
        ? normalizedCoefficient.toString().length - 1
        : normalizedCoefficient.toString().length;
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

export const inspectWireSafeBigDecimal = (value: unknown): WireSafeBigDecimalInspection => {
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

export const isWireSafeBigDecimal = (value: unknown): value is BigDecimal => {
  return inspectWireSafeBigDecimal(value)._tag === "Success";
};

export const wireSafeBigDecimalSemanticKey = (value: unknown): string | undefined => {
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
  if (leftValue === rightValue && Object.is(leftScale, rightScale)) {
    return 0;
  }
  if (leftValue === 0n) {
    return rightValue === 0n ? 0 : rightValue < 0n ? 1 : -1;
  }
  if (rightValue === 0n) {
    return leftValue < 0n ? -1 : 1;
  }
  const leftNegative = leftValue < 0n;
  const rightNegative = rightValue < 0n;
  if (leftNegative !== rightNegative) {
    return leftNegative ? -1 : 1;
  }

  const leftDigits = (leftNegative ? -leftValue : leftValue).toString();
  const rightDigits = (rightNegative ? -rightValue : rightValue).toString();
  const leftMagnitude = BigInt(leftDigits.length) - BigInt(leftScale);
  const rightMagnitude = BigInt(rightDigits.length) - BigInt(rightScale);
  const unsignedComparison =
    leftMagnitude === rightMagnitude
      ? compareUnsignedDecimalDigits(leftDigits, rightDigits)
      : leftMagnitude < rightMagnitude
        ? -1
        : 1;
  return leftNegative ? -unsignedComparison : unsignedComparison;
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

export const compareWireSafeBigDecimal = (left: unknown, right: unknown): number | undefined => {
  const leftInspection = inspectWireSafeBigDecimal(left);
  if (leftInspection._tag !== "Success") {
    return undefined;
  }
  const rightInspection = left === right ? leftInspection : inspectWireSafeBigDecimal(right);
  return rightInspection._tag === "Success"
    ? compareWireSafeBigDecimalParts(
        leftInspection.coefficient,
        leftInspection.scale,
        rightInspection.coefficient,
        rightInspection.scale,
      )
    : undefined;
};
