import { isBigDecimal, type BigDecimal } from "effect/BigDecimal";

export const isWireSafeBigDecimal = (value: unknown): value is BigDecimal =>
  isBigDecimal(value) && typeof value.value === "bigint" && Number.isSafeInteger(value.scale);

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
export const compareWireSafeBigDecimal = (left: unknown, right: unknown): number | undefined => {
  if (!isWireSafeBigDecimal(left) || !isWireSafeBigDecimal(right)) {
    return undefined;
  }
  if (left.value === right.value && Object.is(left.scale, right.scale)) {
    return 0;
  }
  if (left.value === 0n) {
    return right.value === 0n ? 0 : right.value < 0n ? 1 : -1;
  }
  if (right.value === 0n) {
    return left.value < 0n ? -1 : 1;
  }
  const leftNegative = left.value < 0n;
  const rightNegative = right.value < 0n;
  if (leftNegative !== rightNegative) {
    return leftNegative ? -1 : 1;
  }

  const leftDigits = (leftNegative ? -left.value : left.value).toString();
  const rightDigits = (rightNegative ? -right.value : right.value).toString();
  const leftMagnitude = BigInt(leftDigits.length) - BigInt(left.scale);
  const rightMagnitude = BigInt(rightDigits.length) - BigInt(right.scale);
  const unsignedComparison =
    leftMagnitude === rightMagnitude
      ? compareUnsignedDecimalDigits(leftDigits, rightDigits)
      : leftMagnitude < rightMagnitude
        ? -1
        : 1;
  return leftNegative ? -unsignedComparison : unsignedComparison;
};
