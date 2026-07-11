import { format as formatBigDecimal, isBigDecimal } from "effect/BigDecimal";

export const rowField = (row: object, field: string): unknown => {
  for (const [key, value] of Object.entries(row)) {
    if (key === field) {
      return value;
    }
  }
  return undefined;
};

export const rowIds = (rows: ReadonlyArray<object>): ReadonlyArray<unknown> =>
  rows.map((row) => rowField(row, "id"));

export const normalizeDecimalFields = <Row extends object>(
  rows: ReadonlyArray<Row>,
): ReadonlyArray<Record<string, unknown>> =>
  rows.map((row) =>
    Object.fromEntries(
      Object.entries(row).map(([key, value]) => [
        key,
        isBigDecimal(value) ? formatBigDecimal(value) : value,
      ]),
    ),
  );

export const normalizeDecimalAndBigIntFields = <Row extends object>(
  rows: ReadonlyArray<Row>,
): ReadonlyArray<Record<string, unknown>> =>
  rows.map((row) =>
    Object.fromEntries(
      Object.entries(row).map(([key, value]) => [
        key,
        isBigDecimal(value)
          ? formatBigDecimal(value)
          : typeof value === "bigint"
            ? value.toString()
            : value,
      ]),
    ),
  );
