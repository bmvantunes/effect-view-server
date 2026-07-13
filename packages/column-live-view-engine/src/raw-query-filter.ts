import {
  isRawQueryFilterOperatorKey,
  rawQueryFilterOperatorKeys,
} from "@effect-view-server/config/internal";
import { isBigDecimal } from "effect/BigDecimal";
import { isPlainRecord } from "./row-values";

export const filterOperatorKeys = rawQueryFilterOperatorKeys;

export const isDenseArray = (value: ReadonlyArray<unknown>): boolean => {
  for (let index = 0; index < value.length; index += 1) {
    if (!(index in value)) {
      return false;
    }
  }
  return true;
};

export const isOperatorFilterObject = (
  filter: unknown,
): filter is Readonly<Record<string, unknown>> => {
  if (!isPlainRecord(filter) || isBigDecimal(filter)) {
    return false;
  }
  const keys = Object.keys(filter);
  return keys.length > 0 && keys.every(isRawQueryFilterOperatorKey);
};
