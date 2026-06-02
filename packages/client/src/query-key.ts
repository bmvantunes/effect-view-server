import { format, isBigDecimal, normalize } from "effect/BigDecimal";

type StableObjectEntry = readonly [string, StableQueryToken];
type StableMapEntry = readonly [StableQueryToken, StableQueryToken];

type StableQueryToken =
  | readonly ["null"]
  | readonly ["undefined"]
  | readonly ["boolean", boolean]
  | readonly ["number", string]
  | readonly ["string", string]
  | readonly ["bigint", string]
  | readonly ["bigDecimal", string]
  | readonly ["unsupported", string]
  | readonly ["cycle"]
  | readonly ["array", ReadonlyArray<StableQueryToken>]
  | readonly ["object", ReadonlyArray<StableObjectEntry>]
  | readonly ["map", ReadonlyArray<StableMapEntry>]
  | readonly ["set", ReadonlyArray<StableQueryToken>];

const isPlainObject = (value: object): boolean => {
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
};

const stableNumberValue = (value: number): string => {
  if (Object.is(value, -0)) {
    return "-0";
  }
  return String(value);
};

const stableObjectName = (value: object): string => {
  const constructor = value.constructor;
  return typeof constructor === "function" && constructor.name !== "" ? constructor.name : "Object";
};

const stableTokenSortKey = (value: StableQueryToken): string => JSON.stringify(value);

const withCycleTracking = <T extends object>(
  value: T,
  active: WeakSet<object>,
  visit: () => StableQueryToken,
): StableQueryToken => {
  if (active.has(value)) {
    return ["cycle"];
  }
  active.add(value);
  try {
    return visit();
  } finally {
    active.delete(value);
  }
};

const stableQueryValue = (value: unknown, active: WeakSet<object>): StableQueryToken => {
  if (value === null) {
    return ["null"];
  }
  if (value === undefined) {
    return ["undefined"];
  }
  if (typeof value === "boolean") {
    return ["boolean", value];
  }
  if (typeof value === "number") {
    return ["number", stableNumberValue(value)];
  }
  if (typeof value === "string") {
    return ["string", value];
  }
  if (typeof value === "bigint") {
    return ["bigint", value.toString()];
  }
  if (isBigDecimal(value)) {
    return ["bigDecimal", format(normalize(value))];
  }
  if (typeof value === "symbol") {
    return ["unsupported", "symbol"];
  }
  if (typeof value === "function") {
    return ["unsupported", "function"];
  }
  if (Array.isArray(value)) {
    return withCycleTracking(value, active, () => [
      "array",
      value.map((entry) => stableQueryValue(entry, active)),
    ]);
  }
  if (value instanceof Map) {
    return withCycleTracking(value, active, () => {
      const entries: Array<StableMapEntry> = [];
      for (const [key, entry] of value.entries()) {
        entries.push([stableQueryValue(key, active), stableQueryValue(entry, active)]);
      }
      return [
        "map",
        entries.sort((left, right) =>
          stableTokenSortKey(left[0]).localeCompare(stableTokenSortKey(right[0])),
        ),
      ];
    });
  }
  if (value instanceof Set) {
    return withCycleTracking(value, active, () => {
      const entries: Array<StableQueryToken> = [];
      for (const entry of value.values()) {
        entries.push(stableQueryValue(entry, active));
      }
      return [
        "set",
        entries.sort((left, right) =>
          stableTokenSortKey(left).localeCompare(stableTokenSortKey(right)),
        ),
      ];
    });
  }
  if (!isPlainObject(value)) {
    return ["unsupported", stableObjectName(value)];
  }
  return withCycleTracking(value, active, () => [
    "object",
    Object.entries(value)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, entry]) => [key, stableQueryValue(entry, active)]),
  ]);
};

export const stableQueryKey = (query: object): string =>
  JSON.stringify(stableQueryValue(query, new WeakSet<object>()));
