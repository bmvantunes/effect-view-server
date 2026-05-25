const objectIdentities = new WeakMap<object, number>();
let nextObjectIdentity = 0;

const stableObjectIdentity = (value: object): number => {
  const identity = objectIdentities.get(value);
  if (identity !== undefined) {
    return identity;
  }
  nextObjectIdentity += 1;
  objectIdentities.set(value, nextObjectIdentity);
  return nextObjectIdentity;
};

const isPlainObject = (value: object): boolean => {
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
};

const stableQueryValue = (value: unknown): unknown => {
  if (typeof value === "bigint") {
    return ["bigint", value.toString()];
  }
  if (Array.isArray(value)) {
    return ["array", value.map(stableQueryValue)];
  }
  if (typeof value === "object" && value !== null) {
    if (value instanceof Map) {
      return [
        "map",
        Array.from(value.entries())
          .map(([key, entry]) => [stableQueryValue(key), stableQueryValue(entry)])
          .sort(([left], [right]) => JSON.stringify(left).localeCompare(JSON.stringify(right))),
      ];
    }
    if (value instanceof Set) {
      return [
        "set",
        Array.from(value.values())
          .map(stableQueryValue)
          .sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right))),
      ];
    }
    if (!isPlainObject(value)) {
      return ["nonPlainObject", value.constructor.name, stableObjectIdentity(value)];
    }
    return [
      "object",
      Object.entries(value)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, entry]) => [key, stableQueryValue(entry)]),
    ];
  }
  return ["primitive", value];
};

export const stableQueryKey = (query: object): string => JSON.stringify(stableQueryValue(query));
