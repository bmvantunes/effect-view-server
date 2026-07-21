import {
  isWireSafeBigDecimal,
  makeSchemaJsonIdentity,
  wireSafeBigDecimalSemanticKey,
} from "@effect-view-server/effect-utils";
import type { RowSchema } from "@effect-view-server/config";
import { viewServerFilterFieldContracts } from "@effect-view-server/config/internal";
import { Result } from "effect";
import { isBigDecimal, type BigDecimal } from "effect/BigDecimal";
import {
  denseArrayValues,
  hasPlainRecordPrototype,
  plainRecordSnapshot,
} from "./query-structural-data";
import {
  canonicalWhereKey,
  type CanonicalWhereFieldContract,
  type CanonicalWhereFieldContracts,
} from "./query-where-key";

type StableObjectEntry = readonly [string, StableQueryToken];
type StableMapEntry = readonly [StableQueryToken, StableQueryToken];

type StableBigDecimalToken =
  | readonly ["bigDecimal", string]
  | readonly ["bigDecimalExact", string, string];

type StableQueryToken =
  | readonly ["null"]
  | readonly ["undefined"]
  | readonly ["boolean", boolean]
  | readonly ["number", string]
  | readonly ["string", string]
  | readonly ["bigint", string]
  | StableBigDecimalToken
  | readonly ["unsupported", string]
  | readonly ["cycle"]
  | readonly ["array", ReadonlyArray<StableQueryToken>]
  | readonly ["object", ReadonlyArray<StableObjectEntry>]
  | readonly ["map", ReadonlyArray<StableMapEntry>]
  | readonly ["set", ReadonlyArray<StableQueryToken>];

const failStableObject = (): never => {
  throw new TypeError("Stable query objects must be plain data objects.");
};

const failStableObjectProperty = (): never => {
  throw new TypeError("Stable query object fields must be own enumerable data properties.");
};

const failStableArray = (): never => {
  throw new TypeError("Stable query arrays must be plain data arrays.");
};

const failStableArrayEntry = (): never => {
  throw new TypeError("Stable query arrays must be dense data arrays.");
};

const failStableArrayExtraProperty = (): never => {
  throw new TypeError("Stable query arrays must not contain extra properties.");
};

const stableArrayValues = (value: unknown): ReadonlyArray<unknown> =>
  denseArrayValues(value, failStableArray, failStableArrayEntry, failStableArrayExtraProperty);

const stableObjectEntries = (value: unknown): ReadonlyArray<readonly [string, unknown]> =>
  plainRecordSnapshot(value, failStableObject, failStableObjectProperty).entries;

const stableNumberValue = (value: number): string => {
  if (Object.is(value, -0)) {
    return "-0";
  }
  return String(value);
};

const stableObjectName = (_value: object): string => "object";

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

type BigDecimalIdentity = "semantic" | "exact";

const stableBigDecimalToken = (
  value: BigDecimal,
  identity: BigDecimalIdentity,
): StableBigDecimalToken | readonly ["unsupported", "bigDecimal"] => {
  if (identity === "exact") {
    return isWireSafeBigDecimal(value)
      ? ["bigDecimalExact", value.value.toString(), stableNumberValue(value.scale)]
      : ["unsupported", "bigDecimal"];
  }
  const semanticKey = wireSafeBigDecimalSemanticKey(value);
  return semanticKey === undefined ? ["unsupported", "bigDecimal"] : ["bigDecimal", semanticKey];
};

const stableQueryValue = (
  value: unknown,
  active: WeakSet<object>,
  bigDecimalIdentity: BigDecimalIdentity,
): StableQueryToken => {
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
    return stableBigDecimalToken(value, bigDecimalIdentity);
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
      stableArrayValues(value).map((entry) => stableQueryValue(entry, active, bigDecimalIdentity)),
    ]);
  }
  if (value instanceof Map) {
    return withCycleTracking(value, active, () => {
      const entries: Array<StableMapEntry> = [];
      for (const [key, entry] of value.entries()) {
        entries.push([
          stableQueryValue(key, active, bigDecimalIdentity),
          stableQueryValue(entry, active, bigDecimalIdentity),
        ]);
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
        entries.push(stableQueryValue(entry, active, bigDecimalIdentity));
      }
      return [
        "set",
        entries.sort((left, right) =>
          stableTokenSortKey(left).localeCompare(stableTokenSortKey(right)),
        ),
      ];
    });
  }
  if (!hasPlainRecordPrototype(value)) {
    return ["unsupported", stableObjectName(value)];
  }
  return withCycleTracking(value, active, () => [
    "object",
    stableObjectEntries(value)
      .toSorted(([left], [right]) => left.localeCompare(right))
      .map(([key, entry]) => [key, stableQueryValue(entry, active, bigDecimalIdentity)]),
  ]);
};

type StableGraphValue =
  | readonly ["null"]
  | readonly ["undefined"]
  | readonly ["boolean", boolean]
  | readonly ["number", string]
  | readonly ["string", string]
  | readonly ["bigint", string]
  | StableBigDecimalToken
  | readonly ["unsupported", string]
  | readonly ["cycle"]
  | readonly ["reference", number]
  | readonly ["embedded", StableQueryToken];

type StableGraphSlot = {
  value: StableGraphValue | undefined;
};

type StableGraphNode =
  | {
      readonly type: "array";
      readonly values: ReadonlyArray<StableGraphSlot>;
    }
  | {
      readonly type: "object";
      readonly entries: ReadonlyArray<{
        readonly key: string;
        readonly slot: StableGraphSlot;
      }>;
    };

type StableGraphObjectState =
  | { readonly status: "active" }
  | { readonly status: "complete"; readonly nodeId: number };

type StableGraphWork =
  | {
      readonly type: "visit";
      readonly input: unknown;
      readonly slot: StableGraphSlot;
      readonly bigDecimalIdentity: BigDecimalIdentity;
      readonly isRoot: boolean;
    }
  | {
      readonly type: "complete";
      readonly input: object;
      readonly node: StableGraphNode;
      readonly slot: StableGraphSlot;
      readonly bigDecimalIdentity: BigDecimalIdentity;
    };

type StableGraphWorkStack = {
  readonly current: StableGraphWork;
  readonly next: StableGraphWorkStack | undefined;
};

const stableGraphQueryValue = (
  query: object,
): readonly ["graph", StableGraphSlot, ReadonlyArray<StableGraphNode>] => {
  const root: StableGraphSlot = { value: undefined };
  const nodes: Array<StableGraphNode> = [];
  const semanticNodesByValue = new WeakMap<object, StableGraphObjectState>();
  const exactNodesByValue = new WeakMap<object, StableGraphObjectState>();
  const canonicalNodeIds = new Map<string, number>();
  const semanticActive = new WeakSet<object>();
  const exactActive = new WeakSet<object>();
  let work: StableGraphWorkStack | undefined = {
    current: {
      type: "visit",
      input: query,
      slot: root,
      bigDecimalIdentity: "semantic",
      isRoot: true,
    },
    next: undefined,
  };
  const push = (current: StableGraphWork): void => {
    work = { current, next: work };
  };

  while (work !== undefined) {
    const current = work.current;
    work = work.next;

    if (current.type === "complete") {
      const active = current.bigDecimalIdentity === "exact" ? exactActive : semanticActive;
      active.delete(current.input);
      const nodeKey = JSON.stringify(current.node);
      const existingNodeId = canonicalNodeIds.get(nodeKey);
      const nodeId = existingNodeId ?? nodes.length;
      if (existingNodeId === undefined) {
        canonicalNodeIds.set(nodeKey, nodeId);
        nodes.push(current.node);
      }
      const nodesByValue =
        current.bigDecimalIdentity === "exact" ? exactNodesByValue : semanticNodesByValue;
      nodesByValue.set(current.input, { status: "complete", nodeId });
      current.slot.value = ["reference", nodeId];
      continue;
    }

    const value = current.input;
    if (value === null) {
      current.slot.value = ["null"];
      continue;
    }
    if (value === undefined) {
      current.slot.value = ["undefined"];
      continue;
    }
    if (typeof value === "boolean") {
      current.slot.value = ["boolean", value];
      continue;
    }
    if (typeof value === "number") {
      current.slot.value = ["number", stableNumberValue(value)];
      continue;
    }
    if (typeof value === "string") {
      current.slot.value = ["string", value];
      continue;
    }
    if (typeof value === "bigint") {
      current.slot.value = ["bigint", value.toString()];
      continue;
    }
    if (isBigDecimal(value)) {
      current.slot.value = stableBigDecimalToken(value, current.bigDecimalIdentity);
      continue;
    }
    if (typeof value === "symbol") {
      current.slot.value = ["unsupported", "symbol"];
      continue;
    }
    if (typeof value === "function") {
      current.slot.value = ["unsupported", "function"];
      continue;
    }
    if (value instanceof Map || value instanceof Set) {
      const active = current.bigDecimalIdentity === "exact" ? exactActive : semanticActive;
      current.slot.value = [
        "embedded",
        stableQueryValue(value, active, current.bigDecimalIdentity),
      ];
      continue;
    }
    if (!Array.isArray(value) && !hasPlainRecordPrototype(value)) {
      current.slot.value = ["unsupported", stableObjectName(value)];
      continue;
    }

    const nodesByValue =
      current.bigDecimalIdentity === "exact" ? exactNodesByValue : semanticNodesByValue;
    const active = current.bigDecimalIdentity === "exact" ? exactActive : semanticActive;
    const state = nodesByValue.get(value);
    if (state?.status === "active") {
      current.slot.value = ["cycle"];
      continue;
    }
    if (state?.status === "complete") {
      current.slot.value = ["reference", state.nodeId];
      continue;
    }

    nodesByValue.set(value, { status: "active" });
    active.add(value);
    if (Array.isArray(value)) {
      const children = stableArrayValues(value).map((input) => ({
        input,
        slot: { value: undefined } satisfies StableGraphSlot,
      }));
      const node: StableGraphNode = {
        type: "array",
        values: children.map(({ slot }) => slot),
      };
      push({
        type: "complete",
        input: value,
        node,
        slot: current.slot,
        bigDecimalIdentity: current.bigDecimalIdentity,
      });
      children.reverse();
      for (const child of children) {
        push({
          type: "visit",
          input: child.input,
          slot: child.slot,
          bigDecimalIdentity: current.bigDecimalIdentity,
          isRoot: false,
        });
      }
      continue;
    }

    const entries = stableObjectEntries(value)
      .toSorted(([left], [right]) => left.localeCompare(right))
      .map(([key, entry]) => ({ key, entry, slot: { value: undefined } }));
    const node: StableGraphNode = {
      type: "object",
      entries: entries.map(({ key, slot }) => ({ key, slot })),
    };
    push({
      type: "complete",
      input: value,
      node,
      slot: current.slot,
      bigDecimalIdentity: current.bigDecimalIdentity,
    });
    entries.reverse();
    for (const entry of entries) {
      push({
        type: "visit",
        input: entry.entry,
        slot: entry.slot,
        bigDecimalIdentity:
          current.isRoot && entry.key === "routeBy" ? "exact" : current.bigDecimalIdentity,
        isRoot: false,
      });
    }
  }

  return ["graph", root, nodes];
};

const canonicalQueryInput = (
  query: object,
  fieldContracts?: CanonicalWhereFieldContracts,
): object => {
  const canonical: Record<string, unknown> = {};
  for (const [key, value] of stableObjectEntries(query)) {
    if (key === "where") {
      const whereKey = canonicalWhereKey(value, fieldContracts);
      if (whereKey === undefined) {
        continue;
      }
      Object.defineProperty(canonical, key, {
        configurable: true,
        enumerable: true,
        value: Object.freeze({ canonicalWhere: whereKey }),
        writable: true,
      });
      continue;
    }
    Object.defineProperty(canonical, key, {
      configurable: true,
      enumerable: true,
      value,
      writable: true,
    });
  }
  return canonical;
};

const invalidQueryKey = JSON.stringify(["invalidQuery"]);

const canonicalWhereFieldContractsCache = new WeakMap<object, CanonicalWhereFieldContracts>();

const canonicalWhereFieldContracts = (rowSchema: RowSchema): CanonicalWhereFieldContracts => {
  const cached = canonicalWhereFieldContractsCache.get(rowSchema);
  if (cached !== undefined) {
    return cached;
  }
  const contracts = new Map<string, CanonicalWhereFieldContract>();
  for (const [field, contract] of viewServerFilterFieldContracts(rowSchema)) {
    contracts.set(
      field,
      Object.freeze({
        materialize: makeSchemaJsonIdentity(contract.typeSchema).materializeDecoded,
        supportsText: contract.supportsText,
      }),
    );
  }
  canonicalWhereFieldContractsCache.set(rowSchema, contracts);
  return contracts;
};

const stableQueryKeyWithFields = (
  query: object,
  fieldContracts?: CanonicalWhereFieldContracts,
): string => {
  const key = Result.try(() =>
    JSON.stringify(stableGraphQueryValue(canonicalQueryInput(query, fieldContracts))),
  );
  return Result.isFailure(key) ? invalidQueryKey : key.success;
};

export const stableQueryKey = (query: object): string => stableQueryKeyWithFields(query);

export const stableQueryKeyForRowSchema = (query: object, rowSchema: RowSchema): string =>
  stableQueryKeyWithFields(query, canonicalWhereFieldContracts(rowSchema));
