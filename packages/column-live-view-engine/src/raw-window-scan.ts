import type { TopicRowEntry } from "./row-scan";

type RowObject = object;

export type TopicRawOrderByPlan = {
  readonly field: string;
  readonly direction: "asc" | "desc";
};

export type TopicRawPredicateFilterPlan =
  | {
      readonly field: string;
      readonly operator: "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "startsWith";
      readonly value: unknown;
    }
  | {
      readonly field: string;
      readonly operator: "in";
      readonly values: ReadonlyArray<unknown>;
      readonly valueKeys?: ReadonlySet<string>;
    };

export type TopicRawPredicatePlan = {
  /**
   * Safe scalar hints that storage can use to narrow a raw scan.
   * `matches` remains the correctness guard unless an adapter implements a
   * proven equivalent for every emitted hint.
   */
  readonly filters: ReadonlyArray<TopicRawPredicateFilterPlan>;
  /**
   * True when the compiler intentionally omitted part of the predicate from
   * `filters`, for example structured fields or malformed runtime filters.
   */
  readonly callbackRequired: boolean;
  /**
   * True when the compiler proved that `filters` fully represent `matches`.
   * Hand-written plans omit this and stay guarded by the row callback.
   */
  readonly callbackSkippable?: boolean;
};

export type TopicRawWindowScanPlan<Row extends RowObject> = {
  readonly predicate: TopicRawPredicatePlan;
  readonly orderBy: ReadonlyArray<TopicRawOrderByPlan>;
  /**
   * Compiler-proven ordering hint for storage pushdown. `compare` remains the
   * source of truth for custom scan plans unless this hint is present.
   */
  readonly storageOrderBy?: ReadonlyArray<TopicRawOrderByPlan>;
  readonly matches: (row: Row) => boolean;
  readonly compare: (left: TopicRowEntry<Row>, right: TopicRowEntry<Row>) => number;
  readonly offset: number;
  readonly limit: number | undefined;
};

export type TopicRawWindowScanResult<Row extends RowObject> = {
  readonly keys: ReadonlyArray<string>;
  readonly window: ReadonlyArray<TopicRowEntry<Row>>;
  readonly totalRows: number;
};

export type TopicRawWindowScan<Row extends RowObject> = {
  readonly scanRawWindow: (plan: TopicRawWindowScanPlan<Row>) => TopicRawWindowScanResult<Row>;
};
