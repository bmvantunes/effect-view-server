import type { DeltaEvent, SnapshotEvent } from "@effect-view-server/config";
import type { Effect, Option } from "effect";
import type { QueryEvaluation } from "./query-result";
import type { RawQueryPlanWindow } from "./raw-query-plan";
import type { TopicRowEntry } from "./row-scan";

type RowObject = object;

export type LiveQueryExecutionCursor = {
  evaluation: QueryEvaluation<RowObject>;
};

type RawQueryExecutionUpdate<ResultRow extends RowObject> = Effect.Effect<
  Option.Option<DeltaEvent<ResultRow>>,
  never,
  never
>;

export type LiveQueryExecution<ResultRow extends RowObject> = {
  readonly initial: (queryId: string) => SnapshotEvent<ResultRow>;
  readonly createCursor: () => LiveQueryExecutionCursor;
  readonly next: (
    queryId: string,
    cursor: LiveQueryExecutionCursor,
  ) => RawQueryExecutionUpdate<ResultRow>;
};

export type RawQueryExecution<ResultRow extends RowObject> = LiveQueryExecution<ResultRow>;

export type ActiveQueryBaseEvaluation<Row extends RowObject> = {
  readonly keyIndex: ReadonlyMap<string, number>;
  readonly keys: ReadonlyArray<string>;
  readonly retainedWindowFilled: boolean;
  readonly totalRows: number;
  readonly version: number;
  readonly window: ReadonlyArray<RetainedWindowEntry<Row>>;
};

export type RetainedWindowEntry<Row extends RowObject = RowObject> = TopicRowEntry<Row> & {
  readonly key: string;
  readonly row: Row;
  readonly slot?: number;
};

export type ActiveQueryBaseExecution = {
  readonly latest: () => ActiveQueryBaseEvaluation<RowObject>;
};

export type RawQueryExecutionWindowSlot = {
  readonly window: RawQueryPlanWindow;
  refs: number;
};

export type RawQueryExecutionSlot = {
  readonly execution: ActiveQueryBaseExecution;
  readonly releaseRetainedChanges: () => void;
  readonly windows: Map<string, RawQueryExecutionWindowSlot>;
  refs: number;
};

export type MaterializedQueryExecution = {
  readonly diagnostics: () => {
    readonly fullEvaluationCount: number;
    readonly patchedEvaluationCount: number;
  };
  readonly incremental: boolean;
  readonly latest: () => QueryEvaluation<RowObject>;
};

export type MaterializedQueryExecutionSlot = {
  readonly execution: MaterializedQueryExecution;
  readonly releaseRetainedChanges: () => void;
  refs: number;
};

export type ActiveQueryRegistry = {
  readonly raw: Map<string, RawQueryExecutionSlot>;
  readonly materialized: Map<string, MaterializedQueryExecutionSlot>;
};

export type ActiveQueryExecutionCounts = {
  readonly activeFallbackGroupedViews: number;
  readonly activeIncrementalGroupedViews: number;
  readonly activeViews: number;
  readonly groupedFullEvaluationCount: number;
  readonly groupedPatchedEvaluationCount: number;
};

export const createActiveQueryRegistry = (): ActiveQueryRegistry => ({
  raw: new Map(),
  materialized: new Map(),
});
