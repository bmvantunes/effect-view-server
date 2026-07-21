import {
  type GroupState,
  newGroupState,
  updateGroupAggregateState,
} from "./grouped-aggregate-state";
import type { GroupedQueryPlan } from "./grouped-query-plan";
import { emptyGroupedEvaluation, groupedEvaluationFromGroups } from "./grouped-window-evaluation";
import type { QueryEvaluation } from "./query-result";
import { scanTopicRows, type TopicRowScan } from "./row-scan";

type RowObject = object;

const evaluateZeroLimitGroupedRows = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRowScan<Row>,
  plan: GroupedQueryPlan<Row, ResultRow>,
  matches: (row: Row, storageKey?: string) => boolean,
  ownedStorageKeys?: () => Iterable<string>,
): QueryEvaluation<RowObject> => {
  const groupKeys = new Set<string>();
  scanTopicRows(store, ownedStorageKeys, (key, row) => {
    if (matches(row, key)) {
      groupKeys.add(plan.groupKey(row));
    }
  });
  return emptyGroupedEvaluation(groupKeys.size, store.version());
};

export const evaluateGroupedRows = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRowScan<Row>,
  plan: GroupedQueryPlan<Row, ResultRow>,
  matches: (row: Row, storageKey?: string) => boolean,
  ownedStorageKeys?: () => Iterable<string>,
): QueryEvaluation<RowObject> => {
  if (plan.zeroLimit) {
    return evaluateZeroLimitGroupedRows(store, plan, matches, ownedStorageKeys);
  }
  const groups = new Map<string, GroupState>();
  scanTopicRows(store, ownedStorageKeys, (storageKey, row) => {
    if (!matches(row, storageKey)) {
      return;
    }
    const key = plan.groupKey(row);
    let group = groups.get(key);
    if (group === undefined) {
      group = newGroupState(key, plan.groupBy, plan.aggregatePlans, row);
      groups.set(key, group);
    }
    for (const aggregateState of group.aggregates) {
      updateGroupAggregateState(aggregateState, row);
    }
  });
  return groupedEvaluationFromGroups(groups.values(), plan, store.version());
};
