type RuntimeQueryRecord = Readonly<Record<string, unknown>>;

declare const ValidatedRuntimeQueryTypeId: unique symbol;

export type ValidatedRuntimeQuery = RuntimeQueryRecord & {
  readonly [ValidatedRuntimeQueryTypeId]: true;
};

export function trustDecodedRuntimeQuery<Query extends RuntimeQueryRecord>(
  query: Query,
): Query & ValidatedRuntimeQuery;
export function trustDecodedRuntimeQuery(query: RuntimeQueryRecord): RuntimeQueryRecord {
  return query;
}
