import { ownViewServerQuerySnapshot } from "@effect-view-server/effect-utils";

export type EngineQueryWithoutRoute<Query> = Query extends unknown ? Omit<Query, "routeBy"> : never;

export function engineQueryWithoutRoute<Query extends Readonly<Record<string, unknown>>>(
  query: Query,
): EngineQueryWithoutRoute<Query>;
export function engineQueryWithoutRoute<Query extends Readonly<Record<string, unknown>>>(
  query: Query,
) {
  if (!Object.hasOwn(query, "routeBy")) {
    return query;
  }
  const { routeBy: _routeBy, ...engineQuery } = query;
  return ownViewServerQuerySnapshot(Object.freeze(engineQuery));
}
