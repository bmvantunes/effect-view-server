import type {
  TopicDefinitions,
  TopicRow,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import { Effect } from "effect";
import {
  viewServerDecodeGroupedQuery,
  viewServerEncodeGroupedQuery,
  type ViewServerValidatedGroupedQuery,
} from "./protocol-grouped-query-codec";
import { isGroupedQueryInput } from "./protocol-query-common";
import {
  viewServerDecodeRawQuery,
  viewServerEncodeRawQuery,
  type ViewServerValidatedRawQuery,
} from "./protocol-raw-query-codec";

export {
  viewServerDecodeGroupedQuery,
  viewServerEncodeGroupedQuery,
  type ViewServerValidatedGroupedQuery,
} from "./protocol-grouped-query-codec";
export { viewServerDecodeHealthQuery, viewServerDecodeTopic } from "./protocol-query-common";
export {
  viewServerDecodeRawQuery,
  viewServerEncodeRawQuery,
  type ViewServerValidatedRawQuery,
} from "./protocol-raw-query-codec";

export type ViewServerValidatedLiveQuery<Row extends object> =
  | ViewServerValidatedRawQuery<Row>
  | ViewServerValidatedGroupedQuery<Row>;

export const viewServerEncodeLiveQuery = Effect.fn("ViewServerProtocol.liveQuery.encode")(
  function* <const Topics extends TopicDefinitions, Topic extends Extract<keyof Topics, string>>(
    config: { readonly topics: Topics },
    topic: Topic,
    query: unknown,
  ) {
    if (isGroupedQueryInput(query)) {
      return yield* viewServerEncodeGroupedQuery(config, topic, query);
    }
    return yield* viewServerEncodeRawQuery(config, topic, query);
  },
);

const decodeLiveQuery = Effect.fn("ViewServerProtocol.liveQuery.decode")(function* (
  config: { readonly topics: TopicDefinitions },
  topic: string,
  query: unknown,
) {
  if (isGroupedQueryInput(query)) {
    return yield* viewServerDecodeGroupedQuery(config, topic, query);
  }
  return yield* viewServerDecodeRawQuery(config, topic, query);
});

export function viewServerDecodeLiveQuery<
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  config: { readonly topics: Topics },
  topic: Topic,
  query: unknown,
): Effect.Effect<ViewServerValidatedLiveQuery<TopicRow<Topics, Topic>>, ViewServerRuntimeError>;
export function viewServerDecodeLiveQuery(
  config: { readonly topics: TopicDefinitions },
  topic: string,
  query: unknown,
): Effect.Effect<unknown, ViewServerRuntimeError> {
  return decodeLiveQuery(config, topic, query);
}
