import {
  defineViewServerConfig,
  type ExactLiveQueryInputForTopic,
  type GroupedQuery,
  type LiveQueryResult,
  type LiveQueryRow,
  type RawQuery,
  type TopicRow,
} from "../src/index";
import { Order, Position, Trade } from "./schemas";

export const viewServer = defineViewServerConfig({
  topics: {
    orders: { schema: Order, key: "id" },
    trades: { schema: Trade, key: "id" },
    positions: { schema: Position, key: "id" },
  },
});

export type LiveQueryCall<Topics extends object> = {
  <
    Topic extends Extract<keyof Topics, string>,
    const Query extends GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  ): LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>;
  <
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  ): LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>;
};
