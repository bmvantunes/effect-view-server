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
    orders: { schema: Order },
    trades: { schema: Trade },
    positions: { schema: Position },
  },
});

export type LiveQueryCall<Topics extends object> = <
  Topic extends Extract<keyof Topics, string>,
  const Query extends
    | RawQuery<TopicRow<Topics, NoInfer<Topic>>>
    | GroupedQuery<TopicRow<Topics, NoInfer<Topic>>>,
>(
  topic: Topic,
  query: ExactLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
) => LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>;
