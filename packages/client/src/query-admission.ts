import type { TopicDefinitions } from "@effect-view-server/config";
import { viewServerDecodeLiveQuery } from "@effect-view-server/protocol";
import { Effect, Result } from "effect";

export const admitViewServerLiveQuery = <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  config: { readonly topics: Topics },
  topic: Topic,
  query: object,
): object => {
  const admitted = Effect.runSync(
    viewServerDecodeLiveQuery(config, topic, query).pipe(Effect.result),
  );
  if (Result.isFailure(admitted)) {
    throw new TypeError(admitted.failure.message);
  }
  return admitted.success;
};
