import type { TopicDefinitions } from "@effect-view-server/config";
import { viewServerDecodeLiveQuery, viewServerEncodeLiveQuery } from "@effect-view-server/protocol";
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
    Effect.gen(function* () {
      const encoded = yield* viewServerEncodeLiveQuery(config, topic, query);
      return yield* viewServerDecodeLiveQuery(config, topic, encoded);
    }).pipe(Effect.result),
  );
  if (Result.isFailure(admitted)) {
    throw new TypeError(admitted.failure.message);
  }
  return admitted.success;
};
