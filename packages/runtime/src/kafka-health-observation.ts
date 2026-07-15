import type { GroupAssignment, Offsets } from "@platformatic/kafka";
import { Duration, Effect, Exit, MutableRef, Scope } from "effect";
import type { ViewServerKafkaHealthLedger } from "./kafka-health";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export type ViewServerKafkaHealthObservation<Topics extends ViewServerRuntimeTopicDefinitions> =
  Pick<
    ViewServerKafkaHealthLedger<Topics>,
    | "regionConnected"
    | "regionDisconnected"
    | "regionDegraded"
    | "regionRecovered"
    | "regionStopped"
    | "topicConnected"
    | "topicLagSampled"
    | "messageDecoded"
    | "messageSkippedCommitted"
    | "decodeFailed"
    | "mappingFailed"
    | "messagePublishFailed"
    | "messageCommitFailed"
  >;

export type ViewServerKafkaHealthObserver<Topics extends ViewServerRuntimeTopicDefinitions> =
  ViewServerKafkaHealthObservation<Topics> & {
    readonly close: Effect.Effect<void>;
  };

export const assignedPartitionsForSourceTopic = (
  assignments: ReadonlyArray<GroupAssignment> | null | undefined,
  sourceTopic: string,
): number => {
  const assignment = assignments?.find((candidate) => candidate.topic === sourceTopic);
  return assignment?.partitions.length ?? 0;
};

const consumerLagMessagesFromLag = (lags: ReadonlyArray<bigint>): bigint | null => {
  let total = 0n;
  let hasKnownLag = false;
  for (const lag of lags) {
    if (lag >= 0n) {
      hasKnownLag = true;
      total += lag;
    }
  }
  return hasKnownLag ? total : null;
};

export const recordKafkaAssignments = Effect.fn(
  "ViewServerRuntime.kafka.observation.recordAssignments",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  observation: ViewServerKafkaHealthObservation<Topics>,
  region: string,
  topics: ReadonlyArray<string>,
  assignments: ReadonlyArray<GroupAssignment> | null | undefined,
  nowMillis: number,
) {
  yield* observation.regionConnected(region, nowMillis);
  yield* Effect.forEach(
    topics,
    (sourceTopic) =>
      observation.topicConnected(
        sourceTopic,
        region,
        assignedPartitionsForSourceTopic(assignments, sourceTopic),
        nowMillis,
      ),
    { discard: true },
  );
});

export const recordKafkaLag = Effect.fn("ViewServerRuntime.kafka.observation.recordLag")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  observation: ViewServerKafkaHealthObservation<Topics>,
  region: string,
  topics: ReadonlyArray<string>,
  lag: Offsets,
  nowMillis: number,
) {
  yield* observation.regionRecovered(region, nowMillis);
  yield* Effect.forEach(
    topics,
    (sourceTopic) => {
      const sourceTopicLag = lag.get(sourceTopic);
      return observation.topicLagSampled(sourceTopic, region, {
        consumerLagMessages:
          sourceTopicLag === undefined ? null : consumerLagMessagesFromLag(sourceTopicLag),
        nowMillis,
      });
    },
    { discard: true },
  );
});

export const makeViewServerKafkaHealthObserver = Effect.fn(
  "ViewServerRuntime.kafka.observation.make",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  health: ViewServerKafkaHealthLedger<Topics>,
  requestHealthRefresh: Effect.Effect<void>,
  cadence: Duration.Input = "1 second",
) {
  return yield* Effect.uninterruptibleMask(() =>
    Effect.gen(function* () {
      const scope = yield* Scope.make("parallel");
      const dirty = MutableRef.make(false);
      const markDirty = Effect.sync(() => {
        dirty.current = true;
      });
      const observed = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        effect.pipe(Effect.tap(() => markDirty));
      const refreshIfDirty = Effect.fn("ViewServerRuntime.kafka.observation.refreshIfDirty")(
        function* () {
          const shouldRefresh = yield* Effect.sync(() => {
            const current = dirty.current;
            dirty.current = false;
            return current;
          });
          if (shouldRefresh) {
            yield* requestHealthRefresh;
          }
        },
      );
      const refreshIfDirtyUninterruptibly = () => refreshIfDirty().pipe(Effect.uninterruptible);

      yield* Effect.forever(
        Effect.sleep(cadence).pipe(Effect.andThen(refreshIfDirtyUninterruptibly())),
      ).pipe(Effect.forkIn(scope, { startImmediately: true }));

      const close = (yield* Effect.cached(
        Scope.close(scope, Exit.void).pipe(Effect.andThen(refreshIfDirtyUninterruptibly())),
      )).pipe(Effect.uninterruptible);

      return {
        regionConnected: (region: string, nowMillis: number) =>
          observed(health.regionConnected(region, nowMillis)),
        regionDisconnected: (
          region: string,
          message: string,
          options?: {
            readonly preserveTopicErrors?: boolean;
          },
        ) => observed(health.regionDisconnected(region, message, options)),
        regionDegraded: (region: string, message: string) =>
          observed(health.regionDegraded(region, message)),
        regionRecovered: (region: string, nowMillis: number) =>
          observed(health.regionRecovered(region, nowMillis)),
        regionStopped: (region: string) => observed(health.regionStopped(region)),
        topicConnected: (
          sourceTopic: string,
          region: string,
          assignedPartitions: number,
          nowMillis: number,
        ) => observed(health.topicConnected(sourceTopic, region, assignedPartitions, nowMillis)),
        topicLagSampled: (
          sourceTopic: string,
          region: string,
          input: {
            readonly consumerLagMessages: bigint | null;
            readonly nowMillis: number;
          },
        ) => observed(health.topicLagSampled(sourceTopic, region, input)),
        messageDecoded: (
          sourceTopic: string,
          region: string,
          input: {
            readonly bytes: number;
            readonly committedOffset: string;
            readonly nowMillis: number;
            readonly preserveLastError?: boolean;
          },
        ) => observed(health.messageDecoded(sourceTopic, region, input)),
        messageSkippedCommitted: (
          sourceTopic: string,
          region: string,
          input: {
            readonly committedOffset: string;
            readonly nowMillis: number;
          },
        ) => observed(health.messageSkippedCommitted(sourceTopic, region, input)),
        decodeFailed: (
          sourceTopic: string,
          region: string,
          input: {
            readonly bytes: number;
            readonly message: string;
            readonly nowMillis: number;
          },
        ) => observed(health.decodeFailed(sourceTopic, region, input)),
        mappingFailed: (
          sourceTopic: string,
          region: string,
          input: {
            readonly bytes: number;
            readonly message: string;
            readonly nowMillis: number;
          },
        ) => observed(health.mappingFailed(sourceTopic, region, input)),
        messagePublishFailed: (
          sourceTopic: string,
          region: string,
          input: {
            readonly bytes: number;
            readonly message: string;
            readonly nowMillis: number;
          },
        ) => observed(health.messagePublishFailed(sourceTopic, region, input)),
        messageCommitFailed: (
          sourceTopic: string,
          region: string,
          input: {
            readonly bytes: number;
            readonly message: string;
            readonly nowMillis: number;
            readonly recountMessage?: boolean;
          },
        ) => observed(health.messageCommitFailed(sourceTopic, region, input)),
        close,
      } satisfies ViewServerKafkaHealthObserver<Topics>;
    }),
  );
});
