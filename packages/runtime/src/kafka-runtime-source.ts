import type {
  GrpcRuntimeClients,
  RuntimeRegions,
  ViewServerConfig,
} from "@effect-view-server/config";
import type { Effect } from "effect";
import { Effect as RuntimeEffect } from "effect";
import { makeViewServerKafkaHealthLedger, type ViewServerKafkaHealthLedger } from "./kafka-health";
import {
  makeViewServerKafkaHealthObserver,
  type ViewServerKafkaHealthObservation,
  type ViewServerKafkaHealthObserver,
} from "./kafka-health-observation";
import { makeViewServerKafkaIngress, type ViewServerKafkaIngress } from "./kafka-ingress";
import { ViewServerKafkaIngressError } from "./kafka-ingress-error";
import {
  requireKafkaRuntimeOptionsForConfigSources,
  resolveViewServerKafkaRuntimeOptions,
  type ResolvedViewServerKafkaRuntimeOptions,
} from "./kafka-runtime-options";
import type {
  ViewServerRuntimeSourceAdapter,
  ViewServerRuntimeSourceModule,
} from "./runtime-source";
import type { ViewServerRuntimeOptions, ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export const resolveKafkaRuntimeSourceOptions = RuntimeEffect.fn(
  "ViewServerRuntime.source.kafka.options.resolve",
)(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, Clients>,
  runtimeOptions: ViewServerRuntimeOptions<Topics, Regions, Clients> = {},
) {
  if (runtimeOptions.kafka === undefined) {
    yield* requireKafkaRuntimeOptionsForConfigSources(config);
    return undefined;
  }
  return yield* resolveViewServerKafkaRuntimeOptions(config, runtimeOptions.kafka);
});

export type ViewServerKafkaRuntimeSourceDependencies<
  Topics extends ViewServerRuntimeTopicDefinitions,
> = {
  readonly makeHealthLedger: <const Regions extends RuntimeRegions>(
    options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
  ) => ViewServerKafkaHealthLedger<Topics>;
  readonly makeHealthObserver: (
    health: ViewServerKafkaHealthLedger<Topics>,
    refreshHealth: Effect.Effect<void>,
  ) => Effect.Effect<ViewServerKafkaHealthObserver<Topics>>;
  readonly makeIngress: <const Regions extends RuntimeRegions>(
    config: { readonly topics: Topics },
    client: Parameters<typeof makeViewServerKafkaIngress<Topics, Regions>>[1],
    options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
    health: ViewServerKafkaHealthObservation<Topics>,
  ) => Effect.Effect<ViewServerKafkaIngress, ViewServerKafkaIngressError>;
};

export const makeDefaultKafkaRuntimeSourceDependencies = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(): ViewServerKafkaRuntimeSourceDependencies<Topics> => ({
  makeHealthLedger: (options) =>
    makeViewServerKafkaHealthLedger({
      startFrom: options.consume,
      regions: options.regions,
      topics: Object.fromEntries(
        Object.entries(options.topics).map(([sourceTopic, topic]) => [
          sourceTopic,
          {
            regions: topic.regions,
            viewServerTopic: topic.viewServerTopic,
          },
        ]),
      ),
    }),
  makeHealthObserver: makeViewServerKafkaHealthObserver,
  makeIngress: makeViewServerKafkaIngress,
});

export const makeKafkaRuntimeSourceAdapter = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  dependencies: ViewServerKafkaRuntimeSourceDependencies<Topics> = makeDefaultKafkaRuntimeSourceDependencies<Topics>(),
): ViewServerRuntimeSourceAdapter<Topics, ViewServerKafkaIngressError> => ({
  make: (config, runtimeOptions) =>
    RuntimeEffect.gen(function* () {
      const options = yield* resolveKafkaRuntimeSourceOptions(config, runtimeOptions);
      if (options === undefined) {
        return undefined;
      }
      const health = dependencies.makeHealthLedger(options);
      const module: ViewServerRuntimeSourceModule<Topics, ViewServerKafkaIngressError> = {
        healthOverlay: health.healthOverlay,
        ownedTopics: Object.entries(options.topics).map(([sourceTopic, topic]) => ({
          topic: topic.viewServerTopic,
          owner: `Kafka source ${sourceTopic}`,
          conflict: (existingOwner) =>
            new ViewServerKafkaIngressError({
              message: `View Server topic ${topic.viewServerTopic} cannot be owned by both ${existingOwner} and Kafka source ${sourceTopic}.`,
              cause: topic.viewServerTopic,
              sourceTopic,
            }),
        })),
        prepare: (input) =>
          RuntimeEffect.gen(function* () {
            const observation = yield* RuntimeEffect.acquireRelease(
              dependencies.makeHealthObserver(health, input.refreshHealth),
              (resource) => resource.close,
              { interruptible: false },
            );
            return {
              client: input.client,
              liveClient: input.liveClient,
              start: RuntimeEffect.acquireRelease(
                dependencies.makeIngress(
                  { topics: config.topics },
                  input.internalClient,
                  options,
                  observation,
                ),
                (resource) => resource.close,
                { interruptible: true },
              ).pipe(RuntimeEffect.asVoid),
            };
          }),
      };
      return module;
    }),
});
