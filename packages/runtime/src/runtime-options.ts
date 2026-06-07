import type { ViewServerRuntimeCoreOptionsFor } from "@view-server/runtime-core";
import type { ViewServerWebSocketServerOptions } from "@view-server/server";
import type { RuntimeRegions, RuntimeValue } from "@view-server/config";
import { Config, Effect } from "effect";
import type {
  ViewServerKafkaRuntimeOptions,
  ViewServerRuntimeOptions,
  ViewServerRuntimeTopicDefinitions,
} from "./runtime-types";

export type ResolvedViewServerRuntimeOptions<
  Topics extends ViewServerRuntimeTopicDefinitions = ViewServerRuntimeTopicDefinitions,
  Regions extends RuntimeRegions = RuntimeRegions,
> = {
  readonly runtimeCoreOptions: ViewServerRuntimeCoreOptionsFor<Topics>;
  readonly serverOptions: ViewServerWebSocketServerOptions;
  readonly kafkaOptions?: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>;
};

export type ResolvedViewServerKafkaRuntimeOptions<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Regions extends RuntimeRegions = RuntimeRegions,
> = {
  readonly consumerGroupId: string;
  readonly regions: Record<string, string>;
  readonly topics: ViewServerKafkaRuntimeOptions<Topics, Regions>["topics"];
};

const resolveRuntimeValue = <A>(value: RuntimeValue<A>): Effect.Effect<A, Config.ConfigError> =>
  Config.isConfig(value) ? value : Effect.succeed(value);

const resolveKafkaOptions: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  options: ViewServerKafkaRuntimeOptions<Topics, Regions>,
) => Effect.Effect<ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>, Config.ConfigError> =
  Effect.fn("ViewServerRuntime.options.kafka.resolve")(function* <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Regions extends RuntimeRegions,
  >(options: ViewServerKafkaRuntimeOptions<Topics, Regions>) {
    const entries = yield* Effect.forEach(Object.entries(options.regions), ([region, value]) =>
      resolveRuntimeValue(value).pipe(Effect.map((bootstrap) => [region, bootstrap] as const)),
    );
    const regions: Record<string, string> = {};
    for (const [region, bootstrap] of entries) {
      regions[region] = bootstrap;
    }
    return {
      consumerGroupId: options.consumerGroupId ?? "view-server",
      regions,
      topics: options.topics,
    };
  });

export const resolveViewServerRuntimeOptions: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  options: ViewServerRuntimeOptions<Topics, Regions>,
) => Effect.Effect<ResolvedViewServerRuntimeOptions<Topics, Regions>, Config.ConfigError> =
  Effect.fn("ViewServerRuntime.options.resolve")(function* <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Regions extends RuntimeRegions,
  >(options: ViewServerRuntimeOptions<Topics, Regions>) {
    const runtimeCoreOptions = {
      ...(options.groupedIncrementalAdmissionLimits === undefined
        ? {}
        : { groupedIncrementalAdmissionLimits: options.groupedIncrementalAdmissionLimits }),
      ...(options.subscriptionQueueCapacity === undefined
        ? {}
        : { subscriptionQueueCapacity: options.subscriptionQueueCapacity }),
    };
    const serverOptions = {
      ...(options.host === undefined ? {} : { host: options.host }),
      ...(options.websocketPort === undefined ? {} : { port: options.websocketPort }),
      ...(options.rpcPath === undefined ? {} : { path: options.rpcPath }),
      ...(options.healthPath === undefined ? {} : { healthPath: options.healthPath }),
    };
    const kafkaOptions =
      options.kafka === undefined ? undefined : yield* resolveKafkaOptions(options.kafka);
    return {
      runtimeCoreOptions,
      serverOptions,
      ...(kafkaOptions === undefined ? {} : { kafkaOptions }),
    };
  });
