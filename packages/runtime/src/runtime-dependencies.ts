import type {
  GrpcRuntimeClients,
  RuntimeRegions,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import type { ViewServerRuntimeLiveClient } from "@effect-view-server/client";
import { type ViewServerRuntimeCoreOptionsFor } from "@effect-view-server/runtime-core";
import {
  makeViewServerRuntimeCoreInternal,
  type ViewServerRuntimeCoreInternalClient,
  type ViewServerRuntimeCoreInternalLiveClient,
  type ViewServerRuntimeCoreInternalInstance,
} from "@effect-view-server/runtime-core/internal";
import {
  makeViewServerWebSocketServer,
  type ViewServerWebSocketServer,
  type ViewServerWebSocketServerInput,
  type ViewServerWebSocketServerOptions,
} from "@effect-view-server/server";
import type { Effect } from "effect";
import type { HttpServerError } from "effect/unstable/http";
import { makeViewServerKafkaHealthLedger, type ViewServerKafkaHealthLedger } from "./kafka-health";
import {
  makeViewServerKafkaHealthObserver,
  type ViewServerKafkaHealthObservation,
  type ViewServerKafkaHealthObserver,
} from "./kafka-health-observation";
import { makeViewServerGrpcHealthLedger, type ViewServerGrpcHealthLedger } from "./grpc-health";
import {
  makeViewServerGrpcIngress,
  type ViewServerGrpcIngress,
  type ViewServerGrpcIngressError,
} from "./grpc-ingress";
import {
  makeViewServerGrpcLeaseManager,
  type ViewServerGrpcLeaseManager,
} from "./grpc-lease-manager";
import {
  makeViewServerKafkaIngress,
  type ViewServerKafkaIngress,
  type ViewServerKafkaIngressError,
} from "./kafka-ingress";
import {
  makeViewServerTcpPublishIngress,
  type ViewServerTcpPublishIngress,
  type ViewServerTcpPublishIngressError,
  type ViewServerTcpPublishIngressOptions,
} from "./tcp-publish-ingress";
import type {
  ResolvedViewServerGrpcRuntimeOptions,
  ResolvedViewServerKafkaRuntimeOptions,
} from "./runtime-options";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export type ViewServerRuntimeDependencies<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly makeRuntimeCore: (
    config: ViewServerRuntimeDependencyConfig<Topics>,
    options: ViewServerRuntimeCoreOptionsFor<Topics>,
  ) => Effect.Effect<ViewServerRuntimeCoreInternalInstance<Topics>, ViewServerRuntimeError>;
  readonly makeServer: (
    config: ViewServerRuntimeDependencyConfig<Topics>,
    input: ViewServerWebSocketServerInput<Topics>,
    options: ViewServerWebSocketServerOptions,
  ) => Effect.Effect<ViewServerWebSocketServer, HttpServerError.ServeError>;
  readonly makeKafkaHealthLedger: <const Regions extends RuntimeRegions>(
    config: ViewServerRuntimeDependencyConfig<Topics>,
    options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
  ) => ViewServerKafkaHealthLedger<Topics>;
  readonly makeKafkaHealthObserver: (
    health: ViewServerKafkaHealthLedger<Topics>,
    requestHealthRefresh: Effect.Effect<void>,
  ) => Effect.Effect<ViewServerKafkaHealthObserver<Topics>>;
  readonly makeGrpcHealthLedger: <const Clients extends GrpcRuntimeClients>(
    config: ViewServerRuntimeDependencyConfig<Topics>,
    options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
  ) => ViewServerGrpcHealthLedger<Topics>;
  readonly makeGrpcLeaseManager: <const Clients extends GrpcRuntimeClients>(
    config: ViewServerRuntimeDependencyConfig<Topics>,
    runtimeClient: ViewServerRuntimeCoreInternalClient<Topics>,
    liveClient: ViewServerRuntimeLiveClient<Topics>,
    internalLiveClient: ViewServerRuntimeCoreInternalLiveClient<Topics>,
    requestHealthRefresh: Effect.Effect<void>,
    options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
    health: ViewServerGrpcHealthLedger<Topics>,
  ) => Effect.Effect<ViewServerGrpcLeaseManager<Topics>>;
  readonly makeKafkaIngress: <const Regions extends RuntimeRegions>(
    config: ViewServerRuntimeDependencyConfig<Topics>,
    client: ViewServerRuntimeCoreInternalClient<Topics>,
    options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
    health: ViewServerKafkaHealthObservation<Topics>,
  ) => Effect.Effect<ViewServerKafkaIngress, ViewServerKafkaIngressError>;
  readonly makeTcpPublishIngress: (
    config: ViewServerRuntimeDependencyConfig<Topics>,
    client: ViewServerRuntimeCoreInternalClient<Topics>,
    options: ViewServerTcpPublishIngressOptions,
  ) => Effect.Effect<ViewServerTcpPublishIngress, ViewServerTcpPublishIngressError>;
  readonly makeGrpcIngress: <const Clients extends GrpcRuntimeClients>(
    config: ViewServerRuntimeDependencyConfig<Topics>,
    client: ViewServerRuntimeCoreInternalClient<Topics>,
    requestHealthRefresh: Effect.Effect<void>,
    options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
    health: ViewServerGrpcHealthLedger<Topics>,
  ) => Effect.Effect<ViewServerGrpcIngress, ViewServerGrpcIngressError>;
};

export type ViewServerRuntimeDependencyConfig<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly topics: Topics;
};

export const makeDefaultRuntimeDependencies = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(): ViewServerRuntimeDependencies<Topics> => ({
  makeRuntimeCore: makeViewServerRuntimeCoreInternal,
  makeServer: makeViewServerWebSocketServer,
  makeKafkaHealthLedger: (_config, options) =>
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
  makeGrpcHealthLedger: (config, options) => {
    const hasConfiguredTopic = (topic: string): topic is Extract<keyof Topics, string> =>
      Object.hasOwn(config.topics, topic);
    const feeds: Record<
      string,
      {
        readonly client: string;
        readonly lifecycle: "materialized" | "leased";
        readonly topic: Extract<keyof Topics, string>;
      }
    > = Object.create(null);
    for (const [feedName, feed] of Object.entries(options.feeds)) {
      if (hasConfiguredTopic(feed.topic)) {
        feeds[feedName] = {
          client: feed.client,
          lifecycle: feed.lifecycle,
          topic: feed.topic,
        };
      }
    }
    return makeViewServerGrpcHealthLedger({
      clients: options.clientBaseUrls,
      feeds,
    });
  },
  makeGrpcLeaseManager: makeViewServerGrpcLeaseManager,
  makeKafkaHealthObserver: makeViewServerKafkaHealthObserver,
  makeKafkaIngress: makeViewServerKafkaIngress,
  makeTcpPublishIngress: makeViewServerTcpPublishIngress,
  makeGrpcIngress: makeViewServerGrpcIngress,
});
