import type { GrpcRuntimeClients, RuntimeRegions } from "@effect-view-server/config";
import type { ViewServerRuntimeCoreOptionsFor } from "@effect-view-server/runtime-core";
import type { ViewServerWebSocketServerOptions } from "@effect-view-server/server";
import type { ViewServerRuntimeOptions, ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export type ResolvedViewServerRuntimeBaseOptions<
  Topics extends ViewServerRuntimeTopicDefinitions = ViewServerRuntimeTopicDefinitions,
  Regions extends RuntimeRegions = RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
> = {
  readonly auth?: ViewServerRuntimeOptions<Topics, Regions, GrpcClients>["auth"];
  readonly runtimeCoreOptions: ViewServerRuntimeCoreOptionsFor<Topics>;
  readonly serverOptions: ViewServerWebSocketServerOptions;
  readonly tcpPublishOptions?: {
    readonly host?: string;
    readonly maxConnections?: number;
    readonly port: number;
  };
};

export const resolveViewServerRuntimeBaseOptions = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(
  options: ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
): ResolvedViewServerRuntimeBaseOptions<Topics, Regions, GrpcClients> => ({
  ...(options.auth === undefined ? {} : { auth: options.auth }),
  runtimeCoreOptions: {
    ...(options.groupedIncrementalAdmissionLimits === undefined
      ? {}
      : { groupedIncrementalAdmissionLimits: options.groupedIncrementalAdmissionLimits }),
    ...(options.subscriptionQueueCapacity === undefined
      ? {}
      : { subscriptionQueueCapacity: options.subscriptionQueueCapacity }),
  },
  serverOptions: {
    ...(options.host === undefined ? {} : { host: options.host }),
    ...(options.websocketPort === undefined ? {} : { port: options.websocketPort }),
    ...(options.rpcPath === undefined ? {} : { path: options.rpcPath }),
    ...(options.healthPath === undefined ? {} : { healthPath: options.healthPath }),
    ...(options.metricsPath === undefined ? {} : { metricsPath: options.metricsPath }),
  },
  ...(options.tcpPublishPort === undefined
    ? {}
    : {
        tcpPublishOptions: {
          ...(options.tcpPublishHost === undefined ? {} : { host: options.tcpPublishHost }),
          ...(options.tcpPublishMaxConnections === undefined
            ? {}
            : { maxConnections: options.tcpPublishMaxConnections }),
          port: options.tcpPublishPort,
        },
      }),
});
