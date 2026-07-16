import type { ViewServerRuntimeError } from "@effect-view-server/config";
import type { ViewServerRuntimeDecodedMutationClient } from "@effect-view-server/config/internal";
import { type ViewServerRuntimeCoreOptionsFor } from "@effect-view-server/runtime-core";
import {
  makeViewServerRuntimeCoreInternal,
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
import { makeDefaultRuntimeSourceAdapters } from "./runtime-source-adapters";
import type { ViewServerRuntimeSourceError } from "./runtime-source-adapters";
import type { ViewServerRuntimeSourceAdapter } from "./runtime-source";
import {
  makeViewServerTcpPublishIngress,
  type ViewServerTcpPublishIngress,
  type ViewServerTcpPublishIngressError,
  type ViewServerTcpPublishIngressOptions,
} from "./tcp-publish-ingress";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export type ViewServerRuntimeDependencies<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly sourceAdapters: ReadonlyArray<
    ViewServerRuntimeSourceAdapter<Topics, ViewServerRuntimeSourceError>
  >;
  readonly makeRuntimeCore: (
    config: ViewServerRuntimeDependencyConfig<Topics>,
    options: ViewServerRuntimeCoreOptionsFor<Topics>,
  ) => Effect.Effect<ViewServerRuntimeCoreInternalInstance<Topics>, ViewServerRuntimeError>;
  readonly makeServer: (
    config: ViewServerRuntimeDependencyConfig<Topics>,
    input: ViewServerWebSocketServerInput<Topics>,
    options: ViewServerWebSocketServerOptions,
  ) => Effect.Effect<ViewServerWebSocketServer, HttpServerError.ServeError>;
  readonly makeTcpPublishIngress: (
    config: ViewServerRuntimeDependencyConfig<Topics>,
    client: ViewServerRuntimeDecodedMutationClient<Topics>,
    options: ViewServerTcpPublishIngressOptions,
  ) => Effect.Effect<ViewServerTcpPublishIngress, ViewServerTcpPublishIngressError>;
};

export type ViewServerRuntimeDependencyConfig<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly topics: Topics;
};

export const makeDefaultRuntimeDependencies = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(): ViewServerRuntimeDependencies<Topics> => ({
  sourceAdapters: makeDefaultRuntimeSourceAdapters<Topics>(),
  makeRuntimeCore: makeViewServerRuntimeCoreInternal,
  makeServer: makeViewServerWebSocketServer,
  makeTcpPublishIngress: makeViewServerTcpPublishIngress,
});
