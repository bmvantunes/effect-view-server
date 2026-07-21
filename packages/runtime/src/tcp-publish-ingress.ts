import type { ViewServerTopicConfig } from "@effect-view-server/config";
import type {
  ViewServerRuntimeDecodedMutationClient,
  ViewServerRuntimeTopicDefinitions,
} from "@effect-view-server/config/internal";
import type { ViewServerAuth } from "@effect-view-server/server";
import { Effect } from "effect";
import * as Net from "node:net";
import { ViewServerTcpPublishIngressError } from "./tcp-publish-command";
import {
  makeTcpPublishSocketServer,
  type TcpPublishServerFactory,
} from "./tcp-publish-socket-runtime";

export { ViewServerTcpPublishIngressError } from "./tcp-publish-command";
export { tcpPublishUrl, writeTcpJsonLine } from "./tcp-publish-socket-runtime";

export type ViewServerTcpPublishIngressOptions = {
  readonly host?: string;
  readonly maxConnections?: number;
  readonly maxGlobalQueuedCommands?: number;
  readonly maxLineBytes?: number;
  readonly maxQueuedCommands?: number;
  readonly port: number;
  readonly auth?: ViewServerAuth;
};

export type ViewServerTcpPublishIngress = {
  readonly url: string;
  readonly close: Effect.Effect<void>;
};

const validateTcpPublishOptions = (
  options: ViewServerTcpPublishIngressOptions,
): Effect.Effect<void, ViewServerTcpPublishIngressError> => {
  if (!Number.isSafeInteger(options.port) || options.port < 0 || options.port > 65535) {
    return Effect.fail(
      new ViewServerTcpPublishIngressError({
        message: "TCP publish port must be a safe integer between 0 and 65535.",
        cause: options.port,
        phase: "configuration",
      }),
    );
  }
  if (
    options.maxLineBytes !== undefined &&
    (!Number.isSafeInteger(options.maxLineBytes) || options.maxLineBytes <= 0)
  ) {
    return Effect.fail(
      new ViewServerTcpPublishIngressError({
        message: "TCP publish maxLineBytes must be a positive safe integer.",
        cause: options.maxLineBytes,
        phase: "configuration",
      }),
    );
  }
  if (
    options.maxConnections !== undefined &&
    (!Number.isSafeInteger(options.maxConnections) || options.maxConnections <= 0)
  ) {
    return Effect.fail(
      new ViewServerTcpPublishIngressError({
        message: "TCP publish maxConnections must be a positive safe integer.",
        cause: options.maxConnections,
        phase: "configuration",
      }),
    );
  }
  if (
    options.maxQueuedCommands !== undefined &&
    (!Number.isSafeInteger(options.maxQueuedCommands) || options.maxQueuedCommands <= 0)
  ) {
    return Effect.fail(
      new ViewServerTcpPublishIngressError({
        message: "TCP publish maxQueuedCommands must be a positive safe integer.",
        cause: options.maxQueuedCommands,
        phase: "configuration",
      }),
    );
  }
  if (
    options.maxGlobalQueuedCommands !== undefined &&
    (!Number.isSafeInteger(options.maxGlobalQueuedCommands) || options.maxGlobalQueuedCommands <= 0)
  ) {
    return Effect.fail(
      new ViewServerTcpPublishIngressError({
        message: "TCP publish maxGlobalQueuedCommands must be a positive safe integer.",
        cause: options.maxGlobalQueuedCommands,
        phase: "configuration",
      }),
    );
  }
  return Effect.void;
};

/**
 * @internal Package-local Adapter constructor for deterministic socket ownership tests.
 * Node's real server cannot reliably force listen, handoff, and close failures at exact lifecycle
 * boundaries; injecting only the server factory keeps those failure paths observable without
 * exposing test controls from the public runtime package.
 */
export const makeViewServerTcpPublishIngressWithServerFactory = Effect.fn(
  "ViewServerRuntime.tcpPublish.makeWithServerFactory",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  client: ViewServerRuntimeDecodedMutationClient<Topics>,
  options: ViewServerTcpPublishIngressOptions,
  createServer: TcpPublishServerFactory,
) {
  yield* validateTcpPublishOptions(options);
  return yield* makeTcpPublishSocketServer(config, client, options, createServer);
});

export const makeViewServerTcpPublishIngress = Effect.fn("ViewServerRuntime.tcpPublish.make")(
  function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
    config: ViewServerTopicConfig<Topics>,
    client: ViewServerRuntimeDecodedMutationClient<Topics>,
    options: ViewServerTcpPublishIngressOptions,
  ) {
    return yield* makeViewServerTcpPublishIngressWithServerFactory(
      config,
      client,
      options,
      (connectionListener) => Net.createServer(connectionListener),
    );
  },
);
