import type { ViewServerConfig, ViewServerRuntimeError } from "@effect-view-server/config";
import type { ViewServerSourceRequirements } from "@effect-view-server/runtime-core";
import { Effect } from "effect";
import type { HttpServerError } from "effect/unstable/http";
import {
  makeDefaultRuntimeDependencies,
  makeViewServerRuntimeWithDependencies,
  runViewServerRuntimeWithDependencies,
  type ViewServerRuntime,
  type ViewServerRuntimeOptions,
  type ViewServerRuntimeOptionsArgs,
  type ViewServerRuntimeOptionsInput,
  type ViewServerRuntimeTopicDefinitions,
} from "./internal";
import type { ViewServerTcpPublishIngressError } from "./tcp-publish-ingress";

export type {
  ViewServerRuntime,
  ViewServerRuntimeOptions,
  ViewServerRuntimeOptionsArgs,
  ViewServerRuntimeOptionsInput,
};
export type { ViewServerTcpPublishIngressError } from "./tcp-publish-ingress";

const makeViewServerRuntimeEffect = Effect.fn("ViewServerRuntime.make")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Options extends object,
>(config: ViewServerConfig<Topics>, options?: ViewServerRuntimeOptionsInput<Topics, Options>) {
  if (options === undefined) {
    return yield* makeViewServerRuntimeWithDependencies(
      makeDefaultRuntimeDependencies<Topics>(),
      config,
    );
  }
  return yield* makeViewServerRuntimeWithDependencies(
    makeDefaultRuntimeDependencies<Topics>(),
    config,
    options,
  );
});

export function makeViewServerRuntime<
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Options extends object = ViewServerRuntimeOptions<Topics>,
>(
  config: ViewServerConfig<Topics>,
  ...args: ViewServerRuntimeOptionsArgs<NoInfer<Topics>, Options>
): Effect.Effect<
  ViewServerRuntime<Topics>,
  HttpServerError.ServeError | ViewServerRuntimeError | ViewServerTcpPublishIngressError,
  ViewServerSourceRequirements<Topics>
> {
  return makeViewServerRuntimeEffect(config, args[0]);
}

export const createViewServerRuntime = makeViewServerRuntime;

const runViewServerRuntimeEffect = Effect.fn("ViewServerRuntime.run")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Options extends object,
>(config: ViewServerConfig<Topics>, options?: ViewServerRuntimeOptionsInput<Topics, Options>) {
  if (options === undefined) {
    return yield* runViewServerRuntimeWithDependencies(
      makeDefaultRuntimeDependencies<Topics>(),
      config,
    );
  }
  return yield* runViewServerRuntimeWithDependencies(
    makeDefaultRuntimeDependencies<Topics>(),
    config,
    options,
  );
});

export function runViewServerRuntime<
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Options extends object = ViewServerRuntimeOptions<Topics>,
>(
  config: ViewServerConfig<Topics>,
  ...args: ViewServerRuntimeOptionsArgs<NoInfer<Topics>, Options>
): Effect.Effect<
  never,
  HttpServerError.ServeError | ViewServerRuntimeError | ViewServerTcpPublishIngressError,
  ViewServerSourceRequirements<Topics>
> {
  return runViewServerRuntimeEffect(config, args[0]);
}
