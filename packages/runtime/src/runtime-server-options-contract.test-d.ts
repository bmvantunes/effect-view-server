import { describe, expectTypeOf, it } from "@effect/vitest";
import { Context, Effect } from "effect";
import {
  makeViewServerRuntime,
  type RuntimeDependency,
  type ViewServerRuntime,
  type ViewServerRuntimeOptions,
  type ViewServerRuntimeReportingOptions,
} from "./index";

import { viewServer } from "../test-harness/runtime-type-contracts";

class ReportingCallbackDependency extends Context.Service<
  ReportingCallbackDependency,
  { readonly report: () => void }
>()("@effect-view-server/runtime/type-test/ReportingCallbackDependency") {}

describe("Runtime server and TCP option contracts", () => {
  it("accepts valid contracts and rejects invalid contracts", () => {
    // @ts-expect-error runtime options reject string ports.
    const invalidOptions = makeViewServerRuntime(viewServer, {
      websocketPort: "8080",
    });

    const tcpPublishPortOptions = makeViewServerRuntime(viewServer, {
      tcpPublishMaxConnections: 16,
      tcpPublishPort: 8081,
    });

    const compressedWebSocketOptions = makeViewServerRuntime(viewServer, {
      websocketCompression: true,
    });

    expectTypeOf<Effect.Success<typeof compressedWebSocketOptions>>().toExtend<
      ViewServerRuntime<typeof viewServer.topics>
    >();

    const uncompressedWebSocketOptions = makeViewServerRuntime(viewServer, {
      websocketCompression: false,
    });

    expectTypeOf<Effect.Success<typeof uncompressedWebSocketOptions>>().toExtend<
      ViewServerRuntime<typeof viewServer.topics>
    >();

    // @ts-expect-error runtime WebSocket compression rejects string values.
    const _invalidWebSocketCompressionOptions = makeViewServerRuntime(viewServer, {
      websocketCompression: "true",
    });

    expectTypeOf<Effect.Success<typeof tcpPublishPortOptions>>().toMatchTypeOf<
      ViewServerRuntime<typeof viewServer.topics>
    >();

    // @ts-expect-error runtime TCP publish port rejects string ports.
    const invalidTcpPublishPortOptions = makeViewServerRuntime(viewServer, {
      tcpPublishPort: "8081",
    });

    // @ts-expect-error runtime TCP publish connection cap rejects string values.
    const invalidTcpPublishMaxConnectionsOptions = makeViewServerRuntime(viewServer, {
      tcpPublishMaxConnections: "16",
      tcpPublishPort: 8081,
    });

    // @ts-expect-error runtime paths must be absolute HTTP paths.
    const invalidPathOptions = makeViewServerRuntime(viewServer, {
      rpcPath: "runtime-rpc",
    });

    const invalidAuthOptions = {
      auth: {
        validateRequest: () => "not an effect",
      },
    };

    // @ts-expect-error runtime auth validator must return an Effect.
    invalidAuthOptions satisfies ViewServerRuntimeOptions<typeof viewServer.topics>;

    // @ts-expect-error runtime health paths must be absolute HTTP paths.
    const invalidHealthPathOptions = makeViewServerRuntime(viewServer, {
      healthPath: "runtime-health",
    });

    // @ts-expect-error runtime metrics paths must be absolute HTTP paths.
    const invalidMetricsPathOptions = makeViewServerRuntime(viewServer, {
      metricsPath: "runtime-metrics",
    });

    // @ts-expect-error runtime RPC path must be a concrete slash-prefixed client URL path.
    const invalidWildcardRpcPathOptions = makeViewServerRuntime(viewServer, {
      rpcPath: "*",
    });

    // @ts-expect-error runtime health path must be a concrete slash-prefixed client URL path.
    const invalidWildcardHealthPathOptions = makeViewServerRuntime(viewServer, {
      healthPath: "*",
    });

    // @ts-expect-error runtime metrics path must be a concrete slash-prefixed client URL path.
    const invalidWildcardMetricsPathOptions = makeViewServerRuntime(viewServer, {
      metricsPath: "*",
    });

    const invalidGroupedAdmissionLimitKey = makeViewServerRuntime(viewServer, {
      groupedIncrementalAdmissionLimits: {
        // @ts-expect-error grouped admission limits reject unknown keys.
        maxGroupz: 1,
      },
    });

    const invalidGroupedAdmissionLimitValue = makeViewServerRuntime(viewServer, {
      groupedIncrementalAdmissionLimits: {
        // @ts-expect-error grouped admission limits must be numeric.
        maxGroups: "1",
      },
    });

    const reportingOptions = makeViewServerRuntime(viewServer, {
      reporting: {
        heartbeatInterval: "5 seconds",
        dependenciesInterval: "30 seconds",
        onHeartbeat: (heartbeat) => {
          expectTypeOf(heartbeat).toEqualTypeOf<{
            readonly status:
              | "Starting"
              | "Ready"
              | "Degraded"
              | "WaitingToRetry"
              | "Reacquiring"
              | "Exhausted"
              | "Stopping";
            readonly problems: ReadonlyArray<"self" | "dependency">;
          }>();
          return Effect.void;
        },
        onDependenciesUpdate: (dependencies) => {
          expectTypeOf<(typeof dependencies)[number]>().toEqualTypeOf<RuntimeDependency>();
          return Effect.void;
        },
      },
    });

    expectTypeOf<Effect.Success<typeof reportingOptions>>().toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>
    >();

    const missingDependencyCallback = {
      heartbeatInterval: "5 seconds",
      dependenciesInterval: "30 seconds",
      onHeartbeat: () => Effect.void,
    };
    // @ts-expect-error reporting requires both callbacks.
    missingDependencyCallback satisfies ViewServerRuntimeReportingOptions;

    const invalidReportingCallback = {
      heartbeatInterval: "5 seconds",
      dependenciesInterval: "30 seconds",
      onHeartbeat: () => undefined,
      onDependenciesUpdate: () => Effect.void,
    };
    // @ts-expect-error reporting callbacks must return Effects.
    invalidReportingCallback satisfies ViewServerRuntimeReportingOptions;

    const invalidReportingKey = makeViewServerRuntime(viewServer, {
      reporting: {
        heartbeatInterval: "5 seconds",
        dependenciesInterval: "30 seconds",
        onHeartbeat: () => Effect.void,
        onDependenciesUpdate: () => Effect.void,
        // @ts-expect-error reporting rejects unknown keys.
        interval: "1 second",
      },
    });

    // These deliberately invalid Effect environments are the contracts under test.
    // @effect-diagnostics missingEffectContext:off
    const serviceHeartbeatCallback = makeViewServerRuntime(viewServer, {
      reporting: {
        heartbeatInterval: "5 seconds",
        dependenciesInterval: "30 seconds",
        // @ts-expect-error runtime heartbeat callbacks must be closed Effects.
        onHeartbeat: () => ReportingCallbackDependency.pipe(Effect.asVoid),
        onDependenciesUpdate: () => Effect.void,
      },
    });

    const serviceDependenciesCallback = makeViewServerRuntime(viewServer, {
      reporting: {
        heartbeatInterval: "5 seconds",
        dependenciesInterval: "30 seconds",
        onHeartbeat: () => Effect.void,
        // @ts-expect-error runtime dependency callbacks must be closed Effects.
        onDependenciesUpdate: () => ReportingCallbackDependency.pipe(Effect.asVoid),
      },
    });
    // @effect-diagnostics missingEffectContext:on

    expectTypeOf(invalidOptions).not.toBeAny();

    expectTypeOf(invalidPathOptions).not.toBeAny();

    expectTypeOf(invalidHealthPathOptions).not.toBeAny();

    expectTypeOf(invalidMetricsPathOptions).not.toBeAny();

    expectTypeOf(invalidWildcardRpcPathOptions).not.toBeAny();

    expectTypeOf(invalidWildcardHealthPathOptions).not.toBeAny();

    expectTypeOf(invalidWildcardMetricsPathOptions).not.toBeAny();

    expectTypeOf(invalidGroupedAdmissionLimitKey).not.toBeAny();

    expectTypeOf(invalidGroupedAdmissionLimitValue).not.toBeAny();

    expectTypeOf(invalidTcpPublishPortOptions).not.toBeAny();

    expectTypeOf(invalidTcpPublishMaxConnectionsOptions).not.toBeAny();

    expectTypeOf(invalidReportingKey).not.toBeAny();

    expectTypeOf(serviceHeartbeatCallback).not.toBeAny();

    expectTypeOf(serviceDependenciesCallback).not.toBeAny();

    expectTypeOf<ViewServerRuntimeOptions>().not.toHaveProperty("port");

    expectTypeOf<ViewServerRuntimeOptions>().not.toHaveProperty("path");

    expectTypeOf<ViewServerRuntimeOptions>().toHaveProperty("tcpPublishMaxConnections");

    expectTypeOf<ViewServerRuntimeOptions>().toHaveProperty("tcpPublishHost");

    expectTypeOf<ViewServerRuntimeOptions>().toHaveProperty("tcpPublishPort");

    expectTypeOf<ViewServerRuntimeOptions>().not.toHaveProperty("grpc");

    expectTypeOf<ViewServerRuntimeOptions>().not.toHaveProperty("kafka");

    const invalidGrpcBag = makeViewServerRuntime(viewServer, {
      // @ts-expect-error generic runtime options reject transport-specific gRPC bags.
      grpc: {},
    });

    const invalidKafkaBag = makeViewServerRuntime(viewServer, {
      // @ts-expect-error generic runtime options reject transport-specific Kafka bags.
      kafka: {},
    });

    expectTypeOf(invalidGrpcBag).not.toBeAny();

    expectTypeOf(invalidKafkaBag).not.toBeAny();
  });
});
