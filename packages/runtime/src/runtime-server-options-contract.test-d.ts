import { describe, expectTypeOf, it } from "@effect/vitest";
import { Effect } from "effect";
import {
  makeViewServerRuntime,
  type ViewServerRuntime,
  type ViewServerRuntimeOptions,
} from "./index";

import { viewServer } from "../test-harness/runtime-type-contracts";

describe("Runtime server and TCP option contracts", () => {
  it("accepts valid contracts and rejects invalid contracts", () => {
    const invalidOptions = makeViewServerRuntime(viewServer, {
      // @ts-expect-error runtime options reject string ports.
      websocketPort: "8080",
    });

    const tcpPublishPortOptions = makeViewServerRuntime(viewServer, {
      tcpPublishMaxConnections: 16,
      tcpPublishPort: 8081,
    });

    expectTypeOf<Effect.Success<typeof tcpPublishPortOptions>>().toMatchTypeOf<
      ViewServerRuntime<typeof viewServer.topics>
    >();

    const invalidTcpPublishPortOptions = makeViewServerRuntime(viewServer, {
      // @ts-expect-error runtime TCP publish port rejects string ports.
      tcpPublishPort: "8081",
    });

    const invalidTcpPublishMaxConnectionsOptions = makeViewServerRuntime(viewServer, {
      // @ts-expect-error runtime TCP publish connection cap rejects string values.
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

    expectTypeOf<ViewServerRuntimeOptions>().not.toHaveProperty("port");

    expectTypeOf<ViewServerRuntimeOptions>().not.toHaveProperty("path");

    expectTypeOf<ViewServerRuntimeOptions>().toHaveProperty("tcpPublishMaxConnections");

    expectTypeOf<ViewServerRuntimeOptions>().toHaveProperty("tcpPublishHost");

    expectTypeOf<ViewServerRuntimeOptions>().toHaveProperty("tcpPublishPort");

    expectTypeOf<ViewServerRuntimeOptions>().toHaveProperty("grpc");
  });
});
