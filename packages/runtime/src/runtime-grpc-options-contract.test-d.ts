import { describe, expectTypeOf, it } from "@effect/vitest";
import { Effect } from "effect";
import {
  makeViewServerRuntime,
  type ViewServerRuntime,
  type ViewServerGrpcRuntimeOptions,
} from "./index";

import {
  grpcRuntimeClients,
  materializedGrpcRuntimeWithConfigClientsEffect,
  materializedGrpcViewServer,
  materializedGrpcViewServerWithConfigClients,
} from "../test-harness/runtime-type-contracts";

describe("Runtime gRPC option contracts", () => {
  it("accepts valid contracts and rejects invalid contracts", () => {
    const runtimeWithGrpc = makeViewServerRuntime(materializedGrpcViewServer, {
      grpc: {
        materializedReconnect: {
          delay: "100 millis",
          maxReconnects: 5,
        },
      },
    });

    const invalidGrpcReconnectKey = makeViewServerRuntime(materializedGrpcViewServer, {
      grpc: {
        materializedReconnect: {
          delay: "100 millis",
          maxReconnects: 5,
          // @ts-expect-error runtime gRPC reconnect options reject unknown fields.
          maxAttempts: 5,
        },
      },
    });

    const invalidGrpcReconnectMax = {
      delay: "100 millis",
      // @ts-expect-error runtime gRPC reconnect maxReconnects must be a number.
      maxReconnects: "5",
    } satisfies NonNullable<
      ViewServerGrpcRuntimeOptions<
        typeof materializedGrpcViewServer.topics
      >["materializedReconnect"]
    >;

    const invalidGrpcReconnectDelay = {
      // @ts-expect-error runtime gRPC reconnect delay must be a Duration.Input.
      delay: false,
      maxReconnects: 5,
    } satisfies NonNullable<
      ViewServerGrpcRuntimeOptions<
        typeof materializedGrpcViewServer.topics
      >["materializedReconnect"]
    >;

    const invalidGrpcOptionKey = makeViewServerRuntime(materializedGrpcViewServer, {
      grpc: {
        // @ts-expect-error runtime gRPC options reject unknown fields.
        feedz: {},
      },
    });

    const invalidGrpcClientsOption = makeViewServerRuntime(materializedGrpcViewServer, {
      grpc: {
        // @ts-expect-error runtime gRPC options do not accept clients; bind clients in defineViewServerConfig.
        clients: grpcRuntimeClients,
      },
    });

    const invalidGrpcFeedsOption = makeViewServerRuntime(materializedGrpcViewServer, {
      grpc: {
        // @ts-expect-error runtime gRPC options do not accept feed declarations; bind feeds in topic-owned grpcSource.
        feeds: {},
      },
    });

    expectTypeOf<Effect.Success<typeof runtimeWithGrpc>>().toMatchTypeOf<
      ViewServerRuntime<typeof materializedGrpcViewServer.topics>
    >();

    expectTypeOf<
      Effect.Success<typeof materializedGrpcRuntimeWithConfigClientsEffect>
    >().toMatchTypeOf<
      ViewServerRuntime<typeof materializedGrpcViewServerWithConfigClients.topics>
    >();

    expectTypeOf(invalidGrpcReconnectKey).not.toBeAny();

    expectTypeOf(invalidGrpcReconnectMax).not.toBeAny();

    expectTypeOf(invalidGrpcReconnectDelay).not.toBeAny();

    expectTypeOf(invalidGrpcOptionKey).not.toBeAny();

    expectTypeOf(invalidGrpcClientsOption).not.toBeAny();

    expectTypeOf(invalidGrpcFeedsOption).not.toBeAny();
  });
});
