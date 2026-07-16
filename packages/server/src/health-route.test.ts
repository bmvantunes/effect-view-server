import { describe, expect, it } from "@effect/vitest";
import type { ViewServerRuntimeLiveClient } from "@effect-view-server/client";
import { type ViewServerHealth, type ViewServerRuntimeError } from "@effect-view-server/config";
import { ViewServerHealthSchema } from "@effect-view-server/protocol";
import { Effect, Schema } from "effect";
import { AtomRef } from "effect/unstable/reactivity";
import { makeViewServerWebSocketServer } from "./index";
import {
  HealthJson,
  bearerAuth,
  createServerTestRuntime,
  degradedServerHealth,
  fetchJson,
  fetchJsonWithAuthorization,
  kafkaStartFromHealth,
  order,
  viewServer,
} from "../test-harness/server";

describe("Real View Server health route", () => {
  it.live("serves GET /health beside the websocket RPC endpoint", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });

      yield* inMemory.client.publish("orders", order("a", 10));

      const readyHealth = yield* fetchJson(server.healthUrl);
      const readyBody = yield* Schema.decodeUnknownEffect(HealthJson)(readyHealth.value);
      expect(readyHealth.response.status).toBe(200);
      expect(readyBody.status).toBe("ready");
      expect(readyBody.engine.topics.orders.rowCount).toBe(1);

      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("requires auth for GET /health when an auth validator is configured", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        auth: bearerAuth,
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });

      const deniedHealth = yield* fetchJson(server.healthUrl);
      const acceptedHealth = yield* fetchJsonWithAuthorization(
        server.healthUrl,
        "Bearer view-server-test",
      );
      const acceptedBody = yield* Schema.decodeUnknownEffect(HealthJson)(acceptedHealth.value);

      expect(deniedHealth.response.status).toBe(401);
      expect(deniedHealth.value).toStrictEqual({
        _tag: "ViewServerAuthError",
        message: "Missing or invalid authorization header.",
      });
      expect(acceptedHealth.response.status).toBe(200);
      expect(acceptedBody.status).toBe("ready");

      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("returns 500 when runtime health fails", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const healthError: ViewServerRuntimeError = {
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        message: "health unavailable",
      };
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          health: () => Effect.fail(healthError),
        },
      });

      const health = yield* fetchJson(server.healthUrl);

      expect(health.response.status).toBe(500);
      expect(health.value).toStrictEqual(healthError);

      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("returns 500 when runtime health defects", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          health: () => Effect.die("health defect"),
        },
      });

      const health = yield* fetchJson(server.healthUrl);

      expect(health.response.status).toBe(500);
      expect(health.value).toContain("Error: health defect");

      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("returns 500 when runtime health is semantically invalid", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const baseHealth = yield* inMemory.client.health();
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          ...inMemory.client,
          health: () =>
            Effect.succeed({
              ...baseHealth,
              kafka: {
                startFrom: kafkaStartFromHealth,
                regions: {},
                topics: {
                  source_orders: {
                    status: "ready",
                    sourceTopic: "source_orders",
                    viewServerTopic: "missing",
                    regions: {},
                  },
                },
              },
            }),
        },
      });

      const health = yield* fetchJson(server.healthUrl);

      expect(health.response.status).toBe(500);
      expect(health.value).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidRow",
        message: "Health payload references unknown topic: missing",
        topic: "missing",
      });

      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("returns 503 for degraded health and serializes bigint fields", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const baseHealth = yield* inMemory.client.health();
      const degradedHealth = degradedServerHealth(baseHealth);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          health: () => Effect.succeed(degradedHealth),
        },
      });

      const expectedHealth =
        yield* Schema.encodeUnknownEffect(ViewServerHealthSchema)(degradedHealth);
      const health = yield* fetchJson(server.healthUrl);

      expect(health.response.status).toBe(503);
      expect(health.value).toStrictEqual(expectedHealth);

      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("serves fresh runtime health for Kubernetes readiness", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const baseHealth = yield* inMemory.client.health();
      const degradedHealth: ViewServerHealth<typeof viewServer.topics> = {
        ...baseHealth,
        status: "degraded",
        kafka: {
          startFrom: kafkaStartFromHealth,
          regions: {},
          topics: {
            source_orders: {
              status: "ready",
              sourceTopic: "source_orders",
              viewServerTopic: "orders",
              regions: {
                usa: {
                  connected: true,
                  assignedPartitions: 1,
                  messagesPerSecond: 0,
                  bytesPerSecond: 0,
                  decodedMessagesPerSecond: 0,
                  decodeFailuresPerSecond: 0,
                  mappingFailuresPerSecond: 0,
                  publishFailuresPerSecond: 0,
                  commitFailuresPerSecond: 0,
                  processingFailuresPerSecond: 0,
                  lastMessageAt: null,
                  lastCommitAt: null,
                  consumerLagMessages: 42n,
                  lagSampledAt: null,
                  committedOffset: null,
                  lastError: null,
                },
              },
            },
          },
        },
      };
      const cachedHealth = AtomRef.make<ViewServerHealth<typeof viewServer.topics>>(degradedHealth);
      const liveClient: ViewServerRuntimeLiveClient<typeof viewServer.topics> = {
        ...inMemory.liveClient,
        health: cachedHealth,
      };
      let runtimeHealthCalls = 0;
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient,
        runtime: {
          health: () =>
            Effect.sync(() => {
              runtimeHealthCalls += 1;
              return baseHealth;
            }),
        },
      });

      const firstHealth = yield* fetchJson(server.healthUrl);
      const firstBody = yield* Schema.decodeUnknownEffect(HealthJson)(firstHealth.value);
      expect(firstHealth.response.status).toBe(200);
      expect(firstBody.status).toBe("ready");
      expect(runtimeHealthCalls).toBe(1);

      yield* Effect.sync(() => {
        cachedHealth.set(baseHealth);
      });
      const secondHealth = yield* fetchJson(server.healthUrl);
      const secondBody = yield* Schema.decodeUnknownEffect(HealthJson)(secondHealth.value);
      expect(secondHealth.response.status).toBe(200);
      expect(secondBody.status).toBe("ready");
      expect(runtimeHealthCalls).toBe(2);

      yield* server.close;
      yield* inMemory.close;
    }),
  );
});
