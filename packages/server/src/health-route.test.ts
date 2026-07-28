import { describe, expect, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  type ViewServerHealth,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { viewServerEncodeHealth } from "@effect-view-server/protocol";
import { SourceFixture } from "@effect-view-server/source-adapter-testing";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Deferred, Effect, Schema } from "effect";
import { AtomRef } from "effect/unstable/reactivity";
import { makeViewServerWebSocketServer } from "./index";
import {
  HealthJson,
  Order,
  bearerAuth,
  createServerTestRuntime,
  degradedServerHealth,
  fetchJson,
  fetchJsonWithAuthorization,
  order,
  viewServer,
} from "../test-harness/server";

const SourceAggregateHealthJson = Schema.Struct({
  sources: Schema.Struct({
    orders: Schema.Struct({
      adapter: Schema.Struct({
        name: Schema.String,
        version: Schema.String,
      }),
      metrics: Schema.Struct({
        runtime: Schema.Struct({
          rejectedItemCount: Schema.String,
        }),
        adapter: Schema.Struct({
          observed: Schema.String,
        }),
      }),
      sampledAtNanos: Schema.String,
    }),
  }),
});

describe("Real View Server health route", () => {
  it.live("serves GET /health beside the websocket RPC endpoint", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });
      yield* Effect.addFinalizer(() => server.close);

      yield* inMemory.client.publish("orders", order("a", 10));

      const readyHealth = yield* fetchJson(server.healthUrl);
      const readyBody = yield* Schema.decodeUnknownEffect(HealthJson)(readyHealth.value);
      expect(readyHealth.response.status).toBe(200);
      expect(readyBody.status).toBe("ready");
      expect(readyBody.engine.topics.orders.rowCount).toBe(1);

      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("requires auth for GET /health when an auth validator is configured", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        auth: bearerAuth,
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });
      yield* Effect.addFinalizer(() => server.close);

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
    }).pipe(Effect.scoped),
  );

  it.live("returns 500 when runtime health fails", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
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
      yield* Effect.addFinalizer(() => server.close);

      const health = yield* fetchJson(server.healthUrl);

      expect(health.response.status).toBe(500);
      expect(health.value).toStrictEqual(healthError);

      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("returns 500 when runtime health defects", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          health: () => Effect.die("health defect"),
        },
      });
      yield* Effect.addFinalizer(() => server.close);

      const health = yield* fetchJson(server.healthUrl);

      expect(health.response.status).toBe(500);
      expect(health.value).toMatch(/^Error: health defect(?:\n|$)/);

      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("returns 500 when runtime health is semantically invalid", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const baseHealth = yield* inMemory.client.health();
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          ...inMemory.client,
          health: () =>
            Effect.succeed({
              ...baseHealth,
              engine: {
                ...baseHealth.engine,
                topics: {
                  ...baseHealth.engine.topics,
                  missing: {
                    ...baseHealth.engine.topics.orders,
                  },
                },
              },
            }),
        },
      });
      yield* Effect.addFinalizer(() => server.close);

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
    }).pipe(Effect.scoped),
  );

  it.live("keeps readiness successful for degraded canonical health", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const baseHealth = yield* inMemory.client.health();
      const degradedHealth = degradedServerHealth(baseHealth);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          health: () => Effect.succeed(degradedHealth),
        },
      });
      yield* Effect.addFinalizer(() => server.close);

      const expectedHealth = yield* viewServerEncodeHealth(viewServer, degradedHealth);
      const health = yield* fetchJson(server.healthUrl);

      expect(health.response.status).toBe(200);
      expect(health.value).toStrictEqual(expectedHealth);

      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("returns 503 while runtime health is starting", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const baseHealth = yield* inMemory.client.health();
      const startingHealth: ViewServerHealth<typeof viewServer.topics> = {
        ...baseHealth,
        status: "starting",
      };
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          health: () => Effect.succeed(startingHealth),
        },
      });
      yield* Effect.addFinalizer(() => server.close);

      const expectedHealth = yield* viewServerEncodeHealth(viewServer, startingHealth);
      const health = yield* fetchJson(server.healthUrl);
      expect(health.response.status).toBe(503);
      expect(health.value).toStrictEqual(expectedHealth);

      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("keeps readiness successful after a settled Source rejection degrades health", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Order);
      const sourceConfig = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source: fixture.materializedSource({
              label: "degraded-readiness",
            }),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCoreInternal(sourceConfig, {}).pipe(
        Effect.provide(fixture.layer),
      );
      yield* Effect.addFinalizer(() => runtime.close);
      const server = yield* makeViewServerWebSocketServer(sourceConfig, {
        liveClient: runtime.serverLiveClient,
        runtime: runtime.client,
      });
      yield* Effect.addFinalizer(() => server.close);
      yield* fixture.controls.awaitActive({ _tag: "Materialized" });
      const settled = yield* Deferred.make<void>();
      yield* fixture.controls.reject(
        { _tag: "Materialized" },
        SourceFixture.failure("poison item", "stream"),
        { lane: "readiness", offset: 1n },
        () => Deferred.succeed(settled, undefined).pipe(Effect.asVoid),
      );
      yield* Deferred.await(settled);
      yield* runtime.refreshHealth;

      const health = yield* fetchJson(server.healthUrl);
      const body = yield* Schema.decodeUnknownEffect(HealthJson)(health.value);
      const sourceBody = yield* Schema.decodeUnknownEffect(SourceAggregateHealthJson)(health.value);
      expect(health.response.status).toBe(200);
      expect(body.status).toBe("degraded");
      expect(sourceBody.sources.orders.adapter).toStrictEqual({
        name: "controllable-fixture",
        version: "1",
      });
      expect(sourceBody.sources.orders.metrics.runtime.rejectedItemCount).toBe("1");
      expect(sourceBody.sources.orders.metrics.adapter.observed).toBe("0");
      expect(sourceBody.sources.orders.sampledAtNanos).toMatch(/^[0-9]+$/);

      yield* server.close;
      yield* runtime.close;
    }).pipe(Effect.scoped),
  );

  it.live("serves fresh runtime health for Kubernetes readiness", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const baseHealth = yield* inMemory.client.health();
      const degradedHealth: ViewServerHealth<typeof viewServer.topics> = {
        ...baseHealth,
        status: "degraded",
      };
      const cachedHealth = AtomRef.make<ViewServerHealth<typeof viewServer.topics>>(degradedHealth);
      const liveClient = {
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
      yield* Effect.addFinalizer(() => server.close);

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
    }).pipe(Effect.scoped),
  );
});
