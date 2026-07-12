import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, viewSchema } from "@effect-view-server/config";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Effect, Schedule, Schema } from "effect";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";
import { grpcClients, grpcOrderValue, grpcTopicSources } from "../test-harness/grpc-config";
import {
  captureLeasedGrpcDegradation,
  longRunningGrpcStream,
  makeLeasedGrpcHealth,
} from "../test-harness/grpc-leased";
import { resolveLeasedGrpcRuntimeOptions } from "../test-harness/grpc-materialized";
import {
  makeMaterializedGrpcRuntimeHarness,
  readGrpcHealthOverlayNow,
} from "../test-harness/grpc-runtime";

class GrpcDecodedRow extends Schema.Class<GrpcDecodedRow>("GrpcDecodedRow")({
  id: Schema.String,
  amount: Schema.BigIntFromString,
  region: Schema.String,
}) {}
viewSchema.admitClass(GrpcDecodedRow);

const rowsQuery = () => ({
  select: ["id", "amount", "region"] as const,
  orderBy: [{ field: "id", direction: "asc" }] as const,
  limit: 10,
});

const leasedRowsQuery = (region: string) => ({
  ...rowsQuery(),
  where: { region: { eq: region } },
});

describe("gRPC decoded row validation", () => {
  it.live("accepts a decoded materialized mapper row for a root Class topic", () =>
    Effect.gen(function* () {
      const config = defineViewServerConfig({
        grpc: { clients: grpcClients },
        topics: {
          rows: grpcTopicSources.materialized({
            schema: GrpcDecodedRow,
            key: "id",
            client: "orders",
            method: "streamOrders",
            request: () => ({ orderId: "all" }),
            acquire: () => longRunningGrpcStream([grpcOrderValue("materialized", 42)]),
            map: ({ value }) => ({
              id: value.customerId,
              amount: BigInt(value.price),
              region: "global",
            }),
          }),
        },
      });
      const harness = yield* makeMaterializedGrpcRuntimeHarness({ config });

      const snapshot = yield* harness.runtimeCore.client.snapshot("rows", rowsQuery()).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (current) => current.totalRows === 1,
        }),
      );

      expect(snapshot).toStrictEqual({
        rows: [{ id: "materialized", amount: 42n, region: "global" }],
        status: "ready",
        statusCode: "Ready",
        totalRows: 1,
        version: 1,
      });
      yield* harness.close;
    }),
  );

  it.live("rejects an encoded materialized mapper row", () =>
    Effect.gen(function* () {
      const config = defineViewServerConfig({
        grpc: { clients: grpcClients },
        topics: {
          rows: grpcTopicSources.materialized({
            schema: GrpcDecodedRow,
            key: "id",
            client: "orders",
            method: "streamOrders",
            request: () => ({ orderId: "all" }),
            acquire: () => longRunningGrpcStream([grpcOrderValue("materialized-encoded", 42)]),
            map: ({ value }) => ({
              id: value.customerId,
              // @ts-expect-error gRPC mappers return the decoded bigint, not its wire encoding
              amount: String(value.price),
              region: "global",
            }),
          }),
        },
      });
      const harness = yield* makeMaterializedGrpcRuntimeHarness({ config });

      const degraded = yield* readGrpcHealthOverlayNow(
        harness.runtimeCore.client,
        harness.health,
      ).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (health) => health.grpc?.feeds.rows?.materialized["rows"]?.status === "degraded",
        }),
      );
      const snapshot = yield* harness.runtimeCore.client.snapshot("rows", rowsQuery());

      expect(degraded.grpc?.feeds.rows?.materialized["rows"]?.mappingFailuresPerSecond).toBe(1);
      expect(snapshot).toStrictEqual({
        rows: [],
        status: "ready",
        statusCode: "Ready",
        totalRows: 0,
        version: 0,
      });
      yield* harness.close;
    }),
  );

  it.live("rejects a stateful materialized mapper accessor without reading it", () =>
    Effect.gen(function* () {
      let accessorReads = 0;
      const config = defineViewServerConfig({
        grpc: { clients: grpcClients },
        topics: {
          rows: grpcTopicSources.materialized({
            schema: GrpcDecodedRow,
            key: "id",
            client: "orders",
            method: "streamOrders",
            request: () => ({ orderId: "all" }),
            acquire: () => longRunningGrpcStream([grpcOrderValue("materialized-accessor", 42)]),
            map: ({ value }) => ({
              id: value.customerId,
              get amount() {
                accessorReads += 1;
                return accessorReads === 1 ? 1n : 2n;
              },
              region: "global",
            }),
          }),
        },
      });
      const harness = yield* makeMaterializedGrpcRuntimeHarness({ config });

      const degraded = yield* readGrpcHealthOverlayNow(
        harness.runtimeCore.client,
        harness.health,
      ).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (health) => health.grpc?.feeds.rows?.materialized["rows"]?.status === "degraded",
        }),
      );

      expect(accessorReads).toBe(0);
      expect(degraded.grpc?.feeds.rows?.materialized["rows"]?.mappingFailuresPerSecond).toBe(1);
      yield* harness.close;
    }),
  );

  it.live("accepts a decoded leased mapper row for a root Class topic", () =>
    Effect.gen(function* () {
      const config = defineViewServerConfig({
        grpc: { clients: grpcClients },
        topics: {
          rows: grpcTopicSources.leased({
            schema: GrpcDecodedRow,
            key: "id",
            client: "orders",
            method: "streamOrders",
            routeBy: ["region"],
            request: ({ region }) => ({ orderId: region }),
            acquire: () => longRunningGrpcStream([grpcOrderValue("leased", 42)]),
            map: ({ value, route }) => ({
              id: value.customerId,
              amount: BigInt(value.price),
              region: route.region,
            }),
          }),
        },
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(config);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(config, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        config,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const subscription = yield* manager.liveClient.subscribe("rows", leasedRowsQuery("usa"));

      const snapshot = yield* runtimeCore.internalClient
        .snapshot("rows", leasedRowsQuery("usa"))
        .pipe(
          Effect.repeat({
            schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
            until: (current) => current.totalRows === 1,
          }),
        );

      expect(snapshot).toStrictEqual({
        rows: [{ id: "leased", amount: 42n, region: "usa" }],
        status: "ready",
        statusCode: "Ready",
        totalRows: 1,
        version: 1,
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects an encoded leased mapper row", () =>
    Effect.gen(function* () {
      const config = defineViewServerConfig({
        grpc: { clients: grpcClients },
        topics: {
          rows: grpcTopicSources.leased({
            schema: GrpcDecodedRow,
            key: "id",
            client: "orders",
            method: "streamOrders",
            routeBy: ["region"],
            request: ({ region }) => ({ orderId: region }),
            acquire: () => longRunningGrpcStream([grpcOrderValue("leased-encoded", 42)]),
            map: ({ value, route }) => ({
              id: value.customerId,
              // @ts-expect-error gRPC mappers return the decoded bigint, not its wire encoding
              amount: String(value.price),
              region: route.region,
            }),
          }),
        },
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(config);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(config, {});
      const degradationMessages: Array<string> = [];
      const health = captureLeasedGrpcDegradation(
        makeLeasedGrpcHealth(grpcOptions),
        degradationMessages,
      );
      const manager = yield* makeViewServerGrpcLeaseManager(
        config,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const subscription = yield* manager.liveClient.subscribe("rows", leasedRowsQuery("usa"));

      yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 3_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (current) =>
            degradationMessages.length === 1 &&
            Object.keys(current.grpc?.feeds.rows?.leased ?? {}).length === 0,
        }),
      );
      const snapshot = yield* runtimeCore.internalClient.snapshot("rows", leasedRowsQuery("usa"));

      expect(degradationMessages).toStrictEqual([
        "gRPC leased feed rows failed: ViewServerGrpcIngressError: gRPC leased feed mapping produced an invalid row for rows",
      ]);
      expect(snapshot).toStrictEqual({
        rows: [],
        status: "ready",
        statusCode: "Ready",
        totalRows: 0,
        version: 0,
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );
});
