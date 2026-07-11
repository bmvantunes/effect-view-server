import { describe, expectTypeOf, it } from "@effect/vitest";
import type { ViewServerLiveSubscription } from "@effect-view-server/client";
import {
  defineViewServerConfig,
  grpc,
  kafka,
  type GrpcRuntimeClients,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import type { Effect } from "effect";
import type { Stream } from "effect";
import { Schema } from "effect";
import { createViewServerRuntimeCore, makeViewServerRuntimeCore } from "./index";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

declare const grpcRuntimeClients: GrpcRuntimeClients;
declare const grpcRuntimeStream: Stream.Stream<unknown, unknown, never>;

const grpcTopicSources = grpc.topicSources(grpcRuntimeClients);

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

const runtimeCore = createViewServerRuntimeCore(viewServer);
const runtimeCoreEffect = makeViewServerRuntimeCore(viewServer, {});
const runtimeCoreWithGroupedAdmissionLimits = createViewServerRuntimeCore(viewServer, {
  groupedIncrementalAdmissionLimits: {
    maxGroups: 1,
  },
});
const leasedViewServer = defineViewServerConfig({
  grpc: {
    clients: grpcRuntimeClients,
  },
  topics: {
    orders: grpcTopicSources.leased({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      routeBy: ["id"],
      request: ({ id }) => ({ id }),
      acquire: () => grpcRuntimeStream,
      map: ({ route }) => ({
        id: route.id,
        price: 0,
      }),
    }),
  },
});
const leasedRuntimeCore = createViewServerRuntimeCore(leasedViewServer);
const leasedGrpcSourceViewServer = defineViewServerConfig({
  grpc: {
    clients: grpcRuntimeClients,
  },
  topics: {
    orders: grpcTopicSources.leased({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      routeBy: ["id"],
      request: ({ id }) => ({ id }),
      acquire: () => grpcRuntimeStream,
      map: ({ route }) => ({
        id: route.id,
        price: 0,
      }),
    }),
  },
});
const leasedGrpcSourceRuntimeCore = createViewServerRuntimeCore(leasedGrpcSourceViewServer);
const materializedGrpcSourceViewServer = defineViewServerConfig({
  grpc: {
    clients: grpcRuntimeClients,
  },
  topics: {
    orders: grpcTopicSources.materialized({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      request: () => ({}),
      acquire: () => grpcRuntimeStream,
      map: () => ({
        id: "order-1",
        price: 0,
      }),
    }),
  },
});
const materializedGrpcSourceRuntimeCore = createViewServerRuntimeCore(
  materializedGrpcSourceViewServer,
);
const kafkaOwnedViewServer = defineViewServerConfig({
  kafka: {
    usa: "localhost:9092",
  },
  topics: {
    orders: {
      schema: Order,
      key: "id",
      kafkaSource: kafka.source({
        topic: "orders-source",
        regions: ["usa"],
        value: kafka.json(() => Schema.toCodecJson(Order)),
        rowKey: ({ key }) => key,
        map: ({ value }) => ({
          price: value.price,
        }),
      }),
    },
  },
});
const kafkaOwnedRuntimeCore = createViewServerRuntimeCore(kafkaOwnedViewServer);

describe("runtime-core type contracts", () => {
  it("preserves runtime and live client topic types", () => {
    const publish = runtimeCore.client.publish("orders", {
      id: "order-1",
      price: 42,
    });
    const subscription = runtimeCore.liveClient.subscribe("orders", {
      select: ["id"],
    });
    const kafkaSnapshot = kafkaOwnedRuntimeCore.client.snapshot("orders", {
      select: ["id"],
    });
    const materializedGrpcSnapshot = materializedGrpcSourceRuntimeCore.client.snapshot("orders", {
      select: ["id"],
    });
    const materializedGrpcSubscribe = materializedGrpcSourceRuntimeCore.liveClient.subscribe(
      "orders",
      {
        select: ["id"],
      },
    );
    const invalidPatch = runtimeCore.client.patch("orders", "order-1", {
      price: 10,
      // @ts-expect-error patches cannot contain fields outside the topic schema.
      prcie: 10,
    });
    const runtimeCoreWithTransportHealth = createViewServerRuntimeCore(viewServer, {
      transportHealth: (health) => {
        expectTypeOf(health.topics.orders.rowCount).toEqualTypeOf<number>();
        return {
          activeClients: 1,
          activeStreams: health.activeSubscriptions,
          activeSubscriptions: health.activeSubscriptions,
          messagesPerSecond: 0,
          bytesPerSecond: 0,
          queuedMessages: health.queuedEvents,
          queuedBytes: 0,
          droppedClients: 0,
          backpressureEvents: health.backpressureEvents,
          reconnects: 0,
          lastError: null,
        };
      },
    });
    const invalidGroupedAdmissionLimitKey = createViewServerRuntimeCore(viewServer, {
      groupedIncrementalAdmissionLimits: {
        // @ts-expect-error grouped admission limit keys are exact.
        maxGroupz: 1,
      },
    });
    const invalidGroupedAdmissionLimitValue = createViewServerRuntimeCore(viewServer, {
      groupedIncrementalAdmissionLimits: {
        // @ts-expect-error grouped admission limits must be numeric.
        maxGroups: "1",
      },
    });
    // @ts-expect-error public runtime-core instances must not expose route-bypassing internals.
    const _internalLiveClient = runtimeCore.internalLiveClient;
    type _InternalLiveClientFromMake = Effect.Success<
      typeof runtimeCoreEffect
      // @ts-expect-error public runtime-core factory success must not expose route-bypassing internals.
    >["internalLiveClient"];

    expectTypeOf<Effect.Error<typeof publish>>().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<Effect.Success<typeof runtimeCoreEffect>>().toEqualTypeOf<typeof runtimeCore>();
    expectTypeOf<Effect.Success<typeof subscription>>().toEqualTypeOf<
      ViewServerLiveSubscription<{
        readonly id: string;
      }>
    >();
    expectTypeOf(kafkaSnapshot).not.toBeAny();
    expectTypeOf(materializedGrpcSnapshot).not.toBeAny();
    expectTypeOf<Effect.Success<typeof materializedGrpcSubscribe>>().toEqualTypeOf<
      Effect.Success<typeof subscription>
    >();
    expectTypeOf(invalidPatch).not.toBeAny();
    expectTypeOf(runtimeCoreWithTransportHealth.client).toEqualTypeOf<typeof runtimeCore.client>();
    expectTypeOf(runtimeCoreWithGroupedAdmissionLimits.client).toEqualTypeOf<
      typeof runtimeCore.client
    >();
    expectTypeOf(invalidGroupedAdmissionLimitKey).not.toBeAny();
    expectTypeOf(invalidGroupedAdmissionLimitValue).not.toBeAny();
  });

  it("rejects leased gRPC topics from public runtime-core clients", () => {
    const leasedQuery = {
      where: {
        id: { eq: "order-1" },
      },
      select: ["id"],
    } satisfies {
      readonly where: {
        readonly id: {
          readonly eq: "order-1";
        };
      };
      readonly select: readonly ["id"];
    };
    // @ts-expect-error public runtime-core clients reject direct leased gRPC snapshots.
    const invalidLeasedSnapshot = leasedRuntimeCore.client.snapshot("orders", leasedQuery);
    // @ts-expect-error public runtime-core clients reject direct leased gRPC publishes.
    const invalidLeasedPublish = leasedRuntimeCore.client.publish("orders", {
      id: "order-1",
      price: 42,
    });
    // @ts-expect-error public runtime-core clients reject direct leased gRPC batch publishes.
    const invalidLeasedPublishMany = leasedRuntimeCore.client.publishMany("orders", [
      {
        id: "order-1",
        price: 42,
      },
    ]);
    // @ts-expect-error public runtime-core clients reject direct leased gRPC patches.
    const invalidLeasedPatch = leasedRuntimeCore.client.patch("orders", "order-1", {
      price: 10,
    });
    // @ts-expect-error public runtime-core clients reject direct leased gRPC deletes.
    const invalidLeasedDelete = leasedRuntimeCore.client.delete("orders", "order-1");
    // @ts-expect-error public runtime-core clients reject direct leased gRPC reset.
    const invalidLeasedReset = leasedRuntimeCore.client.reset();
    // @ts-expect-error public runtime-core live clients reject direct leased gRPC subscriptions.
    const _invalidLeasedSubscribe = leasedRuntimeCore.liveClient.subscribe("orders", leasedQuery);
    const invalidGrpcSourceLeasedSnapshot = leasedGrpcSourceRuntimeCore.client.snapshot(
      // @ts-expect-error public runtime-core clients reject direct grpcSource leased gRPC snapshots.
      "orders",
      leasedQuery,
    );
    const invalidGrpcSourceLeasedSubscribe = leasedGrpcSourceRuntimeCore.liveClient.subscribe(
      // @ts-expect-error public runtime-core live clients reject direct grpcSource leased gRPC subscriptions.
      "orders",
      leasedQuery,
    );
    // @ts-expect-error source-owned Kafka topics reject direct runtime-core publishes.
    const invalidKafkaOwnedPublish = kafkaOwnedRuntimeCore.client.publish("orders", {
      id: "order-1",
      price: 42,
    });
    // @ts-expect-error source-owned Kafka topics reject direct runtime-core publishMany.
    const invalidKafkaOwnedPublishMany = kafkaOwnedRuntimeCore.client.publishMany("orders", [
      {
        id: "order-1",
        price: 42,
      },
    ]);
    // @ts-expect-error source-owned Kafka topics reject direct runtime-core patches.
    const invalidKafkaOwnedPatch = kafkaOwnedRuntimeCore.client.patch("orders", "order-1", {
      price: 10,
    });
    // @ts-expect-error source-owned Kafka topics reject direct runtime-core deletes.
    const invalidKafkaOwnedDelete = kafkaOwnedRuntimeCore.client.delete("orders", "order-1");
    const invalidMaterializedGrpcPublish = materializedGrpcSourceRuntimeCore.client.publish(
      // @ts-expect-error source-owned materialized gRPC topics reject direct runtime-core publishes.
      "orders",
      {
        id: "order-1",
        price: 42,
      },
    );
    const invalidMaterializedGrpcPublishMany = materializedGrpcSourceRuntimeCore.client.publishMany(
      // @ts-expect-error source-owned materialized gRPC topics reject direct runtime-core publishMany.
      "orders",
      [
        {
          id: "order-1",
          price: 42,
        },
      ],
    );
    const invalidMaterializedGrpcPatch = materializedGrpcSourceRuntimeCore.client.patch(
      // @ts-expect-error source-owned materialized gRPC topics reject direct runtime-core patches.
      "orders",
      "order-1",
      {
        price: 10,
      },
    );
    const invalidMaterializedGrpcDelete = materializedGrpcSourceRuntimeCore.client.delete(
      // @ts-expect-error source-owned materialized gRPC topics reject direct runtime-core deletes.
      "orders",
      "order-1",
    );
    // @ts-expect-error source-owned runtime-core clients reject direct reset.
    const invalidKafkaOwnedReset = kafkaOwnedRuntimeCore.client.reset();
    // @ts-expect-error source-owned runtime-core clients reject direct reset.
    const invalidMaterializedGrpcReset = materializedGrpcSourceRuntimeCore.client.reset();
    const _invalidLeasedServerSubscribe = leasedRuntimeCore.serverLiveClient.subscribe(
      // @ts-expect-error public runtime-core server live clients reject direct leased gRPC subscriptions.
      "orders",
      leasedQuery,
    );
    const _invalidLeasedServerRuntimeSubscribe =
      leasedRuntimeCore.serverLiveClient.subscribeRuntime(
        // @ts-expect-error public runtime-core server live clients reject direct leased gRPC runtime subscriptions.
        "orders",
        leasedQuery,
      );

    expectTypeOf(invalidLeasedSnapshot).not.toBeAny();
    expectTypeOf(invalidLeasedPublish).not.toBeAny();
    expectTypeOf(invalidLeasedPublishMany).not.toBeAny();
    expectTypeOf(invalidLeasedPatch).not.toBeAny();
    expectTypeOf(invalidLeasedDelete).not.toBeAny();
    expectTypeOf(invalidLeasedReset).not.toBeAny();
    expectTypeOf(invalidGrpcSourceLeasedSnapshot).not.toBeAny();
    expectTypeOf(invalidGrpcSourceLeasedSubscribe).not.toBeAny();
    expectTypeOf(invalidKafkaOwnedPublish).not.toBeAny();
    expectTypeOf(invalidKafkaOwnedPublishMany).not.toBeAny();
    expectTypeOf(invalidKafkaOwnedPatch).not.toBeAny();
    expectTypeOf(invalidKafkaOwnedDelete).not.toBeAny();
    expectTypeOf(invalidMaterializedGrpcPublish).not.toBeAny();
    expectTypeOf(invalidMaterializedGrpcPublishMany).not.toBeAny();
    expectTypeOf(invalidMaterializedGrpcPatch).not.toBeAny();
    expectTypeOf(invalidMaterializedGrpcDelete).not.toBeAny();
    expectTypeOf(invalidKafkaOwnedReset).not.toBeAny();
    expectTypeOf(invalidMaterializedGrpcReset).not.toBeAny();
  });
});
