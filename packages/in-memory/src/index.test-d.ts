import { describe, expectTypeOf, it } from "@effect/vitest";
import type { ViewServerLiveSubscription } from "@effect-view-server/client";
import {
  defineViewServerConfig,
  grpc,
  kafka,
  type GrpcRuntimeClients,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import type { Context, Effect } from "effect";
import type { Stream } from "effect";
import { Schema } from "effect";
import { createInMemoryViewServer, makeInMemoryViewServer } from "./index";
import { createInMemoryViewServerTesting, makeInMemoryViewServerTesting } from "./testing";

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

const SourceFailure = Schema.TaggedStruct("InMemorySourceFailure", {
  message: Schema.String,
});
const sourceAdapter = SourceAdapter.make({
  identity: { name: "in-memory-type-source" },
  failure: SourceFailure,
  materialized: {
    metrics: Schema.Struct({ observed: Schema.BigInt }),
    rejectionLocation: Schema.Struct({ offset: Schema.BigInt }),
    definitionOptions: SourceAdapter.definitionOptions<void>(),
  },
  leased: undefined,
});
const canonicalSourceViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: sourceAdapter.materializedSource(undefined),
    },
  },
});

const inMemory = createInMemoryViewServer(viewServer);
const canonicalSourceInMemoryEffect = makeInMemoryViewServer(canonicalSourceViewServer, {});
const canonicalSourceTestingEffect = makeInMemoryViewServerTesting(canonicalSourceViewServer, {});
// @ts-expect-error synchronous in-memory construction cannot provide a Source Adapter service.
const invalidCanonicalSourceInMemory = createInMemoryViewServer(canonicalSourceViewServer);
// @ts-expect-error synchronous in-memory testing construction cannot provide a Source Adapter service.
const invalidCanonicalSourceTesting = createInMemoryViewServerTesting(canonicalSourceViewServer);
const inMemoryWithGroupedAdmissionLimits = createInMemoryViewServer(viewServer, {
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
const leasedInMemory = createInMemoryViewServer(leasedViewServer);
const leasedTestingInMemory = createInMemoryViewServerTesting(leasedViewServer);
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
const materializedGrpcSourceInMemory = createInMemoryViewServer(materializedGrpcSourceViewServer);
const materializedGrpcSourceTestingInMemory = createInMemoryViewServerTesting(
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
const kafkaOwnedInMemory = createInMemoryViewServer(kafkaOwnedViewServer);
const kafkaOwnedTestingInMemory = createInMemoryViewServerTesting(kafkaOwnedViewServer);
const invalidTransportHealthOption = createInMemoryViewServer(viewServer, {
  // @ts-expect-error in-memory does not expose Runtime Core transport adapter hooks.
  transportHealth: () => ({
    activeClients: 0,
    activeStreams: 0,
    activeSubscriptions: 0,
    messagesPerSecond: 0,
    bytesPerSecond: 0,
    queuedMessages: 0,
    queuedBytes: 0,
    droppedClients: 0,
    backpressureEvents: 0,
    reconnects: 0,
    lastError: null,
  }),
});
const invalidGroupedAdmissionLimitKey = createInMemoryViewServer(viewServer, {
  groupedIncrementalAdmissionLimits: {
    // @ts-expect-error grouped admission limit keys are exact.
    maxGroupz: 1,
  },
});
const invalidGroupedAdmissionLimitValue = createInMemoryViewServer(viewServer, {
  groupedIncrementalAdmissionLimits: {
    // @ts-expect-error grouped admission limits must be numeric.
    maxGroups: "1",
  },
});

describe("in-memory type contracts", () => {
  it("preserves runtime and live client topic types", () => {
    const publish = inMemory.client.publish("orders", {
      id: "order-1",
      price: 42,
    });
    const subscription = inMemory.liveClient.subscribe("orders", {
      select: ["id"],
    });
    const kafkaSnapshot = kafkaOwnedInMemory.client.snapshot("orders", {
      select: ["id"],
    });
    const materializedGrpcSnapshot = materializedGrpcSourceInMemory.client.snapshot("orders", {
      select: ["id"],
    });
    const invalidPatch = inMemory.client.patch("orders", "order-1", {
      price: 10,
      // @ts-expect-error patches cannot contain fields outside the topic schema.
      prcie: 10,
    });

    expectTypeOf<Effect.Error<typeof publish>>().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<Effect.Services<typeof canonicalSourceInMemoryEffect>>().toEqualTypeOf<
      Context.Service.Identifier<typeof sourceAdapter.runtimeService>
    >();
    expectTypeOf<Effect.Services<typeof canonicalSourceTestingEffect>>().toEqualTypeOf<
      Context.Service.Identifier<typeof sourceAdapter.runtimeService>
    >();
    expectTypeOf<Effect.Success<typeof subscription>>().toEqualTypeOf<
      ViewServerLiveSubscription<{
        readonly id: string;
      }>
    >();
    expectTypeOf(kafkaSnapshot).not.toBeAny();
    expectTypeOf(materializedGrpcSnapshot).not.toBeAny();
    expectTypeOf(inMemory.liveClient).not.toHaveProperty("subscribeRuntime");
    expectTypeOf(leasedTestingInMemory.serverLiveClient).toHaveProperty("subscribeProtocolQuery");
    expectTypeOf(inMemoryWithGroupedAdmissionLimits.client).toEqualTypeOf<typeof inMemory.client>();
    expectTypeOf(invalidPatch).not.toBeAny();
    expectTypeOf(invalidTransportHealthOption).not.toBeAny();
    expectTypeOf(invalidGroupedAdmissionLimitKey).not.toBeAny();
    expectTypeOf(invalidGroupedAdmissionLimitValue).not.toBeAny();
    expectTypeOf(invalidCanonicalSourceInMemory).not.toBeAny();
    expectTypeOf(invalidCanonicalSourceTesting).not.toBeAny();
  });

  it("rejects leased gRPC topics from public in-memory clients", () => {
    const leasedQuery = {
      where: [{ field: "id", type: "equals", filter: "order-1" }],
      select: ["id"],
    } satisfies {
      readonly where: readonly [
        {
          readonly field: "id";
          readonly type: "equals";
          readonly filter: "order-1";
        },
      ];
      readonly select: readonly ["id"];
    };
    // @ts-expect-error public in-memory clients reject direct leased gRPC snapshots.
    const invalidLeasedSnapshot = leasedInMemory.client.snapshot("orders", leasedQuery);
    // @ts-expect-error public in-memory clients reject direct leased gRPC publishes.
    const invalidLeasedPublish = leasedInMemory.client.publish("orders", {
      id: "order-1",
      price: 42,
    });
    // @ts-expect-error public in-memory clients reject direct leased gRPC batch publishes.
    const invalidLeasedPublishMany = leasedInMemory.client.publishMany("orders", [
      {
        id: "order-1",
        price: 42,
      },
    ]);
    // @ts-expect-error public in-memory clients reject direct leased gRPC patches.
    const invalidLeasedPatch = leasedInMemory.client.patch("orders", "order-1", {
      price: 10,
    });
    // @ts-expect-error public in-memory clients reject direct leased gRPC deletes.
    const invalidLeasedDelete = leasedInMemory.client.delete("orders", "order-1");
    // @ts-expect-error public in-memory clients reject direct leased gRPC reset.
    const invalidLeasedReset = leasedInMemory.client.reset();
    // @ts-expect-error public in-memory live clients reject direct leased gRPC subscriptions.
    const _invalidLeasedSubscribe = leasedInMemory.liveClient.subscribe("orders", leasedQuery);
    // @ts-expect-error source-owned Kafka topics reject direct in-memory publishes.
    const invalidKafkaOwnedPublish = kafkaOwnedInMemory.client.publish("orders", {
      id: "order-1",
      price: 42,
    });
    // @ts-expect-error source-owned Kafka topics reject direct in-memory publishMany.
    const invalidKafkaOwnedPublishMany = kafkaOwnedInMemory.client.publishMany("orders", [
      {
        id: "order-1",
        price: 42,
      },
    ]);
    // @ts-expect-error source-owned Kafka topics reject direct in-memory patches.
    const invalidKafkaOwnedPatch = kafkaOwnedInMemory.client.patch("orders", "order-1", {
      price: 10,
    });
    // @ts-expect-error source-owned Kafka topics reject direct in-memory deletes.
    const invalidKafkaOwnedDelete = kafkaOwnedInMemory.client.delete("orders", "order-1");
    // @ts-expect-error source-owned materialized gRPC topics reject direct in-memory publishes.
    const invalidMaterializedGrpcPublish = materializedGrpcSourceInMemory.client.publish("orders", {
      id: "order-1",
      price: 42,
    });
    const invalidMaterializedGrpcPublishMany = materializedGrpcSourceInMemory.client.publishMany(
      // @ts-expect-error source-owned materialized gRPC topics reject direct in-memory publishMany.
      "orders",
      [
        {
          id: "order-1",
          price: 42,
        },
      ],
    );
    const invalidMaterializedGrpcPatch = materializedGrpcSourceInMemory.client.patch(
      // @ts-expect-error source-owned materialized gRPC topics reject direct in-memory patches.
      "orders",
      "order-1",
      {
        price: 10,
      },
    );
    const invalidMaterializedGrpcDelete = materializedGrpcSourceInMemory.client.delete(
      // @ts-expect-error source-owned materialized gRPC topics reject direct in-memory deletes.
      "orders",
      "order-1",
    );
    // @ts-expect-error source-owned in-memory clients reject direct reset.
    const invalidKafkaOwnedReset = kafkaOwnedInMemory.client.reset();
    // @ts-expect-error source-owned in-memory clients reject direct reset.
    const invalidMaterializedGrpcReset = materializedGrpcSourceInMemory.client.reset();

    expectTypeOf(invalidLeasedSnapshot).not.toBeAny();
    expectTypeOf(invalidLeasedPublish).not.toBeAny();
    expectTypeOf(invalidLeasedPublishMany).not.toBeAny();
    expectTypeOf(invalidLeasedPatch).not.toBeAny();
    expectTypeOf(invalidLeasedDelete).not.toBeAny();
    expectTypeOf(invalidLeasedReset).not.toBeAny();
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

  it("allows leased gRPC topics from testing in-memory clients", () => {
    const leasedQuery = {
      where: [{ field: "id", type: "equals", filter: "order-1" }],
      routeBy: { id: "order-1" },
      select: ["id"],
    } satisfies {
      readonly where: readonly [
        {
          readonly field: "id";
          readonly type: "equals";
          readonly filter: "order-1";
        },
      ];
      readonly routeBy: { readonly id: "order-1" };
      readonly select: readonly ["id"];
    };
    const testingLeasedSubscribe = leasedTestingInMemory.liveClient.subscribe(
      "orders",
      leasedQuery,
    );
    const testingLeasedPublish = leasedTestingInMemory.client.publish("orders", {
      id: "order-1",
      price: 42,
    });
    const testingKafkaOwnedPublish = kafkaOwnedTestingInMemory.client.publish("orders", {
      id: "order-1",
      price: 42,
    });
    const testingMaterializedGrpcPublish = materializedGrpcSourceTestingInMemory.client.publish(
      "orders",
      {
        id: "order-1",
        price: 42,
      },
    );

    expectTypeOf<Effect.Success<typeof testingLeasedSubscribe>>().toEqualTypeOf<
      ViewServerLiveSubscription<{
        readonly id: string;
      }>
    >();
    expectTypeOf<Effect.Success<typeof testingLeasedPublish>>().toEqualTypeOf<void>();
    expectTypeOf<Effect.Success<typeof testingKafkaOwnedPublish>>().toEqualTypeOf<void>();
    expectTypeOf<Effect.Success<typeof testingMaterializedGrpcPublish>>().toEqualTypeOf<void>();
  });
});
