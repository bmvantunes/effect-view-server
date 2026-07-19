import { describe, expectTypeOf, it } from "@effect/vitest";
import { type ViewServerRuntimeError } from "@effect-view-server/config";
import type { Config } from "effect";
import { Effect } from "effect";
import type { HttpServerError } from "effect/unstable/http";
import {
  type ViewServerRuntime,
  type ViewServerGrpcIngressError,
  type ViewServerKafkaIngressError,
  type ViewServerTcpPublishIngressError,
} from "./index";

import {
  kafkaOwnedRuntime,
  leasedRuntime,
  materializedGrpcRuntime,
  multiMaterializedGrpcViewServer,
  runEffect,
  runtime,
  runtimeEffect,
  runtimeWithAuth,
  runtimeWithGroupedAdmissionLimits,
  viewServer,
} from "../test-harness/runtime-type-contracts";

type MultiGrpcSourceVisible = typeof multiMaterializedGrpcViewServer.topics.orders extends {
  readonly grpcSource: object;
}
  ? true
  : false;

describe("Runtime client and source ownership contracts", () => {
  it("accepts valid contracts and rejects invalid contracts", () => {
    expectTypeOf<MultiGrpcSourceVisible>().toEqualTypeOf<true>();

    expectTypeOf(runtime.url).toEqualTypeOf<ViewServerRuntime<typeof viewServer.topics>["url"]>();

    expectTypeOf(runtime.healthUrl).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["healthUrl"]
    >();

    expectTypeOf(runtime.metricsUrl).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["metricsUrl"]
    >();

    expectTypeOf(runtime.tcpPublishUrl).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["tcpPublishUrl"]
    >();

    expectTypeOf(runtime.health).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["health"]
    >();

    expectTypeOf(runtime.close).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["close"]
    >();

    expectTypeOf<Effect.Services<typeof runtimeEffect>>().toEqualTypeOf<never>();

    expectTypeOf<Effect.Success<typeof runEffect>>().toEqualTypeOf<never>();

    expectTypeOf<Effect.Services<typeof runEffect>>().toEqualTypeOf<never>();

    expectTypeOf<Effect.Error<typeof runEffect>>().toEqualTypeOf<
      | HttpServerError.ServeError
      | Config.ConfigError
      | ViewServerRuntimeError
      | ViewServerKafkaIngressError
      | ViewServerGrpcIngressError
      | ViewServerTcpPublishIngressError
    >();

    expectTypeOf<Effect.Success<typeof runtimeWithGroupedAdmissionLimits>>().toMatchTypeOf<
      ViewServerRuntime<typeof viewServer.topics>
    >();

    expectTypeOf<Effect.Success<typeof runtimeWithAuth>>().toMatchTypeOf<
      ViewServerRuntime<typeof viewServer.topics>
    >();

    const publish = runtime.client.publish("orders", {
      id: "order-1",
      price: 10,
    });

    expectTypeOf<Parameters<typeof runtime.client.publish>>().toEqualTypeOf<
      Parameters<ViewServerRuntime<typeof viewServer.topics>["client"]["publish"]>
    >();

    const subscribe = runtime.liveClient.subscribe("orders", {
      select: ["id", "price"],
    });

    const leasedSubscribe = leasedRuntime.liveClient.subscribe("orders", {
      routeBy: { id: "order-1" },
      where: [{ field: "id", type: "equals", filter: "order-1" }],
      select: ["id"],
    });

    const kafkaOwnedSnapshot = kafkaOwnedRuntime.client.snapshot("orders", {
      select: ["id", "price"],
    });

    const materializedGrpcSnapshot = materializedGrpcRuntime.client.snapshot("orders", {
      select: ["id", "price"],
    });

    expectTypeOf<Effect.Error<typeof publish>>().toEqualTypeOf<ViewServerRuntimeError>();

    expectTypeOf(subscribe).not.toBeAny();

    expectTypeOf(leasedSubscribe).not.toBeAny();

    expectTypeOf(kafkaOwnedSnapshot).not.toBeAny();

    expectTypeOf(materializedGrpcSnapshot).not.toBeAny();

    const missingRouteQuery = {
      select: ["id"],
    } satisfies {
      readonly select: readonly ["id"];
    };

    // @ts-expect-error leased gRPC snapshots are live-subscription-only.
    const invalidLeasedSnapshot = leasedRuntime.client.snapshot("orders", missingRouteQuery);

    // @ts-expect-error leased gRPC topics reject direct runtime publishes.
    const invalidLeasedPublish = leasedRuntime.client.publish("orders", {
      id: "order-1",
      price: 10,
    });

    // @ts-expect-error leased gRPC topics reject direct runtime batch publishes.
    const invalidLeasedPublishMany = leasedRuntime.client.publishMany("orders", [
      {
        id: "order-1",
        price: 10,
      },
    ]);

    // @ts-expect-error leased gRPC topics reject direct runtime patches.
    const invalidLeasedPatch = leasedRuntime.client.patch("orders", "order-1", {
      price: 10,
    });

    // @ts-expect-error leased gRPC topics reject direct runtime deletes.
    const invalidLeasedDelete = leasedRuntime.client.delete("orders", "order-1");

    // @ts-expect-error leased gRPC runtimes reject direct runtime reset.
    const _invalidLeasedReset = leasedRuntime.client.reset();

    // @ts-expect-error Kafka-owned topics reject direct runtime publishes.
    const invalidKafkaOwnedPublish = kafkaOwnedRuntime.client.publish("orders", {
      id: "order-1",
      price: 10,
    });

    // @ts-expect-error Kafka-owned topics reject direct runtime patches.
    const invalidKafkaOwnedPatch = kafkaOwnedRuntime.client.patch("orders", "order-1", {
      price: 11,
    });

    // @ts-expect-error Kafka-owned topics reject direct runtime deletes.
    const invalidKafkaOwnedDelete = kafkaOwnedRuntime.client.delete("orders", "order-1");

    // @ts-expect-error source-owned runtimes reject direct runtime reset.
    const invalidKafkaOwnedReset = kafkaOwnedRuntime.client.reset();

    // @ts-expect-error materialized gRPC-owned topics reject direct runtime publishes.
    const invalidMaterializedGrpcPublish = materializedGrpcRuntime.client.publish("orders", {
      id: "order-1",
      price: 10,
    });

    const invalidLeasedSubscribe = leasedRuntime.liveClient.subscribe(
      "orders",
      // @ts-expect-error leased gRPC topics require routeBy.
      missingRouteQuery,
    );

    expectTypeOf(invalidLeasedSnapshot).not.toBeAny();

    expectTypeOf(invalidLeasedPublish).not.toBeAny();

    expectTypeOf(invalidLeasedPublishMany).not.toBeAny();

    expectTypeOf(invalidLeasedPatch).not.toBeAny();

    expectTypeOf(invalidLeasedDelete).not.toBeAny();

    expectTypeOf(invalidKafkaOwnedPublish).not.toBeAny();

    expectTypeOf(invalidKafkaOwnedPatch).not.toBeAny();

    expectTypeOf(invalidKafkaOwnedDelete).not.toBeAny();

    expectTypeOf(invalidKafkaOwnedReset).not.toBeAny();

    expectTypeOf(invalidMaterializedGrpcPublish).not.toBeAny();

    expectTypeOf(invalidLeasedSubscribe).not.toBeAny();

    expectTypeOf(runtime.client.reset).not.toBeAny();

    const invalidPublish = runtime.client.publish("orders", {
      id: "order-1",
      price: 10,
      // @ts-expect-error runtime mutation client rejects fields outside the topic row.
      prcie: 10,
    });

    const invalidSubscribe = runtime.liveClient.subscribe("orders", {
      // @ts-expect-error runtime live client rejects fields outside the topic row.
      select: ["prcie"],
    });

    const invalidTopicPublish = runtime.client.publish(
      // @ts-expect-error runtime mutation client rejects unknown topics.
      "missing",
      {
        id: "order-1",
        price: 10,
      },
    );

    const invalidSnapshot = runtime.client.snapshot("orders", {
      select: ["id"],
      where: [
        // @ts-expect-error filters reject fields outside the Topic Row.
        { field: "prcie", type: "greaterThanOrEqual", filter: 10 },
      ],
    });

    expectTypeOf(invalidPublish).not.toBeAny();

    expectTypeOf(invalidSubscribe).not.toBeAny();

    expectTypeOf(invalidTopicPublish).not.toBeAny();

    expectTypeOf(invalidSnapshot).not.toBeAny();
  });
});
