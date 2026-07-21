import { describe, expectTypeOf, it } from "@effect/vitest";
import type { GrpcLeasedTopicSource, TopicDefinition } from "@effect-view-server/config";
import type { Effect } from "effect";
import { Schema } from "effect";
import type { ViewServerLiveSubscription, ViewServerRuntimeLiveClient } from "./index";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

type OrdinaryTopics = {
  readonly orders: TopicDefinition<typeof Order, "id">;
};

type LeasedTopics = {
  readonly orders: TopicDefinition<typeof Order, "id"> & {
    readonly grpcSource: GrpcLeasedTopicSource<readonly ["id"]>;
  };
};

declare const leasedRuntimeClient: ViewServerRuntimeLiveClient<LeasedTopics>;
declare const ordinaryRuntimeClient: ViewServerRuntimeLiveClient<OrdinaryTopics>;

describe("runtime live client type contracts", () => {
  it("enforces exact leased route ownership", () => {
    const rawSubscription = leasedRuntimeClient.subscribeRuntime("orders", {
      routeBy: { id: "Order-Á" },
      select: ["id", "price"],
      where: [{ field: "price", type: "greaterThan", filter: 10 }],
    });
    const groupedSubscription = leasedRuntimeClient.subscribeRuntime("orders", {
      routeBy: { id: "Order-Á" },
      groupBy: ["id"],
      aggregates: { rowCount: { aggFunc: "count" } },
    });
    // @ts-expect-error runtime subscriptions to leased topics require routeBy.
    const missingRoute = leasedRuntimeClient.subscribeRuntime("orders", { select: ["id"] });
    // @ts-expect-error runtime route values must match their configured fields.
    const wrongRouteValue = leasedRuntimeClient.subscribeRuntime("orders", {
      routeBy: { id: 1 },
      select: ["id"],
    });
    // @ts-expect-error runtime route objects must contain all and only configured fields.
    const extraRouteField = leasedRuntimeClient.subscribeRuntime("orders", {
      routeBy: {
        id: "Order-Á",
        price: 10,
      },
      select: ["id"],
    });
    // @ts-expect-error ordinary topics reject routeBy and therefore the whole query.
    const ordinaryRoute = ordinaryRuntimeClient.subscribeRuntime("orders", {
      routeBy: { id: "Order-Á" },
      select: ["id"],
    });

    expectTypeOf<Effect.Success<typeof rawSubscription>>().toEqualTypeOf<
      ViewServerLiveSubscription<object>
    >();
    expectTypeOf(groupedSubscription).not.toBeAny();
    expectTypeOf(missingRoute).not.toBeAny();
    expectTypeOf(wrongRouteValue).not.toBeAny();
    expectTypeOf(extraRouteField).not.toBeAny();
    expectTypeOf(ordinaryRoute).not.toBeAny();
  });
});
