import { describe, expectTypeOf, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import {
  defineViewServerConfig,
  type ViewServerBackpressureError,
  type ViewServerRuntimeClient,
  type ViewServerRuntimeError,
} from "./index";

import { viewServer } from "../test-harness/live-query";
import { Order } from "../test-harness/schemas";

declare const dynamicRuntimeTopic: "orders" | "trades";

describe("Runtime client and configuration generic contracts", () => {
  it("accepts valid contracts and rejects invalid contracts", () => {
    const assertRuntimeContracts = (runtime: ViewServerRuntimeClient<typeof viewServer.topics>) => {
      const publishEffect = runtime.publish("orders", {
        id: "order-1",
        customerId: "customer-1",
        status: "open",
        price: 42,
        region: "usa",
        updatedAt: 1,
      });
      const snapshotEffect = runtime.snapshot("orders", {
        select: ["id"],
        where: [{ field: "status", type: "equals", filter: "open" }],
      });
      const patchEffect = runtime.patch("orders", "order-1", {
        price: 43,
        status: "closed",
      });

      expectTypeOf<Effect.Error<typeof publishEffect>>().toEqualTypeOf<ViewServerRuntimeError>();
      expectTypeOf<Effect.Error<typeof snapshotEffect>>().toEqualTypeOf<ViewServerRuntimeError>();
      expectTypeOf<Effect.Error<typeof patchEffect>>().toEqualTypeOf<ViewServerRuntimeError>();

      const invalidPublishWrongField = runtime.publish("orders", {
        id: "order-1",
        customerId: "customer-1",
        status: "open",
        price: 42,
        region: "usa",
        // @ts-expect-error publish rows must match the topic schema
        updatedAt: "not-a-number",
      });

      const invalidPublishMissingField = runtime.publish("trades", {
        id: "trade-1",
        symbol: "AAPL",
        quantity: 1,
        price: 42,
        // @ts-expect-error publish rows must include all required topic fields
        updatedAt: 1,
      });

      const invalidPublishTopic = runtime.publish(
        // @ts-expect-error runtime publish topics are constrained to configured topics
        "customers",
        {
          id: "customer-1",
        },
      );

      const invalidPatchField = runtime.patch("orders", "order-1", {
        // @ts-expect-error patch fields must belong to the selected topic row
        missing: true,
      });

      const invalidPatchValue = runtime.patch("orders", "order-1", {
        // @ts-expect-error patch field values must match the selected topic row
        price: "not-a-number",
      });

      const invalidSnapshotTopic = runtime.snapshot(
        // @ts-expect-error snapshot topics are constrained to configured topics
        "customers",
        {},
      );

      const invalidSnapshotFilter = runtime.snapshot("orders", {
        select: ["id"],
        where: [
          // @ts-expect-error filter values must match their Topic Row fields.
          { field: "price", type: "equals", filter: "not-a-number" },
        ],
      });
      const commonDynamicSnapshot = runtime.snapshot(dynamicRuntimeTopic, {
        select: ["id"],
        where: [{ field: "id", type: "equals", filter: "row-1" }],
      });
      const orderOnlyQuery = {
        select: ["id"],
        where: [{ field: "status", type: "equals", filter: "open" }],
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: readonly [
          { readonly field: "status"; readonly type: "equals"; readonly filter: "open" },
        ];
      };
      const invalidDynamicSnapshot = runtime.snapshot(
        dynamicRuntimeTopic,
        // @ts-expect-error dynamic topic-union queries must be valid for every possible topic.
        orderOnlyQuery,
      );
      expectTypeOf<
        Effect.Error<typeof commonDynamicSnapshot>
      >().toEqualTypeOf<ViewServerRuntimeError>();
      expectTypeOf(invalidDynamicSnapshot).not.toBeAny();
      expectTypeOf<
        Effect.Error<typeof invalidPublishWrongField>
      >().toEqualTypeOf<ViewServerRuntimeError>();
      expectTypeOf<
        Effect.Error<typeof invalidPublishMissingField>
      >().toEqualTypeOf<ViewServerRuntimeError>();
      expectTypeOf<
        Effect.Error<typeof invalidPublishTopic>
      >().toEqualTypeOf<ViewServerRuntimeError>();
      expectTypeOf<
        Effect.Error<typeof invalidPatchField>
      >().toEqualTypeOf<ViewServerRuntimeError>();
      expectTypeOf<
        Effect.Error<typeof invalidPatchValue>
      >().toEqualTypeOf<ViewServerRuntimeError>();
      expectTypeOf<
        Effect.Error<typeof invalidSnapshotTopic>
      >().toEqualTypeOf<ViewServerRuntimeError>();
      expectTypeOf<
        Effect.Error<typeof invalidSnapshotFilter>
      >().toEqualTypeOf<ViewServerRuntimeError>();
    };

    expectTypeOf(assertRuntimeContracts).toBeFunction();

    expectTypeOf<ViewServerBackpressureError>().toMatchTypeOf<ViewServerRuntimeError>();

    // @ts-expect-error topic keys must be string fields from the Effect Schema row type
    defineViewServerConfig({
      topics: {
        invalid: {
          schema: Order,
          key: "missing",
        },
      },
    });

    defineViewServerConfig({
      topics: {
        loose: {
          // @ts-expect-error topic schemas must expose concrete fields for query typing and wire validation
          schema: Schema.Record(Schema.String, Schema.String),
          key: "id",
        },
      },
    });

    // @ts-expect-error system health topic names are reserved
    defineViewServerConfig({
      topics: {
        __view_server_health: {
          schema: Order,
          key: "id",
        },
      },
    });
  });
});
