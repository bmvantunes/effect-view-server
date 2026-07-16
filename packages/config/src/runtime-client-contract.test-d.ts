import { describe, expectTypeOf, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import {
  defineViewServerConfig,
  type ViewServerBackpressureError,
  type ViewServerRuntimeClient,
  type ViewServerRuntimeError,
} from "./index";
import {
  type ViewServerRuntimeDecodedMutationClient,
  type ViewServerRuntimeTopicDefinitions,
  viewServerRuntimeDecodedMutationTrust,
} from "./internal";

import { viewServer } from "../test-harness/live-query";
import { Order, Trade } from "../test-harness/schemas";

type OrderRow = typeof Order.Type;
type LimitOrderRow = OrderRow & {
  readonly execution: "limit";
  readonly venue: string;
};
type MarketOrderRow = OrderRow & {
  readonly execution: "market";
  readonly liquidityTaking: boolean;
};
type UnionOrderTopics = {
  readonly unionOrders: {
    readonly schema: typeof Order & {
      readonly Type: LimitOrderRow | MarketOrderRow;
    };
    readonly key: "id";
  };
};

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
        where: {
          status: "open",
        },
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
        // @ts-expect-error invalid query collapse keeps selected fields from being accepted
        select: ["id"],
        where: {
          // @ts-expect-error snapshot filters must use values from the selected topic row
          price: "not-a-number",
        },
      });
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

    const assertGenericDecodedMutation = <const Topics extends ViewServerRuntimeTopicDefinitions>(
      runtime: ViewServerRuntimeDecodedMutationClient<Topics>,
      topic: Extract<keyof Topics, string>,
      row: Topics[Extract<keyof Topics, string>]["schema"]["Type"],
      rows: ReadonlyArray<Topics[Extract<keyof Topics, string>]["schema"]["Type"]>,
      patch: Partial<Topics[Extract<keyof Topics, string>]["schema"]["Type"]>,
    ) => {
      const untrustedPublishEffect = runtime.execute({
        _tag: "PublishDecodedRows",
        topic,
        // @ts-expect-error outer-generic decoded rows require the internal trust capability
        rows: [row],
      });
      const untrustedPublishManyEffect = runtime.execute({
        _tag: "PublishDecodedRows",
        topic,
        // @ts-expect-error outer-generic decoded row arrays require the internal trust capability
        rows,
      });
      const untrustedPatchEffect = runtime.execute({
        _tag: "PatchDecodedFields",
        topic,
        key: "row-1",
        // @ts-expect-error outer-generic decoded patches require the internal trust capability
        patch,
      });
      const trustedPublishEffect = runtime.execute(
        {
          _tag: "PublishDecodedRows",
          topic,
          rows: [row],
        },
        viewServerRuntimeDecodedMutationTrust,
      );
      const trustedPublishManyEffect = runtime.execute(
        {
          _tag: "PublishDecodedRows",
          topic,
          rows,
        },
        viewServerRuntimeDecodedMutationTrust,
      );
      const trustedPatchEffect = runtime.execute(
        {
          _tag: "PatchDecodedFields",
          topic,
          key: "row-1",
          patch,
        },
        viewServerRuntimeDecodedMutationTrust,
      );

      expectTypeOf(untrustedPublishEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(untrustedPublishManyEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(untrustedPatchEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(trustedPublishEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(trustedPublishManyEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(trustedPatchEffect).toMatchTypeOf<Effect.Effect<void, ViewServerRuntimeError>>();
    };

    expectTypeOf(assertGenericDecodedMutation).toBeFunction();

    const assertUnionRowBranchExactness = (
      runtime: ViewServerRuntimeDecodedMutationClient<UnionOrderTopics>,
      limitOrder: LimitOrderRow,
      marketOrder: MarketOrderRow,
      limitPatch: Partial<LimitOrderRow>,
      marketPatch: Partial<MarketOrderRow>,
      limitOrBlendedRow: LimitOrderRow | (LimitOrderRow & { readonly liquidityTaking: true }),
      limitOrBlendedPatch:
        | Partial<LimitOrderRow>
        | (Partial<LimitOrderRow> & { readonly liquidityTaking: true }),
    ) => {
      const validLimitRowEffect = runtime.execute({
        _tag: "PublishDecodedRows",
        topic: "unionOrders",
        rows: [limitOrder],
      });
      const validMarketRowEffect = runtime.execute({
        _tag: "PublishDecodedRows",
        topic: "unionOrders",
        rows: [marketOrder],
      });
      const validLimitPatchEffect = runtime.execute({
        _tag: "PatchDecodedFields",
        topic: "unionOrders",
        key: "order-1",
        patch: limitPatch,
      });
      const validMarketPatchEffect = runtime.execute({
        _tag: "PatchDecodedFields",
        topic: "unionOrders",
        key: "order-1",
        patch: marketPatch,
      });
      const invalidBlendedRowEffect = runtime.execute({
        _tag: "PublishDecodedRows",
        topic: "unionOrders",
        // @ts-expect-error decoded rows cannot blend fields from separate schema union branches
        rows: [
          {
            ...limitOrder,
            liquidityTaking: true,
          },
        ],
      });
      const invalidBlendedPatchEffect = runtime.execute({
        _tag: "PatchDecodedFields",
        topic: "unionOrders",
        key: "order-1",
        // @ts-expect-error decoded patches cannot blend fields from separate schema union branches
        patch: {
          venue: "LSE",
          liquidityTaking: true,
        },
      });
      const invalidUnionBlendedRowEffect = runtime.execute({
        _tag: "PublishDecodedRows",
        topic: "unionOrders",
        // @ts-expect-error a valid branch union cannot hide a blended decoded row member
        rows: [limitOrBlendedRow],
      });
      const invalidUnionBlendedPatchEffect = runtime.execute({
        _tag: "PatchDecodedFields",
        topic: "unionOrders",
        key: "order-1",
        // @ts-expect-error a valid branch union cannot hide a blended decoded patch member
        patch: limitOrBlendedPatch,
      });

      expectTypeOf(validLimitRowEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(validMarketRowEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(validLimitPatchEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(validMarketPatchEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(invalidBlendedRowEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(invalidBlendedPatchEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(invalidUnionBlendedRowEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(invalidUnionBlendedPatchEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
    };

    expectTypeOf(assertUnionRowBranchExactness).toBeFunction();

    const assertDecodedMutationContract = (
      runtime: ViewServerRuntimeDecodedMutationClient<typeof viewServer.topics>,
      tradeRow: typeof Trade.Type,
      tradePatch: { readonly symbol: string },
    ) => {
      const checkEffect = runtime.execute({
        _tag: "CheckMutationAllowed",
        topic: "orders",
      });
      const publishEffect = runtime.execute({
        _tag: "PublishDecodedRows",
        topic: "orders",
        rows: [
          {
            id: "order-1",
            customerId: "customer-1",
            status: "open",
            price: 42,
            region: "usa",
            updatedAt: 1,
          },
        ],
      });
      const patchEffect = runtime.execute({
        _tag: "PatchDecodedFields",
        topic: "orders",
        key: "order-1",
        patch: { status: "closed" },
      });
      const deleteEffect = runtime.execute({
        _tag: "DeleteDecodedRow",
        topic: "orders",
        key: "order-1",
      });
      const invalidTrustedTopicMismatchedRowEffect = runtime.execute(
        {
          _tag: "PublishDecodedRows",
          topic: "orders",
          // @ts-expect-error trusted decoded rows remain correlated with the selected topic
          rows: [tradeRow],
        },
        viewServerRuntimeDecodedMutationTrust,
      );
      const invalidTrustedTopicMismatchedPatchEffect = runtime.execute(
        {
          _tag: "PatchDecodedFields",
          topic: "orders",
          key: "order-1",
          // @ts-expect-error trusted decoded patches remain correlated with the selected topic
          patch: tradePatch,
        },
        viewServerRuntimeDecodedMutationTrust,
      );

      expectTypeOf<Effect.Error<typeof checkEffect>>().toEqualTypeOf<ViewServerRuntimeError>();
      expectTypeOf<Effect.Error<typeof publishEffect>>().toEqualTypeOf<ViewServerRuntimeError>();
      expectTypeOf<Effect.Error<typeof patchEffect>>().toEqualTypeOf<ViewServerRuntimeError>();
      expectTypeOf<Effect.Error<typeof deleteEffect>>().toEqualTypeOf<ViewServerRuntimeError>();
      expectTypeOf<
        Effect.Error<typeof invalidTrustedTopicMismatchedRowEffect>
      >().toEqualTypeOf<ViewServerRuntimeError>();
      expectTypeOf<
        Effect.Error<typeof invalidTrustedTopicMismatchedPatchEffect>
      >().toEqualTypeOf<ViewServerRuntimeError>();

      const invalidTopicEffect = runtime.execute({
        _tag: "PublishDecodedRows",
        // @ts-expect-error decoded mutations are constrained to configured topics
        topic: "customers",
        rows: [],
      });
      const invalidTagEffect = runtime.execute({
        // @ts-expect-error decoded mutations reject unknown operation tags
        _tag: "Reset",
        topic: "orders",
      });
      const invalidIncompleteRowEffect = runtime.execute({
        _tag: "PublishDecodedRows",
        topic: "orders",
        // @ts-expect-error decoded rows must include every required topic field
        rows: [{ id: "order-1" }],
      });
      const invalidExtraRowEffect = runtime.execute({
        _tag: "PublishDecodedRows",
        topic: "orders",
        // @ts-expect-error decoded rows reject fields outside the selected topic
        rows: [
          {
            id: "order-1",
            customerId: "customer-1",
            status: "open",
            price: 42,
            region: "usa",
            updatedAt: 1,
            unexpected: true,
          },
        ],
      });
      const invalidWrongRowFieldEffect = runtime.execute({
        _tag: "PublishDecodedRows",
        topic: "orders",
        rows: [
          {
            id: "order-1",
            customerId: "customer-1",
            status: "open",
            // @ts-expect-error decoded row field values must match the selected topic
            price: "not-a-number",
            region: "usa",
            updatedAt: 1,
          },
        ],
      });
      const invalidTopicMismatchedRowEffect = runtime.execute({
        _tag: "PublishDecodedRows",
        topic: "orders",
        rows: [
          {
            id: "trade-1",
            // @ts-expect-error decoded rows must match the mutation topic
            symbol: "AAPL",
            quantity: 1,
            price: 42,
            updatedAt: 1,
          },
        ],
      });
      const invalidPatchFieldEffect = runtime.execute({
        _tag: "PatchDecodedFields",
        topic: "orders",
        key: "order-1",
        patch: {
          // @ts-expect-error decoded patches reject fields outside the selected topic
          unexpected: true,
        },
      });
      const invalidTopicMismatchedPatchEffect = runtime.execute({
        _tag: "PatchDecodedFields",
        topic: "orders",
        key: "order-1",
        patch: {
          // @ts-expect-error decoded patches remain correlated with the selected topic
          symbol: "AAPL",
        },
      });
      const rowWithExtraField = {
        id: "order-1",
        customerId: "customer-1",
        status: "open",
        price: 42,
        region: "usa",
        updatedAt: 1,
        unexpected: true,
      } as const;
      const invalidExtraRowVariableEffect = runtime.execute({
        _tag: "PublishDecodedRows",
        topic: "orders",
        // @ts-expect-error decoded row variables retain exact topic-field checking
        rows: [rowWithExtraField],
      });
      const patchWithExtraField = {
        status: "closed",
        unexpected: true,
      } as const;
      const invalidExtraPatchVariableEffect = runtime.execute({
        _tag: "PatchDecodedFields",
        topic: "orders",
        key: "order-1",
        // @ts-expect-error decoded patch variables retain exact topic-field checking
        patch: patchWithExtraField,
      });
      const patchWithOptionalExtraField: {
        readonly status?: "closed";
        readonly unexpected?: true;
      } = { status: "closed", unexpected: true };
      const invalidOptionalExtraPatchVariableEffect = runtime.execute({
        _tag: "PatchDecodedFields",
        topic: "orders",
        key: "order-1",
        // @ts-expect-error optional extra patch fields remain rejected when present
        patch: patchWithOptionalExtraField,
      });
      const assertUnionExactness = (
        row: OrderRow | (OrderRow & { readonly unexpected: true }),
        patch: Partial<OrderRow> | (Partial<OrderRow> & { readonly unexpected: true }),
      ) => {
        const invalidUnionRowEffect = runtime.execute({
          _tag: "PublishDecodedRows",
          topic: "orders",
          // @ts-expect-error unions cannot hide decoded row fields outside the selected topic
          rows: [row],
        });
        const invalidUnionPatchEffect = runtime.execute({
          _tag: "PatchDecodedFields",
          topic: "orders",
          key: "order-1",
          // @ts-expect-error unions cannot hide decoded patch fields outside the selected topic
          patch,
        });

        expectTypeOf(invalidUnionRowEffect).toMatchTypeOf<
          Effect.Effect<void, ViewServerRuntimeError>
        >();
        expectTypeOf(invalidUnionPatchEffect).toMatchTypeOf<
          Effect.Effect<void, ViewServerRuntimeError>
        >();
      };

      expectTypeOf(assertUnionExactness).toBeFunction();
      const assertRowContainerUnionExactness = (
        rows: ReadonlyArray<OrderRow> | ReadonlyArray<OrderRow & { readonly unexpected: true }>,
      ) => {
        const invalidRowContainerUnionEffect = runtime.execute({
          _tag: "PublishDecodedRows",
          topic: "orders",
          // @ts-expect-error unions of row containers cannot hide fields outside the selected topic
          rows,
        });

        expectTypeOf(invalidRowContainerUnionEffect).toMatchTypeOf<
          Effect.Effect<void, ViewServerRuntimeError>
        >();
      };

      expectTypeOf(assertRowContainerUnionExactness).toBeFunction();

      expectTypeOf(invalidTopicEffect).toMatchTypeOf<Effect.Effect<void, ViewServerRuntimeError>>();
      expectTypeOf(invalidTagEffect).toMatchTypeOf<Effect.Effect<void, ViewServerRuntimeError>>();
      expectTypeOf(invalidIncompleteRowEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(invalidExtraRowEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(invalidWrongRowFieldEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(invalidTopicMismatchedRowEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(invalidPatchFieldEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(invalidTopicMismatchedPatchEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(invalidExtraRowVariableEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(invalidExtraPatchVariableEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
      expectTypeOf(invalidOptionalExtraPatchVariableEffect).toMatchTypeOf<
        Effect.Effect<void, ViewServerRuntimeError>
      >();
    };

    expectTypeOf(assertDecodedMutationContract).toBeFunction();

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
