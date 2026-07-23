import { describe, expectTypeOf, it } from "@effect/vitest";
import { defineViewServerConfig, type ViewServerRuntimeError } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import type * as Protocol from "./index";
import {
  compileViewServerLiveEventCodec,
  defineViewServerLiveEventQuery,
  viewServerDecodeLiveEvent,
  viewServerDecodeHealthSummaryEvent,
  viewServerDecodeHealthTopicEvent,
  viewServerDecodeTrustedLiveEvent,
  viewServerEncodeHealthSummaryEvent,
  viewServerEncodeHealthTopicEvent,
} from "./index";

const TypeOrder = Schema.Struct({
  id: Schema.String,
});

const typeViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: TypeOrder,
      key: "id",
    },
    trades: {
      schema: TypeOrder,
      key: "id",
    },
  },
});

declare const wireEvent: Protocol.ViewServerWireEvent;
declare const trustedWireEvent: Protocol.ViewServerTrustedWireEvent;

describe("@effect-view-server/protocol type contract", () => {
  it("does not export transport-neutral live client contracts", () => {
    expectTypeOf<keyof typeof Protocol>().not.toEqualTypeOf<
      "ViewServerLiveClient" | "ViewServerLiveEvent" | "ViewServerLiveSubscription"
    >();

    // @ts-expect-error live client contracts belong to @effect-view-server/client.
    expectTypeOf<Protocol.ViewServerLiveClient<never>>().toBeNever();
    // @ts-expect-error live event contracts belong to @effect-view-server/client.
    expectTypeOf<Protocol.ViewServerLiveEvent<never>>().toBeNever();
    // @ts-expect-error live subscription contracts belong to @effect-view-server/client.
    expectTypeOf<Protocol.ViewServerLiveSubscription<never>>().toBeNever();
  });

  it("types health event encoder inputs from configured topics", () => {
    const validSummaryEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "snapshot",
      topic: "__view_server_health_summary",
      queryId: "health-summary",
      version: 1,
      keys: ["summary"],
      rows: [
        {
          id: "summary",
          status: "degraded",
          runtimeStatus: "degraded",
          connectionStatus: "connected",
          unhealthyTopics: ["orders"],
          updatedAtNanos: 1n,
          maxKafkaLag: null,
        },
      ],
      totalRows: 1,
    });
    expectTypeOf(validSummaryEncode).not.toBeAny();

    const invalidConnectedSummaryStatusEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "snapshot",
      topic: "__view_server_health_summary",
      queryId: "health-summary",
      version: 1,
      keys: ["summary"],
      rows: [
        {
          id: "summary",
          // @ts-expect-error connected is a connectionStatus, not a merged health status.
          status: "connected",
          runtimeStatus: "ready",
          connectionStatus: "connected",
          unhealthyTopics: [],
          updatedAtNanos: 1n,
          maxKafkaLag: 0n,
        },
      ],
      totalRows: 1,
    });
    expectTypeOf(invalidConnectedSummaryStatusEncode).not.toBeAny();

    const invalidSummaryEmptyRowsEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "snapshot",
      topic: "__view_server_health_summary",
      queryId: "health-summary",
      version: 1,
      keys: ["summary"],
      // @ts-expect-error health summary snapshots must contain exactly one row.
      rows: [],
      totalRows: 1,
    });
    expectTypeOf(invalidSummaryEmptyRowsEncode).not.toBeAny();

    const invalidSummaryTotalRowsEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "snapshot",
      topic: "__view_server_health_summary",
      queryId: "health-summary",
      version: 1,
      keys: ["summary"],
      rows: [
        {
          id: "summary",
          status: "ready",
          runtimeStatus: "ready",
          connectionStatus: "connected",
          unhealthyTopics: [],
          updatedAtNanos: 1n,
          maxKafkaLag: 0n,
        },
      ],
      // @ts-expect-error health summary totalRows is always 1.
      totalRows: 2,
    });
    expectTypeOf(invalidSummaryTotalRowsEncode).not.toBeAny();

    const invalidSummaryTopicEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "status",
      // @ts-expect-error health summary events must use the summary system topic.
      topic: "__view_server_health",
      queryId: "health-summary",
      status: "ready",
      code: "Ready",
    });
    expectTypeOf(invalidSummaryTopicEncode).not.toBeAny();

    const invalidSummaryKeyEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "snapshot",
      topic: "__view_server_health_summary",
      queryId: "health-summary",
      version: 1,
      // @ts-expect-error health summary snapshot keys are always summary.
      keys: ["orders"],
      rows: [
        {
          id: "summary",
          status: "ready",
          runtimeStatus: "ready",
          connectionStatus: "connected",
          unhealthyTopics: [],
          updatedAtNanos: 1n,
          maxKafkaLag: 0n,
        },
      ],
      totalRows: 1,
    });
    expectTypeOf(invalidSummaryKeyEncode).not.toBeAny();

    const invalidSummaryDeltaKeyEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "delta",
      topic: "__view_server_health_summary",
      queryId: "health-summary",
      fromVersion: 1,
      toVersion: 2,
      operations: [
        {
          type: "move",
          // @ts-expect-error health summary delta keys are always summary.
          key: "orders",
          fromIndex: 0,
          toIndex: 0,
        },
      ],
      totalRows: 1,
    });
    expectTypeOf(invalidSummaryDeltaKeyEncode).not.toBeAny();

    const invalidSummaryEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "snapshot",
      topic: "__view_server_health_summary",
      queryId: "health-summary",
      version: 1,
      keys: ["summary"],
      rows: [
        {
          id: "summary",
          status: "degraded",
          runtimeStatus: "degraded",
          connectionStatus: "connected",
          // @ts-expect-error unknown unhealthy topics are rejected at compile time.
          unhealthyTopics: ["missing"],
          updatedAtNanos: 1n,
          maxKafkaLag: 0n,
        },
      ],
      totalRows: 1,
    });
    expectTypeOf(invalidSummaryEncode).not.toBeAny();

    const validTopicEncode = viewServerEncodeHealthTopicEvent(typeViewServer, {
      type: "delta",
      topic: "__view_server_health",
      queryId: "health-detail",
      fromVersion: 1,
      toVersion: 2,
      operations: [
        {
          type: "update",
          key: "orders",
          row: {
            id: "orders",
            status: "ready",
            rowCount: 1,
            liveRowCount: 1,
            deletedRowCount: 0,
            version: 1,
            lastMutationAt: null,
            mutationsPerSecond: 0,
            rowsPerSecond: 0,
            pendingMutationBatches: 0,
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: 0,
            activeViews: 0,
            groupedFullEvaluationCount: 0,
            groupedPatchedEvaluationCount: 0,
            activeSubscriptions: 0,
            queuedEvents: 0,
            maxQueueDepth: 0,
            backpressureEvents: 0,
            memoryBytes: 0,
            tombstoneCount: 0,
            compactionPending: false,
            kafkaLag: null,
            updatedAtNanos: 1n,
          },
          index: 0,
        },
      ],
      totalRows: 2,
    });
    expectTypeOf(validTopicEncode).not.toBeAny();

    const invalidTopicEventTopicEncode = viewServerEncodeHealthTopicEvent(typeViewServer, {
      type: "status",
      // @ts-expect-error health detail events must use the detail system topic.
      topic: "__view_server_health_summary",
      queryId: "health-detail",
      status: "ready",
      code: "Ready",
    });
    expectTypeOf(invalidTopicEventTopicEncode).not.toBeAny();

    const invalidTopicMoveKeyEncode = viewServerEncodeHealthTopicEvent(typeViewServer, {
      type: "delta",
      topic: "__view_server_health",
      queryId: "health-detail",
      fromVersion: 1,
      toVersion: 2,
      operations: [
        {
          type: "move",
          // @ts-expect-error health detail operation keys must be configured topics.
          key: "missing",
          fromIndex: 0,
          toIndex: 1,
        },
      ],
      totalRows: 2,
    });
    expectTypeOf(invalidTopicMoveKeyEncode).not.toBeAny();

    const mismatchedTopicDeltaRowEncode = viewServerEncodeHealthTopicEvent(typeViewServer, {
      type: "delta",
      topic: "__view_server_health",
      queryId: "health-detail",
      fromVersion: 1,
      toVersion: 2,
      operations: [
        {
          type: "update",
          key: "orders",
          row: {
            // @ts-expect-error health detail operation row ids must match operation keys.
            id: "trades",
            status: "ready",
            rowCount: 1,
            liveRowCount: 1,
            deletedRowCount: 0,
            version: 1,
            lastMutationAt: null,
            mutationsPerSecond: 0,
            rowsPerSecond: 0,
            pendingMutationBatches: 0,
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: 0,
            activeViews: 0,
            groupedFullEvaluationCount: 0,
            groupedPatchedEvaluationCount: 0,
            activeSubscriptions: 0,
            queuedEvents: 0,
            maxQueueDepth: 0,
            backpressureEvents: 0,
            memoryBytes: 0,
            tombstoneCount: 0,
            compactionPending: false,
            kafkaLag: 0n,
            updatedAtNanos: 1n,
          },
          index: 0,
        },
      ],
      totalRows: 2,
    });
    expectTypeOf(mismatchedTopicDeltaRowEncode).not.toBeAny();

    const invalidTopicEncode = viewServerEncodeHealthTopicEvent(typeViewServer, {
      type: "delta",
      topic: "__view_server_health",
      queryId: "health-detail",
      fromVersion: 1,
      toVersion: 2,
      operations: [
        {
          type: "update",
          key: "orders",
          row: {
            // @ts-expect-error unknown health topic rows are rejected at compile time.
            id: "missing",
            status: "ready",
            rowCount: 1,
            liveRowCount: 1,
            deletedRowCount: 0,
            version: 1,
            lastMutationAt: null,
            mutationsPerSecond: 0,
            rowsPerSecond: 0,
            pendingMutationBatches: 0,
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: 0,
            activeViews: 0,
            groupedFullEvaluationCount: 0,
            groupedPatchedEvaluationCount: 0,
            activeSubscriptions: 0,
            queuedEvents: 0,
            maxQueueDepth: 0,
            backpressureEvents: 0,
            memoryBytes: 0,
            tombstoneCount: 0,
            compactionPending: false,
            kafkaLag: 0n,
            updatedAtNanos: 1n,
          },
          index: 0,
        },
      ],
      totalRows: 2,
    });
    expectTypeOf(invalidTopicEncode).not.toBeAny();

    const invalidSummaryRemoveEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "delta",
      topic: "__view_server_health_summary",
      queryId: "health-summary",
      fromVersion: 1,
      toVersion: 2,
      operations: [
        {
          // @ts-expect-error fixed-cardinality health summary deltas cannot remove the summary row.
          type: "remove",
          key: "summary",
        },
      ],
      totalRows: 1,
    });
    expectTypeOf(invalidSummaryRemoveEncode).not.toBeAny();

    const invalidSummaryInsertEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "delta",
      topic: "__view_server_health_summary",
      queryId: "health-summary",
      fromVersion: 1,
      toVersion: 2,
      operations: [
        {
          // @ts-expect-error fixed-cardinality health summary deltas cannot insert the summary row.
          type: "insert",
          key: "summary",
          row: {
            id: "summary",
            status: "ready",
            runtimeStatus: "ready",
            connectionStatus: "connected",
            unhealthyTopics: [],
            updatedAtNanos: 1n,
            maxKafkaLag: 0n,
          },
          index: 0,
        },
      ],
      totalRows: 1,
    });
    expectTypeOf(invalidSummaryInsertEncode).not.toBeAny();

    const invalidTopicRemoveEncode = viewServerEncodeHealthTopicEvent(typeViewServer, {
      type: "delta",
      topic: "__view_server_health",
      queryId: "health-detail",
      fromVersion: 1,
      toVersion: 2,
      operations: [
        {
          // @ts-expect-error fixed-cardinality health detail deltas cannot remove configured topics.
          type: "remove",
          key: "orders",
        },
      ],
      totalRows: 2,
    });
    expectTypeOf(invalidTopicRemoveEncode).not.toBeAny();

    const invalidTopicInsertEncode = viewServerEncodeHealthTopicEvent(typeViewServer, {
      type: "delta",
      topic: "__view_server_health",
      queryId: "health-detail",
      fromVersion: 1,
      toVersion: 2,
      operations: [
        {
          // @ts-expect-error fixed-cardinality health detail deltas cannot insert configured topics.
          type: "insert",
          key: "orders",
          row: {
            id: "orders",
            status: "ready",
            rowCount: 1,
            liveRowCount: 1,
            deletedRowCount: 0,
            version: 1,
            lastMutationAt: null,
            mutationsPerSecond: 0,
            rowsPerSecond: 0,
            pendingMutationBatches: 0,
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: 0,
            activeViews: 0,
            groupedFullEvaluationCount: 0,
            groupedPatchedEvaluationCount: 0,
            activeSubscriptions: 0,
            queuedEvents: 0,
            maxQueueDepth: 0,
            backpressureEvents: 0,
            memoryBytes: 0,
            tombstoneCount: 0,
            compactionPending: false,
            kafkaLag: 0n,
            updatedAtNanos: 1n,
          },
          index: 0,
        },
      ],
      totalRows: 2,
    });
    expectTypeOf(invalidTopicInsertEncode).not.toBeAny();
  });

  it("requires schema-validated event proof for trusted live event decoding", () => {
    const validTrustedDecode = viewServerDecodeTrustedLiveEvent(
      typeViewServer,
      "orders",
      { select: ["id"] },
      trustedWireEvent,
    );
    expectTypeOf(validTrustedDecode).not.toBeAny();

    const invalidTrustedDecode = viewServerDecodeTrustedLiveEvent(
      typeViewServer,
      "orders",
      { select: ["id"] },
      // @ts-expect-error trusted decoder requires ViewServerTrustedWireEvent.
      wireEvent,
    );
    expectTypeOf(invalidTrustedDecode).not.toBeAny();
  });

  it("derives raw and grouped row types through public live event codecs", () => {
    const rawCodec = compileViewServerLiveEventCodec(typeViewServer, "orders", {
      select: ["id"],
    });
    const rawDecoded = rawCodec.decodeTrusted(trustedWireEvent);
    type RawDecodedEvent = Effect.Success<typeof rawDecoded>;
    type RawDecodedSnapshot = Extract<RawDecodedEvent, { readonly type: "snapshot" }>;
    type RawDecodedDeltaOperation = Extract<
      RawDecodedEvent,
      { readonly type: "delta" }
    >["operations"][number];
    type RawDecodedChangedRow = Extract<
      RawDecodedDeltaOperation,
      { readonly type: "insert" | "update" }
    >["row"];
    expectTypeOf<RawDecodedSnapshot["rows"][number]>().toEqualTypeOf<
      Pick<typeof TypeOrder.Type, "id">
    >();
    expectTypeOf<RawDecodedChangedRow>().toEqualTypeOf<Pick<typeof TypeOrder.Type, "id">>();
    expectTypeOf<Effect.Error<typeof rawDecoded>>().toEqualTypeOf<ViewServerRuntimeError>();

    const invalidTrustedDecode = rawCodec.decodeTrusted(
      // @ts-expect-error compiled trusted decoder requires validated wire event proof.
      wireEvent,
    );
    expectTypeOf(invalidTrustedDecode).not.toBeAny();

    const groupedCodec = compileViewServerLiveEventCodec(typeViewServer, "orders", {
      groupBy: ["id"],
      aggregates: {
        rowCount: { aggFunc: "count" },
      },
    });
    const groupedDecoded = groupedCodec.decodeTrusted(trustedWireEvent);
    type GroupedDecodedEvent = Effect.Success<typeof groupedDecoded>;
    type GroupedDecodedSnapshot = Extract<GroupedDecodedEvent, { readonly type: "snapshot" }>;
    type GroupedDecodedDeltaOperation = Extract<
      GroupedDecodedEvent,
      { readonly type: "delta" }
    >["operations"][number];
    type GroupedDecodedChangedRow = Extract<
      GroupedDecodedDeltaOperation,
      { readonly type: "insert" | "update" }
    >["row"];
    type ExpectedGroupedRow = {
      readonly id: string;
      readonly rowCount: bigint;
    };
    expectTypeOf<GroupedDecodedSnapshot["rows"][number]>().toEqualTypeOf<ExpectedGroupedRow>();
    expectTypeOf<GroupedDecodedChangedRow>().toEqualTypeOf<ExpectedGroupedRow>();
    expectTypeOf<Effect.Error<typeof groupedDecoded>>().toEqualTypeOf<ViewServerRuntimeError>();

    // @ts-expect-error compiled codec row type is derived from the topic and query.
    const fabricatedCodec: Protocol.ViewServerLiveEventCodec<{
      readonly fabricated: boolean;
    }> = rawCodec;
    expectTypeOf(fabricatedCodec).not.toBeAny();

    const invalidTopicCodec = compileViewServerLiveEventCodec(
      typeViewServer,
      // @ts-expect-error compiled codec topic must exist in the configured topics.
      "missing",
      { select: ["id"] },
    );
    expectTypeOf(invalidTopicCodec).not.toBeAny();

    // @ts-expect-error selected fields must be own fields of the configured topic row.
    const unknownSelectedFieldCodec = compileViewServerLiveEventCodec(typeViewServer, "orders", {
      select: ["missing"],
    });
    expectTypeOf(unknownSelectedFieldCodec).not.toBeAny();

    // @ts-expect-error inherited object fields are not topic row fields.
    const inheritedSelectedFieldCodec = compileViewServerLiveEventCodec(typeViewServer, "orders", {
      select: ["toString"],
    });
    expectTypeOf(inheritedSelectedFieldCodec).not.toBeAny();

    // @ts-expect-error grouped fields must be own fields of the configured topic row.
    const unknownGroupedFieldCodec = compileViewServerLiveEventCodec(typeViewServer, "orders", {
      groupBy: ["missing"],
      aggregates: { rowCount: { aggFunc: "count" } },
    });
    expectTypeOf(unknownGroupedFieldCodec).not.toBeAny();

    // @ts-expect-error inherited object fields are not topic row fields.
    const inheritedGroupedFieldCodec = compileViewServerLiveEventCodec(typeViewServer, "orders", {
      groupBy: ["toString"],
      aggregates: { rowCount: { aggFunc: "count" } },
    });
    expectTypeOf(inheritedGroupedFieldCodec).not.toBeAny();
  });

  it("defines reusable exact live event queries without as const", () => {
    const rawQuery = defineViewServerLiveEventQuery(typeViewServer, "orders", {
      select: ["id"],
    });
    const rawCodec = compileViewServerLiveEventCodec(typeViewServer, "orders", rawQuery);
    const rawDecoded = rawCodec.decodeTrusted(trustedWireEvent);
    type RawSnapshot = Extract<Effect.Success<typeof rawDecoded>, { readonly type: "snapshot" }>;
    expectTypeOf<RawSnapshot["rows"][number]>().toEqualTypeOf<Pick<typeof TypeOrder.Type, "id">>();

    const groupedQuery = defineViewServerLiveEventQuery(typeViewServer, "orders", {
      groupBy: ["id"],
      aggregates: { rowCount: { aggFunc: "count" } },
    });
    const groupedCodec = compileViewServerLiveEventCodec(typeViewServer, "orders", groupedQuery);
    const groupedDecoded = groupedCodec.decodeTrusted(trustedWireEvent);
    type GroupedSnapshot = Extract<
      Effect.Success<typeof groupedDecoded>,
      { readonly type: "snapshot" }
    >;
    expectTypeOf<GroupedSnapshot["rows"][number]>().toEqualTypeOf<{
      readonly id: string;
      readonly rowCount: bigint;
    }>();

    // @ts-expect-error reusable query builders reject fields outside the topic schema.
    const invalidQuery = defineViewServerLiveEventQuery(typeViewServer, "orders", {
      select: ["missing"],
    });
    expectTypeOf(invalidQuery).not.toBeAny();
  });

  it("derives one-shot live decoder rows and rejects fabricated row claims", () => {
    const rawDecode = viewServerDecodeLiveEvent(
      typeViewServer,
      "orders",
      { select: ["id"] },
      wireEvent,
    );
    const trustedRawDecode = viewServerDecodeTrustedLiveEvent(
      typeViewServer,
      "orders",
      { select: ["id"] },
      trustedWireEvent,
    );
    const groupedDecode = viewServerDecodeLiveEvent(
      typeViewServer,
      "orders",
      {
        groupBy: ["id"],
        aggregates: { rowCount: { aggFunc: "count" } },
      },
      wireEvent,
    );
    type RawSnapshot = Extract<Effect.Success<typeof rawDecode>, { readonly type: "snapshot" }>;
    type TrustedRawSnapshot = Extract<
      Effect.Success<typeof trustedRawDecode>,
      { readonly type: "snapshot" }
    >;
    type GroupedSnapshot = Extract<
      Effect.Success<typeof groupedDecode>,
      { readonly type: "snapshot" }
    >;
    expectTypeOf<RawSnapshot["rows"][number]>().toEqualTypeOf<Pick<typeof TypeOrder.Type, "id">>();
    expectTypeOf<TrustedRawSnapshot["rows"][number]>().toEqualTypeOf<
      Pick<typeof TypeOrder.Type, "id">
    >();
    expectTypeOf<GroupedSnapshot["rows"][number]>().toEqualTypeOf<{
      readonly id: string;
      readonly rowCount: bigint;
    }>();

    const fabricatedDecode = viewServerDecodeLiveEvent<
      typeof typeViewServer.topics,
      "orders",
      // @ts-expect-error the third generic is a query contract, not a caller-selected row type.
      { readonly fabricated: boolean }
    >(typeViewServer, "orders", { fabricated: true }, wireEvent);
    expectTypeOf(fabricatedDecode).not.toBeAny();

    const fabricatedTrustedDecode = viewServerDecodeTrustedLiveEvent<
      typeof typeViewServer.topics,
      "orders",
      // @ts-expect-error the third generic is a query contract, not a caller-selected row type.
      { readonly fabricated: boolean }
    >(typeViewServer, "orders", { fabricated: true }, trustedWireEvent);
    expectTypeOf(fabricatedTrustedDecode).not.toBeAny();
  });

  it("preserves health event decoder output generics", () => {
    const summaryDecode = viewServerDecodeHealthSummaryEvent(typeViewServer, wireEvent);
    type SummaryEvent = Effect.Success<typeof summaryDecode>;
    type SummarySnapshot = Extract<SummaryEvent, { readonly type: "snapshot" }>;
    type SummaryDeltaOperation = Extract<
      SummaryEvent,
      { readonly type: "delta" }
    >["operations"][number];
    expectTypeOf<SummarySnapshot["topic"]>().toEqualTypeOf<"__view_server_health_summary">();
    expectTypeOf<SummarySnapshot["keys"]>().toEqualTypeOf<readonly ["summary"]>();
    expectTypeOf<SummarySnapshot["rows"][0]["id"]>().toEqualTypeOf<"summary">();
    expectTypeOf<SummarySnapshot["rows"][0]["unhealthyTopics"][number]>().toEqualTypeOf<
      "orders" | "trades"
    >();
    expectTypeOf<SummarySnapshot["totalRows"]>().toEqualTypeOf<1>();
    expectTypeOf<SummaryDeltaOperation["key"]>().toEqualTypeOf<"summary">();
    expectTypeOf<
      Extract<SummaryDeltaOperation, { readonly type: "insert" }>
    >().toEqualTypeOf<never>();
    expectTypeOf<
      Extract<SummaryDeltaOperation, { readonly type: "remove" }>
    >().toEqualTypeOf<never>();

    const topicDecode = viewServerDecodeHealthTopicEvent(typeViewServer, wireEvent);
    type TopicEvent = Effect.Success<typeof topicDecode>;
    type TopicSnapshot = Extract<TopicEvent, { readonly type: "snapshot" }>;
    type TopicDeltaOperation = Extract<
      TopicEvent,
      { readonly type: "delta" }
    >["operations"][number];
    expectTypeOf<TopicSnapshot["topic"]>().toEqualTypeOf<"__view_server_health">();
    expectTypeOf<TopicSnapshot["keys"][number]>().toEqualTypeOf<"orders" | "trades">();
    expectTypeOf<TopicSnapshot["rows"][number]["id"]>().toEqualTypeOf<"orders" | "trades">();
    expectTypeOf<TopicDeltaOperation["key"]>().toEqualTypeOf<"orders" | "trades">();
    expectTypeOf<
      Extract<TopicDeltaOperation, { readonly type: "insert" }>
    >().toEqualTypeOf<never>();
    expectTypeOf<
      Extract<TopicDeltaOperation, { readonly type: "remove" }>
    >().toEqualTypeOf<never>();
  });
});
