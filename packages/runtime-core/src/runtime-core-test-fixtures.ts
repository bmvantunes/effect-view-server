import type { ColumnLiveViewEngineHealth } from "@effect-view-server/column-live-view-engine";
import { defineViewServerConfig, type ViewServerRuntimeError } from "@effect-view-server/config";
import { Schema } from "effect";

export const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

export const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});
export const publicLeasedRuntimeAccessError = {
  _tag: "ViewServerRuntimeError",
  code: "UnsupportedQuery",
  topic: "orders",
  message:
    "Leased gRPC topics do not support direct runtime mutations, one-shot snapshots, or runtime-core subscriptions; use the runtime gRPC lease manager so it owns lease lifecycle.",
} satisfies ViewServerRuntimeError;

export const publicSourceOwnedRuntimeMutationError = {
  _tag: "ViewServerRuntimeError",
  code: "UnsupportedQuery",
  topic: "orders",
  message:
    "Source-owned topics do not support direct runtime mutations; publish through the configured Kafka/gRPC source or use an externally-published topic.",
} satisfies ViewServerRuntimeError;

export const publicSourceOwnedRuntimeResetError = {
  _tag: "ViewServerRuntimeError",
  code: "UnsupportedQuery",
  message:
    "Source-owned topics do not support direct runtime reset; close the runtime or reset source-free topics through their owner.",
} satisfies ViewServerRuntimeError;

export type OrderRow = typeof Order.Type;
export type Topics = typeof viewServer.topics;
export const order = (id: string, price: number): OrderRow => ({
  id,
  customerId: `customer-${id}`,
  status: "open",
  price,
  region: "usa",
  updatedAt: price,
});

export const refreshFailed: ViewServerRuntimeError = {
  _tag: "ViewServerRuntimeError",
  code: "RuntimeUnavailable",
  message: "Health refresh failed.",
};

export const engineHealth = (
  status: "ready" | "stopping",
  rowCount: number,
): ColumnLiveViewEngineHealth<Topics> => ({
  status,
  version: status === "ready" ? 1 : 2,
  topics: {
    orders: {
      status: status === "ready" ? "ready" : "degraded",
      rowCount,
      liveRowCount: rowCount,
      deletedRowCount: 0,
      version: status === "ready" ? 1 : 2,
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
    },
  },
  activeSubscriptions: 0,
  queuedEvents: 0,
  maxQueueDepth: 0,
  backpressureEvents: 0,
});
