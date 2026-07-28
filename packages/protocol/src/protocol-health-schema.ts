import type {
  TopicRuntimeHealth,
  TransportHealth,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
} from "@effect-view-server/config";
import { Schema } from "effect";

const StringOrNull = Schema.NullOr(Schema.String);
const NumberOrNull = Schema.NullOr(Schema.Number);
const BigIntString = Schema.BigIntFromString;

const TopicRuntimeHealthSchema: Schema.Codec<TopicRuntimeHealth> = Schema.Struct({
  status: Schema.Literals(["ready", "degraded", "starting"]),
  rowCount: Schema.Number,
  liveRowCount: Schema.Number,
  deletedRowCount: Schema.Number,
  version: Schema.Number,
  lastMutationAt: NumberOrNull,
  mutationsPerSecond: Schema.Number,
  rowsPerSecond: Schema.Number,
  pendingMutationBatches: Schema.Number,
  activeFallbackGroupedViews: Schema.Number,
  activeIncrementalGroupedViews: Schema.Number,
  activeViews: Schema.Number,
  groupedFullEvaluationCount: Schema.Number,
  groupedPatchedEvaluationCount: Schema.Number,
  activeSubscriptions: Schema.Number,
  queuedEvents: Schema.Number,
  maxQueueDepth: Schema.Number,
  backpressureEvents: Schema.Number,
  memoryBytes: Schema.Number,
  tombstoneCount: Schema.Number,
  compactionPending: Schema.Boolean,
});

const TransportHealthSchema: Schema.Codec<TransportHealth> = Schema.Struct({
  activeClients: Schema.Number,
  activeStreams: Schema.Number,
  activeSubscriptions: Schema.Number,
  messagesPerSecond: Schema.Number,
  bytesPerSecond: Schema.Number,
  queuedMessages: Schema.Number,
  queuedBytes: Schema.Number,
  droppedClients: Schema.Number,
  backpressureEvents: Schema.Number,
  reconnects: Schema.Number,
  lastError: StringOrNull,
});

const ViewServerHealthBaseFields = {
  status: Schema.Literals(["ready", "degraded", "starting", "stopping"]),
  version: Schema.Number,
  uptimeMs: Schema.Number,
  engine: Schema.Struct({
    topics: Schema.Record(Schema.String, TopicRuntimeHealthSchema),
  }),
  transport: TransportHealthSchema,
};

export const ViewServerHealthBaseSchema = Schema.Struct(ViewServerHealthBaseFields);

export const ViewServerHealthSchema = Schema.Struct({
  ...ViewServerHealthBaseFields,
  sources: Schema.Record(Schema.String, Schema.Json),
});

export type ViewServerWireHealth = typeof ViewServerHealthSchema.Type;

export const ViewServerHealthSummaryRowSchema: Schema.Codec<
  ViewServerHealthSummaryRow,
  unknown,
  never,
  never
> = Schema.Struct({
  id: Schema.Literal("summary"),
  status: Schema.Literals([
    "ready",
    "degraded",
    "starting",
    "stopping",
    "connecting",
    "disconnected",
  ]),
  runtimeStatus: Schema.Literals(["ready", "degraded", "starting", "stopping"]),
  connectionStatus: Schema.Literals(["connecting", "connected", "disconnected"]),
  unhealthyTopics: Schema.Array(Schema.String),
  updatedAtNanos: BigIntString,
});

export const ViewServerHealthTopicRowSchema: Schema.Codec<
  ViewServerHealthTopicRow,
  unknown,
  never,
  never
> = Schema.Struct({
  id: Schema.String,
  status: Schema.Literals(["ready", "degraded", "starting", "stopping"]),
  rowCount: Schema.Number,
  liveRowCount: Schema.Number,
  deletedRowCount: Schema.Number,
  version: Schema.Number,
  lastMutationAt: NumberOrNull,
  mutationsPerSecond: Schema.Number,
  rowsPerSecond: Schema.Number,
  pendingMutationBatches: Schema.Number,
  activeFallbackGroupedViews: Schema.Number,
  activeIncrementalGroupedViews: Schema.Number,
  activeViews: Schema.Number,
  groupedFullEvaluationCount: Schema.Number,
  groupedPatchedEvaluationCount: Schema.Number,
  activeSubscriptions: Schema.Number,
  queuedEvents: Schema.Number,
  maxQueueDepth: Schema.Number,
  backpressureEvents: Schema.Number,
  memoryBytes: Schema.Number,
  tombstoneCount: Schema.Number,
  compactionPending: Schema.Boolean,
  updatedAtNanos: BigIntString,
});
