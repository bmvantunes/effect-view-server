import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { Schema } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import { SchemaGetter } from "effect";

export const Order = Schema.Struct({
  id: ViewServerId,
  status: Schema.Literals(["open", "closed"]),
  price: Schema.Number,
  quantity: Schema.BigInt,
  decimalPrice: Schema.BigDecimal,
  optionalPrice: Schema.Union([Schema.Number, Schema.Undefined]),
  optionalQuantity: Schema.Union([Schema.BigInt, Schema.Undefined]),
  unset: Schema.Undefined,
  metadata: Schema.Struct({
    _viewServerScalar: Schema.String,
    value: Schema.String,
  }),
});

export const BadJsonField = Schema.String.pipe(
  Schema.encodeTo(Schema.Any, {
    decode: SchemaGetter.transform((value) => (typeof value === "string" ? value : "decoded")),
    encode: SchemaGetter.transform(() => Symbol("not-json")),
  }),
);

export const BadJsonRow = Schema.Struct({
  id: ViewServerId,
});

export const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
    },
    badjson: {
      schema: BadJsonRow,
    },
  },
});

export const nonOwnTopicRowFields = [
  "toString",
  "valueOf",
  "hasOwnProperty",
  "constructor",
  "missing",
] as const;

export const unknownTopicRowFieldError = {
  _tag: "ViewServerRuntimeError",
  code: "InvalidQuery",
  message: "Query references an unknown field for topic: orders",
  topic: "orders",
} as const;

export const formatDecodedDecimal = (value: Schema.Schema.Type<typeof Schema.Unknown>): string =>
  BigDecimal.isBigDecimal(value) ? BigDecimal.format(value) : String(value);

export const topicHealth = {
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
  activeIncrementalGroupedViews: 1,
  activeViews: 1,
  groupedFullEvaluationCount: 0,
  groupedPatchedEvaluationCount: 0,
  activeSubscriptions: 1,
  queuedEvents: 0,
  maxQueueDepth: 0,
  backpressureEvents: 0,
  memoryBytes: 0,
  tombstoneCount: 0,
  compactionPending: false,
} as const;

export const wireHealth = {
  status: "ready",
  version: 1,
  uptimeMs: 10,
  engine: {
    topics: {
      orders: topicHealth,
      badjson: { ...topicHealth, rowCount: 0, liveRowCount: 0 },
    },
  },
  sources: {},
  transport: {
    activeClients: 1,
    activeStreams: 1,
    activeSubscriptions: 1,
    messagesPerSecond: 0,
    bytesPerSecond: 0,
    queuedMessages: 0,
    queuedBytes: 0,
    droppedClients: 0,
    backpressureEvents: 0,
    reconnects: 0,
    lastError: null,
  },
} as const;
