import type { ColumnLiveViewEngineHealth } from "@effect-view-server/column-live-view-engine";
import {
  ViewServerId,
  defineViewServerConfig,
  type ViewServerHealth,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { Option, Schema, Tracer } from "effect";
import { healthFromEngine } from "../health";

export const Order = Schema.Struct({
  id: ViewServerId,
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
    },
  },
});

export type OrderRow = typeof Order.Type;
export type Topics = typeof viewServer.topics;

export type RecordedSpan = {
  readonly attributes: ReadonlyArray<readonly [string, unknown]>;
  readonly name: string;
  readonly parentName: string | null;
  readonly parentSpanId: string | null;
  readonly spanId: string;
  readonly traceId: string;
};

export const order = (id: string, price: number): OrderRow => ({
  id,
  customerId: `customer-${id}`,
  status: "open",
  price,
  region: "usa",
  updatedAt: price,
});

export const publicLeasedRuntimeAccessError = {
  _tag: "ViewServerRuntimeError",
  code: "UnsupportedQuery",
  topic: "orders",
  message:
    "Leased Source topics do not support one-shot snapshots; use a live subscription so Runtime Core owns the source lease lifecycle.",
} satisfies ViewServerRuntimeError;

export const publicSourceOwnedRuntimeMutationError = {
  _tag: "ViewServerRuntimeError",
  code: "UnsupportedQuery",
  topic: "orders",
  message:
    "Source-owned topics do not support direct runtime mutations; publish through the configured Source Adapter or use an externally-published topic.",
} satisfies ViewServerRuntimeError;

export const publicSourceOwnedRuntimeResetError = {
  _tag: "ViewServerRuntimeError",
  code: "UnsupportedQuery",
  message:
    "Source-owned topics do not support direct runtime reset; close the runtime or reset source-free topics through their owner.",
} satisfies ViewServerRuntimeError;

const stableAttributes = (
  attributes: ReadonlyMap<string, unknown>,
): ReadonlyArray<readonly [string, unknown]> =>
  Array.from(attributes.entries()).sort(([left], [right]) => left.localeCompare(right));

const spanName = (span: Tracer.AnySpan): string => (span._tag === "Span" ? span.name : span.spanId);

type RecordingTracer = {
  readonly spans: Array<RecordedSpan>;
  readonly tracer: Tracer.Tracer;
};

export const makeRecordingTracer = (): RecordingTracer => {
  const spans: Array<RecordedSpan> = [];
  let nextSpanId = 0;
  const nextId = (): string => {
    nextSpanId += 1;
    return String(nextSpanId);
  };
  const tracer = Tracer.make({
    span: (options): Tracer.Span => {
      const id = nextId();
      const attributes = new Map<string, unknown>();
      const links = Array.from(options.links);
      let status: Tracer.SpanStatus = {
        _tag: "Started",
        startTime: options.startTime,
      };
      const span: Tracer.Span = {
        _tag: "Span",
        annotations: options.annotations,
        attribute: (key, value) => {
          attributes.set(key, value);
        },
        attributes,
        end: (endTime, exit) => {
          const parent = Option.getOrNull(options.parent);
          status = {
            _tag: "Ended",
            endTime,
            exit,
            startTime: status.startTime,
          };
          spans.push({
            attributes: stableAttributes(attributes),
            name: options.name,
            parentName: parent === null ? null : spanName(parent),
            parentSpanId: parent === null ? null : parent.spanId,
            spanId: span.spanId,
            traceId: span.traceId,
          });
        },
        event: () => {},
        addLinks: (newLinks) => {
          links.push(...newLinks);
        },
        get status() {
          return status;
        },
        kind: options.kind,
        links,
        name: options.name,
        parent: options.parent,
        sampled: options.sampled,
        spanId: `span-${id}`,
        traceId: Option.match(options.parent, {
          onNone: () => `trace-${id}`,
          onSome: (parent) => parent.traceId,
        }),
      };
      return span;
    },
  });
  return { spans, tracer };
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

export const sourceFreeViewServerHealth = (
  health: ColumnLiveViewEngineHealth<Topics>,
): ViewServerHealth<Topics> => ({
  ...healthFromEngine(health),
  sources: {},
});
