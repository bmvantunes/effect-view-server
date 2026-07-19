import { Effect, Option, Schema, SchemaGetter, SchemaIssue } from "effect";

type QueryGraphValue =
  | readonly ["null"]
  | readonly ["string", string]
  | readonly ["boolean", boolean]
  | readonly ["number", number]
  | readonly ["reference", number];

type QueryGraphNode =
  | readonly ["array", ReadonlyArray<QueryGraphValue>]
  | readonly ["record", ReadonlyArray<readonly [string, QueryGraphValue]>];

type PendingQueryGraphNode = {
  readonly id: number;
  readonly value: object;
};

type QueryGraphCycleFrame =
  | { readonly _tag: "enter"; readonly id: number }
  | { readonly _tag: "exit"; readonly id: number };

const denseUnknownArray = (value: unknown): ReadonlyArray<unknown> => {
  if (!Array.isArray(value) || Object.getPrototypeOf(value) !== Array.prototype) {
    throw new TypeError("Expected a plain array.");
  }
  if (Object.getOwnPropertySymbols(value).length > 0) {
    throw new TypeError("Arrays must not contain symbol properties.");
  }
  const output: Array<unknown> = [];
  const allowed = new Set(["length"]);
  for (let index = 0; index < value.length; index += 1) {
    const key = String(index);
    allowed.add(key);
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
      throw new TypeError("Arrays must be dense data arrays.");
    }
    output.push(descriptor.value);
  }
  if (Object.getOwnPropertyNames(value).some((key) => !allowed.has(key))) {
    throw new TypeError("Arrays must not contain extra properties.");
  }
  return output;
};

const isPlainUnknownRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" &&
  value !== null &&
  !Array.isArray(value) &&
  Object.getPrototypeOf(value) === Object.prototype;

const plainUnknownRecord = (value: unknown): Readonly<Record<string, unknown>> => {
  if (!isPlainUnknownRecord(value)) {
    throw new TypeError("Expected a plain record.");
  }
  return value;
};

const recordDataEntries = (
  value: Readonly<Record<string, unknown>>,
): ReadonlyArray<readonly [string, unknown]> => {
  if (Object.getOwnPropertySymbols(value).length > 0) {
    throw new TypeError("Records must not contain symbol properties.");
  }
  const entries: Array<readonly [string, unknown]> = [];
  for (const key of Object.getOwnPropertyNames(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
      throw new TypeError("Record fields must be own enumerable data properties.");
    }
    entries.push([key, descriptor.value]);
  }
  return entries;
};

const queryGraphNodeReferences = (node: QueryGraphNode): ReadonlyArray<number> => {
  const references: Array<number> = [];
  if (node[0] === "array") {
    for (const value of node[1]) {
      if (value[0] === "reference") {
        references.push(value[1]);
      }
    }
    return references;
  }
  for (const [, value] of node[1]) {
    if (value[0] === "reference") {
      references.push(value[1]);
    }
  }
  return references;
};

const requireAcyclicQueryGraph = (
  root: QueryGraphValue,
  nodes: ReadonlyArray<QueryGraphNode>,
): void => {
  if (root[0] !== "reference") {
    return;
  }
  const frames: Array<QueryGraphCycleFrame> = [{ _tag: "enter", id: root[1] }];
  const active = new Set<number>();
  const complete = new Set<number>();
  while (frames.length > 0) {
    const frame = frames.pop()!;
    if (frame._tag === "exit") {
      active.delete(frame.id);
      complete.add(frame.id);
      continue;
    }
    if (active.has(frame.id)) {
      throw new TypeError("Query graph must not contain cycles.");
    }
    if (complete.has(frame.id)) {
      continue;
    }
    active.add(frame.id);
    frames.push({ _tag: "exit", id: frame.id });
    for (const id of queryGraphNodeReferences(nodes[frame.id]!)) {
      frames.push({ _tag: "enter", id });
    }
  }
};

export const encodeQueryGraph = (input: unknown): string => {
  const nodes: Array<QueryGraphNode> = [];
  const pending: Array<PendingQueryGraphNode> = [];
  const ids = new WeakMap<object, number>();
  const encodeValue = (value: unknown): QueryGraphValue => {
    if (value === null) {
      return ["null"];
    }
    if (typeof value === "string") {
      return ["string", value];
    }
    if (typeof value === "boolean") {
      return ["boolean", value];
    }
    if (typeof value === "number" && Number.isFinite(value)) {
      return ["number", Object.is(value, -0) ? 0 : value];
    }
    if (typeof value !== "object") {
      throw new TypeError("Query values must be JSON-safe.");
    }
    const existing = ids.get(value);
    if (existing !== undefined) {
      return ["reference", existing];
    }
    const id = nodes.length;
    ids.set(value, id);
    nodes.push(["array", []]);
    pending.push({ id, value });
    return ["reference", id];
  };

  const root = encodeValue(input);
  for (let index = 0; index < pending.length; index += 1) {
    const pendingNode = pending[index]!;
    if (Array.isArray(pendingNode.value)) {
      nodes[pendingNode.id] = ["array", denseUnknownArray(pendingNode.value).map(encodeValue)];
      continue;
    }
    const record = plainUnknownRecord(pendingNode.value);
    nodes[pendingNode.id] = [
      "record",
      recordDataEntries(record).map(([key, value]) => [key, encodeValue(value)]),
    ];
  }
  requireAcyclicQueryGraph(root, nodes);
  return JSON.stringify({ format: "effect-view-server-query-graph-v1", nodes, root });
};

type DecodedQueryGraphValue =
  | { readonly _tag: "value"; readonly value: null | string | boolean | number }
  | { readonly _tag: "reference"; readonly id: number };

type DecodedQueryGraphNode =
  | {
      readonly _tag: "array";
      readonly target: Array<unknown>;
      readonly values: ReadonlyArray<DecodedQueryGraphValue>;
    }
  | {
      readonly _tag: "record";
      readonly target: Record<string, unknown>;
      readonly entries: ReadonlyArray<{
        readonly key: string;
        readonly value: DecodedQueryGraphValue;
      }>;
    };

const decodedQueryGraphNodeReferences = (node: DecodedQueryGraphNode): ReadonlyArray<number> => {
  const references: Array<number> = [];
  if (node._tag === "array") {
    for (const value of node.values) {
      if (value._tag === "reference") {
        references.push(value.id);
      }
    }
    return references;
  }
  for (const entry of node.entries) {
    if (entry.value._tag === "reference") {
      references.push(entry.value.id);
    }
  }
  return references;
};

const requireValidDecodedQueryGraph = (
  root: DecodedQueryGraphValue,
  nodes: ReadonlyArray<DecodedQueryGraphNode>,
): void => {
  const references = nodes.map(decodedQueryGraphNodeReferences);
  const frames: Array<QueryGraphCycleFrame> =
    root._tag === "reference" ? [{ _tag: "enter", id: root.id }] : [];
  const active = new Set<number>();
  const complete = new Set<number>();
  while (frames.length > 0) {
    const frame = frames.pop()!;
    if (frame._tag === "exit") {
      active.delete(frame.id);
      complete.add(frame.id);
      continue;
    }
    if (active.has(frame.id)) {
      throw new TypeError("Query graph must not contain cycles.");
    }
    if (complete.has(frame.id)) {
      continue;
    }
    active.add(frame.id);
    frames.push({ _tag: "exit", id: frame.id });
    for (const id of references[frame.id]!) {
      frames.push({ _tag: "enter", id });
    }
  }
  if (complete.size !== nodes.length) {
    throw new TypeError("Query graph must not contain unreachable nodes.");
  }
};

const decodeQueryGraphValue = (input: unknown, nodeCount: number): DecodedQueryGraphValue => {
  const value = denseUnknownArray(input);
  const tag = value[0];
  if (tag === "null" && value.length === 1) {
    return { _tag: "value", value: null };
  }
  if (tag === "string" && value.length === 2 && typeof value[1] === "string") {
    return { _tag: "value", value: value[1] };
  }
  if (tag === "boolean" && value.length === 2 && typeof value[1] === "boolean") {
    return { _tag: "value", value: value[1] };
  }
  if (
    tag === "number" &&
    value.length === 2 &&
    typeof value[1] === "number" &&
    Number.isFinite(value[1])
  ) {
    return { _tag: "value", value: value[1] };
  }
  if (
    tag === "reference" &&
    value.length === 2 &&
    typeof value[1] === "number" &&
    Number.isSafeInteger(value[1]) &&
    value[1] >= 0 &&
    value[1] < nodeCount
  ) {
    return { _tag: "reference", id: value[1] };
  }
  throw new TypeError("Invalid query graph value.");
};

export const decodeQueryGraph = (input: string): unknown => {
  const envelope = plainUnknownRecord(JSON.parse(input));
  const entries = recordDataEntries(envelope);
  if (entries.length !== 3 || envelope["format"] !== "effect-view-server-query-graph-v1") {
    throw new TypeError("Invalid query graph envelope.");
  }
  const encodedNodes = denseUnknownArray(envelope["nodes"]);
  const decodedNodes: ReadonlyArray<DecodedQueryGraphNode> = encodedNodes.map((node) => {
    const encodedNode = denseUnknownArray(node);
    if (encodedNode[0] === "array" && encodedNode.length === 2) {
      return {
        _tag: "array",
        target: [],
        values: denseUnknownArray(encodedNode[1]).map((value) =>
          decodeQueryGraphValue(value, encodedNodes.length),
        ),
      };
    }
    if (encodedNode[0] === "record" && encodedNode.length === 2) {
      const seen = new Set<string>();
      const entries = denseUnknownArray(encodedNode[1]).map((entry) => {
        const pair = denseUnknownArray(entry);
        const key = pair[0];
        if (pair.length !== 2 || typeof key !== "string" || seen.has(key)) {
          throw new TypeError("Invalid query graph record entry.");
        }
        seen.add(key);
        return {
          key,
          value: decodeQueryGraphValue(pair[1], encodedNodes.length),
        };
      });
      return { _tag: "record", target: {}, entries };
    }
    throw new TypeError("Invalid query graph node.");
  });
  const decodedRoot = decodeQueryGraphValue(envelope["root"], decodedNodes.length);
  requireValidDecodedQueryGraph(decodedRoot, decodedNodes);
  const placeholders = decodedNodes.map((node) => node.target);
  const resolveValue = (value: DecodedQueryGraphValue): unknown =>
    value._tag === "reference" ? placeholders[value.id] : value.value;
  for (const node of decodedNodes) {
    if (node._tag === "array") {
      for (const value of node.values) {
        node.target.push(resolveValue(value));
      }
      continue;
    }
    for (const entry of node.entries) {
      Object.defineProperty(node.target, entry.key, {
        configurable: true,
        enumerable: true,
        value: resolveValue(entry.value),
        writable: true,
      });
    }
  }
  return decodedRoot._tag === "reference" ? placeholders[decodedRoot.id] : decodedRoot.value;
};

const QueryGraphFromString = Schema.String.pipe(
  Schema.decodeTo(Schema.Unknown, {
    decode: SchemaGetter.transformOrFail<unknown, string>((value) =>
      Effect.try({
        try: () => decodeQueryGraph(value),
        catch: () =>
          new SchemaIssue.InvalidValue(Option.none(), {
            message: "Invalid query graph.",
          }),
      }),
    ),
    encode: SchemaGetter.transformOrFail<string, unknown>((value) =>
      Effect.try({
        try: () => encodeQueryGraph(value),
        catch: () =>
          new SchemaIssue.InvalidValue(Option.none(), {
            message: "Query must be JSON-safe.",
          }),
      }),
    ),
  }),
);

export const ViewServerWireRawQuerySchema = Schema.Struct({
  select: Schema.Array(Schema.String),
  where: Schema.optionalKey(Schema.Array(Schema.Json)),
  routeBy: Schema.optionalKey(Schema.Record(Schema.String, Schema.Json)),
  orderBy: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        field: Schema.String,
        direction: Schema.Literals(["asc", "desc"]),
      }),
    ),
  ),
  offset: Schema.optionalKey(Schema.Number),
  limit: Schema.optionalKey(Schema.Number),
});

export type ViewServerWireRawQuery = typeof ViewServerWireRawQuerySchema.Type;

export const ViewServerWireAggregateSchema = Schema.Union([
  Schema.Struct({
    aggFunc: Schema.Literal("count"),
  }),
  Schema.Struct({
    aggFunc: Schema.Literals(["countDistinct", "sum", "avg", "min", "max"]),
    field: Schema.String,
  }),
]);

export type ViewServerWireAggregate = typeof ViewServerWireAggregateSchema.Type;

export const ViewServerWireGroupedQuerySchema = Schema.Struct({
  groupBy: Schema.Array(Schema.String),
  aggregates: Schema.Record(Schema.String, ViewServerWireAggregateSchema),
  where: Schema.optionalKey(Schema.Array(Schema.Json)),
  routeBy: Schema.optionalKey(Schema.Record(Schema.String, Schema.Json)),
  orderBy: Schema.optionalKey(
    Schema.Array(
      Schema.Union([
        Schema.Struct({
          field: Schema.String,
          direction: Schema.Literals(["asc", "desc"]),
        }),
        Schema.Struct({
          aggregate: Schema.String,
          direction: Schema.Literals(["asc", "desc"]),
        }),
      ]),
    ),
  ),
  offset: Schema.optionalKey(Schema.Number),
  limit: Schema.optionalKey(Schema.Number),
});

export type ViewServerWireGroupedQuery = typeof ViewServerWireGroupedQuerySchema.Type;
export type ViewServerWireLiveQuery = ViewServerWireRawQuery | ViewServerWireGroupedQuery;

export type ViewServerEventRawQuery = Pick<ViewServerWireRawQuery, "select">;
export type ViewServerEventGroupedQuery = Pick<
  ViewServerWireGroupedQuery,
  "groupBy" | "aggregates"
>;
export type ViewServerEventQuery = ViewServerEventRawQuery | ViewServerEventGroupedQuery;

export const ViewServerSubscribePayloadSchema = Schema.Struct({
  topic: Schema.String,
  // Keep the decoded value loose so excess query keys reach strict query validation. Encoding the
  // query as one JSON string prevents RPC's generic JSON validator from recursively walking deep
  // filter expressions before the iterative filter codec can validate them.
  query: QueryGraphFromString,
});

export const ViewServerHealthQuerySchema = Schema.Struct({
  select: Schema.Array(Schema.String),
});

export const LooseWireRawQuerySchema = Schema.Struct({
  select: Schema.Array(Schema.String),
  where: Schema.optionalKey(Schema.Array(Schema.Unknown)),
  routeBy: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
  orderBy: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        field: Schema.String,
        direction: Schema.Literals(["asc", "desc"]),
      }),
    ),
  ),
  offset: Schema.optionalKey(Schema.Number),
  limit: Schema.optionalKey(Schema.Number),
});

export type LooseWireRawQuery = typeof LooseWireRawQuerySchema.Type;

export const LooseWireGroupedQuerySchema = Schema.Struct({
  groupBy: Schema.Array(Schema.String),
  aggregates: Schema.Record(Schema.String, ViewServerWireAggregateSchema),
  where: Schema.optionalKey(Schema.Array(Schema.Unknown)),
  routeBy: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
  orderBy: Schema.optionalKey(
    Schema.Array(
      Schema.Union([
        Schema.Struct({
          field: Schema.String,
          direction: Schema.Literals(["asc", "desc"]),
        }),
        Schema.Struct({
          aggregate: Schema.String,
          direction: Schema.Literals(["asc", "desc"]),
        }),
      ]),
    ),
  ),
  offset: Schema.optionalKey(Schema.Number),
  limit: Schema.optionalKey(Schema.Number),
});

export type LooseWireGroupedQuery = typeof LooseWireGroupedQuerySchema.Type;
