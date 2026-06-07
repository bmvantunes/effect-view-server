// Benchmarks intentionally import Vitest directly: @effect/vitest does not expose `bench`.
import { afterAll, beforeAll, bench, describe, expect } from "vitest";
import { defineViewServerConfig } from "@view-server/config";
import { format as formatBigDecimal, isBigDecimal, type BigDecimal } from "effect/BigDecimal";
import { Cause, Effect, Exit, Schema, Scope, Stream } from "effect";
import {
  createColumnLiveViewEngine,
  type ColumnLiveViewEngine,
  type ColumnLiveViewEngineEvent,
  type ColumnLiveViewSubscription,
} from "./index";
import {
  backpressureCountFromEngineHealth,
  benchmarkOutputJsonPath,
  cleanupLeakCountFromEngineHealth,
  failOnBenchmarkCleanupLeaks,
  isBenchmarkEngineHealth,
  memorySnapshot,
  queuedEventCountFromEngineHealth,
  writeBenchmarkArtifact,
  type BenchmarkMemorySnapshot,
} from "./benchmark-artifact";
import { fieldValue } from "./row-values";

declare const process: {
  readonly env: Record<string, string | undefined>;
};

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  desk: Schema.String,
  price: Schema.Finite,
  quantity: Schema.BigInt,
  region: Schema.String,
  riskScore: Schema.Finite,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  updatedAt: Schema.Number,
  volume: Schema.Finite,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

type Topics = typeof viewServer.topics;
type Engine = ColumnLiveViewEngine<Topics>;
type OrderRow = typeof Order.Type;
type OrderStatus = OrderRow["status"];
type StatusAggregateRow = {
  readonly averagePrice: BigDecimal;
  readonly averageQuantity: BigDecimal;
  readonly distinctRegions: bigint;
  readonly maxPrice: number;
  readonly maxQuantity: bigint;
  readonly maxUpdatedAt: number;
  readonly minPrice: number;
  readonly minQuantity: bigint;
  readonly minUpdatedAt: number;
  readonly rowCount: bigint;
  readonly status: OrderStatus;
  readonly totalPrice: BigDecimal;
  readonly totalQuantity: bigint;
};
type StatusAggregateSubscription = ColumnLiveViewSubscription<StatusAggregateRow>;
type StatusAggregateEvent = ColumnLiveViewEngineEvent<StatusAggregateRow>;
type StatusAggregateDeltaEvent = Extract<StatusAggregateEvent, { readonly type: "delta" }>;
type StatusAggregateDeltaOperation = StatusAggregateDeltaEvent["operations"][number];
type StatusAggregateEventReader = (
  count: number,
) => Effect.Effect<ReadonlyArray<StatusAggregateEvent>, Cause.Done>;
type ValidationSummary = {
  readonly filteredMatchedRows: string;
  readonly filteredTotalRows: number;
  readonly filteredWindowRows: number;
  readonly highCardinalityTotalRows: number;
  readonly highCardinalityWindowRows: number;
  readonly initialEventCount: number;
  readonly initialEventTotalRows: number;
  readonly initialEventType: "snapshot";
  readonly liveInitialMatchedRows: string;
  readonly liveInitialOpenRowCount: string;
  readonly regionStatusRows: number;
  readonly statusMatchedRows: string;
  readonly statusRows: number;
  readonly statusTotalPriceRows: number;
  readonly statusWindowRows: number;
  readonly zeroLimitRows: number;
  readonly zeroLimitWindowRows: number;
};
type LiveDeltaValidation = {
  readonly fromVersion: number;
  readonly operationStatuses: ReadonlyArray<unknown>;
  readonly operationTypes: ReadonlyArray<string>;
  readonly rowCounts: ReadonlyArray<string>;
  readonly toVersion: number;
  readonly totalRows: number;
};

type BenchmarkProfile = {
  readonly rowCount: number;
  engine: Engine | undefined;
  eventReader: StatusAggregateEventReader | undefined;
  liveDeltaValidations: Array<LiveDeltaValidation>;
  liveInitialVersion: number | undefined;
  memoryAfterSetup: BenchmarkMemorySnapshot | undefined;
  nextLiveIndex: number;
  scope: Scope.Closeable | undefined;
  subscription: StatusAggregateSubscription | undefined;
  validation: ValidationSummary | undefined;
};

const defaultBatchSize = 10_000;
const defaultBenchmarkTimeMs = 250;
const defaultIterations = 5;
const defaultRowCount = 100_000;
const defaultWarmupIterations = 0;
const defaultWarmupTimeMs = 0;
const groupedLiveIncrementalMemberTarget = 60_000;
const maxDeskGroupCount = 100_000;
const minimumRowCount = 128;
const priceDomainMax = 1_000_000;

const positiveIntegerFromEnv = (name: string, fallback: number): number => {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const trimmed = raw.trim();
  if (!/^[1-9]\d*$/u.test(trimmed)) {
    throw new Error(`${name} must be a positive integer.`);
  }
  const parsed = Number.parseInt(trimmed, 10);
  if (Number.isSafeInteger(parsed) && parsed > 0) {
    return parsed;
  }
  throw new Error(`${name} must be a positive integer.`);
};

const nonNegativeIntegerFromEnv = (name: string, fallback: number): number => {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const trimmed = raw.trim();
  if (!/^(0|[1-9]\d*)$/u.test(trimmed)) {
    throw new Error(`${name} must be a non-negative integer.`);
  }
  const parsed = Number.parseInt(trimmed, 10);
  if (Number.isSafeInteger(parsed) && parsed >= 0) {
    return parsed;
  }
  throw new Error(`${name} must be a non-negative integer.`);
};

const rowCountFromEnv = (): number => {
  const raw = process.env["VIEW_SERVER_ENGINE_BENCH_ROWS"];
  if (raw === undefined || raw.trim() === "") {
    return defaultRowCount;
  }
  if (raw.includes(",")) {
    throw new Error("VIEW_SERVER_ENGINE_BENCH_ROWS accepts one row count per benchmark run.");
  }
  const rowCount = positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_ROWS", defaultRowCount);
  if (rowCount >= minimumRowCount) {
    return rowCount;
  }
  throw new Error(`VIEW_SERVER_ENGINE_BENCH_ROWS must be at least ${minimumRowCount}.`);
};

const benchmarkRowCount = rowCountFromEnv();
const batchSize = positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE", defaultBatchSize);
const outputJsonPath = benchmarkOutputJsonPath(`grouped-aggregate-${benchmarkRowCount}rows.json`);
const memoryBefore = memorySnapshot();
const benchOptions = {
  iterations: positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_ITERATIONS", defaultIterations),
  time: nonNegativeIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_TIME_MS", defaultBenchmarkTimeMs),
  warmupIterations: nonNegativeIntegerFromEnv(
    "VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS",
    defaultWarmupIterations,
  ),
  warmupTime: nonNegativeIntegerFromEnv(
    "VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS",
    defaultWarmupTimeMs,
  ),
};
const liveBenchOptions = {
  iterations: benchOptions.iterations,
  time: 0,
  warmupIterations: 0,
  warmupTime: 0,
};

const profile: BenchmarkProfile = {
  rowCount: benchmarkRowCount,
  engine: undefined,
  eventReader: undefined,
  liveDeltaValidations: [],
  liveInitialVersion: undefined,
  memoryAfterSetup: undefined,
  nextLiveIndex: benchmarkRowCount,
  scope: undefined,
  subscription: undefined,
  validation: undefined,
};

const orderStatus = (index: number): OrderStatus => {
  if (index % 5 === 0) {
    return "cancelled";
  }
  if (index % 3 === 0) {
    return "closed";
  }
  return "open";
};

const region = (index: number): string => {
  if (index % 7 === 0) {
    return "apac";
  }
  if (index % 5 === 0) {
    return "amer";
  }
  return "emea";
};

const deskGroupCount = (rowCount: number): number =>
  Math.min(maxDeskGroupCount, Math.max(1, Math.floor(rowCount / 4)));

const priceDomainSize = (rowCount: number): number => Math.min(rowCount, priceDomainMax);

const rowCountPriceCycles = (rowCount: number): number =>
  Math.max(1, Math.ceil(rowCount / priceDomainMax));

const filteredTailSize = (rowCount: number): number =>
  Math.max(
    1,
    Math.min(
      1_000,
      priceDomainSize(rowCount),
      Math.floor(groupedLiveIncrementalMemberTarget / rowCountPriceCycles(rowCount)),
    ),
  );

const filteredPriceThreshold = (rowCount: number): number =>
  priceDomainSize(rowCount) - filteredTailSize(rowCount);

const seedOrder = (index: number): OrderRow => ({
  id: `order-${index}`,
  customerId: `customer-${index % 100_000}`,
  desk: `desk-${index % deskGroupCount(benchmarkRowCount)}`,
  price: index % 1_000_000,
  quantity: BigInt((index % 10_000) + 1),
  region: region(index),
  riskScore: (index % 10_000) / 10_000,
  status: orderStatus(index),
  updatedAt: index,
  volume: index % 50_000,
});

const liveDeltaOrder = (index: number): OrderRow => ({
  id: `live-${index}`,
  customerId: `customer-live-${index}`,
  desk: `desk-${index % deskGroupCount(benchmarkRowCount)}`,
  price: 10_000_000 + index,
  quantity: BigInt((index % 10_000) + 1),
  region: "emea",
  riskScore: 1,
  status: "open",
  updatedAt: 10_000_000_000 + index,
  volume: 50_000 + (index % 50_000),
});

const groupByStatusAggregateFields = {
  averagePrice: { aggFunc: "avg", field: "price" },
  averageQuantity: { aggFunc: "avg", field: "quantity" },
  distinctRegions: { aggFunc: "countDistinct", field: "region" },
  maxPrice: { aggFunc: "max", field: "price" },
  maxQuantity: { aggFunc: "max", field: "quantity" },
  maxUpdatedAt: { aggFunc: "max", field: "updatedAt" },
  minPrice: { aggFunc: "min", field: "price" },
  minQuantity: { aggFunc: "min", field: "quantity" },
  minUpdatedAt: { aggFunc: "min", field: "updatedAt" },
  rowCount: { aggFunc: "count" },
  totalPrice: { aggFunc: "sum", field: "price" },
  totalQuantity: { aggFunc: "sum", field: "quantity" },
} as const;

const groupByStatusOrder = [
  { aggregate: "totalPrice", direction: "desc" },
  { field: "status", direction: "asc" },
] as const;

const statusAggregateQuery = () =>
  ({
    groupBy: ["status"],
    aggregates: groupByStatusAggregateFields,
    orderBy: groupByStatusOrder,
    limit: 50,
  }) as const;

const liveStatusAggregateQuery = () =>
  ({
    groupBy: ["status"],
    aggregates: groupByStatusAggregateFields,
    where: {
      region: { eq: "emea" },
      price: { gte: filteredPriceThreshold(benchmarkRowCount) },
    },
    orderBy: groupByStatusOrder,
    limit: 50,
  }) as const;

const regionStatusAggregateQuery = () =>
  ({
    groupBy: ["region", "status"],
    aggregates: {
      averagePrice: { aggFunc: "avg", field: "price" },
      maxUpdatedAt: { aggFunc: "max", field: "updatedAt" },
      minUpdatedAt: { aggFunc: "min", field: "updatedAt" },
      rowCount: { aggFunc: "count" },
      totalPrice: { aggFunc: "sum", field: "price" },
      totalQuantity: { aggFunc: "sum", field: "quantity" },
    },
    orderBy: [
      { aggregate: "rowCount", direction: "desc" },
      { field: "region", direction: "asc" },
      { field: "status", direction: "asc" },
    ],
    limit: 50,
  }) as const;

const highCardinalityDeskQuery = () =>
  ({
    groupBy: ["desk"],
    aggregates: {
      averageRiskScore: { aggFunc: "avg", field: "riskScore" },
      maxVolume: { aggFunc: "max", field: "volume" },
      minVolume: { aggFunc: "min", field: "volume" },
      rowCount: { aggFunc: "count" },
      totalVolume: { aggFunc: "sum", field: "volume" },
    },
    orderBy: [
      { aggregate: "rowCount", direction: "desc" },
      { field: "desk", direction: "asc" },
    ],
    limit: 50,
  }) as const;

const highCardinalityDeskCountOnlyQuery = () =>
  ({
    groupBy: ["desk"],
    aggregates: {
      rowCount: { aggFunc: "count" },
    },
    limit: 0,
  }) as const;

const filteredStatusAggregateQuery = () =>
  ({
    groupBy: ["status"],
    aggregates: {
      averagePrice: { aggFunc: "avg", field: "price" },
      rowCount: { aggFunc: "count" },
      totalPrice: { aggFunc: "sum", field: "price" },
    },
    where: {
      region: { eq: "emea" },
      price: { gte: filteredPriceThreshold(benchmarkRowCount) },
    },
    orderBy: [
      { aggregate: "totalPrice", direction: "desc" },
      { field: "status", direction: "asc" },
    ],
    limit: 50,
  }) as const;

const seedEngine = Effect.fn("ColumnLiveViewEngine.bench.groupedAggregate.seed")(function* (
  engine: Engine,
  rowCount: number,
) {
  let next = 0;
  while (next < rowCount) {
    const count = Math.min(batchSize, rowCount - next);
    const rows = Array.from({ length: count }, (_value, offset) => seedOrder(next + offset));
    yield* engine.publishMany("orders", rows);
    next += count;
  }
});

const profileEngine = (benchmarkProfile: BenchmarkProfile): Engine => {
  if (benchmarkProfile.engine === undefined) {
    throw new Error(
      `Grouped aggregate benchmark ${benchmarkProfile.rowCount} rows is not initialized.`,
    );
  }
  return benchmarkProfile.engine;
};

const profileEventReader = (benchmarkProfile: BenchmarkProfile): StatusAggregateEventReader => {
  if (benchmarkProfile.eventReader === undefined) {
    throw new Error(
      `Grouped aggregate benchmark ${benchmarkProfile.rowCount} rows has no event reader.`,
    );
  }
  return benchmarkProfile.eventReader;
};

const bigintRowField = (row: object, field: string): bigint => {
  const value = fieldValue(row, field);
  if (typeof value === "bigint") {
    return value;
  }
  throw new Error(`Expected bigint row field ${field}.`);
};

const bigDecimalRowField = (row: object, field: string): string => {
  const value = fieldValue(row, field);
  if (isBigDecimal(value)) {
    return formatBigDecimal(value);
  }
  throw new Error(`Expected BigDecimal row field ${field}.`);
};

const sumBigintFields = (rows: ReadonlyArray<object>, field: string): bigint => {
  let total = 0n;
  for (const row of rows) {
    total += bigintRowField(row, field);
  }
  return total;
};

const groupedBigintField = (
  rows: ReadonlyArray<object>,
  groupField: string,
  groupValue: string,
  valueField: string,
): bigint => {
  for (const row of rows) {
    if (fieldValue(row, groupField) === groupValue) {
      return bigintRowField(row, valueField);
    }
  }
  throw new Error(`Expected grouped row ${groupField}=${groupValue}.`);
};

const initialSnapshotValidation = (
  events: ReadonlyArray<StatusAggregateEvent>,
): {
  readonly totalRows: number;
  readonly type: "snapshot";
  readonly version: number;
} => {
  const event = events[0];
  if (events.length !== 1 || event === undefined || event.type !== "snapshot") {
    throw new Error("Expected exactly one grouped initial snapshot event.");
  }
  return {
    totalRows: event.totalRows,
    type: event.type,
    version: event.version,
  };
};

const operationStatus = (operation: StatusAggregateDeltaOperation): unknown => {
  if (operation.type !== "insert" && operation.type !== "update") {
    return undefined;
  }
  return fieldValue(operation.row, "status");
};

const operationRowCount = (operation: StatusAggregateDeltaOperation): string => {
  if (operation.type !== "insert" && operation.type !== "update") {
    return "";
  }
  return bigintRowField(operation.row, "rowCount").toString();
};

const liveDeltaValidation = (events: ReadonlyArray<StatusAggregateEvent>): LiveDeltaValidation => {
  const event = events[0];
  if (events.length !== 1 || event === undefined || event.type !== "delta") {
    throw new Error("Expected exactly one grouped delta event.");
  }
  return {
    fromVersion: event.fromVersion,
    operationStatuses: event.operations.map(operationStatus),
    operationTypes: event.operations.map((operation) => operation.type),
    rowCounts: event.operations.map(operationRowCount),
    toVersion: event.toVersion,
    totalRows: event.totalRows,
  };
};

const makeEventReader = (
  subscription: StatusAggregateSubscription,
  scope: Scope.Closeable,
): Effect.Effect<StatusAggregateEventReader> =>
  Stream.toPull(subscription.events).pipe(
    Effect.map(
      (pull): StatusAggregateEventReader =>
        (count) =>
          Effect.gen(function* () {
            const events: Array<StatusAggregateEvent> = [];
            while (events.length < count) {
              const chunk = yield* pull;
              events.push(...chunk);
            }
            if (events.length !== count) {
              throw new Error(`Expected ${count} event(s), pulled ${events.length}.`);
            }
            return events;
          }),
    ),
    Effect.provideService(Scope.Scope, scope),
  );

beforeAll(async () => {
  const engine = Effect.runSync(createColumnLiveViewEngine({ topics: viewServer.topics }));
  await Effect.runPromise(seedEngine(engine, profile.rowCount));

  const statusSnapshot = await Effect.runPromise(engine.snapshot("orders", statusAggregateQuery()));
  const regionStatusSnapshot = await Effect.runPromise(
    engine.snapshot("orders", regionStatusAggregateQuery()),
  );
  const highCardinalitySnapshot = await Effect.runPromise(
    engine.snapshot("orders", highCardinalityDeskQuery()),
  );
  const zeroLimitSnapshot = await Effect.runPromise(
    engine.snapshot("orders", highCardinalityDeskCountOnlyQuery()),
  );
  const filteredSnapshot = await Effect.runPromise(
    engine.snapshot("orders", filteredStatusAggregateQuery()),
  );
  const liveInitialSnapshot = await Effect.runPromise(
    engine.snapshot("orders", liveStatusAggregateQuery()),
  );

  const subscription = await Effect.runPromise(
    engine.subscribe("orders", liveStatusAggregateQuery()),
  );
  const scope = Effect.runSync(Scope.make("parallel"));
  const eventReader = await Effect.runPromise(makeEventReader(subscription, scope));
  const initialEvents = await Effect.runPromise(eventReader(1));
  const initialValidation = initialSnapshotValidation(initialEvents);

  profile.engine = engine;
  profile.eventReader = eventReader;
  profile.liveInitialVersion = initialValidation.version;
  profile.memoryAfterSetup = memorySnapshot();
  profile.scope = scope;
  profile.subscription = subscription;
  profile.validation = {
    filteredMatchedRows: sumBigintFields(filteredSnapshot.rows, "rowCount").toString(),
    filteredTotalRows: filteredSnapshot.totalRows,
    filteredWindowRows: filteredSnapshot.rows.length,
    highCardinalityTotalRows: highCardinalitySnapshot.totalRows,
    highCardinalityWindowRows: highCardinalitySnapshot.rows.length,
    initialEventCount: initialEvents.length,
    initialEventTotalRows: initialValidation.totalRows,
    initialEventType: initialValidation.type,
    liveInitialMatchedRows: sumBigintFields(liveInitialSnapshot.rows, "rowCount").toString(),
    liveInitialOpenRowCount: groupedBigintField(
      liveInitialSnapshot.rows,
      "status",
      "open",
      "rowCount",
    ).toString(),
    regionStatusRows: regionStatusSnapshot.totalRows,
    statusMatchedRows: sumBigintFields(statusSnapshot.rows, "rowCount").toString(),
    statusRows: statusSnapshot.totalRows,
    statusTotalPriceRows: statusSnapshot.rows.map((row) => bigDecimalRowField(row, "totalPrice"))
      .length,
    statusWindowRows: statusSnapshot.rows.length,
    zeroLimitRows: zeroLimitSnapshot.totalRows,
    zeroLimitWindowRows: zeroLimitSnapshot.rows.length,
  };
}, 0);

afterAll(async () => {
  const memoryAfterSetup = profile.memoryAfterSetup ?? memoryBefore;
  const validation = profile.validation;
  const expectedDeskGroupCount = deskGroupCount(profile.rowCount);
  const expectedLiveVersions = Array.from(
    { length: benchOptions.iterations },
    (_value, index) => (profile.liveInitialVersion ?? 0) + index,
  );
  const initialOpenRowCount = BigInt(validation?.liveInitialOpenRowCount ?? "0");
  expect(validation?.filteredMatchedRows === "0").toBe(false);
  expect(validation?.filteredTotalRows === 0).toBe(false);
  expect(validation?.filteredWindowRows === 0).toBe(false);
  expect(validation?.liveInitialMatchedRows === "0").toBe(false);
  expect(
    BigInt(validation?.liveInitialMatchedRows ?? "0") <= BigInt(groupedLiveIncrementalMemberTarget),
  ).toBe(true);
  expect(validation).toStrictEqual({
    filteredMatchedRows: validation?.filteredMatchedRows,
    filteredTotalRows: validation?.filteredTotalRows,
    filteredWindowRows: validation?.filteredWindowRows,
    highCardinalityTotalRows: expectedDeskGroupCount,
    highCardinalityWindowRows: Math.min(50, expectedDeskGroupCount),
    initialEventCount: 1,
    initialEventTotalRows: validation?.filteredTotalRows,
    initialEventType: "snapshot",
    liveInitialMatchedRows: validation?.liveInitialMatchedRows,
    liveInitialOpenRowCount: validation?.liveInitialOpenRowCount,
    regionStatusRows: 6,
    statusMatchedRows: profile.rowCount.toString(),
    statusRows: 3,
    statusTotalPriceRows: 3,
    statusWindowRows: 3,
    zeroLimitRows: expectedDeskGroupCount,
    zeroLimitWindowRows: 0,
  });
  expect(profile.liveDeltaValidations).toStrictEqual(
    expectedLiveVersions.map((fromVersion, index) => ({
      fromVersion,
      operationStatuses: ["open"],
      operationTypes: ["update"],
      rowCounts: [(initialOpenRowCount + BigInt(index + 1)).toString()],
      toVersion: fromVersion + 1,
      totalRows: validation?.filteredTotalRows,
    })),
  );
  if (profile.engine !== undefined) {
    const healthBeforeCleanup = await Effect.runPromise(profile.engine.health());
    expect(isBenchmarkEngineHealth(healthBeforeCleanup)).toBe(true);
    const benchmarkHealthBeforeCleanup = isBenchmarkEngineHealth(healthBeforeCleanup)
      ? healthBeforeCleanup
      : undefined;
    expect(benchmarkHealthBeforeCleanup?.activeSubscriptions).toBe(1);
    expect(benchmarkHealthBeforeCleanup?.backpressureEvents).toBe(0);
    expect(benchmarkHealthBeforeCleanup?.queuedEvents).toBe(0);
  }
  if (profile.subscription !== undefined) {
    await Effect.runPromise(profile.subscription.close());
    profile.subscription = undefined;
  }
  if (profile.scope !== undefined) {
    await Effect.runPromise(Scope.close(profile.scope, Exit.void));
    profile.scope = undefined;
  }
  let health: unknown = {
    status: "not-started",
  };
  if (profile.engine !== undefined) {
    health = await Effect.runPromise(profile.engine.health());
    expect(isBenchmarkEngineHealth(health)).toBe(true);
    await Effect.runPromise(profile.engine.close());
    profile.engine = undefined;
  }
  profile.eventReader = undefined;
  profile.liveDeltaValidations = [];
  profile.liveInitialVersion = undefined;
  profile.memoryAfterSetup = undefined;
  profile.validation = undefined;
  const memoryAfterBenchmark = memorySnapshot();
  const liveDeltaMutationCount = profile.nextLiveIndex - profile.rowCount;
  const cleanupLeakCount = cleanupLeakCountFromEngineHealth(health);
  writeBenchmarkArtifact({
    artifactKind: "engine-benchmark-summary",
    backpressureCount: backpressureCountFromEngineHealth(health),
    benchmarkCases: [
      "status grouped count/sum/min/max/avg",
      "region+status grouped count/sum/min/max/avg",
      "high-cardinality desk grouped aggregates",
      "high-cardinality desk group count via zero-row window",
      "filtered status grouped aggregates",
      "live grouped aggregate delta after publish",
    ],
    benchmarkName: "grouped aggregate engine benchmark",
    benchmarkScope: "engine-grouped-aggregate",
    cleanupLeakCount,
    health,
    latency: {
      outputJsonPath,
      source: "vitest-output-json",
    },
    memoryAfterBenchmark,
    memoryAfterSetup,
    memoryBefore,
    mutationCount: profile.rowCount + liveDeltaMutationCount,
    notes: [
      "Latency percentiles are emitted by Vitest in outputJsonPath.",
      "Snapshot cases cover grouped count, countDistinct, sum, avg, min, max, aggregate ordering, group-key ordering, filters, and zero-row group counts.",
      "The live grouped delta case is iteration-bound with time and warmup disabled; it uses a selective filter sized to stay below the current grouped incremental member target, but the public benchmark artifact does not expose the internal execution mode directly.",
      `Live grouped delta publishes during benchmark: ${liveDeltaMutationCount}.`,
      "Write-path costs for grouped patch/delete/group-move materialized state are not covered here; future grouped materialized indexes must add matching write-path cases before adoption.",
    ],
    outputJsonPath,
    queuedEventCount: queuedEventCountFromEngineHealth(health),
    rowCount: profile.rowCount,
    subscriberCount: 1,
    topics: ["orders"],
  });
  failOnBenchmarkCleanupLeaks(cleanupLeakCount);
}, 0);

describe(`grouped aggregate engine benchmark: ${profile.rowCount} rows`, () => {
  bench(
    "status grouped count/sum/min/max/avg",
    async () => {
      await Effect.runPromise(profileEngine(profile).snapshot("orders", statusAggregateQuery()));
    },
    benchOptions,
  );

  bench(
    "region+status grouped count/sum/min/max/avg",
    async () => {
      await Effect.runPromise(
        profileEngine(profile).snapshot("orders", regionStatusAggregateQuery()),
      );
    },
    benchOptions,
  );

  bench(
    "high-cardinality desk grouped aggregates",
    async () => {
      await Effect.runPromise(
        profileEngine(profile).snapshot("orders", highCardinalityDeskQuery()),
      );
    },
    benchOptions,
  );

  bench(
    "high-cardinality desk group count via zero-row window",
    async () => {
      await Effect.runPromise(
        profileEngine(profile).snapshot("orders", highCardinalityDeskCountOnlyQuery()),
      );
    },
    benchOptions,
  );

  bench(
    "filtered status grouped aggregates",
    async () => {
      await Effect.runPromise(
        profileEngine(profile).snapshot("orders", filteredStatusAggregateQuery()),
      );
    },
    benchOptions,
  );

  bench(
    "live grouped aggregate delta after publish",
    async () => {
      const engine = profileEngine(profile);
      const readEvent = profileEventReader(profile);
      const row = liveDeltaOrder(profile.nextLiveIndex);
      profile.nextLiveIndex += 1;
      await Effect.runPromise(engine.publish("orders", row));
      const events = await Effect.runPromise(readEvent(1));
      profile.liveDeltaValidations.push(liveDeltaValidation(events));
    },
    liveBenchOptions,
  );
});
