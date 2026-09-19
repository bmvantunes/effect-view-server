import { create, toBinary } from "@bufbuild/protobuf";
import { StringValueSchema } from "@bufbuild/protobuf/wkt";
import { Schema } from "effect";

export const Corpus = Schema.Struct({
  version: Schema.Literal(1),
  seed: Schema.Literal(494),
  topics: Schema.Literal(10),
  rowsPerTopic: Schema.Number.check(
    Schema.isInt(),
    Schema.isBetween({ minimum: 1, maximum: 50_000_000 }),
  ),
  minBytes: Schema.Literal(200),
  maxBytes: Schema.Literal(1024),
  protobuf: Schema.Literal("google.protobuf.StringValue"),
  partitionsPerTopic: Schema.Literal(1),
});

export type Corpus = typeof Corpus.Type;

// Version 1: xorshift32 printable ASCII. The recipe and row index completely
// determine the bytes; no expanded fixture or random state is stored on disk.
export function payload(index: number): string {
  const size = 200 + (index % 825);
  let state = (index + 494) >>> 0;
  let result = "";
  for (let offset = 0; offset < size - 3; offset += 1) {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    result += String.fromCharCode(33 + ((state >>> 0) % 90));
  }
  return result;
}

export const wireRow = (index: number): Uint8Array =>
  toBinary(StringValueSchema, create(StringValueSchema, { value: payload(index) }));

export const topicNames = (rows: number): string[] =>
  Array.from({ length: 10 }, (_, index) => `evs-capacity-v1-${rows}-${index + 1}`);

export const Scenario = Schema.Struct({
  topics: Schema.Literals([1, 3, 10]),
  rows: Schema.Number.check(Schema.isInt(), Schema.isGreaterThan(0)),
  seconds: Schema.Number.check(Schema.isGreaterThan(0)),
  messagesPerSecond: Schema.Number.check(Schema.isGreaterThan(0)),
  cpuSeconds: Schema.Number.check(Schema.isGreaterThanOrEqualTo(0)),
  averageCpuCores: Schema.Number.check(Schema.isGreaterThanOrEqualTo(0)),
  peakRssBytes: Schema.Number.check(Schema.isGreaterThan(0)),
});

export type Scenario = typeof Scenario.Type;

export const Baseline = Schema.Struct({
  corpus: Corpus,
  node: Schema.String,
  platform: Schema.String,
  arch: Schema.String,
  cpu: Schema.String,
  logicalCpus: Schema.Number,
  totalMemoryBytes: Schema.Number,
  heapLimitBytes: Schema.Number.check(Schema.isGreaterThan(0)),
  revision: Schema.String,
  scenarios: Schema.Array(Scenario),
});

export type Baseline = typeof Baseline.Type;

export function report(current: Baseline, previous?: Baseline): string {
  if (
    previous !== undefined &&
    JSON.stringify(current.corpus) !== JSON.stringify(previous.corpus)
  ) {
    throw new Error("Cannot compare different corpus recipes or row counts.");
  }
  const single = current.scenarios.find((scenario) => scenario.topics === 1);
  if (
    single === undefined ||
    current.scenarios.length !== 3 ||
    new Set(current.scenarios.map((scenario) => scenario.topics)).size !== 3
  ) {
    throw new Error("A completed report requires the 1-, 3-, and 10-topic scenarios.");
  }
  const lines = current.scenarios.map((scenario) => {
    const prior = previous?.scenarios.find((entry) => entry.topics === scenario.topics);
    if (previous !== undefined && prior === undefined) {
      throw new Error("Baseline is missing a scenario.");
    }
    const delta =
      prior === undefined
        ? "—"
        : `${((scenario.messagesPerSecond / prior.messagesPerSecond - 1) * 100).toFixed(2)}%`;
    return `| ${scenario.topics} | ${scenario.rows} | ${scenario.seconds.toFixed(3)} | ${scenario.messagesPerSecond.toFixed(0)} | ${(scenario.messagesPerSecond / single.messagesPerSecond).toFixed(2)}x | ${scenario.averageCpuCores.toFixed(2)} | ${(scenario.peakRssBytes / 2 ** 30).toFixed(2)} | ${delta} |`;
  });
  return [
    "# Kafka retained-capacity benchmark",
    "",
    `Revision: ${current.revision}. Node ${current.node}; ${current.platform}/${current.arch}; ${current.cpu}; ${current.logicalCpus} logical CPUs; ${(current.totalMemoryBytes / 2 ** 30).toFixed(1)} GiB host RAM; ${(current.heapLimitBytes / 2 ** 30).toFixed(1)} GiB V8 heap limit.`,
    "",
    `Corpus v${current.corpus.version}, seed ${current.corpus.seed}: ${current.corpus.rowsPerTopic} distinct retained rows per topic; protobuf payloads 200–1024 bytes; one partition per topic.`,
    "",
    "| Topics | Rows | Seconds | Messages/sec | Throughput / single | Average CPU cores | Peak RSS GiB | Throughput change |",
    "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ...lines,
    "",
    "Scope: localhost broker + disk + CPU/GC stress, one production runtime process per scenario. Scenarios run serially; topics within a scenario load concurrently. No worker processes are added to the production runtime. CPU cores = process CPU seconds / elapsed seconds; this includes V8 and native threads, excludes Kafka, and does not prove JavaScript decode runs in parallel.",
    "",
    "Timing starts before runtime creation and ends after post-application Kafka commits and bounded snapshots verify exact retained counts and the final row payload for every topic. It includes protobuf decoding, mapping, Effect schema validation, engine storage/indexing, runtime startup, verification queries, and up to one polling interval. Seeding and runtime teardown are excluded. No full-table result is materialized for verification.",
    "",
    "Peak RSS is the scenario process high-water mark. A fresh process is used for every scenario. Broker/page caches are not flushed; results are single samples, not statistical confidence intervals. Run repeatedly on the same otherwise-idle machine to assess variance. The fixture builds the engine's normal row-key index; it does not prebuild every possible query-specific index.",
    "",
    ...(previous === undefined
      ? []
      : [
          `Compared with revision ${previous.revision} on ${previous.platform}/${previous.arch}, ${previous.cpu}, Node ${previous.node}. Hardware, runtime flags, broker resources and cache state can affect comparisons.`,
          "",
        ]),
  ].join("\n");
}
