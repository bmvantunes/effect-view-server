import { Admin, Producer } from "@platformatic/kafka";
import { StringValueSchema } from "@bufbuild/protobuf/wkt";
import { Buffer } from "node:buffer";
import { randomUUID } from "node:crypto";
import { readFileSync, writeFileSync, renameSync, existsSync, rmSync } from "node:fs";
import { join } from "node:path";
import { getHeapStatistics } from "node:v8";
import { publishCapacityReport } from "./capacity-report.ts";
import { arch, cpus, platform, totalmem } from "node:os";
import { execFileSync } from "node:child_process";
import { Clock, Effect, Schema } from "effect";
import { ViewServerId, defineViewServerConfig } from "effect-view-server/config";
import { kafka } from "effect-view-server/kafka/contract";
import { kafkaNode } from "effect-view-server/kafka/node";
import { makeViewServerRuntime } from "effect-view-server/runtime";
import {
  Baseline,
  Corpus,
  Scenario,
  payload,
  report,
  topicNames,
  wireRow,
} from "./capacity-model.ts";

class CapacityError extends Schema.TaggedError<CapacityError>()("CapacityError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

const attempt = <A>(message: string, operation: () => Promise<A>) =>
  Effect.tryPromise({ try: operation, catch: (cause) => new CapacityError({ message, cause }) });

const readJson = (path: string) =>
  Effect.try({
    try: (): unknown => JSON.parse(readFileSync(path, "utf8")),
    catch: (cause) => new CapacityError({ message: `Cannot read ${path}`, cause }),
  });

const writeJson = (path: string, value: unknown) =>
  Effect.try({
    try: () => {
      writeFileSync(`${path}.tmp`, `${JSON.stringify(value, undefined, 2)}\n`);
      renameSync(`${path}.tmp`, path);
    },
    catch: (cause) => new CapacityError({ message: `Cannot write ${path}`, cause }),
  }).pipe(
    Effect.ensuring(
      Effect.try({
        try: () => rmSync(`${path}.tmp`, { force: true }),
        catch: (cause) => new CapacityError({ message: `Cannot remove temporary ${path}`, cause }),
      }).pipe(Effect.orDie),
    ),
  );

const bootstrapServers = `localhost:${process.env["CAPACITY_KAFKA_PORT"] ?? "9092"}`;

const main = Effect.gen(function* () {
  const mode = yield* Schema.decodeUnknownEffect(
    Schema.Literals(["seed", "1", "3", "10", "report", "compare"]),
  )(process.argv[2]);
  const rows = yield* Schema.decodeUnknownEffect(Corpus.fields.rowsPerTopic)(
    Number(process.argv[3]),
  );
  const directory = yield* Schema.decodeUnknownEffect(Schema.String)(process.argv[4]);
  const recipe = yield* Schema.decodeUnknownEffect(Corpus)(
    yield* readJson("benchmarks/kafka-capacity/corpus.json"),
  );
  const corpus = { ...recipe, rowsPerTopic: rows };
  const topics = topicNames(rows);
  const manifestPath = join(directory, "corpus-ready.json");
  if (mode === "report" || mode === "compare") {
    const scenarios = yield* Effect.forEach([1, 3, 10], (count) =>
      readJson(join(directory, `scenario-${count}.json`)).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Scenario)),
      ),
    );
    const revision = yield* Effect.try({
      try: () => execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
      catch: (cause) => new CapacityError({ message: "Cannot determine revision", cause }),
    });
    const current: Baseline = {
      corpus,
      node: process.version,
      platform: platform(),
      arch: arch(),
      cpu: cpus()[0]?.model ?? "unknown",
      logicalCpus: cpus().length,
      totalMemoryBytes: totalmem(),
      heapLimitBytes: getHeapStatistics().heap_size_limit,
      revision,
      scenarios,
    };
    const baselinePath = join(directory, "current", "benchmark-baseline.json");
    const previous =
      mode === "compare"
        ? yield* Schema.decodeUnknownEffect(Baseline)(yield* readJson(baselinePath))
        : undefined;
    const markdown = yield* Effect.try({
      try: () => report(current, previous),
      catch: (cause) => new CapacityError({ message: "Invalid benchmark comparison", cause }),
    });
    yield* Effect.try({
      try: () =>
        publishCapacityReport(
          directory,
          `${JSON.stringify(current, undefined, 2)}\n`,
          markdown,
          mode === "compare",
        ),
      catch: (cause) => new CapacityError({ message: "Cannot publish report set", cause }),
    });
    return;
  }
  const admin = yield* Effect.acquireRelease(
    Effect.sync(
      () => new Admin({ bootstrapBrokers: [bootstrapServers], clientId: "evs-capacity-admin" }),
    ),
    (client) => attempt("Close Kafka admin", () => client.close()).pipe(Effect.orDie),
  );
  const offsets = Effect.fn("Capacity.offsets")(function* (timestamp: bigint) {
    return yield* attempt("Read corpus offsets", () =>
      admin.listOffsets({
        topics: topics.map((name) => ({ name, partitions: [{ partitionIndex: 0, timestamp }] })),
      }),
    );
  });
  const verifyCorpus = Effect.fn("Capacity.verifyCorpus")(function* () {
    const saved = yield* Schema.decodeUnknownEffect(Corpus)(yield* readJson(manifestPath));
    if (JSON.stringify(saved) !== JSON.stringify(corpus)) {
      return yield* new CapacityError({
        message: "Corpus manifest differs from requested recipe.",
      });
    }
    for (const timestamp of [-2n, -1n]) {
      const found = yield* offsets(timestamp);
      for (const name of topics) {
        const partitions = found.find((topic) => topic.name === name)?.partitions;
        if (
          partitions?.length !== 1 ||
          partitions[0]?.offset !== (timestamp === -2n ? 0n : BigInt(rows))
        ) {
          return yield* new CapacityError({
            message: `Corpus offset mismatch for ${name}; recreate the dedicated corpus.`,
          });
        }
      }
    }
  });
  if (mode === "seed") {
    if (existsSync(manifestPath)) return yield* verifyCorpus();
    // Existing topics deliberately fail creation. Never append a second corpus
    // after a partial seed or reuse a broker based only on a local marker file.
    yield* attempt(
      "Create ten corpus topics (existing/partial corpus must be reset explicitly)",
      () =>
        admin.createTopics({
          topics,
          partitions: 1,
          replicas: 1,
          configs: [
            { name: "cleanup.policy", value: "delete" },
            { name: "retention.ms", value: "-1" },
            { name: "retention.bytes", value: "-1" },
          ],
        }),
    );
    const producer = yield* Effect.acquireRelease(
      Effect.sync(
        () =>
          new Producer<Buffer | null, Buffer | null, Buffer, Buffer>({
            bootstrapBrokers: [bootstrapServers],
            clientId: "evs-capacity-seed",
            idempotent: true,
          }),
      ),
      (client) => attempt("Close Kafka producer", () => client.close()).pipe(Effect.orDie),
    );
    for (let start = 0; start < rows; start += 1000) {
      const batch = Array.from({ length: Math.min(1000, rows - start) }, (_, offset) => ({
        key: Buffer.from(String(start + offset)),
        value: Buffer.from(wireRow(start + offset)),
      }));
      for (const topic of topics) {
        yield* attempt("Seed protobuf batch", () =>
          producer.send({
            messages: batch.map((message) => ({ ...message, topic, partition: 0 })),
          }),
        );
      }
      if (start % 100_000 === 0)
        yield* Effect.logInfo(
          `Seeded ${Math.min(start + 1000, rows)}/${rows} rows in each of 10 topics`,
        );
    }
    yield* writeJson(manifestPath, corpus);
    return yield* verifyCorpus();
  }
  yield* verifyCorpus();
  const count = yield* Schema.decodeUnknownEffect(Scenario.fields.topics)(Number(mode));
  const selected = topics.slice(0, count);
  const groupPrefix = `evs-capacity-${randomUUID()}`;
  const Row = Schema.Struct({ id: ViewServerId, payload: Schema.String });
  const topicDefinitions = Object.fromEntries(
    selected.map(
      (topic) =>
        [
          topic,
          {
            schema: Row,
            source: kafka.source({
              topic,
              regions: ["local"],
              cleanupPolicy: "delete",
              retentionPolicy: "Infinity",
              key: kafka.string(),
              value: kafka.protobuf(StringValueSchema),
              localRowKey: ({ key }) => key,
              map: ({ value }) => ({ payload: value.value }),
              startFrom: "earliest",
            }),
          },
        ] as const,
    ),
  );
  const config = defineViewServerConfig({ topics: topicDefinitions });
  const cpuStart = process.cpuUsage();
  const started = yield* Clock.monotonicTimeNanos;
  const runtime = yield* Effect.acquireRelease(
    makeViewServerRuntime(config, { websocketPort: 0, host: "127.0.0.1" }).pipe(
      Effect.provide(
        kafkaNode.layer(config, {
          consumerGroupPrefix: groupPrefix,
          regions: { local: { bootstrapServers } },
        }),
      ),
    ),
    (current) => current.close,
  );
  const complete = new Set<string>();
  while (complete.size < count) {
    const groups = yield* attempt("Read post-application commits", () =>
      admin.listConsumerGroupOffsets({
        groups: selected.map((topic) => ({
          groupId: `${groupPrefix}:${topic}`,
          topics: [{ name: topic, partitionIndexes: [0] }],
        })),
        requireStable: false,
      }),
    );
    for (const topic of selected) {
      const group = groups.find((entry) => entry.groupId === `${groupPrefix}:${topic}`);
      const committed = group?.topics[0]?.partitions[0]?.committedOffset ?? -1n;
      if (complete.has(topic) || committed < BigInt(rows)) continue;
      const snapshot = yield* runtime.client.snapshot(topic, { select: ["id"], limit: 1 });
      const last = yield* runtime.client.snapshot(topic, {
        select: ["id", "payload"],
        where: [{ field: "id", type: "equals", filter: `local:0:${rows - 1}` }],
        limit: 1,
      });
      if (
        snapshot.status !== "ready" ||
        snapshot.totalRows !== rows ||
        last.rows.length !== 1 ||
        last.rows[0]?.payload !== payload(rows - 1)
      ) {
        return yield* new CapacityError({
          message: `Retained query verification failed for ${topic}`,
        });
      }
      complete.add(topic);
      yield* Effect.logInfo(`Verified ${topic}: ${rows} retained rows`);
    }
    if (complete.size < count) yield* Effect.sleep("1 second");
  }
  const seconds = Number((yield* Clock.monotonicTimeNanos) - started) / 1e9;
  const cpu = process.cpuUsage(cpuStart);
  const cpuSeconds = (cpu.user + cpu.system) / 1e6;
  const result: Scenario = {
    topics: count,
    rows: count * rows,
    seconds,
    messagesPerSecond: (count * rows) / seconds,
    cpuSeconds,
    averageCpuCores: cpuSeconds / seconds,
    peakRssBytes: process.resourceUsage().maxRSS * 1024,
  };
  yield* writeJson(join(directory, `scenario-${count}.json`), result);
  yield* Effect.logInfo(JSON.stringify(result));
});

const controller = new AbortController();
process.once("SIGINT", () => controller.abort());
process.once("SIGTERM", () => controller.abort());
await Effect.runPromise(main.pipe(Effect.scoped), { signal: controller.signal });
