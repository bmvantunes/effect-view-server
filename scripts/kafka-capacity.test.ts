import { describe, expect, it } from "@effect/vitest";
import { fromBinary } from "@bufbuild/protobuf";
import { StringValueSchema } from "@bufbuild/protobuf/wkt";
import { Schema } from "effect";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { Baseline, Corpus, payload, report, topicNames, wireRow } from "../packages/kafka/benchmarks/capacity-model";

const corpus = Schema.decodeUnknownSync(Corpus)(JSON.parse(readFileSync("benchmarks/kafka-capacity/corpus.json", "utf8")));
const current: Baseline = {
  corpus, node: "v26", platform: "linux", arch: "x64", cpu: "test", logicalCpus: 16,
  totalMemoryBytes: 1024 ** 4, revision: "abc",
  scenarios: [1, 3, 10].map((topics) => ({
    topics: Schema.decodeUnknownSync(Schema.Literals([1, 3, 10]))(topics),
    rows: topics * corpus.rowsPerTopic, seconds: 100, messagesPerSecond: topics * 500_000,
    cpuSeconds: 100, averageCpuCores: 1, peakRssBytes: 1024 ** 3,
  })),
};

describe("Kafka capacity corpus and report", () => {
  it("reproduces protobuf bytes and spans the complete 200..1024-byte range", () => {
    expect([0, 824, 49_999_999].map((index) => createHash("sha256").update(wireRow(index)).digest("hex"))).toStrictEqual([
      "70d8e0ef690d9d737f51b0d14d18249df2e76f9496135fc067dc1b324c06afc2",
      "e2f9beb93734718dc41dccab8f1fa7e7a90ae249c03fc896ca2b4844e8c93489",
      "13b64e2e05f71d5a0170c54c20660029d36c73f69f5ab85818967213f1ae6591",
    ]);
    const sizes = new Set<number>();
    for (let index = 0; index < 825; index += 1) {
      const bytes = wireRow(index);
      sizes.add(bytes.byteLength);
      expect(bytes).toStrictEqual(wireRow(index));
      expect(fromBinary(StringValueSchema, bytes).value).toBe(payload(index));
    }
    expect([...sizes]).toStrictEqual(Array.from({ length: 825 }, (_, index) => index + 200));
    expect(wireRow(49_999_999)).toStrictEqual(wireRow(49_999_999));
    expect(payload(0)).not.toBe(payload(825));
  });

  it("uses ten stable, row-count-specific topic names", () => {
    expect(topicNames(20)).toStrictEqual(Array.from({ length: 10 }, (_, index) => `evs-capacity-v1-20-${index + 1}`));
    expect(topicNames(20)).not.toStrictEqual(topicNames(21));
    expect(corpus.rowsPerTopic).toBe(50_000_000);
    expect(() => Schema.decodeUnknownSync(Corpus)({ ...corpus, rowsPerTopic: 0 })).toThrow();
  });

  it("reports throughput, scaling, cores, memory and comparison without changing the baseline", () => {
    const before = JSON.stringify(current);
    const previous = { ...current, scenarios: current.scenarios.map((entry) => ({ ...entry, messagesPerSecond: entry.messagesPerSecond / 2 })) };
    expect(report(current, previous)).toMatch(/\| 10 \| 500000000 \| 100\.000 \| 5000000 \| 10\.00x \| 1\.00 \| 1\.00 \| 100\.00% \|/);
    expect(report(current)).toMatch(/\| 1 \| 50000000 \| 100\.000 \| 500000 \| 1\.00x \| 1\.00 \| 1\.00 \| — \|/);
    expect(JSON.stringify(current)).toBe(before);
  });

  it("rejects mismatched corpora and incomplete scenario sets", () => {
    expect(() => report(current, { ...current, corpus: { ...corpus, rowsPerTopic: 20 } })).toThrow("Cannot compare different corpus");
    expect(() => report({ ...current, scenarios: [] })).toThrow("completed report");
    expect(() => report({ ...current, scenarios: [current.scenarios[0]!, current.scenarios[0]!, current.scenarios[0]!] })).toThrow("completed report");
    expect(() => report(current, { ...current, scenarios: [] })).toThrow("missing a scenario");
  });
});
