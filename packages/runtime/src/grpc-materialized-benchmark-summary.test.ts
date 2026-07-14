import { describe, expect, it } from "@effect/vitest";
import {
  summarizeGrpcMaterializedBenchmarkSamples,
  type GrpcMaterializedBenchmarkSampleEvidence,
} from "../test-harness/grpc-materialized-benchmark";

const streamSample = (
  streamConvergenceMs: number,
  rowsPerSecond: number,
): GrpcMaterializedBenchmarkSampleEvidence => ({
  backpressureCount: 0,
  cleanupLeakCount: 0,
  cleanupMs: 1,
  healthOverlayMs: 0.1,
  name: "gRPC materialized stream batch",
  queuedEventCount: 0,
  resultRowId: "order-1099",
  rows: 100,
  rowsPerSecond,
  seedRows: 1_000,
  snapshotMs: 0.2,
  startTotalRows: 1_000,
  streamConvergenceMs,
  totalRows: 1_100,
});

describe("gRPC materialized benchmark summary", () => {
  it("keeps raw isolated samples and pools throughput across their elapsed time", () => {
    const summary = summarizeGrpcMaterializedBenchmarkSamples(
      [
        streamSample(10, 10_000),
        streamSample(10, 10_000),
        streamSample(10, 10_000),
        streamSample(10, 10_000),
        streamSample(100, 1_000),
      ],
      "gRPC materialized stream batch",
      5,
    );

    expect(summary.meanRowsPerSecond).toBe(8_200);
    expect(summary.medianRowsPerSecond).toBe(10_000);
    expect(summary.mutationCount).toBe(500);
    expect(summary.pooledRowsPerSecond).toBeCloseTo(3_571.428_571, 6);
    expect(summary.sampleCount).toBe(5);
    expect(summary.samples).toStrictEqual([
      streamSample(10, 10_000),
      streamSample(10, 10_000),
      streamSample(10, 10_000),
      streamSample(10, 10_000),
      streamSample(100, 1_000),
    ]);
    expect(summary.seedMutationCount).toBe(5_000);
    expect(summary.startTotalRows).toBe(1_000);
    expect(summary.totalRows).toBe(1_100);
  });

  it("summarizes even and zero-throughput sample sets", () => {
    const even = summarizeGrpcMaterializedBenchmarkSamples(
      [streamSample(100, 1_000), streamSample(100 / 3, 3_000)],
      "gRPC materialized stream batch",
      2,
    );
    const zeroSample = {
      ...streamSample(0, 0),
      resultRowId: null,
      rows: 0,
      totalRows: 1_000,
    };
    const zero = summarizeGrpcMaterializedBenchmarkSamples(
      [zeroSample],
      "gRPC materialized stream batch",
      1,
    );

    expect({
      evenMedianRowsPerSecond: even.medianRowsPerSecond,
      zeroCoefficientOfVariation: zero.rowsPerSecondCoefficientOfVariation,
      zeroPooledRowsPerSecond: zero.pooledRowsPerSecond,
    }).toStrictEqual({
      evenMedianRowsPerSecond: 2_000,
      zeroCoefficientOfVariation: 0,
      zeroPooledRowsPerSecond: 0,
    });
  });

  it("rejects missing, inconsistent, and contaminated raw sample evidence", () => {
    expect(() =>
      summarizeGrpcMaterializedBenchmarkSamples(
        [streamSample(10, 10_000)],
        "gRPC materialized stream batch",
        2,
      ),
    ).toThrow(
      "gRPC materialized benchmark case gRPC materialized stream batch produced 1 sample(s), expected exactly 2.",
    );
    expect(() =>
      summarizeGrpcMaterializedBenchmarkSamples([], "gRPC materialized stream batch", 0),
    ).toThrow(
      "gRPC materialized benchmark case gRPC materialized stream batch produced no samples.",
    );
    expect(() =>
      summarizeGrpcMaterializedBenchmarkSamples(
        [
          streamSample(10, 10_000),
          {
            ...streamSample(10, 10_000),
            rows: 99,
            totalRows: 1_099,
          },
        ],
        "gRPC materialized stream batch",
        2,
      ),
    ).toThrow(
      "gRPC materialized benchmark case gRPC materialized stream batch did not preserve identical seeded sample state.",
    );
    expect(() =>
      summarizeGrpcMaterializedBenchmarkSamples(
        [
          {
            ...streamSample(10, 10_000),
            cleanupLeakCount: 1,
          },
        ],
        "gRPC materialized stream batch",
        1,
      ),
    ).toThrow(
      "gRPC materialized benchmark case gRPC materialized stream batch recorded non-zero cleanup, queue, or backpressure evidence.",
    );
  });
});
