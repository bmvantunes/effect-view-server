# Benchmarks And Capacity

Benchmarks use Vitest benchmark mode through `vp test bench`. Do not add
ad-hoc benchmark runners for engine/runtime performance work.

## Gates

Root scripts provide benchmark profiles and regression comparison:

```sh
pnpm run bench:baseline:smoke
pnpm run bench:baseline:raw-read-write
pnpm run bench:baseline:active-query-sharing
pnpm run bench:baseline:grouped-admission
pnpm run bench:baseline:grouped-order-neutral
pnpm run bench:baseline:websocket-firehose
pnpm run bench:baseline:kafka-ingest
pnpm run bench:baseline:kafka-sustained-firehose
pnpm run bench:baseline:grpc-materialized
pnpm run bench:baseline:grpc-leased
pnpm run bench:baseline:grpc-leased-retained
```

Use `pnpm run pre-grpc:gate` before gRPC-focused work and `pnpm run grpc:gate`
for the gRPC profiles.

## What To Measure

Read optimizations must measure write cost. For example, adding a column vector
or index can improve filtered reads while slowing publish/patch/delete. The
benchmark suite tracks both read latency and write tax for relevant profiles.

Core capacity profiles cover:

- raw snapshots
- filtered snapshots
- sorted top-k windows
- grouped aggregation
- live delta generation
- active query sharing
- WebSocket fanout
- Kafka ingest
- gRPC materialized and leased feeds

## Artifacts

Benchmark artifacts are written under package-local `.artifacts/` directories.
Stable baseline comparisons are managed by `scripts/run-benchmark-baseline.mjs`.
Noisy maximum latency should stay report-only unless repeated runs prove the
threshold is stable enough to gate CI.
