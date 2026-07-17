# Benchmarks And Capacity

Benchmarks use Vitest benchmark mode through `vp test bench`. Do not add
ad-hoc benchmark runners for engine/runtime performance work.

## Gates

Root scripts provide benchmark profiles and regression comparison:

```sh
vp run -w bench:baseline:smoke
vp run -w bench:baseline:raw-read-write
vp run -w bench:baseline:active-query-sharing
vp run -w bench:baseline:grouped-admission
vp run -w bench:baseline:grouped-order-neutral
vp run -w bench:baseline:websocket-firehose
vp run -w bench:baseline:kafka-ingest
vp run -w bench:baseline:kafka-sustained-firehose
vp run -w bench:baseline:grpc-materialized
vp run -w bench:baseline:grpc-leased
vp run -w bench:baseline:grpc-leased-retained
```

Use `vp run -w pre-grpc:gate` before gRPC-focused work and `vp run -w grpc:gate`
for the gRPC profiles.

For a release-candidate capacity pass, run:

```sh
vp run -w release-candidate:capacity
```

This runs example browser/type checks, example builds, `pre-grpc:gate`,
`grpc:gate`, and the broad no-compare `bench:baseline:release` profile
serially. Do not run competing benchmark suites in parallel when recording
release-candidate numbers.

The release profile runs 10M-row engine cases and sets
`NODE_OPTIONS=--max-old-space-size=12288` so the benchmark process is not
limited by Node's default old-space cap.

## Sampling Policy

The smoke profile gives its read-focused raw snapshot, raw predicate, grouped
aggregate, and grouped key-width cases 1,000 minimum measured iterations, a
250 ms measurement floor, five warmup iterations, and a 100 ms warmup floor.
The focused raw read/write profile gives its two read tasks the same 1,000 minimum measured
iterations and time/warmup floors. These are minimum sample
counts: fast read cases continue until the time floor is satisfied. At the
minimum population, roughly ten observations remain above the p99 rank, so a
small pause cluster cannot both exhaust the time floor and define the tail.

The live-delta cases in the affected mixed raw-snapshot and grouped-aggregate
tasks remain iteration-bound with measurement time and warmup disabled so
sample and mutation counts stay exact. The raw snapshot live-delta case uses
exactly five smoke samples or 20 focused-profile samples even though the
snapshot cases in the same benchmark process use read sampling floors. Fanout
and other mutation tasks keep their existing sampling policy. Every affected
benchmark emits this machine-readable policy; the runner rejects missing
samples, non-exact mutation samples, total mutation drift, and policy drift.

The same policy selects `process-peak-over-initial-current` for the RSS gate. Each fresh benchmark
worker records monotonic process peak-RSS checkpoints before setup, after setup, and after benchmark
cleanup. The compared delta is the final process-lifetime peak minus the initial current RSS, so
module startup can only make the capacity signal more conservative and a lucky endpoint GC cannot
make it disappear. Warmup, JIT, GC, and measured allocation are included by design. Ordinary
`process.memoryUsage()` before/setup/after snapshots remain separate diagnostic fields.

Sampling changes do not change performance thresholds. An explicitly accepted
measurement-protocol migration permits refreshing only the affected read
observations and associated policy metadata. Preserve reference values for
unchanged mutation, fanout, retained-delta, write, and browser workloads.
The comparator retains every sample and fails the first run; do not trim
outliers, retry, or select a best result.
The ordinary `--update-baseline` mode replaces the entire profile; use repeated
`--update-baseline-task='<task label>'` arguments for a scoped protocol
migration. A scoped update executes only those named tasks and merges their fresh observations into
the existing baseline. It requires an unchanged task catalog and rejects selected-task workload or
structural drift; only latency samples, nested runtime-operation samples, RSS, minimum sample
counts, sampling-policy metadata, and an explicitly accepted `measurementProtocol` migration may
change. The exact smoke and raw-read/write commands are documented in
`benchmarks/README.md`.

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
Each named execution writes a validated `profile-<name>.json` run artifact containing its profile
identity and fresh task observations, but no comparison thresholds. Stable baseline updates and
comparisons are managed separately by `scripts/run-benchmark-baseline.mjs`.
Tasks that use priming or explicit endpoint GC also emit structural `measurementProtocol` metadata.
The runner derives the expected protocol from the task environment, and baseline comparison rejects
missing or changed protocol metadata before comparing latency or memory values.
Noisy maximum latency should stay report-only unless repeated runs prove the
threshold is stable enough to gate CI.

## Release Candidate Notes

Record the machine/container shape beside any release-candidate benchmark
results:

- CPU model and allocated cores
- memory limit
- Node version
- Kafka broker location and topic partition counts
- row counts per View Server topic
- active browser/client count
- active subscription count
- Kafka input rate
- gRPC leased route count
- WebSocket fanout shape

The baseline gates catch regressions against committed smoke profiles. They are
not a substitute for one production-like capacity run before a real deployment.
