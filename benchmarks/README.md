# Benchmark Baselines

`benchmarks/baselines/smoke.json` is the committed smoke performance baseline.

Run the gate:

```bash
vp run -w bench:baseline:smoke
```

Refresh the smoke baseline only when a performance change is intentionally accepted:

```bash
vp run -w bench:baseline:smoke:update
```

That updater replaces every task in the profile. For an accepted measurement-protocol migration,
run and refresh only the affected tasks so unrelated reference task values remain unchanged:

```bash
node scripts/run-benchmark-baseline.mjs --profile=smoke --update-baseline \
  '--update-baseline-task=raw snapshot 1000 rows' \
  '--update-baseline-task=raw predicate index 1000 rows' \
  '--update-baseline-task=grouped aggregate 1000 rows' \
  '--update-baseline-task=grouped key width 1000 rows'
```

Scoped updates require the committed and current profile task catalogs to match. They may refresh
only timing samples, RSS, minimum sample counts, and sampling-policy metadata for selected tasks;
workload identity or structural metadata drift aborts without rewriting the baseline.

The smoke gate runs the existing Vitest benchmark files serially and compares fresh `.artifacts`
summary/output JSON against the committed baseline. Engine smoke cases use small row counts with
multiple samples; browser smoke stays deliberately tiny to keep CI practical. Read-focused raw
snapshot, raw predicate, grouped aggregate, and grouped key-width cases use 1,000 minimum measured
iterations, a 250 ms measurement floor, five warmup iterations, and a 100 ms warmup floor. The
benchmark summary and committed baseline retain this policy, and the runner rejects fewer samples
or policy drift. The live-delta cases in the affected mixed raw-snapshot and grouped-aggregate tasks
retain exactly five iteration-bound samples with time and warmup disabled; fanout and other mutation
tasks keep their existing sampling policy. The smoke profile also runs five exact samples for each
of three 50k-candidate nested `in` cases: full production query admission/compilation, precompiled
evaluation across 100k partitioned rows, and acquisition of 32 equivalent subscriber leases. The
last case requires exactly one prepared Raw Query Plan compilation and one shared Active Query,
guarding `O(candidateCount + subscribers)` retained plan ownership instead of per-subscriber
membership plans. This keeps unbounded membership support from silently regressing while the
smaller predicate-index cases remain green. The gate fails on
cleanup leaks, backpressure, queued-event growth, RSS growth, mean latency regressions, or p99
latency regressions beyond the code-owned thresholds mirrored in the baseline manifest. Latency
thresholds intentionally use the wider of the ratio and absolute windows because CI smoke runs are
small and noisy; structural metadata, counters, sample counts, and RSS remain strict.

Every named profile execution also writes `profile-<name>.json` beside the first task's package-local
artifacts. This validated `view-server-benchmark-profile-run` artifact identifies the profile and its
fresh task observations without embedding baseline thresholds. The baseline workflow owns subsequent
update or comparison behavior; the profile executor owns only serial process execution and artifact
production.

Do not run benchmark profiles in parallel when comparing results.

Before starting the gRPC ingress adapter, run the serial pre-gRPC gate:

```bash
vp run -w pre-grpc:gate
```

This first runs the full correctness gate (`vp run -w ready`), then runs the strict smoke, raw
read/write, active-query-sharing, grouped admission, grouped order-neutral, WebSocket firehose, Kafka
ingest, and Kafka sustained-firehose baseline gates. It intentionally excludes
`bench:baseline:release`, which is a broad no-compare collection profile rather than a pass/fail gate.

Active-query sharing has a focused engine gate:

```bash
vp run -w bench:baseline:active-query-sharing
```

Refresh it only when an active-query sharing performance change is intentionally accepted:

```bash
vp run -w bench:baseline:active-query-sharing:update
```

This profile runs `raw-live-fanout` across same-window, ten-window, unique-window, and unique-shape
subscription sets. It exists to catch duplicated materialization/fanout regressions separately from
the broader smoke gate while still using Vitest benchmark output and committed baselines.

Raw read/write performance has a focused engine gate:

```bash
vp run -w bench:baseline:raw-read-write
```

Refresh it only when a raw read/write performance change is intentionally accepted:

```bash
vp run -w bench:baseline:raw-read-write:update
```

For a read-path measurement-protocol migration, preserve both write tasks and update only the
affected read observations:

```bash
node scripts/run-benchmark-baseline.mjs --profile=raw-read-write --update-baseline \
  '--update-baseline-task=raw snapshot 100000 rows' \
  '--update-baseline-task=raw predicate index 100000 rows'
```

This profile is localhost CPU/GC engine stress. It runs 100k-row raw snapshots, predicate-index
reads, base writes, and indexed writes. It exists to catch the common failure mode where a read-path
optimization wins filtered/sorted queries but silently taxes ingestion. Keep this gate serial and do
not compare it against runs collected while another benchmark is active.
Read cases use 1,000 minimum measured iterations, a 250 ms measurement floor, five warmup iterations,
and a 100 ms warmup floor. The raw snapshot live-delta case and raw writes keep time and warmup
disabled and run with exactly 20 samples and matching mutation counts. The profile keeps mean latency tighter
than smoke for millisecond-scale work, but keeps a small absolute window for sub-millisecond cases so
GitHub runner jitter does not fail healthy 0.xms operations. p99 remains a wide tail-noise guard.
The 1,000-sample read population leaves roughly ten observations above the p99 rank, so two or three
scheduler/GC pauses cannot both stop sampling at its minimum and define p99. The runner retains every
sample and fails the first comparison; it does not trim outliers, retry, or select a best run.
Affected read tasks measure RSS as the process-lifetime peak reached through setup, warmup, JIT, GC,
and measured work, anchored against the initial current RSS in the fresh benchmark worker. This
conservative metric cannot be lowered by a lucky final garbage collection. The endpoint memory
snapshots remain in the summary for diagnosis, while the runner compares only the policy-owned peak
delta with the existing RSS thresholds.

WebSocket firehose smoke has a focused runtime transport gate:

```bash
vp run -w bench:baseline:websocket-firehose
```

Refresh it only when a WebSocket transport performance change is intentionally accepted:

```bash
vp run -w bench:baseline:websocket-firehose:update
```

This profile starts a real runtime WebSocket server, connects the remote client over Effect RPC
WebSocket + NDJSON, publishes through the runtime mutation client, and waits for subscription deltas
over the wire with bounded stream reads. It currently covers small same-window hot fanout and
ten-window fanout without starting Kafka. It is a deterministic smoke transport gate; larger
50-browser/product-distributed WebSocket firehose profiles belong in a separate manual or release
gate once their local-noise characteristics are stable enough to baseline.

Kafka runtime profiles are separate from the default smoke gate because they start the Apache Kafka
container and exercise real `@platformatic/kafka` producers/consumers:

```bash
vp run -w bench:baseline:kafka-ingest
vp run -w bench:baseline:kafka-sustained-firehose
```

`kafka-ingest` measures single JSON/protobuf source batches plus a mixed burst. `kafka-sustained-firehose`
uses the same Vitest benchmark file in `sustained-firehose` mode and sends repeated mixed producer
batches before waiting for final View Server convergence. Both profiles require exact Kafka lane
completeness in their summary artifacts: produced rows, engine rows, and committed offsets must agree.
They also record per-case write-path throughput from benchmark operation timers. The baseline gate
compares exact produced-row/sample metadata and guards `aggregateRowsPerSecond`, which is total rows
divided by total measured time across samples; per-sample mean/min rows-per-second stay in the
artifact for diagnosis but are intentionally not the regression gate because tiny Kafka sample sets
can contain one unusually fast or slow sample.

gRPC runtime profiles are gated separately from `pre-grpc:gate`:

```bash
vp run -w grpc:gate
```

This runs `vp run -w ready`, then the materialized, leased smoke, and retained leased baselines. The
retained leased profile uses 50k retained rows to keep the expensive local-filter snapshot path under
a committed baseline. Its latency stays gated with the gRPC runtime thresholds; RSS uses the wider
noisy-runtime allowance because the retained profile deliberately exercises a large in-memory row
set. Refresh it only when the retained-feed performance change is intentional:

```bash
vp run -w bench:baseline:grpc-leased-retained:update
```

Use repeated retained runs for local stability investigation before tightening thresholds:

```bash
vp run -w bench:baseline:grpc-leased-retained:repeat
```

The repeat command writes isolated ignored artifacts and stays report-only; it does not update or
compare committed baselines.
