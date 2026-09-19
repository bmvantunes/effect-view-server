# Kafka retained-capacity benchmark

Run on demand from the repository root:

```sh
vp run -w benchmark:kafka --heap-mib=1048576
vp run -w benchmark:kafka --heap-mib=1048576 --compare
```

The first command builds the public package, starts Apache Kafka with plaintext
on port 9092, seeds ten topics, then measures loading 1, 3 and 10 topics. Each topic
has **50 million distinct retained rows**. Every scenario starts a fresh production
View Server process and fresh consumer groups. Topics load concurrently within
that process. Scenarios run serially. This measures the existing runtime's scaling;
it does not introduce workers to make a single-process result look multicore.

Reports default to `packages/kafka/.artifacts/capacity/current/`:

- `benchmark-baseline.json`: successful non-comparison run, used by `--compare`.
- `benchmark.json`: latest successful results.
- `benchmark.md`: detailed results, throughput, scaling, CPU cores, peak RSS and comparison.

The output root also contains:

- `scenario-{1,3,10}.json`: completed scenario results for diagnosing partial runs.
- `corpus-ready.json`: recipe for a completely seeded corpus, checked against broker offsets on reuse.

`--compare` preserves the baseline bytes. All three reports are staged in an immutable
`.report-*` generation, then a single atomic rename switches the `current` symlink.
Readers therefore see a complete previous or new report set, even if a write fails
or the process is interrupted. This requires a POSIX filesystem with atomic rename
and symlink support. Readers needing a consistent multi-file snapshot should resolve
`current` once and read that generation directly. Prior generations remain available and may be removed between
runs, except for the generation targeted by `current`.

A failed child, timeout, signal, missing artifact, offset mismatch or retained-row
mismatch fails the run. Check the exit status; an older report may still exist,
and interruption immediately after publication may leave a complete new report.

## Small validation run

```sh
vp run -w benchmark:kafka --rows=1000 --output=packages/kafka/.artifacts/capacity-small
vp run -w benchmark:kafka --rows=1000 --output=packages/kafka/.artifacts/capacity-small --compare
```

Row count is part of the corpus identity. Reduced runs cannot compare with full runs.
Use `CAPACITY_KAFKA_PORT=9096` when another local broker owns 9092. The dedicated
Compose project is shared by runs and protected by a lock; do not change its port
while another run is active. `--skip-build` reuses an already-built public package.
`--timeout-seconds=86400` sets the maximum duration of each child, including seeding.
`--heap-mib` explicitly controls every worker's Node old-space limit and overrides
that setting in `NODE_OPTIONS`. The default/minimum budget is 2 KiB per retained row
across ten topics plus 2 GiB (978,611 MiB for the full corpus). This is a planning
estimate, not a measured full-capacity memory guarantee; increase it if needed.
The runner rejects a heap budget above 80% of currently available host memory
before building or seeding. The full example requests 1 TiB of heap and needs at
least 1.25 TiB available host memory, plus sufficient Docker disk capacity. Reports
record the effective V8 heap limit. Keep heap and Docker allocations the same when
comparing runs.

## Dataset and resources

`corpus.json` is the compact, versioned fixture. A deterministic xorshift32 recipe
generates printable ASCII in `google.protobuf.StringValue`; each protobuf value is
200–1024 bytes, cycling evenly through those sizes. Each topic has identical bytes
and distinct keys `0` through `49999999`. The producer encodes one bounded batch of
1000 rows and sends it to each topic, so it never materializes the whole dataset.
The production Kafka protobuf codec, mapping, Effect row schema and engine ingestion
all run during measurement. The retained row contains its canonical ID and payload.

The full broker corpus is approximately **306 GB of protobuf payload alone**, plus
keys and Kafka framing/index overhead. Kafka uses a persistent disk volume, one
partition per topic, one replica, delete cleanup, and unlimited retention. Budget
additional disk for broker overhead and sufficient RAM for 500 million rows plus
engine indexes and JavaScript overhead. This is a capacity benchmark intended for
a suitably provisioned machine; a small validation is not evidence of full capacity.

Topic Row Storage and materialized source retained-ID accounting use a string-key
index whose native-map leaves split at 65,536 entries. This avoids a single native
Map/Set's entry ceiling without introducing hash-collision-dependent capacity or
scanning every partition for a lookup. Character-byte routing keeps each branch
at no more than 256 children. Small indexes remain a single native-map leaf.

A collection-only probe on Node v26.9.0 / Apple M1 retained 50 million distinct
`local:0:<index>` keys, verified 50,151 sampled lookups, and checked delete/reinsert
behavior across split boundaries in 28.1 seconds at 6.0 GiB peak RSS. This proves
the key index can cross the native collection ceiling; it is **not** a 50-million-row
View Server measurement or evidence that the full ten-topic workload fits this machine.

Timing covers runtime creation through applied Kafka commits and bounded query
verification. Reports identify the work as localhost disk/CPU/GC stress. CPU cores
are process CPU time divided by wall time (including native/V8 threads, excluding
Kafka). That metric does not establish parallel JavaScript execution. Broker caches
are not flushed, and each scenario is a single sample. Query verification builds
only the indexes the normal engine needs for those queries, not every possible index.

The broker stays running for replay. Stop it without deleting the corpus:

```sh
docker compose -f benchmarks/kafka-capacity/compose.yaml down
```

After an interrupted seed, explicitly delete the dedicated corpus volume with
`docker compose -f benchmarks/kafka-capacity/compose.yaml down --volumes` and remove
the corresponding output directory's `corpus-ready.json` before reseeding. Do the
same if offsets no longer match. The runner never deletes broker data automatically.
If the orchestrator was killed with SIGKILL, remove `benchmarks/kafka-capacity/.run.lock`
only after checking no benchmark process remains.
