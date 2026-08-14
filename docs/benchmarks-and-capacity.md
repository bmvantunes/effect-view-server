# Benchmarks And Capacity

Benchmark comparisons run serially. Do not run competing candidates in
parallel.

## Regression gates

```sh
vp run -w bench:baseline:smoke
vp run -w bench:baseline:raw-read-write
vp run -w bench:baseline:active-query-sharing
vp run -w bench:baseline:grouped-admission
vp run -w bench:baseline:grouped-order-neutral
vp run -w bench:baseline:websocket-firehose
vp run -w bench:baseline:kafka-source-adapter
vp run -w bench:baseline:grpc-source-adapter
```

The Kafka profile contains transport-neutral JSON and protobuf Source Lane
tasks, a 2,000-record mixed-codec burst, sustained mixed-codec ingestion,
multi-region/partition work, and a real Apache Kafka broker task using the
production Platformatic Node Adapter with commit observation. It also measures
finite-retention ingestion/index overhead, large due sweeps, and post-sweep
query convergence. Run the Kafka gates serially so broker, ingestion, and sweep
measurements do not compete for CPU or Kafka resources. The gRPC profile
measures 1,000-row Materialized batches, 50 Leased routes with sharing and
health, and 50,000-row retained capacity through the production Source Adapter
Module. Neither profile depends on a privileged transport branch in the generic
runtime.

`pre-grpc:gate` runs readiness plus engine, query-sharing, grouping, WebSocket,
and Kafka Source Adapter gates. `grpc:gate` runs readiness plus the canonical
gRPC Source Adapter gate.

`release-candidate:capacity` runs example tests/builds, both serial gate groups,
and the broad no-compare release profile.

## Interpreting results

Profiles state whether they measure localhost CPU/GC stress, browser stress, a
real broker, or production-like capacity. Read-path improvements must be
evaluated beside base and indexed write throughput. Fanout work should be
shared by Topic/query/window/plan rather than cloned per subscriber.

The WebSocket compression experiment uses the production Effect RPC + NDJSON path and measures
TCP stream bytes through a local counting proxy. Across the canonical same-window and ten-window
workloads, `permessage-deflate` reduced outbound bytes by about 93–95% while increasing mean
localhost publish-and-fanout latency by 25–27%.
Compression is enabled by default; `websocketCompression: false` is the explicit runtime opt-out.
The canonical WebSocket firehose gate runs both modes and rejects regressions in normalized
outbound bytes per subscriber mutation as well as latency and memory regressions.
The focused `vp run --no-cache runtime#bench:websocket-firehose` task measures the compressed path
by default. Set `VIEW_SERVER_RUNTIME_BENCH_WEBSOCKET_COMPRESSION=false` to measure the opt-out path.
Compressed package-task artifacts use a `-compressed` filename suffix so paired runs cannot
overwrite each other.

Update a committed baseline only for an accepted performance change, using the
matching `:update` task. Never move a baseline merely to make a regression pass.

The combined browser contract fixture has a 128 KiB gzip budget for config plus
Kafka/gRPC contracts and generated descriptors. Separate portable SDK and gRPC
descriptor fixtures retain their 32 KiB and 64 KiB budgets.
