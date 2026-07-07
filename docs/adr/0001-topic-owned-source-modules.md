# ADR 0001: Topic-owned source modules are the only production source model

## Status

Accepted.

## Context

A View Server Topic is the logical table users query. It owns one schema, one Row Key field, and one authoritative store. External Source Topics, gRPC feeds, TCP publish commands, and direct Runtime Client mutations are ways to mutate that View Server Topic.

The early Kafka runtime shape allowed source declarations to be owned by runtime options. That made source ownership shallow: topic schema lived in the View Server config, while Kafka Source Codec, Region, Mapping, source topic name, and delivery behavior could live elsewhere. The Interface forced callers and maintainers to understand two places before they could answer basic questions:

- Which Source Topics can mutate this View Server Topic?
- Which Mapping produces this Topic Row?
- Which Row Key and target schema validate the mutation?
- Which source owns Kafka tombstone behavior?
- Which runtime options are operational settings versus source ownership settings?

That split weakens Locality. It also weakens type inference because runtime-owned source helpers cannot use the configured View Server Topic as the single source of truth as directly as topic-owned source declarations can.

## Decision

Every production source declaration belongs to the View Server Topic it mutates.

Accepted source shapes are:

- `kafkaSource` on a View Server Topic.
- `grpcSource` on a View Server Topic.
- No source declaration for direct Runtime Client or TCP publish ingestion.

The generic `source` alias is not accepted. Source ownership must stay explicit in code, docs, and type errors so there is no ambiguity between Kafka-owned topics, gRPC-owned topics, and directly-published topics.

Runtime options may configure operational concerns such as Kafka consumer group, startup mode, Region broker overrides, ports, and lifecycle. Runtime options must not declare source ownership. In particular, `runtime.kafka.topics` is rejected and must not be reintroduced as a compatibility path.

Runtime-owned Kafka helpers are deleted rather than treated as a compatibility path. The public facade and package subexports must keep negative export tests for removed names so they cannot silently return.

## Consequences

Source Ownership Policy is the focused Module for ownership decisions: given a View Server config, it can answer which topics are source-owned, which topics allow direct runtime/TCP mutation, and which topics require leased gRPC lifecycle.

Kafka Delivery Contract stays local to Kafka source ingestion. Kafka tombstones can be interpreted using the topic-owned source declaration, decoded key, Region, metadata, configured Row Key, and target schema.
Microbatch publishing groups only contiguous compatible messages. It must not globally regroup interleaved topics or source topics, because that would trade a small publish-count win for weaker Kafka-order semantics.

gRPC Source Lifecycle stays local to gRPC source ingestion. Materialized and leased gRPC feeds can use the same topic-owned policy without a second source registry.

Runtime Core remains the shared engine-backed Module for Real View Server and In-Memory View Server. Transport and ingress Adapters vary; source ownership does not.

Tests should cross real Seams:

- Config type tests prove invalid source ownership combinations fail.
- Runtime tests prove runtime-owned source declarations are rejected.
- Kafka tests prove source-owned Source Topics decode, map, publish, delete tombstones, commit, and report health through the Kafka Delivery Contract.
- Package export tests prove deleted helpers are not public API.

## Architecture Deepening Loop

The next architecture slices should preserve this decision:

1. Deepen Source Ownership Policy into a focused Module.
2. Deepen Kafka Delivery Contract around source-owned upserts, tombstones, commits, and health.
3. Deepen gRPC Source Lifecycle around materialized feeds and leased feeds.
4. Deepen TCP Publish Command Interpretation around schema-safe direct mutations.
5. Deepen Live Query Preparation around source-owned topic restrictions and leased-topic routing.
6. Deepen Predicate/Window execution around compiled plans and storage-admissible operations.
7. Deepen Wire Protocol operations around Effect RPC WebSocket NDJSON schemas.
8. Keep public facade packaging shallow only where it is a deliberate re-export Interface.
