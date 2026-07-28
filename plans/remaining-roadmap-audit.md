# Remaining Roadmap Audit

## Production milestone

PRD #383 is delivered by its four ordered vertical slices:

1. #384 — portable Source Adapter SDK, Runtime Core integration, diagnostics,
   and conformance.
2. #385 — first-party Kafka Source Adapter contract/server/Node Layers.
3. #386 — first-party gRPC Source Adapter contract/server/Node Layers.
4. #387 — canonical-only public migration, React Source Diagnostics,
   transport-neutral runtime composition, package hardening, examples, docs,
   and release intent.

The production surface now has one authored Topic tree, exact
`id: ViewServerId`, zero or one canonical `source`, adapter-owned aggregate
Layers, canonical Source Health, and explicit public package seams. The
transport-specific gRPC plan and obsolete source-integration sections of the
umbrella engine plan are historical records, not active contracts.

## Current validation

- `vp run -w ready`
- `vp run -w pre-grpc:gate`
- `vp run -w grpc:gate`
- `vp run -w release-candidate:capacity`

Kafka and gRPC performance are covered by their canonical Source Adapter
profiles. Generic engine, raw read/write, query-sharing, grouped, React, and
WebSocket profiles remain independent of adapter implementation.

## Intentionally deferred

- durable WAL/checkpoints
- multi-source Topics
- finite one-shot sources
- runtime plugin discovery or hot loading
- new browser transport
- session/header forwarding upstream
- automatic browser config projection
- query `having`
- native/Rust/SIMD acceleration without benchmark justification

Any next work requires a new explicit product decision or a promoted issue.
