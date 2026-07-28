---
"effect-view-server": major
---

Complete the hard migration to one canonical Topic `source` and exact required
`id: ViewServerId`, removing configurable Topic keys, transport-specific
source properties, config registration trees, generic Kafka/gRPC runtime bags,
legacy callbacks, and transport-specific health trees.

Publish the portable Source Adapter SDK and first-party Kafka/gRPC contract,
server, and Node Layer seams; add framework-neutral and React Source
Diagnostics with exact Materialized/Leased typing and scoped cleanup; and
harden browser bundles, package exports, staged artifacts, examples, and
documentation around the canonical-only surface.
