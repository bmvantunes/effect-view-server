# ADR 0012: Adapter-owned dependency reporting

## Status

Accepted and implemented.

## Context

The generic runtime must report whether a non-ready Source is failing inside View Server or at an upstream dependency, and must identify the affected Kafka Region or gRPC logical client with its configured endpoints. `SourceExecutionFailure.AdapterFailure` alone cannot answer that question: configuration and Mapping failures are self problems, while acquisition and stream failures are dependency problems. Runtime Core cannot inspect first-party failure tags or platform Layer options without breaking the standard third-party Source Adapter seam.

## Decision

The Source Adapter server implementation optionally contributes dependency reporting through its nominal runtime service. It resolves each Source Definition to logical dependency targets with exact configured endpoint strings and supplies a pure Adapter Failure classifier returning `self` or `dependency` plus affected logical targets when known.

Runtime Core treats this information generically. It projects existing Source Status transitions onto adapter targets, preserves a target's last operational status during self failures, aggregates shared targets deterministically, and caches one complete snapshot. A configured Leased target with no active Feed is `Inactive`. Runtime Core contains no Kafka or gRPC branches and performs no active probes.

The production Runtime owns callback cadence, coalescing, lifecycle `Starting` and `Stopping` reports, and callback supervision. Periodic ticks retain their configured cadences; only semantic-change notifications use the minimum change interval. Multi-Source heartbeat precedence is worst-first: `Exhausted`, `WaitingToRetry`, `Reacquiring`, `Starting`, `Degraded`, then `Ready`. Source-level `Stopping` is ignored because the Runtime owns the single lifecycle `Stopping` report before shutdown. Reporting stays server-local and does not extend the browser health protocol, React hooks, or RPC surface.

## Consequences

First-party Kafka reports one target per configured Region; gRPC reports one target per logical client. Future adapters use the same server seam. Adapters that omit reporting continue to run but contribute no dependency inventory, and their Adapter Failures are conservatively classified as self problems. Endpoint strings are operational configuration disclosed deliberately to the server callback and never sent to browser clients.
