# Runtime heartbeat and dependency reporting

## Goal

Let a Node runtime report its internal state and its configured upstream dependencies through two server-local Effect callbacks. Reporting must stay off the live-event hot path and must preserve the Source Adapter status vocabulary already used by the runtime.

## Public contract

- `reporting.onHeartbeat` receives `{ status, problems }`.
- `reporting.onDependenciesUpdate` receives the complete configured dependency inventory.
- Heartbeat and dependency cadences are configured independently.
- Semantic dependency changes coalesce to the latest snapshot and emit no sooner than the configured change interval, which defaults to 300 milliseconds.
- A dependency change also requests a heartbeat emission.
- Callback defects are logged and reporting continues.
- Callback Effects are closed and carry no outstanding Effect environment requirements.
- The first heartbeat is `Starting`. Dependency reporting begins on its configured cadence; later semantic changes may emit sooner.

Heartbeat status reuses `Starting`, `Ready`, `Degraded`, `WaitingToRetry`, `Reacquiring`, `Exhausted`, and `Stopping`. `problems` is a stable array containing `self`, `dependency`, both in that order, or neither.

When multiple Sources disagree, status precedence is `Exhausted` > `WaitingToRetry` > `Reacquiring` > `Starting` > `Degraded` > `Ready`. Source-level `Stopping` is ignored because the Runtime emits the lifecycle `Stopping` heartbeat before it closes resources.

Dependency entries contain `dependency`, `target`, `endpoints`, and `status`. Status reuses the Source Diagnostics vocabulary and additionally permits the existing leased-source result state `Inactive`.

## Architecture

Source Adapter server implementations own two pieces of reporting knowledge:

1. the dependency targets referenced by a Source Definition;
2. the classification of an adapter failure as a runtime-self or dependency problem, including the affected target when known.

Runtime Core projects Source status transitions onto those adapter-provided targets and exposes one cached reporting snapshot. It does not branch on Kafka or gRPC. The production Runtime owns callback cadence, coalescing, lifecycle `Starting`/`Stopping` emissions, and callback supervision.

Kafka reports one target per configured region and the exact configured bootstrap-server strings. gRPC reports one target per logical client and its exact configured base URL. Both use operational evidence from the existing Source lifecycle; reporting performs no active probes.

## Verification

- Source Adapter contract and server tests cover reporting registration and environment closure.
- Runtime Core tests cover self/dependency provenance, regional targeting, recovery, full inventory, leased inactivity, and deterministic aggregation.
- Runtime tests cover option exactness, closed callback Effects, independent cadence, 300 ms coalescing, recovery emissions, lifecycle emissions, and defect supervision.
- Kafka and gRPC tests cover first-party descriptors and classifiers.
- Public facade/export tests and documentation examples cover both callback JSON shapes.
