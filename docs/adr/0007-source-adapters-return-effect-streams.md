# ADR 0007: Source Adapter attempts acquire Effect Streams

## Status

Accepted. Issue #384 implemented the generic Source Attempt, lane, delivery,
rejection, settlement, supervision, diagnostics, and conformance contracts.
Issues #385 and #386 implemented the first-party Kafka and gRPC integrations.
ADR 0011 supersedes this decision's universal uninterruptible Source Settlement
rule with an accepted target contract that issue #402 will implement: callback
application remains exactly once, while the returned settlement Effect is owned
and interrupted by the Source Attempt Scope. ADR 0011 also supersedes this
decision's rejection-only `Degraded` payload with the accepted canonical
non-empty degradation-reason tuple; the older payload is no longer normative.

## Context

A third-party source integration could receive a Runtime Client, invoke an imperative publish callback, or return a declarative stream. Giving adapters mutation authority would let them bypass Source Ownership Policy, Topic Row validation, leased Route Field congruence, health accounting, and runtime backpressure; callbacks would also make cancellation and resource ownership harder to compose consistently.

## Decision

A Source Adapter lifecycle factory returns a scoped Effect that acquires or subscribes one Source Attempt and yields a non-empty collection of continuous Source Delivery Lanes for a materialized source or acquired Leased Feed. Each lane is an Effect Stream of nominal SDK-created Source Lane Events: either a Source Delivery or Source Item Rejection. A simple pull source yields one lane; a multi-input adapter may yield several lanes so independent external ordering domains do not become globally serialized. Successful Effect acquisition after every required lane resource is acquired is the exact readiness handshake even when all lanes remain idle; acquisition failure rolls back already-acquired resources and never reports ready. View Server owns a fresh child Scope for every attempt. A settled rejection continues its lane, while actual lane Effect failure or unexpected completion terminates the complete attempt, interrupts sibling lanes, and closes that Scope; retry creates a fresh Scope and reacquires every lane. First-delivery readiness, Ready control events, and readiness polling are invalid.

The aggregate platform Layer owns only reusable shared infrastructure such as transports, pools, factories, credential refreshers, and O(1) resource maps. Every source-specific consumer, subscription, channel, iterator, callback registration, or lease is acquired inside the Source Attempt child Scope. Retry therefore reacquires fresh attempt-level resources without rebuilding the application Layer. Lifecycle acquisition and metrics inputs also expose the same logical-lifetime Scope across retries; adapters may attach frozen initial-position caches or equivalent lifetime-local state to it, but may not move attempt resources into it. Final Leased Feed release closes its attempt resources and lifetime-local state without disturbing shared clients used by other topics/routes, while runtime shutdown closes all child attempts before the aggregate Layer resources. A shared transport outage may terminate several dependent attempts, but each source keeps its own Schedule and health state. Hiding a permanent source subscription inside the aggregate Layer is invalid.

Source Attempt release follows Effect's infallible-finalizer contract. Adapter finalizers are infallible and idempotent, and View Server waits for them before reacquiring. Expected failures that affect delivery correctness occur during attempt acquisition, Stream execution, or Source Settlement rather than being deferred to cleanup. If an external client's close operation rejects, the finalizer records that rejection in mandatory adapter metrics and emits a structured log or span containing Source Adapter Identity, an opaque per-runtime Feed Route correlation reference when leased, and attempt number, then completes. View Server owns that reference for the Leased Feed lifetime so retries remain correlatable without logging raw Route Field values, which may contain authorized caller identity. It does not turn cleanup rejection into an untyped defect. Conformance tests exercise finalization on acquisition or Stream failure, retry, interruption, final Leased Feed release, and runtime shutdown. Fallible Source Settlement remains distinct because acknowledgement failure affects replay correctness.

Each Source Delivery contains an ordered `Chunk.NonEmptyChunk` of normalized Source Mutations plus an adapter-owned Source Settlement callback. Topic-bound SDK constructors create both values: Upsert accepts only the exact complete Topic Row including its canonical `id: string`, Delete accepts only the exact Topic Row ID, and raw structurally compatible mutation or delivery objects are rejected. The common SDK exposes neither internal storage keys nor an ID-plus-partial-row Upsert. `Chunk.of(...)` represents one mutation and `Chunk.make(...)` represents one or more. An empty source poll emits no Stream element rather than an empty Source Delivery. An Upsert inserts a missing Topic Row or completely replaces an existing Topic Row; partial patch mutations are not part of the Source Adapter contract. The acquisition Effect and Stream carry typed adapter failures and declare adapter dependencies through their Effect environments. View Server consumes the Stream and owns interruption, finalization, backpressure, Topic Row validation, Route Field congruence, rejection accounting, mutation application, and health accounting.

An external item that intentionally produces no mutation is a Source No-Op Item, not an empty Source Delivery or generic Skip mutation. Examples include heartbeats and adapter-level filter misses. The adapter settles such an item inside its pull-ordered Stream production effect before proceeding. A failure to settle is wrapped in the adapter's declared failure union and enters ordinary Source Supervision. A malformed, unparseable, or otherwise invalid item is not an invisible no-op: it becomes a nominal Source Item Rejection. Tombstones remain real keyed Delete mutations, including idempotent deletion of an already-absent Topic Row.

A Source Item Rejection represents one item-local decode, Mapping, canonical-ID, Route Field congruence, or Topic Schema failure when the underlying ordered source remains usable. It contains the exact schema-backed safe failure, adapter-owned safe source location, `rejectedAtNanos`, and an adapter-owned Rejection Settlement synchronous construction callback that receives the complete rejection-recording `Exit` and returns its settlement Effect; the raw payload is never included automatically. View Server records the rejection, increments `rejectedItemCount`, changes active health to sticky `Degraded`, and runs settlement before consuming the next lane event. Successful rejection recording and settlement continue the lane. Recording or settlement failure terminates the attempt. Kafka commits the rejected offset only after both succeed so later partition records remain consumable; gRPC uses an infallible no-op callback and pulls the next decoded response only when its response stream remains valid. A transport or framing failure that prevents safe continuation remains a Stream failure rather than a rejection.

This follows Effect beta.100's Stream distinction: `Stream.filterMapEffect` retains an element when the callback Effect succeeds with `Result.succeed`, drops it when that Effect succeeds with `Result.fail`, and enters the Stream error channel when the Effect itself fails. Source Item Rejection is instead an explicit adapter-level Source Lane Event because View Server must settle an invalid item sequentially, expose its exact typed diagnostic, and retain sticky Degraded health rather than silently filtering it away.

Adapter-specific APIs may retain ergonomic Local Row Key and Mapping functions. They must assemble and Schema-validate the final complete Topic Row with its canonical Topic Row ID before constructing an Upsert. Delete events do not need a row value. Under ADR 0011, a compaction-capable Kafka tombstone derives its complete policy-specific canonical ID only from Region, partition, and the exact serialized key bytes, without a Local Row Key callback or null-value Mapping; a delete-only null value is rejected. For Leased Feeds, View Server derives any internal partitioned storage identity from Feed Route plus Topic Row ID; that identity never enters the Source Adapter API.

Every Source Adapter supplies a Schema for its complete adapter-specific failure union. The SDK composes that exact type with its schema-backed Source Runtime Failure vocabulary as a tagged Source Execution Failure: one branch carries the adapter failure and one carries a common ingestion failure such as Source Buffer overflow or invalid Source Delivery. Item-local invalid Topic Row and leased Route Field failures may be carried safely by a Source Item Rejection instead of terminating the Stream. Distinct outer tags prevent collisions between adapter-defined and SDK-defined failure tags. Adapters never repeat common SDK failures in their own union. Source health and dependent Live Query status events expose the exact composed failure type inferred from the Topic's Source Definition, together with a convenient human-readable message. Failures are not flattened into strings. Adapters wrap foreign library errors into their declared failure union and own redaction of that branch; every field admitted by the adapter failure Schema is considered safe for consumers. Raw exceptions and opaque unknown errors are not part of the Source Adapter contract.

View Server runs every Source Delivery and Source Item Rejection through an attempt-owned application-or-recording-and-settlement workflow rather than using settlement as an `acquireUseRelease` release/finalizer. Source Settlement and Rejection Settlement are synchronous construction callbacks that must promptly return their Effect without I/O, blocking, timers, Promise work, Effect execution, or unbounded computation; blocking callback application is non-conformant arbitrary user code for which Runtime Core makes no finalization or leak-free guarantee. Runtime Core uses one outer `Effect.uninterruptibleMask((outerRestore) => ...)`: mutation application or rejection recording runs with `outerRestore`, its complete Effect `Exit` is captured, and the corresponding callback is applied to that Exit exactly once while masked to obtain an Effect. Scope closure before or during that operation therefore still produces an interrupted Exit and cannot skip callback application. Each attempt owns a closed atomic cancellation arbitration state. Shutdown or final leased-feed release marks cancellation requested before interrupting attempt fibers; after callback application and before any Source Termination/health/Schedule handoff, the workflow atomically claims ordinary termination only if cancellation has not won. A synchronous callback throw becomes exact redacted `InvalidSourceSettlement` under ordinary Source Supervision only when ordinary termination wins. If cancellation wins—including a request made after a non-cancelled operation `Exit` was captured but before the handoff—callback application and failed-settlement accounting still occur exactly once, while the throw is secondary redacted diagnostics and produces no Source Termination, callback-throw-induced Source Supervision health transition, Schedule advancement, retry/reacquisition, commit, or Runtime Fatal Signal. Successful delivery application or rejection recording settles only after the complete operation succeeds, and typed failure, defect, and cancellation remain distinguishable. The returned Effect's typed failure belongs to the adapter's declared failure union and its requirements remain visible in the View Server runtime Effect. Runtime Core uses the same `outerRestore` only around that returned Effect, so it is owned by the Source Attempt Scope, runs interruptibly, and must terminate promptly when that Scope closes; masking or ignoring Scope interruption is non-conformant. On ordinary paths Runtime Core awaits settlement: success preserves the application or recording outcome, while typed settlement failure becomes the operational Adapter Failure consumed by Source Supervision. Attempt-Scope interruption of the returned Effect is lifecycle cancellation, produces no Source Termination, and triggers no retry. When ordinary application or recording and settlement both fail, View Server records the original operation Exit in spans and logs but does not invent a compound consumer-facing failure. A closed Source Application Transition defect instead follows ADR 0011's supervised-child ownership handoff, original-defect precedence, and root-fatal propagation without awaiting settlement completion. Its child closes over `outerRestore`; it never uses a restore obtained from a nested mask entered while already uninterruptible. A callback throw there is secondary diagnostics only. First-party and conformance fixtures gate both returned settlement Effects and prove callback application plus any fatal-signal handoff complete before that gate opens. For both ordinary delivery and rejection paths, deterministic cancellation tests request closure before application completes and after a successful operation `Exit` is captured but before callback application; a callback throw and an interruptible returned `Effect.never` are joined on Source Attempt Scope closure without Source Termination, callback-throw-induced Source Supervision health transition, Schedule advancement, retry/reacquisition, or commit. Ordinary-termination synchronous callback throw and typed returned failure are covered separately, callback invocation count is exactly one, and masking or ignoring interruption is explicitly non-conformant. Already-applied mutations and recorded rejection state remain visible and are not rolled back. Adapters without external settlement semantics omit settlement and receive the SDK's infallible no-op callback.

Cancellation assertions distinguish callback-induced supervision state from state already produced by the operation. Delivery cancellation produces no callback-throw-induced supervision health transition. After successful rejection recording, cancellation preserves `rejectedItemCount`, the sticky rejection ledger, the `Degraded` publication, and exact metrics subject to operational shutdown-status precedence; the callback throw adds no Source Termination, supervision health transition, Schedule step, retry/reacquisition, commit, or Runtime Fatal Signal.

View Server consumes every Source Lane Event sequentially within its Source Delivery Lane. It applies and settles Source Deliveries in order, or records and settles Source Item Rejections before continuing. Sibling lanes inside the same Source Attempt execute concurrently through structured Effect fibers without first merging through a hidden queue. A settled rejection does not terminate the lane; actual Effect failure or successful completion of any lane terminates the complete attempt, interrupts the remaining lanes, and sends one Source Termination through the attempt's single Source Supervision policy. A Source Delivery is settlement-atomic but not state-atomic: mutations applied before a later mutation fails remain visible and are not rolled back. Complete-row Upserts and Deletes make replay safe after unsuccessful settlement. Independent materialized sources, sibling delivery lanes, and distinct leased-route Source Attempts may execute concurrently.

Every Source Definition contains an exact retry-policy selection: either the matching Source Adapter runtime service's mandatory lifecycle default or an explicit infallible Effect Schedule override supplied to the shared source constructor. `SourceAdapterServer.make(...)` requires one transport-aware default Schedule for each implemented Source Lifecycle, and materialized and leased defaults may differ. The source constructor translates absence of a public override into a concrete internal `UseAdapterDefault` branch rather than storing `undefined`. View Server defines no global retry default. The selected Schedule input is the standard Source Termination union: either an exact Source Execution Failure or UnexpectedCompletion. The Schedule alone owns retry timing, error-sensitive delays, and exhaustion; there is no separate retryable predicate or boolean. Its error type is `never`, its Effect environment remains visible in `runViewServerRuntime(...)`, and a no-retry lifecycle uses `Schedule.recurs(0)`.

View Server owns Source Supervision: it executes the adapter-defined Schedule, creates a fresh child Scope and reacquires the Source Attempt, reads Effect Schedule attempt and elapsed metadata for generic source health, and exposes exhaustion as a schema-backed `RetryExhausted` status containing the last exact Source Termination. Source Adapter v1 Streams are continuous. Successful completion while the source scope remains owned becomes UnexpectedCompletion and consumes the same Schedule while remaining distinguishable from typed failure in health. Fiber interruption is lifecycle cancellation, never becomes Source Termination, and is never retried. Finite one-shot source completion is outside the v1 lifecycle model.

Operational source failures never fail the enclosing View Server Runtime Effect. Source Attempt acquisition, transport/framing, Stream execution, rejection-settlement, and Source Settlement failure affect only that source's supervision and health; unrelated Topics and server transports remain available. Item-local decode, Mapping, ID, Route Field, and Topic Row validation failures instead become settled Source Item Rejections when the underlying source can continue. If a terminating source failure occurs before first readiness, dependent Live Queries retain an empty result and report stale or exhausted error status. If it was previously ready, they retain the last rows. Invalid View Server Config, missing adapter service, exact-resource mismatch, Effect Config failure, and mandatory aggregate Layer acquisition failure remain fatal runtime-composition failures before server ports open and never enter Source Supervision.

Source ingestion availability and Live Query lifetime are independent. While Source Supervision is retrying, View Server keeps dependent Live Query Subscriptions open, preserves their last rows, and reports them as stale. Exhausted retries keep those Subscriptions and rows intact but report an error. If the source recovers, the existing Subscriptions return to ready and resume events. Source failure never becomes SubscriptionClosed and never clears the retained Live Query result.

Source Adapters never receive a Runtime Client, internal mutation client, or imperative publish callback. External connections and protocol decoding remain inside the Source Adapter and use Effect scoped-resource primitives.

A materialized lifecycle factory returns a scoped Effect that begins acquiring one Source Attempt when the View Server runtime Effect starts and remains owned by the runtime Scope independently of Live Queries. A leased lifecycle factory receives the exact frozen Feed Route and returns the scoped acquisition Effect for one Source Attempt on that route. It never receives subscriber objects, reference counts, Runtime Clients, or View Server cleanup callbacks.

View Server owns leased reference counting and Scope lifetime. The first subscriber for an exact Feed Route creates one child Scope and supervised Stream; same-route subscribers share that Leased Feed and its retained rows. Retry and Schedule exhaustion do not release the feed while subscribers remain, and new same-route subscribers join its current ready, stale, or error state. The final subscriber closes the child Scope, runs adapter finalizers, deletes only that feed's route-owned rows, and removes its active health state. A later subscription creates a fresh feed with fresh Schedule state.

Source Adapters never create a ManagedRuntime or call `Effect.run*` inside reusable integration code. Their runtime services retain all Effect requirements in `R`. `runViewServerRuntime(viewServer, options)` exposes the exact union of requirements inferred from the one View Server Config. Application code supplies aggregate adapter and client Layers through Effect's ordinary `Effect.provide(...)` composition before `NodeRuntime.runMain(...)`.

The complete Source Definition options are a browser-safe executable code API, not one serializable boundary value. They may contain Schemas, platform-neutral codecs, Mapping and Local Row Key functions, Schedules, Effects, and other browser-safe Effect requirements. They may not import concrete transports, Node APIs, credentials, sockets, concrete client tokens, or platform Layers. Source Adapters require Schemas for their failure union and any data crossing health or Wire Protocol boundaries, may use Schema or Effect Config for serializable option subtrees, and use exact TypeScript contracts plus adapter-owned construction checks for executable members. The SDK validates and snapshots its common Source Definition envelope, and all adapter-specific construction validation completes before the source runtime service executes it.

Native pull-based Source Streams add no queue. A Source Adapter bridging an external push or callback producer must use one of the SDK's two finite Source Buffer constructors with adapter-configurable capacity. The SDK deliberately separates backpressurable and non-pausable producers rather than exposing a mode flag, raw Queue, or buffering strategy. The backpressurable constructor gives the producer an Effectful emitter that suspends on the internal bounded Queue at capacity and must be composed into the producer's flow-control operation. The non-pausable constructor gives a synchronous emitter with no capacity boolean an adapter could accidentally ignore; when its internal bounded offer first fails because the buffer is full, the SDK increments the overflow count and fails the Stream exactly once with SDK-owned, schema-backed `SourceBufferOverflow`. Later emissions cannot silently revive or corrupt that failed buffer.

Both constructors validate a positive finite integer capacity during pure construction, register and unregister callbacks through the Source Attempt Scope, and update depth and high-water mark through cheap local state. Queue shutdown cancels or completes pending backpressured emission during scoped teardown without turning lifecycle interruption into Source Termination. Sliding, dropping, and unbounded strategies are forbidden because losing one Upsert or Delete can permanently corrupt the materialized view. View Server adds no hidden unbounded prefetch. Source health exposes buffer depth, capacity, high-water mark, and overflow count per Source Delivery Lane rather than collapsing independent pressure into one aggregate buffer.

Each adapter runtime service supplies an infallible Effect that reads the bound source's exact lifecycle-specific Source Adapter Metrics from local state and returns a valid initial snapshot before the Source Stream becomes ready. The Effect performs no broker RPC, network request, or blocking refresh; adapters may implement it with Effect Metric, Ref, or an optimized transport ledger, and its requirements remain visible in the View Server runtime Effect. View Server samples it exactly once per second with Effect Clock, freezes and Schema-validates each result, and exposes only the cached snapshot. Metric-only changes publish at most once per cadence, while lifecycle transitions and Source Item Rejections publish immediately with the latest cached metrics. V1 exposes no global, adapter, source, or subscriber cadence option. Delivery processing may update cheap local counters but never performs health refresh, metrics encoding, or Wire Protocol publication. A schema-invalid snapshot is an `InvalidSourceMetrics` Source Runtime Failure that terminates the current source attempt through ordinary Source Supervision.

Every source health payload pairs those adapter metrics with mandatory SDK-owned Source Runtime Metrics:

```ts
type SourceBufferMetrics =
  | { readonly _tag: "Unbuffered" }
  | {
      readonly _tag: "Bounded";
      readonly capacity: number;
      readonly depth: number;
      readonly highWaterMark: number;
      readonly overflowCount: bigint;
    };

type SourceRuntimeLaneMetrics = {
  readonly id: string;
  readonly buffer: SourceBufferMetrics;
};

type SourceRuntimeMetrics = {
  readonly startedAtNanos: bigint;
  readonly lastAttemptStartedAtNanos: bigint;
  readonly lastDeliveryAtNanos: bigint | null;
  readonly lastRejectionAtNanos: bigint | null;
  readonly lastAppliedMutationAtNanos: bigint | null;
  readonly lastTerminationAtNanos: bigint | null;
  readonly currentAttempt: bigint;
  readonly retryCount: bigint;
  readonly receivedDeliveryCount: bigint;
  readonly rejectedItemCount: bigint;
  readonly attemptedMutationCount: bigint;
  readonly appliedUpsertCount: bigint;
  readonly appliedDeleteCount: bigint;
  readonly failedMutationCount: bigint;
  readonly completedSettlementCount: bigint;
  readonly failedSettlementCount: bigint;
  readonly retainedRowCount: number;
  readonly lanes: readonly [SourceRuntimeLaneMetrics, ...ReadonlyArray<SourceRuntimeLaneMetrics>];
};
```

The epoch-clock migration in this paragraph is accepted target behavior tracked by PRD #400 and implementation issues #401 and #402; it becomes normative only after every public timestamp producer and Kafka broker-boundary calculation is migrated. All absolute times are then epoch nanoseconds derived through Effect `Clock.currentTimeMillis`, validated as non-negative safe-integer wall milliseconds and converted as `BigInt(wallMillis) * 1_000_000n`; number-space nanosecond multiplication is invalid. `Clock.currentTimeNanos` is monotonic and is reserved for elapsed durations and cadence, never public or persisted epoch fields. Contracts and the Wire Protocol carry the raw `bigint`, never Date or Temporal objects; consumers may pass it to `Temporal.Instant.fromEpochNanoseconds`. Cumulative counters use `bigint`, while actual JavaScript collection sizes and capacities use `number`. Delivery, rejection, mutation, and settlement counters remain source-wide aggregates. `lanes` is always non-empty; each entry has a non-empty ID that is unique within the attempt and stable across retries, plus its exact buffer state. A simple source reports one lane, and Kafka uses exact region names as lane IDs. `completedSettlementCount` records completion of the settlement Effect without guessing adapter-specific acknowledgement semantics. Lifecycle status and exact failures remain outside metrics.

Source health uses exhaustive Effect Schema tagged unions rather than status strings plus optional fields:

```ts
type SourceItemRejectionDiagnostic<E, Location> = {
  readonly failure: E;
  readonly location: Location;
  readonly rejectedAtNanos: bigint;
};

type SourceItemRejectionReason<E, Location> = {
  readonly _tag: "SourceItemRejection";
  readonly latestRejection: SourceItemRejectionDiagnostic<E, Location>;
};

type AdapterMaintenanceFailureReason = {
  readonly _tag: "AdapterMaintenanceFailure";
};

type SourceDegradationReasons<E, Location> =
  | readonly [SourceItemRejectionReason<E, Location>]
  | readonly [AdapterMaintenanceFailureReason]
  | readonly [SourceItemRejectionReason<E, Location>, AdapterMaintenanceFailureReason];

type SourceStatus<E, Location> =
  | {
      readonly _tag: "Starting";
      readonly attempt: 1n;
      readonly startedAtNanos: bigint;
    }
  | {
      readonly _tag: "Ready";
      readonly attempt: bigint;
      readonly readyAtNanos: bigint;
    }
  | {
      readonly _tag: "Degraded";
      readonly attempt: bigint;
      readonly degradedAtNanos: bigint;
      readonly reasons: SourceDegradationReasons<E, Location>;
    }
  | {
      readonly _tag: "WaitingToRetry";
      readonly nextAttempt: bigint;
      readonly retryAtNanos: bigint;
      readonly termination: SourceTermination<E>;
    }
  | {
      readonly _tag: "Reacquiring";
      readonly attempt: bigint;
      readonly startedAtNanos: bigint;
      readonly previousTermination: SourceTermination<E>;
    }
  | {
      readonly _tag: "Exhausted";
      readonly exhaustedAtNanos: bigint;
      readonly exhaustion: SourceRetryExhaustion<E>;
    }
  | {
      readonly _tag: "Stopping";
      readonly stoppingAtNanos: bigint;
      readonly reason: "RuntimeShutdown" | "LeaseReleased";
    };

type SourceTarget<Route> =
  | { readonly _tag: "Materialized" }
  | { readonly _tag: "Leased"; readonly route: Route };
```

Every source health value always contains Source Adapter Identity, Source Target, Source Status, mandatory `{ runtime, adapter }` metrics, and `sampledAtNanos`. Route, rejection, failure, retry timing, and exhaustion are never optional fields. Live Queries map both `Ready` and `Degraded` to ready availability, `WaitingToRetry` and `Reacquiring` to stale, `Exhausted` to error, and recovery to ready while preserving their Subscription and retained rows. Exact degraded state remains visible through Source Diagnostics. One settled Source Item Rejection also marks the affected Topic health row and aggregate View Server health summary degraded. Liveness and readiness transports continue to respond successfully while returning that degraded state; they do not evict or restart the instance. An exhausted required source retains its exact `Exhausted` diagnostics and rows but contributes `starting` Topic and aggregate health, keeping readiness unsuccessful until recovery. Operators can alert on degraded status or `rejectedItemCount` without introducing a health refresh on the lane hot path.

Ordinary Live Query Snapshot, Delta, and Status Event APIs stay transport-agnostic and do not carry Source Adapter Metrics or complete Source Health values. Remote Browser Client `subscribeSourceHealth(...)` and React `useSourceHealth(...)` expose exact adapter diagnostics addressed solely by selected View Server Topic and, for leased sources, exact Feed Route. Source-free Topics are rejected; materialized Topics reject `routeBy`; leased Topics require an exact route and reject missing, unknown, or extra Route Fields without `as const`. Source Adapter Identity and transport identifiers never become lookup keys. The scoped client subscription emits the latest cached value immediately and subsequent cached changes, is shared across matching local consumers, and is released through Scope on React unmount or client shutdown. There is no separate one-shot source-health API in v1; callers needing one value explicitly take the first subscription element. These APIs consume only View Server's cadence-cached Source Health; they do not perform a broker RPC, run the adapter metrics Effect, recompute health per Live Query event, or poll from React.

Source Diagnostics are observers rather than Leased Feed owners. A diagnostics subscription never increments the Live Query reference count, invokes the Request Factory, acquires a Source Attempt, delays final release, or preserves route-owned rows. Materialized Source Diagnostics always expose the exact active Source Health type. Leased Source Diagnostics expose an exact tagged union: `Inactive` contains the exact Feed Route and no fabricated metrics; `Active` contains the complete exact Source Health. An inactive route emits `Inactive` immediately, becomes `Active` only while at least one real Live Query owns the Leased Feed, and returns to `Inactive` after final release.

## Consequences

Materialized and leased source execution can share one lifecycle-neutral ingestion and supervision pipeline while differing only in runtime-owned Scope lifetime and Feed Route. A non-empty collection of concurrent ordered Source Delivery Lanes preserves throughput across independent clusters without weakening per-input ordering or adding a hidden merge buffer; structured failure of one lane still makes the Source Attempt one supervision and ownership unit. Child Scopes make Leased Feed acquisition, sharing, interruption, finalization, row cleanup, and later fresh reacquisition follow Effect's structured resource ownership instead of adapter-managed reference counting. Infallible idempotent finalizers match Effect's resource contract, while mandatory metrics and structured diagnostics keep external close rejection visible without introducing defects into Scope cleanup. Mandatory local metrics Effects preserve exact adapter observability without putting remote reads, encoding, or publication on the delivery hot path, while bounded View Server sampling keeps health cost predictable. Kafka commit and queue acknowledgement policies remain adapter-owned while preserving publish-before-settlement ordering. The attempt-owned interruptible settlement workflow makes a typed failed acknowledgement the operational failure while retaining the application Exit as diagnostic context; lifecycle interruption creates no termination or retry, and complete-row Upsert and keyed Delete idempotence make replay safe without rollback. Topic-bound nominal constructors keep row and key inference exact, prevent structural lookalikes, and make empty deliveries unrepresentable. Settling intentional no-op items within sequential Stream production preserves source ordering without adding transport-specific variants to the universal mutation model. Explicit settled Source Item Rejections prevent one poison item from blocking all later ordered input while sticky Degraded health honestly reports possible incompleteness. Complete-row Upserts keep source replay and restart behavior independent of pre-existing Runtime Core state; adapters consuming partial external events must materialize complete Topic Rows before emitting Source Mutations. One typed Schedule keeps classification, error-sensitive delay, exhaustion, and retry metadata in Effect's policy model without redundant callbacks or hand-written loops. Finite lossless buffering prevents push integrations from hiding memory growth or silently corrupting source state. Composing an adapter-specific Schema with the SDK's shared Source Runtime Failure Schema follows Effect's domain-plus-framework error composition pattern, preserves exact diagnostics end to end, and avoids duplicated common variants or tag collisions. Retaining the last Source Termination in `RetryExhausted` makes terminal status actionable without erasing the adapter failure, runtime failure, or unexpected completion that consumed the retry policy. Executable adapter configuration stays strongly typed without pretending functions and Effects are JSON data. Explicit Layer requirements keep resource ownership composable and make missing adapter services a compile-time error. Treating successful completion as unexpected prevents a Source-Owned Topic from silently becoming stale while health remains ready. Keeping source failures non-terminal for Live Query Subscriptions preserves the last useful result and avoids coupling one supervised ingestion fiber to every consumer lifetime. Adapter tests can provide Layers and consume Streams without starting a complete server, while end-to-end runtime tests still prove mutation convergence and cleanup.
