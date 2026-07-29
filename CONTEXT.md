# View Server Context

This context defines the language for the View Server project: a type-safe live view system that serves initial snapshots and live deltas from an authoritative in-memory engine to React applications over a real server or an in-memory test runtime.

## Document Status

Runtime code and published package exports are authoritative for currently available behavior. The transport-neutral Source Adapter SDK, Runtime Core supervision, Source Health protocol, framework-neutral diagnostics, React Source Diagnostics hook, conformance foundation, first-party Kafka and gRPC Source Adapters, and canonical-only `source` configuration described here are implemented. Transport-specific source shapes have been removed. The Runtime Fatal Signal, Source Lifecycle Gate, Source Application Transition, Source Maintenance Operation, Source Maintenance Interface, degradation-reason-set Source Status, Kafka Cleanup Policy, Kafka Retention Policy, Kafka Broker Contract Validation, and Kafka Retention Projection described below are accepted target behavior but are not yet implemented.

## Language

### Product Concepts

**View Server**:
A runtime that owns configured live topics, ingests row mutations, evaluates live queries, and streams snapshots, deltas, and health to clients.
_Avoid_: Database wrapper, cache server, query proxy

**Real View Server**:
A deployed View Server runtime that serves browser clients over Effect RPC WebSocket and receives mutations from server-side sources.
_Avoid_: Production provider, remote mock, websocket provider

**In-Memory View Server**:
A View Server runtime created inside the current process for tests, demos, Storybook, and browser benchmarks. It uses the same Runtime Core as the Real View Server and swaps only the transport Adapter.
_Avoid_: Mock server, fake client, test hook

**View Server Topic**:
A configured logical table with one Topic Schema and one authoritative store. In the accepted target architecture, it has one canonical Topic Row ID.
_Avoid_: Kafka topic, channel, collection

**View Server Config**:
The one frozen browser-safe declaration created by `defineViewServerConfig(...)` and shared by React, Remote Browser Clients, In-Memory View Server, and the real runtime. It is the sole type authority for View Server Topics, Topic Schemas, queries, Feed Routes, and health. In the accepted target architecture, it also owns source failures and adapter metrics; every Topic owns zero or one canonical Source Definition without a mirrored browser contract and server runtime topic tree. That target declaration contains browser-safe Source Adapter options, including Mapping functions, generated descriptors, platform-neutral codecs, and diagnostic Schemas that may contribute to the browser bundle, but no concrete transport client, credential, platform Layer, Node dependency, or ManagedRuntime. V1 accepts that bundle tradeoff rather than introducing code generation, a build transform, automatic projection, or duplicate authoring.
_Avoid_: Separate contract/runtime configs, duplicated topic tree, generated untyped client metadata, server resource in shared config

**View Server Runtime Effect**:
The scoped server-edge Effect returned by `runViewServerRuntime(viewServer, options)`. In the accepted target architecture, its environment is the exact union inferred from the one View Server Config's Source Adapter runtime services, retry Schedules, and application dependencies. Application code will satisfy that union with aggregate adapter/platform Layers through `Effect.provide(...)` before `NodeRuntime.runMain(...)`. Runtime Core races normal server service against one Runtime Fatal Signal shared by every logical source lifetime; completing that signal fails this root Effect and interrupts the listener plus every source Scope.
_Avoid_: Transport-specific runtime option bag, hidden adapter runtime, reusable module calling Effect.run

**Runtime Composition Failure**:
A fatal typed failure before View Server transport availability caused by invalid or tampered View Server Config, a missing Source Adapter Runtime Service, missing or extra aggregate Layer resource entries, Effect Config failure, failure acquiring mandatory aggregate Layer infrastructure, or an invalid static Source Application State registration descriptor. Descriptor shape, nominal linkage, cardinality, Topic binding, initial-state validation, and required-capability presence are validated during composition before any listener, Source Attempt, consumer, or maintenance sweep is acquired. It fails the View Server Runtime Effect and no server port is opened. An operational Source Attempt acquisition or execution failure is normally isolated by Source Supervision while the runtime and unrelated Topics remain available. Kafka Broker Contract Validation is the accepted narrow exception: the aggregate Kafka Node Layer must verify cleanup and retention before it can safely provide its runtime service, so unavailable or invalid broker configuration is a Runtime Composition Failure.
_Avoid_: Ordinary broker delivery outage classified as fatal startup, partially listening invalid runtime, missing Layer converted to source retry, invalid state descriptor discovered after listener acquisition, unverified Kafka cleanup contract

**Runtime Fatal Signal**:
The Runtime Core-owned one-shot typed failure channel shared with every logical source lifetime and raced structurally against normal server service by the View Server Runtime Effect. An internal invariant breach completes it before awaiting best-effort diagnostics or cleanup, so failure of a background supervisor, maintenance sweep, closed state-transition executor, or impossible duplicate, late, or logical-lifetime-mismatched Source Application State registration propagates upward instead of merely terminating a child fiber. Static descriptor errors are Runtime Composition Failures instead. The bounded interruption-safe Source Settlement ownership and callback-invocation handoff required by the exactly-once contract is the only work that may precede completion; Runtime Core never awaits the returned settlement Effect. The winning signal interrupts the server listener and every source Scope. It is not exposed to Source Adapters, Source Supervision, Runtime Client, browser clients, or health APIs and never carries an expected delivery, transport, or maintenance Delete failure.
_Avoid_: Unjoined background failure, adapter fatal callback, health-only invariant defect, source retry of corrupted state, supervision of registration invariant breach

**Topic Row**:
A Topic-Schema-decoded object stored in a View Server Topic. In the accepted target architecture, every Topic Row contains exactly one required canonical `id: string` field.
_Avoid_: Record, document, message

**Topic Row Value Semantics**:
The schema-derived ownership, equivalence, canonical JSON representation, and ordering rules for every configured Topic Row field. The Column Live View Engine compiles these rules once per View Server Topic and reuses them at ingestion, projection, grouping, comparison, Snapshot, and Delta boundaries. Canonical identity normalizes order-insensitive persistent collections while preserving ordinary sequence order. Topic configuration rejects non-injective or unrecognized codec transformations and equality domains without a congruent canonical identity/order witness.
_Avoid_: Deep clone helper, generic object equality, JSON stringify semantics

**Topic Row ID**:
In the accepted target architecture, the required `id: ViewServerId` field declared by every Topic Schema. Its decoded string uniquely identifies a Topic Row and acts as the final deterministic sort tiebreaker; the field name and Schema are not configurable.
_Avoid_: Configurable Row Key, primary key when discussing external databases, optional ID, numeric ID

**Timestamp**:
A Topic Row temporal value represented as a number or bigint. View Server does not model native JavaScript Date values or date-specific query semantics.
_Avoid_: Date object, native date field

### Query Concepts

**Live Query**:
A typed query against one View Server Topic that returns an initial Snapshot and then Deltas for the same result window. Once submitted, it owns a semantic snapshot of its query values and cannot be changed by later mutation of caller-owned input.
_Avoid_: Subscription query, watch, listener

**Raw Query**:
A Live Query that selects one or more explicit row fields and may filter, sort, offset, or limit rows. An empty or absent `select` is not a Raw Query.
_Avoid_: Select-all query, table scan

**Grouped Query**:
A Live Query that filters source rows, groups them by one or more explicit fields, and returns one or more aggregate aliases. Its `where` addresses source-row Filterable Field Paths before grouping and never addresses aggregate aliases.
_Avoid_: Aggregate-only query, report query

**Field Condition**:
An exact typed predicate on one Filterable Scalar whose named condition type determines every permitted property and operand; operands are named filter and, for a two-bound condition, filterTo. It uses full names such as equals and greaterThanOrEqual, and expresses equality explicitly rather than through a bare value.
_Avoid_: Structured-value predicate, scalar filter shorthand, operator bag, implicit equality, surplus property, value, values, from, to, eq, neq, gt, gte, lt, lte

**Filterable Scalar**:
A nonstructured schema value eligible for Field Conditions, including string, number, bigint, BigDecimal, boolean, and schema-admitted literals or null. Object, array, collection, and class values are not Filterable Scalars.
_Avoid_: Structured filter value, deep equality operand

**Filterable Field Path**:
A schema-derived dot-separated route through statically named object fields to a location with at least one Filterable Scalar schema branch. A dot is exclusively a path separator, so traversable schema field names may not contain one; arrays, tuples, records, maps, sets, and arbitrary objects are traversal boundaries.
_Avoid_: Escaped path segment, tuple path, structured field predicate, deep equality path, dynamic collection lookup

**Equality Condition**:
A Field Condition that compares a field and its schema-admitted operand using Text Matching for string values and Topic Row Value Semantics otherwise. `equals` may target `null` or a string that normalizes to the empty string but never accepts `undefined`; `notEqual` is its exact logical complement and does not implicitly exclude blank values.
_Avoid_: SQL null inequality, implicit notBlank, AG Grid blank defaults

**Negated Condition**:
An explicitly named Field Condition that is the exact logical complement of its positive condition, such as `notEqual` or `notContains`. It is the canonical form of a Negation Expression around that positive leaf and matches blank values whenever the positive condition does not, unless combined with `notBlank`.
_Avoid_: Implicit blank exclusion, SQL three-valued logic

**Domain-Selective Condition**:
A Field Condition available when at least one nonblank member of a union field supports its operation. It evaluates only compatible runtime members without coercion; its Negated Condition remains the complement across the entire field domain.
_Avoid_: Whole-union operator intersection, cross-type coercion, stringified numeric matching

**Condition Operand**:
A Field Condition input whose domain depends on the operation: equality and membership require schema-admitted Filterable Scalars, while text search accepts any string and numeric comparison accepts any valid value of the same numeric kind. Number operands must be finite and treat negative zero as zero; number, bigint, and BigDecimal operands never mix.
_Avoid_: One operand rule for every condition, cross-kind numeric comparison, literal-only search threshold

**Text Condition**:
A Domain-Selective Condition that can compare string values, including equality and membership on fields with a string branch. Every Text Condition uses Text Matching and may declare its sensitivity modifiers; conditions that cannot compare strings may not declare them.
_Avoid_: Implicit case-sensitive text filter, locale-dependent matching

**Text Matching**:
The deterministic string comparison rule shared by every Field Condition: it uses Unicode canonical decomposition, removes combining marks unless `accentSensitive: true`, and lowercases unless `caseSensitive: true`. Conditions with the same effective sensitivities and normalized string operands have one semantic query identity; non-string values and candidates are unaffected.
_Avoid_: Locale-dependent collation, linguistic transliteration, query-wide sensitivity flag, mixed string and numeric coercion

**Blank Condition**:
A zero-operand Field Condition available to every Filterable Field Path that matches when an intermediate or leaf field is missing, or when the resolved value is `undefined`, `null`, or the empty string. Its `notBlank` complement matches every other value, including `false`, numeric zero, and whitespace-only strings.
_Avoid_: JavaScript truthiness, whitespace trimming

**Range Condition**:
A two-operand numeric Field Condition whose `filter` lower bound is included and whose `filterTo` upper bound is excluded. The `inRange` condition represents `[filter, filterTo)` for number, bigint, BigDecimal, and Timestamp fields, uses operands of that field's numeric kind, and is valid only when `filter` is strictly less than `filterTo`.
_Avoid_: Ambiguous between, inclusive upper bound, exclusive lower bound

**Filter Expression**:
A finite acyclic recursive typed predicate composed of Field Conditions, nested AND or OR groups, and unary Negation Expressions. It is the only valid Live Query filter form; field-keyed condition maps and cyclic object graphs are not Filter Expressions.
_Avoid_: Per-column-only filter, flat where object

**Negation Expression**:
An exact unary Filter Expression with type `NOT` and one `condition` that matches the logical complement of its normalized child. It can negate any Filter Expression, while convenient named Negated Conditions remain valid leaves.
_Avoid_: NOT group, conditions array, leaf-only negation

**Root Conjunction**:
The top-level array of Filter Expressions in a Live Query filter. Its entries are always combined with AND; an empty Root Conjunction matches every Topic Row, and an OR must be represented by an explicit nested group.
_Avoid_: Root AND wrapper, field-keyed where object

**Filter Normalization**:
The query-language rule that recursively removes logical groups with no effective Filter Expressions, collapses groups with one effective child, flattens nested groups using the same operator, deduplicates equivalent expressions, and gives commutative groups an order-neutral semantic identity. If no Filter Expressions remain, the query matches every Topic Row; invalid Field Conditions are never treated as empty. It does not apply absorption, distribution, or normal-form conversion.
_Avoid_: Empty OR as false, lenient invalid filter handling, Boolean theorem prover

**Wire-Safe Query**:
A Live Query whose schema-aware representation can round-trip through the Wire Protocol without losing or changing meaning. It admits explicitly encoded scalar kinds such as bigint and BigDecimal, requires optional properties to be absent rather than explicitly `undefined`, and excludes values that cannot round-trip faithfully.
_Avoid_: Native JSON.stringify-safe query, best-effort serialization

**Membership Condition**:
An `in` Field Condition whose candidate array represents an unordered semantic set matched through Text Matching for strings and Topic Row Value Semantics otherwise. Candidate order and equivalent duplicates are immaterial; the empty string and `null` remain distinct candidates, while `undefined` is never a candidate.
_Avoid_: AG Grid Set Filter model, string-key membership, implicit blank sentinel

**Open Membership Condition**:
A Membership Condition with no candidate values. It contributes no predicate, allowing every current or future field value rather than matching no Topic Rows.
_Avoid_: Empty set means false, deny-all membership filter

**Snapshot**:
The first event for a Live Query, containing the current result rows, keys, totalRows, and version.
_Avoid_: Initial response, full refresh

**Delta**:
A live event describing inserts, updates, moves, or removals needed to advance a Snapshot result from one version to another.
_Avoid_: Patch when referring to client-visible result changes

**Status Event**:
A transport-agnostic live event describing readiness, staleness, closure, backpressure, or typed query/runtime failure for a Live Query. Source retry and exhaustion affect this status without attaching Source Adapter Metrics or a complete Source Health snapshot to every Snapshot, Delta, or Status Event.
_Avoid_: Error string, log message

**Subscription**:
The server-side lifetime of one Live Query, including its event stream and close/finalizer behavior.
_Avoid_: WebSocket connection, React hook

### Engine Concepts

**Column Live View Engine**:
The authoritative in-memory engine that owns topics, validates rows, evaluates queries, creates snapshots, computes deltas, tracks subscriptions, and reports engine health.
_Avoid_: Database adapter, query helper, transport runtime

**Topic Store Module**:
The per-topic storage and mutation Module behind a View Server Topic. Today its Implementation is row-oriented with private indexes and query helpers; callers must treat it as the storage Seam, not as a public row bag.
_Avoid_: Map wrapper, row array, topic state bag

**Columnar Topic Store**:
The planned high-performance Implementation behind the Topic Store Module seam, where configured Topic Row fields can be stored and scanned as column-oriented vectors.
_Avoid_: Current storage when discussing today's Implementation, public column API

**Topic Column Vector**:
The planned schema-derived per-field storage inside a Columnar Topic Store. A Topic Column Vector may use a specialized representation such as a numeric typed array or a generic object array, but callers interact through the Topic Store Module.
_Avoid_: Public column API, typed-array contract

**Active Query**:
The engine-side representation of a compiled Live Query that can evaluate snapshots and deltas and may be shared by equivalent subscriptions.
_Avoid_: Query object, filter function

**Raw Query Plan**:
The compiled internal representation of a Raw Query, including predicate hints, deterministic ordering, projection, cache keys, and window scan inputs.
_Avoid_: Query object, filter callback, storage scan object

**Raw Predicate Plan**:
The storage-admissible predicate hint set compiled from a Raw Query, including exact scalar filters and whether row callback evaluation is still required.
_Avoid_: Filter helper, where object, matcher callback

**Raw Ordered Window Index**:
The per-topic ordered slot index used to seek bounded Raw Query windows by storage order and predicate range/equality hints.
_Avoid_: Sort cache, ordered array helper, top-k shortcut

**Grouped Query Plan**:
The compiled internal representation of a Grouped Query, including group key calculation, aggregate definitions, ordering, window settings, and cache keys.
_Avoid_: Grouped query object, aggregate config, groupBy helper

**Query Result Semantics**:
The compiled projection witness that owns and compares one Raw or Grouped Query result shape. It materializes consumer-owned semantic values without exposing authoritative Topic Row or Active Query state.
_Avoid_: Result cast, structured clone, caller-selected result generic

**Health Ledger**:
The owner of counters and sampled health state for mutations, subscriptions, queues, backpressure, ingestion, and transport pressure.
_Avoid_: Health object builder, metrics dump

**Runtime Core**:
The shared engine-backed runtime Module that owns the Column Live View Engine instance, Runtime Client, Live Client, pushed health streams, and lifecycle. Real and in-memory View Servers use the same Runtime Core; only transport and ingress Adapters differ.
_Avoid_: In-memory implementation, test runtime, WebSocket server

### Client And Transport Concepts

**Live Client**:
The transport-neutral client interface consumed by React and in-memory adapters to subscribe to Live Queries and read client-side health.
_Avoid_: Remote client, browser client when the transport is not relevant

**Runtime Client**:
The server-side or in-memory mutation interface used to publish, patch, delete, snapshot, reset, and read fresh runtime health.
_Avoid_: Browser client, live client

**Remote Browser Client**:
The read-only browser client adapter that talks to the Real View Server over the Wire Protocol.
_Avoid_: Runtime client, publishing client

**Wire Protocol**:
The Effect RPC WebSocket protocol using NDJSON serialization and schema-aware JSON-safe encoding for configured topic rows and query values.
_Avoid_: Raw WebSocket protocol, HTTP stream, SSE, MessagePack protocol

**Strict JSON Materializer**:
The neutral Effect utility that turns an already schema-encoded value into a fresh canonical JSON tree or a path-aware typed error. It rejects opaque prototypes, cycles, accessors, sparse arrays, symbols, functions, non-finite numbers, and other values that NDJSON would silently erase or change.
_Avoid_: JSON clone, Schema.Json validator, serializer

**Field Filter Codec**:
The Wire Protocol module that encodes and decodes recursive Filter Expressions and their schema-admitted Filterable Scalar operands, including explicit wire representations for bigint and BigDecimal values.
_Avoid_: Filter helper, JSON helper, where encoder

**Raw Query Codec**:
The Wire Protocol module that validates, encodes, and decodes Raw Query wire payloads while preserving configured Topic Row field semantics.
_Avoid_: Raw query helper, select validator, query parser

**Grouped Query Codec**:
The Wire Protocol module that validates, encodes, and decodes Grouped Query wire payloads, including aggregate alias safety, grouped ordering, and numeric aggregate rules.
_Avoid_: Aggregate helper, groupBy validator, grouped query parser

**Aggregate Row Codec**:
The Wire Protocol module that encodes and decodes grouped aggregate row values without precision loss, including bigint and BigDecimal aggregate envelopes.
_Avoid_: Number helper, aggregate JSON helper, sum formatter

**Health Summary Codec**:
The Wire Protocol module that validates, encodes, and decodes the compact pushed health summary stream.
_Avoid_: Health helper, summary JSON helper, status formatter

**Health Topic Codec**:
The Wire Protocol module that validates, encodes, and decodes the pushed per-topic health stream.
_Avoid_: Topic health helper, health row parser, metrics formatter

**Health Payload Codec**:
The Wire Protocol module that validates full runtime health payloads against configured View Server Topics.
_Avoid_: Health object checker, runtime health helper, admin health parser

**View Server Provider**:
The React provider that supplies a Live Client to hooks.
_Avoid_: Runtime provider, in-memory provider when discussing the generic provider

**View Server In-Memory Provider**:
The React testing provider that owns an In-Memory View Server and supplies its Live Client to the same hooks used in production.
_Avoid_: Seed provider, mock provider

**Live Query Viewport**:
The transport-neutral React integration Module for virtualized grids. It binds one typed Live Query, one inclusive absolute row window, and one caller-owned sparse row sink. React observes only query chrome; row payloads flow directly to the sink. Every full replacement and scroll-only window change has switch-latest ownership, so older snapshots, deltas, statuses, failures, and sink writes cannot mutate the current generation.
_Avoid_: Live grid, grid query language, rows in React state, best-effort cancellation

**AG Grid Adapter**:
The client integration boundary that translates AG Grid viewport, filter, sort, and grouping state into typed Live Queries while keeping the View Server query language independent of AG Grid.
_Avoid_: AG Grid where model, AG Grid query language, core FilterModel

**AG Grid Set Key**:
A string key emitted by an AG Grid Set Filter that the AG Grid Adapter decodes into the corresponding schema-admitted Topic Row field value. A consumer-defined key creator owns reversibility; a lossy or schema-invalid key fails with a typed Adapter error.
_Avoid_: Guessed field value, implicit string field, server-side key reconstruction

### Ingestion Concepts

The Source Adapter concepts in this section describe the implemented core SDK,
Runtime Core path, first-party Kafka and gRPC contract, server, and Node
platform Layers, and the completed canonical-only Source configuration.

**Source Topic**:
An external Kafka topic, gRPC server stream, or other server-side source that provides messages to be mapped into a View Server Topic.
_Avoid_: View Server Topic

**Source Adapter**:
A build-time integration declared once with `SourceAdapter.make(...)` and implemented with `SourceAdapterServer.make(...)`. Its browser-safe declaration owns Source Adapter Identity, one complete Source Adapter Failure Schema, mandatory schema-backed Source Adapter Metrics, supported Source Lifecycles, and complete browser-safe Source Definition constructors. Its server implementation provides the matching nominal Source Adapter Runtime Service whose lifecycle factories acquire scoped Source Attempts and yield streams of Source Deliveries while preserving typed configuration, Mapping, environment requirements, failures, and metrics.
_Avoid_: Runtime-discovered plugin, transport built into Runtime Core, imperative Runtime Client integration, untyped callback

**Source Adapter Package Surface**:
The required package-export boundary for a Source Adapter: `/contract` contains browser-safe Source Definition constructors, Schemas, and optional client service-token factories; `/server` contains the matching Source Adapter Runtime Service implementation and transport-neutral Layers; platform exports such as `/node` contain concrete transport-driver Layers. Every published platform export provides the standard `layer(viewServer, resolvedOptions)` and `layerConfig(viewServer, configWrappedOptions)` pair. Both accept exact adapter-wide resource maps inferred from the View Server Config and return one aggregate scoped Layer providing the adapter runtime service plus all concrete clients and resources. The Config variant accepts exact `Config.Wrap<Options>`, calls `Config.unwrap(...)` once during Layer construction, and retains `Config.ConfigError`; other service requirements remain visible in the Layer environment. Adapter authors consume the SDK only through `effect-view-server/source-adapter`, `effect-view-server/source-adapter/server`, and `effect-view-server/source-adapter/testing`; internal or deep SDK imports are invalid. A published adapter peer-depends on `effect-view-server` plus every Effect ecosystem package used by its public or runtime surfaces, and keeps those packages as development dependencies for its own build and tests. SDK conformance tests prove that `/contract` cannot resolve Node APIs, server implementations, concrete clients, or transport-driver packages.
_Avoid_: Adapter-specific run function, hidden Runtime, missing layerConfig, bundled Effect runtime, bundled View Server SDK, undeclared Effect platform peer, one adapter root that mixes browser contracts and broker clients, hidden platform dependency, untested conditional export

**Source Adapter Conformance Kit**:
The mandatory behavioral and package-boundary test suite exported from `effect-view-server/source-adapter/testing`. For every supported Source Lifecycle, a published adapter supplies a controllable test Layer that can acquire a Source Attempt, emit its real transport event model, fail acquisition or execution with its exact adapter failure, complete unexpectedly, expose metrics changes, and observe scoped finalization. Leased adapters additionally expose exact-route acquisition and final-release observation. Every event model runs the same mandatory readiness, retry, rejection, completion, metrics-cadence, finalization, and Leased sharing invariants. The kit then verifies the semantics the real adapter can produce: complete-delivery adapters expose multi-mutation settlement and lane-shape controls, while continuous-upsert adapters such as gRPC exercise ordered decoded-message Upserts, item rejection continuation, and their infallible no-op settlement. SDK attempt-shape and fallible-settlement invariants are certified by the complete-delivery fixture rather than fabricated inside transports that cannot produce them. The shared kit uses `@effect/vitest` scoped Layer suites and TestClock and also verifies package exports, browser safety, peer dependencies, nominal linkage, Schemas, and positive and negative public type inference. First-party and third-party adapters select capabilities through the same public kit; transport names never create privileged branches.
_Avoid_: Shape-only certification, bespoke adapter test semantics, real-time retry sleeps, optional conformance, first-party exception

**Source Adapter Identity**:
Diagnostic metadata carried by every Source Definition: a required adapter name and optional adapter version. View Server validates this metadata and exposes it in source health, typed errors, spans, and logs; it never uses Source Adapter Identity for registration, dispatch, compatibility, or Source Definition equality. Package-manager peer ranges, public TypeScript API compatibility, nominal SDK brands, and runtime envelope validation enforce adapter compatibility; there is no redundant Source Adapter protocol field.
_Avoid_: Adapter registry key, runtime plugin lookup, SDK protocol version, compatibility dispatch, source identity

**Source Adapter Runtime Service**:
The nominal server-only Effect service implemented by `SourceAdapterServer.make(...)` for one exact Source Adapter declaration. `SourceAdapter.make(...)` creates its opaque browser-safe Effect `Context.Service` tag automatically for nominal type and runtime linkage; adapter authors never declare or repeat a tag ID, service interface, failure type, metrics type, or lifecycle list. The contract contains no implementation or Layer. `SourceAdapterServer.make(...)` accepts only the matching Source Adapter handle and implements exactly its declared lifecycles. The resulting service receives the exact frozen adapter-specific Source Definition, Source Target, Topic-Bound Source Toolkit, and logical-lifetime Scope; supplies the matching lifecycle factory, mandatory local metrics Effect, and default Source Retry Policy; and returns a scoped Source Attempt acquisition Effect. The logical-lifetime Scope is stable across supervised retries and closes on materialized runtime shutdown or final leased-feed release, while source consumers and streams remain owned by the ambient attempt Scope. `runViewServerRuntime(...)` requires this service whenever the one View Server Config uses that adapter. View Server resolves the nominal service directly from Effect Context and never dispatches through Source Adapter Identity or a runtime registry.
_Avoid_: Author-defined duplicate Context tag, repeated tag ID, adapter-name registry, string dispatch, hidden ManagedRuntime, transport branch in Runtime Core

**Source Adapter Metrics**:
The mandatory adapter-defined, Schema-backed metrics value included in every source health payload and inferred exactly through the one View Server Config into Remote Browser Client and React APIs. Each supported Source Lifecycle declares its own metrics Schema instead of a boolean capability, so a selected materialized or leased Source Definition has one exact metrics type without a lifecycle union or optional fields. Its Source Adapter Runtime Service supplies an infallible Effect that reads only a valid local metrics snapshot, including before the Source Stream becomes ready; the Effect's requirements remain visible. View Server samples that Effect exactly once per second with Effect Clock, freezes and Schema-validates the result, and publishes only the cached snapshot. Metric-only publications occur at most once per cadence; lifecycle transitions and Source Item Rejections publish immediately with the latest cached metrics. V1 exposes no global, adapter, source, or subscriber cadence setting. Source health always contains both mandatory SDK-owned `runtime` metrics and mandatory adapter-owned `adapter` metrics. A Source Adapter without matching lifecycle metrics Schemas and runtime implementation is invalid.
_Avoid_: Optional details, missing metrics, unknown metrics object, server-only untyped health payload

**Source Rejection Location**:
The mandatory lifecycle-specific Schema-backed adapter value identifying one rejected source item without exposing its raw payload. Each Source Adapter lifecycle declares its exact location Schema beside its metrics Schema, and its Source Item Rejection constructor accepts only that type. Kafka uses safe region, external topic, partition, offset, and phase fields; gRPC uses safe logical client, method, and stream-item context. The exact value round-trips through Source Diagnostics and structured telemetry.
_Avoid_: Raw key or value, unknown location object, optional location, message payload in health

**Source Runtime Metrics**:
The mandatory SDK-owned metrics value paired with Source Adapter Metrics in every source health payload. It contains epoch-nanosecond `bigint` timestamps named with an `AtNanos` suffix, cumulative source-wide `bigint` counters for attempts, retries, deliveries, rejected source items, mutation outcomes, and settlement outcomes, a numeric retained-row count, and a non-empty list of Source Delivery Lane metrics. Every lane has a stable, non-empty, retry-stable unique identifier and an exact unbuffered-or-bounded Source Buffer value; a simple source therefore still reports one lane. Status and failures remain outside metrics. View Server derives every public or persisted epoch timestamp from Effect `Clock.currentTimeMillis`, validates that wall time as a non-negative safe integer, and converts it as `BigInt(wallMillis) * 1_000_000n`; it reserves `Clock.currentTimeNanos` for monotonic elapsed-time measurement and never labels it as epoch time. Contracts and the Wire Protocol carry raw `bigint`, never Date or Temporal objects, so consumers may explicitly construct `Temporal.Instant` without losing precision.
_Avoid_: Aggregate-only buffer metrics, empty lanes, unstable lane ID, millisecond timestamp, number timestamp, ambiguous time unit, Date, Temporal object on wire, optional buffer metrics

**Source Status**:
The mandatory Schema-backed tagged union describing exactly one Source lifecycle state: `Starting`, `Ready`, `Degraded`, `WaitingToRetry`, `Reacquiring`, `Exhausted`, or `Stopping`. Each branch contains only its valid fields and nanosecond timestamps. The `Degraded` reason-set shape below is accepted target behavior tracked by PRD #400 and implementation issues #401 and #402; the existing rejection-only health Schema and consumers remain the pre-implementation state rather than the accepted contract. Runtime Core owns a logical-lifetime degradation ledger with these invariants:

- Each settled Source Item Rejection stores its latest exact safe diagnostic, replacing the prior rejection reason. That reason is never cleared before logical source shutdown because skipped input may leave the view incomplete.
- An Adapter Maintenance Failure means asynchronous adapter-owned correctness work failed without terminating ingestion. It remains active while any corresponding failed work is outstanding and clears only after that backlog reaches zero through successful retry or cancellation.
- `Degraded` means delivery continues while the ledger is non-empty. Its exact public shape is `{ readonly _tag: "Degraded"; readonly attempt: bigint; readonly degradedAtNanos: bigint; readonly reasons: SourceDegradationReasons<AdapterFailure, RejectionLocation> }`; its Schema requires a positive `attempt` and non-negative epoch-nanosecond `degradedAtNanos`. `degradedAtNanos` is the wall-clock epoch time of the logical-lifetime ledger's latest empty-to-non-empty transition. It remains unchanged while any reason remains—including rejection replacement, maintenance activation or recovery, hidden operational-status precedence, retry, reacquisition, and later readiness—then is discarded when the final reason clears; the next empty-to-non-empty episode samples a new value.
- `SourceDegradationReasons` is the canonical non-empty tuple union `readonly [SourceItemRejectionReason] | readonly [AdapterMaintenanceFailureReason] | readonly [SourceItemRejectionReason, AdapterMaintenanceFailureReason]`. `SourceItemRejectionReason` is exactly `{ readonly _tag: "SourceItemRejection"; readonly latestRejection: SourceItemRejectionDiagnostic<AdapterFailure, RejectionLocation> }`; `AdapterMaintenanceFailureReason` is exactly `{ readonly _tag: "AdapterMaintenanceFailure" }`. The combined tuple always orders rejection before maintenance. Empty, duplicate, reversed, unknown, optional, and surplus reason shapes are invalid at both TypeScript and Schema boundaries. Exact adapter-specific maintenance details remain in Source Adapter Metrics.
- Operational `Starting`, `WaitingToRetry`, `Reacquiring`, `Exhausted`, and `Stopping` branches retain precedence while ledger updates continue internally. The next successful attempt becomes `Degraded` when reasons remain and `Ready` otherwise.

Waiting and reacquisition retain the exact Source Termination; exhaustion retains Source Retry Exhaustion; stopping names runtime shutdown or final Leased Feed release. React and Remote Browser Client APIs infer the complete union and narrow exhaustively on `_tag` without optional failure or retry fields. Live Query availability maps `Degraded` to ready while Source Diagnostics, Topic health, aggregate View Server health, and liveness/readiness payloads retain the distinction.
_Avoid_: Status string plus optional fields, nullable failure bag, Kafka-specific generic status branch, adapter maintenance hidden only in metrics, ambiguous retry phase, erased termination

**Source Target**:
The mandatory tagged union identifying the source health subject as either `Materialized` with no route or `Leased` with its exact Feed Route. Route presence is never optional.
_Avoid_: Optional route, lifecycle string with unrelated route field, inferred target

**Source Diagnostics**:
The explicit scoped `subscribeSourceHealth(...)` Remote Browser Client operation and `useSourceHealth(...)` React hook for observing cadence-cached Source Health independently of Live Query events. Both are addressed only by exact View Server Topic and, for a leased source, its exact Feed Route; Source Adapter Identity and transport client names are never lookup keys. Their inputs are inferred from the one View Server Config: source-free Topics are invalid, materialized Topics reject `routeBy`, leased Topics require exact `routeBy`, and unknown, missing, or extra Route Fields fail without `as const`. A subscription emits the latest cached value immediately and then emits only subsequent cached health changes. React consumes it through Effect Stream and Atom integration; local consumers share the same keyed subscription, and Scope closes the remote subscription on unmount or client shutdown. There is no separate one-shot source-health operation in v1. Materialized Source Diagnostics always yield the exact active Source Health type. Leased Source Diagnostics instead yield an exact `Inactive` or `Active` tagged union: `Inactive` contains the exact Feed Route and no fake metrics, while `Active` contains the complete exact Source Health. Observing an inactive route never creates or retains its Leased Feed, executes its Request Factory, acquires a Source Attempt, delays final release, or preserves route-owned rows. Ordinary Live Query data APIs remain transport-agnostic and never carry adapter metrics on their Snapshot or Delta hot path.
_Avoid_: Metrics on every Delta, implicit broker API in useLiveQuery, adapter-ID lookup, transport-client lookup, optional leased route, untyped details bag, live health network request per event

**Source Adapter Failure**:
An adapter-specific typed failure described by the Source Adapter's required Schema and carried either by a Source Item Rejection diagnostic or the Source Stream's Effect error channel. The Source Adapter wraps foreign library errors into its complete failure union and owns redaction; every field admitted by its failure Schema is safe for consumers. An Adapter Failure is one branch of Source Execution Failure and is never required to repeat SDK-owned failure variants. Whether an exact failure is item-local and settled or attempt-terminal is determined by the Source Lane Event versus Stream error channel, not by erasing its type.
_Avoid_: Unknown error, opaque transport exception, raw library error, erased Effect error channel

**Source Runtime Failure**:
A schema-backed SDK-owned failure produced by the common ingestion pipeline, such as Source Buffer overflow, invalid Source Delivery, invalid Topic Row, leased Route Field mismatch, or `InvalidSourceSettlement`. `InvalidSourceSettlement` is the exact redacted branch produced when a Source Settlement or Rejection Settlement callback throws synchronously before returning its Effect and ordinary termination wins the Source Attempt's cancellation arbitration; it never exposes the thrown value. Each attempt owns one closed atomic arbitration state. Shutdown or final leased-feed release marks cancellation requested before interrupting attempt fibers. After callback application and before publishing any Source Termination, supervision-derived health transition, or Schedule input, the workflow atomically claims ordinary termination only if cancellation has not already won; a cancellation request made after any application or rejection-recording `Exit` was captured but before that handoff still wins. When cancellation wins, callback application and failed-settlement accounting remain exactly once while the throw is recorded only as secondary redacted diagnostics, never as a Source Supervision input. This suppresses only callback-throw-induced supervision state: a rejection already recorded before settlement retains its `rejectedItemCount`, sticky rejection ledger, `Degraded` publication, and exact metrics subject to ordinary shutdown-status precedence. The transition-defect fatal path likewise keeps the callback throw secondary to the original invariant breach. Every Source Adapter receives this exact shared failure vocabulary automatically rather than copying it into its adapter-specific failure union. An item-local invalid Topic Row or Route Field mismatch may be carried by a settled Source Item Rejection when its ordered source remains usable; a failure value is not automatically terminal merely because it belongs to this vocabulary.
_Avoid_: Adapter-defined copy of SDK error, string-only runtime failure, untyped ingestion exception

**Source Execution Failure**:
The exact tagged union of either an Adapter Failure carrying the Source Adapter's inferred failure type or a Source Runtime Failure carrying the SDK-owned failure type. The outer tags prevent collisions between adapter and SDK failure tags and preserve both branches through Source Item Rejection diagnostics, supervision, source health, Live Query status, and the Wire Protocol. A Source Execution Failure carried by a settled rejection is non-terminal; the same vocabulary in the Stream error channel terminates its Source Attempt.
_Avoid_: Flattened error union, tag collision, erased adapter failure, message-only status

**Source Definition**:
The topic-owned opaque declaration created once by a Source Adapter's browser-safe materialized or leased constructor inside `defineViewServerConfig(...)`. It carries Source Adapter Identity, Source Lifecycle, Route Fields when leased, exact failure, metrics, and Source Rejection Location Schemas, the nominal Source Adapter Runtime Service requirement, other browser-safe Effect requirements, an exact default-or-override retry selection, and the adapter-owned validated per-source options. It carries no concrete transport client, client service token, credential, platform Layer, Runtime Client, or executable runtime service. Importing and invoking the Source Adapter is build-time declaration; View Server does not resolve adapter names through a registry.
_Avoid_: Arbitrary source object, second server topic declaration, transport-specific topic property, adapter registry entry, adapter ID lookup, concrete client

**Source Definition Constructor**:
A browser-safe adapter-owned public function that validates and snapshots adapter-specific options before invoking exactly one SDK Materialized or Leased lifecycle primitive. Its public name is domain-specific rather than globally prescribed: Kafka may expose `source(...)`, a gRPC helper may expose `materialized(...)` and `leased(...)`, and another adapter may use its transport vocabulary. Constructor spelling never creates a lifecycle; the nominal Source Definition records the exact SDK lifecycle, and conformance proves it.
_Avoid_: Lifecycle inferred from method name, mandatory global constructor spelling, third lifecycle, raw Source Definition object

**Source Adapter Resource Reference**:
An adapter-owned logical literal string in a browser-safe Source Definition that selects one concrete runtime resource from the adapter's aggregate Layer. Each adapter chooses its own field and collection vocabulary, such as Kafka Region, gRPC client, RabbitMQ connection, or another logical endpoint. The value is never a URL, credential, concrete client, or per-resource Context service tag. The aggregate Layer derives the exact required literal union from all matching Source Definitions, rejects missing and extra resource entries, constructs one O(1) lookup map during Layer acquisition, and uses it for every Source Attempt. An adapter needing no named external resource declares no Resource Reference.
_Avoid_: Bootstrap address in Source Definition, credential alias resolved globally, duplicated registration tree, Context tag per connection, repeated linear scan

**Source Lifecycle**:
The explicit runtime-owned lifetime category of a Source Definition: materialized or leased. One materialized source begins Source Attempt acquisition when the View Server Runtime Effect starts and remains active for that runtime Scope independently of Live Queries. A leased source acquires one supervised Source Attempt on demand for each exact Feed Route shared by one or more Subscriptions.
_Avoid_: Implicit adapter lifecycle, generic source mode, transport-specific lifecycle

**Source Attempt**:
One View Server-owned child Scope running the scoped Effect returned by a Source Adapter lifecycle factory. The Effect acquires or subscribes to every attempt-specific consumer, subscription, channel, iterator, callback registration, or lease and yields one or more continuous Source Delivery Lanes. The outer aggregate adapter Layer may provide shared transports, pools, factories, credential refreshers, and resource maps, but it never owns a source's attempt-level subscription permanently. Successful Effect acquisition is the exact readiness handshake even when every lane remains idle; failure before all required lanes are acquired never reports ready. Each lane applies and settles deliveries sequentially, while sibling lanes run concurrently without a hidden merge buffer. Failure or unexpected completion of any lane terminates the complete attempt, interrupts its siblings, closes its Scope, and passes Source Termination to Source Supervision. Source Attempt finalizers follow Effect's infallible-finalizer rule: they are idempotent, record external close rejection in mandatory adapter metrics and structured diagnostics, and complete before retry. Retry creates a fresh Scope and reacquires every attempt-level resource and lane without rebuilding the application Layer.
_Avoid_: Single globally serialized multi-cluster stream, first-delivery readiness, Ready stream event, connection polling, reused failed scope, adapter-owned retry scope

**Source Delivery Lane**:
One continuous ordered Stream of Source Lane Events inside a Source Attempt. A pull source normally exposes one lane; an adapter that acquires several independent ordered inputs may expose a non-empty collection of lanes. View Server consumes each lane event sequentially and sibling lanes concurrently using structured Effect fibers rather than merging them through a hidden queue. A settled Source Item Rejection does not terminate its lane. One lane's actual Effect failure or successful completion terminates the entire Source Attempt, so Source Supervision retries the attempt as one ownership unit.
_Avoid_: Global serialization across independent clusters, unowned fork, hidden merge queue, per-lane retry scope

**Source Lane Event**:
One nominal SDK-created ordered element in a Source Delivery Lane: either a Source Delivery containing non-empty mutations or a Source Item Rejection containing no mutation. View Server applies and settles a delivery or records and settles a rejection before consuming the next event in that lane. Effect Stream failure remains separate and terminates the Source Attempt.
_Avoid_: Raw union lookalike, empty Source Delivery, rejected item in Stream error channel, unordered side-channel

**Topic-Bound Source Toolkit**:
The narrow nominal helper passed by View Server to one Source Adapter lifecycle factory. It exposes the exact View Server Topic name plus Topic-bound `upsert`, `delete`, `delivery`, and `reject` constructors that preserve and runtime-validate the configured Topic Row, canonical ID, adapter failure, lifecycle rejection location, Feed Route, and settlement types. A singleton Source Delivery may carry one closed nominal Source Application Transition that Runtime Core executes inside the serialized mutation critical section after mutation success. Its transport-neutral Source Maintenance Interface accepts only a closed Source Maintenance Operation issued by the SDK-owned Source Application State Module and returns the complete application `Exit` to adapter-owned maintenance. The operation itself binds its success/failure failed-work, exact adapter-metrics, and degradation-ledger transitions, so the Interface exposes no separate report/recover callback. It does not accept an ordinary Source Mutation, so adapters cannot turn maintenance into unordered ingress that bypasses Source Delivery settlement, supervision, backpressure, or accounting. Maintenance application traverses the same Column Live View Engine mutation and Delta path as Source Delivery, but its expected failure is returned to the adapter-owned maintenance Module instead of terminating a Source Attempt. The Interface exposes no Runtime Client, publish callback, Subscriber, session, reference count, browser header, internal Topic Store, mutable config, raw Schema-validation bypass, or arbitrary transition Effect. The adapter's own external codecs perform transport decoding; View Server's constructors own final Topic contract validation.
_Avoid_: Runtime Client, generic publish function, maintenance disguised as Source Delivery, Topic Store handle, raw Schema decoder, mutable topic object

**Source Application State Module**:
The deep server-only SDK Module that owns adapter-specific logical state coupled to View Server mutation application, including Kafka expiry generations, failed-work identities, and the local state from which exact adapter metrics are sampled. Every finite Kafka deadline receives a generation token that is never reused within its logical source lifetime, including after keyed Delete and later reinsertion of the same canonical ID; a lifetime-monotonic `bigint` sequence is the reference model, and only logical-lifetime destruction may reset it. A `SourceAdapterServer.make(...)` lifecycle implementation declares zero or one nominal registration descriptor for its bound Topic; stateless lifecycles declare none, while every retention-capable Kafka materialized lifecycle declares exactly one. Runtime Core validates descriptor shape, nominal linkage, cardinality, Topic binding, initial state, and required capability during Runtime Composition before acquiring any listener, Source Attempt, consumer, or maintenance sweep, and invalid static registration is a typed Runtime Composition Failure. It instantiates the declared Module once per logical source lifetime; the same instance survives retries and closes with that lifetime. An impossible duplicate, late, or logical-lifetime-mismatched registration observed after composition is a Runtime Fatal Signal invariant breach, never Source Supervision or health-only degradation. The descriptor contains a validated immutable initial state plus one total synchronous reducer for the Module's closed nominal command algebra. That reducer is the explicit Adapter seam: it must return promptly, cannot return or run an Effect, Promise, foreign I/O, timer, or blocking work, and is never stored inside an issued operation. The SDK-owned implementation owns synchronization and exposes two narrow faces. Adapters receive nominal constructors for exact Topic-bound Source Application Transitions and Source Maintenance Operations, an infallible local metrics snapshot read, and one deep `runDueSweep(epochNowNanos)` operation for maintenance-capable registrations. `runDueSweep` keeps enumeration inside the SDK-owned Module: it reads the private expiry index, owns every due-candidate generation recheck, interruptible canonical-ID permit bracket, and closed Source Maintenance Operation execution, and returns only its aggregate sweep outcome. It exposes no due-entry collection, cursor, claim token, index, lock, work identity, or arbitrary mutation capability. Runtime Core receives only private application operations that execute the registered reducer under the existing mutation critical section and Source Lifecycle Gate. Runtime Core neither sees the adapter state nor interprets Kafka work identities or metrics. A reducer or `runDueSweep` closed-executor defect is the corresponding invariant breach, while expected per-item maintenance failure remains a closed outcome and ordinary adapter I/O remains outside this Module.
_Avoid_: Per-operation callback, Effectful reducer, late attempt registration, Runtime Core inspecting Kafka state, public Ref, adapter-owned lock, report/recover side channel

**Source Application Transition**:
The transport-neutral closed nominal operation optionally carried by a singleton Source Delivery when adapter-local logical state must change consistently with one Topic Row mutation. It is constructed only through the SDK-owned Source Application State Module and contains no user callback, Effect, requirements, Promise, foreign I/O, or suspension. Runtime Core executes it exactly once after successful row mutation and before leaving the serialized uninterruptible application critical section; typed failure, defect, or cancellation of row application does not execute it. Kafka uses the closed operation while holding the canonical-ID lease to replace or remove the exact expiry-index generation. Transition-bearing deliveries are statically and defensively restricted to exactly one mutation because ordinary multi-mutation Source Delivery is settlement-atomic but not state-atomic. An internal transition-executor defect is an SDK invariant breach. Runtime Core records the original defective application `Exit` inside the one outer delivery `Effect.uninterruptibleMask((outerRestore) => ...)`, then registers an attempt-owned supervised settlement child while still masked. In that child's bounded masked prefix it applies the Source Settlement callback to the Exit exactly once to obtain its Effect while capturing synchronous throw as secondary failure, records the SDK-owned invocation marker, and signals the handoff latch. If callback application returned an Effect, the child evaluates only `outerRestore(returnedSettlementEffect)` afterward. It never creates a nested `uninterruptibleMask` and uses that nested mask's `restore`, because a mask entered while already uninterruptible would restore only to uninterruptible status. The parent awaits only the latch and then completes the Runtime Fatal Signal. Exactly-once means callback application, not settlement completion or execution of the returned Effect's first adapter instruction. Fatal propagation never awaits settlement completion, while concurrent root shutdown cannot reduce callback invocation to zero. Any settlement failure is recorded only as secondary diagnostics and metrics and cannot replace or downgrade the original defect into Source Supervision; Kafka preserves the offset because the application Exit is not successful. Root failure interrupts the registered child while it runs under `outerRestore`. An interruptible `Effect.never` may therefore model a settlement awaiting transport until shutdown; masking or ignoring Scope interruption is non-conformant. Non-completion of the closed transition itself is impossible by construction. Because both row and index are in-memory, safe reconstruction after this fatal defect requires an authoritative replay position or a durable checkpoint; Kafka v1 provides no checkpoint, and `latest`, `committed`, or bounded replay positions do not become authoritative merely because the failed record was left uncommitted.
_Avoid_: Adapter callback, arbitrary Effect, multi-mutation transition, index update in external settlement, foreign I/O under mutation serialization, Kafka branch in Runtime Core, swallowed transition defect

**Source Maintenance Operation**:
A closed nominal SDK-issued capability for exactly one registered adapter-state maintenance action. The SDK-owned Source Application State Module binds its exact Topic, keyed engine mutation, matching state transition, stable work identity, and closed success/failure ledger plus exact adapter-metrics transitions when the adapter registers that state Module; an adapter cannot construct one from an ordinary Source Mutation or attach a user callback, Effect, Promise, I/O action, or structural lookalike. Runtime Core may execute it only through the Source Maintenance Interface while holding the logical source's Source Lifecycle Gate. Inside that admission it applies the mutation, adapter-state transition, failed-work update, exact adapter-metrics update, and transport-neutral degradation-ledger transition as one serialized uninterruptible outcome. An internal maintenance-transition executor defect becomes the defective maintenance application Exit, invokes no Source Settlement, completes the Runtime Fatal Signal, and relies on the surrounding `acquireUseRelease` or child Scope to release the lease.
_Avoid_: Arbitrary maintenance mutation, public operation constructor, unordered ingress, adapter callback, structural operation lookalike

**Source Maintenance Interface**:
The deep transport-neutral Interface nested in a Topic-Bound Source Toolkit for adapter-owned asynchronous correctness work that is not an external delivery lane. It accepts exactly one closed Source Maintenance Operation and no separate report/recover callback. Under the Source Lifecycle Gate it admits the operation only while the visible operational status is `Ready` or `Degraded`, then applies its bound mutation, adapter-state transition, failed-work update, exact adapter-metrics update, and transport-neutral degradation-ledger transition through the ordinary engine path as one serialized uninterruptible outcome before returning the complete application `Exit`. Source Supervision acquires the same Gate for operational status transitions, so readiness cannot change between admission and application. The Interface invokes neither Source Settlement nor Source Supervision. Failed work is an adapter-owned set keyed by stable work identity such as canonical ID plus generation: the first failure adds the item, repeated retry failures update cumulative attempt metrics without increasing backlog cardinality, and successful retry removes exactly that item. Interruption, retry suspension, or exhaustion does not remove a failed-work identity while its matching due generation remains indexed. A successful newer delivery that atomically replaces or removes that generation also cancels the now-stale identity; an explicit stale-generation observation may do the same. The marker clears only when this set is empty. Operational lifecycle status retains precedence as defined by Source Status. Exact adapter-specific failure details remain mandatory in Source Adapter Metrics. A maintenance failure atomically replaces the retained latest safe failure and increments cumulative failure state; successful recovery may clear active failed-work, backlog, and degradation state but never clears that retained failure or its cumulative state. Failure and recovery publish their Source Status and derived Topic/aggregate health transitions immediately with the latest cached metrics. Only metric-only detail changes coalesce into the next ordinary one-second metrics sample, so failure followed by sub-cadence recovery still appears in at least that next bounded metrics publication. The Interface is scoped to the logical source lifetime and cannot query rows, accept ordinary mutations, bypass Topic validation, expose engine storage, or execute adapter code.
_Avoid_: Kafka branch in Runtime Core, maintenance failure on a Source Stream, opaque metrics inspection, direct Topic Store mutation, health-only side channel

**Source Delivery**:
One nominal SDK-created Source Adapter emission containing a `Chunk.NonEmptyChunk` of ordered Source Mutations whose settlement is tied to the outcome of applying those mutations to its Source-Owned Topic. Its constructors are bound to the exact Source Definition and Topic: Upsert accepts only a complete Topic Row and Delete accepts only that Topic's key. Only the dedicated singleton constructor may carry one closed Source Application Transition; ordinary multi-mutation deliveries reject transitions in public types and defensive runtime validation. An empty source poll emits no Stream element rather than an empty Source Delivery. Delivery settlement is atomic, but Runtime Core state application is not transactional: mutations applied before a later mutation fails remain visible.
_Avoid_: Empty delivery, raw lookalike object, bare Topic Row, already-acknowledged message, direct Runtime Client command

**Source No-Op Item**:
An external source item that an adapter intentionally maps to no View Server mutation, such as a heartbeat or adapter-level filter miss. The adapter settles it inside the pull-ordered Source Stream production effect before proceeding; settlement failure becomes an Adapter Failure and enters Source Supervision. A malformed, unparseable, invalid, or authoritative expired Kafka row is not a Source No-Op Item. The SDK defines no Skip mutation or empty Source Delivery, while a tombstone remains a real keyed Delete even when its Topic Row is already absent.
_Avoid_: Empty Source Delivery, generic Skip mutation, transient expired Upsert, treating tombstone as no-op, unordered early acknowledgement

**Source Item Rejection**:
A nominal SDK-created Source Lane Event for one item-local decode, Mapping, canonical-ID, Route Field congruence, or Topic Schema failure that does not make the underlying ordered source unusable. It carries the exact schema-backed safe failure, adapter-owned safe source location, `rejectedAtNanos`, and one Rejection Settlement synchronous construction callback that receives the complete rejection-recording `Exit` and returns its settlement Effect; it never contains the raw payload automatically. Runtime Core applies the same outer-mask ownership rule as Source Delivery: rejection recording runs through the outer mask's restore, callback application occurs exactly once in the bounded masked prefix, and only the returned Effect runs through that same outer restore in the Source Attempt Scope. A synchronous callback throw becomes exact redacted `InvalidSourceSettlement`, and a returned typed failure enters ordinary Source Supervision, only when ordinary termination wins the attempt cancellation arbitration before supervision handoff. If cancellation was requested before that handoff—even after a non-cancelled recording `Exit` was captured—lifecycle cancellation has precedence: the callback still runs exactly once, but its synchronous throw is secondary redacted diagnostics only, with no Source Termination, callback-throw-induced supervision health transition, retry-Schedule step, reacquisition, or commit. The successfully recorded rejection retains its `rejectedItemCount`, sticky rejection ledger, `Degraded` health publication, and exact metrics subject to ordinary shutdown-status precedence. View Server records the rejection, increments `rejectedItemCount`, marks Source Health, the affected Topic health row, and aggregate View Server health `Degraded`, and executes settlement before pulling the next lane event. Kafka commits only after both rejection recording and settlement succeed, while gRPC uses a no-op callback and continues only when the decoded response stream remains usable. `Degraded` is sticky for the logical source lifetime and Live Query availability remains ready. Liveness and readiness transports remain successful while reporting degraded state rather than removing or restarting the instance.
_Avoid_: Poison pill retry loop, silent drop, raw payload in health, fake mutation, immediate recovery to Ready, transport failure treated as item rejection

**Source Buffer**:
A finite SDK-owned bridge used only when an external push or callback producer cannot remain directly pull-driven by its Source Stream. The SDK exposes separate constructors for backpressurable and non-pausable producers so their emission contracts cannot be confused. The backpressurable emitter returns an Effect that suspends at capacity and must be composed by the producer. The non-pausable synchronous emitter returns no ignorable capacity result; on the first full-buffer emission, the SDK increments overflow metrics and fails the Stream exactly once with `SourceBufferOverflow`. Both constructors use an internal bounded Queue, validate a positive finite integer capacity during pure construction, own callback unregistration through Scope, and update depth and high-water mark locally. The Queue and its strategy are never exposed. Sliding, dropping, and unbounded buffering are invalid because losing one Upsert or Delete can corrupt the authoritative materialized view.
_Avoid_: Public Queue, strategy option, shared callback constructor, ignored offer boolean, unbounded callback queue, silent drop, sliding mutation buffer, hidden View Server prefetch

**Source Mutation**:
A nominal SDK-created complete-row change inside a Source Delivery: either an Upsert containing one complete Topic Row with its canonical Topic Row ID, or a Delete identifying one Topic Row ID. Source Mutation constructors are bound to the exact Source Definition and Topic so invalid rows, IDs, and structural substitutes fail before reaching View Server. The common SDK exposes no separate storage key and no ID-plus-partial-row Upsert. An Upsert inserts a missing row or completely replaces an existing row; partial patch mutations are not part of the Source Adapter contract.
_Avoid_: Raw mutation object, partial source patch, merge-upsert, transport-specific event, Runtime Client command

**Kafka Local Row Key**:
The transport-local string returned only by a delete-only first-party Kafka Source Definition's `localRowKey` callback before the adapter constructs `region:partition:localRowKey`. It receives decoded key, decoded non-null value, and Region because delete retention has no broker key-equality contract. Region identifiers are non-empty and may not contain `:`; partition is the canonical non-negative decimal integer; local keys may contain later colons. Compaction-capable Source Definitions expose no `localRowKey`: their canonical identity is derived exclusively from Region, partition, and Serialized Kafka Key Identity, so application behavior cannot split one Kafka compaction key across several rows.
_Avoid_: Compact-source `localRowKey`, user-composed Kafka public key, `rowKey` callback, hidden region or partition namespace, delimiter without validation, region normalization

**Serialized Kafka Key Identity**:
The SDK-owned reversible injective text encoding of the exact non-null serialized Kafka key bytes captured before decoding for `compact` and `compact-and-delete`. Its canonical segment is `k` followed by the unpadded base64url bytes, contains no `:`, and forms the complete Topic Row ID `region:partition:k<unpadded-base64url(serializedKeyBytes)>`. Within one Region and partition, byte-for-byte equal inputs therefore produce the same ID and byte-distinct inputs produce distinct IDs, regardless of codec output, mutable state, randomness, or application callbacks; Region and partition deliberately remain separate identity namespaces. Application code cannot provide, normalize, hash, or override it. Policy-specific browser-safe helpers accept `Uint8Array`, never Node `Buffer`, and disclose that the public ID reversibly represents serialized key bytes. The Kafka server Adapter uses the same operation for Upserts, compaction-capable tombstones, already-expired Deletes, deadlines, leases, and failed-work identities; Runtime Core receives only the nominal final ID and never sees Kafka bytes.
_Avoid_: Decoded-key identity, callback suffix, collision-prone digest, secret-key assumption, Runtime Core byte handling

**Kafka Cleanup Policy**:
The mandatory browser-safe semantic declaration on every first-party Kafka Source Definition: `delete`, `compact`, or `compact-and-delete`. The aggregate Kafka Node Layer compares it during Kafka Broker Contract Validation with the effective `cleanup.policy` of the external topic in every selected Region. Broker text is interpreted as an unordered comma-separated set with insignificant surrounding whitespace, so `compact,delete`, `compact, delete`, and `delete,compact` are equivalent. A compaction-capable declaration requires a metadata-free Kafka Compaction Key Codec whose decode input contains only the non-null serialized key bytes; null keys are Source Item Rejections. It exposes no `localRowKey`, and the adapter derives canonical identity exclusively from Region, partition, and Serialized Kafka Key Identity before decoding. Offset, timestamp, headers, decoded-key equivalence, mutable state, and randomness therefore cannot split or collapse Kafka compaction identity. A `delete` declaration may use the ordinary Kafka key codec and supplies decoded key, decoded non-null value, and Region to its required `localRowKey`; a null-valued record on that source is a Source Item Rejection rather than a tombstone.
_Avoid_: Unvalidated cleanup hint, ordered policy string, value-derived compacted key, delete-only tombstone

**Kafka Retention Policy**:
The mandatory browser-safe row-lifetime declaration on every first-party Kafka Source Definition: `match-kafka-retention` or an Effect `Duration.Input`. Matching Kafka retention resolves independently in every selected Region from the effective external-topic `retention.ms`; it retains forever for a compact-only topic or a Kafka value of `-1`, and otherwise uses the resolved non-negative duration. A matched zero keeps the universal deadline rule: deadline equals Kafka record timestamp plus zero, so a record is immediately eligible only when its timestamp is not later than the sweep's Effect Clock wall time; a future-dated record becomes eligible at its own timestamp. Kafka record timestamps and deadlines are Unix-epoch nanoseconds. The adapter converts validated Kafka millisecond timestamps and non-negative safe-integer `Clock.currentTimeMillis` values separately with `BigInt(milliseconds) * 1_000_000n`, performs duration addition and comparison in `bigint`, and never uses number-space nanosecond multiplication or compares a deadline with monotonic `Clock.currentTimeNanos`. Wall-clock jumps honestly affect eligibility, while Effect sleep/TestClock owns the monotonic sweep cadence. An explicit positive duration is authoritative local policy and may be shorter or longer than the broker value; Effect infinity explicitly retains forever, while explicit zero and negative durations are invalid configuration. The phrase “match Kafka retention” means matching configured time-based retention, not observing exact physical segment deletion or size-based `retention.bytes` behavior.
_Avoid_: Exact broker-log mirror, implicit retention default, per-record wall-clock duration input, retention.bytes emulation

**Kafka Broker Contract Validation**:
The mandatory startup operation performed by the aggregate Kafka Node Layer before it provides the Kafka Source Adapter Runtime Service. It batches effective topic-configuration reads per Region through the Kafka Admin client, validates every declared Kafka Cleanup Policy, and resolves every matching Kafka Retention Policy. Missing `DESCRIBE_CONFIGS` authority, an unavailable or malformed configuration response, a cleanup mismatch, or an unusable `retention.ms` is a typed Layer acquisition failure naming every affected Region and external topic; the View Server Runtime Effect does not start and no server port opens. This is a deliberate narrow exception to ordinary Source Supervision because ingestion under an unverified cleanup/retention contract could silently retain or delete the wrong Topic Rows. The validated broker values are a point-in-time startup snapshot. Changing `cleanup.policy` or `retention.ms` while the runtime is active is an unsupported operational mutation: operators must stop every affected View Server instance, change the broker configuration, and restart so validation runs before ingestion resumes. V1 deliberately performs no background revalidation and makes no correctness guarantee for an out-of-contract broker mutation that bypasses this coordinated restart.
_Avoid_: Raw Kafka protocol client, first-record validation, partially started runtime, health-only cleanup mismatch

**Kafka Retention Projection**:
The best-effort local expiration of Kafka-owned Topic Rows according to their Kafka Retention Policy. One logical Materialized Topic lifetime owns the keyed lock/index Module and a coarse Effect Clock sweep across Source Attempt retries. Its mutation sequence is:

1. Each Kafka Region flattens or `Stream.rechunk(1)`s every upstream transport batch into singleton record chunks before the first mapping stage that can acquire a canonical-ID permit. For one singleton, every item-local rejection-prone operation completes first: required decoding, delete-only Local Row Key callback, Mapping, canonical-ID validation, and complete Topic Row validation. A Source Item Rejection therefore settles without acquiring a permit. Only a fully validated Upsert or keyed Delete—including a compaction-capable tombstone or an already-expired non-null authoritative record—may then acquire its permit and transfer its idempotent lease into the attempt-owned live-lease registry before yielding the Source Delivery. This ordering prevents a mapper from retaining one record's lease while the same upstream chunk waits to acquire another lease for the same canonical ID, and prevents a settled rejection from stranding a lease. Permit contention waits interruptibly: inside `Effect.uninterruptibleMask`, explicitly restore or make only `acquirePermit(id)` interruptible, then keep the bounded post-acquisition registry insertion and ownership transfer masked. An interrupted wait owns no permit. Registration failure or defect releases the idempotent lease before leaving the mask; successful insertion transfers fallback release to the attempt registry. One Source Attempt Scope finalizer drains that registry, and ordinary release removes its entry immediately, so finalizer registrations and shutdown work are bounded by concurrent live leases rather than historical deliveries.
2. Runtime Core requires every transition-bearing retention delivery to contain exactly one mutation, rejecting multi-mutation shapes in public types and defensive runtime validation. It applies that singleton row mutation and executes its closed Source Application Transition in one serialized uninterruptible critical section. A successful finite-retention Upsert allocates a fresh never-reused logical-lifetime generation, replaces the indexed deadline from the Kafka record timestamp, and cancels any failed-work identity for the replaced generation. Every successful keyed Delete—including a compaction-capable tombstone or already-expired non-null record—removes the matching deadline and failed-work identity without making its generation reusable. Rejection or failed application leaves the prior generation unchanged.
3. The Source Application Transition releases the idempotent lease after the index change. The attempt registry is the fallback before that transition; Source Settlement then receives the complete application Exit exactly once and performs external commit work without owning row/index consistency.
4. Launch new expiration attempts only while Source Status is `Ready` or `Degraded`. For a due entry, use `Effect.acquireUseRelease(Effect.interruptible(acquirePermit(id)), use, releaseLease)` or an equivalent fresh child-Scope bracket whose blocking wait is explicitly interruptible and whose post-acquisition finalizer registration, ownership transfer, and infallible idempotent release are masked. Bare `Effect.acquireUseRelease(acquirePermit(id), ...)` is non-conformant because Effect masks acquisition. Scope closure while waiting cancels the waiter without waiting for the current holder and performs no mutation or failed-work transition. After acquisition, ask the Source Maintenance Interface to acquire the shared Source Lifecycle Gate. While holding both, it atomically rechecks the indexed never-reused generation, admits only `Ready` or `Degraded`, and linearizes admission with maintenance application plus the outcome-ledger transition. A stale candidate performs no row mutation, preserves the current newer indexed generation, and removes only the obsolete due entry and its exact failed-work identity. A candidate captured before Delete and same-ID reinsertion can never match the reinserted row's fresh generation. An inactive status performs no mutation but leaves the same due entry and failed-work identity intact for the next active sweep. No expiration lease is attached to the logical-lifetime sweep Scope.
5. Successful row deletion and removal of the matching indexed generation complete in the same serialized uninterruptible critical section while the lease is held. A failed Delete leaves both the matching row and generation available for retry and failed-backlog accounting.

An already-expired non-null record never produces a transient Upsert: because Kafka timestamps need not be monotonic, it produces a singleton keyed Delete with the same lease-transfer and closed state transition so a later-offset expired record removes any previously materialized row and deadline. A compaction-capable tombstone takes that keyed Delete path regardless of timestamp; a delete-only null record is a settled Source Item Rejection. Failed expiration adds its canonical-ID-plus-generation work identity to the failed-work set, exposes the exact safe failure in Kafka Source Adapter Metrics, and never pauses or terminates Kafka ingestion. Repeated failures of the same work item increase a cumulative retry-failure metric but not backlog cardinality. While the source is `Ready` or `Degraded`, a non-empty failed-work set selects the transport-neutral Adapter Maintenance Failure reason and degrades Topic and aggregate View Server health; operational lifecycle states retain their existing derived-health precedence while preserving the ledger for the next readiness transition. Failure and recovery publish Source Status plus derived Topic/aggregate health transitions immediately with the latest cached metrics. Success removes the exact failed-work identity with its row/index generation. Sweep interruption retains an existing identity while its generation remains due; only atomic generation replacement/removal or an observed stale generation cancels it. The latest exact safe maintenance failure and cumulative retry-failure state remain in Kafka Source Adapter Metrics for the logical lifetime; recovery clears active backlog and health state but not those retained diagnostics. Only metric-only detail changes coalesce into the next ordinary one-second sample, so even failure and recovery between samples becomes observable on at least that next bounded metrics publication without an immediate metrics refresh.

The logical Materialized Topic Scope owns the sweep fiber and its supervisor joins it. Interruption is expected only while that Scope closes. Any other sweep-fiber failure or completion outside the per-item maintenance Exit contract—including failure while sleeping, reading the index, acquiring a lease, or executing the closed outcome transition—is an invariant breach: the supervisor completes the shared Runtime Fatal Signal, which wins the root View Server Runtime Effect race and closes the listener plus every logical lifetime. Expected per-item Delete failures remain represented by the atomic failed-work/ledger transition and never take this fatal path.
_Avoid_: Exact TTL timer, fiber per row, full Topic Store scan, ingestion-fatal expiration, stale deadline Delete

**Kafka Retention Sweep Interval**:
The positive finite Effect `Duration.Input` supplied once to the aggregate `kafkaNode.layer(...)` or `kafkaNode.layerConfig(...)`, defaulting to 15 minutes. It controls the coarse Kafka Retention Projection cadence for every Kafka Source Definition in that Layer and therefore bounds ordinary expiration lateness rather than promising exact deletion time. It is deployment-specific operational configuration, not part of a browser-safe Source Definition or the transport-neutral View Server Runtime Effect.
_Avoid_: Per-row timer, per-topic sweep option, generic runtime Kafka option, zero or infinite cadence

**Kafka Start Position**:
The mandatory exact policy on a first-party Kafka Source Definition that selects initial offsets independently in every selected Region and partition. Its five variants are `earliest`, `latest`, `committed`, absolute `timestamp` with epoch-nanosecond `bigint`, and `durationAgo` with Effect `Duration.Input`. `committed`, `timestamp`, and `durationAgo` carry a mandatory `earliest`, `latest`, or `fail` fallback for a partition without a usable offset. Relative time is read through Effect Clock and fixed once per Materialized Topic lifetime; the adapter converts nanoseconds to Kafka's millisecond timestamp resolution before using timestamp offset lookup and freezes the resolved initial partition offsets. A Source Attempt retry resumes the derived active consumer group's latest successfully committed offsets for the current lifetime and uses the frozen initial offset where that lifetime has not successfully committed the partition. A complete runtime restart creates a new Materialized Topic lifetime and reevaluates `durationAgo`. Kafka v1 supports Materialized sources only, and there is no implicit start-position default.
_Avoid_: Moving retry window, optional start position, Date, millisecond number without unit, hidden earliest fallback, Effect-internal `_tag` in authored configuration

**Kafka Consumer Group Prefix**:
The mandatory non-empty logical View Server replica identity supplied once to the aggregate `kafkaNode.layer(...)`. The Kafka Source Adapter Runtime Service deterministically derives each Topic's active Kafka Consumer Group ID from this prefix and the exact View Server Topic name by independently applying canonical UTF-8 percent encoding to both components and joining them with `:`. For example, prefix `my-view-server` and Topic `orders` resolve to `my-view-server:orders`. The encoded result must fit Kafka's 32,767-byte protocol-string ceiling and is rejected during pure Layer construction otherwise. This prevents two Topics using one replica prefix from accidentally sharing an active group while keeping the usual value readable. Every concurrently running View Server replica that builds a complete local view must use a distinct prefix; the same logical replica may reuse its stable prefix across restarts. The same resolved ID applies in every cluster selected by that Topic and owns the commits used for connection recovery and Source Attempt retry. The prefix is never optional, repeated per Source Definition, stored in an individual Kafka region entry, or supplied by generic runtime options, and the resolved ID is present in exact Kafka adapter health so deployment collisions are diagnosable. A `committed` Kafka Start Position may name a different literal Consumer Group ID whose offsets seed the initial position, but subsequent commits belong to the resolved active group.
_Avoid_: Shared group across live full-view replicas, misleading final `consumerGroupId` input, optional prefix, raw string concatenation, generic runtime option, per-source repetition, random group, implicit cross-topic sharing

**Kafka Aggregate Node Layer**:
The scoped Layer returned by either `kafkaNode.layer(viewServer, options)` for resolved options or `kafkaNode.layerConfig(viewServer, config)` for an Effect Config-wrapped option tree. Both infer all required Regions from the View Server Config, reject missing and extra entries, and provide the Kafka Source Adapter Runtime Service backed by scoped clients; the Config-backed form resolves once during Layer construction and preserves typed configuration failure.
_Avoid_: Empty Region map, missing or extra Region, repeated per-Topic region-to-client map, mixed resolved/Config leaf union, repeated consumer-group prefix, one mandatory Layer per Kafka client, hidden ManagedRuntime

**gRPC Logical Client**:
A literal name from a gRPC Source helper's generated service-descriptor record. It selects the exact service and server-streaming methods at declaration time and addresses the matching scoped runtime client options in the aggregate gRPC Layer; it is not an endpoint or client instance.
_Avoid_: Base URL as client name, browser client, untyped string, Context token

**gRPC Request Factory**:
The synchronous exact function that constructs the selected server-streaming method's generated request-init object once per logical source lifetime. A materialized factory takes no Route; a leased factory receives its exact Feed Route, so a different Feed Route creates a different request while same-route subscribers and retries reuse one snapshotted request.
_Avoid_: Network call, Promise, Effect, Option, `undefined`, `any`, unknown payload, reusable mutable request object, unchecked object factory

**gRPC Source Invocation**:
The first-party gRPC adapter-owned execution of a Source Definition's selected client, server-streaming method, and Request Factory inside a scoped Source Attempt. Topic declarations provide no acquisition or release callback; the adapter owns Stream conversion, interruption, and finalization.
_Avoid_: Per-Topic acquire callback, per-Topic release callback, caller-owned AbortController, imperative Runtime Client bridge

**Source Settlement**:
The adapter-owned synchronous construction callback for a Source Delivery, applied to the complete success, typed failure, defect, or cancellation `Exit` of View Server mutation application exactly once to obtain an Effect. Callback application must return that Effect promptly and perform no I/O, blocking work, timer, Promise work, Effect execution, or unbounded computation; a callback that blocks before returning is non-conformant arbitrary user code for which Runtime Core makes no finalization or leak-free guarantee. The returned Effect's typed failure belongs to the adapter's declared failure union and its requirements remain visible. Its separate conformance contract requires prompt termination when the Source Attempt Scope interrupts it; masking or ignoring interruption is invalid. On ordinary application outcomes, Runtime Core uses one outer `Effect.uninterruptibleMask((outerRestore) => ...)` for the bounded callback handoff: it runs mutation application with `outerRestore`, captures the complete `Exit`, applies the callback exactly once while masked to obtain an Effect, then uses the same `outerRestore` to await only that returned Effect in the Source Attempt Scope. Scope closure before or during application therefore still produces an interrupted application `Exit` and cannot reduce callback application to zero. Each Source Attempt also owns a closed atomic cancellation arbitration state: shutdown or final leased-feed release records cancellation before interrupting attempt fibers, while the settlement workflow may claim ordinary termination only after callback application and immediately before any Source Termination/health/Schedule handoff. If callback application throws and ordinary termination wins that arbitration, Runtime Core catches the throw at this SDK boundary, increments failed settlement once, discards the raw value, and emits the exact Schema-backed Source Runtime Failure `{ _tag: "InvalidSourceSettlement", message: "Source Settlement callback threw before returning an Effect" }` as the Source Execution Failure consumed by ordinary Source Supervision. If cancellation wins—even when it was requested after a successful, typed-failed, or defective application `Exit` was captured but before the supervision handoff—callback application remains exactly once, its throw increments failed settlement once and is recorded as secondary redacted diagnostics, but it produces no Source Termination, supervision, retry/reacquisition, health transition, or commit. No returned Effect exists, already-applied rows are not rolled back, and the complete application Exit remains diagnostic context. Settlement is not an `acquireUseRelease` release/finalizer. Settlement success preserves the application outcome, while typed settlement failure becomes the operational Adapter Failure and the original application Exit remains diagnostic context rather than a fabricated compound Wire Protocol error. Attempt-Scope interruption of the returned Effect is lifecycle cancellation, produces no Source Termination, and triggers no retry. On an internal transition-executor defect, Runtime Core instead registers the supervised child inside that same outer mask: the child's masked prefix applies the callback once, records the SDK-owned invocation marker, and signals the handoff latch; only then does Runtime Core complete the Runtime Fatal Signal without awaiting the returned Effect. A synchronous callback throw on this fatal path is secondary diagnostics only. The child runs the returned Effect through the captured `outerRestore`, never a nested mask's restore, so root Scope closure terminates even an interruptible `Effect.never`; blocking callback application and a returned Effect that masks or ignores interruption are separately non-conformant rather than behavior Runtime Core claims it can forcibly terminate.
In that cancellation rule, no health transition means no callback-throw-induced Source Supervision health transition. A successfully recorded rejection keeps its `rejectedItemCount`, sticky rejection ledger, `Degraded` publication, and exact metrics, subject to the documented operational shutdown-status precedence.
_Avoid_: Acknowledge before publish, swallowed commit failure, View Server transport-specific acknowledgement, uninterruptible settlement, settlement that ignores Scope interruption

**Source Termination**:
The standard input to a Source Definition's selected supervision Schedule: either one exact Source Execution Failure or an unexpected successful completion of the continuous Source Stream. The infallible Schedule alone decides retry timing and exhaustion; fiber interruption bypasses Source Termination and is never retried.
_Avoid_: Interruption as failure, adapter retry loop, separate retryable boolean, swallowed successful completion

**Source Retry Policy**:
The mandatory infallible Effect Schedule selected for every Source Definition and supervised by View Server. Each Source Adapter Runtime Service declares one transport-aware default per supported Source Lifecycle, while every shared source constructor accepts a standard optional override and records an exact `UseAdapterDefault` or `Override` branch rather than `undefined`. View Server supplies no global default. The Schedule input is the exact Source Termination type, its error is `never`, and its environment requirements remain visible in the View Server Runtime Effect. A no-retry lifecycle defaults to `Schedule.recurs(0)`.
_Avoid_: View Server global retry, hidden adapter loop, retry boolean, undefined policy selection, erased Schedule environment

**Source Retry Exhaustion**:
The schema-backed `RetryExhausted` status produced when a Source Termination Schedule stops. It retains the last exact Source Termination, including the complete Adapter Failure or Source Runtime Failure when termination was caused by failure, so consumers can diagnose exhaustion without losing its cause.
_Avoid_: Generic retries-exhausted message, discarded final failure, transport error replacement

**Source Supervision**:
The View Server-owned execution and health observation of a continuous Source Adapter's mandatory infallible Effect Schedule over Source Termination. The Schedule owns retry timing and exhaustion for typed failures and unexpected successful completion; View Server creates a fresh child Scope and reacquires the Source Attempt, reads Schedule metadata for health, never retries interruption, and exposes exhaustion as terminal source failure. Every operational Source Status transition acquires the logical source's Source Lifecycle Gate, so a maintenance operation admitted as ready linearizes before a competing retry/exhaustion transition, or observes the inactive status and does nothing. A retrying source makes dependent Live Queries stale, terminal exhaustion makes them error, and recovery makes them ready again without closing their Subscriptions or discarding their last rows.
_Avoid_: Adapter-owned reconnect loop, transport-blind retry, retrying interruption, unobserved reconnect

**Source Lifecycle Gate**:
The transport-neutral Runtime Core-owned serialization primitive for one logical source lifetime. Source Supervision holds it while changing operational Source Status, and the Source Maintenance Interface holds it from status admission through the closed mutation/state/failed-work/degradation-ledger outcome transition. It creates one linearization order without exposing a lock or callback to adapters: maintenance either completes as ready before an inactive transition, or observes inactivity and preserves due work. It does not serialize ordinary independent delivery lanes beyond their existing Topic/canonical-ID mutation guards.
_Avoid_: Check-then-act Source Status read, Kafka-owned lifecycle lock, global source mutex, adapter-visible semaphore

**Source-Owned Topic**:
A View Server Topic whose mutations are owned by its configured `source` declaration. Direct Runtime Client mutation, TCP publish mutation, and direct reset are rejected for Source-Owned Topics.
_Avoid_: Runtime-owned topic, external mutation topic

**Source Ownership Policy**:
The Runtime Core Module that derives ownership facts from View Server Topic declarations: which topics are source-owned, which topics allow direct mutation, and which topics require leased gRPC lifecycle. It is the single Seam for source ownership decisions.
_Avoid_: Source helper, source registry, runtime topic helper

**Route Fields**:
The non-empty ordered list of top-level Filterable Scalar fields declared as `routeBy` by a leased source. Every Route Field must receive one exact schema-admitted value before the source can identify a feed; nested paths and structured fields are not Route Fields.
_Avoid_: Local filter fields, optional route keys, inferred where fields, nested route path, structured route value

**Feed Route**:
The exact `routeBy` object on a leased-topic Live Query that supplies one value for every Route Field and selects exactly one Leased Feed. Route values retain their supplied scalar identity without Text Matching, case folding, accent folding, trimming, or other query normalization; the Feed Route is mandatory for leased topics and invalid for topics without leased source lifecycle.
_Avoid_: Route array, multi-feed query, route extracted from where

**Leased Feed**:
An on-demand source-backed Topic Row partition identified by one exact Feed Route and owned by one runtime child Scope. The first subscriber creates the feed; same-route subscribers share its supervised Source Stream and retained rows while applying independent local filtering. Retry and exhaustion retain that feed while subscribers remain. The final subscriber closes its Scope, runs adapter finalizers, deletes its route-owned rows, and removes active health state; a later subscription creates a fresh feed and Schedule state.
_Avoid_: Public View Server Topic, permanent materialized source, query-specific upstream filter

**Package Surface Policy**:
The repository Module that declares every private workspace package entrypoint, public facade projection, manifest target, Vite+ pack entry, package-direction allowance, runtime export sentinel, and rejected deep-import probe. Repository checks inspect TypeScript modules with the TypeScript compiler and project this one policy across source, built output, and package resolution.
Package inspection is syntactic and non-interprocedural: it rejects direct loader roots, explicit capability escapes, and direct CommonJS or dynamic acquisition of `node:module`, but it does not resolve arbitrary data-flow aliases such as loaders returned by concise arrows or arrays. New loader idioms require an explicit policy fixture. Unlabeled and unknown-language Markdown fences use TSX parsing to avoid JSX-text false positives; examples that use TypeScript angle-bracket assertions must label the fence `ts`.
_Avoid_: Export allowlist, pack-entry copy, bespoke source parser

**Release Publish Orchestration**:
The repository Module that builds one sanitized temporary npm artifact, asks npm for the current version state, stages an unpublished version with provenance, reconciles the pending marker tag, and guarantees temporary-artifact cleanup. Its Interface receives the repository root, trusted release environment, generic command Adapter, output sinks, and temporary parent directory. Tests cross the same Interface with a real temporary package tree and an in-memory command transcript; the CLI Adapter alone owns process exit.
_Avoid_: Publish helper, release script, npm wrapper

**Kafka Source Codec**:
A typed decoder contract for Kafka message keys and values before Mapping, such as protobuf, JSON, string, bytes, or a custom Effectful decoder. It is the source-format Seam; the View Server Topic schema remains the target truth.
The JSON Adapter receives a lazy factory for Effect's canonical `Schema.toCodecJson(RowSchema)` codec and constructs it once. Versioned or non-canonical wire formats use the named custom Adapter instead.
_Avoid_: Topic schema, row schema, serializer

**Region**:
A named Kafka/source deployment location configured for ingestion.
_Avoid_: Location string, environment, cluster unless discussing infrastructure

**Mapping**:
The synchronous typed ingress transformation from decoded source values into a View Server Topic Row. First-party Mapping is a plain hot-path function: it returns its exact row-shaped result immediately and never returns Effect, Promise, Option, `undefined`, or another asynchronous or optional wrapper. Kafka `map` is cleanup-policy-specific. Delete-only Mapping receives decoded key, decoded non-null value, Region, the already-derived delete-only Local Row Key, and the allowed record metadata. Compaction-capable Mapping receives decoded key, decoded non-null value, Region, and the allowed metadata but no Local Row Key, Serialized Kafka Key Identity, canonical ID, or other identity override. Both return only the exact non-ID Topic Row fields; the adapter injects its policy-specific canonical `id` and validates the completed Topic Row through its Topic Schema. The Kafka schema is not exposed on `map`'s public typed input. Each Source Adapter owns its Mapping and ID-producing convenience; the common SDK requires only the resulting complete Topic Row and does not impose Kafka's Local Row Key concept on other transports. A synchronous Mapping throw becomes that adapter's exact schema-backed Mapping Failure inside a Source Item Rejection, so a still-usable source can settle the rejected item and continue. Asynchronous enrichment, service lookup, adapter-specific filtering, and effectful transformation belong upstream or in a custom Source Adapter API.
_Avoid_: Effectful first-party mapper, Promise mapper, optional mapping result, serializer, mapper when it obscures the target Topic Row contract

**gRPC Mapping**:
The synchronous typed transformation from one selected server-streaming method response into one complete Topic Row. Materialized Mapping receives only the exact `value`; leased Mapping additionally receives the exact Feed Route, while neither receives the Topic Schema. It follows the first-party Mapping contract and therefore returns the complete Topic Row immediately.
_Avoid_: Schema callback argument, client, request, session, transport options, asynchronous enrichment

**Upstream Source Authentication**:
The adapter-runtime-owned credentials and authentication mechanism used to connect to an external source. They are provided through the Source Adapter's aggregate Layer and transport interceptors, may be refreshed inside that scoped runtime service, and are never copied implicitly from a Remote Browser Client or Subscription. View Server authenticates and authorizes each browser query before source acquisition. When caller identity genuinely changes the upstream dataset, that distinction must be represented explicitly in the exact Feed Route and authorized at the View Server boundary so Leased Feed sharing remains deterministic.
_Avoid_: Forwarded browser headers, subscriber session in Source Definition, first-subscriber credential capture, implicit per-user Leased Feed

**Mapping Failure**:
The exact schema-backed Source Adapter Failure produced when a first-party Mapping callback throws synchronously. It preserves safe diagnostic context owned by that adapter and becomes a Source Item Rejection when the underlying ordered source can continue. Only inability to settle that rejection or continue the underlying source enters Source Supervision; the raw thrown value is never exposed to consumers.
_Avoid_: Defect, raw unknown exception, rejected Promise, swallowed row, poison-pill retry loop

**Kafka Delivery Contract**:
The operational guarantee for first-party Kafka ingestion. Kafka v1 supports Materialized sources only. Every configured Region is one independent Source Delivery Lane, sibling lanes run concurrently, and records within a lane are consumed sequentially through the ordinary Source Adapter contract. One valid record becomes one nominal Source Delivery; an item-local decode, Mapping, canonical-ID validation, Topic Schema, or delete-only Local Row Key callback failure becomes one nominal Source Item Rejection. Compact identity construction is SDK-owned from exact serialized key bytes and has no callback-failure phase. Runtime Core applies and settles each lane event before that lane pulls the next record. Successful Delivery or Rejection settlement commits the record offset and then records success health. Typed application failure, non-transition mutation-application defect, returned-settlement-Effect failure, and offset commit failure leave the record uncommitted and terminate the Source Attempt through ordinary supervision. A closed Source Application Transition executor defect instead completes Runtime Fatal Signal and closes the complete runtime under the fatal handoff contract. Source Attempt Scope interruption is lifecycle cancellation: it preserves the uncommitted record without producing Source Termination, stepping the retry Schedule, or reacquiring an attempt. Every Topic-owned Source Definition declares one of all five Kafka Start Positions, while the aggregate Node Layer derives the active Consumer Group ID from its prefix and the exact View Server Topic name. Across process restarts, the in-memory Runtime Core has no durable row checkpoint, so active-group commits are delivery checkpoints rather than a complete rebuild strategy.
_Avoid_: Exactly-once claim, durable recovery, database replication

**Kafka Consumer Group Assumption**:
The first-party adapter starts one consumer per configured Region for each Materialized Kafka Source Definition. Every consumer for that Topic uses the same active Consumer Group ID derived from the aggregate Layer's Consumer Group Prefix and the exact View Server Topic name; a `committed` Start Position may read another literal group's offsets only to seed initial positions. The adapter records assignments and lag for the current process, but does not implement a full rebalance/revoke recovery story or durable checkpoint handoff between consumers. Concurrent replicas that each build a complete materialized view therefore use distinct Consumer Group Prefixes.
_Avoid_: Clustered recovery, multi-consumer partition handoff

**Publish**:
A server-side mutation that inserts or replaces a Topic Row in a View Server Topic.
_Avoid_: Browser write, send, emit

## Relationships

- A **View Server** owns one or more **View Server Topics**.
- A **View Server Topic** has exactly one canonical **Topic Row ID** declared as `id: ViewServerId` in its Topic Schema.
- A **Topic Row** belongs to exactly one **View Server Topic**.
- A **Timestamp** is a numeric Topic Row field and uses the same typed comparison semantics as its number or bigint representation.
- **Topic Row Value Semantics** are derived from that Topic Row's configured schema and are shared by local and Wire Protocol ownership boundaries.
- A **Live Query** targets exactly one **View Server Topic**.
- Every **Live Query** is a **Wire-Safe Query**, including when it is used through an in-process Adapter.
- A **Wire-Safe Query** never uses explicit `undefined` for an optional query property; the property is absent or carries a valid defined value.
- Changing caller-owned input after submitting a **Live Query** does not change that query; a caller submits another Live Query to request different semantics.
- A **Raw Query** returns selected Topic Row fields.
- A **Raw Query** selects at least one existing Topic Row field; an empty or absent selection is invalid.
- A **Grouped Query** returns group fields plus aggregate aliases.
- A **Grouped Query** always names at least one valid group field; an empty `groupBy` is invalid rather than an omitted or global grouping.
- A **Grouped Query** always defines at least one aggregate alias; an empty or absent `aggregates` is invalid.
- A **Grouped Query** applies its **Root Conjunction** to source Topic Rows before grouping and aggregation; aggregate aliases are not Filterable Field Paths.
- A **Raw Query** or **Grouped Query** may filter Topic Rows through one **Root Conjunction**.
- A **Root Conjunction** applies AND to each contained **Filter Expression**.
- An empty **Root Conjunction** and an omitted filter both match every Topic Row.
- **Filter Normalization** removes empty AND and OR groups before the **Root Conjunction** is evaluated.
- **Filter Normalization** replaces an AND or OR group with its sole effective child when only one remains.
- **Filter Normalization** flattens an AND nested directly inside AND and an OR nested directly inside OR.
- **Filter Normalization** gives reordered AND or OR children the same semantic query identity.
- **Filter Normalization** deduplicates equivalent AND or OR children using their Field Condition semantics, including Text Matching and Topic Row Value Semantics.
- **Filter Normalization** removes an **Open Membership Condition** as an absent predicate.
- **Filter Normalization** removes a **Negation Expression** whose child has normalized away, because absence is not a Boolean value to complement.
- **Filter Normalization** collapses two adjacent **Negation Expressions** to their shared child without applying De Morgan or other expression-expanding rewrites.
- **Filter Normalization** replaces a **Negation Expression** around a leaf with its exact named positive or Negated Condition when one exists; both forms have one semantic query identity.
- **Filter Normalization** stops before absorption, distribution, or CNF/DNF conversion.
- A malformed or unsupported **Field Condition** fails query validation rather than being removed by **Filter Normalization**.
- Every **Field Condition** and logical group is an exact shape; unknown properties and properties belonging to another condition type fail query validation rather than being ignored.
- Structured object, array, collection, and class values do not receive **Field Conditions**; only **Filterable Scalars** may be filtered.
- A **Field Condition** identifies its **Filterable Scalar** through one strongly typed **Filterable Field Path**, including when the scalar is nested inside structured fields.
- Topic configuration rejects a traversable field name containing `.` so every **Filterable Field Path** has one unambiguous meaning.
- A **Filterable Field Path** crosses only statically named object fields and never indexes a collection or resolves a dynamic key.
- A recursive schema reference stops a **Filterable Field Path** rather than introducing an arbitrary traversal-depth limit.
- A **Filterable Field Path** follows the accepted decoded object shape through refinements, brands, admitted classes, and transformations with a statically inspectable decoded shape; opaque decoded shapes stop traversal.
- A statically named scalar leaf present in any fixed-shape object union branch forms a **Filterable Field Path**; branches without that leaf resolve the path as blank.
- A **Filterable Field Path** may resolve to a Filterable Scalar in one union branch and a structured value in another; positive **Domain-Selective Conditions** do not match the structured value, their negations remain exact complements, and no structured value is compared deeply.
- A **Field Condition** is a leaf of a **Filter Expression**; AND and OR groups and **Negation Expressions** may recursively contain Filter Expressions.
- **Filter Expressions** accept only exact discriminators: uppercase `AND`, `OR`, and `NOT` for logical nodes and the defined camelCase names for Field Conditions; differently cased aliases are invalid.
- A **Filter Expression** has no language-defined limit on depth, node count, membership candidates, or text-operand length; size changes its execution cost rather than its validity.
- Reusing one acyclic **Filter Expression** value in multiple branches is valid and follows ordinary **Filter Normalization**, while a cyclic object graph fails query validation.
- An **Equality Condition** uses **Text Matching** for string values and **Topic Row Value Semantics** otherwise; `notEqual` matches every value that its corresponding `equals` does not.
- Boolean fields use ordinary **Equality Conditions** and **Membership Conditions** with boolean operands rather than dedicated true or false condition types.
- The empty string and schema-admitted `null` remain exact **Equality Condition** operands, while `undefined` and missing presence are represented only by a **Blank Condition**.
- Equality and membership may explicitly match an actual empty string or schema-admitted `null`; other positive value **Field Conditions** do not match blank values, and corresponding **Negated Conditions** match them as exact complements.
- A union field exposes every **Domain-Selective Condition** supported by any nonblank member, while incompatible runtime members do not satisfy the positive condition.
- A **Condition Operand** for equality or membership belongs to the field schema, while a text or numeric search threshold belongs to its compatible primitive operator domain.
- A non-finite number is never a valid **Condition Operand**; negative and positive zero have one equality meaning and one semantic query identity.
- A **Text Condition** includes string equality and membership as well as text search operations.
- The `contains`, `notContains`, `startsWith`, and `endsWith` **Text Conditions** require a search operand that remains non-empty after that condition's Text Matching normalization; an empty string remains valid for equality and membership only.
- Equality and membership are **Text Conditions** when their field schema contains a string branch; their Text Matching modifiers affect only string comparisons within that field domain.
- Numeric comparison, range, and blank conditions are not **Text Conditions** and reject Text Matching modifiers.
- **Text Matching** uses the same normalized representation for string equality, membership, and search operations; `caseSensitive: true` and `accentSensitive: true` independently preserve their respective distinctions and participate in semantic query identity.
- A string equality or membership operand that normalizes to the empty string matches only an actual empty-string value and shares its semantic query identity; it does not match missing, `undefined`, or `null` values.
- **Filter Normalization** treats an omitted Text Matching sensitivity modifier as `false` and deduplicates operands that normalize to the same string.
- A **Blank Condition** intentionally treats missing, `undefined`, `null`, and the empty string alike for filtering without making them equal under **Topic Row Value Semantics**.
- A **Filterable Field Path** resolves as blank when any intermediate object is missing, `undefined`, or `null`; all positive and negated conditions then follow their ordinary blank semantics.
- A **Blank Condition** remains valid when a Topic Row schema makes `blank` match no rows or `notBlank` match every row.
- A **Membership Condition** applies **Text Matching** to string candidates and does not give `null` or the empty string blank-sentinel meaning.
- **Filter Normalization** gives reordered or equivalently duplicated candidates in a **Membership Condition** one semantic query identity.
- A **Range Condition** includes its lower bound and excludes its upper bound.
- BigDecimal fields support the same equality, membership, comparison, and **Range Conditions** as other numeric fields without cross-kind coercion.
- A BigDecimal query or route operand is wire-safe only when Effect's BigDecimal JSON codec round-trips it injectively; exponent/scale combinations that lose numeric identity are rejected before keying or transport.
- An equal or reversed **Range Condition** fails query validation rather than representing an empty result.
- An **AG Grid Adapter** preserves condition type names when AG Grid and **Field Condition** semantics coincide.
- An **AG Grid Adapter** adds an explicit **Blank Condition** when AG Grid's configured equality behavior differs from a core **Equality Condition**.
- An **AG Grid Adapter** translates AG Grid's exclusive or inclusive range configuration into equivalent **Filter Expressions** because neither is a half-open **Range Condition**.
- An **AG Grid Adapter** translates AG Grid Set Filter's `null` sentinel into a **Blank Condition**, keeping nonblank keys in a **Membership Condition**.
- A **Subscription** belongs to one **Live Query** and emits one **Snapshot** followed by zero or more **Deltas** and **Status Events**.
- A **Column Live View Engine** owns one **Topic Store Module** per **View Server Topic**.
- The current **Topic Store Module** Implementation is row-oriented and owns its private indexes, mutation ordering, row storage, query helpers, and health.
- A future **Columnar Topic Store** Implementation may sit behind the **Topic Store Module** Seam.
- A **Columnar Topic Store** would own one **Topic Column Vector** per configured Topic Row field.
- A **Runtime Core** owns one **Column Live View Engine** instance and exposes both a **Runtime Client** and a **Live Client**.
- A **Raw Query Plan** is compiled once from a **Raw Query** before the **Topic Store Module** scans rows.
- A **Raw Predicate Plan** is part of a **Raw Query Plan** and lets storage narrow scans without replacing the correctness callback unless it is proven exact.
- A **Topic Store Module** may maintain **Raw Ordered Window Indexes** to accelerate bounded **Raw Query** windows.
- A **Grouped Query Plan** is compiled once from a **Grouped Query** before grouped full-scan or incremental execution.
- Every compiled Raw or Grouped Query carries **Query Result Semantics** determined by its selected fields, group fields, and aggregate definitions.
- An **Active Query** may serve many equivalent **Subscriptions**.
- A **Live Client** can subscribe to **Live Queries** but cannot publish mutations.
- A **Runtime Client** can publish mutations but is not exposed to browsers by the Real View Server.
- A **Remote Browser Client** is a **Live Client** adapter for the **Wire Protocol**.
- React, the **Remote Browser Client**, In-Memory View Server, and the real runtime consume the same browser-safe **View Server Config**; applications never author a mirrored server topic tree.
- React hooks derive topic names, selected result rows, valid filter paths and operators, sort fields, group fields, aggregate fields, and aliases directly from the **View Server Config** without requiring `as const`. In the accepted target architecture, they will also derive Feed Routes and Source Adapter Failure unions from that config.
- `runViewServerRuntime(viewServer, options)` returns a **View Server Runtime Effect**. In the accepted target architecture, its requirements will preserve the union inferred from every Source Definition's nominal Source Adapter Runtime Service, retry Schedule, and application dependencies.
- Application code will satisfy the **View Server Runtime Effect** with aggregate adapter and platform Layers through `Effect.provide(...)` before `NodeRuntime.runMain(...)`.
- A Source Adapter will never create a hidden ManagedRuntime or call `Effect.run*` inside reusable integration code.
- Pure View Server Config, Source Definition, and aggregate adapter Layer constructors will throw named configuration errors immediately for deterministic programmer mistakes and return frozen snapshots rather than Effects or hidden running resources.
- Environment, file, and secret configuration will be decoded through Effect Config or Schema, while Layer construction, resource acquisition, and source execution failures will remain in typed Effect error channels.
- Runtime startup will defensively revalidate every common Source Definition envelope before invoking its nominal Source Adapter Runtime Service, even though pure builders already validated it.
- Every **View Server Topic** will appear once in the **View Server Config** and declare zero or one nominal **Source Definition** through the matching adapter's materialized or leased constructor.
- A materialized Source Definition will reject `routeBy`; a leased Source Definition will require a non-empty unique `routeBy` containing statically named top-level supported scalar fields from its Topic Row Schema, inferred without `as const`.
- External source names, browser-safe codecs, Mapping functions, Local Row Key functions, Start Position, Schedules, Effects, and other browser-safe Effect requirements may belong to Source Definition options; credentials, concrete client tokens, concrete clients, sockets, transport-driver packages, Node APIs, and platform Layers will not.
- A **Source Adapter Runtime Service** will execute only Source Definitions created by its exact nominal Source Adapter declaration, and View Server will reject structural substitutes.
- The **Strict JSON Materializer** makes local semantic materialization and NDJSON acceptance agree; explicit schema codecs restore semantic runtime values after the strict JSON boundary.
- A **Field Filter Codec** protects the **Wire Protocol** from unsafe or incorrectly typed filter values.
- A **Raw Query Codec** protects Raw Query wire payloads from unknown fields, unsafe filters, and invalid windows.
- A **Grouped Query Codec** protects Grouped Query wire payloads from invalid group fields, aggregate aliases, aggregate fields, grouped ordering, and invalid windows.
- An **Aggregate Row Codec** protects grouped aggregate row values from JSON precision loss over the **Wire Protocol**.
- A **Health Summary Codec** protects the compact health summary stream from impossible status combinations and unknown unhealthy topic names.
- A **Health Topic Codec** protects the per-topic health stream from missing, duplicate, unknown, or mismatched topic rows.
- A **Health Payload Codec** protects full runtime health payloads from missing or unknown configured topics.
- A **View Server Provider** supplies a **Live Client** to React hooks.
- A **View Server In-Memory Provider** supplies the same hook behavior through an **In-Memory View Server**.
- A **Live Query Viewport** keeps viewport rows out of React state, derives its query window, and gives every replacement or scroll-only window change switch-latest ownership over row, count, status, and failure delivery.
- An **AG Grid Adapter** accepts AG Grid state without making AG Grid state the canonical View Server query language.
- An **AG Grid Adapter** validates every **AG Grid Set Key** against the bound Topic Row field schema without attempting to repair a lossy key creator.
- A **Real View Server** and **In-Memory View Server** differ only by transport and ingress **Adapters**, not by query, storage, health, or subscription logic.
- A **Source Topic** uses one **Kafka Source Codec** for its value and optionally one **Kafka Source Codec** for its key.
- A **Source Topic** is mapped into a **View Server Topic** through a **Mapping**.
- A **Source Adapter** is imported and composed at build time; runtime plugin discovery is not part of the current source model.
- `SourceAdapter.make(...)` declares Source Adapter Identity and the complete Source Adapter Failure Schema exactly once in `/contract`; each supported Source Lifecycle is declared by its mandatory Source Adapter Metrics Schema and mandatory Source Rejection Location Schema rather than a boolean.
- `SourceAdapter.make(...)` creates the adapter's opaque browser-safe `Context.Service` tag; adapter authors do not manually declare or repeat a runtime service tag or service contract.
- `SourceAdapterServer.make(...)` accepts that exact nominal adapter handle and can implement only its declared lifecycles, failure, metrics, and runtime service shape.
- The browser-safe opaque service tag provides linkage only; its implementation, concrete transport, clients, and Layers remain absent from `/contract`.
- Each Source Adapter lifecycle factory receives exactly the frozen adapter-specific Source Definition, exact Source Target, and Topic-Bound Source Toolkit; a leased target includes the exact Feed Route.
- The Topic-Bound Source Toolkit exposes exact nominal `upsert`, `delete`, `delivery`, and `reject` constructors plus Topic name, performs the final runtime validation promised by their public types, and owns the transport-neutral Source Maintenance Interface for SDK-issued closed operations and isolated health transitions; it exposes no arbitrary maintenance-mutation entry point.
- Lifecycle factories receive no Runtime Client, publish callback, Subscriber, session, reference count, browser headers, internal Topic Store, mutable config, or raw Topic Schema-validation escape hatch.
- Materialized and leased Source Lifecycles may declare different Source Adapter Metrics Schemas, and a selected Source Definition carries only its exact lifecycle metrics type without optional fields.
- Materialized and leased Source Lifecycles each declare one exact Source Rejection Location Schema; Source Item Rejection constructors and Source Diagnostics preserve that exact type without unknown or optional location bags.
- Every source health payload contains mandatory `{ runtime, adapter }` metrics: SDK-owned runtime metrics and the exact adapter-owned lifecycle metrics.
- Mandatory **Source Runtime Metrics** contain `startedAtNanos`, `lastAttemptStartedAtNanos`, nullable `lastDeliveryAtNanos`, nullable `lastAppliedMutationAtNanos`, and nullable `lastTerminationAtNanos`, all as epoch-nanosecond `bigint` values derived integer-safely from Effect `Clock.currentTimeMillis`; monotonic `Clock.currentTimeNanos` is used only for elapsed durations and cadence.
- Source Runtime Metrics use cumulative `bigint` values for current attempt, retry count, received deliveries, `rejectedItemCount`, attempted mutations, applied Upserts, applied Deletes, failed mutations, completed settlements, failed settlements, and Source Buffer overflow count; `lastRejectionAtNanos` is a mandatory nullable epoch-nanosecond timestamp.
- Source Runtime Metrics use `number` only for actual in-memory sizes and capacities: retained rows, Source Buffer capacity, depth, and high-water mark.
- Source Runtime Metrics contain a non-empty `lanes` tuple whose entries have stable non-empty unique IDs and exactly one `{ _tag: "Unbuffered" }` or `{ _tag: "Bounded", capacity, depth, highWaterMark, overflowCount }` buffer value; neither a lane nor its buffer is optional.
- Source Delivery Lane IDs remain stable across retries so cached health and metrics continuity do not depend on array position; the first-party Kafka adapter uses its exact region names.
- `completedSettlementCount` means the settlement Effect completed and does not infer adapter-specific acknowledge, negative-acknowledge, or commit semantics; those belong in Source Adapter Metrics.
- Source lifecycle status and exact failures remain outside metrics rather than duplicating state inside Source Runtime Metrics.
- **Source Status** is an exhaustive Schema tagged union of `Starting`, `Ready`, `Degraded`, `WaitingToRetry`, `Reacquiring`, `Exhausted`, and `Stopping`; no branch uses optional status-dependent fields.
- `Starting` is initial attempt `1n`; successful Source Attempt acquisition establishes operational readiness immediately, then the logical-lifetime degradation ledger selects visible `Ready` when empty or `Degraded` with the exact canonical non-empty `reasons` tuple defined by Source Status when reasons remain. Rejection degradation is sticky and maintenance degradation clears only when its backlog clears. `WaitingToRetry`, `Reacquiring`, `Exhausted`, and `Stopping` retain precedence while ledger transitions continue internally, and every later successful acquisition makes the same `Ready`-versus-`Degraded` selection.
- Topic and aggregate health derive from the currently visible operational Source Status: a retained maintenance marker does not override `Starting`, `WaitingToRetry`, `Reacquiring`, `Exhausted`, or `Stopping` or their existing health mapping. New retention expiration attempts run only in `Ready` or `Degraded`; a marker already in the hidden ledger becomes visible again on the next readiness transition.
- **Source Target** is exactly `{ _tag: "Materialized" }` or `{ _tag: "Leased", route }`; Feed Route is never optional.
- Every source health value always contains Source Adapter Identity, Source Target, Source Status, mandatory `{ runtime, adapter }` metrics, and `sampledAtNanos`.
- Live Queries map `Ready` and `Degraded` to ready availability, `WaitingToRetry` and `Reacquiring` to stale, `Exhausted` to error, and recovery to ready while retaining their existing Subscription and rows; exact degraded state remains visible through Source Diagnostics.
- While the visible operational Source Status is `Ready` or `Degraded`, a Source Item Rejection or Adapter Maintenance Failure selects `Degraded` Source Health, an affected degraded Topic health row, and a degraded aggregate View Server health summary, while Live Query availability stays ready and later valid source items continue. During `Starting`, `WaitingToRetry`, `Reacquiring`, `Exhausted`, or `Stopping`, that operational branch and its existing derived-health mapping retain precedence while the degradation ledger remains hidden until the next successful acquisition.
- Liveness and readiness endpoints remain successful for a degraded source and return the degraded state in their payload; they do not evict or restart the View Server automatically.
- An exhausted required source keeps its exact `Exhausted` diagnostics and retained rows, but contributes `starting` Topic and aggregate health so readiness remains unsuccessful until the source recovers.
- Operators may alert on aggregate degraded status or increasing `rejectedItemCount` without putting health RPCs or refresh work on the source-event hot path.
- Ordinary Live Query Snapshot, Delta, and Status Event APIs remain transport-agnostic and never carry Source Adapter Metrics or full Source Health payloads on the live-event hot path.
- Remote Browser Client and React expose Source Diagnostics through an explicit separately subscribed or read API whose inputs and exact result are inferred from the selected Topic Source Definition.
- `subscribeSourceHealth(...)` and `useSourceHealth(...)` accept only an exact source-owned Topic; source-free Topics are rejected at compile time.
- A materialized Topic's Source Diagnostics input rejects `routeBy`, while a leased Topic requires its exact Feed Route and rejects unknown, missing, or extra Route Fields without requiring `as const`.
- Source Diagnostics never use Source Adapter Identity, Kafka Region, gRPC client name, or another transport-specific identifier as the public lookup key.
- Source Diagnostics read only View Server's cadence-cached Source Health; they never trigger a broker RPC, adapter metrics Effect, or health recomputation per query event.
- `subscribeSourceHealth(...)` emits the current cached health immediately and subsequent cached changes through one scoped subscription shared by matching local consumers; React unmount and client shutdown release it.
- Source Diagnostics define no one-shot source-health operation in v1; a non-reactive caller may consume the first element of the scoped subscription explicitly.
- A Source Diagnostics subscription is an observer and never increments a Leased Feed's Live Query reference count, invokes its Request Factory, acquires its Source Attempt, or delays its final release and route-owned row cleanup.
- Materialized Source Diagnostics expose their exact active Source Health directly; leased Source Diagnostics expose exactly `Inactive` with Feed Route or `Active` with complete Source Health, with no optional health or fabricated zero metrics.
- An inactive leased route emits `Inactive` immediately, transitions to `Active` only when a real Live Query owns that Leased Feed, and returns to `Inactive` after the last owning Live Query releases it.
- Every Source Adapter Runtime Service supplies an infallible Effect for each bound source's exact Source Adapter Metrics value, returns a valid initial local snapshot before readiness, and preserves that Effect's requirements in the View Server Runtime Effect.
- A Source Adapter Metrics Effect performs no broker RPC, network request, or blocking refresh; an adapter may maintain its local snapshot with Effect Metric, Ref, or an optimized transport ledger.
- View Server samples Source Adapter Metrics exactly once per second with Effect Clock, freezes and Schema-validates each sample, and publishes only the cached snapshot.
- Metric-only Source Health changes publish at most once per one-second cadence; lifecycle transitions and Source Item Rejections publish immediately with the latest cached metrics.
- V1 exposes no global, adapter-specific, source-specific, or subscriber-specific health cadence setting.
- Delivery processing may update cheap local metrics state but never invokes health refresh, metrics Schema encoding, or Wire Protocol publication.
- A schema-invalid Source Adapter Metrics sample becomes an `InvalidSourceMetrics` Source Runtime Failure and terminates the current source attempt through ordinary Source Supervision.
- `SourceAdapterServer.make(...)` imports that exact declaration in `/server` and must implement every declared Source Lifecycle with no undeclared lifecycle factory.
- Source Adapter lifecycle factory inputs, outputs, failure type, and Effect requirements remain inferred from the declaration and implementation without repeated identity strings, Schemas, or linking casts.
- A **Source Adapter** provides runtime Schemas for its failure union and every value crossing health or Wire Protocol boundaries, but its complete browser-safe Source Definition options do not require one encompassing Schema or JSON representation.
- Every source health payload contains exact Source Adapter Metrics validated by the declared metrics Schema; Remote Browser Client and React types infer that value without casts or `as const`.
- Serializable adapter option subtrees may use Effect Schema or Effect Config, while executable codecs, Mapping, Row Key functions, Schedules, Effects, and service references use exact TypeScript contracts plus adapter-owned construction validation.
- The Source Adapter SDK validates and snapshots its common Source Definition envelope, and adapter-specific option validation completes before the Source Adapter Runtime Service starts it.
- Every **Source Adapter Package Surface** exposes a browser-safe `/contract`, a server-only `/server`, and optional platform-specific Layer exports such as `/node`.
- View Server exposes the Source Adapter SDK only through `effect-view-server/source-adapter`, `effect-view-server/source-adapter/server`, and `effect-view-server/source-adapter/testing`; package export checks reject deep or internal SDK imports.
- The first-party Kafka Source Adapter is an ordinary SDK consumer exposed through `effect-view-server/kafka/contract`, `effect-view-server/kafka/server`, and `effect-view-server/kafka/node`.
- The first-party gRPC Source Adapter is an ordinary SDK consumer exposed through `effect-view-server/grpc/contract`, `effect-view-server/grpc/server`, and `effect-view-server/grpc/node`.
- A published Source Adapter package declares `effect-view-server` and every Effect ecosystem package used by its public or runtime surfaces as peer dependencies and keeps them as development dependencies for its own build and tests; it never bundles private runtime copies of those packages.
- While Effect remains beta or View Server remains pre-1.0, a published Source Adapter declares exact peer versions for View Server and every Effect ecosystem package it uses. After both are stable, an adapter may widen a peer range only across versions its conformance matrix executes successfully.
- Source Adapter SDK conformance tests reject a `/contract` export that resolves Node APIs, Source Adapter Runtime Service implementations, concrete clients, platform Layers, or transport-driver packages.
- Conformance builds a real browser fixture for every Source Adapter contract and enforces a documented bundle-size budget so browser-safe Mapping functions, descriptors, codecs, and Schemas cannot accidentally pull large server dependency graphs.
- V1 deliberately accepts browser bundle contribution from runtime contract values in the one authored View Server Config and introduces no mirrored browser config, code generation, custom build transform, automatic projection, or second Topic tree.
- Every published platform adapter export provides both `layer(viewServer, resolvedOptions)` and `layerConfig(viewServer, configWrappedOptions)`; a custom startup function or hidden Runtime is non-conformant.
- `layerConfig(...)` accepts exact Effect `Config.Wrap<Options>`, resolves it once with `Config.unwrap(...)` during Layer construction, and preserves `Config.ConfigError` in the Layer error channel.
- Both platform constructors infer all and only the adapter resources required by the supplied View Server Config, reject missing and extra entries through public types and runtime validation, and provide one aggregate scoped adapter-plus-clients Layer.
- Any other Effect service requirements of a platform Layer remain visible in its environment and are composed by the application at the Effect boundary.
- A published Source Adapter is conformant only when every declared Source Lifecycle runs the shared behavioral conformance kit with an adapter-supplied controllable test Layer; TypeScript shape compatibility alone is insufficient.
- The conformance Layer must make acquisition, valid delivery, adapter failure, unexpected completion, metrics changes, and scoped finalization observable; a leased lifecycle must additionally make exact-route acquisition and final release observable.
- The conformance kit uses Effect scoped Layer suites and TestClock to prove the mandatory lifecycle invariants for every adapter, then ordering, settlement, lane-shape, and bounded-buffer semantics according to the adapter's declared transport event model.
- Source Adapter conformance includes positive and negative public type tests, exact Schema tests, nominal-linkage rejection, package exports, required peer dependencies, duplicate-bundle rejection, and browser-safety checks.
- Dedicated public `.test-d.ts` coverage proves `SourceAdapterServer.make(...)` lifecycle registration infers exact initial-state, command, reducer-result, and metrics-snapshot types without `as const`; accepts zero descriptors for stateless lifecycles and exactly one correctly Topic-bound descriptor for each retention-capable Kafka materialized lifecycle; and rejects missing required, duplicate, wrong-lifecycle, cross-Topic, and structural descriptors plus async, Promise-returning, Effect-returning, fallible, or wrong-result reducers.
- Settlement conformance covers an ordinary callback throw for successful, typed-failed, and non-transition-defective application Exits when ordinary termination wins arbitration: callback count is one, no returned Effect starts, the raw throw is absent, exact `InvalidSourceSettlement` reaches Source Supervision and health, failed-settlement count increments once, no record commits, retry/reacquisition or exhaustion follows policy, unrelated sources/listener remain available, and Runtime Fatal Signal stays empty. Cancellation-precedence cases request attempt-Scope closure both before application completes and after a successful operation `Exit` is captured but before masked callback application. Delivery assertions require callback count and failed-settlement count one, secondary redacted diagnostics, and no Source Termination, callback-throw-induced supervision health transition, Schedule step, retry/reacquisition, commit, or Runtime Fatal Signal. Rejection assertions require those same outcomes while preserving the already-recorded `rejectedItemCount`, sticky rejection ledger, `Degraded` publication, and exact metrics subject to operational shutdown precedence. A separate transition-defect case proves callback throw is secondary to the original root-fatal cause.
- Transition-defect settlement conformance proves the child runs an interruptible `Effect.never` through the outer delivery mask's captured restore rather than a nested restore: after callback/marker/latch and fatal completion, root Scope closure interrupts and joins the child without a leak while preserving the original transition defect.
- Source Adapter package conformance rejects peer ranges broader than the versions covered by the adapter's conformance matrix.
- First-party Kafka and gRPC Source Adapters pass the same conformance kit as published third-party adapters without exceptions.
- Runtime Core and the generic runtime contain no privileged Kafka or gRPC Source Lifecycle path; transport acquisition, decoding, Mapping, settlement, metrics, and external-resource finalization live behind those first-party Source Adapter modules.
- Generic View Server Runtime options contain no `kafka`, `grpc`, or future transport-specific configuration bags.
- Invalid or tampered View Server Config, missing adapter service, missing or extra aggregate resource entries, Effect Config failure, mandatory aggregate Layer acquisition failure, and invalid static Source Application State descriptors are fatal Runtime Composition Failures; descriptor validation occurs before any listener, Source Attempt, consumer, or sweep acquisition, so View Server opens no server ports.
- An impossible duplicate, late, or logical-lifetime-mismatched Source Application State registration after composition completes the Runtime Fatal Signal; it never enters Source Supervision or health-only degradation.
- After mandatory aggregate Layer acquisition succeeds, operational Source Attempt acquisition, transport/framing, Stream, rejection-settlement, or Source Settlement failure enters that source's independent Source Supervision and never terminates the View Server Runtime Effect or unrelated Topics; Kafka Broker Contract Validation remains a pre-service Layer prerequisite rather than a Source Attempt.
- Internal Runtime Core invariant defects are not operational source failures: every logical source lifetime can complete the shared Runtime Fatal Signal, and `runViewServerRuntime(...)` structurally races normal server service against it so background supervisor/sweep/closed-transition failure closes the complete runtime instead of entering Source Supervision or becoming an unobserved child exit.
- A source that fails before first readiness exposes empty retained rows with stale or exhausted error status; a source that previously became ready preserves its last rows through retry and exhaustion.
- Source-specific external topic or feed names, browser-safe codecs, mappings, Start Position, retry overrides, and consumer behavior belong to the one shared Source Definition constructor; brokers, endpoints, credentials, TLS, connection pools, and concrete transport clients belong to adapter platform Layers.
- A Source Adapter may define its own logical literal Resource Reference fields in Source Definitions; the SDK does not impose one universal property or collection name.
- Source Adapter Resource References contain no URLs, credentials, concrete clients, or per-resource Context tags and require no separate registration tree in `defineViewServerConfig(...)`.
- The aggregate adapter Layer derives the exact required Resource Reference union from the supplied View Server Config, rejects missing and extra runtime entries through types and validation, and builds one O(1) resource lookup map during scoped Layer acquisition.
- A Source Adapter that needs no named external resource declares no Resource Reference rather than fabricating a singleton name.
- A **View Server Topic** declares zero or one `source` containing a **Source Definition**; transport-specific source properties are not part of the source model.
- View Server owns the common **Source Definition** envelope, while its **Source Adapter** owns the type and runtime validation of adapter-specific configuration.
- A **Source Definition** carries its Source Adapter's nominal Runtime Service requirement and browser-safe options; View Server never resolves adapter IDs through a registry.
- Every **Source Definition** carries a **Source Adapter Identity** with a required adapter name and optional adapter version.
- Every **Source Definition** is nominally bound to its exact Source Adapter Runtime Service and containing View Server Topic.
- **Source Adapter Identity** appears in source health, typed errors, spans, and logs, but is never a registration key, dispatch key, or Source Definition equality key.
- Source Adapter compatibility is enforced through declared peer dependency ranges, public TypeScript API compatibility, nominal SDK brands, and runtime Source Definition envelope validation rather than a protocol field injected by the same runtime SDK.
- Every **Source Adapter** supplies a Schema for its complete typed **Source Adapter Failure** union and wraps foreign library errors before they enter either a Source Item Rejection diagnostic or the Source Stream's Effect error channel.
- The SDK composes each adapter-specific **Source Adapter Failure** with the shared **Source Runtime Failure** vocabulary as one exact tagged **Source Execution Failure**; adapters do not redeclare SDK-owned variants.
- The shared Source Runtime Failure vocabulary includes exact redacted `InvalidSourceSettlement`; a synchronous Source Settlement callback throw becomes this typed Source Supervision input only when ordinary termination wins the attempt cancellation arbitration, increments failed settlement once, exposes no raw throw, commits nothing, and never completes Runtime Fatal Signal. When cancellation was requested before supervision handoff—including after a non-cancelled application `Exit` was captured—the same redacted throw and metric are secondary only and cancellation produces no supervision or callback-throw-induced supervision health transition; previously applied or recorded state remains. On a transition-executor defect, the throw likewise remains secondary to the original root-fatal cause.
- The Adapter Failure and Source Runtime Failure branches have distinct outer tags, preventing collisions between adapter-defined and SDK-defined failure tags.
- Source health and dependent Live Query status events expose the exact schema-backed **Source Execution Failure** inferred from the Topic's Source Definition; they do not flatten either branch into a message.
- A **Source Execution Failure** retains a human-readable message for convenience, while the Source Adapter owns redaction of its branch and treats every field admitted by its failure Schema as consumer-visible.
- A Source Definition preserves its adapter runtime, client token, Schedule, and application Effect requirements, which application code satisfies with Layers at the application composition edge.
- Every **Source Definition** explicitly declares exactly one **Source Lifecycle** through a materialized or leased Source Adapter constructor; there is no generic source constructor.
- A **Source Adapter** may support either **Source Lifecycle** or both and exposes only its supported constructors.
- Source Adapters may give their public Source Definition Constructors domain-appropriate names; the SDK does not require literal `.materialized(...)` or `.leased(...)` method names.
- Every public Source Definition Constructor wraps exactly one nominal SDK Materialized or Leased primitive, and the conformance kit proves that lifecycle independently of constructor spelling.
- A Source Adapter cannot create or emulate a third Source Lifecycle through an adapter-specific constructor.
- One materialized source begins scoped Source Attempt acquisition with the View Server Runtime Effect, independently of Live Query lifetime.
- A leased lifecycle factory receives only the exact frozen Feed Route and its typed adapter options; it returns a scoped Source Attempt acquisition Effect and never receives subscriber objects, reference counts, Runtime Clients, or View Server cleanup callbacks.
- View Server creates one child Scope and supervised Source Stream for the first subscriber to an exact Feed Route, then shares that Leased Feed with every same-route subscriber.
- A retrying or exhausted Leased Feed retains its rows and remains shared while at least one subscriber exists.
- The last Leased Feed subscriber closes its child Scope, runs adapter finalizers, deletes only that feed's route-owned rows, and removes its active health state.
- A subscription arriving after complete Leased Feed release creates a fresh feed with fresh Source Termination Schedule state.
- A **Source Adapter** lifecycle factory returns a scoped Effect that acquires one **Source Attempt** and yields its non-empty Source Delivery Lanes of nominal Source Lane Events; View Server owns the attempt Scope, lane consumption, interruption, backpressure, Topic Row validation, rejection accounting, and Runtime Core mutation.
- A Source Attempt contains a non-empty collection of **Source Delivery Lanes**; a simple source has one lane, while a multi-input adapter may preserve independent input ordering with several lanes.
- View Server applies and settles each Source Delivery or records and settles each Source Item Rejection sequentially within its lane, while running sibling lanes concurrently with structured Effect fibers and no merge queue.
- A settled Source Item Rejection continues its lane and marks health Degraded; actual Effect failure or unexpected completion of any Source Delivery Lane terminates the complete Source Attempt, interrupts sibling lanes, awaits all attempt finalizers, and makes Source Supervision reacquire every lane together.
- Successful Source Attempt acquisition establishes operational readiness immediately, including when its Stream has not emitted a Source Delivery; the retained degradation ledger selects visible `Ready` when empty or `Degraded` when non-empty. First-delivery readiness, Ready control events, and polling are invalid.
- Source Attempt acquisition failure enters Source Supervision without reporting ready; Stream failure or unexpected completion closes the attempt Scope and does the same.
- Every retry creates a fresh child Scope and reacquires a fresh Source Attempt rather than rerunning work inside a failed Scope.
- Aggregate platform Layers own only reusable shared infrastructure such as transports, pools, factories, credential refreshers, and O(1) resource maps; Source Attempt Scopes own consumers, subscriptions, channels, iterators, callback registrations, and leases.
- Source retry reacquires every attempt-level resource without rebuilding the aggregate platform Layer or disturbing unrelated Topics and Feed Routes.
- Final Leased Feed release closes only that feed's attempt-level resources; shared adapter resources remain available to other active sources until the enclosing View Server Runtime Scope closes.
- Runtime shutdown interrupts and finalizes all Source Attempts before releasing aggregate adapter Layer resources through ordinary nested Effect Scope ordering.
- A shared transport outage may terminate several dependent Source Attempts, but each source remains independently supervised by its own Source Retry Policy and health state.
- An adapter that permanently hides a source-specific subscription or consumer in its aggregate Layer is non-conformant.
- Source Attempt finalizers are infallible and idempotent, matching Effect `acquireRelease`, Stream `onExit`, and Stream `ensuring`; View Server awaits them before reacquisition.
- An external close rejection is recorded in mandatory Source Adapter Metrics and a structured log or span containing Source Adapter Identity, an opaque per-runtime Feed Route correlation reference when leased, and attempt number; View Server keeps that reference stable across retries without logging raw Route Field values, and cleanup failure never becomes an untyped defect.
- Any expected failure that affects delivery correctness must occur during Source Attempt acquisition, Stream execution, or Source Settlement rather than being deferred to Scope cleanup.
- Only Topic-bound SDK constructors create nominal **Source Mutations** and **Source Deliveries**; View Server rejects raw structurally compatible objects.
- A Source Upsert constructor accepts only the exact complete Topic Row, while a Source Delete constructor accepts only the exact Topic key.
- The complete Upsert row contains its canonical Topic Row ID; the common SDK exposes neither a separate storage key nor an ID-plus-partial-row Upsert.
- A Source Adapter may offer adapter-specific Row Key and Mapping conveniences, but it assembles and Schema-validates the final complete Topic Row before constructing an Upsert.
- A tombstone or equivalent delete event may derive the Topic Row ID from transport key metadata and construct a Delete without decoding, mapping, or fabricating a row value.
- The first-party Kafka adapter exposes `localRowKey` only for delete-only sources; it never trusts a callback to include Region or partition in the canonical Topic Row ID.
- The Kafka adapter validates each region as a non-empty string without `:`, validates Kafka partition as a non-negative integer, constructs delete-only public row IDs as `region:partition:localRowKey`, and constructs compaction-capable IDs exclusively as `region:partition:k<unpadded-base64url(serializedKeyBytes)>`; policy-specific browser-safe `Uint8Array` construction and decoding helpers preserve those exact nominal shapes without Node imports.
- Kafka Region text and delete-only local-row-key text are preserved exactly without casing, accent, or whitespace transformation. For compaction-capable sources the adapter captures exact non-null serialized key bytes before decoding and owns the complete identity; distinct byte strings remain distinct even when the codec produces equal decoded values, while equal bytes always produce the same identity because no application callback participates.
- Kafka applies the composite public row-key rule to every source, including one-region or one-partition sources, and uses the identical composition path for Upserts and every keyed Delete.
- Every first-party Kafka Source Definition declares one mandatory exact **Kafka Cleanup Policy** and one mandatory exact **Kafka Retention Policy**; neither has a compatibility default.
- `compact` and `compact-and-delete` require a metadata-free Kafka Compaction Key Codec that decodes only non-null serialized key bytes and reject any `localRowKey` property; the adapter owns the complete Region-, partition-, and serialized-key-based canonical identity. `delete` requires `localRowKey`, supplies decoded key, decoded non-null value, and Region, and treats a null-valued record as a Source Item Rejection rather than a tombstone.
- The **Kafka Aggregate Node Layer** completes **Kafka Broker Contract Validation** for every external topic and Region before providing its runtime service; a typed validation failure prevents the complete View Server Runtime Effect from starting.
- Kafka Broker Contract Validation normalizes `cleanup.policy` as an unordered comma-separated set with insignificant surrounding whitespace and uses the effective `retention.ms` reported by `@platformatic/kafka` Admin.
- Validated Kafka topic configuration is a startup-only point-in-time snapshot. Changing `cleanup.policy` or `retention.ms` requires a coordinated stop, broker change, and restart of every affected instance; mutation behind a running instance is outside the v1 correctness contract and is not detected by background polling.
- `match-kafka-retention` resolves per Region, retains forever for compact-only sources and `retention.ms = -1`, and otherwise projects the configured time horizon without promising exact segment or `retention.bytes` behavior.
- A matched broker `retention.ms = 0` is valid and sets each deadline to the Kafka record timestamp; the next sweep considers it due only when that timestamp is not in the future according to epoch wall time read from Effect `Clock.currentTimeMillis`.
- An explicit Kafka Retention Policy duration may be shorter or longer than broker retention; positive finite durations and Effect infinity are valid, while explicit zero and negative durations fail configuration.
- **Kafka Retention Projection** anchors deadlines to Kafka record timestamps, maps an already-expired non-null authoritative record directly to a keyed Delete without a transient Upsert, replaces a row's deadline only after successful Upsert, and cancels it on every successful keyed Delete.
- Kafka Retention Projection uses generation-guarded ordinary Deletes so stale deadlines cannot remove newer rows and Live Queries observe the same Delta semantics as any other Delete.
- One logical Materialized Topic lifetime retains Kafka expiry state across Source Attempt retries and owns one coarse sweep whose positive finite **Kafka Retention Sweep Interval** comes from the Kafka Aggregate Node Layer and defaults to 15 minutes.
- Each Kafka Region flattens or `Stream.rechunk(1)`s upstream transport batches to singleton records before any mapping stage can acquire a canonical-ID permit. All rejection-prone work finishes before acquisition, so a Source Item Rejection settles with no lease and two same-ID records from one transport chunk cannot self-deadlock while the first delivery lease awaits downstream application.
- Kafka conformance includes a deterministic upstream-batching fixture with at least two same-canonical-ID records in one multi-record transport chunk; it proves the first singleton reaches Runtime Core and releases its lease before the second acquires that lease, with ordered convergence and no deadlock.
- Kafka compaction conformance uses distinct serialized byte keys that legally decode to the same logical key, proving distinct canonical IDs, rows, deadlines, leases, and failed-work identities; within one Region and partition, repeated identical bytes always produce one identity, while Region and partition remain distinct namespaces, and tombstone or expiry removes only its matching byte identity. Public helper/type tests reject `localRowKey` on compact definitions, prove application code cannot supply or replace Serialized Kafka Key Identity, and prove browser builds require only `Uint8Array`.
- Kafka Mapping conformance includes public type, browser/package-surface, and hostile-runtime coverage for both policy branches. Compact Mapping omits Local Row Key and every identity input; delete-only Mapping includes only its already-derived Local Row Key identity input; both return exact non-ID fields. Runtime validation rejects `id`, identity-bearing, and surplus Mapping results before injection, proving they cannot replace or influence the Adapter-owned canonical ID.
- Delivery lease tests first prove every decode, delete-only Local Row Key, Mapping, canonical-ID, and Topic Row rejection settles before acquisition with zero lease or registry entry. Delivery and sweep lease tests then hold one canonical-ID permit, block a second waiter, and close the waiter’s Source Attempt or logical lifetime before releasing the holder. The waiter joins interruptibly with zero registry transfer, delivery, settlement, maintenance mutation, failed work, or fatal signal; a post-release probe reacquires. A separate interruption immediately after acquisition proves masked ownership transfer completes or releases exactly once.
- A dual controllable Clock fixture deliberately diverges wall milliseconds from monotonic nanoseconds. It proves ingestion/deadline eligibility, matched zero, future timestamps, `durationAgo`, and every public epoch `AtNanos` value use `BigInt(currentTimeMillis) * 1_000_000n`, while sweep sleep/cadence follows monotonic TestClock; advancing either clock alone produces only its documented effect.
- Exact Source Status transition tables prove `degradedAtNanos` is sampled only on empty-to-non-empty ledger transitions, remains stable through reason replacement/combination/recovery and hidden retry states, clears with the final reason, and changes on the next degradation episode across Schema, Wire Protocol, Remote Browser Client, and React diagnostics.
- Kafka retention uses idempotent canonical-ID leases across delivery application and sweep application. Permit contention waits are explicitly interruptible; only post-acquisition registry/finalizer transfer and idempotent release are masked. One attempt-scoped live-lease registry finalizer drains only currently held delivery leases; normal release removes entries immediately. The closed Source Application Transition atomically couples a successful singleton row mutation to its exact expiry-index transition and releases before external settlement; the registry is the fallback before transition. Each due sweep item uses `Effect.acquireUseRelease(Effect.interruptible(acquirePermit), use, release)` or an equivalent fresh child-Scope bracket and then enters the Source Maintenance Interface while holding the lease; bare masked permit acquisition is invalid. That Interface acquires the Source Lifecycle Gate and atomically linearizes status/generation admission, engine mutation, adapter state, failed-work state, and transport-neutral degradation-ledger transition; it cannot retain a lease in the logical-lifetime Scope. A deterministic stale-candidate test pauses a captured generation before permit acquisition, completes keyed Delete and same-ID reinsertion, then resumes it and proves the never-reused token prevents deletion or deadline mutation of the reinserted row while only obsolete candidate/backlog state is cleared.
- The logical Materialized Topic Scope owns the sweep fiber and a supervisor joins it. Scope-close interruption is ordinary; every other failure or premature completion outside the expected per-item maintenance Exit contract completes the Runtime Fatal Signal, whose root race fails the View Server Runtime Effect and closes the listener plus every logical lifetime.
- A failed retention Delete remains eligible for a later sweep, adds one canonical-ID-plus-generation identity to the failed-work set, activates the transport-neutral Adapter Maintenance Failure reason while operationally ready, publishes its exact safe detail and retry-attempt count in Kafka Source Adapter Metrics, and never pauses or terminates Kafka ingestion. Repeated failures do not inflate backlog cardinality; interruption and inactive operational states retain the identity while its generation remains due; successful retry, atomic replacement/removal, or stale-generation observation removes it; and the maintenance reason clears only after the exact failed-work set is empty.
- Exact Kafka Source Adapter Metrics expose declared and observed cleanup, configured and resolved retention, tracked rows, expired rows, already-expired authoritative Deletes, unique failed-work backlog, cumulative sweep retry failures, and last-sweep timing per Topic and Region. Maintenance failure and recovery publish Source Status plus derived Topic/aggregate health transitions immediately with the latest cached metrics. The metrics retain the latest exact safe maintenance failure and cumulative failure state for the logical source lifetime; recovery clears active backlog/health state but not those diagnostics, and only metric-only detail changes coalesce into the next ordinary one-second sample so sub-cadence failure and recovery appears in at least that next metrics publication without an immediate metrics refresh.
- Every first-party Kafka Source Definition declares one mandatory exact **Kafka Start Position**; Kafka aggregate Layer region entries do not select source offsets.
- A Kafka Source Definition selects clients with the same non-empty literal `regions` tuple that supplies its Mapping and diagnostics region union; it declares no client service token or repeated region-to-client map.
- One **Kafka Aggregate Node Layer** receives the View Server Config, a non-empty **Kafka Consumer Group Prefix**, and one exact record mapping all and only the required logical regions to concrete platform options.
- The Kafka Aggregate Node Layer provides the Kafka Source Adapter Runtime Service backed by all configured scoped clients, so ordinary Node applications require one `Effect.provide(KafkaLive)` call regardless of Kafka Topic count.
- The Kafka adapter canonically derives the active Consumer Group ID from the prefix and exact View Server Topic name, exposes that resolved ID in adapter health, uses it in every cluster selected by that Topic, and owns all commits after the initial Start Position.
- Concurrent View Server replicas that each build a complete local view use distinct Kafka Consumer Group Prefixes; one logical replica may retain its stable prefix across process restarts.
- Kafka Start Position is exactly `earliest`, `latest`, `committed`, `timestamp`, or `durationAgo`; the latter three carry a mandatory `earliest`, `latest`, or `fail` missing-offset fallback.
- `timestamp.atNanos` is a non-negative epoch-nanosecond `bigint`; `durationAgo.duration` is a finite non-negative Effect Duration input resolved from Effect `Clock.currentTimeMillis` wall time, never Date, `Date.now()`, or monotonic `Clock.currentTimeNanos`.
- The Kafka adapter converts the requested nanosecond boundary to Kafka's millisecond timestamp resolution and asks each selected cluster for the earliest partition offset at or after that boundary.
- `durationAgo` is evaluated once per Materialized Topic lifetime, and the resulting initial partition offsets are frozen for that lifetime.
- A Kafka Source Attempt retry or connection recovery uses the active consumer group's latest committed offset only for a partition successfully committed in the current Materialized Topic lifetime; every other partition reuses its frozen initial position and fallback instead of inheriting stale group progress.
- The Kafka Node Adapter commits a delivery record only when Source Settlement receives a successful application Exit; typed application failure and defect preserve it for replay and enter their specified supervision or fatal path, while Source Attempt Scope interruption preserves it as lifecycle cancellation without Source Termination or retry. A successfully settled Source Item Rejection commits its rejected offset before the lane continues.
- The accepted Kafka Source Adapter performance gate serially exercises both the transport-neutral multi-region/partition server path and the production Platformatic Kafka Node Adapter against Apache Kafka, and the broker-backed case observes the active consumer group's committed offset after Runtime Core convergence.
- A complete runtime restart creates a new Materialized Topic lifetime and reevaluates that source's `durationAgo`; Kafka v1 has no Leased Source Definition.
- For a Leased Feed, View Server alone derives internal partitioned storage identity from the exact Feed Route and Topic Row ID.
- Every **Source Delivery** contains a `Chunk.NonEmptyChunk` of Source Mutations; one mutation uses `Chunk.of(...)`, several use `Chunk.make(...)`, and an empty source poll emits no Stream element.
- A **Source No-Op Item** is an intentional heartbeat or filter miss settled by its adapter inside sequential Stream production before that source proceeds; it does not create an empty Source Delivery, Source Item Rejection, shared Skip mutation, or authoritative expired-row shortcut.
- An item-local decode, Mapping, canonical-ID, Route Field, or Topic Schema failure becomes a nominal **Source Item Rejection** instead of a Source No-Op Item or Stream failure when the underlying ordered source remains usable.
- View Server records a Source Item Rejection, increments `rejectedItemCount`, stores its latest exact safe diagnostic in sticky Degraded health, then runs rejection settlement before consuming the next lane event.
- Successful rejection settlement continues the lane; rejection settlement failure enters Source Supervision. Kafka commits the rejected offset, while gRPC continues with an infallible no-op settlement only when its decoded response stream remains usable.
- Source Item Rejection health and logs never expose the raw source payload automatically; adapter-owned safe location metadata identifies the item.
- Failure while settling a Source No-Op Item is an Adapter Failure and consumes the ordinary Source Termination Schedule.
- A source tombstone emits a keyed Delete and is not a Source No-Op Item, including when the target Topic Row is already absent.
- A pull-based Source Stream adds no **Source Buffer**; a push or callback integration uses a finite adapter-configurable Source Buffer and never an unbounded queue.
- A full **Source Buffer** pauses a backpressurable producer or fails a non-pausable producer with a typed schema-backed overflow failure; silent dropping and sliding are forbidden.
- The Source Adapter SDK exposes distinct Source Buffer constructors for backpressurable and non-pausable producers rather than a mode flag or raw Queue.
- A backpressurable Source Buffer emitter returns an Effect that suspends at capacity; its adapter must compose that Effect into the producer's flow-control operation.
- A non-pausable Source Buffer emitter is synchronous and returns no capacity boolean an adapter could ignore; the first failed bounded offer increments overflow metrics and fails the Stream exactly once with SDK-owned `SourceBufferOverflow`.
- Source Buffer capacity is a positive finite integer validated during pure construction; callback registration and unregistration are scoped, and the SDK maintains depth and high-water mark through cheap local updates.
- View Server adds no hidden unbounded source prefetch, and source health exposes Source Buffer depth, capacity, high-water mark, and overflow count.
- A **Source Adapter** never receives a Runtime Client or imperative publish callback.
- Every **Source Mutation** is either a complete-row Upsert or a keyed Delete; Source Adapters do not emit partial patches.
- A Source Upsert inserts a missing Topic Row or completely replaces the existing Topic Row with the same key.
- View Server processes **Source Deliveries** from one source Stream sequentially and applies the Source Mutations inside each delivery in order.
- One **Source Delivery** is settlement-atomic but not state-atomic: settlement succeeds only when all its mutations succeed, while mutations applied before a later failure are not rolled back.
- Independent materialized source Streams and distinct leased-route source Streams may execute concurrently.
- View Server uses one outer `Effect.uninterruptibleMask((outerRestore) => ...)` around each **Source Delivery** handoff: mutation application runs with `outerRestore`, its complete `Exit` is captured, **Source Settlement** is applied exactly once while masked, and only the returned settlement Effect is evaluated through that same `outerRestore` inside the Source Attempt Scope; settlement is not an `acquireUseRelease` release/finalizer.
- A successful **Source Settlement** occurs only after every mutation in its **Source Delivery** has been applied successfully.
- Source Settlement is a callback applied exactly once to the application Exit to obtain an Effect whose typed failure is the adapter's declared failure type and whose requirements remain visible in the View Server Runtime Effect. Exactly-once counts callback application, not completion or the first instruction of the returned Effect. That Effect must terminate promptly when the Source Attempt Scope interrupts it; masking or ignoring interruption is non-conformant, while an interruptible `Effect.never` remains a valid fatal-path fixture because Scope close terminates it.
- Source Settlement callback application is synchronous construction only: it must promptly return the Effect without I/O, blocking, timers, Promise work, Effect execution, or unbounded computation. First-party and conformance fixtures gate the returned Effect and prove callback application plus fatal-signal handoff complete before that gate opens. Blocking callback application is non-conformant arbitrary code for which Runtime Core makes no finalization or leak-free guarantee.
- Successful settlement preserves the original mutation-application outcome; typed settlement failure becomes the operational Adapter Failure consumed by Source Supervision. Source Attempt Scope interruption is lifecycle cancellation and creates no Source Termination or retry.
- When mutation application and settlement both fail, View Server retains the original application Exit in spans and logs but does not invent a compound consumer-facing failure value.
- A closed Source Application Transition defect uses `Effect.uninterruptibleMask` to register an attempt-owned supervised settlement child whose masked prefix applies the Source Settlement callback exactly once, captures synchronous throw, records the SDK invocation marker, and signals the handoff latch before the parent completes the Runtime Fatal Signal. The child restores only the returned Effect. Root failure does not await settlement completion; Scope close interrupts the already-owned child while any settlement failure remains secondary diagnostics.
- Mutations applied before settlement failure remain visible and are not rolled back; retry safety relies on complete-row Upsert and keyed Delete idempotence.
- A Source Delivery may omit external settlement semantics, in which case the SDK supplies an infallible no-op settlement.
- Every Source Definition selects one mandatory infallible Effect Schedule whose input is **Source Termination** and whose environment requirements remain visible in the View Server Runtime Effect.
- `SourceAdapterServer.make(...)` requires one default **Source Retry Policy** for each implemented Source Lifecycle; materialized and leased defaults may differ.
- Every shared Source Definition constructor accepts a standard optional Source Retry Policy override and records an exact `UseAdapterDefault` or `Override` selection rather than `undefined`.
- View Server defines no global or fallback Source Retry Policy.
- A Source Retry Policy has the exact Source Termination input, an error type of `never`, and preserves its Effect environment; a lifecycle that never retries uses `Schedule.recurs(0)`.
- The Source Termination Schedule alone decides retry timing and exhaustion; there is no separate retryable predicate, retry boolean, or adapter-owned retry loop.
- A failed Source Termination contains one exact **Source Execution Failure**; an unexpected successful completion remains its own distinguishable Source Termination branch.
- When the Schedule stops, **Source Retry Exhaustion** retains its last exact Source Termination rather than replacing it with a generic terminal error.
- View Server owns **Source Supervision**, fresh scoped Source Attempt acquisition, and retry health derived from Effect Schedule metadata.
- **Source Supervision** never retries fiber interruption.
- A retrying source keeps dependent Live Query Subscriptions open, preserves their last rows, and reports them as stale.
- Exhausted source retries keep dependent Live Query Subscriptions open, preserve their last rows, and report them as error rather than closing them.
- A recovered source returns dependent Live Queries to ready availability and resumes events through their existing Subscriptions; Source Diagnostics returns to Degraded instead of Ready when that logical source lifetime already settled a rejection.
- Source failure never becomes SubscriptionClosed and never erases retained Live Query rows.
- Source Adapter v1 Streams are continuous: successful completion while the source scope remains owned becomes an `UnexpectedCompletion` **Source Termination** and consumes the same adapter-defined Schedule as typed failure.
- Finite one-shot source completion is not part of the Source Adapter v1 lifecycle model.
- A leased source declaration defines one ordered set of **Route Fields**, while each leased-topic **Live Query** supplies one exact **Feed Route** through its `routeBy` object.
- One **Feed Route** identifies exactly one **Leased Feed**; a Live Query never fans out across several Leased Feeds.
- Remote Browser Client headers, credentials, and session identity are never forwarded automatically to a Source Adapter or upstream source.
- View Server authenticates and authorizes each Live Query at its own boundary; the Source Adapter's aggregate Layer and transport interceptors own all upstream credentials and refresh behavior.
- If caller identity changes the upstream dataset, that distinction must be an explicit authorized Route Field so exact Feed Route identity continues to determine Leased Feed sharing.
- **Feed Route** identity preserves each supplied scalar value exactly and never uses **Text Matching** or query normalization; differently cased or accented strings remain different routes and are passed unchanged to the leased source Adapter.
- Every Topic Row admitted to a **Leased Feed** has Route Field values congruent with that feed's Feed Route; a mismatched mapped row is invalid rather than rewritten or retained.
- A leased-topic **Live Query** applies its **Root Conjunction** only after its Feed Route has selected the Leased Feed, so `where` never owns source routing.
- A Live Query for a View Server Topic without leased source lifecycle may not contain a **Feed Route**.
- The **Package Surface Policy** is the single Seam for private package exports, consumer facade projections, pack entries, package direction, and deep-import rejection.
- **Release Publish Orchestration** owns npm staging decisions, pending marker-tag reconciliation, and temporary-artifact cleanup; the release CLI only adapts process state to its Interface.
- The current **Kafka Delivery Contract** is live-process at-least-once after successful publish-then-commit sequencing, but not durable restart recovery unless Kafka is replayed from an authoritative position.
- A **Kafka Consumer Group Assumption** must be documented anywhere runtime options expose consumer-group resume behavior.
- **Health Ledger** state feeds engine health, runtime health, transport health, and React health.

## Example Dialogue

> **Dev:** "Can the browser publish an **Order** row through the **Remote Browser Client**?"
>
> **Domain expert:** "No. The browser only uses the **Live Client** side: it starts a **Live Query** and receives a **Snapshot**, **Deltas**, and **Status Events**. Server-side ingestion uses a **Runtime Client** or runtime adapters to **Publish** rows."
>
> **Dev:** "Can I put an arbitrary object in a topic's `source` property?"
>
> **Domain expert:** "No. `source` contains a **Source Definition** created by a **Source Adapter**. View Server validates its common envelope, and that Source Adapter validates its own configuration."
>
> **Dev:** "For tests, should we mock the hook?"
>
> **Domain expert:** "No. Use the **View Server In-Memory Provider**. It gives the same hook behavior as the **View Server Provider**, backed by an **In-Memory View Server** and the real **Column Live View Engine**."
>
> **Dev:** "Should an AG Grid filter model replace the `where` shape of a **Live Query**?"
>
> **Domain expert:** "No. Give the unchanged grid state to the **AG Grid Adapter**; it translates that state into the canonical typed **Live Query** language."
>
> **Dev:** "Should a Topic Row use a JavaScript Date for a timestamp?"
>
> **Domain expert:** "No. Model the **Timestamp** as a number or bigint and filter it with the matching numeric semantics."
>
> **Dev:** "Does a bare value in a query imply equality?"
>
> **Domain expert:** "No. State equality as an explicit **Field Condition** so the predicate is never implicit."
>
> **Dev:** "Should a leased query infer its upstream feed from equality conditions in `where`?"
>
> **Domain expert:** "No. It supplies one exact `routeBy` **Feed Route** containing every configured **Route Field**; `where` remains independent local filtering."
>
> **Dev:** "Can a leased source route by `profile.country` or an object-valued field?"
>
> **Domain expert:** "No. **Route Fields** are top-level Filterable Scalars so one Feed Route has exact, wire-safe identity."
>
> **Dev:** "Does case-insensitive Text Matching make `routeBy: { region: \"USA\" }` share a feed with `usa`?"
>
> **Domain expert:** "No. A **Feed Route** preserves the supplied scalar value exactly: no case folding, accent folding, trimming, or other query normalization. Text Matching belongs only to Field Conditions."
>
> **Dev:** "Can a Raw Query use `select: []` to return rows without fields?"
>
> **Domain expert:** "No. A **Raw Query** selects one or more existing Topic Row fields; an empty or absent selection is invalid."
>
> **Dev:** "Can filtering express `(age > 23 OR sport ends with ing) AND country contains united`?"
>
> **Domain expert:** "Yes. Represent it as a **Filter Expression** with a nested OR group inside an AND group."
>
> **Dev:** "Can a Grouped Query use `where` to filter an aggregate alias?"
>
> **Domain expert:** "No. Its **Root Conjunction** filters source Topic Rows before grouping and aggregation; aggregate-result filtering is a separate query concept."
>
> **Dev:** "Can `groupBy: []` mean no grouping or one global aggregate group?"
>
> **Domain expert:** "No. A **Grouped Query** names one or more existing group fields; an empty `groupBy` is invalid."
>
> **Dev:** "Can `aggregates: {}` make a Grouped Query return distinct group values?"
>
> **Domain expert:** "No. A **Grouped Query** defines one or more aggregate aliases; an empty or absent `aggregates` is invalid."
>
> **Dev:** "Can I negate an entire nested expression rather than inventing a negated version of every leaf condition?"
>
> **Domain expert:** "Yes. A unary **Negation Expression** complements any normalized **Filter Expression**."
>
> **Dev:** "Can I spell a logical discriminator as lowercase `and` or a leaf as uppercase `EQUALS`?"
>
> **Domain expert:** "No. **Filter Expressions** have one exact discriminator spelling: uppercase logical tags and defined camelCase Field Condition names."
>
> **Dev:** "Does a large but otherwise valid filter fail an arbitrary query-complexity budget?"
>
> **Domain expert:** "No. **Filter Expressions** have no language-defined size ceiling; callers may submit large queries and bear their execution cost."
>
> **Dev:** "Can a local JavaScript query contain a cycle, or reuse one condition object in several branches?"
>
> **Domain expert:** "A cycle is not a **Filter Expression** and fails validation. Reusing an acyclic condition is valid; **Filter Normalization** handles equivalent occurrences by meaning."
>
> **Dev:** "Must every query value work with native `JSON.stringify`?"
>
> **Domain expert:** "No. It must form a **Wire-Safe Query**: bigint and BigDecimal use their schema-aware encodings, while values that cannot round-trip without semantic loss are invalid."
>
> **Dev:** "Can I write `where: undefined` or `caseSensitive: undefined` to mean omitted?"
>
> **Domain expert:** "No. A **Wire-Safe Query** omits an optional property or supplies a valid defined value; explicit `undefined` is invalid. Use an allowed empty collection such as an empty **Root Conjunction** when generated emptiness is intentional."
>
> **Dev:** "If I mutate my `where` array after subscribing, does the active query change?"
>
> **Domain expert:** "No. The submitted **Live Query** owns a semantic snapshot, so later caller mutation has no effect; submit another query to change its meaning."
>
> **Dev:** "Can I pass an object whose keys are fields and rely on an implicit AND?"
>
> **Domain expert:** "No. Use one explicit **Filter Expression**; field-keyed condition maps are invalid."
>
> **Dev:** "What joins entries at the top level of a query filter?"
>
> **Domain expert:** "The top-level array is the **Root Conjunction**, so its entries are joined by AND. Use an explicit nested group for OR."
>
> **Dev:** "What does an empty **Root Conjunction** mean?"
>
> **Domain expert:** "It matches every Topic Row, like an omitted filter or SQL's `WHERE 1 = 1`."
>
> **Dev:** "Does an empty nested OR mean false?"
>
> **Domain expert:** "No. **Filter Normalization** removes empty generated groups regardless of their operator; an entirely empty filter matches every Topic Row."
>
> **Dev:** "What happens when an AND or OR group has one effective child after empty groups are removed?"
>
> **Domain expert:** "**Filter Normalization** collapses the group to that child because the operator no longer changes its meaning."
>
> **Dev:** "Does `NOT` around an empty generated group become a deny-all predicate?"
>
> **Domain expert:** "No. The empty child is absent rather than Boolean true or false, so its **Negation Expression** also normalizes away."
>
> **Dev:** "Does `NOT(NOT(A))` remain a distinct query or trigger De Morgan rewrites?"
>
> **Domain expert:** "Neither. Double negation collapses directly to `A`, while **Filter Normalization** does not expand negations through AND or OR groups."
>
> **Dev:** "Are `NOT(equals)` and `notEqual` different queries?"
>
> **Domain expert:** "No. **Filter Normalization** chooses the exact named complement when one exists, so both forms share one identity."
>
> **Dev:** "Does a membership condition with no selected values hide every row?"
>
> **Domain expert:** "No. It is an **Open Membership Condition**, so it contributes no predicate and future unseen values remain eligible. Use a real non-matching value to intentionally return no rows."
>
> **Dev:** "Are `in: [\"open\", \"closed\"]` and `in: [\"closed\", \"open\", \"open\"]` different queries?"
>
> **Domain expert:** "No. A **Membership Condition** is an unordered semantic set, so candidate order and equivalent duplicates do not change its identity."
>
> **Dev:** "Does an AND nested directly inside another AND remain nested?"
>
> **Domain expert:** "No. **Filter Normalization** flattens adjacent groups with the same operator because they express one logical group."
>
> **Dev:** "Are `AND(A, B)` and `AND(B, A)` different queries?"
>
> **Domain expert:** "No. AND and OR child order is not semantic, so **Filter Normalization** gives reordered expressions one query identity."
>
> **Dev:** "Does repeating the same condition change a query?"
>
> **Domain expert:** "No. **Filter Normalization** deduplicates equivalent conditions using Topic Row Value Semantics."
>
> **Dev:** "Will normalization prove every logically equivalent Boolean expression identical?"
>
> **Domain expert:** "No. It performs the defined structural rules but does not apply absorption, distribution, or normal-form conversion."
>
> **Dev:** "Should a greater-than-or-equal condition be called `gte`?"
>
> **Domain expert:** "No. **Field Conditions** use the full `greaterThanOrEqual` name shared with AG Grid."
>
> **Dev:** "Can a Field Condition call its operand `value` or `from`?"
>
> **Domain expert:** "No. Operands are named `filter` and, when a second bound is required, `filterTo`."
>
> **Dev:** "Can a blank condition carry an unused `filter`, or can an OR group carry a `field`?"
>
> **Domain expert:** "No. Every **Field Condition** and logical group has an exact shape; irrelevant and unknown properties are invalid."
>
> **Dev:** "Does a boolean field need special `true` and `false` condition types?"
>
> **Domain expert:** "No. Boolean values use ordinary **Equality Conditions** and **Membership Conditions**; an Adapter translates any external boolean-specific form."
>
> **Dev:** "Can I use `equals` to compare an entire profile object or tags array?"
>
> **Domain expert:** "No. **Field Conditions** operate only on **Filterable Scalars** and never perform deep structured-value comparison."
>
> **Dev:** "Can I still filter the scalar country inside a profile object?"
>
> **Domain expert:** "Yes. A **Filterable Field Path** traverses the profile to its country leaf, and the **Field Condition** compares only that scalar."
>
> **Dev:** "Can a schema field literally be named `profile.country`?"
>
> **Domain expert:** "No. A dot exclusively separates segments of a **Filterable Field Path**; field names containing dots are invalid rather than escaped."
>
> **Dev:** "Can a path select `tags.0` or a key inside a Record?"
>
> **Domain expert:** "No. A **Filterable Field Path** crosses only statically named object fields; collections and dynamic-key structures stop traversal."
>
> **Dev:** "What does `profile.country` resolve to when the optional profile is absent?"
>
> **Domain expert:** "It resolves as blank. The path then follows the same **Blank Condition** and exact-complement rules as a missing top-level field."
>
> **Dev:** "Must a nested scalar exist in every object-union branch before I can filter it?"
>
> **Domain expert:** "No. A statically named scalar in any branch forms a **Filterable Field Path**; branches without it resolve as blank."
>
> **Dev:** "What if one union branch stores a string at `value` and another stores an object there?"
>
> **Domain expert:** "The scalar branch still makes `value` a **Filterable Field Path** for its supported conditions. The object branch is incompatible with the positive condition and is never compared deeply; its nested scalar paths remain independently filterable."
>
> **Dev:** "Can a recursive employee schema generate `manager.manager.name` paths?"
>
> **Domain expert:** "No. A recursive reference stops the **Filterable Field Path**; the language has no arbitrary hidden depth cap."
>
> **Dev:** "Does branding or safely transforming a fixed-shape object hide its scalar paths?"
>
> **Domain expert:** "No. A **Filterable Field Path** follows the accepted decoded schema shape; only an opaque decoded shape stops traversal."
>
> **Dev:** "Is text matching case-sensitive unless I say otherwise?"
>
> **Domain expert:** "No. A **Text Condition** is case-insensitive by default and opts into case-sensitive matching explicitly."
>
> **Dev:** "Are string `equals` and `in` case-sensitive even though `contains` is not?"
>
> **Domain expert:** "No. **Text Matching** applies to every string operation; `caseSensitive` and `accentSensitive` can change its two sensitivity axes independently."
>
> **Dev:** "Does `resume` match `Résumé` by default?"
>
> **Domain expert:** "Yes. **Text Matching** ignores both case and accents by default; either sensitivity can be enabled independently on the Field Condition."
>
> **Dev:** "Does accent-insensitive matching make `straße` equal `strasse`?"
>
> **Domain expert:** "No. **Text Matching** removes marks exposed by Unicode canonical decomposition; it does not apply locale-aware substitutions or transliteration."
>
> **Dev:** "Are default-insensitive `Résumé` and explicit-insensitive `resume` different queries?"
>
> **Domain expert:** "No. Their effective **Text Matching** semantics and normalized operands are identical, so they share one semantic query identity."
>
> **Dev:** "Can I put `caseSensitive: true` on an age comparison or a blank check?"
>
> **Domain expert:** "No. Only a **Text Condition** may declare Text Matching modifiers; irrelevant modifiers fail query validation."
>
> **Dev:** "Does `contains` with an empty search operand mean no filter?"
>
> **Domain expert:** "No. Empty text-search operands are invalid; omit the condition for no filter or use an **Equality Condition** to target the empty string."
>
> **Dev:** "What if a non-empty search contains only combining marks removed by accent-insensitive matching?"
>
> **Domain expert:** "It is still invalid. Text-search non-emptiness is measured after that condition's **Text Matching** normalization."
>
> **Dev:** "What if an equality or membership operand normalizes to the empty string?"
>
> **Domain expert:** "It remains valid and matches only the actual empty string. A **Blank Condition** is still required to include missing, `undefined`, and `null`."
>
> **Dev:** "Does `notBlank` mean JavaScript-truthy?"
>
> **Domain expert:** "No. A **Blank Condition** matches only missing, `undefined`, `null`, and the empty string. Values such as `false`, `0`, `0n`, and whitespace-only text are not blank."
>
> **Dev:** "Does `inRange` from 3 to 5 include 5?"
>
> **Domain expert:** "No. A **Range Condition** is half-open: it includes 3 and excludes 5."
>
> **Dev:** "Can a BigDecimal field use comparison and range conditions with number operands?"
>
> **Domain expert:** "It supports the full numeric condition family, but its operands must also be BigDecimal; numeric kinds never coerce across conditions."
>
> **Dev:** "Can a number condition use `NaN`, infinity, or distinguish `-0` from `0`?"
>
> **Domain expert:** "No. Non-finite numbers are invalid **Condition Operands**, and both zero representations have one semantic identity."
>
> **Dev:** "Does `inRange` from 5 to 5 intentionally match nothing?"
>
> **Domain expert:** "No. A **Range Condition** requires a strictly increasing pair; equal or reversed bounds are invalid."
>
> **Dev:** "Does `notEqual: 5` silently exclude blank values?"
>
> **Domain expert:** "No. An **Equality Condition** makes `notEqual` the exact complement of `equals`; combine it with `notBlank` when blanks must be excluded."
>
> **Dev:** "Does `equals` with an empty-string operand mean `blank`?"
>
> **Domain expert:** "No. An **Equality Condition** can match the empty string or schema-admitted `null` exactly. Use a **Blank Condition** to include missing and `undefined` values too."
>
> **Dev:** "Does a blank value satisfy `notContains: \"x\"`?"
>
> **Domain expert:** "Yes. A **Negated Condition** is the exact complement of its positive condition; add `notBlank` when blank values must be excluded."
>
> **Dev:** "Can a `string | number` field use both `contains` and `greaterThan`?"
>
> **Domain expert:** "Yes. Each is a **Domain-Selective Condition**: text is never coerced to a number and numbers are never stringified for text matching."
>
> **Dev:** "Can a field restricted to numeric literals `1 | 2 | 3` use `greaterThan` with `1.5`?"
>
> **Domain expert:** "Yes. A comparison **Condition Operand** may be any valid value of the same numeric kind, while equality and membership still require schema-admitted values."
>
> **Dev:** "Does `in` with `[null]` mean every blank value?"
>
> **Domain expert:** "No. A core **Membership Condition** matches actual `null` exactly. Only the **AG Grid Adapter** interprets Set Filter's `null` sentinel as a **Blank Condition**."
>
> **Dev:** "Will the server guess what my custom Set Filter key means?"
>
> **Domain expert:** "No. An **AG Grid Set Key** must decode through the bound field schema; a lossy key creator is the consumer's responsibility."

## Flagged Ambiguities

- "topic" can mean **Source Topic** or **View Server Topic**. Use the full term when ingestion is involved.
- "client" can mean **Live Client**, **Runtime Client**, or **Remote Browser Client**. Use the precise term because each has different mutation permissions.
- "provider" can mean **View Server Provider** or **View Server In-Memory Provider**. Use the precise term when ownership/cleanup matters.
- "protocol" means the **Wire Protocol** unless explicitly discussing an internal TypeScript interface.
- "subscription" is not a WebSocket connection; a single connection can carry multiple **Subscriptions**.
- "health" should specify **Health Ledger**, engine health, runtime health, transport health, or React health when the owner matters.
- "view" is overloaded in database/UI language; prefer **Live Query**, **Snapshot**, or **Grouped Query** depending on the intended concept.
- `routeBy` names the ordered **Route Fields** in a leased source declaration and the exact **Feed Route** object in a leased-topic Live Query; use those domain terms when the shape matters.
- "Kafka consumer group resume" is not equivalent to **View Server** recovery until durable checkpoints/WAL exist. If the runtime must rebuild rows after restart, replay Kafka from an authoritative position such as the beginning of the relevant Source Topics.
