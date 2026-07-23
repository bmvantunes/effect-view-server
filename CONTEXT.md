# View Server Context

This context defines the language for the View Server project: a type-safe live view system that serves initial snapshots and live deltas from an authoritative in-memory engine to React applications over a real server or an in-memory test runtime.

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
A configured logical table with one Topic Schema, one canonical Topic Row ID, and one authoritative store.
_Avoid_: Kafka topic, channel, collection

**View Server Config**:
The one frozen browser-safe declaration created by `defineViewServerConfig(...)` and shared by React, Remote Browser Clients, In-Memory View Server, and the real runtime. It is the sole type authority for View Server Topics, Topic Schemas, queries, Feed Routes, source failures, health, and adapter metrics. Every Topic appears once and owns zero or one canonical Source Definition; there is no mirrored browser contract and server runtime topic tree. It contains browser-safe Source Adapter options, including Mapping functions, generated descriptors, platform-neutral codecs, and diagnostic Schemas that may contribute to the browser bundle, but no concrete transport client, credential, platform Layer, Node dependency, or ManagedRuntime. V1 accepts that bundle tradeoff rather than introducing code generation, a build transform, automatic projection, or duplicate authoring.
_Avoid_: Separate contract/runtime configs, duplicated topic tree, generated untyped client metadata, server resource in shared config

**View Server Runtime Effect**:
The scoped server-edge Effect returned by `runViewServerRuntime(viewServer, options)`. Its environment is the exact union inferred from the one View Server Config's Source Adapter runtime services, retry Schedules, and application dependencies. Application code satisfies that union with aggregate adapter/platform Layers through `Effect.provide(...)` before `NodeRuntime.runMain(...)`.
_Avoid_: Transport-specific runtime option bag, hidden adapter runtime, reusable module calling Effect.run

**Runtime Composition Failure**:
A fatal typed failure before View Server transport availability caused by invalid or tampered View Server Config, a missing Source Adapter Runtime Service, missing or extra aggregate Layer resource entries, Effect Config failure, or failure acquiring mandatory aggregate Layer infrastructure. It fails the View Server Runtime Effect and no server port is opened. An operational Source Attempt acquisition or execution failure is never a Runtime Composition Failure; Source Supervision isolates it while the runtime and unrelated Topics remain available.
_Avoid_: Broker outage classified as fatal startup, partially listening invalid runtime, missing Layer converted to source retry

**Topic Row**:
A Topic-Schema-decoded object stored in a View Server Topic. Every Topic Row contains exactly one required canonical `id: string` field.
_Avoid_: Record, document, message

**Topic Row Value Semantics**:
The schema-derived ownership, equivalence, canonical JSON representation, and ordering rules for every configured Topic Row field. The Column Live View Engine compiles these rules once per View Server Topic and reuses them at ingestion, projection, grouping, comparison, Snapshot, and Delta boundaries. Canonical identity normalizes order-insensitive persistent collections while preserving ordinary sequence order. Topic configuration rejects non-injective or unrecognized codec transformations and equality domains without a congruent canonical identity/order witness.
_Avoid_: Deep clone helper, generic object equality, JSON stringify semantics

**Topic Row ID**:
The required `id: Schema.String` field declared by every Topic Schema. Its decoded string uniquely identifies a Topic Row and acts as the final deterministic sort tiebreaker; the field name and Schema are not configurable.
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

**AG Grid Adapter**:
The client integration boundary that translates AG Grid viewport, filter, sort, and grouping state into typed Live Queries while keeping the View Server query language independent of AG Grid.
_Avoid_: AG Grid where model, AG Grid query language, core FilterModel

**AG Grid Set Key**:
A string key emitted by an AG Grid Set Filter that the AG Grid Adapter decodes into the corresponding schema-admitted Topic Row field value. A consumer-defined key creator owns reversibility; a lossy or schema-invalid key fails with a typed Adapter error.
_Avoid_: Guessed field value, implicit string field, server-side key reconstruction

### Ingestion Concepts

**Source Topic**:
An external Kafka topic or future server-side source that provides messages to be mapped into a View Server Topic.
_Avoid_: View Server Topic

**Source Adapter**:
A build-time integration declared once with `SourceAdapter.make(...)` and implemented with `SourceAdapterServer.make(...)`. Its browser-safe declaration owns Source Adapter Identity, one complete Source Adapter Failure Schema, mandatory schema-backed Source Adapter Metrics, supported Source Lifecycles, and complete browser-safe Source Definition constructors. Its server implementation provides the matching nominal Source Adapter Runtime Service whose lifecycle factories acquire scoped Source Attempts and yield streams of Source Deliveries while preserving typed configuration, Mapping, environment requirements, failures, and metrics.
_Avoid_: Runtime-discovered plugin, transport built into Runtime Core, imperative Runtime Client integration, untyped callback

**Source Adapter Package Surface**:
The required package-export boundary for a Source Adapter: `/contract` contains browser-safe Source Definition constructors, Schemas, and optional client service-token factories; `/server` contains the matching Source Adapter Runtime Service implementation and transport-neutral Layers; platform exports such as `/node` contain concrete transport-driver Layers. Every published platform export provides the standard `layer(viewServer, resolvedOptions)` and `layerConfig(viewServer, configWrappedOptions)` pair. Both accept exact adapter-wide resource maps inferred from the View Server Config and return one aggregate scoped Layer providing the adapter runtime service plus all concrete clients and resources. The Config variant accepts exact `Config.Wrap<Options>`, calls `Config.unwrap(...)` once during Layer construction, and retains `Config.ConfigError`; other service requirements remain visible in the Layer environment. Adapter authors consume the SDK only through `effect-view-server/source-adapter`, `effect-view-server/source-adapter/server`, and `effect-view-server/source-adapter/testing`; internal or deep SDK imports are invalid. A published adapter peer-depends on `effect-view-server` plus every Effect ecosystem package used by its public or runtime surfaces, and keeps those packages as development dependencies for its own build and tests. SDK conformance tests prove that `/contract` cannot resolve Node APIs, server implementations, concrete clients, or transport-driver packages.
_Avoid_: Adapter-specific run function, hidden Runtime, missing layerConfig, bundled Effect runtime, bundled View Server SDK, undeclared Effect platform peer, one adapter root that mixes browser contracts and broker clients, hidden platform dependency, untested conditional export

**Source Adapter Conformance Kit**:
The mandatory behavioral and package-boundary test suite exported from `effect-view-server/source-adapter/testing`. For every supported Source Lifecycle, a published adapter supplies a controllable test Layer that can acquire a Source Attempt, emit valid deliveries, fail acquisition or execution with its exact adapter failure, complete unexpectedly, expose metrics changes, and observe scoped finalization. Leased adapters additionally expose exact-route acquisition and final-release observation. The shared kit uses `@effect/vitest` scoped Layer suites and TestClock to verify readiness, sequential delivery and settlement, retry and exhaustion, interruption, finalizer completion, metrics validation, bounded-buffer behavior when used, leased sharing and cleanup, and recovery without hidden runtimes. It also verifies package exports, browser safety, peer dependencies, nominal linkage, Schemas, and positive and negative public type inference. First-party and third-party adapters meet the same contract.
_Avoid_: Shape-only certification, bespoke adapter test semantics, real-time retry sleeps, optional conformance, first-party exception

**Source Adapter Identity**:
Diagnostic metadata carried by every Source Definition: a required adapter name and optional adapter version. View Server validates this metadata and exposes it in source health, typed errors, spans, and logs; it never uses Source Adapter Identity for registration, dispatch, compatibility, or Source Definition equality. Package-manager peer ranges, public TypeScript API compatibility, nominal SDK brands, and runtime envelope validation enforce adapter compatibility; there is no redundant Source Adapter protocol field.
_Avoid_: Adapter registry key, runtime plugin lookup, SDK protocol version, compatibility dispatch, source identity

**Source Adapter Runtime Service**:
The nominal server-only Effect service implemented by `SourceAdapterServer.make(...)` for one exact Source Adapter declaration. `SourceAdapter.make(...)` creates its opaque browser-safe Effect `Context.Service` tag automatically for nominal type and runtime linkage; adapter authors never declare or repeat a tag ID, service interface, failure type, metrics type, or lifecycle list. The contract contains no implementation or Layer. `SourceAdapterServer.make(...)` accepts only the matching Source Adapter handle and implements exactly its declared lifecycles. The resulting service receives the exact frozen adapter-specific Source Definition, Source Target, and Topic-Bound Source Toolkit; supplies the matching lifecycle factory, mandatory local metrics Effect, and default Source Retry Policy; and returns a scoped Source Attempt acquisition Effect. `runViewServerRuntime(...)` requires this service whenever the one View Server Config uses that adapter. View Server resolves the nominal service directly from Effect Context and never dispatches through Source Adapter Identity or a runtime registry.
_Avoid_: Author-defined duplicate Context tag, repeated tag ID, adapter-name registry, string dispatch, hidden ManagedRuntime, transport branch in Runtime Core

**Source Adapter Metrics**:
The mandatory adapter-defined, Schema-backed metrics value included in every source health payload and inferred exactly through the one View Server Config into Remote Browser Client and React APIs. Each supported Source Lifecycle declares its own metrics Schema instead of a boolean capability, so a selected materialized or leased Source Definition has one exact metrics type without a lifecycle union or optional fields. Its Source Adapter Runtime Service supplies an infallible Effect that reads only a valid local metrics snapshot, including before the Source Stream becomes ready; the Effect's requirements remain visible. View Server samples that Effect exactly once per second with Effect Clock, freezes and Schema-validates the result, and publishes only the cached snapshot. Metric-only publications occur at most once per cadence; lifecycle transitions and Source Item Rejections publish immediately with the latest cached metrics. V1 exposes no global, adapter, source, or subscriber cadence setting. Source health always contains both mandatory SDK-owned `runtime` metrics and mandatory adapter-owned `adapter` metrics. A Source Adapter without matching lifecycle metrics Schemas and runtime implementation is invalid.
_Avoid_: Optional details, missing metrics, unknown metrics object, server-only untyped health payload

**Source Rejection Location**:
The mandatory lifecycle-specific Schema-backed adapter value identifying one rejected source item without exposing its raw payload. Each Source Adapter lifecycle declares its exact location Schema beside its metrics Schema, and its Source Item Rejection constructor accepts only that type. Kafka uses safe region, external topic, partition, offset, and phase fields; gRPC uses safe logical client, method, and stream-item context. The exact value round-trips through Source Diagnostics and structured telemetry.
_Avoid_: Raw key or value, unknown location object, optional location, message payload in health

**Source Runtime Metrics**:
The mandatory SDK-owned metrics value paired with Source Adapter Metrics in every source health payload. It contains epoch-nanosecond `bigint` timestamps named with an `AtNanos` suffix, cumulative source-wide `bigint` counters for attempts, retries, deliveries, rejected source items, mutation outcomes, and settlement outcomes, a numeric retained-row count, and a non-empty list of Source Delivery Lane metrics. Every lane has a stable, non-empty, retry-stable unique identifier and an exact unbuffered-or-bounded Source Buffer value; a simple source therefore still reports one lane. Status and failures remain outside metrics. View Server reads time only through `Clock.currentTimeNanos`; contracts and the Wire Protocol carry raw `bigint`, never Date or Temporal objects, so consumers may explicitly construct `Temporal.Instant` without losing precision.
_Avoid_: Aggregate-only buffer metrics, empty lanes, unstable lane ID, millisecond timestamp, number timestamp, ambiguous time unit, Date, Temporal object on wire, optional buffer metrics

**Source Status**:
The mandatory Schema-backed tagged union describing exactly one Source lifecycle state: `Starting`, `Ready`, `Degraded`, `WaitingToRetry`, `Reacquiring`, `Exhausted`, or `Stopping`. Each branch contains only its valid fields and nanosecond timestamps. `Degraded` means delivery continues after at least one settled Source Item Rejection and contains the latest exact safe rejection; it remains sticky for that logical source lifetime because skipped input may leave the view incomplete. Waiting and reacquisition retain the exact Source Termination; exhaustion retains Source Retry Exhaustion; stopping names runtime shutdown or final Leased Feed release. React and Remote Browser Client APIs infer the complete union and narrow exhaustively on `_tag` without optional failure or retry fields. Live Query availability maps `Degraded` to ready while Source Diagnostics retains the distinction.
_Avoid_: Status string plus optional fields, nullable failure bag, ambiguous retry phase, erased termination

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
A schema-backed SDK-owned failure produced by the common ingestion pipeline, such as Source Buffer overflow, invalid Source Delivery, invalid Topic Row, or leased Route Field mismatch. Every Source Adapter receives this exact shared failure vocabulary automatically rather than copying it into its adapter-specific failure union. An item-local invalid Topic Row or Route Field mismatch may be carried by a settled Source Item Rejection when its ordered source remains usable; a failure value is not automatically terminal merely because it belongs to this vocabulary.
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
The narrow nominal helper passed by View Server to one Source Adapter lifecycle factory. It exposes the exact View Server Topic name plus Topic-bound `upsert`, `delete`, `delivery`, and `reject` constructors that preserve and runtime-validate the configured Topic Row, canonical ID, adapter failure, lifecycle rejection location, Feed Route, and settlement types. It exposes no Runtime Client, publish callback, Subscriber, session, reference count, browser header, internal Topic Store, mutable config, or raw Schema-validation bypass. The adapter's own external codecs perform transport decoding; View Server's constructors own final Topic contract validation.
_Avoid_: Runtime Client, generic publish function, Topic Store handle, raw Schema decoder, mutable topic object

**Source Delivery**:
One nominal SDK-created Source Adapter emission containing a `Chunk.NonEmptyChunk` of ordered Source Mutations whose settlement is tied to the outcome of applying those mutations to its Source-Owned Topic. Its constructors are bound to the exact Source Definition and Topic: Upsert accepts only a complete Topic Row and Delete accepts only that Topic's key. An empty source poll emits no Stream element rather than an empty Source Delivery. Delivery settlement is atomic, but Runtime Core state application is not transactional: mutations applied before a later mutation fails remain visible.
_Avoid_: Empty delivery, raw lookalike object, bare Topic Row, already-acknowledged message, direct Runtime Client command

**Source No-Op Item**:
An external source item that an adapter intentionally maps to no View Server mutation, such as a heartbeat or adapter-level filter miss. The adapter settles it inside the pull-ordered Source Stream production effect before proceeding; settlement failure becomes an Adapter Failure and enters Source Supervision. A malformed, unparseable, or invalid item is instead a visible Source Item Rejection. The SDK defines no Skip mutation or empty Source Delivery, while a tombstone remains a real keyed Delete even when its Topic Row is already absent.
_Avoid_: Empty Source Delivery, generic Skip mutation, treating tombstone as no-op, unordered early acknowledgement

**Source Item Rejection**:
A nominal SDK-created Source Lane Event for one item-local decode, Mapping, canonical-ID, Route Field congruence, or Topic Schema failure that does not make the underlying ordered source unusable. It carries the exact schema-backed safe failure, adapter-owned safe source location, `rejectedAtNanos`, and rejection settlement; it never contains the raw payload automatically. View Server records the rejection, increments `rejectedItemCount`, marks Source Health, the affected Topic health row, and aggregate View Server health `Degraded`, and executes settlement before pulling the next lane event. Successful settlement continues delivery; settlement failure terminates the Source Attempt through ordinary Source Supervision. Kafka settlement commits the rejected offset, while gRPC uses a no-op settlement and continues only when the decoded response stream remains usable. `Degraded` is sticky for the logical source lifetime and Live Query availability remains ready. Liveness and readiness transports remain successful while reporting degraded state rather than removing or restarting the instance.
_Avoid_: Poison pill retry loop, silent drop, raw payload in health, fake mutation, immediate recovery to Ready, transport failure treated as item rejection

**Source Buffer**:
A finite SDK-owned bridge used only when an external push or callback producer cannot remain directly pull-driven by its Source Stream. The SDK exposes separate constructors for backpressurable and non-pausable producers so their emission contracts cannot be confused. The backpressurable emitter returns an Effect that suspends at capacity and must be composed by the producer. The non-pausable synchronous emitter returns no ignorable capacity result; on the first full-buffer emission, the SDK increments overflow metrics and fails the Stream exactly once with `SourceBufferOverflow`. Both constructors use an internal bounded Queue, validate a positive finite integer capacity during pure construction, own callback unregistration through Scope, and update depth and high-water mark locally. The Queue and its strategy are never exposed. Sliding, dropping, and unbounded buffering are invalid because losing one Upsert or Delete can corrupt the authoritative materialized view.
_Avoid_: Public Queue, strategy option, shared callback constructor, ignored offer boolean, unbounded callback queue, silent drop, sliding mutation buffer, hidden View Server prefetch

**Source Mutation**:
A nominal SDK-created complete-row change inside a Source Delivery: either an Upsert containing one complete Topic Row with its canonical Topic Row ID, or a Delete identifying one Topic Row ID. Source Mutation constructors are bound to the exact Source Definition and Topic so invalid rows, IDs, and structural substitutes fail before reaching View Server. The common SDK exposes no separate storage key and no ID-plus-partial-row Upsert. An Upsert inserts a missing row or completely replaces an existing row; partial patch mutations are not part of the Source Adapter contract.
_Avoid_: Raw mutation object, partial source patch, merge-upsert, transport-specific event, Runtime Client command

**Kafka Local Row Key**:
The transport-local string returned by a first-party Kafka Source Definition's `localRowKey` callback before the adapter constructs the canonical Topic Row ID. The Kafka adapter always combines the exact Region and local key as `region:localRowKey`, injects that composite into `id`, and uses the same operation for Upserts and tombstone Deletes. Region identifiers are non-empty and may not contain `:`; local keys may contain `:` because decoding splits only at the first separator. The adapter exposes canonical construction and decoding helpers. This rule applies even to a one-region source, so independent Kafka clusters cannot silently collide in one View Server Topic.
_Avoid_: User-composed Kafka public key, `rowKey` callback, hidden region namespace, delimiter without validation, region normalization

**Kafka Start Position**:
The mandatory exact policy on a first-party Kafka Source Definition that selects initial offsets independently in every selected cluster and partition. Its variants are `earliest`, `latest`, `committed`, absolute `timestamp` with epoch-nanosecond `bigint`, and `durationAgo` with Effect `Duration.Input`. `committed`, `timestamp`, and `durationAgo` carry a mandatory `earliest`, `latest`, or `fail` fallback for a partition without a usable offset. Relative time is read through Effect Clock and fixed once per logical source lifetime; the adapter converts nanoseconds to Kafka's millisecond timestamp resolution before using timestamp offset lookup and freezes the resolved initial partition offsets. A Source Attempt retry resumes the active consumer group's latest committed offsets and uses the frozen initial offset only where no commit exists. A complete materialized runtime restart or fresh Leased Feed after final release creates a new logical lifetime and reevaluates `durationAgo`. There is no implicit default.
_Avoid_: Moving retry window, optional start position, Date, millisecond number without unit, hidden earliest fallback, Effect-internal `_tag` in authored configuration

**Kafka Consumer Group Prefix**:
The mandatory non-empty logical View Server replica identity supplied once to the aggregate `kafkaNode.layer(...)`. The Kafka Source Adapter Runtime Service deterministically derives each Topic's active Kafka Consumer Group ID from this prefix and the exact View Server Topic name by independently applying canonical UTF-8 percent encoding to both components and joining them with `:`. For example, prefix `my-view-server` and Topic `orders` resolve to `my-view-server:orders`. This prevents two Topics using one replica prefix from accidentally sharing an active group while keeping the usual value readable. Every concurrently running View Server replica that builds a complete local view must use a distinct prefix; the same logical replica may reuse its stable prefix across restarts. The same resolved ID applies in every cluster selected by that Topic and owns the commits used for connection recovery and Source Attempt retry. The prefix is never optional, repeated per Source Definition, stored in an individual Kafka region entry, or supplied by generic runtime options, and the resolved ID is present in exact Kafka adapter health so deployment collisions are diagnosable. A `committed` Kafka Start Position may name a different literal Consumer Group ID whose offsets seed the initial position, but subsequent commits belong to the resolved active group.
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
The adapter-owned completion action for a Source Delivery, selected from the complete success, typed failure, defect, or cancellation `Exit` of View Server mutation application. It returns an Effect whose typed failure belongs to the adapter's declared failure union and whose requirements remain visible. Following Effect's `acquireUseRelease` semantics, successful settlement preserves the application outcome, while settlement failure becomes the operational Adapter Failure; the original application Exit remains diagnostic context rather than a fabricated compound Wire Protocol error.
_Avoid_: Acknowledge before publish, swallowed commit failure, View Server transport-specific acknowledgement

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
The View Server-owned execution and health observation of a continuous Source Adapter's mandatory infallible Effect Schedule over Source Termination. The Schedule owns retry timing and exhaustion for typed failures and unexpected successful completion; View Server creates a fresh child Scope and reacquires the Source Attempt, reads Schedule metadata for health, never retries interruption, and exposes exhaustion as terminal source failure. A retrying source makes dependent Live Queries stale, terminal exhaustion makes them error, and recovery makes them ready again without closing their Subscriptions or discarding their last rows.
_Avoid_: Adapter-owned reconnect loop, transport-blind retry, retrying interruption, unobserved reconnect

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
The synchronous typed ingress transformation from decoded source values into a View Server Topic Row. First-party Mapping is a plain hot-path function: it returns its exact row-shaped result immediately and never returns Effect, Promise, Option, `undefined`, or another asynchronous or optional wrapper. Kafka `map` receives the decoded value and key, Region, derived Local Row Key, and metadata, then returns the non-ID fields; the adapter constructs `id` and validates the completed Topic Row through its Topic Schema. The Kafka schema is not exposed on `map`'s public typed input. Each Source Adapter owns its Mapping and ID-producing convenience; the common SDK requires only the resulting complete Topic Row and does not impose Kafka's Local Row Key concept on other transports. A synchronous Mapping throw becomes that adapter's exact schema-backed Mapping Failure inside a Source Item Rejection, so a still-usable source can settle the rejected item and continue. Asynchronous enrichment, service lookup, adapter-specific filtering, and effectful transformation belong upstream or in a custom Source Adapter API.
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
The operational guarantee for Kafka ingestion. During a live process, Kafka messages are decoded and mapped, grouped into short microbatches, published into Runtime Core with `publishMany`, and committed only after the relevant Runtime Core publish succeeds. Success health is recorded after commit. A failed publish leaves the original Kafka messages uncommitted. If a batch contains a bad later message after earlier messages were decoded, the decoded prefix is published and committed before the decode/mapping failure is surfaced. Kafka startup uses an explicit `startFrom` policy: `earliest`, `latest`, or a committed consumer group with fallback; health exposes the normalized consumer group, mode, and fallback mode actually used. Across process restarts, the in-memory Runtime Core has no durable row checkpoint yet, so committed consumer-group resume is not a full rebuild strategy by itself.
_Avoid_: Exactly-once claim, durable recovery, database replication

**Kafka Consumer Group Assumption**:
The current runtime starts one consumer per configured Region using the configured consumer group. It records assignments and lag for the current process, but does not yet implement a full rebalance/revoke recovery story or durable checkpoint handoff between consumers.
_Avoid_: Clustered recovery, multi-consumer partition handoff

**Publish**:
A server-side mutation that inserts or replaces a Topic Row in a View Server Topic.
_Avoid_: Browser write, send, emit

## Relationships

- A **View Server** owns one or more **View Server Topics**.
- A **View Server Topic** has exactly one canonical **Topic Row ID** declared as `id: Schema.String` in its Topic Schema.
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
- React hooks derive topic names, selected result rows, valid filter paths and operators, sort fields, group fields, aggregate fields and aliases, Feed Routes, and Source Adapter Failure unions directly from the **View Server Config** without requiring `as const`.
- `runViewServerRuntime(viewServer, options)` returns a **View Server Runtime Effect** whose requirements preserve the union inferred from every Source Definition's nominal Source Adapter Runtime Service, retry Schedule, and application dependencies.
- Application code satisfies the **View Server Runtime Effect** with aggregate adapter and platform Layers through `Effect.provide(...)` before `NodeRuntime.runMain(...)`.
- A Source Adapter never creates a hidden ManagedRuntime or calls `Effect.run*` inside reusable integration code.
- Pure View Server Config, Source Definition, and aggregate adapter Layer constructors throw named configuration errors immediately for deterministic programmer mistakes and return frozen snapshots rather than Effects or hidden running resources.
- Environment, file, and secret configuration is decoded through Effect Config or Schema, while Layer construction, resource acquisition, and source execution failures remain in typed Effect error channels.
- Runtime startup defensively revalidates every common Source Definition envelope before invoking its nominal Source Adapter Runtime Service, even though pure builders already validated it.
- Every **View Server Topic** appears once in the **View Server Config** and declares zero or one nominal **Source Definition** through the matching adapter's materialized or leased constructor.
- A materialized Source Definition rejects `routeBy`; a leased Source Definition requires a non-empty unique `routeBy` containing statically named top-level supported scalar fields from its Topic Row Schema, inferred without `as const`.
- External source names, browser-safe codecs, Mapping functions, Local Row Key functions, Start Position, Schedules, Effects, and other browser-safe Effect requirements may belong to Source Definition options; credentials, concrete client tokens, concrete clients, sockets, transport-driver packages, Node APIs, and platform Layers may not.
- A **Source Adapter Runtime Service** executes only Source Definitions created by its exact nominal Source Adapter declaration, and View Server rejects structural substitutes.
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
- The Topic-Bound Source Toolkit exposes exact nominal `upsert`, `delete`, `delivery`, and `reject` constructors plus Topic name, and performs the final runtime validation promised by their public types.
- Lifecycle factories receive no Runtime Client, publish callback, Subscriber, session, reference count, browser headers, internal Topic Store, mutable config, or raw Topic Schema-validation escape hatch.
- Materialized and leased Source Lifecycles may declare different Source Adapter Metrics Schemas, and a selected Source Definition carries only its exact lifecycle metrics type without optional fields.
- Materialized and leased Source Lifecycles each declare one exact Source Rejection Location Schema; Source Item Rejection constructors and Source Diagnostics preserve that exact type without unknown or optional location bags.
- Every source health payload contains mandatory `{ runtime, adapter }` metrics: SDK-owned runtime metrics and the exact adapter-owned lifecycle metrics.
- Mandatory **Source Runtime Metrics** contain `startedAtNanos`, `lastAttemptStartedAtNanos`, nullable `lastDeliveryAtNanos`, nullable `lastAppliedMutationAtNanos`, and nullable `lastTerminationAtNanos`, all as epoch-nanosecond `bigint` values from Effect Clock.
- Source Runtime Metrics use cumulative `bigint` values for current attempt, retry count, received deliveries, `rejectedItemCount`, attempted mutations, applied Upserts, applied Deletes, failed mutations, completed settlements, failed settlements, and Source Buffer overflow count; `lastRejectionAtNanos` is a mandatory nullable epoch-nanosecond timestamp.
- Source Runtime Metrics use `number` only for actual in-memory sizes and capacities: retained rows, Source Buffer capacity, depth, and high-water mark.
- Source Runtime Metrics contain a non-empty `lanes` tuple whose entries have stable non-empty unique IDs and exactly one `{ _tag: "Unbuffered" }` or `{ _tag: "Bounded", capacity, depth, highWaterMark, overflowCount }` buffer value; neither a lane nor its buffer is optional.
- Source Delivery Lane IDs remain stable across retries so cached health and metrics continuity do not depend on array position; the first-party Kafka adapter uses its exact region names.
- `completedSettlementCount` means the settlement Effect completed and does not infer adapter-specific acknowledge, negative-acknowledge, or commit semantics; those belong in Source Adapter Metrics.
- Source lifecycle status and exact failures remain outside metrics rather than duplicating state inside Source Runtime Metrics.
- **Source Status** is an exhaustive Schema tagged union of `Starting`, `Ready`, `Degraded`, `WaitingToRetry`, `Reacquiring`, `Exhausted`, and `Stopping`; no branch uses optional status-dependent fields.
- `Starting` is initial attempt `1n`; `Ready` begins immediately after successful Source Attempt acquisition; `Degraded` contains the latest exact settled Source Item Rejection and remains sticky for the logical source lifetime; `WaitingToRetry` contains the exact termination, next attempt, and `retryAtNanos`; `Reacquiring` contains the exact previous termination; `Exhausted` contains Source Retry Exhaustion; and `Stopping` identifies runtime shutdown or final Leased Feed release.
- **Source Target** is exactly `{ _tag: "Materialized" }` or `{ _tag: "Leased", route }`; Feed Route is never optional.
- Every source health value always contains Source Adapter Identity, Source Target, Source Status, mandatory `{ runtime, adapter }` metrics, and `sampledAtNanos`.
- Live Queries map `Ready` and `Degraded` to ready availability, `WaitingToRetry` and `Reacquiring` to stale, `Exhausted` to error, and recovery to ready while retaining their existing Subscription and rows; exact degraded state remains visible through Source Diagnostics.
- A Source Item Rejection makes its exact Source Health, affected Topic health row, and aggregate View Server health summary degraded, while Live Query availability stays ready and later valid source items continue.
- Liveness and readiness endpoints remain successful for a degraded source and return the degraded state in their payload; they do not evict or restart the View Server automatically.
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
- First-party Kafka and gRPC Source Adapters are ordinary SDK consumers exposed through `effect-view-server/kafka/contract`, `effect-view-server/kafka/server`, `effect-view-server/kafka/node`, `effect-view-server/grpc/contract`, `effect-view-server/grpc/server`, and `effect-view-server/grpc/node`.
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
- The conformance kit uses Effect scoped Layer suites and TestClock to prove readiness, ordering, settlement, retry and exhaustion, interruption, finalization, metrics, bounded buffering when present, recovery, leased sharing and cleanup, and absence of hidden runtimes.
- Source Adapter conformance includes positive and negative public type tests, exact Schema tests, nominal-linkage rejection, package exports, required peer dependencies, duplicate-bundle rejection, and browser-safety checks.
- Source Adapter package conformance rejects peer ranges broader than the versions covered by the adapter's conformance matrix.
- First-party Kafka and gRPC Source Adapters pass the same conformance kit as published third-party adapters without exceptions.
- Runtime Core and the generic runtime contain no privileged Kafka or gRPC Source Lifecycle path; transport acquisition, decoding, Mapping, settlement, metrics, and external-resource finalization live behind those first-party Source Adapter modules.
- Generic View Server Runtime options contain no `kafka`, `grpc`, or future transport-specific configuration bags.
- Invalid or tampered View Server Config, missing adapter service, missing or extra aggregate resource entries, Effect Config failure, and mandatory aggregate Layer acquisition failure are fatal Runtime Composition Failures; View Server opens no server ports.
- Operational Source Attempt acquisition, transport/framing, Stream, rejection-settlement, or Source Settlement failure enters that source's independent Source Supervision and never terminates the View Server Runtime Effect or unrelated Topics; item-local decode, Mapping, ID, Route Field, and row-validation failures instead become settled Source Item Rejections when the underlying source can continue.
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
- Successful Source Attempt acquisition marks the source ready immediately, including when its Stream has not emitted a Source Delivery; first-delivery readiness, Ready control events, and polling are invalid.
- Source Attempt acquisition failure enters Source Supervision without reporting ready; Stream failure or unexpected completion closes the attempt Scope and does the same.
- Every retry creates a fresh child Scope and reacquires a fresh Source Attempt rather than rerunning work inside a failed Scope.
- Aggregate platform Layers own only reusable shared infrastructure such as transports, pools, factories, credential refreshers, and O(1) resource maps; Source Attempt Scopes own consumers, subscriptions, channels, iterators, callback registrations, and leases.
- Source retry reacquires every attempt-level resource without rebuilding the aggregate platform Layer or disturbing unrelated Topics and Feed Routes.
- Final Leased Feed release closes only that feed's attempt-level resources; shared adapter resources remain available to other active sources until the enclosing View Server Runtime Scope closes.
- Runtime shutdown interrupts and finalizes all Source Attempts before releasing aggregate adapter Layer resources through ordinary nested Effect Scope ordering.
- A shared transport outage may terminate several dependent Source Attempts, but each source remains independently supervised by its own Source Retry Policy and health state.
- An adapter that permanently hides a source-specific subscription or consumer in its aggregate Layer is non-conformant.
- Source Attempt finalizers are infallible and idempotent, matching Effect `acquireRelease`, Stream `onExit`, and Stream `ensuring`; View Server awaits them before reacquisition.
- An external close rejection is recorded in mandatory Source Adapter Metrics and a structured log or span containing Source Adapter Identity, Feed Route when leased, and attempt number; it never becomes an untyped defect.
- Any expected failure that affects delivery correctness must occur during Source Attempt acquisition, Stream execution, or Source Settlement rather than being deferred to Scope cleanup.
- Only Topic-bound SDK constructors create nominal **Source Mutations** and **Source Deliveries**; View Server rejects raw structurally compatible objects.
- A Source Upsert constructor accepts only the exact complete Topic Row, while a Source Delete constructor accepts only the exact Topic key.
- The complete Upsert row contains its canonical Topic Row ID; the common SDK exposes neither a separate storage key nor an ID-plus-partial-row Upsert.
- A Source Adapter may offer adapter-specific Row Key and Mapping conveniences, but it assembles and Schema-validates the final complete Topic Row before constructing an Upsert.
- A tombstone or equivalent delete event may derive the Topic Row ID from transport key metadata and construct a Delete without decoding, mapping, or fabricating a row value.
- The first-party Kafka adapter names its callback `localRowKey`; it never trusts a callback to include Region in the canonical Topic Row ID.
- The Kafka adapter validates each region as a non-empty string without `:`, constructs every public Kafka-owned row key as `region:localRowKey`, and exposes matching construction and decoding helpers.
- Kafka region text and local row-key text are preserved exactly without casing, accent, or whitespace transformation; the first `:` is the canonical boundary and a local row key may contain later colons.
- Kafka applies the composite public row-key rule to every source, including one-region sources, and uses the identical composition path for Upserts and tombstone Deletes.
- Every first-party Kafka Source Definition declares one mandatory exact **Kafka Start Position**; Kafka aggregate Layer region entries do not select source offsets.
- A Kafka Source Definition selects clients with the same non-empty literal `regions` tuple that supplies its Mapping and diagnostics region union; it declares no client service token or repeated region-to-client map.
- One **Kafka Aggregate Node Layer** receives the View Server Config, a non-empty **Kafka Consumer Group Prefix**, and one exact record mapping all and only the required logical regions to concrete platform options.
- The Kafka Aggregate Node Layer provides the Kafka Source Adapter Runtime Service backed by all configured scoped clients, so ordinary Node applications require one `Effect.provide(KafkaLive)` call regardless of Kafka Topic count.
- The Kafka adapter canonically derives the active Consumer Group ID from the prefix and exact View Server Topic name, exposes that resolved ID in adapter health, uses it in every cluster selected by that Topic, and owns all commits after the initial Start Position.
- Concurrent View Server replicas that each build a complete local view use distinct Kafka Consumer Group Prefixes; one logical replica may retain its stable prefix across process restarts.
- Kafka Start Position is exactly `earliest`, `latest`, `committed`, `timestamp`, or `durationAgo`; the latter three carry a mandatory `earliest`, `latest`, or `fail` missing-offset fallback.
- `timestamp.atNanos` is a non-negative epoch-nanosecond `bigint`; `durationAgo.duration` is a finite non-negative Effect Duration input resolved with Effect Clock, never Date or `Date.now()`.
- The Kafka adapter converts the requested nanosecond boundary to Kafka's millisecond timestamp resolution and asks each selected cluster for the earliest partition offset at or after that boundary.
- `durationAgo` is evaluated once per materialized runtime lifetime or acquired Leased Feed lifetime, and the resulting initial partition offsets are frozen for that lifetime.
- A Kafka Source Attempt retry or connection recovery uses the active consumer group's latest committed offset per partition; only a partition without a commit reuses its frozen initial position and fallback.
- A complete runtime restart reevaluates a materialized source's `DurationAgo`; final Leased Feed release discards its frozen positions, so a later fresh subscription reevaluates them as well.
- For a Leased Feed, View Server alone derives internal partitioned storage identity from the exact Feed Route and Topic Row ID.
- Every **Source Delivery** contains a `Chunk.NonEmptyChunk` of Source Mutations; one mutation uses `Chunk.of(...)`, several use `Chunk.make(...)`, and an empty source poll emits no Stream element.
- A **Source No-Op Item** is an intentional heartbeat or filter miss settled by its adapter inside sequential Stream production before that source proceeds; it does not create an empty Source Delivery, Source Item Rejection, or shared Skip mutation.
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
- View Server applies each **Source Delivery** inside an Effect resource bracket and passes the application outcome to **Source Settlement** exactly once.
- A successful **Source Settlement** occurs only after every mutation in its **Source Delivery** has been applied successfully.
- Source Settlement returns an Effect whose typed failure is the adapter's declared failure type and whose Effect requirements remain visible in the View Server Runtime Effect.
- Successful settlement preserves the original mutation-application outcome; failed settlement becomes the operational Adapter Failure consumed by Source Supervision, matching Effect's `acquireUseRelease` semantics.
- When mutation application and settlement both fail, View Server retains the original application Exit in spans and logs but does not invent a compound consumer-facing failure value.
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
