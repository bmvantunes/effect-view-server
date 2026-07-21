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
A configured logical table with one schema, one row key field, and one authoritative store.
_Avoid_: Kafka topic, channel, collection

**Topic Row**:
A schema-decoded object stored in a View Server Topic.
_Avoid_: Record, document, message

**Topic Row Value Semantics**:
The schema-derived ownership, equivalence, canonical JSON representation, and ordering rules for every configured Topic Row field. The Column Live View Engine compiles these rules once per View Server Topic and reuses them at ingestion, projection, grouping, comparison, Snapshot, and Delta boundaries. Canonical identity normalizes order-insensitive persistent collections while preserving ordinary sequence order. Topic configuration rejects non-injective or unrecognized codec transformations and equality domains without a congruent canonical identity/order witness.
_Avoid_: Deep clone helper, generic object equality, JSON stringify semantics

**Row Key**:
The configured string field that uniquely identifies a Topic Row and acts as the final deterministic sort tiebreaker.
_Avoid_: Primary key when discussing external databases, id unless the configured field is actually named id

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
A live event describing readiness, staleness, closure, backpressure, or typed query/runtime failure for a Live Query.
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

**Source-Owned Topic**:
A View Server Topic whose mutations are owned by a configured source declaration such as `kafkaSource` or `grpcSource`. Direct Runtime Client mutation, TCP publish mutation, and direct reset are rejected for Source-Owned Topics.
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
An on-demand source-backed Topic Row partition identified by one Feed Route and retained while one or more Subscriptions lease it. Different Live Queries may share the same Leased Feed while applying independent local filtering.
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
The typed ingress transformation from decoded source values into a View Server Topic Row. Kafka
`map` receives the decoded value and key, Region, derived `rowKey`, and metadata, then returns the
non-key fields; the runtime combines the derived key with those fields and validates the completed
Topic Row through the containing schema. The Kafka schema is not exposed on `map`'s public typed
input. gRPC `map` receives the streamed value, the leased route when present, and the containing
Topic schema, then returns the complete Topic Row for runtime validation.
_Avoid_: Serializer, mapper when it obscures the target Topic Row contract

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
- A **View Server Topic** has exactly one configured **Row Key**.
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
- A leased source declaration defines one ordered set of **Route Fields**, while each leased-topic **Live Query** supplies one exact **Feed Route** through its `routeBy` object.
- One **Feed Route** identifies exactly one **Leased Feed**; a Live Query never fans out across several Leased Feeds.
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
