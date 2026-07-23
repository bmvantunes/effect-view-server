# ADR 0009: Standard Source Adapter package exports

## Status

Accepted design. Implementation is pending, so the package exports specified below are planned rather than currently available.

## Context

Source Adapters can wrap broker clients, sockets, native modules, protobuf or other codecs, and environment-specific resource Layers. The shared View Server Config and React client must still import exact Source Definitions and failure Schemas in a browser. Leaving package boundaries to convention would let one transitive Node dependency break or bloat consumer browser builds.

Effect separates portable contracts and services from concrete platform implementations, including dedicated Node and browser packages. Source Adapter packages need an equally explicit boundary that an SDK test kit can verify.

## Decision

When implemented, every Source Adapter package will expose these standard public seams:

- `/contract` contains browser-safe Source Definition constructors, Source Adapter Identity metadata, Source Adapter Failure Schemas, and mandatory Source Adapter Metrics Schemas.
- `/server` contains the matching Source Adapter runtime service implementation, lifecycle factories, and transport-neutral runtime Layers.
- Platform exports such as `/node` contain concrete transport-driver Services and Layers. Every published platform export provides paired aggregate `layer(...)` and `layerConfig(...)` constructors that derive the exact required logical-client map from the View Server Config and provide the adapter runtime.

The publishable View Server package will expose the SDK through exactly three matching public modules:

- `effect-view-server/source-adapter` contains the portable declaration and Source Definition API.
- `effect-view-server/source-adapter/server` contains server-only Source Adapter runtime-service APIs and executable helpers.
- `effect-view-server/source-adapter/testing` contains the reusable conformance kit.

Adapter packages do not import internal workspace packages, `src` paths, `dist` paths, or unapproved nested SDK modules. View Server package-export checks cover all three approved modules and reject those deep alternatives.

Kafka and gRPC will prove the extension seam by becoming ordinary first-party SDK consumers. Because this repository publishes only the `effect-view-server` package, their planned standard adapter surfaces are these package subpaths:

- `effect-view-server/kafka/contract`
- `effect-view-server/kafka/server`
- `effect-view-server/kafka/node`
- `effect-view-server/grpc/contract`
- `effect-view-server/grpc/server`
- `effect-view-server/grpc/node`

Runtime Core and the generic View Server Runtime have no private Kafka or gRPC lifecycle hooks. External acquisition, protocol decoding, Mapping, settlement, adapter metrics, and transport finalization move behind the same Source Adapter Interface available to third parties. The `/node` modules provide concrete Node transport Layers; the portable contract and generic server implementation seams do not import those concrete Layers.

The hard migration removes top-level `runtimeOptions.kafka` and `runtimeOptions.grpc` completely, with no aliases. Generic runtime options retain only generic server, query-engine, admission, authentication, health, and subscription concerns. Per-source external topic or feed names, browser-safe codecs, Mapping and Row Key functions, Start Position, and consumer behavior belong to the adapter's shared Source Definition constructor in the one View Server Config. Brokers, endpoints, credentials, TLS, connection pools, and concrete transport clients belong to adapter platform Layers. Application composition supplies aggregate adapter Layers to `runViewServerRuntime(viewServer, options)` through `Effect.provide(...)` at the application edge.

Runtime composition and source availability are separate failure domains. Invalid/tampered View Server Config, a missing adapter service, missing or extra aggregate resources, `Config.unwrap(...)` failure, or failure acquiring mandatory aggregate Layer infrastructure fails the View Server Runtime Effect before transport ports open. Once composition succeeds, operational Source Attempt acquisition or execution failures remain inside that source's typed supervision, retry, and health state; they do not stop server transports or unrelated Topics.

The first-party Kafka contract uses each Source Definition's non-empty `regions` tuple as its logical client selection. Region strings are not bootstrap addresses or credential values. `kafkaNode.layer(viewServer, ...)` derives the exact union of required region names across Kafka-owned Topics and accepts one exact `regions` record mapping those names to concrete resolved platform options. `kafkaNode.layerConfig(viewServer, ...)` accepts the corresponding exact `Config.Wrap` tree. Both reject missing and extra entries; the Config-backed constructor uses Effect `Config.unwrap(...)` once during Layer construction and retains `Config.ConfigError` in its typed error channel. They provide brokers, credentials, TLS, scoped client resources, and the Kafka Source Adapter runtime service in one aggregate Layer.

The first-party gRPC contract creates its source helper from one exact record of browser-safe generated service descriptors. The helper's materialized and leased constructors return only nominal Source Definitions for the canonical Topic `source` property; they never return or own the surrounding Topic definition, Topic Schema, or canonical Topic Row ID contract. A Source Definition's `client` is constrained to the descriptor record's literal keys. After selecting a client, `method` is constrained to the exact server-streaming method names declared by that service; unary and other method shapes are invalid. The materialized `request` factory takes no route argument, while the leased factory receives the exact Route Fields object. Its return is checked recursively against the selected generated method's request-init type: extra top-level or nested fields, incompatible values, and `any` are invalid. Materialized Mapping receives only `{ value }`; leased Mapping receives only `{ value, route }`; `value` is the exact generated response-message type and the Topic Schema is never passed into the callback. Mapping is a synchronous plain-object transformation and cannot return Effect, Promise, Option, `undefined`, or another asynchronous or optional wrapper. It returns the complete Topic Row immediately. A synchronous Mapping throw, invalid mapped row, or Route Field mismatch becomes an exact schema-backed Source Item Rejection with safe gRPC location metadata; the adapter uses an infallible no-op rejection settlement and pulls the next already-decoded response while the upstream stream remains usable. A transport or protobuf framing failure that invalidates the stream remains terminal for the Source Attempt. Public type tests prove all inference and rejection behavior without `as const`.

The logical gRPC client name is not a URL, concrete client, connection, or browser concern. At runtime, the gRPC adapter uses it for O(1) lookup in the exact client options record supplied once to `grpcNode.layer(viewServer, ...)` or `grpcNode.layerConfig(viewServer, ...)`. The latter accepts Config-wrapped endpoints and transport options and preserves typed Config failure during scoped Layer construction.

First-party gRPC Source Definitions expose no user-authored `acquire` or `release` callback. The adapter invokes the selected typed client method with the exact request, converts its AsyncIterable through Effect's scoped `Stream.fromAsyncIterable(...)`, wraps transport failures in its declared exact failure Schema, and owns iterator return, optional AbortController cancellation, and all other finalization in the Source Attempt Scope. Custom acquisition behavior belongs to another Source Adapter implementation or a controllable adapter test Layer, not a callback escape hatch repeated across Topics.

A gRPC Request Factory is synchronous and returns only the selected generated method's exact request-init object; Promise, Effect, Option, `undefined`, `any`, incompatible protobuf messages, and extra nested fields are invalid. It is evaluated once per logical source lifetime and its result is snapshotted before invocation. A materialized source evaluates it when its runtime lifetime starts. A leased source evaluates it when the first subscriber creates one exact Feed Route's Leased Feed. Same-route subscribers share that request and Source Stream, and Source Attempt retries reuse the same request. A different exact Feed Route creates a different Leased Feed and evaluates the factory again; final release followed by later reacquisition does the same. Changing only local query filtering, sorting, grouping, projection, or pagination while retaining the exact Feed Route does not restart the source or reevaluate its request. A synchronous throw becomes the adapter's exact request-construction failure. Dynamic configuration, credentials, refreshed authentication, and headers belong to gRPC Layer transport configuration or interceptors rather than mutable request capture.

Remote Browser Client headers, credentials, and session identity are never forwarded automatically to gRPC or any other upstream Source Adapter. View Server authenticates and authorizes the Live Query at its own boundary. The aggregate adapter Layer and its interceptors own upstream service authentication and refresh. Because one Leased Feed is shared by exact Feed Route, capturing the first subscriber's identity would be nondeterministic and could leak data between subscribers. If caller identity genuinely changes the upstream dataset, it must be represented explicitly in an exact Route Field and authorized by View Server; it is never a hidden request or Mapping input.

First-party Kafka Mapping follows the same synchronous boundary: it returns the exact non-ID row fields immediately and cannot return Effect, Promise, Option, `undefined`, or another asynchronous or optional wrapper. The Kafka adapter constructs the canonical ID and validates the complete Topic Row. An item-local key/value decode, Mapping, canonical-ID, or Topic Schema failure becomes an exact schema-backed Source Item Rejection containing safe region, external topic, partition, offset, phase, and message context but never the raw key or value. View Server records sticky Degraded health and then Kafka commits that rejected offset before pulling the next record. Successful commit continues the lane so one poison record cannot pin a partition; commit failure terminates the Source Attempt and enters retry. Effectful or asynchronous enrichment belongs upstream or in a custom Source Adapter rather than in the first-party per-message hot path.

A Kafka Source Definition accepts a non-empty `regions` tuple. Its literal values become the exact `region` union supplied to codecs, Local Row Key, Mapping, failures, and adapter metrics without `as const`; the aggregate Layer resolves each value through its one exact region record in O(1). One Kafka Source Attempt acquires all selected regions before reporting ready and rolls back earlier acquisitions if a later acquisition fails. Each region supplies an independent Source Delivery Lane: delivery and settlement stay sequential within a region, regions execute concurrently, and termination of one region terminates the complete attempt and reacquires all regions through the one Source Retry Policy. This preserves the current multi-cluster single-topic behavior and concurrency without a transport-specific Runtime Core path.

Kafka does not rely on application code to avoid cross-region Topic Row ID collisions. Its shared source constructor names the callback `localRowKey`, and that callback returns only the transport-local string derived from the decoded Kafka key or metadata. The adapter validates every region identifier as non-empty and free of `:`, then constructs and injects the canonical Topic Row ID as `region:localRowKey`. It preserves both strings exactly without case, accent, or whitespace normalization. A local key may itself contain colons because the first colon is the canonical boundary. Public `kafka.rowId(...)` construction and decoding helpers own this stable representation so consumers do not hand-assemble it.

The composite rule applies even when a Kafka source selects one region. The Mapping omits `id`; the adapter injects the complete composite into that canonical field before Topic Schema validation and Source Upsert construction. A tombstone decodes its transport key, invokes `localRowKey`, composes the same region-qualified Topic Row ID, and emits Delete without decoding or mapping its null value. Conformance tests prove distinct regions cannot collide for identical local keys, colons in local keys roundtrip, invalid region identifiers fail pure construction, and Upsert/Delete use byte-for-byte identical ID composition.

Every Kafka Source Definition also declares a mandatory exact start-position policy; no client Layer or adapter default chooses offsets:

```ts
type KafkaStartPosition =
  | "earliest"
  | "latest"
  | {
      readonly mode: "committed";
      readonly consumerGroupId: string;
      readonly fallback: "earliest" | "latest" | "fail";
    }
  | {
      readonly mode: "timestamp";
      readonly atNanos: bigint;
      readonly fallback: "earliest" | "latest" | "fail";
    }
  | {
      readonly mode: "durationAgo";
      readonly duration: Duration.Input;
      readonly fallback: "earliest" | "latest" | "fail";
    };
```

`Timestamp.atNanos` is a non-negative epoch-nanosecond `bigint`. `DurationAgo.duration` is a finite non-negative Effect Duration input evaluated through Effect Clock, never Date or `Date.now()`. The adapter converts the requested boundary to Kafka's millisecond timestamp resolution and uses the driver's timestamp offset lookup to select the earliest available record at or after that boundary in every partition. `Committed`, `Timestamp`, and `DurationAgo` require an explicit per-partition missing-offset fallback; invalid, infinite, negative, or incomplete policies fail construction. The active source's derived Consumer Group ID remains separate from the `Committed` branch's literal seed group whose offsets are used as the initial position.

Application code supplies one explicit non-empty `consumerGroupPrefix` to the aggregate `kafkaNode.layer(viewServer, ...)`, rather than supplying a misleading final `consumerGroupId` or repeating the prefix in every Source Definition. The Layer represents one logical Kafka-consuming View Server replica and provides the Kafka Source Adapter runtime service backed by its mandatory exact region record. Pure Layer construction validates and snapshots deterministic options; Effect Config may resolve dynamic deployment input through the Layer's typed construction channel.

The adapter deterministically derives each active Kafka Consumer Group ID from `(consumerGroupPrefix, exact View Server Topic name)` when the runtime service binds a Source Definition to its containing Topic. It canonically UTF-8 percent-encodes each component independently with uppercase hexadecimal escapes and joins the components with one literal `:`, so ordinary inputs remain readable while embedded separators, percent signs, Unicode, and other punctuation cannot create ambiguous or colliding IDs. Prefix `my-view-server` and Topic `orders` therefore resolve to `my-view-server:orders`. Runtime binding rejects an empty prefix or a derived ID that violates the Kafka driver's accepted group-ID constraints before Source Attempt acquisition.

The prefix identifies one logical View Server replica. Every concurrently running replica that builds its own complete local Topic Stores must use a distinct prefix because Kafka consumer groups distribute partitions rather than broadcast every partition to every member. Sharing one resolved group across live replicas would give each replica only a partial view. A logical replica may preserve its stable prefix across process restarts; the selected Kafka Start Position still follows the separately specified new-lifetime semantics. Cross-process uniqueness is an explicit deployment invariant rather than a claim the local runtime can prove.

The prefix is never optional, inherited as a hidden default, read from generic runtime options, repeated per source, or selected by an individual Kafka region entry. The resolved active ID is mandatory exact Kafka adapter health data so operators can identify collisions across replicas. It applies independently in every cluster selected by that Topic and owns all commits produced by successful Source Settlements. A `committed` start-position branch may read initial offsets from another literal named group, but after initial positioning every commit and reconnect belongs to the Topic's resolved active group. Conformance tests prove canonical encoding, derivation stability, cross-topic separation under one prefix, invalid-input rejection, identical IDs across the Topic's selected clusters, and health exposure.

Relative start positioning is scoped to one logical source lifetime, not one retry attempt. At materialized runtime start or first acquisition of a Leased Feed, the adapter reads Effect Clock once, resolves `DurationAgo`, and freezes the resulting initial offsets for every known cluster partition. `Timestamp`, `Earliest`, `Latest`, and `Committed` initial resolutions are frozen at the same boundary. If one Source Delivery Lane loses its connection or any other failure causes Source Attempt reacquisition, the adapter first resumes each partition from the active source consumer group's latest successfully committed offset. Only a partition without an active-group commit reuses its frozen initial offset and explicit fallback. It never recalculates a moving relative window during retry, so a long outage cannot silently skip the early part of that outage.

A complete process/runtime restart creates a new materialized source lifetime and deliberately reevaluates `DurationAgo`; requesting five minutes therefore rebuilds from the latest five-minute window on every restart even when an earlier process used the same group. A Leased Feed retains its frozen offsets across retries while subscribers remain. Final subscriber release destroys that logical feed lifetime, and a later subscription creates a fresh feed and reevaluates the policy. Kafka integration tests cover reconnect after partial progress, commit failure replay, outage longer than the relative duration, process restart, leased retry, and leased release followed by reacquisition.

A published Source Adapter declares `effect-view-server` plus every Effect ecosystem package used by its public or runtime surfaces as peer dependencies and repeats them as development dependencies only for its own build and tests. An adapter using `effect`, `@effect/platform-node`, or another Effect integration therefore declares each package it actually uses, while an adapter that does not expose React values does not force `@effect/atom-react` onto consumers. It never bundles private runtime copies of those packages. This follows Effect's own integration-package model, where platform and transport packages peer-depend on `effect`, and preserves one shared set of Effect and Source Adapter SDK runtime identities in the consuming application.

While Effect remains beta or View Server remains pre-1.0, those peer versions are exact. Once both are stable, an adapter may publish a wider compatible peer range only when its CI conformance matrix executes the complete kit against every admitted View Server and Effect version combination. Package conformance rejects a range broader than the tested matrix. Peer dependency resolution and tested public API compatibility are the compatibility contract; the SDK does not add a tautological runtime protocol field.

The `/contract` dependency graph may use Effect and the browser-safe Source Adapter SDK surface. It may not resolve Node APIs, `/server`, Source Adapter runtime implementations, transport-driver packages, concrete clients, credentials, sockets, or platform-specific Layers. Its materialized and leased Source Definition constructors accept the adapter's complete browser-safe per-source options; leased additionally requires the non-empty unique Route Fields needed by the one View Server Config. It must declare Schema-backed Source Adapter Metrics that every source health payload contains; optional details bags and adapters without metrics are invalid. External source names, Schemas, platform-neutral codecs, Mapping functions, Local Row Key functions, Start Position, and Schedule overrides may live in `/contract`; transport implementation and platform resources may not. These runtime contract values may contribute to the consumer's browser bundle. V1 accepts that explicit tradeoff to preserve one authored View Server Config and adds no mirrored browser contract, code generation, custom build transform, or automatic projection. Conformance builds a real browser fixture and enforces a documented bundle-size budget for every adapter contract.

`SourceAdapter.make(...)` creates one opaque browser-safe Effect `Context.Service` tag as part of the nominal adapter handle. This follows Effect v4's service model while preventing adapter authors from manually duplicating a tag ID, service interface, failure type, metrics type, or lifecycle declaration. The tag supplies type and runtime linkage only; `/contract` contains no runtime service implementation, concrete transport, client, or Layer. `SourceAdapterServer.make(adapter, implementation)` accepts that exact handle and can implement only its declared lifecycles, failures, metrics, and runtime service contract.

Every published platform export uses the mandatory standard Effect constructor pair: `layer(viewServer, resolvedOptions)` and `layerConfig(viewServer, configWrappedOptions)`. Both infer all and only the adapter-wide resources required by the supplied View Server Config, reject missing and extra entries through public types and runtime validation, and return one aggregate scoped Layer that provides the adapter runtime service plus its concrete clients/resources. The Config variant accepts exact `Config.Wrap<Options>`, invokes `Config.unwrap(...)` once during Layer construction, preserves `Config.ConfigError`, and leaves every other Effect service requirement visible in its Layer environment. Package conformance rejects a platform adapter that omits either constructor or substitutes an adapter-specific startup function, hidden Runtime, or internal `Effect.run*` call.

An adapter may define browser-safe logical resource references in its Source Definition options, using its own domain vocabulary such as `connection`, `cluster`, or `endpoint`. These are literal names only, never URLs, credentials, concrete clients, or per-resource Effect service tags. There is no duplicated adapter registration tree in `defineViewServerConfig(...)`. The aggregate platform Layer derives the exact required literal union from all matching Source Definitions, accepts all and only those runtime resource entries, and builds one O(1) lookup map during Layer acquisition. An adapter requiring no named external resource declares none rather than fabricating a singleton reference. The SDK standardizes inference and validation behavior without forcing one resource property name across transports.

The aggregate platform Layer owns reusable infrastructure only: transports, connection pools, factories, credential refreshers, and exact resource maps. Source-specific consumers, subscriptions, channels, iterators, callback registrations, and leases are attempt-level resources acquired with Effect's scoped primitives inside each View Server-owned Source Attempt child Scope. A source retry reacquires those resources without rebuilding the aggregate Layer. Final Leased Feed release cannot close shared infrastructure used by another topic or route; runtime shutdown closes child attempts before outer adapter resources through nested Scope ownership. A shared resource outage may fail several Source Streams, but each one remains independently supervised by its own retry Schedule and health. Package conformance rejects an adapter that permanently hides a source-specific subscription in its aggregate Layer.

Materialized and Leased are the only SDK Source Lifecycle primitives, but the SDK does not force every adapter to publish methods literally named `.materialized(...)` and `.leased(...)`. An adapter may expose domain-appropriate browser-safe wrappers such as Kafka `source(...)`, gRPC `materialized(...)` and `leased(...)`, or another transport-specific constructor name. Each wrapper must create exactly one nominal SDK Materialized or Leased Source Definition, and conformance proves the recorded lifecycle independently of public method spelling. Adapter-specific naming cannot introduce a third lifecycle.

The Source Adapter SDK provides a mandatory conformance kit, not only reusable shape assertions. For every supported Source Lifecycle, the adapter's own tests supply a controllable transport Layer that can acquire a Source Attempt, emit valid deliveries, fail acquisition or Stream execution with the exact adapter failure, complete unexpectedly, expose local metrics changes, and observe scoped finalization. A leased lifecycle's driver also makes exact-route acquisition, same-route sharing, distinct-route isolation, and final release observable. The kit uses `@effect/vitest` Layer-scoped suites, scoped Effects, and TestClock rather than hidden ManagedRuntimes or wall-clock sleeps.

The behavioral suite verifies acquisition readiness, sequential delivery and mutation ordering within a lane, concurrent sibling lanes, whole-attempt termination when one lane fails or completes, settlement on every application Exit, item-local Source Item Rejection recording and continuation, sticky Degraded health, rejection-settlement failure, retry timing and exhaustion, recovery, interruption, awaited idempotent finalization, mandatory metrics and invalid-metrics termination, bounded-buffer behavior when the adapter uses a callback bridge, and leased route sharing and cleanup. It rejects empty lane collections, empty or duplicate lane IDs, IDs that change across retry, missing per-lane buffer metrics, raw payloads in rejection diagnostics, and Source Item Rejections that lack exact safe location. Package conformance separately verifies export presence, nominal Source Definition/runtime-service linkage and lookalike rejection, exact failure, metrics, and rejection-location Schemas, positive and negative public type inference, required peer dependencies, forbidden dependency resolution, duplicate bundled SDK or Effect code, and browser bundling. Platform conformance verifies aggregate Layers reject empty, missing, extra, or duplicate logical-client entries and satisfy their exact runtime services. A published adapter is not conformant merely because its TypeScript values structurally fit. First-party Kafka and gRPC modules pass the same complete kit required of third-party adapters and may not use a privileged internal Runtime Core path.

Adapter authors declare the portable contract once:

```ts
export const rabbitMq = SourceAdapter.make({
  name: "@company/rabbitmq-adapter",
  version: "1.0.0",
  failure: RabbitMqFailure,
  lifecycles: {
    materialized: {
      metrics: RabbitMqMaterializedMetrics,
      rejectionLocation: RabbitMqMaterializedRejectionLocation,
    },
    leased: {
      metrics: RabbitMqLeasedMetrics,
      rejectionLocation: RabbitMqLeasedRejectionLocation,
    },
  },
});
```

The `/server` export implements that exact declaration:

```ts
export const rabbitMqServer = SourceAdapterServer.make(rabbitMq, {
  materialized: {
    retry: materializedRetry,
    make: makeMaterialized,
  },
  leased: {
    retry: leasedRetry,
    make: makeLeased,
  },
});
```

Source Adapter Identity and the complete Source Adapter Failure Schema are never repeated. Each supported Source Lifecycle is declared by its mandatory Source Adapter Metrics Schema and mandatory Source Rejection Location Schema rather than a boolean, and the contract exposes only those materialized or leased Source Definition constructors. `SourceAdapterServer.make(...)` requires every declared lifecycle factory, an infallible local metrics Effect matching the lifecycle Schema, and one default infallible Source Retry Policy for each; it rejects undeclared factories and preserves each factory's input, output, failure, exact lifecycle metrics, exact rejection location, metrics Effect requirements, Schedule environment, and other Effect requirements without linking casts. Shared source constructors accept a standard optional retry override and store an exact `UseAdapterDefault` or `Override` selection rather than `undefined`. View Server supplies no global retry policy.

Every lifecycle `make` factory receives one narrow exact input: the frozen adapter-specific Source Definition options, the Source Target (`Materialized` or `Leased` with exact Feed Route), and a Topic-Bound Source Toolkit. That toolkit exposes exact Topic name and nominal `upsert`, `delete`, `delivery`, and `reject` constructors. Those constructors preserve and runtime-validate Topic Row, canonical ID, adapter failure, rejection-location, route, and settlement types. The factory receives no Runtime Client, imperative publish callback, Subscriber, session, reference count, browser headers, internal Topic Store, mutable config, or raw Schema-validation bypass. The adapter's external codecs own transport decoding; the toolkit owns the final View Server Topic boundary.

The Source Definition options consumed by server lifecycle factories are executable typed APIs rather than one schema-decoded JSON object. Each factory receives the exact frozen Topic binding and adapter-owned browser-safe options, then returns a scoped Effect whose successful acquisition yields the continuous Source Delivery Stream for one Source Attempt; successful acquisition is readiness, and both acquisition and Stream failures retain the declared adapter error type and Effect requirements. An adapter may use Effect Schema or Effect Config for serializable option subtrees, while functions, codecs, Schedules, Effects, and service references remain exact TypeScript values with adapter-owned construction validation. Failure unions and all consumer-visible or wire-visible data remain schema-backed. The SDK snapshots and validates the common Source Definition envelope before the runtime service executes it.

Source Adapter declaration, Source Definition, and Layer constructors are pure. Deterministic static mistakes throw a named adapter configuration error immediately, and successful construction returns a frozen snapshot. Dynamic configuration decoding, resource acquisition, and execution failures are not converted into constructor throws; they remain typed Effect failures in Config, Layer, or Source Stream channels. View Server revalidates the shared Source Definition envelope when the runtime Effect starts as a defense against hostile JavaScript input or post-construction tampering.

## Consequences

React imports stay transport-agnostic while preserving exact failure and per-lifecycle metrics decoding from the same View Server Config used by the server. Ordinary Live Query APIs expose transport-neutral data and availability status; an explicit Source Diagnostics API exposes the exact adapter health type without adding metrics to every Snapshot or Delta. Every source health value contains mandatory `{ runtime, adapter }` metrics, with no optional details or lifecycle-wide optional-field union. Metric-only diagnostics publish at the SDK's fixed one-second Effect Clock cadence, while lifecycle transitions and item rejections publish immediately; adapters cannot introduce per-source polling behavior. Adapter authors get a predictable package vocabulary, declare identity and failures once, and can publish several platform Layer implementations without changing their shared Source Definitions. Lifecycle or metrics drift between `/contract` and `/server` becomes a construction error. An adapter that mixes its broker client into `/contract` is invalid even if a particular bundler happens to tree-shake the dependency.
