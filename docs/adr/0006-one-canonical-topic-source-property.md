# ADR 0006: One canonical topic source property

## Status

Accepted.

## Context

Transport-specific `kafkaSource` and `grpcSource` properties keep ownership visible, but force configuration, public types, validation, Source Ownership Policy, and runtime composition to know every source kind. That prevents a third-party Source Adapter from joining the same source model without changing View Server itself.

Splitting one application into a browser-safe topic contract and a second server-only topic tree would keep dependencies separate, but would make users describe the same View Server topology twice. That duplication is more error-prone and materially worse than the existing single-config API.

## Decision

A View Server Topic declares zero or one canonical `source` property in the one shared `defineViewServerConfig(...)` call. The value is an SDK-branded Source Definition created by a Source Adapter's browser-safe materialized or leased constructor. Arbitrary structural lookalikes are invalid. `kafkaSource` and `grpcSource` are removed in a hard migration with no aliases or compatibility layer.

The Source Definition is the topic-owned declaration and type authority for that source. It carries Source Adapter Identity, Source Lifecycle, Route Fields when leased, exact failure and metrics Schemas, a nominal reference to the adapter's Effect runtime service, and the adapter-owned browser-safe source options needed by its runtime implementation. The Source Adapter validates and freezes those options during the single View Server Config construction. A Source Definition never contains a Runtime Client, concrete transport client, platform Layer, secret value, or imperative publish callback.

The matching server adapter package provides the nominal runtime service through an Effect Layer. View Server asks that exact service to acquire Source Attempts; it never switches on Kafka, gRPC, adapter names, or runtime string IDs. Importing a Source Adapter constructor and providing its matching Layer is build-time composition rather than runtime discovery. Source Adapter Identity remains diagnostic metadata for health, failures, spans, and logs and is not the service lookup key or a compatibility protocol.

The standard server entrypoint remains one call over the same View Server Config. Its Effect requirements are inferred from every Source Definition's adapter runtime service, Schedule requirements, and other explicit dependencies. A platform aggregate Layer may satisfy all ordinary requirements for one adapter at once. For example, the first-party Kafka Node Layer provides the Kafka runtime service backed by every required region client; `runViewServerRuntime(...)` itself remains transport-agnostic.

Materialized and leased Source Definitions are distinct. A Source Adapter may support either Source Lifecycle or both and exposes only those constructors. Leased construction requires a non-empty set of Route Fields, while materialized construction rejects `routeBy`. View Server validates the shared Source Definition envelope and the adapter runtime service validates its own nominal definition before acquisition.

## Consequences

Applications author every Topic, Topic Schema with canonical `id: Schema.String`, and Source Definition once. React and the server consume the same frozen View Server Config, so no second topic tree can drift. Adapter contract modules and every option accepted by their shared source constructors must remain browser-safe; credentials, sockets, concrete clients, Node libraries, and platform resources move to `/server` or `/node` Layers. Kafka and gRPC become ordinary first-party Source Adapters instead of privileged Runtime Core cases. Topics without `source` retain direct Runtime Client and TCP publish ingestion.
