# Every Topic Row has one canonical string ID

## Status

Accepted. Issue #384 enforces canonical `id: Schema.String` for generic
Source-owned Topics and their runtime mutation boundary. Repository-wide
removal of the separate `key` property remains part of the final migration in
issue #387.

Every user-provided Topic Schema must declare the required field `id: Schema.String`, and Topic configuration has no separate `key` property. View Server rejects a missing, optional, transformed, refined, branded, or non-string `id` at compile time and defensively at runtime. This deliberately gives storage, queries, mutations, React rows, Wire Protocol schemas, and Source Adapters one universal identity contract. The common Source Adapter SDK requires only complete Upserts and ID-addressed Deletes; each adapter owns its ergonomic ID-producing API. Kafka composes `region:localRowKey`, while an adapter without that transport concept may require its Mapping to return the complete Topic Row directly.
