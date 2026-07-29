# Every Topic Row has one canonical string ID

## Status

Accepted and implemented. Issues #384–#387 enforce canonical
`id: ViewServerId` for Source-owned Topics and their runtime mutation boundary
and remove the separate `key` property repository-wide. ADR 0011's accepted
target behavior supersedes only this decision's Kafka composition rule:
canonical Kafka identity becomes policy-specific: delete-only uses `region:partition:localRowKey`, while compaction-capable sources use only `region:partition:k<unpadded-base64url(serializedKeyBytes)>`.

Every user-provided Topic Schema must declare the required field `id: ViewServerId`, and Topic configuration has no separate `key` property. View Server rejects a missing, optional, transformed, refined, branded, or non-string `id` at compile time and defensively at runtime. This deliberately gives storage, queries, mutations, React rows, Wire Protocol schemas, and Source Adapters one universal identity contract. The common Source Adapter SDK requires only complete Upserts and ID-addressed Deletes; each adapter owns its ergonomic ID-producing API. Under ADR 0011's accepted target behavior Kafka composes policy-specific partition-qualified IDs and adds a reversible injective serialized-key-byte segment for compaction-capable sources; an adapter without that transport concept may require its Mapping to return the complete Topic Row directly.
