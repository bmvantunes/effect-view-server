# ADR 0013: Generated Protobuf Schema Registry decoding

## Status

Accepted and implemented.

## Context

Kafka keys and values may carry Confluent Schema Registry framing, but dynamic Protobuf decoding erases
the configured TypeScript message type and adds reflection work to every record. Platformatic Kafka's
experimental Registry integration dynamically decodes Protobuf and does not provide the startup
subject-history validation required by the View Server fail-early contract.

## Decision

Schema Registry decoding is an explicit Kafka key/value codec backed by a Buf-generated message
descriptor. Each Kafka Region owns at most one scoped Registry resource shared by all Sources in that
Region. Platformatic Kafka delivers raw bytes; the Kafka module validates Confluent framing and schema
history, then Buf decodes the payload.

Every used subject and custom reference subject must resolve to effective `FULL_TRANSITIVE` policy.
The generated descriptor must have one active registered mutually wire-compatible anchor, every
active version's complete reachable message graph must be safely readable by the generated code, and
active history is validated using Buf `WIRE` semantics. The application reads Registry state but
never mutates it.

Startup performs complete validation before consumer acquisition. Runtime first-seen schema IDs are
validated synchronously before Mapping, application settlement, or commit. A validated ID with a
malformed payload follows the ordinary item-rejection settlement path without Mapping or mutation.
One Region-shared monitor detects later policy or schema drift. Kafka publishes typed Source
failures; generic runtime reporting projects them onto a separate `schema-registry` dependency target
without learning Kafka-specific failure tags.

Drift failure is isolated to Sources whose contracts depend on it. A multi-Region Source remains one
Source Attempt ownership unit: failure of one Region lane terminates and reacquires that Source's
complete attempt, while unrelated Sources continue.

## Consequences

Record decoding stays statically typed and avoids dynamic reflection. Registry parsing and
compatibility work occur only on control-plane paths. Confluent's serialized Protobuf response gives
the module a base64 `FileDescriptorProto`, so both control-plane validation and record decoding use
`@bufbuild/protobuf`; no dynamic Protobuf runtime is needed. Keys and values can opt in independently,
and a single registry codec also satisfies compaction-key decoding. Version one intentionally supports
only Protobuf payload-prefix framing and default Topic Name Strategy.
