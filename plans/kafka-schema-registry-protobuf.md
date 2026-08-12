# Kafka Schema Registry Protobuf

## Goal

Add opt-in Confluent Schema Registry decoding to the Kafka Source Adapter without weakening its
generated TypeScript types or moving dynamic Protobuf work onto the record hot path.

## Public contract

- `kafka.schemaRegistry.protobuf(MessageSchema)` accepts one Buf-generated `DescMessage`.
- The returned codec is valid for a Kafka value, an ordinary key, or a compaction key.
- Keys and values opt in independently. Configuring a Registry resource never changes another codec.
- A Kafka Node Region accepts one optional `schemaRegistry` resource configuration. Every Region used
  by a registry-backed Source must provide it.
- Version one uses Confluent's default Topic Name Strategy only: `<source-topic>-key` and
  `<source-topic>-value`.
- There is no dynamic fallback, descriptor-free decoding, Avro, JSON Schema, subject override,
  Record Name Strategy, or Topic Record Name Strategy.

## Runtime boundary

- Platformatic Kafka stays responsible for broker transport and returns raw key/value bytes.
- The Kafka Node module owns one scoped Registry client, cache, and drift monitor per Region.
- Registry schemas are requested with Protobuf `format=serialized`, decoded as base64
  `FileDescriptorProto` values, and assembled into descriptor graphs with `@bufbuild/protobuf`.
- Record decoding always uses Buf's generated descriptor with `fromBinary`; protobufjs is never a
  record deserializer.
- No dynamic Protobuf runtime or `.proto` text parser is required, including on control-plane paths.
- The application never registers, updates, deletes, or changes compatibility settings in Schema
  Registry.

## Startup contract validation

Before the Kafka aggregate Layer is provided, validate every configured registry-backed key/value
subject and all recursively reachable reference subjects:

1. Resolve the effective compatibility policy and require `FULL_TRANSITIVE`.
2. Require every active schema to be `PROTOBUF`.
3. Load every active version, recursively resolve references, and reject soft-deleted versions.
4. Reject detectable version gaps because hard-deleted live schema history is unsupported.
5. Require the Buf-generated descriptor to be mutually wire-compatible with at least one active
   registered version (the anchor).
6. Require the generated code to read every active version's recursively reachable message graph;
   only IDs satisfying that directional consumer check enter the runtime cache.
7. Verify the complete active history with Buf `WIRE` semantics. A stricter producer-side Buf `FILE`
   policy is welcome but not required by this consumer.
8. Treat canonical `google/protobuf/*` well-known types as Buf-owned descriptors rather than requiring
   Registry subjects for them.

Startup validation failures are structured `KafkaSchemaRegistryContractValidationFailure` Layer
errors. Runtime transport does not start and no Kafka consumer is acquired.

## Compatibility semantics

Implement the Buf `WIRE` rules over descriptor models, including field number reservation, enum
number reservation, preserved reservations, compatible scalar families, cardinality, oneof
membership, defaults, required fields, package identity, and recursively referenced message/enum
types. Apply field compatibility rules to extension fields and RPC rules across file moves, matching
Buf's graph-wide field/service pairing.

Consequences include:

- adding a fresh-tag field is allowed;
- deleting a field while reserving its number is allowed;
- deleting a field without reserving its number is rejected;
- deleting a reservation is rejected;
- renaming an enum value while retaining its number is binary-wire compatible;
- reserving the deleted field name is recommended for JSON safety but is not required for this
  binary-only contract.

## Wire decoding

- Accept Confluent payload-prefix framing version `0` only.
- Decode the big-endian schema ID and the Protobuf message-index vector, including Confluent's compact
  `[0]` representation.
- Resolve the selected message from the registered top-level schema and require it to match the
  configured Buf descriptor contract.
- Cache validated schema IDs per Region and subject role.
- An unknown/deleted/incompatible schema ID terminates the affected Source attempt before Mapping,
  application settlement, or offset commit. A valid ID whose payload is malformed is an ordinary
  key/value decode rejection: it cannot map or mutate rows and follows the adapter's normal rejected
  record settlement policy.

## Drift monitoring

One deduplicated monitor per Region periodically rechecks effective policies, subject/reference
versions, deletions/gaps, and compatibility. Compatible additions warm the schema-ID cache. A
violation fails only Source lanes whose key/value contracts depend on the violated subject. Source
supervision controls `WaitingToRetry` and eventual `Exhausted` status.

The reporting module does not perform probes. It projects the Kafka module's typed failures.

## Runtime reporting

- Keep heartbeat lifecycle status unchanged (`WaitingToRetry`, `Exhausted`, and so on).
- Schema Registry failures produce `problems: ["dependency"]`.
- Report Kafka brokers and Schema Registry as separate dependency identities for each Region.
- Extend dependency snapshots with stable operator issues containing the affected View Server Source,
  failure code, detailed message, and ordered string attributes.
- The full typed `KafkaSchemaRegistry...Failure` remains in Source Diagnostics; the dependency issue is
  its server-local operational projection.

Example projection:

```ts
{
  dependency: "schema-registry",
  target: "eu-west-1",
  endpoints: ["https://schema-registry.eu.example.com"],
  status: "Exhausted",
  issues: [
    {
      source: "orders",
      code: "KafkaSchemaRegistrySchemaMismatch",
      message: "...",
      attributes: [
        { name: "subject", value: "orders-value" },
        { name: "side", value: "value" }
      ]
    }
  ]
}
```

## Verification

- Type tests prove exact Buf message inference, ordinary-key and compaction-key use, independent
  key/value opt-in, and rejection of `any`, `unknown`, unions, dynamic schemas, and extra options.
- Frame tests cover malformed prefixes, schema IDs, message-index vectors, nested messages, and Buf
  decoding.
- Compatibility tests cover all supported Buf `WIRE` rules, recursive imports, well-known types,
  anchors, policy inheritance, deletions, and version gaps.
- Node tests use a fake HTTP Registry to cover auth, TLS-facing option validation, retries, caching,
  startup failure, first-seen IDs, monitor recovery/failure, and scoped cleanup.
- Kafka server tests prove no Mapping or commit occurs after Registry failure and unrelated lanes keep
  running.
- reporting tests prove separate broker/Registry dependency states and detailed issue recovery.
- Run focused tests, Kafka/runtime package tests with 100% coverage, `vp check`, strict Effect
  diagnostics, and the canonical Kafka benchmark gate.
