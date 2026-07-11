# ADR 0002: Kafka JSON uses an explicit canonical JSON codec

## Status

Accepted on 2026-07-11. Implementation is tracked by issue #305.

This decision issue changes no compatibility behavior.

## Context

A Kafka Source Codec turns Kafka message bytes into the typed value consumed by a Source Topic Mapping. `kafka.json(...)` is the JSON Adapter in that Module.

Today its public Interface accepts every non-`any` View Server `RowSchema`, and the returned public `KafkaJsonCodec` retains that schema in a runtime `schema` field:

```ts
value: kafka.json(KafkaTrade);
```

At runtime the Adapter parses the bytes with `JSON.parse`, derives a codec with Effect's public `Schema.toCodecJson(schema)`, and decodes the parsed value. Before deriving that codec, however, it walks the supplied schema and its AST to decide whether the schema is safe enough to use.

The current accepted set is broad. The implementation's explicit recognition tables include `BigDecimal`, `Date`, `Duration`, `Error`, `File`, `FormData`, `Json`, `MutableJson`, `RegExp`, `URL`, and `URLSearchParams`, plus the parameterized `Cause`, `CauseReason`, `Chunk`, `Exit`, `HashMap`, `HashSet`, `Option`, `ReadonlyMap`, `ReadonlySet`, `Redacted`, and `Result` declarations. Inclusion in those tables is implementation evidence, not by itself an executable compatibility guarantee.

The positive compatibility matrix directly demonstrates:

- ordinary structs, Schema classes, literals, primitives, optional fields, arrays, tuples, tuple rest elements, unions, tagged unions, recursive `Suspend` schemas, and transformations;
- JSON-safe records with supported string, number, literal, symbol, template-literal, and supported union key schemas;
- high-precision `BigInt` and `BigDecimal` values plus `Duration`, `Error`, `File`, `FormData`, `Json`, `MutableJson`, and `RegExp` declarations;
- `Chunk`, `HashMap`, `HashSet`, `Option`, `ReadonlyMap`, `ReadonlySet`, and `Redacted` declarations;
- Schema classes and custom declarations whose JSON codec resolves to a concrete supported wire shape; and
- nested combinations of the preceding schemas, including transformed high-precision values.

The current Adapter rejects declarations without a JSON codec, unsupported or ambiguous Record keys, erased suspended empty-object shapes, declarations that spoof known Effect declarations, declarations whose JSON codec produces an ambiguous object-like target, and malformed payloads. The repository's large positive and adversarial sentinel matrix is authoritative for the behavior it covers; entries present only in an implementation recognition table do not gain an executable compatibility guarantee by implication.

That snapshot is not a stable public contract. The guard combines publicly exposed Effect annotation hooks with identities, source text, and object layouts that Effect v4 beta does not promise as stable recognition mechanisms:

- annotation property names such as `toCodecJson`, `toCodec`, and `typeConstructor`;
- Schema AST tags, encoding links, declaration `run` functions, and type-parameter layout;
- declaration and parser function identity;
- `Function.prototype.toString` output;
- schema object property names and `Object.keys(schema)` ordering; and
- the object layout of built-in and parameterized Effect Schema declarations.

An Effect beta upgrade can change any of those details while preserving Effect's public Schema Interface. Duplicate Effect installations can break function identity. Bundling, transpilation, or minification can change function source text. Those changes can silently narrow or broaden the accepted schema set even when View Server source does not change.

The project therefore needs an explicit contract: either retain that compatibility cost behind one Adapter, or stop reverse-engineering schemas and make the JSON wire codec an explicit caller-owned choice.

## Options considered

### Option A: Preserve the current broad set behind a Compatibility Adapter

Keep `kafka.json(RowSchema)`. Move all private inspection into one dedicated Compatibility Adapter and keep the existing sentinel matrix.

- **Compatibility:** highest; existing callers do not migrate.
- **Type safety:** unchanged. TypeScript still cannot distinguish a schema supported by the runtime guard from one rejected later by the decoder.
- **Runtime safety:** can preserve today's defensive rejections, but every Effect upgrade must revalidate private representation assumptions.
- **Upgrade and build risk:** high. Effect beta changes, duplicate package instances, bundling, transpilation, or minification may alter identity, source text, or layout.
- **Maintenance cost:** high. The Adapter retains hundreds of lines of representation-specific inspection and a large test matrix.
- **Architecture:** better Locality than scattered checks, but the Seam remains tied to an Implementation that View Server does not own.

### Option B: Require an explicit canonical JSON codec

Make the caller pass a lazy factory for Effect's public canonical JSON codec:

```ts
value: kafka.json(() => Schema.toCodecJson(KafkaTrade));
```

The stable JSON Adapter Interface is a zero-argument factory whose return type is specifically `Schema.toCodecJson<SourceSchema>`, not a broad structural `Schema.Codec`. The returned canonical codec carries its original Row Schema in Effect's public `schema` witness, so TypeScript can infer and tie the decoded row without a second schema input. The JSON Adapter does not copy or expose that schema as a `KafkaJsonCodec` field. This also lets the Adapter represent a caught factory failure honestly: it can retain the typed error without inventing a schema value that the failed factory never returned.

The canonical codec's public encoded type is `Schema.Json`; its decoded type is the Source Topic Mapping's `value` field. It is service-free because a View Server Row Schema has `never` decoding and encoding services. The factory is lazy so the JSON Adapter can invoke it once, catch synchronous derivation failures, and preserve the existing typed `KafkaDecodeError` failure mode when the source is decoded. View Server parses bytes as JSON and decodes with the resulting codec. It does not inspect raw Schema annotations, AST internals, constructor identity, object layout, or function source.

Schemas that need a different wire representation, services, custom validation, or behavior outside that canonical Interface use the existing typed custom Adapter:

```ts
value: kafka.codec({
  name: "trade-json-v1",
  decode: decodeTradeJson,
});
```

- **Compatibility:** intentionally narrower at the `kafka.json` call site. Existing calls must make their canonical JSON choice explicit.
- **Public shape:** `KafkaJsonCodec.schema` is removed. It is unused by the current runtime and would make a failed lazy factory impossible to represent without violating the public type.
- **Type safety:** strongest. The Kafka JSON Interface requires a codec whose encoded and decoded types describe the actual wire operation; mapping inference continues from the decoded type.
- **Runtime safety:** strong. The JSON Adapter owns synchronous codec-factory failures, `JSON.parse` establishes a JSON value, and the resulting Schema codec validates and transforms it. Unsupported custom behavior has an explicit typed Adapter rather than an inferred exception.
- **Upgrade and build risk:** lowest in View Server. Effect owns the behavior of its public `Schema.toCodecJson` Module; View Server no longer depends on Effect's private representation.
- **Maintenance cost:** lowest. Stable contract tests replace most representation-specific inspection tests; custom codecs cover deliberate exceptions.
- **Architecture:** deepest Module. `KafkaCodec` and its typed decode contract remain the Kafka Source Codec Seam. `kafka.json` and `kafka.codec` are Adapter constructors. The supplied canonical Schema codec is an explicit collaborator behind the JSON Adapter Interface, giving the compatibility decision strong Locality without pretending the custom Adapter implements the Schema codec Interface.

### Option C: Support a structural or constructor whitelist

Define a smaller list such as structs, arrays, tuples, unions, records, and selected built-ins while retaining `kafka.json(RowSchema)`.

- **Compatibility:** between Options A and B.
- **Type safety:** only improves if the whitelist can also be expressed in the public type, which is difficult for composed Schema values.
- **Runtime safety:** can be conservative, but nested declarations still require traversal and classification.
- **Upgrade and build risk:** medium to high. Reliable constructor or AST recognition recreates part of the private-inspection problem.
- **Maintenance cost:** medium to high. Every supported constructor and nesting rule needs a stable classifier and adversarial tests.
- **Architecture:** a narrower Compatibility Adapter, but still a shallow contract whose meaning depends on representation knowledge.

## Decision

Choose **Option B: require an explicit canonical JSON codec**.

The accepted contract is:

1. `kafka.json(...)` accepts a lazy zero-argument factory returning exactly `Schema.toCodecJson<SourceSchema>`; a merely structural `Schema.Codec`, an `any` factory, a factory returning `any`, or a factory inferred to return `never` is not the accepted Interface.
2. The codec's encoded value is Effect's public `Schema.Json` type and its decoded value is the Source Topic Mapping's `value` field.
3. The canonical codec's public `schema` witness ties `SourceSchema` at the type level, but the JSON Adapter does not copy or expose it as a `KafkaJsonCodec.schema` field.
4. The JSON Adapter invokes the factory once and maps a synchronous derivation failure to the same typed `KafkaDecodeError` observed when that source is decoded; the failure does not escape as a configuration-time throw and no placeholder schema is fabricated.
5. View Server performs no annotation, AST, constructor, parser, object-layout, or function-source identity checks to recognize supported declarations.
6. A schema or wire format outside that contract uses `kafka.codec(...)`, which remains fully typed in both its decoded value and error channel.
7. Effect upgrades must pass focused sentinel tests for the public canonical codec Interface and its exact representative JSON wire fixtures, but View Server does not preserve private Effect declaration representations.

This decision deliberately makes the wire choice visible at configuration Locality. It does not claim that every schema accepted by today's Compatibility Adapter remains a supported `kafka.json` input.

## Migration and release implications

Ordinary callers migrate mechanically:

```ts
// Before
value: kafka.json(KafkaTrade);

// After
value: kafka.json(() => Schema.toCodecJson(KafkaTrade));
```

Callers relying on custom declarations or a non-canonical JSON representation either provide a canonical codec that satisfies the accepted Interface or move to `kafka.codec(...)` and own the byte decoding explicitly.

Code that reads the currently public `KafkaJsonCodec.schema` field must stop doing so. The field has no runtime responsibility after the canonical codec becomes caller-owned; source Mapping inference remains available from `KafkaCodecType<typeof codec>` and from the typed Mapping callback.

Issue #305 must provide:

- positive runtime and type coverage for ordinary structs, transformations, high-precision values, recursive schemas, and representative public Effect declarations through explicit canonical codecs;
- negative type coverage for passing a raw Row Schema, a canonical codec whose decoded value is not a row, a non-JSON encoded codec, a codec requiring decoding services, a codec requiring encoding services, an `any` factory, a factory returning `any`, a factory returning `never`, or an invalid Mapping result;
- type coverage proving the accepted canonical codec retains the public `Schema.toCodecJson<SourceSchema>.schema` witness and that a merely structural JSON-shaped `Schema.Codec` cannot satisfy the Interface;
- positive and negative type coverage proving `KafkaJsonCodec` no longer promises a runtime `schema` field while Mapping value inference remains exact;
- representative composition sentinels for Schema classes, optional fields, arrays, tuple rest elements, unions, tagged unions, supported records, `Suspend`, custom declarations, and the explicitly supported public Effect declarations;
- exact encoded JSON fixtures for representative primitives, transformations, high-precision values, recursive schemas, and public Effect declarations so Effect upgrades cannot change Kafka wire data silently;
- coverage proving synchronous canonical-codec derivation failures become typed `KafkaDecodeError` values rather than escaping from configuration;
- runtime coverage for malformed JSON and codec decode failures with `KafkaDecodeError` preserved;
- a documented custom-codec migration example; and
- a Changeset because the accepted public configuration Interface changes.

The migration must update the Kafka call sites in package tests, runtime tests and benchmarks, in-memory tests, package type tests, `docs/kafka-mapping.md`, package README files, active plans, `examples/kafka-react`, `examples/combined-sources-react`, and any other Kafka example. Consumer-facing examples must continue using publishable `effect-view-server` subpaths.

The release notes must call the change out as a breaking Kafka configuration change. Version selection follows the repository's release policy; this ADR does not publish a package.

Had Option A been accepted instead, no migration or Changeset would have been required solely for the decision, but #305 would have needed to isolate the present compatibility machinery behind one Adapter and retain upgrade, duplicate-installation, bundle, transpilation, and minification sentinels.

## Consequences

The Kafka Source Codec Module retains `KafkaCodec` as its small stable Seam with high Leverage. The JSON Adapter owns canonical codec construction, JSON parsing, and canonical codec decoding. The custom Adapter owns intentionally non-canonical decoding. Effect owns its public canonical codec Implementation.

The large private-representation Compatibility Adapter can be deleted. Tests move toward the stable Module Interface and domain behavior instead of Effect's internal declaration layout, which also unblocks the test-locality work in issue #306.

Some callers must edit configuration even when their existing schema already works. That cost is intentional: the encoded JSON representation becomes explicit and reviewable, and future Effect upgrades cannot silently change View Server's recognition logic through minified function text or private object layout.

Effect beta upgrades can still change the canonical JSON wire representation produced by Effect. Exact representative wire fixtures must detect that drift. An accepted wire change requires migration guidance and release intent; callers that require a versioned wire representation should own it in a named `kafka.codec(...)` Adapter.

No production behavior changes in issue #304. Implementation belongs exclusively to issue #305 after this ADR is explicitly accepted.

## Maintainer decision record

On 2026-07-11, maintainer Bruno Antunes explicitly selected Option B in the active PRD #292 implementation task. This approval unlocks issue #305; it does not authorize compatibility behavior to change in issue #304 itself.
