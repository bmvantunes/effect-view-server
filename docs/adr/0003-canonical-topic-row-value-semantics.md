# ADR 0003: Topic Row values use canonical schema-derived semantics

## Status

Accepted on 2026-07-12. Implementation is tracked by issue #324.

## Context

A Topic Row schema promises more than a TypeScript object shape. Effect schemas can decode values such as Schema classes, `Option`, `Chunk`, `HashMap`, and `BigDecimal` whose prototypes and equality rules are part of their runtime meaning. Raw and Grouped Live Queries must preserve that meaning while keeping authoritative Topic Row and Active Query state isolated from callers.

Generic cloning cannot satisfy both requirements. A platform structured clone may detach mutable state, but it does not reconstruct arbitrary schema-decoded values through their declared codec. Plain recursive cloning preserves only JSON-like containers. Referential equality also treats separately decoded but semantically equal values as changes, while broad stringification does not provide a collision-safe group identity for prototype-bearing values.

The production Wire Protocol adds a second constraint. Effect RPC WebSocket uses NDJSON, so every encoded payload must be a real JSON tree. Effect's `Schema.Json` decoder is intentionally permissive enough to accept some JavaScript container values that `JSON.stringify` subsequently changes or erases. For example, an opaque `Map` accepted beneath `Schema.ObjectKeyword` does not have a faithful NDJSON representation. Local and remote View Servers must not disagree about whether such a Topic Row is valid.

The project therefore needs one explicit semantic contract for ownership, equivalence, canonical identity, and JSON safety.

## Decision

Each View Server Topic compiles **Topic Row Value Semantics** from its configured schema once.

1. Schema decoding remains the authority for the Topic Row type.
2. `Schema.toCodecJson` is the authority for the canonical wire representation of explicit schema values.
3. A neutral **Strict JSON Materializer** validates and copies the encoded value before it can cross an ownership or NDJSON boundary.
4. Decoding that fresh canonical JSON value through the same codec reconstructs a fresh semantic runtime value.
5. Enumerable own-field presence is part of Topic Row identity. For equally present values, `Schema.toEquivalence` is the authority for semantic equality. Separately instantiated values that the configured schema considers equal do not advance a Topic version or emit a Delta.
6. Raw and Grouped Query Plans carry **Query Result Semantics** derived from their selected fields, group fields, and aggregate results. Active Query caches may retain engine-owned values; one-shot results, Snapshots, and row-bearing Delta operations receive fresh consumer-owned values.
7. Missing optional fields remain omitted. A present field whose declared value includes `undefined` remains present and follows its canonical codec.
8. Raw equality filters canonicalize their operands through the configured field schema and use the same field equivalence. Raw ordering uses schema-compatible field comparison, and equivalent query cache keys use schema-canonical filter tokens.
9. Group identity, `countDistinct`, and ordered aggregate state use schema-canonical field tokens and schema-compatible comparison. They do not use object identity or generic object stringification.
10. Topic configuration rejects schema domains whose canonical JSON codec is not a semantic identity witness. This includes unions whose distinct runtime members can decode from the same JSON value, arbitrary custom equivalence or codec transformations without an explicit canonical/order witness, unrecognized declaration codecs, and native `ReadonlyMap`/`ReadonlySet` domains whose iteration-ordered codec conflicts with order-insensitive equality.
11. `Option`, `Chunk`, `HashMap`, and `HashSet` are admitted through the corresponding public `viewSchema` factories. `viewSchema.BigDecimal` is the admitted `Schema.BigDecimal` declaration, so either spelling is valid. A concrete Effect Schema class is admitted explicitly with `viewSchema.admitClass(Profile)`. Admission belongs to that exact declaration, is idempotent for reuse, and is independent for each concrete class. Class annotations belong in the `Schema.Class` definition before admission; a subsequently derived codec is a distinct schema and does not inherit admission.
12. Topic Row data types come from `Schema.Struct.Type<S["fields"]>` for the configured row schema `S`. A decoded class instance may retain its methods, but only declared schema fields participate in topic keys, source mappings, queries, aggregates, results, and mutations.

The Strict JSON Materializer accepts only:

- `null`, strings, booleans, and finite numbers, with negative zero normalized to zero;
- dense arrays with no extra own properties; and
- plain or null-prototype records containing enumerable own string-keyed data properties.

It recursively produces fresh arrays and records. It preserves an own `__proto__` key as data rather than invoking the prototype setter. It tracks the active recursion path so a shared acyclic input value is accepted and copied at each location, while a cycle is rejected.

It rejects values that NDJSON would silently omit, coerce, invoke, or degrade: `undefined`, naked `bigint`, symbols, functions, non-finite numbers, symbol keys, accessors, non-enumerable properties, sparse arrays, extra array properties, opaque or custom prototypes, cycles, and failed reflection through hostile proxies. Failures retain an exact value path and reason so callers can map them to the typed boundary error they own.

The Strict JSON Materializer does not inspect Schema ASTs and does not decide which semantic types are supported. Explicit schema codecs own that decision. A value beneath `Schema.Unknown` or `Schema.ObjectKeyword` is accepted only when its encoded result is already a faithful strict JSON tree.

## Group identity and custom equivalence

Canonical field tokens come from the strict JSON output of the field's canonical codec. The compiled identity plan recursively sorts encoded `HashMap` entries and `HashSet` elements by their normalized JSON tokens, including collision-node values whose iteration order depends on insertion order. Ordinary arrays and `Chunk` values remain order-sensitive. Structured tokens may be cached by object identity as a performance optimization, but the token content remains the source of group identity.

`Schema.overrideToEquivalence` can define equality that has no lawful matching hash or total order. A custom encode transformation can likewise collapse distinct decoded values into one JSON value. The current configuration Interface has no separate canonical/order witness, so arbitrary custom equivalence and unrecognized codec transformations are rejected recursively when the Topic is defined. The public `viewSchema` members provide the identity witness for the admitted Effect declarations, and `viewSchema.admitClass` provides it for one already-defined concrete Schema class. A future Interface may admit other custom behavior only together with an explicit witness whose canonical key is identical for equivalent values and whose comparison orders equivalence classes lawfully.

Likewise, a JSON codec must be injective over the admitted runtime domain. For example, `Schema.Union([Schema.Null, Schema.Undefined])` maps two distinct values to JSON `null`, while `Schema.Union([Schema.String, Schema.BigInt])` can map both `"1"` and `1n` to the same JSON string. Such unions are rejected recursively at Topic configuration time, before Raw, Grouped, gRPC, or NDJSON behavior can diverge. Disjoint unions such as string-or-undefined and tagged object unions remain supported.

Effect's `HashMap` and `HashSet` codecs preserve their runtime types but may expose collision-node insertion order in their encoded arrays. The shared schema identity Module normalizes that encoded order before deriving engine, grouped-public-key, or leased-route identity. Native `ReadonlyMap` and `ReadonlySet` are not admitted as Topic fields because their codecs preserve iteration order while their schema equivalence does not. They can be reconsidered only with an order-neutral canonical codec shared by the engine, Wire Protocol, and gRPC route-key derivation.

## Consequences

Local, in-memory, WebSocket, and NDJSON behavior share one acceptance contract. Semantic values retain their declared runtime form in Raw and Grouped results, and consumer mutation cannot reach authoritative storage or shared Active Query cursors.

Opaque local-only values beneath broad schemas are rejected with typed `InvalidRow` errors instead of being silently changed by transport serialization. This deliberately narrows that behavior and requires release intent.

Non-injective and equivalence-incongruent schema domains fail while defining the View Server configuration rather than accepting typed values that would later merge, reorder, or change across transports.

The Column Live View Engine and Wire Protocol may depend on the neutral `@effect-view-server/effect-utils` package for strict JSON materialization. The utility remains independent of View Server packages. Schema decoding, projection shape, grouping, aggregate behavior, and typed boundary errors stay in their owning Modules.

Canonical encode/decode adds work at ownership boundaries. Compiled codecs, equivalence functions, field plans, and structured canonical tokens must be reused. Performance gates must cover both Raw read/write and grouped admission/order-neutral behavior so semantic correctness cannot hide an unacceptable ingestion or subscription cost.

Issue #325 may replace caller-selected result generics and identity overloads with the runtime projection witnesses introduced here. This ADR does not preserve those overloads as compatibility APIs.
