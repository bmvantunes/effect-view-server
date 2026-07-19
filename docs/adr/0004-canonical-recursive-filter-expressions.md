# Adopt canonical recursive filter expressions

Live Query filtering uses one schema-derived language: `where` is an implicit-AND array of exact Field Conditions and recursive `AND`, `OR`, or `NOT` expressions, with semantic normalization for query identity. We reject both the former field-keyed filter map and direct adoption of an external grid model because a single expressive core language gives raw and grouped queries strong field/operator typing while allowing Adapters such as AG Grid to translate their own models without owning the View Server contract.

## Considered options

- Keep the field-keyed map and add special cases for cross-field OR expressions.
- Adopt AG Grid FilterModel and Advanced Filter Model as the View Server query language.
- Require an explicit `AND` object at the root instead of making the root array a conjunction.

## Consequences

- This is an intentional breaking public and wire-contract change with no compatibility form for field-keyed filters.
- Empty generated predicates normalize to no filter, while malformed conditions remain invalid.
- Query validation, normalization, identity, and execution must agree on schema-derived scalar paths, exact complements, and wire-safe values.
- Subscription APIs take an owned query snapshot at call time; caller mutation after `subscribe` cannot alter validation, routing, identity, or execution.
- External filter models remain Adapter inputs and never add third-party discriminators to the core query language.
