# Separate leased feed routing from filtering

A Live Query for a leased topic must carry an exact `routeBy` object with one schema-admitted value for every top-level scalar Route Field declared by its source; that object selects exactly one Leased Feed, while `where` performs only local filtering inside the selected feed. Keeping routing explicit prevents Boolean filters from accidentally acquiring multiple feeds, makes missing routes impossible at typed and runtime boundaries, and preserves one source-neutral contract for gRPC and future leased-source adapters.

## Considered options

- Infer a feed route from root-level equality filters in `where`.
- Accept an array of route objects and fan out one Live Query across several feeds.
- Use an exact query `routeBy` object and reject it for topics without leased lifecycle.

## Consequences

- Every leased-topic query supplies all and only its configured Route Fields; local filters may independently mention those fields or omit them.
- Feed identity uses configured field order and exact supplied scalar values, never Text Matching or query normalization; source Adapters receive those values unchanged apart from their transport encoding.
- Rows admitted to a Leased Feed must remain congruent with its Feed Route.
- The public query contract does not expose a gRPC-specific routing API.
