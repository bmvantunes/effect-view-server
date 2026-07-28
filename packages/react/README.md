# @effect-view-server/react contributor reference

> This README documents a private workspace implementation package. Application authors install
> `effect-view-server` and must not depend on `@effect-view-server/*` packages directly.

Consumer production code imports from `effect-view-server/react`. Browser tests import
`createInMemoryViewServerReact` from `effect-view-server/react/testing`.

For contributors, the private React implementation package depends on the private client, config,
and Effect utility packages plus Effect and React. Its testing subpath uses the private in-memory
Adapter as a development dependency and optional peer. The testing helper must be created from the
same `createViewServerReact(...)` binding object used by application hooks, so the test provider and
hook contexts cannot drift apart.

`useLiveQuery` exposes the canonical query Interface unchanged: `where` is an
implicit-`AND` array of typed Field Conditions or nested `AND`, `OR`, and `NOT`
expressions. The React Module must not introduce field-keyed filter shorthands or
transport-specific filter models.

`useSourceHealth(...)` is the exact Topic-bound Source Diagnostics hook. It
uses the framework-neutral scoped client subscription and Effect reactivity;
React does not poll, add adapter-specific hooks, or put health refreshes on the
Live Query event path. Materialized Topics accept only `topic`; Leased Topics
require the exact `routeBy` object. Source-free Topics are rejected.

The in-memory testing provider creates no Source Adapter Layer. It may publish
to source-free Topics, but source-owned component tests must use a remote or
controllable adapter fixture, or mock the hook boundary for presentation-only
coverage.
