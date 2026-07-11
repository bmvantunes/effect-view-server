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
