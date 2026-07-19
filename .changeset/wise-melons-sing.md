---
"effect-view-server": major
---

Replace field-keyed `where` clauses with strongly typed recursive filter expressions. Queries now use a root AND array containing field conditions or nested `AND`, `OR`, and `NOT` groups, which makes filter intent explicit and supports advanced boolean expressions without ambiguity.

Leased topics now require a separate, exact `routeBy` object containing every configured route field. Routing values are snapshotted at subscription time and sent through the leased gRPC lifecycle without case folding, accent folding, trimming, or schema transformations.

Consumers must migrate field-keyed filters such as `where: { name: "Bruno" }` to conditions such as `where: [{ field: "name", type: "equals", filter: "Bruno" }]`. Consumers of leased topics must also provide `routeBy` independently of `where`.
