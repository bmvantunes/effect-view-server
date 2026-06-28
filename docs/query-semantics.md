# Query Semantics

## Raw Queries

Raw queries return selected rows from one topic. `select` is required.

```ts
useLiveQuery("orders", {
  select: ["id", "price", "status"],
  where: {
    status: { eq: "open" },
    customerId: { startsWith: "customer-" },
    price: { gte: 10, lt: 100 },
  },
  orderBy: [{ field: "price", direction: "desc" }],
  offset: 0,
  limit: 20,
});
```

Supported filter operators:

- strings: `eq`, `neq`, `in`, `startsWith`
- numbers, bigint, BigDecimal: `eq`, `neq`, `in`, `gt`, `gte`, `lt`, `lte`
- booleans and other equality-only fields: `eq`, `neq`, `in`

Raw `orderBy` entries reference topic row fields only.

## Grouped Queries

Grouped queries require non-empty `groupBy` and an aggregate object. `select` is
not allowed.

```ts
useLiveQuery("orders", {
  groupBy: ["status"],
  aggregates: {
    rowCount: { aggFunc: "count" },
    totalPrice: { aggFunc: "sum", field: "price" },
    distinctCustomers: { aggFunc: "countDistinct", field: "customerId" },
    maxUpdatedAt: { aggFunc: "max", field: "updatedAt" },
  },
  orderBy: [
    { aggregate: "totalPrice", direction: "desc" },
    { field: "status", direction: "asc" },
  ],
  limit: 10,
});
```

Supported aggregates:

- `count`
- `countDistinct`
- `sum`
- `min`
- `max`
- `avg`

Aggregate result precision:

- `count` and `countDistinct` return `bigint`.
- `sum` over `bigint` returns `bigint`.
- `sum` over number/BigDecimal-compatible values returns Effect `BigDecimal`.
- `avg` returns Effect `BigDecimal`.
- `min` and `max` return the source field type.

Grouped `orderBy` entries may reference group fields through `field` or
aggregate aliases through `aggregate`. An order entry must not use both.

## Live Results

Snapshots and deltas always include `totalRows`. Empty result sets report
`totalRows: 0`.

Rows are ordered deterministically. When user-provided sort fields tie, the
configured topic key is used as the final stable tie-breaker.
