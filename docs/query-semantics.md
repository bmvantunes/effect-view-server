# Query Semantics

## Raw Queries

Raw queries return selected rows from one topic. `select` is required.

```ts
useLiveQuery("orders", {
  select: ["id", "price", "status"],
  where: [
    { field: "status", type: "equals", filter: "open" },
    { field: "customerId", type: "startsWith", filter: "customer-" },
    { field: "price", type: "inRange", filter: 10, filterTo: 100 },
  ],
  orderBy: [{ field: "price", direction: "desc" }],
  offset: 0,
  limit: 20,
});
```

The root `where` array is an implicit `AND`. Recursive groups make cross-field
Boolean expressions explicit:

```ts
where: [
  { field: "country", type: "contains", filter: "united" },
  {
    type: "OR",
    conditions: [
      { field: "age", type: "greaterThan", filter: 23 },
      { field: "sport", type: "endsWith", filter: "ing" },
    ],
  },
  {
    type: "NOT",
    condition: { field: "status", type: "equals", filter: "closed" },
  },
];
```

Supported conditions are derived from the Topic Row schema:

- strings: `equals`, `notEqual`, `in`, `contains`, `notContains`,
  `startsWith`, `endsWith`, `blank`, `notBlank`
- number, bigint, and BigDecimal: `equals`, `notEqual`, `in`, `greaterThan`,
  `greaterThanOrEqual`, `lessThan`, `lessThanOrEqual`, `inRange`, `blank`,
  `notBlank`
- booleans and other scalar fields: `equals`, `notEqual`, `in`, `blank`,
  `notBlank`

`inRange` is half-open: `filter <= value < filterTo`. Text comparison is
case-insensitive and accent-insensitive by default, so `Résumé` matches
`resume`. Add `caseSensitive: true` and/or `accentSensitive: true` to a text
condition to opt into exact handling for that dimension. `blank` means `""`,
`null`, `undefined`, a missing leaf field, or a missing intermediate field in a
dot path; `notBlank` is its exact complement.

BigDecimal operands must have an injective round trip through Effect's
BigDecimal JSON codec. Values whose formatted exponent loses scale precision
are rejected before query identity or wire encoding.

Statically named nested scalar fields use dot paths such as
`profile.country`. Structured objects, arrays, maps, sets, dynamic record keys,
and other deep values are not filterable.

`where: []`, empty `AND`/`OR` groups, `NOT` around an empty expression, and an
empty `in` list normalize away as no filter. This supports dynamically generated
queries without accidentally hiding all rows. To intentionally return no rows,
use a real condition whose operand cannot match the data.

Only this canonical array form is accepted. A field-keyed object such as
`where: { status: { type: "equals", filter: "open" } }` is invalid.

Calling `subscribe` snapshots the complete query immediately. Later mutation of
the caller's `where`, groups, operands, `routeBy`, sorting, grouping, or window
values cannot change the active subscription.

Raw `orderBy` entries reference topic row fields only.

## Leased Source Routes

A leased-source query also carries an exact `routeBy` object containing all and
only the Route Fields declared by the source:

```ts
useLiveQuery("regionalOrders", {
  routeBy: { region: "ÁbCDEfgh" },
  select: ["id", "status"],
  where: [{ field: "status", type: "equals", filter: "open" }],
});
```

`routeBy` selects one upstream Leased Feed; `where` only filters rows inside
that feed. Route values are opaque and exact: no case folding, accent folding,
trimming, or filter normalization occurs before the source Adapter builds its
request. Topics without leased lifecycle reject `routeBy`.

## Grouped Queries

Grouped queries require non-empty `groupBy` and an aggregate object. `select` is
not allowed.

```ts
useLiveQuery("orders", {
  where: [{ field: "price", type: "greaterThanOrEqual", filter: 10 }],
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

Grouped `where` expressions filter source Topic Rows before grouping and cannot
reference aggregate aliases. Aggregate-result filtering is not part of this
query contract.

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
