import { describe, expectTypeOf, it } from "@effect/vitest";
import type {
  GroupedQuery,
  GroupedResult,
  PickRawFields,
  RawQuery,
} from "@effect-view-server/config";
import type { Effect } from "effect";
import { Schema } from "effect";
import type { BigDecimal } from "effect/BigDecimal";
import { prepareGroupedQuery, prepareRuntimeGroupedQuery } from "./grouped-query-compiler";
import { decodeTypedGroupedQuery } from "./grouped-query-decoder";
import {
  prepareRawQuery,
  prepareRuntimeRawQuery,
  rawQueryCompilerMetadata,
  type RawQueryCompilerMetadata,
} from "./raw-query-compiler";
import { decodeTypedRawQuery } from "./raw-query-decoder";
import type { QueryEvaluation } from "./query-result";
import {
  groupedQueryResultSemantics,
  rawQueryResultSemantics,
  type QueryResultTopicStorageProjectionProof,
  type QueryResultSemantics,
} from "./query-result-semantics";
import { makeQueryResultTopicStorageProjectionProof } from "./query-result-topic-storage-proof";
import {
  bindTopicStorageProjection,
  type TopicStorageProjectionCapability,
} from "./topic-storage-projection";
import {
  type ExecutableQuery,
  prepareRuntimeExecutableQuery,
  snapshotGroupedExecutableQuery,
  snapshotRawExecutableQuery,
} from "./query-execution";
import type { TopicStore } from "./topic-store";

const Order = Schema.Struct({
  id: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Finite,
});

type OrderRow = typeof Order.Type;

const enumerableProperties = <Value extends object>(
  value: Value,
): { [Key in keyof Value]: Value[Key] } => ({ ...value });
type EffectSuccess<Value> =
  Value extends Effect.Effect<infer Success, infer _Error, infer _Services> ? Success : never;
type EvaluationRow<Value> = Value extends QueryEvaluation<infer Row> ? Row : never;
type ExecutableRow<Value> = Value extends ExecutableQuery<infer Row> ? Row : never;
type SemanticsRow<Value> = Value extends QueryResultSemantics<infer Row> ? Row : never;

const rawQuery = {
  select: ["id", "price"],
} satisfies RawQuery<OrderRow>;

const groupedQuery = {
  groupBy: ["status"],
  aggregates: {
    rowCount: { aggFunc: "count" },
    totalPrice: { aggFunc: "sum", field: "price" },
  },
} satisfies GroupedQuery<OrderRow>;
const orderWitnessMetadata = rawQueryCompilerMetadata(Order);
const idOnlyRawQuery = { select: ["id"] } satisfies RawQuery<OrderRow>;
const countOnlyGroupedQuery = {
  groupBy: ["status"],
  aggregates: { rowCount: { aggFunc: "count" } },
} satisfies GroupedQuery<OrderRow>;
const idOnlyRawWitness = decodeTypedRawQuery("orders", orderWitnessMetadata, idOnlyRawQuery);
const countOnlyGroupedWitness = decodeTypedGroupedQuery(
  "orders",
  orderWitnessMetadata,
  countOnlyGroupedQuery,
);
declare const decodedIdOnlyRaw: EffectSuccess<typeof idOnlyRawWitness>;
declare const decodedCountOnlyGrouped: EffectSuccess<typeof countOnlyGroupedWitness>;

type RawResult = PickRawFields<OrderRow, typeof rawQuery>;
type GroupedResultRow = GroupedResult<OrderRow, typeof groupedQuery>;
type ExpectedGroupedResult = {
  readonly status: "open" | "closed" | "cancelled";
  readonly rowCount: bigint;
  readonly totalPrice: BigDecimal;
};
type ForgedResult = { readonly forged: true };

const NumericId = Schema.Struct({ id: Schema.Number });
const StringId = Schema.Struct({ id: Schema.String });
const numericMetadata = rawQueryCompilerMetadata(NumericId);
const stringMetadata = rawQueryCompilerMetadata(StringId);
const numericIdQuery = { select: ["id"] } satisfies RawQuery<typeof NumericId.Type>;
const stringIdQuery = { select: ["id"] } satisfies RawQuery<typeof StringId.Type>;
const numericIdGroupedQuery = {
  groupBy: ["id"],
  aggregates: { rowCount: { aggFunc: "count" } },
} satisfies GroupedQuery<typeof NumericId.Type>;
const stringIdGroupedQuery = {
  groupBy: ["id"],
  aggregates: { rowCount: { aggFunc: "count" } },
} satisfies GroupedQuery<typeof StringId.Type>;
const numericRawWitness = decodeTypedRawQuery("numeric", numericMetadata, numericIdQuery);
const numericGroupedWitness = decodeTypedGroupedQuery(
  "numeric",
  numericMetadata,
  numericIdGroupedQuery,
);
declare const decodedNumericRaw: EffectSuccess<typeof numericRawWitness>;
declare const decodedNumericGrouped: EffectSuccess<typeof numericGroupedWitness>;

declare const unknownQuery: unknown;
declare const store: TopicStore;
declare const orderRow: OrderRow;
declare const unequalSelect: readonly ["id"] | readonly ["id", "price"];
declare const commonSelect: readonly ["id", "price"] | readonly ["id", "status"];
declare const dynamicGroupBy: readonly ["status"] | readonly ["id"];
declare const unequalGroupBy: readonly ["status"] | readonly ["status", "id"];
declare const commonGroupBy: readonly ["status", "id"] | readonly ["status", "price"];
declare const storageProjectionCapability: TopicStorageProjectionCapability;
declare const stringProjectionProof: QueryResultTopicStorageProjectionProof<{
  readonly id: string;
}>;

describe("compiled Query Result Semantics", () => {
  it("keeps schema provenance nominal across metadata and result proofs", () => {
    makeQueryResultTopicStorageProjectionProof<{ readonly forged: true }>(
      stringMetadata.valueSemantics,
      ["id"],
      // @ts-expect-error proof output requires a real narrower for the selected result row.
      (row: object) => row,
    );
    const stringProjectionSession = bindTopicStorageProjection(
      storageProjectionCapability,
      stringProjectionProof,
    );
    expectTypeOf(stringProjectionSession.projectResultRow(0)).toEqualTypeOf<{
      readonly id: string;
    }>();
    // @ts-expect-error only the concrete Topic Row Storage can construct its projection capability.
    const invalidStorageCapability: TopicStorageProjectionCapability = {};
    const structuralProjectionProof = {
      matchesValueSemantics: () => true,
      selectedFields: ["id"],
    };
    // @ts-expect-error structural fields cannot forge the private projection-proof provenance.
    const invalidStructuralProjectionProof: QueryResultTopicStorageProjectionProof<{
      readonly id: string;
    }> = structuralProjectionProof;
    const spreadProjectionProof = enumerableProperties(stringProjectionProof);
    // @ts-expect-error spreading a compiled proof loses its nominal proof provenance.
    const _invalidSpreadProjectionProof: QueryResultTopicStorageProjectionProof<{
      readonly id: string;
    }> = spreadProjectionProof;
    // @ts-expect-error a query result projection proof cannot be changed to another result row.
    const invalidProjectionProof: QueryResultTopicStorageProjectionProof<{ readonly id: number }> =
      stringProjectionProof;
    const invalidMetadata = {
      ...numericMetadata,
      schema: StringId,
      valueSemantics: stringMetadata.valueSemantics,
      // @ts-expect-error replacing the public schema cannot replace hidden metadata provenance.
    } satisfies RawQueryCompilerMetadata<typeof StringId.Type>;
    const invalidRawProof = rawQueryResultSemantics<typeof StringId.Type, typeof stringIdQuery>(
      stringMetadata.valueSemantics,
      // @ts-expect-error a numeric query witness cannot prove a string result row.
      decodedNumericRaw,
    );
    const invalidGroupedProof = groupedQueryResultSemantics<
      typeof StringId.Type,
      typeof stringIdGroupedQuery
    >(
      stringMetadata.valueSemantics,
      // @ts-expect-error a numeric grouped witness cannot prove a string result row.
      decodedNumericGrouped,
    );
    const invalidStructuralRawProof = rawQueryResultSemantics(
      stringMetadata.valueSemantics,
      // @ts-expect-error result semantics require an authenticated decoded query witness.
      stringIdQuery,
    );
    const invalidStructuralGroupedProof = groupedQueryResultSemantics(
      stringMetadata.valueSemantics,
      // @ts-expect-error grouped result semantics require an authenticated decoded query witness.
      stringIdGroupedQuery,
    );
    const invalidRawQueryRebrand = rawQueryResultSemantics<OrderRow, typeof rawQuery>(
      orderWitnessMetadata.valueSemantics,
      // @ts-expect-error an authenticated id-only witness cannot prove an id-and-price result.
      decodedIdOnlyRaw,
    );
    const invalidGroupedQueryRebrand = groupedQueryResultSemantics<OrderRow, typeof groupedQuery>(
      orderWitnessMetadata.valueSemantics,
      // @ts-expect-error a count-only witness cannot prove a query with totalPrice.
      decodedCountOnlyGrouped,
    );

    void invalidMetadata;
    void invalidStorageCapability;
    void invalidStructuralProjectionProof;
    void invalidProjectionProof;
    void invalidRawProof;
    void invalidGroupedProof;
    void invalidStructuralRawProof;
    void invalidStructuralGroupedProof;
    void invalidRawQueryRebrand;
    void invalidGroupedQueryRebrand;
  });

  it("derives raw plan projection and ownership from schema metadata plus the query", () => {
    const metadata = rawQueryCompilerMetadata(Order);
    const prepared = prepareRawQuery("orders", metadata, rawQuery);
    type Compiled = EffectSuccess<typeof prepared>;

    expectTypeOf<ReturnType<Compiled["plan"]["project"]>>().toEqualTypeOf<RawResult>();
    expectTypeOf<SemanticsRow<Compiled["plan"]["resultSemantics"]>>().toEqualTypeOf<RawResult>();

    // @ts-expect-error the second generic is a query witness, not a caller-selected result row.
    const invalidForgedRaw = prepareRawQuery<OrderRow, ForgedResult>("orders", metadata, rawQuery);
    // @ts-expect-error typed preparation rejects an unknown runtime query.
    const invalidUnknownRaw = prepareRawQuery("orders", metadata, unknownQuery);

    void invalidForgedRaw;
    void invalidUnknownRaw;

    const unequalQuery = { select: unequalSelect } satisfies RawQuery<OrderRow>;
    const unequalPrepared = prepareRawQuery("orders", metadata, unequalQuery);
    type UnequalResult = SemanticsRow<
      EffectSuccess<typeof unequalPrepared>["plan"]["resultSemantics"]
    >;
    const unequalShort = { id: "a" } satisfies UnequalResult;
    const unequalLong = { id: "a", price: orderRow.price } satisfies UnequalResult;

    const commonQuery = { select: commonSelect } satisfies RawQuery<OrderRow>;
    const commonPrepared = prepareRawQuery("orders", metadata, commonQuery);
    type CommonResult = SemanticsRow<
      EffectSuccess<typeof commonPrepared>["plan"]["resultSemantics"]
    >;
    const commonPrice = { id: "a", price: orderRow.price } satisfies CommonResult;
    const commonStatus = { id: "a", status: "open" } satisfies CommonResult;
    // @ts-expect-error every conditional select branch requires id.
    const invalidCommon = { price: orderRow.price } satisfies CommonResult;

    void unequalShort;
    void unequalLong;
    void commonPrice;
    void commonStatus;
    void invalidCommon;
  });

  it("derives grouped evaluation and ownership from schema metadata plus the query", () => {
    const metadata = rawQueryCompilerMetadata(Order);
    const prepared = prepareGroupedQuery("orders", metadata, groupedQuery);
    type Compiled = EffectSuccess<typeof prepared>;

    expectTypeOf<
      ReturnType<Compiled["plan"]["resultSemantics"]["projectRow"]>
    >().toEqualTypeOf<GroupedResultRow>();
    expectTypeOf<GroupedResultRow>().toEqualTypeOf<ExpectedGroupedResult>();
    expectTypeOf<
      SemanticsRow<Compiled["plan"]["resultSemantics"]>
    >().toEqualTypeOf<GroupedResultRow>();
    const groupedStorageProofMustStayUnavailable = (
      semantics: Compiled["plan"]["resultSemantics"],
    ) => {
      // @ts-expect-error grouped results cannot be projected directly from raw Topic Storage rows.
      return semantics.topicStorageProjectionProof;
    };

    // @ts-expect-error the second generic is a query witness, not a caller-selected result row.
    const invalidForgedGrouped = prepareGroupedQuery<OrderRow, ForgedResult>(
      "orders",
      metadata,
      groupedQuery,
    );
    // @ts-expect-error typed preparation rejects an unknown runtime query.
    const invalidUnknownGrouped = prepareGroupedQuery("orders", metadata, unknownQuery);

    void invalidForgedGrouped;
    void invalidUnknownGrouped;
    void groupedStorageProofMustStayUnavailable;
  });

  it("keeps conditional grouped fields optional in the proven result", () => {
    const metadata = rawQueryCompilerMetadata(Order);
    const query = {
      groupBy: dynamicGroupBy,
      aggregates: {
        rowCount: { aggFunc: "count" },
      },
    } satisfies GroupedQuery<OrderRow>;
    const prepared = prepareGroupedQuery("orders", metadata, query);
    type DirectResult = GroupedResult<OrderRow, typeof query>;
    type Result = SemanticsRow<EffectSuccess<typeof prepared>["plan"]["resultSemantics"]>;

    expectTypeOf<DirectResult>().toEqualTypeOf<{
      readonly id?: string;
      readonly status?: "open" | "closed" | "cancelled";
      readonly rowCount: bigint;
    }>();
    expectTypeOf<Result>().toEqualTypeOf<{
      readonly id?: string;
      readonly status?: "open" | "closed" | "cancelled";
      readonly rowCount: bigint;
    }>();

    const unequalQuery = {
      groupBy: unequalGroupBy,
      aggregates: {
        rowCount: { aggFunc: "count" },
      },
    } satisfies GroupedQuery<OrderRow>;
    const unequalPrepared = prepareGroupedQuery("orders", metadata, unequalQuery);
    type UnequalResult = SemanticsRow<
      EffectSuccess<typeof unequalPrepared>["plan"]["resultSemantics"]
    >;
    const unequalShort = { status: "open", rowCount: 1n } satisfies UnequalResult;
    const unequalLong = { status: "open", id: "a", rowCount: 1n } satisfies UnequalResult;

    const commonQuery = {
      groupBy: commonGroupBy,
      aggregates: {
        rowCount: { aggFunc: "count" },
      },
    } satisfies GroupedQuery<OrderRow>;
    const commonPrepared = prepareGroupedQuery("orders", metadata, commonQuery);
    type CommonResult = SemanticsRow<
      EffectSuccess<typeof commonPrepared>["plan"]["resultSemantics"]
    >;
    const commonId = { status: "open", id: "a", rowCount: 1n } satisfies CommonResult;
    const commonPrice = {
      status: "open",
      price: orderRow.price,
      rowCount: 1n,
    } satisfies CommonResult;
    // @ts-expect-error every conditional groupBy branch requires status.
    const invalidCommon = { id: "a", rowCount: 1n } satisfies CommonResult;

    void unequalShort;
    void unequalLong;
    void commonId;
    void commonPrice;
    void invalidCommon;
  });

  it("keeps unknown runtime queries result-erased", () => {
    const metadata = rawQueryCompilerMetadata(Order);
    const runtimeRaw = prepareRuntimeRawQuery("orders", metadata, unknownQuery);
    const runtimeGrouped = prepareRuntimeGroupedQuery("orders", metadata, unknownQuery);

    expectTypeOf<
      ReturnType<EffectSuccess<typeof runtimeRaw>["plan"]["project"]>
    >().toEqualTypeOf<object>();
    expectTypeOf<
      EvaluationRow<ReturnType<EffectSuccess<typeof runtimeGrouped>["evaluate"]>>
    >().toEqualTypeOf<object>();

    const runtimeExecutable = prepareRuntimeExecutableQuery(store, unknownQuery);
    expectTypeOf<ExecutableRow<EffectSuccess<typeof runtimeExecutable>>>().toEqualTypeOf<object>();
    expectTypeOf<
      ExecutableRow<EffectSuccess<typeof runtimeExecutable>>
    >().not.toEqualTypeOf<ForgedResult>();

    void runtimeExecutable;
  });

  it("derives typed one-shot results from the compiled proof", () => {
    const metadata = rawQueryCompilerMetadata(Order);
    const rawSnapshot = snapshotRawExecutableQuery(store, metadata, rawQuery);
    const groupedSnapshot = snapshotGroupedExecutableQuery(store, metadata, groupedQuery);

    expectTypeOf<EffectSuccess<typeof rawSnapshot>["rows"][number]>().toEqualTypeOf<RawResult>();
    expectTypeOf<
      EffectSuccess<typeof groupedSnapshot>["rows"][number]
    >().toEqualTypeOf<GroupedResultRow>();
  });
});
