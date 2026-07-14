import type { PickRawFields, RawQuery } from "@effect-view-server/config";
import { Effect } from "effect";
import { compareQueryValue, stableQueryValueString } from "./query-value";
import type { CompiledRawPredicate } from "./raw-predicate-compiler";
import { isRangePlanValue } from "./raw-predicate-plan";
import { makeRawQueryPlan, type RawQueryPlan } from "./raw-query-plan";
import {
  decodeRawQuery,
  decodeTypedRawQuery,
  InvalidQueryError,
  type RuntimeRawQuery,
  validateRuntimeQuery,
} from "./raw-query-decoder";
import {
  rawQueryCompilerMetadata,
  rawQueryCompilerMetadataMatchesSchema,
  type RawQueryCompilerMetadata,
} from "./raw-query-metadata";
import {
  rawQueryResultSemantics,
  runtimeRawQueryResultSemantics,
  type TopicStorageProjectableQueryResultSemantics,
} from "./query-result-semantics";

type RowObject = object;
const compiledRawQueryBrand: unique symbol = Symbol("CompiledRawQuery");

export { rawQueryCompilerMetadata };
export { rawQueryCompilerMetadataMatchesSchema };
export { compareQueryValue, stableQueryValueString };
export { isRangePlanValue };
export { InvalidQueryError };
export type { RawQueryCompilerMetadata, RuntimeRawQuery };

export type CompiledRawQuery<Row extends RowObject, ResultRow extends RowObject> = {
  readonly [compiledRawQueryBrand]: true;
  readonly plan: RawQueryPlan<Row, ResultRow>;
};

export type { CompiledRawPredicate };

export const ensureRawQueryCompilerMetadata = Effect.fn(
  "ColumnLiveViewEngine.rawQuery.metadata.ensure",
)(function* <Row extends RowObject>(topic: string, metadata: RawQueryCompilerMetadata<Row>) {
  if (!rawQueryCompilerMetadataMatchesSchema(metadata, metadata.schema)) {
    return yield* InvalidQueryError.make({
      topic,
      message: "Query compiler metadata schema does not match its provenance.",
    });
  }
});

const compileRawQuery = <SchemaRow extends RowObject, ResultRow extends RowObject>(
  metadata: RawQueryCompilerMetadata<SchemaRow>,
  query: RuntimeRawQuery,
  resultSemantics: TopicStorageProjectableQueryResultSemantics<ResultRow>,
): CompiledRawQuery<RowObject, ResultRow> => {
  const plan = makeRawQueryPlan<RowObject, ResultRow, SchemaRow>(metadata, query, resultSemantics);
  return Object.freeze({
    [compiledRawQueryBrand]: true,
    plan,
  });
};

export const prepareRawQuery = Effect.fn("ColumnLiveViewEngine.rawQuery.prepare")(function* <
  Row extends RowObject,
  const Query extends RawQuery<NoInfer<Row>>,
>(topic: string, metadata: RawQueryCompilerMetadata<Row>, query: Query) {
  yield* ensureRawQueryCompilerMetadata(topic, metadata);
  const decoded = yield* decodeTypedRawQuery(topic, metadata, query);
  yield* validateRuntimeQuery(topic, metadata, decoded);
  return compileRawQuery<Row, PickRawFields<Row, Query>>(
    metadata,
    decoded,
    rawQueryResultSemantics<Row, Query>(metadata.valueSemantics, decoded),
  );
});

export const prepareRuntimeRawQuery = Effect.fn("ColumnLiveViewEngine.rawQuery.prepareRuntime")(
  function* <Row extends RowObject>(
    topic: string,
    metadata: RawQueryCompilerMetadata<Row>,
    query: unknown,
  ) {
    yield* ensureRawQueryCompilerMetadata(topic, metadata);
    const decoded = yield* decodeRawQuery(topic, metadata, query);
    yield* validateRuntimeQuery(topic, metadata, decoded);
    return compileRawQuery(
      metadata,
      decoded,
      runtimeRawQueryResultSemantics(metadata.valueSemantics, decoded.select),
    );
  },
);
