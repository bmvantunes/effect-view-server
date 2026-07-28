import { Schema } from "effect";

const ViewServerIdSchemaTypeId: unique symbol = Symbol.for(
  "@effect-view-server/config/ViewServerId",
);

export interface ViewServerIdSchema extends Schema.Codec<string, string, never, never> {
  readonly [ViewServerIdSchemaTypeId]: true;
}

/**
 * The one canonical Topic Row identifier schema.
 *
 * Its decoded and encoded values remain plain strings. The schema-level nominal
 * marker lets configuration type checks reject arbitrary string refinements
 * before runtime.
 */
export const ViewServerId: ViewServerIdSchema = Object.freeze(
  Object.assign(Schema.String.annotate({ identifier: "ViewServerId" }), {
    [ViewServerIdSchemaTypeId]: true as const,
  }),
);

export const isViewServerIdSchema = (candidate: unknown): candidate is ViewServerIdSchema => {
  if (!Schema.isSchema(candidate)) {
    return false;
  }
  const ast = candidate.ast;
  return (
    Reflect.get(candidate, ViewServerIdSchemaTypeId) === true &&
    ast._tag === "String" &&
    ast.checks === undefined &&
    ast.encoding === undefined &&
    ast.context === undefined &&
    ast.annotations?.["identifier"] === "ViewServerId"
  );
};
