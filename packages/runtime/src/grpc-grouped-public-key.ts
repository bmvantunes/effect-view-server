import type { RowSchema } from "@effect-view-server/config";
import {
  compileGroupedKeyIdentity,
  makeSchemaJsonIdentity,
  type GroupedKeyIdentityField,
} from "@effect-view-server/effect-utils";
import { Result } from "effect";

export type GrpcGroupedPublicKey = {
  readonly key: (row: object) => string | undefined;
};

const compileGroupedKeyFields = (
  schema: RowSchema,
  groupBy: ReadonlyArray<string>,
): ReadonlyArray<GroupedKeyIdentityField> | undefined => {
  const fields: Array<GroupedKeyIdentityField> = [];
  for (const field of groupBy) {
    const fieldSchema = schema.fields[field];
    if (fieldSchema === undefined) {
      return undefined;
    }
    const compiled = Result.try(() => {
      const identity = makeSchemaJsonIdentity(fieldSchema);
      return {
        field,
        canonicalKey: identity.canonicalKey,
      } satisfies GroupedKeyIdentityField;
    });
    if (Result.isFailure(compiled)) {
      return undefined;
    }
    fields.push(compiled.success);
  }
  return fields;
};

export const compileGrpcGroupedPublicKey = (
  schema: RowSchema,
  groupBy: ReadonlyArray<string>,
): GrpcGroupedPublicKey | undefined => {
  const fields = compileGroupedKeyFields(schema, groupBy);
  return fields === undefined ? undefined : compileGroupedKeyIdentity(fields, "undefined");
};
