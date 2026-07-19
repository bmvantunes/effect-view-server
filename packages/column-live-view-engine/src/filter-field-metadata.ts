import {
  viewServerFilterFieldContracts,
  type ViewServerFilterNumericKind,
} from "@effect-view-server/config/internal";
import type { Schema } from "effect";
import { immutableReadonlyMap, immutableReadonlySet } from "./immutable-readonly-collection";
import { makeSchemaValueSemantics, type SchemaValueSemantics } from "./topic-row-value-semantics";

export type FilterNumericKind = ViewServerFilterNumericKind;

export type FilterFieldMetadata = {
  readonly path: string;
  readonly segments: ReadonlyArray<string>;
  readonly semantics: SchemaValueSemantics;
  readonly hasString: boolean;
  readonly numericKinds: ReadonlySet<FilterNumericKind>;
};

export const makeFilterFieldMetadata = (
  schema: Schema.Codec<object, unknown, never, never> & {
    readonly fields?: Readonly<Record<string, unknown>>;
  },
): ReadonlyMap<string, FilterFieldMetadata> => {
  if (schema.fields === undefined) {
    return immutableReadonlyMap(new Map());
  }
  const fields = new Map<string, FilterFieldMetadata>();
  for (const field of viewServerFilterFieldContracts(schema)) {
    const [path, contract] = field;
    fields.set(
      path,
      Object.freeze({
        path,
        segments: contract.segments,
        semantics: makeSchemaValueSemantics(contract.typeSchema),
        hasString: contract.supportsText,
        numericKinds: immutableReadonlySet(contract.numericKinds),
      }),
    );
  }
  return immutableReadonlyMap(fields);
};
