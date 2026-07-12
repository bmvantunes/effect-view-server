import {
  missingSchemaValuePresenceToken,
  presentSchemaValuePresenceToken,
  schemaValuePresenceKey,
} from "./schema-value-presence";

export type GroupedKeyIdentityField = {
  readonly field: string;
  readonly canonicalKey: (value: unknown) => string;
};

export type CompiledGroupedKeyIdentity<Row extends object, Key extends string | undefined> = {
  readonly key: (row: Row) => Key;
};

type CompiledGroupedKeyFrame = GroupedKeyIdentityField & {
  readonly prefix: string;
};

const missingGroupedValueKey = schemaValuePresenceKey(missingSchemaValuePresenceToken);

const compileGroupedKeyFrames = (
  fields: ReadonlyArray<GroupedKeyIdentityField>,
): ReadonlyArray<CompiledGroupedKeyFrame> =>
  fields.map((field) => ({
    ...field,
    prefix: `[${JSON.stringify(field.field)},`,
  }));

const groupedKeyFromRow = <Row extends object>(
  fields: ReadonlyArray<CompiledGroupedKeyFrame>,
  row: Row,
): string => {
  const tokens: Array<string> = [];
  for (const field of fields) {
    const presenceKey = Object.prototype.propertyIsEnumerable.call(row, field.field)
      ? schemaValuePresenceKey(
          presentSchemaValuePresenceToken(field.canonicalKey(Reflect.get(row, field.field))),
        )
      : missingGroupedValueKey;
    tokens.push(`${field.prefix}${JSON.stringify(presenceKey)}]`);
  }
  return `[${tokens.join(",")}]`;
};

export function compileGroupedKeyIdentity<Row extends object>(
  fields: ReadonlyArray<GroupedKeyIdentityField>,
  failureMode: "throw",
): CompiledGroupedKeyIdentity<Row, string>;
export function compileGroupedKeyIdentity<Row extends object>(
  fields: ReadonlyArray<GroupedKeyIdentityField>,
  failureMode: "undefined",
): CompiledGroupedKeyIdentity<Row, string | undefined>;
export function compileGroupedKeyIdentity<Row extends object>(
  fields: ReadonlyArray<GroupedKeyIdentityField>,
  failureMode: "throw" | "undefined",
): CompiledGroupedKeyIdentity<Row, string | undefined> {
  const compiled = compileGroupedKeyFrames(fields);
  if (failureMode === "throw") {
    return {
      key: (row) => groupedKeyFromRow(compiled, row),
    };
  }
  return {
    key: (row) => {
      try {
        return groupedKeyFromRow(compiled, row);
      } catch {
        return undefined;
      }
    },
  };
}
