import { Result, Schema, SchemaAST } from "effect";
import { materializeStrictJson } from "./strict-json-materialization";

type ValueSchema<Type = unknown> = Schema.Codec<Type, unknown, never, never>;
type JsonNormalizer = (value: Schema.Json) => Schema.Json;

export type SchemaJsonIdentity<Type = unknown> = {
  readonly canonicalJson: (value: unknown) => Schema.Json;
  readonly canonicalKey: (value: unknown) => string;
  readonly decodeEncoded: (value: unknown) => Type;
  readonly materializeDecoded: (value: unknown) => Type;
};

const typeConstructorTag = (ast: SchemaAST.AST): unknown => {
  if (!SchemaAST.isDeclaration(ast)) {
    return undefined;
  }
  const typeConstructor = ast.annotations?.["typeConstructor"];
  return typeof typeConstructor === "object" && typeConstructor !== null
    ? Reflect.get(typeConstructor, "_tag")
    : undefined;
};

const compareStrings = (left: string, right: string): number =>
  Number(left > right) - Number(left < right);

const isJsonArray = (value: Schema.Json): value is Schema.JsonArray => Array.isArray(value);

const isJsonObject = (value: Schema.Json): value is Schema.JsonObject =>
  value !== null && typeof value === "object" && !isJsonArray(value);

const canonicalJsonString = (value: Schema.Json): string => {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (isJsonObject(value)) {
    return `{${Object.keys(value)
      .toSorted(compareStrings)
      .map((key) => `${JSON.stringify(key)}:${canonicalJsonString(value[key]!)}`)
      .join(",")}}`;
  }
  return `[${value.map(canonicalJsonString).join(",")}]`;
};

const defineJsonProperty = (
  output: Record<string, Schema.Json>,
  key: string,
  value: Schema.Json,
): void => {
  Object.defineProperty(output, key, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
};

export const makeSchemaJsonNormalizer = (root: SchemaAST.AST): JsonNormalizer => {
  const compiled = new Map<SchemaAST.AST, JsonNormalizer>();

  const compile = (ast: SchemaAST.AST): JsonNormalizer => {
    const cached = compiled.get(ast);
    if (cached !== undefined) {
      return cached;
    }

    let implementation: JsonNormalizer = (value) => value;
    const normalizer: JsonNormalizer = (value) => implementation(value);
    compiled.set(ast, normalizer);

    if (SchemaAST.isSuspend(ast)) {
      let suspended: JsonNormalizer | undefined;
      implementation = (value) => {
        suspended ??= compile(ast.thunk());
        return suspended(value);
      };
      return normalizer;
    }

    if (ast.encoding !== undefined && ast.encoding.length > 0) {
      const encoded = compile(ast.encoding[ast.encoding.length - 1]!.to);
      const tag = typeConstructorTag(ast);
      if (tag === "effect/HashMap" || tag === "effect/HashSet") {
        implementation = (value) => {
          const normalized = encoded(value);
          return isJsonArray(normalized)
            ? normalized.toSorted((left, right) =>
                compareStrings(canonicalJsonString(left), canonicalJsonString(right)),
              )
            : normalized;
        };
      } else {
        implementation = encoded;
      }
      return normalizer;
    }

    if (SchemaAST.isUnion(ast)) {
      const members = ast.types.map((member) => ({
        is: Schema.is(
          Schema.make<Schema.Codec<unknown, unknown, never, never>>(SchemaAST.toEncoded(member)),
        ),
        normalize: compile(member),
      }));
      implementation = (value) =>
        members.find((member) => member.is(value))?.normalize(value) ?? value;
      return normalizer;
    }

    if (SchemaAST.isObjects(ast)) {
      const properties = new Map(
        ast.propertySignatures
          .filter((property) => typeof property.name === "string")
          .map((property) => [property.name, compile(property.type)] as const),
      );
      const indexes = ast.indexSignatures.map((index) => ({
        accepts: Schema.is(
          Schema.make<Schema.Codec<unknown, unknown, never, never>>(
            SchemaAST.toEncoded(index.parameter),
          ),
        ),
        normalize: compile(index.type),
      }));
      implementation = (value) => {
        if (!isJsonObject(value)) {
          return value;
        }
        const output: Record<string, Schema.Json> = {};
        for (const key of Object.keys(value)) {
          const property = properties.get(key);
          const index = indexes.find((candidate) => candidate.accepts(key));
          const fieldValue = value[key]!;
          defineJsonProperty(
            output,
            key,
            property?.(fieldValue) ?? index?.normalize(fieldValue) ?? fieldValue,
          );
        }
        return output;
      };
      return normalizer;
    }

    if (SchemaAST.isArrays(ast)) {
      const elements = ast.elements.map(compile);
      const rest = ast.rest.map(compile);
      implementation = (value) => {
        if (!isJsonArray(value)) {
          return value;
        }
        const [head, ...tail] = rest;
        const tailThreshold = value.length - tail.length;
        return value.map((entry, index) => {
          const item =
            index < elements.length
              ? elements[index]
              : index >= tailThreshold
                ? tail[index - tailThreshold]
                : head;
          return item?.(entry) ?? entry;
        });
      };
    }

    return normalizer;
  };

  return compile(root);
};

const strictJson = (value: unknown): Schema.Json => {
  const materialized = materializeStrictJson(value);
  if (Result.isFailure(materialized)) {
    throw materialized.failure;
  }
  return materialized.success;
};

export const makeSchemaJsonIdentity = <Type>(
  schema: ValueSchema<Type>,
): SchemaJsonIdentity<Type> => {
  const codec = Schema.toCodecJson(schema);
  const decode = Schema.decodeUnknownSync(codec);
  const encode = Schema.encodeUnknownSync(codec);
  const normalize = makeSchemaJsonNormalizer(codec.ast);
  const strictEncoded = (value: unknown): Schema.Json => strictJson(encode(value));
  const canonicalJson = (value: unknown): Schema.Json => normalize(strictEncoded(value));
  const materializeDecoded = (value: unknown): Type => decode(strictEncoded(value));
  const decodeEncoded = (value: unknown): Type => {
    const decoded = decode(strictJson(value));
    return materializeDecoded(decoded);
  };
  return {
    canonicalJson,
    canonicalKey: (value) => canonicalJsonString(canonicalJson(value)),
    decodeEncoded,
    materializeDecoded,
  };
};
