import { Result, Schema, SchemaAST } from "effect";
import {
  materializeStrictJson,
  StrictJsonMaterializationError,
} from "./strict-json-materialization";

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
  const representation = ast.annotations?.["representation"];
  if (typeof representation === "object" && representation !== null) {
    const id = String(Reflect.get(representation, "id"));
    return `effect/${id.slice("effect/schema/".length)}`;
  }
  return undefined;
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

type StrictJsonObjectKeywordGuard = (value: unknown, path: string) => void;

const simplePathKey = /^[A-Za-z_$][A-Za-z0-9_$]*$/;

const appendStrictJsonPropertyPath = (path: string, key: string): string =>
  simplePathKey.test(key) ? `${path}.${key}` : `${path}[${JSON.stringify(key)}]`;

const rebaseStrictJsonMaterializationError = (
  error: StrictJsonMaterializationError,
  path: string,
): StrictJsonMaterializationError => {
  const rebasedPath = `${path}${error.path.slice(1)}`;
  return StrictJsonMaterializationError.make({
    path: rebasedPath,
    reason: error.reason,
    message: error.message.replace(error.path, rebasedPath),
  });
};

const strictJsonPropertyPath = (path: string, key: string | symbol): string =>
  typeof key === "string" ? appendStrictJsonPropertyPath(path, key) : `${path}[${String(key)}]`;

const strictJsonReflectionFailure = (path: string): StrictJsonMaterializationError =>
  StrictJsonMaterializationError.make({
    path,
    reason: "reflection-failure",
    message: `Could not inspect JSON value at ${path}.`,
  });

const strictJsonNonEnumerableProperty = (path: string): StrictJsonMaterializationError =>
  StrictJsonMaterializationError.make({
    path,
    reason: "non-enumerable-property",
    message: `Expected an enumerable data property at ${path}.`,
  });

const strictJsonAccessorProperty = (path: string): StrictJsonMaterializationError =>
  StrictJsonMaterializationError.make({
    path,
    reason: "accessor-property",
    message: `Accessor properties are not valid JSON data at ${path}.`,
  });

type OwnDataProperty =
  | { readonly present: false }
  | { readonly present: true; readonly value: unknown };

const readOwnDataProperty = (value: object, key: string, path: string): OwnDataProperty => {
  let descriptor: PropertyDescriptor | undefined;
  try {
    descriptor = Object.getOwnPropertyDescriptor(value, key);
  } catch {
    throw strictJsonReflectionFailure(path);
  }
  if (descriptor === undefined) {
    return { present: false };
  }
  if (descriptor.enumerable !== true) {
    throw strictJsonNonEnumerableProperty(path);
  }
  if (!("value" in descriptor)) {
    throw strictJsonAccessorProperty(path);
  }
  return { present: true, value: descriptor.value };
};

const readArrayLength = (value: Array<unknown>, path: string): number => {
  try {
    return value.length;
  } catch {
    throw strictJsonReflectionFailure(`${path}.length`);
  }
};

const assertNoAccessorProperties = (
  value: unknown,
  path: string,
  active: WeakSet<object> = new WeakSet(),
): void => {
  if (value === null || (typeof value !== "object" && typeof value !== "function")) {
    return;
  }
  if (active.has(value)) {
    return;
  }
  active.add(value);

  let keys: ReadonlyArray<string | symbol>;
  try {
    keys = Reflect.ownKeys(value);
  } catch {
    throw strictJsonReflectionFailure(path);
  }
  for (const key of keys) {
    const propertyPath = strictJsonPropertyPath(path, key);
    let descriptor: PropertyDescriptor | undefined;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      throw strictJsonReflectionFailure(propertyPath);
    }
    if (descriptor === undefined) {
      continue;
    }
    if (!("value" in descriptor)) {
      throw strictJsonAccessorProperty(propertyPath);
    }
    assertNoAccessorProperties(descriptor.value, propertyPath, active);
  }
};

const makeStrictJsonObjectKeywordGuard = (root: SchemaAST.AST) => {
  const compiled = new Map<SchemaAST.AST, StrictJsonObjectKeywordGuard>();

  const compile = (ast: SchemaAST.AST): StrictJsonObjectKeywordGuard => {
    const cached = compiled.get(ast);
    if (cached !== undefined) {
      return cached;
    }

    let implementation: StrictJsonObjectKeywordGuard = () => undefined;
    const guard: StrictJsonObjectKeywordGuard = (value, path) => implementation(value, path);
    compiled.set(ast, guard);

    if (SchemaAST.isObjectKeyword(ast)) {
      implementation = (value, path) => {
        const materialized = materializeStrictJson(value);
        if (Result.isFailure(materialized)) {
          throw rebaseStrictJsonMaterializationError(materialized.failure, path);
        }
      };
      return guard;
    }

    if (SchemaAST.isSuspend(ast)) {
      implementation = (value, path) => compile(ast.thunk())(value, path);
      return guard;
    }

    if (ast.encoding !== undefined && ast.encoding.length > 0) {
      implementation = compile(ast.encoding[ast.encoding.length - 1]!.to);
      return guard;
    }

    if (SchemaAST.isUnion(ast)) {
      const members = ast.types.map((member) => ({
        is: Schema.is(Schema.make<Schema.Codec<unknown, unknown, never, never>>(member)),
        guard: compile(member),
      }));
      implementation = (value, path) => {
        assertNoAccessorProperties(value, path);
        members.find((member) => member.is(value))?.guard(value, path);
      };
      return guard;
    }

    if (SchemaAST.isObjects(ast)) {
      const properties = ast.propertySignatures
        .filter(
          (property): property is typeof property & { readonly name: string } =>
            typeof property.name === "string",
        )
        .map((property) => ({
          name: property.name,
          guard: compile(property.type),
        }));
      const indexes = ast.indexSignatures.map((index) => ({
        accepts: Schema.is(
          Schema.make<Schema.Codec<unknown, unknown, never, never>>(index.parameter),
        ),
        guard: compile(index.type),
      }));
      implementation = (value, path) => {
        if (value === null || (typeof value !== "object" && typeof value !== "function")) {
          return;
        }
        for (const property of properties) {
          const propertyPath = appendStrictJsonPropertyPath(path, property.name);
          const propertyValue = readOwnDataProperty(value, property.name, propertyPath);
          if (propertyValue.present) {
            property.guard(propertyValue.value, propertyPath);
          }
        }
        for (const key of Object.keys(value)) {
          const index = indexes.find((candidate) => candidate.accepts(key));
          if (index !== undefined) {
            const propertyPath = appendStrictJsonPropertyPath(path, key);
            const propertyValue = readOwnDataProperty(value, key, propertyPath);
            if (propertyValue.present) {
              index.guard(propertyValue.value, propertyPath);
            }
          }
        }
      };
      return guard;
    }

    if (SchemaAST.isArrays(ast)) {
      const elements = ast.elements.map(compile);
      const rest = ast.rest.map(compile);
      implementation = (value, path) => {
        if (!Array.isArray(value)) {
          return;
        }
        const [head, ...tail] = rest;
        const tailThreshold = readArrayLength(value, path) - tail.length;
        for (let index = 0; index < tailThreshold + tail.length; index += 1) {
          const item =
            index < elements.length
              ? elements[index]
              : index >= tailThreshold
                ? tail[index - tailThreshold]
                : head;
          const itemPath = `${path}[${index}]`;
          const entry = readOwnDataProperty(value, String(index), itemPath);
          if (entry.present) {
            item?.(entry.value, itemPath);
          }
        }
      };
    }

    return guard;
  };

  const guard = compile(root);
  return (value: unknown): void => guard(value, "$");
};

export type StrictJsonSchemaGuard = (
  value: unknown,
) => Result.Result<void, StrictJsonMaterializationError>;

export const makeStrictJsonSchemaGuard = (root: SchemaAST.AST): StrictJsonSchemaGuard => {
  const guard = makeStrictJsonObjectKeywordGuard(root);
  return (value) =>
    Result.try({
      try: () => {
        guard(value);
      },
      catch: (error) =>
        error instanceof StrictJsonMaterializationError
          ? error
          : StrictJsonMaterializationError.make({
              path: "$",
              reason: "reflection-failure",
              message: "Could not inspect JSON value at $.",
            }),
    });
};

export const makeSchemaJsonIdentity = <Type>(
  schema: ValueSchema<Type>,
): SchemaJsonIdentity<Type> => {
  const codec = Schema.toCodecJson(schema);
  const decode = Schema.decodeUnknownSync(codec);
  const encode = Schema.encodeUnknownSync(codec);
  const normalize = makeSchemaJsonNormalizer(codec.ast);
  const strictObjectKeywordGuard = makeStrictJsonSchemaGuard(codec.ast);
  const strictEncoded = (value: unknown): Schema.Json => {
    const guardResult = strictObjectKeywordGuard(value);
    if (Result.isFailure(guardResult)) {
      throw guardResult.failure;
    }
    return strictJson(encode(value));
  };
  const canonicalJson = (value: unknown): Schema.Json => normalize(strictEncoded(value));
  const canonicalKey =
    SchemaAST.isString(codec.ast) &&
    codec.ast.encoding === undefined &&
    (codec.ast.checks?.length ?? 0) === 0
      ? (value: unknown): string =>
          typeof value === "string"
            ? canonicalJsonString(value)
            : canonicalJsonString(canonicalJson(value))
      : (value: unknown): string => canonicalJsonString(canonicalJson(value));
  const materializeDecoded = (value: unknown): Type => decode(strictEncoded(value));
  const decodeEncoded = (value: unknown): Type => {
    const decoded = decode(strictJson(value));
    return materializeDecoded(decoded);
  };
  return {
    canonicalJson,
    canonicalKey,
    decodeEncoded,
    materializeDecoded,
  };
};
