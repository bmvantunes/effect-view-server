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

type StrictJsonObjectKeywordGuard = (value: unknown, path: string) => unknown;
type StrictJsonGuardSide = "decoded" | "encoded";
type StrictJsonGuardMode = "validate" | "snapshot";

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

const cloneWithReplacements = (
  value: object,
  replacements: ReadonlyMap<string, unknown>,
): object => {
  const output = Array.isArray(value) ? [] : Object.create(Object.getPrototypeOf(value));
  const copiedReplacements = new Set<string>();
  for (const key of Reflect.ownKeys(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor !== undefined) {
      if (typeof key === "string" && replacements.has(key) && "value" in descriptor) {
        Object.defineProperty(output, key, {
          ...descriptor,
          value: replacements.get(key),
        });
        copiedReplacements.add(key);
      } else {
        Object.defineProperty(output, key, descriptor);
      }
    }
  }
  for (const [key, replacement] of replacements) {
    if (!copiedReplacements.has(key)) {
      Object.defineProperty(output, key, {
        configurable: true,
        enumerable: true,
        value: replacement,
        writable: true,
      });
    }
  }
  return output;
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

const makeStrictJsonObjectKeywordGuard = (
  root: SchemaAST.AST,
  side: StrictJsonGuardSide,
  mode: StrictJsonGuardMode,
) => {
  const compiled = new Map<SchemaAST.AST, StrictJsonObjectKeywordGuard>();

  const compile = (ast: SchemaAST.AST): StrictJsonObjectKeywordGuard => {
    const cached = compiled.get(ast);
    if (cached !== undefined) {
      return cached;
    }

    let implementation: StrictJsonObjectKeywordGuard = (value) => value;
    const guard: StrictJsonObjectKeywordGuard = (value, path) => implementation(value, path);
    compiled.set(ast, guard);

    if (SchemaAST.isObjectKeyword(ast)) {
      implementation = (value, path) => {
        const materialized = materializeStrictJson(value);
        if (Result.isFailure(materialized)) {
          throw rebaseStrictJsonMaterializationError(materialized.failure, path);
        }
        return mode === "snapshot" ? materialized.success : value;
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
        is: Schema.is(
          Schema.make<Schema.Codec<unknown, unknown, never, never>>(
            side === "encoded" ? SchemaAST.toEncoded(member) : member,
          ),
        ),
        guard: compile(member),
      }));
      implementation = (value, path) => {
        assertNoAccessorProperties(value, path);
        const member = members.find((candidate) => candidate.is(value));
        return member === undefined ? value : member.guard(value, path);
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
          Schema.make<Schema.Codec<unknown, unknown, never, never>>(
            side === "encoded" ? SchemaAST.toEncoded(index.parameter) : index.parameter,
          ),
        ),
        guard: compile(index.type),
      }));
      implementation = (value, path) => {
        if (value === null || (typeof value !== "object" && typeof value !== "function")) {
          return value;
        }
        const enumerableValues = new Map<string, unknown>();
        const replacements = mode === "snapshot" ? new Map<string, unknown>() : undefined;
        const readProperty = (name: string, propertyPath: string): OwnDataProperty =>
          enumerableValues.has(name)
            ? { present: true, value: enumerableValues.get(name) }
            : readOwnDataProperty(value, name, propertyPath);
        for (const property of properties) {
          const propertyPath = appendStrictJsonPropertyPath(path, property.name);
          const propertyValue = readProperty(property.name, propertyPath);
          if (propertyValue.present) {
            enumerableValues.set(property.name, propertyValue.value);
            const snapshot = property.guard(propertyValue.value, propertyPath);
            if (replacements !== undefined && snapshot !== propertyValue.value) {
              replacements.set(property.name, snapshot);
            }
          }
        }
        let keys: ReadonlyArray<string> = [];
        if (indexes.length > 0) {
          try {
            keys = Object.keys(value);
          } catch {
            throw strictJsonReflectionFailure(path);
          }
          for (const key of keys) {
            const propertyPath = appendStrictJsonPropertyPath(path, key);
            const propertyValue = readProperty(key, propertyPath);
            if (propertyValue.present) {
              enumerableValues.set(key, propertyValue.value);
            }
          }
        }
        for (const key of keys) {
          const index = indexes.find((candidate) => candidate.accepts(key));
          if (index !== undefined) {
            const propertyPath = appendStrictJsonPropertyPath(path, key);
            const propertyValue = readProperty(key, propertyPath);
            if (propertyValue.present) {
              const snapshot = index.guard(propertyValue.value, propertyPath);
              if (replacements !== undefined && snapshot !== propertyValue.value) {
                replacements.set(key, snapshot);
              }
            }
          }
        }
        return replacements === undefined || replacements.size === 0
          ? value
          : cloneWithReplacements(value, replacements);
      };
      return guard;
    }

    if (SchemaAST.isArrays(ast)) {
      const elements = ast.elements.map(compile);
      const rest = ast.rest.map(compile);
      implementation = (value, path) => {
        if (!Array.isArray(value)) {
          return value;
        }
        const [head, ...tail] = rest;
        const tailThreshold = readArrayLength(value, path) - tail.length;
        const replacements = mode === "snapshot" ? new Map<string, unknown>() : undefined;
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
            const snapshot = item?.(entry.value, itemPath) ?? entry.value;
            if (replacements !== undefined && snapshot !== entry.value) {
              replacements.set(String(index), snapshot);
            }
          }
        }
        return replacements === undefined || replacements.size === 0
          ? value
          : cloneWithReplacements(value, replacements);
      };
    }

    return guard;
  };

  const guard = compile(root);
  return (value: unknown): unknown => guard(value, "$");
};

export const schemaAstContainsObjectKeyword = (root: SchemaAST.AST): boolean => {
  const visited = new Set<SchemaAST.AST>();
  const visit = (ast: SchemaAST.AST): boolean => {
    if (visited.has(ast)) {
      return false;
    }
    visited.add(ast);
    if (SchemaAST.isObjectKeyword(ast)) {
      return true;
    }
    if (SchemaAST.isSuspend(ast) && visit(ast.thunk())) {
      return true;
    }
    if (
      SchemaAST.isObjects(ast) &&
      (ast.propertySignatures.some((property) => visit(property.type)) ||
        ast.indexSignatures.some((index) => visit(index.parameter) || visit(index.type)))
    ) {
      return true;
    }
    if (SchemaAST.isArrays(ast) && (ast.elements.some(visit) || ast.rest.some(visit))) {
      return true;
    }
    if (SchemaAST.isUnion(ast) && ast.types.some(visit)) {
      return true;
    }
    return ast.encoding?.some((link) => visit(link.to)) ?? false;
  };
  return visit(root);
};

export type StrictJsonSchemaGuard = (
  value: unknown,
) => Result.Result<void, StrictJsonMaterializationError>;

export type StrictJsonSchemaSnapshot = (
  value: unknown,
) => Result.Result<unknown, StrictJsonMaterializationError>;

export const makeStrictJsonSchemaSnapshot = (
  root: SchemaAST.AST,
  side: "decoded" | "encoded" = "decoded",
): StrictJsonSchemaSnapshot => {
  const snapshot = makeStrictJsonObjectKeywordGuard(root, side, "snapshot");
  return (value) =>
    Result.try({
      try: () => snapshot(value),
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

export const makeStrictJsonSchemaGuard = (
  root: SchemaAST.AST,
  side: "decoded" | "encoded" = "decoded",
): StrictJsonSchemaGuard => {
  const guard = makeStrictJsonObjectKeywordGuard(root, side, "validate");
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
  const encodeRaw = Schema.encodeUnknownSync(schema);
  const encodedSchema = Schema.make<Schema.Codec<unknown, unknown, never, never>>(
    SchemaAST.toEncoded(schema.ast),
  );
  const encodeJson = Schema.encodeUnknownSync(Schema.toCodecJson(encodedSchema));
  const normalize = makeSchemaJsonNormalizer(codec.ast);
  const containsObjectKeyword = schemaAstContainsObjectKeyword(codec.ast);
  const strictDecodedObjectKeywordGuard = makeStrictJsonSchemaGuard(codec.ast);
  const strictEncodedObjectKeywordSnapshot = makeStrictJsonSchemaSnapshot(codec.ast, "encoded");
  const strictEncoded = (value: unknown): Schema.Json => {
    if (!containsObjectKeyword) {
      return strictJson(encode(value));
    }
    const guardResult = strictDecodedObjectKeywordGuard(value);
    if (Result.isFailure(guardResult)) {
      throw guardResult.failure;
    }
    const encodedValue = encodeRaw(value);
    const encodedSnapshotResult = strictEncodedObjectKeywordSnapshot(encodedValue);
    if (Result.isFailure(encodedSnapshotResult)) {
      throw encodedSnapshotResult.failure;
    }
    return strictJson(encodeJson(encodedSnapshotResult.success));
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
