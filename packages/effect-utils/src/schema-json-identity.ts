import {
  Cause,
  Chunk,
  Exit,
  HashMap,
  HashSet,
  Option,
  Redacted,
  Result,
  Schema,
  SchemaAST,
} from "effect";
import {
  materializeStrictJson,
  StrictJsonMaterializationError,
} from "./strict-json-materialization";

type ValueSchema<Type = unknown> = Schema.Codec<Type, unknown, never, never>;
type JsonNormalizer = (value: Schema.Json) => Schema.Json;

export type StrictJsonSchemaCodec<Type = unknown> = {
  readonly codec: Schema.Codec<Type, Schema.Json, never, never>;
  readonly encodedCodec: Schema.Codec<unknown, Schema.Json, never, never>;
  readonly hasObjectKeyword: boolean;
  readonly strictJson: StrictJsonSchemaGuard;
  readonly strictJsonSnapshot: StrictJsonSchemaSnapshot;
  readonly strictEncodedJson: StrictJsonSchemaSnapshot;
  readonly strictEncoded: (
    value: unknown,
  ) => Result.Result<
    (encodedValue: unknown) => Result.Result<unknown, StrictJsonMaterializationError>,
    StrictJsonMaterializationError
  >;
};

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
    const id = Reflect.get(representation, "id");
    return typeof id === "string" && id.startsWith("effect/schema/")
      ? `effect/${id.slice("effect/schema/".length)}`
      : undefined;
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

type IndexSignatureParameter = SchemaAST.IndexSignature["parameter"];

const numberIndexKey = /^(?:[+-]?\d*\.?\d+(?:[Ee][+-]?\d+)?|Infinity|-Infinity|NaN)$/;

const indexSignatureAcceptsPropertyKey = (
  key: string,
  parameter: IndexSignatureParameter,
): boolean => {
  if (SchemaAST.isUnion(parameter)) {
    return parameter.types.some((member) => indexSignatureAcceptsPropertyKey(key, member));
  }
  const encodedParameter = SchemaAST.toEncoded(parameter);
  const accepts = Schema.is(
    Schema.make<Schema.Codec<unknown, unknown, never, never>>(encodedParameter),
  );
  if (SchemaAST.isNumber(encodedParameter)) {
    return numberIndexKey.test(key) && accepts(globalThis.Number(key));
  }
  return accepts(key);
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
        accepts: (key: string) => indexSignatureAcceptsPropertyKey(key, index.parameter),
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

const strictJsonUnsupportedSchema = (path: string): StrictJsonMaterializationError =>
  StrictJsonMaterializationError.make({
    path,
    reason: "unsupported-schema",
    message: `Cannot safely validate ObjectKeyword data inside an unknown schema declaration at ${path}.`,
  });

// Reflection is an input boundary. Keep hostile iterators and union graphs from
// turning one validation into an unbounded allocation or traversal.
const strictJsonMaxIterableEntries = 10_000;
const strictJsonMaxGraphEntries = 10_000;

type OwnDataProperty =
  | { readonly present: false }
  | { readonly present: true; readonly value: unknown };
type StrictJsonKey = string | symbol;

const readOwnDataProperty = (
  value: object,
  key: StrictJsonKey,
  path: string,
  requireEnumerable = true,
): OwnDataProperty => {
  let descriptor: PropertyDescriptor | undefined;
  try {
    descriptor = Object.getOwnPropertyDescriptor(value, key);
  } catch {
    throw strictJsonReflectionFailure(path);
  }
  if (descriptor === undefined) {
    return { present: false };
  }
  if (requireEnumerable && descriptor.enumerable !== true) {
    throw strictJsonNonEnumerableProperty(path);
  }
  if (!("value" in descriptor)) {
    throw strictJsonAccessorProperty(path);
  }
  return { present: true, value: descriptor.value };
};

const readVisibleDataProperty = (
  value: object,
  key: StrictJsonKey,
  path: string,
): OwnDataProperty => {
  let current: object | null = value;
  while (current !== null) {
    const property = readOwnDataProperty(current, key, path, false);
    if (property.present) {
      return property;
    }
    try {
      current = Object.getPrototypeOf(current);
    } catch {
      throw strictJsonReflectionFailure(path);
    }
  }
  return { present: false };
};

const readIterableValues = (value: object, path: string): ReadonlyArray<unknown> => {
  const iteratorPath = strictJsonPropertyPath(path, Symbol.iterator);
  const iteratorProperty = readVisibleDataProperty(value, Symbol.iterator, iteratorPath);
  if (!iteratorProperty.present || typeof iteratorProperty.value !== "function") {
    throw strictJsonReflectionFailure(iteratorPath);
  }
  let iterator: unknown;
  try {
    iterator = Function.prototype.call.call(iteratorProperty.value, value);
  } catch {
    throw strictJsonReflectionFailure(path);
  }
  if (iterator === null || (typeof iterator !== "object" && typeof iterator !== "function")) {
    throw strictJsonReflectionFailure(iteratorPath);
  }
  const nextProperty = readVisibleDataProperty(iterator, "next", `${iteratorPath}.next`);
  if (!nextProperty.present || typeof nextProperty.value !== "function") {
    throw strictJsonReflectionFailure(`${iteratorPath}.next`);
  }
  const values: Array<unknown> = [];
  let index = 0;
  while (true) {
    let result: unknown;
    try {
      result = Function.prototype.call.call(nextProperty.value, iterator);
    } catch {
      throw strictJsonReflectionFailure(path);
    }
    if (result === null || (typeof result !== "object" && typeof result !== "function")) {
      throw strictJsonReflectionFailure(`${iteratorPath}[${index}]`);
    }
    const resultPath = `${iteratorPath}[${index}]`;
    const doneProperty = readVisibleDataProperty(result, "done", `${resultPath}.done`);
    if (doneProperty.present && Boolean(doneProperty.value)) {
      return values;
    }
    if (index >= strictJsonMaxIterableEntries) {
      throw strictJsonReflectionFailure(path);
    }
    const valueProperty = readVisibleDataProperty(result, "value", `${resultPath}.value`);
    values.push(valueProperty.present ? valueProperty.value : undefined);
    index += 1;
  }
};

type CapturedOwnProperties = {
  readonly keys: ReadonlyArray<StrictJsonKey>;
  readonly descriptors: ReadonlyMap<StrictJsonKey, PropertyDescriptor>;
  readonly prototype: object | null;
};

const captureOwnProperties = (value: object, path: string): CapturedOwnProperties => {
  let keys: ReadonlyArray<StrictJsonKey>;
  try {
    keys = Reflect.ownKeys(value);
  } catch {
    throw strictJsonReflectionFailure(path);
  }
  const descriptors = new Map<StrictJsonKey, PropertyDescriptor>();
  for (const key of keys) {
    let descriptor: PropertyDescriptor | undefined;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      throw strictJsonReflectionFailure(strictJsonPropertyPath(path, key));
    }
    if (descriptor !== undefined) {
      descriptors.set(key, descriptor);
    }
  }
  let prototype: object | null;
  try {
    prototype = Object.getPrototypeOf(value);
  } catch {
    throw strictJsonReflectionFailure(path);
  }
  return { keys, descriptors, prototype };
};

const readCapturedDataProperty = (
  captured: CapturedOwnProperties,
  key: StrictJsonKey,
  path: string,
): OwnDataProperty => {
  const descriptor = captured.descriptors.get(key);
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
  captured: CapturedOwnProperties,
): object => {
  const output = Array.isArray(value) ? [] : Object.create(captured.prototype);
  const copiedReplacements = new Set<string>();
  for (const key of captured.keys) {
    const descriptor = captured.descriptors.get(key);
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

const arrayLengthFromDescriptor = (
  descriptor: PropertyDescriptor | undefined,
  path: string,
): number => {
  if (
    descriptor === undefined ||
    !("value" in descriptor) ||
    typeof descriptor.value !== "number" ||
    !Number.isSafeInteger(descriptor.value) ||
    descriptor.value < 0 ||
    descriptor.value > 0xffffffff
  ) {
    throw strictJsonReflectionFailure(`${path}.length`);
  }
  return descriptor.value;
};

const readArrayLength = (value: Array<unknown>, path: string): number => {
  let descriptor: PropertyDescriptor | undefined;
  try {
    descriptor = Object.getOwnPropertyDescriptor(value, "length");
  } catch {
    throw strictJsonReflectionFailure(`${path}.length`);
  }
  return arrayLengthFromDescriptor(descriptor, path);
};

const readCapturedArrayLength = (captured: CapturedOwnProperties, path: string): number => {
  return arrayLengthFromDescriptor(captured.descriptors.get("length"), path);
};

const assertNoOwnIteratorOverride = (value: object, path: string): void => {
  const iteratorPath = strictJsonPropertyPath(path, Symbol.iterator);
  const iteratorProperty = readOwnDataProperty(value, Symbol.iterator, iteratorPath, false);
  if (iteratorProperty.present) {
    throw strictJsonReflectionFailure(iteratorPath);
  }
};

// Schema.is recursively walks union members. For recursive unions, give it one
// descriptor-captured graph so matching cannot rescan stateful proxies at every depth.
const makeAccessorSafeSnapshot = (
  value: unknown,
  path: string,
  snapshots: WeakMap<object, unknown>,
  state: { count: number } = { count: 0 },
): unknown => {
  if (value === null || (typeof value !== "object" && typeof value !== "function")) {
    return value;
  }
  const existing = snapshots.get(value);
  if (existing !== undefined) {
    return existing;
  }
  if (state.count >= strictJsonMaxGraphEntries) {
    throw strictJsonReflectionFailure(path);
  }
  state.count += 1;
  const isArray = Array.isArray(value);
  const isCallable = typeof value === "function";
  let isMap = false;
  let isSet = false;
  let prototype: object | null = null;
  try {
    isMap = value instanceof Map;
    isSet = value instanceof Set;
    if (!isArray && !isCallable && !isMap && !isSet) {
      prototype = Object.getPrototypeOf(value);
    }
  } catch {
    throw strictJsonReflectionFailure(path);
  }
  const snapshot = isArray ? [] : isCallable || isMap || isSet ? value : Object.create(prototype);
  snapshots.set(value, snapshot);

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
    const childSnapshot = makeAccessorSafeSnapshot(
      descriptor.value,
      propertyPath,
      snapshots,
      state,
    );
    if (snapshot !== value) {
      Object.defineProperty(snapshot, key, {
        ...descriptor,
        value: childSnapshot,
      });
    }
  }
  return snapshot;
};

const assertNoAccessorProperties = (
  value: unknown,
  path: string,
  active: WeakSet<object>,
  state: { count: number } = { count: 0 },
): void => {
  if (value === null || (typeof value !== "object" && typeof value !== "function")) {
    return;
  }
  if (active.has(value)) {
    return;
  }
  if (state.count >= strictJsonMaxGraphEntries) {
    throw strictJsonReflectionFailure(path);
  }
  state.count += 1;
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
    assertNoAccessorProperties(descriptor.value, propertyPath, active, state);
  }
};

const guardDeclarationValue = (
  guard: StrictJsonObjectKeywordGuard,
  value: unknown,
  path: string,
): unknown => guard(value, path);

export function schemaAstContainsObjectKeyword(root: SchemaAST.AST): boolean {
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
    if (SchemaAST.isDeclaration(ast) && ast.typeParameters.some(visit)) {
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
}

const schemaAstContainsUnion = (root: SchemaAST.AST, side: StrictJsonGuardSide): boolean => {
  const visited = new Set<SchemaAST.AST>();
  const visit = (ast: SchemaAST.AST): boolean => {
    if (visited.has(ast)) {
      return false;
    }
    visited.add(ast);
    if (SchemaAST.isUnion(ast)) {
      return true;
    }
    if (SchemaAST.isSuspend(ast) && visit(ast.thunk())) {
      return true;
    }
    if (SchemaAST.isDeclaration(ast) && ast.typeParameters.some(visit)) {
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
    return side === "encoded" && (ast.encoding?.some((link) => visit(link.to)) ?? false);
  };
  return visit(root);
};

const schemaAstReferences = (
  root: SchemaAST.AST,
  target: SchemaAST.AST,
  side: StrictJsonGuardSide,
  visited: Set<SchemaAST.AST> = new Set(),
): boolean => {
  if (root === target) {
    return true;
  }
  if (visited.has(root)) {
    return false;
  }
  visited.add(root);
  if (SchemaAST.isSuspend(root) && schemaAstReferences(root.thunk(), target, side, visited)) {
    return true;
  }
  if (
    SchemaAST.isDeclaration(root) &&
    root.typeParameters.some((parameter) => schemaAstReferences(parameter, target, side, visited))
  ) {
    return true;
  }
  if (
    SchemaAST.isObjects(root) &&
    (root.propertySignatures.some((property) =>
      schemaAstReferences(property.type, target, side, visited),
    ) ||
      root.indexSignatures.some(
        (index) =>
          schemaAstReferences(index.parameter, target, side, visited) ||
          schemaAstReferences(index.type, target, side, visited),
      ))
  ) {
    return true;
  }
  if (
    SchemaAST.isArrays(root) &&
    (root.elements.some((element) => schemaAstReferences(element, target, side, visited)) ||
      root.rest.some((rest) => schemaAstReferences(rest, target, side, visited)))
  ) {
    return true;
  }
  if (
    SchemaAST.isUnion(root) &&
    root.types.some((type) => schemaAstReferences(type, target, side, visited))
  ) {
    return true;
  }
  return (
    side === "encoded" &&
    (root.encoding?.some((link) => schemaAstReferences(link.to, target, side, visited)) ?? false)
  );
};

const schemaAstContainsRecursiveUnion = (
  root: SchemaAST.AST,
  side: StrictJsonGuardSide,
  recursiveUnionCache: Map<SchemaAST.AST, boolean>,
): boolean => {
  const visited = new Set<SchemaAST.AST>();
  const visit = (ast: SchemaAST.AST): boolean => {
    if (visited.has(ast)) {
      return false;
    }
    visited.add(ast);
    if (SchemaAST.isUnion(ast)) {
      const cached = recursiveUnionCache.get(ast);
      if (cached !== undefined) {
        return cached;
      }
      const recursive = ast.types.some((member) => schemaAstReferences(member, ast, side));
      recursiveUnionCache.set(ast, recursive);
      if (recursive) {
        return true;
      }
    }
    if (SchemaAST.isSuspend(ast) && visit(ast.thunk())) {
      return true;
    }
    if (SchemaAST.isDeclaration(ast) && ast.typeParameters.some(visit)) {
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
    return side === "encoded" && (ast.encoding?.some((link) => visit(link.to)) ?? false);
  };
  return visit(root);
};

const knownDeclarationExpected = new Map([
  ["effect/Cause", "Cause"],
  ["effect/CauseReason", "Cause.Failure"],
  ["effect/Chunk", "Chunk"],
  ["effect/Exit", "Exit"],
  ["effect/HashMap", "HashMap"],
  ["effect/HashSet", "HashSet"],
  ["effect/Option", "Option"],
  ["effect/ReadonlyMap", "ReadonlyMap"],
  ["effect/ReadonlySet", "ReadonlySet"],
  ["effect/Redacted", "Redacted"],
  ["effect/Result", "Result"],
]);

const declarationRunSource = (ast: SchemaAST.Declaration): string | undefined => {
  try {
    const run = ast.run(ast.typeParameters);
    return typeof run === "function" ? String(run) : undefined;
  } catch {
    return undefined;
  }
};

// Effect beta106 does not expose a stable runtime brand for declaration ASTs. Keep the
// built-in allow-list conservative: the generated `run` implementation plus annotations
// must match a canonical factory, otherwise a copied/spoofed declaration fails closed.
const knownDeclarationRunSources = new Map<string, string | undefined>();
const registerKnownDeclarationRunSource = (tag: string, ast: SchemaAST.Declaration): void => {
  knownDeclarationRunSources.set(tag, declarationRunSource(ast));
};
registerKnownDeclarationRunSource("effect/Cause", Schema.Cause(Schema.String, Schema.String).ast);
registerKnownDeclarationRunSource(
  "effect/CauseReason",
  Schema.CauseReason(Schema.String, Schema.String).ast,
);
registerKnownDeclarationRunSource("effect/Chunk", Schema.Chunk(Schema.String).ast);
registerKnownDeclarationRunSource(
  "effect/Exit",
  Schema.Exit(Schema.String, Schema.String, Schema.String).ast,
);
registerKnownDeclarationRunSource(
  "effect/HashMap",
  Schema.HashMap(Schema.String, Schema.String).ast,
);
registerKnownDeclarationRunSource("effect/HashSet", Schema.HashSet(Schema.String).ast);
registerKnownDeclarationRunSource("effect/Option", Schema.Option(Schema.String).ast);
registerKnownDeclarationRunSource(
  "effect/ReadonlyMap",
  Schema.ReadonlyMap(Schema.String, Schema.String).ast,
);
registerKnownDeclarationRunSource("effect/ReadonlySet", Schema.ReadonlySet(Schema.String).ast);
registerKnownDeclarationRunSource("effect/Redacted", Schema.Redacted(Schema.String).ast);
registerKnownDeclarationRunSource("effect/Result", Schema.Result(Schema.String, Schema.String).ast);

const isKnownDeclaration = (ast: SchemaAST.Declaration, tag: unknown): boolean => {
  if (typeof tag !== "string" || !knownDeclarationExpected.has(tag)) {
    return false;
  }
  return (
    knownDeclarationExpected.get(tag) === ast.annotations?.["expected"] &&
    knownDeclarationRunSources.get(tag) === declarationRunSource(ast) &&
    typeof ast.annotations?.["toCode"] === "function" &&
    typeof ast.annotations?.["toArbitrary"] === "function" &&
    typeof ast.annotations?.["toEquivalence"] === "function" &&
    typeof ast.annotations?.["toFormatter"] === "function" &&
    (typeof ast.annotations?.["toCodec"] === "function" ||
      typeof ast.annotations?.["toCodecJson"] === "function")
  );
};

const isSchemaClassDeclaration = (ast: SchemaAST.Declaration): boolean => {
  const constructor = ast.annotations?.["~constructor"];
  if (typeof constructor !== "function") {
    return false;
  }
  try {
    const descriptor = Reflect.apply(constructor, undefined, [ast.typeParameters]);
    return (
      descriptor !== null &&
      typeof descriptor === "object" &&
      typeof Reflect.get(descriptor, "isConstructed") === "function" &&
      Reflect.get(descriptor, "link") !== undefined
    );
  } catch {
    return false;
  }
};

const makeStrictJsonObjectKeywordGuard = (
  root: SchemaAST.AST,
  side: StrictJsonGuardSide,
  mode: StrictJsonGuardMode,
) => {
  const compiled = new Map<SchemaAST.AST, StrictJsonObjectKeywordGuard>();
  const recursiveUnionCache = new Map<SchemaAST.AST, boolean>();
  let accessorSafeSnapshots: WeakMap<object, unknown> | undefined;

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

    if (SchemaAST.isDeclaration(ast)) {
      // Declaration ASTs retain type parameters but erase their runtime shape. Inspect the
      // built-in Effect representations explicitly so ObjectKeyword values cannot hide inside
      // collections, causes, or result wrappers.
      const tag = typeConstructorTag(ast);
      if (
        !isKnownDeclaration(ast, tag) &&
        !isSchemaClassDeclaration(ast) &&
        schemaAstContainsObjectKeyword(ast)
      ) {
        implementation = (_value, path) => {
          throw strictJsonUnsupportedSchema(path);
        };
        return guard;
      }
      if (side === "encoded" && ast.encoding !== undefined && ast.encoding.length > 0) {
        implementation = compile(ast.encoding[ast.encoding.length - 1]!.to);
        return guard;
      }
      const typeParameters = ast.typeParameters.map(compile);
      const typeParameter = (index: number): StrictJsonObjectKeywordGuard => typeParameters[index]!;
      const guardKnownCauseReason = (
        reason: unknown,
        path: string,
        errorGuard: StrictJsonObjectKeywordGuard,
        defectGuard: StrictJsonObjectKeywordGuard,
      ): Cause.Reason<unknown> => {
        if (reason === null || (typeof reason !== "object" && typeof reason !== "function")) {
          throw strictJsonReflectionFailure(path);
        }
        const tagProperty = readVisibleDataProperty(reason, "_tag", `${path}._tag`);
        if (!tagProperty.present) {
          throw strictJsonReflectionFailure(path);
        }
        if (tagProperty.value === "Fail") {
          const errorProperty = readOwnDataProperty(reason, "error", `${path}.error`);
          const error = errorProperty.present ? errorProperty.value : undefined;
          const snapshot = errorProperty.present
            ? guardDeclarationValue(errorGuard, error, `${path}.error`)
            : error;
          return Cause.makeFailReason(mode === "snapshot" ? snapshot : error);
        }
        if (tagProperty.value === "Die") {
          const defectProperty = readOwnDataProperty(reason, "defect", `${path}.defect`);
          const defect = defectProperty.present ? defectProperty.value : undefined;
          const snapshot = defectProperty.present
            ? guardDeclarationValue(defectGuard, defect, `${path}.defect`)
            : defect;
          return Cause.makeDieReason(mode === "snapshot" ? snapshot : defect);
        }
        if (tagProperty.value === "Interrupt") {
          const fiberIdProperty = readOwnDataProperty(reason, "fiberId", `${path}.fiberId`);
          const fiberId = fiberIdProperty.present ? fiberIdProperty.value : undefined;
          if (fiberId !== undefined && typeof fiberId !== "number") {
            throw strictJsonReflectionFailure(`${path}.fiberId`);
          }
          return Cause.makeInterruptReason(fiberId);
        }
        throw strictJsonReflectionFailure(path);
      };
      const guardCauseReason = (
        reason: unknown,
        path: string,
        errorGuard: StrictJsonObjectKeywordGuard,
        defectGuard: StrictJsonObjectKeywordGuard,
      ): unknown =>
        reason === null || (typeof reason !== "object" && typeof reason !== "function")
          ? reason
          : guardKnownCauseReason(reason, path, errorGuard, defectGuard);
      const guardCause = (
        value: unknown,
        path: string,
        errorGuard: StrictJsonObjectKeywordGuard,
        defectGuard: StrictJsonObjectKeywordGuard,
      ): { readonly value: unknown; readonly changed: boolean } => {
        if (!Cause.isCause(value)) {
          return { value, changed: false };
        }
        const reasonsProperty = readOwnDataProperty(value, "reasons", `${path}.reasons`);
        if (!reasonsProperty.present || !Array.isArray(reasonsProperty.value)) {
          return { value, changed: false };
        }
        const reasons = reasonsProperty.value;
        const capturedReasons = captureOwnProperties(reasons, `${path}.reasons`);
        const length = readCapturedArrayLength(capturedReasons, `${path}.reasons`);
        const snapshots: Array<Cause.Reason<unknown>> = [];
        let changed = false;
        for (let index = 0; index < length; index += 1) {
          const reasonPath = `${path}.reasons[${index}]`;
          const reasonProperty = readCapturedDataProperty(
            capturedReasons,
            String(index),
            reasonPath,
          );
          if (!reasonProperty.present) {
            throw strictJsonReflectionFailure(reasonPath);
          }
          const snapshot = guardKnownCauseReason(
            reasonProperty.value,
            reasonPath,
            errorGuard,
            defectGuard,
          );
          snapshots.push(snapshot);
          changed ||= snapshot !== reasonProperty.value;
        }
        return {
          value: mode === "snapshot" ? Cause.fromReasons(snapshots) : value,
          changed,
        };
      };

      if (tag === "effect/CauseReason") {
        implementation = (value, path) =>
          guardCauseReason(value, path, typeParameter(0), typeParameter(1));
        return guard;
      }

      if (tag === "effect/Cause") {
        implementation = (value, path) =>
          guardCause(value, path, typeParameter(0), typeParameter(1)).value;
        return guard;
      }

      if (tag === "effect/Option") {
        implementation = (value, path) => {
          if (!Option.isOption(value)) {
            return value;
          }
          const tagProperty = readVisibleDataProperty(value, "_tag", `${path}._tag`);
          if (!tagProperty.present || tagProperty.value !== "Some") {
            return value;
          }
          const optionValueProperty = readOwnDataProperty(value, "value", `${path}.value`);
          if (!optionValueProperty.present) {
            return value;
          }
          const optionValue = optionValueProperty.value;
          const snapshot = guardDeclarationValue(typeParameter(0), optionValue, `${path}.value`);
          if (mode !== "snapshot" || snapshot === optionValue) {
            return value;
          }
          return cloneWithReplacements(
            value,
            new Map([["value", snapshot]]),
            captureOwnProperties(value, path),
          );
        };
        return guard;
      }

      if (tag === "effect/Result") {
        implementation = (value, path) => {
          if (!Result.isResult(value)) {
            return value;
          }
          const tagProperty = readVisibleDataProperty(value, "_tag", `${path}._tag`);
          if (
            !tagProperty.present ||
            (tagProperty.value !== "Success" && tagProperty.value !== "Failure")
          ) {
            return value;
          }
          if (tagProperty.value === "Success") {
            const successProperty = readOwnDataProperty(value, "success", `${path}.success`);
            if (!successProperty.present) {
              return value;
            }
            const success = successProperty.value;
            const snapshot = guardDeclarationValue(typeParameter(0), success, `${path}.success`);
            if (mode !== "snapshot" || snapshot === success) {
              return value;
            }
            return cloneWithReplacements(
              value,
              new Map([["success", snapshot]]),
              captureOwnProperties(value, path),
            );
          }
          const failureProperty = readOwnDataProperty(value, "failure", `${path}.failure`);
          if (!failureProperty.present) {
            return value;
          }
          const failure = failureProperty.value;
          const snapshot = guardDeclarationValue(typeParameter(1), failure, `${path}.failure`);
          if (mode !== "snapshot" || snapshot === failure) {
            return value;
          }
          return cloneWithReplacements(
            value,
            new Map([["failure", snapshot]]),
            captureOwnProperties(value, path),
          );
        };
        return guard;
      }

      if (tag === "effect/Redacted") {
        implementation = (value, path) => {
          if (!Redacted.isRedacted(value)) {
            return value;
          }
          const labelProperty = readOwnDataProperty(value, "label", `${path}.label`);
          const redactedValue = Redacted.value(value);
          const snapshot = guardDeclarationValue(typeParameter(0), redactedValue, `${path}.value`);
          if (mode !== "snapshot" || snapshot === redactedValue) {
            return value;
          }
          return Redacted.make(
            snapshot,
            labelProperty.present && typeof labelProperty.value === "string"
              ? { label: labelProperty.value }
              : undefined,
          );
        };
        return guard;
      }

      if (tag === "effect/Exit") {
        implementation = (value, path) => {
          if (!Exit.isExit(value)) {
            return value;
          }
          const tagProperty = readVisibleDataProperty(value, "_tag", `${path}._tag`);
          if (
            !tagProperty.present ||
            (tagProperty.value !== "Success" && tagProperty.value !== "Failure")
          ) {
            return value;
          }
          const isSuccess = tagProperty.value === "Success";
          const payloadKey = isSuccess ? "value" : "cause";
          const payloadPath = `${path}.${payloadKey}`;
          const ownPayload = readOwnDataProperty(value, payloadKey, payloadPath);
          const payloadProperty = ownPayload.present
            ? ownPayload
            : readOwnDataProperty(value, "~effect/Effect/args", payloadPath);
          if (!payloadProperty.present) {
            return value;
          }
          if (isSuccess) {
            const success = payloadProperty.value;
            const snapshot = guardDeclarationValue(typeParameter(0), success, `${path}.value`);
            if (mode !== "snapshot" || snapshot === success) {
              return value;
            }
            return cloneWithReplacements(
              value,
              new Map([["value", snapshot]]),
              captureOwnProperties(value, path),
            );
          }
          const cause = guardCause(
            payloadProperty.value,
            `${path}.cause`,
            typeParameter(1),
            typeParameter(2),
          );
          if (mode !== "snapshot" || !cause.changed) {
            return value;
          }
          return cloneWithReplacements(
            value,
            new Map([["cause", cause.value]]),
            captureOwnProperties(value, path),
          );
        };
        return guard;
      }

      if (tag === "effect/ReadonlyMap") {
        implementation = (value, path) => {
          if (!(value instanceof globalThis.Map)) {
            return value;
          }
          assertNoOwnIteratorOverride(value, path);
          const snapshots: Array<readonly [unknown, unknown]> = [];
          let index = 0;
          for (const [key, entryValue] of globalThis.Map.prototype.entries.call(value)) {
            const entryPath = `${path}.entries[${index}]`;
            const keySnapshot = guardDeclarationValue(typeParameter(0), key, `${entryPath}[0]`);
            const valueSnapshot = guardDeclarationValue(
              typeParameter(1),
              entryValue,
              `${entryPath}[1]`,
            );
            snapshots.push([keySnapshot, valueSnapshot]);
            index += 1;
          }
          return mode === "snapshot" ? new Map(snapshots) : value;
        };
        return guard;
      }

      if (tag === "effect/ReadonlySet") {
        implementation = (value, path) => {
          if (!(value instanceof globalThis.Set)) {
            return value;
          }
          assertNoOwnIteratorOverride(value, path);
          const entries = globalThis.Set.prototype.values.call(value);
          const snapshots: Array<unknown> = [];
          let index = 0;
          for (const entry of entries) {
            const snapshot = guardDeclarationValue(
              typeParameter(0),
              entry,
              `${path}.values[${index}]`,
            );
            snapshots.push(snapshot);
            index += 1;
          }
          return mode === "snapshot" ? new Set(snapshots) : value;
        };
        return guard;
      }

      if (tag === "effect/HashMap") {
        implementation = (value, path) => {
          if (!HashMap.isHashMap(value)) {
            return value;
          }
          const snapshots: Array<readonly [unknown, unknown]> = [];
          for (const [index, entry] of Array.prototype.entries.call(
            readIterableValues(value, path),
          )) {
            const entryPath = `${path}.entries[${index}]`;
            if (!Array.isArray(entry)) {
              throw strictJsonReflectionFailure(entryPath);
            }
            const capturedEntry = captureOwnProperties(entry, entryPath);
            const length = readCapturedArrayLength(capturedEntry, entryPath);
            if (length < 2) {
              throw strictJsonReflectionFailure(entryPath);
            }
            const keyProperty = readCapturedDataProperty(capturedEntry, "0", `${entryPath}[0]`);
            const valueProperty = readCapturedDataProperty(capturedEntry, "1", `${entryPath}[1]`);
            if (!keyProperty.present || !valueProperty.present) {
              throw strictJsonReflectionFailure(entryPath);
            }
            const key = keyProperty.value;
            const entryValue = valueProperty.value;
            const keySnapshot = guardDeclarationValue(typeParameter(0), key, `${entryPath}[0]`);
            const valueSnapshot = guardDeclarationValue(
              typeParameter(1),
              entryValue,
              `${entryPath}[1]`,
            );
            snapshots.push([keySnapshot, valueSnapshot]);
          }
          return mode === "snapshot" ? HashMap.fromIterable(snapshots) : value;
        };
        return guard;
      }

      if (tag === "effect/HashSet") {
        implementation = (value, path) => {
          if (!HashSet.isHashSet(value)) {
            return value;
          }
          const snapshots: Array<unknown> = [];
          for (const [index, entry] of Array.prototype.entries.call(
            readIterableValues(value, path),
          )) {
            const snapshot = guardDeclarationValue(
              typeParameter(0),
              entry,
              `${path}.values[${index}]`,
            );
            snapshots.push(snapshot);
          }
          return mode === "snapshot" ? HashSet.fromIterable(snapshots) : value;
        };
        return guard;
      }

      if (tag === "effect/Chunk") {
        implementation = (value, path) => {
          if (!Chunk.isChunk(value)) {
            return value;
          }
          const snapshots: Array<unknown> = [];
          for (const [index, entry] of Array.prototype.entries.call(
            readIterableValues(value, path),
          )) {
            const snapshot = guardDeclarationValue(
              typeParameter(0),
              entry,
              `${path}.values[${index}]`,
            );
            snapshots.push(snapshot);
          }
          return mode === "snapshot" ? Chunk.fromIterable(snapshots) : value;
        };
        return guard;
      }
    }

    if (side === "encoded" && ast.encoding !== undefined && ast.encoding.length > 0) {
      implementation = compile(ast.encoding[ast.encoding.length - 1]!.to);
      return guard;
    }

    if (SchemaAST.isUnion(ast)) {
      let isRecursiveUnion = recursiveUnionCache.get(ast);
      if (isRecursiveUnion === undefined) {
        isRecursiveUnion = ast.types.some((member) => schemaAstReferences(member, ast, side));
        recursiveUnionCache.set(ast, isRecursiveUnion);
      }
      const members = ast.types.map((member) => ({
        is: Schema.is(
          Schema.make<Schema.Codec<unknown, unknown, never, never>>(
            side === "encoded" ? SchemaAST.toEncoded(member) : member,
          ),
        ),
        guard: compile(member),
      }));
      implementation = (value, path) => {
        let memberValue = value;
        if (
          isRecursiveUnion &&
          value !== null &&
          (typeof value === "object" || typeof value === "function")
        ) {
          const snapshots = accessorSafeSnapshots;
          if (snapshots === undefined || !snapshots.has(value)) {
            throw strictJsonReflectionFailure(path);
          }
          memberValue = snapshots.get(value);
        }
        const member = members.find((candidate) => candidate.is(memberValue));
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
        accepts: (key: string) => indexSignatureAcceptsPropertyKey(key, index.parameter),
        guard: compile(index.type),
      }));
      implementation = (value, path) => {
        if (value === null || (typeof value !== "object" && typeof value !== "function")) {
          return value;
        }
        const captured = mode === "snapshot" ? captureOwnProperties(value, path) : undefined;
        const enumerableValues = new Map<string, unknown>();
        const replacements = mode === "snapshot" ? new Map<string, unknown>() : undefined;
        const readProperty = (name: string, propertyPath: string): OwnDataProperty =>
          enumerableValues.has(name)
            ? { present: true, value: enumerableValues.get(name) }
            : captured === undefined
              ? readOwnDataProperty(value, name, propertyPath)
              : readCapturedDataProperty(captured, name, propertyPath);
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
            keys =
              captured === undefined
                ? Object.keys(value)
                : captured.keys.filter(
                    (key): key is string =>
                      typeof key === "string" && captured.descriptors.get(key)?.enumerable === true,
                  );
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
        return replacements === undefined
          ? value
          : cloneWithReplacements(value, replacements, captured!);
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
        const captured = mode === "snapshot" ? captureOwnProperties(value, path) : undefined;
        const length =
          captured === undefined
            ? readArrayLength(value, path)
            : readCapturedArrayLength(captured, path);
        const [head, ...tail] = rest;
        const tailThreshold = Math.max(length - tail.length, elements.length);
        const replacements = mode === "snapshot" ? new Map<string, unknown>() : undefined;
        for (let index = 0; index < tailThreshold + tail.length; index += 1) {
          const item =
            index < elements.length
              ? elements[index]
              : index >= tailThreshold
                ? tail[index - tailThreshold]
                : head;
          const itemPath = `${path}[${index}]`;
          const entry =
            captured === undefined
              ? readOwnDataProperty(value, String(index), itemPath)
              : readCapturedDataProperty(captured, String(index), itemPath);
          if (entry.present) {
            const snapshot = item?.(entry.value, itemPath) ?? entry.value;
            if (replacements !== undefined && snapshot !== entry.value) {
              replacements.set(String(index), snapshot);
            }
          }
        }
        return replacements === undefined
          ? value
          : cloneWithReplacements(value, replacements, captured!);
      };
    }

    return guard;
  };

  const guard = compile(root);
  const containsUnion = schemaAstContainsUnion(root, side);
  const containsRecursiveUnion = schemaAstContainsRecursiveUnion(root, side, recursiveUnionCache);
  return (value: unknown): unknown => {
    const previousAccessorSafeSnapshots = accessorSafeSnapshots;
    accessorSafeSnapshots = containsRecursiveUnion ? new WeakMap<object, unknown>() : undefined;
    try {
      if (accessorSafeSnapshots !== undefined) {
        makeAccessorSafeSnapshot(value, "$", accessorSafeSnapshots);
      } else if (containsUnion) {
        assertNoAccessorProperties(value, "$", new WeakSet());
      }
      return guard(value, "$");
    } finally {
      accessorSafeSnapshots = previousAccessorSafeSnapshots;
    }
  };
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

export const makeStrictJsonSchemaCodec = <Type>(
  schema: ValueSchema<Type>,
): StrictJsonSchemaCodec<Type> => {
  const codec = Schema.toCodecJson(schema);
  const encodedCodec = Schema.make<Schema.Codec<unknown, unknown, never, never>>(
    SchemaAST.toEncoded(schema.ast),
  );
  const strictJson = makeStrictJsonSchemaGuard(codec.ast);
  const strictJsonSnapshot = makeStrictJsonSchemaSnapshot(codec.ast);
  const strictEncodedJson = makeStrictJsonSchemaSnapshot(schema.ast, "encoded");
  return {
    codec,
    encodedCodec: Schema.toCodecJson(encodedCodec),
    hasObjectKeyword: schemaAstContainsObjectKeyword(codec.ast),
    strictJson,
    strictJsonSnapshot,
    strictEncodedJson,
    strictEncoded: (value) => {
      const guardResult = strictJson(value);
      if (Result.isFailure(guardResult)) {
        return Result.fail(guardResult.failure);
      }
      return Result.succeed((encodedValue: unknown) => strictEncodedJson(encodedValue));
    },
  };
};

export const makeSchemaJsonIdentity = <Type>(
  schema: ValueSchema<Type>,
): SchemaJsonIdentity<Type> => {
  const compiled = makeStrictJsonSchemaCodec(schema);
  const decode = Schema.decodeUnknownSync(compiled.codec);
  const encode = Schema.encodeUnknownSync(compiled.codec);
  const encodeRaw = Schema.encodeUnknownSync(schema);
  const encodeJson = Schema.encodeUnknownSync(compiled.encodedCodec);
  const strictEncoded = (value: unknown): Schema.Json => {
    if (!compiled.hasObjectKeyword) {
      return strictJson(encode(value));
    }
    const strictDecodedSnapshotResult = compiled.strictJsonSnapshot(value);
    if (Result.isFailure(strictDecodedSnapshotResult)) {
      throw strictDecodedSnapshotResult.failure;
    }
    const encodedValue = encodeRaw(strictDecodedSnapshotResult.success);
    const encodedSnapshotResult = compiled.strictEncodedJson(encodedValue);
    if (Result.isFailure(encodedSnapshotResult)) {
      throw encodedSnapshotResult.failure;
    }
    return strictJson(encodeJson(encodedSnapshotResult.success));
  };
  const normalize = makeSchemaJsonNormalizer(compiled.codec.ast);
  const canonicalJson = (value: unknown): Schema.Json => normalize(strictEncoded(value));
  const canonicalKey =
    SchemaAST.isString(compiled.codec.ast) &&
    compiled.codec.ast.encoding === undefined &&
    (compiled.codec.ast.checks?.length ?? 0) === 0
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
