import { Schema, SchemaAST } from "effect";
import { viewServerTopicNameIsReserved } from "./health-contract";
import type { RowSchema } from "./topic-contract";

type TopicRegistry = Record<
  string,
  {
    readonly schema: RowSchema;
    readonly source?: object | undefined;
  } & Readonly<Record<string, unknown>>
>;

const schemaSnapshots = new WeakMap<RowSchema, RowSchema>();

export function snapshotViewServerRowSchema<const S extends RowSchema>(schema: S): S;
export function snapshotViewServerRowSchema(schema: RowSchema): RowSchema {
  const cached = schemaSnapshots.get(schema);
  if (cached !== undefined) {
    return cached;
  }
  const fields = Object.freeze({ ...schema.fields });
  const ast = schema.ast;
  const snapshot = new Proxy(schema, {
    get: (target, property) => {
      if (property === "fields") {
        return fields;
      }
      if (property === "ast") {
        return ast;
      }
      return Reflect.get(target, property, target);
    },
    set: () => false,
    defineProperty: () => false,
    deleteProperty: () => false,
    setPrototypeOf: () => false,
    preventExtensions: () => false,
  });
  schemaSnapshots.set(schema, snapshot);
  schemaSnapshots.set(snapshot, snapshot);
  return snapshot;
}

const rowSchemaObjectsAst = (schema: RowSchema): SchemaAST.Objects | undefined => {
  if (SchemaAST.isObjects(schema.ast)) {
    return schema.ast;
  }
  if (SchemaAST.isDeclaration(schema.ast) && schema.ast.typeParameters.length === 1) {
    const parameter = schema.ast.typeParameters[0];
    return parameter !== undefined && SchemaAST.isObjects(parameter) ? parameter : undefined;
  }
  return undefined;
};

export const viewServerRowSchemaFieldsMatchAst = (schema: RowSchema): boolean => {
  const objects = rowSchemaObjectsAst(schema);
  if (objects === undefined || objects.indexSignatures.length > 0) {
    return false;
  }
  const astFields = new Map<string, SchemaAST.AST>();
  for (const property of objects.propertySignatures) {
    if (typeof property.name !== "string" || astFields.has(property.name)) {
      return false;
    }
    astFields.set(property.name, property.type);
  }
  const fieldNames = Object.keys(schema.fields);
  if (fieldNames.length !== astFields.size) {
    return false;
  }
  for (const field of fieldNames) {
    const fieldSchema = schema.fields[field];
    if (!Schema.isSchema(fieldSchema) || fieldSchema.ast !== astFields.get(field)) {
      return false;
    }
  }
  return true;
};

const snapshotOwnProperties = (topic: string, value: object): { [key: PropertyKey]: unknown } => {
  const copied: { [key: PropertyKey]: unknown } = {};
  for (const property of Reflect.ownKeys(value)) {
    if (viewServerTopicDefinitionPropertyIsIntrinsic(value, property)) {
      continue;
    }
    if (typeof value === "function" && property !== "schema" && property !== "source") {
      throw new Error(
        `View Server topic ${topic} contains unsupported property: ${String(property)}.`,
      );
    }
    Object.defineProperty(copied, property, {
      configurable: true,
      enumerable: true,
      value: Reflect.get(value, property, value),
      writable: true,
    });
  }
  return copied;
};

export const viewServerTopicDefinitionPropertyIsIntrinsic = (
  value: object,
  property: PropertyKey,
): boolean => {
  if (typeof value !== "function") {
    return false;
  }
  const descriptor = Object.getOwnPropertyDescriptor(value, property);
  if (descriptor === undefined || descriptor.enumerable || !("value" in descriptor)) {
    return false;
  }
  if (property === "length") {
    return (
      typeof descriptor.value === "number" &&
      descriptor.writable === false &&
      descriptor.configurable === true
    );
  }
  if (property === "name") {
    return (
      typeof descriptor.value === "string" &&
      descriptor.writable === false &&
      descriptor.configurable === true
    );
  }
  if (property === "arguments" || property === "caller") {
    return (
      descriptor.value === null &&
      descriptor.writable === false &&
      descriptor.configurable === false
    );
  }
  if (
    property !== "prototype" ||
    descriptor.value === null ||
    typeof descriptor.value !== "object" ||
    descriptor.configurable
  ) {
    return false;
  }
  const functionTag = Object.prototype.toString.call(value);
  const prototypeTag = Object.prototype.toString.call(descriptor.value);
  if (
    (functionTag === "[object GeneratorFunction]" && prototypeTag === "[object Generator]") ||
    (functionTag === "[object AsyncGeneratorFunction]" &&
      prototypeTag === "[object AsyncGenerator]")
  ) {
    return true;
  }
  try {
    Reflect.construct(Object, [], value);
  } catch {
    return false;
  }
  return Reflect.get(descriptor.value, "constructor", descriptor.value) === value;
};

const copySnapshotProperties = (
  target: object,
  properties: { [key: PropertyKey]: unknown },
): void => {
  for (const property of Reflect.ownKeys(properties)) {
    Object.defineProperty(target, property, {
      configurable: true,
      enumerable: true,
      value: Reflect.get(properties, property, properties),
      writable: true,
    });
  }
};

const snapshotTopicDefinition = (topic: string, definition: unknown) => {
  if (definition === null || (typeof definition !== "object" && typeof definition !== "function")) {
    throw new Error(`View Server topic ${topic} row schema must be an Effect Schema Struct.`);
  }
  const copied = snapshotOwnProperties(topic, definition);
  const schema = copied["schema"];
  if (isViewServerRowSchema(schema)) {
    copied["schema"] = snapshotViewServerRowSchema(schema);
  }
  if (typeof definition === "function") {
    const callableTarget = function (this: unknown, ...arguments_: ReadonlyArray<unknown>) {
      return Reflect.apply(definition, this, arguments_);
    };
    if (Object.hasOwn(definition, "prototype")) {
      Object.defineProperty(callableTarget, "prototype", {
        value: Reflect.get(definition, "prototype", definition),
      });
    }
    let callableSnapshot: typeof callableTarget;
    callableSnapshot = new Proxy(callableTarget, {
      construct: (_target, arguments_, newTarget) =>
        Reflect.construct(
          definition,
          arguments_,
          newTarget === callableSnapshot ? definition : newTarget,
        ),
    });
    copySnapshotProperties(callableSnapshot, copied);
    return Object.freeze(callableSnapshot);
  }
  return Object.freeze({ ...copied });
};

export function snapshotViewServerTopics<const Topics extends TopicRegistry>(
  topics: Topics,
): Topics;
export function snapshotViewServerTopics(topics: TopicRegistry): TopicRegistry {
  const snapshot: TopicRegistry = Object.create(null);
  for (const topic of Object.keys(topics)) {
    if (viewServerTopicNameIsReserved(topic)) {
      throw new Error(`View Server topic name is reserved for system health streams: ${topic}`);
    }
    Object.defineProperty(snapshot, topic, {
      configurable: false,
      enumerable: true,
      value: snapshotTopicDefinition(topic, topics[topic]),
      writable: false,
    });
  }
  return Object.freeze(snapshot);
}

export const isViewServerRowSchema = (schema: unknown): schema is RowSchema =>
  Schema.isSchema(schema) && "fields" in schema;
