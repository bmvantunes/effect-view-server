import { Schema, SchemaAST } from "effect";
import type { RowSchema } from "./topic-contract";

type TopicRegistry = Record<
  string,
  {
    readonly schema: RowSchema;
    readonly key?: string;
    readonly kafkaSource?: object | undefined;
    readonly grpcSource?: object | undefined;
    readonly source?: object | undefined;
  }
>;

type GrpcClientRegistry = Record<string, object>;

const schemaSnapshots = new WeakMap<RowSchema, RowSchema>();
const sourceDefinitionsWithAuthoredKeys = new WeakSet<object>();

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

export const viewServerRowSchemasShareOrigin = (left: RowSchema, right: RowSchema): boolean =>
  snapshotViewServerRowSchema(left) === snapshotViewServerRowSchema(right);

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

const snapshotOwnProperties = (value: object): { [key: PropertyKey]: unknown } => {
  const copied: { [key: PropertyKey]: unknown } = {};
  for (const property of Reflect.ownKeys(value)) {
    Object.defineProperty(copied, property, {
      configurable: true,
      enumerable: true,
      value: Reflect.get(value, property, value),
      writable: true,
    });
  }
  return copied;
};

export function snapshotViewServerGrpcClients<const Clients extends GrpcClientRegistry>(
  clients: Clients,
): Clients;
export function snapshotViewServerGrpcClients(clients: GrpcClientRegistry): GrpcClientRegistry {
  const snapshot: GrpcClientRegistry = {};
  for (const clientName of Object.keys(clients)) {
    Object.defineProperty(snapshot, clientName, {
      configurable: false,
      enumerable: true,
      value: Object.freeze(snapshotOwnProperties(clients[clientName]!)),
      writable: false,
    });
  }
  return Object.freeze(snapshot);
}

const snapshotSource = (source: unknown): unknown => {
  if (typeof source !== "object" || source === null) {
    return source;
  }
  const copied = snapshotOwnProperties(source);
  const routeBy = Object.hasOwn(copied, "routeBy") ? Reflect.get(copied, "routeBy") : undefined;
  const regions = Object.hasOwn(copied, "regions") ? Reflect.get(copied, "regions") : undefined;
  return Object.freeze({
    ...copied,
    ...(Array.isArray(routeBy) ? { routeBy: Object.freeze([...routeBy]) } : {}),
    ...(Array.isArray(regions) ? { regions: Object.freeze([...regions]) } : {}),
  });
};

const snapshotTopicDefinition = (definition: TopicRegistry[string]) => {
  const copied = snapshotOwnProperties(definition);
  const schema = copied["schema"];
  const kafkaSource = copied["kafkaSource"];
  const grpcSource = copied["grpcSource"];
  const source = copied["source"];
  const snapshot = Object.freeze({
    ...copied,
    ...(source === undefined ? {} : { key: "id", source }),
    ...(isViewServerRowSchema(schema) ? { schema: snapshotViewServerRowSchema(schema) } : {}),
    ...(!Object.hasOwn(copied, "kafkaSource") || kafkaSource === undefined
      ? {}
      : { kafkaSource: snapshotSource(kafkaSource) }),
    ...(!Object.hasOwn(copied, "grpcSource") || grpcSource === undefined
      ? {}
      : { grpcSource: snapshotSource(grpcSource) }),
  });
  if (source !== undefined && Object.hasOwn(copied, "key")) {
    sourceDefinitionsWithAuthoredKeys.add(snapshot);
  }
  return snapshot;
};

export const viewServerSourceDefinitionHadAuthoredKey = (definition: object): boolean =>
  sourceDefinitionsWithAuthoredKeys.has(definition);

export function snapshotViewServerTopics<const Topics extends TopicRegistry>(
  topics: Topics,
): Topics;
export function snapshotViewServerTopics(topics: TopicRegistry): TopicRegistry {
  const snapshot: TopicRegistry = Object.create(null);
  for (const topic of Object.keys(topics)) {
    Object.defineProperty(snapshot, topic, {
      configurable: false,
      enumerable: true,
      value: snapshotTopicDefinition(topics[topic]!),
      writable: false,
    });
  }
  return Object.freeze(snapshot);
}

export const isViewServerRowSchema = (schema: unknown): schema is RowSchema =>
  Schema.isSchema(schema) && "fields" in schema;
