import { fromBinary } from "@bufbuild/protobuf";
import type { DescMessage, MessageShape } from "@bufbuild/protobuf";
import { Effect, Schema, SchemaAST } from "effect";
import type { Config } from "effect";
import type { RowSchema, TopicRow } from "./topic-contract";

export type RuntimeValue<A> = A | Config.Config<A>;
export type RuntimeRegions = Record<string, RuntimeValue<string>>;
export type NonEmptyReadonlyArray<A> = readonly [A, ...ReadonlyArray<A>];

type RejectExtraKeys<Candidate, Shape> = {
  readonly [Key in Exclude<keyof Candidate, keyof Shape>]: never;
};

type IsAny<A> = 0 extends 1 & A ? true : false;

type IsUnknown<A> =
  IsAny<A> extends true
    ? false
    : unknown extends A
      ? [A] extends [unknown]
        ? true
        : false
      : false;

type RejectAnyCodecValue<A> =
  IsAny<A> extends true ? { readonly __viewServerKafkaCodecValueCannotBeAny: never } : unknown;

type RejectAnyCodecError<E> =
  IsAny<E> extends true ? { readonly __viewServerKafkaCodecErrorCannotBeAny: never } : unknown;

type ExactObject<Candidate, Shape> = Candidate & Shape & RejectExtraKeys<Candidate, Shape>;

type ExactMappingReturn<
  Input,
  Row,
  Mapping extends (...args: ReadonlyArray<never>) => unknown,
> = Mapping extends (input: Input) => infer Output
  ? IsAny<Output> extends true
    ? never
    : [Output] extends [never]
      ? Mapping
      : Output extends ExactObject<Output, Row>
        ? Mapping
        : never
  : never;

type ExactStringReturn<
  Input,
  Mapper extends (...args: ReadonlyArray<never>) => unknown,
> = Mapper extends (input: Input) => infer Output
  ? IsAny<Output> extends true
    ? never
    : [Output] extends [string]
      ? Mapper
      : never
  : never;

type RejectAnyReturn<Mapper extends (...args: ReadonlyArray<never>) => unknown> = Mapper extends (
  ...args: ReadonlyArray<never>
) => infer Output
  ? IsAny<Output> extends true
    ? { readonly __viewServerCallbackReturnCannotBeAny: never }
    : unknown
  : never;

const KafkaCodecValueTypeId = Symbol("@effect-view-server/config/KafkaCodecValue");
const KafkaCodecErrorTypeId = Symbol("@effect-view-server/config/KafkaCodecError");
const KafkaCodecDecodeTypeId = Symbol("@effect-view-server/config/KafkaCodecDecode");
const KafkaTopicDecodeTypeId = Symbol("@effect-view-server/config/KafkaTopicDecode");
const KafkaResolvedSourceTopicTypeId = Symbol(
  "@effect-view-server/config/KafkaResolvedSourceTopic",
);
const EffectSchemaClassAnnotationKey = "~effect/Schema/Class";

export type KafkaDecodeError = {
  readonly _tag: "KafkaDecodeError";
  readonly message: string;
  readonly cause?: unknown;
};

const KafkaMappingErrorTypeId: unique symbol = Symbol(
  "@effect-view-server/config/KafkaMappingError",
);

export type KafkaMappingError = {
  readonly _tag: "KafkaMappingError";
  readonly [KafkaMappingErrorTypeId]: typeof KafkaMappingErrorTypeId;
  readonly message: string;
  readonly cause?: unknown;
};

export const kafkaErrorIsMapping = (error: unknown): error is KafkaMappingError =>
  typeof error === "object" &&
  error !== null &&
  Object.hasOwn(error, KafkaMappingErrorTypeId) &&
  Reflect.get(error, KafkaMappingErrorTypeId) === KafkaMappingErrorTypeId;

export type KafkaCodec<A, E = never> = {
  readonly [KafkaCodecValueTypeId]: ReadonlyArray<A>;
  readonly [KafkaCodecErrorTypeId]: ReadonlyArray<E>;
  readonly [KafkaCodecDecodeTypeId]: (input: KafkaCodecDecodeInput) => Effect.Effect<A, E>;
  readonly format: string;
};

export type KafkaBytesCodec = KafkaCodec<Uint8Array> & { readonly format: "bytes" };
export type KafkaStringCodec = KafkaCodec<string> & { readonly format: "string" };
export type KafkaJsonCodec<SourceSchema extends RowSchema = RowSchema> = KafkaCodec<
  SourceSchema["Type"],
  KafkaDecodeError
> & {
  readonly format: "json";
  readonly schema: SourceSchema;
};
export type KafkaProtobufCodec<Proto extends DescMessage = DescMessage> = KafkaCodec<
  MessageShape<Proto>,
  KafkaDecodeError
> & {
  readonly format: "protobuf";
  readonly descriptor: Proto;
};
export type KafkaCustomCodec<A = unknown, E = unknown> = KafkaCodec<A, E> & {
  readonly format: "custom";
  readonly name: string;
  readonly decode: (input: KafkaCodecDecodeInput) => Effect.Effect<A, E, never>;
};

export type KafkaSourceCodec =
  | KafkaBytesCodec
  | KafkaStringCodec
  | KafkaJsonCodec
  | KafkaProtobufCodec
  | KafkaCustomCodec;

export type KafkaCodecType<Codec> = Codec extends KafkaCodec<infer A, infer _E> ? A : never;
export type KafkaCodecError<Codec> = Codec extends KafkaCodec<infer _A, infer E> ? E : never;

export type KafkaCodecDecodeInput = {
  readonly bytes: Uint8Array;
  readonly metadata: KafkaMessageMetadata;
};

export type KafkaProtobufType<Proto extends DescMessage> = MessageShape<Proto>;

type SupportedKafkaProtobufInput<Proto> = IsAny<Proto> extends true ? never : unknown;

type SupportedKafkaCodec<Codec extends KafkaCodec<unknown, unknown>> =
  IsAny<Codec> extends true
    ? never
    : IsAny<KafkaCodecType<Codec>> extends true
      ? never
      : IsAny<KafkaCodecError<Codec>> extends true
        ? never
        : IsUnknown<KafkaCodecError<Codec>> extends true
          ? never
          : Codec & KafkaCodec<KafkaCodecType<Codec>, KafkaCodecError<Codec>>;

type SupportedKafkaJsonSchema<SourceSchema extends RowSchema> =
  IsAny<SourceSchema> extends true ? never : unknown;

type KafkaTopicSourceDecodeInput<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
> = {
  readonly keyBytes: Uint8Array;
  readonly valueBytes: Uint8Array | null;
  readonly region: Region;
  readonly metadata: KafkaMessageMetadata<Region>;
  readonly rowKeyField: KafkaTopicKeyField<Topics, ViewTopic>;
  readonly schema: KafkaTopicSchemaValue<Topics, ViewTopic>;
  readonly viewServerTopic: ViewTopic;
};

type KafkaDecodedTopicSourceResult<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
> =
  | {
      readonly row: KafkaTopicSchemaValue<Topics, ViewTopic>["Type"];
      readonly rowKey: string;
      readonly viewServerTopic: ViewTopic;
    }
  | {
      readonly rowKey: string;
      readonly tombstone: true;
      readonly viewServerTopic: ViewTopic;
    };

type KafkaTopicSourceDecoder<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  E,
> = {
  readonly [KafkaTopicDecodeTypeId]: {
    bivarianceHack(
      input: KafkaTopicSourceDecodeInput<Topics, ViewTopic, Region>,
    ): Effect.Effect<KafkaDecodedTopicSourceResult<Topics, ViewTopic>, E>;
  }["bivarianceHack"];
};

type KafkaTopicSchemaValue<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
> = Topics[ViewTopic]["schema"];

type KafkaTopicKeyField<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
> = string extends keyof Topics
  ? string
  : Topics[ViewTopic] extends { readonly key: infer Key extends string }
    ? Extract<Key, keyof TopicRow<Topics, ViewTopic>>
    : never;

type KafkaTopicMappedSourceRow<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  KeyField extends string = KafkaTopicKeyField<Topics, ViewTopic>,
> = Omit<TopicRow<Topics, ViewTopic>, Extract<KeyField, keyof TopicRow<Topics, ViewTopic>>>;

type KafkaTopicSchemaRegistry = Record<
  string,
  {
    readonly key: string;
    readonly schema: RowSchema;
    readonly kafkaSource?: unknown;
    readonly grpcSource?: unknown;
  }
>;

const utf8Decoder = new TextDecoder();

const kafkaDecodeError = (message: string, cause: unknown): KafkaDecodeError => ({
  _tag: "KafkaDecodeError",
  message,
  cause,
});

const kafkaMappingError = (message: string, cause: unknown): KafkaMappingError => ({
  _tag: "KafkaMappingError",
  [KafkaMappingErrorTypeId]: KafkaMappingErrorTypeId,
  message,
  cause,
});

const isInspectableObject = (value: unknown): value is object =>
  (typeof value === "object" && value !== null) || typeof value === "function";

const isKafkaCodec = (value: unknown): value is KafkaCodec<unknown, unknown> =>
  isInspectableObject(value) &&
  typeof Reflect.get(value, "format") === "string" &&
  typeof Reflect.get(value, KafkaCodecDecodeTypeId) === "function";

const isSupportedJsonObjectRecordKeyAst = (ast: SchemaAST.AST): boolean => {
  if (SchemaAST.isLiteral(ast)) {
    return typeof ast.literal === "string" || typeof ast.literal === "symbol";
  }
  if (SchemaAST.isUniqueSymbol(ast) || SchemaAST.isTemplateLiteral(ast)) {
    return true;
  }
  if (SchemaAST.isUnion(ast)) {
    return (
      ast.types.length > 0 && ast.types.every((member) => isSupportedJsonObjectRecordKeyAst(member))
    );
  }
  return ast._tag === "String" || ast._tag === "Number" || ast._tag === "Symbol";
};

const isSupportedJsonObjectRecordKeySourceSchema = (schema: object): boolean => {
  const from = Reflect.get(schema, "from");
  if (isInspectableObject(from)) {
    return isSupportedJsonObjectRecordKeySourceSchema(from);
  }
  const members = Reflect.get(schema, "members");
  if (Array.isArray(members)) {
    return (
      members.length > 0 &&
      members.every(
        (member) =>
          isInspectableObject(member) && isSupportedJsonObjectRecordKeySourceSchema(member),
      )
    );
  }
  return isSupportedJsonObjectRecordKeyAst(Reflect.get(schema, "ast"));
};

const isSupportedJsonObjectRecordKeyDecodedSchema = (schema: object): boolean => {
  const to = Reflect.get(schema, "to");
  if (isInspectableObject(to)) {
    return isSupportedJsonObjectRecordKeyDecodedSchema(to);
  }
  const members = Reflect.get(schema, "members");
  if (Array.isArray(members)) {
    return (
      members.length > 0 &&
      members.every(
        (member) => isInspectableObject(member) && isSupportedJsonObjectRecordKeySchema(member),
      )
    );
  }
  return isSupportedJsonObjectRecordKeyAst(Reflect.get(schema, "ast"));
};

const isSupportedJsonObjectRecordKeySchema = (schema: object): boolean =>
  isSupportedJsonObjectRecordKeyAst(Reflect.get(schema, "ast")) &&
  isSupportedJsonObjectRecordKeySourceSchema(schema) &&
  isSupportedJsonObjectRecordKeyDecodedSchema(schema);

const isJsonLikeDeclarationAst = (ast: SchemaAST.AST): boolean =>
  SchemaAST.isDeclaration(ast) &&
  ast.typeParameters.length === 0 &&
  ast.encoding !== undefined &&
  ast.encoding.some((link) => SchemaAST.isUnknown(link.to));

const isObjectLikeJsonCodecTargetAst = (ast: SchemaAST.AST): boolean =>
  SchemaAST.isObjects(ast) ||
  SchemaAST.isObjectKeyword(ast) ||
  SchemaAST.isUnknown(ast) ||
  SchemaAST.isAny(ast) ||
  isJsonLikeDeclarationAst(ast);

const validateCustomDeclarationJsonCodecTargetAst = (
  ast: SchemaAST.AST,
  seen: Set<SchemaAST.AST>,
  requiresConcreteWireShape = false,
): boolean => {
  if (seen.has(ast)) {
    return false;
  }
  seen.add(ast);
  if (SchemaAST.isSuspend(ast)) {
    return validateCustomDeclarationJsonCodecTargetAst(
      ast.thunk(),
      seen,
      requiresConcreteWireShape,
    );
  }
  if (
    SchemaAST.isDeclaration(ast) &&
    ast.typeParameters.length === 0 &&
    typeof declarationJsonLink(ast) !== "function"
  ) {
    throw new Error("Declaration schemas must define Kafka JSON codecs");
  }
  if (ast.encoding !== undefined) {
    const classTarget =
      SchemaAST.isDeclaration(ast) && isSchemaClassDeclarationAst(ast)
        ? ast.typeParameters[0]
        : undefined;
    let hasJsonWireLink = false;
    for (const link of ast.encoding) {
      if (link.to !== classTarget) {
        hasJsonWireLink =
          validateCustomDeclarationJsonCodecTargetAst(
            link.to,
            seen,
            isObjectLikeJsonCodecTargetAst(ast),
          ) || hasJsonWireLink;
      }
    }
    if (classTarget !== undefined && !hasJsonWireLink && SchemaAST.isObjects(classTarget)) {
      validateCustomDeclarationJsonCodecTargetAst(classTarget, seen);
    }
    if (isObjectLikeJsonCodecTargetAst(ast) && !hasJsonWireLink) {
      throw new Error("Declaration JSON codecs must not produce object-like codecs");
    }
    return hasJsonWireLink;
  }
  if (isObjectLikeJsonCodecTargetAst(ast)) {
    throw new Error("Declaration JSON codecs must not produce object-like codecs");
  }
  if (SchemaAST.isDeclaration(ast) && requiresConcreteWireShape) {
    return false;
  }
  if (SchemaAST.isArrays(ast)) {
    for (const element of ast.elements) {
      validateCustomDeclarationJsonCodecTargetAst(element, seen);
    }
    for (const rest of ast.rest) {
      validateCustomDeclarationJsonCodecTargetAst(rest, seen);
    }
    return true;
  }
  if (SchemaAST.isUnion(ast)) {
    for (const member of ast.types) {
      validateCustomDeclarationJsonCodecTargetAst(member, seen);
    }
    return true;
  }
  if (SchemaAST.isTemplateLiteral(ast)) {
    for (const part of ast.parts) {
      validateCustomDeclarationJsonCodecTargetAst(part, seen);
    }
  }
  return true;
};

const declarationJsonLink = (ast: SchemaAST.AST): unknown =>
  Reflect.get(Object(Reflect.get(ast, "annotations")), "toCodecJson") ??
  Reflect.get(Object(Reflect.get(ast, "annotations")), "toCodec");

const effectDeclarationDescriptor = (schema: { readonly ast: SchemaAST.AST }) => ({
  run: Reflect.get(schema.ast, "run"),
  link: declarationJsonLink(schema.ast),
});

const knownEffectJsonDeclarations = [
  Schema.BigDecimal,
  Schema.Date,
  Schema.Duration,
  Schema.Error(),
  Schema.Error({ includeStack: true }),
  Schema.Error({ excludeCause: true }),
  Schema.Error({ includeStack: true, excludeCause: true }),
  Schema.File,
  Schema.FormData,
  Schema.Json,
  Schema.MutableJson,
  Schema.RegExp,
  Schema.URL,
  Schema.URLSearchParams,
].map(effectDeclarationDescriptor) satisfies ReadonlyArray<{
  readonly run: unknown;
  readonly link: unknown;
}>;

const parametricDeclarationKey = (tag: string, shape: string): string => `${tag}\n${shape}`;

const functionSource = (fn: unknown): string => Function.prototype.toString.call(fn);

// Effect v4 beta does not expose stable constructor identities for parametric declarations.
// Kafka topic schemas are trusted application config; this guard catches accidental/custom
// declaration drift while preserving real Effect JSON codecs such as Option and ReadonlyMap.
const declarationLinkSource = (schema: { readonly ast: SchemaAST.Declaration }): string =>
  functionSource(declarationJsonLink(schema.ast));

const declarationParserSource = (schema: { readonly ast: SchemaAST.Declaration }): string =>
  functionSource(schema.ast.run(schema.ast.typeParameters));

type ParametricDeclarationDescriptor = {
  readonly annotationKey: "toCodec" | "toCodecJson";
  readonly parameterKeys: ReadonlyArray<string>;
  readonly parameterPaths: ReadonlyArray<string>;
  readonly parserSources: ReadonlyArray<string>;
  readonly sources: ReadonlyArray<string>;
};

const parametricDeclarationDescriptor = (
  schema: { readonly ast: SchemaAST.Declaration },
  annotationKey: "toCodec" | "toCodecJson",
  parameterKeys: ReadonlyArray<string>,
  parameterPaths: ReadonlyArray<string> = parameterKeys,
): ParametricDeclarationDescriptor => ({
  annotationKey,
  parameterKeys,
  parameterPaths,
  parserSources: [declarationParserSource(schema)],
  sources: [declarationLinkSource(schema)],
});

const knownEffectParametricDeclarationSources = new Map<string, ParametricDeclarationDescriptor>([
  [
    parametricDeclarationKey("ReadonlyMap", "key,value,ast,rebuild,makeEffect,make,makeOption"),
    parametricDeclarationDescriptor(Schema.ReadonlyMap(Schema.String, Schema.String), "toCodec", [
      "key",
      "value",
    ]),
  ],
  [
    parametricDeclarationKey("ReadonlySet", "value,ast,rebuild,makeEffect,make,makeOption"),
    parametricDeclarationDescriptor(Schema.ReadonlySet(Schema.String), "toCodec", ["value"]),
  ],
  [
    parametricDeclarationKey("effect/Cause", "error,defect,ast,rebuild,makeEffect,make,makeOption"),
    parametricDeclarationDescriptor(Schema.Cause(Schema.String, Schema.String), "toCodec", [
      "error",
      "defect",
    ]),
  ],
  [
    parametricDeclarationKey(
      "effect/Cause/Failure",
      "error,defect,ast,rebuild,makeEffect,make,makeOption",
    ),
    parametricDeclarationDescriptor(Schema.CauseReason(Schema.String, Schema.String), "toCodec", [
      "error",
      "defect",
    ]),
  ],
  [
    parametricDeclarationKey("effect/Chunk", "value,ast,rebuild,makeEffect,make,makeOption"),
    parametricDeclarationDescriptor(Schema.Chunk(Schema.String), "toCodec", ["value"]),
  ],
  [
    parametricDeclarationKey(
      "effect/Exit",
      "value,error,defect,ast,rebuild,makeEffect,make,makeOption",
    ),
    parametricDeclarationDescriptor(
      Schema.Exit(Schema.String, Schema.String, Schema.String),
      "toCodec",
      ["value", "error", "defect"],
    ),
  ],
  [
    parametricDeclarationKey("effect/HashMap", "key,value,ast,rebuild,makeEffect,make,makeOption"),
    parametricDeclarationDescriptor(Schema.HashMap(Schema.String, Schema.String), "toCodec", [
      "key",
      "value",
    ]),
  ],
  [
    parametricDeclarationKey("effect/HashSet", "value,ast,rebuild,makeEffect,make,makeOption"),
    parametricDeclarationDescriptor(Schema.HashSet(Schema.String), "toCodec", ["value"]),
  ],
  [
    parametricDeclarationKey("effect/Option", "value,ast,rebuild,makeEffect,make,makeOption"),
    parametricDeclarationDescriptor(Schema.Option(Schema.String), "toCodec", ["value"]),
  ],
  [
    parametricDeclarationKey("effect/Option", "from,to,ast,rebuild,makeEffect,make,makeOption"),
    parametricDeclarationDescriptor(
      Schema.OptionFromNullOr(Schema.String),
      "toCodec",
      ["from", "to"],
      ["to.value"],
    ),
  ],
  [
    parametricDeclarationKey("effect/Redacted", "value,ast,rebuild,makeEffect,make,makeOption"),
    parametricDeclarationDescriptor(Schema.Redacted(Schema.String), "toCodecJson", ["value"]),
  ],
  [
    parametricDeclarationKey(
      "effect/Result",
      "success,failure,ast,rebuild,makeEffect,make,makeOption",
    ),
    parametricDeclarationDescriptor(Schema.Result(Schema.String, Schema.String), "toCodec", [
      "success",
      "failure",
    ]),
  ],
]);

const knownEffectParametricDeclarationAstDescriptors = Array.from(
  knownEffectParametricDeclarationSources,
  ([key, descriptor]) => ({
    parameterKeys: descriptor.parameterKeys,
    tag: key.slice(0, key.indexOf("\n")),
  }),
);

const schemaParametersMatchDeclarationAst = (
  schema: object,
  ast: SchemaAST.Declaration,
  parameterPaths: ReadonlyArray<string>,
): boolean =>
  parameterPaths.length === ast.typeParameters.length &&
  parameterPaths.every((path, index) => {
    const parameterSchema = path
      .split(".")
      .reduce<unknown>((current, key) => Reflect.get(Object(current), key), schema);
    return (
      isInspectableObject(parameterSchema) &&
      Reflect.get(parameterSchema, "ast") === ast.typeParameters[index]
    );
  });

const isKnownEffectParametricJsonDeclarationSchema = (
  schema: object,
  ast: SchemaAST.Declaration,
  getLink: unknown,
): boolean => {
  const annotations = Object(ast.annotations);
  const typeConstructor = Reflect.get(annotations, "typeConstructor");
  if (!isInspectableObject(typeConstructor)) {
    return false;
  }
  const expectedSources = knownEffectParametricDeclarationSources.get(
    parametricDeclarationKey(
      String(Reflect.get(typeConstructor, "_tag")),
      Object.keys(schema).join(","),
    ),
  );
  return (
    expectedSources !== undefined &&
    schemaParametersMatchDeclarationAst(schema, ast, expectedSources.parameterPaths) &&
    (expectedSources.annotationKey === "toCodecJson" ||
      Reflect.get(annotations, "toCodecJson") === undefined) &&
    getLink === Reflect.get(annotations, expectedSources.annotationKey) &&
    expectedSources.parserSources.includes(functionSource(ast.run(ast.typeParameters))) &&
    expectedSources.sources.includes(functionSource(getLink))
  );
};

const isKnownEffectParametricJsonDeclarationAst = (
  ast: SchemaAST.Declaration,
  getLink: unknown,
): boolean => {
  const annotations = Object(ast.annotations);
  const typeConstructor = Reflect.get(annotations, "typeConstructor");
  if (!isInspectableObject(typeConstructor)) {
    return false;
  }
  const typeConstructorTag = String(Reflect.get(typeConstructor, "_tag"));
  return knownEffectParametricDeclarationAstDescriptors.some(({ parameterKeys, tag }) => {
    if (tag !== typeConstructorTag || parameterKeys.length !== ast.typeParameters.length) {
      return false;
    }
    const syntheticSchema = Schema.make(
      ast,
      Object.fromEntries(
        ast.typeParameters.map((typeParameter, index) => [
          String(parameterKeys[index]),
          Schema.make(typeParameter),
        ]),
      ),
    );
    return isKnownEffectParametricJsonDeclarationSchema(syntheticSchema, ast, getLink);
  });
};

const isKnownEffectJsonDeclarationSchema = (
  schema: object,
  ast: SchemaAST.Declaration,
  getLink: unknown,
): boolean =>
  knownEffectJsonDeclarations.some(
    (declaration) => declaration.run === ast.run && declaration.link === getLink,
  ) || isKnownEffectParametricJsonDeclarationSchema(schema, ast, getLink);

const isKnownEffectJsonDeclarationAst = (ast: SchemaAST.Declaration, getLink: unknown): boolean =>
  knownEffectJsonDeclarations.some(
    (declaration) => declaration.run === ast.run && declaration.link === getLink,
  ) || isKnownEffectParametricJsonDeclarationAst(ast, getLink);

const isSchemaClassDeclarationAst = (ast: SchemaAST.Declaration): boolean => {
  const classLink = Reflect.get(Object(ast.annotations), EffectSchemaClassAnnotationKey);
  const toCodecLink = Reflect.get(Object(ast.annotations), "toCodec");
  if (
    typeof classLink !== "function" ||
    typeof toCodecLink !== "function" ||
    ast.typeParameters.length !== 1
  ) {
    return false;
  }
  const classTarget = ast.typeParameters[0];
  if (classTarget === undefined || !SchemaAST.isObjects(classTarget)) {
    return false;
  }
  const classLinkResult = classLink([classTarget]);
  const toCodecLinkResult = toCodecLink([Schema.make(classTarget)]);
  return (
    isInspectableObject(classLinkResult) &&
    isInspectableObject(toCodecLinkResult) &&
    Reflect.get(classLinkResult, "to") === classTarget &&
    Reflect.get(toCodecLinkResult, "to") === classTarget
  );
};

const isDefaultSchemaClassDeclarationAst = (ast: SchemaAST.Declaration): boolean =>
  Reflect.get(Object(ast.annotations), "toCodecJson") === undefined &&
  isSchemaClassDeclarationAst(ast);

const rejectSuspendedRecordKeySchemas = (schema: unknown): void => {
  const customDeclarationJsonCodecTargetAstFromAst = (ast: SchemaAST.Declaration): unknown => {
    const getLink = declarationJsonLink(ast);
    if (
      typeof getLink !== "function" ||
      isKnownEffectJsonDeclarationAst(ast, getLink) ||
      isDefaultSchemaClassDeclarationAst(ast)
    ) {
      return undefined;
    }
    const link = getLink(
      ast.typeParameters.map((typeParameter) => Schema.make(SchemaAST.toEncoded(typeParameter))),
    );
    return Schema.toCodecJson(Schema.make(link.to)).ast;
  };
  const visitAst = (root: SchemaAST.AST): void => {
    const seen = new Set<SchemaAST.AST>();
    const visitCurrentAst = (current: SchemaAST.AST): void => {
      if (seen.has(current)) {
        return;
      }
      seen.add(current);
      if (SchemaAST.isSuspend(current)) {
        visitCurrentAst(current.thunk());
        return;
      }
      if (!SchemaAST.isDeclaration(current) && current.encoding !== undefined) {
        for (const link of current.encoding) {
          visitCurrentAst(link.to);
        }
        return;
      }
      if (SchemaAST.isDeclaration(current)) {
        if (
          current.typeParameters.length === 0 &&
          typeof declarationJsonLink(current) !== "function"
        ) {
          throw new Error("Declaration schemas must define Kafka JSON codecs");
        }
        const isSchemaClass = isSchemaClassDeclarationAst(current);
        const classTarget = isSchemaClass ? current.typeParameters[0] : undefined;
        const customJsonCodecTargetAst = customDeclarationJsonCodecTargetAstFromAst(current);
        if (SchemaAST.isAST(customJsonCodecTargetAst)) {
          validateCustomDeclarationJsonCodecTargetAst(customJsonCodecTargetAst, new Set());
        }
        let inspectedDeclarationEncoding = false;
        if (current.encoding !== undefined) {
          for (const link of current.encoding) {
            if (!isSchemaClass || link.to !== classTarget) {
              visitCurrentAst(link.to);
              inspectedDeclarationEncoding = true;
            }
          }
        }
        if (inspectedDeclarationEncoding) {
          return;
        }
        if (isSchemaClass) {
          if (
            classTarget !== undefined &&
            SchemaAST.isObjects(classTarget) &&
            classTarget.propertySignatures.length === 0 &&
            classTarget.indexSignatures.length === 0
          ) {
            return;
          }
        }
        for (const typeParameter of current.typeParameters) {
          visitCurrentAst(typeParameter);
        }
        return;
      }
      if (SchemaAST.isArrays(current)) {
        for (const element of current.elements) {
          visitCurrentAst(element);
        }
        for (const rest of current.rest) {
          visitCurrentAst(rest);
        }
        return;
      }
      if (SchemaAST.isObjects(current)) {
        // Effect erases unsupported record-key schemas inside Suspend to Objects without
        // stable source metadata. Reject the ambiguous empty shape instead of silently
        // accepting a schema whose JSON codec can skip record-value decoding.
        // Non-empty erased record-key unions are indistinguishable from ordinary suspended
        // structs at this boundary; Kafka schemas are trusted config and must avoid those.
        if (current.propertySignatures.length === 0 && current.indexSignatures.length === 0) {
          throw new Error("Suspended empty object schemas are not supported by Kafka JSON codecs");
        }
        for (const property of current.propertySignatures) {
          visitCurrentAst(property.type);
        }
        for (const index of current.indexSignatures) {
          visitCurrentAst(index.parameter);
          visitCurrentAst(index.type);
        }
        return;
      }
      if (SchemaAST.isUnion(current)) {
        for (const member of current.types) {
          visitCurrentAst(member);
        }
        return;
      }
      if (SchemaAST.isTemplateLiteral(current)) {
        for (const part of current.parts) {
          visitCurrentAst(part);
        }
      }
    };
    visitCurrentAst(root);
  };
  const customDeclarationJsonCodecTargetAst = (current: object): unknown => {
    const ast = Reflect.get(current, "ast");
    if (!SchemaAST.isAST(ast) || !SchemaAST.isDeclaration(ast)) {
      return undefined;
    }
    const getLink = declarationJsonLink(ast);
    if (
      typeof getLink !== "function" ||
      isKnownEffectJsonDeclarationSchema(current, ast, getLink) ||
      isDefaultSchemaClassDeclarationAst(ast)
    ) {
      return undefined;
    }
    const link = getLink(
      ast.typeParameters.map((typeParameter) => Schema.make(SchemaAST.toEncoded(typeParameter))),
    );
    return Schema.toCodecJson(Schema.make(link.to)).ast;
  };
  const visitChild = (current: object, key: string): void => {
    visit(Reflect.get(current, key));
  };
  const visitChildren = (current: object, key: string): void => {
    const children = Reflect.get(current, key);
    if (Array.isArray(children)) {
      for (const child of children) {
        visit(child);
      }
    }
  };
  const visitRecordChildren = (current: object, key: string): void => {
    const children = Reflect.get(current, key);
    if (isInspectableObject(children)) {
      for (const child of Object.values(children)) {
        visit(child);
      }
    }
  };
  const visit = (current: unknown): void => {
    if (!isInspectableObject(current)) {
      return;
    }
    const customJsonCodecTargetAst = customDeclarationJsonCodecTargetAst(current);
    if (SchemaAST.isAST(customJsonCodecTargetAst)) {
      validateCustomDeclarationJsonCodecTargetAst(customJsonCodecTargetAst, new Set());
    }
    const key = Reflect.get(current, "key");
    const ast = Reflect.get(current, "ast");
    if (SchemaAST.isAST(ast) && SchemaAST.isSuspend(ast)) {
      visitAst(ast);
    }
    if (SchemaAST.isAST(ast) && SchemaAST.isDeclaration(ast)) {
      if (ast.typeParameters.length === 0 && typeof declarationJsonLink(ast) !== "function") {
        throw new Error("Declaration schemas must define Kafka JSON codecs");
      }
    }
    if (
      SchemaAST.isAST(ast) &&
      SchemaAST.isObjects(ast) &&
      Object.hasOwn(current, "key") &&
      (!isInspectableObject(key) || !isSupportedJsonObjectRecordKeySchema(key))
    ) {
      throw new Error("Unsupported record key schemas are not supported by Kafka JSON codecs");
    }
    visitChild(current, "key");
    visitChild(current, "value");
    visitChild(current, "schema");
    visitChild(current, "from");
    visitChild(current, "success");
    visitChild(current, "failure");
    visitChild(current, "error");
    visitChild(current, "defect");
    visitChildren(current, "members");
    visitChildren(current, "elements");
    visitChildren(current, "rest");
    visitChildren(current, "records");
    visitRecordChildren(current, "cases");
    const fields = Reflect.get(current, "fields");
    if (isInspectableObject(fields)) {
      for (const fieldSchema of Object.values(fields)) {
        visit(fieldSchema);
      }
    }
  };
  visit(schema);
};

const forceSuspendedJsonCodecBranches = (ast: SchemaAST.AST): void => {
  const seen = new Set<SchemaAST.AST>();
  const visit = (current: SchemaAST.AST): SchemaAST.AST => {
    if (seen.has(current)) {
      return current;
    }
    seen.add(current);
    if (current.encoding !== undefined) {
      for (const link of current.encoding) {
        visit(link.to);
      }
    }
    if (SchemaAST.isSuspend(current)) {
      visit(current.thunk());
      return current;
    }
    if (SchemaAST.isDeclaration(current)) {
      for (const typeParameter of current.typeParameters) {
        visit(typeParameter);
      }
      return current;
    }
    if (SchemaAST.isArrays(current)) {
      for (const element of current.elements) {
        visit(element);
      }
      for (const rest of current.rest) {
        visit(rest);
      }
      return current;
    }
    if (SchemaAST.isObjects(current)) {
      for (const property of current.propertySignatures) {
        visit(property.type);
      }
      for (const index of current.indexSignatures) {
        visit(index.parameter);
        visit(index.type);
      }
      return current;
    }
    if (SchemaAST.isUnion(current)) {
      for (const member of current.types) {
        visit(member);
      }
      return current;
    }
    if (SchemaAST.isTemplateLiteral(current)) {
      for (const part of current.parts) {
        visit(part);
      }
      return current;
    }
    return current;
  };
  visit(ast);
};

const makeKafkaCodec = <A, E, Format extends string>(
  format: Format,
  decode: (input: KafkaCodecDecodeInput) => Effect.Effect<A, E>,
): KafkaCodec<A, E> & {
  readonly format: Format;
} => ({
  [KafkaCodecValueTypeId]: [],
  [KafkaCodecErrorTypeId]: [],
  [KafkaCodecDecodeTypeId]: decode,
  format,
});

type KafkaTopicSourceHelperMapWithoutKey<
  TopicRegions extends NonEmptyReadonlyArray<string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
> = (input: KafkaTopicSourceHelperMapInputWithoutKey<TopicRegions[number], ValueCodec>) => object;

type KafkaTopicSourceHelperMapWithKey<
  TopicRegions extends NonEmptyReadonlyArray<string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
> = (
  input: KafkaTopicSourceHelperMapInputWithKey<TopicRegions[number], ValueCodec, KeyCodec>,
) => object;

type KafkaTopicSourceHelperRowKeyWithoutKey<TopicRegions extends NonEmptyReadonlyArray<string>> = (
  input: KafkaTopicSourceHelperRowKeyInputWithoutKey<TopicRegions[number]>,
) => string;

type KafkaTopicSourceHelperRowKeyWithKey<
  TopicRegions extends NonEmptyReadonlyArray<string>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
> = (input: KafkaTopicSourceHelperRowKeyInputWithKey<TopicRegions[number], KeyCodec>) => string;

type KafkaTopicSourceHelperInputWithoutKey<
  SourceTopic extends string,
  TopicRegions extends NonEmptyReadonlyArray<string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  Mapping extends KafkaTopicSourceHelperMapWithoutKey<TopicRegions, ValueCodec> =
    KafkaTopicSourceHelperMapWithoutKey<TopicRegions, ValueCodec>,
  RowKey extends KafkaTopicSourceHelperRowKeyWithoutKey<TopicRegions> =
    KafkaTopicSourceHelperRowKeyWithoutKey<TopicRegions>,
> = {
  readonly topic: SourceTopic;
  readonly regions: TopicRegions;
  readonly value: SupportedKafkaCodec<ValueCodec>;
  readonly rowKey: RowKey;
  readonly map: Mapping;
} & RejectAnyReturn<RowKey>;

type KafkaTopicSourceHelperInputWithKey<
  SourceTopic extends string,
  TopicRegions extends NonEmptyReadonlyArray<string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  Mapping extends KafkaTopicSourceHelperMapWithKey<TopicRegions, ValueCodec, KeyCodec> =
    KafkaTopicSourceHelperMapWithKey<TopicRegions, ValueCodec, KeyCodec>,
  RowKey extends KafkaTopicSourceHelperRowKeyWithKey<TopicRegions, KeyCodec> =
    KafkaTopicSourceHelperRowKeyWithKey<TopicRegions, KeyCodec>,
> = {
  readonly topic: SourceTopic;
  readonly regions: TopicRegions;
  readonly value: SupportedKafkaCodec<ValueCodec>;
  readonly key: SupportedKafkaCodec<KeyCodec>;
  readonly rowKey: RowKey;
  readonly map: Mapping;
} & RejectAnyReturn<RowKey>;

function defineKafkaSource<
  const SourceTopic extends string,
  const TopicRegions extends NonEmptyReadonlyArray<string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  const RowKey extends KafkaTopicSourceHelperRowKeyWithKey<TopicRegions, KeyCodec>,
  const Mapping extends (
    input: KafkaTopicSourceHelperMapInputWithKey<TopicRegions[number], ValueCodec, KeyCodec>,
  ) => object,
>(
  topic: KafkaTopicSourceHelperInputWithKey<
    SourceTopic,
    TopicRegions,
    ValueCodec,
    KeyCodec,
    Mapping,
    RowKey
  >,
): KafkaTopicSourceHelperInputWithKey<
  SourceTopic,
  TopicRegions,
  ValueCodec,
  KeyCodec,
  Mapping,
  RowKey
>;
function defineKafkaSource<
  const SourceTopic extends string,
  const TopicRegions extends NonEmptyReadonlyArray<string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  const RowKey extends KafkaTopicSourceHelperRowKeyWithoutKey<TopicRegions>,
  const Mapping extends KafkaTopicSourceHelperMapWithoutKey<TopicRegions, ValueCodec> =
    KafkaTopicSourceHelperMapWithoutKey<TopicRegions, ValueCodec>,
>(
  topic: KafkaTopicSourceHelperInputWithoutKey<
    SourceTopic,
    TopicRegions,
    ValueCodec,
    Mapping,
    RowKey
  >,
): KafkaTopicSourceHelperInputWithoutKey<SourceTopic, TopicRegions, ValueCodec, Mapping, RowKey>;
function defineKafkaSource(topic: object): object {
  return topic;
}

export const kafka = {
  bytes: (): KafkaBytesCodec =>
    makeKafkaCodec<Uint8Array, never, "bytes">("bytes", (input) => Effect.succeed(input.bytes)),
  string: (): KafkaStringCodec =>
    makeKafkaCodec<string, never, "string">("string", (input) =>
      Effect.succeed(utf8Decoder.decode(input.bytes)),
    ),
  stringKey: (): KafkaStringCodec =>
    makeKafkaCodec<string, never, "string">("string", (input) =>
      Effect.succeed(utf8Decoder.decode(input.bytes)),
    ),
  json: <const SourceSchema extends RowSchema>(
    schema: SourceSchema & SupportedKafkaJsonSchema<SourceSchema>,
  ): KafkaJsonCodec<SourceSchema> => {
    const jsonDecoder = (() => {
      try {
        rejectSuspendedRecordKeySchemas(schema);
        forceSuspendedJsonCodecBranches(schema.ast);
        const jsonCodec = Schema.toCodecJson(schema);
        rejectSuspendedRecordKeySchemas(jsonCodec);
        forceSuspendedJsonCodecBranches(jsonCodec.ast);
        const decodeJsonRow = Schema.decodeUnknownEffect(jsonCodec);
        return {
          _tag: "valid",
          decodeJsonRow,
        } as const;
      } catch (cause) {
        return {
          _tag: "invalid",
          error: kafkaDecodeError("Kafka JSON schema is not JSON-compatible", cause),
        } as const;
      }
    })();
    return {
      ...makeKafkaCodec<SourceSchema["Type"], KafkaDecodeError, "json">("json", (input) =>
        Effect.gen(function* () {
          if (jsonDecoder._tag === "invalid") {
            return yield* Effect.fail(jsonDecoder.error);
          }
          const decodedJson = yield* Effect.try({
            try: (): unknown => JSON.parse(utf8Decoder.decode(input.bytes)),
            catch: (cause) => kafkaDecodeError("Failed to parse Kafka JSON payload", cause),
          });
          return yield* jsonDecoder
            .decodeJsonRow(decodedJson)
            .pipe(
              Effect.mapError((cause) =>
                kafkaDecodeError("Failed to decode Kafka JSON payload", cause),
              ),
            );
        }),
      ),
      schema,
    };
  },
  protobuf: <const Proto extends DescMessage>(
    descriptor: Proto & SupportedKafkaProtobufInput<Proto>,
  ): KafkaProtobufCodec<Proto> => ({
    ...makeKafkaCodec<MessageShape<Proto>, KafkaDecodeError, "protobuf">("protobuf", (input) => {
      const messageDescriptor: Proto = descriptor;
      return Effect.try({
        try: () => fromBinary(messageDescriptor, input.bytes),
        catch: (cause) => kafkaDecodeError("Failed to decode Kafka protobuf payload", cause),
      });
    }),
    descriptor,
  }),
  codec: <A, E>(
    definition: {
      readonly name: string;
      readonly decode: (input: KafkaCodecDecodeInput) => Effect.Effect<A, E, never>;
    } & RejectAnyCodecValue<NoInfer<A>> &
      RejectAnyCodecError<NoInfer<E>>,
  ): KafkaCustomCodec<A, E> => ({
    ...makeKafkaCodec<A, E, "custom">("custom", definition.decode),
    name: definition.name,
    decode: definition.decode,
  }),
  source: defineKafkaSource,
};

export const decodeKafkaCodec: <A, E>(
  codec: KafkaCodec<A, E>,
  input: KafkaCodecDecodeInput,
) => Effect.Effect<A, E> = Effect.fn("ViewServerConfig.kafka.codec.decode")(function* <A, E>(
  codec: KafkaCodec<A, E>,
  input: KafkaCodecDecodeInput,
) {
  return yield* codec[KafkaCodecDecodeTypeId](input);
});

export type KafkaMessageMetadata<Region extends string = string> = {
  readonly sourceTopic: string;
  readonly sourceRegion: Region;
  readonly partition: number;
  readonly offset: string;
  readonly timestamp: number | null;
  readonly headers: Readonly<
    Record<string, string | Uint8Array | ReadonlyArray<string | Uint8Array>>
  >;
};

export type KafkaTopicSourceMapInput<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown> | undefined,
> = [KeyCodec] extends [undefined]
  ? KafkaTopicSourceMapInputWithoutKey<Topics, ViewTopic, Region, ValueCodec>
  : KafkaTopicSourceMapInputWithKey<
      Topics,
      ViewTopic,
      Region,
      ValueCodec,
      Extract<KeyCodec, KafkaCodec<unknown, unknown>>
    >;

type KafkaTopicSourceMapInputWithoutKey<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  ValueCodec extends KafkaCodec<unknown, unknown>,
> = {
  readonly key: string;
  readonly value: KafkaCodecType<ValueCodec>;
  readonly region: Region;
  readonly rowKey: string;
  readonly schema: KafkaTopicSchemaValue<Topics, ViewTopic>;
  readonly metadata: KafkaMessageMetadata<Region>;
};

type KafkaTopicSourceMapInputWithKey<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
> = {
  readonly key: KafkaCodecType<KeyCodec>;
  readonly value: KafkaCodecType<ValueCodec>;
  readonly region: Region;
  readonly rowKey: string;
  readonly schema: KafkaTopicSchemaValue<Topics, ViewTopic>;
  readonly metadata: KafkaMessageMetadata<Region>;
};

type KafkaTopicSourceHelperMapInputWithoutKey<
  Region extends string,
  ValueCodec extends KafkaCodec<unknown, unknown>,
> = {
  readonly key: string;
  readonly value: KafkaCodecType<ValueCodec>;
  readonly region: Region;
  readonly rowKey: string;
  readonly metadata: KafkaMessageMetadata<Region>;
};

type KafkaTopicSourceHelperMapInputWithKey<
  Region extends string,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
> = {
  readonly key: KafkaCodecType<KeyCodec>;
  readonly value: KafkaCodecType<ValueCodec>;
  readonly region: Region;
  readonly rowKey: string;
  readonly metadata: KafkaMessageMetadata<Region>;
};

type KafkaTopicSourceRowKeyInputWithoutKey<Region extends string> = {
  readonly key: string;
  readonly region: Region;
  readonly metadata: KafkaMessageMetadata<Region>;
};

type KafkaTopicSourceRowKeyInputWithKey<
  Region extends string,
  KeyCodec extends KafkaCodec<unknown, unknown>,
> = {
  readonly key: KafkaCodecType<KeyCodec>;
  readonly region: Region;
  readonly metadata: KafkaMessageMetadata<Region>;
};

type KafkaTopicSourceHelperRowKeyInputWithoutKey<Region extends string> =
  KafkaTopicSourceRowKeyInputWithoutKey<Region>;

type KafkaTopicSourceHelperRowKeyInputWithKey<
  Region extends string,
  KeyCodec extends KafkaCodec<unknown, unknown>,
> = KafkaTopicSourceRowKeyInputWithKey<Region, KeyCodec>;

type KafkaTopicSourceInputWithoutKey<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  KeyField extends string,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaTopicSourceMapInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => KafkaTopicMappedSourceRow<Topics, ViewTopic, KeyField> = (
    input: KafkaTopicSourceMapInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => KafkaTopicMappedSourceRow<Topics, ViewTopic, KeyField>,
  SourceTopic extends string = string,
  RowKey extends (input: KafkaTopicSourceRowKeyInputWithoutKey<TopicRegions[number]>) => unknown = (
    input: KafkaTopicSourceRowKeyInputWithoutKey<TopicRegions[number]>,
  ) => string,
> = {
  readonly topic: SourceTopic;
  readonly regions: TopicRegions;
  readonly value: SupportedKafkaCodec<ValueCodec>;
  readonly rowKey: ExactStringReturn<
    KafkaTopicSourceRowKeyInputWithoutKey<TopicRegions[number]>,
    RowKey
  >;
  readonly map: ExactMappingReturn<
    KafkaTopicSourceMapInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
    KafkaTopicMappedSourceRow<Topics, ViewTopic, KeyField>,
    Mapping
  >;
};

type KafkaTopicSourceInputWithKey<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  KeyField extends string,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaTopicSourceMapInputWithKey<
      Topics,
      ViewTopic,
      TopicRegions[number],
      ValueCodec,
      KeyCodec
    >,
  ) => KafkaTopicMappedSourceRow<Topics, ViewTopic, KeyField> = (
    input: KafkaTopicSourceMapInputWithKey<
      Topics,
      ViewTopic,
      TopicRegions[number],
      ValueCodec,
      KeyCodec
    >,
  ) => KafkaTopicMappedSourceRow<Topics, ViewTopic, KeyField>,
  SourceTopic extends string = string,
  RowKey extends (
    input: KafkaTopicSourceRowKeyInputWithKey<TopicRegions[number], KeyCodec>,
  ) => unknown = (
    input: KafkaTopicSourceRowKeyInputWithKey<TopicRegions[number], KeyCodec>,
  ) => string,
> = {
  readonly topic: SourceTopic;
  readonly regions: TopicRegions;
  readonly value: SupportedKafkaCodec<ValueCodec>;
  readonly key: SupportedKafkaCodec<KeyCodec>;
  readonly rowKey: ExactStringReturn<
    KafkaTopicSourceRowKeyInputWithKey<TopicRegions[number], KeyCodec>,
    RowKey
  >;
  readonly map: ExactMappingReturn<
    KafkaTopicSourceMapInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
    KafkaTopicMappedSourceRow<Topics, ViewTopic, KeyField>,
    Mapping
  >;
};

type KafkaDecodedTopicSourceMessage<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
> = KafkaDecodedTopicSourceResult<Topics, ViewTopic>;

export type KafkaTopicSourceDefinition<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string> = Extract<keyof Topics, string>,
  KeyField extends string = KafkaTopicKeyField<Topics, ViewTopic>,
  ValueCodec extends KafkaCodec<unknown, unknown> = KafkaCodec<unknown, unknown>,
  KeyCodec = undefined,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>> =
    NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  MappingWithoutKey extends (
    input: KafkaTopicSourceMapInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => KafkaTopicMappedSourceRow<Topics, ViewTopic, KeyField> = (
    input: KafkaTopicSourceMapInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => KafkaTopicMappedSourceRow<Topics, ViewTopic, KeyField>,
  MappingWithKey extends (
    input: KafkaTopicSourceMapInputWithKey<
      Topics,
      ViewTopic,
      TopicRegions[number],
      ValueCodec,
      Extract<KeyCodec, KafkaCodec<unknown, unknown>>
    >,
  ) => KafkaTopicMappedSourceRow<Topics, ViewTopic, KeyField> = (
    input: KafkaTopicSourceMapInputWithKey<
      Topics,
      ViewTopic,
      TopicRegions[number],
      ValueCodec,
      Extract<KeyCodec, KafkaCodec<unknown, unknown>>
    >,
  ) => KafkaTopicMappedSourceRow<Topics, ViewTopic, KeyField>,
  SourceTopic extends string = string,
> =
  | KafkaTopicSourceInputWithoutKey<
      Topics,
      Regions,
      ViewTopic,
      KeyField,
      ValueCodec,
      TopicRegions,
      MappingWithoutKey,
      SourceTopic
    >
  | (KeyCodec extends KafkaCodec<unknown, unknown>
      ? MappingWithKey extends (
          input: KafkaTopicSourceMapInputWithKey<
            Topics,
            ViewTopic,
            TopicRegions[number],
            ValueCodec,
            KeyCodec
          >,
        ) => KafkaTopicMappedSourceRow<Topics, ViewTopic, KeyField>
        ? KafkaTopicSourceInputWithKey<
            Topics,
            Regions,
            ViewTopic,
            KeyField,
            ValueCodec,
            KeyCodec,
            TopicRegions,
            MappingWithKey,
            SourceTopic
          >
        : never
      : never);

export type KafkaResolvedSourceTopicDefinition<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string> = Extract<keyof Topics, string>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>> =
    NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
> = KafkaTopicSourceDecoder<Topics, ViewTopic, TopicRegions[number], unknown> & {
  readonly [KafkaResolvedSourceTopicTypeId]: true;
  readonly regions: TopicRegions;
  readonly topic: string;
  readonly viewServerTopic: ViewTopic;
};

const decodeKafkaStringKey = (input: KafkaCodecDecodeInput): string =>
  utf8Decoder.decode(input.bytes);

const mapKafkaPayload = <A>(map: () => A): Effect.Effect<A, KafkaMappingError> =>
  Effect.try({
    try: map,
    catch: (cause) => kafkaMappingError("Failed to map Kafka payload", cause),
  });

const mapKafkaRowKey = (map: () => string): Effect.Effect<string, KafkaMappingError> =>
  Effect.try({
    try: map,
    catch: (cause) => kafkaMappingError("Failed to map Kafka row key", cause),
  }).pipe(
    Effect.flatMap((rowKey) =>
      typeof rowKey === "string"
        ? Effect.succeed(rowKey)
        : Effect.fail(kafkaMappingError("Kafka rowKey must return a string", rowKey)),
    ),
  );

const kafkaMappedRowParseOptions = { onExcessProperty: "error" } as const;

const validateKafkaMappedRow = <
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
>(
  schema: KafkaTopicSchemaValue<Topics, ViewTopic>,
  row: KafkaTopicSchemaValue<Topics, ViewTopic>["Type"],
): Effect.Effect<KafkaTopicSchemaValue<Topics, ViewTopic>["Type"], KafkaMappingError> =>
  Schema.encodeUnknownEffect(schema)(row, kafkaMappedRowParseOptions).pipe(
    Effect.mapError((cause) => kafkaMappingError("Kafka mapped row failed topic schema", cause)),
    Effect.as(row),
  );

type AnyKafkaResolvedSourceTopic = KafkaTopicSourceDecoder<
  KafkaTopicSchemaRegistry,
  string,
  string,
  unknown
> & {
  readonly regions: NonEmptyReadonlyArray<string>;
  readonly topic: string;
  readonly viewServerTopic: string;
};

type DecodedKafkaSourceMessage<ViewTopic extends string = string> =
  | {
      readonly row: object;
      readonly rowKey: string;
      readonly viewServerTopic: ViewTopic;
    }
  | {
      readonly rowKey: string;
      readonly tombstone: true;
      readonly viewServerTopic: ViewTopic;
    };

type DecodedKafkaSourceTopicMessage<ViewTopic extends string = string> =
  | {
      readonly row: object;
      readonly rowKey: string;
      readonly viewServerTopic: ViewTopic;
    }
  | {
      readonly rowKey: string;
      readonly tombstone: true;
      readonly viewServerTopic: ViewTopic;
    };

const decodeKafkaTopicMessageEffect: (
  topic: AnyKafkaResolvedSourceTopic,
  input: KafkaTopicSourceDecodeInput<KafkaTopicSchemaRegistry, string, string>,
) => Effect.Effect<DecodedKafkaSourceMessage, unknown> = Effect.fn(
  "ViewServerConfig.kafka.topic.decodeMessage",
)(function* (
  topic: AnyKafkaResolvedSourceTopic,
  input: KafkaTopicSourceDecodeInput<KafkaTopicSchemaRegistry, string, string>,
) {
  if (
    input.schema === undefined ||
    input.rowKeyField === undefined ||
    input.viewServerTopic === undefined
  ) {
    return yield* Effect.fail(
      kafkaMappingError("Topic-owned Kafka source decode is missing topic metadata", {
        rowKeyField: input.rowKeyField,
        schema: input.schema,
        viewServerTopic: input.viewServerTopic,
      }),
    );
  }
  const decoded = yield* topic[KafkaTopicDecodeTypeId]({
    keyBytes: input.keyBytes,
    valueBytes: input.valueBytes,
    region: input.region,
    metadata: input.metadata,
    rowKeyField: input.rowKeyField,
    schema: input.schema,
    viewServerTopic: input.viewServerTopic,
  });
  if ("tombstone" in decoded) {
    return {
      rowKey: decoded.rowKey,
      tombstone: true,
      viewServerTopic: decoded.viewServerTopic,
    };
  }
  return {
    row: decoded.row,
    rowKey: decoded.rowKey,
    viewServerTopic: decoded.viewServerTopic,
  };
});

type DecodedTopicRegion<Topic> = Topic extends {
  readonly regions: NonEmptyReadonlyArray<infer Region extends string>;
}
  ? Region
  : never;

type DecodedSourceTopicTopics<Topic> =
  Topic extends KafkaTopicSourceDecoder<infer Topics, infer _ViewTopic, infer _Region, infer _Error>
    ? Topics
    : never;

type DecodedSourceTopicViewTopic<Topic> =
  Topic extends KafkaTopicSourceDecoder<infer _Topics, infer ViewTopic, infer _Region, infer _Error>
    ? ViewTopic
    : never;

type DecodedSourceTopicMessage<Topic> =
  DecodedSourceTopicViewTopic<Topic> extends Extract<keyof DecodedSourceTopicTopics<Topic>, string>
    ? KafkaDecodedTopicSourceMessage<
        DecodedSourceTopicTopics<Topic>,
        DecodedSourceTopicViewTopic<Topic>
      >
    : never;

type DecodedSourceTopicInput<Topic> =
  DecodedSourceTopicViewTopic<Topic> extends Extract<keyof DecodedSourceTopicTopics<Topic>, string>
    ? KafkaTopicSourceDecodeInput<
        DecodedSourceTopicTopics<Topic>,
        DecodedSourceTopicViewTopic<Topic>,
        DecodedTopicRegion<Topic>
      >
    : never;

export function decodeKafkaTopicMessage<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
>(
  topic: KafkaTopicSourceDecoder<Topics, ViewTopic, Region, unknown> & {
    readonly regions: NonEmptyReadonlyArray<Region>;
    readonly topic: string;
    readonly viewServerTopic: ViewTopic;
  },
  input: KafkaTopicSourceDecodeInput<Topics, ViewTopic, Region>,
): Effect.Effect<KafkaDecodedTopicSourceMessage<Topics, ViewTopic>, unknown>;
export function decodeKafkaTopicMessage<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
>(
  topic: KafkaResolvedSourceTopicDefinition<Topics, Regions, Extract<keyof Topics, string>>,
  input: KafkaTopicSourceDecodeInput<
    KafkaTopicSchemaRegistry,
    string,
    Extract<keyof Regions, string>
  >,
): Effect.Effect<DecodedKafkaSourceTopicMessage<Extract<keyof Topics, string>>, unknown>;
export function decodeKafkaTopicMessage<Topic extends AnyKafkaResolvedSourceTopic>(
  topic: Topic,
  input: DecodedSourceTopicInput<Topic>,
): Effect.Effect<DecodedSourceTopicMessage<Topic>, unknown>;
export function decodeKafkaTopicMessage(
  topic: AnyKafkaResolvedSourceTopic,
  input: KafkaTopicSourceDecodeInput<KafkaTopicSchemaRegistry, string, string>,
): Effect.Effect<DecodedKafkaSourceMessage, unknown> {
  return decodeKafkaTopicMessageEffect(topic, input);
}

export type ValidateKafkaTopicSource<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  KeyField extends string,
  Candidate,
> = Candidate extends {
  readonly topic: string;
  readonly regions: infer TopicRegions extends NonEmptyReadonlyArray<
    Extract<keyof Regions, string>
  >;
  readonly value: infer ValueCodec extends KafkaCodec<unknown, unknown>;
  readonly key: infer KeyCodec extends KafkaCodec<unknown, unknown>;
}
  ? Candidate extends {
      readonly rowKey: infer RowKey extends (
        input: KafkaTopicSourceRowKeyInputWithKey<TopicRegions[number], KeyCodec>,
      ) => unknown;
      readonly map: infer Mapping extends (
        input: KafkaTopicSourceMapInputWithKey<
          Topics,
          ViewTopic,
          TopicRegions[number],
          ValueCodec,
          KeyCodec
        >,
      ) => KafkaTopicMappedSourceRow<Topics, ViewTopic, KeyField>;
    }
    ? Candidate extends KafkaTopicSourceInputWithKey<
        Topics,
        Regions,
        ViewTopic,
        KeyField,
        ValueCodec,
        KeyCodec,
        TopicRegions,
        Mapping,
        string,
        RowKey
      >
      ? Candidate &
          RejectExtraKeys<
            Candidate,
            KafkaTopicSourceInputWithKey<
              Topics,
              Regions,
              ViewTopic,
              KeyField,
              ValueCodec,
              KeyCodec,
              TopicRegions,
              Mapping,
              string,
              RowKey
            >
          >
      : never
    : never
  : "key" extends keyof Candidate
    ? never
    : Candidate extends {
          readonly topic: string;
          readonly regions: infer TopicRegions extends NonEmptyReadonlyArray<
            Extract<keyof Regions, string>
          >;
          readonly value: infer ValueCodec extends KafkaCodec<unknown, unknown>;
        }
      ? Candidate extends {
          readonly rowKey: infer RowKey extends (
            input: KafkaTopicSourceRowKeyInputWithoutKey<TopicRegions[number]>,
          ) => unknown;
          readonly map: infer Mapping extends (
            input: KafkaTopicSourceMapInputWithoutKey<
              Topics,
              ViewTopic,
              TopicRegions[number],
              ValueCodec
            >,
          ) => KafkaTopicMappedSourceRow<Topics, ViewTopic, KeyField>;
        }
        ? Candidate extends KafkaTopicSourceInputWithoutKey<
            Topics,
            Regions,
            ViewTopic,
            KeyField,
            ValueCodec,
            TopicRegions,
            Mapping,
            string,
            RowKey
          >
          ? Candidate &
              RejectExtraKeys<
                Candidate,
                KafkaTopicSourceInputWithoutKey<
                  Topics,
                  Regions,
                  ViewTopic,
                  KeyField,
                  ValueCodec,
                  TopicRegions,
                  Mapping,
                  string,
                  RowKey
                >
              >
          : never
        : never
      : never;

export type ViewServerKafkaCommittedStartFrom = {
  readonly committedConsumerGroup: string;
  readonly fallback?: "earliest" | "latest" | "fail";
};

export type ViewServerKafkaStartFrom = "earliest" | "latest" | ViewServerKafkaCommittedStartFrom;

export type RuntimeOptions<
  Topics extends KafkaTopicSchemaRegistry,
  ConfigRegions extends RuntimeRegions,
  Options,
> = {
  readonly [Key in Extract<keyof Options, "websocketPort">]: RuntimeValue<number>;
} & (Options extends { readonly kafka: infer CandidateKafka }
  ? {
      readonly kafka: RuntimeKafkaOptions<Topics, ConfigRegions, CandidateKafka>;
    }
  : {
      readonly kafka?: undefined;
    });

type RuntimeKafkaOptions<
  _Topics extends KafkaTopicSchemaRegistry,
  _ConfigRegions extends RuntimeRegions,
  CandidateKafka,
> = CandidateKafka extends {
  readonly consumerGroupId: string;
}
  ? {
      readonly consumerGroupId: string;
      readonly startFrom?: ViewServerKafkaStartFrom;
    } & (CandidateKafka extends { readonly regions: infer Regions extends RuntimeRegions }
      ? { readonly regions: Regions }
      : { readonly regions?: undefined })
  : never;

export type RuntimeOptionsCandidate = {
  readonly websocketPort?: RuntimeValue<number>;
  readonly kafka?: {
    readonly consumerGroupId: string;
    readonly startFrom?: ViewServerKafkaStartFrom;
    readonly regions?: RuntimeRegions;
  };
};

export type ValidateRuntimeOptions<
  Topics extends KafkaTopicSchemaRegistry,
  ConfigRegions extends RuntimeRegions,
  Options,
> = Options extends RuntimeOptionsCandidate
  ? RuntimeOptions<Topics, ConfigRegions, Options>
  : never;

export type RuntimeOptionsDefinition<
  Topics extends KafkaTopicSchemaRegistry,
  ConfigRegions extends RuntimeRegions,
  Options,
> = ValidateRuntimeOptions<Topics, ConfigRegions, Options>;

type RejectExtraRuntimeKafkaKeys<Options, Shape> = Options extends {
  readonly kafka: infer CandidateKafka;
}
  ? Shape extends {
      readonly kafka: infer RuntimeKafka;
    }
    ? {
        readonly kafka: CandidateKafka & RejectExtraKeys<CandidateKafka, RuntimeKafka>;
      }
    : unknown
  : unknown;

type RejectExtraRuntimeKafkaStartFromKeys<Options> = Options extends {
  readonly kafka: {
    readonly startFrom: infer CandidateStartFrom;
  };
}
  ? CandidateStartFrom extends object
    ? {
        readonly kafka: {
          readonly startFrom: CandidateStartFrom &
            RejectExtraKeys<CandidateStartFrom, ViewServerKafkaCommittedStartFrom>;
        };
      }
    : unknown
  : unknown;

type TopicOwnedKafkaSourceTopic<Topics extends object> = Extract<
  {
    readonly [Topic in keyof Topics]: Topics[Topic] extends {
      readonly kafkaSource: object;
    }
      ? Topic
      : never;
  }[keyof Topics],
  string
>;

type TopicOwnedKafkaSourceRegion<Topics extends object> = Extract<
  {
    readonly [Topic in keyof Topics]: Topics[Topic] extends {
      readonly kafkaSource: {
        readonly regions: infer Regions extends ReadonlyArray<string>;
      };
    }
      ? Regions[number]
      : never;
  }[keyof Topics],
  string
>;

type RuntimeRegionsAreBroad<Regions extends RuntimeRegions> = string extends keyof Regions
  ? true
  : false;

type RuntimeKafkaSourceRegionConstraint<
  Topics extends object,
  ConfigRegions extends RuntimeRegions,
  Options,
> = [TopicOwnedKafkaSourceRegion<Topics>] extends [never]
  ? unknown
  : Options extends {
        readonly kafka: {
          readonly regions: infer Regions extends RuntimeRegions;
        };
      }
    ? RuntimeRegionsAreBroad<Regions> extends true
      ? {
          readonly kafka: {
            readonly regions: never;
          };
        }
      : Exclude<TopicOwnedKafkaSourceRegion<Topics>, keyof Regions> extends never
        ? unknown
        : {
            readonly kafka: {
              readonly regions: never;
            };
          }
    : RuntimeRegionsAreBroad<ConfigRegions> extends true
      ? {
          readonly kafka: {
            readonly regions: never;
          };
        }
      : unknown;

type RuntimeKafkaSourceOwnershipConstraint<Topics extends object, Options> = [
  TopicOwnedKafkaSourceTopic<Topics>,
] extends [never]
  ? unknown
  : Options extends {
        readonly kafka: infer CandidateKafka;
      }
    ? {
        readonly kafka: CandidateKafka & {
          readonly consumerGroupId: string;
        };
      }
    : {
        readonly kafka: never;
      };

export type ExactRuntimeOptions<
  Topics extends KafkaTopicSchemaRegistry,
  ConfigRegions extends RuntimeRegions,
  Options,
> = Options &
  ValidateRuntimeOptions<Topics, ConfigRegions, Options> &
  RejectExtraKeys<Options, ValidateRuntimeOptions<Topics, ConfigRegions, Options>> &
  RejectExtraRuntimeKafkaKeys<Options, ValidateRuntimeOptions<Topics, ConfigRegions, Options>> &
  RejectExtraRuntimeKafkaStartFromKeys<Options> &
  RuntimeKafkaSourceOwnershipConstraint<Topics, Options> &
  RuntimeKafkaSourceRegionConstraint<Topics, ConfigRegions, Options>;

const makeKafkaResolvedSourceTopicWithKey = <
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaTopicSourceMapInputWithKey<
      Topics,
      ViewTopic,
      TopicRegions[number],
      ValueCodec,
      KeyCodec
    >,
  ) => KafkaTopicMappedSourceRow<Topics, ViewTopic, KafkaTopicKeyField<Topics, ViewTopic>>,
>(
  viewServerTopic: ViewTopic,
  topic: KafkaTopicSourceInputWithKey<
    Topics,
    Regions,
    ViewTopic,
    KafkaTopicKeyField<Topics, ViewTopic>,
    ValueCodec,
    KeyCodec,
    TopicRegions,
    Mapping
  >,
): KafkaResolvedSourceTopicDefinition<Topics, Regions, ViewTopic, TopicRegions> => ({
  ...topic,
  [KafkaResolvedSourceTopicTypeId]: true,
  viewServerTopic,
  [KafkaTopicDecodeTypeId]: (input) =>
    Effect.gen(function* () {
      const key = yield* decodeKafkaCodec(topic.key, {
        bytes: input.keyBytes,
        metadata: input.metadata,
      });
      const rowKey = yield* mapKafkaRowKey(() =>
        topic.rowKey({
          key,
          region: input.region,
          metadata: input.metadata,
        }),
      );
      if (input.valueBytes === null) {
        const tombstone: KafkaDecodedTopicSourceMessage<Topics, ViewTopic> = {
          rowKey,
          tombstone: true,
          viewServerTopic,
        };
        return tombstone;
      }
      const value = yield* decodeKafkaCodec(topic.value, {
        bytes: input.valueBytes,
        metadata: input.metadata,
      });
      const mappedRow = yield* mapKafkaPayload(() => ({
        ...topic.map({
          key,
          value,
          region: input.region,
          rowKey,
          schema: input.schema,
          metadata: input.metadata,
        }),
        [input.rowKeyField]: rowKey,
      }));
      const row = yield* validateKafkaMappedRow(input.schema, mappedRow);
      const decoded: KafkaDecodedTopicSourceMessage<Topics, ViewTopic> = {
        row,
        rowKey,
        viewServerTopic,
      };
      return decoded;
    }),
});

const makeKafkaResolvedSourceTopicWithoutKey = <
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaTopicSourceMapInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => KafkaTopicMappedSourceRow<Topics, ViewTopic, KafkaTopicKeyField<Topics, ViewTopic>>,
>(
  viewServerTopic: ViewTopic,
  topic: KafkaTopicSourceInputWithoutKey<
    Topics,
    Regions,
    ViewTopic,
    KafkaTopicKeyField<Topics, ViewTopic>,
    ValueCodec,
    TopicRegions,
    Mapping
  >,
): KafkaResolvedSourceTopicDefinition<Topics, Regions, ViewTopic, TopicRegions> => ({
  ...topic,
  [KafkaResolvedSourceTopicTypeId]: true,
  viewServerTopic,
  [KafkaTopicDecodeTypeId]: (input) =>
    Effect.gen(function* () {
      const key = decodeKafkaStringKey({
        bytes: input.keyBytes,
        metadata: input.metadata,
      });
      const rowKey = yield* mapKafkaRowKey(() =>
        topic.rowKey({
          key,
          region: input.region,
          metadata: input.metadata,
        }),
      );
      if (input.valueBytes === null) {
        const tombstone: KafkaDecodedTopicSourceMessage<Topics, ViewTopic> = {
          rowKey,
          tombstone: true,
          viewServerTopic,
        };
        return tombstone;
      }
      const value = yield* decodeKafkaCodec(topic.value, {
        bytes: input.valueBytes,
        metadata: input.metadata,
      });
      const mappedRow = yield* mapKafkaPayload(() => ({
        ...topic.map({
          key,
          value,
          region: input.region,
          rowKey,
          schema: input.schema,
          metadata: input.metadata,
        }),
        [input.rowKeyField]: rowKey,
      }));
      const row = yield* validateKafkaMappedRow(input.schema, mappedRow);
      const decoded: KafkaDecodedTopicSourceMessage<Topics, ViewTopic> = {
        row,
        rowKey,
        viewServerTopic,
      };
      return decoded;
    }),
});

const makeKafkaResolvedSourceTopic = <
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
>(
  viewServerTopic: ViewTopic,
  topic: KafkaTopicSourceDefinition<
    Topics,
    Regions,
    ViewTopic,
    KafkaTopicKeyField<Topics, ViewTopic>,
    ValueCodec,
    KeyCodec,
    TopicRegions
  >,
): KafkaResolvedSourceTopicDefinition<Topics, Regions, ViewTopic, TopicRegions> => {
  if ("key" in topic) {
    return makeKafkaResolvedSourceTopicWithKey(viewServerTopic, topic);
  }
  return makeKafkaResolvedSourceTopicWithoutKey(viewServerTopic, topic);
};

type KafkaSourceTopicRegistry<
  Topics extends KafkaTopicSchemaRegistry,
  _Regions extends RuntimeRegions,
> = {
  readonly [Topic in keyof Topics]: {
    readonly kafkaSource?: object | undefined;
  };
};

export const makeKafkaResolvedSourceTopics = <
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
>(
  topics: KafkaSourceTopicRegistry<Topics, Regions>,
): ReadonlyArray<
  KafkaResolvedSourceTopicDefinition<Topics, Regions, Extract<keyof Topics, string>>
> => {
  const sourceTopics: Array<
    KafkaResolvedSourceTopicDefinition<Topics, Regions, Extract<keyof Topics, string>>
  > = [];
  for (const viewServerTopic in topics) {
    if (!Object.prototype.hasOwnProperty.call(topics, viewServerTopic)) {
      continue;
    }
    const topicDefinition = Object.getOwnPropertyDescriptor(topics, viewServerTopic)?.value;
    if (!isInspectableObject(topicDefinition)) {
      continue;
    }
    const kafkaSource = Object.getOwnPropertyDescriptor(topicDefinition, "kafkaSource")?.value;
    if (kafkaSource === undefined) {
      continue;
    }
    if (!isKafkaTopicSourceDefinition<Topics, Regions, typeof viewServerTopic>(kafkaSource)) {
      throw new Error(`View Server topic ${viewServerTopic} has an invalid Kafka source.`);
    }
    sourceTopics.push(makeKafkaResolvedSourceTopic(viewServerTopic, kafkaSource));
  }
  return sourceTopics;
};

export const isKafkaTopicSourceDefinition = <
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
>(
  topic: unknown,
): topic is KafkaTopicSourceDefinition<Topics, Regions, ViewTopic> => {
  if (!isInspectableObject(topic)) {
    return false;
  }
  const ownKeys = Object.getOwnPropertyNames(topic);
  const hasOwn = (key: string) => Object.prototype.hasOwnProperty.call(topic, key);
  const sourceKeys = ["topic", "regions", "value", "key", "rowKey", "map"];
  if (sourceKeys.some((key) => key in topic && !hasOwn(key))) {
    return false;
  }
  const hasOwnKey = hasOwn("key");
  const allowedKeys = hasOwnKey
    ? ["topic", "regions", "value", "key", "rowKey", "map"]
    : ["topic", "regions", "value", "rowKey", "map"];
  if (!ownKeys.every((key) => allowedKeys.includes(key))) {
    return false;
  }
  if (!allowedKeys.every((key) => hasOwn(key))) {
    return false;
  }
  const sourceTopic = Object.getOwnPropertyDescriptor(topic, "topic")?.value;
  const regions = Object.getOwnPropertyDescriptor(topic, "regions")?.value;
  const value = Object.getOwnPropertyDescriptor(topic, "value")?.value;
  const key = Object.getOwnPropertyDescriptor(topic, "key")?.value;
  const rowKey = Object.getOwnPropertyDescriptor(topic, "rowKey")?.value;
  const map = Object.getOwnPropertyDescriptor(topic, "map")?.value;
  return (
    typeof sourceTopic === "string" &&
    Array.isArray(regions) &&
    regions.length > 0 &&
    regions.every((region) => typeof region === "string") &&
    isKafkaCodec(value) &&
    ((key === undefined && !hasOwnKey) || isKafkaCodec(key)) &&
    typeof rowKey === "function" &&
    typeof map === "function"
  );
};

export const isKafkaResolvedSourceTopicDefinition = (
  topic: unknown,
): topic is KafkaResolvedSourceTopicDefinition<
  KafkaTopicSchemaRegistry,
  RuntimeRegions,
  string
> => {
  if (typeof topic !== "object" || topic === null) {
    return false;
  }
  return Reflect.get(topic, KafkaResolvedSourceTopicTypeId) === true;
};
