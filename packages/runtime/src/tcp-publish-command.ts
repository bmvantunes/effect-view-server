import type { RowSchema, ViewServerRuntimeError } from "@effect-view-server/config";
import {
  type ViewServerRuntimeDecodedMutationClient,
  type ViewServerRuntimeTopicDefinitions,
  viewServerRuntimeDecodedMutationTrust,
} from "@effect-view-server/config/internal";
import type { ViewServerAuth, ViewServerAuthRequest } from "@effect-view-server/server";
import { validateViewServerAuthRequest, ViewServerAuthError } from "@effect-view-server/server";
import { Effect, Option, Result, Schema } from "effect";

export class ViewServerTcpPublishIngressError extends Schema.TaggedError<ViewServerTcpPublishIngressError>()(
  "ViewServerTcpPublishIngressError",
  {
    cause: Schema.Unknown,
    message: Schema.String,
    phase: Schema.Literals(["configuration", "listen", "decode", "runtime", "backpressure"]),
    topic: Schema.optional(Schema.String),
  },
) {}

export type TcpPublishCommandError =
  | ViewServerAuthError
  | ViewServerRuntimeError
  | ViewServerTcpPublishIngressError;

export type TcpPublishCommandOptions = {
  readonly auth?: ViewServerAuth;
};

export type TcpPublishCommandAuthContext = {
  readonly remoteAddress: Option.Option<string>;
};

type TcpFieldSchema = NonNullable<RowSchema["fields"][string]>;
type TcpDecodePhase = "key" | "patch" | "row";
type TcpFieldInput = Schema.Schema.Type<typeof Schema.Unknown>;
type TcpFieldDefaultDecoder = Effect.Effect<Option.Option<unknown>>;
type TcpConfiguredTopic<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Topic extends Extract<keyof Topics, string> = Extract<keyof Topics, string>,
> = {
  readonly fieldSchemas: ReadonlyMap<string, TcpFieldSchema>;
  readonly schema: Topics[Topic]["schema"];
  readonly topic: Topic;
};

const TcpJsonObject = Schema.Record(Schema.String, Schema.Json);
const TcpHeaders = Schema.Record(Schema.String, Schema.String);
const TcpJsonFromString = Schema.fromJsonString(Schema.Unknown);

const TcpPublishCommandSchema = Schema.Union([
  Schema.Struct({
    headers: Schema.optional(TcpHeaders),
    op: Schema.Literal("publish"),
    topic: Schema.String,
    row: TcpJsonObject,
  }),
  Schema.Struct({
    headers: Schema.optional(TcpHeaders),
    op: Schema.Literal("publishMany"),
    topic: Schema.String,
    rows: Schema.Array(TcpJsonObject),
  }),
  Schema.Struct({
    headers: Schema.optional(TcpHeaders),
    op: Schema.Literal("patch"),
    topic: Schema.String,
    key: Schema.String,
    patch: TcpJsonObject,
  }),
  Schema.Struct({
    headers: Schema.optional(TcpHeaders),
    op: Schema.Literal("delete"),
    topic: Schema.String,
    key: Schema.String,
  }),
]);

type TcpPublishCommand = typeof TcpPublishCommandSchema.Type;

const TcpPublishResponseErrorSchema = Schema.Struct({
  _tag: Schema.String,
  code: Schema.optional(Schema.String),
  message: Schema.String,
  phase: Schema.optional(Schema.String),
  status: Schema.optional(Schema.Union([Schema.Literal(401), Schema.Literal(403)])),
  topic: Schema.optional(Schema.String),
});

export const TcpPublishResponseSchema = Schema.Union([
  Schema.Struct({ ok: Schema.Literal(true) }),
  Schema.Struct({
    error: TcpPublishResponseErrorSchema,
    ok: Schema.Literal(false),
  }),
]);

export type TcpPublishResponse = typeof TcpPublishResponseSchema.Type;

const strictParseOptions = {
  onExcessProperty: "error",
} as const;

const isObjectValue = (value: Schema.Schema.Type<typeof Schema.Unknown>): value is object =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isPlainRecordValue = (
  value: Schema.Schema.Type<typeof Schema.Unknown>,
): value is Record<string, unknown> => {
  if (!isObjectValue(value)) return false;
  try {
    const prototype = Object.getPrototypeOf(value);
    return prototype === Object.prototype || prototype === null;
  } catch {
    return false;
  }
};

const isSchemaFieldRecord = (value: Schema.Schema.Type<typeof Schema.Unknown>): boolean => {
  const inspected = Result.try(() => {
    if (!isPlainRecordValue(value)) return false;
    return Object.keys(value).every((field) => Schema.isSchema(Reflect.get(value, field)));
  });
  return Result.isSuccess(inspected) && inspected.success;
};

const hasConfiguredTopic = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  topics: Topics,
  topic: string,
): topic is Extract<keyof Topics, string> => Object.hasOwn(topics, topic);

const isTopicDefinitionWithSchema = (
  value: Schema.Schema.Type<typeof Schema.Unknown>,
): value is { readonly schema: RowSchema } => {
  const inspected = Result.try(
    () =>
      isObjectValue(value) &&
      Schema.is(Schema.Struct({ schema: Schema.Unknown }))(value) &&
      "schema" in value &&
      Schema.isSchema(value.schema) &&
      "fields" in value.schema &&
      isSchemaFieldRecord(value.schema.fields),
  );
  return Result.isSuccess(inspected) && inspected.success;
};

const tcpDecodeError = (line: string, cause: unknown): ViewServerTcpPublishIngressError =>
  new ViewServerTcpPublishIngressError({
    message: "TCP publish command must be valid JSON.",
    cause: { cause, line },
    phase: "decode",
  });

const parseCommand = Effect.fn("ViewServerRuntime.tcpPublish.command.parse")(function* (
  line: string,
) {
  const value = yield* Schema.decodeUnknownEffect(TcpJsonFromString)(line, strictParseOptions).pipe(
    Effect.mapError((cause) => tcpDecodeError(line, cause)),
  );
  return yield* Result.match(
    Schema.decodeUnknownResult(TcpPublishCommandSchema)(value, strictParseOptions),
    {
      onSuccess: Effect.succeed,
      onFailure: (cause) =>
        Effect.fail(
          new ViewServerTcpPublishIngressError({
            message: "TCP publish command must match the publish command schema.",
            cause,
            phase: "decode",
          }),
        ),
    },
  );
});

const tcpAuthRequest = (
  command: TcpPublishCommand,
  context: TcpPublishCommandAuthContext,
): ViewServerAuthRequest => ({
  headers: command.headers ?? {},
  method: "POST",
  remoteAddress: context.remoteAddress,
  url: "tcp://view-server/tcp-publish",
});

const topicSchema = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: { readonly topics: Topics },
  topic: string,
): Effect.Effect<TcpConfiguredTopic<Topics>, ViewServerTcpPublishIngressError> => {
  if (!hasConfiguredTopic(config.topics, topic)) {
    return Effect.fail(
      new ViewServerTcpPublishIngressError({
        message: `TCP publish cannot find View Server topic ${topic}.`,
        cause: topic,
        phase: "decode",
        topic,
      }),
    );
  }
  const topicDefinition = config.topics[topic];
  if (!isTopicDefinitionWithSchema(topicDefinition)) {
    return Effect.fail(
      new ViewServerTcpPublishIngressError({
        message: `TCP publish cannot find View Server topic ${topic}.`,
        cause: topic,
        phase: "decode",
        topic,
      }),
    );
  }
  const fieldSchemas = new Map(
    Object.entries(topicDefinition.schema.fields).filter(
      (entry): entry is [string, TcpFieldSchema] => entry[1] !== undefined,
    ),
  );
  const keySchema = fieldSchemas.get("id");
  if (keySchema === undefined) {
    return Effect.fail(
      new ViewServerTcpPublishIngressError({
        message: `TCP publish cannot find canonical id field for View Server topic ${topic}.`,
        cause: { key: "id", topic },
        phase: "decode",
        topic,
      }),
    );
  }
  return Effect.succeed({
    fieldSchemas,
    schema: topicDefinition.schema,
    topic,
  });
};

const tcpDecodeSchemaError = (
  topic: string,
  phase: TcpDecodePhase,
  cause: unknown,
): ViewServerTcpPublishIngressError =>
  new ViewServerTcpPublishIngressError({
    message: `TCP publish ${phase} did not match View Server topic ${topic}.`,
    cause,
    phase: "decode",
    topic,
  });

const tcpRuntimeError = (
  topic: string,
  operation: "delete" | "patch" | "publish" | "publishMany",
  cause: unknown,
): ViewServerTcpPublishIngressError =>
  new ViewServerTcpPublishIngressError({
    message: `TCP publish runtime ${operation} failed for topic ${topic}.`,
    cause,
    phase: "runtime",
    topic,
  });

const setDecodedField = <Value>(
  record: Record<string, unknown>,
  field: string,
  value: Value,
): void => {
  Object.defineProperty(record, field, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
};

const decodeTcpFieldForRuntimeInternal = Effect.fn(
  "ViewServerRuntime.tcpPublish.field.decode.internal",
)(function* <A>(
  schema: Schema.Codec<A, unknown, never, never>,
  topic: string,
  phase: TcpDecodePhase,
  value: TcpFieldInput,
) {
  return yield* Result.match(
    Schema.decodeUnknownResult(Schema.toCodecJson(schema))(value, strictParseOptions),
    {
      onSuccess: Effect.succeed,
      onFailure: () =>
        Result.match(Schema.decodeUnknownResult(schema)(value, strictParseOptions), {
          onSuccess: Effect.succeed,
          onFailure: (cause) => Effect.fail(tcpDecodeSchemaError(topic, phase, cause)),
        }),
    },
  );
});

const decodeTcpFieldForRuntime = Effect.fn("ViewServerRuntime.tcpPublish.field.decode")(function* <
  A,
  Value,
>(
  schema: Schema.Codec<A, unknown, never, never>,
  topic: string,
  phase: TcpDecodePhase,
  value: Value,
) {
  return yield* decodeTcpFieldForRuntimeInternal(schema, topic, phase, value);
});

const makeTcpMissingFieldDefaultDecoder = (
  schema: TcpFieldSchema,
  field: string,
): TcpFieldDefaultDecoder => {
  const fieldDefaultSchema = Schema.Struct({ [field]: schema });
  return Effect.suspend(() =>
    Result.match(Schema.decodeUnknownResult(fieldDefaultSchema)({}, strictParseOptions), {
      onSuccess: (decodedDefault) =>
        Object.hasOwn(decodedDefault, field)
          ? Effect.succeed(Option.some(decodedDefault[field]))
          : Effect.succeed(Option.none()),
      onFailure: () => Effect.succeed(Option.none()),
    }),
  ).pipe(Effect.withSpan("ViewServerRuntime.tcpPublish.field.default.decode"));
};

const makeTcpRowDefaultDecoders = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  topicDefinition: TcpConfiguredTopic<Topics, Topic>,
): ReadonlyMap<string, TcpFieldDefaultDecoder> => {
  const defaultDecoders = new Map<string, TcpFieldDefaultDecoder>();
  for (const [field, fieldSchema] of topicDefinition.fieldSchemas) {
    defaultDecoders.set(field, makeTcpMissingFieldDefaultDecoder(fieldSchema, field));
  }
  return defaultDecoders;
};

const decodeTcpKey = Effect.fn("ViewServerRuntime.tcpPublish.key.decode")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(topicDefinition: TcpConfiguredTopic<Topics, Topic>, key: string) {
  const decodedKey = yield* decodeTcpFieldForRuntime(
    Schema.String,
    topicDefinition.topic,
    "key",
    key,
  );
  return decodedKey;
});

const decodeTcpRow = Effect.fn("ViewServerRuntime.tcpPublish.row.decode")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  topicDefinition: TcpConfiguredTopic<Topics, Topic>,
  topic: string,
  row: Record<string, unknown>,
  defaultDecoders: ReadonlyMap<string, TcpFieldDefaultDecoder>,
) {
  const decodedRow: Record<string, unknown> = {};
  for (const [field, value] of Object.entries(row)) {
    const fieldSchema = topicDefinition.fieldSchemas.get(field);
    if (fieldSchema === undefined) {
      return yield* tcpDecodeSchemaError(topic, "row", { field });
    }
    setDecodedField(
      decodedRow,
      field,
      yield* decodeTcpFieldForRuntime(fieldSchema, topic, "row", value),
    );
  }
  for (const [field, defaultDecoder] of defaultDecoders) {
    if (!Object.hasOwn(row, field)) {
      const defaultValue = yield* defaultDecoder;
      if (Option.isSome(defaultValue)) {
        setDecodedField(decodedRow, field, defaultValue.value);
      }
    }
  }
  return yield* topicDefinition.schema
    .makeEffect(decodedRow)
    .pipe(Effect.mapError((cause) => tcpDecodeSchemaError(topic, "row", cause)));
});

const decodeTcpRows = Effect.fn("ViewServerRuntime.tcpPublish.rows.decode")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  topicDefinition: TcpConfiguredTopic<Topics, Topic>,
  topic: string,
  rows: ReadonlyArray<Record<string, unknown>>,
) {
  const defaultDecoders = makeTcpRowDefaultDecoders(topicDefinition);
  return yield* Effect.forEach(rows, (row) =>
    decodeTcpRow(topicDefinition, topic, row, defaultDecoders),
  );
});

const decodeTcpPatch = Effect.fn("ViewServerRuntime.tcpPublish.patch.decode")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  topicDefinition: TcpConfiguredTopic<Topics, Topic>,
  topic: string,
  patch: Record<string, unknown>,
) {
  const decodedPatch: Partial<Topics[Topic]["schema"]["Type"]> = {};
  for (const [field, value] of Object.entries(patch)) {
    const fieldSchema = topicDefinition.fieldSchemas.get(field);
    if (fieldSchema === undefined) {
      return yield* tcpDecodeSchemaError(topic, "patch", { field });
    }
    setDecodedField(
      decodedPatch,
      field,
      yield* decodeTcpFieldForRuntime(fieldSchema, topic, "patch", value),
    );
  }
  return decodedPatch;
});

const mapRuntimeError =
  (topic: string, operation: "delete" | "patch" | "publish" | "publishMany") =>
  (cause: unknown): TcpPublishCommandError =>
    isViewServerRuntimeError(cause) ? cause : tcpRuntimeError(topic, operation, cause);

export const isViewServerRuntimeError = (
  value: Schema.Schema.Type<typeof Schema.Unknown>,
): value is ViewServerRuntimeError => {
  const inspected = Result.try(() => {
    if (!isPlainRecordValue(value)) return false;
    const tag = Reflect.get(value, "_tag");
    const code = Reflect.get(value, "code");
    const message = Reflect.get(value, "message");
    if (
      !Schema.is(Schema.String)(tag) ||
      !Schema.is(Schema.String)(code) ||
      !Schema.is(Schema.String)(message)
    ) {
      return false;
    }
    const ownKeysAre = (expected: ReadonlyArray<string>): boolean => {
      const keys = Reflect.ownKeys(value);
      return (
        keys.length === expected.length &&
        keys.every((key) => typeof key === "string" && expected.includes(key))
      );
    };
    const optionalString = (key: "topic" | "queryId") =>
      !Object.hasOwn(value, key) || Schema.is(Schema.String)(Reflect.get(value, key));
    const optionalNumber = (key: "queuedEvents" | "maxQueueDepth") => {
      if (!Object.hasOwn(value, key)) return true;
      const candidate = Reflect.get(value, key);
      return typeof candidate === "number" && Number.isFinite(candidate);
    };
    if (tag === "ViewServerRuntimeError") {
      const expected = ["_tag", "code", "message"];
      if (Object.hasOwn(value, "topic")) expected.push("topic");
      return (
        [
          "InvalidTopic",
          "InvalidRow",
          "InvalidQuery",
          "UnsupportedQuery",
          "SnapshotStale",
          "RuntimeUnavailable",
          "RuntimeResetFailed",
        ].includes(code) &&
        ownKeysAre(expected) &&
        optionalString("topic")
      );
    }
    if (tag === "ViewServerBackpressureError") {
      const expected = ["_tag", "code", "message"];
      for (const key of ["topic", "queryId", "queuedEvents", "maxQueueDepth"] as const) {
        if (Object.hasOwn(value, key)) expected.push(key);
      }
      return (
        code === "BackpressureExceeded" &&
        ownKeysAre(expected) &&
        optionalString("topic") &&
        optionalString("queryId") &&
        optionalNumber("queuedEvents") &&
        optionalNumber("maxQueueDepth")
      );
    }
    return false;
  });
  return Result.isSuccess(inspected) && inspected.success;
};

export const handleTcpPublishCommandLine = Effect.fn("ViewServerRuntime.tcpPublish.command.handle")(
  function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
    context: TcpPublishCommandAuthContext,
    config: { readonly topics: Topics },
    client: ViewServerRuntimeDecodedMutationClient<Topics>,
    options: TcpPublishCommandOptions,
    line: string,
  ) {
    const command = yield* parseCommand(line);
    yield* validateViewServerAuthRequest(options.auth, tcpAuthRequest(command, context));
    const topicDefinition = yield* topicSchema(config, command.topic);
    yield* client.execute({
      _tag: "CheckMutationAllowed",
      topic: topicDefinition.topic,
    });
    if (command.op === "publish") {
      const row = yield* decodeTcpRow(
        topicDefinition,
        topicDefinition.topic,
        command.row,
        makeTcpRowDefaultDecoders(topicDefinition),
      );
      yield* client
        .execute(
          {
            _tag: "PublishDecodedRows",
            topic: topicDefinition.topic,
            rows: [row],
          },
          viewServerRuntimeDecodedMutationTrust,
        )
        .pipe(Effect.mapError(mapRuntimeError(topicDefinition.topic, "publish")));
      return;
    }
    if (command.op === "publishMany") {
      const rows = yield* decodeTcpRows(topicDefinition, topicDefinition.topic, command.rows);
      yield* client
        .execute(
          {
            _tag: "PublishDecodedRows",
            topic: topicDefinition.topic,
            rows,
          },
          viewServerRuntimeDecodedMutationTrust,
        )
        .pipe(Effect.mapError(mapRuntimeError(topicDefinition.topic, "publishMany")));
      return;
    }
    if (command.op === "patch") {
      const key = yield* decodeTcpKey(topicDefinition, command.key);
      const patch = yield* decodeTcpPatch(topicDefinition, topicDefinition.topic, command.patch);
      yield* client
        .execute(
          {
            _tag: "PatchDecodedFields",
            topic: topicDefinition.topic,
            key,
            patch,
          },
          viewServerRuntimeDecodedMutationTrust,
        )
        .pipe(Effect.mapError(mapRuntimeError(topicDefinition.topic, "patch")));
      return;
    }
    const key = yield* decodeTcpKey(topicDefinition, command.key);
    yield* client
      .execute({
        _tag: "DeleteDecodedRow",
        topic: topicDefinition.topic,
        key,
      })
      .pipe(Effect.mapError(mapRuntimeError(topicDefinition.topic, "delete")));
  },
);
