import type { RowSchema, ViewServerRuntimeError } from "@effect-view-server/config";
import type {
  SourceOwnershipPolicy,
  ViewServerRuntimeCoreInternalClient,
} from "@effect-view-server/runtime-core/internal";
import type { ViewServerAuth, ViewServerAuthRequest } from "@effect-view-server/server";
import { validateViewServerAuthRequest, ViewServerAuthError } from "@effect-view-server/server";
import { Effect, Option, Result, Schema } from "effect";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export class ViewServerTcpPublishIngressError extends Schema.TaggedErrorClass<ViewServerTcpPublishIngressError>()(
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
type TcpConfiguredTopic<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly fieldSchemas: ReadonlyMap<string, TcpFieldSchema>;
  readonly keyField: string;
  readonly keySchema: TcpFieldSchema;
  readonly schema: RowSchema;
  readonly topic: Extract<keyof Topics, string>;
};

const TcpJsonObject = Schema.Record(Schema.String, Schema.Json);
const TcpHeaders = Schema.Record(Schema.String, Schema.String);

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

const strictParseOptions = {
  onExcessProperty: "error",
} as const;

const hasConfiguredTopic = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  topics: Topics,
  topic: string,
): topic is Extract<keyof Topics, string> => Object.hasOwn(topics, topic);

const isTopicDefinitionWithSchema = (
  value: unknown,
): value is { readonly key: string; readonly schema: RowSchema } =>
  typeof value === "object" &&
  value !== null &&
  "key" in value &&
  typeof value.key === "string" &&
  "schema" in value &&
  Schema.isSchema(value.schema);

const tcpDecodeError = (line: string, cause: unknown): ViewServerTcpPublishIngressError =>
  new ViewServerTcpPublishIngressError({
    message: "TCP publish command must be valid JSON.",
    cause: { cause, line },
    phase: "decode",
  });

const parseCommand = Effect.fn("ViewServerRuntime.tcpPublish.command.parse")(function* (
  line: string,
) {
  const value = yield* Schema.decodeUnknownEffect(Schema.UnknownFromJsonString)(
    line,
    strictParseOptions,
  ).pipe(Effect.mapError((cause) => tcpDecodeError(line, cause)));
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
  const keySchema = fieldSchemas.get(topicDefinition.key);
  if (keySchema === undefined) {
    return Effect.fail(
      new ViewServerTcpPublishIngressError({
        message: `TCP publish cannot find key field ${topicDefinition.key} for View Server topic ${topic}.`,
        cause: { key: topicDefinition.key, topic },
        phase: "decode",
        topic,
      }),
    );
  }
  return Effect.succeed({
    fieldSchemas,
    keyField: topicDefinition.key,
    keySchema,
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

const setDecodedField = (record: Record<string, unknown>, field: string, value: unknown): void => {
  Object.defineProperty(record, field, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
};

const decodeTcpFieldForRuntimeInternal: (
  schema: TcpFieldSchema,
  topic: string,
  phase: TcpDecodePhase,
  value: unknown,
) => Effect.Effect<unknown, ViewServerTcpPublishIngressError> = Effect.fn(
  "ViewServerRuntime.tcpPublish.field.decode.internal",
)(function* (schema, topic, phase, value) {
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

const decodeTcpFieldForRuntime = Effect.fn("ViewServerRuntime.tcpPublish.field.decode")(function* (
  schema: TcpFieldSchema,
  topic: string,
  phase: TcpDecodePhase,
  value: unknown,
) {
  return yield* decodeTcpFieldForRuntimeInternal(schema, topic, phase, value);
});

const decodeTcpKey = Effect.fn("ViewServerRuntime.tcpPublish.key.decode")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(topicDefinition: TcpConfiguredTopic<Topics>, key: string) {
  const decodedKey = yield* decodeTcpFieldForRuntime(
    topicDefinition.keySchema,
    topicDefinition.topic,
    "key",
    key,
  );
  if (typeof decodedKey !== "string") {
    return yield* tcpDecodeSchemaError(topicDefinition.topic, "key", {
      key: topicDefinition.keyField,
      value: key,
    });
  }
  return decodedKey;
});

const decodeTcpRow = Effect.fn("ViewServerRuntime.tcpPublish.row.decode")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(topicDefinition: TcpConfiguredTopic<Topics>, topic: string, row: Record<string, unknown>) {
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
  yield* Schema.decodeUnknownEffect(Schema.toType(topicDefinition.schema))(
    decodedRow,
    strictParseOptions,
  ).pipe(
    Effect.asVoid,
    Effect.mapError((cause) => tcpDecodeSchemaError(topic, "row", cause)),
  );
  return decodedRow;
});

const decodeTcpRows = Effect.fn("ViewServerRuntime.tcpPublish.rows.decode")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  topicDefinition: TcpConfiguredTopic<Topics>,
  topic: string,
  rows: ReadonlyArray<Record<string, unknown>>,
) {
  return yield* Effect.forEach(rows, (row) => decodeTcpRow(topicDefinition, topic, row));
});

const decodeTcpPatch = Effect.fn("ViewServerRuntime.tcpPublish.patch.decode")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(topicDefinition: TcpConfiguredTopic<Topics>, topic: string, patch: Record<string, unknown>) {
  const decodedPatch: Record<string, unknown> = {};
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

const ensureTopicCanBeMutated = (
  topic: string,
  sourceOwnership: SourceOwnershipPolicy,
): Effect.Effect<void, ViewServerRuntimeError> =>
  sourceOwnership.requirePublicMutationAllowed(topic, "runtimeCore");

const mapRuntimeError =
  (topic: string, operation: "delete" | "patch" | "publish" | "publishMany") =>
  (cause: unknown): TcpPublishCommandError =>
    isViewServerRuntimeError(cause) ? cause : tcpRuntimeError(topic, operation, cause);

const isViewServerRuntimeError = (value: unknown): value is ViewServerRuntimeError =>
  typeof value === "object" &&
  value !== null &&
  "_tag" in value &&
  (value._tag === "ViewServerRuntimeError" || value._tag === "ViewServerBackpressureError");

export const handleTcpPublishCommandLine = Effect.fn("ViewServerRuntime.tcpPublish.command.handle")(
  function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
    context: TcpPublishCommandAuthContext,
    config: { readonly topics: Topics },
    client: ViewServerRuntimeCoreInternalClient<Topics>,
    options: TcpPublishCommandOptions,
    sourceOwnership: SourceOwnershipPolicy,
    line: string,
  ) {
    const command = yield* parseCommand(line);
    yield* validateViewServerAuthRequest(options.auth, tcpAuthRequest(command, context));
    const topicDefinition = yield* topicSchema(config, command.topic);
    yield* ensureTopicCanBeMutated(topicDefinition.topic, sourceOwnership);
    if (command.op === "publish") {
      const row = yield* decodeTcpRow(topicDefinition, topicDefinition.topic, command.row);
      yield* client
        .publishManyDecodedRows(topicDefinition.topic, [row])
        .pipe(Effect.mapError(mapRuntimeError(topicDefinition.topic, "publish")));
      return;
    }
    if (command.op === "publishMany") {
      const rows = yield* decodeTcpRows(topicDefinition, topicDefinition.topic, command.rows);
      yield* client
        .publishManyDecodedRows(topicDefinition.topic, rows)
        .pipe(Effect.mapError(mapRuntimeError(topicDefinition.topic, "publishMany")));
      return;
    }
    if (command.op === "patch") {
      const key = yield* decodeTcpKey(topicDefinition, command.key);
      const patch = yield* decodeTcpPatch(topicDefinition, topicDefinition.topic, command.patch);
      yield* client
        .patchDecodedFields(topicDefinition.topic, key, patch)
        .pipe(Effect.mapError(mapRuntimeError(topicDefinition.topic, "patch")));
      return;
    }
    const key = yield* decodeTcpKey(topicDefinition, command.key);
    yield* client
      .delete(topicDefinition.topic, key)
      .pipe(Effect.mapError(mapRuntimeError(topicDefinition.topic, "delete")));
  },
);
