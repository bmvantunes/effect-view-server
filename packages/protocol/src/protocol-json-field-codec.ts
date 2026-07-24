import { materializeStrictJson } from "@effect-view-server/effect-utils";
import { Effect, Result, Schema } from "effect";

export type JsonFieldSchema = Schema.Codec<unknown, unknown, never, never>;

type JsonFieldCodecErrors<E> = {
  readonly invalid: (message: string) => E;
  readonly notJsonSafe: (message: string) => E;
};

type JsonFieldCodecContext<E> = {
  readonly invalid: (message: string) => E;
  readonly invalidMessage: (message: string) => string;
};

type JsonFieldEncodeContext<E> = JsonFieldCodecContext<E> & {
  readonly notJsonSafe: (message: string) => E;
  readonly notJsonSafeMessage: (message: string) => string;
};

type NamedJsonFieldCodecContext<E> = {
  readonly field: string;
  readonly invalid: (message: string) => E;
  readonly invalidPrefix: string;
};

type NamedJsonFieldEncodeContext<E> = NamedJsonFieldCodecContext<E> & {
  readonly notJsonSafePrefix: string;
};

type TopicNamedJsonFieldCodecContext<E> = {
  readonly invalid: (topic: string, message: string) => E;
  readonly invalidPrefix: string;
};

type TopicNamedJsonFieldEncodeContext<E> = TopicNamedJsonFieldCodecContext<E> & {
  readonly notJsonSafePrefix: string;
};

type CompiledJsonFieldCodec<Row> = {
  readonly accepts: (value: unknown) => boolean;
  readonly decode: (value: unknown) => Effect.Effect<Row, Schema.SchemaError>;
  readonly encode: (value: unknown) => Effect.Effect<Schema.Json, Schema.SchemaError>;
};

const compiledJsonFieldCodecCache = new WeakMap<JsonFieldSchema, CompiledJsonFieldCodec<unknown>>();

const schemaErrorMessage = (error: Schema.SchemaError): string =>
  Result.match(
    Result.try(() => error.message),
    {
      onFailure: () => "Schema validation failed without a safely printable diagnostic.",
      onSuccess: (message) => message,
    },
  );

function compiledJsonFieldCodec<Row>(
  schema: Schema.Codec<Row, unknown, never, never>,
): CompiledJsonFieldCodec<Row>;
function compiledJsonFieldCodec(schema: JsonFieldSchema): CompiledJsonFieldCodec<unknown> {
  const cached = compiledJsonFieldCodecCache.get(schema);
  if (cached !== undefined) {
    return cached;
  }
  const codec = Schema.toCodecJson(schema);
  const compiled: CompiledJsonFieldCodec<unknown> = {
    accepts: Schema.is(schema),
    decode: Schema.decodeUnknownEffect(codec),
    encode: Schema.encodeUnknownEffect(codec),
  };
  compiledJsonFieldCodecCache.set(schema, compiled);
  return compiled;
}

export const materializeJsonFieldValue = Effect.fn("ViewServerProtocol.jsonField.materialize")(
  function* <E>(value: unknown, invalid: (message: string) => E) {
    return yield* Result.match(materializeStrictJson(value), {
      onSuccess: Effect.succeed,
      onFailure: (error) => Effect.fail(invalid(error.message)),
    });
  },
);

export const encodeJsonFieldValue = Effect.fn("ViewServerProtocol.jsonField.encode")(function* <E>(
  schema: JsonFieldSchema,
  value: unknown,
  errors: JsonFieldCodecErrors<E>,
) {
  const compiled = compiledJsonFieldCodec(schema);
  const encoded = yield* compiled.encode(value).pipe(
    Effect.catch((error) => {
      const message = schemaErrorMessage(error);
      if (!compiled.accepts(value)) {
        return Effect.fail(errors.invalid(message));
      }
      return materializeJsonFieldValue(value, errors.notJsonSafe).pipe(
        Effect.andThen(Effect.fail(errors.invalid(message))),
      );
    }),
  );
  return yield* materializeJsonFieldValue(encoded, errors.notJsonSafe);
});

export const decodeMaterializedJsonFieldValue = Effect.fn(
  "ViewServerProtocol.jsonField.decodeMaterialized",
)(function* <Row, E>(
  schema: Schema.Codec<Row, unknown, never, never>,
  value: Schema.Json,
  errors: Pick<JsonFieldCodecErrors<E>, "invalid">,
) {
  return yield* compiledJsonFieldCodec(schema)
    .decode(value)
    .pipe(Effect.mapError((error) => errors.invalid(schemaErrorMessage(error))));
});

export const decodeJsonFieldValue = Effect.fn("ViewServerProtocol.jsonField.decode")(function* <
  Row,
  E,
>(
  schema: Schema.Codec<Row, unknown, never, never>,
  value: unknown,
  errors: Pick<JsonFieldCodecErrors<E>, "invalid">,
) {
  const materialized = yield* materializeJsonFieldValue(value, errors.invalid);
  return yield* decodeMaterializedJsonFieldValue(schema, materialized, errors);
});

export const encodeContextualJsonFieldValue = Effect.fn(
  "ViewServerProtocol.jsonField.contextual.encode",
)(function* <E>(schema: JsonFieldSchema, value: unknown, context: JsonFieldEncodeContext<E>) {
  return yield* encodeJsonFieldValue(schema, value, {
    invalid: (message) => context.invalid(context.invalidMessage(message)),
    notJsonSafe: (message) => context.notJsonSafe(context.notJsonSafeMessage(message)),
  });
});

export const decodeContextualJsonFieldValue = Effect.fn(
  "ViewServerProtocol.jsonField.contextual.decode",
)(function* <Row, E>(
  schema: Schema.Codec<Row, unknown, never, never>,
  value: unknown,
  context: JsonFieldCodecContext<E>,
) {
  return yield* decodeJsonFieldValue(schema, value, {
    invalid: (message) => context.invalid(context.invalidMessage(message)),
  });
});

export const encodeNamedJsonFieldValue = Effect.fn("ViewServerProtocol.jsonField.named.encode")(
  function* <E>(
    schema: JsonFieldSchema,
    value: unknown,
    { field, invalid, invalidPrefix, notJsonSafePrefix }: NamedJsonFieldEncodeContext<E>,
  ) {
    return yield* encodeContextualJsonFieldValue(schema, value, {
      invalid,
      invalidMessage: (message) => `${invalidPrefix} ${field}: ${message}`,
      notJsonSafe: invalid,
      notJsonSafeMessage: (message) => `${notJsonSafePrefix} ${field} is not JSON-safe: ${message}`,
    });
  },
);

export const decodeNamedJsonFieldValue = Effect.fn("ViewServerProtocol.jsonField.named.decode")(
  function* <Row, E>(
    schema: Schema.Codec<Row, unknown, never, never>,
    value: unknown,
    { field, invalid, invalidPrefix }: NamedJsonFieldCodecContext<E>,
  ) {
    return yield* decodeContextualJsonFieldValue(schema, value, {
      invalid,
      invalidMessage: (message) => `${invalidPrefix} ${field}: ${message}`,
    });
  },
);

export const encodeTopicNamedJsonFieldValue = Effect.fn(
  "ViewServerProtocol.jsonField.topicNamed.encode",
)(function* <E>(
  topic: string,
  field: string,
  schema: JsonFieldSchema,
  value: unknown,
  context: TopicNamedJsonFieldEncodeContext<E>,
) {
  return yield* encodeNamedJsonFieldValue(schema, value, {
    field,
    invalid: (message) => context.invalid(topic, message),
    invalidPrefix: context.invalidPrefix,
    notJsonSafePrefix: context.notJsonSafePrefix,
  });
});

export const decodeTopicNamedJsonFieldValue = Effect.fn(
  "ViewServerProtocol.jsonField.topicNamed.decode",
)(function* <Row, E>(
  topic: string,
  field: string,
  schema: Schema.Codec<Row, unknown, never, never>,
  value: unknown,
  context: TopicNamedJsonFieldCodecContext<E>,
) {
  return yield* decodeNamedJsonFieldValue(schema, value, {
    field,
    invalid: (message) => context.invalid(topic, message),
    invalidPrefix: context.invalidPrefix,
  });
});
