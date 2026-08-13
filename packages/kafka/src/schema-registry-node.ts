import { createUndiciAgent } from "@platformatic/kafka";
import { fromBinary } from "@bufbuild/protobuf";
import { FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { Buffer } from "node:buffer";
import type { ConnectionOptions as NodeTlsConnectionOptions } from "node:tls";
import { Duration, Effect, Schedule, Schema, Scope } from "effect";
import type {
  KafkaSchemaRegistryReader,
  KafkaSchemaRegistryRequestFailure,
  KafkaSchemaRegistrySchemaVersion,
} from "./schema-registry-contract";

export type KafkaSchemaRegistryHttpAuth =
  | {
      readonly _tag: "Basic";
      readonly username: string;
      readonly password: string;
    }
  | {
      readonly _tag: "Bearer";
      readonly token: string;
    };

export type KafkaSchemaRegistryHttpOptions = {
  readonly url: string;
  readonly auth?: KafkaSchemaRegistryHttpAuth;
  readonly headers: Readonly<Record<string, string>>;
  readonly timeout: number;
  readonly retries: number;
  readonly retryDelay: number;
  readonly tls?: NodeTlsConnectionOptions;
};

type HttpFailure = KafkaSchemaRegistryRequestFailure & {
  readonly retryable: boolean;
};

interface KafkaSchemaRegistryDispatcher {
  readonly close?: () => unknown;
}

const retriableSchemaRegistryStatuses = new Set([408, 425, 429, 500, 502, 503, 504]);

const requestFailure = (message: string, retryable: boolean): HttpFailure => ({
  message,
  retryable,
});

const closeDispatcher = (dispatcher: KafkaSchemaRegistryDispatcher): Effect.Effect<void> =>
  Effect.tryPromise({
    try: () => {
      const close = Reflect.get(dispatcher, "close");
      return typeof close === "function"
        ? Promise.resolve(Reflect.apply(close, dispatcher, []))
        : Promise.resolve();
    },
    catch: () => ({ _tag: "KafkaSchemaRegistryDispatcherCloseFailure" as const }),
  }).pipe(
    Effect.catch(() => Effect.void),
    Effect.asVoid,
  );

const makeDispatcher = (
  tls: NodeTlsConnectionOptions | undefined,
): Effect.Effect<
  KafkaSchemaRegistryDispatcher | undefined,
  KafkaSchemaRegistryRequestFailure,
  Scope.Scope
> =>
  tls === undefined
    ? Effect.void.pipe(Effect.as(undefined))
    : Effect.acquireRelease(
        Effect.try({
          try: () => {
            const dispatcher: unknown = createUndiciAgent({ connect: tls });
            if (typeof dispatcher !== "object" || dispatcher === null) {
              throw new TypeError("Platformatic returned an invalid Undici dispatcher.");
            }
            return dispatcher;
          },
          catch: () => ({ message: "Schema Registry TLS dispatcher creation failed." }),
        }),
        closeDispatcher,
      );

const authorization = (auth: KafkaSchemaRegistryHttpAuth | undefined): string | undefined =>
  auth?._tag === "Basic"
    ? `Basic ${Buffer.from(`${auth.username}:${auth.password}`, "utf8").toString("base64")}`
    : auth?._tag === "Bearer"
      ? `Bearer ${auth.token}`
      : undefined;

type SchemaRegistryHttpResponse =
  | {
      readonly _tag: "BodyFailure";
    }
  | {
      readonly _tag: "Response";
      readonly body: string;
      readonly ok: boolean;
      readonly status: number;
    };

const requestAttempt = (
  options: KafkaSchemaRegistryHttpOptions,
  dispatcher: KafkaSchemaRegistryDispatcher | undefined,
  path: string,
): Effect.Effect<string, HttpFailure> => {
  const url = new URL(path.slice(1), options.url);
  const auth = authorization(options.auth);
  const headers = {
    accept: "application/vnd.schemaregistry.v1+json",
    ...options.headers,
    ...(auth === undefined ? {} : { authorization: auth }),
  };
  return Effect.tryPromise({
    try: async (signal): Promise<SchemaRegistryHttpResponse> => {
      const response = await fetch(url, {
        method: "GET",
        headers,
        signal,
        ...(dispatcher === undefined ? {} : { dispatcher }),
      });
      try {
        return {
          _tag: "Response",
          body: await response.text(),
          ok: response.ok,
          status: response.status,
        };
      } catch {
        return { _tag: "BodyFailure" };
      }
    },
    catch: () => requestFailure("Schema Registry request failed.", true),
  }).pipe(
    Effect.timeout(Duration.millis(options.timeout)),
    Effect.mapError((failure) =>
      "retryable" in failure ? failure : requestFailure("Schema Registry request timed out.", true),
    ),
    Effect.flatMap((response) => {
      if (response._tag === "BodyFailure") {
        return Effect.fail(
          requestFailure("Schema Registry response body could not be read.", true),
        );
      }
      return response.ok
        ? Effect.succeed(response.body)
        : Effect.fail(
            requestFailure(
              `Schema Registry request failed with HTTP ${String(response.status)}.`,
              retriableSchemaRegistryStatuses.has(response.status),
            ),
          );
    }),
  );
};

const request = (
  options: KafkaSchemaRegistryHttpOptions,
  dispatcher: KafkaSchemaRegistryDispatcher | undefined,
  path: string,
): Effect.Effect<string, KafkaSchemaRegistryRequestFailure> =>
  requestAttempt(options, dispatcher, path).pipe(
    Effect.retry({
      schedule: Schedule.exponential(Duration.millis(options.retryDelay)).pipe(
        Schedule.jittered,
        Schedule.modifyDelay(({ duration }) =>
          Effect.succeed(Duration.millis(Math.ceil(Duration.toMillis(duration)))),
        ),
      ),
      times: options.retries,
      while: (failure) => failure.retryable,
    }),
    Effect.mapError((failure) => ({ message: failure.message })),
  );

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
const CompatibilityResponseJson = Schema.fromJsonString(
  Schema.Struct({ compatibilityLevel: Schema.NonEmptyString }),
);
const VersionsResponseJson = Schema.fromJsonString(Schema.Array(PositiveInt));
const ReferenceResponse = Schema.Struct({
  name: Schema.NonEmptyString,
  subject: Schema.NonEmptyString,
  version: PositiveInt,
});
const SchemaResponseJson = Schema.fromJsonString(
  Schema.Struct({
    subject: Schema.NonEmptyString,
    version: PositiveInt,
    id: PositiveInt,
    schemaType: Schema.NonEmptyString,
    schema: Schema.String,
    references: Schema.optionalKey(Schema.Array(ReferenceResponse)),
  }),
);

const decodeCompatibilityResponse = Schema.decodeUnknownEffect(CompatibilityResponseJson);
const decodeVersionsResponse = Schema.decodeUnknownEffect(VersionsResponseJson);
const decodeSchemaResponse = Schema.decodeUnknownEffect(SchemaResponseJson);

const compatibilityResponse = (
  body: string,
): Effect.Effect<string, KafkaSchemaRegistryRequestFailure> =>
  decodeCompatibilityResponse(body).pipe(
    Effect.map((response) => response.compatibilityLevel),
    Effect.mapError(() => ({
      message: "Schema Registry returned malformed compatibility JSON.",
    })),
  );

const versionsResponse = (
  body: string,
): Effect.Effect<ReadonlyArray<number>, KafkaSchemaRegistryRequestFailure> =>
  decodeVersionsResponse(body).pipe(
    Effect.map((versions) => Object.freeze([...versions])),
    Effect.mapError(() => ({ message: "Schema Registry returned malformed versions JSON." })),
  );

const strictBase64 = (value: string): Uint8Array | undefined => {
  if (
    value.length === 0 ||
    value.length % 4 !== 0 ||
    !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)
  ) {
    return undefined;
  }
  return Buffer.from(value, "base64");
};

const schemaResponse = (
  expectedSubject: string,
  expectedVersion: number,
  body: string,
): Effect.Effect<KafkaSchemaRegistrySchemaVersion, KafkaSchemaRegistryRequestFailure> =>
  decodeSchemaResponse(body).pipe(
    Effect.mapError(() => ({ message: "Schema Registry returned malformed schema JSON." })),
    Effect.flatMap((response) => {
      if (response.subject !== expectedSubject || response.version !== expectedVersion) {
        return Effect.fail({ message: "Schema Registry returned malformed schema JSON." });
      }
      const bytes = strictBase64(response.schema);
      if (bytes === undefined) {
        return Effect.fail({
          message: "Schema Registry returned malformed serialized protobuf.",
        });
      }
      return Effect.try({
        try: (): KafkaSchemaRegistrySchemaVersion => ({
          subject: response.subject,
          version: response.version,
          id: response.id,
          schemaType: response.schemaType,
          references: Object.freeze([...(response.references ?? [])]),
          descriptor: fromBinary(FileDescriptorProtoSchema, bytes),
        }),
        catch: () => ({ message: "Schema Registry returned malformed serialized protobuf." }),
      });
    }),
  );

export const makeKafkaSchemaRegistryReader = Effect.fn("KafkaSchemaRegistryNode.makeReader")(
  function* (
    options: KafkaSchemaRegistryHttpOptions,
  ): Effect.fn.Return<KafkaSchemaRegistryReader, KafkaSchemaRegistryRequestFailure, Scope.Scope> {
    const dispatcher = yield* makeDispatcher(options.tls);
    const encodedSubject = (subject: string): string => encodeURIComponent(subject);
    return {
      effectiveCompatibility: (subject) =>
        request(
          options,
          dispatcher,
          `/config/${encodedSubject(subject)}?defaultToGlobal=true`,
        ).pipe(Effect.flatMap(compatibilityResponse)),
      versions: (subject, includeDeleted) =>
        request(
          options,
          dispatcher,
          `/subjects/${encodedSubject(subject)}/versions${includeDeleted ? "?deleted=true" : ""}`,
        ).pipe(Effect.flatMap(versionsResponse)),
      schema: (subject, version) =>
        request(
          options,
          dispatcher,
          `/subjects/${encodedSubject(subject)}/versions/${String(version)}?deleted=true&format=serialized`,
        ).pipe(Effect.flatMap((value) => schemaResponse(subject, version, value))),
    };
  },
);

export const kafkaSchemaRegistryHttpDefaults = Object.freeze({
  timeout: 5_000,
  retries: 3,
  retryDelay: 1_000,
});
