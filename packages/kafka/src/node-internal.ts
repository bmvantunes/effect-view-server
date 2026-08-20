import { Admin, ConfigResourceTypes, Consumer } from "@platformatic/kafka";
import type { ConsumerGroupJoinPayload, Message, Offsets } from "@platformatic/kafka";
import { Buffer } from "node:buffer";
import type { ConnectionOptions as NodeTlsConnectionOptions } from "node:tls";
import {
  Config,
  Deferred,
  Duration,
  Effect,
  Exit,
  Layer,
  Option,
  Result,
  Schema,
  SchemaIssue,
  Scope,
  Stream,
} from "effect";
import type {
  SourceApplicationExit,
  SourceDefinitionAdapter,
  SourceDefinitionOptions,
} from "effect-view-server/source-adapter";
import {
  KafkaSourceAdapter,
  KafkaSourceConfigurationError,
  decodeKafkaDurationInput,
  kafkaConsumerGroupId,
  isKafkaSchemaRegistryProtobufCodec,
  type KafkaAdapterFailure,
  type KafkaCodecSchemaRegistryRequirement,
  type KafkaRegionMetrics,
} from "./contract";
import {
  type KafkaBrokerConfigResource,
  type KafkaBrokerContractDeclaration,
  type KafkaBrokerContractValidationFailure as KafkaBrokerContractValidationFailureType,
  type KafkaBrokerRegionDiscovery,
  type KafkaResolvedBrokerContract,
  resolveKafkaBrokerContracts,
} from "./broker-contract";
import {
  makeKafkaServerLayer,
  type KafkaServerRecord,
  type KafkaServerRegion,
  type KafkaServerRegionAcquireInput,
  type KafkaServerRegionMetricsInput,
} from "./server";
import {
  type KafkaSchemaRegistryContractValidationFailure as KafkaSchemaRegistryContractValidationFailureType,
  type KafkaSchemaRegistryDeclaration,
} from "./schema-registry-contract";
import {
  kafkaSchemaRegistryHttpDefaults,
  makeKafkaSchemaRegistryReader,
  type KafkaSchemaRegistryHttpAuth,
  type KafkaSchemaRegistryHttpOptions,
} from "./schema-registry-node";
import {
  makeKafkaSchemaRegistryRuntime,
  makeKafkaServerSchemaRegistry,
  type KafkaServerSchemaRegistry,
} from "./schema-registry-runtime";

type KafkaNodeViewServer = {
  readonly topics: Readonly<Record<string, object>>;
};

type KafkaDefinitionForSource<Source> = Source extends unknown
  ? SourceDefinitionAdapter<Source> extends typeof KafkaSourceAdapter
    ? Source
    : never
  : never;

type KafkaDefinitionForTopic<Topic> = Topic extends { readonly source: infer Source }
  ? KafkaDefinitionForSource<Source>
  : never;

type KafkaRegionsForTopic<Topic> = [KafkaDefinitionForTopic<Topic>] extends [never]
  ? never
  : SourceDefinitionOptions<KafkaDefinitionForTopic<Topic>> extends {
        readonly regions: readonly [
          infer Region extends string,
          ...ReadonlyArray<infer Region extends string>,
        ];
      }
    ? Region
    : never;

type KafkaSchemaRegistryRegionsForTopic<Topic> = [KafkaDefinitionForTopic<Topic>] extends [never]
  ? never
  : SourceDefinitionOptions<KafkaDefinitionForTopic<Topic>> extends {
        readonly regions: readonly [
          infer Region extends string,
          ...ReadonlyArray<infer Region extends string>,
        ];
        readonly key: infer Key;
        readonly value: infer Value;
      }
    ? true extends KafkaCodecSchemaRegistryRequirement<Key | Value>
      ? Region
      : never
    : never;

export type KafkaRequiredRegion<ViewServer extends KafkaNodeViewServer> = Extract<
  {
    readonly [Topic in keyof ViewServer["topics"]]: KafkaRegionsForTopic<
      ViewServer["topics"][Topic]
    >;
  }[keyof ViewServer["topics"]],
  string
>;

export type KafkaSchemaRegistryRequiredRegion<ViewServer extends KafkaNodeViewServer> = Extract<
  {
    readonly [Topic in keyof ViewServer["topics"]]: KafkaSchemaRegistryRegionsForTopic<
      ViewServer["topics"][Topic]
    >;
  }[keyof ViewServer["topics"]],
  string
>;

export type KafkaNodeTlsOptions = {
  readonly ca?: string | Uint8Array | ReadonlyArray<string | Uint8Array>;
  readonly cert?: string | Uint8Array | ReadonlyArray<string | Uint8Array>;
  readonly key?: string | Uint8Array | ReadonlyArray<string | Uint8Array>;
  readonly rejectUnauthorized?: boolean;
  readonly serverName?: string;
};

export type KafkaNodeSaslOptions =
  | {
      readonly mechanism: "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512";
      readonly username: string;
      readonly password: string;
    }
  | {
      readonly mechanism: "OAUTHBEARER";
      readonly token: string;
    };

export type KafkaNodeSchemaRegistryAuthOptions =
  | {
      readonly username: string;
      readonly password: string;
      readonly token?: never;
    }
  | {
      readonly token: string;
      readonly username?: never;
      readonly password?: never;
    };

export type KafkaNodeSchemaRegistryOptions = {
  readonly url: string;
  readonly auth?: KafkaNodeSchemaRegistryAuthOptions;
  readonly headers?: Readonly<Record<string, string>>;
  readonly timeout?: number;
  readonly retries?: number;
  readonly retryDelay?: number;
  readonly monitorInterval?: Duration.Input;
  readonly tls?: KafkaNodeTlsOptions;
};

export type KafkaNodeRegionOptions = {
  readonly bootstrapServers: string | readonly [string, ...ReadonlyArray<string>];
  readonly clientId?: string;
  readonly connectTimeout?: number;
  readonly requestTimeout?: number;
  readonly timeout?: number;
  readonly retries?: number | boolean;
  readonly metadataMaxAge?: number;
  readonly sasl?: KafkaNodeSaslOptions;
  readonly tls?: KafkaNodeTlsOptions;
  readonly schemaRegistry?: KafkaNodeSchemaRegistryOptions;
};

type KafkaNodeSchemaRegistrySnapshot = Omit<KafkaNodeSchemaRegistryOptions, "monitorInterval"> & {
  readonly headers: Readonly<Record<string, string>>;
  readonly timeout: number;
  readonly retries: number;
  readonly retryDelay: number;
  readonly monitorInterval: Duration.Duration;
};

type KafkaNodeRegionSnapshot = Omit<
  KafkaNodeRegionOptions,
  "bootstrapServers" | "schemaRegistry"
> & {
  readonly bootstrapServers: readonly [string, ...ReadonlyArray<string>];
  readonly schemaRegistry?: KafkaNodeSchemaRegistrySnapshot;
};

export type KafkaNodeLayerOptions<ViewServer extends KafkaNodeViewServer> = [
  KafkaRequiredRegion<ViewServer>,
] extends [never]
  ? never
  : {
      readonly consumerGroupPrefix: string;
      readonly regions: {
        readonly [Region in KafkaRequiredRegion<ViewServer>]: KafkaNodeRegionOptions &
          (Region extends KafkaSchemaRegistryRequiredRegion<ViewServer>
            ? { readonly schemaRegistry: KafkaNodeSchemaRegistryOptions }
            : unknown);
      };
      readonly retentionSweepInterval?: Duration.Input;
    };

type IsConfigPlainObject<Value> = [Value] extends [object]
  ? [keyof Value] extends [never]
    ? false
    : [keyof Value] extends [string]
      ? true
      : false
  : false;

type KafkaNodeConfigValue<Value> = Value extends object
  ? {
      readonly [Key in keyof Value]: Key extends "retentionSweepInterval" | "monitorInterval"
        ? NonNullable<Value[Key]> | string
        : Value[Key];
    }
  : Value;

type KafkaNodeConfigWrap<Value> = [NonNullable<Value>] extends [infer NonNullValue]
  ? IsConfigPlainObject<NonNullValue> extends true
    ?
        | {
            readonly [Key in keyof NonNullValue]: KafkaNodeConfigWrap<
              Key extends "retentionSweepInterval" | "monitorInterval"
                ? NonNullable<NonNullValue[Key]> | string
                : NonNullValue[Key]
            >;
          }
        | Config.Config<KafkaNodeConfigValue<NonNullValue>>
    : Config.Config<NonNullValue>
  : Config.Config<Value>;

type RejectExtraKeys<Candidate, Shape> = {
  readonly [Key in Exclude<keyof Candidate, keyof Shape>]: never;
};

type IsAny<Value> = 0 extends 1 & Value ? true : false;

type IsUnknown<Value> =
  IsAny<Value> extends true
    ? false
    : unknown extends Value
      ? [Value] extends [unknown]
        ? true
        : false
      : false;

type IsNever<Value> = [Value] extends [never] ? true : false;

type ContainsAny<Value, Seen = never> =
  IsAny<Value> extends true
    ? true
    : Value extends Config.Config<infer ConfigValue>
      ? ContainsAny<ConfigValue, Seen | Value>
      : Value extends (...arguments_: ReadonlyArray<never>) => unknown
        ? false
        : Value extends Seen
          ? false
          : Value extends ReadonlyArray<infer Item>
            ? ContainsAny<Item, Seen | Value>
            : Value extends object
              ? true extends {
                  readonly [Key in keyof Value]-?: ContainsAny<Value[Key], Seen | Value>;
                }[keyof Value]
                ? true
                : false
              : false;

type NodeCandidateField<Candidate, Key extends PropertyKey> = Candidate extends unknown
  ? Key extends keyof Candidate
    ? Candidate[Key]
    : never
  : never;

type RejectAnyNodeField<Candidate, Key extends PropertyKey> =
  IsAny<NodeCandidateField<Candidate, Key>> extends true ? never : unknown;

type ExactSaslOptions<Candidate> =
  IsAny<Candidate> extends true
    ? never
    : Candidate extends KafkaNodeSaslOptions
      ? IsAny<Candidate[keyof Candidate]> extends true
        ? never
        : Candidate extends { readonly mechanism: "OAUTHBEARER" }
          ? Candidate &
              RejectExtraKeys<
                Candidate,
                {
                  readonly mechanism: "OAUTHBEARER";
                  readonly token: string;
                }
              >
          : Candidate &
              RejectExtraKeys<
                Candidate,
                {
                  readonly mechanism: "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512";
                  readonly username: string;
                  readonly password: string;
                }
              >
      : never;

type ExactTlsOptions<Candidate> =
  IsAny<Candidate> extends true
    ? never
    : Candidate extends KafkaNodeTlsOptions
      ? IsAny<Candidate[keyof Candidate]> extends true
        ? never
        : Candidate & RejectExtraKeys<Candidate, KafkaNodeTlsOptions>
      : never;

type ExactSchemaRegistryAuthOptions<Candidate> =
  IsAny<Candidate> extends true
    ? never
    : Candidate extends KafkaNodeSchemaRegistryAuthOptions
      ? IsAny<Candidate[keyof Candidate]> extends true
        ? never
        : Candidate extends { readonly token: string }
          ? Candidate & RejectExtraKeys<Candidate, { readonly token: string }>
          : Candidate &
              RejectExtraKeys<Candidate, { readonly username: string; readonly password: string }>
      : never;

type ExactSchemaRegistryOptions<Candidate> =
  IsAny<Candidate> extends true
    ? never
    : Candidate extends KafkaNodeSchemaRegistryOptions
      ? IsAny<Candidate[keyof Candidate]> extends true
        ? never
        : Candidate &
            RejectExtraKeys<Candidate, KafkaNodeSchemaRegistryOptions> &
            (Candidate extends { readonly auth: infer Auth }
              ? { readonly auth: ExactSchemaRegistryAuthOptions<Auth> }
              : unknown) &
            (Candidate extends { readonly tls: infer Tls }
              ? { readonly tls: ExactTlsOptions<Tls> }
              : unknown)
      : never;

type ExactRegionOptions<Candidate> =
  IsAny<Candidate> extends true
    ? never
    : Candidate extends KafkaNodeRegionOptions
      ? IsAny<Candidate[keyof Candidate]> extends true
        ? never
        : Candidate &
            RejectExtraKeys<Candidate, KafkaNodeRegionOptions> &
            (Candidate extends { readonly sasl: infer Sasl }
              ? { readonly sasl: ExactSaslOptions<Sasl> }
              : unknown) &
            (Candidate extends { readonly tls: infer Tls }
              ? { readonly tls: ExactTlsOptions<Tls> }
              : unknown) &
            (Candidate extends { readonly schemaRegistry: infer Registry }
              ? { readonly schemaRegistry: ExactSchemaRegistryOptions<Registry> }
              : unknown)
      : never;

type ExactNodeOptions<ViewServer extends KafkaNodeViewServer, Candidate> =
  Candidate extends KafkaNodeLayerOptions<ViewServer>
    ? Candidate &
        RejectExtraKeys<Candidate, KafkaNodeLayerOptions<ViewServer>> & {
          readonly regions: Candidate["regions"] &
            RejectExtraKeys<Candidate["regions"], KafkaNodeLayerOptions<ViewServer>["regions"]> & {
              readonly [Region in keyof Candidate["regions"]]: ExactRegionOptions<
                Candidate["regions"][Region]
              >;
            };
        } & RejectAnyNodeField<Candidate, "retentionSweepInterval">
    : never;

type KafkaNodeOptionsGuard<ViewServer, Options> =
  IsAny<ViewServer> extends true
    ? never
    : IsAny<Options> extends true
      ? never
      : IsUnknown<Options> extends true
        ? never
        : IsNever<Options> extends true
          ? never
          : ViewServer extends KafkaNodeViewServer
            ? Options extends ExactNodeOptions<ViewServer, Options>
              ? unknown
              : never
            : never;

type UnwrapDurationCandidate<Candidate> =
  IsAny<Candidate> extends true
    ? Candidate
    : Candidate extends Config.Config<infer Value>
      ? IsAny<Value> extends true
        ? Value
        : NonNullable<Value> extends string
          ? Duration.Input
          : NonNullable<Value>
      : Candidate;

type UnwrapConfigCandidate<Candidate> =
  IsAny<Candidate> extends true
    ? Candidate
    : Candidate extends Config.Config<infer Value>
      ? NonNullable<Value>
      : Candidate extends object
        ? {
            readonly [Key in keyof Candidate]: Key extends
              | "retentionSweepInterval"
              | "monitorInterval"
              ? UnwrapDurationCandidate<Candidate[Key]>
              : UnwrapConfigCandidate<Candidate[Key]>;
          }
        : Candidate;

type KafkaBufferMessage = Omit<
  Message<Buffer | null, Buffer | null, Buffer, Buffer>,
  "key" | "value"
> & {
  readonly key: Buffer | null | undefined;
  readonly value: Buffer | null | undefined;
};

type KafkaConsumer = Consumer<Buffer | null, Buffer | null, Buffer, Buffer>;

type KafkaStream = AsyncIterable<KafkaBufferMessage> & {
  readonly close: () => Promise<void>;
};

type KafkaInitialPosition = {
  readonly offsets: ReadonlyArray<{
    readonly topic: string;
    readonly partition: number;
    readonly offset: bigint;
  }>;
  readonly latestOffsets: ReadonlyArray<bigint>;
};

type KafkaMutableRegionMetrics = {
  readonly assignments: Map<
    number,
    {
      partition: number;
      offset: bigint;
      lag: bigint;
    }
  >;
  readonly activePartitions: Set<number>;
  commits: bigint;
  commitFailures: bigint;
  decoded: bigint;
  decodeFailures: bigint;
  mapped: bigint;
  mappingFailures: bigint;
  rejections: bigint;
  reconnects: bigint;
  rebalances: bigint;
  closes: bigint;
  closeFailures: bigint;
};

type KafkaBindingState = {
  readonly committedPartitions: Set<number>;
  initial: KafkaInitialPosition | undefined;
  readonly metrics: KafkaMutableRegionMetrics;
  attempts: bigint;
};

const allowedRegionOptionKeys = [
  "bootstrapServers",
  "clientId",
  "connectTimeout",
  "requestTimeout",
  "timeout",
  "retries",
  "metadataMaxAge",
  "sasl",
  "tls",
  "schemaRegistry",
] as const;

const allowedRegionOptions = new Set<string>(allowedRegionOptionKeys);

const captureDataFields = (value: object, message: string): ReadonlyMap<string, unknown> => {
  const captured = Result.try(() => {
    const fields = new Map<string, unknown>();
    for (const key of Reflect.ownKeys(value)) {
      if (typeof key !== "string") {
        throw new Error("symbol field");
      }
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (descriptor === undefined || descriptor.enumerable !== true || !("value" in descriptor)) {
        throw new Error("non-data field");
      }
      fields.set(key, descriptor.value);
    }
    return fields;
  });
  if (Result.isFailure(captured)) {
    throw new KafkaSourceConfigurationError(message);
  }
  return captured.success;
};

const ownDataKeys = (value: object): ReadonlyArray<string> =>
  Array.from(
    captureDataFields(
      value,
      "Kafka Node options must contain enumerable string data fields.",
    ).keys(),
  );

type CapturedKafkaSource = {
  readonly viewServerTopic: string;
  readonly source: object;
};

const captureKafkaSources = (
  viewServer: KafkaNodeViewServer,
): ReadonlyArray<CapturedKafkaSource> => {
  const sources: Array<CapturedKafkaSource> = [];
  for (const viewServerTopic of Object.keys(viewServer.topics)) {
    const configured = viewServer.topics[viewServerTopic];
    const source =
      typeof configured === "object" && configured !== null && Object.hasOwn(configured, "source")
        ? Reflect.get(configured, "source")
        : undefined;
    if (
      typeof source === "object" &&
      source !== null &&
      Reflect.get(source, "adapter") === KafkaSourceAdapter
    ) {
      sources.push(Object.freeze({ viewServerTopic, source }));
    }
  }
  return Object.freeze(sources);
};

const captureDenseDataArray = (value: unknown, message: string): ReadonlyArray<unknown> => {
  const captured = Result.try(() => {
    if (!Array.isArray(value)) {
      throw new Error("not an array");
    }
    const keys = Reflect.ownKeys(value);
    const length = Option.getOrThrow(
      Option.liftPredicate(
        Reflect.get(
          Option.getOrThrow(
            Option.fromUndefinedOr(Object.getOwnPropertyDescriptor(value, "length")),
          ),
          "value",
        ),
        (candidate): candidate is number => typeof candidate === "number",
      ),
    );
    if (keys.length !== length + 1) {
      throw new Error("invalid length");
    }
    const entries: Array<unknown> = [];
    for (let index = 0; index < length; index += 1) {
      const key = String(index);
      const descriptor = Option.getOrThrow(
        Option.fromUndefinedOr(Object.getOwnPropertyDescriptor(value, key)),
      );
      if (descriptor.enumerable !== true || !("value" in descriptor)) {
        throw new Error("sparse or accessor entry");
      }
      entries.push(descriptor.value);
    }
    return Object.freeze(entries);
  });
  if (Result.isFailure(captured)) {
    throw new KafkaSourceConfigurationError(message);
  }
  return captured.success;
};

const bootstrapServers = (value: unknown): readonly [string, ...ReadonlyArray<string>] => {
  const message = "Kafka Region bootstrapServers must contain only non-empty brokers.";
  const entries = captureDenseDataArray(
    typeof value === "string" ? value.split(",") : value,
    message,
  );
  const [first, ...rest] = entries;
  if (typeof first !== "string" || first.trim().length === 0) {
    throw new KafkaSourceConfigurationError(message);
  }
  const snapshot: [string, ...Array<string>] = [first.trim()];
  for (const entry of rest) {
    if (typeof entry !== "string" || entry.trim().length === 0) {
      throw new KafkaSourceConfigurationError(message);
    }
    snapshot.push(entry.trim());
  }
  return Object.freeze(snapshot);
};

const copyBytes = (value: Uint8Array): Uint8Array => Uint8Array.from(value);

const snapshotTlsValue = (
  value: unknown,
): string | Uint8Array | ReadonlyArray<string | Uint8Array> => {
  const message = "Kafka Region tls options are invalid.";
  const captured = Result.try(() => {
    if (typeof value === "string") {
      return value;
    }
    if (value instanceof Uint8Array) {
      return copyBytes(value);
    }
    const entries = captureDenseDataArray(value, message);
    const snapshot: Array<string | Uint8Array> = [];
    for (const entry of entries) {
      if (typeof entry === "string") {
        snapshot.push(entry);
      } else if (entry instanceof Uint8Array) {
        snapshot.push(copyBytes(entry));
      } else {
        throw new Error("invalid TLS entry");
      }
    }
    return Object.freeze(snapshot);
  });
  if (Result.isFailure(captured)) {
    throw new KafkaSourceConfigurationError(message);
  }
  return captured.success;
};

const copyTlsValue = (
  value: string | Uint8Array | ReadonlyArray<string | Uint8Array>,
): string | Uint8Array | ReadonlyArray<string | Uint8Array> => snapshotTlsValue(value);

const snapshotSasl = (sasl: unknown): KafkaNodeSaslOptions => {
  if (typeof sasl !== "object" || sasl === null || Array.isArray(sasl)) {
    throw new KafkaSourceConfigurationError("Kafka Region sasl options are invalid.");
  }
  const fields = captureDataFields(sasl, "Kafka Region sasl options are invalid.");
  const mechanism = fields.get("mechanism");
  const token = fields.get("token");
  if (mechanism === "OAUTHBEARER") {
    if (fields.size !== 2 || !fields.has("token") || typeof token !== "string") {
      throw new KafkaSourceConfigurationError("Kafka Region sasl options are invalid.");
    }
    return Object.freeze({
      mechanism,
      token,
    });
  }
  const username = fields.get("username");
  const password = fields.get("password");
  if (
    (mechanism !== "PLAIN" && mechanism !== "SCRAM-SHA-256" && mechanism !== "SCRAM-SHA-512") ||
    fields.size !== 3 ||
    !fields.has("username") ||
    !fields.has("password") ||
    typeof username !== "string" ||
    typeof password !== "string"
  ) {
    throw new KafkaSourceConfigurationError("Kafka Region sasl options are invalid.");
  }
  return Object.freeze({
    mechanism,
    username,
    password,
  });
};

const snapshotTls = (tls: unknown): KafkaNodeTlsOptions => {
  const allowed = ["ca", "cert", "key", "rejectUnauthorized", "serverName"];
  if (typeof tls !== "object" || tls === null || Array.isArray(tls)) {
    throw new KafkaSourceConfigurationError("Kafka Region tls options are invalid.");
  }
  const fields = captureDataFields(tls, "Kafka Region tls options are invalid.");
  const ca = fields.get("ca");
  const cert = fields.get("cert");
  const key = fields.get("key");
  const rejectUnauthorized = fields.get("rejectUnauthorized");
  const serverName = fields.get("serverName");
  if (
    Array.from(fields.keys()).some((name) => !allowed.includes(name)) ||
    (rejectUnauthorized !== undefined && typeof rejectUnauthorized !== "boolean") ||
    (serverName !== undefined && typeof serverName !== "string")
  ) {
    throw new KafkaSourceConfigurationError("Kafka Region tls options are invalid.");
  }
  const caSnapshot = ca === undefined ? undefined : snapshotTlsValue(ca);
  const certSnapshot = cert === undefined ? undefined : snapshotTlsValue(cert);
  const keySnapshot = key === undefined ? undefined : snapshotTlsValue(key);
  return Object.freeze({
    ...(caSnapshot === undefined ? {} : { ca: caSnapshot }),
    ...(certSnapshot === undefined ? {} : { cert: certSnapshot }),
    ...(keySnapshot === undefined ? {} : { key: keySnapshot }),
    ...(rejectUnauthorized === undefined ? {} : { rejectUnauthorized }),
    ...(serverName === undefined ? {} : { serverName }),
  });
};

const nodeTlsValue = (
  value: string | Uint8Array | ReadonlyArray<string | Uint8Array>,
): string | Buffer | Array<string | Buffer> => {
  if (typeof value === "string") {
    return value;
  }
  if (value instanceof Uint8Array) {
    return Buffer.from(value);
  }
  return value.map((entry) => (typeof entry === "string" ? entry : Buffer.from(entry)));
};

const nodeTlsOptions = (tls: KafkaNodeTlsOptions): NodeTlsConnectionOptions => ({
  ...(tls.ca === undefined ? {} : { ca: nodeTlsValue(tls.ca) }),
  ...(tls.cert === undefined ? {} : { cert: nodeTlsValue(tls.cert) }),
  ...(tls.key === undefined ? {} : { key: nodeTlsValue(tls.key) }),
  ...(tls.rejectUnauthorized === undefined ? {} : { rejectUnauthorized: tls.rejectUnauthorized }),
  ...(tls.serverName === undefined ? {} : { servername: tls.serverName }),
});

const schemaRegistryHttpAuth = (
  auth: KafkaNodeSchemaRegistryAuthOptions,
): KafkaSchemaRegistryHttpAuth =>
  "token" in auth
    ? {
        _tag: "Bearer",
        token: auth.token,
      }
    : {
        _tag: "Basic",
        username: auth.username,
        password: auth.password,
      };

const schemaRegistryHttpOptions = (
  options: KafkaNodeSchemaRegistrySnapshot,
): KafkaSchemaRegistryHttpOptions => ({
  url: options.url,
  ...(options.auth === undefined ? {} : { auth: schemaRegistryHttpAuth(options.auth) }),
  headers: options.headers,
  timeout: options.timeout,
  retries: options.retries,
  retryDelay: options.retryDelay,
  ...(options.tls === undefined ? {} : { tls: nodeTlsOptions(options.tls) }),
});

const finiteNonNegative = (value: number | undefined): boolean =>
  value === undefined || (Number.isFinite(value) && value >= 0);

const positiveDuration = (value: unknown, message: string): Duration.Duration => {
  const duration = decodeKafkaDurationInput(value);
  const nanos = Option.flatMap(duration, Duration.toNanos);
  if (Option.isNone(nanos) || nanos.value <= 0n) {
    throw new KafkaSourceConfigurationError(message);
  }
  return Option.getOrThrow(duration);
};

const snapshotSchemaRegistryAuth = (value: unknown): KafkaNodeSchemaRegistryAuthOptions => {
  const message = "Kafka Schema Registry auth options are invalid.";
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new KafkaSourceConfigurationError(message);
  }
  const fields = captureDataFields(value, message);
  const token = fields.get("token");
  if (fields.size === 1 && typeof token === "string" && token.length > 0) {
    return Object.freeze({ token });
  }
  const username = fields.get("username");
  const password = fields.get("password");
  if (
    fields.size !== 2 ||
    typeof username !== "string" ||
    username.length === 0 ||
    typeof password !== "string"
  ) {
    throw new KafkaSourceConfigurationError(message);
  }
  return Object.freeze({ username, password });
};

const snapshotSchemaRegistryHeaders = (
  value: unknown,
  hasAuth: boolean,
): Readonly<Record<string, string>> => {
  const message = "Kafka Schema Registry headers are invalid.";
  if (value === undefined) {
    return Object.freeze({});
  }
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new KafkaSourceConfigurationError(message);
  }
  const snapshot: Record<string, string> = Object.create(null);
  for (const [name, header] of captureDataFields(value, message)) {
    const normalized = name.toLowerCase();
    if (
      !/^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/.test(name) ||
      typeof header !== "string" ||
      !/^[\t\x20-\x7e\x80-\xff]*$/.test(header) ||
      Object.hasOwn(snapshot, normalized) ||
      (hasAuth && normalized === "authorization")
    ) {
      throw new KafkaSourceConfigurationError(message);
    }
    snapshot[normalized] = header;
  }
  return Object.freeze(snapshot);
};

const snapshotSchemaRegistry = (value: unknown): KafkaNodeSchemaRegistrySnapshot => {
  const message = "Kafka Schema Registry options are invalid.";
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new KafkaSourceConfigurationError(message);
  }
  const fields = captureDataFields(value, message);
  const allowed = new Set([
    "url",
    "auth",
    "headers",
    "timeout",
    "retries",
    "retryDelay",
    "monitorInterval",
    "tls",
  ]);
  const url = fields.get("url");
  const auth = fields.get("auth");
  const timeout = fields.get("timeout") ?? kafkaSchemaRegistryHttpDefaults.timeout;
  const retries = fields.get("retries") ?? kafkaSchemaRegistryHttpDefaults.retries;
  const retryDelay = fields.get("retryDelay") ?? kafkaSchemaRegistryHttpDefaults.retryDelay;
  const monitorInterval = fields.get("monitorInterval") ?? Duration.seconds(30);
  const tls = fields.get("tls");
  if (
    Array.from(fields.keys()).some((name) => !allowed.has(name)) ||
    typeof url !== "string" ||
    url.length === 0 ||
    typeof timeout !== "number" ||
    !Number.isFinite(timeout) ||
    timeout <= 0 ||
    typeof retries !== "number" ||
    !Number.isSafeInteger(retries) ||
    retries < 0 ||
    typeof retryDelay !== "number" ||
    !Number.isFinite(retryDelay) ||
    retryDelay < 0 ||
    (tls !== undefined && (typeof tls !== "object" || tls === null))
  ) {
    throw new KafkaSourceConfigurationError(message);
  }
  const normalizedUrl = Result.try(() => {
    const parsed = new URL(url);
    if (
      (parsed.protocol !== "http:" && parsed.protocol !== "https:") ||
      (tls !== undefined && parsed.protocol !== "https:") ||
      parsed.username.length > 0 ||
      parsed.password.length > 0 ||
      parsed.search.length > 0 ||
      parsed.hash.length > 0
    ) {
      throw new Error("invalid URL");
    }
    parsed.pathname = parsed.pathname.endsWith("/") ? parsed.pathname : `${parsed.pathname}/`;
    return parsed.toString();
  });
  if (Result.isFailure(normalizedUrl)) {
    throw new KafkaSourceConfigurationError(message);
  }
  const authSnapshot = auth === undefined ? undefined : snapshotSchemaRegistryAuth(auth);
  return Object.freeze({
    url: normalizedUrl.success,
    ...(authSnapshot === undefined ? {} : { auth: authSnapshot }),
    headers: snapshotSchemaRegistryHeaders(fields.get("headers"), authSnapshot !== undefined),
    timeout,
    retries,
    retryDelay,
    monitorInterval: positiveDuration(
      monitorInterval,
      "Kafka Schema Registry monitorInterval must be a positive finite Effect Duration.",
    ),
    ...(tls === undefined ? {} : { tls: snapshotTls(tls) }),
  });
};

const snapshotRegionOptions = (options: unknown): KafkaNodeRegionSnapshot => {
  if (typeof options !== "object" || options === null || Array.isArray(options)) {
    throw new KafkaSourceConfigurationError("Kafka Region options are invalid.");
  }
  const fields = captureDataFields(options, "Kafka Region options are invalid.");
  const bootstrapServerList = fields.get("bootstrapServers");
  const clientId = fields.get("clientId");
  const connectTimeout = fields.get("connectTimeout");
  const requestTimeout = fields.get("requestTimeout");
  const timeout = fields.get("timeout");
  const retries = fields.get("retries");
  const metadataMaxAge = fields.get("metadataMaxAge");
  const sasl = fields.get("sasl");
  const tls = fields.get("tls");
  const schemaRegistry = fields.get("schemaRegistry");
  if (
    !fields.has("bootstrapServers") ||
    Array.from(fields.keys()).some((key) => !allowedRegionOptions.has(key)) ||
    (connectTimeout !== undefined &&
      (typeof connectTimeout !== "number" || !finiteNonNegative(connectTimeout))) ||
    (requestTimeout !== undefined &&
      (typeof requestTimeout !== "number" || !finiteNonNegative(requestTimeout))) ||
    (timeout !== undefined && (typeof timeout !== "number" || !finiteNonNegative(timeout))) ||
    (metadataMaxAge !== undefined &&
      (typeof metadataMaxAge !== "number" || !finiteNonNegative(metadataMaxAge))) ||
    (retries !== undefined &&
      typeof retries !== "boolean" &&
      (typeof retries !== "number" || !Number.isSafeInteger(retries) || retries < 0)) ||
    (clientId !== undefined && (typeof clientId !== "string" || clientId.length === 0)) ||
    (sasl !== undefined && (typeof sasl !== "object" || sasl === null)) ||
    (tls !== undefined && (typeof tls !== "object" || tls === null)) ||
    (schemaRegistry !== undefined &&
      (typeof schemaRegistry !== "object" || schemaRegistry === null))
  ) {
    throw new KafkaSourceConfigurationError("Kafka Region options are invalid.");
  }
  return Object.freeze({
    bootstrapServers: bootstrapServers(bootstrapServerList),
    ...(clientId === undefined ? {} : { clientId }),
    ...(connectTimeout === undefined ? {} : { connectTimeout }),
    ...(requestTimeout === undefined ? {} : { requestTimeout }),
    ...(timeout === undefined ? {} : { timeout }),
    ...(retries === undefined ? {} : { retries }),
    ...(metadataMaxAge === undefined ? {} : { metadataMaxAge }),
    ...(sasl === undefined ? {} : { sasl: snapshotSasl(sasl) }),
    ...(tls === undefined ? {} : { tls: snapshotTls(tls) }),
    ...(schemaRegistry === undefined
      ? {}
      : { schemaRegistry: snapshotSchemaRegistry(schemaRegistry) }),
  });
};

const kafkaSourceRegionsFrom = (
  sources: ReadonlyArray<CapturedKafkaSource>,
): ReadonlySet<string> => {
  const regions = new Set<string>();
  for (const captured of sources) {
    const { source, viewServerTopic: topic } = captured;
    const options = Reflect.get(source, "options");
    const sourceRegions =
      typeof options === "object" && options !== null && Object.hasOwn(options, "regions")
        ? Reflect.get(options, "regions")
        : undefined;
    if (
      !Array.isArray(sourceRegions) ||
      sourceRegions.length === 0 ||
      !sourceRegions.every(
        (region) => typeof region === "string" && region.length > 0 && !region.includes(":"),
      ) ||
      new Set(sourceRegions).size !== sourceRegions.length
    ) {
      throw new KafkaSourceConfigurationError(
        `Kafka source for Topic ${topic} contains invalid Regions.`,
      );
    }
    for (const region of sourceRegions) {
      regions.add(region);
    }
  }
  return regions;
};

const kafkaSourceRegions = (viewServer: KafkaNodeViewServer): ReadonlySet<string> => {
  if (
    typeof viewServer !== "object" ||
    viewServer === null ||
    typeof viewServer.topics !== "object" ||
    viewServer.topics === null
  ) {
    throw new KafkaSourceConfigurationError("Kafka Node Layer requires a View Server Config.");
  }
  const regions = kafkaSourceRegionsFrom(captureKafkaSources(viewServer));
  if (regions.size === 0) {
    throw new KafkaSourceConfigurationError(
      "Kafka Node Layer requires at least one Kafka Source Definition.",
    );
  }
  return regions;
};

const kafkaSchemaRegistryDeclarations = (
  viewServer: KafkaNodeViewServer,
): ReadonlyArray<KafkaSchemaRegistryDeclaration> =>
  kafkaSchemaRegistryDeclarationsFrom(captureKafkaSources(viewServer));

const kafkaSchemaRegistryDeclarationsFrom = (
  sources: ReadonlyArray<CapturedKafkaSource>,
): ReadonlyArray<KafkaSchemaRegistryDeclaration> => {
  const declarations: Array<KafkaSchemaRegistryDeclaration> = [];
  for (const captured of sources) {
    const { source, viewServerTopic } = captured;
    const options = Reflect.get(source, "options");
    if (typeof options !== "object" || options === null) {
      throw new KafkaSourceConfigurationError(
        `Kafka source for Topic ${viewServerTopic} contains invalid options.`,
      );
    }
    const sourceTopic = Reflect.get(options, "topic");
    const regions = Reflect.get(options, "regions");
    const key = Reflect.get(options, "key");
    const value = Reflect.get(options, "value");
    if (
      typeof sourceTopic !== "string" ||
      sourceTopic.length === 0 ||
      !Array.isArray(regions) ||
      regions.length === 0
    ) {
      throw new KafkaSourceConfigurationError(
        `Kafka source for Topic ${viewServerTopic} contains invalid Schema Registry options.`,
      );
    }
    const codecs = [
      ["key", key],
      ["value", value],
    ] as const;
    for (const [side, codec] of codecs) {
      if (!isKafkaSchemaRegistryProtobufCodec(codec)) {
        continue;
      }
      for (const region of regions) {
        if (typeof region !== "string" || region.length === 0) {
          throw new KafkaSourceConfigurationError(
            `Kafka source for Topic ${viewServerTopic} contains invalid Regions.`,
          );
        }
        declarations.push(
          Object.freeze({
            region,
            viewServerTopic,
            sourceTopic,
            side,
            subject: `${sourceTopic}-${side}`,
            descriptor: codec.descriptor,
          }),
        );
      }
    }
  }
  return Object.freeze(declarations);
};

const capturedRetentionPolicy = (
  value: unknown,
): KafkaBrokerContractDeclaration["retentionPolicy"] => {
  if (typeof value !== "object" || value === null) {
    throw new KafkaSourceConfigurationError("Kafka source contains an invalid retention policy.");
  }
  const tag = Reflect.get(value, "_tag");
  if (tag === "MatchKafkaRetention") {
    return {
      _tag: "MatchKafkaRetention",
    };
  }
  if (tag === "Forever") {
    return {
      _tag: "Forever",
    };
  }
  const durationNanos = Reflect.get(value, "durationNanos");
  if (tag === "Finite" && typeof durationNanos === "bigint" && durationNanos > 0n) {
    return {
      _tag: "Finite",
      durationNanos,
    };
  }
  throw new KafkaSourceConfigurationError("Kafka source contains an invalid retention policy.");
};

const kafkaBrokerDeclarations = (
  viewServer: KafkaNodeViewServer,
): ReadonlyArray<KafkaBrokerContractDeclaration> =>
  kafkaBrokerDeclarationsFrom(captureKafkaSources(viewServer));

const kafkaBrokerDeclarationsFrom = (
  sources: ReadonlyArray<CapturedKafkaSource>,
): ReadonlyArray<KafkaBrokerContractDeclaration> => {
  const declarations: Array<KafkaBrokerContractDeclaration> = [];
  for (const captured of sources) {
    const { source, viewServerTopic: topic } = captured;
    const options = Reflect.get(source, "options");
    if (typeof options !== "object" || options === null) {
      throw new KafkaSourceConfigurationError(
        `Kafka source for Topic ${topic} contains invalid options.`,
      );
    }
    const sourceTopic = Reflect.get(options, "topic");
    const cleanupPolicy = Reflect.get(options, "cleanupPolicy");
    const regions = Reflect.get(options, "regions");
    if (
      typeof sourceTopic !== "string" ||
      sourceTopic.length === 0 ||
      (cleanupPolicy !== "delete" &&
        cleanupPolicy !== "compact" &&
        cleanupPolicy !== "compact-and-delete") ||
      !Array.isArray(regions) ||
      regions.length === 0
    ) {
      throw new KafkaSourceConfigurationError(
        `Kafka source for Topic ${topic} contains invalid broker contract options.`,
      );
    }
    const retentionPolicy = capturedRetentionPolicy(Reflect.get(options, "retentionPolicy"));
    for (const region of regions) {
      if (typeof region !== "string" || region.length === 0) {
        throw new KafkaSourceConfigurationError(
          `Kafka source for Topic ${topic} contains invalid Regions.`,
        );
      }
      declarations.push({
        viewServerTopic: topic,
        sourceTopic,
        region,
        cleanupPolicy,
        retentionPolicy,
      });
    }
  }
  return Object.freeze(declarations);
};

const defaultRetentionSweepIntervalNanos = 15n * 60n * 1_000_000_000n;

const retentionSweepIntervalNanos = (input: unknown): bigint => {
  if (input === undefined) {
    return defaultRetentionSweepIntervalNanos;
  }
  const duration = decodeKafkaDurationInput(input);
  const nanos = Option.flatMap(duration, Duration.toNanos);
  if (Option.isNone(nanos) || nanos.value <= 0n) {
    throw new KafkaSourceConfigurationError(
      "Kafka Node Layer retentionSweepInterval must be a positive finite Effect Duration.",
    );
  }
  return nanos.value;
};

const validateConsumerGroupIdsFrom = (
  sources: ReadonlyArray<CapturedKafkaSource>,
  consumerGroupPrefix: string,
): void => {
  for (const source of sources) {
    kafkaConsumerGroupId(consumerGroupPrefix, source.viewServerTopic);
  }
};

type KafkaLayerSnapshot = {
  readonly consumerGroupPrefix: string;
  readonly regions: ReadonlyMap<string, KafkaNodeRegionSnapshot>;
  readonly retentionSweepIntervalNanos: bigint;
  readonly brokerDeclarations: ReadonlyArray<KafkaBrokerContractDeclaration>;
  readonly schemaRegistryDeclarations: ReadonlyArray<KafkaSchemaRegistryDeclaration>;
};

const snapshotLayerOptions = <ViewServer extends KafkaNodeViewServer>(
  viewServer: ViewServer,
  options: unknown,
): KafkaLayerSnapshot => {
  if (
    typeof viewServer !== "object" ||
    viewServer === null ||
    typeof viewServer.topics !== "object" ||
    viewServer.topics === null
  ) {
    throw new KafkaSourceConfigurationError("Kafka Node Layer requires a View Server Config.");
  }
  if (typeof options !== "object" || options === null || Array.isArray(options)) {
    throw new KafkaSourceConfigurationError(
      "Kafka Node Layer requires exactly consumerGroupPrefix and regions.",
    );
  }
  const fields = captureDataFields(
    options,
    "Kafka Node Layer requires exactly consumerGroupPrefix and regions.",
  );
  const consumerGroupPrefix = fields.get("consumerGroupPrefix");
  const regionOptions = fields.get("regions");
  const sweepInterval = fields.get("retentionSweepInterval");
  if (
    (fields.size !== 2 && fields.size !== 3) ||
    !fields.has("consumerGroupPrefix") ||
    !fields.has("regions") ||
    Array.from(fields.keys()).some(
      (key) =>
        key !== "consumerGroupPrefix" && key !== "regions" && key !== "retentionSweepInterval",
    ) ||
    typeof consumerGroupPrefix !== "string" ||
    consumerGroupPrefix.length === 0 ||
    typeof regionOptions !== "object" ||
    regionOptions === null ||
    Array.isArray(regionOptions)
  ) {
    throw new KafkaSourceConfigurationError(
      "Kafka Node Layer requires exactly consumerGroupPrefix and regions.",
    );
  }
  const sources = captureKafkaSources(viewServer);
  const required = kafkaSourceRegionsFrom(sources);
  if (required.size === 0) {
    throw new KafkaSourceConfigurationError(
      "Kafka Node Layer requires at least one Kafka Source Definition.",
    );
  }
  validateConsumerGroupIdsFrom(sources, consumerGroupPrefix);
  const provided = captureDataFields(
    regionOptions,
    "Kafka Node Layer regions must contain all and only referenced Regions.",
  );
  if (
    provided.size !== required.size ||
    Array.from(provided.keys()).some((region) => !required.has(region))
  ) {
    throw new KafkaSourceConfigurationError(
      "Kafka Node Layer regions must contain all and only referenced Regions.",
    );
  }
  const regions = new Map<string, KafkaNodeRegionSnapshot>();
  for (const [region, value] of provided) {
    if (typeof value !== "object" || value === null) {
      throw new KafkaSourceConfigurationError(`Kafka Node Region ${region} options are invalid.`);
    }
    regions.set(region, snapshotRegionOptions(value));
  }
  const schemaRegistryDeclarations = kafkaSchemaRegistryDeclarationsFrom(sources);
  const registryRegions = new Set(
    schemaRegistryDeclarations.map((declaration) => declaration.region),
  );
  for (const region of registryRegions) {
    if (regions.get(region)?.schemaRegistry === undefined) {
      throw new KafkaSourceConfigurationError(
        `Kafka Region ${region} requires one Schema Registry configuration because a Source uses kafka.schemaRegistry.protobuf(...).`,
      );
    }
  }
  return Object.freeze({
    consumerGroupPrefix,
    regions,
    retentionSweepIntervalNanos: retentionSweepIntervalNanos(sweepInterval),
    brokerDeclarations: kafkaBrokerDeclarationsFrom(sources),
    schemaRegistryDeclarations,
  });
};

const acquisitionFailure = (input: KafkaServerRegionAcquireInput): KafkaAdapterFailure => ({
  _tag: "KafkaAcquisitionFailure",
  region: input.region,
  topic: input.sourceTopic,
  message: "Kafka Region consumer acquisition failed.",
});

const consumeFailure = (input: KafkaServerRegionAcquireInput): KafkaAdapterFailure => ({
  _tag: "KafkaConsumeFailure",
  region: input.region,
  topic: input.sourceTopic,
  message: "Kafka Region consumer stream failed.",
});

const partitionGrowthFailure = (input: KafkaServerRegionAcquireInput): KafkaAdapterFailure => ({
  _tag: "KafkaConsumeFailure",
  region: input.region,
  topic: input.sourceTopic,
  message: "Kafka Region discovered a new partition and is reacquiring configured start offsets.",
});

const commitFailure = (input: KafkaServerRegionAcquireInput): KafkaAdapterFailure => ({
  _tag: "KafkaCommitFailure",
  region: input.region,
  topic: input.sourceTopic,
  message: "Kafka record offset commit failed.",
});

const settleCommittedRecord =
  (
    commit: Effect.Effect<void, KafkaAdapterFailure>,
  ): ((applicationExit: SourceApplicationExit) => Effect.Effect<void, KafkaAdapterFailure>) =>
  (applicationExit) =>
    Exit.isSuccess(applicationExit) ? commit : Effect.void;

const releaseFailure = (input: KafkaServerRegionAcquireInput): KafkaAdapterFailure => ({
  _tag: "KafkaReleaseFailure",
  region: input.region,
  topic: input.sourceTopic,
  message: "Kafka Region consumer release failed.",
});

const configValidationFallbackMessage = "Kafka Node Layer configuration validation failed.";

const configValidationMessage = (cause: unknown): string =>
  Result.try(() => {
    if (!(cause instanceof KafkaSourceConfigurationError)) {
      return configValidationFallbackMessage;
    }
    const message = cause.message;
    return typeof message === "string" ? message : configValidationFallbackMessage;
  }).pipe(
    Result.match({
      onFailure: () => configValidationFallbackMessage,
      onSuccess: (message) => message,
    }),
  );

const configValidationFailure = (cause: unknown): Config.ConfigError =>
  new Config.ConfigError(
    new Schema.SchemaError(
      new SchemaIssue.InvalidValue({
        message: configValidationMessage(cause),
      }),
    ),
  );

const emptyMutableMetrics = (): KafkaMutableRegionMetrics => ({
  assignments: new Map(),
  activePartitions: new Set(),
  commits: 0n,
  commitFailures: 0n,
  decoded: 0n,
  decodeFailures: 0n,
  mapped: 0n,
  mappingFailures: 0n,
  rejections: 0n,
  reconnects: 0n,
  rebalances: 0n,
  closes: 0n,
  closeFailures: 0n,
});

const bindingKey = (input: KafkaServerRegionMetricsInput): string =>
  JSON.stringify([input.activeGroupId, input.region, input.sourceTopic]);

const offsetsForTopic = (offsets: Offsets, topic: string): ReadonlyArray<bigint> =>
  offsets.get(topic) ?? [];

const offsetList = (
  topic: string,
  offsets: ReadonlyArray<bigint>,
): ReadonlyArray<{
  readonly topic: string;
  readonly partition: number;
  readonly offset: bigint;
}> =>
  offsets.map((offset, partition) => ({
    topic,
    partition,
    offset,
  }));

const listOffsets = Effect.fn("KafkaNode.offsets.list")(function* (
  consumer: KafkaConsumer,
  input: KafkaServerRegionAcquireInput,
  timestamp: bigint,
) {
  return yield* Effect.tryPromise({
    try: () =>
      consumer.listOffsets({
        topics: [input.sourceTopic],
        timestamp,
      }),
    catch: () => acquisitionFailure(input),
  });
});

const listCommitted = Effect.fn("KafkaNode.offsets.committed")(function* (
  consumer: KafkaConsumer,
  input: KafkaServerRegionAcquireInput,
  partitions: ReadonlyArray<number>,
) {
  return yield* Effect.tryPromise({
    try: () =>
      consumer.listCommittedOffsets({
        topics: [
          {
            topic: input.sourceTopic,
            partitions: [...partitions],
          },
        ],
      }),
    catch: () => acquisitionFailure(input),
  });
});

const recordReleaseFailure = Effect.fn("KafkaNode.release.failure")(function* (
  failure: KafkaAdapterFailure,
  input: KafkaServerRegionAcquireInput,
  metrics: KafkaMutableRegionMetrics,
  attempt: bigint,
) {
  metrics.closeFailures += 1n;
  yield* Effect.logError(failure).pipe(
    Effect.annotateLogs({
      sourceAdapterName: "kafka",
      sourceAdapterVersion: "1",
      sourceTopic: input.sourceTopic,
      viewServerTopic: input.viewServerTopic,
      sourceRegion: input.region,
      sourceAttempt: attempt.toString(),
    }),
  );
});

const removeConsumerListeners = Effect.fn("KafkaNode.consumer.listeners.remove")(function* (
  consumer: KafkaConsumer,
  groupJoin: (payload: ConsumerGroupJoinPayload) => void,
  rebalance: () => void,
  lag: (current: Offsets) => void,
  input: KafkaServerRegionAcquireInput,
  metrics: KafkaMutableRegionMetrics,
  attempt: bigint,
) {
  const remove = (operation: () => void) =>
    Effect.try({
      try: operation,
      catch: () => releaseFailure(input),
    }).pipe(Effect.catch((failure) => recordReleaseFailure(failure, input, metrics, attempt)));
  yield* remove(() => consumer.off("consumer:group:join", groupJoin));
  yield* remove(() => consumer.off("consumer:group:rebalance", rebalance));
  yield* remove(() => consumer.off("consumer:lag", lag));
});

const installConsumerListeners = Effect.fn("KafkaNode.consumer.listeners.install")(function* (
  consumer: KafkaConsumer,
  groupJoin: (payload: ConsumerGroupJoinPayload) => void,
  rebalance: () => void,
  lag: (current: Offsets) => void,
  input: KafkaServerRegionAcquireInput,
  metrics: KafkaMutableRegionMetrics,
  attempt: bigint,
) {
  return yield* Effect.try({
    try: () => {
      consumer.on("consumer:group:join", groupJoin);
      consumer.on("consumer:group:rebalance", rebalance);
      consumer.on("consumer:lag", lag);
    },
    catch: () => acquisitionFailure(input),
  }).pipe(
    Effect.tapError(() =>
      removeConsumerListeners(consumer, groupJoin, rebalance, lag, input, metrics, attempt),
    ),
  );
});

const closeConsumer = Effect.fn("KafkaNode.consumer.close")(function* (
  consumer: KafkaConsumer,
  input: KafkaServerRegionAcquireInput,
  metrics: KafkaMutableRegionMetrics,
  attempt: bigint,
) {
  return yield* Effect.tryPromise({
    try: () => Promise.resolve(consumer.close(true)),
    catch: () => releaseFailure(input),
  }).pipe(
    Effect.matchEffect({
      onFailure: (failure) => recordReleaseFailure(failure, input, metrics, attempt),
      onSuccess: () =>
        Effect.sync(() => {
          metrics.closes += 1n;
        }),
    }),
  );
});

const closeStream = Effect.fn("KafkaNode.stream.close")(function* (
  stream: KafkaStream,
  input: KafkaServerRegionAcquireInput,
  metrics: KafkaMutableRegionMetrics,
  attempt: bigint,
) {
  return yield* Effect.tryPromise({
    try: () => Promise.resolve(stream.close()),
    catch: () => releaseFailure(input),
  }).pipe(Effect.catch((failure) => recordReleaseFailure(failure, input, metrics, attempt)));
});

const stopLagMonitoring = Effect.fn("KafkaNode.lag.stop")(function* (
  consumer: KafkaConsumer,
  input: KafkaServerRegionAcquireInput,
  metrics: KafkaMutableRegionMetrics,
  attempt: bigint,
) {
  return yield* Effect.try({
    try: () => consumer.stopLagMonitoring(),
    catch: () => releaseFailure(input),
  }).pipe(
    Effect.asVoid,
    Effect.catch((failure) => recordReleaseFailure(failure, input, metrics, attempt)),
  );
});

const resolveFallback = Effect.fn("KafkaNode.offsets.fallback")(function* (
  consumer: KafkaConsumer,
  input: KafkaServerRegionAcquireInput,
  offsets: ReadonlyArray<bigint>,
  fallback: "earliest" | "latest" | "fail",
) {
  const missing = offsets.some((offset) => offset < 0n);
  if (!missing) {
    return offsets;
  }
  if (fallback === "fail") {
    return yield* Effect.fail(acquisitionFailure(input));
  }
  const fallbackOffsets = offsetsForTopic(
    yield* listOffsets(consumer, input, fallback === "earliest" ? -2n : -1n),
    input.sourceTopic,
  );
  return offsets.map((offset, partition) =>
    offset >= 0n ? offset : (fallbackOffsets[partition] ?? -1n),
  );
});

const makeResolverConsumer = Effect.fn("KafkaNode.consumer.make")(function* (
  regionOptions: KafkaNodeRegionSnapshot,
  groupId: string,
  input: KafkaServerRegionAcquireInput,
) {
  return yield* Effect.try({
    try: () =>
      new Consumer<Buffer | null, Buffer | null, Buffer, Buffer>({
        autocreateTopics: false,
        bootstrapBrokers: [...regionOptions.bootstrapServers],
        clientId: regionOptions.clientId ?? `effect-view-server-${input.region}`,
        groupId,
        retries: regionOptions.retries ?? true,
        ...(regionOptions.connectTimeout === undefined
          ? {}
          : { connectTimeout: regionOptions.connectTimeout }),
        ...(regionOptions.requestTimeout === undefined
          ? {}
          : { requestTimeout: regionOptions.requestTimeout }),
        ...(regionOptions.timeout === undefined ? {} : { timeout: regionOptions.timeout }),
        ...(regionOptions.metadataMaxAge === undefined
          ? {}
          : { metadataMaxAge: regionOptions.metadataMaxAge }),
        ...(regionOptions.sasl === undefined ? {} : { sasl: regionOptions.sasl }),
        ...(regionOptions.tls === undefined ? {} : { tls: nodeTlsOptions(regionOptions.tls) }),
      }),
    catch: () => acquisitionFailure(input),
  });
});

const resolveInitial = Effect.fn("KafkaNode.offsets.initial")(function* (
  regionOptions: KafkaNodeRegionSnapshot,
  input: KafkaServerRegionAcquireInput,
  activeConsumer: KafkaConsumer,
  metrics: KafkaMutableRegionMetrics,
  attempt: bigint,
): Effect.fn.Return<KafkaInitialPosition, KafkaAdapterFailure> {
  const start = input.start;
  const latest = yield* listOffsets(activeConsumer, input, -1n);
  const latestOffsets = offsetsForTopic(latest, input.sourceTopic);
  const partitions = latestOffsets.map((_offset, partition) => partition);
  let offsets: ReadonlyArray<bigint>;
  if (start.mode === "earliest") {
    offsets = offsetsForTopic(yield* listOffsets(activeConsumer, input, -2n), input.sourceTopic);
  } else if (start.mode === "latest") {
    offsets = latestOffsets;
  } else if (start.mode === "committed") {
    const fallback = start.fallback;
    offsets = yield* Effect.acquireUseRelease(
      makeResolverConsumer(regionOptions, start.consumerGroupId, input),
      (consumer) =>
        listCommitted(consumer, input, partitions).pipe(
          Effect.map((committed) => offsetsForTopic(committed, input.sourceTopic)),
          Effect.flatMap((committed) =>
            resolveFallback(activeConsumer, input, committed, fallback),
          ),
        ),
      (consumer) => closeConsumer(consumer, input, metrics, attempt),
    );
  } else {
    const boundary = start.atMillis;
    const fallback = start.fallback;
    const atBoundary = offsetsForTopic(
      yield* listOffsets(activeConsumer, input, boundary),
      input.sourceTopic,
    );
    offsets = yield* resolveFallback(activeConsumer, input, atBoundary, fallback);
  }
  if (offsets.length !== latestOffsets.length || offsets.some((offset) => offset < 0n)) {
    return yield* Effect.fail(acquisitionFailure(input));
  }
  return {
    offsets: Object.freeze(
      offsetList(input.sourceTopic, offsets).map((offset) => Object.freeze(offset)),
    ),
    latestOffsets: Object.freeze([...latestOffsets]),
  };
});

const activeOffsets = Effect.fn("KafkaNode.offsets.active")(function* (
  consumer: KafkaConsumer,
  input: KafkaServerRegionAcquireInput,
  initial: KafkaInitialPosition,
  committedPartitions: ReadonlySet<number>,
) {
  const partitions = initial.offsets
    .filter((offset) => committedPartitions.has(offset.partition))
    .map((offset) => offset.partition);
  if (partitions.length === 0) {
    return initial.offsets;
  }
  const committed = offsetsForTopic(
    yield* listCommitted(consumer, input, partitions),
    input.sourceTopic,
  );
  return initial.offsets.map((initialOffset) => {
    if (!committedPartitions.has(initialOffset.partition)) {
      return initialOffset;
    }
    const active = committed[initialOffset.partition];
    return active !== undefined && active >= 0n
      ? {
          topic: initialOffset.topic,
          partition: initialOffset.partition,
          offset: active,
        }
      : initialOffset;
  });
});

const textDecoder = new TextDecoder("utf-8", { fatal: true });

const headersFromMessage = (
  headers: ReadonlyMap<Buffer, Buffer>,
): Readonly<Record<string, Uint8Array | ReadonlyArray<Uint8Array>>> => {
  const decoded: Record<string, Uint8Array | Array<Uint8Array>> = Object.create(null);
  for (const [key, value] of headers) {
    const name = textDecoder.decode(key);
    const existing = decoded[name];
    if (existing === undefined) {
      decoded[name] = value;
    } else if (existing instanceof Uint8Array) {
      decoded[name] = [existing, value];
    } else {
      existing.push(value);
    }
  }
  for (const value of Object.values(decoded)) {
    if (!(value instanceof Uint8Array)) {
      Object.freeze(value);
    }
  }
  return Object.freeze(decoded);
};

const scopedKafkaIterable = (
  iterator: AsyncIterator<KafkaBufferMessage>,
): AsyncIterable<KafkaBufferMessage> => ({
  [Symbol.asyncIterator]: () => ({
    next: () => iterator.next(),
    return: () =>
      Promise.resolve({
        done: true,
        value: undefined,
      }),
  }),
});

const snapshotMetrics = (
  region: string,
  metrics: KafkaMutableRegionMetrics,
): KafkaRegionMetrics => ({
  region,
  assignments: Array.from(metrics.activePartitions)
    .sort((left, right) => left - right)
    .flatMap((partition) => {
      const assignment = metrics.assignments.get(partition);
      return assignment === undefined
        ? []
        : [
            {
              partition: assignment.partition,
              offset: assignment.offset,
              lag: assignment.lag,
            },
          ];
    }),
  commits: metrics.commits,
  commitFailures: metrics.commitFailures,
  decoded: metrics.decoded,
  decodeFailures: metrics.decodeFailures,
  mapped: metrics.mapped,
  mappingFailures: metrics.mappingFailures,
  rejections: metrics.rejections,
  reconnects: metrics.reconnects,
  rebalances: metrics.rebalances,
  closes: metrics.closes,
  closeFailures: metrics.closeFailures,
});

const updateAssignmentOffsets = (
  metrics: KafkaMutableRegionMetrics,
  offsets: KafkaInitialPosition["offsets"],
  latestOffsets: ReadonlyArray<bigint>,
  partitions: ReadonlyArray<number>,
): void => {
  metrics.activePartitions.clear();
  const offsetsByPartition = new Map(
    offsets.map((candidate) => [candidate.partition, candidate.offset]),
  );
  for (const partition of partitions) {
    metrics.activePartitions.add(partition);
    const existing = metrics.assignments.get(partition);
    if (existing !== undefined) {
      continue;
    }
    const offset = offsetsByPartition.get(partition) ?? 0n;
    const latest = latestOffsets[partition] ?? offset;
    metrics.assignments.set(partition, {
      partition,
      offset,
      lag: latest > offset ? latest - offset : 0n,
    });
  }
};

const resetAttemptAssignments = (
  metrics: KafkaMutableRegionMetrics,
  offsets: KafkaInitialPosition["offsets"],
  latestOffsets: ReadonlyArray<bigint>,
): void => {
  metrics.assignments.clear();
  metrics.activePartitions.clear();
  for (const current of offsets) {
    const latest = latestOffsets[current.partition] ?? current.offset;
    metrics.assignments.set(current.partition, {
      partition: current.partition,
      offset: current.offset,
      lag: latest > current.offset ? latest - current.offset : 0n,
    });
    metrics.activePartitions.add(current.partition);
  }
};

const updateLag = (
  metrics: KafkaMutableRegionMetrics,
  sourceTopic: string,
  offsets: Offsets,
): void => {
  const lag = offsetsForTopic(offsets, sourceTopic);
  for (const partition of metrics.activePartitions) {
    const assignment = metrics.assignments.get(partition);
    if (assignment === undefined) {
      continue;
    }
    const partitionLag = lag[partition];
    if (partitionLag !== undefined && partitionLag >= 0n) {
      assignment.lag = partitionLag;
    }
  }
};

const updateCommit = (
  metrics: KafkaMutableRegionMetrics,
  initial: KafkaInitialPosition,
  partition: number,
  offset: bigint,
): void => {
  const assignment = metrics.assignments.get(partition);
  if (assignment === undefined) {
    const latest = initial.latestOffsets[partition] ?? offset;
    metrics.assignments.set(partition, {
      partition,
      offset,
      lag: latest > offset ? latest - offset : 0n,
    });
    metrics.activePartitions.add(partition);
  } else {
    const advancedBy = offset > assignment.offset ? offset - assignment.offset : 0n;
    assignment.offset = offset;
    assignment.lag = assignment.lag > advancedBy ? assignment.lag - advancedBy : 0n;
  }
};

const makeNodeRegion = (regionOptions: KafkaNodeRegionSnapshot): KafkaServerRegion => {
  const lifetimes = new Map<Scope.Scope, Map<string, KafkaBindingState>>();
  const lifetimeStates = Effect.fn("KafkaNode.region.lifetime.state")(function* (
    lifetimeScope: Scope.Scope,
  ) {
    return yield* Effect.uninterruptible(
      Effect.gen(function* () {
        const existing = lifetimes.get(lifetimeScope);
        if (existing !== undefined) {
          return existing;
        }
        const states = new Map<string, KafkaBindingState>();
        lifetimes.set(lifetimeScope, states);
        yield* Scope.addFinalizer(
          lifetimeScope,
          Effect.sync(() => {
            lifetimes.delete(lifetimeScope);
          }),
        );
        return states;
      }),
    );
  });
  const bindingState = Effect.fn("KafkaNode.region.binding.state")(function* (
    input: KafkaServerRegionMetricsInput,
  ) {
    const states = yield* lifetimeStates(input.lifetimeScope);
    const key = bindingKey(input);
    const existing = states.get(key);
    if (existing !== undefined) {
      return existing;
    }
    const state: KafkaBindingState = {
      attempts: 0n,
      committedPartitions: new Set(),
      initial: undefined,
      metrics: emptyMutableMetrics(),
    };
    states.set(key, state);
    return state;
  });
  const acquire = Effect.fn("KafkaNode.region.acquire")(function* (
    input: KafkaServerRegionAcquireInput,
  ) {
    const state = yield* bindingState(input);
    const isRetry = state.attempts > 0n;
    if (isRetry) {
      state.metrics.reconnects += 1n;
    }
    state.attempts += 1n;
    const attempt = state.attempts;
    const metrics = state.metrics;
    const consumer = yield* Effect.acquireRelease(
      makeResolverConsumer(regionOptions, input.activeGroupId, input),
      (current) => closeConsumer(current, input, metrics, attempt),
    );
    const initial =
      state.initial ?? (yield* resolveInitial(regionOptions, input, consumer, metrics, attempt));
    if (state.initial === undefined) {
      state.initial = initial;
    }
    const offsets = isRetry
      ? yield* activeOffsets(consumer, input, initial, state.committedPartitions)
      : initial.offsets;
    resetAttemptAssignments(metrics, offsets, initial.latestOffsets);
    const assignedPartitionOutsideResolvedOffsets = yield* Deferred.make<
      never,
      KafkaAdapterFailure
    >();
    const resolvedPartitions = new Set(offsets.map((offset) => offset.partition));
    const signalPartitionGrowth = (): KafkaAdapterFailure => {
      const failure = partitionGrowthFailure(input);
      state.initial = undefined;
      Deferred.doneUnsafe(assignedPartitionOutsideResolvedOffsets, Effect.fail(failure));
      return failure;
    };
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        metrics.activePartitions.clear();
      }),
    );
    const groupJoin = (payload: ConsumerGroupJoinPayload): void => {
      const assignments = payload.assignments ?? consumer.assignments ?? [];
      const partitions = assignments.flatMap((assignment) => assignment.partitions);
      updateAssignmentOffsets(metrics, offsets, initial.latestOffsets, partitions);
      if (partitions.some((partition) => !resolvedPartitions.has(partition))) {
        // Platformatic listener callbacks sit outside Effect; synchronously bridge partition
        // growth into the owned Source Attempt so supervision can reacquire configured offsets.
        signalPartitionGrowth();
      }
    };
    const rebalance = (): void => {
      metrics.rebalances += 1n;
      metrics.activePartitions.clear();
    };
    const lag = (current: Offsets): void => {
      updateLag(metrics, input.sourceTopic, current);
    };
    yield* Effect.acquireRelease(
      installConsumerListeners(consumer, groupJoin, rebalance, lag, input, metrics, attempt),
      () => removeConsumerListeners(consumer, groupJoin, rebalance, lag, input, metrics, attempt),
    );
    const stream = yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: () =>
          consumer.consume({
            autocommit: false,
            topics: [input.sourceTopic],
            mode: "manual",
            offsets: [...offsets],
          }),
        catch: () => acquisitionFailure(input),
      }),
      (current) => closeStream(current, input, metrics, attempt),
    );
    yield* Effect.acquireRelease(
      Effect.try({
        try: () =>
          consumer.startLagMonitoring(
            {
              topics: [input.sourceTopic],
            },
            1_000,
          ),
        catch: () => acquisitionFailure(input),
      }),
      () => stopLagMonitoring(consumer, input, metrics, attempt),
    );
    const iterator = yield* Effect.try({
      try: () => stream[Symbol.asyncIterator](),
      catch: () => consumeFailure(input),
    });
    const recordsFromConsumer = Stream.fromAsyncIterable(scopedKafkaIterable(iterator), () =>
      consumeFailure(input),
    );
    const records = recordsFromConsumer.pipe(
      Stream.interruptWhen(Deferred.await(assignedPartitionOutsideResolvedOffsets)),
      Stream.mapEffect((message) => {
        if (!resolvedPartitions.has(message.partition)) {
          return Effect.fail(signalPartitionGrowth());
        }
        return Effect.try({
          try: (): KafkaServerRecord => ({
            key: message.key === null || message.key === undefined ? null : message.key,
            value: message.value === null || message.value === undefined ? null : message.value,
            metadata: {
              sourceTopic: input.sourceTopic,
              sourceRegion: input.region,
              partition: message.partition,
              offset: message.offset,
              timestampNanos: message.timestamp * 1_000_000n,
              headers: headersFromMessage(message.headers),
            },
            settlement: settleCommittedRecord(
              Effect.tryPromise({
                try: () => Promise.resolve(message.commit()),
                catch: () => commitFailure(input),
              }).pipe(
                Effect.tap(() =>
                  Effect.sync(() => {
                    metrics.commits += 1n;
                    updateCommit(metrics, initial, message.partition, message.offset + 1n);
                    state.committedPartitions.add(message.partition);
                  }),
                ),
                Effect.tapError(() =>
                  Effect.sync(() => {
                    metrics.commitFailures += 1n;
                  }),
                ),
              ),
            ),
          }),
          catch: () => consumeFailure(input),
        });
      }),
    );
    return {
      records,
      recordDecoded: Effect.sync(() => {
        metrics.decoded += 1n;
      }),
      recordDecodeFailure: Effect.sync(() => {
        metrics.decodeFailures += 1n;
      }),
      recordMapped: Effect.sync(() => {
        metrics.mapped += 1n;
      }),
      recordMappingFailure: Effect.sync(() => {
        metrics.mappingFailures += 1n;
      }),
      recordRejection: Effect.sync(() => {
        metrics.rejections += 1n;
      }),
    };
  });
  return {
    endpoints: regionOptions.bootstrapServers,
    acquire,
    metrics: (input) =>
      bindingState(input).pipe(Effect.map((state) => snapshotMetrics(input.region, state.metrics))),
  };
};

const adminOptions = (
  region: string,
  options: KafkaNodeRegionSnapshot,
): ConstructorParameters<typeof Admin>[0] => ({
  bootstrapBrokers: [...options.bootstrapServers],
  clientId: options.clientId ?? `effect-view-server-${region}-broker-validation`,
  retries: options.retries ?? true,
  ...(options.connectTimeout === undefined ? {} : { connectTimeout: options.connectTimeout }),
  ...(options.requestTimeout === undefined ? {} : { requestTimeout: options.requestTimeout }),
  ...(options.timeout === undefined ? {} : { timeout: options.timeout }),
  ...(options.metadataMaxAge === undefined ? {} : { metadataMaxAge: options.metadataMaxAge }),
  ...(options.sasl === undefined ? {} : { sasl: options.sasl }),
  ...(options.tls === undefined ? {} : { tls: nodeTlsOptions(options.tls) }),
});

const unsafeConfigValue = (
  configs: ReadonlyArray<unknown>,
  name: "cleanup.policy" | "retention.ms",
): string | undefined => {
  let matching: object | undefined;
  for (const config of configs) {
    if (typeof config === "object" && config !== null && Reflect.get(config, "name") === name) {
      if (matching !== undefined) {
        return undefined;
      }
      matching = config;
    }
  }
  if (matching === undefined) {
    return undefined;
  }
  const value = Reflect.get(matching, "value");
  return typeof value === "string" ? value : undefined;
};

const configValue = (
  configs: ReadonlyArray<unknown>,
  name: "cleanup.policy" | "retention.ms",
): string | undefined => {
  const parsed = Result.try(() => unsafeConfigValue(configs, name));
  return Result.isFailure(parsed) ? undefined : parsed.success;
};

const unsafeBrokerConfigResource = (value: unknown): KafkaBrokerConfigResource | undefined => {
  if (
    typeof value !== "object" ||
    value === null ||
    Reflect.get(value, "resourceType") !== ConfigResourceTypes.TOPIC
  ) {
    return undefined;
  }
  const resourceName = Reflect.get(value, "resourceName");
  const configs = Reflect.get(value, "configs");
  if (typeof resourceName !== "string" || resourceName.length === 0 || !Array.isArray(configs)) {
    return undefined;
  }
  const cleanupPolicy = configValue(configs, "cleanup.policy");
  const retentionMs = configValue(configs, "retention.ms");
  if (cleanupPolicy === undefined) {
    return {
      resourceName,
      malformedConfiguration: "cleanup.policy",
    };
  }
  if (retentionMs === undefined) {
    return {
      resourceName,
      malformedConfiguration: "retention.ms",
    };
  }
  return {
    resourceName,
    cleanupPolicy,
    retentionMs,
  };
};

const brokerConfigResource = (value: unknown): KafkaBrokerConfigResource | undefined => {
  const parsed = Result.try(() => unsafeBrokerConfigResource(value));
  return Result.isFailure(parsed) ? undefined : parsed.success;
};

const snapshotAdminResponse = (
  response: unknown,
): ReadonlyArray<KafkaBrokerConfigResource> | undefined => {
  const parsed = Result.try(() => {
    if (!Array.isArray(response)) {
      return undefined;
    }
    const candidates: ReadonlyArray<unknown> = response;
    const resources: Array<KafkaBrokerConfigResource> = [];
    for (const candidate of candidates) {
      const resource = brokerConfigResource(candidate);
      if (resource !== undefined) {
        resources.push(resource);
      }
    }
    return Object.freeze(resources);
  });
  return Result.isFailure(parsed) ? undefined : parsed.success;
};

const discoverKafkaBrokerRegion = Effect.fn("KafkaNode.brokerContract.discoverRegion")(function* (
  region: string,
  options: KafkaNodeRegionSnapshot,
  topics: ReadonlyArray<string>,
): Effect.fn.Return<KafkaBrokerRegionDiscovery> {
  const available = yield* Effect.acquireUseRelease(
    Effect.try({
      try: () => new Admin(adminOptions(region, options)),
      catch: () => undefined,
    }),
    (admin) =>
      Effect.tryPromise({
        try: () =>
          admin.describeConfigs({
            resources: topics.map((topic) => ({
              resourceType: ConfigResourceTypes.TOPIC,
              resourceName: topic,
              configurationKeys: ["cleanup.policy", "retention.ms"],
            })),
          }),
        catch: () => undefined,
      }),
    (admin) =>
      Effect.tryPromise({
        try: () => admin.close(),
        catch: () => undefined,
      }),
  ).pipe(Effect.option);
  if (Option.isNone(available) || available.value === undefined) {
    return {
      _tag: "Unavailable",
      region,
    };
  }
  const resources = snapshotAdminResponse(available.value);
  return resources === undefined
    ? {
        _tag: "Malformed",
        region,
      }
    : {
        _tag: "Available",
        region,
        resources,
      };
});

const validateKafkaBrokerContracts = Effect.fn("KafkaNode.brokerContract.validate")(function* (
  snapshot: KafkaLayerSnapshot,
): Effect.fn.Return<
  ReadonlyArray<KafkaResolvedBrokerContract>,
  KafkaBrokerContractValidationFailureType
> {
  const declarations = snapshot.brokerDeclarations;
  const topicsByRegion = new Map<string, Set<string>>();
  for (const declaration of declarations) {
    const topics = topicsByRegion.get(declaration.region) ?? new Set<string>();
    topics.add(declaration.sourceTopic);
    topicsByRegion.set(declaration.region, topics);
  }
  const discoveries = yield* Effect.forEach(
    topicsByRegion,
    ([region, topics]) =>
      discoverKafkaBrokerRegion(
        region,
        Option.getOrThrow(Option.fromUndefinedOr(snapshot.regions.get(region))),
        [...topics].sort(),
      ),
    { concurrency: "unbounded" },
  );
  const resolution = resolveKafkaBrokerContracts(declarations, discoveries);
  return resolution._tag === "Resolved" ? resolution.contracts : yield* Effect.fail(resolution);
});

const schemaRegistryStartupFailure = (
  declarations: readonly [
    KafkaSchemaRegistryDeclaration,
    ...ReadonlyArray<KafkaSchemaRegistryDeclaration>,
  ],
  message: string,
): KafkaSchemaRegistryContractValidationFailureType => {
  const startupIssue = (declaration: KafkaSchemaRegistryDeclaration) =>
    ({
      _tag: "KafkaSchemaRegistryContractIssue",
      region: declaration.region,
      viewServerTopic: declaration.viewServerTopic,
      sourceTopic: declaration.sourceTopic,
      side: declaration.side,
      subject: declaration.subject,
      code: "RegistryUnavailable",
      version: null,
      schemaId: null,
      message,
    }) satisfies import("./schema-registry-contract").KafkaSchemaRegistryContractIssue;
  const [first, ...remaining] = declarations;
  return {
    _tag: "KafkaSchemaRegistryContractValidationFailure",
    message: "Kafka Schema Registry Protobuf validation failed before runtime startup.",
    issues: [startupIssue(first), ...remaining.map(startupIssue)],
  };
};

const makeManagedKafkaSchemaRegistry = Effect.fn("KafkaNode.schemaRegistry.managed")(
  function* (input: {
    readonly region: string;
    readonly layerScope: Scope.Scope;
    readonly options: KafkaNodeSchemaRegistrySnapshot;
    readonly declarations: readonly [
      KafkaSchemaRegistryDeclaration,
      ...ReadonlyArray<KafkaSchemaRegistryDeclaration>,
    ];
  }): Effect.fn.Return<
    KafkaServerSchemaRegistry,
    KafkaSchemaRegistryContractValidationFailureType
  > {
    return yield* makeKafkaServerSchemaRegistry({
      layerScope: input.layerScope,
      acquire: Effect.gen(function* () {
        const reader = yield* makeKafkaSchemaRegistryReader(
          schemaRegistryHttpOptions(input.options),
        ).pipe(
          Effect.mapError((failure) =>
            schemaRegistryStartupFailure(input.declarations, failure.message),
          ),
        );
        return yield* makeKafkaSchemaRegistryRuntime({
          region: input.region,
          endpoint: input.options.url,
          declarations: input.declarations,
          reader,
          monitorInterval: input.options.monitorInterval,
        });
      }),
    });
  },
);

const makeLayerFromSnapshot = (snapshot: KafkaLayerSnapshot) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const layerScope = yield* Effect.scope;
      const contracts = yield* validateKafkaBrokerContracts(snapshot);
      const declarations = snapshot.schemaRegistryDeclarations;
      const schemaRegistries = new Map<string, KafkaServerSchemaRegistry>();
      for (const [region, options] of snapshot.regions) {
        const regionDeclarations = declarations.filter(
          (declaration) => declaration.region === region,
        );
        const firstDeclaration = regionDeclarations[0];
        if (firstDeclaration === undefined) {
          continue;
        }
        const registryOptions = Option.getOrThrow(Option.fromUndefinedOr(options.schemaRegistry));
        const registry = yield* makeManagedKafkaSchemaRegistry({
          region,
          layerScope,
          options: registryOptions,
          declarations: [firstDeclaration, ...regionDeclarations.slice(1)],
        });
        schemaRegistries.set(region, registry);
      }
      const regions = new Map<string, KafkaServerRegion>();
      for (const [region, options] of snapshot.regions) {
        regions.set(region, makeNodeRegion(options));
      }
      return makeKafkaServerLayer({
        consumerGroupPrefix: snapshot.consumerGroupPrefix,
        regions,
        schemaRegistries,
        brokerContracts: contracts,
        retentionSweepIntervalNanos: snapshot.retentionSweepIntervalNanos,
      });
    }),
  );

export const layer = <
  const ViewServer extends KafkaNodeViewServer,
  const Options extends KafkaNodeLayerOptions<NoInfer<ViewServer>>,
>(
  viewServer: ViewServer,
  options: Options &
    ([KafkaRequiredRegion<ViewServer>] extends [never] ? never : unknown) &
    KafkaNodeOptionsGuard<NoInfer<ViewServer>, Options> &
    (true extends ContainsAny<NoInfer<Options>> ? never : unknown),
): Layer.Layer<
  import("effect").Context.Service.Identifier<typeof KafkaSourceAdapter.runtimeService>,
  KafkaBrokerContractValidationFailureType | KafkaSchemaRegistryContractValidationFailureType
> => makeLayerFromSnapshot(snapshotLayerOptions(viewServer, options));

export const layerConfig = <
  const ViewServer extends KafkaNodeViewServer,
  const Options extends KafkaNodeConfigWrap<KafkaNodeLayerOptions<NoInfer<ViewServer>>>,
>(
  viewServer: ViewServer,
  options: Options &
    ([KafkaRequiredRegion<ViewServer>] extends [never] ? never : unknown) &
    KafkaNodeOptionsGuard<NoInfer<ViewServer>, UnwrapConfigCandidate<Options>> &
    (true extends ContainsAny<NoInfer<Options>> ? never : unknown),
): Layer.Layer<
  import("effect").Context.Service.Identifier<typeof KafkaSourceAdapter.runtimeService>,
  | Config.ConfigError
  | KafkaBrokerContractValidationFailureType
  | KafkaSchemaRegistryContractValidationFailureType
> =>
  Layer.unwrap(
    Config.unwrap(options).pipe(
      Effect.flatMap((resolved) =>
        Effect.try({
          try: () => snapshotLayerOptions(viewServer, resolved),
          catch: configValidationFailure,
        }),
      ),
      Effect.map(makeLayerFromSnapshot),
    ),
  );

export const kafkaNode: {
  readonly layer: typeof layer;
  readonly layerConfig: typeof layerConfig;
} = Object.freeze({
  layer,
  layerConfig,
});

export const kafkaNodeInternals = Object.freeze({
  acquisitionFailure,
  bindingKey,
  brokerConfigResource,
  bootstrapServers,
  capturedRetentionPolicy,
  commitFailure,
  configValue,
  consumeFailure,
  copyTlsValue,
  emptyMutableMetrics,
  finiteNonNegative,
  headersFromMessage,
  kafkaBrokerDeclarations,
  kafkaSchemaRegistryDeclarations,
  kafkaSourceRegions,
  nodeTlsOptions,
  nodeTlsValue,
  offsetList,
  offsetsForTopic,
  ownDataKeys,
  releaseFailure,
  retentionSweepIntervalNanos,
  snapshotAdminResponse,
  schemaRegistryHttpAuth,
  schemaRegistryHttpOptions,
  snapshotSchemaRegistry,
  snapshotLayerOptions,
  snapshotMetrics,
  settleCommittedRecord,
  snapshotRegionOptions,
  snapshotSasl,
  snapshotTls,
  updateAssignmentOffsets,
  updateCommit,
  updateLag,
  resetAttemptAssignments,
  makeNodeRegion,
});
