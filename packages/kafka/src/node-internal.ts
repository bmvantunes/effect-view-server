import { Consumer } from "@platformatic/kafka";
import type { ConsumerGroupJoinPayload, Message, Offsets } from "@platformatic/kafka";
import { Buffer } from "node:buffer";
import type { ConnectionOptions as NodeTlsConnectionOptions } from "node:tls";
import { Config, Effect, Exit, Layer, Option, Schema, SchemaIssue, Stream } from "effect";
import type {
  SourceApplicationExit,
  SourceDefinitionAdapter,
  SourceDefinitionOptions,
} from "effect-view-server/source-adapter";
import {
  KafkaSourceAdapter,
  KafkaSourceConfigurationError,
  kafkaConsumerGroupId,
  type KafkaAdapterFailure,
  type KafkaRegionMetrics,
} from "./contract";
import {
  makeKafkaServerLayer,
  type KafkaServerRecord,
  type KafkaServerRegion,
  type KafkaServerRegionAcquireInput,
  type KafkaServerRegionMetricsInput,
} from "./server";

type KafkaNodeViewServer = {
  readonly topics: Readonly<Record<string, object>>;
};

type KafkaDefinitionForTopic<Topic> = Topic extends { readonly source: infer Source }
  ? SourceDefinitionAdapter<Source> extends typeof KafkaSourceAdapter
    ? Source
    : never
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

export type KafkaRequiredRegion<ViewServer extends KafkaNodeViewServer> = Extract<
  {
    readonly [Topic in keyof ViewServer["topics"]]: KafkaRegionsForTopic<
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
};

export type KafkaNodeLayerOptions<ViewServer extends KafkaNodeViewServer> = [
  KafkaRequiredRegion<ViewServer>,
] extends [never]
  ? never
  : {
      readonly consumerGroupPrefix: string;
      readonly regions: {
        readonly [Region in KafkaRequiredRegion<ViewServer>]: KafkaNodeRegionOptions;
      };
    };

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
        }
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

type UnwrapConfigCandidate<Candidate> =
  IsAny<Candidate> extends true
    ? Candidate
    : Candidate extends Config.Config<infer Value>
      ? Value
      : Candidate extends object
        ? { readonly [Key in keyof Candidate]: UnwrapConfigCandidate<Candidate[Key]> }
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
] as const;

const allowedRegionOptions = new Set<string>(allowedRegionOptionKeys);

const ownDataKeys = (value: object): ReadonlyArray<string> => {
  const keys = Reflect.ownKeys(value);
  if (
    keys.some((key) => typeof key !== "string") ||
    keys.some((key) => {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      return descriptor === undefined || descriptor.enumerable !== true || !("value" in descriptor);
    })
  ) {
    throw new KafkaSourceConfigurationError(
      "Kafka Node options must contain enumerable string data fields.",
    );
  }
  return Object.keys(value);
};

const exactKeys = (value: object, expected: ReadonlyArray<string>): boolean => {
  const keys = ownDataKeys(value);
  return keys.length === expected.length && keys.every((key) => expected.includes(key));
};

const bootstrapServers = (
  value: KafkaNodeRegionOptions["bootstrapServers"],
): readonly [string, ...ReadonlyArray<string>] => {
  const entries = typeof value === "string" ? value.split(",") : value;
  if (!Array.isArray(entries)) {
    throw new KafkaSourceConfigurationError(
      "Kafka Region bootstrapServers must contain only non-empty brokers.",
    );
  }
  const [first, ...rest] = entries;
  if (
    typeof first !== "string" ||
    first.trim().length === 0 ||
    !rest.every((entry) => typeof entry === "string" && entry.trim().length > 0)
  ) {
    throw new KafkaSourceConfigurationError(
      "Kafka Region bootstrapServers must contain only non-empty brokers.",
    );
  }
  return Object.freeze([first.trim(), ...rest.map((entry) => entry.trim())]);
};

const copyBytes = (value: Uint8Array): Uint8Array => Uint8Array.from(value);

const copyTlsValue = (
  value: string | Uint8Array | ReadonlyArray<string | Uint8Array>,
): string | Uint8Array | ReadonlyArray<string | Uint8Array> => {
  if (typeof value === "string") {
    return value;
  }
  if (value instanceof Uint8Array) {
    return copyBytes(value);
  }
  return Object.freeze(
    value.map((entry) => (typeof entry === "string" ? entry : copyBytes(entry))),
  );
};

const snapshotSasl = (sasl: KafkaNodeSaslOptions): KafkaNodeSaslOptions => {
  if (typeof sasl !== "object" || sasl === null || Array.isArray(sasl)) {
    throw new KafkaSourceConfigurationError("Kafka Region sasl options are invalid.");
  }
  const keys = ownDataKeys(sasl);
  if (sasl.mechanism === "OAUTHBEARER") {
    if (keys.length !== 2 || !keys.includes("token") || typeof sasl.token !== "string") {
      throw new KafkaSourceConfigurationError("Kafka Region sasl options are invalid.");
    }
    return Object.freeze({
      mechanism: sasl.mechanism,
      token: sasl.token,
    });
  }
  if (
    !["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"].includes(sasl.mechanism) ||
    keys.length !== 3 ||
    !keys.includes("username") ||
    !keys.includes("password") ||
    typeof sasl.username !== "string" ||
    typeof sasl.password !== "string"
  ) {
    throw new KafkaSourceConfigurationError("Kafka Region sasl options are invalid.");
  }
  return Object.freeze({
    mechanism: sasl.mechanism,
    username: sasl.username,
    password: sasl.password,
  });
};

const snapshotTls = (tls: KafkaNodeTlsOptions): KafkaNodeTlsOptions => {
  const allowed = ["ca", "cert", "key", "rejectUnauthorized", "serverName"];
  if (
    typeof tls !== "object" ||
    tls === null ||
    Array.isArray(tls) ||
    ownDataKeys(tls).some((key) => !allowed.includes(key)) ||
    (tls.ca !== undefined && !isTlsValue(tls.ca)) ||
    (tls.cert !== undefined && !isTlsValue(tls.cert)) ||
    (tls.key !== undefined && !isTlsValue(tls.key)) ||
    (tls.rejectUnauthorized !== undefined && typeof tls.rejectUnauthorized !== "boolean") ||
    (tls.serverName !== undefined && typeof tls.serverName !== "string")
  ) {
    throw new KafkaSourceConfigurationError("Kafka Region tls options are invalid.");
  }
  return Object.freeze({
    ...(tls.ca === undefined ? {} : { ca: copyTlsValue(tls.ca) }),
    ...(tls.cert === undefined ? {} : { cert: copyTlsValue(tls.cert) }),
    ...(tls.key === undefined ? {} : { key: copyTlsValue(tls.key) }),
    ...(tls.rejectUnauthorized === undefined ? {} : { rejectUnauthorized: tls.rejectUnauthorized }),
    ...(tls.serverName === undefined ? {} : { serverName: tls.serverName }),
  });
};

const isTlsValue = (
  value: unknown,
): value is string | Uint8Array | ReadonlyArray<string | Uint8Array> =>
  typeof value === "string" ||
  value instanceof Uint8Array ||
  (Array.isArray(value) &&
    value.every((entry) => typeof entry === "string" || entry instanceof Uint8Array));

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

const finiteNonNegative = (value: number | undefined): boolean =>
  value === undefined || (Number.isFinite(value) && value >= 0);

const snapshotRegionOptions = (options: KafkaNodeRegionOptions): KafkaNodeRegionOptions => {
  if (
    typeof options !== "object" ||
    options === null ||
    !ownDataKeys(options).includes("bootstrapServers") ||
    ownDataKeys(options).some((key) => !allowedRegionOptions.has(key)) ||
    !finiteNonNegative(options.connectTimeout) ||
    !finiteNonNegative(options.requestTimeout) ||
    !finiteNonNegative(options.timeout) ||
    !finiteNonNegative(options.metadataMaxAge) ||
    (options.retries !== undefined &&
      typeof options.retries !== "boolean" &&
      (!Number.isSafeInteger(options.retries) || options.retries < 0)) ||
    (options.clientId !== undefined &&
      (typeof options.clientId !== "string" || options.clientId.length === 0))
  ) {
    throw new KafkaSourceConfigurationError("Kafka Region options are invalid.");
  }
  return Object.freeze({
    bootstrapServers: bootstrapServers(options.bootstrapServers),
    ...(options.clientId === undefined ? {} : { clientId: options.clientId }),
    ...(options.connectTimeout === undefined ? {} : { connectTimeout: options.connectTimeout }),
    ...(options.requestTimeout === undefined ? {} : { requestTimeout: options.requestTimeout }),
    ...(options.timeout === undefined ? {} : { timeout: options.timeout }),
    ...(options.retries === undefined ? {} : { retries: options.retries }),
    ...(options.metadataMaxAge === undefined ? {} : { metadataMaxAge: options.metadataMaxAge }),
    ...(options.sasl === undefined ? {} : { sasl: snapshotSasl(options.sasl) }),
    ...(options.tls === undefined ? {} : { tls: snapshotTls(options.tls) }),
  });
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
  const regions = new Set<string>();
  for (const topic of Object.keys(viewServer.topics)) {
    const definition = viewServer.topics[topic];
    const source =
      typeof definition === "object" && definition !== null && Object.hasOwn(definition, "source")
        ? Reflect.get(definition, "source")
        : undefined;
    if (
      typeof source !== "object" ||
      source === null ||
      Reflect.get(source, "adapter") !== KafkaSourceAdapter
    ) {
      continue;
    }
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
  if (regions.size === 0) {
    throw new KafkaSourceConfigurationError(
      "Kafka Node Layer requires at least one Kafka Source Definition.",
    );
  }
  return regions;
};

const validateConsumerGroupIds = (
  viewServer: KafkaNodeViewServer,
  consumerGroupPrefix: string,
): void => {
  for (const topic of Object.keys(viewServer.topics)) {
    const definition = viewServer.topics[topic];
    const source =
      typeof definition === "object" && definition !== null && Object.hasOwn(definition, "source")
        ? Reflect.get(definition, "source")
        : undefined;
    if (
      typeof source === "object" &&
      source !== null &&
      Reflect.get(source, "adapter") === KafkaSourceAdapter
    ) {
      kafkaConsumerGroupId(consumerGroupPrefix, topic);
    }
  }
};

const snapshotLayerOptions = <ViewServer extends KafkaNodeViewServer>(
  viewServer: ViewServer,
  options: KafkaNodeLayerOptions<ViewServer>,
): {
  readonly consumerGroupPrefix: string;
  readonly regions: ReadonlyMap<string, KafkaNodeRegionOptions>;
} => {
  if (
    typeof options !== "object" ||
    options === null ||
    !exactKeys(options, ["consumerGroupPrefix", "regions"]) ||
    typeof options.consumerGroupPrefix !== "string" ||
    options.consumerGroupPrefix.length === 0 ||
    typeof options.regions !== "object" ||
    options.regions === null ||
    Array.isArray(options.regions)
  ) {
    throw new KafkaSourceConfigurationError(
      "Kafka Node Layer requires exactly consumerGroupPrefix and regions.",
    );
  }
  const required = kafkaSourceRegions(viewServer);
  validateConsumerGroupIds(viewServer, options.consumerGroupPrefix);
  const provided = ownDataKeys(options.regions);
  if (provided.length !== required.size || provided.some((region) => !required.has(region))) {
    throw new KafkaSourceConfigurationError(
      "Kafka Node Layer regions must contain all and only referenced Regions.",
    );
  }
  const regions = new Map<string, KafkaNodeRegionOptions>();
  for (const region of provided) {
    const value = Reflect.get(options.regions, region);
    if (typeof value !== "object" || value === null) {
      throw new KafkaSourceConfigurationError(`Kafka Node Region ${region} options are invalid.`);
    }
    regions.set(region, snapshotRegionOptions(value));
  }
  return Object.freeze({
    consumerGroupPrefix: options.consumerGroupPrefix,
    regions,
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

const configValidationFailure = (): Config.ConfigError =>
  new Config.ConfigError(
    new Schema.SchemaError(
      new SchemaIssue.InvalidValue(Option.none(), {
        message: "Kafka Node Layer resolved options are invalid.",
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
  regionOptions: KafkaNodeRegionOptions,
  groupId: string,
  input: KafkaServerRegionAcquireInput,
) {
  return yield* Effect.try({
    try: () =>
      new Consumer<Buffer | null, Buffer | null, Buffer, Buffer>({
        autocreateTopics: false,
        bootstrapBrokers: [...bootstrapServers(regionOptions.bootstrapServers)],
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
  regionOptions: KafkaNodeRegionOptions,
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
) {
  const partitions = initial.offsets.map((offset) => offset.partition);
  const committed = offsetsForTopic(
    yield* listCommitted(consumer, input, partitions),
    input.sourceTopic,
  );
  return initial.offsets.map((initialOffset) => {
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

const textDecoder = new TextDecoder();

const headersFromMessage = (
  headers: ReadonlyMap<Buffer, Buffer>,
): Readonly<Record<string, Uint8Array | ReadonlyArray<Uint8Array>>> => {
  const decoded: Record<string, Uint8Array | Array<Uint8Array>> = Object.create(null);
  for (const [key, value] of headers) {
    const name = textDecoder.decode(key);
    const existing = decoded[name];
    if (existing === undefined) {
      decoded[name] = Uint8Array.from(value);
    } else if (existing instanceof Uint8Array) {
      decoded[name] = [existing, Uint8Array.from(value)];
    } else {
      existing.push(Uint8Array.from(value));
    }
  }
  return decoded;
};

const scopedKafkaIterable = (stream: KafkaStream): AsyncIterable<KafkaBufferMessage> => ({
  [Symbol.asyncIterator]: () => {
    const iterator = stream[Symbol.asyncIterator]();
    return {
      next: () => iterator.next(),
      return: () =>
        Promise.resolve({
          done: true,
          value: undefined,
        }),
    };
  },
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

const makeNodeRegion = (regionOptions: KafkaNodeRegionOptions): KafkaServerRegion => {
  const states = new Map<string, KafkaBindingState>();
  const metricsFor = (input: KafkaServerRegionMetricsInput): KafkaMutableRegionMetrics => {
    const existing = states.get(bindingKey(input));
    if (existing !== undefined) {
      return existing.metrics;
    }
    return emptyMutableMetrics();
  };
  const acquire = Effect.fn("KafkaNode.region.acquire")(function* (
    input: KafkaServerRegionAcquireInput,
  ) {
    const key = bindingKey(input);
    const existing = states.get(key);
    const state: KafkaBindingState = existing ?? {
      attempts: 0n,
      initial: undefined,
      metrics: emptyMutableMetrics(),
    };
    if (existing === undefined) {
      states.set(key, state);
    } else {
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
    const offsets = yield* activeOffsets(consumer, input, initial);
    resetAttemptAssignments(metrics, offsets, initial.latestOffsets);
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        metrics.activePartitions.clear();
      }),
    );
    const groupJoin = (payload: ConsumerGroupJoinPayload): void => {
      const assignments = payload.assignments ?? consumer.assignments ?? [];
      const partitions = assignments.flatMap((assignment) => assignment.partitions);
      updateAssignmentOffsets(metrics, offsets, initial.latestOffsets, partitions);
    };
    const rebalance = (): void => {
      metrics.rebalances += 1n;
    };
    const lag = (current: Offsets): void => {
      updateLag(metrics, input.sourceTopic, current);
    };
    yield* Effect.acquireRelease(
      Effect.sync(() => {
        consumer.on("consumer:group:join", groupJoin);
        consumer.on("consumer:group:rebalance", rebalance);
        consumer.on("consumer:lag", lag);
      }),
      () =>
        Effect.sync(() => {
          consumer.off("consumer:group:join", groupJoin);
          consumer.off("consumer:group:rebalance", rebalance);
          consumer.off("consumer:lag", lag);
        }),
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
    const records = Stream.fromAsyncIterable(scopedKafkaIterable(stream), () =>
      consumeFailure(input),
    ).pipe(
      Stream.map(
        (message): KafkaServerRecord => ({
          key:
            message.key === null || message.key === undefined ? null : Uint8Array.from(message.key),
          value:
            message.value === null || message.value === undefined
              ? null
              : Uint8Array.from(message.value),
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
      ),
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
    acquire,
    metrics: (input) => Effect.sync(() => snapshotMetrics(input.region, metricsFor(input))),
  };
};

const makeLayerFromSnapshot = (snapshot: {
  readonly consumerGroupPrefix: string;
  readonly regions: ReadonlyMap<string, KafkaNodeRegionOptions>;
}) =>
  Layer.unwrap(
    Effect.sync(() => {
      const regions = new Map<string, KafkaServerRegion>();
      for (const [region, options] of snapshot.regions) {
        regions.set(region, makeNodeRegion(options));
      }
      return makeKafkaServerLayer({
        consumerGroupPrefix: snapshot.consumerGroupPrefix,
        regions,
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
    KafkaNodeOptionsGuard<NoInfer<ViewServer>, Options>,
): Layer.Layer<
  import("effect").Context.Service.Identifier<typeof KafkaSourceAdapter.runtimeService>
> => makeLayerFromSnapshot(snapshotLayerOptions(viewServer, options));

export const layerConfig = <
  const ViewServer extends KafkaNodeViewServer,
  const Options extends Config.Wrap<KafkaNodeLayerOptions<NoInfer<ViewServer>>>,
>(
  viewServer: ViewServer,
  options: Options &
    ([KafkaRequiredRegion<ViewServer>] extends [never] ? never : unknown) &
    KafkaNodeOptionsGuard<NoInfer<ViewServer>, UnwrapConfigCandidate<Options>>,
): Layer.Layer<
  import("effect").Context.Service.Identifier<typeof KafkaSourceAdapter.runtimeService>,
  Config.ConfigError
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
  bootstrapServers,
  commitFailure,
  consumeFailure,
  copyTlsValue,
  emptyMutableMetrics,
  finiteNonNegative,
  headersFromMessage,
  kafkaSourceRegions,
  nodeTlsOptions,
  nodeTlsValue,
  offsetList,
  offsetsForTopic,
  ownDataKeys,
  releaseFailure,
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
