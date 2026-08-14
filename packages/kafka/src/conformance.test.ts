import { definedFields } from "./optional-fields";
import { fileURLToPath } from "node:url";
import {
  SourceAdapterConformanceDriver,
  makeSourceAdapterConformanceDriver,
  registerSourceAdapterPackageConformance,
  type SourceAdapterConformanceAttemptFault,
  type SourceAdapterConformanceCommand,
  type SourceAdapterConformanceTarget,
  type SourceAdapterConformanceTransportObservation,
  type SourceAdapterPackageInspectionOptions,
  type SourceAdapterPackageInspectionError,
} from "@effect-view-server/source-adapter-conformance-host";
import {
  makeSourceApplicationTransition,
  makeSourceTransitionDelivery,
  resolveSourceApplicationTransition,
} from "@effect-view-server/source-adapter/internal";
import type { SourceApplicationExit } from "effect-view-server/source-adapter";
import {
  Chunk,
  Cause,
  Config,
  Context,
  Deferred,
  Effect,
  Layer,
  Option,
  Queue,
  Schedule,
  Schema,
  Stream,
  SubscriptionRef,
} from "effect";

const isString = (value: unknown): value is string => typeof value === "string";
import {
  KafkaSourceAdapter,
  kafka,
  kafkaRowId,
  type KafkaAdapterFailure,
  type KafkaMaterializedMetrics,
  type KafkaRegionMetrics,
  type KafkaSourceRejectionLocation,
  type KafkaSourceRetryPolicy,
} from "@effect-view-server/kafka/contract";
import {
  makeKafkaServerLayer,
  type KafkaServerRecord,
  type KafkaServerRegion,
} from "@effect-view-server/kafka/server";

const kafkaResourceValidationFailure = (failure: SourceAdapterPackageInspectionError): boolean =>
  failure.cause instanceof Error && failure.cause.name === "KafkaSourceConfigurationError";

const kafkaExternalValidationFailure = (failure: SourceAdapterPackageInspectionError): boolean => {
  if (!Cause.isCause(failure.cause)) {
    return false;
  }
  const brokerFailure = Cause.findErrorOption(failure.cause);
  return (
    Option.isSome(brokerFailure) &&
    typeof brokerFailure.value === "object" &&
    brokerFailure.value !== null &&
    Reflect.get(brokerFailure.value, "_tag") === "KafkaBrokerContractValidationFailure"
  );
};

type ActiveAttempt = {
  readonly queues: ReadonlyMap<
    string,
    Queue.Queue<KafkaServerRecord, KafkaAdapterFailure | import("effect").Cause.Done>
  >;
  acquisitions: bigint;
  finalizations: bigint;
};

type Corruption = {
  readonly field: string;
  readonly value: unknown;
};

class KafkaConformanceProduction extends Context.Service<
  KafkaConformanceProduction,
  Context.Service.Shape<typeof KafkaSourceAdapter.runtimeService>
>()("@effect-view-server/kafka/ConformanceProduction") {}

const acquisitionFailure: KafkaAdapterFailure = {
  _tag: "KafkaConfigurationFailure",
  message: "conformance acquisition failure",
};

const streamFailure: KafkaAdapterFailure = {
  _tag: "KafkaConsumeFailure",
  region: "primary",
  topic: "conformance",
  message: "conformance lane failure",
};

const settlementFailure: KafkaAdapterFailure = {
  _tag: "KafkaCommitFailure",
  region: "primary",
  topic: "conformance",
  message: "conformance settlement failure",
};

const rejectionFailure = (_phase: "acquire" | "stream" | "settlement"): KafkaAdapterFailure => ({
  _tag: "KafkaDecodeFailure",
  region: "primary",
  topic: "conformance",
  message: "Kafka value codec rejected the record.",
});

const rejectionLocation = (
  target: SourceAdapterConformanceTarget,
  offset: bigint,
): KafkaSourceRejectionLocation => ({
  region: target.lane,
  topic: "conformance",
  partition: target.lane === "primary" ? 0 : 1,
  offset,
  phase: "valueDecode",
  message: "Kafka value codec rejected the record.",
});

const settlement = (
  settle: ((exit: SourceApplicationExit) => Effect.Effect<void, unknown>) | undefined,
): ((exit: SourceApplicationExit) => Effect.Effect<void, KafkaAdapterFailure>) =>
  settle === undefined
    ? () => Effect.void
    : (exit) => settle(exit).pipe(Effect.mapError(() => settlementFailure));

const textEncoder = new TextEncoder();

const emptyRegionMetrics = (region: string): KafkaRegionMetrics => ({
  region,
  assignments: [],
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

const foreverRetentionMetrics = (cleanupPolicy: "delete" | "compact") => ({
  declaredCleanupPolicy: cleanupPolicy,
  observedCleanupPolicy: cleanupPolicy,
  configuredRetention: { _tag: "Forever" as const },
  resolvedRetention: { _tag: "Forever" as const },
  trackedRows: 0,
  lastSweepRetryableFailures: 0,
  expiredRows: 0n,
  authoritativeExpiredDeletes: 0n,
  failedWorkBacklog: 0,
  expirationRetryFailures: 0n,
  latestExpirationFailure: null,
  lastSweepAtNanos: null,
  lastSweepDurationNanos: null,
  sweepIntervalNanos: 900_000_000_000n,
});

const emptyMaterializedRegionMetrics = (region: string) => ({
  ...emptyRegionMetrics(region),
  retention: foreverRetentionMetrics("compact"),
});

const recordMetadata = (
  target: SourceAdapterConformanceTarget,
  offset: bigint,
): KafkaServerRecord["metadata"] => ({
  sourceTopic: "conformance",
  sourceRegion: target.lane,
  partition: target.lane === "primary" ? 0 : 1,
  offset,
  timestampNanos: offset * 1_000_000n,
  headers: {},
});

const nominalOverride = <Value extends object, Override>(
  source: Value,
  property: string,
  value: Override,
): Value => {
  const clone: Value = Object.create(Object.getPrototypeOf(source));
  Object.assign(clone, source);
  Object.defineProperty(clone, property, {
    enumerable: true,
    value,
  });
  for (const brand of Object.getOwnPropertySymbols(source)) {
    Object.defineProperty(clone, brand, {
      value: () => clone,
    });
  }
  return Object.freeze(clone);
};

const makeKafkaConformanceDriver = Effect.fn("KafkaSourceAdapter.conformance.driver")(function* () {
  const active = new Map<string, ActiveAttempt>();
  const pendingQueues = new Map<
    string,
    Queue.Queue<KafkaServerRecord, KafkaAdapterFailure | import("effect").Cause.Done>
  >();
  const corruptions = new Map<string, Corruption>();
  const counts = {
    acquisitions: 0n,
    finalizations: 0n,
  };
  let partialAcquisitionFinalizations = 0n;
  let partialAcquisitionPending = false;
  let failedAcquisition:
    | {
        readonly partial: boolean;
        readonly failure: KafkaAdapterFailure;
      }
    | undefined;
  const attemptFaults: Array<SourceAdapterConformanceAttemptFault> = [];
  let finalizerBlock: Deferred.Deferred<void> | undefined;
  let finalizerStarted = false;
  let metricActiveGroupId: unknown = "conformance:rows";
  let transitionDefect: Error | undefined;
  const activity = yield* SubscriptionRef.make(0n);
  const observe = (): SourceAdapterConformanceTransportObservation => ({
    acquisitions: counts.acquisitions,
    finalizations: counts.finalizations,
    partialAcquisitionFinalizations,
    registrations: 0n,
    callbackFinalizations: 0n,
    finalizerStarted,
  });
  const changed = SubscriptionRef.update(activity, (version) => version + 1n);

  const region = (regionName: "primary" | "sibling"): KafkaServerRegion => ({
    acquire: () =>
      Effect.gen(function* () {
        const plannedFailure = failedAcquisition;
        const shouldFail =
          plannedFailure !== undefined &&
          ((plannedFailure.partial && regionName === "sibling") ||
            (!plannedFailure.partial && regionName === "primary"));
        if (shouldFail) {
          failedAcquisition = undefined;
          if (plannedFailure.partial) {
            partialAcquisitionPending = true;
          }
          return yield* Effect.fail(plannedFailure.failure);
        }
        const queue = yield* Queue.bounded<
          KafkaServerRecord,
          KafkaAdapterFailure | import("effect").Cause.Done
        >(128);
        pendingQueues.set(regionName, queue);
        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            if (pendingQueues.get(regionName) === queue) {
              pendingQueues.delete(regionName);
            }
            if (regionName === "primary" && partialAcquisitionPending) {
              partialAcquisitionPending = false;
              partialAcquisitionFinalizations += 1n;
              yield* changed;
            }
            yield* Queue.shutdown(queue);
          }),
        );
        return {
          records: Stream.fromQueue(queue),
          recordDecoded: Effect.void,
          recordDecodeFailure: Effect.void,
          recordMapped: Effect.void,
          recordMappingFailure: Effect.void,
          recordRejection: Effect.void,
        };
      }),
    metrics: () => Effect.succeed(emptyRegionMetrics(regionName)),
  });

  const productionLayer = makeKafkaServerLayer({
    brokerContracts: [
      {
        viewServerTopic: "rows",
        sourceTopic: "conformance",
        region: "primary",
        cleanupPolicy: "compact",
        retentionPolicy: { _tag: "Forever" },
        observedCleanupPolicy: "compact",
        observedRetentionMs: -1n,
        resolvedRetention: { _tag: "Forever" },
      },
      {
        viewServerTopic: "rows",
        sourceTopic: "conformance",
        region: "sibling",
        cleanupPolicy: "compact",
        retentionPolicy: { _tag: "Forever" },
        observedCleanupPolicy: "compact",
        observedRetentionMs: -1n,
        resolvedRetention: { _tag: "Forever" },
      },
    ],
    retentionSweepIntervalNanos: 900_000_000_000n,
    consumerGroupPrefix: "conformance",
    regions: new Map([
      ["primary", region("primary")],
      ["sibling", region("sibling")],
    ]),
  });

  const productionBridge = Layer.effect(KafkaConformanceProduction)(
    KafkaSourceAdapter.runtimeService,
  ).pipe(Layer.provide(productionLayer));

  const decoratedLayer = Layer.effect(KafkaSourceAdapter.runtimeService)(
    Effect.gen(function* () {
      const service = yield* KafkaConformanceProduction;
      const materialized = service.materialized;
      if (materialized === undefined) {
        return yield* Effect.die(
          new Error("Kafka conformance requires the Materialized production service."),
        );
      }
      const decorated: typeof service = {
        adapter: service.adapter,
        reporting: service.reporting,
        leased: service.leased,
        materialized: {
          ...definedFields(materialized.applicationState, (applicationState) => ({
            applicationState,
          })),
          ...definedFields(materialized.initialLaneIds, (initialLaneIds) => ({ initialLaneIds })),
          acquire: (input) =>
            Effect.gen(function* () {
              const attempt = yield* materialized.acquire(input);
              counts.acquisitions += 1n;
              const current: ActiveAttempt = {
                queues: new Map(pendingQueues),
                acquisitions: counts.acquisitions,
                finalizations: counts.finalizations,
              };
              active.set("materialized", current);
              yield* Effect.addFinalizer(() =>
                Effect.gen(function* () {
                  counts.finalizations += 1n;
                  current.finalizations = counts.finalizations;
                  active.delete("materialized");
                  if (finalizerBlock !== undefined) {
                    finalizerStarted = true;
                    yield* changed;
                    yield* Deferred.await(finalizerBlock);
                    finalizerBlock = undefined;
                    finalizerStarted = false;
                  }
                  yield* changed;
                }),
              );
              yield* changed;

              type AttemptLane = (typeof attempt.lanes)[number];
              const decorateLane = (lane: AttemptLane): AttemptLane =>
                nominalOverride(
                  lane,
                  "events",
                  lane.events.pipe(
                    Stream.mapEffect((event) => {
                      if (
                        transitionDefect !== undefined &&
                        event._tag === "SourceDelivery" &&
                        event.transition !== undefined
                      ) {
                        const defect = transitionDefect;
                        transitionDefect = undefined;
                        const transition = Option.getOrThrow(
                          Option.fromUndefinedOr(
                            resolveSourceApplicationTransition(event.transition),
                          ),
                        );
                        return Effect.succeed(
                          makeSourceTransitionDelivery(
                            Chunk.headNonEmpty(event.mutations),
                            event.settle,
                            makeSourceApplicationTransition(
                              transition.topic,
                              () => {
                                transition.apply();
                                throw defect;
                              },
                              transition.cancelledMaintenanceWorkIds,
                              transition.lifetimeIdentity,
                            ),
                          ),
                        );
                      }
                      let corruptionApplied = true;
                      if (event._tag === "SourceDelivery") {
                        for (const mutation of event.mutations) {
                          if (mutation._tag !== "Upsert") {
                            continue;
                          }
                          const id: unknown = Reflect.get(mutation.row, "id");
                          if (!isString(id)) {
                            continue;
                          }
                          const corruption = corruptions.get(id);
                          if (corruption !== undefined) {
                            corruptions.delete(id);
                            corruptionApplied = Reflect.set(
                              mutation.row,
                              corruption.field,
                              corruption.value,
                            );
                          }
                        }
                      }
                      return corruptionApplied
                        ? Effect.succeed(event)
                        : Effect.die(
                            new Error(
                              "Kafka conformance mutation corruption could not be applied.",
                            ),
                          );
                    }),
                  ),
                );
              const decoratedLanes = [
                decorateLane(attempt.lanes[0]),
                ...attempt.lanes.slice(1).map(decorateLane),
              ] as const;
              const fault = attemptFaults.shift();
              if (fault === undefined) {
                return nominalOverride(attempt, "lanes", Object.freeze(decoratedLanes));
              }
              const faultedLane = <Lane extends object, Override>(
                lane: Lane,
                property: "id" | "bufferMetrics",
                value: Override,
              ): Lane => nominalOverride(lane, property, value);
              const faultedLanes =
                fault === "EmptyLanes"
                  ? []
                  : fault === "EmptyLaneId"
                    ? [faultedLane(decoratedLanes[0], "id", ""), ...decoratedLanes.slice(1)]
                    : fault === "DuplicateLaneId"
                      ? [
                          decoratedLanes[0],
                          faultedLane(
                            decoratedLanes[1] ?? decoratedLanes[0],
                            "id",
                            decoratedLanes[0].id,
                          ),
                        ]
                      : fault === "ChangedLaneIds"
                        ? decoratedLanes.map((lane) =>
                            faultedLane(lane, "id", `changed-${lane.id}`),
                          )
                        : [
                            faultedLane(decoratedLanes[0], "bufferMetrics", {}),
                            ...decoratedLanes.slice(1),
                          ];
              return nominalOverride(attempt, "lanes", Object.freeze(faultedLanes));
            }),
          metrics: (input): Effect.Effect<KafkaMaterializedMetrics> =>
            materialized.metrics(input).pipe(
              Effect.map((metrics) => {
                Object.defineProperty(metrics, "activeGroupId", {
                  enumerable: true,
                  get: () => metricActiveGroupId,
                });
                return metrics;
              }),
            ),
          retryDefault: materialized.retryDefault,
        },
      };
      return decorated;
    }),
  ).pipe(Layer.provide(productionBridge));

  const offer = (
    target: SourceAdapterConformanceTarget,
    record: KafkaServerRecord,
  ): Effect.Effect<void, KafkaAdapterFailure> => {
    const current = active.get("materialized");
    const queue = current?.queues.get(target.lane);
    return queue === undefined ? Effect.fail(streamFailure) : Queue.offer(queue, record);
  };

  const nextOffsets = new Map<string, bigint>();
  const nextOffset = (target: SourceAdapterConformanceTarget): bigint => {
    const current = nextOffsets.get(target.lane) ?? 0n;
    const offset = current + 1n;
    nextOffsets.set(target.lane, offset);
    return offset;
  };

  const record = (
    target: SourceAdapterConformanceTarget,
    localId: string,
    value: { readonly region: string; readonly value: string } | null,
    settle: ((exit: SourceApplicationExit) => Effect.Effect<void, unknown>) | undefined,
    offset = nextOffset(target),
  ): KafkaServerRecord => ({
    key: textEncoder.encode(localId),
    value: value === null ? null : textEncoder.encode(JSON.stringify(value)),
    metadata: recordMetadata(target, offset),
    settlement: settlement(settle),
  });

  const offerMutation = (
    target: SourceAdapterConformanceTarget,
    mutation: Extract<
      SourceAdapterConformanceCommand,
      { readonly _tag: "Delivery" }
    >["mutations"][number],
    settle: ((exit: SourceApplicationExit) => Effect.Effect<void, unknown>) | undefined,
  ): Effect.Effect<void, KafkaAdapterFailure> =>
    offer(
      target,
      mutation._tag === "Delete"
        ? record(target, mutation.id, null, settle)
        : record(
            target,
            mutation.row.id,
            {
              region: mutation.row.region,
              value: mutation.row.value,
            },
            settle,
          ),
    );

  const command = (input: SourceAdapterConformanceCommand): Effect.Effect<void, unknown> => {
    if (input._tag === "Delivery") {
      return Effect.forEach(
        input.mutations,
        (mutation, index) =>
          offerMutation(
            input.target,
            mutation,
            index === input.mutations.length - 1 ? input.settle : undefined,
          ),
        { discard: true },
      );
    }
    if (input._tag === "CorruptLaterMutation") {
      corruptions.set(
        kafkaRowId({
          cleanupPolicy: "compact",
          region: input.target.lane,
          partition: input.target.lane === "primary" ? 0 : 1,
          serializedKeyBytes: textEncoder.encode(input.laterRow.id),
        }),
        {
          field: input.field,
          value: input.value,
        },
      );
      return offerMutation(
        input.target,
        {
          _tag: "Upsert",
          row: input.firstRow,
        },
        undefined,
      ).pipe(
        Effect.andThen(
          offerMutation(
            input.target,
            {
              _tag: "Upsert",
              row: input.laterRow,
            },
            input.settle,
          ),
        ),
      );
    }
    if (input._tag === "TransitionDefect") {
      transitionDefect = new Error("conformance transition defect");
      return offerMutation(
        input.target,
        {
          _tag: "Upsert",
          row: input.row,
        },
        input.settle,
      );
    }
    if (input._tag === "Reject") {
      return offer(input.target, {
        key: textEncoder.encode(`rejection-${input.offset}`),
        value: textEncoder.encode("{"),
        metadata: recordMetadata(input.target, input.offset),
        settlement: settlement(input.settle),
      });
    }
    if (input._tag === "FailLane") {
      const queue = active.get("materialized")?.queues.get(input.target.lane);
      return queue === undefined ? Effect.fail(streamFailure) : Queue.fail(queue, streamFailure);
    }
    if (input._tag === "CompleteLane") {
      const queue = active.get("materialized")?.queues.get(input.target.lane);
      return queue === undefined ? Effect.fail(streamFailure) : Queue.end(queue);
    }
    if (input._tag === "FailNextAcquisition") {
      return Effect.sync(() => {
        failedAcquisition = {
          partial: input.afterFirstResource,
          failure: acquisitionFailure,
        };
      });
    }
    if (input._tag === "ConfigureNextAttempt") {
      return Effect.sync(() => {
        attemptFaults.push(input.fault);
      });
    }
    if (input._tag === "SetMetrics") {
      return Effect.sync(() => {
        metricActiveGroupId =
          input.sample === "updated"
            ? "conformance-updated:rows"
            : input.sample === "invalid"
              ? 1
              : "conformance:rows";
      });
    }
    if (input._tag === "BlockNextFinalizer") {
      return Effect.gen(function* () {
        finalizerBlock = yield* Deferred.make<void>();
      });
    }
    if (input._tag === "ReleaseFinalizer") {
      return Effect.suspend(() =>
        finalizerBlock === undefined
          ? Effect.void
          : Deferred.succeed(finalizerBlock, undefined).pipe(Effect.asVoid),
      );
    }
    input satisfies never;
    return Effect.die(new Error("Unsupported Kafka conformance command."));
  };

  const ConformanceWireRow = Schema.Struct({
    region: Schema.String,
    value: Schema.String,
  });
  const makeDefinition = (retry?: KafkaSourceRetryPolicy<"primary" | "sibling">) =>
    kafka.source(
      {
        cleanupPolicy: "compact",
        retentionPolicy: "Infinity",
        topic: "conformance",
        regions: ["primary", "sibling"],
        key: kafka.compactionKey.string(),
        value: kafka.json(() => Schema.toCodecJson(ConformanceWireRow)),
        map: ({ value }) => ({
          region: value.region,
          value: value.value,
        }),
        startFrom: "earliest",
      },
      retry ?? Schedule.recurs(3),
    );
  const conformanceDriverInput = {
    adapter: KafkaSourceAdapter,
    expectations: {
      materialized: {
        acquisitionFailure,
        partialAcquisitionFinalizationCount: 1n,
        streamFailure,
        settlementFailure,
        rejectionFailure,
        rejectionLocation: (
          target: Extract<SourceAdapterConformanceTarget, { readonly _tag: "Materialized" }>,
          offset: bigint,
        ) => rejectionLocation(target, offset),
        rowId: (
          target: Extract<SourceAdapterConformanceTarget, { readonly _tag: "Materialized" }>,
          localId: string,
        ) =>
          kafkaRowId({
            cleanupPolicy: "compact",
            region: target.lane,
            partition: target.lane === "primary" ? 0 : 1,
            serializedKeyBytes: textEncoder.encode(localId),
          }),
        updatedMetrics: {
          activeGroupId: "conformance-updated:rows",
          start: {
            _tag: "Resolved",
            position: {
              mode: "earliest",
            },
          },
          regions: [
            emptyMaterializedRegionMetrics("primary"),
            emptyMaterializedRegionMetrics("sibling"),
          ],
        },
      },
      leased: undefined,
    },
    runtimeLayer: decoratedLayer,
    materialized: {
      source: makeDefinition(),
      delayedRetrySource: makeDefinition(
        Schedule.spaced("1 second").pipe(Schedule.upTo({ times: 1 })),
      ),
      singleRetrySource: makeDefinition(Schedule.recurs(1)),
    },
    transport: {
      command,
      observe: () => Effect.succeed(observe()),
      changes: () => SubscriptionRef.changes(activity).pipe(Stream.map(observe)),
    },
  };
  return makeSourceAdapterConformanceDriver(conformanceDriverInput);
});

const assertCallable = <Value>(value: Value, label: string) => {
  if (typeof value !== "function") {
    throw new TypeError(`${label} is not callable.`);
  }
  return value;
};

const readMember = <Value>(value: Value, key: string, label: string): unknown => {
  if ((typeof value !== "object" || value === null) && typeof value !== "function") {
    throw new TypeError(`${label} is not an object.`);
  }
  return Reflect.get(value, key);
};

const definitionInput = <Module extends object>(contractModule: Module) => {
  const builtKafka = readMember(contractModule, "kafka", "Kafka contract module");
  const stringCodec = assertCallable(
    readMember(builtKafka, "string", "Kafka helper"),
    "kafka.string",
  );
  return {
    cleanupPolicy: "delete",
    retentionPolicy: "Infinity",
    topic: "conformance",
    regions: ["primary", "sibling"],
    key: Reflect.apply(stringCodec, undefined, []),
    value: Reflect.apply(stringCodec, undefined, []),
    localRowKey: (input: { readonly key: string }) => input.key,
    map: (input: { readonly region: string; readonly value: string }) => ({
      region: input.region,
      value: input.value,
    }),
    startFrom: "earliest",
  };
};

const builtDefinition = <Module extends object>(contractModule: Module): unknown => {
  const builtKafka = readMember(contractModule, "kafka", "Kafka contract module");
  const source = assertCallable(readMember(builtKafka, "source", "Kafka helper"), "kafka.source");
  return Reflect.apply(source, undefined, [definitionInput(contractModule)]);
};

const packageRoot = fileURLToPath(new URL("..", import.meta.url));

const packageInspection: SourceAdapterPackageInspectionOptions = {
  name: "first-party Kafka Source Adapter",
  packageRoot,
  contract: {
    adapterExport: "KafkaSourceAdapter",
    serverAdapterExport: "KafkaSourceAdapterServer",
    failure: {
      valid: {
        _tag: "KafkaConfigurationFailure",
        message: "invalid configuration",
      },
      invalid: {
        _tag: "KafkaConfigurationFailure",
        message: 1,
      },
    },
    lifecycles: [
      {
        lifecycle: "materialized",
        definitionExport: ["kafka", "source"],
        definitionArguments: (contractModule) => [definitionInput(contractModule)],
        metrics: {
          valid: {
            activeGroupId: "conformance:rows",
            start: { _tag: "Pending" },
            regions: [
              {
                ...emptyRegionMetrics("primary"),
                retention: foreverRetentionMetrics("delete"),
              },
              {
                ...emptyRegionMetrics("sibling"),
                retention: foreverRetentionMetrics("delete"),
              },
            ],
          },
          invalid: {
            activeGroupId: 1,
            start: { _tag: "Pending" },
            regions: [],
          },
        },
        rejectionLocation: {
          valid: {
            region: "primary",
            topic: "conformance",
            partition: 0,
            offset: 1n,
            phase: "mapping",
            message: "invalid row",
          },
          invalid: {
            region: "primary",
            topic: "conformance",
            partition: 0,
            offset: 1,
            phase: "mapping",
            message: "invalid row",
          },
        },
      },
    ],
  },
  typeTestProject: "tsconfig.json",
  browser: {
    budgetBytes: 96 * 1024,
    additionalForbiddenModulePatterns: ["@platformatic/kafka", "node:"],
  },
  platforms: [
    {
      export: "./node",
      exactLayerAcquisition: "external-validation-failure",
      externalValidationFailure: kafkaExternalValidationFailure,
      viewServer: <Module extends object>(contractModule: Module) => ({
        topics: {
          rows: {
            source: builtDefinition(contractModule),
          },
        },
      }),
      exactResources: {
        consumerGroupPrefix: "conformance",
        regions: {
          primary: {
            bootstrapServers: "127.0.0.1:1",
            connectTimeout: 1,
            requestTimeout: 1,
            retries: false,
            timeout: 1,
          },
          sibling: {
            bootstrapServers: "127.0.0.1:1",
            connectTimeout: 1,
            requestTimeout: 1,
            retries: false,
            timeout: 1,
          },
        },
      },
      emptyResources: {
        consumerGroupPrefix: "conformance",
        regions: {},
      },
      missingResources: {
        consumerGroupPrefix: "conformance",
      },
      extraResources: {
        consumerGroupPrefix: "conformance",
        regions: {
          primary: {
            bootstrapServers: "localhost:9092",
          },
          sibling: {
            bootstrapServers: "localhost:9093",
          },
          extra: {
            bootstrapServers: "localhost:9094",
          },
        },
      },
      duplicateResources: {
        consumerGroupPrefix: "conformance",
        regions: [
          {
            primary: {
              bootstrapServers: "localhost:9092",
            },
          },
          {
            primary: {
              bootstrapServers: "localhost:9092",
            },
          },
        ],
      },
      exactConfigResources: {
        consumerGroupPrefix: Config.succeed("conformance"),
        regions: {
          primary: {
            bootstrapServers: Config.succeed("127.0.0.1:1"),
            connectTimeout: Config.succeed(1),
            requestTimeout: Config.succeed(1),
            retries: Config.succeed(false),
            timeout: Config.succeed(1),
          },
          sibling: {
            bootstrapServers: Config.succeed("127.0.0.1:1"),
            connectTimeout: Config.succeed(1),
            requestTimeout: Config.succeed(1),
            retries: Config.succeed(false),
            timeout: Config.succeed(1),
          },
        },
      },
      resourceValidationFailure: kafkaResourceValidationFailure,
    },
  ],
};

registerSourceAdapterPackageConformance({
  inspection: packageInspection,
  behavioral: {
    name: "first-party Kafka Source Adapter",
    layer: Layer.effect(SourceAdapterConformanceDriver, makeKafkaConformanceDriver()),
    materialized: true,
  },
});
