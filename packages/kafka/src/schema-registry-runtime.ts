import {
  Cause,
  Duration,
  Effect,
  Exit,
  Option,
  Scope,
  Semaphore,
  Stream,
  SubscriptionRef,
} from "effect";
import type { KafkaAdapterFailure } from "./contract";
import {
  inspectKafkaSchemaRegistryContracts,
  resolveKafkaSchemaRegistryContracts,
  validateKafkaSchemaRegistryFrame,
  type KafkaResolvedSchemaRegistryContract,
  type KafkaSchemaRegistryContractIssue,
  type KafkaSchemaRegistryContractValidationFailure,
  type KafkaSchemaRegistryDeclaration,
  type KafkaSchemaRegistryReader,
  type KafkaSchemaRegistrySide,
} from "./schema-registry-contract";

export type KafkaSchemaRegistryBindingInput = {
  readonly viewServerTopic: string;
  readonly sourceTopic: string;
  readonly sides: ReadonlyArray<KafkaSchemaRegistrySide>;
};

export type KafkaSchemaRegistryRecordValidationInput = KafkaSchemaRegistryBindingInput & {
  readonly key: Uint8Array | null;
  readonly value: Uint8Array | null;
};

export type KafkaSchemaRegistryRecordPayloads = {
  readonly key: Uint8Array | undefined;
  readonly value: Uint8Array | undefined;
};

export type KafkaSchemaRegistryRecordDecodeFailure = {
  readonly _tag: "KafkaSchemaRegistryRecordDecodeFailure";
  readonly side: KafkaSchemaRegistrySide;
  readonly failure: Extract<KafkaAdapterFailure, { readonly _tag: "KafkaDecodeFailure" }>;
};

type KafkaSchemaRegistryAttemptFailure = Exclude<
  KafkaAdapterFailure,
  { readonly _tag: "KafkaDecodeFailure" }
>;

export type KafkaSchemaRegistryRecordValidationFailure =
  | KafkaSchemaRegistryAttemptFailure
  | KafkaSchemaRegistryRecordDecodeFailure;

export type KafkaSchemaRegistryRuntime = {
  readonly endpoints: ReadonlyArray<string>;
  readonly guard: (
    input: KafkaSchemaRegistryBindingInput,
  ) => Effect.Effect<void, KafkaSchemaRegistryAttemptFailure>;
  readonly failures: (
    input: KafkaSchemaRegistryBindingInput,
  ) => Stream.Stream<never, KafkaSchemaRegistryAttemptFailure>;
  readonly validateRecord: (
    input: KafkaSchemaRegistryRecordValidationInput,
  ) => Effect.Effect<KafkaSchemaRegistryRecordPayloads, KafkaSchemaRegistryRecordValidationFailure>;
};

export type KafkaServerSchemaRegistry = KafkaSchemaRegistryRuntime & {
  readonly retain: (lifetimeScope: Scope.Scope) => Effect.Effect<void>;
};

export const makeKafkaServerSchemaRegistry = Effect.fn("KafkaSchemaRegistryRuntime.server")(
  function* <E>(input: {
    readonly layerScope: Scope.Scope;
    readonly acquire: Effect.Effect<KafkaSchemaRegistryRuntime, E, Scope.Scope>;
  }): Effect.fn.Return<KafkaServerSchemaRegistry, E> {
    const resourceScope = yield* Scope.make("sequential");
    const closeResource = (yield* Effect.cached(Scope.close(resourceScope, Exit.void))).pipe(
      Effect.uninterruptible,
    );
    const lock = Semaphore.makeUnsafe(1);
    const retainedLifetimes = new Set<Scope.Scope>();
    let layerReleased = false;
    let resourceClosing = false;
    const closeIfUnowned = lock
      .withPermit(
        Effect.sync(() => {
          if (resourceClosing || !layerReleased || retainedLifetimes.size > 0) {
            return false;
          }
          resourceClosing = true;
          return true;
        }),
      )
      .pipe(Effect.flatMap((shouldClose) => (shouldClose ? closeResource : Effect.void)));
    const releaseLifetime = (lifetimeScope: Scope.Scope) =>
      lock
        .withPermit(
          Effect.sync(() => {
            retainedLifetimes.delete(lifetimeScope);
          }),
        )
        .pipe(Effect.andThen(closeIfUnowned));
    const retain = Effect.fn("KafkaSchemaRegistryRuntime.retain")(function* (
      lifetimeScope: Scope.Scope,
    ) {
      yield* Effect.uninterruptible(
        Effect.gen(function* () {
          const retained = yield* lock.withPermit(
            Effect.sync(() => {
              if (resourceClosing) {
                return false;
              }
              retainedLifetimes.add(lifetimeScope);
              return true;
            }),
          );
          if (!retained) {
            return yield* Effect.interrupt;
          }
          yield* Scope.addFinalizer(lifetimeScope, releaseLifetime(lifetimeScope));
        }),
      );
    });
    yield* Scope.addFinalizer(
      input.layerScope,
      Effect.uninterruptible(
        lock
          .withPermit(
            Effect.sync(() => {
              layerReleased = true;
            }),
          )
          .pipe(Effect.andThen(closeIfUnowned)),
      ),
    );
    const runtime = yield* input.acquire.pipe(
      Effect.provideService(Scope.Scope, resourceScope),
      Effect.onExit((exit) =>
        Exit.isFailure(exit) ? Scope.close(resourceScope, exit) : Effect.void,
      ),
    );
    const serverRuntime: KafkaServerSchemaRegistry = {
      ...runtime,
      retain,
    };
    return serverRuntime;
  },
);

type RegistryState = {
  readonly revision: number;
  readonly contracts: ReadonlyMap<
    string,
    ReadonlyMap<KafkaSchemaRegistrySide, KafkaResolvedSchemaRegistryContract>
  >;
  readonly failures: ReadonlyMap<
    string,
    ReadonlyMap<KafkaSchemaRegistrySide, KafkaSchemaRegistryAttemptFailure>
  >;
};

const runtimeFailure = (
  issue: KafkaSchemaRegistryContractIssue,
): KafkaSchemaRegistryAttemptFailure => ({
  _tag:
    issue.code === "RegistryUnavailable"
      ? "KafkaSchemaRegistryUnavailable"
      : issue.code === "CompatibilityPolicyMismatch"
        ? "KafkaSchemaRegistryPolicyMismatch"
        : "KafkaSchemaRegistrySchemaMismatch",
  region: issue.region,
  topic: issue.sourceTopic,
  subject: issue.subject,
  side: issue.side,
  schemaId: issue.schemaId,
  message: issue.message,
});

const contractsByTopic = (
  contracts: ReadonlyArray<KafkaResolvedSchemaRegistryContract>,
): RegistryState["contracts"] => {
  const topics = new Map<
    string,
    Map<KafkaSchemaRegistrySide, KafkaResolvedSchemaRegistryContract>
  >();
  for (const contract of contracts) {
    const sides = topics.get(contract.viewServerTopic) ?? new Map();
    sides.set(contract.side, contract);
    topics.set(contract.viewServerTopic, sides);
  }
  return topics;
};

const failuresByTopic = (
  issues: ReadonlyArray<KafkaSchemaRegistryContractIssue>,
): RegistryState["failures"] => {
  const failures = new Map<
    string,
    Map<KafkaSchemaRegistrySide, KafkaSchemaRegistryAttemptFailure>
  >();
  for (const contractIssue of issues) {
    const sides = failures.get(contractIssue.viewServerTopic) ?? new Map();
    sides.set(contractIssue.side, runtimeFailure(contractIssue));
    failures.set(contractIssue.viewServerTopic, sides);
  }
  return failures;
};

const monitorFailures = (
  declarations: ReadonlyArray<KafkaSchemaRegistryDeclaration>,
): RegistryState["failures"] => {
  const failures = new Map<
    string,
    Map<KafkaSchemaRegistrySide, KafkaSchemaRegistryAttemptFailure>
  >();
  for (const declaration of declarations) {
    const sides = failures.get(declaration.viewServerTopic) ?? new Map();
    sides.set(declaration.side, monitorFailure(declaration));
    failures.set(declaration.viewServerTopic, sides);
  }
  return failures;
};

const monitorFailure = (
  declaration: KafkaSchemaRegistryDeclaration,
): KafkaSchemaRegistryAttemptFailure => ({
  _tag: "KafkaSchemaRegistryUnavailable",
  region: declaration.region,
  topic: declaration.sourceTopic,
  subject: declaration.subject,
  side: declaration.side,
  schemaId: null,
  message: "Kafka Schema Registry drift monitor terminated unexpectedly.",
});

const bindingFailure = (
  state: RegistryState,
  input: KafkaSchemaRegistryBindingInput,
): KafkaSchemaRegistryAttemptFailure | undefined => {
  const failures = state.failures.get(input.viewServerTopic);
  for (const side of input.sides) {
    const failure = failures?.get(side);
    if (failure !== undefined) {
      return failure;
    }
  }
  return undefined;
};

const missingContractFailure = (
  region: string,
  input: {
    readonly sourceTopic: string;
    readonly side: KafkaSchemaRegistrySide;
  },
): Extract<KafkaAdapterFailure, { readonly _tag: "KafkaSchemaRegistrySchemaMismatch" }> => ({
  _tag: "KafkaSchemaRegistrySchemaMismatch",
  region,
  topic: input.sourceTopic,
  subject: `${input.sourceTopic}-${input.side}`,
  side: input.side,
  schemaId: null,
  message: `Kafka Schema Registry contract for ${input.side} subject ${JSON.stringify(`${input.sourceTopic}-${input.side}`)} is unavailable.`,
});

type SettledRecordStateValidation =
  | {
      readonly _tag: "Failure";
      readonly failure: KafkaSchemaRegistryRecordValidationFailure;
    }
  | {
      readonly _tag: "Success";
      readonly payloads: KafkaSchemaRegistryRecordPayloads;
    };

type RecordStateValidation =
  | SettledRecordStateValidation
  | {
      readonly _tag: "Refresh";
    };

function validateRecordState(
  region: string,
  state: RegistryState,
  input: KafkaSchemaRegistryRecordValidationInput,
  refreshUnknownIds: false,
): SettledRecordStateValidation;
function validateRecordState(
  region: string,
  state: RegistryState,
  input: KafkaSchemaRegistryRecordValidationInput,
  refreshUnknownIds: true,
): RecordStateValidation;
function validateRecordState(
  region: string,
  state: RegistryState,
  input: KafkaSchemaRegistryRecordValidationInput,
  refreshUnknownIds: boolean,
): RecordStateValidation {
  const failure = bindingFailure(state, input);
  if (failure !== undefined) {
    return { _tag: "Failure", failure };
  }
  let key: Uint8Array | undefined;
  let value: Uint8Array | undefined;
  let requiresRefresh = false;
  let recordDecodeFailure: KafkaSchemaRegistryRecordDecodeFailure | undefined;
  for (const side of input.sides) {
    const bytes = side === "key" ? input.key : input.value;
    if (bytes === null) {
      continue;
    }
    const contract = state.contracts.get(input.viewServerTopic)?.get(side);
    if (contract === undefined) {
      return {
        _tag: "Failure",
        failure: missingContractFailure(region, {
          sourceTopic: input.sourceTopic,
          side,
        }),
      };
    }
    const result = validateKafkaSchemaRegistryFrame(contract, bytes);
    if (result._tag === "Mismatch") {
      if (result.reason === "frame") {
        recordDecodeFailure ??= {
          _tag: "KafkaSchemaRegistryRecordDecodeFailure",
          side,
          failure: {
            _tag: "KafkaDecodeFailure",
            region,
            topic: input.sourceTopic,
            message: result.message,
          },
        };
        continue;
      }
      if (
        refreshUnknownIds &&
        result.schemaId !== null &&
        !contract.schemaIds.has(result.schemaId)
      ) {
        requiresRefresh = true;
        continue;
      }
      return {
        _tag: "Failure",
        failure: {
          _tag: "KafkaSchemaRegistrySchemaMismatch",
          region,
          topic: input.sourceTopic,
          subject: contract.subject,
          side,
          schemaId: result.schemaId,
          message: result.message,
        },
      };
    }
    if (side === "key") {
      key = result.payload;
    } else {
      value = result.payload;
    }
  }
  if (requiresRefresh) {
    return { _tag: "Refresh" };
  }
  if (recordDecodeFailure !== undefined) {
    return { _tag: "Failure", failure: recordDecodeFailure };
  }
  return {
    _tag: "Success",
    payloads: Object.freeze({ key, value }),
  };
}

export const makeKafkaSchemaRegistryRuntime = Effect.fn("KafkaSchemaRegistryRuntime.make")(
  function* (input: {
    readonly region: string;
    readonly endpoint: string;
    readonly declarations: ReadonlyArray<KafkaSchemaRegistryDeclaration>;
    readonly reader: KafkaSchemaRegistryReader;
    readonly monitorInterval: Duration.Duration;
  }): Effect.fn.Return<
    KafkaSchemaRegistryRuntime,
    KafkaSchemaRegistryContractValidationFailure,
    Scope.Scope
  > {
    const initial = yield* resolveKafkaSchemaRegistryContracts(input.declarations, input.reader);
    const state = yield* SubscriptionRef.make<RegistryState>({
      revision: 0,
      contracts: contractsByTopic(initial),
      failures: new Map(),
    });
    const refreshLock = Semaphore.makeUnsafe(1);
    let refreshSequence = 0;
    let completedRefreshSequence = 0;
    let monitorTerminated = false;
    const refresh = Effect.fn("KafkaSchemaRegistryRuntime.refresh")(function* (
      completedAfterSequence?: number,
    ) {
      return yield* refreshLock.withPermit(
        Effect.gen(function* () {
          const current = SubscriptionRef.getUnsafe(state);
          if (monitorTerminated) {
            return;
          }
          if (
            completedAfterSequence !== undefined &&
            completedRefreshSequence > completedAfterSequence
          ) {
            return;
          }
          refreshSequence += 1;
          const currentRefreshSequence = refreshSequence;
          const resolution = yield* inspectKafkaSchemaRegistryContracts(
            input.declarations,
            input.reader,
          );
          const contracts = new Map(current.contracts);
          for (const contract of resolution.contracts) {
            const sides = new Map(
              Option.getOrThrow(Option.fromUndefinedOr(contracts.get(contract.viewServerTopic))),
            );
            sides.set(contract.side, contract);
            contracts.set(contract.viewServerTopic, sides);
          }
          yield* SubscriptionRef.set(state, {
            revision: current.revision + 1,
            contracts,
            failures: failuresByTopic(resolution.issues),
          });
          completedRefreshSequence = currentRefreshSequence;
          return resolution;
        }),
      );
    });
    const monitor = Effect.forever(
      Effect.sleep(input.monitorInterval).pipe(Effect.andThen(refresh()), Effect.asVoid),
    ).pipe(
      Effect.catchCauseIf(
        (cause) => !Cause.hasInterruptsOnly(cause),
        (cause) =>
          refreshLock.withPermit(
            Effect.gen(function* () {
              monitorTerminated = true;
              yield* Effect.logError(
                "Kafka Schema Registry drift monitor terminated unexpectedly.",
              ).pipe(Effect.annotateLogs({ cause: Cause.pretty(cause) }));
              yield* SubscriptionRef.update(state, (current) => ({
                revision: current.revision + 1,
                contracts: current.contracts,
                failures: monitorFailures(input.declarations),
              }));
            }),
          ),
      ),
    );
    yield* Effect.forkScoped(monitor);

    const guard = (
      binding: KafkaSchemaRegistryBindingInput,
    ): Effect.Effect<void, KafkaSchemaRegistryAttemptFailure> =>
      SubscriptionRef.get(state).pipe(
        Effect.flatMap((current) => {
          const failure = bindingFailure(current, binding);
          return failure === undefined ? Effect.void : Effect.fail(failure);
        }),
      );
    const failures = (
      binding: KafkaSchemaRegistryBindingInput,
    ): Stream.Stream<never, KafkaSchemaRegistryAttemptFailure> =>
      SubscriptionRef.changes(state).pipe(
        Stream.mapEffect((current) => {
          const failure = bindingFailure(current, binding);
          return failure === undefined ? Effect.void : Effect.fail(failure);
        }),
        Stream.drain,
      );
    const validateRecord = (
      validation: KafkaSchemaRegistryRecordValidationInput,
    ): Effect.Effect<
      KafkaSchemaRegistryRecordPayloads,
      KafkaSchemaRegistryRecordValidationFailure
    > =>
      Effect.gen(function* () {
        const current = SubscriptionRef.getUnsafe(state);
        const initialValidation = validateRecordState(input.region, current, validation, true);
        if (initialValidation._tag === "Failure") {
          return yield* Effect.fail(initialValidation.failure);
        }
        if (initialValidation._tag === "Success") {
          return initialValidation.payloads;
        }
        const observedRefreshSequence = refreshSequence;
        yield* refresh(observedRefreshSequence);
        const refreshedValidation = validateRecordState(
          input.region,
          SubscriptionRef.getUnsafe(state),
          validation,
          false,
        );
        if (refreshedValidation._tag === "Failure") {
          return yield* Effect.fail(refreshedValidation.failure);
        }
        return refreshedValidation.payloads;
      });
    return {
      endpoints: Object.freeze([input.endpoint]),
      guard,
      failures,
      validateRecord,
    };
  },
);
