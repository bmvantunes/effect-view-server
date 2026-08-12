import { Cause, Duration, Effect, Option, Scope, Semaphore, Stream, SubscriptionRef } from "effect";
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

export type KafkaSchemaRegistryValidationInput = {
  readonly viewServerTopic: string;
  readonly sourceTopic: string;
  readonly side: KafkaSchemaRegistrySide;
  readonly bytes: Uint8Array;
};

export type KafkaServerSchemaRegistry = {
  readonly endpoints: ReadonlyArray<string>;
  readonly guard: (
    input: KafkaSchemaRegistryBindingInput,
  ) => Effect.Effect<void, KafkaAdapterFailure>;
  readonly failures: (
    input: KafkaSchemaRegistryBindingInput,
  ) => Stream.Stream<never, KafkaAdapterFailure>;
  readonly validate: (
    input: KafkaSchemaRegistryValidationInput,
  ) => Effect.Effect<Uint8Array, KafkaAdapterFailure>;
};

type RegistryState = {
  readonly revision: number;
  readonly contracts: ReadonlyMap<
    string,
    ReadonlyMap<KafkaSchemaRegistrySide, KafkaResolvedSchemaRegistryContract>
  >;
  readonly failures: ReadonlyMap<string, ReadonlyMap<KafkaSchemaRegistrySide, KafkaAdapterFailure>>;
};

const runtimeFailure = (issue: KafkaSchemaRegistryContractIssue): KafkaAdapterFailure => ({
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
  const failures = new Map<string, Map<KafkaSchemaRegistrySide, KafkaAdapterFailure>>();
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
  const failures = new Map<string, Map<KafkaSchemaRegistrySide, KafkaAdapterFailure>>();
  for (const declaration of declarations) {
    const sides = failures.get(declaration.viewServerTopic) ?? new Map();
    sides.set(declaration.side, monitorFailure(declaration));
    failures.set(declaration.viewServerTopic, sides);
  }
  return failures;
};

const monitorFailure = (declaration: KafkaSchemaRegistryDeclaration): KafkaAdapterFailure => ({
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
): KafkaAdapterFailure | undefined => {
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
  input: KafkaSchemaRegistryValidationInput,
): KafkaAdapterFailure => ({
  _tag: "KafkaSchemaRegistrySchemaMismatch",
  region,
  topic: input.sourceTopic,
  subject: `${input.sourceTopic}-${input.side}`,
  side: input.side,
  schemaId: null,
  message: `Kafka Schema Registry contract for ${input.side} subject ${JSON.stringify(`${input.sourceTopic}-${input.side}`)} is unavailable.`,
});

export const makeKafkaSchemaRegistryRuntime = Effect.fn("KafkaSchemaRegistryRuntime.make")(
  function* (input: {
    readonly region: string;
    readonly endpoint: string;
    readonly declarations: ReadonlyArray<KafkaSchemaRegistryDeclaration>;
    readonly reader: KafkaSchemaRegistryReader;
    readonly monitorInterval: Duration.Duration;
  }): Effect.fn.Return<
    KafkaServerSchemaRegistry,
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
    const refresh = Effect.fn("KafkaSchemaRegistryRuntime.refresh")(function* (
      expectedRevision?: number,
    ) {
      return yield* refreshLock.withPermit(
        Effect.gen(function* () {
          const current = SubscriptionRef.getUnsafe(state);
          if (expectedRevision !== undefined && current.revision !== expectedRevision) {
            return;
          }
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
          return resolution;
        }),
      );
    });
    const monitor = Effect.forever(
      Effect.sleep(input.monitorInterval).pipe(Effect.andThen(refresh()), Effect.asVoid),
    ).pipe(
      Effect.catchCauseIf(
        (cause) => !Cause.hasInterruptsOnly(cause),
        () =>
          SubscriptionRef.update(state, (current) => ({
            revision: current.revision + 1,
            contracts: current.contracts,
            failures: monitorFailures(input.declarations),
          })),
      ),
    );
    yield* Effect.forkScoped(monitor);

    const guard = (
      binding: KafkaSchemaRegistryBindingInput,
    ): Effect.Effect<void, KafkaAdapterFailure> => {
      const failure = bindingFailure(SubscriptionRef.getUnsafe(state), binding);
      return failure === undefined ? Effect.void : Effect.fail(failure);
    };
    const failures = (
      binding: KafkaSchemaRegistryBindingInput,
    ): Stream.Stream<never, KafkaAdapterFailure> =>
      SubscriptionRef.changes(state).pipe(
        Stream.mapEffect((current) => {
          const failure = bindingFailure(current, binding);
          return failure === undefined ? Effect.void : Effect.fail(failure);
        }),
        Stream.drain,
      );
    const validate = (
      validation: KafkaSchemaRegistryValidationInput,
    ): Effect.Effect<Uint8Array, KafkaAdapterFailure> =>
      Effect.gen(function* () {
        const current = SubscriptionRef.getUnsafe(state);
        const currentFailure = current.failures
          .get(validation.viewServerTopic)
          ?.get(validation.side);
        if (currentFailure !== undefined) {
          return yield* Effect.fail(currentFailure);
        }
        let contract = current.contracts.get(validation.viewServerTopic)?.get(validation.side);
        if (contract === undefined) {
          return yield* Effect.fail(missingContractFailure(input.region, validation));
        }
        let result = validateKafkaSchemaRegistryFrame(contract, validation.bytes);
        if (
          result._tag === "Mismatch" &&
          result.schemaId !== null &&
          !contract.schemaIds.has(result.schemaId)
        ) {
          yield* refresh(current.revision);
          const refreshedState = SubscriptionRef.getUnsafe(state);
          const failure = refreshedState.failures
            .get(validation.viewServerTopic)
            ?.get(validation.side);
          if (failure !== undefined) {
            return yield* Effect.fail(failure);
          }
          const refreshed = Option.getOrThrow(
            Option.fromUndefinedOr(
              refreshedState.contracts.get(validation.viewServerTopic)?.get(validation.side),
            ),
          );
          contract = refreshed;
          result = validateKafkaSchemaRegistryFrame(refreshed, validation.bytes);
        }
        if (result._tag === "Mismatch") {
          const failure: KafkaAdapterFailure = {
            _tag: "KafkaSchemaRegistrySchemaMismatch",
            region: input.region,
            topic: validation.sourceTopic,
            subject: contract.subject,
            side: validation.side,
            schemaId: result.schemaId,
            message: result.message,
          };
          return yield* Effect.fail(failure);
        }
        return result.payload;
      });

    return {
      endpoints: Object.freeze([input.endpoint]),
      guard,
      failures,
      validate,
    };
  },
);
