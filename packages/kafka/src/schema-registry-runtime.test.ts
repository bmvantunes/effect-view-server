import { clone } from "@bufbuild/protobuf";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { describe, expect, it } from "@effect/vitest";
import {
  Cause,
  Clock,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Option,
  Scope,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import {
  type KafkaSchemaRegistryDeclaration,
  type KafkaSchemaRegistryReader,
  type KafkaSchemaRegistrySchemaVersion,
} from "./schema-registry-contract";
import {
  makeKafkaSchemaRegistryRuntime,
  makeKafkaServerSchemaRegistry,
  type KafkaSchemaRegistryRuntime,
} from "./schema-registry-runtime";
import { OrderValueSchema } from "./test-fixtures/orders_pb";

type MutableSubject = {
  compatibility: string;
  active: Array<number>;
  all: Array<number>;
  readonly schemas: Map<number, KafkaSchemaRegistrySchemaVersion>;
};

type ReaderCalls = {
  compatibility: number;
  versions: number;
  schemas: number;
};

const schemaVersion = (
  subject: string,
  version: number,
  id: number,
  descriptor = OrderValueSchema.file.proto,
): KafkaSchemaRegistrySchemaVersion => ({
  subject,
  version,
  id,
  schemaType: "PROTOBUF",
  references: [],
  descriptor,
});

const directionalBytesDescriptor = () => {
  const descriptor = clone(FileDescriptorProtoSchema, OrderValueSchema.file.proto);
  const field = descriptor.messageType[0]?.field[0];
  if (field === undefined) {
    throw new Error("OrderValue customer_id fixture is missing");
  }
  field.type = FieldDescriptorProto_Type.BYTES;
  return descriptor;
};

const mutableSubject = (subject: string, id: number): MutableSubject => ({
  compatibility: "FULL_TRANSITIVE",
  active: [1],
  all: [1],
  schemas: new Map([[1, schemaVersion(subject, 1, id)]]),
});

const mutableReader = (
  subjects: ReadonlyMap<string, MutableSubject>,
  calls: ReaderCalls,
): KafkaSchemaRegistryReader => ({
  effectiveCompatibility: (subject) =>
    Effect.gen(function* () {
      calls.compatibility += 1;
      const state = subjects.get(subject);
      return state === undefined
        ? yield* Effect.fail({ message: "compatibility unavailable" })
        : state.compatibility;
    }),
  versions: (subject, includeDeleted) =>
    Effect.gen(function* () {
      calls.versions += 1;
      const state = subjects.get(subject);
      return state === undefined
        ? yield* Effect.fail({ message: "versions unavailable" })
        : includeDeleted
          ? state.all
          : state.active;
    }),
  schema: (subject, version) =>
    Effect.gen(function* () {
      calls.schemas += 1;
      const schema = subjects.get(subject)?.schemas.get(version);
      return schema === undefined ? yield* Effect.fail({ message: "schema unavailable" }) : schema;
    }),
});

const declaration = (
  viewServerTopic: string,
  sourceTopic: string,
  side: "key" | "value" = "value",
): KafkaSchemaRegistryDeclaration => ({
  region: "eu",
  viewServerTopic,
  sourceTopic,
  side,
  subject: `${sourceTopic}-${side}`,
  descriptor: OrderValueSchema,
});

const frame = (schemaId: number): Uint8Array =>
  Uint8Array.from([
    0,
    Math.floor(schemaId / 0x1_00_00_00) % 0x100,
    Math.floor(schemaId / 0x1_00_00) % 0x100,
    Math.floor(schemaId / 0x1_00) % 0x100,
    schemaId % 0x100,
    0,
  ]);

const validateSide = (
  runtime: KafkaSchemaRegistryRuntime,
  input: {
    readonly viewServerTopic: string;
    readonly sourceTopic: string;
    readonly side: "key" | "value";
    readonly bytes: Uint8Array;
  },
) =>
  runtime
    .validateRecord({
      viewServerTopic: input.viewServerTopic,
      sourceTopic: input.sourceTopic,
      sides: [input.side],
      key: input.side === "key" ? input.bytes : null,
      value: input.side === "value" ? input.bytes : null,
    })
    .pipe(
      Effect.map((payloads) =>
        Option.getOrThrow(
          Option.fromUndefinedOr(input.side === "key" ? payloads.key : payloads.value),
        ),
      ),
    );

const serverRuntimeFixture: KafkaSchemaRegistryRuntime = {
  endpoints: [],
  guard: () => Effect.void,
  failures: () => Stream.never,
  validateRecord: () => Effect.succeed({ key: undefined, value: undefined }),
};

describe("Kafka Schema Registry server lifetime", () => {
  it.effect("interrupts retention after resource closure becomes irreversible", () =>
    Effect.gen(function* () {
      const layerScope = yield* Scope.make("sequential");
      const lateLifetimeScope = yield* Scope.make("sequential");
      const finalizerStarted = yield* Deferred.make<void>();
      const releaseFinalizer = yield* Deferred.make<void>();
      let closeCount = 0;
      const runtime = yield* makeKafkaServerSchemaRegistry({
        layerScope,
        acquire: Effect.acquireRelease(Effect.succeed(serverRuntimeFixture), () =>
          Effect.gen(function* () {
            closeCount += 1;
            yield* Deferred.succeed(finalizerStarted, undefined);
            yield* Deferred.await(releaseFinalizer);
          }),
        ),
      });

      const layerCloseFiber = yield* Scope.close(layerScope, Exit.void).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(finalizerStarted);
      const lateRetainExit = yield* runtime.retain(lateLifetimeScope).pipe(Effect.exit);
      const closeCountBeforeRelease = closeCount;

      yield* Deferred.succeed(releaseFinalizer, undefined);
      yield* Fiber.join(layerCloseFiber);
      yield* Scope.close(lateLifetimeScope, Exit.void);

      expect(Exit.isFailure(lateRetainExit) && Cause.hasInterruptsOnly(lateRetainExit.cause)).toBe(
        true,
      );
      expect(closeCountBeforeRelease).toBe(1);
      expect(closeCount).toBe(1);
    }),
  );

  it.effect("keeps a retained lifetime alive through layer closure and closes exactly once", () =>
    Effect.gen(function* () {
      const layerScope = yield* Scope.make("sequential");
      const lifetimeScope = yield* Scope.make("sequential");
      let closeCount = 0;
      const runtime = yield* makeKafkaServerSchemaRegistry({
        layerScope,
        acquire: Effect.acquireRelease(Effect.succeed(serverRuntimeFixture), () =>
          Effect.sync(() => {
            closeCount += 1;
          }),
        ),
      });

      yield* runtime.retain(lifetimeScope);
      yield* Scope.close(layerScope, Exit.void);
      expect(closeCount).toBe(0);

      yield* Scope.close(lifetimeScope, Exit.void);
      yield* Scope.close(layerScope, Exit.void);
      yield* Scope.close(lifetimeScope, Exit.void);
      expect(closeCount).toBe(1);
    }),
  );
});

describe("Kafka Schema Registry Region runtime", () => {
  it.effect("classifies a malformed record frame as an item-local decode failure", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const orders = mutableSubject("source-orders-value", 41);
        const runtime = yield* makeKafkaSchemaRegistryRuntime({
          region: "eu",
          endpoint: "https://registry.eu.example.com",
          declarations: [declaration("orders", "source-orders")],
          reader: mutableReader(new Map([["source-orders-value", orders]]), {
            compatibility: 0,
            versions: 0,
            schemas: 0,
          }),
          monitorInterval: Duration.seconds(1),
        });

        expect(
          yield* validateSide(runtime, {
            viewServerTopic: "orders",
            sourceTopic: "source-orders",
            side: "value",
            bytes: Uint8Array.from([]),
          }).pipe(Effect.flip),
        ).toStrictEqual({
          _tag: "KafkaSchemaRegistryRecordDecodeFailure",
          side: "value",
          failure: {
            _tag: "KafkaDecodeFailure",
            region: "eu",
            topic: "source-orders",
            message:
              "Confluent Schema Registry Protobuf frame is shorter than its six-byte minimum prefix.",
          },
        });
      }),
    ),
  );

  it.effect("prioritizes an attempt-level schema failure over a malformed sibling frame", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const key = mutableSubject("source-orders-key", 40);
        const value = mutableSubject("source-orders-value", 41);
        const runtime = yield* makeKafkaSchemaRegistryRuntime({
          region: "eu",
          endpoint: "https://registry.eu.example.com",
          declarations: [
            declaration("orders", "source-orders", "key"),
            declaration("orders", "source-orders"),
          ],
          reader: mutableReader(
            new Map([
              ["source-orders-key", key],
              ["source-orders-value", value],
            ]),
            { compatibility: 0, versions: 0, schemas: 0 },
          ),
          monitorInterval: Duration.seconds(1),
        });

        expect(
          yield* runtime
            .validateRecord({
              viewServerTopic: "orders",
              sourceTopic: "source-orders",
              sides: ["key", "value"],
              key: Uint8Array.from([]),
              value: frame(99),
            })
            .pipe(Effect.flip),
        ).toStrictEqual({
          _tag: "KafkaSchemaRegistrySchemaMismatch",
          region: "eu",
          topic: "source-orders",
          subject: "source-orders-value",
          side: "value",
          schemaId: 99,
          message:
            'Schema ID 99 is not an active validated version of subject "source-orders-value".',
        });
      }),
    ),
  );

  it.effect("groups key and value contracts for one View Server Topic", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const key = mutableSubject("source-orders-key", 40);
        const value = mutableSubject("source-orders-value", 41);
        const calls: ReaderCalls = { compatibility: 0, versions: 0, schemas: 0 };
        const runtime = yield* makeKafkaSchemaRegistryRuntime({
          region: "eu",
          endpoint: "https://registry.eu.example.com",
          declarations: [
            declaration("orders", "source-orders", "key"),
            declaration("orders", "source-orders"),
          ],
          reader: mutableReader(
            new Map([
              ["source-orders-key", key],
              ["source-orders-value", value],
            ]),
            calls,
          ),
          monitorInterval: Duration.seconds(1),
        });

        yield* runtime.guard({
          viewServerTopic: "orders",
          sourceTopic: "source-orders",
          sides: ["key", "value"],
        });
        yield* TestClock.adjust(Duration.seconds(1));
        expect(
          yield* Effect.all([
            validateSide(runtime, {
              viewServerTopic: "orders",
              sourceTopic: "source-orders",
              side: "key",
              bytes: frame(40),
            }),
            validateSide(runtime, {
              viewServerTopic: "orders",
              sourceTopic: "source-orders",
              side: "value",
              bytes: frame(41),
            }),
          ]),
        ).toStrictEqual([Uint8Array.from([]), Uint8Array.from([])]);
      }),
    ),
  );

  it.effect("warms compatible IDs and isolates monitor failures to dependent lanes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const orders = mutableSubject("source-orders-value", 41);
        const inventory = mutableSubject("source-inventory-value", 51);
        const calls: ReaderCalls = { compatibility: 0, versions: 0, schemas: 0 };
        const runtime = yield* makeKafkaSchemaRegistryRuntime({
          region: "eu",
          endpoint: "https://registry.eu.example.com",
          declarations: [
            declaration("orders", "source-orders"),
            declaration("inventory", "source-inventory"),
          ],
          reader: mutableReader(
            new Map([
              ["source-orders-value", orders],
              ["source-inventory-value", inventory],
            ]),
            calls,
          ),
          monitorInterval: Duration.seconds(1),
        });
        const ordersBinding = {
          viewServerTopic: "orders",
          sourceTopic: "source-orders",
          sides: ["value"] as const,
        };
        const inventoryBinding = {
          viewServerTopic: "inventory",
          sourceTopic: "source-inventory",
          sides: ["value"] as const,
        };

        orders.active = [1, 2];
        orders.all = [1, 2];
        orders.schemas.set(2, schemaVersion("source-orders-value", 2, 42));
        yield* TestClock.adjust(Duration.seconds(1));
        const callsAfterWarm = { ...calls };
        yield* validateSide(runtime, {
          viewServerTopic: "orders",
          sourceTopic: "source-orders",
          side: "value",
          bytes: frame(42),
        });
        expect(calls).toStrictEqual(callsAfterWarm);

        const failureFiber = yield* runtime
          .failures(ordersBinding)
          .pipe(Stream.runDrain, Effect.flip, Effect.forkChild({ startImmediately: true }));
        const unrelatedFailureFiber = yield* runtime
          .failures(inventoryBinding)
          .pipe(Stream.runDrain, Effect.flip, Effect.forkChild({ startImmediately: true }));
        orders.compatibility = "BACKWARD";
        yield* TestClock.adjust(Duration.seconds(1));
        const failure = yield* Fiber.join(failureFiber);
        const guardedOrders = yield* runtime.guard(ordersBinding).pipe(Effect.flip);
        yield* runtime.guard(inventoryBinding);

        expect(failure).toStrictEqual({
          _tag: "KafkaSchemaRegistryPolicyMismatch",
          region: "eu",
          topic: "source-orders",
          subject: "source-orders-value",
          side: "value",
          schemaId: null,
          message:
            'Subject "source-orders-value" requires effective FULL_TRANSITIVE compatibility; observed "BACKWARD".',
        });
        expect(guardedOrders).toStrictEqual(failure);
        expect(unrelatedFailureFiber.pollUnsafe()).toBeUndefined();

        orders.compatibility = "FULL_TRANSITIVE";
        yield* TestClock.adjust(Duration.seconds(1));
        yield* runtime.guard(ordersBinding);
        expect(unrelatedFailureFiber.pollUnsafe()).toBeUndefined();
        expect(runtime.endpoints).toStrictEqual(["https://registry.eu.example.com"]);
      }),
    ),
  );

  it.effect("coalesces concurrent first-seen IDs and rejects IDs absent after refresh", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const orders = mutableSubject("source-orders-value", 41);
        const calls: ReaderCalls = { compatibility: 0, versions: 0, schemas: 0 };
        const runtime = yield* makeKafkaSchemaRegistryRuntime({
          region: "eu",
          endpoint: "https://registry.eu.example.com",
          declarations: [declaration("orders", "source-orders")],
          reader: mutableReader(new Map([["source-orders-value", orders]]), calls),
          monitorInterval: Duration.hours(1),
        });

        orders.active = [1, 2];
        orders.all = [1, 2];
        orders.schemas.set(2, schemaVersion("source-orders-value", 2, 42));
        const validate42 = validateSide(runtime, {
          viewServerTopic: "orders",
          sourceTopic: "source-orders",
          side: "value",
          bytes: frame(42),
        });
        yield* Effect.all([validate42, validate42], { concurrency: "unbounded" });
        expect(calls).toStrictEqual({ compatibility: 2, versions: 4, schemas: 3 });

        const missing = yield* validateSide(runtime, {
          viewServerTopic: "orders",
          sourceTopic: "source-orders",
          side: "value",
          bytes: frame(99),
        }).pipe(Effect.flip);
        expect(missing).toStrictEqual({
          _tag: "KafkaSchemaRegistrySchemaMismatch",
          region: "eu",
          topic: "source-orders",
          subject: "source-orders-value",
          side: "value",
          schemaId: 99,
          message:
            'Schema ID 99 is not an active validated version of subject "source-orders-value".',
        });

        const missingContract = yield* validateSide(runtime, {
          viewServerTopic: "unknown",
          sourceTopic: "source-unknown",
          side: "value",
          bytes: frame(41),
        }).pipe(Effect.flip);
        expect(missingContract).toStrictEqual({
          _tag: "KafkaSchemaRegistrySchemaMismatch",
          region: "eu",
          topic: "source-unknown",
          subject: "source-unknown-value",
          side: "value",
          schemaId: null,
          message:
            'Kafka Schema Registry contract for value subject "source-unknown-value" is unavailable.',
        });
      }),
    ),
  );

  it.effect("revalidates the complete key-value binding after refresh and guards tombstones", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const key = mutableSubject("source-orders-key", 40);
        const value = mutableSubject("source-orders-value", 41);
        const runtime = yield* makeKafkaSchemaRegistryRuntime({
          region: "eu",
          endpoint: "https://registry.eu.example.com",
          declarations: [
            declaration("orders", "source-orders", "key"),
            declaration("orders", "source-orders"),
          ],
          reader: mutableReader(
            new Map([
              ["source-orders-key", key],
              ["source-orders-value", value],
            ]),
            { compatibility: 0, versions: 0, schemas: 0 },
          ),
          monitorInterval: Duration.seconds(1),
        });

        key.active = [2];
        key.all = [1, 2];
        key.schemas.set(2, schemaVersion("source-orders-key", 2, 43));
        value.active = [1, 2];
        value.all = [1, 2];
        value.schemas.set(2, schemaVersion("source-orders-value", 2, 42));

        const keyFailure = yield* runtime
          .validateRecord({
            viewServerTopic: "orders",
            sourceTopic: "source-orders",
            sides: ["key", "value"],
            key: frame(40),
            value: frame(42),
          })
          .pipe(Effect.flip);
        expect(keyFailure).toStrictEqual({
          _tag: "KafkaSchemaRegistrySchemaMismatch",
          region: "eu",
          topic: "source-orders",
          subject: "source-orders-key",
          side: "key",
          schemaId: null,
          message: 'Subject "source-orders-key" version 1 is soft-deleted.',
        });

        key.active = [1, 2];
        value.compatibility = "BACKWARD";
        yield* TestClock.adjust(Duration.seconds(1));
        const tombstoneFailure = yield* runtime
          .validateRecord({
            viewServerTopic: "orders",
            sourceTopic: "source-orders",
            sides: ["key", "value"],
            key: frame(40),
            value: null,
          })
          .pipe(Effect.flip);
        expect(tombstoneFailure).toStrictEqual({
          _tag: "KafkaSchemaRegistryPolicyMismatch",
          region: "eu",
          topic: "source-orders",
          subject: "source-orders-value",
          side: "value",
          schemaId: null,
          message:
            'Subject "source-orders-value" requires effective FULL_TRANSITIVE compatibility; observed "BACKWARD".',
        });

        value.compatibility = "FULL_TRANSITIVE";
        yield* TestClock.adjust(Duration.seconds(1));
        expect(
          yield* runtime.validateRecord({
            viewServerTopic: "orders",
            sourceTopic: "source-orders",
            sides: ["key", "value"],
            key: frame(40),
            value: null,
          }),
        ).toStrictEqual({ key: Uint8Array.from([]), value: undefined });
      }),
    ),
  );

  it.effect("rejects a first-seen active ID that generated code cannot read", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const orders = mutableSubject("source-orders-value", 41);
        const calls: ReaderCalls = { compatibility: 0, versions: 0, schemas: 0 };
        const runtime = yield* makeKafkaSchemaRegistryRuntime({
          region: "eu",
          endpoint: "https://registry.eu.example.com",
          declarations: [declaration("orders", "source-orders")],
          reader: mutableReader(new Map([["source-orders-value", orders]]), calls),
          monitorInterval: Duration.hours(1),
        });

        orders.active = [1, 2];
        orders.all = [1, 2];
        orders.schemas.set(
          2,
          schemaVersion("source-orders-value", 2, 42, directionalBytesDescriptor()),
        );

        expect(
          yield* validateSide(runtime, {
            viewServerTopic: "orders",
            sourceTopic: "source-orders",
            side: "value",
            bytes: frame(42),
          }).pipe(Effect.flip),
        ).toStrictEqual({
          _tag: "KafkaSchemaRegistrySchemaMismatch",
          region: "eu",
          topic: "source-orders",
          subject: "source-orders-value",
          side: "value",
          schemaId: 42,
          message:
            'Subject "source-orders-value" version 2 is not decodable by generated message "viewserver.runtime.test.OrderValue": Buf WIRE rule FIELD_WIRE_COMPATIBLE_TYPE at viewserver.runtime.test.OrderValue.customer_id: Field type is not directionally wire-compatible.',
        });
      }),
    ),
  );

  it.effect("projects and recovers directional generated-code drift from the monitor", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const orders = mutableSubject("source-orders-value", 41);
        const calls: ReaderCalls = { compatibility: 0, versions: 0, schemas: 0 };
        const runtime = yield* makeKafkaSchemaRegistryRuntime({
          region: "eu",
          endpoint: "https://registry.eu.example.com",
          declarations: [declaration("orders", "source-orders")],
          reader: mutableReader(new Map([["source-orders-value", orders]]), calls),
          monitorInterval: Duration.seconds(1),
        });
        const binding = {
          viewServerTopic: "orders",
          sourceTopic: "source-orders",
          sides: ["value"] as const,
        };

        orders.active = [1, 2];
        orders.all = [1, 2];
        orders.schemas.set(
          2,
          schemaVersion("source-orders-value", 2, 42, directionalBytesDescriptor()),
        );
        yield* TestClock.adjust(Duration.seconds(1));
        expect(yield* runtime.guard(binding).pipe(Effect.flip)).toStrictEqual({
          _tag: "KafkaSchemaRegistrySchemaMismatch",
          region: "eu",
          topic: "source-orders",
          subject: "source-orders-value",
          side: "value",
          schemaId: 42,
          message:
            'Subject "source-orders-value" version 2 is not decodable by generated message "viewserver.runtime.test.OrderValue": Buf WIRE rule FIELD_WIRE_COMPATIBLE_TYPE at viewserver.runtime.test.OrderValue.customer_id: Field type is not directionally wire-compatible.',
        });

        orders.schemas.set(2, schemaVersion("source-orders-value", 2, 42));
        yield* TestClock.adjust(Duration.seconds(1));
        yield* runtime.guard(binding);
        expect(
          yield* validateSide(runtime, {
            viewServerTopic: "orders",
            sourceTopic: "source-orders",
            side: "value",
            bytes: frame(42),
          }),
        ).toStrictEqual(Uint8Array.from([]));
      }),
    ),
  );

  it.effect("drops stale overlapping refreshes and reports dependent refresh failures", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const orders = mutableSubject("source-orders-value", 41);
        const calls: ReaderCalls = { compatibility: 0, versions: 0, schemas: 0 };
        const baseReader = mutableReader(new Map([["source-orders-value", orders]]), calls);
        const refreshStarted = yield* Deferred.make<void>();
        const releaseRefresh = yield* Deferred.make<void>();
        let compatibilityReads = 0;
        const gatedReader: KafkaSchemaRegistryReader = {
          ...baseReader,
          effectiveCompatibility: (subject) =>
            Effect.gen(function* () {
              compatibilityReads += 1;
              if (compatibilityReads === 2) {
                yield* Deferred.succeed(refreshStarted, undefined);
                yield* Deferred.await(releaseRefresh);
              }
              return yield* baseReader.effectiveCompatibility(subject);
            }),
        };
        const runtime = yield* makeKafkaSchemaRegistryRuntime({
          region: "eu",
          endpoint: "https://registry.eu.example.com",
          declarations: [declaration("orders", "source-orders")],
          reader: gatedReader,
          monitorInterval: Duration.hours(1),
        });
        const validateMissing = (schemaId: number) =>
          validateSide(runtime, {
            viewServerTopic: "orders",
            sourceTopic: "source-orders",
            side: "value",
            bytes: frame(schemaId),
          }).pipe(Effect.flip);

        const firstFiber = yield* validateMissing(98).pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Deferred.await(refreshStarted);
        const secondFiber = yield* validateMissing(99).pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Effect.yieldNow;
        yield* Deferred.succeed(releaseRefresh, undefined);
        const failures = yield* Effect.all([Fiber.join(firstFiber), Fiber.join(secondFiber)]);

        expect(failures).toStrictEqual([
          {
            _tag: "KafkaSchemaRegistrySchemaMismatch",
            region: "eu",
            topic: "source-orders",
            subject: "source-orders-value",
            side: "value",
            schemaId: 98,
            message:
              'Schema ID 98 is not an active validated version of subject "source-orders-value".',
          },
          {
            _tag: "KafkaSchemaRegistrySchemaMismatch",
            region: "eu",
            topic: "source-orders",
            subject: "source-orders-value",
            side: "value",
            schemaId: 99,
            message:
              'Schema ID 99 is not an active validated version of subject "source-orders-value".',
          },
        ]);
        expect(compatibilityReads).toBe(3);
        expect(calls.compatibility).toBe(3);

        orders.compatibility = "BACKWARD";
        const policyFailure = yield* validateMissing(100);
        expect(policyFailure).toStrictEqual({
          _tag: "KafkaSchemaRegistryPolicyMismatch",
          region: "eu",
          topic: "source-orders",
          subject: "source-orders-value",
          side: "value",
          schemaId: null,
          message:
            'Subject "source-orders-value" requires effective FULL_TRANSITIVE compatibility; observed "BACKWARD".',
        });
      }),
    ),
  );

  it.effect("refreshes after a stale monitor snapshot misses a first-seen active ID", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const orders = mutableSubject("source-orders-value", 41);
        const calls: ReaderCalls = { compatibility: 0, versions: 0, schemas: 0 };
        const baseReader = mutableReader(new Map([["source-orders-value", orders]]), calls);
        const staleSnapshotCaptured = yield* Deferred.make<void>();
        const releaseStaleRefresh = yield* Deferred.make<void>();
        let captureNextOrdersSnapshot = false;
        let staleOrdersSnapshot:
          | {
              readonly active: ReadonlyArray<number>;
              readonly all: ReadonlyArray<number>;
            }
          | undefined;
        const reader: KafkaSchemaRegistryReader = {
          ...baseReader,
          versions: (subject, includeDeleted) =>
            Effect.gen(function* () {
              if (subject === "source-orders-value" && captureNextOrdersSnapshot) {
                if (!includeDeleted) {
                  staleOrdersSnapshot = {
                    active: [...orders.active],
                    all: [...orders.all],
                  };
                  return staleOrdersSnapshot.active;
                }
                if (staleOrdersSnapshot !== undefined) {
                  captureNextOrdersSnapshot = false;
                  yield* Deferred.succeed(staleSnapshotCaptured, undefined);
                  return staleOrdersSnapshot.all;
                }
              }
              return yield* baseReader.versions(subject, includeDeleted);
            }),
          schema: (subject, version) =>
            Effect.gen(function* () {
              if (
                subject === "source-orders-value" &&
                version === 1 &&
                staleOrdersSnapshot !== undefined
              ) {
                yield* Deferred.await(releaseStaleRefresh);
                staleOrdersSnapshot = undefined;
              }
              return yield* baseReader.schema(subject, version);
            }),
        };
        const runtime = yield* makeKafkaSchemaRegistryRuntime({
          region: "eu",
          endpoint: "https://registry.eu.example.com",
          declarations: [declaration("orders", "source-orders")],
          reader,
          monitorInterval: Duration.seconds(1),
        });
        captureNextOrdersSnapshot = true;
        const monitorFiber = yield* TestClock.adjust(Duration.seconds(1)).pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Deferred.await(staleSnapshotCaptured);

        orders.active = [1, 2];
        orders.all = [1, 2];
        orders.schemas.set(2, schemaVersion("source-orders-value", 2, 42));
        const validationFiber = yield* validateSide(runtime, {
          viewServerTopic: "orders",
          sourceTopic: "source-orders",
          side: "value",
          bytes: frame(42),
        }).pipe(Effect.forkChild({ startImmediately: true }));
        const coalescedValidationFiber = yield* validateSide(runtime, {
          viewServerTopic: "orders",
          sourceTopic: "source-orders",
          side: "value",
          bytes: frame(42),
        }).pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.succeed(releaseStaleRefresh, undefined);

        expect(yield* Fiber.join(validationFiber)).toStrictEqual(Uint8Array.from([]));
        expect(yield* Fiber.join(coalescedValidationFiber)).toStrictEqual(Uint8Array.from([]));
        yield* Fiber.join(monitorFiber);
        expect(calls.compatibility).toBe(3);
      }),
    ),
  );

  it.effect("projects unavailable and schema-drift monitor failures into validation guards", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const orders = mutableSubject("source-orders-value", 41);
        const subjects = new Map([["source-orders-value", orders]]);
        const calls: ReaderCalls = { compatibility: 0, versions: 0, schemas: 0 };
        const runtime = yield* makeKafkaSchemaRegistryRuntime({
          region: "eu",
          endpoint: "https://registry.eu.example.com",
          declarations: [declaration("orders", "source-orders")],
          reader: mutableReader(subjects, calls),
          monitorInterval: Duration.seconds(1),
        });
        const binding = {
          viewServerTopic: "orders",
          sourceTopic: "source-orders",
          sides: ["value"] as const,
        };

        subjects.delete("source-orders-value");
        yield* TestClock.adjust(Duration.seconds(1));
        const unavailable = yield* runtime.guard(binding).pipe(Effect.flip);
        expect(unavailable).toStrictEqual({
          _tag: "KafkaSchemaRegistryUnavailable",
          region: "eu",
          topic: "source-orders",
          subject: "source-orders-value",
          side: "value",
          schemaId: null,
          message:
            'Schema Registry request for subject "source-orders-value" failed: compatibility unavailable',
        });

        subjects.set("source-orders-value", orders);
        yield* TestClock.adjust(Duration.seconds(1));
        const original = orders.schemas.get(1);
        if (original === undefined) {
          throw new Error("schema fixture missing");
        }
        orders.schemas.set(1, { ...original, schemaType: "AVRO" });
        yield* TestClock.adjust(Duration.seconds(1));
        const mismatch = yield* validateSide(runtime, {
          viewServerTopic: "orders",
          sourceTopic: "source-orders",
          side: "value",
          bytes: frame(41),
        }).pipe(Effect.flip);
        expect(mismatch).toStrictEqual({
          _tag: "KafkaSchemaRegistrySchemaMismatch",
          region: "eu",
          topic: "source-orders",
          subject: "source-orders-value",
          side: "value",
          schemaId: 41,
          message: 'Subject "source-orders-value" version 1 is "AVRO", not PROTOBUF.',
        });

        orders.schemas.set(1, original);
        yield* TestClock.adjust(Duration.seconds(1));
        yield* runtime.guard(binding);
      }),
    ),
  );

  it.effect(
    "keeps an unrelated contract usable when first-seen refresh finds drift elsewhere",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const orders = mutableSubject("source-orders-value", 41);
          const inventory = mutableSubject("source-inventory-value", 51);
          const calls: ReaderCalls = { compatibility: 0, versions: 0, schemas: 0 };
          const runtime = yield* makeKafkaSchemaRegistryRuntime({
            region: "eu",
            endpoint: "https://registry.eu.example.com",
            declarations: [
              declaration("orders", "source-orders"),
              declaration("inventory", "source-inventory"),
            ],
            reader: mutableReader(
              new Map([
                ["source-orders-value", orders],
                ["source-inventory-value", inventory],
              ]),
              calls,
            ),
            monitorInterval: Duration.hours(1),
          });

          orders.active = [1, 2];
          orders.all = [1, 2];
          orders.schemas.set(2, schemaVersion("source-orders-value", 2, 42));
          inventory.compatibility = "BACKWARD";
          expect(
            yield* validateSide(runtime, {
              viewServerTopic: "orders",
              sourceTopic: "source-orders",
              side: "value",
              bytes: frame(42),
            }),
          ).toStrictEqual(Uint8Array.from([]));
          yield* runtime.guard({
            viewServerTopic: "orders",
            sourceTopic: "source-orders",
            sides: ["value"],
          });
          expect(
            yield* runtime
              .guard({
                viewServerTopic: "inventory",
                sourceTopic: "source-inventory",
                sides: ["value"],
              })
              .pipe(Effect.flip),
          ).toStrictEqual({
            _tag: "KafkaSchemaRegistryPolicyMismatch",
            region: "eu",
            topic: "source-inventory",
            subject: "source-inventory-value",
            side: "value",
            schemaId: null,
            message:
              'Subject "source-inventory-value" requires effective FULL_TRANSITIVE compatibility; observed "BACKWARD".',
          });
        }),
      ),
  );

  it.effect("surfaces an unexpected monitor defect to every dependent Source", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const orders = mutableSubject("source-orders-value", 41);
        const calls: ReaderCalls = { compatibility: 0, versions: 0, schemas: 0 };
        const baseReader = mutableReader(new Map([["source-orders-value", orders]]), calls);
        let compatibilityReads = 0;
        const reader: KafkaSchemaRegistryReader = {
          ...baseReader,
          effectiveCompatibility: (subject) => {
            compatibilityReads += 1;
            return compatibilityReads === 2
              ? Effect.die(new Error("injected monitor defect"))
              : baseReader.effectiveCompatibility(subject);
          },
        };
        const runtime = yield* makeKafkaSchemaRegistryRuntime({
          region: "eu",
          endpoint: "https://registry.eu.example.com",
          declarations: [declaration("orders", "source-orders")],
          reader,
          monitorInterval: Duration.seconds(1),
        });
        const binding = {
          viewServerTopic: "orders",
          sourceTopic: "source-orders",
          sides: ["value"] as const,
        };
        const failureFiber = yield* runtime
          .failures(binding)
          .pipe(Stream.runDrain, Effect.flip, Effect.forkChild({ startImmediately: true }));

        yield* TestClock.adjust(Duration.seconds(1));
        const failure = yield* Fiber.join(failureFiber);

        expect(failure).toStrictEqual({
          _tag: "KafkaSchemaRegistryUnavailable",
          region: "eu",
          topic: "source-orders",
          subject: "source-orders-value",
          side: "value",
          schemaId: null,
          message: "Kafka Schema Registry drift monitor terminated unexpectedly.",
        });
        expect(yield* runtime.guard(binding).pipe(Effect.flip)).toStrictEqual(failure);
      }),
    ),
  );

  it.effect("keeps terminal monitor failure after an overlapping first-seen refresh", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const orders = mutableSubject("source-orders-value", 41);
        const calls: ReaderCalls = { compatibility: 0, versions: 0, schemas: 0 };
        const baseReader = mutableReader(new Map([["source-orders-value", orders]]), calls);
        const monitorSleeping = yield* Deferred.make<void>();
        const terminateMonitor = yield* Deferred.make<void>();
        const firstSeenRefreshStarted = yield* Deferred.make<void>();
        const releaseFirstSeenRefresh = yield* Deferred.make<void>();
        let compatibilityReads = 0;
        const reader: KafkaSchemaRegistryReader = {
          ...baseReader,
          effectiveCompatibility: (subject) =>
            Effect.gen(function* () {
              compatibilityReads += 1;
              if (compatibilityReads === 2) {
                yield* Deferred.succeed(firstSeenRefreshStarted, undefined);
                yield* Deferred.await(releaseFirstSeenRefresh);
              }
              return yield* baseReader.effectiveCompatibility(subject);
            }),
        };
        const clock: Clock.Clock = {
          currentTimeMillisUnsafe: () => 0,
          currentTimeMillis: Effect.succeed(0),
          currentTimeNanosUnsafe: () => 0n,
          currentTimeNanos: Effect.succeed(0n),
          monotonicTimeNanosUnsafe: () => 0n,
          monotonicTimeNanos: Effect.succeed(0n),
          sleep: () =>
            Effect.gen(function* () {
              yield* Deferred.succeed(monitorSleeping, undefined);
              yield* Deferred.await(terminateMonitor);
              return yield* Effect.die(new Error("injected monitor sleep defect"));
            }),
        };
        const runtime = yield* makeKafkaSchemaRegistryRuntime({
          region: "eu",
          endpoint: "https://registry.eu.example.com",
          declarations: [declaration("orders", "source-orders")],
          reader,
          monitorInterval: Duration.seconds(1),
        }).pipe(Effect.provideService(Clock.Clock, clock));
        const binding = {
          viewServerTopic: "orders",
          sourceTopic: "source-orders",
          sides: ["value"] as const,
        };
        const failureFiber = yield* runtime
          .failures(binding)
          .pipe(Stream.runDrain, Effect.flip, Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(monitorSleeping);
        const validationFiber = yield* validateSide(runtime, {
          viewServerTopic: "orders",
          sourceTopic: "source-orders",
          side: "value",
          bytes: frame(99),
        }).pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(firstSeenRefreshStarted);
        yield* Deferred.succeed(terminateMonitor, undefined);
        yield* Effect.yieldNow;
        const queuedValidationFiber = yield* validateSide(runtime, {
          viewServerTopic: "orders",
          sourceTopic: "source-orders",
          side: "value",
          bytes: frame(98),
        }).pipe(Effect.flip, Effect.forkChild({ startImmediately: true }));
        yield* Effect.yieldNow;
        yield* Deferred.succeed(releaseFirstSeenRefresh, undefined);
        yield* Fiber.join(validationFiber);
        const failure = yield* Fiber.join(failureFiber);
        const queuedFailure = yield* Fiber.join(queuedValidationFiber);

        expect(failure).toStrictEqual({
          _tag: "KafkaSchemaRegistryUnavailable",
          region: "eu",
          topic: "source-orders",
          subject: "source-orders-value",
          side: "value",
          schemaId: null,
          message: "Kafka Schema Registry drift monitor terminated unexpectedly.",
        });
        expect(queuedFailure).toStrictEqual(failure);
        expect(yield* runtime.guard(binding).pipe(Effect.flip)).toStrictEqual(failure);
      }),
    ),
  );
});
