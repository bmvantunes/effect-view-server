import { clone } from "@bufbuild/protobuf";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { describe, expect, it } from "@effect/vitest";
import { Deferred, Duration, Effect, Fiber, Stream } from "effect";
import { TestClock } from "effect/testing";
import {
  type KafkaSchemaRegistryDeclaration,
  type KafkaSchemaRegistryReader,
  type KafkaSchemaRegistrySchemaVersion,
} from "./schema-registry-contract";
import { makeKafkaSchemaRegistryRuntime } from "./schema-registry-runtime";
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

describe("Kafka Schema Registry Region runtime", () => {
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
            runtime.validate({
              viewServerTopic: "orders",
              sourceTopic: "source-orders",
              side: "key",
              bytes: frame(40),
            }),
            runtime.validate({
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
        yield* runtime.validate({
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
        const validate42 = runtime.validate({
          viewServerTopic: "orders",
          sourceTopic: "source-orders",
          side: "value",
          bytes: frame(42),
        });
        yield* Effect.all([validate42, validate42], { concurrency: "unbounded" });
        expect(calls).toStrictEqual({ compatibility: 2, versions: 4, schemas: 3 });

        const missing = yield* runtime
          .validate({
            viewServerTopic: "orders",
            sourceTopic: "source-orders",
            side: "value",
            bytes: frame(99),
          })
          .pipe(Effect.flip);
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

        const missingContract = yield* runtime
          .validate({
            viewServerTopic: "unknown",
            sourceTopic: "source-unknown",
            side: "value",
            bytes: frame(41),
          })
          .pipe(Effect.flip);
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
          yield* runtime
            .validate({
              viewServerTopic: "orders",
              sourceTopic: "source-orders",
              side: "value",
              bytes: frame(42),
            })
            .pipe(Effect.flip),
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
          yield* runtime.validate({
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
          runtime
            .validate({
              viewServerTopic: "orders",
              sourceTopic: "source-orders",
              side: "value",
              bytes: frame(schemaId),
            })
            .pipe(Effect.flip);

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
        expect(compatibilityReads).toBe(2);
        expect(calls.compatibility).toBe(2);

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
        const mismatch = yield* runtime
          .validate({
            viewServerTopic: "orders",
            sourceTopic: "source-orders",
            side: "value",
            bytes: frame(41),
          })
          .pipe(Effect.flip);
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
            yield* runtime.validate({
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
});
