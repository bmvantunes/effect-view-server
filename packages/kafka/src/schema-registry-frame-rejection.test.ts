import { describe, expect, it } from "@effect/vitest";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { makeViewServerRuntimeCore } from "@effect-view-server/runtime-core";
import { Deferred, Effect, Option, Schedule, Schema, Stream } from "effect";
import { kafka, type KafkaAdapterFailure } from "./contract";
import { parseKafkaSchemaRegistryProtobufFrame } from "./schema-registry-frame";
import type { KafkaSchemaRegistryRecordDecodeFailure } from "./schema-registry-runtime";
import { makeKafkaServerLayer, type KafkaServerRecord, type KafkaServerRegion } from "./server";
import {
  bytes,
  foreverBrokerContract,
  metadata,
  schemaRegistryRecordPayloads,
  valueFrame,
} from "./test-fixtures/schema-registry";
import { OrderValueSchema } from "./test-fixtures/orders_pb";

const Order = Schema.Struct({
  id: ViewServerId,
  price: Schema.Number,
  region: Schema.String,
});

describe("schema registry frame rejection", () => {
  it.effect("rejects a malformed frame and still applies the following valid record", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const settlements: Array<bigint> = [];
        let rejections = 0;
        const records: ReadonlyArray<KafkaServerRecord> = [
          {
            key: bytes("poison"),
            value: bytes("this is not a schema registry frame"),
            metadata: metadata("eu", 1n),
            settlement: () =>
              Effect.sync(() => {
                settlements.push(1n);
              }),
          },
          {
            key: bytes("order-2"),
            value: valueFrame(42, 2),
            metadata: metadata("eu", 2n),
            settlement: () =>
              Effect.sync(() => {
                settlements.push(2n);
              }),
          },
        ];
        const region: KafkaServerRegion = {
          acquire: () =>
            Effect.succeed({
              records: Stream.fromIterable(records).pipe(Stream.concat(Stream.never)),
              recordDecoded: Effect.void,
              recordDecodeFailure: Effect.void,
              recordMapped: Effect.void,
              recordMappingFailure: Effect.void,
              recordRejection: Effect.sync(() => {
                rejections += 1;
              }),
            }),
          metrics: () =>
            Effect.succeed({
              region: "eu",
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
            }),
        };
        const source = kafka.source(
          {
            cleanupPolicy: "delete",
            retentionPolicy: "Infinity",
            topic: "source-orders",
            regions: ["eu"],
            key: kafka.string(),
            value: kafka.schemaRegistry.protobuf(OrderValueSchema),
            localRowKey: ({ key }) => key,
            map: ({ value, region: sourceRegion }) => ({
              price: value.price,
              region: String(sourceRegion),
            }),
            startFrom: "earliest",
          },
          Schedule.recurs(0),
        );
        const config = defineViewServerConfig({
          topics: { orders: { schema: Order, source } },
        });
        const runtime = yield* Effect.acquireRelease(
          makeViewServerRuntimeCore(config, {}).pipe(
            Effect.provide(
              makeKafkaServerLayer({
                brokerContracts: [foreverBrokerContract("orders", "source-orders", "eu")],
                retentionSweepIntervalNanos: 900_000_000_000n,
                consumerGroupPrefix: "frame-rejection",
                regions: new Map([["eu", region]]),
                schemaRegistries: new Map([
                  [
                    "eu",
                    {
                      endpoints: ["https://registry.eu.example.com"],
                      retain: () => Effect.void,
                      guard: () => Effect.void,
                      failures: () => Stream.empty,
                      validateRecord: (input) => {
                        const value = input.value;
                        if (value !== null && value !== undefined) {
                          const frame = parseKafkaSchemaRegistryProtobufFrame(value);
                          if (frame._tag === "KafkaSchemaRegistryFrameParseFailure") {
                            return Effect.fail<KafkaSchemaRegistryRecordDecodeFailure>({
                              _tag: "KafkaSchemaRegistryRecordDecodeFailure",
                              side: "value",
                              failure: {
                                _tag: "KafkaDecodeFailure",
                                region: "eu",
                                topic: "source-orders",
                                message: frame.message,
                              },
                            });
                          }
                        }
                        return Effect.succeed(schemaRegistryRecordPayloads(input));
                      },
                    },
                  ],
                ]),
              }),
            ),
          ),
          (activeRuntime) => activeRuntime.close,
        );

        const diagnostics = yield* Effect.acquireRelease(
          runtime.liveClient.subscribeSourceHealth({ topic: "orders" }),
          (activeDiagnostics) => activeDiagnostics.close().pipe(Effect.ignore),
        );
        const subscription = yield* Effect.acquireRelease(
          runtime.liveClient.subscribe("orders", {
            select: ["id", "price"],
            limit: 10,
          }),
          (activeSubscription) => activeSubscription.close().pipe(Effect.ignore),
        );

        const rowApplied = subscription.events.pipe(
          Stream.filter(
            (event) =>
              (event.type === "snapshot" && event.rows.some((row) => row.id === "eu:0:order-2")) ||
              (event.type === "delta" &&
                event.operations.some(
                  (operation) => operation.type === "insert" && operation.key === "eu:0:order-2",
                )),
          ),
          Stream.take(1),
          Stream.runHead,
          Effect.as("row-applied" as const),
        );
        const health = Option.getOrThrow(
          yield* diagnostics.events.pipe(
            Stream.filter((value) => value.status._tag === "Degraded"),
            Stream.take(1),
            Stream.runHead,
            Effect.timeout("1 second"),
          ),
        );
        const degraded = Option.getOrThrow(
          Option.liftPredicate(health.status, (status) => status._tag === "Degraded"),
        );
        const rejection = Option.getOrThrow(
          Option.liftPredicate(
            degraded.reasons[0],
            (reason) => reason._tag === "SourceItemRejection",
          ),
        );
        const outcome = yield* rowApplied.pipe(Effect.timeout("1 second"));

        expect({
          outcome,
          rejections,
          settlements,
          latestRejection: rejection.latestRejection,
        }).toStrictEqual({
          outcome: "row-applied",
          rejections: 1,
          settlements: [1n, 2n],
          latestRejection: {
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "KafkaDecodeFailure",
                region: "eu",
                topic: "source-orders",
                message:
                  "Confluent Schema Registry Protobuf frame uses unsupported payload-prefix version 116.",
              },
            },
            location: {
              region: "eu",
              topic: "source-orders",
              partition: 0,
              offset: 1n,
              phase: "valueDecode",
              message:
                "Confluent Schema Registry Protobuf frame uses unsupported payload-prefix version 116.",
            },
            rejectedAtNanos: 0n,
          },
        });
      }),
    ),
  );

  it.effect("reports a malformed registry-backed key as a key decode rejection", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const settlements: Array<bigint> = [];
        const settled = yield* Deferred.make<void>();
        const malformedRecord: KafkaServerRecord = {
          key: bytes("malformed registry key"),
          value: valueFrame(42, 1),
          metadata: metadata("eu", 1n),
          settlement: () =>
            Effect.sync(() => {
              settlements.push(1n);
            }).pipe(Effect.andThen(Deferred.succeed(settled, undefined))),
        };
        const region: KafkaServerRegion = {
          acquire: () =>
            Effect.succeed({
              records: Stream.make(malformedRecord).pipe(Stream.concat(Stream.never)),
              recordDecoded: Effect.void,
              recordDecodeFailure: Effect.void,
              recordMapped: Effect.void,
              recordMappingFailure: Effect.void,
              recordRejection: Effect.void,
            }),
          metrics: () =>
            Effect.succeed({
              region: "eu",
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
            }),
        };
        const source = kafka.source(
          {
            cleanupPolicy: "delete",
            retentionPolicy: "Infinity",
            topic: "source-orders",
            regions: ["eu"],
            key: kafka.schemaRegistry.protobuf(OrderValueSchema),
            value: kafka.schemaRegistry.protobuf(OrderValueSchema),
            localRowKey: ({ key }) => key.customerId,
            map: ({ value, region: sourceRegion }) => ({
              price: value.price,
              region: String(sourceRegion),
            }),
            startFrom: "earliest",
          },
          Schedule.recurs(0),
        );
        const config = defineViewServerConfig({
          topics: { orders: { schema: Order, source } },
        });
        const runtime = yield* Effect.acquireRelease(
          makeViewServerRuntimeCore(config, {}).pipe(
            Effect.provide(
              makeKafkaServerLayer({
                brokerContracts: [foreverBrokerContract("orders", "source-orders", "eu")],
                retentionSweepIntervalNanos: 900_000_000_000n,
                consumerGroupPrefix: "key-frame-rejection",
                regions: new Map([["eu", region]]),
                schemaRegistries: new Map([
                  [
                    "eu",
                    {
                      endpoints: ["https://registry.eu.example.com"],
                      retain: () => Effect.void,
                      guard: () => Effect.void,
                      failures: () => Stream.empty,
                      validateRecord: (input) => {
                        const key = input.key;
                        if (key !== null && key !== undefined) {
                          const frame = parseKafkaSchemaRegistryProtobufFrame(key);
                          if (frame._tag === "KafkaSchemaRegistryFrameParseFailure") {
                            return Effect.fail<KafkaSchemaRegistryRecordDecodeFailure>({
                              _tag: "KafkaSchemaRegistryRecordDecodeFailure",
                              side: "key",
                              failure: {
                                _tag: "KafkaDecodeFailure",
                                region: "eu",
                                topic: "source-orders",
                                message: frame.message,
                              },
                            });
                          }
                        }
                        return Effect.succeed(schemaRegistryRecordPayloads(input));
                      },
                    },
                  ],
                ]),
              }),
            ),
          ),
          (activeRuntime) => activeRuntime.close,
        );
        const diagnostics = yield* Effect.acquireRelease(
          runtime.liveClient.subscribeSourceHealth({ topic: "orders" }),
          (activeDiagnostics) => activeDiagnostics.close().pipe(Effect.ignore),
        );
        const health = Option.getOrThrow(
          yield* diagnostics.events.pipe(
            Stream.filter((value) => value.status._tag === "Degraded"),
            Stream.take(1),
            Stream.runHead,
            Effect.timeout("1 second"),
          ),
        );
        const degraded = Option.getOrThrow(
          Option.liftPredicate(health.status, (status) => status._tag === "Degraded"),
        );
        const rejection = Option.getOrThrow(
          Option.liftPredicate(
            degraded.reasons[0],
            (reason) => reason._tag === "SourceItemRejection",
          ),
        );
        yield* Deferred.await(settled);

        expect({ phase: rejection.latestRejection.location.phase, settlements }).toStrictEqual({
          phase: "keyDecode",
          settlements: [1n],
        });
      }),
    ),
  );

  it.effect("keeps registry infrastructure failures at the Source Attempt level", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const settlements: Array<bigint> = [];
        let rejections = 0;
        const record: KafkaServerRecord = {
          key: bytes("order-1"),
          value: valueFrame(42, 1),
          metadata: metadata("eu", 1n),
          settlement: () =>
            Effect.sync(() => {
              settlements.push(1n);
            }),
        };
        const region: KafkaServerRegion = {
          acquire: () =>
            Effect.succeed({
              records: Stream.make(record).pipe(Stream.concat(Stream.never)),
              recordDecoded: Effect.void,
              recordDecodeFailure: Effect.void,
              recordMapped: Effect.void,
              recordMappingFailure: Effect.void,
              recordRejection: Effect.sync(() => {
                rejections += 1;
              }),
            }),
          metrics: () =>
            Effect.succeed({
              region: "eu",
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
            }),
        };
        const source = kafka.source(
          {
            cleanupPolicy: "delete",
            retentionPolicy: "Infinity",
            topic: "source-orders",
            regions: ["eu"],
            key: kafka.string(),
            value: kafka.schemaRegistry.protobuf(OrderValueSchema),
            localRowKey: ({ key }) => key,
            map: ({ value, region: sourceRegion }) => ({
              price: value.price,
              region: String(sourceRegion),
            }),
            startFrom: "earliest",
          },
          Schedule.recurs(0),
        );
        const config = defineViewServerConfig({
          topics: { orders: { schema: Order, source } },
        });
        const runtime = yield* Effect.acquireRelease(
          makeViewServerRuntimeCore(config, {}).pipe(
            Effect.provide(
              makeKafkaServerLayer({
                brokerContracts: [foreverBrokerContract("orders", "source-orders", "eu")],
                retentionSweepIntervalNanos: 900_000_000_000n,
                consumerGroupPrefix: "registry-infrastructure-failure",
                regions: new Map([["eu", region]]),
                schemaRegistries: new Map([
                  [
                    "eu",
                    {
                      endpoints: ["https://registry.eu.example.com"],
                      retain: () => Effect.void,
                      guard: () => Effect.void,
                      failures: () => Stream.empty,
                      validateRecord: () =>
                        Effect.fail<
                          Extract<
                            KafkaAdapterFailure,
                            { readonly _tag: "KafkaSchemaRegistryUnavailable" }
                          >
                        >({
                          _tag: "KafkaSchemaRegistryUnavailable",
                          region: "eu",
                          topic: "source-orders",
                          subject: "source-orders-value",
                          side: "value",
                          schemaId: null,
                          message: "Schema Registry is unavailable.",
                        }),
                    },
                  ],
                ]),
              }),
            ),
          ),
          (activeRuntime) => activeRuntime.close,
        );
        const diagnostics = yield* Effect.acquireRelease(
          runtime.liveClient.subscribeSourceHealth({ topic: "orders" }),
          (activeDiagnostics) => activeDiagnostics.close().pipe(Effect.ignore),
        );
        const health = Option.getOrThrow(
          yield* diagnostics.events.pipe(
            Stream.filter((value) => value.status._tag === "Exhausted"),
            Stream.take(1),
            Stream.runHead,
            Effect.timeout("1 second"),
          ),
        );

        expect({ status: health.status._tag, rejections, settlements }).toStrictEqual({
          status: "Exhausted",
          rejections: 0,
          settlements: [],
        });
      }),
    ),
  );
});
