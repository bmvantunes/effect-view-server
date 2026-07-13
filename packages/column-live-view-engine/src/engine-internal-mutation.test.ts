import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, type StatusEvent } from "@effect-view-server/config";
import { Effect, Schema, Stream } from "effect";
import { createColumnLiveViewEngine, InvalidRowError } from "./index";
import { createColumnLiveViewEngineInternal } from "./internal";
import { expectSnapshotEvent, firstEvent, makeEventReader } from "../test-harness/events";
import { order, viewServer } from "../test-harness/public-engine";

describe("ColumnLiveViewEngine internal mutation", () => {
  it.effect("observes producer terminal occurrence before the terminal queue is ready", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({
        topics: viewServer.topics,
        subscriptionQueueCapacity: 1,
      });
      const phases: Array<string> = [];
      const observedStatuses: Array<StatusEvent> = [];
      const observer = {
        onQueryRegistered: (queryId: string) =>
          Effect.sync(() => {
            phases.push(`registered:${queryId}`);
          }),
        onTerminalOccurrence: (event: StatusEvent) =>
          Effect.sync(() => {
            phases.push("occurrence");
            observedStatuses.push(event);
          }),
        onTerminalReady: (event: StatusEvent) =>
          Effect.sync(() => {
            phases.push("ready");
            observedStatuses.push(event);
          }),
      };
      const subscription = yield* engine.subscribeObserved(
        "orders",
        {
          select: ["id"],
          limit: 10,
        },
        observer,
      );

      expect(phases).toStrictEqual(["registered:query-0"]);
      yield* engine.publish("orders", order("observed", "open", 10, 1));
      expect(phases).toStrictEqual(["registered:query-0", "occurrence", "ready"]);
      const events = yield* subscription.events.pipe(Stream.runCollect);
      const expectedStatus = {
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "closed",
        code: "BackpressureExceeded",
        message: "Subscription closed because its event queue exceeded capacity.",
      } as const;
      expect(Array.from(events)).toStrictEqual([expectedStatus]);
      expect(observedStatuses).toStrictEqual([expectedStatus, expectedStatus]);

      const runtimeQueryIds: Array<string> = [];
      const runtimeSubscription = yield* engine.subscribeRuntimeObserved(
        "orders",
        { select: ["id"] },
        {
          onQueryRegistered: (queryId) =>
            Effect.sync(() => {
              runtimeQueryIds.push(queryId);
            }),
          onTerminalOccurrence: () => Effect.void,
          onTerminalReady: () => Effect.void,
        },
      );
      expect(runtimeQueryIds).toStrictEqual(["query-1"]);
      yield* runtimeSubscription.close();
      yield* subscription.close();
      yield* engine.close();
    }),
  );

  it.effect("publishes decoded runtime rows through the internal engine entrypoint", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({
        topics: viewServer.topics,
      });

      yield* engine.publishManyDecodedRows("orders", [
        {
          customerId: "customer-decoded-1",
          id: "decoded-1",
          price: 42,
          region: "emea",
          status: "open",
          updatedAt: 1,
        },
      ]);
      const snapshot = yield* engine.snapshot("orders", {
        select: ["id", "price", "status"],
        limit: 10,
      });

      expect(snapshot).toStrictEqual({
        rows: [
          {
            id: "decoded-1",
            price: 42,
            status: "open",
          },
        ],
        status: "ready",
        statusCode: "Ready",
        totalRows: 1,
        version: 1,
      });
    }),
  );

  it.effect("patches decoded runtime fields through the internal engine entrypoint", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({
        topics: viewServer.topics,
      });

      yield* engine.publishManyDecodedRows("orders", [
        {
          customerId: "customer-decoded-1",
          id: "decoded-1",
          price: 42,
          region: "emea",
          status: "open",
          updatedAt: 1,
        },
      ]);
      yield* engine.patchDecodedFields("orders", "decoded-1", {
        price: 43,
        status: "closed",
      });
      const snapshot = yield* engine.snapshot("orders", {
        select: ["id", "price", "status"],
        limit: 10,
      });

      expect(snapshot).toStrictEqual({
        rows: [
          {
            id: "decoded-1",
            price: 43,
            status: "closed",
          },
        ],
        status: "ready",
        statusCode: "Ready",
        totalRows: 1,
        version: 2,
      });
    }),
  );

  it.effect("validates decoded runtime patches against the merged topic row schema", () =>
    Effect.gen(function* () {
      const PublicOrder = Schema.Struct({
        id: Schema.String.pipe(Schema.check(Schema.isPattern(/^public-/))),
        price: Schema.Number,
      });
      const publicViewServer = defineViewServerConfig({
        topics: {
          orders: {
            schema: PublicOrder,
            key: "id",
          },
        },
      });
      const engine = yield* createColumnLiveViewEngineInternal({
        topics: publicViewServer.topics,
      });

      yield* engine.publishManyDecodedRows("orders", [
        {
          id: "public-1",
          price: 42,
        },
      ]);
      const error = yield* Effect.flip(
        engine.patchDecodedFields("orders", "public-1", {
          id: "private-1",
        }),
      );
      const snapshot = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      });

      expect(error).toMatchObject({
        _tag: "InvalidRowError",
        topic: "orders",
      });
      expect(error).toBeInstanceOf(InvalidRowError);
      expect(snapshot).toStrictEqual({
        rows: [
          {
            id: "public-1",
            price: 42,
          },
        ],
        status: "ready",
        statusCode: "Ready",
        totalRows: 1,
        version: 1,
      });
    }),
  );

  it.effect("does not decode internal decoded runtime rows twice", () =>
    Effect.gen(function* () {
      const TransformId = Schema.StringFromUriComponent;
      const TransformOrder = Schema.Struct({
        id: TransformId,
        price: Schema.Number,
      });
      const transformViewServer = defineViewServerConfig({
        topics: {
          orders: {
            schema: TransformOrder,
            key: "id",
          },
        },
      });
      const engine = yield* createColumnLiveViewEngineInternal({
        topics: transformViewServer.topics,
      });

      yield* engine.publishManyDecodedRows("orders", [
        {
          id: "decoded%2D1",
          price: 42,
        },
      ]);
      const snapshot = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      });

      expect(snapshot).toStrictEqual({
        rows: [
          {
            id: "decoded%2D1",
            price: 42,
          },
        ],
        status: "ready",
        statusCode: "Ready",
        totalRows: 1,
        version: 1,
      });
    }),
  );

  it.effect("rejects encoded values at every decoded runtime mutation boundary", () =>
    Effect.gen(function* () {
      const DecodedOrder = Schema.Struct({
        id: Schema.String,
        amount: Schema.BigIntFromString,
      });
      const decodedViewServer = defineViewServerConfig({
        topics: {
          orders: {
            schema: DecodedOrder,
            key: "id",
          },
        },
      });
      const engine = yield* createColumnLiveViewEngineInternal({
        topics: decodedViewServer.topics,
      });
      yield* engine.publishManyDecodedRows("orders", [{ id: "stable", amount: 1n }]);

      const publishError = yield* Effect.flip(
        engine.publishManyDecodedRows("orders", [
          {
            id: "encoded-publish",
            // This internal source boundary is intentionally object-typed and must reject at runtime.
            amount: "42",
          },
        ]),
      );
      const storageKeyError = yield* Effect.flip(
        engine.publishManyDecodedRowsWithStorageKeys("orders", [
          {
            storageKey: "encoded-storage-key",
            row: {
              id: "encoded-storage",
              // This internal source boundary is intentionally object-typed and must reject at runtime.
              amount: "43",
            },
          },
        ]),
      );
      const patchError = yield* Effect.flip(
        engine.patchDecodedFields("orders", "stable", {
          // This internal source boundary is intentionally object-typed and must reject at runtime.
          amount: "44",
        }),
      );
      const snapshot = yield* engine.snapshot("orders", {
        select: ["id", "amount"],
        limit: 10,
      });

      expect(publishError).toBeInstanceOf(InvalidRowError);
      expect(storageKeyError).toBeInstanceOf(InvalidRowError);
      expect(patchError).toBeInstanceOf(InvalidRowError);
      expect({
        publishError,
        storageKeyError,
        patchError,
        snapshot,
      }).toStrictEqual({
        publishError: InvalidRowError.make({
          topic: "orders",
          message: 'SchemaError(Expected bigint, got "42"\n  at ["amount"])',
        }),
        storageKeyError: InvalidRowError.make({
          topic: "orders",
          message: 'SchemaError(Expected bigint, got "43"\n  at ["amount"])',
        }),
        patchError: InvalidRowError.make({
          topic: "orders",
          message: 'SchemaError(Expected bigint, got "44"\n  at ["amount"])',
        }),
        snapshot: {
          rows: [{ id: "stable", amount: 1n }],
          status: "ready",
          statusCode: "Ready",
          totalRows: 1,
          version: 1,
        },
      });
    }),
  );

  it.effect("fails decoded runtime rows when normalization throws", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({
        topics: viewServer.topics,
      });
      let accessorReads = 0;
      let descriptorReads = 0;
      const accessorRow = {};
      Object.defineProperty(accessorRow, "id", {
        enumerable: true,
        get: () => {
          accessorReads += 1;
          throw new Error("decoded row getter failed");
        },
      });
      const brokenRow = new Proxy(accessorRow, {
        getOwnPropertyDescriptor(target, property) {
          if (property === "id") {
            descriptorReads += 1;
          }
          return Reflect.getOwnPropertyDescriptor(target, property);
        },
      });

      const error = yield* Effect.flip(engine.publishManyDecodedRows("orders", [brokenRow]));

      expect(error).toMatchObject({
        _tag: "InvalidRowError",
        topic: "orders",
        message: "TypeError: Topic Row field id must be an own data property.",
      });
      expect(error).toBeInstanceOf(InvalidRowError);
      expect(accessorReads).toBe(0);
      expect(descriptorReads).toBe(1);
    }),
  );

  it.effect("publishes decoded runtime rows with internal storage keys", () =>
    Effect.gen(function* () {
      const PublicOrder = Schema.Struct({
        id: Schema.String.pipe(Schema.check(Schema.isPattern(/^public-/))),
        price: Schema.Number,
      });
      const publicViewServer = defineViewServerConfig({
        topics: {
          orders: {
            schema: PublicOrder,
            key: "id",
          },
        },
      });
      const engine = yield* createColumnLiveViewEngineInternal({
        topics: publicViewServer.topics,
      });

      yield* engine.publishManyDecodedRowsWithStorageKeys("orders", [
        {
          storageKey: "orders/lease/row/public-order-1",
          row: {
            id: "public-order-1",
            price: 42,
          },
        },
      ]);
      const subscription = yield* engine.subscribe("orders", {
        select: ["id", "price"],
        limit: 10,
      });
      const read = yield* makeEventReader(subscription);
      const event = firstEvent(yield* read(1));
      expectSnapshotEvent(event);

      expect(event.keys).toStrictEqual(["orders/lease/row/public-order-1"]);
      expect(event.rows).toStrictEqual([
        {
          id: "public-order-1",
          price: 42,
        },
      ]);
      yield* subscription.close();
    }),
  );

  it.effect(
    "validates storage-key rows against public row keys while storing by internal key",
    () =>
      Effect.gen(function* () {
        const PublicOrder = Schema.Struct({
          id: Schema.String.pipe(Schema.check(Schema.isPattern(/^public-/))),
          price: Schema.Number,
        });
        const publicViewServer = defineViewServerConfig({
          topics: {
            orders: {
              schema: PublicOrder,
              key: "id",
            },
          },
        });
        const engine = yield* createColumnLiveViewEngineInternal({
          topics: publicViewServer.topics,
        });

        yield* engine.publishManyWithStorageKeys("orders", [
          {
            storageKey: "orders/lease/row/public-order-1",
            row: {
              id: "public-order-1",
              price: 42,
            },
          },
        ]);
        const subscription = yield* engine.subscribe("orders", {
          select: ["id", "price"],
          limit: 10,
        });
        const read = yield* makeEventReader(subscription);
        const event = firstEvent(yield* read(1));
        expectSnapshotEvent(event);
        expect(event.keys).toStrictEqual(["orders/lease/row/public-order-1"]);
        expect(event.rows).toStrictEqual([
          {
            id: "public-order-1",
            price: 42,
          },
        ]);

        yield* engine.delete("orders", "orders/lease/row/public-order-1");
        const snapshot = yield* engine.snapshot("orders", {
          select: ["id", "price"],
          limit: 10,
        });

        expect(snapshot).toStrictEqual({
          rows: [],
          totalRows: 0,
          version: 2,
          status: "ready",
          statusCode: "Ready",
        });
        yield* subscription.close();
      }),
  );

  it.effect("does not expose internal decoded mutations from the public engine factory", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngine({
        topics: viewServer.topics,
      });

      expect({
        patchDecodedFields: "patchDecodedFields" in engine,
        publishManyDecodedRows: "publishManyDecodedRows" in engine,
        publishManyDecodedRowsWithStorageKeys: "publishManyDecodedRowsWithStorageKeys" in engine,
        publishManyWithStorageKeys: "publishManyWithStorageKeys" in engine,
      }).toStrictEqual({
        patchDecodedFields: false,
        publishManyDecodedRows: false,
        publishManyDecodedRowsWithStorageKeys: false,
        publishManyWithStorageKeys: false,
      });
    }),
  );
});
