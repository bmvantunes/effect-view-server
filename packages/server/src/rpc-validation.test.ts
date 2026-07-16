import { describe, expect, it } from "@effect/vitest";
import type { ViewServerLiveEvent } from "@effect-view-server/client";
import { makeViewServerClient } from "@effect-view-server/client/remote";
import { VIEW_SERVER_HEALTH_TOPIC } from "@effect-view-server/config";
import { ViewServerRpcErrorSchema } from "@effect-view-server/protocol";
import { Effect, Schema, Stream } from "effect";
import { makeViewServerWebSocketServer } from "./index";
import {
  createServerTestRuntime,
  edgeViewServer,
  makeRawRpcClient,
  safeEdgeViewServer,
  serverTestLiveClientWithSubscribe,
  viewServer,
} from "../test-harness/server";

describe("Real View Server RPC validation and typed errors", () => {
  it.live("preserves typed server errors for raw RPC clients", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });
      yield* Effect.addFinalizer(() => server.close);
      const raw = yield* makeRawRpcClient(server.url);
      yield* Effect.addFinalizer(() => raw.close);

      const unknownSubscribeTopic = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "missing",
          query: { select: ["id"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(unknownSubscribeTopic.code).toBe("InvalidTopic");

      const malformedHealthQuery = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: VIEW_SERVER_HEALTH_TOPIC,
          query: { select: ["rowCount"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(malformedHealthQuery.code).toBe("InvalidQuery");
      expect(malformedHealthQuery.message).toBe("Health query select must be exactly: id");

      const unknownSelect = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["missing"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(unknownSelect.code).toBe("InvalidQuery");

      const unknownWhere = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            where: { missing: { eq: "x" } },
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(unknownWhere.code).toBe("InvalidQuery");

      const unknownOrderBy = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            orderBy: [{ field: "missing", direction: "asc" }],
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(unknownOrderBy.code).toBe("InvalidQuery");

      const emptySelect = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: [] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(emptySelect.code).toBe("InvalidQuery");
      expect(emptySelect.message).toBe("Query select must include at least one field");

      const invalidOffset = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"], offset: -1 },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(invalidOffset.code).toBe("InvalidQuery");
      expect(invalidOffset.message).toBe("Query offset must be a non-negative integer");

      const invalidLimit = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"], limit: -1 },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(invalidLimit.code).toBe("InvalidQuery");
      expect(invalidLimit.message).toBe("Query limit must be a non-negative integer");

      const extraTopLevelQueryKey = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"], whre: { id: "a" } },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(extraTopLevelQueryKey.code).toBe("InvalidQuery");

      const extraOrderByKey = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            orderBy: [{ field: "id", direction: "asc", aggregate: "total" }],
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(extraOrderByKey.code).toBe("InvalidQuery");

      const invalidFilter = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            where: { price: { gt: "bad" } },
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(invalidFilter.code).toBe("InvalidQuery");

      const invalidStartsWith = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            where: { price: { startsWith: "1" } },
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(invalidStartsWith.code).toBe("InvalidQuery");

      const invalidNumericStartsWith = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            where: { price: { startsWith: 1 } },
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(invalidNumericStartsWith.code).toBe("InvalidQuery");
      expect(invalidNumericStartsWith.message).toBe("Filter price does not support startsWith");

      const unsupportedFilter = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            where: {
              id: {
                startsWith: "a",
                raw: "value",
              },
            },
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(unsupportedFilter.code).toBe("InvalidQuery");

      const richFilterEvents = yield* raw.rpc["ViewServer.Subscribe"]({
        topic: "orders",
        query: {
          select: ["id", "price"],
          where: {
            id: {
              in: ["a", "b"],
              startsWith: "a",
            },
            price: 10,
          },
          offset: 0,
        },
      }).pipe(Stream.take(1), Stream.runCollect);
      expect(richFilterEvents[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });

      yield* raw.close;
      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("rejects malformed live-client rows during server event encoding", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const makeServerForEvent = Effect.fn("ViewServerServer.test.malformedEventServer.make")(
        function* (event: ViewServerLiveEvent<object>) {
          const liveClient = serverTestLiveClientWithSubscribe(inMemory.liveClient, () =>
            Effect.succeed({
              events: Stream.make(event),
              close: () => Effect.void,
            }),
          );
          const server = yield* makeViewServerWebSocketServer(viewServer, {
            liveClient,
            runtime: inMemory.client,
          });
          yield* Effect.addFinalizer(() => server.close);
          const raw = yield* makeRawRpcClient(server.url);
          yield* Effect.addFinalizer(() => raw.close);
          return { raw, server };
        },
      );
      const makeServerForRow = (row: object) =>
        makeServerForEvent({
          type: "snapshot",
          topic: "orders",
          queryId: "malformed",
          version: 0,
          keys: ["bad"],
          rows: [row],
          totalRows: 1,
        });

      const invalidFieldType = yield* makeServerForRow({ id: 1 });
      const invalidFieldTypeError = yield* Effect.flip(
        invalidFieldType.raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(invalidFieldTypeError.code).toBe("InvalidRow");
      yield* invalidFieldType.raw.close;
      yield* invalidFieldType.server.close;

      const unknownField = yield* makeServerForRow({ id: "ok", missing: "bad" });
      const unknownFieldError = yield* Effect.flip(
        unknownField.raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(unknownFieldError.code).toBe("InvalidRow");
      expect(unknownFieldError.message).toBe("Unexpected row field for topic orders: missing");
      yield* unknownField.raw.close;
      yield* unknownField.server.close;

      const missingField = yield* makeServerForRow({ price: 10 });
      const missingFieldError = yield* Effect.flip(
        missingField.raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(missingFieldError.code).toBe("InvalidRow");
      expect(missingFieldError.message).toBe("Missing row field for topic orders: id");
      yield* missingField.raw.close;
      yield* missingField.server.close;

      const wrongTopic = yield* makeServerForEvent({
        type: "status",
        topic: "trades",
        queryId: "wrong-topic",
        status: "ready",
        code: "Ready",
      });
      const wrongTopicError = yield* Effect.flip(
        wrongTopic.raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(wrongTopicError.code).toBe("InvalidRow");
      expect(wrongTopicError.message).toBe("Received event for trades while subscribed to orders");
      yield* wrongTopic.raw.close;
      yield* wrongTopic.server.close;

      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("rejects non-json schema encodings during server event encoding", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(safeEdgeViewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const event: ViewServerLiveEvent<object> = {
        type: "snapshot",
        topic: "badjson",
        queryId: "badjson",
        version: 0,
        keys: ["bad"],
        rows: [{ id: "bad" }],
        totalRows: 1,
      };
      const liveClient = serverTestLiveClientWithSubscribe(inMemory.liveClient, () =>
        Effect.succeed({
          events: Stream.make(event),
          close: () => Effect.void,
        }),
      );
      const server = yield* makeViewServerWebSocketServer(edgeViewServer, {
        liveClient,
        runtime: inMemory.client,
      });
      yield* Effect.addFinalizer(() => server.close);
      const raw = yield* makeRawRpcClient(server.url);
      yield* Effect.addFinalizer(() => raw.close);

      const error = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "badjson",
          query: { select: ["id"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidRow",
        message: 'Field id is not JSON-safe: Unsupported JSON value type "symbol" at $.',
        topic: "badjson",
      });

      yield* raw.close;
      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("serves custom paths and maps hostile remote inputs", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const server = yield* makeViewServerWebSocketServer(
        viewServer,
        {
          liveClient: inMemory.liveClient,
          runtime: inMemory.client,
        },
        { host: "127.0.0.1", path: "/custom-rpc" },
      );
      yield* Effect.addFinalizer(() => server.close);
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      yield* Effect.addFinalizer(() => client.close);

      const invalidSubscribeTopic = yield* Effect.flip(
        // @ts-expect-error hostile callers can still send unknown topics over the wire.
        client.subscribe("missing", {
          select: ["id"],
        }),
      );
      expect(invalidSubscribeTopic.code).toBe("InvalidTopic");

      const invalidQuery = yield* Effect.flip(
        client.subscribe("orders", {
          // @ts-expect-error hostile callers can still send malformed queries over the wire.
          select: [1],
        }),
      );
      expect(invalidQuery.code).toBe("InvalidQuery");
      expect(invalidQuery.message).toBe('Expected string, got 1\n  at ["select"][0]');

      const unknownSelect = yield* Effect.flip(
        client.subscribe("orders", {
          // @ts-expect-error hostile callers can still send unknown projected fields.
          select: ["missing"],
        }),
      );
      expect(unknownSelect.code).toBe("InvalidQuery");
      expect(unknownSelect.message).toBe("Query references an unknown field for topic: orders");

      const unknownWhere = yield* Effect.flip(
        client.subscribe("orders", {
          // @ts-expect-error invalid query collapse keeps selected fields from being accepted.
          select: ["id"],
          where: {
            // @ts-expect-error hostile callers can still send unknown filter fields.
            missing: { eq: "x" },
          },
        }),
      );
      expect(unknownWhere.code).toBe("InvalidQuery");
      expect(unknownWhere.message).toBe("Query references an unknown field for topic: orders");

      const unknownOrderBy = yield* Effect.flip(
        client.subscribe("orders", {
          // @ts-expect-error invalid query collapse keeps selected fields from being accepted.
          select: ["id"],
          orderBy: [
            {
              // @ts-expect-error hostile callers can still send unknown sort fields.
              field: "missing",
              direction: "asc",
            },
          ],
        }),
      );
      expect(unknownOrderBy.code).toBe("InvalidQuery");
      expect(unknownOrderBy.message).toBe("Query references an unknown field for topic: orders");

      yield* client.close;
      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );
});
