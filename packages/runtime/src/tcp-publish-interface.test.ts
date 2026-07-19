import { describe, expect, it } from "@effect/vitest";
import { ViewServerAuthError } from "@effect-view-server/server";
import { defineViewServerConfig, kafka } from "@effect-view-server/config";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Deferred, Effect, Fiber, Option, Schema, Stream } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import type { ViewServerRuntimeDependencies } from "./internal";
import { makeDefaultRuntimeDependencies, makeViewServerRuntimeWithDependencies } from "./internal";
import { makeViewServerRuntime } from "./index";
import {
  makeDefaultGrpcRuntimeSourceDependencies,
  makeGrpcRuntimeSourceAdapter,
} from "./grpc-runtime-source";
import {
  makeDefaultKafkaRuntimeSourceDependencies,
  makeKafkaRuntimeSourceAdapter,
} from "./kafka-runtime-source";
import { makeViewServerTcpPublishIngress } from "./tcp-publish-ingress";
import {
  connectTcpPublishSocket,
  readTcpPublishResponse,
  sendTcpPublishCommand,
  sendTcpPublishLine,
} from "../test-harness/runtime";

import { bearerAuth, order, Order, Trade, viewServer } from "../test-harness/runtime-config";
import {
  defaultedTcpViewServer,
  jsonCodecTcpRecursiveViewServer,
  jsonCodecTcpViewServer,
  keyTransformTcpViewServer,
  nestedTcpViewServer,
  nonStringKeyTcpViewServer,
  positivePriceTcpViewServer,
  transformTcpViewServer,
  unionCodecTcpViewServer,
} from "../test-harness/tcp-publish";
import { grpcClients, GrpcOrder, grpcTopicSources } from "../test-harness/grpc-config";

describe("TCP publish Interface", () => {
  it.live("accepts TCP publish commands through the runtime mutation path", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpPublishUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);
      const subscription = yield* runtime.liveClient.subscribe("orders", {
        select: ["id", "price"],
        where: [{ field: "price", type: "greaterThanOrEqual", filter: 10 }],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const eventsFiber = yield* subscription.events.pipe(
        Stream.take(5),
        Stream.runCollect,
        Effect.forkChild,
      );

      const responses = [
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publish",
          topic: "orders",
          row: order("a", 10),
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "patch",
          topic: "orders",
          key: "a",
          patch: { price: 5 },
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publishMany",
          topic: "orders",
          rows: [order("b", 20), order("c", 30)],
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "delete",
          topic: "orders",
          key: "b",
        }),
      ];

      expect(responses).toStrictEqual([{ ok: true }, { ok: true }, { ok: true }, { ok: true }]);

      const events = yield* Fiber.join(eventsFiber);
      expect(events).toStrictEqual([
        {
          type: "snapshot",
          topic: "orders",
          queryId: "query-0",
          version: 0,
          keys: [],
          rows: [],
          totalRows: 0,
        },
        {
          type: "delta",
          topic: "orders",
          queryId: "query-0",
          fromVersion: 0,
          toVersion: 1,
          operations: [{ type: "insert", key: "a", row: { id: "a", price: 10 }, index: 0 }],
          totalRows: 1,
        },
        {
          type: "delta",
          topic: "orders",
          queryId: "query-0",
          fromVersion: 1,
          toVersion: 2,
          operations: [{ type: "remove", key: "a" }],
          totalRows: 0,
        },
        {
          type: "delta",
          topic: "orders",
          queryId: "query-0",
          fromVersion: 2,
          toVersion: 3,
          operations: [
            { type: "insert", key: "b", row: { id: "b", price: 20 }, index: 0 },
            { type: "insert", key: "c", row: { id: "c", price: 30 }, index: 1 },
          ],
          totalRows: 2,
        },
        {
          type: "delta",
          topic: "orders",
          queryId: "query-0",
          fromVersion: 3,
          toVersion: 4,
          operations: [{ type: "remove", key: "b" }],
          totalRows: 1,
        },
      ]);
      yield* subscription.close();
      yield* runtime.close;
    }),
  );

  it.live("passes TCP JSON rows to runtime core without double-decoding transform schemas", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(transformTcpViewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const responses = [
        yield* sendTcpPublishCommand(tcpUrl, {
          op: "publish",
          topic: "orders",
          row: { id: "a", quantity: "9007199254740993" },
        }),
        yield* sendTcpPublishCommand(tcpUrl, {
          op: "patch",
          topic: "orders",
          key: "a",
          patch: { quantity: "9007199254740995" },
        }),
        yield* sendTcpPublishCommand(tcpUrl, {
          op: "publishMany",
          topic: "orders",
          rows: [{ id: "b", quantity: "9007199254740997" }],
        }),
      ];
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "quantity"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(responses).toStrictEqual([{ ok: true }, { ok: true }, { ok: true }]);
      expect(snapshot).toStrictEqual({
        rows: [
          { id: "a", quantity: 9007199254740995n },
          { id: "b", quantity: 9007199254740997n },
        ],
        totalRows: 2,
        version: 3,
        status: "ready",
        statusCode: "Ready",
      });
      yield* runtime.close;
    }),
  );

  it.live("decodes TCP command keys before patching and deleting rows", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(keyTransformTcpViewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const publishResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "publish",
        topic: "orders",
        row: { id: "%61", price: 10 },
      });
      const patchResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "%61",
        patch: { price: 20 },
      });
      const patchedSnapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      });
      const deleteResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "delete",
        topic: "orders",
        key: "%61",
      });
      const deletedSnapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      });

      expect(publishResponse).toStrictEqual({ ok: true });
      expect(patchResponse).toStrictEqual({ ok: true });
      expect(patchedSnapshot).toStrictEqual({
        rows: [{ id: "a", price: 20 }],
        totalRows: 1,
        version: 2,
        status: "ready",
        statusCode: "Ready",
      });
      expect(deleteResponse).toStrictEqual({ ok: true });
      expect(deletedSnapshot).toStrictEqual({
        rows: [],
        totalRows: 0,
        version: 3,
        status: "ready",
        statusCode: "Ready",
      });
      yield* runtime.close;
    }),
  );

  it.live("rejects TCP command keys that decode to non-string runtime keys", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(nonStringKeyTcpViewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const patchResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "1",
        patch: { price: 20 },
      });
      const deleteResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "delete",
        topic: "orders",
        key: "1",
      });

      expect([patchResponse, deleteResponse]).toStrictEqual([
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish key did not match View Server topic orders.",
            phase: "decode",
            topic: "orders",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish key did not match View Server topic orders.",
            phase: "decode",
            topic: "orders",
          },
        },
      ]);
      yield* runtime.close;
    }),
  );

  it.live("decodes TCP union JSON codec fields through the matching member", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(unionCodecTcpViewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const responses = [
        yield* sendTcpPublishCommand(tcpUrl, {
          op: "publish",
          topic: "orders",
          row: { id: "a", quantity: "9007199254740993" },
        }),
        yield* sendTcpPublishCommand(tcpUrl, {
          op: "patch",
          topic: "orders",
          key: "a",
          patch: { quantity: "9007199254740995" },
        }),
      ];
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "quantity"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(responses).toStrictEqual([{ ok: true }, { ok: true }]);
      expect(snapshot).toStrictEqual({
        rows: [{ id: "a", quantity: 9007199254740995n }],
        totalRows: 1,
        version: 2,
        status: "ready",
        statusCode: "Ready",
      });
      yield* runtime.close;
    }),
  );

  it.live("preserves TCP publish rows materialized by the topic schema decoder", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(defaultedTcpViewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const response = yield* sendTcpPublishCommand(tcpUrl, {
        op: "publish",
        topic: "orders",
        row: { id: "a", price: 12 },
      });
      const publishManyResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "publishMany",
        topic: "orders",
        rows: [
          { id: "b", price: 24 },
          { id: "c", price: 36 },
        ],
      });
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "price", "status"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(response).toStrictEqual({ ok: true });
      expect(publishManyResponse).toStrictEqual({ ok: true });
      expect(snapshot).toStrictEqual({
        rows: [
          { id: "a", price: 12, status: "open" },
          { id: "b", price: 24, status: "open" },
          { id: "c", price: 36, status: "open" },
        ],
        totalRows: 3,
        version: 2,
        status: "ready",
        statusCode: "Ready",
      });
      yield* runtime.close;
    }),
  );

  it.live("decodes TCP rows and patches through topic JSON codecs before publishing", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(jsonCodecTcpViewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const responses = [
        yield* sendTcpPublishCommand(tcpUrl, {
          op: "publish",
          topic: "orders",
          row: {
            allocations: {
              ["__proto__"]: {
                encodedQuantity: "2002",
                runtimeAmount: "22.25",
                runtimeQuantity: "21002",
              },
              primary: {
                encodedQuantity: "2001",
                runtimeAmount: "21.25",
                runtimeQuantity: "21001",
              },
            },
            fills: [
              {
                encodedQuantity: "3001",
                runtimeAmount: "31.25",
                runtimeQuantity: "31001",
              },
            ],
            id: "a",
            amount: "123.45",
            meta: {
              encodedQuantity: "1001",
              runtimeAmount: "11.25",
              runtimeQuantity: "11001",
            },
            nullableMeta: {
              encodedQuantity: "7001",
              runtimeAmount: "71.25",
            },
            optionalMeta: {
              encodedQuantity: "8001",
              runtimeAmount: "81.25",
            },
            optionalValueMeta: {
              encodedQuantity: "9001",
              runtimeAmount: "91.25",
            },
            checkedSuspendedEmptyMeta: {},
            quantity: "9007199254740993",
            suspendedMeta: {
              encodedQuantity: "11001",
              runtimeAmount: "111.25",
              runtimeQuantity: "111001",
            },
            tuple: [
              {
                encodedQuantity: "4001",
                runtimeAmount: "41.25",
                runtimeQuantity: "41001",
              },
            ],
            tupleRest: [
              {
                encodedQuantity: "5001",
                runtimeAmount: "51.25",
                runtimeQuantity: "51001",
              },
              {
                encodedQuantity: "5002",
                runtimeAmount: "52.25",
                runtimeQuantity: "51002",
              },
            ],
            tupleRestTrailing: [
              {
                encodedQuantity: "6001",
                runtimeAmount: "61.25",
                runtimeQuantity: "61001",
              },
              {
                encodedQuantity: "6002",
                runtimeAmount: "62.25",
                runtimeQuantity: "61002",
              },
              {
                encodedQuantity: "6003",
                runtimeAmount: "63.25",
                runtimeQuantity: "61003",
              },
            ],
            unionMeta: {
              encodedQuantity: "10001",
              runtimeAmount: "101.25",
              runtimeQuantity: "101001",
            },
          },
        }),
        yield* sendTcpPublishCommand(tcpUrl, {
          op: "patch",
          topic: "orders",
          key: "a",
          patch: {
            allocations: {
              ["__proto__"]: {
                encodedQuantity: "2004",
                runtimeAmount: "24.25",
                runtimeQuantity: "21004",
              },
              primary: {
                encodedQuantity: "2003",
                runtimeAmount: "23.25",
                runtimeQuantity: "21003",
              },
            },
            amount: "678.90",
            fills: [
              {
                encodedQuantity: "3003",
                runtimeAmount: "33.25",
                runtimeQuantity: "31003",
              },
            ],
            meta: {
              encodedQuantity: "1003",
              runtimeAmount: "33.75",
              runtimeQuantity: "11003",
            },
            nullableMeta: {
              encodedQuantity: "7003",
              runtimeAmount: "73.25",
            },
            optionalMeta: {
              encodedQuantity: "8003",
              runtimeAmount: "83.25",
            },
            optionalValueMeta: {
              encodedQuantity: "9003",
              runtimeAmount: "93.25",
            },
            quantity: "9007199254740995",
            suspendedMeta: {
              encodedQuantity: "11003",
              runtimeAmount: "113.25",
              runtimeQuantity: "111003",
            },
            tuple: [
              {
                encodedQuantity: "4003",
                runtimeAmount: "43.25",
                runtimeQuantity: "41003",
              },
            ],
            tupleRest: [
              {
                encodedQuantity: "5003",
                runtimeAmount: "53.25",
                runtimeQuantity: "51003",
              },
              {
                encodedQuantity: "5004",
                runtimeAmount: "54.25",
                runtimeQuantity: "51004",
              },
            ],
            tupleRestTrailing: [
              {
                encodedQuantity: "6003",
                runtimeAmount: "63.25",
                runtimeQuantity: "61003",
              },
              {
                encodedQuantity: "6004",
                runtimeAmount: "64.25",
                runtimeQuantity: "61004",
              },
              {
                encodedQuantity: "6005",
                runtimeAmount: "65.25",
                runtimeQuantity: "61005",
              },
            ],
            unionMeta: {
              encodedQuantity: "10003",
              runtimeAmount: "103.25",
              runtimeQuantity: "101003",
            },
          },
        }),
        yield* sendTcpPublishCommand(tcpUrl, {
          op: "publishMany",
          topic: "orders",
          rows: [
            {
              allocations: {
                primary: {
                  encodedQuantity: "2005",
                  runtimeAmount: "25.25",
                },
              },
              fills: [
                {
                  encodedQuantity: "3005",
                  runtimeAmount: "35.25",
                },
              ],
              id: "b",
              amount: "42.25",
              meta: {
                encodedQuantity: "1005",
                runtimeAmount: "55.50",
                runtimeQuantity: "11005",
              },
              nullableMeta: null,
              optionalMeta: {
                encodedQuantity: "8005",
                runtimeAmount: "85.25",
              },
              optionalValueMeta: {
                encodedQuantity: "9005",
                runtimeAmount: "95.25",
              },
              quantity: "9007199254740997",
              suspendedMeta: {
                encodedQuantity: "11005",
                runtimeAmount: "115.25",
              },
              tuple: [
                {
                  encodedQuantity: "4005",
                  runtimeAmount: "45.25",
                },
              ],
              tupleRest: [
                {
                  encodedQuantity: "5005",
                  runtimeAmount: "55.25",
                },
                {
                  encodedQuantity: "5006",
                  runtimeAmount: "56.25",
                },
              ],
              tupleRestTrailing: [
                {
                  encodedQuantity: "6005",
                  runtimeAmount: "65.25",
                },
                {
                  encodedQuantity: "6006",
                  runtimeAmount: "66.25",
                },
                {
                  encodedQuantity: "6007",
                  runtimeAmount: "67.25",
                },
              ],
              unionMeta: {
                encodedQuantity: "10005",
                runtimeAmount: "105.25",
              },
            },
          ],
        }),
      ];
      const invalidNestedResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          meta: {
            encodedQuantity: "1007",
          },
        },
      });
      const invalidNullNestedResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          meta: null,
        },
      });
      const invalidArrayResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          fills: [],
        },
      });
      const invalidRecordResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          allocations: {},
        },
      });
      const invalidTupleResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          tuple: [],
        },
      });
      const invalidTupleExtraResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          tuple: [
            {
              encodedQuantity: "4007",
              runtimeAmount: "47.25",
              runtimeQuantity: "41007",
            },
            {
              encodedQuantity: "4008",
              runtimeAmount: "48.25",
              runtimeQuantity: "41008",
            },
          ],
        },
      });
      const invalidTupleRestResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          tupleRest: [],
        },
      });
      const invalidOptionalWrapperResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          optionalMeta: {
            encodedQuantity: "8007",
          },
        },
      });
      const invalidNullableWrapperResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          nullableMeta: {
            encodedQuantity: "7007",
          },
        },
      });
      const invalidUnionWrapperResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          unionMeta: {
            encodedQuantity: "10007",
          },
        },
      });
      const invalidCheckedWrapperResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          checkedOptionalMeta: {
            encodedQuantity: "11007",
            runtimeAmount: "111.25",
          },
        },
      });
      const invalidSuspendedWrapperResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          suspendedMeta: {
            encodedQuantity: "11009",
          },
        },
      });
      const invalidCheckedSuspendedWrapperResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          checkedSuspendedMeta: {
            encodedQuantity: "12009",
            runtimeAmount: "129.25",
            runtimeQuantity: "121009",
          },
        },
      });
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: [
          "allocations",
          "amount",
          "fills",
          "id",
          "meta",
          "nullableMeta",
          "optionalMeta",
          "optionalValueMeta",
          "quantity",
          "suspendedMeta",
          "tuple",
          "tupleRest",
          "tupleRestTrailing",
          "unionMeta",
        ],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(responses).toStrictEqual([{ ok: true }, { ok: true }, { ok: true }]);
      expect(invalidNestedResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidNullNestedResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidArrayResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidRecordResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidTupleResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidTupleExtraResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidTupleRestResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidOptionalWrapperResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidNullableWrapperResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidUnionWrapperResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidCheckedWrapperResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidSuspendedWrapperResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidCheckedSuspendedWrapperResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(snapshot).toStrictEqual({
        rows: [
          {
            allocations: {
              ["__proto__"]: {
                encodedQuantity: 2004n,
                runtimeAmount: BigDecimal.fromStringUnsafe("24.25"),
                runtimeQuantity: 21004n,
              },
              primary: {
                encodedQuantity: 2003n,
                runtimeAmount: BigDecimal.fromStringUnsafe("23.25"),
                runtimeQuantity: 21003n,
              },
            },
            id: "a",
            amount: BigDecimal.fromStringUnsafe("678.9"),
            fills: [
              {
                encodedQuantity: 3003n,
                runtimeAmount: BigDecimal.fromStringUnsafe("33.25"),
                runtimeQuantity: 31003n,
              },
            ],
            meta: {
              encodedQuantity: 1003n,
              runtimeAmount: BigDecimal.fromStringUnsafe("33.75"),
              runtimeQuantity: 11003n,
            },
            nullableMeta: {
              encodedQuantity: 7003n,
              runtimeAmount: BigDecimal.fromStringUnsafe("73.25"),
            },
            optionalMeta: {
              encodedQuantity: 8003n,
              runtimeAmount: BigDecimal.fromStringUnsafe("83.25"),
            },
            optionalValueMeta: {
              encodedQuantity: 9003n,
              runtimeAmount: BigDecimal.fromStringUnsafe("93.25"),
            },
            quantity: 9007199254740995n,
            suspendedMeta: {
              encodedQuantity: 11003n,
              runtimeAmount: BigDecimal.fromStringUnsafe("113.25"),
              runtimeQuantity: 111003n,
            },
            tuple: [
              {
                encodedQuantity: 4003n,
                runtimeAmount: BigDecimal.fromStringUnsafe("43.25"),
                runtimeQuantity: 41003n,
              },
            ],
            tupleRest: [
              {
                encodedQuantity: 5003n,
                runtimeAmount: BigDecimal.fromStringUnsafe("53.25"),
                runtimeQuantity: 51003n,
              },
              {
                encodedQuantity: 5004n,
                runtimeAmount: BigDecimal.fromStringUnsafe("54.25"),
                runtimeQuantity: 51004n,
              },
            ],
            tupleRestTrailing: [
              {
                encodedQuantity: 6003n,
                runtimeAmount: BigDecimal.fromStringUnsafe("63.25"),
                runtimeQuantity: 61003n,
              },
              {
                encodedQuantity: 6004n,
                runtimeAmount: BigDecimal.fromStringUnsafe("64.25"),
                runtimeQuantity: 61004n,
              },
              {
                encodedQuantity: 6005n,
                runtimeAmount: BigDecimal.fromStringUnsafe("65.25"),
                runtimeQuantity: 61005n,
              },
            ],
            unionMeta: {
              encodedQuantity: 10003n,
              runtimeAmount: BigDecimal.fromStringUnsafe("103.25"),
              runtimeQuantity: 101003n,
            },
          },
          {
            allocations: {
              primary: {
                encodedQuantity: 2005n,
                runtimeAmount: BigDecimal.fromStringUnsafe("25.25"),
              },
            },
            id: "b",
            amount: BigDecimal.fromStringUnsafe("42.25"),
            fills: [
              {
                encodedQuantity: 3005n,
                runtimeAmount: BigDecimal.fromStringUnsafe("35.25"),
              },
            ],
            meta: {
              encodedQuantity: 1005n,
              runtimeAmount: BigDecimal.fromStringUnsafe("55.5"),
              runtimeQuantity: 11005n,
            },
            nullableMeta: null,
            optionalMeta: {
              encodedQuantity: 8005n,
              runtimeAmount: BigDecimal.fromStringUnsafe("85.25"),
            },
            optionalValueMeta: {
              encodedQuantity: 9005n,
              runtimeAmount: BigDecimal.fromStringUnsafe("95.25"),
            },
            quantity: 9007199254740997n,
            suspendedMeta: {
              encodedQuantity: 11005n,
              runtimeAmount: BigDecimal.fromStringUnsafe("115.25"),
            },
            tuple: [
              {
                encodedQuantity: 4005n,
                runtimeAmount: BigDecimal.fromStringUnsafe("45.25"),
              },
            ],
            tupleRest: [
              {
                encodedQuantity: 5005n,
                runtimeAmount: BigDecimal.fromStringUnsafe("55.25"),
              },
              {
                encodedQuantity: 5006n,
                runtimeAmount: BigDecimal.fromStringUnsafe("56.25"),
              },
            ],
            tupleRestTrailing: [
              {
                encodedQuantity: 6005n,
                runtimeAmount: BigDecimal.fromStringUnsafe("65.25"),
              },
              {
                encodedQuantity: 6006n,
                runtimeAmount: BigDecimal.fromStringUnsafe("66.25"),
              },
              {
                encodedQuantity: 6007n,
                runtimeAmount: BigDecimal.fromStringUnsafe("67.25"),
              },
            ],
            unionMeta: {
              encodedQuantity: 10005n,
              runtimeAmount: BigDecimal.fromStringUnsafe("105.25"),
            },
          },
        ],
        totalRows: 2,
        version: 3,
        status: "ready",
        statusCode: "Ready",
      });
      yield* runtime.close;
    }),
  );

  it.live("decodes recursive suspended TCP rows through topic JSON codecs", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(jsonCodecTcpRecursiveViewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const response = yield* sendTcpPublishCommand(tcpUrl, {
        op: "publish",
        topic: "orders",
        row: {
          id: "recursive",
          node: {
            id: "1",
            amount: "10.25",
            runtimeQuantity: "9007199254740993",
            child: {
              id: "2",
              amount: "20.25",
              runtimeQuantity: "9007199254740995",
              child: {
                id: "3",
                amount: "30.25",
                runtimeQuantity: "9007199254740997",
                child: null,
              },
            },
          },
        },
      });
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "node"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(response).toStrictEqual({ ok: true });
      expect(snapshot).toStrictEqual({
        rows: [
          {
            id: "recursive",
            node: {
              id: 1n,
              amount: BigDecimal.fromStringUnsafe("10.25"),
              runtimeQuantity: 9007199254740993n,
              child: {
                id: 2n,
                amount: BigDecimal.fromStringUnsafe("20.25"),
                runtimeQuantity: 9007199254740995n,
                child: {
                  id: 3n,
                  amount: BigDecimal.fromStringUnsafe("30.25"),
                  runtimeQuantity: 9007199254740997n,
                  child: null,
                },
              },
            },
          },
        ],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      yield* runtime.close;
    }),
  );

  it.live("returns a usable bracketed TCP publish URL for IPv6 hosts", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        tcpPublishHost: "::1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);
      const response = yield* sendTcpPublishCommand(tcpUrl, {
        op: "publish",
        topic: "orders",
        row: order("ipv6", 42),
      });
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      });

      expect(tcpUrl.startsWith("tcp://[")).toBe(true);
      expect(tcpUrl.includes("]:")).toBe(true);
      expect(response).toStrictEqual({ ok: true });
      expect(snapshot).toStrictEqual({
        rows: [{ id: "ipv6", price: 42 }],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      yield* runtime.close;
    }),
  );

  it.live("rejects invalid TCP publish batches without partial mutation", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpPublishUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const response = yield* sendTcpPublishCommand(tcpPublishUrl, {
        op: "publishMany",
        topic: "orders",
        rows: [order("valid", 10), { id: "invalid", price: "not-a-number" }],
      });
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(response).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish row did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(snapshot).toStrictEqual({
        rows: [],
        totalRows: 0,
        version: 0,
        status: "ready",
        statusCode: "Ready",
      });
      yield* runtime.close;
    }),
  );

  it.live("rejects TCP publish rows that do not match the target topic schema", () =>
    Effect.gen(function* () {
      const schemaSafetyViewServer = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            key: "id",
          },
          trades: {
            schema: Trade,
            key: "id",
          },
        },
      });
      const runtime = yield* makeViewServerRuntime(schemaSafetyViewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpPublishUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const tradeShapedOrderResponse = yield* sendTcpPublishCommand(tcpPublishUrl, {
        op: "publish",
        topic: "orders",
        row: {
          id: "trade-1",
          symbol: "AAPL",
        },
      });
      const extraFieldOrderResponse = yield* sendTcpPublishCommand(tcpPublishUrl, {
        op: "publish",
        topic: "orders",
        row: {
          id: "order-1",
          price: 10,
          symbol: "AAPL",
        },
      });
      const missingRequiredOrderResponse = yield* sendTcpPublishCommand(tcpPublishUrl, {
        op: "publish",
        topic: "orders",
        row: {
          id: "order-2",
        },
      });
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(tradeShapedOrderResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish row did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(extraFieldOrderResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish row did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(missingRequiredOrderResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish row did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(snapshot).toStrictEqual({
        rows: [],
        totalRows: 0,
        version: 0,
        status: "ready",
        statusCode: "Ready",
      });
      yield* runtime.close;
    }),
  );

  it.live("preserves runtime error codes in TCP publish responses", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const response = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "missing",
        patch: { price: 11 },
      });

      expect(response).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidRow",
          message: "Cannot patch missing key: missing",
          topic: "orders",
        },
      });
      yield* runtime.close;
    }),
  );

  it.live("rejects TCP decoded patches that violate the merged row schema", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(positivePriceTcpViewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const publishResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "publish",
        topic: "orders",
        row: {
          id: "order-1",
          price: 10,
        },
      });
      const patchResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "order-1",
        patch: { price: -1 },
      });
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      });

      expect(publishResponse).toStrictEqual({ ok: true });
      expect(patchResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidRow",
          message: expect.any(String),
          topic: "orders",
        },
      });
      expect(snapshot).toStrictEqual({
        rows: [
          {
            id: "order-1",
            price: 10,
          },
        ],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      yield* runtime.close;
    }),
  );

  it.live("returns typed TCP publish errors for malformed commands", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpPublishUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const responses = [
        yield* sendTcpPublishLine(tcpPublishUrl, "{"),
        yield* sendTcpPublishLine(tcpPublishUrl, JSON.stringify("not-object")),
        yield* sendTcpPublishLine(tcpPublishUrl, "   \n{}"),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publish",
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publish",
          topic: "orders",
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publishMany",
          topic: "orders",
          rows: {},
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "patch",
          topic: "orders",
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "patch",
          topic: "orders",
          key: "a",
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "delete",
          topic: "orders",
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "noop",
          topic: "orders",
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publish",
          topic: "orders",
          row: order("a", 10),
          rows: [order("b", 20)],
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publish",
          topic: "orders",
          row: order("a", 10),
          unknown: true,
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publish",
          topic: "unknown",
          row: order("a", 10),
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publish",
          topic: "orders",
          row: { ...order("a", 10), unknown: true },
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publish",
          topic: "orders",
          row: { id: "missing-price" },
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "patch",
          topic: "orders",
          key: "a",
          patch: { unknown: true },
        }),
        yield* sendTcpPublishLine(
          tcpPublishUrl,
          `{"op":"patch","topic":"orders","key":"a","patch":{"constructor":10}}\n`,
        ),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "patch",
          topic: "orders",
          key: "a",
          patch: { price: "expensive" },
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "delete",
          topic: "unknown",
          key: "a",
        }),
      ];

      expect(responses).toStrictEqual([
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must be valid JSON.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish cannot find View Server topic unknown.",
            phase: "decode",
            topic: "unknown",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish row did not match View Server topic orders.",
            phase: "decode",
            topic: "orders",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish row did not match View Server topic orders.",
            phase: "decode",
            topic: "orders",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish patch did not match View Server topic orders.",
            phase: "decode",
            topic: "orders",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish patch did not match View Server topic orders.",
            phase: "decode",
            topic: "orders",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish patch did not match View Server topic orders.",
            phase: "decode",
            topic: "orders",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish cannot find View Server topic unknown.",
            phase: "decode",
            topic: "unknown",
          },
        },
      ]);
      yield* runtime.close;
    }),
  );

  it.live("rejects TCP publish commands for malformed runtime topic definitions", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      const missingTopicSchemaConfig = {
        topics: {
          orders: {
            key: "id",
          },
        },
      };
      const missingTopicKeyFieldConfig = {
        topics: {
          orders: {
            schema: Order,
            key: "missing",
          },
        },
      };
      const missingTopicSchemaIngress = yield* makeViewServerTcpPublishIngress(
        // @ts-expect-error intentionally malformed config for the runtime defensive guard.
        missingTopicSchemaConfig,
        runtimeCore.internalClient,
        { port: 0 },
      );
      const missingTopicKeyFieldIngress = yield* makeViewServerTcpPublishIngress(
        missingTopicKeyFieldConfig,
        runtimeCore.internalClient,
        { port: 0 },
      );

      const missingTopicSchemaSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(missingTopicSchemaIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      const missingTopicKeyFieldSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(missingTopicKeyFieldIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      const command = `${JSON.stringify({
        op: "publish",
        topic: "orders",
        row: order("a", 10),
      })}\n`;
      missingTopicSchemaSocket.write(command);
      missingTopicKeyFieldSocket.write(command);
      const missingTopicSchemaResponse = yield* readTcpPublishResponse(
        missingTopicSchemaSocket,
      ).pipe(Effect.timeout("1 second"));
      const missingTopicKeyFieldResponse = yield* readTcpPublishResponse(
        missingTopicKeyFieldSocket,
      ).pipe(Effect.timeout("1 second"));

      expect([missingTopicSchemaResponse, missingTopicKeyFieldResponse]).toStrictEqual([
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish cannot find View Server topic orders.",
            phase: "decode",
            topic: "orders",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish cannot find key field missing for View Server topic orders.",
            phase: "decode",
            topic: "orders",
          },
        },
      ]);
      yield* missingTopicSchemaIngress.close;
      yield* missingTopicKeyFieldIngress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects non-strict TCP publish patch field values at the decode boundary", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(nestedTcpViewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpPublishUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const response = yield* sendTcpPublishCommand(tcpPublishUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: { meta: { desk: "LDN", unknown: true } },
      });

      expect(response).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      yield* runtime.close;
    }),
  );

  it.live("rejects invalid TCP publish server options before listening", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      const invalidOptions = [
        { port: -1 },
        { port: 65536 },
        { port: 1.5 },
        { port: Number.NaN },
        { port: Number.POSITIVE_INFINITY },
        { maxLineBytes: 0, port: 0 },
        { maxConnections: 0, port: 0 },
        { maxQueuedCommands: 0, port: 0 },
        { maxGlobalQueuedCommands: 0, port: 0 },
      ];
      const errors = yield* Effect.forEach(invalidOptions, (options) =>
        makeViewServerTcpPublishIngress(viewServer, runtimeCore.internalClient, options).pipe(
          Effect.flip,
        ),
      );

      expect(
        errors.map((error) => ({
          message: error.message,
          phase: error.phase,
        })),
      ).toStrictEqual([
        {
          message: "TCP publish port must be a safe integer between 0 and 65535.",
          phase: "configuration",
        },
        {
          message: "TCP publish port must be a safe integer between 0 and 65535.",
          phase: "configuration",
        },
        {
          message: "TCP publish port must be a safe integer between 0 and 65535.",
          phase: "configuration",
        },
        {
          message: "TCP publish port must be a safe integer between 0 and 65535.",
          phase: "configuration",
        },
        {
          message: "TCP publish port must be a safe integer between 0 and 65535.",
          phase: "configuration",
        },
        {
          message: "TCP publish maxLineBytes must be a positive safe integer.",
          phase: "configuration",
        },
        {
          message: "TCP publish maxConnections must be a positive safe integer.",
          phase: "configuration",
        },
        {
          message: "TCP publish maxQueuedCommands must be a positive safe integer.",
          phase: "configuration",
        },
        {
          message: "TCP publish maxGlobalQueuedCommands must be a positive safe integer.",
          phase: "configuration",
        },
      ]);
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects TCP publish commands for source-owned topics", () =>
    Effect.gen(function* () {
      const sourceOwnedViewServer = defineViewServerConfig({
        kafka: {
          local: "localhost:9092",
        },
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "orders-source",
              regions: ["local"],
              value: kafka.json(() => Schema.toCodecJson(Order)),
              key: kafka.stringKey(),
              rowKey: ({ key }) => key,
              map: ({ value }) => ({
                price: value.price,
              }),
            }),
          },
        },
      });
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(sourceOwnedViewServer, {});
      const ingress = yield* makeViewServerTcpPublishIngress(
        sourceOwnedViewServer,
        runtimeCore.internalClient,
        {
          port: 0,
        },
      );

      const response = yield* sendTcpPublishCommand(ingress.url, {
        op: "publish",
        topic: "orders",
        row: {
          id: "a",
          price: "not-a-number",
        },
      });

      expect(response).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          message:
            "Source-owned topics do not support direct runtime mutations; publish through the configured Kafka/gRPC source or use an externally-published topic.",
          topic: "orders",
        },
      });
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("requires auth for TCP publish mutations when runtime auth is configured", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        auth: bearerAuth,
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpPublishUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const rejected = yield* sendTcpPublishCommand(tcpPublishUrl, {
        op: "publish",
        topic: "orders",
        row: order("rejected", 10),
      });
      const accepted = yield* sendTcpPublishCommand(tcpPublishUrl, {
        headers: {
          authorization: "Bearer view-server-test",
        },
        op: "publish",
        topic: "orders",
        row: order("accepted", 20),
      });
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(rejected).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerAuthError",
          message: "Missing or invalid authorization header.",
          status: 401,
        },
      });
      expect(accepted).toStrictEqual({ ok: true });
      expect(snapshot).toStrictEqual({
        rows: [{ id: "accepted", price: 20 }],
        status: "ready",
        statusCode: "Ready",
        totalRows: 1,
        version: 1,
      });
      yield* runtime.close;
    }),
  );

  it.live("passes TCP publish peer address into auth validation", () =>
    Effect.gen(function* () {
      const remoteAddress = yield* Deferred.make<string>();
      const runtime = yield* makeViewServerRuntime(viewServer, {
        auth: {
          validateRequest: (request) =>
            Option.match(request.remoteAddress, {
              onNone: () =>
                Effect.fail(
                  new ViewServerAuthError({
                    message: "TCP auth did not receive a peer address.",
                    status: 403,
                  }),
                ),
              onSome: (address) =>
                Deferred.succeed(remoteAddress, address).pipe(
                  Effect.as({
                    forwardedHeaders: {},
                    id: "tcp-session",
                    systemHeaders: {},
                  }),
                ),
            }),
        },
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpPublishUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const accepted = yield* sendTcpPublishCommand(tcpPublishUrl, {
        op: "publish",
        topic: "orders",
        row: order("accepted", 20),
      });
      const observedRemoteAddress = yield* Deferred.await(remoteAddress);

      expect(accepted).toStrictEqual({ ok: true });
      expect(observedRemoteAddress).toBe("127.0.0.1");
      yield* runtime.close;
    }),
  );

  it.live("passes matching config into TCP publish ingress", () =>
    Effect.gen(function* () {
      const regions = {
        local: "localhost:9092",
      };
      const mixedSourceViewServer = defineViewServerConfig({
        kafka: regions,
        grpc: {
          clients: grpcClients,
        },
        topics: {
          orders: grpcTopicSources.materialized({
            schema: GrpcOrder,
            key: "id",
            client: "orders",
            method: "streamOrders",
            request: () => ({ orderId: "all" }),
            acquire: () => Stream.never,
            map: ({ value }) => ({
              id: value.customerId,
              customerId: value.customerId,
              status: value.status,
              price: value.price,
              region: "usa",
              updatedAt: value.updatedAt,
            }),
          }),
          audit: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "audit-source",
              regions: ["local"],
              value: kafka.json(() => Schema.toCodecJson(Order)),
              key: kafka.stringKey(),
              rowKey: ({ key }) => key,
              map: ({ value }) => ({
                price: value.price,
              }),
            }),
          },
        },
      });
      type MixedTopics = typeof mixedSourceViewServer.topics;
      type RuntimeDependencies = ViewServerRuntimeDependencies<MixedTopics>;
      let tcpPublishTopics: ReadonlyArray<string> = [];
      let tcpPublishOptionKeys: ReadonlyArray<string> = [];
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<MixedTopics>(),
        sourceAdapters: [
          makeKafkaRuntimeSourceAdapter({
            ...makeDefaultKafkaRuntimeSourceDependencies<MixedTopics>(),
            makeIngress: () => Effect.succeed({ close: Effect.void }),
          }),
          makeGrpcRuntimeSourceAdapter({
            ...makeDefaultGrpcRuntimeSourceDependencies<MixedTopics>(),
            makeIngress: () => Effect.succeed({ close: Effect.void }),
          }),
        ],
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.void,
          }),
        makeTcpPublishIngress: (tcpConfig, _client, options) => {
          tcpPublishTopics = Object.keys(tcpConfig.topics);
          tcpPublishOptionKeys = Object.keys(options).sort();
          return Effect.succeed({
            url: "tcp://127.0.0.1:1235",
            close: Effect.void,
          });
        },
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        mixedSourceViewServer,
        {
          host: "127.0.0.1",
          tcpPublishPort: 1235,
          kafka: {
            consumerGroupId: "view-server-tcp-source-owned",
          },
        },
      );

      expect({
        tcpPublishOptionKeys,
        tcpPublishTopics,
        tcpPublishUrl: runtime.tcpPublishUrl,
      }).toStrictEqual({
        tcpPublishOptionKeys: ["port"],
        tcpPublishTopics: ["orders", "audit"],
        tcpPublishUrl: "tcp://127.0.0.1:1235",
      });
      yield* runtime.close;
    }),
  );
});
