import { create, toBinary, type Message } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { type Interceptor } from "@connectrpc/connect";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import {
  applyEvent,
  initialClientState,
  liveQueryResult,
  type ViewServerLiveSubscription,
} from "@effect-view-server/client";
import { makeViewServerClient } from "@effect-view-server/client/remote";
import {
  ViewServerId,
  defineViewServerConfig,
  type LiveQueryResult,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { makeViewServerRuntimeCore } from "@effect-view-server/runtime-core";
import { makeViewServerWebSocketServer } from "@effect-view-server/server";
import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Option, Schedule, Schema, Stream } from "effect";
import * as Http2 from "node:http2";
import { grpc } from "./model";
import { grpcNode } from "./node";
import { awaitTestCondition } from "./test-support";

type RequestMessage = Message<"grpc.connect.Request"> & {
  readonly region: string;
};

type EventMessage = Message<"grpc.connect.Event"> & {
  readonly id: string;
  readonly price: number;
  readonly region: string;
};

type Row = {
  readonly id: string;
  readonly price: number;
  readonly region: string;
};

class ConnectRpcTestError extends Schema.TaggedErrorClass<ConnectRpcTestError>()(
  "ConnectRpcTestError",
  {
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Unknown),
  },
) {}

const descriptorFile = fileDesc(
  globalThis.btoa(
    Array.from(
      toBinary(
        FileDescriptorProtoSchema,
        create(FileDescriptorProtoSchema, {
          name: "grpc/connect.proto",
          package: "grpc.connect",
          syntax: "proto3",
          messageType: [
            {
              name: "Request",
              field: [
                {
                  name: "region",
                  number: 1,
                  type: FieldDescriptorProto_Type.STRING,
                },
              ],
            },
            {
              name: "Event",
              field: [
                {
                  name: "id",
                  number: 1,
                  type: FieldDescriptorProto_Type.STRING,
                },
                {
                  name: "price",
                  number: 2,
                  type: FieldDescriptorProto_Type.DOUBLE,
                },
                {
                  name: "region",
                  number: 3,
                  type: FieldDescriptorProto_Type.STRING,
                },
              ],
            },
          ],
          service: [
            {
              name: "Orders",
              method: [
                {
                  name: "Stream",
                  inputType: ".grpc.connect.Request",
                  outputType: ".grpc.connect.Event",
                  serverStreaming: true,
                },
              ],
            },
          ],
        }),
      ),
      (byte) => String.fromCharCode(byte),
    ).join(""),
  ),
);

const RequestSchema = messageDesc<RequestMessage>(descriptorFile, 0);
const EventSchema = messageDesc<EventMessage>(descriptorFile, 1);
const OrdersService = serviceDesc<{
  readonly stream: {
    readonly input: typeof RequestSchema;
    readonly output: typeof EventSchema;
    readonly methodKind: "server_streaming";
  };
}>(descriptorFile, 0);

const Order = Schema.Struct({
  id: ViewServerId,
  price: Schema.Number,
  region: Schema.String,
});

const order = (id: string, price: number, region: string): EventMessage =>
  create(EventSchema, { id, price, region });

const waitForAbort = (signal: AbortSignal): Promise<void> =>
  signal.aborted
    ? Promise.resolve()
    : new Promise((resolve) => {
        signal.addEventListener("abort", () => resolve(), { once: true });
      });

const makeServer = Effect.fn("GrpcSourceAdapter.test.connect.server")(function* () {
  const requests = new Map<string, number>();
  const requestHeaders = new Map<
    string,
    {
      readonly authorization: string | null;
      readonly browserMetadata: string | null;
      readonly browserSession: string | null;
      readonly cookie: string | null;
      readonly upstreamCredential: string | null;
    }
  >();
  const aborts = new Map<string, number>();
  const sessions = new Set<Http2.ServerHttp2Session>();
  const server = yield* Effect.acquireRelease(
    Effect.callback<Http2.Http2Server, ConnectRpcTestError>((resume) => {
      const http2Server = Http2.createServer(
        connectNodeAdapter({
          routes: (router) =>
            router.service(OrdersService, {
              stream: async function* (request, context) {
                const requestCount = (requests.get(request.region) ?? 0) + 1;
                requests.set(request.region, requestCount);
                requestHeaders.set(request.region, {
                  authorization: context.requestHeader.get("authorization"),
                  browserMetadata: context.requestHeader.get("x-browser-metadata"),
                  browserSession: context.requestHeader.get("x-view-session-id"),
                  cookie: context.requestHeader.get("cookie"),
                  upstreamCredential: context.requestHeader.get("x-upstream-credential"),
                });
                if (request.region === "complete") {
                  return;
                }
                if (request.region === "fail") {
                  throw new Error("planned real ConnectRPC stream failure");
                }
                if (request.region === "cleanup" && requestCount > 1) {
                  await waitForAbort(context.signal);
                  aborts.set(request.region, (aborts.get(request.region) ?? 0) + 1);
                  return;
                }
                const rows =
                  request.region === "all"
                    ? [order("all-one", 1, "eu"), order("all-two", 2, "us")]
                    : [
                        order(`${request.region}-one`, 1, request.region),
                        order(`${request.region}-two`, 2, request.region),
                      ];
                for (const row of rows) {
                  yield row;
                }
                await waitForAbort(context.signal);
                aborts.set(request.region, (aborts.get(request.region) ?? 0) + 1);
              },
            }),
        }),
      );
      http2Server.on("session", (session) => {
        sessions.add(session);
        session.once("close", () => {
          sessions.delete(session);
        });
      });
      const onError = (cause: unknown) => {
        http2Server.off("error", onError);
        resume(
          Effect.fail(
            new ConnectRpcTestError({
              message: "ConnectRPC test server failed to start.",
              cause,
            }),
          ),
        );
      };
      http2Server.once("error", onError);
      http2Server.listen(0, "127.0.0.1", () => {
        http2Server.off("error", onError);
        resume(Effect.succeed(http2Server));
      });
    }),
    (http2Server) =>
      Effect.callback<void>((resume) => {
        for (const session of sessions) {
          session.close();
          session.destroy();
        }
        http2Server.close(() => resume(Effect.void));
      }),
  );
  const address = server.address();
  if (typeof address !== "object" || address === null || typeof address.port !== "number") {
    return yield* new ConnectRpcTestError({
      message: "ConnectRPC test server did not expose a TCP port.",
      cause: address,
    });
  }
  return {
    baseUrl: `http://127.0.0.1:${address.port}`,
    requests,
    requestHeaders,
    aborts,
    sessionCount: () => sessions.size,
  };
});

const waitForSnapshot = Effect.fn("GrpcSourceAdapter.test.connect.snapshot")(function* (
  effect: Effect.Effect<LiveQueryResult<Row>, ViewServerRuntimeError>,
) {
  const poll = (remaining: number): Effect.Effect<LiveQueryResult<Row>, ViewServerRuntimeError> =>
    effect.pipe(
      Effect.flatMap((snapshot) =>
        snapshot.totalRows === 2 || remaining === 0
          ? Effect.succeed(snapshot)
          : Effect.sleep("5 millis").pipe(Effect.andThen(poll(remaining - 1))),
      ),
    );
  return yield* poll(100);
});

const waitForLiveRows = Effect.fn("GrpcSourceAdapter.test.connect.live")(function* (
  subscription: ViewServerLiveSubscription<Row>,
) {
  let state = initialClientState<Row>();
  const ready = yield* Deferred.make<LiveQueryResult<Row>>();
  yield* subscription.events.pipe(
    Stream.runForEach((event) => {
      state = applyEvent(state, event);
      const result = liveQueryResult(state);
      return result.totalRows === 2
        ? Deferred.succeed(ready, result).pipe(Effect.asVoid)
        : Effect.void;
    }),
    Effect.forkChild,
  );
  return yield* Deferred.await(ready);
});

const awaitCount = (
  label: string,
  read: () => number,
  expected: number,
  remaining = 1_000,
): Effect.Effect<void> =>
  awaitTestCondition(
    () => `${label} count ${expected}; last observed ${read()}`,
    () => read() === expected,
    remaining,
  );

describe("gRPC Source Adapter real ConnectRPC integration", () => {
  it.live("runs Materialized and shared Leased streams with only Layer-owned credentials", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const connectServer = yield* makeServer();
        const sources = grpc.topicSources({ orders: OrdersService });
        const config = defineViewServerConfig({
          topics: {
            allOrders: {
              schema: Order,
              source: sources.materialized({
                client: "orders",
                method: "stream",
                request: () => ({ region: "all" }),
                map: ({ value }) => ({
                  id: value.id,
                  price: value.price,
                  region: value.region,
                }),
              }),
            },
            regionOrders: {
              schema: Order,
              source: sources.leased({
                client: "orders",
                method: "stream",
                routeBy: ["region"],
                request: (route) => ({ region: route.region }),
                map: ({ value }) => ({
                  id: value.id,
                  price: value.price,
                  region: value.region,
                }),
              }),
            },
          },
        });
        const credentialInterceptor: Interceptor = (next) => (request) => {
          request.header.set("x-upstream-credential", "layer-owned");
          return next(request);
        };
        yield* Effect.gen(function* () {
          const runtime = yield* makeViewServerRuntimeCore(config, {});
          const materialized = yield* waitForSnapshot(
            runtime.client.snapshot("allOrders", {
              select: ["id", "price", "region"],
              orderBy: [{ field: "id", direction: "asc" }],
            }),
          );
          const first = yield* runtime.liveClient.subscribe("regionOrders", {
            routeBy: { region: "eu" },
            select: ["id", "price", "region"],
          });
          const second = yield* runtime.liveClient.subscribe("regionOrders", {
            routeBy: { region: "eu" },
            where: [{ field: "price", type: "greaterThan", filter: 0 }],
            select: ["id", "price", "region"],
            orderBy: [{ field: "price", direction: "desc" }],
          });
          const firstRows = yield* waitForLiveRows(first);
          const secondRows = yield* waitForLiveRows(second);
          const cleanup = yield* runtime.liveClient.subscribe("regionOrders", {
            routeBy: { region: "cleanup" },
            select: ["id", "price", "region"],
          });
          const cleanupRows = yield* waitForLiveRows(cleanup);

          expect({
            materializedRows: materialized.rows,
            firstRows: firstRows.rows,
            secondRows: secondRows.rows,
            cleanupRows: cleanupRows.rows,
            allRequests: connectServer.requests.get("all"),
            euRequests: connectServer.requests.get("eu"),
            allHeaders: connectServer.requestHeaders.get("all"),
            euHeaders: connectServer.requestHeaders.get("eu"),
          }).toStrictEqual({
            materializedRows: [
              { id: "all-one", price: 1, region: "eu" },
              { id: "all-two", price: 2, region: "us" },
            ],
            firstRows: [
              { id: "eu-one", price: 1, region: "eu" },
              { id: "eu-two", price: 2, region: "eu" },
            ],
            secondRows: [
              { id: "eu-two", price: 2, region: "eu" },
              { id: "eu-one", price: 1, region: "eu" },
            ],
            cleanupRows: [
              {
                id: "cleanup-one",
                price: 1,
                region: "cleanup",
              },
              {
                id: "cleanup-two",
                price: 2,
                region: "cleanup",
              },
            ],
            allRequests: 1,
            euRequests: 1,
            allHeaders: {
              authorization: null,
              browserMetadata: null,
              browserSession: null,
              cookie: null,
              upstreamCredential: "layer-owned",
            },
            euHeaders: {
              authorization: null,
              browserMetadata: null,
              browserSession: null,
              cookie: null,
              upstreamCredential: "layer-owned",
            },
          });

          yield* second.close();
          expect(connectServer.aborts.get("eu")).toBeUndefined();
          yield* first.close();
          yield* awaitCount("eu abort", () => connectServer.aborts.get("eu") ?? 0, 1);
          yield* cleanup.close();
          yield* awaitCount(
            "first cleanup abort",
            () => connectServer.aborts.get("cleanup") ?? 0,
            1,
          );
          const cleanupReacquired = yield* runtime.liveClient.subscribe("regionOrders", {
            routeBy: { region: "cleanup" },
            select: ["id", "price", "region"],
          });
          yield* awaitCount("cleanup request", () => connectServer.requests.get("cleanup") ?? 0, 2);
          const cleanupSnapshot = Option.getOrThrow(
            yield* cleanupReacquired.events.pipe(
              Stream.filter((event) => event.type === "snapshot"),
              Stream.take(1),
              Stream.runHead,
            ),
          );
          expect({
            requests: connectServer.requests.get("cleanup"),
            rows: cleanupSnapshot.rows,
            totalRows: cleanupSnapshot.totalRows,
          }).toStrictEqual({
            requests: 2,
            rows: [],
            totalRows: 0,
          });
          yield* cleanupReacquired.close();
          yield* awaitCount(
            "second cleanup abort",
            () => connectServer.aborts.get("cleanup") ?? 0,
            2,
          );
          yield* runtime.close;
          yield* awaitCount("all abort", () => connectServer.aborts.get("all") ?? 0, 1);
        }).pipe(
          Effect.provide(
            grpcNode.layer(config, {
              orders: {
                baseUrl: connectServer.baseUrl,
                interceptors: [credentialInterceptor],
              },
            }),
          ),
        );
        yield* awaitCount("active session", connectServer.sessionCount, 0);
      }),
    ),
  );

  it.live(
    "retries real transport failure and unexpected completion with one request snapshot",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const connectServer = yield* makeServer();
          const sources = grpc.topicSources({ orders: OrdersService });
          let completionRequestCalls = 0;
          let failureRequestCalls = 0;
          const config = defineViewServerConfig({
            topics: {
              completedOrders: {
                schema: Order,
                source: sources.materialized(
                  {
                    client: "orders",
                    method: "stream",
                    request: () => {
                      completionRequestCalls += 1;
                      return { region: "complete" };
                    },
                    map: ({ value }) => ({
                      id: value.id,
                      price: value.price,
                      region: value.region,
                    }),
                  },
                  Schedule.recurs(1),
                ),
              },
              failedOrders: {
                schema: Order,
                source: sources.materialized(
                  {
                    client: "orders",
                    method: "stream",
                    request: () => {
                      failureRequestCalls += 1;
                      return { region: "fail" };
                    },
                    map: ({ value }) => ({
                      id: value.id,
                      price: value.price,
                      region: value.region,
                    }),
                  },
                  Schedule.recurs(1),
                ),
              },
            },
          });
          yield* Effect.gen(function* () {
            const runtime = yield* makeViewServerRuntimeCore(config, {});
            const completedDiagnostics = yield* runtime.liveClient.subscribeSourceHealth({
              topic: "completedOrders",
            });
            const failedDiagnostics = yield* runtime.liveClient.subscribeSourceHealth({
              topic: "failedOrders",
            });
            const completedHealth = Option.getOrThrow(
              yield* completedDiagnostics.events.pipe(
                Stream.filter((health) => health.status._tag === "Exhausted"),
                Stream.take(1),
                Stream.runHead,
              ),
            );
            const failedHealth = Option.getOrThrow(
              yield* failedDiagnostics.events.pipe(
                Stream.filter((health) => health.status._tag === "Exhausted"),
                Stream.take(1),
                Stream.runHead,
              ),
            );
            if (
              completedHealth.status._tag !== "Exhausted" ||
              failedHealth.status._tag !== "Exhausted"
            ) {
              return yield* Effect.die("Expected both real gRPC streams to exhaust.");
            }

            expect({
              completionRequestCalls,
              completionRequests: connectServer.requests.get("complete"),
              completionTermination: completedHealth.status.exhaustion.lastTermination,
              failureRequestCalls,
              failureRequests: connectServer.requests.get("fail"),
              failureTermination: failedHealth.status.exhaustion.lastTermination,
            }).toStrictEqual({
              completionRequestCalls: 1,
              completionRequests: 2,
              completionTermination: {
                _tag: "UnexpectedCompletion",
              },
              failureRequestCalls: 1,
              failureRequests: 2,
              failureTermination: {
                _tag: "Failed",
                failure: {
                  _tag: "AdapterFailure",
                  failure: {
                    _tag: "GrpcStreamFailure",
                    client: "orders",
                    code: "INTERNAL",
                    message: "The upstream gRPC response stream failed.",
                    method: "stream",
                  },
                },
              },
            });

            yield* completedDiagnostics.close();
            yield* failedDiagnostics.close();
            yield* runtime.close;
          }).pipe(
            Effect.provide(
              grpcNode.layer(config, {
                orders: {
                  baseUrl: connectServer.baseUrl,
                },
              }),
            ),
          );
          yield* awaitCount("active session", connectServer.sessionCount, 0);
        }),
      ),
  );

  it.live("does not forward authenticated browser session metadata to upstream gRPC", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const connectServer = yield* makeServer();
        const sources = grpc.topicSources({ orders: OrdersService });
        const config = defineViewServerConfig({
          topics: {
            regionOrders: {
              schema: Order,
              source: sources.leased({
                client: "orders",
                method: "stream",
                routeBy: ["region"],
                request: (route) => ({ region: route.region }),
                map: ({ value }) => ({
                  id: value.id,
                  price: value.price,
                  region: value.region,
                }),
              }),
            },
          },
        });
        const credentialInterceptor: Interceptor = (next) => (request) => {
          request.header.set("x-upstream-credential", "layer-owned");
          return next(request);
        };

        yield* Effect.gen(function* () {
          const runtime = yield* makeViewServerRuntimeCore(config, {});
          yield* Effect.addFinalizer(() => runtime.close);
          const server = yield* makeViewServerWebSocketServer(config, {
            auth: {
              validateRequest: () =>
                Effect.succeed({
                  id: "browser-session",
                  forwardedHeaders: {
                    authorization: "Bearer browser-secret",
                    cookie: "browser-cookie=secret",
                    "x-browser-metadata": "browser-only",
                  },
                  systemHeaders: {
                    "x-view-session-id": "browser-session",
                  },
                }),
            },
            liveClient: runtime.serverLiveClient,
            runtime: runtime.client,
          });
          yield* Effect.addFinalizer(() => server.close);
          const client = yield* makeViewServerClient(config, { url: server.url });
          yield* Effect.addFinalizer(() => client.close);
          const subscription = yield* client.subscribe("regionOrders", {
            routeBy: { region: "browser" },
            select: ["id", "price", "region"],
          });
          yield* Effect.addFinalizer(() => subscription.close().pipe(Effect.ignore));
          const rows = yield* waitForLiveRows(subscription);

          expect({
            rows: rows.rows,
            upstreamHeaders: connectServer.requestHeaders.get("browser"),
          }).toStrictEqual({
            rows: [
              { id: "browser-one", price: 1, region: "browser" },
              { id: "browser-two", price: 2, region: "browser" },
            ],
            upstreamHeaders: {
              authorization: null,
              browserMetadata: null,
              browserSession: null,
              cookie: null,
              upstreamCredential: "layer-owned",
            },
          });

          yield* subscription.close();
          yield* client.close;
          yield* server.close;
          yield* runtime.close;
        }).pipe(
          Effect.provide(
            grpcNode.layer(config, {
              orders: {
                baseUrl: connectServer.baseUrl,
                interceptors: [credentialInterceptor],
              },
            }),
          ),
        );
        yield* awaitCount("active session", connectServer.sessionCount, 0);
      }),
    ),
  );
});
