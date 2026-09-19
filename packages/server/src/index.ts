import { NodeHttpServer } from "@effect/platform-node";
import type { TopicDefinitions, ViewServerTopicConfig } from "@effect-view-server/config";
import { ViewServerRpcs } from "@effect-view-server/protocol";
import { Context, Effect, Exit, Layer, Scope } from "effect";
import { HttpRouter, HttpServer, HttpServerError, HttpServerRequest } from "effect/unstable/http";
import { RpcSerialization, RpcServer } from "effect/unstable/rpc";
import * as Http from "node:http";
import { validateViewServerHttpRequest, viewServerAuthErrorResponse } from "./auth";
import type {
  ViewServerAuth,
  ViewServerAuthRequest,
  ViewServerAuthValidator,
  ViewServerSession,
} from "./auth";
import { makeViewServerHealthRoute } from "./health-route";
import { makeViewServerMetricsRoute } from "./metrics-route";
import { NodeHttpServerFactory } from "./node-http-server-factory";
import { makeViewServerRpcHandlers } from "./rpc-handlers";
import type {
  Jsonify,
  ViewServerHealthHttpJson,
  ViewServerWebSocketServer,
  ViewServerWebSocketServerInput,
  ViewServerWebSocketServerOptions,
} from "./server-types";
import {
  closeTrackedSockets,
  makeTrackedUpgradeRequest,
  type ActiveSocketClosers,
} from "./websocket-tracking";

const makeTrackedWebSocketProtocol = Effect.fn("ViewServerServer.websocket.protocol.make")(
  function* <const Topics extends TopicDefinitions>(
    path: `/${string}`,
    input: ViewServerWebSocketServerInput<Topics>,
    activeSocketClosers: ActiveSocketClosers,
  ) {
    const router = yield* HttpRouter.HttpRouter;
    const clientOpened = input.transport?.clientOpened ?? Effect.void;
    const clientClosed = input.transport?.clientClosed ?? Effect.void;
    const { httpEffect, protocol } = yield* RpcServer.makeProtocolWithHttpEffectWebsocket;
    const trackedHttpEffect = Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      return yield* validateViewServerHttpRequest(input.auth, request).pipe(
        Effect.matchEffect({
          onFailure: (error) => Effect.succeed(viewServerAuthErrorResponse(error)),
          onSuccess: () =>
            httpEffect.pipe(
              Effect.provideService(
                HttpServerRequest.HttpServerRequest,
                makeTrackedUpgradeRequest(request, clientOpened, clientClosed, activeSocketClosers),
              ),
            ),
        }),
      );
    });
    yield* router.add("GET", path, trackedHttpEffect);
    return protocol;
  },
);

const makeTrackedWebSocketProtocolLayer = <const Topics extends TopicDefinitions>(
  path: `/${string}`,
  input: ViewServerWebSocketServerInput<Topics>,
  activeSocketClosers: ActiveSocketClosers,
) =>
  Layer.effect(RpcServer.Protocol)(makeTrackedWebSocketProtocol(path, input, activeSocketClosers));

const closeNodeServer = (server: Http.Server): Effect.Effect<void> =>
  Effect.callback<void>((resume) => {
    server.close(() => resume(Effect.void));
  });

export type {
  Jsonify,
  ViewServerAuth,
  ViewServerAuthRequest,
  ViewServerAuthValidator,
  ViewServerHealthHttpJson,
  ViewServerSession,
  ViewServerWebSocketServer,
  ViewServerWebSocketServerInput,
  ViewServerWebSocketServerOptions,
};
export {
  ViewServerAuthError,
  anonymousViewServerSession,
  validateViewServerAuthRequest,
} from "./auth";

export const makeViewServerWebSocketServer: <const Topics extends TopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  input: ViewServerWebSocketServerInput<Topics>,
  options?: ViewServerWebSocketServerOptions,
) => Effect.Effect<ViewServerWebSocketServer, HttpServerError.ServeError> = Effect.fn(
  "ViewServerServer.websocket.make",
)(function* <const Topics extends TopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  input: ViewServerWebSocketServerInput<Topics>,
  options: ViewServerWebSocketServerOptions = {},
) {
  return yield* Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const path = options.path ?? "/rpc";
      const healthPath = options.healthPath ?? "/health";
      const metricsPath = options.metricsPath ?? "/metrics";
      const handlerScope = yield* Scope.make("parallel");
      const serverScope = yield* Scope.make("parallel");
      const makeNodeServer = yield* NodeHttpServerFactory;
      const nodeServer = makeNodeServer();
      const activeSocketClosers: ActiveSocketClosers = new Set();
      const protocol = makeTrackedWebSocketProtocolLayer(path, input, activeSocketClosers).pipe(
        Layer.provide(HttpRouter.layer),
      );
      const handlers = ViewServerRpcs.toLayer(
        makeViewServerRpcHandlers(config, input, handlerScope),
      );
      const healthRoute = makeViewServerHealthRoute(config, input, healthPath);
      const metricsRoute = makeViewServerMetricsRoute(config, input, metricsPath);
      const httpApp = Layer.mergeAll(protocol, healthRoute, metricsRoute);
      const rpcLayer = RpcServer.layer(ViewServerRpcs, {
        disableFatalDefects: true,
      }).pipe(
        Layer.provide(handlers),
        Layer.provideMerge(protocol),
        Layer.provide(
          HttpRouter.serve(httpApp, {
            disableListenLog: true,
            disableLogger: true,
          }),
        ),
        Layer.provideMerge(
          NodeHttpServer.layer(() => nodeServer, {
            host: options.host,
            port: options.port ?? 0,
            ...(options.websocketCompression !== false
              ? { websocket: { perMessageDeflate: true } }
              : {}),
          }),
        ),
        Layer.provide(RpcSerialization.layerNdjson),
      );
      const context = yield* restore(Layer.buildWithScope(rpcLayer, serverScope)).pipe(
        Effect.onExit((exit) =>
          Exit.isFailure(exit)
            ? closeNodeServer(nodeServer).pipe(
                Effect.andThen(Scope.close(handlerScope, exit)),
                Effect.andThen(Scope.close(serverScope, exit)),
              )
            : Effect.void,
        ),
      );
      const server = Context.get(context, HttpServer.HttpServer);
      const httpUrl = HttpServer.formatAddress(server.address);
      const serverUrl = httpUrl.startsWith("http://0.0.0.0:")
        ? `ws://127.0.0.1:${httpUrl.slice("http://0.0.0.0:".length)}`
        : httpUrl.replace("http://", "ws://");
      const publicHttpUrl = serverUrl.replace("ws://", "http://");
      return {
        url: `${serverUrl}${path}`,
        healthUrl: `${publicHttpUrl}${healthPath}`,
        metricsUrl: `${publicHttpUrl}${metricsPath}`,
        close: Scope.close(handlerScope, Exit.void).pipe(
          Effect.andThen(closeTrackedSockets(activeSocketClosers)),
          Effect.andThen(Scope.close(serverScope, Exit.void)),
        ),
      };
    }),
  );
});

export const createViewServerWebSocketServer = makeViewServerWebSocketServer;
