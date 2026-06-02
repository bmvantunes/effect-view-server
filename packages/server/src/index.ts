import { NodeHttpServer } from "@effect/platform-node";
import type {
  TopicDefinitions,
  ViewServerConfig,
  ViewServerHealth,
  ViewServerRuntimeClient,
} from "@view-server/config";
import { VIEW_SERVER_HEALTH_SUMMARY_TOPIC, VIEW_SERVER_HEALTH_TOPIC } from "@view-server/config";
import type { ViewServerRuntimeLiveClient } from "@view-server/client";
import {
  ViewServerRpcs,
  viewServerDecodeHealthQuery,
  viewServerDecodeLiveQuery,
  viewServerDecodeTopic,
  viewServerEncodeHealthSummaryEvent,
  viewServerEncodeHealthTopicEvent,
  viewServerEncodeLiveEvent,
} from "@view-server/protocol";
import { Context, Effect, Layer, ManagedRuntime, Stream } from "effect";
import { HttpRouter, HttpServer, HttpServerError, HttpServerResponse } from "effect/unstable/http";
import { RpcSerialization, RpcServer } from "effect/unstable/rpc";
import * as Http from "node:http";

type ViewServerServerRuntime<Topics extends TopicDefinitions> = Pick<
  ViewServerRuntimeClient<Topics>,
  "health"
>;

export type ViewServerWebSocketServerInput<Topics extends TopicDefinitions> = {
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
  readonly runtime: ViewServerServerRuntime<Topics>;
};

export type ViewServerWebSocketServerOptions = {
  readonly host?: string;
  readonly port?: number;
  readonly path?: `/${string}`;
  readonly healthPath?: `/${string}`;
};

export type ViewServerWebSocketServer = {
  readonly url: string;
  readonly healthUrl: string;
  readonly close: Effect.Effect<void>;
};

export type Jsonify<T> = T extends bigint
  ? string
  : T extends string | number | boolean | null
    ? T
    : T extends ReadonlyArray<infer Item>
      ? ReadonlyArray<Jsonify<Item>>
      : T extends object
        ? { readonly [Key in keyof T]: Jsonify<T[Key]> }
        : never;

export type ViewServerHealthHttpJson<Topics extends TopicDefinitions = TopicDefinitions> = Jsonify<
  ViewServerHealth<Topics>
>;

const jsonStringify = (value: unknown): string =>
  JSON.stringify(value, (_key, nextValue: unknown) =>
    typeof nextValue === "bigint" ? nextValue.toString() : nextValue,
  );

const jsonResponse = (status: number, value: unknown): HttpServerResponse.HttpServerResponse =>
  HttpServerResponse.text(jsonStringify(value), {
    status,
    contentType: "application/json",
  });

const makeHandlers = <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerWebSocketServerInput<Topics>,
) => {
  return ViewServerRpcs.of({
    "ViewServer.Health": () => input.runtime.health(),
    "ViewServer.Subscribe": (payload) =>
      Stream.unwrap(
        Effect.gen(function* () {
          if (payload.topic === VIEW_SERVER_HEALTH_SUMMARY_TOPIC) {
            yield* viewServerDecodeHealthQuery(payload.topic, payload.query);
            const subscription = yield* input.liveClient.subscribeHealthSummary();
            return subscription.events.pipe(
              Stream.mapEffect(viewServerEncodeHealthSummaryEvent),
              Stream.ensuring(subscription.close().pipe(Effect.ignore)),
            );
          }
          if (payload.topic === VIEW_SERVER_HEALTH_TOPIC) {
            yield* viewServerDecodeHealthQuery(payload.topic, payload.query);
            const subscription = yield* input.liveClient.subscribeHealth();
            return subscription.events.pipe(
              Stream.mapEffect(viewServerEncodeHealthTopicEvent),
              Stream.ensuring(subscription.close().pipe(Effect.ignore)),
            );
          }
          const topic = yield* viewServerDecodeTopic(config, payload.topic);
          const query = yield* viewServerDecodeLiveQuery<Topics, typeof topic>(
            config,
            topic,
            payload.query,
          );
          const subscription = yield* input.liveClient.subscribeRuntime(topic, query);
          return subscription.events.pipe(
            Stream.mapEffect((event) => viewServerEncodeLiveEvent(config, topic, query, event)),
            Stream.ensuring(subscription.close().pipe(Effect.ignore)),
          );
        }),
      ),
  });
};

const makeHealthRoute = <const Topics extends TopicDefinitions>(
  input: ViewServerWebSocketServerInput<Topics>,
  path: `/${string}`,
) =>
  HttpRouter.add(
    "GET",
    path,
    input.runtime.health().pipe(
      Effect.match({
        onFailure: (error) => jsonResponse(500, error),
        onSuccess: (health) => jsonResponse(health.status === "ready" ? 200 : 503, health),
      }),
    ),
  );

export const makeViewServerWebSocketServer: <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerWebSocketServerInput<Topics>,
  options?: ViewServerWebSocketServerOptions,
) => Effect.Effect<ViewServerWebSocketServer, HttpServerError.ServeError> = Effect.fn(
  "ViewServerServer.websocket.make",
)(function* <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerWebSocketServerInput<Topics>,
  options: ViewServerWebSocketServerOptions = {},
) {
  const path = options.path ?? "/rpc";
  const healthPath = options.healthPath ?? "/health";
  const protocol = RpcServer.layerProtocolWebsocket({ path }).pipe(Layer.provide(HttpRouter.layer));
  const handlers = ViewServerRpcs.toLayer(makeHandlers(config, input));
  const healthRoute = makeHealthRoute(input, healthPath);
  const httpApp = Layer.merge(protocol, healthRoute);
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
      NodeHttpServer.layer(Http.createServer, {
        host: options.host,
        port: options.port ?? 0,
      }),
    ),
    Layer.provide(RpcSerialization.layerNdjson),
  );
  const managedRuntime = ManagedRuntime.make(rpcLayer);
  const context = yield* managedRuntime.contextEffect;
  const server = Context.get(context, HttpServer.HttpServer);
  const httpUrl = HttpServer.formatAddress(server.address);
  const serverUrl = httpUrl.startsWith("http://0.0.0.0:")
    ? `ws://127.0.0.1:${httpUrl.slice("http://0.0.0.0:".length)}`
    : httpUrl.replace("http://", "ws://");
  const publicHttpUrl = serverUrl.replace("ws://", "http://");
  return {
    url: `${serverUrl}${path}`,
    healthUrl: `${publicHttpUrl}${healthPath}`,
    close: managedRuntime.disposeEffect,
  };
});

export const createViewServerWebSocketServer = makeViewServerWebSocketServer;
