import type { TopicDefinitions, ViewServerTopicConfig } from "@effect-view-server/config";
import { viewServerEncodeHealth } from "@effect-view-server/protocol";
import { Cause, Effect, Option } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { validateViewServerHttpRequest, viewServerAuthErrorResponse } from "./auth";
import type { ViewServerWebSocketServerInput } from "./server-types";

const jsonStringify = <Value>(value: Value): string => JSON.stringify(value);

const jsonResponse = <Value>(status: number, value: Value): HttpServerResponse.HttpServerResponse =>
  HttpServerResponse.text(jsonStringify(value), {
    status,
    contentType: "application/json",
  });

const failureJsonResponse = (cause: Cause.Cause<unknown>): HttpServerResponse.HttpServerResponse =>
  jsonResponse(500, Cause.findErrorOption(cause).pipe(Option.getOrElse(() => Cause.pretty(cause))));

const readinessStatus = (status: string): number =>
  status === "ready" || status === "degraded" ? 200 : 503;

export const makeViewServerHealthRoute = <const Topics extends TopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  input: ViewServerWebSocketServerInput<Topics>,
  path: `/${string}`,
) =>
  HttpRouter.add(
    "GET",
    path,
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      return yield* validateViewServerHttpRequest(input.auth, request).pipe(
        Effect.matchEffect({
          onFailure: (error) => Effect.succeed(viewServerAuthErrorResponse(error)),
          onSuccess: () =>
            Effect.gen(function* () {
              const health = yield* input.runtime.health();
              return yield* viewServerEncodeHealth(config, health);
            }).pipe(
              Effect.map((health) => jsonResponse(readinessStatus(health.status), health)),
              Effect.catchCause((cause) => Effect.succeed(failureJsonResponse(cause))),
            ),
        }),
      );
    }),
  );
