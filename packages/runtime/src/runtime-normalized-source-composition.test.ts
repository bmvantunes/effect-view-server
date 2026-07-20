import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import { ViewServerGrpcIngressError } from "./grpc-source-lifecycle";
import { makeDefaultRuntimeDependencies, makeViewServerRuntimeWithDependencies } from "./internal";
import type { ViewServerRuntimeDependencies } from "./runtime-dependencies";
import {
  validateRuntimeSourceOwnership,
  type ViewServerRuntimeSourceAdapter,
  type ViewServerRuntimeSourceModule,
} from "./runtime-source";
import type { ViewServerRuntimeCoreProtocolQuerySubscriber } from "@effect-view-server/runtime-core/internal";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

class SourceOwnershipTestError extends Schema.TaggedErrorClass<SourceOwnershipTestError>()(
  "SourceOwnershipTestError",
  { message: Schema.String },
) {}

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

describe("Normalized runtime source composition", () => {
  it.effect("prepares and starts registered source Modules around the server lifecycle", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const replacementProtocolQuerySubscriber = {
        subscribeProtocolQuery: () => Effect.die("protocol query sentinel must not be invoked"),
      } satisfies ViewServerRuntimeCoreProtocolQuerySubscriber<typeof viewServer.topics>;
      let serverProtocolQuerySubscriber: unknown;
      const sourceAdapter: ViewServerRuntimeSourceAdapter<typeof viewServer.topics> = {
        make: () =>
          Effect.succeed({
            healthOverlay: (health) => health,
            ownedTopics: [],
            prepare: ({ client, liveClient, protocolQuerySubscriber }) =>
              Effect.acquireRelease(
                Effect.sync(() => {
                  events.push("prepare:source");
                }),
                () =>
                  Effect.sync(() => {
                    events.push("close:prepared-source");
                  }),
                { interruptible: true },
              ).pipe(
                Effect.map(() => ({
                  client,
                  liveClient,
                  protocolQuerySubscriber: replacementProtocolQuerySubscriber,
                  start: Effect.acquireRelease(
                    Effect.sync(() => {
                      events.push("start:source");
                    }),
                    () =>
                      Effect.sync(() => {
                        events.push("close:running-source");
                      }),
                    { interruptible: true },
                  ).pipe(Effect.asVoid),
                })),
              ),
          }),
      };
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        sourceAdapters: [sourceAdapter],
        makeServer: (_config, input) =>
          Effect.sync(() => {
            events.push("acquire:server");
            serverProtocolQuerySubscriber = input.liveClient.subscribeProtocolQuery;
            return {
              url: "ws://127.0.0.1:0/rpc",
              healthUrl: "http://127.0.0.1:0/health",
              metricsUrl: "http://127.0.0.1:0/metrics",
              close: Effect.sync(() => {
                events.push("close:server");
              }),
            };
          }),
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer);
      yield* runtime.close;

      expect(serverProtocolQuerySubscriber).toBe(
        replacementProtocolQuerySubscriber.subscribeProtocolQuery,
      );
      expect(events).toStrictEqual([
        "prepare:source",
        "acquire:server",
        "start:source",
        "close:running-source",
        "close:server",
        "close:prepared-source",
      ]);
    }),
  );

  it.effect("allows one source Module to describe the same owned topic more than once", () =>
    Effect.gen(function* () {
      let conflictCalls = 0;
      const sourceModule: ViewServerRuntimeSourceModule<
        typeof viewServer.topics,
        SourceOwnershipTestError
      > = {
        healthOverlay: (health) => health,
        ownedTopics: ["first", "second"].map((owner) => ({
          topic: "orders",
          owner,
          conflict: () => {
            conflictCalls += 1;
            return new SourceOwnershipTestError({
              message: "same Module ownership is not a conflict",
            });
          },
        })),
        prepare: ({ client, liveClient, protocolQuerySubscriber }) =>
          Effect.succeed({
            client,
            liveClient,
            protocolQuerySubscriber,
            start: Effect.void,
          }),
      };

      yield* validateRuntimeSourceOwnership([sourceModule]);

      expect(conflictCalls).toBe(0);
    }),
  );

  it.effect("rejects two registered source Adapters that own the same topic", () =>
    Effect.gen(function* () {
      const makeSourceAdapter = (
        owner: string,
      ): ViewServerRuntimeSourceAdapter<typeof viewServer.topics, ViewServerGrpcIngressError> => ({
        make: () =>
          Effect.succeed({
            healthOverlay: (health) => health,
            ownedTopics: [
              {
                topic: "orders",
                owner,
                conflict: (existingOwner) =>
                  new ViewServerGrpcIngressError({
                    message: `orders cannot be owned by both ${existingOwner} and ${owner}`,
                    cause: "orders",
                    feedName: owner,
                    topic: "orders",
                  }),
              },
            ],
            prepare: ({ client, liveClient, protocolQuerySubscriber }) =>
              Effect.succeed({
                client,
                liveClient,
                protocolQuerySubscriber,
                start: Effect.void,
              }),
          }),
      });
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        sourceAdapters: [
          makeSourceAdapter("Kafka source orders-source"),
          makeSourceAdapter("gRPC feed orders"),
        ],
      };
      const conflict = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer).pipe(
        Effect.flip,
      );

      expect(conflict).toBeInstanceOf(ViewServerGrpcIngressError);
      expect(conflict.message).toBe(
        "orders cannot be owned by both Kafka source orders-source and gRPC feed orders",
      );
    }),
  );
});
