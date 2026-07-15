import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, kafka } from "@effect-view-server/config";
import { Config, Effect, Schema } from "effect";
import type { ViewServerRuntimeDependencies } from "./internal";
import { makeDefaultRuntimeDependencies, makeViewServerRuntimeWithDependencies } from "./internal";
import type {
  ResolvedViewServerGrpcRuntimeOptions,
  ResolvedViewServerKafkaRuntimeOptions,
} from "./runtime-options";
import { nullRecord } from "../test-harness/runtime";
import { Order } from "../test-harness/runtime-config";
import { grpcTopicOwnedSourceViewServer } from "../test-harness/grpc-config";

describe("Runtime source composition and options", () => {
  it.live("resolves Kafka runtime options and starts configured ingress", () =>
    Effect.gen(function* () {
      const regions = {
        local: Config.succeed("localhost:9092"),
      };
      const kafkaBackedViewServer = defineViewServerConfig({
        kafka: regions,
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
      type RuntimeDependencies = ViewServerRuntimeDependencies<typeof kafkaBackedViewServer.topics>;
      let kafkaOptionsSummary:
        | {
            readonly consume: ResolvedViewServerKafkaRuntimeOptions<
              typeof kafkaBackedViewServer.topics
            >["consume"];
            readonly consumerGroupId: string;
            readonly regions: Readonly<Record<string, string>>;
            readonly startFrom: ResolvedViewServerKafkaRuntimeOptions<
              typeof kafkaBackedViewServer.topics
            >["startFrom"];
            readonly topics: Readonly<
              Record<
                string,
                { readonly regions: ReadonlyArray<string>; readonly viewServerTopic: string }
              >
            >;
          }
        | undefined;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof kafkaBackedViewServer.topics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.void,
          }),
        makeKafkaIngress: (_config, _client, options) => {
          kafkaOptionsSummary = {
            consume: options.consume,
            consumerGroupId: options.consumerGroupId,
            regions: options.regions,
            startFrom: options.startFrom,
            topics: Object.fromEntries(
              Object.entries(options.topics).map(([sourceTopic, topic]) => [
                sourceTopic,
                {
                  regions: topic.regions,
                  viewServerTopic: topic.viewServerTopic,
                },
              ]),
            ),
          };
          return Effect.succeed({
            close: Effect.void,
          });
        },
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        kafkaBackedViewServer,
        {
          kafka: {
            consumerGroupId: "view-server-test-runtime",
          },
        },
      );

      expect({
        consume: kafkaOptionsSummary?.consume,
        consumerGroupId: kafkaOptionsSummary?.consumerGroupId,
        regions: kafkaOptionsSummary?.regions,
        startFrom: kafkaOptionsSummary?.startFrom,
        topics: kafkaOptionsSummary?.topics,
      }).toStrictEqual({
        consume: {
          consumerGroupId: "view-server-test-runtime",
          fallbackMode: "earliest",
          mode: "committed",
        },
        consumerGroupId: "view-server-test-runtime",
        regions: nullRecord([["local", "localhost:9092"]]),
        startFrom: {
          committedConsumerGroup: "view-server-test-runtime",
        },
        topics: {
          "orders-source": {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });

      yield* runtime.close;
    }),
  );

  it.live("resolves topic-owned gRPC runtime options and starts configured ingress", () =>
    Effect.gen(function* () {
      type TopicOwnedGrpcTopics = typeof grpcTopicOwnedSourceViewServer.topics;
      type RuntimeDependencies = ViewServerRuntimeDependencies<TopicOwnedGrpcTopics>;
      let grpcOptionsSummary:
        | {
            readonly clientBaseUrls: Readonly<Record<string, string>>;
            readonly clientNames: ReadonlyArray<string>;
            readonly feeds: Readonly<
              Record<
                string,
                {
                  readonly client: string;
                  readonly lifecycle: string;
                  readonly method: string;
                  readonly topic: string;
                }
              >
            >;
            readonly materializedReconnect: ResolvedViewServerGrpcRuntimeOptions<TopicOwnedGrpcTopics>["materializedReconnect"];
          }
        | undefined;
      let grpcHealthLedgerCreated = false;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<TopicOwnedGrpcTopics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.void,
          }),
        makeGrpcHealthLedger: (config, options) => {
          grpcHealthLedgerCreated =
            config.topics === grpcTopicOwnedSourceViewServer.topics &&
            options.clientBaseUrls["orders"] === "https://orders.example.test";
          return makeDefaultRuntimeDependencies<TopicOwnedGrpcTopics>().makeGrpcHealthLedger(
            config,
            options,
          );
        },
        makeGrpcIngress: (_config, _client, _requestHealthRefresh, options) => {
          grpcOptionsSummary = {
            clientBaseUrls: options.clientBaseUrls,
            clientNames: Object.keys(options.clients),
            feeds: Object.fromEntries(
              Object.entries(options.feeds).map(([feedName, resolvedFeed]) => [
                feedName,
                {
                  client: resolvedFeed.client,
                  lifecycle: resolvedFeed.lifecycle,
                  method: resolvedFeed.method,
                  topic: resolvedFeed.topic,
                },
              ]),
            ),
            materializedReconnect: options.materializedReconnect,
          };
          return Effect.succeed({
            close: Effect.void,
          });
        },
      };

      const grpcRuntimeOptions = {
        grpc: {
          materializedReconnect: {
            delay: "100 millis",
            maxReconnects: 5,
          },
        },
      };
      const runtime = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        grpcTopicOwnedSourceViewServer,
        grpcRuntimeOptions,
      );

      expect(grpcHealthLedgerCreated).toBe(true);
      expect({
        clientBaseUrls: grpcOptionsSummary?.clientBaseUrls,
        clientNames: grpcOptionsSummary?.clientNames,
        feeds: grpcOptionsSummary?.feeds,
        materializedReconnect: grpcOptionsSummary?.materializedReconnect,
      }).toStrictEqual({
        clientBaseUrls: nullRecord([["orders", "https://orders.example.test"]]),
        clientNames: ["orders"],
        feeds: {
          orders: {
            client: "orders",
            lifecycle: "materialized",
            method: "streamOrders",
            topic: "orders",
          },
          routedOrders: {
            client: "orders",
            lifecycle: "leased",
            method: "streamOrders",
            topic: "routedOrders",
          },
        },
        materializedReconnect: {
          delay: "100 millis",
          maxReconnects: 5,
        },
      });

      yield* runtime.close;
    }),
  );

  it.live("resolves gRPC runtime clients from topic config", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<
        typeof grpcTopicOwnedSourceViewServer.topics
      >;
      let grpcOptionsSummary:
        | {
            readonly clientBaseUrls: Readonly<Record<string, string>>;
            readonly clientNames: ReadonlyArray<string>;
          }
        | undefined;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof grpcTopicOwnedSourceViewServer.topics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.void,
          }),
        makeGrpcIngress: (_config, _client, _requestHealthRefresh, options) => {
          grpcOptionsSummary = {
            clientBaseUrls: options.clientBaseUrls,
            clientNames: Object.keys(options.clients),
          };
          return Effect.succeed({
            close: Effect.void,
          });
        },
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        grpcTopicOwnedSourceViewServer,
      );

      expect(grpcOptionsSummary).toStrictEqual({
        clientBaseUrls: nullRecord([["orders", "https://orders.example.test"]]),
        clientNames: ["orders"],
      });

      yield* runtime.close;
    }),
  );

  it.live("derives gRPC runtime feeds from topic-owned source bindings", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<
        typeof grpcTopicOwnedSourceViewServer.topics
      >;
      let grpcOptionsSummary:
        | {
            readonly clientBaseUrls: Readonly<Record<string, string>>;
            readonly clientNames: ReadonlyArray<string>;
            readonly feeds: Readonly<
              Record<
                string,
                {
                  readonly client: string;
                  readonly lifecycle: string;
                  readonly method: string;
                  readonly routeBy?: ReadonlyArray<string>;
                  readonly topic: string;
                }
              >
            >;
          }
        | undefined;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof grpcTopicOwnedSourceViewServer.topics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.void,
          }),
        makeGrpcIngress: (_config, _client, _requestHealthRefresh, options) => {
          grpcOptionsSummary = {
            clientBaseUrls: options.clientBaseUrls,
            clientNames: Object.keys(options.clients),
            feeds: Object.fromEntries(
              Object.entries(options.feeds).map(([feedName, resolvedFeed]) => [
                feedName,
                {
                  client: resolvedFeed.client,
                  lifecycle: resolvedFeed.lifecycle,
                  method: resolvedFeed.method,
                  ...(resolvedFeed.routeBy === undefined ? {} : { routeBy: resolvedFeed.routeBy }),
                  topic: resolvedFeed.topic,
                },
              ]),
            ),
          };
          return Effect.succeed({
            close: Effect.void,
          });
        },
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        grpcTopicOwnedSourceViewServer,
      );

      expect(grpcOptionsSummary).toStrictEqual({
        clientBaseUrls: nullRecord([["orders", "https://orders.example.test"]]),
        clientNames: ["orders"],
        feeds: {
          orders: {
            client: "orders",
            lifecycle: "materialized",
            method: "streamOrders",
            topic: "orders",
          },
          routedOrders: {
            client: "orders",
            lifecycle: "leased",
            method: "streamOrders",
            routeBy: ["region"],
            topic: "routedOrders",
          },
        },
      });

      yield* runtime.close;
    }),
  );
});
