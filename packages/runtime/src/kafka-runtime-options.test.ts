import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, kafka } from "@effect-view-server/config";
import { Config, Effect, Schema } from "effect";
import type { ViewServerRuntimeDependencies } from "./internal";
import { makeDefaultRuntimeDependencies, makeViewServerRuntimeWithDependencies } from "./internal";
import { messageFromUnknown, ViewServerKafkaIngressError } from "./kafka-ingress";
import {
  makeDefaultKafkaRuntimeSourceDependencies,
  makeKafkaRuntimeSourceAdapter,
  resolveKafkaRuntimeSourceOptions as resolveViewServerRuntimeOptions,
} from "./kafka-runtime-source";
import { fetchHealth, nullRecord } from "../test-harness/runtime";

import { Order, Trade, viewServer } from "../test-harness/runtime-config";

describe("Kafka runtime options and health", () => {
  it.effect("resolves explicit Kafka start policies", () =>
    Effect.gen(function* () {
      const regions = {
        local: "localhost:9092",
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

      const earliest = yield* resolveViewServerRuntimeOptions(kafkaBackedViewServer, {
        kafka: {
          consumerGroupId: "view-server-earliest",
          startFrom: "earliest",
        },
      });
      const latest = yield* resolveViewServerRuntimeOptions(kafkaBackedViewServer, {
        kafka: {
          consumerGroupId: "view-server-latest",
          startFrom: "latest",
        },
      });
      const committed = yield* resolveViewServerRuntimeOptions(kafkaBackedViewServer, {
        kafka: {
          consumerGroupId: "view-server-default",
          startFrom: {
            committedConsumerGroup: "view-server-existing-group",
            fallback: "fail",
          },
        },
      });
      const committedWithDefaultFallback = yield* resolveViewServerRuntimeOptions(
        kafkaBackedViewServer,
        {
          kafka: {
            consumerGroupId: "view-server-default-fallback",
            startFrom: {
              committedConsumerGroup: "view-server-existing-default-fallback-group",
            },
          },
        },
      );

      expect({
        committed: committed?.consume,
        committedWithDefaultFallback: committedWithDefaultFallback?.consume,
        earliest: earliest?.consume,
        latest: latest?.consume,
      }).toStrictEqual({
        committed: {
          consumerGroupId: "view-server-existing-group",
          fallbackMode: "fail",
          mode: "committed",
        },
        committedWithDefaultFallback: {
          consumerGroupId: "view-server-existing-default-fallback-group",
          fallbackMode: "earliest",
          mode: "committed",
        },
        earliest: {
          consumerGroupId: "view-server-earliest",
          fallbackMode: "earliest",
          mode: "earliest",
        },
        latest: {
          consumerGroupId: "view-server-latest",
          fallbackMode: "latest",
          mode: "latest",
        },
      });
    }),
  );

  it.effect("derives Kafka source topics from topic-owned config", () =>
    Effect.gen(function* () {
      const regions = {
        local: "localhost:9092",
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

      const options = yield* resolveViewServerRuntimeOptions(kafkaBackedViewServer, {
        kafka: {
          consumerGroupId: "view-server-derived-kafka-source",
        },
      });
      const clonedOptions = yield* resolveViewServerRuntimeOptions(
        { ...kafkaBackedViewServer },
        {
          kafka: {
            consumerGroupId: "view-server-derived-kafka-source-cloned",
          },
        },
      );

      expect({
        consumerGroupId: options?.consumerGroupId,
        regions: options?.regions,
        sourceTopics: Object.keys(options?.topics ?? {}),
        topicRegions: options?.topics["orders-source"]?.regions,
        viewServerTopic: options?.topics["orders-source"]?.viewServerTopic,
        clonedConsumerGroupId: clonedOptions?.consumerGroupId,
        clonedSourceTopics: Object.keys(clonedOptions?.topics ?? {}),
      }).toStrictEqual({
        consumerGroupId: "view-server-derived-kafka-source",
        regions: nullRecord([["local", "localhost:9092"]]),
        sourceTopics: ["orders-source"],
        topicRegions: ["local"],
        viewServerTopic: "orders",
        clonedConsumerGroupId: "view-server-derived-kafka-source-cloned",
        clonedSourceTopics: ["orders-source"],
      });
    }),
  );

  it.effect("reports malformed config-owned Kafka sources as typed runtime errors", () =>
    Effect.gen(function* () {
      const regions = {
        local: "localhost:9092",
      };
      // @ts-expect-error malformed topic-owned Kafka sources are also guarded at runtime.
      const malformedKafkaBackedViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: {},
          },
        },
      });
      const validKafkaBackedViewServer = defineViewServerConfig({
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
      const extraKeyKafkaBackedViewServer = {
        ...validKafkaBackedViewServer,
        topics: {
          ...validKafkaBackedViewServer.topics,
          orders: {
            ...validKafkaBackedViewServer.topics.orders,
            kafkaSource: {
              ...validKafkaBackedViewServer.topics.orders.kafkaSource,
              extra: true,
            },
          },
        },
      };

      const error = yield* Effect.flip(
        resolveViewServerRuntimeOptions(malformedKafkaBackedViewServer, {
          kafka: {
            consumerGroupId: "view-server-malformed-config-kafka-source",
          },
        }),
      );
      const extraKeyError = yield* Effect.flip(
        resolveViewServerRuntimeOptions(extraKeyKafkaBackedViewServer, {
          kafka: {
            consumerGroupId: "view-server-extra-key-config-kafka-source",
          },
        }),
      );

      expect({
        errorIsTyped: error instanceof ViewServerKafkaIngressError,
        message: error.message,
        causeMessage: messageFromUnknown(error.cause),
        extraKeyErrorIsTyped: extraKeyError instanceof ViewServerKafkaIngressError,
        extraKeyMessage: extraKeyError.message,
        extraKeyCauseMessage: messageFromUnknown(extraKeyError.cause),
      }).toStrictEqual({
        errorIsTyped: true,
        message:
          "Invalid topic-owned Kafka source configuration: View Server topic orders has an invalid Kafka source.",
        causeMessage: "View Server topic orders has an invalid Kafka source.",
        extraKeyErrorIsTyped: true,
        extraKeyMessage:
          "Invalid topic-owned Kafka source configuration: View Server topic orders has an invalid Kafka source.",
        extraKeyCauseMessage: "View Server topic orders has an invalid Kafka source.",
      });
    }),
  );

  it.effect("requires Kafka runtime options when topic-owned Kafka sources exist", () =>
    Effect.gen(function* () {
      const regions = {
        local: "localhost:9092",
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

      const error = yield* Effect.flip(resolveViewServerRuntimeOptions(kafkaBackedViewServer));

      expect(error).toStrictEqual(
        new ViewServerKafkaIngressError({
          message:
            "Kafka sources are configured, but runtime options.kafka.consumerGroupId was not provided.",
          cause: "missing-kafka-consumer-group",
        }),
      );
      expect(error.cause).toBe("missing-kafka-consumer-group");
    }),
  );

  it.effect(
    "rejects Kafka runtime options without consumer group when config owns Kafka sources",
    () =>
      Effect.gen(function* () {
        const regions = {
          local: "localhost:9092",
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

        const error = yield* Effect.flip(
          resolveViewServerRuntimeOptions(kafkaBackedViewServer, {
            // @ts-expect-error typed callers must provide consumerGroupId when Kafka options are present.
            kafka: {
              regions,
            },
          }),
        );

        expect(error).toStrictEqual(
          new ViewServerKafkaIngressError({
            message:
              "Kafka sources are configured, but runtime options.kafka.consumerGroupId was not provided.",
            cause: "missing-kafka-consumer-group",
          }),
        );
        expect(error.cause).toBe("missing-kafka-consumer-group");
      }),
  );

  it.effect("rejects explicit runtime Kafka topics", () =>
    Effect.gen(function* () {
      const regions = {
        local: "localhost:9092",
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

      const error = yield* Effect.flip(
        resolveViewServerRuntimeOptions(kafkaBackedViewServer, {
          kafka: {
            consumerGroupId: "view-server-mixed-kafka-sources",
            // @ts-expect-error runtime-owned Kafka topics are not supported.
            topics: {},
          },
        }),
      );

      expect(error).toStrictEqual(
        new ViewServerKafkaIngressError({
          message:
            "runtime options.kafka.topics is not supported; declare Kafka sources on View Server topics with kafkaSource.",
          cause: "unsupported-runtime-kafka-topics",
        }),
      );
      expect(error.cause).toBe("unsupported-runtime-kafka-topics");
    }),
  );

  it.effect("rejects runtime Kafka options without configured source topics", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        resolveViewServerRuntimeOptions(viewServer, {
          kafka: {
            consumerGroupId: "view-server-no-kafka-topics",
            regions: {
              local: "localhost:9092",
            },
          },
        }),
      );

      expect(error).toStrictEqual(
        new ViewServerKafkaIngressError({
          message:
            "runtime options.kafka was provided, but no topic-owned Kafka sources were declared; remove options.kafka or add kafkaSource to a View Server topic.",
          cause: "missing-kafka-source-topics",
        }),
      );
      expect(error.cause).toBe("missing-kafka-source-topics");
    }),
  );

  it.effect("rejects source-free Kafka options before resolving Kafka region config", () =>
    Effect.gen(function* () {
      const sourceFreeConfigBackedViewServer = defineViewServerConfig({
        kafka: {
          local: Config.string("VIEW_SERVER_RUNTIME_TEST_UNSET_KAFKA_BOOTSTRAP"),
        },
        topics: {
          orders: {
            schema: Order,
            key: "id",
          },
        },
      });

      const error = yield* Effect.flip(
        resolveViewServerRuntimeOptions(sourceFreeConfigBackedViewServer, {
          kafka: {
            consumerGroupId: "view-server-no-kafka-topics-config-first",
          },
        }),
      );

      expect(error).toStrictEqual(
        new ViewServerKafkaIngressError({
          message:
            "runtime options.kafka was provided, but no topic-owned Kafka sources were declared; remove options.kafka or add kafkaSource to a View Server topic.",
          cause: "missing-kafka-source-topics",
        }),
      );
      expect(error.cause).toBe("missing-kafka-source-topics");
    }),
  );

  it.effect("rejects duplicate topic-owned Kafka source topic names", () =>
    Effect.gen(function* () {
      const regions = {
        local: "localhost:9092",
      };
      const duplicateKafkaSourceViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "shared-source",
              regions: ["local"],
              value: kafka.json(() => Schema.toCodecJson(Order)),
              key: kafka.stringKey(),
              rowKey: ({ key }) => key,
              map: ({ value }) => ({
                price: value.price,
              }),
            }),
          },
          trades: {
            schema: Trade,
            key: "id",
            kafkaSource: kafka.source({
              topic: "shared-source",
              regions: ["local"],
              value: kafka.json(() => Schema.toCodecJson(Trade)),
              key: kafka.stringKey(),
              rowKey: ({ key }) => key,
              map: ({ value }) => ({
                symbol: value.symbol,
              }),
            }),
          },
        },
      });

      const error = yield* Effect.flip(
        resolveViewServerRuntimeOptions(duplicateKafkaSourceViewServer, {
          kafka: {
            consumerGroupId: "view-server-duplicate-kafka-source",
          },
        }),
      );

      expect(error).toStrictEqual(
        new ViewServerKafkaIngressError({
          message: "Kafka source topic is configured more than once: shared-source",
          cause: "shared-source",
          sourceTopic: "shared-source",
        }),
      );
      expect(error.cause).toBe("shared-source");
    }),
  );

  it.effect("requires Kafka regions for topic-owned Kafka sources", () =>
    Effect.gen(function* () {
      // @ts-expect-error runtime still guards JavaScript callers that omit concrete Kafka regions.
      const kafkaBackedViewServer = defineViewServerConfig({
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

      const error = yield* Effect.flip(
        resolveViewServerRuntimeOptions(kafkaBackedViewServer, {
          kafka: {
            consumerGroupId: "view-server-missing-kafka-regions",
          },
        }),
      );

      expect(error).toStrictEqual(
        new ViewServerKafkaIngressError({
          message:
            "Kafka sources are configured, but no Kafka regions were provided on config.kafka or runtime options.kafka.regions.",
          cause: "missing-kafka-regions",
        }),
      );
      expect(error.cause).toBe("missing-kafka-regions");
    }),
  );

  it.effect("rejects topic-owned Kafka sources that reference unresolved regions", () =>
    Effect.gen(function* () {
      const kafkaBackedViewServer = defineViewServerConfig({
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

      const error = yield* Effect.flip(
        resolveViewServerRuntimeOptions(kafkaBackedViewServer, {
          kafka: {
            consumerGroupId: "view-server-unresolved-kafka-region",
            regions: {
              // @ts-expect-error runtime still guards JavaScript callers with unknown region keys.
              remote: "localhost:9092",
            },
          },
        }),
      );

      expect(error).toStrictEqual(
        new ViewServerKafkaIngressError({
          message: "Kafka source topic orders-source references unknown Kafka region: local",
          cause: {
            region: "local",
            sourceTopic: "orders-source",
          },
          region: "local",
          sourceTopic: "orders-source",
        }),
      );
      expect(error.cause).toStrictEqual({
        region: "local",
        sourceTopic: "orders-source",
      });
    }),
  );

  it.live("preserves dangerous Kafka runtime option keys", () =>
    Effect.gen(function* () {
      const protoRegion = "__proto__";
      const regions = {
        [protoRegion]: Config.succeed("localhost:9092"),
      };
      const kafkaBackedViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "__proto__",
              regions: ["__proto__"],
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
            readonly consumerGroupId: string;
            readonly regions: Readonly<Record<string, string>>;
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
        sourceAdapters: [
          makeKafkaRuntimeSourceAdapter({
            ...makeDefaultKafkaRuntimeSourceDependencies<typeof kafkaBackedViewServer.topics>(),
            makeIngress: (_config, _client, options) => {
              kafkaOptionsSummary = {
                consumerGroupId: options.consumerGroupId,
                regions: options.regions,
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
          }),
        ],
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.void,
          }),
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        kafkaBackedViewServer,
        {
          kafka: {
            consumerGroupId: "view-server-dangerous-key-test-runtime",
          },
        },
      );

      expect(Object.hasOwn(kafkaOptionsSummary?.regions ?? {}, "__proto__")).toBe(true);
      expect(Object.hasOwn(kafkaOptionsSummary?.topics ?? {}, "__proto__")).toBe(true);
      expect({
        consumerGroupId: kafkaOptionsSummary?.consumerGroupId,
        region: kafkaOptionsSummary?.regions["__proto__"],
        topicRegions: kafkaOptionsSummary?.topics["__proto__"]?.regions,
        viewServerTopic: kafkaOptionsSummary?.topics["__proto__"]?.viewServerTopic,
      }).toStrictEqual({
        consumerGroupId: "view-server-dangerous-key-test-runtime",
        region: "localhost:9092",
        topicRegions: ["__proto__"],
        viewServerTopic: "orders",
      });

      yield* runtime.close;
    }),
  );

  it.live("returns unavailable health when Kafka ingress is degraded", () =>
    Effect.gen(function* () {
      const regions = {
        local: "localhost:9092",
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
      const dependencies: ViewServerRuntimeDependencies<typeof kafkaBackedViewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof kafkaBackedViewServer.topics>(),
        sourceAdapters: [
          makeKafkaRuntimeSourceAdapter({
            ...makeDefaultKafkaRuntimeSourceDependencies<typeof kafkaBackedViewServer.topics>(),
            makeIngress: (_config, _client, _options, observation) =>
              observation.regionDisconnected("local", "lost").pipe(
                Effect.as({
                  close: Effect.void,
                }),
              ),
          }),
        ],
      };

      yield* Effect.acquireUseRelease(
        makeViewServerRuntimeWithDependencies(dependencies, kafkaBackedViewServer, {
          kafka: {
            consumerGroupId: "view-server-test-degraded",
          },
        }),
        (runtime) =>
          Effect.gen(function* () {
            const health = yield* fetchHealth(runtime.healthUrl);

            expect(health.response.status).toBe(503);
            expect(health.health.status).toBe("degraded");
            expect(health.health.engine.topics.orders.rowCount).toBe(0);
          }),
        (runtime) => runtime.close,
      );
    }),
  );
});
