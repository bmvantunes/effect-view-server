import { describe, expectTypeOf, it } from "@effect/vitest";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { Config, Effect, Layer, Schema, Scope } from "effect";
import { SourceAdapter } from "effect-view-server/source-adapter";
import {
  KafkaSourceAdapter,
  kafka,
  type KafkaAdapterFailure,
  type KafkaRegionMetrics,
} from "./contract";
import {
  layer,
  layerConfig,
  type KafkaBrokerContractValidationFailure,
  type KafkaRequiredRegion,
} from "./node";
import type {
  KafkaServerRecord,
  KafkaServerRegion,
  KafkaServerRegionAcquireInput,
  KafkaServerRegionConsumer,
  KafkaServerRegionMetricsInput,
} from "./server";

const Row = Schema.Struct({
  id: ViewServerId,
  price: Schema.Number,
  region: Schema.String,
});

const config = defineViewServerConfig({
  topics: {
    orders: {
      schema: Row,
      source: kafka.source({
        cleanupPolicy: "delete",
        retentionPolicy: "Infinity",
        topic: "source-orders",
        regions: ["eu", "us"],
        key: kafka.string(),
        value: kafka.json(() =>
          Schema.toCodecJson(
            Schema.Struct({
              price: Schema.Number,
            }),
          ),
        ),
        localRowKey: ({ key }) => key,
        map: ({ value, region }) => ({
          price: value.price,
          region: String(region),
        }),
        startFrom: "earliest",
      }),
    },
  },
});

const sourceFreeConfig = defineViewServerConfig({
  topics: {
    orders: {
      schema: Row,
    },
  },
});

const OtherAdapter = SourceAdapter.make({
  identity: {
    name: "other",
    version: "1",
  },
  failure: Schema.String,
  materialized: {
    metrics: Schema.Struct({}),
    rejectionLocation: Schema.Struct({}),
    definitionOptions: SourceAdapter.definitionOptions<undefined>(),
  },
  leased: undefined,
});
const otherSource = OtherAdapter.materializedSource<{ readonly id: string }>(undefined);
type ConditionalSourceViewServer = {
  readonly topics: {
    readonly orders: {
      readonly source: (typeof config)["topics"]["orders"]["source"] | typeof otherSource;
    };
  };
};

declare const unsafeAny: any;
declare const euRegion: KafkaServerRegion<"eu">;
declare const euAcquireInput: KafkaServerRegionAcquireInput<"eu">;
declare const euMetricsInput: KafkaServerRegionMetricsInput<"eu">;
declare const apacRecord: KafkaServerRecord<"apac">;

describe("Kafka Node type contract", () => {
  it("binds every server seam result and failure to its exact Region", () => {
    const acquisition = euRegion.acquire(euAcquireInput);
    const metrics = euRegion.metrics(euMetricsInput);
    expectTypeOf(acquisition).toEqualTypeOf<
      Effect.Effect<KafkaServerRegionConsumer<"eu">, KafkaAdapterFailure<"eu">, Scope.Scope>
    >();
    expectTypeOf(metrics).toEqualTypeOf<Effect.Effect<KafkaRegionMetrics<"eu">>>();
    expectTypeOf<KafkaServerRecord<"eu">["metadata"]["sourceRegion"]>().toEqualTypeOf<"eu">();
    expectTypeOf<Effect.Error<ReturnType<KafkaServerRecord<"eu">["settlement"]>>>().toEqualTypeOf<
      KafkaAdapterFailure<"eu">
    >();
    expectTypeOf<
      Extract<Effect.Error<typeof acquisition>, { readonly region: string }>["region"]
    >().toEqualTypeOf<"eu">();
    // @ts-expect-error records from another Region cannot cross the server seam.
    const wrongRecord: KafkaServerRecord<"eu"> = apacRecord;
    expectTypeOf(wrongRecord).toEqualTypeOf<KafkaServerRecord<"eu">>();
  });

  it("requires all and only source Regions", () => {
    expectTypeOf<KafkaRequiredRegion<typeof config>>().toEqualTypeOf<"eu" | "us">();
    expectTypeOf<KafkaRequiredRegion<ConditionalSourceViewServer>>().toEqualTypeOf<"eu" | "us">();
    const runtimeLayer = layer(config, {
      consumerGroupPrefix: "replica",
      retentionSweepInterval: "15 minutes",
      regions: {
        eu: { bootstrapServers: "eu:9092" },
        us: { bootstrapServers: ["us:9092"] },
      },
    });
    expectTypeOf(runtimeLayer).toEqualTypeOf<
      Layer.Layer<
        import("effect").Context.Service.Identifier<typeof KafkaSourceAdapter.runtimeService>,
        KafkaBrokerContractValidationFailure
      >
    >();
    const securedLayer = layer(config, {
      consumerGroupPrefix: "replica",
      regions: {
        eu: {
          bootstrapServers: "eu:9092",
          sasl: {
            mechanism: "PLAIN",
            username: "plain-user",
            password: "plain-password",
          },
          tls: {
            ca: ["root-ca", Uint8Array.from([1, 2, 3])],
            cert: "client-cert",
            key: Uint8Array.from([4, 5, 6]),
            rejectUnauthorized: true,
            serverName: "eu.kafka.internal",
          },
        },
        us: {
          bootstrapServers: "us:9092",
          sasl: {
            mechanism: "SCRAM-SHA-256",
            username: "scram-user",
            password: "scram-password",
          },
        },
      },
    });
    const alternateSecuredLayer = layer(config, {
      consumerGroupPrefix: "replica",
      regions: {
        eu: {
          bootstrapServers: "eu:9092",
          sasl: {
            mechanism: "SCRAM-SHA-512",
            username: "scram-user",
            password: "scram-password",
          },
        },
        us: {
          bootstrapServers: "us:9092",
          sasl: {
            mechanism: "OAUTHBEARER",
            token: "oauth-token",
          },
        },
      },
    });
    expectTypeOf(securedLayer).toEqualTypeOf<typeof runtimeLayer>();
    expectTypeOf(alternateSecuredLayer).toEqualTypeOf<typeof runtimeLayer>();

    layer(config, {
      consumerGroupPrefix: "replica",
      // @ts-expect-error retention sweep cadence accepts only Effect Duration inputs.
      retentionSweepInterval: true,
      regions: {
        eu: { bootstrapServers: "eu:9092" },
        us: { bootstrapServers: "us:9092" },
      },
    });
    layer(config, {
      consumerGroupPrefix: "replica",
      // @ts-expect-error every referenced Region is required.
      regions: {
        eu: { bootstrapServers: "eu:9092" },
      },
    });
    // @ts-expect-error unreferenced Regions are rejected.
    layer(config, {
      consumerGroupPrefix: "replica",
      regions: {
        eu: { bootstrapServers: "eu:9092" },
        us: { bootstrapServers: "us:9092" },
        apac: { bootstrapServers: "apac:9092" },
      },
    });
    // @ts-expect-error Region options are exact.
    layer(config, {
      consumerGroupPrefix: "replica",
      regions: {
        eu: {
          bootstrapServers: "eu:9092",
          extra: true,
        },
        us: { bootstrapServers: "us:9092" },
      },
    });

    // @ts-expect-error aggregate Layer options cannot be any.
    layer(config, unsafeAny);
    const variableWithExtraRegionOption = {
      consumerGroupPrefix: "replica",
      regions: {
        eu: { bootstrapServers: "eu:9092", extra: true },
        us: { bootstrapServers: "us:9092" },
      },
    };
    // @ts-expect-error Region options remain exact through variables.
    layer(config, variableWithExtraRegionOption);
    const variableWithExtraSaslOption = {
      consumerGroupPrefix: "replica",
      regions: {
        eu: {
          bootstrapServers: "eu:9092",
          sasl: {
            mechanism: "PLAIN" as const,
            username: "user",
            password: "secret",
            extra: true,
          },
        },
        us: { bootstrapServers: "us:9092" },
      },
    };
    // @ts-expect-error nested SASL options remain exact through variables.
    layer(config, variableWithExtraSaslOption);
    layer(config, {
      consumerGroupPrefix: "replica",
      regions: {
        eu: {
          bootstrapServers: "eu:9092",
          // @ts-expect-error unsupported GSSAPI cannot be configured.
          sasl: { mechanism: "GSSAPI" },
        },
        us: { bootstrapServers: "us:9092" },
      },
    });
    layer(config, {
      consumerGroupPrefix: "replica",
      regions: {
        eu: {
          bootstrapServers: "eu:9092",
          // @ts-expect-error username/password mechanisms require both credentials.
          sasl: { mechanism: "SCRAM-SHA-256", username: "user" },
        },
        us: { bootstrapServers: "us:9092" },
      },
    });
    layer(config, {
      consumerGroupPrefix: "replica",
      regions: {
        eu: {
          bootstrapServers: "eu:9092",
          // @ts-expect-error OAUTHBEARER requires a token.
          sasl: { mechanism: "OAUTHBEARER" },
        },
        us: { bootstrapServers: "us:9092" },
      },
    });
    // @ts-expect-error nested Region values cannot be any.
    layer(config, {
      consumerGroupPrefix: "replica",
      regions: {
        eu: { bootstrapServers: unsafeAny },
        us: { bootstrapServers: "us:9092" },
      },
    });
  });

  it("preserves Config errors for aggregate Layer config", () => {
    const configured = layerConfig(config, {
      consumerGroupPrefix: Config.succeed("replica"),
      retentionSweepInterval: Config.succeed("15 minutes"),
      regions: {
        eu: {
          bootstrapServers: Config.succeed("eu:9092"),
        },
        us: {
          bootstrapServers: Config.succeed("us:9092"),
        },
      },
    });
    expectTypeOf(configured).toEqualTypeOf<
      Layer.Layer<
        import("effect").Context.Service.Identifier<typeof KafkaSourceAdapter.runtimeService>,
        Config.ConfigError | KafkaBrokerContractValidationFailure
      >
    >();
    // @ts-expect-error aggregate Config Layer options cannot be any.
    layerConfig(config, unsafeAny);

    layerConfig(config, {
      consumerGroupPrefix: Config.succeed("replica"),
      // @ts-expect-error Config-wrapped retention sweep cadence accepts only Effect Duration inputs.
      retentionSweepInterval: Config.succeed(true),
      regions: {
        eu: { bootstrapServers: Config.succeed("eu:9092") },
        us: { bootstrapServers: Config.succeed("us:9092") },
      },
    });

    const configWithTopLevelExtra = {
      consumerGroupPrefix: Config.succeed("replica"),
      regions: {
        eu: { bootstrapServers: Config.succeed("eu:9092") },
        us: { bootstrapServers: Config.succeed("us:9092") },
      },
      extra: Config.succeed(true),
    };
    // @ts-expect-error Config-wrapped aggregate options remain exact.
    layerConfig(config, configWithTopLevelExtra);

    const configWithExtraRegion = {
      consumerGroupPrefix: Config.succeed("replica"),
      regions: {
        eu: { bootstrapServers: Config.succeed("eu:9092") },
        us: { bootstrapServers: Config.succeed("us:9092") },
        apac: { bootstrapServers: Config.succeed("apac:9092") },
      },
    };
    // @ts-expect-error Config-wrapped Regions remain exact.
    layerConfig(config, configWithExtraRegion);

    const configWithExtraRegionField = {
      consumerGroupPrefix: Config.succeed("replica"),
      regions: {
        eu: {
          bootstrapServers: Config.succeed("eu:9092"),
          extra: Config.succeed(true),
        },
        us: { bootstrapServers: Config.succeed("us:9092") },
      },
    };
    // @ts-expect-error Config-wrapped Region options remain exact.
    layerConfig(config, configWithExtraRegionField);

    const configWithExtraSaslField = {
      consumerGroupPrefix: Config.succeed("replica"),
      regions: {
        eu: {
          bootstrapServers: Config.succeed("eu:9092"),
          sasl: Config.succeed({
            mechanism: "PLAIN" as const,
            username: "user",
            password: "secret",
            extra: true,
          }),
        },
        us: { bootstrapServers: Config.succeed("us:9092") },
      },
    };
    // @ts-expect-error Config-wrapped nested SASL options remain exact.
    layerConfig(config, configWithExtraSaslField);

    const configWithAnyBroker = {
      consumerGroupPrefix: Config.succeed("replica"),
      regions: Config.succeed({
        eu: { bootstrapServers: unsafeAny },
        us: { bootstrapServers: "us:9092" },
      }),
    };
    // @ts-expect-error nested any cannot cross a Config wrapper.
    layerConfig(config, configWithAnyBroker);

    const configWithAnySasl = {
      consumerGroupPrefix: Config.succeed("replica"),
      regions: {
        eu: {
          bootstrapServers: Config.succeed("eu:9092"),
          sasl: Config.succeed(unsafeAny),
        },
        us: { bootstrapServers: Config.succeed("us:9092") },
      },
    };
    // @ts-expect-error Config-wrapped SASL cannot be any.
    layerConfig(config, configWithAnySasl);

    const configWithAnyToken = {
      consumerGroupPrefix: Config.succeed("replica"),
      regions: {
        eu: {
          bootstrapServers: Config.succeed("eu:9092"),
          sasl: Config.succeed({
            mechanism: "OAUTHBEARER" as const,
            token: unsafeAny,
          }),
        },
        us: { bootstrapServers: Config.succeed("us:9092") },
      },
    };
    // @ts-expect-error Config-wrapped SASL fields cannot be any.
    layerConfig(config, configWithAnyToken);

    const wholeConfigWithExtra = Config.succeed({
      consumerGroupPrefix: "replica",
      regions: {
        eu: { bootstrapServers: "eu:9092" },
        us: { bootstrapServers: "us:9092" },
      },
      extra: true,
    });
    // @ts-expect-error a whole Config wrapper cannot hide extra fields.
    layerConfig(config, wholeConfigWithExtra);
  });

  it("rejects View Server Configs without a Kafka Source Definition", () => {
    // @ts-expect-error a Kafka aggregate Layer requires at least one Kafka Source Definition.
    layer(sourceFreeConfig, {
      consumerGroupPrefix: "replica",
      regions: {},
    });
    // @ts-expect-error a Kafka Config Layer requires at least one Kafka Source Definition.
    layerConfig(sourceFreeConfig, {
      consumerGroupPrefix: Config.succeed("replica"),
      regions: {},
    });
  });
});
