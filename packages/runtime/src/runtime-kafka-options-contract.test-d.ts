import { describe, expectTypeOf, it } from "@effect/vitest";
import type { RuntimeRegions } from "@effect-view-server/config";
import { Effect } from "effect";
import {
  makeViewServerRuntime,
  runViewServerRuntime,
  type ViewServerRuntime,
  type ViewServerRuntimeOptionsInput,
} from "./index";

import {
  kafkaOwnedViewServer,
  usaKafkaRegions,
  viewServer,
} from "../test-harness/runtime-type-contracts";

describe("Runtime Kafka option contracts", () => {
  it("accepts valid contracts and rejects invalid contracts", () => {
    const londonKafkaRegions = {
      london: "localhost:9093",
    };
    const broadKafkaRegions: RuntimeRegions = usaKafkaRegions;

    const invalidSourceFreeRuntimeWithKafka = makeViewServerRuntime(viewServer, {
      // @ts-expect-error source-free runtimes reject Kafka options.
      kafka: {
        consumerGroupId: "view-server-source-free-type-test",
        regions: usaKafkaRegions,
      },
    });

    const kafkaOwnedRuntimeWithExplicitRegionsEffect = makeViewServerRuntime(kafkaOwnedViewServer, {
      kafka: {
        consumerGroupId: "view-server-kafka-owned-explicit-regions-type-test",
        regions: usaKafkaRegions,
      },
    });

    const invalidKafkaOwnedRuntimeWithWrongRegions = makeViewServerRuntime(kafkaOwnedViewServer, {
      kafka: {
        consumerGroupId: "view-server-kafka-owned-wrong-regions-type-test",
        // @ts-expect-error runtime Kafka regions for a source-owned topic must include the source regions.
        regions: londonKafkaRegions,
      },
    });

    const invalidKafkaOwnedRuntimeWithBroadRegions = makeViewServerRuntime(kafkaOwnedViewServer, {
      kafka: {
        consumerGroupId: "view-server-kafka-owned-broad-regions-type-test",
        // @ts-expect-error source-owned Kafka runtime regions must be exact enough to prove source coverage.
        regions: broadKafkaRegions,
      },
    });

    // @ts-expect-error Kafka-owned source configs require runtime Kafka options with a consumer group.
    const invalidKafkaOwnedRuntimeWithoutOptions = makeViewServerRuntime(kafkaOwnedViewServer);

    const invalidKafkaOwnedRuntimeWithExplicitTopics = makeViewServerRuntime(kafkaOwnedViewServer, {
      kafka: {
        consumerGroupId: "view-server-kafka-owned-explicit-topics",
        // @ts-expect-error Kafka-owned source configs reject explicit runtime Kafka topics.
        topics: {},
      },
    });

    const invalidSourceFreeRunRuntimeWithKafka = runViewServerRuntime(viewServer, {
      // @ts-expect-error source-free runtimes reject Kafka options.
      kafka: {
        consumerGroupId: "view-server-source-free-run-type-test",
        regions: usaKafkaRegions,
      },
    });

    type SourceFreeRuntimeOptionsInput = ViewServerRuntimeOptionsInput<typeof viewServer.topics>;
    const validSourceFreeRuntimeOptionsInput: SourceFreeRuntimeOptionsInput = {
      websocketPort: 3_800,
    };
    const invalidSourceFreeRuntimeOptionsInput: SourceFreeRuntimeOptionsInput = {
      // @ts-expect-error exported source-free runtime options reject Kafka options.
      kafka: {
        consumerGroupId: "view-server-source-free-exported-input-type-test",
        regions: usaKafkaRegions,
      },
    };

    const runtimeWithKafkaStart = makeViewServerRuntime(kafkaOwnedViewServer, {
      kafka: {
        consumerGroupId: "view-server-type-test",
        startFrom: {
          committedConsumerGroup: "view-server-existing-group",
          fallback: "fail",
        },
      },
    });

    const invalidKafkaStartFrom = makeViewServerRuntime(kafkaOwnedViewServer, {
      kafka: {
        consumerGroupId: "view-server-type-test",
        // @ts-expect-error runtime Kafka startFrom only accepts earliest, latest, or committed group config.
        startFrom: "middle",
      },
    });

    const invalidCommittedKafkaStartFallback = makeViewServerRuntime(kafkaOwnedViewServer, {
      kafka: {
        consumerGroupId: "view-server-type-test",
        // @ts-expect-error committed Kafka start fallback must be earliest, latest, or fail.
        startFrom: {
          committedConsumerGroup: "view-server-existing-group",
          fallback: "middle",
        },
      },
    });

    const invalidCommittedKafkaStartMissingGroup = makeViewServerRuntime(kafkaOwnedViewServer, {
      kafka: {
        consumerGroupId: "view-server-type-test",
        // @ts-expect-error committed Kafka start config requires committedConsumerGroup.
        startFrom: {
          fallback: "earliest",
        },
      },
    });

    const invalidCommittedKafkaStartKey = makeViewServerRuntime(kafkaOwnedViewServer, {
      kafka: {
        consumerGroupId: "view-server-type-test",
        startFrom: {
          committedConsumerGroup: "view-server-existing-group",
          // @ts-expect-error committed Kafka start config rejects unknown keys.
          committedConsumerGroupId: "view-server-typo",
        },
      },
    });

    const invalidKafkaOptionKey = makeViewServerRuntime(kafkaOwnedViewServer, {
      kafka: {
        consumerGroupId: "view-server-type-test",
        // @ts-expect-error runtime Kafka options reject misspelled consumer group keys.
        consumerGroupID: "view-server-typo",
      },
    });

    const invalidMissingKafkaConsumerGroup = makeViewServerRuntime(kafkaOwnedViewServer, {
      // @ts-expect-error runtime Kafka options require an explicit per-runtime consumer group id.
      kafka: {},
    });

    expectTypeOf<Effect.Success<typeof runtimeWithKafkaStart>>().toMatchTypeOf<
      ViewServerRuntime<typeof kafkaOwnedViewServer.topics>
    >();

    expectTypeOf<Effect.Success<typeof kafkaOwnedRuntimeWithExplicitRegionsEffect>>().toMatchTypeOf<
      ViewServerRuntime<typeof kafkaOwnedViewServer.topics>
    >();

    expectTypeOf(validSourceFreeRuntimeOptionsInput).toMatchTypeOf<SourceFreeRuntimeOptionsInput>();

    expectTypeOf(invalidSourceFreeRuntimeWithKafka).not.toBeAny();

    expectTypeOf(invalidKafkaOwnedRuntimeWithBroadRegions).not.toBeAny();

    expectTypeOf(invalidSourceFreeRunRuntimeWithKafka).not.toBeAny();

    expectTypeOf(invalidSourceFreeRuntimeOptionsInput).not.toBeAny();

    expectTypeOf(invalidKafkaOwnedRuntimeWithWrongRegions).not.toBeAny();

    expectTypeOf(invalidKafkaStartFrom).not.toBeAny();

    expectTypeOf(invalidCommittedKafkaStartFallback).not.toBeAny();

    expectTypeOf(invalidCommittedKafkaStartMissingGroup).not.toBeAny();

    expectTypeOf(invalidCommittedKafkaStartKey).not.toBeAny();

    expectTypeOf(invalidKafkaOptionKey).not.toBeAny();

    expectTypeOf(invalidMissingKafkaConsumerGroup).not.toBeAny();

    expectTypeOf(invalidKafkaOwnedRuntimeWithoutOptions).not.toBeAny();

    expectTypeOf(invalidKafkaOwnedRuntimeWithExplicitTopics).not.toBeAny();
  });
});
