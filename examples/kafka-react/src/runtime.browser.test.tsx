import { describe, expect, it, vi } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { viewServer } from "./view-server.config";

describe("Kafka React example runtime", () => {
  it("composes the topic-owned config with the Kafka runtime options", async () => {
    const runtimeProgram = Effect.void;
    const kafkaLayer = Layer.empty;
    const layer = vi.fn(() => kafkaLayer);
    const runMain = vi.fn();
    const runViewServerRuntime = vi.fn(() => runtimeProgram);
    vi.doMock("@effect/platform-node", () => ({
      NodeRuntime: { runMain },
    }));
    vi.doMock("effect-view-server/runtime", () => ({
      runViewServerRuntime,
    }));
    vi.doMock("effect-view-server/kafka/node", () => ({
      kafkaNode: { layer },
    }));

    await import("./runtime");

    expect(layer.mock.calls).toStrictEqual([
      [
        viewServer,
        {
          consumerGroupPrefix: "view-server-example-kafka-react",
          regions: {
            usa: { bootstrapServers: "127.0.0.1:9092" },
            london: { bootstrapServers: "127.0.0.1:9094" },
          },
        },
      ],
    ]);
    expect(runViewServerRuntime.mock.calls).toStrictEqual([
      [
        viewServer,
        {
          host: "127.0.0.1",
          websocketPort: 8080,
          tcpPublishHost: "127.0.0.1",
          tcpPublishPort: 8081,
        },
      ],
    ]);
    expect(runMain).toHaveBeenCalledOnce();
    expect(runMain).not.toHaveBeenCalledWith(runtimeProgram);
  });
});
