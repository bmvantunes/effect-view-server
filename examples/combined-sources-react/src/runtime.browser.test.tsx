import { describe, expect, it, vi } from "@effect/vitest";
import { Effect } from "effect";
import { viewServer } from "./view-server.config";

describe("combined-sources React example runtime", () => {
  it("composes the topic-owned config with the Kafka runtime options", async () => {
    const runtimeProgram = Effect.void;
    const runMain = vi.fn();
    const runViewServerRuntime = vi.fn(() => runtimeProgram);
    vi.doMock("@effect/platform-node", () => ({
      NodeRuntime: { runMain },
    }));
    vi.doMock("effect-view-server/runtime", () => ({
      runViewServerRuntime,
    }));

    await import("./runtime");

    expect(runViewServerRuntime.mock.calls).toStrictEqual([
      [
        viewServer,
        {
          websocketPort: 8080,
          kafka: {
            consumerGroupId: "view-server-example-combined-sources-react",
            startFrom: "latest",
          },
        },
      ],
    ]);
    expect(runMain.mock.calls).toStrictEqual([[runtimeProgram]]);
  });
});
