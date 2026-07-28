import { describe, expect, it, vi } from "@effect/vitest";
import { Context, Effect, Layer } from "effect";
import { viewServer } from "./view-server.config";

class GrpcLayerSentinel extends Context.Service<GrpcLayerSentinel, { readonly provided: true }>()(
  "examples/grpc-leased-react/GrpcLayerSentinel",
) {}

describe("leased gRPC React example runtime", () => {
  it("composes the topic-owned config with the runtime options", async () => {
    const sentinel = { provided: true } as const;
    const runtimeProgram = GrpcLayerSentinel;
    const grpcLayer = Layer.succeed(GrpcLayerSentinel)(sentinel);
    const layer = vi.fn(() => grpcLayer);
    const runMain = vi.fn((program: Effect.Effect<unknown, unknown>) => program);
    const runViewServerRuntime = vi.fn(() => runtimeProgram);
    vi.doMock("@effect/platform-node", () => ({
      NodeRuntime: { runMain },
    }));
    vi.doMock("effect-view-server/runtime", () => ({
      runViewServerRuntime,
    }));
    vi.doMock("effect-view-server/grpc/node", () => ({
      grpcNode: { layer },
    }));

    await import("./runtime");

    expect(layer.mock.calls).toStrictEqual([
      [
        viewServer,
        {
          orders: {
            baseUrl: "http://127.0.0.1:4317",
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
    const mainProgram =
      runMain.mock.calls.at(0)?.at(0) ?? Effect.fail("runMain did not receive a program.");
    const mainResult = await Effect.runPromise(mainProgram);
    expect(mainResult).toStrictEqual(sentinel);
  });
});
