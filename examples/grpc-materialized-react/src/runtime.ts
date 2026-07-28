import { NodeRuntime } from "@effect/platform-node";
import { Effect } from "effect";
import { grpcNode } from "effect-view-server/grpc/node";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { viewServer } from "./view-server.config";

const GrpcLive = grpcNode.layer(viewServer, {
  strategies: {
    baseUrl: "http://127.0.0.1:4318",
  },
});

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    host: "127.0.0.1",
    websocketPort: 8080,
    tcpPublishHost: "127.0.0.1",
    tcpPublishPort: 8081,
  }).pipe(Effect.provide(GrpcLive)),
);
