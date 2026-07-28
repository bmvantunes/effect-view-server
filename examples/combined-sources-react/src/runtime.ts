import { NodeRuntime } from "@effect/platform-node";
import { Effect } from "effect";
import { grpcNode } from "effect-view-server/grpc/node";
import { kafkaNode } from "effect-view-server/kafka/node";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { viewServer } from "./view-server.config";

const KafkaLive = kafkaNode.layer(viewServer, {
  consumerGroupPrefix: "view-server-example-combined-sources-react",
  regions: {
    usa: { bootstrapServers: "127.0.0.1:9092" },
    london: { bootstrapServers: "127.0.0.1:9094" },
  },
});
const GrpcLive = grpcNode.layer(viewServer, {
  orders: { baseUrl: "http://127.0.0.1:4319" },
  strategies: { baseUrl: "http://127.0.0.1:4320" },
});

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    host: "127.0.0.1",
    websocketPort: 8080,
    tcpPublishHost: "127.0.0.1",
    tcpPublishPort: 8081,
  }).pipe(Effect.provide([KafkaLive, GrpcLive])),
);
