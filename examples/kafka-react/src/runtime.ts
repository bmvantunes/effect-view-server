import { NodeRuntime } from "@effect/platform-node";
import { Effect } from "effect";
import { kafkaNode } from "effect-view-server/kafka/node";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { viewServer } from "./view-server.config";

const KafkaLive = kafkaNode.layer(viewServer, {
  consumerGroupPrefix: "view-server-example-kafka-react",
  regions: {
    usa: { bootstrapServers: "127.0.0.1:9092" },
    london: { bootstrapServers: "127.0.0.1:9094" },
  },
});

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    host: "127.0.0.1",
    websocketPort: 8080,
    tcpPublishHost: "127.0.0.1",
    tcpPublishPort: 8081,
  }).pipe(Effect.provide(KafkaLive)),
);
