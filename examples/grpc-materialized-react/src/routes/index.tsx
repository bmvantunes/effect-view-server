import { createFileRoute } from "@tanstack/react-router";
import { GrpcMaterializedExampleApp, ViewServerProvider } from "../view-server.example";

export const Route = createFileRoute("/")({ component: Home });

function Home() {
  return (
    <ViewServerProvider url="ws://127.0.0.1:8080/rpc">
      <GrpcMaterializedExampleApp />
    </ViewServerProvider>
  );
}
