/// <reference types="vitest/globals" />

import { describe, expect, it } from "@effect/vitest";
import type { ViewServerHealthSummary } from "effect-view-server/config";
import { createInMemoryViewServerReact } from "effect-view-server/react/testing";
import { Effect } from "effect";
import { render } from "vitest-browser-react";
import { KafkaExampleApp } from "./view-server.example";
import { viewServerReact } from "./view-server.config";

vi.mock("./view-server.config", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./view-server.config")>();
  const summary = {
    status: "ready",
    runtimeStatus: "ready",
    connectionStatus: "connected",
    unhealthyTopics: [],
    updatedAtNanos: 1n,
    maxKafkaLag: 7n,
  } satisfies ViewServerHealthSummary<typeof actual.viewServer.topics>;
  return {
    ...actual,
    useViewServerHealthSummary: () => summary,
  };
});

describe("Kafka React example health", () => {
  it("renders a numeric Kafka lag", async () => {
    const inMemoryExample = createInMemoryViewServerReact(viewServerReact);
    await Effect.runPromise(inMemoryExample.client.reset());

    const screen = await render(
      <inMemoryExample.ViewServerInMemoryProvider>
        <KafkaExampleApp />
      </inMemoryExample.ViewServerInMemoryProvider>,
    );

    await expect.element(screen.getByText("Max Kafka lag: 7", { exact: true })).toBeVisible();
    await screen.unmount();
    await Effect.runPromise(inMemoryExample.close);
  });
});
