import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { viewServerDecodeHealth } from "./index";

import { topicHealth, viewServer, wireHealth } from "../test-harness/protocol";

describe("Invalid health payload wire inputs", () => {
  it.effect("rejects malformed, missing, and unknown canonical runtime health", () =>
    Effect.gen(function* () {
      const missingHealthTopic = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          engine: { topics: { orders: topicHealth } },
        }),
      );
      expect(missingHealthTopic.message).toBe("Health payload is missing topic: badjson");

      const extraHealthTopic = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          engine: { topics: { ...wireHealth.engine.topics, missing: topicHealth } },
        }),
      );
      expect(extraHealthTopic.message).toBe("Health payload references unknown topic: missing");

      const healthWithExtras = {
        ...wireHealth,
        extraRoot: "drop-me",
        engine: {
          topics: {
            orders: { ...topicHealth, extraTopic: "drop-me" },
            badjson: {
              ...topicHealth,
              rowCount: 0,
              liveRowCount: 0,
              extraTopic: "drop-me",
            },
          },
        },
        transport: { ...wireHealth.transport, extraTransport: "drop-me" },
      };
      const normalizedHealth = yield* viewServerDecodeHealth(viewServer, healthWithExtras);
      expect(Object.hasOwn(normalizedHealth, "extraRoot")).toBe(false);
      expect(Object.hasOwn(normalizedHealth.transport, "extraTransport")).toBe(false);
      expect(Object.hasOwn(normalizedHealth.engine.topics.orders, "extraTopic")).toBe(false);

      const malformedHealthStatus = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          // @ts-expect-error hostile runtime adapters can return malformed health status.
          status: "broken",
        }),
      );
      expect(malformedHealthStatus.message).toMatch(/Invalid health payload/);

      const malformedHealthTransport = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          transport: {
            ...wireHealth.transport,
            // @ts-expect-error hostile runtime adapters can return malformed health counters.
            activeClients: "1",
          },
        }),
      );
      expect(malformedHealthTransport.message).toMatch(/Invalid health payload/);
    }),
  );
});
