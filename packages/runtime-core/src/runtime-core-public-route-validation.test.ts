import { describe, expect, it } from "@effect/vitest";
import { trustDecodedRuntimeQuery } from "@effect-view-server/config/internal";
import { Effect } from "effect";
import { makeViewServerRuntimeCoreInternal } from "./internal";
import { viewServer } from "./test-support/runtime-test-fixtures";

describe("@effect-view-server/runtime-core", () => {
  it.effect("rejects routeBy on ordinary runtime subscription boundaries", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      const observer = {
        onQueryRegistered: () => Effect.void,
        onTerminalOccurrence: () => Effect.void,
        onTerminalReady: () => Effect.void,
      };
      const routedQuery = { select: ["id"] } satisfies { select: ["id"] };
      Object.defineProperty(routedQuery, "routeBy", {
        configurable: true,
        enumerable: true,
        value: { region: "UsÁ" },
      });
      const decodedRoutedQuery = trustDecodedRuntimeQuery(routedQuery);
      const errors = [
        yield* Effect.flip(runtimeCore.internalLiveClient.subscribeInternal("orders", routedQuery)),
        yield* Effect.flip(
          runtimeCore.internalLiveClient.subscribeObservedInternal("orders", routedQuery, observer),
        ),
        yield* Effect.flip(runtimeCore.liveClient.subscribeRuntime("orders", routedQuery)),
        yield* Effect.flip(
          runtimeCore.protocolQuerySubscriber.subscribeProtocolQuery("orders", decodedRoutedQuery),
        ),
      ];
      const expectedError = {
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "orders",
        message: "Topic orders does not accept routeBy.",
      };

      expect(errors).toStrictEqual([expectedError, expectedError, expectedError, expectedError]);

      yield* runtimeCore.close;
    }),
  );
});
