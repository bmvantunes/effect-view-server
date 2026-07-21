import { describe, expect, it } from "@effect/vitest";
import type { ViewServerRuntimeError } from "@effect-view-server/config";
import { trustDecodedRuntimeQuery } from "@effect-view-server/config/internal";
import { Effect } from "effect";
import { makeViewServerRuntimeCoreInternal } from "./internal";
import { viewServer } from "./test-support/runtime-test-fixtures";

describe("@effect-view-server/runtime-core", () => {
  it.effect("rejects unsnapshotable queries at every erased runtime subscription entrypoint", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      const hostileThrownValue = {
        toString: () => {
          throw new Error("hostile toString must never run");
        },
      };
      const hostileQuery = new Proxy({ select: ["id"] } satisfies { select: ["id"] }, {
        ownKeys: () => {
          throw hostileThrownValue;
        },
      });
      const observer = {
        onQueryRegistered: () => Effect.void,
        onTerminalOccurrence: () => Effect.void,
        onTerminalReady: () => Effect.void,
      };
      const hostileProtocolQuery = trustDecodedRuntimeQuery(hostileQuery);
      const snapshotError = {
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "orders",
        message: "Query input could not be snapshotted.",
      } satisfies ViewServerRuntimeError;
      const errors = [
        yield* Effect.flip(runtimeCore.liveClient.subscribe("orders", hostileQuery)),
        yield* Effect.flip(
          runtimeCore.internalLiveClient.subscribeInternal("orders", hostileQuery),
        ),
        yield* Effect.flip(
          runtimeCore.internalLiveClient.subscribeObservedInternal(
            "orders",
            hostileQuery,
            observer,
          ),
        ),
        yield* Effect.flip(runtimeCore.liveClient.subscribeRuntime("orders", hostileQuery)),
        yield* Effect.flip(
          runtimeCore.internalLiveClient.subscribeRuntimeInternal("orders", hostileQuery),
        ),
        yield* Effect.flip(
          runtimeCore.internalLiveClient.subscribeRuntimeRoutedInternal("orders", hostileQuery),
        ),
        yield* Effect.flip(
          runtimeCore.internalLiveClient.subscribeRuntimeObservedInternal(
            "orders",
            hostileQuery,
            observer,
          ),
        ),
        yield* Effect.flip(
          runtimeCore.protocolQuerySubscriber.subscribeProtocolQuery(
            "orders",
            hostileProtocolQuery,
          ),
        ),
      ];

      expect(errors).toStrictEqual([
        snapshotError,
        snapshotError,
        snapshotError,
        snapshotError,
        snapshotError,
        snapshotError,
        snapshotError,
        snapshotError,
      ]);

      yield* runtimeCore.close;
    }),
  );
});
