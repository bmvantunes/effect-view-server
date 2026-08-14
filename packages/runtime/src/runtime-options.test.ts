import { describe, expect, it } from "@effect/vitest";
import type { ViewServerRuntimeError } from "@effect-view-server/config";
import { Duration, Effect } from "effect";
import {
  resolveViewServerRuntimeBaseOptions,
  validateViewServerRuntimeOptions,
} from "./runtime-options";

const validateHostileOptions = (options: object): Effect.Effect<unknown, ViewServerRuntimeError> =>
  Reflect.apply(validateViewServerRuntimeOptions, undefined, [options]);

describe("Runtime reporting options", () => {
  it.effect("validates the exact reporting bag and resolves the default change interval", () =>
    Effect.gen(function* () {
      const reportingObjectError = yield* Effect.flip(validateHostileOptions({ reporting: [] }));
      const unknownKeyError = yield* Effect.flip(
        validateHostileOptions({
          reporting: {
            heartbeatInterval: "1 second",
            dependenciesInterval: "2 seconds",
            onHeartbeat: () => Effect.void,
            onDependenciesUpdate: () => Effect.void,
            interval: "3 seconds",
          },
        }),
      );
      const callbacksError = yield* Effect.flip(
        validateHostileOptions({
          reporting: {
            heartbeatInterval: "1 second",
            dependenciesInterval: "2 seconds",
          },
        }),
      );
      const intervalError = yield* Effect.flip(
        validateHostileOptions({
          reporting: {
            heartbeatInterval: Duration.zero,
            dependenciesInterval: Duration.infinity,
            changeInterval: "not a duration",
            onHeartbeat: () => Effect.void,
            onDependenciesUpdate: () => Effect.void,
          },
        }),
      );
      const infiniteHeartbeatError = yield* Effect.flip(
        validateHostileOptions({
          reporting: {
            heartbeatInterval: Duration.infinity,
            dependenciesInterval: "2 seconds",
            onHeartbeat: () => Effect.void,
            onDependenciesUpdate: () => Effect.void,
          },
        }),
      );
      const missingDependencyCallbackError = yield* Effect.flip(
        validateHostileOptions({
          reporting: {
            heartbeatInterval: "1 second",
            dependenciesInterval: "2 seconds",
            onHeartbeat: () => Effect.void,
          },
        }),
      );
      const invalidChangeIntervalError = yield* Effect.flip(
        validateHostileOptions({
          reporting: {
            heartbeatInterval: "1 second",
            dependenciesInterval: "2 seconds",
            changeInterval: "not a duration",
            onHeartbeat: () => Effect.void,
            onDependenciesUpdate: () => Effect.void,
          },
        }),
      );

      expect(Reflect.get(reportingObjectError, "message")).toBe(
        "View Server runtime option reporting must be an object.",
      );
      expect(Reflect.get(unknownKeyError, "message")).toBe(
        "View Server runtime option reporting contains unsupported property: interval.",
      );
      expect(Reflect.get(callbacksError, "message")).toBe(
        "View Server runtime reporting requires onHeartbeat and onDependenciesUpdate Effect callbacks.",
      );
      expect(Reflect.get(intervalError, "message")).toBe(
        "View Server runtime reporting intervals must be positive finite Effect Durations.",
      );
      expect(Reflect.get(infiniteHeartbeatError, "message")).toBe(
        "View Server runtime reporting intervals must be positive finite Effect Durations.",
      );
      expect(Reflect.get(missingDependencyCallbackError, "message")).toBe(
        "View Server runtime reporting requires onHeartbeat and onDependenciesUpdate Effect callbacks.",
      );
      expect(Reflect.get(invalidChangeIntervalError, "message")).toBe(
        "View Server runtime reporting intervals must be positive finite Effect Durations.",
      );

      const validated = yield* validateViewServerRuntimeOptions({
        reporting: {
          heartbeatInterval: "1 second",
          dependenciesInterval: "2 seconds",
          onHeartbeat: () => Effect.void,
          onDependenciesUpdate: () => Effect.void,
        },
      });
      const resolved = resolveViewServerRuntimeBaseOptions(validated);
      expect(Duration.toMillis(resolved.reporting?.heartbeatInterval ?? Duration.zero)).toBe(1_000);
      expect(Duration.toMillis(resolved.reporting?.dependenciesInterval ?? Duration.zero)).toBe(
        2_000,
      );
      expect(Duration.toMillis(resolved.reporting?.changeInterval ?? Duration.zero)).toBe(300);

      const explicitChangeInterval = resolveViewServerRuntimeBaseOptions(
        yield* validateViewServerRuntimeOptions({
          reporting: {
            heartbeatInterval: "1 second",
            dependenciesInterval: "2 seconds",
            changeInterval: "400 millis",
            onHeartbeat: () => Effect.void,
            onDependenciesUpdate: () => Effect.void,
          },
        }),
      );
      expect(
        Duration.toMillis(explicitChangeInterval.reporting?.changeInterval ?? Duration.zero),
      ).toBe(400);
    }),
  );

  it.effect("captures hostile reporting getters exactly once before validation", () =>
    Effect.gen(function* () {
      const reads = {
        heartbeatInterval: 0,
        dependenciesInterval: 0,
        changeInterval: 0,
        onHeartbeat: 0,
        onDependenciesUpdate: 0,
      };
      const reporting = Object.defineProperties(
        {},
        {
          heartbeatInterval: {
            enumerable: true,
            get: () => {
              reads.heartbeatInterval += 1;
              return reads.heartbeatInterval === 1 ? Duration.seconds(1) : Duration.zero;
            },
          },
          dependenciesInterval: {
            enumerable: true,
            get: () => {
              reads.dependenciesInterval += 1;
              return reads.dependenciesInterval === 1 ? Duration.seconds(2) : Duration.zero;
            },
          },
          changeInterval: {
            enumerable: true,
            get: () => {
              reads.changeInterval += 1;
              return reads.changeInterval === 1 ? Duration.millis(300) : Duration.zero;
            },
          },
          onHeartbeat: {
            enumerable: true,
            get: () => {
              reads.onHeartbeat += 1;
              return reads.onHeartbeat === 1 ? () => Effect.void : undefined;
            },
          },
          onDependenciesUpdate: {
            enumerable: true,
            get: () => {
              reads.onDependenciesUpdate += 1;
              return reads.onDependenciesUpdate === 1 ? () => Effect.void : undefined;
            },
          },
        },
      );

      yield* validateHostileOptions({ reporting });

      expect(reads).toStrictEqual({
        heartbeatInterval: 1,
        dependenciesInterval: 1,
        changeInterval: 1,
        onHeartbeat: 1,
        onDependenciesUpdate: 1,
      });
    }),
  );

  it.effect("captures a hostile WebSocket compression getter exactly once", () =>
    Effect.gen(function* () {
      let reads = 0;
      const options = Object.defineProperty({}, "websocketCompression", {
        enumerable: true,
        get: () => {
          reads += 1;
          return reads === 1 ? true : "true";
        },
      });

      const validated = yield* validateHostileOptions(options);

      expect(reads).toBe(1);
      expect(validated).toStrictEqual({ websocketCompression: true });
    }),
  );
});
