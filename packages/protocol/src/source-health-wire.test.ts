import { describe, expect, it } from "@effect/vitest";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { defineViewServerConfig, type ViewServerRuntimeError } from "@effect-view-server/config";
import { Effect, Exit, Option, Schema, SchemaGetter } from "effect";
import { make as makeBigDecimal } from "effect/BigDecimal";
import {
  viewServerDecodeSourceHealth,
  viewServerDecodeSourceHealthRequest,
  viewServerEncodeSourceHealth,
  viewServerEncodeSourceHealthRequest,
} from "./source-health-wire";

const Failure = Schema.TaggedStruct("WireSourceFailure", {
  message: Schema.String,
  offset: Schema.BigInt,
});
const Metrics = Schema.Struct({
  observed: Schema.BigInt,
  watermark: Schema.BigDecimal,
});
const Location = Schema.Struct({
  offset: Schema.BigInt,
  amount: Schema.BigDecimal,
});

const adapter = SourceAdapter.make({
  identity: {
    name: "wire-fixture",
    version: "1",
  },
  failure: Failure,
  materialized: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<{ readonly label: string }>(),
  },
  leased: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<{ readonly label: string }>(),
  },
});

const Row = Schema.Struct({
  id: Schema.String,
  region: Schema.String,
  shard: Schema.BigInt,
  amount: Schema.BigDecimal,
});

const config = defineViewServerConfig({
  topics: {
    materialized: {
      schema: Row,
      source: adapter.materializedSource({ label: "materialized" }),
    },
    leased: {
      schema: Row,
      source: adapter.leasedSource(["region", "shard", "amount"], { label: "leased" }),
    },
  },
});

const runtimeMetrics = {
  startedAtNanos: 1n,
  lastAttemptStartedAtNanos: 2n,
  lastDeliveryAtNanos: 3n,
  lastRejectionAtNanos: 4n,
  lastAppliedMutationAtNanos: 5n,
  lastTerminationAtNanos: null,
  currentAttempt: 2n,
  retryCount: 1n,
  receivedDeliveryCount: 2n,
  rejectedItemCount: 1n,
  attemptedMutationCount: 2n,
  appliedUpsertCount: 1n,
  appliedDeleteCount: 1n,
  failedMutationCount: 0n,
  completedSettlementCount: 3n,
  failedSettlementCount: 0n,
  retainedRowCount: 1,
  lanes: [
    {
      id: "wire",
      buffer: {
        _tag: "Bounded",
        capacity: 16,
        depth: 1,
        highWaterMark: 4,
        overflowCount: 0n,
      },
    },
  ],
} as const;

const route = {
  region: "eu",
  shard: 7n,
  amount: makeBigDecimal(123n, 2),
};

const rejection = {
  failure: {
    _tag: "AdapterFailure",
    failure: {
      _tag: "WireSourceFailure",
      message: "invalid item",
      offset: 9n,
    },
  },
  location: {
    offset: 9n,
    amount: makeBigDecimal(999n, 2),
  },
  rejectedAtNanos: 10n,
} as const;

const materializedHealth = {
  adapter: {
    name: "wire-fixture",
    version: "1",
  },
  target: {
    _tag: "Materialized",
  },
  status: {
    _tag: "Degraded",
    attempt: 2n,
    degradedAtNanos: 10n,
    latestRejection: rejection,
  },
  metrics: {
    runtime: runtimeMetrics,
    adapter: {
      observed: 11n,
      watermark: makeBigDecimal(456n, 2),
    },
  },
  sampledAtNanos: 12n,
} as const;

const leasedHealth = {
  adapter: {
    name: "wire-fixture",
    version: "1",
  },
  target: {
    _tag: "Leased",
    route,
  },
  status: {
    _tag: "Ready",
    attempt: 1n,
    readyAtNanos: 11n,
  },
  metrics: {
    runtime: runtimeMetrics,
    adapter: {
      observed: 11n,
      watermark: makeBigDecimal(456n, 2),
    },
  },
  sampledAtNanos: 12n,
} as const;

const materializedDefinition = config.topics.materialized.source;

const nominalClone = <Value extends object>(
  value: Value,
  overrides: Readonly<Record<string, unknown>>,
): Value => {
  const clone: Value = Object.create(Object.getPrototypeOf(value));
  for (const property of Reflect.ownKeys(value)) {
    const descriptor = Reflect.getOwnPropertyDescriptor(value, property);
    if (descriptor === undefined) {
      continue;
    }
    const next =
      typeof property === "symbol" &&
      "value" in descriptor &&
      typeof descriptor.value === "function"
        ? {
            ...descriptor,
            value: () => clone,
          }
        : typeof property === "string" &&
            Object.hasOwn(overrides, property) &&
            "value" in descriptor
          ? {
              ...descriptor,
              value: overrides[property],
            }
          : descriptor;
    Object.defineProperty(clone, property, next);
  }
  return Object.freeze(clone);
};

const proxyMaterializedDefinition = (overrides: Readonly<Record<string, unknown>>) =>
  nominalClone(materializedDefinition, overrides);

const configWithMaterializedDefinition = (source: typeof materializedDefinition) => ({
  topics: {
    materialized: {
      schema: Row,
      key: "id",
      source,
    },
  },
});

const invalidQueryTag = (exit: Exit.Exit<unknown, ViewServerRuntimeError>) => {
  const error = Option.getOrThrow(Exit.findErrorOption(exit));
  return {
    _tag: error._tag,
    code: error.code,
    topic: error.topic,
  };
};

describe("Source Health wire contract", () => {
  it.effect("round-trips exact materialized requests and health", () =>
    Effect.gen(function* () {
      const request = yield* viewServerEncodeSourceHealthRequest(config, "materialized", []);
      expect(request).toStrictEqual({ topic: "materialized" });
      expect(yield* viewServerDecodeSourceHealthRequest(config, request)).toStrictEqual({
        topic: "materialized",
        route: [],
      });

      const encoded = yield* viewServerEncodeSourceHealth(
        config,
        "materialized",
        materializedHealth,
      );
      const decoded = yield* viewServerDecodeSourceHealth(config, "materialized", encoded);
      expect(yield* viewServerEncodeSourceHealth(config, "materialized", decoded)).toStrictEqual(
        encoded,
      );
    }),
  );

  it.effect("round-trips bigint and BigDecimal leased routes and health", () =>
    Effect.gen(function* () {
      const request = yield* viewServerEncodeSourceHealthRequest(config, "leased", [route]);
      const decodedRequest = yield* viewServerDecodeSourceHealthRequest(config, request);
      expect(
        yield* viewServerEncodeSourceHealthRequest(
          config,
          decodedRequest.topic,
          decodedRequest.route,
        ),
      ).toStrictEqual(request);
      const inactive = {
        _tag: "Inactive",
        route,
      } as const;
      const encoded = yield* viewServerEncodeSourceHealth(config, "leased", inactive);
      const decoded = yield* viewServerDecodeSourceHealth(config, "leased", encoded);
      expect(yield* viewServerEncodeSourceHealth(config, "leased", decoded)).toStrictEqual(encoded);

      const active = {
        _tag: "Active",
        route,
        health: leasedHealth,
      } as const;
      const encodedActive = yield* viewServerEncodeSourceHealth(config, "leased", active);
      const decodedActive = yield* viewServerDecodeSourceHealth(config, "leased", encodedActive);
      expect(yield* viewServerEncodeSourceHealth(config, "leased", decodedActive)).toStrictEqual(
        encodedActive,
      );
    }),
  );

  it.effect("rejects invalid topics, lifecycle arguments, and health", () =>
    Effect.gen(function* () {
      const sourceFree = defineViewServerConfig({
        topics: {
          manual: {
            schema: Row,
            key: "id",
          },
        },
      });
      const mismatchedRouteConfig = {
        topics: {
          mismatched: {
            schema: Row,
            key: "id",
            source: adapter.leasedSource(["missing"], {
              label: "mismatched-route",
            }),
          },
        },
      };
      const failures = yield* Effect.all([
        Effect.exit(viewServerEncodeSourceHealthRequest(config, "materialized", [route])),
        Effect.exit(
          viewServerDecodeSourceHealthRequest(config, {
            topic: "materialized",
            routeBy: {},
          }),
        ),
        Effect.exit(viewServerEncodeSourceHealthRequest(config, "leased", [])),
        Effect.exit(
          viewServerEncodeSourceHealthRequest(config, "leased", [
            {
              ...route,
              extra: "must-not-be-stripped",
            },
          ]),
        ),
        Effect.exit(
          viewServerDecodeSourceHealthRequest(config, {
            topic: "leased",
          }),
        ),
        Effect.exit(
          viewServerDecodeSourceHealthRequest(config, {
            topic: "leased",
            routeBy: {
              region: "eu",
              shard: "7",
              amount: "1.23",
              extra: "must-not-be-stripped",
            },
          }),
        ),
        Effect.exit(
          viewServerEncodeSourceHealth(config, "leased", {
            _tag: "Inactive",
            route: {
              ...route,
              extra: "must-not-be-stripped",
            },
          }),
        ),
        Effect.exit(
          viewServerEncodeSourceHealth(config, "leased", {
            _tag: "Inactive",
            route: null,
          }),
        ),
        Effect.exit(viewServerEncodeSourceHealth(config, "leased", null)),
        Effect.exit(
          viewServerEncodeSourceHealth(
            config,
            "leased",
            new Proxy(
              {},
              {
                get() {
                  throw new Error("hostile health");
                },
              },
            ),
          ),
        ),
        Effect.exit(
          viewServerEncodeSourceHealth(config, "leased", {
            _tag: "Inactive",
            route: new Proxy(route, {
              ownKeys() {
                throw new Error("hostile route");
              },
            }),
          }),
        ),
        Effect.exit(
          viewServerDecodeSourceHealth(config, "leased", {
            _tag: "Inactive",
            route: {
              region: "eu",
              shard: "7",
              amount: "1.23",
              extra: "must-not-be-stripped",
            },
          }),
        ),
        Effect.exit(
          viewServerEncodeSourceHealth(config, "materialized", {
            ...materializedHealth,
            sampledAtNanos: 12,
          }),
        ),
        Effect.exit(
          viewServerEncodeSourceHealth(config, "materialized", {
            ...materializedHealth,
            status: {
              _tag: "Starting",
              attempt: 2n,
              startedAtNanos: 1n,
            },
          }),
        ),
        Effect.exit(viewServerEncodeSourceHealthRequest(sourceFree, "manual", [])),
        Effect.exit(viewServerEncodeSourceHealthRequest(config, "missing", [])),
        Effect.exit(
          viewServerEncodeSourceHealthRequest(mismatchedRouteConfig, "mismatched", [
            { missing: "value" },
          ]),
        ),
      ]);
      expect(failures.map(invalidQueryTag)).toStrictEqual([
        {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "materialized",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "materialized",
        },
        { _tag: "ViewServerRuntimeError", code: "InvalidQuery", topic: "leased" },
        { _tag: "ViewServerRuntimeError", code: "InvalidQuery", topic: "leased" },
        { _tag: "ViewServerRuntimeError", code: "InvalidQuery", topic: "leased" },
        { _tag: "ViewServerRuntimeError", code: "InvalidQuery", topic: "leased" },
        { _tag: "ViewServerRuntimeError", code: "InvalidQuery", topic: "leased" },
        { _tag: "ViewServerRuntimeError", code: "InvalidQuery", topic: "leased" },
        { _tag: "ViewServerRuntimeError", code: "InvalidQuery", topic: "leased" },
        { _tag: "ViewServerRuntimeError", code: "InvalidQuery", topic: "leased" },
        { _tag: "ViewServerRuntimeError", code: "InvalidQuery", topic: "leased" },
        { _tag: "ViewServerRuntimeError", code: "InvalidQuery", topic: "leased" },
        {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "materialized",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "materialized",
        },
        { _tag: "ViewServerRuntimeError", code: "InvalidQuery", topic: "manual" },
        { _tag: "ViewServerRuntimeError", code: "InvalidQuery", topic: "missing" },
        {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "mismatched",
        },
      ]);
    }),
  );

  it.effect("rejects malformed nominal Source metadata defensively", () =>
    Effect.gen(function* () {
      const invalidAdapterFailure = nominalClone(adapter, {
        failureSchema: null,
      });
      const missingDeclaration = nominalClone(adapter, {
        materialized: undefined,
      });
      const invalidMetrics = nominalClone(adapter.materialized, {
        metrics: null,
      });
      const invalidLocation = nominalClone(adapter.materialized, {
        rejectionLocation: null,
      });
      const adapterWithInvalidMetrics = nominalClone(adapter, {
        materialized: invalidMetrics,
      });
      const adapterWithInvalidLocation = nominalClone(adapter, {
        materialized: invalidLocation,
      });
      const malformed = [
        proxyMaterializedDefinition({ lifecycle: "invalid" }),
        proxyMaterializedDefinition({ adapter: invalidAdapterFailure }),
        proxyMaterializedDefinition({ adapter: missingDeclaration }),
        proxyMaterializedDefinition({ adapter: adapterWithInvalidMetrics }),
        proxyMaterializedDefinition({ adapter: adapterWithInvalidLocation }),
        proxyMaterializedDefinition({ routeBy: "invalid" }),
        proxyMaterializedDefinition({ routeBy: [1] }),
        proxyMaterializedDefinition({ routeBy: ["missing"] }),
      ];
      const failures = yield* Effect.forEach(malformed, (source) =>
        viewServerEncodeSourceHealthRequest(
          configWithMaterializedDefinition(source),
          "materialized",
          [],
        ).pipe(Effect.exit),
      );

      expect(failures.map(invalidQueryTag)).toStrictEqual([
        {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "materialized",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "materialized",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "materialized",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "materialized",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "materialized",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "materialized",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "materialized",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "materialized",
        },
      ]);
    }),
  );

  it.effect("rejects schema-valid Source Health that encodes outside JSON", () =>
    Effect.gen(function* () {
      const UnsafeMetrics = Schema.String.pipe(
        Schema.encodeTo(Schema.Any, {
          decode: SchemaGetter.transform((value) =>
            typeof value === "string" ? value : "decoded",
          ),
          encode: SchemaGetter.transform(() => Symbol("not-json")),
        }),
      );
      const unsafeAdapter = SourceAdapter.make({
        identity: {
          name: "wire-unsafe",
        },
        failure: Failure,
        materialized: {
          metrics: UnsafeMetrics,
          rejectionLocation: Location,
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
        leased: undefined,
      });
      const unsafeConfig = defineViewServerConfig({
        topics: {
          unsafe: {
            schema: Row,
            source: unsafeAdapter.materializedSource(undefined),
          },
        },
      });
      const exit = yield* viewServerEncodeSourceHealth(unsafeConfig, "unsafe", {
        adapter: unsafeAdapter.identity,
        target: {
          _tag: "Materialized",
        },
        status: {
          _tag: "Ready",
          attempt: 1n,
          readyAtNanos: 1n,
        },
        metrics: {
          runtime: runtimeMetrics,
          adapter: "unsafe",
        },
        sampledAtNanos: 1n,
      }).pipe(Effect.exit);

      expect(invalidQueryTag(exit)).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "unsafe",
      });
    }),
  );

  it.effect("rejects a hostile metrics codec that invalidates its decoded value", () =>
    Effect.gen(function* () {
      let validationCount = 0;
      const HostileMetrics = Schema.String.check(
        Schema.makeFilter(() => {
          validationCount += 1;
          return validationCount === 1;
        }),
      );
      const hostileAdapter = SourceAdapter.make({
        identity: {
          name: "wire-hostile-decoder",
        },
        failure: Failure,
        materialized: {
          metrics: HostileMetrics,
          rejectionLocation: Location,
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
        leased: undefined,
      });
      const hostileConfig = defineViewServerConfig({
        topics: {
          hostile: {
            schema: Row,
            source: hostileAdapter.materializedSource(undefined),
          },
        },
      });
      const encoded = yield* viewServerEncodeSourceHealth(hostileConfig, "hostile", {
        adapter: hostileAdapter.identity,
        target: {
          _tag: "Materialized",
        },
        status: {
          _tag: "Ready",
          attempt: 1n,
          readyAtNanos: 1n,
        },
        metrics: {
          runtime: runtimeMetrics,
          adapter: "valid-before-decode",
        },
        sampledAtNanos: 1n,
      });
      validationCount = 0;
      const exit = yield* viewServerDecodeSourceHealth(hostileConfig, "hostile", encoded).pipe(
        Effect.exit,
      );

      expect(Option.getOrThrow(Exit.findErrorOption(exit))).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "hostile",
        message: "Configured Source Health decoder returned an invalid value.",
      });
    }),
  );
});
