import { describe, expect, it } from "@effect/vitest";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { Schema } from "effect";
import { defineViewServerConfig } from "./index";
import { grpcSourceMarkers } from "./internal";

const Failure = Schema.TaggedStruct("ConfigSourceFailure", {
  message: Schema.String,
});
const Metrics = Schema.Struct({
  connected: Schema.Boolean,
});
const Location = Schema.Struct({
  offset: Schema.BigInt,
});

const adapter = SourceAdapter.make({
  identity: {
    name: "config-source",
    version: "1",
  },
  failure: Failure,
  materialized: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<{
      readonly stream: string;
    }>(),
  },
  leased: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<{
      readonly stream: string;
    }>(),
  },
});

const Row = Schema.Struct({
  id: Schema.String,
  region: Schema.String,
  shard: Schema.BigInt,
});

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

describe("Source Adapter config", () => {
  it("owns canonical id and snapshots exact Materialized and Leased definitions", () => {
    const materialized = adapter.materializedSource({ stream: "all" });
    const leased = adapter.leasedSource(["region", "shard"], { stream: "routed" });
    const config = defineViewServerConfig({
      topics: {
        materialized: {
          schema: Row,
          source: materialized,
        },
        leased: {
          schema: Row,
          source: leased,
        },
      },
    });

    expect({
      materializedKey: config.topics.materialized.key,
      materializedSource: config.topics.materialized.source,
      leasedKey: config.topics.leased.key,
      leasedRoute: config.topics.leased.source.routeBy,
      topicsFrozen: Object.isFrozen(config.topics),
      definitionsFrozen:
        Object.isFrozen(config.topics.materialized) && Object.isFrozen(config.topics.leased),
    }).toStrictEqual({
      materializedKey: "id",
      materializedSource: materialized,
      leasedKey: "id",
      leasedRoute: ["region", "shard"],
      topicsFrozen: true,
      definitionsFrozen: true,
    });
  });

  it("rejects structural sources, explicit keys, conflicts, and unsupported lifecycles", () => {
    const structuralLifecycle = "materialized" as const;
    const structuralSource = {
      adapter,
      identity: adapter.identity,
      lifecycle: structuralLifecycle,
      options: { stream: "all" },
      routeBy: [],
      retry: { _tag: "UseAdapterDefault" },
    };
    expect(() =>
      defineViewServerConfig({
        topics: {
          // @ts-expect-error Source Definitions are nominal.
          structural: {
            schema: Row,
            source: structuralSource,
          },
        },
      }),
    ).toThrow("View Server topic structural source must be created by SourceAdapter.make(...).");

    expect(() =>
      // @ts-expect-error source-owned topics cannot declare a key.
      defineViewServerConfig({
        topics: {
          explicit: {
            schema: Row,
            key: "id",
            source: adapter.materializedSource({ stream: "all" }),
          },
        },
      }),
    ).toThrow("View Server topic explicit uses canonical source-owned id and cannot declare key.");

    const conflicting = {
      schema: Row,
      source: adapter.materializedSource({ stream: "all" }),
    };
    Object.defineProperty(conflicting, "grpcSource", {
      enumerable: true,
      value: grpcSourceMarkers.materialized(),
    });
    expect(() =>
      defineViewServerConfig({
        topics: {
          conflict: conflicting,
        },
      }),
    ).toThrow(
      "View Server topic conflict cannot declare more than one source owner: source, grpcSource.",
    );

    const materializedAdapter = nominalClone(adapter, {
      materialized: undefined,
    });
    const unsupportedMaterialized = nominalClone(adapter.materializedSource({ stream: "all" }), {
      adapter: materializedAdapter,
    });
    expect(() =>
      defineViewServerConfig({
        topics: {
          unsupportedMaterialized: {
            schema: Row,
            source: unsupportedMaterialized,
          },
        },
      }),
    ).toThrow(
      "View Server topic unsupportedMaterialized source must be created by SourceAdapter.make(...).",
    );

    const leasedAdapter = nominalClone(adapter, {
      leased: undefined,
    });
    const unsupportedLeased = nominalClone(adapter.leasedSource(["region"], { stream: "routed" }), {
      adapter: leasedAdapter,
    });
    expect(() =>
      defineViewServerConfig({
        topics: {
          unsupportedLeased: {
            schema: Row,
            source: unsupportedLeased,
          },
        },
      }),
    ).toThrow(
      "View Server topic unsupportedLeased source must be created by SourceAdapter.make(...).",
    );
  });

  it("requires exact Schema.String canonical ids and complete scalar Leased routes", () => {
    const source = adapter.materializedSource({ stream: "all" });
    const invalidIds = [
      Schema.Struct({ region: Schema.String }),
      Schema.Struct({ id: Schema.optionalKey(Schema.String), region: Schema.String }),
      Schema.Struct({ id: Schema.NonEmptyString, region: Schema.String }),
      Schema.Struct({ id: Schema.Number, region: Schema.String }),
    ];

    for (const schema of invalidIds) {
      expect(() =>
        // @ts-expect-error malformed source-owned row ids remain runtime guarded.
        defineViewServerConfig({ topics: { invalid: { schema, source } } }),
      ).toThrow(
        "View Server topic invalid source-owned row schema must define canonical id as Schema.String.",
      );
    }

    expect(() =>
      // @ts-expect-error Leased route fields must exist in the row.
      defineViewServerConfig({
        topics: {
          invalidRoute: {
            schema: Row,
            source: adapter.leasedSource(["missing"], { stream: "routed" }),
          },
        },
      }),
    ).toThrow(
      "View Server topic invalidRoute leased source route field missing must have a complete supported scalar schema domain.",
    );
  });
});
