import { describe, expectTypeOf, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import {
  SourceAdapter,
  type SourceDefinitionAdapter,
  type SourceDefinitionAny,
  type SourceDefinitionRetryServices,
} from "@effect-view-server/source-adapter";
import type { ViewServerSourceRequirements } from "@effect-view-server/runtime-core";
import { Context, Effect, Schema } from "effect";
import { makeViewServerRuntime, runViewServerRuntime } from "./index";

const Failure = Schema.TaggedStruct("RuntimeTypeSourceFailure", {
  message: Schema.String,
});
const adapter = SourceAdapter.make({
  identity: {
    name: "runtime-type-source",
  },
  failure: Failure,
  materialized: {
    metrics: Schema.Struct({
      observed: Schema.BigInt,
    }),
    rejectionLocation: Schema.Struct({
      offset: Schema.BigInt,
    }),
    definitionOptions: SourceAdapter.definitionOptions<void>(),
  },
  leased: undefined,
});
const Row = Schema.Struct({
  id: Schema.String,
  value: Schema.String,
});
const config = defineViewServerConfig({
  topics: {
    sourced: {
      schema: Row,
      source: adapter.materializedSource(undefined),
    },
    manual: {
      schema: Row,
      key: "id",
    },
  },
});
const sourceFreeConfig = defineViewServerConfig({
  topics: {
    manual: {
      schema: Row,
      key: "id",
    },
  },
});
const runtimeEffect = makeViewServerRuntime(config);
const sourceFreeRuntimeEffect = makeViewServerRuntime(sourceFreeConfig);
declare const runtime: Effect.Success<typeof runtimeEffect>;

type EffectRequirements<Value> =
  Value extends Effect.Effect<infer _Success, infer _Error, infer Requirements>
    ? Requirements
    : never;

describe("production Source Adapter runtime contract", () => {
  it("preserves the generated runtime service requirement", () => {
    const make = makeViewServerRuntime(config);
    const run = runViewServerRuntime(config);

    expectTypeOf<typeof config.topics.sourced.source>().toExtend<SourceDefinitionAny>();
    expectTypeOf<SourceDefinitionAdapter<typeof config.topics.sourced.source>>().toEqualTypeOf<
      typeof adapter
    >();
    expectTypeOf<
      SourceDefinitionRetryServices<typeof config.topics.sourced.source>
    >().toEqualTypeOf<never>();
    expectTypeOf<typeof adapter.runtimeService>().toExtend<Context.Service.Any>();
    expectTypeOf<keyof typeof config.topics>().toEqualTypeOf<"sourced" | "manual">();
    expectTypeOf<ViewServerSourceRequirements<typeof config.topics>>().toEqualTypeOf<
      Context.Service.Identifier<typeof adapter.runtimeService>
    >();
    expectTypeOf<EffectRequirements<typeof make>>().toEqualTypeOf<
      Context.Service.Identifier<typeof adapter.runtimeService>
    >();
    expectTypeOf<EffectRequirements<typeof run>>().toEqualTypeOf<
      Context.Service.Identifier<typeof adapter.runtimeService>
    >();
    expectTypeOf<
      ViewServerSourceRequirements<typeof sourceFreeConfig.topics>
    >().toEqualTypeOf<never>();
    expectTypeOf<EffectRequirements<typeof sourceFreeRuntimeEffect>>().toEqualTypeOf<never>();
  });

  it("removes Source-Owned Topics from production mutation APIs", () => {
    void runtime.client.publish("manual", {
      id: "manual",
      value: "accepted",
    });
    // @ts-expect-error Source-Owned Topics reject direct production runtime mutation.
    void runtime.client.publish("sourced", {
      id: "sourced",
      value: "rejected",
    });
  });
});
