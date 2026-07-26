import { describe, expect, it } from "@effect/vitest";
import {
  SourceAdapterConformanceDriver,
  SourceAdapterConformanceRow,
  SourceFixture,
} from "@effect-view-server/source-adapter-testing";
import { Effect, Layer } from "effect";
import { registerSourceAdapterConformance } from "./index";

const driverLayer = Layer.effect(
  SourceAdapterConformanceDriver,
  SourceFixture.make(SourceAdapterConformanceRow).pipe(Effect.map(SourceFixture.conformanceDriver)),
);

registerSourceAdapterConformance({
  name: "Controllable Source Adapter materialized conformance",
  layer: driverLayer,
  materialized: true,
});

registerSourceAdapterConformance({
  name: "Controllable Source Adapter leased conformance",
  layer: driverLayer,
  leased: true,
});

registerSourceAdapterConformance({
  name: "Controllable Source Adapter materialized continuous-upserts conformance",
  layer: driverLayer,
  materialized: true,
  eventModel: "continuous-upserts",
});

registerSourceAdapterConformance({
  name: "Controllable Source Adapter leased continuous-upserts conformance",
  layer: driverLayer,
  leased: true,
  eventModel: "continuous-upserts",
});

registerSourceAdapterConformance({
  name: "Controllable Source Adapter callback conformance",
  layer: driverLayer,
  materialized: true,
  callbackBridge: true,
});

describe("Source Adapter conformance registration", () => {
  it("rejects vacuous capability input at runtime", () => {
    expect(() =>
      Reflect.apply(registerSourceAdapterConformance, undefined, [
        {
          name: "vacuous",
          layer: driverLayer,
        },
      ]),
    ).toThrow("at least one enabled lifecycle capability");
  });

  it("rejects callback-only registration at runtime", () => {
    expect(() =>
      Reflect.apply(registerSourceAdapterConformance, undefined, [
        {
          name: "callback-only",
          layer: driverLayer,
          callbackBridge: true,
        },
      ]),
    ).toThrow("callback conformance requires materialized lifecycle conformance");
  });

  it("rejects an unknown transport event model at runtime", () => {
    expect(() =>
      Reflect.apply(registerSourceAdapterConformance, undefined, [
        {
          name: "unknown-profile",
          layer: driverLayer,
          materialized: true,
          eventModel: "unknown",
        },
      ]),
    ).toThrow("supported transport event model");
  });
});
