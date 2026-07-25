import { describe, expectTypeOf, it } from "@effect/vitest";
import {
  SourceAdapterConformanceDriver,
  type SourceAdapterPackageInspectionOptions,
} from "@effect-view-server/source-adapter-testing";
import { Layer } from "effect";
import { registerSourceAdapterConformance, registerSourceAdapterPackageConformance } from "./index";

declare const layer: Layer.Layer<SourceAdapterConformanceDriver>;
declare const inspection: SourceAdapterPackageInspectionOptions;

describe("Source Adapter conformance host type contracts", () => {
  it("requires at least one behavioral capability", () => {
    expectTypeOf(
      registerSourceAdapterConformance({
        name: "materialized",
        layer,
        materialized: true,
      }),
    ).toEqualTypeOf<void>();

    // @ts-expect-error behavioral conformance cannot register zero capabilities.
    registerSourceAdapterConformance({
      name: "vacuous",
      layer,
      materialized: false,
    });

    registerSourceAdapterPackageConformance({
      inspection,
      behavioral: {
        name: "built materialized adapter",
        layer,
        materialized: true,
      },
    });

    registerSourceAdapterConformance({
      name: "callback materialized",
      layer,
      materialized: true,
      callbackBridge: true,
    });

    registerSourceAdapterConformance({
      name: "continuous upsert",
      layer,
      materialized: true,
      eventModel: "continuous-upserts",
    });

    registerSourceAdapterConformance({
      name: "invalid event model",
      layer,
      materialized: true,
      // @ts-expect-error transport event models are exact.
      eventModel: "unknown",
    });

    // @ts-expect-error callback checks supplement materialized lifecycle conformance.
    registerSourceAdapterConformance({
      name: "callback only",
      layer,
      callbackBridge: true,
    });
  });
});
