import { describe, expectTypeOf, it } from "@effect/vitest";
import { Result } from "effect";
import { captureSourceHealthInput, type CapturedSourceHealthInput } from "./query-input-snapshot";

describe("Source Health input inference", () => {
  it("preserves literal topics for ordinary object-literal calls", () => {
    const captured = captureSourceHealthInput({ topic: "orders" });

    expectTypeOf<Result.Result.Success<typeof captured>>().toEqualTypeOf<
      CapturedSourceHealthInput<"orders">
    >();
  });

  it("preserves literal topics for leased input", () => {
    const captured = captureSourceHealthInput({
      topic: "orders",
      routeBy: { region: "eu" },
    });

    expectTypeOf<Result.Result.Success<typeof captured>>().toEqualTypeOf<
      CapturedSourceHealthInput<"orders">
    >();
  });
});
