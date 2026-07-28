import { describe, expect, it, vi } from "@effect/vitest";
import { render } from "vitest-browser-react";

vi.doMock("./view-server.config", () => ({
  useLiveQuery: () => ({
    rows: [
      {
        id: "strategy-browser-row",
        strategyId: "strategy-browser",
        region: "usa",
        notional: 1_000,
      },
    ],
    totalRows: 1,
  }),
  useViewServerHealthSummary: () => ({ status: "ready" }),
  ViewServerProvider: () => null,
}));

describe("materialized gRPC React example", () => {
  it("renders the materialized result", async () => {
    const { GrpcMaterializedExampleApp } = await import("./view-server.example");
    const screen = await render(<GrpcMaterializedExampleApp />);

    await expect
      .element(
        screen.getByRole("heading", {
          name: "Startup materialized strategy stream",
          exact: true,
        }),
      )
      .toBeVisible();
    await expect
      .element(screen.getByRole("heading", { name: "Active strategies", exact: true }))
      .toBeVisible();
    await expect.element(screen.getByRole("status")).toHaveTextContent(/^Runtime status: ready$/);
    await expect
      .element(screen.getByText("strategy-browser / usa / 1000", { exact: true }))
      .toBeVisible();
    await screen.unmount();
  });
});
