import { describe, expect, it, vi } from "@effect/vitest";
import { render } from "vitest-browser-react";

vi.doMock("./view-server.config", () => ({
  useLiveQuery: () => ({
    rows: [
      {
        id: "leased-order-browser",
        customerId: "customer-leased-browser",
        price: 77,
      },
    ],
    totalRows: 1,
  }),
  useViewServerHealthSummary: () => ({ status: "ready" }),
  ViewServerProvider: () => null,
}));

describe("leased gRPC React example", () => {
  it("renders the leased route result", async () => {
    const { GrpcLeasedExampleApp } = await import("./view-server.example");
    const screen = await render(<GrpcLeasedExampleApp />);

    await expect
      .element(screen.getByRole("heading", { name: "On-demand shared gRPC route", exact: true }))
      .toBeVisible();
    await expect
      .element(screen.getByRole("heading", { name: "Strategy alpha orders", exact: true }))
      .toBeVisible();
    await expect.element(screen.getByRole("status")).toHaveTextContent(/^Runtime status: ready$/);
    await expect
      .element(
        screen.getByText("leased-order-browser / customer-leased-browser / 77", { exact: true }),
      )
      .toBeVisible();
    await screen.unmount();
  });
});
