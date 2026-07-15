/// <reference types="vitest/globals" />

import { describe, expect, it } from "@effect/vitest";
import { createInMemoryViewServerReact } from "effect-view-server/react/testing";
import { Effect } from "effect";
import { renderToStaticMarkup } from "react-dom/server";
import { render } from "vitest-browser-react";
import { SsrExampleApp } from "./view-server.example";
import { viewServerReact } from "./view-server.config";

vi.mock("./view-server.config", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./view-server.config")>();
  const { createElement } = await import("react");
  return {
    ...actual,
    ViewServerProvider: function TestViewServerProvider() {
      return createElement(
        "section",
        { "aria-label": "default remote provider" },
        "Default remote provider selected.",
      );
    },
  };
});

describe("SSR React example", () => {
  it("renders the complete live-data placeholder without mounting a browser provider", () => {
    expect(renderToStaticMarkup(SsrExampleApp())).toBe(
      '<main class="example-shell"><header><p class="eyebrow">SSR shell</p><h1>TanStack Start shell with client-only live data</h1><p>The page shell is safe to server-render. The View Server WebSocket provider only mounts in the browser.</p></header><section class="panel" aria-label="ssr placeholder"><h2>Live data</h2><p>Live queries hydrate in the browser.</p></section></main>',
    );
  });

  it("selects the default remote provider when the wrapper prop is omitted", async () => {
    const screen = await render(<SsrExampleApp />);

    await screen.getByRole("button", { name: "Connect live data", exact: true }).click();
    await expect
      .element(screen.getByRole("region", { name: "default remote provider", exact: true }))
      .toBeVisible();
    await screen.unmount();
  });

  it("renders the browser-only live panel with an in-memory provider", async () => {
    const inMemoryExample = createInMemoryViewServerReact(viewServerReact);
    await Effect.runPromise(inMemoryExample.client.reset());

    const screen = await render(
      <SsrExampleApp
        wrapLiveOrdersPanel={(liveOrdersPanel) => (
          <inMemoryExample.ViewServerInMemoryProvider>
            {liveOrdersPanel}
          </inMemoryExample.ViewServerInMemoryProvider>
        )}
      />,
    );

    await expect
      .element(
        screen.getByRole("heading", {
          name: "TanStack Start shell with client-only live data",
          exact: true,
        }),
      )
      .toBeVisible();
    await screen.getByRole("button", { name: "Connect live data", exact: true }).click();
    await expect
      .element(screen.getByRole("heading", { name: "Live orders", exact: true }))
      .toBeVisible();
    await Effect.runPromise(
      inMemoryExample.client.publish("orders", {
        id: "order-ssr-browser",
        customerId: "customer-ssr-browser",
        status: "open",
        price: 99,
        region: "usa",
        updatedAt: 1,
      }),
    );
    await expect
      .element(screen.getByText("order-ssr-browser / customer-ssr-browser / 99", { exact: true }))
      .toBeVisible();
    await screen.unmount();
    await Effect.runPromise(inMemoryExample.close);
  });
});
