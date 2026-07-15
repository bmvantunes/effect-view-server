import { describe, expect, it } from "@effect/vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { SsrExampleApp } from "./view-server.example";

describe("SSR React example server rendering", () => {
  it("renders the complete live-data placeholder in the Node SSR runtime", () => {
    expect(renderToStaticMarkup(createElement(SsrExampleApp))).toBe(
      '<main class="example-shell"><header><p class="eyebrow">SSR shell</p><h1>TanStack Start shell with client-only live data</h1><p>The page shell is safe to server-render. The View Server WebSocket provider only mounts in the browser.</p></header><section class="panel" aria-label="ssr placeholder"><h2>Live data</h2><p>Live queries hydrate in the browser.</p></section></main>',
    );
  });
});
