import { describe, expect, it } from "@effect/vitest";
import type { ReactNode } from "react";
import { StrictMode } from "react";
import { AppRoot } from "./app-root";
import {
  defaultExampleRuntimeConfig,
  mountExampleApp,
  renderExampleApp,
  resolveExampleRuntimeConfig,
} from "./main";

describe("example browser entry helpers", () => {
  it("derives websocket URLs from the browser location", () => {
    expect(defaultExampleRuntimeConfig({ host: "example.test", protocol: "http:" })).toStrictEqual({
      VIEW_SERVER_URL: "ws://example.test/rpc",
    });
    expect(
      defaultExampleRuntimeConfig({ host: "secure.example.test", protocol: "https:" }),
    ).toStrictEqual({
      VIEW_SERVER_URL: "wss://secure.example.test/rpc",
    });
  });

  it("uses explicit runtime config before deriving from location", () => {
    expect(
      resolveExampleRuntimeConfig({
        __VIEW_SERVER_EXAMPLE_CONFIG__: {
          VIEW_SERVER_URL: "ws://configured.example.test/rpc",
        },
        location: {
          host: "ignored.example.test",
          protocol: "https:",
        },
      }),
    ).toStrictEqual({
      VIEW_SERVER_URL: "ws://configured.example.test/rpc",
    });
    expect(
      resolveExampleRuntimeConfig({
        location: {
          host: "fallback.example.test",
          protocol: "https:",
        },
      }),
    ).toStrictEqual({
      VIEW_SERVER_URL: "wss://fallback.example.test/rpc",
    });
  });

  it("renders the app root with the provided runtime config", () => {
    const config = {
      VIEW_SERVER_URL: "ws://runtime.example.test/rpc",
    };

    expect(renderExampleApp(config)).toStrictEqual(
      <StrictMode>
        <AppRoot config={config} />
      </StrictMode>,
    );
  });

  it("mounts through an injected React root factory", () => {
    const container = document.createElement("main");
    const config = {
      VIEW_SERVER_URL: "ws://runtime.example.test/rpc",
    };
    let renderedNode: ReactNode = null;

    mountExampleApp(container, config, () => ({
      render: (node) => {
        renderedNode = node;
      },
    }));

    expect(renderedNode).toStrictEqual(renderExampleApp(config));
  });

  it("fails clearly when the root element is missing", () => {
    expect(() =>
      mountExampleApp(null, { VIEW_SERVER_URL: "ws://runtime.example.test/rpc" }),
    ).toThrow("View Server example root element was not found.");
  });
});
