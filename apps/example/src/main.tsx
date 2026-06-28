import { StrictMode, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { AppRoot, type ExampleRuntimeConfig } from "./app-root";

declare global {
  interface Window {
    readonly __VIEW_SERVER_EXAMPLE_CONFIG__?: ExampleRuntimeConfig;
  }
}

export type ExampleRuntimeWindow = {
  readonly __VIEW_SERVER_EXAMPLE_CONFIG__?: ExampleRuntimeConfig;
  readonly location: Pick<Location, "host" | "protocol">;
};

export type ExampleRoot = {
  readonly render: (node: ReactNode) => void;
};

export type CreateExampleRoot = (container: Element) => ExampleRoot;

export function defaultExampleRuntimeConfig(
  location: Pick<Location, "host" | "protocol">,
): ExampleRuntimeConfig {
  return {
    VIEW_SERVER_URL: `${location.protocol === "https:" ? "wss:" : "ws:"}//${location.host}/rpc`,
  };
}

export function resolveExampleRuntimeConfig(windowRef: ExampleRuntimeWindow): ExampleRuntimeConfig {
  return (
    windowRef.__VIEW_SERVER_EXAMPLE_CONFIG__ ?? defaultExampleRuntimeConfig(windowRef.location)
  );
}

export function renderExampleApp(config: ExampleRuntimeConfig): ReactNode {
  return (
    <StrictMode>
      <AppRoot config={config} />
    </StrictMode>
  );
}

export function mountExampleApp(
  container: Element | null,
  config: ExampleRuntimeConfig,
  createExampleRoot: CreateExampleRoot = createRoot,
): void {
  if (container === null) {
    throw new Error("View Server example root element was not found.");
  }
  createExampleRoot(container).render(renderExampleApp(config));
}
