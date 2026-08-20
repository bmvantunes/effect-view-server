import tailwindcss from "@tailwindcss/vite";
import viteReact from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { adaptTanStackStart, defineTanStackReactExampleConfig } from "../vite.config.shared";

export default defineTanStackReactExampleConfig({
  createTanStackStartPlugins: adaptTanStackStart(tanstackStart),
  createTailwindPlugin: tailwindcss,
  createReactPlugins: viteReact,
  browserProvider: playwright(),
  enforceAllSourceCoverage: true,
  optimizeDepsInclude: [
    "@effect/platform-node",
    "effect/unstable/http",
    "effect/unstable/socket/Socket",
    "effect-view-server > @connectrpc/connect",
    "effect-view-server > @connectrpc/connect-node",
    "effect-view-server > @platformatic/kafka",
  ],
});
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
