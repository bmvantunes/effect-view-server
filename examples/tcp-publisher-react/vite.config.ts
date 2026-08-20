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
  includeNodeTests: true,
  optimizeDepsInclude: ["@effect/platform-node"],
});
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
