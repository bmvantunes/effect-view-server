import tailwindcss from "@tailwindcss/vite";
import viteReact from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { adaptTanStackStart, defineTanStackReactExampleConfig } from "../vite.config.shared";

export default defineTanStackReactExampleConfig({
  createTanStackStartPlugins: adaptTanStackStart(tanstackStart),
  plugins: (tanStackStartPlugins) => [tailwindcss(), tanStackStartPlugins, viteReact()],
  browserProvider: playwright(),
  enforceAllSourceCoverage: true,
});
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
