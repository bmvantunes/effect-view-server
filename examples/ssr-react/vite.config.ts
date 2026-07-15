import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import tailwindcss from "@tailwindcss/vite";
import viteReact from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { defineTanStackReactExampleConfig } from "../vite.config.shared";

export default defineTanStackReactExampleConfig({
  plugins: [tailwindcss(), tanstackStart(), viteReact()],
  browserProvider: playwright(),
  enforceAllSourceCoverage: true,
  optimizeDepsInclude: ["react-dom/server"],
});
