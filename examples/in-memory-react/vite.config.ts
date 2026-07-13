import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import tailwindcss from "@tailwindcss/vite";
import viteReact from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { defineTanStackReactExampleConfig } from "../vite.config.shared";

export default defineTanStackReactExampleConfig({
  plugins: [tailwindcss(), tanstackStart(), viteReact()],
  browserProvider: playwright(),
  coverage: {
    provider: "istanbul",
    include: ["src/**/*.ts", "src/**/*.tsx"],
    exclude: [
      "src/router.tsx",
      "src/routeTree.gen.ts",
      "src/routes/**/*.tsx",
      "src/**/*.test.ts",
      "src/**/*.test.tsx",
      "src/**/*.test-d.ts",
    ],
    reporter: ["text"],
    thresholds: {
      "100": true,
    },
  },
});
