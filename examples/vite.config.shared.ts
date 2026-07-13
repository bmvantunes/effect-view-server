import { defineConfig, type PluginOption, type TestUserConfig } from "vite-plus";
import type { BrowserProviderOption } from "vite-plus/test/node";

interface TanStackReactExampleConfigOptions {
  readonly plugins: Array<PluginOption>;
  readonly browserProvider: BrowserProviderOption;
  readonly coverage?: NonNullable<TestUserConfig["coverage"]>;
}

export const defineTanStackReactExampleConfig = ({
  plugins,
  browserProvider,
  coverage,
}: TanStackReactExampleConfigOptions) =>
  defineConfig({
    optimizeDeps: {
      include: ["@effect/vitest", "effect/Array", "react-dom/client", "vitest-browser-react"],
      exclude: ["@tanstack/react-router", "@tanstack/react-start", "@tanstack/router-plugin"],
    },
    plugins,
    test: {
      include: ["src/**/*.test.ts"],
      typecheck: {
        enabled: true,
        checker: "tsc",
        include: ["src/**/*.test-d.ts", "src/**/*.browser.test.tsx"],
        tsconfig: "./tsconfig.json",
      },
      browser: {
        enabled: true,
        provider: browserProvider,
        headless: true,
        instances: [
          {
            browser: "chromium",
            name: "chromium",
            include: ["src/**/*.browser.test.tsx"],
          },
          {
            browser: "firefox",
            name: "firefox",
            include: ["src/**/*.browser.test.tsx"],
          },
          {
            browser: "webkit",
            name: "webkit",
            include: ["src/**/*.browser.test.tsx"],
          },
        ],
      },
      ...(coverage === undefined ? {} : { coverage }),
    },
    lint: {
      options: {
        typeAware: true,
        typeCheck: true,
      },
    },
    fmt: {},
  });
