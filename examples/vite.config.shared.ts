import { defineConfig, lazyPlugins, type PluginOption, type TestUserConfig } from "vite-plus";
import type { BrowserProviderOption } from "vite-plus/test/node";

interface ExampleRouteGenerationRuntime {
  readonly command: "build" | "serve";
  readonly mode: string;
}

type TanStackStartPluginFactory = (options: {
  readonly router: {
    readonly enableRouteGeneration: boolean;
  };
}) => Array<PluginOption>;

const shouldGenerateExampleRoutes = ({ command, mode }: ExampleRouteGenerationRuntime): boolean =>
  !(command === "serve" && mode === "test");

const createExampleTanStackStartPlugins = (
  tanstackStart: TanStackStartPluginFactory,
  environment: ExampleRouteGenerationRuntime,
): Array<PluginOption> =>
  tanstackStart({
    router: {
      enableRouteGeneration: shouldGenerateExampleRoutes(environment),
    },
  });

interface TanStackReactExampleConfigOptions {
  readonly createTanStackStartPlugins: TanStackStartPluginFactory;
  readonly plugins: (tanStackStartPlugins: Array<PluginOption>) => Array<PluginOption>;
  readonly browserProvider: BrowserProviderOption;
  readonly enforceAllSourceCoverage?: boolean;
  readonly includeNodeTests?: boolean;
  readonly optimizeDepsInclude?: ReadonlyArray<string>;
}

const exactAllSourceCoverage = {
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
} satisfies NonNullable<TestUserConfig["coverage"]>;

export const defineTanStackReactExampleConfig = ({
  createTanStackStartPlugins,
  plugins,
  browserProvider,
  enforceAllSourceCoverage,
  includeNodeTests,
  optimizeDepsInclude,
}: TanStackReactExampleConfigOptions) =>
  defineConfig((environment) => ({
    optimizeDeps: {
      include: [
        "@effect/vitest",
        "effect/Array",
        "react-dom/client",
        "vitest-browser-react",
        ...(optimizeDepsInclude ?? []),
      ],
      exclude: ["@tanstack/react-router", "@tanstack/react-start", "@tanstack/router-plugin"],
    },
    plugins: lazyPlugins(() =>
      plugins(createExampleTanStackStartPlugins(createTanStackStartPlugins, environment)),
    ),
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
      ...(includeNodeTests === true
        ? {
            projects: [
              {
                extends: true,
                test: {
                  name: "node",
                  browser: { enabled: false },
                  include: ["src/**/*.test.ts"],
                },
              },
              {
                extends: true,
                test: {
                  name: "browser",
                  typecheck: { enabled: false },
                },
              },
            ],
          }
        : {}),
      ...(enforceAllSourceCoverage === true ? { coverage: exactAllSourceCoverage } : {}),
    },
    lint: {
      options: {
        typeAware: true,
        typeCheck: true,
      },
    },
    fmt: {},
  }));
