import { defineConfig, lazyPlugins, type PluginOption, type TestUserConfig } from "vite-plus";
import type { BrowserProviderOption } from "vite-plus/test/node";

interface ExampleViteEnvironment {
  readonly command: "build" | "serve";
  readonly mode: string;
}

const shouldGenerateExampleRoutes = ({ command, mode }: ExampleViteEnvironment): boolean =>
  !(command === "serve" && mode === "test");

export const reactCompilerForExample = ({ command, mode }: ExampleViteEnvironment) =>
  command === "serve" && mode === "test" ? false : { sources: ["/examples/"] };

type TanStackStartPluginFactory = (enableRouteGeneration: boolean) => Array<PluginOption>;

export const adaptTanStackStart =
  (
    tanstackStart: (options: {
      readonly router: { readonly enableRouteGeneration: boolean };
    }) => Array<PluginOption>,
  ): TanStackStartPluginFactory =>
  (enableRouteGeneration) =>
    tanstackStart({ router: { enableRouteGeneration } });

const createExampleTanStackStartPlugins = (
  tanstackStart: TanStackStartPluginFactory,
  environment: ExampleViteEnvironment,
): Array<PluginOption> => tanstackStart(shouldGenerateExampleRoutes(environment));

interface TanStackReactExampleConfigOptions {
  readonly createTanStackStartPlugins: TanStackStartPluginFactory;
  readonly createTailwindPlugin: () => PluginOption;
  readonly createReactPlugins: (options: {
    readonly compiler: ReturnType<typeof reactCompilerForExample>;
  }) => Array<PluginOption>;
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
  createTailwindPlugin,
  createReactPlugins,
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
    plugins: lazyPlugins(() => [
      createTailwindPlugin(),
      createExampleTanStackStartPlugins(createTanStackStartPlugins, environment),
      createReactPlugins({ compiler: reactCompilerForExample(environment) }),
    ]),
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
