export type PackageSurfaceEntrypoint = {
  readonly exportKey: string;
  readonly sourceEntrypoint: string;
  readonly facade?: {
    readonly additionalAllReexports?: ReadonlyArray<string>;
    readonly exportKey: string;
    readonly sourceEntrypoint: string;
  };
};

export type PrivatePackageSurface = {
  readonly architecture: {
    readonly allowedWorkspaceSpecifiers: ReadonlyArray<string>;
    readonly message: string;
    readonly relativeOverrides: ReadonlyArray<{
      readonly allowedWorkspaceSpecifiers: ReadonlyArray<string>;
      readonly relativePath: string;
    }>;
  };
  readonly directory: string;
  readonly packageName: string;
  readonly entrypoints: ReadonlyArray<PackageSurfaceEntrypoint>;
  readonly packOnlyEntrypoints?: ReadonlyArray<string>;
};

export type PackageSurfacePolicy = {
  readonly deepImportProbes: ReadonlyArray<string>;
  readonly deepImportSuffixes: ReadonlyArray<string>;
  readonly facade: {
    readonly directory: string;
    readonly packageName: string;
  };
  readonly packages: ReadonlyArray<PrivatePackageSurface>;
  readonly runtimeSymbols: ReadonlyArray<{
    readonly consumerForbidden?: ReadonlyArray<string>;
    readonly forbidden: ReadonlyArray<string>;
    readonly forbiddenSourceExports?: ReadonlyArray<string>;
    readonly required: ReadonlyArray<string>;
    readonly workspaceSpecifier: string;
  }>;
};

export const sourceModuleExtensions = [".ts", ".tsx", ".mts", ".cts"] as const;

export const packageSurfacePolicy = {
  deepImportProbes: [
    "@effect-view-server/client/src/live-client",
    "@effect-view-server/client/src/remote-client",
    "@effect-view-server/client/remote/client",
    "@effect-view-server/client/remote/internal",
    "@effect-view-server/column-live-view-engine/src/topic-store-state",
    "@effect-view-server/column-live-view-engine/topic-store-state",
    "@effect-view-server/config/src/grpc-contract",
    "@effect-view-server/config/grpc",
    "@effect-view-server/config/kafka",
    "@effect-view-server/config/src/source-contract",
    "@effect-view-server/config/src/source-query-contract",
    "@effect-view-server/config/dist/grpc-contract.js",
    "@effect-view-server/config/dist/source-contract.js",
    "@effect-view-server/config/dist/source-query-contract.js",
    "@effect-view-server/config/dist/topic-contract.js",
    "@effect-view-server/config/src/topic-contract",
    "@effect-view-server/grpc/src/model",
    "@effect-view-server/grpc/model",
    "@effect-view-server/grpc/internal",
    "@effect-view-server/config/query/raw-query-contract",
    "@effect-view-server/config/query/src/topic-contract",
    "@effect-view-server/protocol/src/protocol-json-field-codec",
    "@effect-view-server/protocol/protocol-json-field-codec",
    "@effect-view-server/protocol/src/protocol-row-codec",
    "@effect-view-server/protocol/protocol-row-codec",
    "@effect-view-server/react/dist/testing.js",
    "@effect-view-server/react/src/testing",
    "@effect-view-server/react/testing/internal",
    "@effect-view-server/runtime/src/internal",
    "@effect-view-server/runtime/internal",
    "@effect-view-server/runtime-core/src/health",
    "@effect-view-server/runtime-core/health",
    "@effect-view-server/server/src/rpc-handlers",
    "@effect-view-server/server/rpc-handlers",
    "@effect-view-server/source-adapter/src/model",
    "@effect-view-server/source-adapter/model",
    "@effect-view-server/source-adapter-testing/src/index",
    "effect-view-server/config/grpc",
    "effect-view-server/config/kafka",
  ],
  deepImportSuffixes: [
    "dist/index.d.ts",
    "dist/index.js",
    "dist/internal",
    "dist/internal.js",
    "internal",
    "src/index",
    "src/index.ts",
    "src/internal",
  ],
  facade: {
    directory: "effect-view-server",
    packageName: "effect-view-server",
  },
  packages: [
    {
      architecture: {
        allowedWorkspaceSpecifiers: [
          "@effect-view-server/config",
          "@effect-view-server/config/internal",
          "@effect-view-server/effect-utils",
          "@effect-view-server/protocol",
          "@effect-view-server/source-adapter",
        ],
        message: "Client code must not depend on runtime, server, React, in-memory, or engine code.",
        relativeOverrides: [],
      },
      directory: "client",
      packageName: "@effect-view-server/client",
      entrypoints: [
        {
          exportKey: ".",
          sourceEntrypoint: "src/index.ts",
          facade: { exportKey: "./client", sourceEntrypoint: "src/client.ts" },
        },
        {
          exportKey: "./remote",
          sourceEntrypoint: "src/remote.ts",
          facade: {
            exportKey: "./client/remote",
            sourceEntrypoint: "src/client-remote.ts",
          },
        },
      ],
    },
    {
      architecture: {
        allowedWorkspaceSpecifiers: [
          "effect-view-server/source-adapter",
          "effect-view-server/source-adapter/server",
        ],
        message:
          "The first-party Kafka Adapter must consume only the public Source Adapter SDK seams.",
        relativeOverrides: [
          {
            relativePath: "src/server.test.ts",
            allowedWorkspaceSpecifiers: [
              "@effect-view-server/config",
              "@effect-view-server/runtime-core",
              "effect-view-server/source-adapter",
              "effect-view-server/source-adapter/server",
            ],
          },
          {
            relativePath: "src/node.test.ts",
            allowedWorkspaceSpecifiers: [
              "@effect-view-server/config",
              "@effect-view-server/runtime-core",
              "effect-view-server/source-adapter",
              "effect-view-server/source-adapter/server",
            ],
          },
          {
            relativePath: "src/node.test-d.ts",
            allowedWorkspaceSpecifiers: [
              "@effect-view-server/config",
              "effect-view-server/source-adapter",
              "effect-view-server/source-adapter/server",
            ],
          },
        ],
      },
      directory: "kafka",
      packageName: "@effect-view-server/kafka",
      entrypoints: [
        {
          exportKey: "./contract",
          sourceEntrypoint: "src/contract.ts",
          facade: {
            exportKey: "./kafka/contract",
            sourceEntrypoint: "src/kafka-contract.ts",
          },
        },
        {
          exportKey: "./server",
          sourceEntrypoint: "src/server.ts",
          facade: {
            exportKey: "./kafka/server",
            sourceEntrypoint: "src/kafka-server.ts",
          },
        },
        {
          exportKey: "./node",
          sourceEntrypoint: "src/node.ts",
          facade: {
            exportKey: "./kafka/node",
            sourceEntrypoint: "src/kafka-node.ts",
          },
        },
      ],
    },
    {
      architecture: {
        allowedWorkspaceSpecifiers: [
          "@effect-view-server/config",
          "@effect-view-server/runtime-core",
          "@effect-view-server/source-adapter",
          "@effect-view-server/source-adapter-testing",
        ],
        message:
          "The Source Adapter conformance host may compose Runtime Core with the portable test driver only.",
        relativeOverrides: [],
      },
      directory: "source-adapter-conformance-host",
      packageName: "@effect-view-server/source-adapter-conformance-host",
      entrypoints: [
        {
          exportKey: ".",
          sourceEntrypoint: "src/index.ts",
          facade: {
            additionalAllReexports: ["@effect-view-server/source-adapter-testing"],
            exportKey: "./source-adapter/testing",
            sourceEntrypoint: "src/source-adapter-testing.ts",
          },
        },
      ],
    },
    {
      architecture: {
        allowedWorkspaceSpecifiers: [
          "@effect-view-server/config",
          "@effect-view-server/config/internal",
          "@effect-view-server/effect-utils",
        ],
        message:
          "The engine may use neutral Effect utilities but must stay transport/runtime independent.",
        relativeOverrides: [],
      },
      directory: "column-live-view-engine",
      packageName: "@effect-view-server/column-live-view-engine",
      entrypoints: [
        {
          exportKey: ".",
          sourceEntrypoint: "src/index.ts",
          facade: {
            exportKey: "./column-live-view-engine",
            sourceEntrypoint: "src/column-live-view-engine.ts",
          },
        },
        { exportKey: "./internal", sourceEntrypoint: "src/internal.ts" },
      ],
    },
    {
      architecture: {
        allowedWorkspaceSpecifiers: [
          "@effect-view-server/source-adapter",
          "@effect-view-server/source-adapter/definition",
          "@effect-view-server/source-adapter/internal",
        ],
        message: "Config contracts may depend only on the portable Source Adapter SDK.",
        relativeOverrides: [],
      },
      directory: "config",
      packageName: "@effect-view-server/config",
      entrypoints: [
        {
          exportKey: ".",
          sourceEntrypoint: "src/index.ts",
          facade: { exportKey: "./config", sourceEntrypoint: "src/config.ts" },
        },
        {
          exportKey: "./runtime",
          sourceEntrypoint: "src/runtime.ts",
          facade: { exportKey: "./config/runtime", sourceEntrypoint: "src/config-runtime.ts" },
        },
        {
          exportKey: "./query",
          sourceEntrypoint: "src/topic-contract.ts",
          facade: { exportKey: "./config/query", sourceEntrypoint: "src/config-query.ts" },
        },
        {
          exportKey: "./health",
          sourceEntrypoint: "src/health-contract.ts",
          facade: { exportKey: "./config/health", sourceEntrypoint: "src/config-health.ts" },
        },
        {
          exportKey: "./live-protocol",
          sourceEntrypoint: "src/live-protocol.ts",
          facade: {
            exportKey: "./config/live-protocol",
            sourceEntrypoint: "src/config-live-protocol.ts",
          },
        },
        { exportKey: "./internal", sourceEntrypoint: "src/internal.ts" },
      ],
    },
    {
      architecture: {
        allowedWorkspaceSpecifiers: [],
        message: "Effect utility helpers must stay independent of View Server packages.",
        relativeOverrides: [],
      },
      directory: "effect-utils",
      packageName: "@effect-view-server/effect-utils",
      entrypoints: [{ exportKey: ".", sourceEntrypoint: "src/index.ts" }],
    },
    {
      architecture: {
        allowedWorkspaceSpecifiers: [
          "effect-view-server/source-adapter",
          "effect-view-server/source-adapter/server",
        ],
        message:
          "The first-party gRPC Adapter must consume only the public Source Adapter SDK seams.",
        relativeOverrides: [],
      },
      directory: "grpc",
      packageName: "@effect-view-server/grpc",
      entrypoints: [
        {
          exportKey: "./contract",
          sourceEntrypoint: "src/contract.ts",
          facade: {
            exportKey: "./grpc/contract",
            sourceEntrypoint: "src/grpc-contract.ts",
          },
        },
        {
          exportKey: "./server",
          sourceEntrypoint: "src/server.ts",
          facade: {
            exportKey: "./grpc/server",
            sourceEntrypoint: "src/grpc-server.ts",
          },
        },
        {
          exportKey: "./node",
          sourceEntrypoint: "src/node.ts",
          facade: {
            exportKey: "./grpc/node",
            sourceEntrypoint: "src/grpc-node.ts",
          },
        },
      ],
    },
    {
      architecture: {
        allowedWorkspaceSpecifiers: [
          "@effect-view-server/client",
          "@effect-view-server/config",
          "@effect-view-server/runtime-core",
        ],
        message: "The in-memory Adapter must use runtime-core instead of reaching into lower layers.",
        relativeOverrides: [
          {
            allowedWorkspaceSpecifiers: ["@effect-view-server/runtime-core/internal"],
            relativePath: "src/testing.ts",
          },
        ],
      },
      directory: "in-memory",
      packageName: "@effect-view-server/in-memory",
      entrypoints: [
        {
          exportKey: ".",
          sourceEntrypoint: "src/index.ts",
          facade: { exportKey: "./in-memory", sourceEntrypoint: "src/in-memory.ts" },
        },
        {
          exportKey: "./testing",
          sourceEntrypoint: "src/testing.ts",
          facade: {
            exportKey: "./in-memory/testing",
            sourceEntrypoint: "src/in-memory-testing.ts",
          },
        },
      ],
    },
    {
      architecture: {
        allowedWorkspaceSpecifiers: [
          "@effect-view-server/config",
          "@effect-view-server/config/internal",
          "@effect-view-server/effect-utils",
          "@effect-view-server/source-adapter",
          "@effect-view-server/source-adapter/internal",
        ],
        message: "Protocol may depend on config contracts and neutral Effect utilities only.",
        relativeOverrides: [],
      },
      directory: "protocol",
      packageName: "@effect-view-server/protocol",
      entrypoints: [{ exportKey: ".", sourceEntrypoint: "src/index.ts" }],
    },
    {
      architecture: {
        allowedWorkspaceSpecifiers: [
          "@effect-view-server/client",
          "@effect-view-server/client/remote",
          "@effect-view-server/config",
          "@effect-view-server/effect-utils",
        ],
        message:
          "React bindings may use client transports but must not import runtime, server, engine, or in-memory outside the testing entrypoint.",
        relativeOverrides: [
          {
            allowedWorkspaceSpecifiers: [
              "@effect-view-server/in-memory",
              "@effect-view-server/in-memory/testing",
            ],
            relativePath: "src/testing.tsx",
          },
        ],
      },
      directory: "react",
      packageName: "@effect-view-server/react",
      entrypoints: [
        {
          exportKey: ".",
          sourceEntrypoint: "src/index.tsx",
          facade: { exportKey: "./react", sourceEntrypoint: "src/react.ts" },
        },
        {
          exportKey: "./testing",
          sourceEntrypoint: "src/testing.tsx",
          facade: { exportKey: "./react/testing", sourceEntrypoint: "src/react-testing.ts" },
        },
      ],
    },
    {
      architecture: {
        allowedWorkspaceSpecifiers: [
          "@effect-view-server/client",
          "@effect-view-server/config",
          "@effect-view-server/config/internal",
          "@effect-view-server/effect-utils",
          "@effect-view-server/runtime-core",
          "@effect-view-server/runtime-core/internal",
          "@effect-view-server/server",
        ],
        message: "Production runtime must compose runtime-core/server directly.",
        relativeOverrides: [],
      },
      directory: "runtime",
      packageName: "@effect-view-server/runtime",
      entrypoints: [
        {
          exportKey: ".",
          sourceEntrypoint: "src/index.ts",
          facade: { exportKey: "./runtime", sourceEntrypoint: "src/runtime.ts" },
        },
      ],
    },
    {
      architecture: {
        allowedWorkspaceSpecifiers: [
          "@effect-view-server/client",
          "@effect-view-server/config",
          "@effect-view-server/config/internal",
          "@effect-view-server/effect-utils",
          "@effect-view-server/column-live-view-engine",
          "@effect-view-server/column-live-view-engine/internal",
          "@effect-view-server/source-adapter",
          "@effect-view-server/source-adapter/internal",
        ],
        message: "Runtime core may compose client contracts, config, effect utils, and engine only.",
        relativeOverrides: [],
      },
      directory: "runtime-core",
      packageName: "@effect-view-server/runtime-core",
      entrypoints: [
        { exportKey: ".", sourceEntrypoint: "src/index.ts" },
        { exportKey: "./internal", sourceEntrypoint: "src/internal.ts" },
      ],
    },
    {
      architecture: {
        allowedWorkspaceSpecifiers: [],
        message: "The portable Source Adapter SDK must remain independent of View Server packages.",
        relativeOverrides: [],
      },
      directory: "source-adapter",
      packageName: "@effect-view-server/source-adapter",
      entrypoints: [
        {
          exportKey: ".",
          sourceEntrypoint: "src/index.ts",
          facade: {
            exportKey: "./source-adapter",
            sourceEntrypoint: "src/source-adapter.ts",
          },
        },
        {
          exportKey: "./server",
          sourceEntrypoint: "src/server.ts",
          facade: {
            exportKey: "./source-adapter/server",
            sourceEntrypoint: "src/source-adapter-server.ts",
          },
        },
        { exportKey: "./definition", sourceEntrypoint: "src/definition.ts" },
        { exportKey: "./internal", sourceEntrypoint: "src/internal.ts" },
      ],
    },
    {
      architecture: {
        allowedWorkspaceSpecifiers: [
          "@effect-view-server/source-adapter",
          "@effect-view-server/source-adapter/internal",
          "@effect-view-server/source-adapter/server",
        ],
        message: "Source Adapter test fixtures may depend only on the portable and server SDKs.",
        relativeOverrides: [],
      },
      directory: "source-adapter-testing",
      packageName: "@effect-view-server/source-adapter-testing",
      entrypoints: [
        {
          exportKey: ".",
          sourceEntrypoint: "src/index.ts",
        },
      ],
    },
    {
      architecture: {
        allowedWorkspaceSpecifiers: [
          "@effect-view-server/client",
          "@effect-view-server/config",
          "@effect-view-server/effect-utils",
          "@effect-view-server/protocol",
        ],
        message: "Server code may depend on protocol/client contracts, not runtime or React adapters.",
        relativeOverrides: [],
      },
      directory: "server",
      packageName: "@effect-view-server/server",
      entrypoints: [
        {
          exportKey: ".",
          sourceEntrypoint: "src/index.ts",
          facade: { exportKey: "./server", sourceEntrypoint: "src/server.ts" },
        },
      ],
    },
  ],
  runtimeSymbols: [
    {
      forbidden: [
        "makeViewServerClient",
        "createViewServerClient",
        "ViewServerRpcs",
        "ignoreLoggedTypedFailuresPreserveNonTypedFailures",
      ],
      required: ["stableQueryKey", "applyEvent"],
      workspaceSpecifier: "@effect-view-server/client",
    },
    {
      forbidden: [],
      required: ["makeViewServerClient", "createViewServerClient"],
      workspaceSpecifier: "@effect-view-server/client/remote",
    },
    {
      forbidden: ["createColumnLiveViewEngineInternal"],
      required: ["createColumnLiveViewEngine", "InvalidTopicError"],
      workspaceSpecifier: "@effect-view-server/column-live-view-engine",
    },
    {
      forbidden: [],
      required: ["createColumnLiveViewEngineInternal"],
      workspaceSpecifier: "@effect-view-server/column-live-view-engine/internal",
    },
    {
      forbidden: [
        "decodeKafkaCodec",
        "decodeKafkaTopicMessage",
        "defineGrpcFeed",
        "defineKafkaTopic",
        "grpc",
        "kafka",
      ],
      forbiddenSourceExports: [
        "decodeKafkaTopicMessage",
        "KafkaDecodedTopicMessage",
        "KafkaDecodedTopicSourceMessage",
        "KafkaResolvedSourceTopicDefinition",
      ],
      required: ["defineViewServerConfig", "viewSchema"],
      workspaceSpecifier: "@effect-view-server/config",
    },
    {
      forbidden: ["grpcNode", "grpcServer"],
      required: ["grpc", "GrpcSourceAdapter", "GrpcAdapterFailure"],
      workspaceSpecifier: "@effect-view-server/grpc/contract",
    },
    {
      forbidden: ["grpcNode"],
      required: ["grpcServer", "grpcServerLayer"],
      workspaceSpecifier: "@effect-view-server/grpc/server",
    },
    {
      forbidden: [],
      required: ["grpcNode", "grpcNodeLayer", "grpcNodeLayerConfig"],
      workspaceSpecifier: "@effect-view-server/grpc/node",
    },
    {
      forbidden: [],
      required: [],
      workspaceSpecifier: "@effect-view-server/config/health",
    },
    {
      forbidden: [
        "decodeKafkaCodec",
        "defineGrpcFeed",
        "defineKafkaTopic",
        "grpc",
        "kafka",
      ],
      required: [],
      workspaceSpecifier: "@effect-view-server/config/internal",
    },
    {
      forbidden: [],
      required: [],
      workspaceSpecifier: "@effect-view-server/config/live-protocol",
    },
    {
      forbidden: ["grpc", "defineGrpcFeed", "GrpcTopicSource", "GrpcLeasedTopicSource"],
      required: [],
      workspaceSpecifier: "@effect-view-server/config/query",
    },
    {
      forbidden: [],
      required: ["runtimeConfig", "runtimeEnvironmentConfig"],
      workspaceSpecifier: "@effect-view-server/config/runtime",
    },
    {
      forbidden: [],
      required: ["ignoreLoggedTypedFailuresPreserveNonTypedFailures"],
      workspaceSpecifier: "@effect-view-server/effect-utils",
    },
    {
      forbidden: [
        "createInMemoryViewServerTesting",
        "makeInMemoryViewServerTesting",
        "readHealth",
        "refreshHealth",
        "makeHealthRefreshScheduler",
      ],
      required: ["createInMemoryViewServer", "makeInMemoryViewServer"],
      workspaceSpecifier: "@effect-view-server/in-memory",
    },
    {
      forbidden: [],
      required: ["createInMemoryViewServerTesting", "makeInMemoryViewServerTesting"],
      workspaceSpecifier: "@effect-view-server/in-memory/testing",
    },
    {
      forbidden: [],
      required: ["ViewServerRpcs", "ViewServerWireRowSchema"],
      workspaceSpecifier: "@effect-view-server/protocol",
    },
    {
      forbidden: ["createInMemoryViewServerReact"],
      required: ["createViewServerReact"],
      workspaceSpecifier: "@effect-view-server/react",
    },
    {
      forbidden: [],
      required: ["createInMemoryViewServerReact"],
      workspaceSpecifier: "@effect-view-server/react/testing",
    },
    {
      forbidden: [],
      required: ["makeViewServerRuntime", "createViewServerRuntime", "runViewServerRuntime"],
      workspaceSpecifier: "@effect-view-server/runtime",
    },
    {
      forbidden: [
        "makeViewServerRuntimeCoreInternal",
        "makeSourceOwnershipPolicy",
        "makeRuntimeCoreMutationPipeline",
        "makeRuntimeCoreSourceManager",
        "getViewServerRuntimeCoreInternalLiveClient",
        "ViewServerRuntimeCoreInternalInstance",
        "ViewServerRuntimeCoreInternalLiveClient",
        "readHealth",
        "refreshHealth",
        "makeHealthRefreshScheduler",
      ],
      required: ["createViewServerRuntimeCore", "makeViewServerRuntimeCore"],
      workspaceSpecifier: "@effect-view-server/runtime-core",
    },
    {
      forbidden: [],
      required: [
        "makeViewServerRuntimeCoreInternal",
        "makeSourceOwnershipPolicy",
        "makeRuntimeCoreMutationPipeline",
      ],
      workspaceSpecifier: "@effect-view-server/runtime-core/internal",
    },
    {
      forbidden: [],
      required: ["makeViewServerWebSocketServer", "createViewServerWebSocketServer"],
      workspaceSpecifier: "@effect-view-server/server",
    },
    {
      forbidden: [],
      required: ["SourceAdapter"],
      workspaceSpecifier: "@effect-view-server/source-adapter",
    },
    {
      forbidden: [],
      required: ["isSourceDefinition"],
      workspaceSpecifier: "@effect-view-server/source-adapter/definition",
    },
    {
      forbidden: [],
      required: ["isSourceDefinition"],
      workspaceSpecifier: "@effect-view-server/source-adapter/internal",
    },
    {
      forbidden: [],
      required: ["SourceAdapterServer"],
      workspaceSpecifier: "@effect-view-server/source-adapter/server",
    },
    {
      forbidden: [],
      required: ["SourceFixture"],
      workspaceSpecifier: "@effect-view-server/source-adapter-testing",
    },
    {
      forbidden: [
        "KafkaSourceAdapterServer",
        "kafkaNode",
        "kafkaServer",
        "layer",
        "layerConfig",
        "makeKafkaServerLayer",
      ],
      required: ["KafkaSourceAdapter", "kafka"],
      workspaceSpecifier: "@effect-view-server/kafka/contract",
    },
    {
      forbidden: ["kafka", "kafkaNode", "layer", "layerConfig"],
      required: [
        "KafkaSourceAdapterServer",
        "kafkaServer",
        "makeKafkaServerLayer",
      ],
      workspaceSpecifier: "@effect-view-server/kafka/server",
    },
    {
      forbidden: [
        "KafkaSourceAdapter",
        "KafkaSourceAdapterServer",
        "kafka",
        "kafkaServer",
        "makeKafkaServerLayer",
      ],
      required: ["kafkaNode", "layer", "layerConfig"],
      workspaceSpecifier: "@effect-view-server/kafka/node",
    },
    {
      forbidden: [],
      required: [
        "registerSourceAdapterConformance",
        "registerSourceAdapterPackageConformance",
      ],
      workspaceSpecifier: "@effect-view-server/source-adapter-conformance-host",
    },
  ],
} as const satisfies PackageSurfacePolicy;

const privatePackageSurfacePolicies: ReadonlyArray<PrivatePackageSurface> =
  packageSurfacePolicy.packages;
const runtimeSymbolPolicyEntries: PackageSurfacePolicy["runtimeSymbols"] =
  packageSurfacePolicy.runtimeSymbols;

export const packageSpecifierFor = (packageName: string, exportKey: string): string =>
  exportKey === "." ? packageName : `${packageName}/${exportKey.slice(2)}`;

export const workspacePackageSpecifiers: ReadonlyArray<string> = packageSurfacePolicy.packages.flatMap(
  (packagePolicy) =>
    packagePolicy.entrypoints.map((entrypoint) =>
      packageSpecifierFor(packagePolicy.packageName, entrypoint.exportKey),
    ),
);

export const workspaceEntrypointPolicies = packageSurfacePolicy.packages.flatMap((packagePolicy) =>
  packagePolicy.entrypoints.map((entrypoint) => ({
    directory: packagePolicy.directory,
    sourceEntrypoint: entrypoint.sourceEntrypoint,
    specifier: packageSpecifierFor(packagePolicy.packageName, entrypoint.exportKey),
  })),
);

export const consumerPackageSpecifiers: ReadonlyArray<string> =
  packageSurfacePolicy.packages.flatMap((packagePolicy) =>
    packagePolicy.entrypoints.flatMap((entrypoint) =>
      "facade" in entrypoint
        ? [packageSpecifierFor(packageSurfacePolicy.facade.packageName, entrypoint.facade.exportKey)]
        : [],
    ),
  );

export const approvedPackageSpecifiers: ReadonlyArray<string> = [
  ...workspacePackageSpecifiers,
  ...consumerPackageSpecifiers,
];

const approvedPackageSpecifierSet = new Set(approvedPackageSpecifiers);

export const stalePackageSpecifiers: ReadonlyArray<string> = workspacePackageSpecifiers.map(
  (specifier) => specifier.replace("@effect-view-server", "@view-server"),
);

export const forbiddenDeepImportSpecifiers: ReadonlyArray<string> = Array.from(
  new Set([
    ...approvedPackageSpecifiers
      .flatMap((specifier) =>
        packageSurfacePolicy.deepImportSuffixes.map((suffix) => `${specifier}/${suffix}`),
      )
      .filter((specifier) => !approvedPackageSpecifierSet.has(specifier)),
    ...packageSurfacePolicy.deepImportProbes,
  ]),
).sort();

export const packageDistStemForSourceEntrypoint = (sourceEntrypoint: string): string => {
  const extension = sourceModuleExtensions.find((candidate) =>
    sourceEntrypoint.endsWith(candidate),
  );
  if (!sourceEntrypoint.startsWith("src/") || extension === undefined) {
    throw new Error(`Unsupported package source entrypoint: ${sourceEntrypoint}`);
  }
  const stem = sourceEntrypoint.slice("src/".length, -extension.length);
  if (stem.split("/").some((segment) => segment === "" || segment === "." || segment === "..")) {
    throw new Error(`Unsafe package source entrypoint: ${sourceEntrypoint}`);
  }
  return stem;
};

export type ExpectedManifestExport = {
  readonly exportKey: string;
  readonly importTarget: string;
  readonly typesTarget: string;
};

const expectedManifestExport = (
  exportKey: string,
  sourceEntrypoint: string,
): ExpectedManifestExport => {
  const stem = packageDistStemForSourceEntrypoint(sourceEntrypoint);
  return {
    exportKey,
    importTarget: `./dist/${stem}.js`,
    typesTarget: `./dist/${stem}.d.ts`,
  };
};

export type FacadeProjection = {
  readonly consumerExportKey: string;
  readonly consumerSourceEntrypoint: string;
  readonly consumerSpecifier: string;
  readonly workspaceSpecifier: string;
  readonly workspaceSpecifiers: ReadonlyArray<string>;
};

export const facadeProjections: ReadonlyArray<FacadeProjection> =
  privatePackageSurfacePolicies.flatMap((packagePolicy) =>
    packagePolicy.entrypoints.flatMap((entrypoint) => {
      const facade = entrypoint.facade;
      if (facade === undefined) {
        return [];
      }
      return [
        {
          consumerExportKey: facade.exportKey,
          consumerSourceEntrypoint: facade.sourceEntrypoint,
          consumerSpecifier: packageSpecifierFor(
            packageSurfacePolicy.facade.packageName,
            facade.exportKey,
          ),
          workspaceSpecifier: packageSpecifierFor(packagePolicy.packageName, entrypoint.exportKey),
          workspaceSpecifiers: [
            packageSpecifierFor(packagePolicy.packageName, entrypoint.exportKey),
            ...(facade.additionalAllReexports ?? []),
          ],
        },
      ];
    }),
  );

export const runtimeSymbolPolicies = [
  ...runtimeSymbolPolicyEntries.map((symbolPolicy) => ({
    forbidden: symbolPolicy.forbidden,
    required: symbolPolicy.required,
    specifier: symbolPolicy.workspaceSpecifier,
  })),
  ...facadeProjections.map((projection) => {
    const projectedSymbols = runtimeSymbolPolicyEntries.filter((symbolPolicy) =>
      projection.workspaceSpecifiers.includes(symbolPolicy.workspaceSpecifier),
    );
    return {
      forbidden: projectedSymbols.flatMap((symbolPolicy) => [
        ...symbolPolicy.forbidden,
        ...(symbolPolicy.consumerForbidden ?? []),
      ]),
      required: projectedSymbols.flatMap((symbolPolicy) => symbolPolicy.required),
      specifier: projection.consumerSpecifier,
    };
  }),
];

export type SourceForbiddenExportPolicy = {
  readonly directory: string;
  readonly forbidden: ReadonlyArray<string>;
  readonly sourceEntrypoint: string;
  readonly specifier: string;
};

export const sourceForbiddenExportPolicies: ReadonlyArray<SourceForbiddenExportPolicy> =
  runtimeSymbolPolicyEntries.flatMap((symbolPolicy) => {
    const forbidden = symbolPolicy.forbiddenSourceExports;
    if (forbidden === undefined) {
      return [];
    }
    return workspaceEntrypointPolicies
      .filter((entrypoint) => entrypoint.specifier === symbolPolicy.workspaceSpecifier)
      .map((entrypoint) => ({
        ...entrypoint,
        forbidden,
      }));
  });

export type ExpectedPackageSurface = {
  readonly directory: string;
  readonly manifestExports: ReadonlyArray<ExpectedManifestExport>;
  readonly packageName: string;
  readonly packEntrypoints: ReadonlyArray<string>;
};

export const expectedPackageSurfaces: ReadonlyArray<ExpectedPackageSurface> = [
  ...privatePackageSurfacePolicies.map((packagePolicy) => ({
    directory: packagePolicy.directory,
    packageName: packagePolicy.packageName,
    manifestExports: packagePolicy.entrypoints.map((entrypoint) =>
      expectedManifestExport(entrypoint.exportKey, entrypoint.sourceEntrypoint),
    ),
    packEntrypoints: [
      ...packagePolicy.entrypoints.map((entrypoint) => entrypoint.sourceEntrypoint),
      ...(packagePolicy.packOnlyEntrypoints ?? []),
    ],
  })),
  {
    directory: packageSurfacePolicy.facade.directory,
    packageName: packageSurfacePolicy.facade.packageName,
    manifestExports: facadeProjections.map((projection) =>
      expectedManifestExport(projection.consumerExportKey, projection.consumerSourceEntrypoint),
    ),
    packEntrypoints: facadeProjections.map((projection) => projection.consumerSourceEntrypoint),
  },
];

const unknownPolicyDirectory = (kind: string, directory: string): never => {
  throw new Error(`Unknown ${kind} policy directory: ${directory}`);
};

export const privatePackageSurfaceFor = (directory: string): PrivatePackageSurface =>
  packageSurfacePolicy.packages.find((packagePolicy) => packagePolicy.directory === directory) ??
  unknownPolicyDirectory("private package", directory);

export const expectedPackageSurfaceFor = (directory: string): ExpectedPackageSurface =>
  expectedPackageSurfaces.find((surface) => surface.directory === directory) ??
  unknownPolicyDirectory("package surface", directory);

export const facadeProjectionFor = (consumerSpecifier: string): (typeof facadeProjections)[number] =>
  facadeProjections.find((projection) => projection.consumerSpecifier === consumerSpecifier) ??
  unknownPolicyDirectory("facade projection", consumerSpecifier);

export const sourceForbiddenExportPolicyFor = (
  workspaceSpecifier: string,
): SourceForbiddenExportPolicy =>
  sourceForbiddenExportPolicies.find((policy) => policy.specifier === workspaceSpecifier) ??
  unknownPolicyDirectory("source forbidden export", workspaceSpecifier);
