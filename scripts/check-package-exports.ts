import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import type { ViewServerLiveClient } from "@effect-view-server/client";
import type { ColumnLiveViewEngine } from "@effect-view-server/column-live-view-engine";
import type { ViewServerHealth } from "@effect-view-server/config/health";
import type { LiveSubscription } from "@effect-view-server/config/live-protocol";
import type { RawQuery } from "@effect-view-server/config/query";
import type { RuntimeEnvironmentConfig } from "@effect-view-server/config/runtime";
import type { ViewServerWireEvent } from "@effect-view-server/protocol";
import type { ViewServerRuntime } from "@effect-view-server/runtime";
import type { ViewServerHealthHttpJson, ViewServerWebSocketServer } from "@effect-view-server/server";
import type {
  ExactRuntimeOptions as PublicKafkaExactRuntimeOptions,
  KafkaTopicSourceDefinition as PublicKafkaTopicSourceDefinition,
  KafkaTopicSourceMapInput as PublicKafkaTopicSourceMapInput,
  ValidateKafkaTopicSource as PublicValidateKafkaTopicSource,
} from "effect-view-server/config/kafka";
// @ts-expect-error standalone gRPC topic selectors are not workspace config exports.
import type { GrpcLeasedTopic as RemovedWorkspaceGrpcLeasedTopic } from "@effect-view-server/config";
// @ts-expect-error standalone gRPC topic selectors are not workspace config exports.
import type { GrpcMaterializedTopic as RemovedWorkspaceGrpcMaterializedTopic } from "@effect-view-server/config";
// @ts-expect-error standalone gRPC topic selectors are not workspace gRPC exports.
import type { GrpcLeasedTopic as RemovedWorkspaceGrpcModuleLeasedTopic } from "@effect-view-server/config/grpc";
// @ts-expect-error standalone gRPC topic selectors are not workspace gRPC exports.
import type { GrpcMaterializedTopic as RemovedWorkspaceGrpcModuleMaterializedTopic } from "@effect-view-server/config/grpc";
// @ts-expect-error standalone gRPC feed definitions are not internal config exports.
import type { GrpcFeedDefinition as RemovedWorkspaceGrpcFeedDefinition } from "@effect-view-server/config/internal";
// @ts-expect-error standalone gRPC topic selectors are not public facade config exports.
import type { GrpcLeasedTopic as RemovedPublicGrpcLeasedTopic } from "effect-view-server/config";
// @ts-expect-error standalone gRPC topic selectors are not public facade config exports.
import type { GrpcMaterializedTopic as RemovedPublicGrpcMaterializedTopic } from "effect-view-server/config";
// @ts-expect-error standalone gRPC topic selectors are not public facade gRPC exports.
import type { GrpcLeasedTopic as RemovedPublicGrpcModuleLeasedTopic } from "effect-view-server/config/grpc";
// @ts-expect-error standalone gRPC topic selectors are not public facade gRPC exports.
import type { GrpcMaterializedTopic as RemovedPublicGrpcModuleMaterializedTopic } from "effect-view-server/config/grpc";
// @ts-expect-error runtime-only Kafka decoding is not a workspace config-root export.
import type { decodeKafkaTopicMessage as RemovedWorkspaceDecodeKafkaTopicMessage } from "@effect-view-server/config";
// @ts-expect-error decoded Kafka message internals are not workspace config-root exports.
import type { KafkaDecodedTopicMessage as RemovedWorkspaceKafkaDecodedTopicMessage } from "@effect-view-server/config";
// @ts-expect-error decoded Kafka source-message internals are not workspace config-root exports.
import type { KafkaDecodedTopicSourceMessage as RemovedWorkspaceKafkaDecodedTopicSourceMessage } from "@effect-view-server/config";
// @ts-expect-error resolved Kafka source internals are not workspace config-root exports.
import type { KafkaResolvedSourceTopicDefinition as RemovedWorkspaceKafkaResolvedSourceTopicDefinition } from "@effect-view-server/config";
// @ts-expect-error runtime-only Kafka decoding is not a public facade config-root export.
import type { decodeKafkaTopicMessage as RemovedPublicDecodeKafkaTopicMessage } from "effect-view-server/config";
// @ts-expect-error decoded Kafka message internals are not public facade config-root exports.
import type { KafkaDecodedTopicMessage as RemovedPublicKafkaDecodedTopicMessage } from "effect-view-server/config";
// @ts-expect-error decoded Kafka source-message internals are not public facade config-root exports.
import type { KafkaDecodedTopicSourceMessage as RemovedPublicKafkaDecodedTopicSourceMessage } from "effect-view-server/config";
// @ts-expect-error resolved Kafka source internals are not public facade config-root exports.
import type { KafkaResolvedSourceTopicDefinition as RemovedPublicKafkaResolvedSourceTopicDefinition } from "effect-view-server/config";
import {
  approvedPackageSpecifiers,
  forbiddenDeepImportSpecifiers,
  packageSurfacePolicy,
  runtimeSymbolPolicies,
  stalePackageSpecifiers,
} from "./package-surface-policy.ts";
import { inspectPrivateWorkspaceLeaks } from "./typescript-module-inspection.ts";
import {
  assertNoPackageSurfaceViolations,
  collectPackageSurfaceViolations,
} from "./check-internal-seams.ts";

type RemovedGrpcTypeExports = readonly [
  RemovedWorkspaceGrpcLeasedTopic,
  RemovedWorkspaceGrpcMaterializedTopic,
  RemovedWorkspaceGrpcModuleLeasedTopic,
  RemovedWorkspaceGrpcModuleMaterializedTopic,
  RemovedWorkspaceGrpcFeedDefinition,
  RemovedPublicGrpcLeasedTopic,
  RemovedPublicGrpcMaterializedTopic,
  RemovedPublicGrpcModuleLeasedTopic,
  RemovedPublicGrpcModuleMaterializedTopic,
];

type RemovedKafkaRootExports = readonly [
  typeof RemovedWorkspaceDecodeKafkaTopicMessage,
  RemovedWorkspaceKafkaDecodedTopicMessage,
  RemovedWorkspaceKafkaDecodedTopicSourceMessage,
  RemovedWorkspaceKafkaResolvedSourceTopicDefinition,
  typeof RemovedPublicDecodeKafkaTopicMessage,
  RemovedPublicKafkaDecodedTopicMessage,
  RemovedPublicKafkaDecodedTopicSourceMessage,
  RemovedPublicKafkaResolvedSourceTopicDefinition,
];

type PublicKafkaExportTopics = {
  readonly orders: {
    readonly key: "id";
    readonly schema: never;
  };
};
type PublicKafkaExportRegions = {
  readonly local: string;
};
type PublicKafkaTypeExports = readonly [
  PublicKafkaExactRuntimeOptions<PublicKafkaExportTopics, PublicKafkaExportRegions, {}>,
  PublicKafkaTopicSourceDefinition<PublicKafkaExportTopics, PublicKafkaExportRegions, "orders">,
  PublicKafkaTopicSourceMapInput<PublicKafkaExportTopics, "orders", "local", never, undefined>,
  PublicValidateKafkaTopicSource<
    PublicKafkaExportTopics,
    PublicKafkaExportRegions,
    "orders",
    "id",
    unknown
  >,
];

const removedGrpcTypeExports: RemovedGrpcTypeExports | undefined = undefined;
const removedKafkaRootExports: RemovedKafkaRootExports | undefined = undefined;
const publicKafkaTypeExports: PublicKafkaTypeExports | undefined = undefined;
const clientType: ViewServerLiveClient<Record<string, never>> | undefined = undefined;
const engineType: ColumnLiveViewEngine<Record<string, never>> | undefined = undefined;
const runtimeConfigType: RuntimeEnvironmentConfig | undefined = undefined;
const queryType: RawQuery<{ readonly id: string }> | undefined = undefined;
const healthType: ViewServerHealth<{ readonly orders: { readonly id: string } }> | undefined =
  undefined;
const subscriptionType: LiveSubscription<{ readonly id: string }> | undefined = undefined;
const serverType: ViewServerWebSocketServer | undefined = undefined;
const runtimeType: ViewServerRuntime<never> | undefined = undefined;
const healthHttpJsonType: ViewServerHealthHttpJson | undefined = undefined;
const wireEventType: ViewServerWireEvent | undefined = undefined;

void removedGrpcTypeExports;
void removedKafkaRootExports;
void publicKafkaTypeExports;
void clientType;
void engineType;
void runtimeConfigType;
void queryType;
void healthType;
void subscriptionType;
void serverType;
void runtimeType;
void healthHttpJsonType;
void wireEventType;

const repoRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const facadeDistRoot = join(
  repoRoot,
  "packages",
  packageSurfacePolicy.facade.directory,
  "dist",
);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const requireResolvable = (specifier: string): void => {
  try {
    import.meta.resolve(specifier);
  } catch (error) {
    throw new Error(`${specifier} should resolve as an approved package export.`, { cause: error });
  }
};

const rejectResolvable = (specifier: string): void => {
  let resolved: string | undefined;
  try {
    resolved = import.meta.resolve(specifier);
  } catch {
    return;
  }
  throw new Error(`${specifier} unexpectedly resolves as a package export: ${resolved}`);
};

const loadModule = async (specifier: string): Promise<Record<string, unknown>> => {
  const moduleValue: unknown = await import(specifier);
  if (!isRecord(moduleValue)) {
    throw new Error(`${specifier} did not resolve to an ES module namespace object.`);
  }
  return moduleValue;
};

assertNoPackageSurfaceViolations(collectPackageSurfaceViolations());

for (const specifier of approvedPackageSpecifiers) {
  requireResolvable(specifier);
}
for (const specifier of stalePackageSpecifiers) {
  rejectResolvable(specifier);
}
rejectResolvable(packageSurfacePolicy.facade.packageName);
for (const specifier of forbiddenDeepImportSpecifiers) {
  rejectResolvable(specifier);
}

for (const symbolPolicy of runtimeSymbolPolicies) {
  const moduleValue = await loadModule(symbolPolicy.specifier);
  for (const exportName of symbolPolicy.required) {
    if (!(exportName in moduleValue)) {
      throw new Error(`${symbolPolicy.specifier} is missing export ${exportName}.`);
    }
  }
  for (const exportName of symbolPolicy.forbidden) {
    if (exportName in moduleValue) {
      throw new Error(`${symbolPolicy.specifier} unexpectedly exports ${exportName}.`);
    }
  }
}

const distFiles = (directory: string): ReadonlyArray<string> =>
  readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      return distFiles(path);
    }
    return entry.isFile() && (entry.name.endsWith(".js") || entry.name.endsWith(".d.ts"))
      ? [path]
      : [];
  });

const emittedLeakViolations = distFiles(facadeDistRoot).flatMap((path) => {
  const relativePath = relative(repoRoot, path).replaceAll("\\", "/");
  const inspection = inspectPrivateWorkspaceLeaks({
    fileName: path,
    privateScope: "@effect-view-server",
    source: readFileSync(path, "utf8"),
  });
  return [
    ...inspection.privateSpecifiers.map(
      (specifier) => `${relativePath} imports private workspace package ${specifier}.`,
    ),
    ...inspection.violations.map(
      (violation) =>
        `${relativePath}:${violation.line}:${violation.column} contains unsupported ${violation.kind} module loading through ${violation.loader}.`,
    ),
  ];
});

if (emittedLeakViolations.length > 0) {
  throw new Error(
    [
      "Public effect-view-server output must not leak private workspace packages.",
      ...emittedLeakViolations.map((violation) => `- ${violation}`),
    ].join("\n"),
  );
}
