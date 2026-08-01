import { Schema } from "effect";
import type {
  KafkaCapturedRetentionPolicy,
  KafkaCleanupPolicy,
  KafkaResolvedRetention,
} from "./contract";

const NonNegativeBigInt = Schema.BigInt.check(Schema.isGreaterThanOrEqualToBigInt(0n));

export const KafkaBrokerContractIssue = Schema.Union([
  Schema.TaggedStruct("BrokerConfigurationUnavailable", {
    region: Schema.NonEmptyString,
    topic: Schema.NonEmptyString,
  }),
  Schema.TaggedStruct("MalformedBrokerConfiguration", {
    region: Schema.NonEmptyString,
    topic: Schema.NonEmptyString,
    configuration: Schema.Literals(["response", "cleanup.policy", "retention.ms"]),
  }),
  Schema.TaggedStruct("CleanupPolicyMismatch", {
    region: Schema.NonEmptyString,
    topic: Schema.NonEmptyString,
    declared: Schema.Literals(["delete", "compact", "compact-and-delete"]),
    observed: Schema.Literals(["delete", "compact", "compact-and-delete"]),
  }),
  Schema.TaggedStruct("InvalidRetentionMs", {
    region: Schema.NonEmptyString,
    topic: Schema.NonEmptyString,
  }),
]);
export type KafkaBrokerContractIssue = typeof KafkaBrokerContractIssue.Type;

export const KafkaBrokerContractValidationFailure = Schema.TaggedStruct(
  "KafkaBrokerContractValidationFailure",
  {
    message: Schema.Literal(
      "Kafka broker cleanup and retention validation failed before runtime startup.",
    ),
    issues: Schema.NonEmptyArray(KafkaBrokerContractIssue),
  },
);
export type KafkaBrokerContractValidationFailure = typeof KafkaBrokerContractValidationFailure.Type;

export type KafkaBrokerContractDeclaration = {
  readonly viewServerTopic: string;
  readonly sourceTopic: string;
  readonly region: string;
  readonly cleanupPolicy: KafkaCleanupPolicy;
  readonly retentionPolicy: KafkaCapturedRetentionPolicy;
};

export type KafkaBrokerConfigResource =
  | {
      readonly resourceName: string;
      readonly cleanupPolicy: string;
      readonly retentionMs: string;
    }
  | {
      readonly resourceName: string;
      readonly malformedConfiguration: "cleanup.policy" | "retention.ms";
    };

export type KafkaResolvedBrokerContract = KafkaBrokerContractDeclaration & {
  readonly observedCleanupPolicy: KafkaCleanupPolicy;
  readonly observedRetentionMs: bigint;
  readonly resolvedRetention: KafkaResolvedRetention;
};

const snapshotCapturedRetentionPolicy = (
  policy: KafkaCapturedRetentionPolicy,
): KafkaCapturedRetentionPolicy =>
  policy._tag === "Finite"
    ? Object.freeze({
        _tag: policy._tag,
        durationNanos: policy.durationNanos,
      })
    : Object.freeze({
        _tag: policy._tag,
      });

const snapshotResolvedRetention = (retention: KafkaResolvedRetention): KafkaResolvedRetention =>
  retention._tag === "Finite"
    ? Object.freeze({
        _tag: retention._tag,
        durationNanos: retention.durationNanos,
      })
    : Object.freeze({
        _tag: retention._tag,
      });

export const snapshotKafkaResolvedBrokerContract = (
  contract: KafkaResolvedBrokerContract,
): KafkaResolvedBrokerContract =>
  Object.freeze({
    viewServerTopic: contract.viewServerTopic,
    sourceTopic: contract.sourceTopic,
    region: contract.region,
    cleanupPolicy: contract.cleanupPolicy,
    retentionPolicy: snapshotCapturedRetentionPolicy(contract.retentionPolicy),
    observedCleanupPolicy: contract.observedCleanupPolicy,
    observedRetentionMs: contract.observedRetentionMs,
    resolvedRetention: snapshotResolvedRetention(contract.resolvedRetention),
  });

export const kafkaBrokerContractKey = (viewServerTopic: string, region: string): string =>
  JSON.stringify([viewServerTopic, region]);

export type KafkaBrokerRegionDiscovery =
  | {
      readonly _tag: "Available";
      readonly region: string;
      readonly resources: ReadonlyArray<KafkaBrokerConfigResource>;
    }
  | {
      readonly _tag: "Unavailable";
      readonly region: string;
    };

export type KafkaBrokerContractResolution =
  | {
      readonly _tag: "Resolved";
      readonly contracts: ReadonlyArray<KafkaResolvedBrokerContract>;
    }
  | KafkaBrokerContractValidationFailure;

const validationFailure = (
  issues: readonly [KafkaBrokerContractIssue, ...ReadonlyArray<KafkaBrokerContractIssue>],
): KafkaBrokerContractValidationFailure => ({
  _tag: "KafkaBrokerContractValidationFailure",
  message: "Kafka broker cleanup and retention validation failed before runtime startup.",
  issues,
});

export const normalizeKafkaCleanupPolicy = (value: string): KafkaCleanupPolicy | undefined => {
  const parts = value
    .split(",")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
  const values = new Set(parts);
  if (values.size === 1 && values.has("delete")) {
    return "delete";
  }
  if (values.size === 1 && values.has("compact")) {
    return "compact";
  }
  if (values.size === 2 && values.has("compact") && values.has("delete")) {
    return "compact-and-delete";
  }
  return undefined;
};

export const parseKafkaRetentionMs = (value: string): bigint | undefined => {
  if (!/^-?\d+$/.test(value)) {
    return undefined;
  }
  const milliseconds = BigInt(value);
  return milliseconds === -1n || milliseconds >= 0n ? milliseconds : undefined;
};

export const resolveKafkaRetention = (
  cleanupPolicy: KafkaCleanupPolicy,
  configured: KafkaCapturedRetentionPolicy,
  observedRetentionMs: bigint,
): KafkaResolvedRetention => {
  if (configured._tag === "Forever") {
    return {
      _tag: "Forever",
    };
  }
  if (configured._tag === "Finite") {
    return {
      _tag: "Finite",
      durationNanos: configured.durationNanos,
    };
  }
  if (cleanupPolicy === "compact" || observedRetentionMs === -1n) {
    return {
      _tag: "Forever",
    };
  }
  return {
    _tag: "Finite",
    durationNanos: observedRetentionMs * 1_000_000n,
  };
};

const unavailableIssue = (
  declaration: KafkaBrokerContractDeclaration,
): KafkaBrokerContractIssue => ({
  _tag: "BrokerConfigurationUnavailable",
  region: declaration.region,
  topic: declaration.sourceTopic,
});

const malformedIssue = (
  declaration: KafkaBrokerContractDeclaration,
  configuration: "response" | "cleanup.policy" | "retention.ms",
): KafkaBrokerContractIssue => ({
  _tag: "MalformedBrokerConfiguration",
  region: declaration.region,
  topic: declaration.sourceTopic,
  configuration,
});

export const resolveKafkaBrokerContracts = (
  declarations: ReadonlyArray<KafkaBrokerContractDeclaration>,
  discoveries: ReadonlyArray<KafkaBrokerRegionDiscovery>,
): KafkaBrokerContractResolution => {
  const discoveryByRegion = new Map<string, KafkaBrokerRegionDiscovery>();
  for (const discovery of discoveries) {
    if (discoveryByRegion.has(discovery.region)) {
      const affected = declarations.filter(
        (declaration) => declaration.region === discovery.region,
      );
      const issues = affected.map((declaration) => malformedIssue(declaration, "response"));
      const first = issues[0];
      return first === undefined
        ? validationFailure([
            {
              _tag: "MalformedBrokerConfiguration",
              region: discovery.region,
              topic: "<unknown>",
              configuration: "response",
            },
          ])
        : validationFailure([first, ...issues.slice(1)]);
    }
    discoveryByRegion.set(discovery.region, discovery);
  }

  const issues: Array<KafkaBrokerContractIssue> = [];
  const contracts: Array<KafkaResolvedBrokerContract> = [];
  for (const declaration of declarations) {
    const discovery = discoveryByRegion.get(declaration.region);
    if (discovery === undefined || discovery._tag === "Unavailable") {
      issues.push(unavailableIssue(declaration));
      continue;
    }
    let resource: KafkaBrokerConfigResource | undefined;
    let resourceCount = 0;
    for (const candidate of discovery.resources) {
      if (candidate.resourceName === declaration.sourceTopic) {
        resource = candidate;
        resourceCount += 1;
      }
    }
    if (resourceCount !== 1 || resource === undefined) {
      issues.push(malformedIssue(declaration, "response"));
      continue;
    }
    if ("malformedConfiguration" in resource) {
      issues.push(malformedIssue(declaration, resource.malformedConfiguration));
      continue;
    }
    const observedCleanupPolicy = normalizeKafkaCleanupPolicy(resource.cleanupPolicy);
    if (observedCleanupPolicy === undefined) {
      issues.push(malformedIssue(declaration, "cleanup.policy"));
      continue;
    }
    const observedRetentionMs = parseKafkaRetentionMs(resource.retentionMs);
    if (observedRetentionMs === undefined) {
      issues.push({
        _tag: "InvalidRetentionMs",
        region: declaration.region,
        topic: declaration.sourceTopic,
      });
      continue;
    }
    if (observedCleanupPolicy !== declaration.cleanupPolicy) {
      issues.push({
        _tag: "CleanupPolicyMismatch",
        region: declaration.region,
        topic: declaration.sourceTopic,
        declared: declaration.cleanupPolicy,
        observed: observedCleanupPolicy,
      });
      continue;
    }
    contracts.push({
      ...declaration,
      observedCleanupPolicy,
      observedRetentionMs,
      resolvedRetention: resolveKafkaRetention(
        declaration.cleanupPolicy,
        declaration.retentionPolicy,
        observedRetentionMs,
      ),
    });
  }
  const firstIssue = issues[0];
  return firstIssue === undefined
    ? {
        _tag: "Resolved",
        contracts,
      }
    : validationFailure([firstIssue, ...issues.slice(1)]);
};

export const KafkaResolvedRetentionSchema = Schema.Union([
  Schema.TaggedStruct("Forever", {}),
  Schema.TaggedStruct("Finite", {
    durationNanos: NonNegativeBigInt,
  }),
]);
