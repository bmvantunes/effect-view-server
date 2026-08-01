import { describe, expect, it } from "@effect/vitest";
import {
  kafkaBrokerContractKey,
  normalizeKafkaCleanupPolicy,
  parseKafkaRetentionMs,
  resolveKafkaBrokerContracts,
  resolveKafkaRetention,
  type KafkaBrokerContractDeclaration,
} from "./broker-contract";

const declaration = (
  input: Partial<KafkaBrokerContractDeclaration> = {},
): KafkaBrokerContractDeclaration => ({
  viewServerTopic: "orders",
  sourceTopic: "source-orders",
  region: "eu",
  cleanupPolicy: "delete",
  retentionPolicy: {
    _tag: "MatchKafkaRetention",
  },
  ...input,
});

describe("Kafka broker contract", () => {
  it("normalizes every supported broker cleanup-policy spelling", () => {
    expect([
      normalizeKafkaCleanupPolicy("delete"),
      normalizeKafkaCleanupPolicy(" compact "),
      normalizeKafkaCleanupPolicy("compact,delete"),
      normalizeKafkaCleanupPolicy(" delete , compact "),
      normalizeKafkaCleanupPolicy("compact,\n delete"),
    ]).toStrictEqual([
      "delete",
      "compact",
      "compact-and-delete",
      "compact-and-delete",
      "compact-and-delete",
    ]);
    expect([
      normalizeKafkaCleanupPolicy(""),
      normalizeKafkaCleanupPolicy("delete,delete"),
      normalizeKafkaCleanupPolicy("compact,delete,other"),
      normalizeKafkaCleanupPolicy("other"),
    ]).toStrictEqual([undefined, "delete", undefined, undefined]);
  });

  it("accepts only Kafka's integral retention.ms domain", () => {
    expect([
      parseKafkaRetentionMs("-1"),
      parseKafkaRetentionMs("0"),
      parseKafkaRetentionMs("9223372036854775807"),
      parseKafkaRetentionMs("-2"),
      parseKafkaRetentionMs(" 1"),
      parseKafkaRetentionMs("1.5"),
      parseKafkaRetentionMs("not-a-number"),
    ]).toStrictEqual([
      -1n,
      0n,
      9_223_372_036_854_775_807n,
      undefined,
      undefined,
      undefined,
      undefined,
    ]);
  });

  it("resolves declared and broker-matched retention without losing precision", () => {
    expect([
      resolveKafkaRetention("delete", { _tag: "Forever" }, 1n),
      resolveKafkaRetention("delete", { _tag: "Finite", durationNanos: 123n }, -1n),
      resolveKafkaRetention("compact", { _tag: "MatchKafkaRetention" }, 5n),
      resolveKafkaRetention("compact-and-delete", { _tag: "MatchKafkaRetention" }, -1n),
      resolveKafkaRetention("delete", { _tag: "MatchKafkaRetention" }, 5n),
    ]).toStrictEqual([
      { _tag: "Forever" },
      { _tag: "Finite", durationNanos: 123n },
      { _tag: "Forever" },
      { _tag: "Forever" },
      { _tag: "Finite", durationNanos: 5_000_000n },
    ]);
    expect(kafkaBrokerContractKey("orders", "eu")).toBe('["orders","eu"]');
  });

  it("resolves all declarations from batched Region discoveries", () => {
    const compact = declaration({
      viewServerTopic: "inventory",
      sourceTopic: "source-inventory",
      cleanupPolicy: "compact-and-delete",
      retentionPolicy: { _tag: "Finite", durationNanos: 60n },
    });
    expect(
      resolveKafkaBrokerContracts(
        [declaration(), compact],
        [
          {
            _tag: "Available",
            region: "eu",
            resources: [
              {
                resourceName: "source-orders",
                cleanupPolicy: " delete ",
                retentionMs: "2500",
              },
              {
                resourceName: "source-inventory",
                cleanupPolicy: "compact, delete",
                retentionMs: "-1",
              },
            ],
          },
        ],
      ),
    ).toStrictEqual({
      _tag: "Resolved",
      contracts: [
        {
          ...declaration(),
          observedCleanupPolicy: "delete",
          observedRetentionMs: 2_500n,
          resolvedRetention: { _tag: "Finite", durationNanos: 2_500_000_000n },
        },
        {
          ...compact,
          observedCleanupPolicy: "compact-and-delete",
          observedRetentionMs: -1n,
          resolvedRetention: { _tag: "Finite", durationNanos: 60n },
        },
      ],
    });
    expect(resolveKafkaBrokerContracts([], [])).toStrictEqual({
      _tag: "Resolved",
      contracts: [],
    });
  });

  it("aggregates unavailable, malformed, mismatched, and invalid broker contracts", () => {
    const declarations = [
      declaration({ sourceTopic: "unavailable", region: "missing" }),
      declaration({ sourceTopic: "missing-resource" }),
      declaration({ sourceTopic: "duplicate-resource" }),
      declaration({ sourceTopic: "malformed-cleanup" }),
      declaration({ sourceTopic: "invalid-retention" }),
      declaration({ sourceTopic: "mismatched" }),
    ];
    expect(
      resolveKafkaBrokerContracts(declarations, [
        {
          _tag: "Unavailable",
          region: "missing",
        },
        {
          _tag: "Available",
          region: "eu",
          resources: [
            {
              resourceName: "duplicate-resource",
              cleanupPolicy: "delete",
              retentionMs: "-1",
            },
            {
              resourceName: "duplicate-resource",
              cleanupPolicy: "delete",
              retentionMs: "-1",
            },
            {
              resourceName: "malformed-cleanup",
              cleanupPolicy: "delete,compact,archive",
              retentionMs: "-1",
            },
            {
              resourceName: "invalid-retention",
              cleanupPolicy: "delete",
              retentionMs: "-2",
            },
            {
              resourceName: "mismatched",
              cleanupPolicy: "compact",
              retentionMs: "-1",
            },
          ],
        },
      ]),
    ).toStrictEqual({
      _tag: "KafkaBrokerContractValidationFailure",
      message: "Kafka broker cleanup and retention validation failed before runtime startup.",
      issues: [
        {
          _tag: "BrokerConfigurationUnavailable",
          region: "missing",
          topic: "unavailable",
        },
        {
          _tag: "MalformedBrokerConfiguration",
          region: "eu",
          topic: "missing-resource",
          configuration: "response",
        },
        {
          _tag: "MalformedBrokerConfiguration",
          region: "eu",
          topic: "duplicate-resource",
          configuration: "response",
        },
        {
          _tag: "MalformedBrokerConfiguration",
          region: "eu",
          topic: "malformed-cleanup",
          configuration: "cleanup.policy",
        },
        {
          _tag: "InvalidRetentionMs",
          region: "eu",
          topic: "invalid-retention",
        },
        {
          _tag: "CleanupPolicyMismatch",
          region: "eu",
          topic: "mismatched",
          declared: "delete",
          observed: "compact",
        },
      ],
    });
  });

  it("preserves the exact malformed broker configuration field", () => {
    expect(
      resolveKafkaBrokerContracts(
        [
          declaration({ sourceTopic: "missing-cleanup" }),
          declaration({ sourceTopic: "missing-retention" }),
        ],
        [
          {
            _tag: "Available",
            region: "eu",
            resources: [
              {
                resourceName: "missing-cleanup",
                malformedConfiguration: "cleanup.policy",
              },
              {
                resourceName: "missing-retention",
                malformedConfiguration: "retention.ms",
              },
            ],
          },
        ],
      ),
    ).toStrictEqual({
      _tag: "KafkaBrokerContractValidationFailure",
      message: "Kafka broker cleanup and retention validation failed before runtime startup.",
      issues: [
        {
          _tag: "MalformedBrokerConfiguration",
          region: "eu",
          topic: "missing-cleanup",
          configuration: "cleanup.policy",
        },
        {
          _tag: "MalformedBrokerConfiguration",
          region: "eu",
          topic: "missing-retention",
          configuration: "retention.ms",
        },
      ],
    });
  });

  it("rejects duplicate Region discovery responses deterministically", () => {
    expect(
      resolveKafkaBrokerContracts(
        [declaration()],
        [
          { _tag: "Available", region: "eu", resources: [] },
          { _tag: "Unavailable", region: "eu" },
        ],
      ),
    ).toStrictEqual({
      _tag: "KafkaBrokerContractValidationFailure",
      message: "Kafka broker cleanup and retention validation failed before runtime startup.",
      issues: [
        {
          _tag: "MalformedBrokerConfiguration",
          region: "eu",
          topic: "source-orders",
          configuration: "response",
        },
      ],
    });
    expect(
      resolveKafkaBrokerContracts(
        [],
        [
          { _tag: "Available", region: "orphan", resources: [] },
          { _tag: "Unavailable", region: "orphan" },
        ],
      ),
    ).toStrictEqual({
      _tag: "KafkaBrokerContractValidationFailure",
      message: "Kafka broker cleanup and retention validation failed before runtime startup.",
      issues: [
        {
          _tag: "MalformedBrokerConfiguration",
          region: "orphan",
          topic: "<unknown>",
          configuration: "response",
        },
      ],
    });
  });
});
