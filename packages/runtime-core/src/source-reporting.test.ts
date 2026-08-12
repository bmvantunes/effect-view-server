import { describe, expect, it } from "@effect/vitest";
import type {
  SourceExecutionFailure,
  SourceFailureClassification,
  SourceStatus,
} from "@effect-view-server/source-adapter";
import { Option } from "effect";
import {
  makeRuntimeSourceReportingState,
  runtimeSourceReportingSnapshot,
  sameRuntimeSourceReportingSnapshot,
  updateRuntimeSourceReportingState,
  type RuntimeSourceReportingDefinition,
} from "./source-reporting";

type Failure = {
  readonly _tag: "Self" | "Dependency";
  readonly target?: string;
};

const kafka: RuntimeSourceReportingDefinition = {
  source: "orders",
  dependency: "kafka",
  lifecycle: "materialized",
  targets: [
    { target: "tokyo", endpoints: ["b-1.kafka-tky.com", "b-2.kafka-tky.com"] },
    { target: "oregon", endpoints: ["b-1.kafka-oregon.com"] },
  ],
  classifyFailure: (failure) => {
    if (
      typeof failure === "object" &&
      failure !== null &&
      Reflect.get(failure, "_tag") === "Dependency"
    ) {
      const target = Reflect.get(failure, "target");
      return typeof target === "string"
        ? { problem: "dependency", targets: [target] }
        : { problem: "dependency" };
    }
    return { problem: "self" };
  },
};

const grpc: RuntimeSourceReportingDefinition = {
  source: "orders",
  dependency: "grpc",
  lifecycle: "leased",
  targets: [{ target: "orders", endpoints: ["https://orders.grpc-tky.com"] }],
  classifyFailure: () => ({ problem: "dependency" }),
};

const adapterFailure = (failure: Failure): SourceExecutionFailure<unknown> => ({
  _tag: "AdapterFailure",
  failure,
});

const waiting = (failure: SourceExecutionFailure<unknown>): SourceStatus<unknown, unknown> => ({
  _tag: "WaitingToRetry",
  nextAttempt: 2n,
  termination: { _tag: "Failed", failure },
  retryAtNanos: 2n,
});

const ready: SourceStatus<unknown, unknown> = {
  _tag: "Ready",
  attempt: 1n,
  readyAtNanos: 1n,
};

const malformedClassification = (value: unknown): SourceFailureClassification =>
  Reflect.apply((classification: SourceFailureClassification) => classification, undefined, [
    value,
  ]);

const throwingClassification = malformedClassification(
  new Proxy(
    {},
    {
      get: () => {
        throw new Error("hostile classification getter");
      },
    },
  ),
);

describe("Runtime Source reporting", () => {
  it("keeps delimiter-containing dependency identities distinct", () => {
    const left: RuntimeSourceReportingDefinition = {
      source: "left",
      dependency: "a\u0000b",
      lifecycle: "materialized",
      targets: [{ target: "c", endpoints: ["left"] }],
      classifyFailure: () => ({ problem: "dependency" }),
    };
    const right: RuntimeSourceReportingDefinition = {
      source: "right",
      dependency: "a",
      lifecycle: "materialized",
      targets: [{ target: "b\u0000c", endpoints: ["right"] }],
      classifyFailure: () => ({ problem: "dependency" }),
    };

    expect(runtimeSourceReportingSnapshot([left, right], []).dependencies).toStrictEqual([
      {
        dependency: "a",
        target: "b\u0000c",
        endpoints: ["right"],
        status: "Starting",
        issues: [],
      },
      {
        dependency: "a\u0000b",
        target: "c",
        endpoints: ["left"],
        status: "Starting",
        issues: [],
      },
    ]);
  });

  it("reports full inventory, regional dependency failures, recovery, and self failures", () => {
    const initial = runtimeSourceReportingSnapshot([kafka, grpc], []);

    expect(initial).toStrictEqual({
      heartbeat: { status: "Ready", problems: [] },
      dependencies: [
        {
          dependency: "grpc",
          target: "orders",
          endpoints: ["https://orders.grpc-tky.com"],
          status: "Inactive",
          issues: [],
        },
        {
          dependency: "kafka",
          target: "oregon",
          endpoints: ["b-1.kafka-oregon.com"],
          status: "Starting",
          issues: [],
        },
        {
          dependency: "kafka",
          target: "tokyo",
          endpoints: ["b-1.kafka-tky.com", "b-2.kafka-tky.com"],
          status: "Starting",
          issues: [],
        },
      ],
    });

    const state = makeRuntimeSourceReportingState(kafka, ready);
    const healthy = runtimeSourceReportingSnapshot([kafka, grpc], [state]);
    expect(healthy.heartbeat).toStrictEqual({ status: "Ready", problems: [] });
    expect(healthy.dependencies.map(({ target, status }) => ({ target, status }))).toStrictEqual([
      { target: "orders", status: "Inactive" },
      { target: "oregon", status: "Ready" },
      { target: "tokyo", status: "Ready" },
    ]);

    updateRuntimeSourceReportingState(
      state,
      waiting(adapterFailure({ _tag: "Dependency", target: "tokyo" })),
    );
    const failed = runtimeSourceReportingSnapshot([kafka, grpc], [state]);
    expect(failed.heartbeat).toStrictEqual({
      status: "WaitingToRetry",
      problems: ["dependency"],
    });
    expect(failed.dependencies.map(({ target, status }) => ({ target, status }))).toStrictEqual([
      { target: "orders", status: "Inactive" },
      { target: "oregon", status: "Ready" },
      { target: "tokyo", status: "WaitingToRetry" },
    ]);

    updateRuntimeSourceReportingState(state, ready);
    const recovered = runtimeSourceReportingSnapshot([kafka, grpc], [state]);
    expect(recovered.dependencies.map(({ target, status }) => ({ target, status }))).toStrictEqual([
      { target: "orders", status: "Inactive" },
      { target: "oregon", status: "Ready" },
      { target: "tokyo", status: "Ready" },
    ]);

    updateRuntimeSourceReportingState(state, waiting(adapterFailure({ _tag: "Self" })));
    const selfFailure = runtimeSourceReportingSnapshot([kafka, grpc], [state]);
    expect(selfFailure.heartbeat).toStrictEqual({
      status: "WaitingToRetry",
      problems: ["self"],
    });
    expect(
      selfFailure.dependencies.map(({ target, status }) => ({ target, status })),
    ).toStrictEqual([
      { target: "orders", status: "Inactive" },
      { target: "oregon", status: "Ready" },
      { target: "tokyo", status: "Ready" },
    ]);
  });

  it("keeps broker and Schema Registry health separate and clears detailed issues on recovery", () => {
    const definition: RuntimeSourceReportingDefinition = {
      source: "orders",
      dependency: "kafka",
      lifecycle: "materialized",
      targets: [
        {
          dependency: "kafka",
          target: "tokyo",
          endpoints: ["b-1.kafka-tky.com"],
        },
        {
          dependency: "schema-registry",
          target: "tokyo",
          endpoints: ["https://registry.kafka-tky.com"],
        },
      ],
      classifyFailure: (failure) => {
        const tag =
          typeof failure === "object" && failure !== null
            ? Reflect.get(failure, "_tag")
            : undefined;
        if (tag === "Registry") {
          return {
            problem: "dependency",
            targets: [{ dependency: "schema-registry", target: "tokyo" }],
            issue: {
              code: "KafkaSchemaRegistrySchemaMismatch",
              message: "Schema ID 42 is incompatible.",
              attributes: [
                { name: "subject", value: "source-orders-value" },
                { name: "side", value: "value" },
                { name: "schemaId", value: "42" },
              ],
            },
          };
        }
        return tag === "Kafka"
          ? {
              problem: "dependency",
              targets: [{ dependency: "kafka", target: "tokyo" }],
            }
          : { problem: "self" };
      },
    };
    const state = makeRuntimeSourceReportingState(definition, ready);

    updateRuntimeSourceReportingState(
      state,
      waiting({
        _tag: "AdapterFailure",
        failure: { _tag: "Registry" },
      }),
    );
    expect(runtimeSourceReportingSnapshot([definition], [state])).toStrictEqual({
      heartbeat: {
        status: "WaitingToRetry",
        problems: ["dependency"],
      },
      dependencies: [
        {
          dependency: "kafka",
          target: "tokyo",
          endpoints: ["b-1.kafka-tky.com"],
          status: "Ready",
          issues: [],
        },
        {
          dependency: "schema-registry",
          target: "tokyo",
          endpoints: ["https://registry.kafka-tky.com"],
          status: "WaitingToRetry",
          issues: [
            {
              source: "orders",
              code: "KafkaSchemaRegistrySchemaMismatch",
              message: "Schema ID 42 is incompatible.",
              attributes: [
                { name: "subject", value: "source-orders-value" },
                { name: "side", value: "value" },
                { name: "schemaId", value: "42" },
              ],
            },
          ],
        },
      ],
    });

    updateRuntimeSourceReportingState(
      state,
      waiting({
        _tag: "AdapterFailure",
        failure: { _tag: "Kafka" },
      }),
    );
    expect(runtimeSourceReportingSnapshot([definition], [state])).toStrictEqual({
      heartbeat: {
        status: "WaitingToRetry",
        problems: ["dependency"],
      },
      dependencies: [
        {
          dependency: "kafka",
          target: "tokyo",
          endpoints: ["b-1.kafka-tky.com"],
          status: "WaitingToRetry",
          issues: [],
        },
        {
          dependency: "schema-registry",
          target: "tokyo",
          endpoints: ["https://registry.kafka-tky.com"],
          status: "Ready",
          issues: [],
        },
      ],
    });

    updateRuntimeSourceReportingState(
      state,
      waiting({
        _tag: "AdapterFailure",
        failure: { _tag: "Self" },
      }),
    );
    expect(runtimeSourceReportingSnapshot([definition], [state])).toStrictEqual({
      heartbeat: {
        status: "WaitingToRetry",
        problems: ["self"],
      },
      dependencies: [
        {
          dependency: "kafka",
          target: "tokyo",
          endpoints: ["b-1.kafka-tky.com"],
          status: "Ready",
          issues: [],
        },
        {
          dependency: "schema-registry",
          target: "tokyo",
          endpoints: ["https://registry.kafka-tky.com"],
          status: "Ready",
          issues: [],
        },
      ],
    });

    updateRuntimeSourceReportingState(state, ready);
    expect(runtimeSourceReportingSnapshot([definition], [state]).dependencies).toStrictEqual([
      {
        dependency: "kafka",
        target: "tokyo",
        endpoints: ["b-1.kafka-tky.com"],
        status: "Ready",
        issues: [],
      },
      {
        dependency: "schema-registry",
        target: "tokyo",
        endpoints: ["https://registry.kafka-tky.com"],
        status: "Ready",
        issues: [],
      },
    ]);
  });

  it("applies a detailed dependency issue to every target when no targets are classified", () => {
    const definition: RuntimeSourceReportingDefinition = {
      ...grpc,
      classifyFailure: () => ({
        problem: "dependency",
        issue: {
          code: "GrpcUnavailable",
          message: "The gRPC dependency is unavailable.",
          attributes: [],
        },
      }),
    };
    const state = makeRuntimeSourceReportingState(
      definition,
      waiting(adapterFailure({ _tag: "Dependency" })),
    );

    expect(runtimeSourceReportingSnapshot([definition], [state]).dependencies).toStrictEqual([
      {
        dependency: "grpc",
        target: "orders",
        endpoints: ["https://orders.grpc-tky.com"],
        status: "WaitingToRetry",
        issues: [
          {
            source: "orders",
            code: "GrpcUnavailable",
            message: "The gRPC dependency is unavailable.",
            attributes: [],
          },
        ],
      },
    ]);
  });

  it("deduplicates, orders, and semantically compares detailed dependency issues", () => {
    const issueDefinition = (
      source: string,
      code: string,
      message = `${source}:${code}`,
      attributes: ReadonlyArray<{ readonly name: string; readonly value: string }> = [
        { name: "subject", value: `${source}-value` },
      ],
    ): RuntimeSourceReportingDefinition => ({
      source,
      dependency: "schema-registry",
      lifecycle: "materialized",
      targets: [
        {
          dependency: "schema-registry",
          target: "tokyo",
          endpoints: ["https://registry.kafka-tky.com"],
        },
      ],
      classifyFailure: () => ({
        problem: "dependency",
        targets: [{ dependency: "schema-registry", target: "tokyo" }],
        issue: {
          code,
          message,
          attributes,
        },
      }),
    });
    const degraded = (rejectedAtNanos: bigint): SourceStatus<unknown, unknown> => ({
      _tag: "Degraded",
      attempt: 1n,
      degradedAtNanos: rejectedAtNanos,
      reasons: [
        {
          _tag: "SourceItemRejection",
          latestRejection: {
            failure: adapterFailure({ _tag: "Dependency" }),
            location: `record-${String(rejectedAtNanos)}`,
            rejectedAtNanos,
          },
        },
      ],
    });
    const inventory = issueDefinition("inventory", "A");
    const ordersAFirst = issueDefinition("orders", "A", "orders:A:first");
    const ordersASecond = issueDefinition("orders", "A", "orders:A:second");
    const ordersB = issueDefinition("orders", "B");
    const ordersCSubjectZulu = issueDefinition("orders", "C", "orders:C", [
      { name: "subject", value: "zulu" },
    ]);
    const ordersCSubjectAlphaAndVersion = issueDefinition("orders", "C", "orders:C", [
      { name: "subject", value: "alpha" },
      { name: "version", value: "1" },
    ]);
    const ordersCSubjectAlpha = issueDefinition("orders", "C", "orders:C", [
      { name: "subject", value: "alpha" },
    ]);
    const ordersCRegion = issueDefinition("orders", "C", "orders:C", [
      { name: "region", value: "zulu" },
    ]);
    const definitions = [
      ordersASecond,
      ordersB,
      ordersCSubjectZulu,
      ordersCSubjectAlphaAndVersion,
      inventory,
      ordersCSubjectAlpha,
      ordersAFirst,
      ordersCRegion,
    ];
    const makeStates = () =>
      definitions.map((definition) => makeRuntimeSourceReportingState(definition, degraded(1n)));
    const firstStates = makeStates();
    const ordersASecondDuplicateState = Option.getOrThrow(Option.fromUndefinedOr(firstStates[0]));

    expect(updateRuntimeSourceReportingState(ordersASecondDuplicateState, degraded(2n))).toBe(
      false,
    );
    const first = runtimeSourceReportingSnapshot(definitions, [
      ...firstStates,
      ordersASecondDuplicateState,
    ]);
    const second = runtimeSourceReportingSnapshot(definitions, [...makeStates()].reverse());

    expect(first).toStrictEqual({
      heartbeat: { status: "Degraded", problems: ["dependency"] },
      dependencies: [
        {
          dependency: "schema-registry",
          target: "tokyo",
          endpoints: ["https://registry.kafka-tky.com"],
          status: "Degraded",
          issues: [
            {
              source: "inventory",
              code: "A",
              message: "inventory:A",
              attributes: [{ name: "subject", value: "inventory-value" }],
            },
            {
              source: "orders",
              code: "A",
              message: "orders:A:first",
              attributes: [{ name: "subject", value: "orders-value" }],
            },
            {
              source: "orders",
              code: "A",
              message: "orders:A:second",
              attributes: [{ name: "subject", value: "orders-value" }],
            },
            {
              source: "orders",
              code: "B",
              message: "orders:B",
              attributes: [{ name: "subject", value: "orders-value" }],
            },
            {
              source: "orders",
              code: "C",
              message: "orders:C",
              attributes: [{ name: "region", value: "zulu" }],
            },
            {
              source: "orders",
              code: "C",
              message: "orders:C",
              attributes: [
                { name: "subject", value: "alpha" },
                { name: "version", value: "1" },
              ],
            },
            {
              source: "orders",
              code: "C",
              message: "orders:C",
              attributes: [{ name: "subject", value: "alpha" }],
            },
            {
              source: "orders",
              code: "C",
              message: "orders:C",
              attributes: [{ name: "subject", value: "zulu" }],
            },
          ],
        },
      ],
    });
    expect(sameRuntimeSourceReportingSnapshot(first, second)).toBe(true);
  });

  it("aggregates canonical status precedence and ordered problem provenance", () => {
    const selfState = makeRuntimeSourceReportingState(
      kafka,
      waiting({
        _tag: "RuntimeFailure",
        failure: { _tag: "InvalidSourceMetrics", message: "bad metrics" },
      }),
    );
    const dependencyState = makeRuntimeSourceReportingState(grpc, {
      _tag: "Exhausted",
      exhaustion: {
        _tag: "RetryExhausted",
        lastTermination: { _tag: "UnexpectedCompletion" },
      },
      exhaustedAtNanos: 3n,
    });
    const snapshot = runtimeSourceReportingSnapshot([kafka, grpc], [selfState, dependencyState]);

    expect(snapshot.heartbeat).toStrictEqual({
      status: "Exhausted",
      problems: ["self", "dependency"],
    });
    expect(snapshot.dependencies[0]).toStrictEqual({
      dependency: "grpc",
      target: "orders",
      endpoints: ["https://orders.grpc-tky.com"],
      status: "Exhausted",
      issues: [],
    });
  });

  it("applies every heartbeat precedence level and ignores Source-level stopping", () => {
    const states: Array<ReturnType<typeof makeRuntimeSourceReportingState>> = [];
    const append = (status: SourceStatus<unknown, unknown>) => {
      states.push(makeRuntimeSourceReportingState(kafka, status));
      return runtimeSourceReportingSnapshot([kafka], states).heartbeat.status;
    };

    expect(append(ready)).toBe("Ready");
    expect(
      append({
        _tag: "Degraded",
        attempt: 1n,
        degradedAtNanos: 2n,
        reasons: [{ _tag: "AdapterMaintenanceFailure" }],
      }),
    ).toBe("Degraded");
    expect(append({ _tag: "Starting", attempt: 1n, startedAtNanos: 3n })).toBe("Starting");
    expect(
      append({
        _tag: "Reacquiring",
        previousTermination: { _tag: "UnexpectedCompletion" },
        attempt: 2n,
        startedAtNanos: 4n,
      }),
    ).toBe("Reacquiring");
    expect(append(waiting(adapterFailure({ _tag: "Dependency" })))).toBe("WaitingToRetry");
    expect(
      append({
        _tag: "Exhausted",
        exhaustion: {
          _tag: "RetryExhausted",
          lastTermination: { _tag: "UnexpectedCompletion" },
        },
        exhaustedAtNanos: 5n,
      }),
    ).toBe("Exhausted");
    expect(
      append({
        _tag: "Stopping",
        reason: "runtime-shutdown",
        stoppingAtNanos: 6n,
      }),
    ).toBe("Exhausted");
  });

  it("projects degraded evidence, reacquisition, stopping, and malformed classifications", () => {
    const throwing: RuntimeSourceReportingDefinition = {
      ...kafka,
      classifyFailure: () => {
        throw new Error("classifier defect");
      },
    };
    const state = makeRuntimeSourceReportingState(throwing, {
      _tag: "Degraded",
      attempt: 1n,
      degradedAtNanos: 2n,
      reasons: [
        {
          _tag: "SourceItemRejection",
          latestRejection: {
            failure: adapterFailure({ _tag: "Dependency" }),
            location: "record",
            rejectedAtNanos: 2n,
          },
        },
        { _tag: "AdapterMaintenanceFailure" },
      ],
    });
    expect(runtimeSourceReportingSnapshot([throwing], [state]).heartbeat).toStrictEqual({
      status: "Degraded",
      problems: ["self"],
    });

    updateRuntimeSourceReportingState(state, {
      _tag: "Reacquiring",
      previousTermination: { _tag: "UnexpectedCompletion" },
      attempt: 2n,
      startedAtNanos: 3n,
    });
    expect(runtimeSourceReportingSnapshot([throwing], [state]).heartbeat).toStrictEqual({
      status: "Reacquiring",
      problems: ["dependency"],
    });

    updateRuntimeSourceReportingState(state, {
      _tag: "Stopping",
      reason: "runtime-shutdown",
      stoppingAtNanos: 4n,
    });
    expect(runtimeSourceReportingSnapshot([throwing], [state]).heartbeat).toStrictEqual({
      status: "Ready",
      problems: [],
    });
  });

  it.each([
    {
      label: "undefined",
      value: malformedClassification(undefined),
      reason: "Source failure classification must be an object.",
    },
    {
      label: "a primitive",
      value: malformedClassification("dependency"),
      reason: "Source failure classification must be an object.",
    },
    {
      label: "an invalid problem",
      value: malformedClassification({ problem: "upstream" }),
      reason: "Source failure classification has an invalid problem.",
    },
    {
      label: "dependency targets on self provenance",
      value: malformedClassification({ problem: "self", targets: ["tokyo"] }),
      reason: "Self failure classification cannot contain dependency fields.",
    },
    {
      label: "a dependency issue on self provenance",
      value: malformedClassification({ problem: "self", issue: { code: "Failure" } }),
      reason: "Self failure classification cannot contain dependency fields.",
    },
    {
      label: "an explicit undefined dependency target on self provenance",
      value: malformedClassification({ problem: "self", targets: undefined }),
      reason: "Self failure classification cannot contain dependency fields.",
    },
    {
      label: "an explicit undefined dependency issue on self provenance",
      value: malformedClassification({ problem: "self", issue: undefined }),
      reason: "Self failure classification cannot contain dependency fields.",
    },
    {
      label: "non-array targets",
      value: malformedClassification({ problem: "dependency", targets: "tokyo" }),
      reason: "Source failure classification has invalid dependency targets.",
    },
    {
      label: "non-string targets",
      value: malformedClassification({ problem: "dependency", targets: [1] }),
      reason: "Source failure classification has invalid dependency targets.",
    },
    {
      label: "empty targets",
      value: malformedClassification({ problem: "dependency", targets: [""] }),
      reason: "Source failure classification has invalid dependency targets.",
    },
    {
      label: "an invalid structured target",
      value: malformedClassification({
        problem: "dependency",
        targets: [{ dependency: "schema-registry", target: "" }],
      }),
      reason: "Source failure classification has invalid dependency targets.",
    },
    {
      label: "a primitive dependency issue",
      value: malformedClassification({ problem: "dependency", issue: "invalid" }),
      reason: "Source failure classification has an invalid dependency issue.",
    },
    {
      label: "invalid dependency issue fields",
      value: malformedClassification({
        problem: "dependency",
        issue: { code: "Failure", message: "invalid", attributes: "invalid" },
      }),
      reason: "Source failure classification has an invalid dependency issue.",
    },
    {
      label: "an empty dependency issue code",
      value: malformedClassification({
        problem: "dependency",
        issue: { code: "", message: "invalid", attributes: [] },
      }),
      reason: "Source failure classification has an invalid dependency issue.",
    },
    {
      label: "a primitive dependency attribute",
      value: malformedClassification({
        problem: "dependency",
        issue: { code: "Failure", message: "invalid", attributes: [1] },
      }),
      reason: "Source failure classification has invalid dependency attributes.",
    },
    {
      label: "invalid dependency attribute fields",
      value: malformedClassification({
        problem: "dependency",
        issue: {
          code: "Failure",
          message: "invalid",
          attributes: [{ name: "subject", value: 1 }],
        },
      }),
      reason: "Source failure classification has invalid dependency attributes.",
    },
    {
      label: "an empty dependency attribute name",
      value: malformedClassification({
        problem: "dependency",
        issue: {
          code: "Failure",
          message: "invalid",
          attributes: [{ name: "", value: "x" }],
        },
      }),
      reason: "Source failure classification has invalid dependency attributes.",
    },
    {
      label: "a throwing getter",
      value: throwingClassification,
      reason: "hostile classification getter",
    },
  ])("diagnoses $label classification as self provenance", ({ value, reason }) => {
    const malformed: RuntimeSourceReportingDefinition = {
      ...kafka,
      classifyFailure: () => value,
    };
    const state = makeRuntimeSourceReportingState(
      malformed,
      waiting(adapterFailure({ _tag: "Dependency", target: "tokyo" })),
    );
    const snapshot = runtimeSourceReportingSnapshot([malformed], [state]);

    expect(snapshot.heartbeat).toStrictEqual({
      status: "WaitingToRetry",
      problems: ["self"],
    });
    expect(snapshot.dependencies).toStrictEqual([
      {
        dependency: "kafka",
        target: "oregon",
        endpoints: ["b-1.kafka-oregon.com"],
        status: "Starting",
        issues: [
          {
            source: "orders",
            code: "InvalidSourceFailureClassification",
            message: "The Source Adapter returned an invalid failure classification.",
            attributes: [{ name: "reason", value: reason }],
          },
        ],
      },
      {
        dependency: "kafka",
        target: "tokyo",
        endpoints: ["b-1.kafka-tky.com", "b-2.kafka-tky.com"],
        status: "Starting",
        issues: [
          {
            source: "orders",
            code: "InvalidSourceFailureClassification",
            message: "The Source Adapter returned an invalid failure classification.",
            attributes: [{ name: "reason", value: reason }],
          },
        ],
      },
    ]);
  });

  it("diagnoses classifiers that fail with non-Error values", () => {
    const malformed: RuntimeSourceReportingDefinition = {
      ...kafka,
      classifyFailure: () => {
        throw "classifier defect";
      },
    };
    const state = makeRuntimeSourceReportingState(
      malformed,
      waiting(adapterFailure({ _tag: "Dependency", target: "tokyo" })),
    );

    expect(
      runtimeSourceReportingSnapshot([malformed], [state]).dependencies[0]?.issues,
    ).toStrictEqual([
      {
        source: "orders",
        code: "InvalidSourceFailureClassification",
        message: "The Source Adapter returned an invalid failure classification.",
        attributes: [{ name: "reason", value: "The classifier failed with a non-Error value." }],
      },
    ]);
  });

  it("deduplicates inventories and compares semantic snapshots", () => {
    const duplicate: RuntimeSourceReportingDefinition = {
      ...kafka,
      lifecycle: "leased",
      targets: [{ target: "tokyo", endpoints: ["b-2.kafka-tky.com", "b-3.kafka-tky.com"] }],
    };
    const first = runtimeSourceReportingSnapshot([kafka, duplicate], []);
    const second = runtimeSourceReportingSnapshot([kafka, duplicate], []);
    const changed = runtimeSourceReportingSnapshot([grpc], []);

    expect(first.dependencies[1]).toStrictEqual({
      dependency: "kafka",
      target: "tokyo",
      endpoints: ["b-1.kafka-tky.com", "b-2.kafka-tky.com", "b-3.kafka-tky.com"],
      status: "Starting",
      issues: [],
    });
    expect(sameRuntimeSourceReportingSnapshot(first, second)).toBe(true);
    expect(sameRuntimeSourceReportingSnapshot(first, changed)).toBe(false);
  });

  it("falls back to every target when dependency evidence omits or misses target identity", () => {
    const state = makeRuntimeSourceReportingState(kafka, ready);
    updateRuntimeSourceReportingState(state, waiting(adapterFailure({ _tag: "Dependency" })));
    expect(runtimeSourceReportingSnapshot([kafka], [state]).dependencies).toStrictEqual([
      {
        dependency: "kafka",
        target: "oregon",
        endpoints: ["b-1.kafka-oregon.com"],
        status: "WaitingToRetry",
        issues: [],
      },
      {
        dependency: "kafka",
        target: "tokyo",
        endpoints: ["b-1.kafka-tky.com", "b-2.kafka-tky.com"],
        status: "WaitingToRetry",
        issues: [],
      },
    ]);

    updateRuntimeSourceReportingState(
      state,
      waiting(adapterFailure({ _tag: "Dependency", target: "unknown" })),
    );
    expect(runtimeSourceReportingSnapshot([kafka], [state]).dependencies).toStrictEqual([
      {
        dependency: "kafka",
        target: "oregon",
        endpoints: ["b-1.kafka-oregon.com"],
        status: "WaitingToRetry",
        issues: [],
      },
      {
        dependency: "kafka",
        target: "tokyo",
        endpoints: ["b-1.kafka-tky.com", "b-2.kafka-tky.com"],
        status: "WaitingToRetry",
        issues: [],
      },
    ]);

    const partiallyUnknown: RuntimeSourceReportingDefinition = {
      ...kafka,
      classifyFailure: () => ({
        problem: "dependency",
        targets: ["tokyo", "orgeon"],
      }),
    };
    const partiallyUnknownState = makeRuntimeSourceReportingState(partiallyUnknown, ready);
    updateRuntimeSourceReportingState(
      partiallyUnknownState,
      waiting(adapterFailure({ _tag: "Dependency" })),
    );
    expect(
      runtimeSourceReportingSnapshot([partiallyUnknown], [partiallyUnknownState]).dependencies,
    ).toStrictEqual([
      {
        dependency: "kafka",
        target: "oregon",
        endpoints: ["b-1.kafka-oregon.com"],
        status: "WaitingToRetry",
        issues: [],
      },
      {
        dependency: "kafka",
        target: "tokyo",
        endpoints: ["b-1.kafka-tky.com", "b-2.kafka-tky.com"],
        status: "WaitingToRetry",
        issues: [],
      },
    ]);
  });

  it("reports whether a per-source semantic projection actually changed", () => {
    const state = makeRuntimeSourceReportingState(kafka, ready);

    expect(updateRuntimeSourceReportingState(state, ready)).toBe(false);
    expect(
      updateRuntimeSourceReportingState(state, {
        _tag: "Ready",
        attempt: 2n,
        readyAtNanos: 2n,
      }),
    ).toBe(false);

    const tokyoDegraded: SourceStatus<unknown, unknown> = {
      _tag: "Degraded",
      attempt: 2n,
      degradedAtNanos: 3n,
      reasons: [
        {
          _tag: "SourceItemRejection",
          latestRejection: {
            failure: adapterFailure({ _tag: "Dependency", target: "tokyo" }),
            location: "record-1",
            rejectedAtNanos: 3n,
          },
        },
      ],
    };
    expect(updateRuntimeSourceReportingState(state, tokyoDegraded)).toBe(true);
    expect(
      updateRuntimeSourceReportingState(state, {
        ...tokyoDegraded,
        degradedAtNanos: 4n,
        reasons: [
          {
            _tag: "SourceItemRejection",
            latestRejection: {
              failure: adapterFailure({ _tag: "Dependency", target: "tokyo" }),
              location: "record-2",
              rejectedAtNanos: 4n,
            },
          },
        ],
      }),
    ).toBe(false);
    expect(
      updateRuntimeSourceReportingState(state, {
        ...tokyoDegraded,
        degradedAtNanos: 5n,
        reasons: [
          {
            _tag: "SourceItemRejection",
            latestRejection: {
              failure: adapterFailure({ _tag: "Dependency", target: "oregon" }),
              location: "record-3",
              rejectedAtNanos: 5n,
            },
          },
        ],
      }),
    ).toBe(true);
  });

  it("classifies degraded runtime and untargeted adapter rejections", () => {
    const state = makeRuntimeSourceReportingState(kafka, {
      _tag: "Degraded",
      attempt: 1n,
      degradedAtNanos: 1n,
      reasons: [
        {
          _tag: "SourceItemRejection",
          latestRejection: {
            failure: {
              _tag: "RuntimeFailure",
              failure: { _tag: "InvalidSourceDelivery", message: "invalid" },
            },
            location: "record",
            rejectedAtNanos: 1n,
          },
        },
      ],
    });
    expect(runtimeSourceReportingSnapshot([kafka], [state]).heartbeat).toStrictEqual({
      status: "Degraded",
      problems: ["self"],
    });

    updateRuntimeSourceReportingState(state, {
      _tag: "Degraded",
      attempt: 1n,
      degradedAtNanos: 2n,
      reasons: [
        {
          _tag: "SourceItemRejection",
          latestRejection: {
            failure: adapterFailure({ _tag: "Dependency" }),
            location: "record",
            rejectedAtNanos: 2n,
          },
        },
      ],
    });
    expect(runtimeSourceReportingSnapshot([kafka], [state]).heartbeat).toStrictEqual({
      status: "Degraded",
      problems: ["dependency"],
    });
  });

  it("compares every dependency field semantically", () => {
    const base = runtimeSourceReportingSnapshot(
      [kafka],
      [makeRuntimeSourceReportingState(kafka, ready)],
    );
    const dependency = Option.getOrThrow(Option.fromUndefinedOr(base.dependencies[0]));
    const changedProblem = {
      ...base,
      heartbeat: { status: "Ready" as const, problems: ["self" as const] },
    };
    const changedDependency = {
      ...base,
      dependencies: [{ ...dependency, dependency: "queue" }],
    };
    const changedTarget = {
      ...base,
      dependencies: [{ ...dependency, target: "another" }],
    };
    const changedStatus = {
      ...base,
      dependencies: [{ ...dependency, status: "Degraded" as const }],
    };
    const changedEndpoints = {
      ...base,
      dependencies: [{ ...dependency, endpoints: ["another"] }],
    };
    const delimiterCollision = {
      ...base,
      dependencies: [{ ...dependency, endpoints: ["a\u0000b"] }],
    };
    const splitDelimiterCollision = {
      ...base,
      dependencies: [{ ...dependency, endpoints: ["a", "b"] }],
    };
    const missingCandidate = { ...base, dependencies: [] };

    expect(sameRuntimeSourceReportingSnapshot(base, changedProblem)).toBe(false);
    expect(sameRuntimeSourceReportingSnapshot(base, changedDependency)).toBe(false);
    expect(sameRuntimeSourceReportingSnapshot(base, changedTarget)).toBe(false);
    expect(sameRuntimeSourceReportingSnapshot(base, changedStatus)).toBe(false);
    expect(sameRuntimeSourceReportingSnapshot(base, changedEndpoints)).toBe(false);
    expect(sameRuntimeSourceReportingSnapshot(base, missingCandidate)).toBe(false);
    expect(sameRuntimeSourceReportingSnapshot(delimiterCollision, splitDelimiterCollision)).toBe(
      false,
    );
  });
});
