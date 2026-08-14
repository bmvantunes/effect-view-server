import type {
  SourceDependencyFailureTarget,
  SourceDependencyIssue,
  SourceDependencyTarget,
  SourceFailureClassification,
  SourceProblem,
  SourceStatus,
  SourceTermination,
} from "@effect-view-server/source-adapter";
import { Result } from "effect";

export type RuntimeHeartbeatStatus = SourceStatus<unknown, unknown>["_tag"];

export type RuntimeDependencyStatus = RuntimeHeartbeatStatus | "Inactive";

export type RuntimeHeartbeat = {
  readonly status: RuntimeHeartbeatStatus;
  readonly problems: ReadonlyArray<SourceProblem>;
};

export type RuntimeDependency = {
  readonly dependency: string;
  readonly target: string;
  readonly endpoints: ReadonlyArray<string>;
  readonly status: RuntimeDependencyStatus;
  readonly issues: ReadonlyArray<RuntimeDependencyIssue>;
};

export type RuntimeDependencyIssue = {
  readonly source: string;
  readonly code: string;
  readonly message: string;
  readonly attributes: ReadonlyArray<{
    readonly name: string;
    readonly value: string;
  }>;
};

export type RuntimeSourceReportingSnapshot = {
  readonly heartbeat: RuntimeHeartbeat;
  readonly dependencies: ReadonlyArray<RuntimeDependency>;
};

export type RuntimeSourceReportingDefinition<Failure = unknown> = {
  readonly source: string;
  readonly dependency: string;
  readonly lifecycle: "materialized" | "leased";
  readonly targets: ReadonlyArray<SourceDependencyTarget>;
  readonly classifyFailure: (failure: Failure) => SourceFailureClassification;
};

export type RuntimeSourceReportingState<Failure = unknown> = {
  readonly definition: RuntimeSourceReportingDefinition<Failure>;
  readonly dependencyStatuses: Map<string, RuntimeHeartbeatStatus>;
  readonly dependencyIssues: Map<string, RuntimeDependencyIssue>;
  dependencyBaselineStatus: Extract<RuntimeHeartbeatStatus, "Ready" | "Starting" | "Stopping">;
  status: SourceStatus<Failure, unknown>;
};

type StatusEvidence = {
  readonly problems: ReadonlyArray<SourceProblem>;
  readonly dependencyTargets: ReadonlySet<string>;
  readonly dependencyIssues: ReadonlyMap<string, RuntimeDependencyIssue>;
};

type FailureClassificationOutcome =
  | {
      readonly _tag: "Valid";
      readonly classification: SourceFailureClassification;
    }
  | {
      readonly _tag: "Invalid";
      readonly issue: SourceDependencyIssue;
    };

type CapturedDependencyClassification = {
  readonly problem: "dependency";
  targets?: ReadonlyArray<SourceDependencyFailureTarget>;
  issue?: SourceDependencyIssue;
};

const isNonNullObject = (value: unknown): value is object =>
  typeof value === "object" && value !== null;

const emptyEvidence: StatusEvidence = {
  problems: [],
  dependencyTargets: new Set(),
  dependencyIssues: new Map(),
};

const dependencyTargetKey = (dependency: string, target: string): string =>
  JSON.stringify([dependency, target]);

const targetDependency = <Failure>(
  definition: RuntimeSourceReportingDefinition<Failure>,
  target: SourceDependencyTarget,
): string => target.dependency ?? definition.dependency;

const definitionTargetKeys = <Failure>(
  definition: RuntimeSourceReportingDefinition<Failure>,
): ReadonlyArray<string> =>
  definition.targets.map((target) =>
    dependencyTargetKey(targetDependency(definition, target), target.target),
  );

const captureClassificationTargets = (targets: unknown): SourceFailureClassification["targets"] => {
  if (targets === undefined) {
    return undefined;
  }
  if (!Array.isArray(targets)) {
    throw new TypeError("Source failure classification has invalid dependency targets.");
  }
  const captured: Array<string | { readonly dependency: string; readonly target: string }> = [];
  for (const target of targets) {
    if (typeof target === "string" && target.length > 0) {
      captured.push(target);
      continue;
    }
    if (typeof target !== "object" || target === null || Array.isArray(target)) {
      throw new TypeError("Source failure classification has invalid dependency targets.");
    }
    const dependency = Reflect.get(target, "dependency");
    const targetName = Reflect.get(target, "target");
    if (
      typeof dependency !== "string" ||
      dependency.length === 0 ||
      typeof targetName !== "string" ||
      targetName.length === 0
    ) {
      throw new TypeError("Source failure classification has invalid dependency targets.");
    }
    captured.push(Object.freeze({ dependency, target: targetName }));
  }
  return Object.freeze(captured);
};

const captureClassificationIssue = (
  issue: unknown,
): Extract<SourceFailureClassification, { readonly problem: "dependency" }>["issue"] => {
  if (issue === undefined) {
    return undefined;
  }
  if (typeof issue !== "object" || issue === null || Array.isArray(issue)) {
    throw new TypeError("Source failure classification has an invalid dependency issue.");
  }
  const code = Reflect.get(issue, "code");
  const message = Reflect.get(issue, "message");
  const attributes = Reflect.get(issue, "attributes");
  if (
    typeof code !== "string" ||
    code.length === 0 ||
    typeof message !== "string" ||
    !Array.isArray(attributes)
  ) {
    throw new TypeError("Source failure classification has an invalid dependency issue.");
  }
  const capturedAttributes: Array<{ readonly name: string; readonly value: string }> = [];
  for (const attribute of attributes) {
    if (typeof attribute !== "object" || attribute === null || Array.isArray(attribute)) {
      throw new TypeError("Source failure classification has invalid dependency attributes.");
    }
    const name = Reflect.get(attribute, "name");
    const value = Reflect.get(attribute, "value");
    if (typeof name !== "string" || name.length === 0 || typeof value !== "string") {
      throw new TypeError("Source failure classification has invalid dependency attributes.");
    }
    capturedAttributes.push(Object.freeze({ name, value }));
  }
  return Object.freeze({ code, message, attributes: Object.freeze(capturedAttributes) });
};

const classification = <Failure>(
  classifyFailure: RuntimeSourceReportingDefinition<Failure>["classifyFailure"],
  failure: Failure,
): FailureClassificationOutcome =>
  Result.match(
    Result.try((): SourceFailureClassification => {
      const candidate: unknown = classifyFailure(failure);
      if (!isNonNullObject(candidate) || Array.isArray(candidate)) {
        throw new TypeError("Source failure classification must be an object.");
      }
      const problem = Reflect.get(candidate, "problem");
      if (problem === "self") {
        if (Object.hasOwn(candidate, "targets") || Object.hasOwn(candidate, "issue")) {
          throw new TypeError("Self failure classification cannot contain dependency fields.");
        }
        return { problem: "self" };
      }
      if (problem !== "dependency") {
        throw new TypeError("Source failure classification has an invalid problem.");
      }
      const targets = captureClassificationTargets(Reflect.get(candidate, "targets"));
      const issue = captureClassificationIssue(Reflect.get(candidate, "issue"));
      const captured: CapturedDependencyClassification = { problem: "dependency" };
      if (targets !== undefined) {
        captured.targets = targets;
      }
      if (issue !== undefined) {
        captured.issue = issue;
      }
      return captured;
    }),
    {
      onFailure: (failure) => ({
        _tag: "Invalid",
        issue: Object.freeze({
          code: "InvalidSourceFailureClassification",
          message: "The Source Adapter returned an invalid failure classification.",
          attributes: Object.freeze([
            Object.freeze({
              name: "reason",
              value:
                failure instanceof Error
                  ? failure.message
                  : "The classifier failed with a non-Error value.",
            }),
          ]),
        }),
      }),
      onSuccess: (value) => ({ _tag: "Valid", classification: value }),
    },
  );

const classifiedDependencyTargets = <Failure>(
  classified: {
    readonly problem: "dependency";
    readonly targets?: ReadonlyArray<
      string | { readonly dependency: string; readonly target: string }
    >;
  },
  definition: RuntimeSourceReportingDefinition<Failure>,
): ReadonlySet<string> => {
  const allTargets = definitionTargetKeys(definition);
  if (classified.targets === undefined || classified.targets.length === 0) {
    return new Set(allTargets);
  }
  const selected = classified.targets.map((target) =>
    typeof target === "string"
      ? dependencyTargetKey(definition.dependency, target)
      : dependencyTargetKey(target.dependency, target.target),
  );
  const knownTargets = new Set(allTargets);
  return selected.every((target) => knownTargets.has(target))
    ? new Set(selected)
    : new Set(allTargets);
};

const dependencyIssuesForTargets = <Failure>(
  issue: SourceDependencyIssue | undefined,
  targets: ReadonlySet<string>,
  definition: RuntimeSourceReportingDefinition<Failure>,
): ReadonlyMap<string, RuntimeDependencyIssue> => {
  if (issue === undefined) {
    return new Map();
  }
  const dependencyIssue: RuntimeDependencyIssue = Object.freeze({
    source: definition.source,
    code: issue.code,
    message: issue.message,
    attributes: issue.attributes,
  });
  return new Map([...targets].map((target) => [target, dependencyIssue]));
};

const classificationEvidence = <Failure>(
  outcome: FailureClassificationOutcome,
  definition: RuntimeSourceReportingDefinition<Failure>,
): StatusEvidence => {
  if (outcome._tag === "Invalid") {
    return {
      problems: ["self"],
      dependencyTargets: new Set(),
      dependencyIssues: dependencyIssuesForTargets(
        outcome.issue,
        new Set(definitionTargetKeys(definition)),
        definition,
      ),
    };
  }
  const classified = outcome.classification;
  const dependencyTargets =
    classified.problem === "dependency"
      ? classifiedDependencyTargets(classified, definition)
      : new Set<string>();
  return {
    problems: [classified.problem],
    dependencyTargets,
    dependencyIssues:
      classified.problem === "dependency"
        ? dependencyIssuesForTargets(classified.issue, dependencyTargets, definition)
        : new Map(),
  };
};

const terminationEvidence = <Failure>(
  termination: SourceTermination<Failure>,
  definition: RuntimeSourceReportingDefinition<Failure>,
): StatusEvidence => {
  if (termination._tag === "UnexpectedCompletion") {
    return {
      problems: ["dependency"],
      dependencyTargets: new Set(definitionTargetKeys(definition)),
      dependencyIssues: new Map(),
    };
  }
  if (termination.failure._tag === "RuntimeFailure") {
    return {
      problems: ["self"],
      dependencyTargets: new Set(),
      dependencyIssues: new Map(),
    };
  }
  return classificationEvidence(
    classification(definition.classifyFailure, termination.failure.failure),
    definition,
  );
};

const statusEvidence = <Failure>(
  status: SourceStatus<Failure, unknown>,
  definition: RuntimeSourceReportingDefinition<Failure>,
): StatusEvidence => {
  if (status._tag === "Starting" || status._tag === "Ready" || status._tag === "Stopping") {
    return emptyEvidence;
  }
  if (status._tag === "WaitingToRetry") {
    return terminationEvidence(status.termination, definition);
  }
  if (status._tag === "Reacquiring") {
    return terminationEvidence(status.previousTermination, definition);
  }
  if (status._tag === "Exhausted") {
    return terminationEvidence(status.exhaustion.lastTermination, definition);
  }
  const problems = new Set<SourceProblem>();
  const dependencyTargets = new Set<string>();
  const dependencyIssues = new Map<string, RuntimeDependencyIssue>();
  for (const reason of status.reasons) {
    if (reason._tag === "AdapterMaintenanceFailure") {
      problems.add("self");
      continue;
    }
    const rejection = reason.latestRejection.failure;
    if (rejection._tag === "RuntimeFailure") {
      problems.add("self");
      continue;
    }
    const evidence = classificationEvidence(
      classification(definition.classifyFailure, rejection.failure),
      definition,
    );
    for (const problem of evidence.problems) {
      problems.add(problem);
    }
    for (const target of evidence.dependencyTargets) {
      dependencyTargets.add(target);
    }
    for (const [target, dependencyIssue] of evidence.dependencyIssues) {
      dependencyIssues.set(target, dependencyIssue);
    }
  }
  return {
    problems: orderedProblems(problems),
    dependencyTargets,
    dependencyIssues,
  };
};

const orderedProblems = (problems: ReadonlySet<SourceProblem>): ReadonlyArray<SourceProblem> =>
  Object.freeze([
    ...(problems.has("self") ? (["self"] as const) : []),
    ...(problems.has("dependency") ? (["dependency"] as const) : []),
  ]);

const updateDependencyStatuses = <Failure>(
  state: RuntimeSourceReportingState<Failure>,
  status: SourceStatus<Failure, unknown>,
): boolean => {
  let changed = false;
  if (status._tag === "Starting" || status._tag === "Ready" || status._tag === "Stopping") {
    state.dependencyBaselineStatus = status._tag;
    for (const target of definitionTargetKeys(state.definition)) {
      const statusChanged = state.dependencyStatuses.get(target) !== status._tag;
      const issueDeleted = state.dependencyIssues.delete(target);
      changed = changed || statusChanged || issueDeleted;
      state.dependencyStatuses.set(target, status._tag);
    }
    return changed;
  }
  const evidence = statusEvidence(status, state.definition);
  for (const target of definitionTargetKeys(state.definition)) {
    const nextStatus = evidence.dependencyTargets.has(target)
      ? status._tag
      : state.dependencyBaselineStatus;
    changed ||= state.dependencyStatuses.get(target) !== nextStatus;
    state.dependencyStatuses.set(target, nextStatus);
    const nextIssue = evidence.dependencyIssues.get(target);
    const previousIssue = state.dependencyIssues.get(target);
    changed ||= !sameDependencyIssue(previousIssue, nextIssue);
    if (nextIssue === undefined) {
      state.dependencyIssues.delete(target);
    } else {
      state.dependencyIssues.set(target, nextIssue);
    }
  }
  return changed;
};

const dependencyIssueAttributesKey = (attributes: RuntimeDependencyIssue["attributes"]): string =>
  JSON.stringify(attributes.map((attribute) => [attribute.name, attribute.value]));

const sameDependencyIssue = (
  left: RuntimeDependencyIssue | undefined,
  right: RuntimeDependencyIssue | undefined,
): boolean => {
  if (left === right) return true;
  if (
    left === undefined ||
    right === undefined ||
    left.source !== right.source ||
    left.code !== right.code ||
    left.message !== right.message
  ) {
    return false;
  }
  return (
    dependencyIssueAttributesKey(left.attributes) === dependencyIssueAttributesKey(right.attributes)
  );
};

const compareDependencyIssues = (
  left: RuntimeDependencyIssue,
  right: RuntimeDependencyIssue,
): number => {
  const source = left.source.localeCompare(right.source);
  if (source !== 0) return source;
  const code = left.code.localeCompare(right.code);
  if (code !== 0) return code;
  const message = left.message.localeCompare(right.message);
  if (message !== 0) return message;
  return dependencyIssueAttributesKey(left.attributes).localeCompare(
    dependencyIssueAttributesKey(right.attributes),
  );
};

const sameStatusEvidence = (left: StatusEvidence, right: StatusEvidence): boolean =>
  left.problems.join("\u0000") === right.problems.join("\u0000") &&
  left.dependencyTargets.size === right.dependencyTargets.size &&
  [...left.dependencyTargets].every((target) => right.dependencyTargets.has(target)) &&
  left.dependencyIssues.size === right.dependencyIssues.size &&
  [...left.dependencyIssues].every(([target, dependencyIssue]) =>
    sameDependencyIssue(dependencyIssue, right.dependencyIssues.get(target)),
  );

export const makeRuntimeSourceReportingState = <Failure>(
  definition: RuntimeSourceReportingDefinition<Failure>,
  status: SourceStatus<NoInfer<Failure>, unknown>,
): RuntimeSourceReportingState<Failure> => {
  const state: RuntimeSourceReportingState<Failure> = {
    definition,
    dependencyStatuses: new Map(
      definitionTargetKeys(definition).map((target) => [target, "Starting"] as const),
    ),
    dependencyIssues: new Map(),
    dependencyBaselineStatus: "Starting",
    status,
  };
  updateDependencyStatuses(state, status);
  return state;
};

export const updateRuntimeSourceReportingState = <Failure>(
  state: RuntimeSourceReportingState<Failure>,
  status: SourceStatus<NoInfer<Failure>, unknown>,
): boolean => {
  if (state.status === status) {
    return false;
  }
  const projectionChanged =
    state.status._tag !== status._tag ||
    !sameStatusEvidence(
      statusEvidence(state.status, state.definition),
      statusEvidence(status, state.definition),
    );
  state.status = status;
  return updateDependencyStatuses(state, status) || projectionChanged;
};

const statusRank: Readonly<Record<RuntimeDependencyStatus, number>> = {
  Inactive: 0,
  Ready: 1,
  Degraded: 2,
  Starting: 3,
  Reacquiring: 4,
  WaitingToRetry: 5,
  Exhausted: 6,
  Stopping: 7,
};

const worseStatus = <Status extends RuntimeDependencyStatus>(
  left: Status,
  right: Status,
): Status => (statusRank[left] >= statusRank[right] ? left : right);

const heartbeatStatus = <Failure>(
  states: Iterable<RuntimeSourceReportingState<Failure>>,
): RuntimeHeartbeat => {
  let status: RuntimeHeartbeatStatus = "Ready";
  const problems = new Set<SourceProblem>();
  for (const state of states) {
    if (state.status._tag === "Stopping") {
      continue;
    }
    status = worseStatus(status, state.status._tag);
    for (const problem of statusEvidence(state.status, state.definition).problems) {
      problems.add(problem);
    }
  }
  return Object.freeze({
    status,
    problems: orderedProblems(problems),
  });
};

type MutableDependency = {
  readonly dependency: string;
  readonly target: string;
  readonly endpoints: Set<string>;
  readonly statuses: Array<RuntimeDependencyStatus>;
  readonly issues: Array<RuntimeDependencyIssue>;
  hasMaterializedDefinition: boolean;
};

export const runtimeSourceReportingSnapshot = <Failure>(
  definitions: Iterable<RuntimeSourceReportingDefinition<Failure>>,
  states: Iterable<RuntimeSourceReportingState<Failure>>,
): RuntimeSourceReportingSnapshot => {
  const capturedStates = Array.from(states);
  const dependencies = new Map<string, Map<string, MutableDependency>>();
  const dependenciesByKey = new Map<string, MutableDependency>();
  for (const definition of definitions) {
    for (const target of definition.targets) {
      const dependencyName = targetDependency(definition, target);
      const targets = dependencies.get(dependencyName);
      const current = targets?.get(target.target);
      if (current === undefined) {
        const dependency = {
          dependency: dependencyName,
          target: target.target,
          endpoints: new Set(target.endpoints),
          statuses: [],
          issues: [],
          hasMaterializedDefinition: definition.lifecycle === "materialized",
        } satisfies MutableDependency;
        dependenciesByKey.set(dependencyTargetKey(dependencyName, target.target), dependency);
        if (targets === undefined) {
          dependencies.set(dependencyName, new Map([[target.target, dependency]]));
        } else {
          targets.set(target.target, dependency);
        }
        continue;
      }
      current.hasMaterializedDefinition ||= definition.lifecycle === "materialized";
      for (const endpoint of target.endpoints) {
        current.endpoints.add(endpoint);
      }
    }
  }
  for (const state of capturedStates) {
    for (const [key, status] of state.dependencyStatuses) {
      dependenciesByKey.get(key)?.statuses.push(status);
    }
    for (const [key, dependencyIssue] of state.dependencyIssues) {
      const mutable = dependenciesByKey.get(key);
      if (
        mutable !== undefined &&
        !mutable.issues.some((candidate) => sameDependencyIssue(candidate, dependencyIssue))
      ) {
        mutable.issues.push(dependencyIssue);
      }
    }
  }
  const snapshot = Array.from(dependencies.values())
    .flatMap((targets) => Array.from(targets.values()))
    .map((dependency): RuntimeDependency => {
      const status =
        dependency.statuses.length === 0
          ? dependency.hasMaterializedDefinition
            ? "Starting"
            : "Inactive"
          : dependency.statuses.reduce<RuntimeDependencyStatus>(worseStatus, "Inactive");
      return Object.freeze({
        dependency: dependency.dependency,
        target: dependency.target,
        endpoints: Object.freeze([...dependency.endpoints]),
        status,
        issues: Object.freeze([...dependency.issues].sort(compareDependencyIssues)),
      });
    })
    .sort((left, right) =>
      left.dependency === right.dependency
        ? left.target.localeCompare(right.target)
        : left.dependency.localeCompare(right.dependency),
    );
  return Object.freeze({
    heartbeat: heartbeatStatus(capturedStates),
    dependencies: Object.freeze(snapshot),
  });
};

export const sameRuntimeDependencies = (
  left: ReadonlyArray<RuntimeDependency>,
  right: ReadonlyArray<RuntimeDependency>,
): boolean => {
  if (left.length !== right.length) {
    return false;
  }
  return left.every((dependency, index) => {
    const candidate = right[index];
    return (
      candidate !== undefined &&
      dependency.dependency === candidate.dependency &&
      dependency.target === candidate.target &&
      dependency.status === candidate.status &&
      dependency.endpoints.length === candidate.endpoints.length &&
      dependency.endpoints.every(
        (endpoint, endpointIndex) => endpoint === candidate.endpoints[endpointIndex],
      ) &&
      dependency.issues.length === candidate.issues.length &&
      dependency.issues.every((dependencyIssue, issueIndex) =>
        sameDependencyIssue(dependencyIssue, candidate.issues[issueIndex]),
      )
    );
  });
};

export const sameRuntimeSourceReportingSnapshot = (
  left: RuntimeSourceReportingSnapshot,
  right: RuntimeSourceReportingSnapshot,
): boolean =>
  left.heartbeat.status === right.heartbeat.status &&
  left.heartbeat.problems.join("\u0000") === right.heartbeat.problems.join("\u0000") &&
  sameRuntimeDependencies(left.dependencies, right.dependencies);
