import type {
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
};

export type RuntimeSourceReportingSnapshot = {
  readonly heartbeat: RuntimeHeartbeat;
  readonly dependencies: ReadonlyArray<RuntimeDependency>;
};

export type RuntimeSourceReportingDefinition = {
  readonly dependency: string;
  readonly lifecycle: "materialized" | "leased";
  readonly targets: ReadonlyArray<SourceDependencyTarget>;
  readonly classifyFailure: (failure: unknown) => SourceFailureClassification;
};

export type RuntimeSourceReportingState = {
  readonly definition: RuntimeSourceReportingDefinition;
  readonly dependencyStatuses: Map<string, RuntimeHeartbeatStatus>;
  status: SourceStatus<unknown, unknown>;
};

type StatusEvidence = {
  readonly problems: ReadonlyArray<SourceProblem>;
  readonly dependencyTargets: ReadonlySet<string>;
};

const emptyEvidence: StatusEvidence = {
  problems: [],
  dependencyTargets: new Set(),
};

const classification = (
  classifyFailure: RuntimeSourceReportingDefinition["classifyFailure"],
  failure: unknown,
): SourceFailureClassification =>
  Result.match(
    Result.try((): SourceFailureClassification => {
      const candidate: unknown = classifyFailure(failure);
      if (typeof candidate !== "object" || candidate === null || Array.isArray(candidate)) {
        throw new TypeError("Source failure classification must be an object.");
      }
      const problem = Reflect.get(candidate, "problem");
      if (problem === "self") {
        return { problem: "self" };
      }
      if (problem !== "dependency") {
        throw new TypeError("Source failure classification has an invalid problem.");
      }
      const targets: unknown = Reflect.get(candidate, "targets");
      if (targets === undefined) {
        return { problem: "dependency" };
      }
      if (!Array.isArray(targets)) {
        throw new TypeError("Source failure classification has invalid dependency targets.");
      }
      const capturedTargets: Array<string> = [];
      for (const target of targets) {
        if (typeof target !== "string" || target.length === 0) {
          throw new TypeError("Source failure classification has invalid dependency targets.");
        }
        capturedTargets.push(target);
      }
      return {
        problem: "dependency",
        targets: Object.freeze(capturedTargets),
      };
    }),
    {
      onFailure: () => ({ problem: "self" }),
      onSuccess: (value) => value,
    },
  );

const classifiedDependencyTargets = (
  classified: {
    readonly problem: "dependency";
    readonly targets?: ReadonlyArray<string>;
  },
  definition: RuntimeSourceReportingDefinition,
): ReadonlySet<string> => {
  const allTargets = definition.targets.map(({ target }) => target);
  if (classified.targets === undefined || classified.targets.length === 0) {
    return new Set(allTargets);
  }
  const knownTargets = new Set(allTargets);
  return classified.targets.every((target) => knownTargets.has(target))
    ? new Set(classified.targets)
    : new Set(allTargets);
};

const terminationEvidence = (
  termination: SourceTermination<unknown>,
  definition: RuntimeSourceReportingDefinition,
): StatusEvidence => {
  if (termination._tag === "UnexpectedCompletion") {
    return {
      problems: ["dependency"],
      dependencyTargets: new Set(definition.targets.map(({ target }) => target)),
    };
  }
  if (termination.failure._tag === "RuntimeFailure") {
    return {
      problems: ["self"],
      dependencyTargets: new Set(),
    };
  }
  const classified = classification(definition.classifyFailure, termination.failure.failure);
  return {
    problems: [classified.problem],
    dependencyTargets:
      classified.problem === "dependency"
        ? classifiedDependencyTargets(classified, definition)
        : new Set(),
  };
};

const statusEvidence = (
  status: SourceStatus<unknown, unknown>,
  definition: RuntimeSourceReportingDefinition,
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
    const classified = classification(definition.classifyFailure, rejection.failure);
    problems.add(classified.problem);
    if (classified.problem === "dependency") {
      for (const target of classifiedDependencyTargets(classified, definition)) {
        dependencyTargets.add(target);
      }
    }
  }
  return {
    problems: orderedProblems(problems),
    dependencyTargets,
  };
};

const orderedProblems = (problems: ReadonlySet<SourceProblem>): ReadonlyArray<SourceProblem> =>
  Object.freeze([
    ...(problems.has("self") ? (["self"] as const) : []),
    ...(problems.has("dependency") ? (["dependency"] as const) : []),
  ]);

const updateDependencyStatuses = (
  state: RuntimeSourceReportingState,
  status: SourceStatus<unknown, unknown>,
): boolean => {
  let changed = false;
  if (status._tag === "Starting" || status._tag === "Ready" || status._tag === "Stopping") {
    for (const { target } of state.definition.targets) {
      changed ||= state.dependencyStatuses.get(target) !== status._tag;
      state.dependencyStatuses.set(target, status._tag);
    }
    return changed;
  }
  const evidence = statusEvidence(status, state.definition);
  if (!evidence.problems.includes("dependency")) {
    return false;
  }
  for (const target of evidence.dependencyTargets) {
    changed ||= state.dependencyStatuses.get(target) !== status._tag;
    state.dependencyStatuses.set(target, status._tag);
  }
  return changed;
};

const sameStatusEvidence = (left: StatusEvidence, right: StatusEvidence): boolean =>
  left.problems.join("\u0000") === right.problems.join("\u0000") &&
  left.dependencyTargets.size === right.dependencyTargets.size &&
  [...left.dependencyTargets].every((target) => right.dependencyTargets.has(target));

export const makeRuntimeSourceReportingState = (
  definition: RuntimeSourceReportingDefinition,
  status: SourceStatus<unknown, unknown>,
): RuntimeSourceReportingState => {
  const state: RuntimeSourceReportingState = {
    definition,
    dependencyStatuses: new Map(
      definition.targets.map(({ target }) => [target, "Starting"] as const),
    ),
    status,
  };
  updateDependencyStatuses(state, status);
  return state;
};

export const updateRuntimeSourceReportingState = (
  state: RuntimeSourceReportingState,
  status: SourceStatus<unknown, unknown>,
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

const worseStatus = <Status extends RuntimeDependencyStatus>(left: Status, right: Status): Status =>
  statusRank[left] >= statusRank[right] ? left : right;

const heartbeatStatus = (states: Iterable<RuntimeSourceReportingState>): RuntimeHeartbeat => {
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
  hasMaterializedDefinition: boolean;
};

export const runtimeSourceReportingSnapshot = (
  definitions: Iterable<RuntimeSourceReportingDefinition>,
  states: Iterable<RuntimeSourceReportingState>,
): RuntimeSourceReportingSnapshot => {
  const capturedStates = Array.from(states);
  const dependencies = new Map<string, Map<string, MutableDependency>>();
  for (const definition of definitions) {
    for (const target of definition.targets) {
      const targets = dependencies.get(definition.dependency);
      const current = targets?.get(target.target);
      if (current === undefined) {
        const dependency = {
          dependency: definition.dependency,
          target: target.target,
          endpoints: new Set(target.endpoints),
          statuses: [],
          hasMaterializedDefinition: definition.lifecycle === "materialized",
        } satisfies MutableDependency;
        if (targets === undefined) {
          dependencies.set(definition.dependency, new Map([[target.target, dependency]]));
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
    for (const [target, status] of state.dependencyStatuses) {
      dependencies.get(state.definition.dependency)?.get(target)?.statuses.push(status);
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

export const sameRuntimeSourceReportingSnapshot = (
  left: RuntimeSourceReportingSnapshot,
  right: RuntimeSourceReportingSnapshot,
): boolean => {
  if (
    left.heartbeat.status !== right.heartbeat.status ||
    left.heartbeat.problems.join("\u0000") !== right.heartbeat.problems.join("\u0000") ||
    left.dependencies.length !== right.dependencies.length
  ) {
    return false;
  }
  return left.dependencies.every((dependency, index) => {
    const candidate = right.dependencies[index];
    return (
      candidate !== undefined &&
      dependency.dependency === candidate.dependency &&
      dependency.target === candidate.target &&
      dependency.status === candidate.status &&
      dependency.endpoints.length === candidate.endpoints.length &&
      dependency.endpoints.every(
        (endpoint, endpointIndex) => endpoint === candidate.endpoints[endpointIndex],
      )
    );
  });
};
