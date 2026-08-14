import { definedFields } from "@effect-view-server/effect-utils";
import type { ViewServerRuntimeError } from "@effect-view-server/config";
import type {
  GroupedIncrementalAdmissionLimits,
  ViewServerRuntimeCoreOptionsFor,
} from "@effect-view-server/runtime-core";
import type { ViewServerWebSocketServerOptions } from "@effect-view-server/server";
import { Duration, Effect, Option } from "effect";
import type { ViewServerRuntimeOptions, ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export type ResolvedViewServerRuntimeBaseOptions<
  Topics extends ViewServerRuntimeTopicDefinitions = ViewServerRuntimeTopicDefinitions,
> = {
  readonly auth?: ViewServerRuntimeOptions<Topics>["auth"];
  readonly runtimeCoreOptions: ViewServerRuntimeCoreOptionsFor<Topics>;
  readonly serverOptions: ViewServerWebSocketServerOptions;
  readonly tcpPublishOptions?: {
    readonly host?: string;
    readonly maxConnections?: number;
    readonly port: number;
  };
  readonly reporting?: {
    readonly heartbeatInterval: Duration.Duration;
    readonly dependenciesInterval: Duration.Duration;
    readonly changeInterval: Duration.Duration;
    readonly onHeartbeat: NonNullable<ViewServerRuntimeOptions<Topics>["reporting"]>["onHeartbeat"];
    readonly onDependenciesUpdate: NonNullable<
      ViewServerRuntimeOptions<Topics>["reporting"]
    >["onDependenciesUpdate"];
  };
};

const runtimeOptionKeyRecord = {
  auth: true,
  groupedIncrementalAdmissionLimits: true,
  healthPath: true,
  host: true,
  metricsPath: true,
  rpcPath: true,
  reporting: true,
  subscriptionQueueCapacity: true,
  tcpPublishHost: true,
  tcpPublishMaxConnections: true,
  tcpPublishPort: true,
  websocketPort: true,
} satisfies {
  readonly [Key in keyof ViewServerRuntimeOptions<ViewServerRuntimeTopicDefinitions>]-?: true;
};

const runtimeOptionKeys = new Set<PropertyKey>(Reflect.ownKeys(runtimeOptionKeyRecord));

const groupedIncrementalAdmissionLimitKeyRecord = {
  maxGroups: true,
  maxMembers: true,
  maxMembersPerGroup: true,
  maxRetainedValueEntries: true,
} satisfies { readonly [Key in keyof GroupedIncrementalAdmissionLimits]-?: true };

const groupedIncrementalAdmissionLimitKeys = new Set<PropertyKey>(
  Reflect.ownKeys(groupedIncrementalAdmissionLimitKeyRecord),
);

const reportingOptionKeyRecord = {
  changeInterval: true,
  dependenciesInterval: true,
  heartbeatInterval: true,
  onDependenciesUpdate: true,
  onHeartbeat: true,
} satisfies {
  readonly [Key in keyof NonNullable<
    ViewServerRuntimeOptions<ViewServerRuntimeTopicDefinitions>["reporting"]
  >]-?: true;
};

const reportingOptionKeys = new Set<PropertyKey>(Reflect.ownKeys(reportingOptionKeyRecord));

const positiveDuration = (value: Duration.Input): Duration.Duration | undefined =>
  Option.getOrUndefined(
    Option.filter(Duration.fromInput(value), (duration) =>
      Option.match(Duration.toNanos(duration), {
        onNone: () => false,
        onSome: (nanos) => nanos > 0n,
      }),
    ),
  );

const runtimeOptionsError = (message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "RuntimeUnavailable",
  message,
});

const unsupportedOwnProperty = <Value extends object>(
  value: Value,
  allowedKeys: ReadonlySet<PropertyKey>,
): PropertyKey | undefined => Reflect.ownKeys(value).find((key) => !allowedKeys.has(key));

export const validateViewServerRuntimeOptions = Effect.fn("ViewServerRuntime.options.validate")(
  function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
    options: ViewServerRuntimeOptions<Topics>,
  ) {
    return yield* Effect.try({
      try: () => {
        const unsupportedRuntimeOption = unsupportedOwnProperty(options, runtimeOptionKeys);
        if (unsupportedRuntimeOption !== undefined) {
          throw new TypeError(
            `View Server runtime options contain unsupported property: ${String(unsupportedRuntimeOption)}.`,
          );
        }
        const groupedLimits = options.groupedIncrementalAdmissionLimits;
        if (
          groupedLimits !== undefined &&
          (typeof groupedLimits !== "object" ||
            groupedLimits === null ||
            Array.isArray(groupedLimits))
        ) {
          throw new TypeError(
            "View Server runtime option groupedIncrementalAdmissionLimits must be an object.",
          );
        }
        if (groupedLimits !== undefined) {
          const unsupportedGroupedLimit = unsupportedOwnProperty(
            groupedLimits,
            groupedIncrementalAdmissionLimitKeys,
          );
          if (unsupportedGroupedLimit !== undefined) {
            throw new TypeError(
              `View Server runtime option groupedIncrementalAdmissionLimits contains unsupported property: ${String(unsupportedGroupedLimit)}.`,
            );
          }
          for (const key of groupedIncrementalAdmissionLimitKeys) {
            const value = Reflect.get(groupedLimits, key);
            if (
              value !== undefined &&
              (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0)
            ) {
              throw new TypeError(
                `View Server runtime option groupedIncrementalAdmissionLimits.${String(key)} must be a positive safe integer.`,
              );
            }
          }
        }
        const reporting = options.reporting;
        if (
          reporting !== undefined &&
          (typeof reporting !== "object" || reporting === null || Array.isArray(reporting))
        ) {
          throw new TypeError("View Server runtime option reporting must be an object.");
        }
        const capturedReporting =
          reporting === undefined
            ? undefined
            : (() => {
                const unsupportedReportingOption = unsupportedOwnProperty(
                  reporting,
                  reportingOptionKeys,
                );
                if (unsupportedReportingOption !== undefined) {
                  throw new TypeError(
                    `View Server runtime option reporting contains unsupported property: ${String(unsupportedReportingOption)}.`,
                  );
                }
                const onHeartbeat = reporting.onHeartbeat;
                const onDependenciesUpdate = reporting.onDependenciesUpdate;
                const heartbeatInterval = positiveDuration(reporting.heartbeatInterval);
                const dependenciesInterval = positiveDuration(reporting.dependenciesInterval);
                const changeIntervalInput = reporting.changeInterval;
                const changeInterval =
                  changeIntervalInput === undefined
                    ? undefined
                    : positiveDuration(changeIntervalInput);
                if (
                  typeof onHeartbeat !== "function" ||
                  typeof onDependenciesUpdate !== "function"
                ) {
                  throw new TypeError(
                    "View Server runtime reporting requires onHeartbeat and onDependenciesUpdate Effect callbacks.",
                  );
                }
                if (
                  heartbeatInterval === undefined ||
                  dependenciesInterval === undefined ||
                  (changeIntervalInput !== undefined && changeInterval === undefined)
                ) {
                  throw new TypeError(
                    "View Server runtime reporting intervals must be positive finite Effect Durations.",
                  );
                }
                return {
                  heartbeatInterval,
                  dependenciesInterval,
                  ...definedFields(changeInterval, (changeInterval) => ({ changeInterval })),
                  onHeartbeat,
                  onDependenciesUpdate,
                };
              })();
        const capturedGroupedLimits =
          groupedLimits === undefined
            ? undefined
            : {
                ...definedFields(groupedLimits.maxGroups, (maxGroups) => ({ maxGroups })),
                ...definedFields(groupedLimits.maxMembers, (maxMembers) => ({ maxMembers })),
                ...definedFields(groupedLimits.maxMembersPerGroup, (maxMembersPerGroup) => ({
                  maxMembersPerGroup,
                })),
                ...definedFields(
                  groupedLimits.maxRetainedValueEntries,
                  (maxRetainedValueEntries) => ({ maxRetainedValueEntries }),
                ),
              };
        return {
          ...definedFields(options.auth, (auth) => ({ auth })),
          ...definedFields(capturedGroupedLimits, (groupedIncrementalAdmissionLimits) => ({
            groupedIncrementalAdmissionLimits,
          })),
          ...definedFields(options.healthPath, (healthPath) => ({ healthPath })),
          ...definedFields(options.host, (host) => ({ host })),
          ...definedFields(options.metricsPath, (metricsPath) => ({ metricsPath })),
          ...definedFields(options.rpcPath, (rpcPath) => ({ rpcPath })),
          ...definedFields(capturedReporting, (reporting) => ({ reporting })),
          ...definedFields(options.subscriptionQueueCapacity, (subscriptionQueueCapacity) => ({
            subscriptionQueueCapacity,
          })),
          ...definedFields(options.tcpPublishHost, (tcpPublishHost) => ({ tcpPublishHost })),
          ...definedFields(options.tcpPublishMaxConnections, (tcpPublishMaxConnections) => ({
            tcpPublishMaxConnections,
          })),
          ...definedFields(options.tcpPublishPort, (tcpPublishPort) => ({ tcpPublishPort })),
          ...definedFields(options.websocketPort, (websocketPort) => ({ websocketPort })),
        } satisfies ViewServerRuntimeOptions<Topics>;
      },
      catch: (error) =>
        runtimeOptionsError(
          error instanceof Error
            ? error.message
            : "View Server runtime options could not be inspected.",
        ),
    });
  },
);

export const resolveViewServerRuntimeBaseOptions = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  options: ViewServerRuntimeOptions<Topics>,
): ResolvedViewServerRuntimeBaseOptions<Topics> => ({
  ...definedFields(options.auth, (auth) => ({ auth })),
  runtimeCoreOptions: {
    ...definedFields(
      options.groupedIncrementalAdmissionLimits,
      (groupedIncrementalAdmissionLimits) => ({ groupedIncrementalAdmissionLimits }),
    ),
    ...definedFields(options.subscriptionQueueCapacity, (subscriptionQueueCapacity) => ({
      subscriptionQueueCapacity,
    })),
  },
  serverOptions: {
    ...definedFields(options.host, (host) => ({ host })),
    ...definedFields(options.websocketPort, (port) => ({ port })),
    ...definedFields(options.rpcPath, (path) => ({ path })),
    ...definedFields(options.healthPath, (healthPath) => ({ healthPath })),
    ...definedFields(options.metricsPath, (metricsPath) => ({ metricsPath })),
  },
  ...definedFields(options.reporting, (reporting) => ({
    reporting: {
      heartbeatInterval: Option.getOrThrow(Duration.fromInput(reporting.heartbeatInterval)),
      dependenciesInterval: Option.getOrThrow(Duration.fromInput(reporting.dependenciesInterval)),
      changeInterval:
        reporting.changeInterval === undefined
          ? Duration.millis(300)
          : Option.getOrThrow(Duration.fromInput(reporting.changeInterval)),
      onHeartbeat: reporting.onHeartbeat,
      onDependenciesUpdate: reporting.onDependenciesUpdate,
    },
  })),
  ...definedFields(options.tcpPublishPort, (port) => ({
    tcpPublishOptions: {
      ...definedFields(options.tcpPublishHost, (host) => ({ host })),
      ...definedFields(options.tcpPublishMaxConnections, (maxConnections) => ({
        maxConnections,
      })),
      port,
    },
  })),
});
