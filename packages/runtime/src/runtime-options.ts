import type { ViewServerRuntimeError } from "@effect-view-server/config";
import type {
  GroupedIncrementalAdmissionLimits,
  ViewServerRuntimeCoreOptionsFor,
} from "@effect-view-server/runtime-core";
import type { ViewServerWebSocketServerOptions } from "@effect-view-server/server";
import { Effect } from "effect";
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
};

const runtimeOptionKeyRecord = {
  auth: true,
  groupedIncrementalAdmissionLimits: true,
  healthPath: true,
  host: true,
  metricsPath: true,
  rpcPath: true,
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

const runtimeOptionsError = (message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "RuntimeUnavailable",
  message,
});

const unsupportedOwnProperty = (
  value: object,
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
        const capturedGroupedLimits =
          groupedLimits === undefined
            ? undefined
            : {
                ...(groupedLimits.maxGroups === undefined
                  ? {}
                  : { maxGroups: groupedLimits.maxGroups }),
                ...(groupedLimits.maxMembers === undefined
                  ? {}
                  : { maxMembers: groupedLimits.maxMembers }),
                ...(groupedLimits.maxMembersPerGroup === undefined
                  ? {}
                  : { maxMembersPerGroup: groupedLimits.maxMembersPerGroup }),
                ...(groupedLimits.maxRetainedValueEntries === undefined
                  ? {}
                  : { maxRetainedValueEntries: groupedLimits.maxRetainedValueEntries }),
              };
        return {
          ...(options.auth === undefined ? {} : { auth: options.auth }),
          ...(capturedGroupedLimits === undefined
            ? {}
            : { groupedIncrementalAdmissionLimits: capturedGroupedLimits }),
          ...(options.healthPath === undefined ? {} : { healthPath: options.healthPath }),
          ...(options.host === undefined ? {} : { host: options.host }),
          ...(options.metricsPath === undefined ? {} : { metricsPath: options.metricsPath }),
          ...(options.rpcPath === undefined ? {} : { rpcPath: options.rpcPath }),
          ...(options.subscriptionQueueCapacity === undefined
            ? {}
            : { subscriptionQueueCapacity: options.subscriptionQueueCapacity }),
          ...(options.tcpPublishHost === undefined
            ? {}
            : { tcpPublishHost: options.tcpPublishHost }),
          ...(options.tcpPublishMaxConnections === undefined
            ? {}
            : { tcpPublishMaxConnections: options.tcpPublishMaxConnections }),
          ...(options.tcpPublishPort === undefined
            ? {}
            : { tcpPublishPort: options.tcpPublishPort }),
          ...(options.websocketPort === undefined ? {} : { websocketPort: options.websocketPort }),
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
  ...(options.auth === undefined ? {} : { auth: options.auth }),
  runtimeCoreOptions: {
    ...(options.groupedIncrementalAdmissionLimits === undefined
      ? {}
      : { groupedIncrementalAdmissionLimits: options.groupedIncrementalAdmissionLimits }),
    ...(options.subscriptionQueueCapacity === undefined
      ? {}
      : { subscriptionQueueCapacity: options.subscriptionQueueCapacity }),
  },
  serverOptions: {
    ...(options.host === undefined ? {} : { host: options.host }),
    ...(options.websocketPort === undefined ? {} : { port: options.websocketPort }),
    ...(options.rpcPath === undefined ? {} : { path: options.rpcPath }),
    ...(options.healthPath === undefined ? {} : { healthPath: options.healthPath }),
    ...(options.metricsPath === undefined ? {} : { metricsPath: options.metricsPath }),
  },
  ...(options.tcpPublishPort === undefined
    ? {}
    : {
        tcpPublishOptions: {
          ...(options.tcpPublishHost === undefined ? {} : { host: options.tcpPublishHost }),
          ...(options.tcpPublishMaxConnections === undefined
            ? {}
            : { maxConnections: options.tcpPublishMaxConnections }),
          port: options.tcpPublishPort,
        },
      }),
});
