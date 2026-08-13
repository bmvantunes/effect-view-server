import type { ViewServerLiveClient } from "@effect-view-server/client";
import type {
  ExactLiveQueryInputForTopic,
  ExactPatch,
  GroupedQuery,
  LiveQueryResult,
  LiveQueryRow,
  RawQuery,
  TopicRouteBy,
  TopicRow,
  ViewServerHealth,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import type { ViewServerRuntimeTopicDefinitions } from "@effect-view-server/config/internal";
import type {
  GroupedIncrementalAdmissionLimits,
  RuntimeDependency,
  RuntimeHeartbeat,
} from "@effect-view-server/runtime-core";
import type { ViewServerAuth } from "@effect-view-server/server";
import type { Duration, Effect } from "effect";

export type { ViewServerRuntimeTopicDefinitions } from "@effect-view-server/config/internal";

type RuntimeHttpPath = `/${string}`;

export type ViewServerRuntimeReportingOptions = {
  readonly heartbeatInterval: Duration.Input;
  readonly dependenciesInterval: Duration.Input;
  readonly changeInterval?: Duration.Input;
  readonly onHeartbeat: (heartbeat: RuntimeHeartbeat) => Effect.Effect<void>;
  readonly onDependenciesUpdate: (
    dependencies: ReadonlyArray<RuntimeDependency>,
  ) => Effect.Effect<void>;
};

export type ViewServerRuntimeOptions<
  _Topics extends ViewServerRuntimeTopicDefinitions = ViewServerRuntimeTopicDefinitions,
> = {
  readonly host?: string;
  readonly websocketPort?: number;
  readonly tcpPublishHost?: string;
  readonly tcpPublishMaxConnections?: number;
  readonly tcpPublishPort?: number;
  readonly rpcPath?: RuntimeHttpPath;
  readonly healthPath?: RuntimeHttpPath;
  readonly metricsPath?: RuntimeHttpPath;
  readonly auth?: ViewServerAuth;
  readonly groupedIncrementalAdmissionLimits?: Partial<GroupedIncrementalAdmissionLimits>;
  readonly subscriptionQueueCapacity?: number;
  readonly reporting?: ViewServerRuntimeReportingOptions;
};

type RejectExtraKeys<Candidate, Expected> = {
  readonly [Key in Exclude<keyof Candidate, keyof Expected>]: never;
};

type RuntimeGroupedIncrementalAdmissionLimitsExactKeysConstraint<Options> = Options extends {
  readonly groupedIncrementalAdmissionLimits: infer CandidateLimits;
}
  ? {
      readonly groupedIncrementalAdmissionLimits: CandidateLimits &
        RejectExtraKeys<CandidateLimits, Partial<GroupedIncrementalAdmissionLimits>>;
    }
  : unknown;

type RuntimeReportingExactKeysConstraint<Options> = Options extends {
  readonly reporting: infer Candidate;
}
  ? {
      readonly reporting: Candidate & RejectExtraKeys<Candidate, ViewServerRuntimeReportingOptions>;
    }
  : unknown;

export type ViewServerRuntimeOptionsInput<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Options extends object = ViewServerRuntimeOptions<Topics>,
> = Options &
  ViewServerRuntimeOptions<Topics> &
  RejectExtraKeys<Options, ViewServerRuntimeOptions<Topics>> &
  RuntimeGroupedIncrementalAdmissionLimitsExactKeysConstraint<Options> &
  RuntimeReportingExactKeysConstraint<Options>;

export type ViewServerRuntimeOptionsArgs<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Options extends object = ViewServerRuntimeOptions<Topics>,
> = [options?: ViewServerRuntimeOptionsInput<Topics, Options>];

type RuntimeSourceOwnedTopic<Topics extends object> = Extract<
  {
    readonly [Topic in keyof Topics]: Topics[Topic] extends {
      readonly source: object;
    }
      ? Topic
      : never;
  }[keyof Topics],
  string
>;

type RuntimePublicMutationTopic<Topics extends object> = Extract<
  {
    readonly [Topic in keyof Topics]: Topic extends RuntimeSourceOwnedTopic<Topics>
      ? never
      : [TopicRouteBy<Topics, Topic>] extends [never]
        ? Topic
        : never;
  }[keyof Topics],
  string
>;

type RuntimePublicSnapshotTopic<Topics extends object> = Extract<
  {
    readonly [Topic in keyof Topics]: [TopicRouteBy<Topics, Topic>] extends [never] ? Topic : never;
  }[keyof Topics],
  string
>;

type RuntimePublicReset<Topics extends object> = [RuntimeSourceOwnedTopic<Topics>] extends [never]
  ? [Extract<keyof Topics, string>] extends [RuntimePublicMutationTopic<Topics>]
    ? {
        readonly reset: ViewServerRuntimeClient<Topics>["reset"];
      }
    : {
        readonly reset: (...args: never) => ReturnType<ViewServerRuntimeClient<Topics>["reset"]>;
      }
  : {
      readonly reset: (...args: never) => ReturnType<ViewServerRuntimeClient<Topics>["reset"]>;
    };

type RuntimePublicSnapshot<Topics extends object> = <
  Topic extends RuntimePublicSnapshotTopic<Topics>,
  const Query extends
    | RawQuery<TopicRow<Topics, NoInfer<Topic>>>
    | GroupedQuery<TopicRow<Topics, NoInfer<Topic>>>,
>(
  topic: Topic,
  query: ExactLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
) => Effect.Effect<
  LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
  ViewServerRuntimeError
>;

type ViewServerPublicRuntimeClient<Topics extends object> = Omit<
  ViewServerRuntimeClient<Topics>,
  "delete" | "patch" | "publish" | "publishMany" | "reset" | "snapshot"
> & {
  readonly publish: <Topic extends RuntimePublicMutationTopic<Topics>>(
    topic: Topic,
    row: TopicRow<Topics, Topic>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly publishMany: <Topic extends RuntimePublicMutationTopic<Topics>>(
    topic: Topic,
    rows: ReadonlyArray<TopicRow<Topics, Topic>>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly patch: <Topic extends RuntimePublicMutationTopic<Topics>, const Patch>(
    topic: Topic,
    key: string,
    patch: Patch & Partial<TopicRow<Topics, Topic>> & ExactPatch<TopicRow<Topics, Topic>, Patch>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly delete: <Topic extends RuntimePublicMutationTopic<Topics>>(
    topic: Topic,
    key: string,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly snapshot: RuntimePublicSnapshot<Topics>;
} & RuntimePublicReset<Topics>;

export type ViewServerRuntime<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly url: string;
  readonly healthUrl: string;
  readonly metricsUrl: string;
  readonly tcpPublishUrl?: string;
  readonly client: ViewServerPublicRuntimeClient<Topics>;
  readonly liveClient: ViewServerLiveClient<Topics>;
  readonly health: () => Effect.Effect<ViewServerHealth<Topics>, ViewServerRuntimeError>;
  readonly close: Effect.Effect<void>;
};
