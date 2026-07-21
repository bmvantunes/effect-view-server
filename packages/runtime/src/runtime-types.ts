import type { ViewServerLiveClient } from "@effect-view-server/client";
import type { GroupedIncrementalAdmissionLimits } from "@effect-view-server/runtime-core";
import type { ViewServerAuth } from "@effect-view-server/server";
import type {
  LiveQueryRow,
  LiveQueryResult,
  RawQuery,
  GroupedQuery,
  ExactLiveQueryInputForTopic,
  ExactPatch,
  RuntimeRegions,
  TopicRouteBy,
  TopicRow,
  ViewServerHealth,
  ViewServerKafkaStartFrom,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
  GrpcRuntimeClients,
} from "@effect-view-server/config";
import type {
  RuntimeKafkaSourceOwnershipConstraint,
  RuntimeKafkaSourceRegionConstraint,
  TopicOwnedKafkaSourceTopic,
  ViewServerRuntimeTopicDefinitions,
} from "@effect-view-server/config/internal";
import type { Effect } from "effect";
import type { ViewServerGrpcRuntimeOptions } from "./grpc-runtime-option-contract";
import type { ViewServerKafkaRuntimeOptions } from "./kafka-runtime-option-contract";

export type { ViewServerGrpcRuntimeOptions } from "./grpc-runtime-option-contract";
export type { ViewServerKafkaRuntimeOptions } from "./kafka-runtime-option-contract";
export type { ViewServerRuntimeTopicDefinitions } from "@effect-view-server/config/internal";

type RuntimeHttpPath = `/${string}`;

export type ViewServerRuntimeOptions<
  Topics extends ViewServerRuntimeTopicDefinitions = ViewServerRuntimeTopicDefinitions,
  Regions extends RuntimeRegions = RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
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
  readonly kafka?: ViewServerKafkaRuntimeOptions<Topics, Regions>;
  readonly grpc?: ViewServerGrpcRuntimeOptions<Topics, GrpcClients>;
  readonly groupedIncrementalAdmissionLimits?: Partial<GroupedIncrementalAdmissionLimits>;
  readonly subscriptionQueueCapacity?: number;
};

type RejectExtraKeys<Candidate, Shape> = {
  readonly [Key in Exclude<keyof Candidate, keyof Shape>]: never;
};

type RuntimeKafkaExactKeysConstraint<Options> = Options extends {
  readonly kafka: infer CandidateKafka;
}
  ? {
      readonly kafka: CandidateKafka &
        RejectExtraKeys<
          CandidateKafka,
          ViewServerKafkaRuntimeOptions<ViewServerRuntimeTopicDefinitions>
        >;
    }
  : unknown;

type RuntimeKafkaStartFromExactKeysConstraint<Options> = Options extends {
  readonly kafka: {
    readonly startFrom: infer CandidateStartFrom;
  };
}
  ? CandidateStartFrom extends object
    ? {
        readonly kafka: {
          readonly startFrom: CandidateStartFrom &
            RejectExtraKeys<CandidateStartFrom, Extract<ViewServerKafkaStartFrom, object>>;
        };
      }
    : unknown
  : unknown;

type RuntimeGrpcExactKeysConstraint<Options> = Options extends {
  readonly grpc: infer CandidateGrpc;
}
  ? {
      readonly grpc: CandidateGrpc &
        RejectExtraKeys<
          CandidateGrpc,
          ViewServerGrpcRuntimeOptions<ViewServerRuntimeTopicDefinitions>
        >;
    }
  : unknown;

type RuntimeGrpcMaterializedReconnectExactKeysConstraint<Options> = Options extends {
  readonly grpc: {
    readonly materializedReconnect: infer CandidateReconnect;
  };
}
  ? {
      readonly grpc: {
        readonly materializedReconnect: CandidateReconnect &
          RejectExtraKeys<
            CandidateReconnect,
            NonNullable<
              ViewServerGrpcRuntimeOptions<ViewServerRuntimeTopicDefinitions>["materializedReconnect"]
            >
          >;
      };
    }
  : unknown;

type RuntimeGroupedIncrementalAdmissionLimitsExactKeysConstraint<Options> = Options extends {
  readonly groupedIncrementalAdmissionLimits: infer CandidateLimits;
}
  ? {
      readonly groupedIncrementalAdmissionLimits: CandidateLimits &
        RejectExtraKeys<CandidateLimits, Partial<GroupedIncrementalAdmissionLimits>>;
    }
  : unknown;

type TopicOwnedGrpcSourceTopic<Topics extends object> = Extract<
  {
    readonly [Topic in keyof Topics]: Topics[Topic] extends {
      readonly grpcSource: object;
    }
      ? Topic
      : never;
  }[keyof Topics],
  string
>;

type TopicOwnedSourceTopic<Topics extends object> =
  | TopicOwnedKafkaSourceTopic<Topics>
  | TopicOwnedGrpcSourceTopic<Topics>;

type RuntimeSourceOwnedTopic<Topics extends object, _Options> = Extract<
  TopicOwnedSourceTopic<Topics>,
  Extract<keyof Topics, string>
>;

type RuntimeGrpcSourceOwnershipConstraint<Topics extends object, _Options> = [
  TopicOwnedGrpcSourceTopic<Topics>,
] extends [never]
  ? unknown
  : unknown;

type RuntimeRegionsOf<Options, ConfigRegions extends RuntimeRegions> = Options extends {
  readonly kafka: {
    readonly regions: infer Regions extends RuntimeRegions;
  };
}
  ? Regions
  : ConfigRegions;

type RuntimeKafkaSourceFreeInputConstraint<Topics extends object> = [
  TopicOwnedKafkaSourceTopic<Topics>,
] extends [never]
  ? {
      readonly kafka?: never;
    }
  : unknown;

export type ViewServerRuntimeOptionsInput<
  Topics extends ViewServerRuntimeTopicDefinitions,
  ConfigRegions extends RuntimeRegions = RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
  Options extends object = ViewServerRuntimeOptions<Topics, ConfigRegions, GrpcClients>,
> = Options &
  ViewServerRuntimeOptions<Topics, RuntimeRegionsOf<Options, ConfigRegions>, GrpcClients> &
  RejectExtraKeys<
    Options,
    ViewServerRuntimeOptions<Topics, RuntimeRegionsOf<Options, ConfigRegions>, GrpcClients>
  > &
  RuntimeKafkaExactKeysConstraint<Options> &
  RuntimeKafkaStartFromExactKeysConstraint<Options> &
  RuntimeGrpcExactKeysConstraint<Options> &
  RuntimeGrpcMaterializedReconnectExactKeysConstraint<Options> &
  RuntimeGroupedIncrementalAdmissionLimitsExactKeysConstraint<Options> &
  RuntimeKafkaSourceFreeInputConstraint<Topics> &
  RuntimeKafkaSourceOwnershipConstraint<Topics, Options> &
  RuntimeKafkaSourceRegionConstraint<Topics, ConfigRegions, Options> &
  RuntimeGrpcSourceOwnershipConstraint<Topics, Options>;

export type ViewServerRuntimeOptionsArgs<
  Topics extends ViewServerRuntimeTopicDefinitions,
  ConfigRegions extends RuntimeRegions = RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
  Options extends object = ViewServerRuntimeOptions<Topics, ConfigRegions, GrpcClients>,
> = [TopicOwnedKafkaSourceTopic<Topics>] extends [never]
  ? [options?: ViewServerRuntimeOptionsInput<Topics, ConfigRegions, GrpcClients, Options>]
  : [options: ViewServerRuntimeOptionsInput<Topics, ConfigRegions, GrpcClients, Options>];

type RuntimePublicMutationTopic<Topics extends object, SourceOwnedTopics extends string> = Extract<
  {
    readonly [Topic in keyof Topics]: Topic extends SourceOwnedTopics
      ? never
      : Topics[Topic] extends { readonly kafkaSource: object }
        ? never
        : Topics[Topic] extends { readonly grpcSource: object }
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

type RuntimeLeasedTopic<Topics extends object> = Extract<
  {
    readonly [Topic in keyof Topics]: [TopicRouteBy<Topics, Topic>] extends [never] ? never : Topic;
  }[keyof Topics],
  string
>;

type RuntimeSourceOwnedOrLeasedTopic<Topics extends object, SourceOwnedTopics extends string> =
  | SourceOwnedTopics
  | RuntimeLeasedTopic<Topics>;

type RuntimePublicReset<Topics extends object, SourceOwnedTopics extends string> = [
  RuntimeSourceOwnedOrLeasedTopic<Topics, SourceOwnedTopics>,
] extends [never]
  ? [Extract<keyof Topics, string>] extends [RuntimePublicMutationTopic<Topics, SourceOwnedTopics>]
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

type ViewServerPublicRuntimeClient<Topics extends object, SourceOwnedTopics extends string> = Omit<
  ViewServerRuntimeClient<Topics>,
  "delete" | "patch" | "publish" | "publishMany" | "reset" | "snapshot"
> & {
  readonly publish: <Topic extends RuntimePublicMutationTopic<Topics, SourceOwnedTopics>>(
    topic: Topic,
    row: TopicRow<Topics, Topic>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly publishMany: <Topic extends RuntimePublicMutationTopic<Topics, SourceOwnedTopics>>(
    topic: Topic,
    rows: ReadonlyArray<TopicRow<Topics, Topic>>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly patch: <
    Topic extends RuntimePublicMutationTopic<Topics, SourceOwnedTopics>,
    const Patch,
  >(
    topic: Topic,
    key: string,
    patch: Patch & Partial<TopicRow<Topics, Topic>> & ExactPatch<TopicRow<Topics, Topic>, Patch>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly delete: <Topic extends RuntimePublicMutationTopic<Topics, SourceOwnedTopics>>(
    topic: Topic,
    key: string,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly snapshot: RuntimePublicSnapshot<Topics>;
} & RuntimePublicReset<Topics, SourceOwnedTopics>;

export type ViewServerRuntime<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Options extends object = object,
> = {
  readonly url: string;
  readonly healthUrl: string;
  readonly metricsUrl: string;
  readonly tcpPublishUrl?: string;
  readonly client: ViewServerPublicRuntimeClient<Topics, RuntimeSourceOwnedTopic<Topics, Options>>;
  readonly liveClient: ViewServerLiveClient<Topics>;
  readonly health: () => Effect.Effect<ViewServerHealth<Topics>, ViewServerRuntimeError>;
  readonly close: Effect.Effect<void>;
};
