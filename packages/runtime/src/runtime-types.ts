import type { ViewServerLiveClient } from "@view-server/client";
import type { GroupedIncrementalAdmissionLimits } from "@view-server/runtime-core";
import type {
  KafkaRuntimeTopicDefinition,
  RuntimeRegions,
  RowSchema,
  TopicDefinitions,
  ViewServerHealth,
  ViewServerKafkaStartFrom,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
  GrpcFeedDefinition,
  GrpcRuntimeClients,
} from "@view-server/config";
import type { Effect, Schema } from "effect";

export type ViewServerRuntimeTopicDefinitions = TopicDefinitions &
  Record<
    string,
    {
      readonly schema: RowSchema & Schema.Decoder<object>;
      readonly key: string;
    }
  >;

type RuntimeHttpPath = `/${string}`;

export type ViewServerKafkaRuntimeOptions<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Regions extends RuntimeRegions = RuntimeRegions,
> = {
  readonly consumerGroupId: string;
  readonly startFrom?: ViewServerKafkaStartFrom;
  readonly regions: Regions;
  readonly topics: Record<string, KafkaRuntimeTopicDefinition<Topics, Regions>>;
};

export type ViewServerGrpcRuntimeOptions<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Clients extends GrpcRuntimeClients = GrpcRuntimeClients,
> = {
  readonly clients: Clients;
  readonly feeds: Record<string, GrpcFeedDefinition<Topics, Clients>>;
};

export type ViewServerRuntimeOptions<
  Topics extends ViewServerRuntimeTopicDefinitions = ViewServerRuntimeTopicDefinitions,
  Regions extends RuntimeRegions = RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
> = {
  readonly host?: string;
  readonly websocketPort?: number;
  readonly rpcPath?: RuntimeHttpPath;
  readonly healthPath?: RuntimeHttpPath;
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

type RuntimeKafkaRegionConstraint<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Options,
> = Options extends {
  readonly kafka: {
    readonly regions: infer Regions extends RuntimeRegions;
    readonly topics: infer KafkaTopics extends Record<string, object>;
  };
}
  ? {
      readonly kafka: {
        readonly topics: {
          readonly [SourceTopic in keyof KafkaTopics]: KafkaTopics[SourceTopic] extends KafkaRuntimeTopicDefinition<
            Topics,
            Regions
          >
            ? KafkaTopics[SourceTopic]
            : never;
        };
      };
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

type RuntimeGrpcFeedConstraint<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Options,
> = Options extends {
  readonly grpc: {
    readonly clients: infer Clients extends GrpcRuntimeClients;
    readonly feeds: infer Feeds extends Record<string, object>;
  };
}
  ? {
      readonly grpc: {
        readonly feeds: {
          readonly [FeedName in keyof Feeds]: Feeds[FeedName] extends GrpcFeedDefinition<
            Topics,
            Clients
          >
            ? Feeds[FeedName]
            : never;
        };
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

type RuntimeRegionsOf<Options> = Options extends {
  readonly kafka: {
    readonly regions: infer Regions extends RuntimeRegions;
  };
}
  ? Regions
  : RuntimeRegions;

type RuntimeGrpcClientsOf<Options> = Options extends {
  readonly grpc: {
    readonly clients: infer Clients extends GrpcRuntimeClients;
  };
}
  ? Clients
  : GrpcRuntimeClients;

export type ViewServerRuntimeOptionsInput<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Options extends object = ViewServerRuntimeOptions<Topics>,
> = Options &
  ViewServerRuntimeOptions<Topics, RuntimeRegionsOf<Options>, RuntimeGrpcClientsOf<Options>> &
  RejectExtraKeys<
    Options,
    ViewServerRuntimeOptions<Topics, RuntimeRegionsOf<Options>, RuntimeGrpcClientsOf<Options>>
  > &
  RuntimeKafkaExactKeysConstraint<Options> &
  RuntimeKafkaRegionConstraint<Topics, Options> &
  RuntimeKafkaStartFromExactKeysConstraint<Options> &
  RuntimeGrpcExactKeysConstraint<Options> &
  RuntimeGrpcFeedConstraint<Topics, Options> &
  RuntimeGroupedIncrementalAdmissionLimitsExactKeysConstraint<Options>;

export type ViewServerRuntime<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly url: string;
  readonly healthUrl: string;
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly liveClient: ViewServerLiveClient<Topics>;
  readonly health: () => Effect.Effect<ViewServerHealth<Topics>, ViewServerRuntimeError>;
  readonly close: Effect.Effect<void>;
};
