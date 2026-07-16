import { makeGrpcRuntimeSourceAdapter } from "./grpc-runtime-source";
import { makeKafkaRuntimeSourceAdapter } from "./kafka-runtime-source";
import type { ViewServerRuntimeSourceAdapter } from "./runtime-source";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export const makeDefaultRuntimeSourceAdapters = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>() => [makeKafkaRuntimeSourceAdapter<Topics>(), makeGrpcRuntimeSourceAdapter<Topics>()] as const;

type RuntimeSourceAdapterError<Adapter> =
  Adapter extends ViewServerRuntimeSourceAdapter<infer _Topics, infer SourceError>
    ? SourceError
    : never;

export type ViewServerRuntimeSourceError = RuntimeSourceAdapterError<
  ReturnType<typeof makeDefaultRuntimeSourceAdapters>[number]
>;
