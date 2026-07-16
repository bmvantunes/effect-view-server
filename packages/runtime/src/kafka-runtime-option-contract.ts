import type { RuntimeRegions, ViewServerKafkaStartFrom } from "@effect-view-server/config";

export type ViewServerKafkaRuntimeOptions<
  _Topics extends object,
  Regions extends RuntimeRegions = RuntimeRegions,
> = {
  readonly consumerGroupId: string;
  readonly startFrom?: ViewServerKafkaStartFrom;
  readonly regions?: Regions;
};
