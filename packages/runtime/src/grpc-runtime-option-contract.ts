import type { GrpcRuntimeClients } from "@effect-view-server/config";
import type { Duration } from "effect";

export type ViewServerGrpcRuntimeOptions<
  _Topics extends object,
  _Clients extends GrpcRuntimeClients = GrpcRuntimeClients,
> = {
  readonly materializedReconnect?: {
    readonly maxReconnects?: number;
    readonly delay?: Duration.Input;
  };
};
