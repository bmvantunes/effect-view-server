import { Effect, Layer } from "effect";
import { adapter } from "./contract.js";

const lifecycle = {
  acquire: () => Effect.die(new Error("Package fixture acquisition is not exercised.")),
  metrics: () => Effect.succeed({ observed: 0n }),
  retryDefault: (effect) => effect,
};

const service = {
  adapter,
  materialized: lifecycle,
  leased: lifecycle,
};

export const layer = () => Layer.succeed(adapter.runtimeService, service);
export const layerConfig = layer;
