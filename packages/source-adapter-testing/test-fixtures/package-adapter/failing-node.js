import { Effect, Layer } from "effect";

export const layer = () => Layer.effectDiscard(Effect.fail("layer acquisition failed"));

export const layerConfig = layer;
