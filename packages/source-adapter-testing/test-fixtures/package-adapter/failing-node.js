import { Config, Effect, Layer } from "effect";

const validate = (options) => {
  if (
    typeof options !== "object" ||
    options === null ||
    Object.keys(options).length !== 1 ||
    !Array.isArray(options.resources) ||
    options.resources.length !== 1 ||
    options.resources[0] !== "client"
  ) {
    throw new TypeError("Expected exactly one logical client resource.");
  }
};

export const layer = (_viewServer, options) => {
  validate(options);
  return Layer.effectDiscard(Effect.fail("layer acquisition failed"));
};

export const layerConfig = (viewServer, options) =>
  Layer.unwrap(Config.unwrap(options).pipe(Effect.map((resolved) => layer(viewServer, resolved))));
