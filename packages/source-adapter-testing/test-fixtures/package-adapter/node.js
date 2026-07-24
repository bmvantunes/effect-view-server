import { SourceAdapterServer } from "effect-view-server/source-adapter/server";
import { Config, Effect, Layer, Schedule } from "effect";
import { adapter } from "./contract.js";

const lifecycle = {
  acquire: () => Effect.die(new Error("Package fixture acquisition is not exercised.")),
  metrics: () => Effect.succeed({ observed: 0n }),
  retry: Schedule.recurs(0),
};

const runtimeLayer = SourceAdapterServer.make(adapter, {
  materialized: lifecycle,
  leased: lifecycle,
});

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
  return options;
};

export const layer = (_viewServer, options) => {
  validate(options);
  return runtimeLayer;
};

export const layerConfig = (viewServer, options) =>
  Layer.unwrap(Config.unwrap(options).pipe(Effect.map((resolved) => layer(viewServer, resolved))));
