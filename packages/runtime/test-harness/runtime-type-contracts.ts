import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import type { ViewServerAuth } from "@effect-view-server/server";
import { Effect, Schema } from "effect";
import { makeViewServerRuntime, runViewServerRuntime } from "../src/index";

export const Order = Schema.Struct({
  id: ViewServerId,
  price: Schema.Number,
});

export const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
    },
  },
});

export const runtimeEffect = makeViewServerRuntime(viewServer);

export const runtimeWithGroupedAdmissionLimits = makeViewServerRuntime(viewServer, {
  groupedIncrementalAdmissionLimits: {
    maxGroups: 1,
  },
});

export const runtimeWithAuth = makeViewServerRuntime(viewServer, {
  auth: {
    validateRequest: () =>
      Effect.succeed({
        forwardedHeaders: {},
        id: null,
        systemHeaders: {},
      }),
  } satisfies ViewServerAuth,
});

export const runEffect = runViewServerRuntime(viewServer);

export declare const runtime: Effect.Success<typeof runtimeEffect>;
