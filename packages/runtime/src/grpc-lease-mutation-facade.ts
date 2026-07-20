import type { ViewServerRuntimeClient, ViewServerRuntimeError } from "@effect-view-server/config";
import { Effect } from "effect";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export type GrpcLeaseMutationFacade<Topics extends ViewServerRuntimeTopicDefinitions> = Pick<
  ViewServerRuntimeClient<Topics>,
  "publish" | "publishMany" | "patch" | "delete" | "reset"
>;

export const makeGrpcLeaseMutationFacade = <Topics extends ViewServerRuntimeTopicDefinitions>(
  client: ViewServerRuntimeClient<Topics>,
  requirePublicMutationAllowed: (
    topic: Extract<keyof Topics, string>,
  ) => Effect.Effect<void, ViewServerRuntimeError>,
  requirePublicResetAllowed: Effect.Effect<void, ViewServerRuntimeError>,
): GrpcLeaseMutationFacade<Topics> => {
  const publish: ViewServerRuntimeClient<Topics>["publish"] = (topic, row) =>
    requirePublicMutationAllowed(topic).pipe(Effect.flatMap(() => client.publish(topic, row)));
  const publishMany: ViewServerRuntimeClient<Topics>["publishMany"] = (topic, rows) =>
    requirePublicMutationAllowed(topic).pipe(Effect.flatMap(() => client.publishMany(topic, rows)));
  const patch: ViewServerRuntimeClient<Topics>["patch"] = (topic, key, patchValue) =>
    requirePublicMutationAllowed(topic).pipe(
      Effect.flatMap(() => client.patch(topic, key, patchValue)),
    );
  const deleteRow: ViewServerRuntimeClient<Topics>["delete"] = (topic, key) =>
    requirePublicMutationAllowed(topic).pipe(Effect.flatMap(() => client.delete(topic, key)));
  const reset: ViewServerRuntimeClient<Topics>["reset"] = () =>
    requirePublicResetAllowed.pipe(Effect.flatMap(() => client.reset()));

  return { publish, publishMany, patch, delete: deleteRow, reset };
};
