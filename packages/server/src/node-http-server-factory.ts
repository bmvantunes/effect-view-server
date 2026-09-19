import { Context } from "effect";
import * as Http from "node:http";

export type NodeHttpServerFactory = () => Http.Server;

export const NodeHttpServerFactory = Context.Reference<NodeHttpServerFactory>(
  "@effect-view-server/server/NodeHttpServerFactory",
  {
    defaultValue: () => Http.createServer,
  },
);
