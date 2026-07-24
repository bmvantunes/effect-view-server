import { Socket as unusedSocket } from "node:net";
import { privateEffect as unusedPrivateEffect } from "./private-peer-runtime/node_modules/effect/index.js";
import { SourceAdapterServer as unusedServerSdk } from "effect-view-server/source-adapter/server";
import { NodeContext as unusedPlatformLayer } from "@effect/platform-node";

void unusedSocket;
void unusedPrivateEffect;
void unusedServerSdk;
void unusedPlatformLayer;

export { adapter, leasedSource, source } from "../package-adapter/contract.js";
