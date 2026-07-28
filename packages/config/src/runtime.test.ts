import { describe, expect, it } from "@effect/vitest";
import { viewServerRuntimeDecodedMutationTrust } from "./internal";
import { runtimeConfig, runtimeEnvironmentConfig } from "./runtime";

describe("runtime configuration", () => {
  it("exposes only transport-neutral runtime configuration helpers", () => {
    expect(runtimeEnvironmentConfig.websocketPort).toBeDefined();
    expect(runtimeConfig.port("VIEW_SERVER_TEST_PORT")).toBeDefined();
    expect(Reflect.ownKeys(runtimeConfig)).toStrictEqual(["port"]);
    expect(typeof viewServerRuntimeDecodedMutationTrust).toBe("symbol");
  });
});
