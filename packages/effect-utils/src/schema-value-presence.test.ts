import { describe, expect, it } from "@effect/vitest";
import {
  missingSchemaValuePresenceToken,
  presentSchemaValuePresenceToken,
  schemaValuePresenceKey,
} from "./schema-value-presence";

describe("Schema value presence", () => {
  it("frames missing and present canonical values without token collisions", () => {
    expect(schemaValuePresenceKey(missingSchemaValuePresenceToken)).toBe('["missing"]');
    expect(schemaValuePresenceKey(presentSchemaValuePresenceToken('["missing"]'))).toBe(
      '["present","[\\"missing\\"]"]',
    );
  });
});
