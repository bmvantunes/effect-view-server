import { describe, expect, it } from "@effect/vitest";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { Schema } from "effect";
import { admitViewServerLiveQuery } from "./query-admission";

const Order = Schema.Struct({
  id: ViewServerId,
  status: Schema.Literals(["open", "closed"]),
});

const config = defineViewServerConfig({
  topics: {
    orders: { schema: Order },
  },
});

describe("query admission", () => {
  it("returns the owned admitted query and rejects invalid field values", () => {
    expect(
      admitViewServerLiveQuery(config, "orders", {
        select: ["id"],
        where: [{ field: "status", type: "equals", filter: "open" }],
      }),
    ).toStrictEqual({
      select: ["id"],
      where: [{ field: "status", type: "equals", filter: "open" }],
    });
    expect(() =>
      admitViewServerLiveQuery(config, "orders", {
        select: ["id"],
        where: [{ field: "status", type: "equals", filter: "missing" }],
      }),
    ).toThrow("status");
  });
});
