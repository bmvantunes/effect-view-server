import { describe, expect, it } from "@effect/vitest";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { BigDecimal, Schema } from "effect";
import { admitViewServerLiveQuery } from "./query-admission";

const Order = Schema.Struct({
  id: ViewServerId,
  status: Schema.Literals(["open", "closed"]),
  quantity: Schema.BigInt,
  amount: Schema.BigDecimal,
});

const config = defineViewServerConfig({
  topics: {
    orders: { schema: Order },
  },
});

describe("query admission", () => {
  it("owns native exact numeric ranges through the topic codec", () => {
    const amount = BigDecimal.fromStringUnsafe("1.000000000000000000000000000001");
    const query = {
      select: ["id"],
      where: [
        {
          field: "quantity",
          type: "inRange",
          filter: 90071992547409931234567891n,
          filterTo: 90071992547409931234567892n,
        },
        {
          field: "amount",
          type: "inRange",
          filter: amount,
          filterTo: BigDecimal.fromStringUnsafe("1.000000000000000000000000000002"),
        },
      ],
    };
    const admitted = admitViewServerLiveQuery(config, "orders", query);
    query.where.length = 0;
    Reflect.set(amount, "value", 0n);
    expect(admitted).toStrictEqual({
      select: ["id"],
      where: [
        {
          field: "quantity",
          type: "inRange",
          filter: 90071992547409931234567891n,
          filterTo: 90071992547409931234567892n,
        },
        {
          field: "amount",
          type: "inRange",
          filter: BigDecimal.fromStringUnsafe("1.000000000000000000000000000001"),
          filterTo: BigDecimal.fromStringUnsafe("1.000000000000000000000000000002"),
        },
      ],
    });
  });
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
    expect(() =>
      admitViewServerLiveQuery(config, "orders", {
        select: ["id"],
        where: [{ field: "quantity", type: "equals", filter: "90071992547409931234567891" }],
      }),
    ).toThrow("quantity");
    expect(() =>
      admitViewServerLiveQuery(config, "orders", {
        select: ["id"],
        where: [{ field: "amount", type: "equals", filter: "1.000000000000000000000000000001" }],
      }),
    ).toThrow("amount");
  });
});
