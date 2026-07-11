import { describe, expect, it } from "@effect/vitest";
import { makeGrpcGroupedKeyTranslations } from "./grpc-grouped-key-translations";

const publicKeyFromRow = (row: object): string | undefined => {
  const customerId = Reflect.get(row, "customerId");
  return typeof customerId === "string" ? "customer:" + customerId : undefined;
};

const makeTranslations = () =>
  makeGrpcGroupedKeyTranslations({
    externalizeRow: (row: object) => row,
    publicKeyFromRow,
  });

describe("gRPC grouped-key translations", () => {
  it("translates snapshots atomically and stages delta changes until commit", () => {
    const translations = makeTranslations();
    const internalKeys = ["internal-a", "internal-b"];
    const rows = [{ customerId: "a" }, { customerId: "b" }];

    expect(translations.translateSnapshot(internalKeys, rows)).toStrictEqual([
      "customer:a",
      "customer:b",
    ]);
    expect(translations.translateSnapshot(["missing-row"], [])).toBeUndefined();
    expect(translations.translateSnapshot(["invalid"], [{ customerId: 1 }])).toBeUndefined();
    expect(
      translations.translateDelta([{ type: "move", key: "internal-b", fromIndex: 1, toIndex: 0 }]),
    ).toStrictEqual([{ type: "move", key: "customer:b", fromIndex: 1, toIndex: 0 }]);

    expect(
      translations.translateDelta([
        { type: "update", key: "internal-a", row: { customerId: "a2" }, index: 0 },
        { type: "move", key: "internal-a", fromIndex: 0, toIndex: 1 },
        { type: "remove", key: "internal-b" },
      ]),
    ).toStrictEqual([
      { type: "update", key: "customer:a2", row: { customerId: "a2" }, index: 0 },
      { type: "move", key: "customer:a2", fromIndex: 0, toIndex: 1 },
      { type: "remove", key: "customer:b" },
    ]);

    expect(
      translations.translateDelta([
        { type: "remove", key: "internal-a" },
        { type: "insert", key: "invalid", row: { customerId: 1 }, index: 0 },
      ]),
    ).toBeUndefined();
    expect(
      translations.translateDelta([{ type: "move", key: "internal-a", fromIndex: 1, toIndex: 0 }]),
    ).toStrictEqual([{ type: "move", key: "customer:a2", fromIndex: 1, toIndex: 0 }]);

    expect(
      translations.translateSnapshot(["replacement"], [{ customerId: "replacement" }]),
    ).toStrictEqual(["customer:replacement"]);
    expect(
      translations.translateDelta([{ type: "move", key: "internal-a", fromIndex: 0, toIndex: 1 }]),
    ).toBeUndefined();

    translations.clear();
    expect(
      translations.translateDelta([{ type: "move", key: "replacement", fromIndex: 0, toIndex: 1 }]),
    ).toBeUndefined();
  });
});
