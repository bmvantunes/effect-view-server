import { describe, expect, it } from "@effect/vitest";
import { Schema } from "effect";
import { fromStringUnsafe } from "effect/BigDecimal";
import { rawQueryCompilerMetadata } from "./raw-query-compiler";
import {
  columnScalarEqualityKey,
  columnValue,
  createTopicColumnValues,
} from "./topic-column-vector";

const Row = Schema.Struct({
  amount: Schema.BigInt,
  payload: Schema.Unknown,
  price: Schema.BigDecimal,
});

describe("column-live-view-engine topic column vector", () => {
  it("supports lifecycle operations for generic, bigint, and BigDecimal columns", () => {
    const metadata = rawQueryCompilerMetadata(Row);
    const payload = createTopicColumnValues("payload", metadata);
    const amount = createTopicColumnValues("amount", metadata);
    const price = createTopicColumnValues("price", metadata);
    const decimal = fromStringUnsafe("12.50");

    payload.reserve(3);
    payload.set(0, { id: "first" });
    payload.set(1, { id: "second" });
    payload.copySlot(0, 1);
    payload.pop();

    amount.reserve(3);
    amount.set(0, 10n);
    amount.set(1, "not-bigint");
    amount.copySlot(1, 0);
    amount.pop();

    price.reserve(3);
    price.set(0, decimal);
    price.set(1, "not-decimal");
    price.copySlot(1, 0);
    price.pop();

    expect({
      amount: {
        key: columnScalarEqualityKey(amount, 0),
        length: amount.length,
        value: columnValue(amount, 0),
      },
      payload: {
        key: columnScalarEqualityKey(payload, 0),
        length: payload.length,
        value: columnValue(payload, 0),
      },
      price: {
        key: columnScalarEqualityKey(price, 0),
        length: price.length,
        value: columnValue(price, 0),
      },
    }).toStrictEqual({
      amount: {
        key: "bigint:10",
        length: 1,
        value: 10n,
      },
      payload: {
        key: undefined,
        length: 1,
        value: { id: "second" },
      },
      price: {
        key: "bigDecimal:12.5",
        length: 1,
        value: decimal,
      },
    });
  });
});
