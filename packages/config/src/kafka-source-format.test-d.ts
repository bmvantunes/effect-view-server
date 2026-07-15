import { describe, expectTypeOf, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { kafka } from "./index";

import { ordersValueSchema } from "../test-harness/protobuf";
import type { OrdersValueMessage } from "../test-harness/protobuf";

const JsonValue = Schema.Struct({ payload: Schema.String });

describe("Kafka source format public types", () => {
  it("infers every source format through the same source declaration", () => {
    kafka.source({
      topic: "bytes-source",
      regions: ["usa"],
      value: kafka.bytes(),
      rowKey: ({ key }) => key,
      map: ({ value }) => {
        expectTypeOf(value).toEqualTypeOf<Uint8Array>();
        return { byteLength: value.byteLength };
      },
    });

    kafka.source({
      topic: "string-source",
      regions: ["usa"],
      value: kafka.string(),
      rowKey: ({ key }) => key,
      map: ({ value }) => {
        expectTypeOf(value).toEqualTypeOf<string>();
        return { value };
      },
    });

    kafka.source({
      topic: "json-source",
      regions: ["usa"],
      value: kafka.json(() => Schema.toCodecJson(JsonValue)),
      rowKey: ({ key }) => key,
      map: ({ value }) => {
        expectTypeOf(value).toEqualTypeOf<typeof JsonValue.Type>();
        return value;
      },
    });

    kafka.source({
      topic: "protobuf-source",
      regions: ["usa"],
      value: kafka.protobuf(ordersValueSchema),
      rowKey: ({ key }) => key,
      map: ({ value }) => {
        expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
        return { customerId: value.customerId };
      },
    });

    kafka.source({
      topic: "custom-source",
      regions: ["usa"],
      value: kafka.codec({
        name: "custom",
        decode: (): Effect.Effect<{ readonly value: string }, never> =>
          Effect.succeed({ value: "custom" }),
      }),
      rowKey: ({ key }) => key,
      map: ({ value }) => {
        expectTypeOf(value).toEqualTypeOf<{ readonly value: string }>();
        return value;
      },
    });
  });

  it("rejects format-specific options on the wrong public factory", () => {
    // @ts-expect-error bytes codecs do not accept source-format options.
    kafka.bytes({ encoding: "utf8" });

    // @ts-expect-error string codecs do not accept source-format options.
    kafka.string({ descriptor: ordersValueSchema });

    // @ts-expect-error protobuf codecs accept one generated message descriptor only.
    kafka.protobuf(ordersValueSchema, { framing: "delimited" });

    kafka.codec({
      name: "custom",
      decode: () => Effect.succeed("value"),
      // @ts-expect-error custom codecs cannot declare protobuf options.
      descriptor: ordersValueSchema,
    });
  });
});
