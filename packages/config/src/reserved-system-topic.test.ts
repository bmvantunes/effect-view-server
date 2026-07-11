import { describe, expect, it } from "@effect/vitest";
import { Schema } from "effect";
import { defineViewServerConfig, VIEW_SERVER_HEALTH_SUMMARY_TOPIC } from "./index";

import { Order } from "../test-harness/schemas";

describe("Reserved system topic validation", () => {
  it("rejects reserved health topic names at runtime", () => {
    const reservedTopicName: string = VIEW_SERVER_HEALTH_SUMMARY_TOPIC;
    expect(() =>
      defineViewServerConfig({
        topics: {
          [reservedTopicName]: {
            schema: Order,
            key: "id",
          },
        },
      }),
    ).toThrow("View Server topic name is reserved for system health streams");
  });

  it("rejects reserved row field names at runtime", () => {
    const reservedFieldName = "__proto__";
    const BadRow = Schema.Struct({
      id: Schema.String,
      [reservedFieldName]: Schema.String,
    });

    expect(() =>
      defineViewServerConfig({
        topics: {
          badRows: {
            schema: BadRow,
            key: "id",
          },
        },
      }),
    ).toThrow("uses a reserved row field name: __proto__");
  });
});
