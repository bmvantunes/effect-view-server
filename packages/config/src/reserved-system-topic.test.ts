import { describe, expect, it } from "@effect/vitest";
import { Schema, SchemaAST } from "effect";
import {
  defineViewServerConfig,
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  viewServerUnsupportedRuntimeFieldDomain,
} from "./index";

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

  it("reserves dots for unambiguous nested filter paths", () => {
    const dottedFieldName = "profile.country";
    const DottedRootRow = Schema.Struct({
      id: Schema.String,
      [dottedFieldName]: Schema.String,
    });
    const DottedNestedRow = Schema.Struct({
      id: Schema.String,
      profile: Schema.Struct({
        [dottedFieldName]: Schema.String,
      }),
    });

    expect(() =>
      defineViewServerConfig({
        topics: { dotted: { schema: DottedRootRow, key: "id" } },
      }),
    ).toThrow("uses a reserved row field name: profile.country");
    expect(() =>
      defineViewServerConfig({
        topics: { dotted: { schema: DottedNestedRow, key: "id" } },
      }),
    ).toThrow(
      "field profile uses unsupported runtime domain: statically named object field contains a reserved dot: profile.country",
    );
  });

  it("finds reserved dots through supported schema containers", () => {
    const dottedFieldName = "country.code";
    const DottedObject = Schema.Struct({
      profile: Schema.Struct({
        [dottedFieldName]: Schema.String,
      }),
    });
    const DottedUnion = Schema.Union([Schema.String, DottedObject]);
    class DottedClass extends Schema.Class<DottedClass>("DottedClass")({
      profile: Schema.Struct({
        [dottedFieldName]: Schema.String,
      }),
    }) {}
    const SymbolicDottedObject = {
      ast: new SchemaAST.Objects(
        [
          new SchemaAST.PropertySignature(Symbol("metadata"), Schema.String.ast),
          new SchemaAST.PropertySignature(dottedFieldName, Schema.String.ast),
        ],
        [],
      ),
    };

    expect(viewServerUnsupportedRuntimeFieldDomain(DottedObject)).toBe(
      "statically named object field contains a reserved dot: profile.country.code",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(SymbolicDottedObject)).toBe(
      "statically named object field contains a reserved dot: country.code",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(DottedUnion)).toBe(
      "statically named object field contains a reserved dot: profile.country.code",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(DottedClass)).toBe(
      "statically named object field contains a reserved dot: profile.country.code",
    );
  });
});
