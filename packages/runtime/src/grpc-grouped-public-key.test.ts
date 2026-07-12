import { describe, expect, it } from "@effect/vitest";
import { viewSchema } from "@effect-view-server/config";
import { HashMap, HashSet, Schema } from "effect";
import { compileGrpcGroupedPublicKey } from "./grpc-grouped-public-key";

const GroupedKeyRow = Schema.Struct({
  text: Schema.String,
  amount: Schema.Number,
  active: Schema.Boolean,
  none: Schema.Null,
  nested: Schema.Struct({ desk: Schema.String }),
  maybe: Schema.optionalKey(Schema.Union([Schema.String, Schema.Undefined])),
  maybeArray: Schema.optionalKey(Schema.Array(Schema.String)),
  opaque: Schema.Unknown,
  hashMap: viewSchema.HashMap(Schema.String, Schema.String),
  hashSet: viewSchema.HashSet(Schema.String),
});

const missingPresenceKey = JSON.stringify(["missing"]);

const presentPresenceKey = (canonicalKey: string): string =>
  JSON.stringify(["present", canonicalKey]);

const groupedPublicKey = (
  entries: ReadonlyArray<readonly [field: string, presenceKey: string]>,
): string => JSON.stringify(entries);

describe("leased gRPC grouped public keys", () => {
  it("compiles canonical field codecs once and distinguishes missing from present undefined", () => {
    const key = compileGrpcGroupedPublicKey(GroupedKeyRow, [
      "text",
      "amount",
      "active",
      "none",
      "nested",
      "maybe",
    ]);

    expect(
      key?.key({
        text: "route",
        amount: -0,
        active: false,
        none: null,
        nested: { desk: "equities" },
      }),
    ).toBe(
      groupedPublicKey([
        ["text", presentPresenceKey('"route"')],
        ["amount", presentPresenceKey("0")],
        ["active", presentPresenceKey("false")],
        ["none", presentPresenceKey("null")],
        ["nested", presentPresenceKey('{"desk":"equities"}')],
        ["maybe", missingPresenceKey],
      ]),
    );
    expect(
      key?.key({
        text: "route",
        amount: 1,
        active: true,
        none: null,
        nested: { desk: "equities" },
        maybe: undefined,
      }),
    ).toBe(
      groupedPublicKey([
        ["text", presentPresenceKey('"route"')],
        ["amount", presentPresenceKey("1")],
        ["active", presentPresenceKey("true")],
        ["none", presentPresenceKey("null")],
        ["nested", presentPresenceKey('{"desk":"equities"}')],
        ["maybe", presentPresenceKey("null")],
      ]),
    );
  });

  it('keeps a missing value distinct from a present ["missing"] value', () => {
    const key = compileGrpcGroupedPublicKey(GroupedKeyRow, ["maybeArray"]);
    const missingKey = key?.key({});
    const presentKey = key?.key({ maybeArray: ["missing"] });

    expect(missingKey).toBe(groupedPublicKey([["maybeArray", missingPresenceKey]]));
    expect(presentKey).toBe(groupedPublicKey([["maybeArray", presentPresenceKey('["missing"]')]]));
    expect(presentKey).not.toBe(missingKey);
  });

  it("rejects missing schemas and synchronous codec compilation failures", () => {
    const hostileField = new Proxy(Schema.String, {
      get(target, property, receiver) {
        if (property === "ast") {
          throw new Error("codec compilation failed");
        }
        return Reflect.get(target, property, receiver);
      },
    });
    const hostileSchema = new Proxy(Schema.Struct({ value: Schema.String }), {
      get(target, property, receiver) {
        if (property === "fields") {
          return { value: hostileField };
        }
        return Reflect.get(target, property, receiver);
      },
    });

    expect(compileGrpcGroupedPublicKey(GroupedKeyRow, ["missing"])).toBeUndefined();
    expect(compileGrpcGroupedPublicKey(hostileSchema, ["value"])).toBeUndefined();
  });

  it("returns no key for codec, strict materialization, or row reflection failures", () => {
    const stringKey = compileGrpcGroupedPublicKey(GroupedKeyRow, ["text"]);
    const opaqueKey = compileGrpcGroupedPublicKey(GroupedKeyRow, ["opaque"]);
    const hostileRow = new Proxy(
      { text: "route" },
      {
        getOwnPropertyDescriptor() {
          throw new Error("row reflection failed");
        },
      },
    );

    expect(stringKey?.key({ text: 1 })).toBeUndefined();
    expect(opaqueKey?.key({ opaque: new Map([["desk", "equities"]]) })).toBeUndefined();
    expect(stringKey?.key(hostileRow)).toBeUndefined();
  });

  it("derives one public key for equivalent collision-node HashMap and HashSet values", () => {
    const key = compileGrpcGroupedPublicKey(GroupedKeyRow, ["hashMap", "hashSet"]);
    const collisionLeft = "8ocpIaaa";
    const collisionRight = "GpcpIaaa";

    expect(
      key?.key({
        hashMap: HashMap.make([collisionLeft, "left"], [collisionRight, "right"]),
        hashSet: HashSet.make(collisionLeft, collisionRight),
      }),
    ).toBe(
      key?.key({
        hashMap: HashMap.make([collisionRight, "right"], [collisionLeft, "left"]),
        hashSet: HashSet.make(collisionRight, collisionLeft),
      }),
    );
  });
});
