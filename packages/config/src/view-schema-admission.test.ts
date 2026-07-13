import { describe, expect, it } from "@effect/vitest";
import { Schema } from "effect";
import { StructuredProfile } from "../test-harness/schemas";
import {
  defineViewServerConfig,
  viewSchema,
  viewServerUnsupportedRuntimeFieldDomain,
} from "./index";

describe("viewSchema admission", () => {
  it("admits only declarations created or explicitly admitted through viewSchema", () => {
    class RawProfile extends Schema.Class<RawProfile>("RawProfile")({
      code: Schema.String,
    }) {}

    const registeredOption = viewSchema.Option(Schema.String);
    const registeredChunk = viewSchema.Chunk(Schema.String);
    const registeredHashMap = viewSchema.HashMap(Schema.String, Schema.BigInt);
    const registeredNumberBigIntMap = viewSchema.HashMap(Schema.Number, Schema.BigInt);
    const registeredHashSet = viewSchema.HashSet(Schema.String);
    const registeredNested = viewSchema.Option(
      viewSchema.HashMap(Schema.Number, viewSchema.Chunk(Schema.BigInt)),
    );
    const rebuiltOption = registeredOption.annotate({ title: "RebuiltOption" });
    const rebuiltClass = StructuredProfile.annotate({ title: "RebuiltStructuredProfile" });
    const forgedOption = Schema.declare((input): input is string => typeof input === "string", {
      typeConstructor: { _tag: "effect/Option" },
    });

    expect(viewServerUnsupportedRuntimeFieldDomain(viewSchema.BigDecimal)).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(registeredOption)).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(registeredChunk)).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(registeredHashMap)).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(registeredNumberBigIntMap)).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(registeredHashSet)).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(registeredNested)).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(StructuredProfile)).toBe(undefined);
    expect(viewSchema.admitClass(StructuredProfile)).toBe(StructuredProfile);

    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Option(Schema.String))).toBe(
      "custom equivalence without canonical identity witness",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Chunk(Schema.String))).toBe(
      "custom equivalence without canonical identity witness",
    );
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.HashMap(Schema.String, Schema.BigInt)),
    ).toBe("custom equivalence without canonical identity witness");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.HashSet(Schema.String))).toBe(
      "custom equivalence without canonical identity witness",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(RawProfile)).toBe(
      "custom codec transformation without canonical identity witness",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(rebuiltOption)).toBe(
      "custom equivalence without canonical identity witness",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(rebuiltClass)).toBe(
      "custom codec transformation without canonical identity witness",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(forgedOption)).toBe(
      "custom codec transformation without canonical identity witness",
    );
    expect(
      viewServerUnsupportedRuntimeFieldDomain(viewSchema.Option(Schema.Option(Schema.String))),
    ).toBe("custom equivalence without canonical identity witness");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        viewSchema.HashMap(Schema.String, Schema.HashSet(Schema.String)),
      ),
    ).toBe("custom equivalence without canonical identity witness");

    expect(() =>
      defineViewServerConfig({
        topics: {
          rawClass: {
            schema: RawProfile,
            key: "code",
          },
        },
      }),
    ).toThrow(
      "View Server topic rawClass row schema uses unsupported runtime domain: custom codec transformation without canonical identity witness",
    );

    expect(() =>
      defineViewServerConfig({
        topics: {
          accepted: {
            schema: Schema.Struct({
              id: Schema.String,
              decimal: viewSchema.BigDecimal,
              option: registeredOption,
              chunk: registeredChunk,
              hashMap: registeredNumberBigIntMap,
              hashSet: registeredHashSet,
              nested: registeredNested,
              profile: StructuredProfile,
            }),
            key: "id",
          },
        },
      }),
    ).not.toThrow();
  });

  it("admits concrete classes independently of reuse and schema inspection order", () => {
    class FirstProfile extends Schema.Class<FirstProfile>("FirstProfile")({
      id: Schema.String,
    }) {}
    class SecondProfile extends Schema.Class<SecondProfile>("SecondProfile")({
      id: Schema.String,
    }) {}

    expect(viewServerUnsupportedRuntimeFieldDomain(FirstProfile)).toBe(
      "custom codec transformation without canonical identity witness",
    );
    expect(FirstProfile.ast).toBe(FirstProfile.ast);
    expect(viewSchema.admitClass(FirstProfile)).toBe(FirstProfile);
    expect(viewSchema.admitClass(FirstProfile)).toBe(FirstProfile);
    expect(viewSchema.admitClass(SecondProfile)).toBe(SecondProfile);
    expect(viewServerUnsupportedRuntimeFieldDomain(FirstProfile)).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(SecondProfile)).toBe(undefined);

    expect(() =>
      Reflect.apply(viewSchema.admitClass, undefined, [Schema.Struct({ id: Schema.String })]),
    ).toThrow("viewSchema.admitClass requires a concrete Effect Schema.Class.");
  });
});
