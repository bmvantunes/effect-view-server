import { describe, expectTypeOf, it } from "@effect/vitest";
import { Schema } from "effect";
import type * as EffectChunk from "effect/Chunk";
import type * as EffectHashMap from "effect/HashMap";
import type * as EffectHashSet from "effect/HashSet";
import * as EffectOption from "effect/Option";
import { defineViewServerConfig, viewSchema } from "./index";

const OptionNumber = viewSchema.Option(Schema.NumberFromString);
const ChunkNumber = viewSchema.Chunk(Schema.NumberFromString);
const NumberBigIntMap = viewSchema.HashMap(Schema.NumberFromString, Schema.BigIntFromString);
const StringSet = viewSchema.HashSet(Schema.String);
const Nested = viewSchema.Option(
  viewSchema.HashMap(Schema.String, viewSchema.Chunk(Schema.BigIntFromString)),
);

class Profile extends Schema.Class<Profile>("Profile")({
  id: Schema.String,
  score: Schema.NumberFromString,
  backup: viewSchema.Option(Schema.String),
}) {
  upperId(): string {
    return this.id.toUpperCase();
  }
}
const admittedProfile = viewSchema.admitClass(Profile);
const AnnotatedProfile = Profile.annotate({ title: "AnnotatedProfile" });

class SecondaryProfile extends Schema.Class<SecondaryProfile>("SecondaryProfile")(
  { id: Schema.String },
  { title: "Secondary profile" },
) {}
const admittedSecondaryProfile = viewSchema.admitClass(SecondaryProfile);

const profileConfig = defineViewServerConfig({
  topics: {
    profiles: {
      schema: Profile,
      key: "id",
    },
  },
});

describe("viewSchema public type contracts", () => {
  it("preserves declaration Type and Encoded parameters", () => {
    expectTypeOf<typeof OptionNumber.Type>().toEqualTypeOf<EffectOption.Option<number>>();
    expectTypeOf<typeof OptionNumber.Encoded>().toEqualTypeOf<EffectOption.Option<string>>();
    expectTypeOf<typeof ChunkNumber.Type>().toEqualTypeOf<EffectChunk.Chunk<number>>();
    expectTypeOf<typeof ChunkNumber.Encoded>().toEqualTypeOf<EffectChunk.Chunk<string>>();
    expectTypeOf<typeof NumberBigIntMap.Type>().toEqualTypeOf<
      EffectHashMap.HashMap<number, bigint>
    >();
    expectTypeOf<typeof NumberBigIntMap.Encoded>().toEqualTypeOf<
      EffectHashMap.HashMap<string, string>
    >();
    expectTypeOf<typeof StringSet.Type>().toEqualTypeOf<EffectHashSet.HashSet<string>>();
    expectTypeOf<typeof StringSet.Encoded>().toEqualTypeOf<EffectHashSet.HashSet<string>>();
    expectTypeOf<typeof Nested.Type>().toEqualTypeOf<
      EffectOption.Option<EffectHashMap.HashMap<string, EffectChunk.Chunk<bigint>>>
    >();
    expectTypeOf<typeof Nested.Encoded>().toEqualTypeOf<
      EffectOption.Option<EffectHashMap.HashMap<string, EffectChunk.Chunk<string>>>
    >();
  });

  it("preserves Class construction, methods, and config inference", () => {
    const made = Profile.make({
      id: "profile-1",
      score: 42,
      backup: EffectOption.none(),
    });
    const constructed = new Profile({
      id: "profile-2",
      score: 43,
      backup: EffectOption.some("profile-1"),
    });

    expectTypeOf(made).toEqualTypeOf<Profile>();
    expectTypeOf(constructed).toEqualTypeOf<Profile>();
    expectTypeOf(made.upperId()).toEqualTypeOf<string>();
    expectTypeOf(admittedProfile).toEqualTypeOf<typeof Profile>();
    expectTypeOf(admittedSecondaryProfile).toEqualTypeOf<typeof SecondaryProfile>();
    expectTypeOf<typeof Profile.Type>().toEqualTypeOf<Profile>();
    expectTypeOf<typeof Profile.Encoded>().toEqualTypeOf<{
      readonly id: string;
      readonly score: string;
      readonly backup: EffectOption.Option<string>;
    }>();
    expectTypeOf(profileConfig.topics.profiles.schema).toEqualTypeOf<typeof Profile>();
    expectTypeOf<typeof profileConfig.topics.profiles.schema.Type>().toEqualTypeOf<Profile>();
  });

  it("reports misuse without requiring consumer casts", () => {
    // @ts-expect-error admitClass requires a concrete Effect Schema.Class.
    viewSchema.admitClass(Schema.Struct({ id: Schema.String }));
    // @ts-expect-error declarations without Class fields are not concrete Schema classes.
    viewSchema.admitClass(Schema.Option(Schema.String));
    // @ts-expect-error Class annotation rebuilds do not retain the concrete Class field Interface.
    viewSchema.admitClass(AnnotatedProfile);
    // @ts-expect-error admitClass requires an Effect Schema value.
    viewSchema.admitClass({ identifier: "NotASchema" });
    // @ts-expect-error viewSchema.Option requires an Effect Schema.
    viewSchema.Option("not-a-schema");
    // @ts-expect-error viewSchema.HashMap requires key and value schemas.
    viewSchema.HashMap(Schema.String);
  });
});
