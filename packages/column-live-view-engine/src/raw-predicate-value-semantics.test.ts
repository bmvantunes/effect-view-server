import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, viewSchema } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import { createColumnLiveViewEngine } from "./index";
import {
  applyDelta,
  expectDeltaEvent,
  expectSnapshotEvent,
  firstEvent,
  makeEventReader,
  stateFromSnapshot,
} from "../test-harness/events";

class Profile extends Schema.Class<Profile>("Profile")({
  code: Schema.String,
  aliases: Schema.mutable(Schema.Array(Schema.String)),
}) {}
viewSchema.admitClass(Profile);

const ProfileRow = Schema.Struct({
  id: Schema.String,
  profile: Profile,
});

type ProfileRow = typeof ProfileRow.Type;

const profileViewServer = defineViewServerConfig({
  topics: {
    profileRows: {
      schema: ProfileRow,
      key: "id",
    },
  },
});

const profile = (code: string): Profile =>
  Profile.make({
    code,
    aliases: [`${code}-alias`],
  });

const profileRow = (id: string, code: string): ProfileRow => ({
  id,
  profile: profile(code),
});

const makeProfileEngine = () =>
  createColumnLiveViewEngine({
    topics: profileViewServer.topics,
  });

describe("Raw predicate field value semantics", () => {
  it.effect("matches decoded Schema.Class operands and rejects encoded objects", () =>
    Effect.gen(function* () {
      const engine = yield* makeProfileEngine();
      yield* engine.publishMany("profileRows", [
        profileRow("alpha", "alpha"),
        profileRow("beta", "beta"),
      ]);

      const literal = yield* engine.snapshot("profileRows", {
        select: ["id"],
        where: { profile: profile("alpha") },
      });
      const plainLiteral = yield* Effect.flip(
        engine.snapshot("profileRows", {
          select: ["id"],
          where: {
            profile: {
              code: "alpha",
              aliases: ["alpha-alias"],
            },
          },
        }),
      );
      const equal = yield* engine.snapshot("profileRows", {
        select: ["id"],
        where: { profile: { eq: profile("alpha") } },
      });
      const plainEqual = yield* Effect.flip(
        engine.snapshot("profileRows", {
          select: ["id"],
          where: {
            profile: {
              eq: {
                code: "alpha",
                aliases: ["alpha-alias"],
              },
            },
          },
        }),
      );
      const notEqual = yield* engine.snapshot("profileRows", {
        select: ["id"],
        where: { profile: { neq: profile("alpha") } },
      });
      const oneOf = yield* engine.snapshot("profileRows", {
        select: ["id"],
        where: { profile: { in: [profile("alpha")] } },
      });
      const plainOneOf = yield* Effect.flip(
        engine.snapshot("profileRows", {
          select: ["id"],
          where: {
            profile: {
              in: [
                {
                  code: "alpha",
                  aliases: ["alpha-alias"],
                },
              ],
            },
          },
        }),
      );
      const invalidPlainLiteral = yield* Effect.flip(
        engine.subscribeRuntime("profileRows", {
          select: ["id"],
          where: {
            profile: {
              code: 1,
              aliases: ["alpha-alias"],
            },
          },
        }),
      );

      expect({
        literal: literal.rows,
        equal: equal.rows,
        notEqual: notEqual.rows,
        oneOf: oneOf.rows,
      }).toStrictEqual({
        literal: [{ id: "alpha" }],
        equal: [{ id: "alpha" }],
        notEqual: [{ id: "beta" }],
        oneOf: [{ id: "alpha" }],
      });
      expect(plainLiteral._tag).toBe("InvalidQueryError");
      expect(plainEqual._tag).toBe("InvalidQueryError");
      expect(plainOneOf._tag).toBe("InvalidQueryError");
      expect(plainLiteral.message).toBe(
        "Raw query where field profile does not satisfy its configured schema.",
      );
      expect(plainEqual.message).toBe(
        "Raw query where field profile does not satisfy its configured schema.",
      );
      expect(plainOneOf.message).toBe(
        "Raw query where field profile does not satisfy its configured schema.",
      );
      expect(invalidPlainLiteral._tag).toBe("InvalidQueryError");
      expect(invalidPlainLiteral.message).toBe(
        "Raw query where field profile does not satisfy its configured schema.",
      );
    }),
  );

  it.effect("keeps active Schema.Class predicates correct across row changes", () =>
    Effect.gen(function* () {
      const engine = yield* makeProfileEngine();
      yield* engine.publishMany("profileRows", [
        profileRow("alpha", "alpha"),
        profileRow("beta", "beta"),
      ]);

      const subscription = yield* engine.subscribe("profileRows", {
        select: ["id"],
        where: { profile: { eq: profile("alpha") } },
      });
      const read = yield* makeEventReader(subscription);
      const initial = firstEvent(yield* read(1));
      expectSnapshotEvent(initial);
      let state = stateFromSnapshot(initial);
      expect(state.rows).toStrictEqual([{ id: "alpha" }]);

      yield* engine.publish("profileRows", profileRow("gamma", "alpha"));
      const inserted = firstEvent(yield* read(1));
      expectDeltaEvent(inserted);
      state = applyDelta(state, inserted);
      expect(state.rows).toStrictEqual([{ id: "alpha" }, { id: "gamma" }]);

      yield* engine.publish("profileRows", profileRow("alpha", "beta"));
      const removed = firstEvent(yield* read(1));
      expectDeltaEvent(removed);
      state = applyDelta(state, removed);
      expect(state.rows).toStrictEqual([{ id: "gamma" }]);

      yield* subscription.close();
    }),
  );

  it.effect("does not share active executions across distinct Schema.Class predicates", () =>
    Effect.gen(function* () {
      const engine = yield* makeProfileEngine();
      yield* engine.publishMany("profileRows", [
        profileRow("alpha", "alpha"),
        profileRow("beta", "beta"),
      ]);

      const alphaSubscription = yield* engine.subscribe("profileRows", {
        select: ["id"],
        where: { profile: { eq: profile("alpha") } },
      });
      const betaSubscription = yield* engine.subscribe("profileRows", {
        select: ["id"],
        where: { profile: { eq: profile("beta") } },
      });
      const readAlpha = yield* makeEventReader(alphaSubscription);
      const readBeta = yield* makeEventReader(betaSubscription);
      const alphaSnapshot = firstEvent(yield* readAlpha(1));
      const betaSnapshot = firstEvent(yield* readBeta(1));
      expectSnapshotEvent(alphaSnapshot);
      expectSnapshotEvent(betaSnapshot);

      expect({ alpha: alphaSnapshot.rows, beta: betaSnapshot.rows }).toStrictEqual({
        alpha: [{ id: "alpha" }],
        beta: [{ id: "beta" }],
      });

      yield* alphaSubscription.close();
      yield* betaSubscription.close();
    }),
  );

  it.effect("does not share grouped executions across distinct Schema.Class predicates", () =>
    Effect.gen(function* () {
      const engine = yield* makeProfileEngine();
      yield* engine.publishMany("profileRows", [
        profileRow("alpha", "alpha"),
        profileRow("beta", "beta"),
      ]);

      const alphaSubscription = yield* engine.subscribe("profileRows", {
        groupBy: ["id"],
        aggregates: { count: { aggFunc: "count" } },
        where: { profile: { eq: profile("alpha") } },
      });
      const betaSubscription = yield* engine.subscribe("profileRows", {
        groupBy: ["id"],
        aggregates: { count: { aggFunc: "count" } },
        where: { profile: { eq: profile("beta") } },
      });
      const readAlpha = yield* makeEventReader(alphaSubscription);
      const readBeta = yield* makeEventReader(betaSubscription);
      const alphaSnapshot = firstEvent(yield* readAlpha(1));
      const betaSnapshot = firstEvent(yield* readBeta(1));
      expectSnapshotEvent(alphaSnapshot);
      expectSnapshotEvent(betaSnapshot);

      expect({ alpha: alphaSnapshot.rows, beta: betaSnapshot.rows }).toStrictEqual({
        alpha: [{ id: "alpha", count: 1n }],
        beta: [{ id: "beta", count: 1n }],
      });

      yield* alphaSubscription.close();
      yield* betaSubscription.close();
    }),
  );
});
