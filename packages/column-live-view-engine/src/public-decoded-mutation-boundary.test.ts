import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, viewSchema } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import { InvalidRowError } from "./index";
import { createColumnLiveViewEngineInternal } from "./internal";

const TransformedRow = Schema.Struct({
  id: Schema.String,
  amount: Schema.BigIntFromString,
  uriPart: Schema.StringFromUriComponent,
});

class RootClassTransformedRow extends Schema.Class<RootClassTransformedRow>(
  "RootClassTransformedRow",
)({
  id: Schema.String,
  amount: Schema.BigIntFromString,
}) {}
viewSchema.admitClass(RootClassTransformedRow);

const transformedViewServer = defineViewServerConfig({
  topics: {
    rows: {
      schema: TransformedRow,
      key: "id",
    },
  },
});

const rootClassViewServer = defineViewServerConfig({
  topics: {
    rows: {
      schema: RootClassTransformedRow,
      key: "id",
    },
  },
});

describe("public decoded mutation boundary", () => {
  it.effect(
    "accepts decoded values across public row, batch, patch, and storage-key mutations",
    () =>
      Effect.gen(function* () {
        const engine = yield* createColumnLiveViewEngineInternal({
          topics: transformedViewServer.topics,
        });

        yield* engine.publish("rows", { id: "published", amount: 1n, uriPart: "%25" });
        yield* engine.publishMany("rows", [{ id: "batch", amount: 2n, uriPart: "batch" }]);
        yield* engine.patch("rows", "published", { amount: 3n });
        yield* engine.publishManyWithStorageKeys("rows", [
          {
            storageKey: "storage-key",
            row: { id: "external", amount: 4n, uriPart: "external" },
          },
        ]);

        const snapshot = yield* engine.snapshot("rows", {
          select: ["id", "amount", "uriPart"],
          orderBy: [{ field: "id", direction: "asc" }],
          limit: 10,
        });

        expect(snapshot).toStrictEqual({
          rows: [
            { id: "batch", amount: 2n, uriPart: "batch" },
            { id: "external", amount: 4n, uriPart: "external" },
            { id: "published", amount: 3n, uriPart: "%25" },
          ],
          status: "ready",
          statusCode: "Ready",
          totalRows: 3,
          version: 4,
        });
        yield* engine.close();
      }),
  );

  it.effect("rejects encoded values across every decoded-typed public mutation", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({
        topics: transformedViewServer.topics,
      });
      yield* engine.publish("rows", { id: "stable", amount: 1n, uriPart: "stable" });

      const publishError = yield* Effect.flip(
        // @ts-expect-error public mutations accept the decoded bigint, not its wire encoding
        engine.publish("rows", { id: "published", amount: "10", uriPart: "published" }),
      );
      const batchError = yield* Effect.flip(
        engine.publishMany("rows", [
          // @ts-expect-error public mutations accept the decoded bigint, not its wire encoding
          { id: "batch", amount: "20", uriPart: "batch" },
        ]),
      );
      const patchError = yield* Effect.flip(
        // @ts-expect-error public patches accept the decoded bigint, not its wire encoding
        engine.patch("rows", "stable", { amount: "30" }),
      );
      const storageKeyError = yield* Effect.flip(
        engine.publishManyWithStorageKeys("rows", [
          {
            storageKey: "storage-key",
            // @ts-expect-error storage-key mutations accept the decoded bigint, not its wire encoding
            row: { id: "external", amount: "40", uriPart: "external" },
          },
        ]),
      );
      const snapshot = yield* engine.snapshot("rows", {
        select: ["id", "amount", "uriPart"],
        limit: 10,
      });

      expect(publishError).toBeInstanceOf(InvalidRowError);
      expect(batchError).toBeInstanceOf(InvalidRowError);
      expect(patchError).toBeInstanceOf(InvalidRowError);
      expect(storageKeyError).toBeInstanceOf(InvalidRowError);
      expect({ publishError, batchError, patchError, storageKeyError, snapshot }).toStrictEqual({
        publishError: InvalidRowError.make({
          topic: "rows",
          message: 'SchemaError(Expected bigint, got "10"\n  at ["amount"])',
        }),
        batchError: InvalidRowError.make({
          topic: "rows",
          message: 'SchemaError(Expected bigint, got "20"\n  at ["amount"])',
        }),
        patchError: InvalidRowError.make({
          topic: "rows",
          message: 'SchemaError(Expected bigint, got "30"\n  at ["amount"])',
        }),
        storageKeyError: InvalidRowError.make({
          topic: "rows",
          message: 'SchemaError(Expected bigint, got "40"\n  at ["amount"])',
        }),
        snapshot: {
          rows: [{ id: "stable", amount: 1n, uriPart: "stable" }],
          status: "ready",
          statusCode: "Ready",
          totalRows: 1,
          version: 1,
        },
      });
      yield* engine.close();
    }),
  );

  it.effect("accepts plain decoded fields for admitted root Class topic schemas", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({
        topics: rootClassViewServer.topics,
      });

      yield* engine.publish("rows", { id: "public", amount: 1n });
      yield* engine.publishManyWithStorageKeys("rows", [
        {
          storageKey: "external-storage-key",
          row: { id: "external", amount: 2n },
        },
      ]);
      const snapshot = yield* engine.snapshot("rows", {
        select: ["id", "amount"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(snapshot).toStrictEqual({
        rows: [
          { id: "external", amount: 2n },
          { id: "public", amount: 1n },
        ],
        status: "ready",
        statusCode: "Ready",
        totalRows: 2,
        version: 2,
      });
      yield* engine.close();
    }),
  );

  it.effect("rejects a stateful row accessor without reading it", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({
        topics: rootClassViewServer.topics,
      });
      let accessorReads = 0;
      const row = {
        id: "stateful",
        get amount() {
          accessorReads += 1;
          return accessorReads === 1 ? 1n : 2n;
        },
      };

      const error = yield* Effect.flip(engine.publish("rows", row));
      const snapshot = yield* engine.snapshot("rows", {
        select: ["id", "amount"],
        limit: 10,
      });

      expect(accessorReads).toBe(0);
      expect(error).toStrictEqual(
        InvalidRowError.make({
          topic: "rows",
          message: "DecodedRowSnapshotError: Decoded row field must be a data property: amount.",
        }),
      );
      expect(snapshot).toStrictEqual({
        rows: [],
        status: "ready",
        statusCode: "Ready",
        totalRows: 0,
        version: 0,
      });
      yield* engine.close();
    }),
  );
});
