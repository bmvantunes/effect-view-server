import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import {
  compileViewServerGroupedRowContract,
  decodeGroupedRow,
  decodeProjectedRow,
  encodeGroupedRow,
  encodeProjectedRow,
} from "./protocol-row-codec";

import { viewServer } from "../test-harness/protocol";

const invalidRow = (message: string) => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidRow",
  message,
  topic: "orders",
});

const selectedId = new Set(["id"]);

const groupedQuery = {
  groupBy: ["id"],
  aggregates: {
    rowCount: { aggFunc: "count" },
  },
} as const;

const groupedContract = compileViewServerGroupedRowContract(groupedQuery);

describe("Protocol row reflection boundaries", () => {
  it.effect(
    "rejects non-enumerable and accessor raw encode fields without invoking accessors",
    () =>
      Effect.gen(function* () {
        const nonEnumerableRow = {};
        Object.defineProperty(nonEnumerableRow, "id", {
          enumerable: false,
          value: "a",
        });

        const nonEnumerableError = yield* Effect.flip(
          encodeProjectedRow(viewServer, "orders", selectedId, nonEnumerableRow),
        );
        expect(nonEnumerableError).toStrictEqual(
          invalidRow("Row field for topic orders must be enumerable: id"),
        );

        const accessorRow = {};
        Object.defineProperty(accessorRow, "id", {
          enumerable: true,
          get: () => {
            throw new Error("The row getter must not be invoked.");
          },
        });

        const accessorError = yield* Effect.flip(
          encodeProjectedRow(viewServer, "orders", selectedId, accessorRow),
        );
        expect(accessorError).toStrictEqual(
          invalidRow("Row field for topic orders must be a data property: id"),
        );
      }),
  );

  it.effect("rejects raw encode symbol fields and hostile reflection", () =>
    Effect.gen(function* () {
      const symbolRow = { id: "a" };
      Object.defineProperty(symbolRow, Symbol("secret"), {
        enumerable: true,
        value: "hidden",
      });

      const symbolError = yield* Effect.flip(
        encodeProjectedRow(viewServer, "orders", selectedId, symbolRow),
      );
      expect(symbolError).toStrictEqual(
        invalidRow("Unexpected row symbol field for topic orders: Symbol(secret)"),
      );

      const ownKeysFailure = new Proxy(
        { id: "a" },
        {
          ownKeys: () => {
            throw new Error("ownKeys failed");
          },
        },
      );
      const ownKeysError = yield* Effect.flip(
        encodeProjectedRow(viewServer, "orders", selectedId, ownKeysFailure),
      );
      expect(ownKeysError).toStrictEqual(invalidRow("Could not inspect row for topic orders"));

      const descriptorFailure = new Proxy(
        {},
        {
          getOwnPropertyDescriptor: () => {
            throw new Error("descriptor failed");
          },
          ownKeys: () => ["id"],
        },
      );
      const descriptorError = yield* Effect.flip(
        encodeProjectedRow(viewServer, "orders", selectedId, descriptorFailure),
      );
      expect(descriptorError).toStrictEqual(
        invalidRow("Could not inspect row field for topic orders: id"),
      );

      const missingDescriptor = new Proxy(
        {},
        {
          getOwnPropertyDescriptor: () => undefined,
          ownKeys: () => ["id"],
        },
      );
      const missingDescriptorError = yield* Effect.flip(
        encodeProjectedRow(viewServer, "orders", selectedId, missingDescriptor),
      );
      expect(missingDescriptorError).toStrictEqual(
        invalidRow("Could not inspect row field for topic orders: id"),
      );
    }),
  );

  it.effect("rejects non-data grouped encode fields and symbol extras", () =>
    Effect.gen(function* () {
      const nonEnumerableRow = { rowCount: 1n };
      Object.defineProperty(nonEnumerableRow, "id", {
        enumerable: false,
        value: "a",
      });
      const nonEnumerableError = yield* Effect.flip(
        encodeGroupedRow(viewServer, "orders", groupedContract, nonEnumerableRow),
      );
      expect(nonEnumerableError).toStrictEqual(
        invalidRow("Grouped row field for topic orders must be enumerable: id"),
      );

      const accessorRow = { id: "a" };
      Object.defineProperty(accessorRow, "rowCount", {
        enumerable: true,
        get: () => {
          throw new Error("The aggregate getter must not be invoked.");
        },
      });
      const accessorError = yield* Effect.flip(
        encodeGroupedRow(viewServer, "orders", groupedContract, accessorRow),
      );
      expect(accessorError).toStrictEqual(
        invalidRow("Grouped row field for topic orders must be a data property: rowCount"),
      );

      const symbolRow = { id: "a", rowCount: 1n };
      Object.defineProperty(symbolRow, Symbol("secret"), {
        enumerable: true,
        value: "hidden",
      });
      const symbolError = yield* Effect.flip(
        encodeGroupedRow(viewServer, "orders", groupedContract, symbolRow),
      );
      expect(symbolError).toStrictEqual(
        invalidRow("Unexpected grouped row symbol field for topic orders: Symbol(secret)"),
      );
    }),
  );

  it.effect("strictly materializes raw and grouped wire rows before decoding fields", () =>
    Effect.gen(function* () {
      const nonEnumerableWireRow = {};
      Object.defineProperty(nonEnumerableWireRow, "id", {
        enumerable: false,
        value: "a",
      });
      const rawError = yield* Effect.flip(
        decodeProjectedRow(viewServer, "orders", selectedId, nonEnumerableWireRow),
      );
      expect(rawError).toStrictEqual(
        invalidRow("Invalid row for topic orders: Expected an enumerable data property at $.id."),
      );

      const accessorWireRow = {
        id: "a",
        get rowCount(): never {
          throw new Error("The aggregate wire getter must not be invoked.");
        },
      };
      const groupedError = yield* Effect.flip(
        decodeGroupedRow(viewServer, "orders", groupedContract, accessorWireRow),
      );
      expect(groupedError).toStrictEqual(
        invalidRow(
          "Invalid grouped row for topic orders: Accessor properties are not valid JSON data at $.rowCount.",
        ),
      );

      const symbolWireRow = { id: "a" };
      Object.defineProperty(symbolWireRow, Symbol("secret"), {
        enumerable: true,
        value: "hidden",
      });
      const symbolError = yield* Effect.flip(
        decodeProjectedRow(viewServer, "orders", selectedId, symbolWireRow),
      );
      expect(symbolError).toStrictEqual(
        invalidRow(
          "Invalid row for topic orders: Symbol-keyed properties are not valid JSON data at $[Symbol(secret)].",
        ),
      );

      const hostileWireRow = new Proxy(
        { id: "a", rowCount: { _viewServerAggregate: "bigint", value: "1" } },
        {
          ownKeys: () => {
            throw new Error("wire ownKeys failed");
          },
        },
      );
      const reflectionError = yield* Effect.flip(
        decodeGroupedRow(viewServer, "orders", groupedContract, hostileWireRow),
      );
      expect(reflectionError).toStrictEqual(
        invalidRow("Invalid grouped row for topic orders: Could not inspect JSON value at $."),
      );

      const nonObjectError = yield* Effect.flip(
        decodeProjectedRow(
          viewServer,
          "orders",
          selectedId,
          // @ts-expect-error hostile callers can bypass the public wire-row object type.
          ["a"],
        ),
      );
      expect(nonObjectError).toStrictEqual(
        invalidRow("Invalid row for topic orders: Expected a JSON object."),
      );
    }),
  );
});
