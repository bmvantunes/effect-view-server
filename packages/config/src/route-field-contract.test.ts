import { describe, expect, it } from "@effect/vitest";
import { Schema } from "effect";
import { viewServerRouteFieldSchemaHasCompleteScalarDomain } from "./route-field-contract";

type RecursiveRoute = string | { readonly next: RecursiveRoute };
const RecursiveRoute: Schema.Codec<RecursiveRoute, unknown, never, never> = Schema.suspend(() =>
  Schema.Union([Schema.String, Schema.Struct({ next: RecursiveRoute })]),
);
const SelfSuspendedRoute: Schema.Codec<never, unknown, never, never> = Schema.suspend(
  () => SelfSuspendedRoute,
);

describe("leased route field schema contract", () => {
  it("accepts complete defined scalar domains", () => {
    const supported = [
      Schema.String,
      Schema.TemplateLiteral(["route-", Schema.String]),
      Schema.Number,
      Schema.NumberFromString,
      Schema.BigInt,
      Schema.BigDecimal,
      Schema.Boolean,
      Schema.Null,
      Schema.Literal("emea"),
      Schema.Literal(1),
      Schema.Literal(1n),
      Schema.Literal(true),
      Schema.Enum({ Europe: "emea", America: "amer" }),
      Schema.Enum({ Low: 1, High: 2 }),
      Schema.Union([Schema.String, Schema.Number, Schema.Null]),
      Schema.Union([Schema.String, Schema.Undefined]),
      Schema.Union([Schema.String, Schema.Never]),
      Schema.suspend(() => Schema.String),
    ];

    expect(supported.map(viewServerRouteFieldSchemaHasCompleteScalarDomain)).toStrictEqual(
      supported.map(() => true),
    );
  });

  it("rejects empty, unknown, structured, mixed, and malformed domains", () => {
    // @ts-expect-error Runtime admission also protects JavaScript callers from undefined literals.
    const UndefinedLiteral = Schema.Literal(undefined);
    const HostileSchema = new Proxy(Schema.String, {
      get(target, property, receiver) {
        if (property === "ast") {
          throw new Error("route schema AST failed");
        }
        return Reflect.get(target, property, receiver);
      },
    });
    const unsupported = [
      Schema.Any,
      Schema.Unknown,
      Schema.ObjectKeyword,
      Schema.Symbol,
      Schema.Undefined,
      UndefinedLiteral,
      Schema.Never,
      Schema.Enum({}),
      Schema.Union([Schema.Undefined, Schema.Never]),
      Schema.Struct({ country: Schema.String }),
      Schema.Array(Schema.String),
      Schema.Union([Schema.String, Schema.Struct({ country: Schema.String })]),
      RecursiveRoute,
      SelfSuspendedRoute,
      Schema.Option(Schema.String),
      HostileSchema,
      {},
    ];

    expect(unsupported.map(viewServerRouteFieldSchemaHasCompleteScalarDomain)).toStrictEqual(
      unsupported.map(() => false),
    );
  });
});
