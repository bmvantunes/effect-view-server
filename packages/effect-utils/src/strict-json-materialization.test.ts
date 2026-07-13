import { describe, expect, it } from "@effect/vitest";
import { Option, Result, type Schema } from "effect";
import {
  materializeStrictJson,
  type StrictJsonMaterializationError,
  type StrictJsonMaterializationReason,
} from "./index";

const successValue = (
  result: Result.Result<Schema.Json, StrictJsonMaterializationError>,
): Schema.Json => Option.getOrThrow(Result.getSuccess(result));

const expectFailure = (
  value: unknown,
  expected: {
    readonly path: string;
    readonly reason: StrictJsonMaterializationReason;
    readonly message: string;
  },
): void => {
  const error = Option.getOrThrow(Result.getFailure(materializeStrictJson(value)));

  expect({
    _tag: error._tag,
    path: error.path,
    reason: error.reason,
    message: error.message,
  }).toStrictEqual({
    _tag: "StrictJsonMaterializationError",
    ...expected,
  });
};

describe("materializeStrictJson", () => {
  it("materializes JSON primitives and normalizes negative zero", () => {
    expect(successValue(materializeStrictJson(null))).toBe(null);
    expect(successValue(materializeStrictJson("value"))).toBe("value");
    expect(successValue(materializeStrictJson(true))).toBe(true);
    expect(successValue(materializeStrictJson(42))).toBe(42);

    const zero = successValue(materializeStrictJson(-0));
    expect(zero).toBe(0);
    expect(Object.is(zero, -0)).toBe(false);
  });

  it("recursively materializes fresh dense arrays and plain records", () => {
    const nested = { enabled: true };
    const input = { rows: [nested, { enabled: false }] };
    const output = successValue(materializeStrictJson(input));

    expect(output).toStrictEqual({ rows: [{ enabled: true }, { enabled: false }] });
    expect(output).not.toBe(input);

    const outputRows = Object.getOwnPropertyDescriptor(output, "rows")?.value;
    expect(outputRows).not.toBe(input.rows);
    expect(Object.getOwnPropertyDescriptor(outputRows, "0")?.value).not.toBe(nested);
  });

  it("accepts null-prototype records and returns fresh plain data", () => {
    const input = Object.setPrototypeOf({ value: { nested: true } }, null);
    const output = successValue(materializeStrictJson(input));

    expect(output).toStrictEqual({ value: { nested: true } });
    expect(output).not.toBe(input);
    expect(Object.getPrototypeOf(output)).toBe(Object.prototype);
  });

  it("preserves an own __proto__ data key without changing the output prototype", () => {
    const input = { safe: true };
    Object.defineProperty(input, "__proto__", {
      configurable: true,
      enumerable: true,
      value: { polluted: false },
      writable: true,
    });

    const output = successValue(materializeStrictJson(input));

    expect(Object.getPrototypeOf(output)).toBe(Object.prototype);
    expect(Object.hasOwn(Object(output), "__proto__")).toBe(true);
    expect(Object.getOwnPropertyDescriptor(output, "__proto__")?.value).toStrictEqual({
      polluted: false,
    });
  });

  it("allows shared acyclic aliases while materializing each branch independently", () => {
    const shared = { value: 1 };
    const output = successValue(materializeStrictJson({ left: shared, right: shared }));
    const left = Object.getOwnPropertyDescriptor(output, "left")?.value;
    const right = Object.getOwnPropertyDescriptor(output, "right")?.value;

    expect(left).toStrictEqual({ value: 1 });
    expect(right).toStrictEqual({ value: 1 });
    expect(left).not.toBe(shared);
    expect(right).not.toBe(shared);
    expect(left).not.toBe(right);
  });

  it.each([
    [undefined, "undefined"],
    [1n, "bigint"],
    [Symbol("value"), "symbol"],
    [() => "value", "function"],
  ])("rejects unsupported %s values", (value, valueType) => {
    expectFailure(value, {
      path: "$",
      reason: "unsupported-type",
      message: `Unsupported JSON value type "${valueType}" at $.`,
    });
  });

  it("reports the exact nested path for unsupported values", () => {
    expectFailure(
      { payload: { "unsafe key": undefined } },
      {
        path: '$.payload["unsafe key"]',
        reason: "unsupported-type",
        message: 'Unsupported JSON value type "undefined" at $.payload["unsafe key"].',
      },
    );
  });

  it.each([Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY])(
    "rejects non-finite numbers",
    (value) => {
      expectFailure(
        { value },
        {
          path: "$.value",
          reason: "non-finite-number",
          message: "Expected a finite JSON number at $.value.",
        },
      );
    },
  );

  it.each([new Map([["value", 1]]), new Set([1]), new Date(0)])(
    "rejects built-in objects with semantic prototypes",
    (value) => {
      expectFailure(value, {
        path: "$",
        reason: "unsupported-prototype",
        message: "Expected a plain data record or dense array at $.",
      });
    },
  );

  it("rejects custom class instances", () => {
    class Box {
      readonly value = 1;
    }

    expectFailure(
      { payload: new Box() },
      {
        path: "$.payload",
        reason: "unsupported-prototype",
        message: "Expected a plain data record or dense array at $.payload.",
      },
    );
  });

  it("rejects arrays with a replaced prototype", () => {
    const value = Object.setPrototypeOf([1, 2], null);

    expectFailure(value, {
      path: "$",
      reason: "unsupported-prototype",
      message: "Expected a plain data record or dense array at $.",
    });
  });

  it("rejects active record cycles but not completed aliases", () => {
    type RecursiveRecord = { self?: RecursiveRecord };
    const value: RecursiveRecord = {};
    value.self = value;

    expectFailure(value, {
      path: "$.self",
      reason: "cyclic-reference",
      message: "Cyclic reference detected at $.self.",
    });
  });

  it("rejects active array cycles", () => {
    const value: Array<unknown> = [];
    value.push(value);

    expectFailure(value, {
      path: "$[0]",
      reason: "cyclic-reference",
      message: "Cyclic reference detected at $[0].",
    });
  });

  it("rejects sparse arrays", () => {
    const value: Array<unknown> = [];
    value.length = 3;
    value[1] = "present";

    expectFailure(
      { payload: value },
      {
        path: "$.payload[0]",
        reason: "sparse-array",
        message: "Sparse arrays are not valid JSON data at $.payload[0].",
      },
    );
  });

  it.each(["extra", "-1", "4294967295", "01"])("rejects the extra array property %s", (key) => {
    const value = [1, 2];
    Object.defineProperty(value, key, {
      configurable: true,
      enumerable: true,
      value: 2,
    });

    const path = key === "extra" ? "$.extra" : `$["${key}"]`;
    expectFailure(value, {
      path,
      reason: "extra-array-property",
      message: `Unexpected array property at ${path}.`,
    });
  });

  it("reports the first trailing sparse-array index", () => {
    const value = ["first", "second"];
    value.length = 3;

    expectFailure(value, {
      path: "$[2]",
      reason: "sparse-array",
      message: "Sparse arrays are not valid JSON data at $[2].",
    });
  });

  it("sanitizes array length reflection failures", () => {
    const value = new Proxy([1], {
      get: (target, key, receiver) => {
        if (key === "length") {
          throw new Error("native length detail");
        }
        return Reflect.get(target, key, receiver);
      },
    });

    expectFailure(value, {
      path: "$.length",
      reason: "reflection-failure",
      message: "Could not inspect JSON value at $.length.",
    });
  });

  it("rejects invalid reflected array lengths", () => {
    const value = new Proxy([], {
      get: (target, key, receiver) =>
        key === "length" ? "invalid" : Reflect.get(target, key, receiver),
    });

    expectFailure(value, {
      path: "$.length",
      reason: "reflection-failure",
      message: "Could not inspect JSON value at $.length.",
    });
  });

  it("sanitizes array element descriptor reflection failures", () => {
    const value = new Proxy([1], {
      getOwnPropertyDescriptor: (target, key) => {
        if (key === "0") {
          throw new Error("native element descriptor detail");
        }
        return Reflect.getOwnPropertyDescriptor(target, key);
      },
    });

    expectFailure(value, {
      path: "$[0]",
      reason: "reflection-failure",
      message: "Could not inspect JSON value at $[0].",
    });
  });

  it("rejects array properties even when they are non-enumerable", () => {
    const value = [1];
    Object.defineProperty(value, "extra", {
      configurable: true,
      enumerable: false,
      value: 2,
    });

    expectFailure(value, {
      path: "$.extra",
      reason: "extra-array-property",
      message: "Unexpected array property at $.extra.",
    });
  });

  it("rejects symbol-keyed record properties", () => {
    const key = Symbol("secret");
    const value = { safe: true };
    Object.defineProperty(value, key, {
      configurable: true,
      enumerable: true,
      value: "hidden",
    });

    expectFailure(value, {
      path: "$[Symbol(secret)]",
      reason: "symbol-key",
      message: "Symbol-keyed properties are not valid JSON data at $[Symbol(secret)].",
    });
  });

  it("rejects symbol-keyed array properties", () => {
    const key = Symbol();
    const value = [1];
    Object.defineProperty(value, key, {
      configurable: true,
      enumerable: true,
      value: "hidden",
    });

    expectFailure(value, {
      path: "$[Symbol()]",
      reason: "symbol-key",
      message: "Symbol-keyed properties are not valid JSON data at $[Symbol()].",
    });
  });

  it("rejects non-enumerable record properties", () => {
    const value = { visible: true };
    Object.defineProperty(value, "hidden", {
      configurable: true,
      enumerable: false,
      value: 1,
    });

    expectFailure(value, {
      path: "$.hidden",
      reason: "non-enumerable-property",
      message: "Expected an enumerable data property at $.hidden.",
    });
  });

  it("rejects non-enumerable array elements", () => {
    const value = [1];
    Object.defineProperty(value, "0", {
      configurable: true,
      enumerable: false,
      value: 1,
      writable: true,
    });

    expectFailure(value, {
      path: "$[0]",
      reason: "non-enumerable-property",
      message: "Expected an enumerable data property at $[0].",
    });
  });

  it("rejects accessors without invoking them", () => {
    const value = { safe: true };
    Object.defineProperty(value, "danger", {
      configurable: true,
      enumerable: true,
      get: () => {
        throw new Error("getter must not run");
      },
    });

    expectFailure(value, {
      path: "$.danger",
      reason: "accessor-property",
      message: "Accessor properties are not valid JSON data at $.danger.",
    });
  });

  it("rejects array accessors without invoking them", () => {
    const value = [1];
    Object.defineProperty(value, "0", {
      configurable: true,
      enumerable: true,
      get: () => {
        throw new Error("getter must not run");
      },
    });

    expectFailure(value, {
      path: "$[0]",
      reason: "accessor-property",
      message: "Accessor properties are not valid JSON data at $[0].",
    });
  });

  it("sanitizes revoked proxy failures", () => {
    const revoked = Proxy.revocable({}, {});
    revoked.revoke();

    expectFailure(
      { payload: revoked.proxy },
      {
        path: "$.payload",
        reason: "reflection-failure",
        message: "Could not inspect JSON value at $.payload.",
      },
    );
  });

  it("sanitizes getPrototypeOf reflection failures", () => {
    const value = new Proxy(
      {},
      {
        getPrototypeOf: () => {
          throw new Error("native prototype detail");
        },
      },
    );

    expectFailure(value, {
      path: "$",
      reason: "reflection-failure",
      message: "Could not inspect JSON value at $.",
    });
  });

  it("sanitizes ownKeys reflection failures", () => {
    const value = new Proxy(
      {},
      {
        ownKeys: () => {
          throw new Error("native ownKeys detail");
        },
      },
    );

    expectFailure(value, {
      path: "$",
      reason: "reflection-failure",
      message: "Could not inspect JSON value at $.",
    });
  });

  it("sanitizes property descriptor reflection failures", () => {
    const value = new Proxy(
      { field: 1 },
      {
        getOwnPropertyDescriptor: () => {
          throw new Error("native descriptor detail");
        },
      },
    );

    expectFailure(value, {
      path: "$.field",
      reason: "reflection-failure",
      message: "Could not inspect JSON value at $.field.",
    });
  });

  it("rejects unstable reflection when an own key has no descriptor", () => {
    const value = new Proxy(
      {},
      {
        ownKeys: () => ["ghost"],
      },
    );

    expectFailure(value, {
      path: "$.ghost",
      reason: "reflection-failure",
      message: "Could not inspect JSON value at $.ghost.",
    });
  });
});
