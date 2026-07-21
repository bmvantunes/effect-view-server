import { describe, expect, it } from "@effect/vitest";
import {
  hasPlainRecordPrototype,
  inspectArrayData,
  inspectDenseArrayData,
  inspectPlainRecordData,
  inspectPlainRecordShape,
} from "./structural-data";

describe("structural data inspection", () => {
  it("recognizes only records with the built-in object prototype", () => {
    const hostilePrototype = new Proxy(
      {},
      {
        getPrototypeOf: () => {
          throw new Error("prototype reflection failed");
        },
      },
    );

    expect(hasPlainRecordPrototype({ value: true })).toBe(true);
    expect(hasPlainRecordPrototype("not-a-record")).toBe(false);
    expect(hasPlainRecordPrototype(null)).toBe(false);
    expect(hasPlainRecordPrototype([])).toBe(false);
    expect(hasPlainRecordPrototype(Object.create(null))).toBe(false);
    expect(hasPlainRecordPrototype(hostilePrototype)).toBe(false);
  });

  it("snapshots plain data records without invoking hostile properties", () => {
    const record = { value: true };
    const symbolic = { value: true };
    Object.defineProperty(symbolic, Symbol("metadata"), { enumerable: true, value: true });
    const accessor = {};
    Object.defineProperty(accessor, "value", {
      enumerable: true,
      get: () => {
        throw new Error("accessor must not run");
      },
    });

    expect(inspectPlainRecordData(record)).toStrictEqual({
      _tag: "Success",
      snapshot: { source: record, entries: [["value", true]] },
    });
    expect(inspectPlainRecordData(new Map())).toStrictEqual({
      _tag: "Failure",
      reason: "invalidRecord",
    });
    expect(inspectPlainRecordData(symbolic)).toStrictEqual({
      _tag: "Failure",
      reason: "invalidRecord",
    });
    expect(inspectPlainRecordData(accessor)).toStrictEqual({
      _tag: "Failure",
      reason: "invalidProperty",
    });
  });

  it("turns hostile record reflection traps into inspection failures", () => {
    const revoked = Proxy.revocable({}, {});
    revoked.revoke();
    const prototypeFailure = new Proxy(
      {},
      {
        getPrototypeOf: () => {
          throw new Error("prototype reflection failed");
        },
      },
    );
    const keysFailure = new Proxy(
      {},
      {
        ownKeys: () => {
          throw new Error("key reflection failed");
        },
      },
    );
    const descriptorFailure = new Proxy(
      { value: true },
      {
        getOwnPropertyDescriptor: () => {
          throw new Error("descriptor reflection failed");
        },
      },
    );

    for (const value of [revoked.proxy, prototypeFailure, keysFailure, descriptorFailure]) {
      expect(inspectPlainRecordData(value)).toStrictEqual({
        _tag: "Failure",
        reason: "invalidRecord",
      });
    }
  });

  it("captures record shape once and memoizes data descriptor inspection", () => {
    let prototypeReads = 0;
    let keyReads = 0;
    let descriptorReads = 0;
    const stateful = new Proxy(
      { value: "first" },
      {
        getPrototypeOf: (target) => {
          prototypeReads += 1;
          return Reflect.getPrototypeOf(target);
        },
        ownKeys: (target) => {
          keyReads += 1;
          return Reflect.ownKeys(target);
        },
        getOwnPropertyDescriptor: (target, key) => {
          descriptorReads += 1;
          const descriptor = Reflect.getOwnPropertyDescriptor(target, key);
          return descriptor === undefined
            ? undefined
            : { ...descriptor, value: descriptorReads === 1 ? "first" : "changed" };
        },
      },
    );
    const metadata = Symbol("metadata");
    const symbolic = { value: true };
    Object.defineProperty(symbolic, metadata, { enumerable: true, value: true });
    const accessor = {};
    Object.defineProperty(accessor, "value", { enumerable: true, get: () => true });
    const descriptorFailure = new Proxy(
      { value: true },
      {
        getOwnPropertyDescriptor: () => {
          throw new Error("descriptor reflection failed");
        },
      },
    );
    const keysFailure = new Proxy(
      {},
      {
        ownKeys: () => {
          throw new Error("key reflection failed");
        },
      },
    );

    const statefulInspection = inspectPlainRecordShape(stateful);
    const statefulSnapshot =
      statefulInspection._tag === "Success" ? statefulInspection.snapshot : undefined;
    const symbolicInspection = inspectPlainRecordShape(symbolic);
    const symbolicSnapshot =
      symbolicInspection._tag === "Success" ? symbolicInspection.snapshot : undefined;
    const accessorInspection = inspectPlainRecordShape(accessor);
    const accessorSnapshot =
      accessorInspection._tag === "Success" ? accessorInspection.snapshot : undefined;
    const descriptorInspection = inspectPlainRecordShape(descriptorFailure);
    const descriptorSnapshot =
      descriptorInspection._tag === "Success" ? descriptorInspection.snapshot : undefined;

    expect(statefulSnapshot?.stringKeys).toStrictEqual(["value"]);
    expect(statefulSnapshot?.symbolKeys).toStrictEqual([]);
    expect(statefulSnapshot?.inspectData("value")).toStrictEqual({
      _tag: "Data",
      value: "first",
    });
    expect(statefulSnapshot?.inspectData("value")).toStrictEqual({
      _tag: "Data",
      value: "first",
    });
    expect(statefulSnapshot?.inspectData("missing")).toStrictEqual({ _tag: "Missing" });
    expect(symbolicSnapshot?.symbolKeys).toStrictEqual([metadata]);
    expect(accessorSnapshot?.inspectData("value")).toStrictEqual({
      _tag: "InvalidProperty",
    });
    expect(descriptorSnapshot?.inspectData("value")).toStrictEqual({
      _tag: "ReflectionFailure",
    });
    expect(inspectPlainRecordShape(new Map())).toStrictEqual({
      _tag: "Failure",
      reason: "invalidRecord",
    });
    expect(inspectPlainRecordShape(keysFailure)).toStrictEqual({
      _tag: "Failure",
      reason: "invalidReflection",
    });
    expect(prototypeReads).toBe(1);
    expect(keyReads).toBe(1);
    expect(descriptorReads).toBe(2);
  });

  it("snapshots only dense built-in data arrays", () => {
    const values = ["a", "b"];
    let proxyReads = 0;
    const transparentProxy = new Proxy(values, {
      get: () => {
        proxyReads += 1;
        throw new Error("array values must be read through descriptors");
      },
    });
    const metadataSymbol = Symbol("metadata");
    const symbolic: Array<unknown> = [];
    Object.defineProperty(symbolic, metadataSymbol, { enumerable: true, value: true });
    const sparse: Array<unknown> = [];
    sparse.length = 1;
    const accessor: Array<unknown> = [];
    Object.defineProperty(accessor, "0", {
      enumerable: true,
      get: () => {
        throw new Error("accessor must not run");
      },
    });
    accessor.length = 1;
    const extra = ["a"];
    Object.defineProperty(extra, "metadata", { enumerable: true, value: true });
    const extraAccessor: Array<unknown> = [];
    Object.defineProperty(extraAccessor, "metadata", {
      enumerable: true,
      get: () => {
        throw new Error("extra accessor must not run");
      },
    });

    expect(inspectDenseArrayData(values)).toStrictEqual({ _tag: "Success", values: ["a", "b"] });
    expect(inspectDenseArrayData(transparentProxy)).toStrictEqual({
      _tag: "Success",
      values: ["a", "b"],
    });
    expect(proxyReads).toBe(0);
    expect(inspectDenseArrayData({ 0: "a", length: 1 })).toStrictEqual({
      _tag: "Failure",
      reason: "invalidArray",
    });
    expect(inspectDenseArrayData(symbolic)).toStrictEqual({
      _tag: "Failure",
      reason: "invalidExtraProperty",
      key: metadataSymbol,
    });
    expect(inspectDenseArrayData(sparse)).toStrictEqual({
      _tag: "Failure",
      reason: "invalidEntry",
    });
    expect(inspectDenseArrayData(accessor)).toStrictEqual({
      _tag: "Failure",
      reason: "invalidEntry",
    });
    expect(inspectDenseArrayData(extra)).toStrictEqual({
      _tag: "Failure",
      reason: "invalidExtraProperty",
      key: "metadata",
    });
    expect(inspectArrayData(extra)).toStrictEqual({
      _tag: "Success",
      snapshot: {
        source: extra,
        values: ["a"],
        extraEntries: [["metadata", true]],
      },
    });
    expect(inspectArrayData(extraAccessor)).toStrictEqual({
      _tag: "Failure",
      reason: "invalidExtraProperty",
      key: "metadata",
    });
  });

  it("turns hostile array reflection traps into inspection failures", () => {
    const revoked = Proxy.revocable<Array<unknown>>([], {});
    revoked.revoke();
    const prototypeFailure = new Proxy<Array<unknown>>([], {
      getPrototypeOf: () => {
        throw new Error("prototype reflection failed");
      },
    });
    const keysFailure = new Proxy<Array<unknown>>([], {
      ownKeys: () => {
        throw new Error("key reflection failed");
      },
    });
    const descriptorFailure = new Proxy<Array<unknown>>([true], {
      getOwnPropertyDescriptor: () => {
        throw new Error("descriptor reflection failed");
      },
    });

    for (const value of [revoked.proxy, prototypeFailure, keysFailure, descriptorFailure]) {
      expect(inspectDenseArrayData(value)).toStrictEqual({
        _tag: "Failure",
        reason: "invalidReflection",
      });
    }
  });

  it("rejects hostile array lengths before inspecting numeric entries", () => {
    let infiniteDescriptorReads = 0;
    const infiniteLength = new Proxy<Array<unknown>>([], {
      getOwnPropertyDescriptor: (target, key) => {
        infiniteDescriptorReads += 1;
        const descriptor = Reflect.getOwnPropertyDescriptor(target, key);
        return key === "length" && descriptor !== undefined
          ? { ...descriptor, value: Number.POSITIVE_INFINITY }
          : descriptor;
      },
    });
    let largeDescriptorReads = 0;
    const largeLength = new Proxy<Array<unknown>>([], {
      getOwnPropertyDescriptor: (target, key) => {
        largeDescriptorReads += 1;
        const descriptor = Reflect.getOwnPropertyDescriptor(target, key);
        return key === "length" && descriptor !== undefined
          ? { ...descriptor, value: 1_000_000_000 }
          : descriptor;
      },
    });
    const stringLength = new Proxy<Array<unknown>>([], {
      getOwnPropertyDescriptor: (target, key) => {
        const descriptor = Reflect.getOwnPropertyDescriptor(target, key);
        return key === "length" && descriptor !== undefined
          ? { ...descriptor, value: "0" }
          : descriptor;
      },
    });
    const negativeLength = new Proxy<Array<unknown>>([], {
      getOwnPropertyDescriptor: (target, key) => {
        const descriptor = Reflect.getOwnPropertyDescriptor(target, key);
        return key === "length" && descriptor !== undefined
          ? { ...descriptor, value: -1 }
          : descriptor;
      },
    });
    const swappedKey = new Proxy<Array<unknown>>([true], {
      ownKeys: () => ["length", "metadata"],
    });

    expect(inspectDenseArrayData(infiniteLength)).toStrictEqual({
      _tag: "Failure",
      reason: "invalidArray",
    });
    expect(inspectDenseArrayData(largeLength)).toStrictEqual({
      _tag: "Failure",
      reason: "invalidEntry",
    });
    expect(inspectDenseArrayData(stringLength)).toStrictEqual({
      _tag: "Failure",
      reason: "invalidArray",
    });
    expect(inspectDenseArrayData(negativeLength)).toStrictEqual({
      _tag: "Failure",
      reason: "invalidArray",
    });
    expect(inspectDenseArrayData(swappedKey)).toStrictEqual({
      _tag: "Failure",
      reason: "invalidEntry",
    });
    expect(infiniteDescriptorReads).toBe(1);
    expect(largeDescriptorReads).toBe(1);
  });
});
