import { describe, expect, it } from "@effect/vitest";
import { Schema } from "effect";
import { snapshotLeasedGrpcQuery } from "./grpc-leased-query-snapshot";

const SnapshotRow = Schema.Struct({
  id: Schema.String,
  label: Schema.String,
  route: Schema.Struct({
    desk: Schema.Trim,
  }),
  rank: Schema.Number,
  score: Schema.Number,
});

describe("leased gRPC query snapshots", () => {
  it("owns nested query arrays, filters, aggregate definitions, and opaque values", () => {
    const orderByEntry = { aggregate: "total", direction: "desc" };
    const query = {
      select: ["id"],
      where: {
        route: { eq: { desk: "equities" } },
        rank: { eq: "not-a-number" },
        score: { in: [1, 2] },
        label: { startsWith: "e" },
        missing: { custom: { enabled: true } },
      },
      groupBy: ["route"],
      aggregates: {
        total: { aggFunc: "sum", field: "score" },
      },
      orderBy: [orderByEntry],
      metadata: new Date(0),
    };

    const snapshot = snapshotLeasedGrpcQuery(SnapshotRow, query);

    query.select.push("label");
    Reflect.set(query.where.route.eq, "desk", "changed");
    query.where.score.in.push(3);
    Reflect.set(query.where.missing.custom, "enabled", false);
    query.groupBy.push("label");
    Reflect.set(query.aggregates.total, "field", "missing");
    Reflect.set(orderByEntry, "direction", "asc");
    query.metadata.setUTCFullYear(2020);

    expect(snapshot).toStrictEqual({
      select: ["id"],
      where: {
        route: { eq: { desk: "equities" } },
        rank: { eq: "not-a-number" },
        score: { in: [1, 2] },
        label: { startsWith: "e" },
        missing: { custom: { enabled: true } },
      },
      groupBy: ["route"],
      aggregates: {
        total: { aggFunc: "sum", field: "score" },
      },
      orderBy: [{ aggregate: "total", direction: "desc" }],
      metadata: new Date(0),
    });
    expect(snapshot.where.route.eq).not.toBe(query.where.route.eq);
    expect(snapshot.where.score.in).not.toBe(query.where.score.in);
    expect(snapshot.metadata).not.toBe(query.metadata);
  });

  it("snapshots non-record where values without consulting row fields", () => {
    const query = {
      select: ["id"],
      where: null,
    };

    expect(snapshotLeasedGrpcQuery(SnapshotRow, query)).toStrictEqual(query);
  });

  it("owns decoded direct field literals without decoding encoded query forms", () => {
    const labelFilter = { custom: { prefix: "e" } };
    const nullPrototype: Record<string, unknown> = { enabled: true };
    Object.setPrototypeOf(nullPrototype, null);
    const query = {
      select: ["id"],
      where: {
        route: { desk: "  equities  " },
        rank: 1,
        score: "not-a-number",
        label: labelFilter,
      },
      extension: nullPrototype,
    };

    const snapshot = snapshotLeasedGrpcQuery(SnapshotRow, query);
    Reflect.set(labelFilter.custom, "prefix", "changed");
    Reflect.set(nullPrototype, "enabled", false);

    expect(snapshot).toStrictEqual({
      select: ["id"],
      where: {
        route: { desk: "  equities  " },
        rank: 1,
        score: "not-a-number",
        label: { custom: { prefix: "e" } },
      },
      extension: { enabled: true },
    });
  });

  it("rejects cyclic query values", () => {
    const cyclic: Record<string, unknown> = {};
    cyclic["self"] = cyclic;

    expect(() =>
      snapshotLeasedGrpcQuery(SnapshotRow, {
        select: ["id"],
        extension: cyclic,
      }),
    ).toThrow("Leased gRPC query contains a cycle.");
  });

  it("rejects cyclic query arrays", () => {
    const cyclic: Array<unknown> = [];
    cyclic.push(cyclic);

    expect(() =>
      snapshotLeasedGrpcQuery(SnapshotRow, {
        select: ["id"],
        extension: cyclic,
      }),
    ).toThrow("Leased gRPC query contains a cycle.");
  });

  it("rejects cyclic schema-field objects and arrays", () => {
    const cyclicRoute: Record<string, unknown> = {};
    cyclicRoute["desk"] = cyclicRoute;
    const cyclicScores: Array<unknown> = [];
    cyclicScores.push(cyclicScores);

    expect(() =>
      snapshotLeasedGrpcQuery(SnapshotRow, {
        select: ["id"],
        where: { route: cyclicRoute },
      }),
    ).toThrow("Leased gRPC query contains a cycle.");
    expect(() =>
      snapshotLeasedGrpcQuery(SnapshotRow, {
        select: ["id"],
        where: { score: { in: cyclicScores } },
      }),
    ).toThrow("Leased gRPC query contains a cycle.");
  });

  it("rejects accessor-backed top-level query fields", () => {
    const query: Record<string, unknown> = {
      select: ["id"],
    };
    Object.defineProperty(query, "where", {
      enumerable: true,
      get: () => ({ route: { eq: { desk: "equities" } } }),
    });

    expect(() => snapshotLeasedGrpcQuery(SnapshotRow, query)).toThrow(
      "Leased gRPC query fields must be own data properties.",
    );
  });

  it("rejects accessor-backed nested query fields", () => {
    const extension: Record<string, unknown> = {};
    Object.defineProperty(extension, "value", {
      enumerable: true,
      get: () => "computed",
    });

    expect(() =>
      snapshotLeasedGrpcQuery(SnapshotRow, {
        select: ["id"],
        extension,
      }),
    ).toThrow("Leased gRPC query fields must be own data properties.");
  });

  it("rejects accessor-backed where fields and operators without invoking them", () => {
    let whereReads = 0;
    const where: Record<string, unknown> = {};
    Object.defineProperty(where, "score", {
      enumerable: true,
      get: () => {
        whereReads += 1;
        return { eq: 1 };
      },
    });
    let operatorReads = 0;
    const operator: Record<string, unknown> = {};
    Object.defineProperty(operator, "eq", {
      enumerable: true,
      get: () => {
        operatorReads += 1;
        return 1;
      },
    });

    expect(() =>
      snapshotLeasedGrpcQuery(SnapshotRow, {
        select: ["id"],
        where,
      }),
    ).toThrow("Leased gRPC query fields must be own data properties.");
    expect(() =>
      snapshotLeasedGrpcQuery(SnapshotRow, {
        select: ["id"],
        where: { score: operator },
      }),
    ).toThrow("Leased gRPC query fields must be own data properties.");
    expect({ operatorReads, whereReads }).toStrictEqual({
      operatorReads: 0,
      whereReads: 0,
    });
  });

  it("rejects accessor-backed decoded field literals without invoking them", () => {
    let fieldReads = 0;
    const route: Record<string, unknown> = {};
    Object.defineProperty(route, "desk", {
      enumerable: true,
      get: () => {
        fieldReads += 1;
        return "equities";
      },
    });

    expect(() =>
      snapshotLeasedGrpcQuery(SnapshotRow, {
        select: ["id"],
        where: { route },
      }),
    ).toThrow("Leased gRPC query fields must be own data properties.");
    expect(fieldReads).toBe(0);
  });

  it("rejects accessor-backed and sparse query array entries without invoking them", () => {
    let accessorReads = 0;
    const accessorArray: Array<unknown> = [undefined];
    Object.defineProperty(accessorArray, "0", {
      enumerable: true,
      get: () => {
        accessorReads += 1;
        return 1;
      },
    });
    const sparseArray: Array<unknown> = [];
    sparseArray.length = 1;

    expect(() =>
      snapshotLeasedGrpcQuery(SnapshotRow, {
        select: accessorArray,
      }),
    ).toThrow("Leased gRPC query array entries must be own data properties.");
    expect(() =>
      snapshotLeasedGrpcQuery(SnapshotRow, {
        where: { score: { in: sparseArray } },
      }),
    ).toThrow("Leased gRPC query array entries must be own data properties.");
    expect(accessorReads).toBe(0);
  });

  it("captures proxied array length without invoking its get trap", () => {
    let lengthReads = 0;
    const select = new Proxy(["id"], {
      get: (target, property, receiver) => {
        if (property === "length") {
          lengthReads += 1;
        }
        return Reflect.get(target, property, receiver);
      },
    });

    expect(snapshotLeasedGrpcQuery(SnapshotRow, { select })).toStrictEqual({
      select: ["id"],
    });
    expect(lengthReads).toBe(0);
  });

  it("ignores non-enumerable query metadata and rejects enumerable symbols", () => {
    const hiddenQuery: Record<string, unknown> = {
      select: ["id"],
    };
    Object.defineProperty(hiddenQuery, "hidden", {
      enumerable: false,
      value: true,
    });
    const symbolicQuery: Record<string, unknown> = {
      select: ["id"],
    };
    Object.defineProperty(symbolicQuery, Symbol("hidden"), {
      enumerable: true,
      value: true,
    });

    expect(snapshotLeasedGrpcQuery(SnapshotRow, hiddenQuery)).toStrictEqual({
      select: ["id"],
    });
    expect(() => snapshotLeasedGrpcQuery(SnapshotRow, symbolicQuery)).toThrow(
      "Leased gRPC query fields must be own data properties.",
    );
  });

  it("rejects query properties that disappear during descriptor capture", () => {
    const query = new Proxy(
      { select: ["id"] },
      {
        getOwnPropertyDescriptor: () => undefined,
      },
    );

    expect(() => snapshotLeasedGrpcQuery(SnapshotRow, query)).toThrow(
      "Leased gRPC query fields could not be inspected.",
    );
  });

  it("does not re-enumerate a caller proxy after capturing its data properties", () => {
    const injectedRoute = { desk: "equities" };
    let ownKeysReads = 0;
    const query = new Proxy(
      {
        select: ["id"],
      },
      {
        ownKeys: (target) => {
          ownKeysReads += 1;
          return ownKeysReads === 1 ? Reflect.ownKeys(target) : ["select", "where"];
        },
        getOwnPropertyDescriptor: (target, property) =>
          property === "where"
            ? {
                configurable: true,
                enumerable: true,
                value: { route: { eq: injectedRoute } },
                writable: true,
              }
            : Reflect.getOwnPropertyDescriptor(target, property),
        get: (target, property, receiver) =>
          property === "where"
            ? { route: { eq: injectedRoute } }
            : Reflect.get(target, property, receiver),
      },
    );

    const snapshot = snapshotLeasedGrpcQuery(SnapshotRow, query);
    Reflect.set(injectedRoute, "desk", "changed");

    expect(ownKeysReads).toBe(1);
    expect(snapshot).toStrictEqual({ select: ["id"] });
  });
});
