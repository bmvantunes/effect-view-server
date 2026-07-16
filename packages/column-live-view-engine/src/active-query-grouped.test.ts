import { describe, expect, it } from "@effect/vitest";
import { Effect, Option } from "effect";
import { fromStringUnsafe } from "effect/BigDecimal";
import { defaultGroupedIncrementalAdmissionLimits, InvalidRowError } from "./index";
import {
  acquireMaterializedQueryExecution,
  activeQueryTestInterface,
  releaseMaterializedQueryExecution,
} from "../test-harness/active-query-interface";
import { rawQueryCompilerMetadata } from "./raw-query-compiler";
import { prepareRuntimeGroupedQuery } from "./grouped-query-compiler";
import { makeIncrementalGroupedQueryExecution } from "./grouped-incremental-execution";
import type { TopicRowChangeBatch } from "./row-scan";
import { publishTopicStoreRow, TopicStore } from "./topic-store";
import {
  applyDelta,
  expectDefined,
  expectDeltaEvent,
  expectSnapshotEvent,
  firstEvent,
  makeEventReader,
  stateFromSnapshot,
} from "../test-harness/events";
import { makeEngine, order, Order, position, Position } from "../test-harness/public-engine";
import { normalizeDecimalAndBigIntFields, normalizeDecimalFields } from "../test-harness/rows";

describe("Grouped incremental query execution", () => {
  it.effect("updates grouped bigint sums incrementally through live subscriptions", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("positions", [
        position("1", "AAPL", 10n, "1.00"),
        position("2", "AAPL", 20n, "2.00"),
        position("3", "MSFT", 5n, "3.00"),
      ]);
      const query = {
        groupBy: ["symbol"],
        aggregates: {
          totalQuantity: { aggFunc: "sum", field: "quantity" },
          averageQuantity: { aggFunc: "avg", field: "quantity" },
          totalPrice: { aggFunc: "sum", field: "price" },
          averagePrice: { aggFunc: "avg", field: "price" },
          minQuantity: { aggFunc: "min", field: "quantity" },
          maxQuantity: { aggFunc: "max", field: "quantity" },
        },
        orderBy: [{ field: "symbol", direction: "asc" }],
      } satisfies {
        readonly groupBy: readonly ["symbol"];
        readonly aggregates: {
          readonly totalQuantity: { readonly aggFunc: "sum"; readonly field: "quantity" };
          readonly averageQuantity: { readonly aggFunc: "avg"; readonly field: "quantity" };
          readonly totalPrice: { readonly aggFunc: "sum"; readonly field: "price" };
          readonly averagePrice: { readonly aggFunc: "avg"; readonly field: "price" };
          readonly minQuantity: { readonly aggFunc: "min"; readonly field: "quantity" };
          readonly maxQuantity: { readonly aggFunc: "max"; readonly field: "quantity" };
        };
        readonly orderBy: readonly [{ readonly field: "symbol"; readonly direction: "asc" }];
      };

      const subscription = yield* engine.subscribe("positions", query);
      const read = yield* makeEventReader(subscription);
      const snapshot = firstEvent(yield* read(1));
      expectSnapshotEvent(snapshot);
      let state = stateFromSnapshot(snapshot);
      expect(normalizeDecimalAndBigIntFields(state.rows)).toStrictEqual([
        {
          symbol: "AAPL",
          totalQuantity: "30",
          averageQuantity: "15",
          totalPrice: "3",
          averagePrice: "1.5",
          minQuantity: "10",
          maxQuantity: "20",
        },
        {
          symbol: "MSFT",
          totalQuantity: "5",
          averageQuantity: "5",
          totalPrice: "3",
          averagePrice: "3",
          minQuantity: "5",
          maxQuantity: "5",
        },
      ]);

      yield* engine.patch("positions", "2", {
        price: fromStringUnsafe("4.00"),
        quantity: 30n,
      });
      const patchedDelta = firstEvent(yield* read(1));
      expectDeltaEvent(patchedDelta);
      state = applyDelta(state, patchedDelta);
      expect(normalizeDecimalAndBigIntFields(state.rows)).toStrictEqual([
        {
          symbol: "AAPL",
          totalQuantity: "40",
          averageQuantity: "20",
          totalPrice: "5",
          averagePrice: "2.5",
          minQuantity: "10",
          maxQuantity: "30",
        },
        {
          symbol: "MSFT",
          totalQuantity: "5",
          averageQuantity: "5",
          totalPrice: "3",
          averagePrice: "3",
          minQuantity: "5",
          maxQuantity: "5",
        },
      ]);

      yield* engine.delete("positions", "1");
      const deletedDelta = firstEvent(yield* read(1));
      expectDeltaEvent(deletedDelta);
      state = applyDelta(state, deletedDelta);
      expect(normalizeDecimalAndBigIntFields(state.rows)).toStrictEqual([
        {
          symbol: "AAPL",
          totalQuantity: "30",
          averageQuantity: "30",
          totalPrice: "4",
          averagePrice: "4",
          minQuantity: "30",
          maxQuantity: "30",
        },
        {
          symbol: "MSFT",
          totalQuantity: "5",
          averageQuantity: "5",
          totalPrice: "3",
          averagePrice: "3",
          minQuantity: "5",
          maxQuantity: "5",
        },
      ]);

      yield* engine.patch("positions", "2", {
        price: fromStringUnsafe("5.00"),
        quantity: 25n,
        symbol: "MSFT",
      });
      const movedDelta = firstEvent(yield* read(1));
      expectDeltaEvent(movedDelta);
      state = applyDelta(state, movedDelta);
      expect(normalizeDecimalAndBigIntFields(state.rows)).toStrictEqual([
        {
          symbol: "MSFT",
          totalQuantity: "30",
          averageQuantity: "15",
          totalPrice: "8",
          averagePrice: "4",
          minQuantity: "5",
          maxQuantity: "25",
        },
      ]);

      yield* subscription.close();
    }),
  );

  it.effect("applies incremental grouped change batches without rescanning rows", () =>
    Effect.gen(function* () {
      let version = 0;
      let scanCount = 0;
      let batches: ReadonlyArray<TopicRowChangeBatch<object>> = [];
      const rows = new Map<string, object>([
        ["1", order("1", "open", 10, 1, "emea")],
        ["2", order("2", "open", 20, 2, "amer")],
      ]);
      const store = {
        changesSince: () => batches,
        scanRows: (visitor: (key: string, row: object) => void) => {
          scanCount += 1;
          for (const [key, row] of rows) {
            visitor(key, row);
          }
        },
        version: () => version,
      };
      const compiled = yield* prepareRuntimeGroupedQuery(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          where: {
            region: "emea",
          },
          orderBy: [{ field: "status", direction: "asc" }],
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});

      expect(execution.latest().rows).toStrictEqual([{ status: "open", rowCount: 1n }]);
      expect(execution.latest().rows).toStrictEqual([{ status: "open", rowCount: 1n }]);
      expect(scanCount).toBe(1);

      const ignored = order("3", "closed", 30, 3, "amer");
      const inserted = order("4", "closed", 40, 4, "emea");
      rows.set("3", ignored);
      rows.set("4", inserted);
      version = 1;
      batches = [
        {
          version,
          changes: [
            {
              key: "ignored-old",
              previous: order("ignored-old", "open", 5, 5, "amer"),
              next: undefined,
            },
            {
              key: "missing-old-group",
              previous: order("missing-old-group", "cancelled", 5, 5, "emea"),
              next: undefined,
            },
            {
              key: "missing-open-member",
              previous: order("missing-open-member", "open", 5, 5, "emea"),
              next: undefined,
            },
            {
              key: "missing-replace-member",
              previous: order("missing-replace-member", "open", 6, 6, "emea"),
              next: order("missing-replace-member", "open", 7, 7, "emea"),
            },
            {
              key: "missing-replace-group",
              previous: order("missing-replace-group", "cancelled", 8, 8, "emea"),
              next: order("missing-replace-group", "cancelled", 9, 9, "emea"),
            },
            {
              key: "1",
              previous: undefined,
              next: order("1", "open", 15, 6, "emea"),
            },
            { key: "3", previous: undefined, next: ignored },
            { key: "4", previous: undefined, next: inserted },
          ],
        },
      ];

      expect(execution.latest().rows).toStrictEqual([
        { status: "cancelled", rowCount: 1n },
        { status: "closed", rowCount: 1n },
        { status: "open", rowCount: 2n },
      ]);
      expect(scanCount).toBe(1);
    }),
  );

  it.effect("patches order-neutral grouped aggregate rows without changing window order", () =>
    Effect.gen(function* () {
      let version = 0;
      let scanCount = 0;
      let batches: ReadonlyArray<TopicRowChangeBatch<object>> = [];
      const rows = new Map<string, object>([
        ["cancelled", order("cancelled", "cancelled", 5, 1, "emea")],
        ["open-a", order("open-a", "open", 10, 2, "emea")],
        ["open-b", order("open-b", "open", 20, 3, "emea")],
        ["closed", order("closed", "closed", 7, 4, "emea")],
      ]);
      const store = {
        changesSince: () => batches,
        scanRows: (visitor: (key: string, row: object) => void) => {
          scanCount += 1;
          for (const [key, row] of rows) {
            visitor(key, row);
          }
        },
        version: () => version,
      };
      let fullEvaluationCount = 0;
      let patchedEvaluationCount = 0;
      const compiled = yield* prepareRuntimeGroupedQuery(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: {
            maxPrice: { aggFunc: "max", field: "price" },
            rowCount: { aggFunc: "count" },
            totalPrice: { aggFunc: "sum", field: "price" },
          },
          orderBy: [{ field: "status", direction: "asc" }],
          limit: 3,
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(
        store,
        compiled,
        () => {},
        defaultGroupedIncrementalAdmissionLimits,
        {
          onFullEvaluation: () => {
            fullEvaluationCount += 1;
          },
          onPatchedEvaluation: () => {
            patchedEvaluationCount += 1;
          },
        },
      );
      expect(normalizeDecimalAndBigIntFields(execution.latest().rows)).toStrictEqual([
        {
          status: "cancelled",
          maxPrice: 5,
          rowCount: "1",
          totalPrice: "5",
        },
        {
          status: "closed",
          maxPrice: 7,
          rowCount: "1",
          totalPrice: "7",
        },
        {
          status: "open",
          maxPrice: 20,
          rowCount: "2",
          totalPrice: "30",
        },
      ]);

      const previous = order("open-a", "open", 10, 2, "emea");
      const next = order("open-a", "open", 15, 5, "emea");
      rows.set("open-a", next);
      version = 1;
      batches = [
        {
          version,
          changes: [{ key: "open-a", previous, next }],
        },
      ];

      expect(normalizeDecimalAndBigIntFields(execution.latest().rows)).toStrictEqual([
        {
          status: "cancelled",
          maxPrice: 5,
          rowCount: "1",
          totalPrice: "5",
        },
        {
          status: "closed",
          maxPrice: 7,
          rowCount: "1",
          totalPrice: "7",
        },
        {
          status: "open",
          maxPrice: 20,
          rowCount: "2",
          totalPrice: "35",
        },
      ]);
      expect(execution.latest().version).toBe(1);
      expect(scanCount).toBe(1);
      expect(fullEvaluationCount).toBe(0);
      expect(patchedEvaluationCount).toBe(1);

      const recomputedExtremumPrevious = order("open-b", "open", 20, 3, "emea");
      const recomputedExtremumNext = order("open-b", "open", 12, 6, "emea");
      rows.set("open-b", recomputedExtremumNext);
      version = 2;
      batches = [
        {
          version,
          changes: [
            {
              key: "open-b",
              previous: recomputedExtremumPrevious,
              next: recomputedExtremumNext,
            },
          ],
        },
      ];

      expect(normalizeDecimalAndBigIntFields(execution.latest().rows)).toStrictEqual([
        {
          status: "cancelled",
          maxPrice: 5,
          rowCount: "1",
          totalPrice: "5",
        },
        {
          status: "closed",
          maxPrice: 7,
          rowCount: "1",
          totalPrice: "7",
        },
        {
          status: "open",
          maxPrice: 15,
          rowCount: "2",
          totalPrice: "27",
        },
      ]);
      expect(execution.latest().version).toBe(2);
      expect(scanCount).toBe(1);
      expect(fullEvaluationCount).toBe(0);
      expect(patchedEvaluationCount).toBe(2);

      const unchangedAggregatePrevious = order("open-b", "open", 12, 6, "emea");
      const unchangedAggregateNext = order("open-b", "open", 12, 7, "emea");
      rows.set("open-b", unchangedAggregateNext);
      version = 3;
      batches = [
        {
          version,
          changes: [
            {
              key: "open-b",
              previous: unchangedAggregatePrevious,
              next: unchangedAggregateNext,
            },
          ],
        },
      ];

      expect(normalizeDecimalAndBigIntFields(execution.latest().rows)).toStrictEqual([
        {
          status: "cancelled",
          maxPrice: 5,
          rowCount: "1",
          totalPrice: "5",
        },
        {
          status: "closed",
          maxPrice: 7,
          rowCount: "1",
          totalPrice: "7",
        },
        {
          status: "open",
          maxPrice: 15,
          rowCount: "2",
          totalPrice: "27",
        },
      ]);
      expect(execution.latest().version).toBe(3);
      expect(scanCount).toBe(1);
      expect(fullEvaluationCount).toBe(0);
      expect(patchedEvaluationCount).toBe(3);
    }),
  );

  it.effect("ignores unbranded changed-field metadata from manual grouped scans", () =>
    Effect.gen(function* () {
      let version = 0;
      let batches: ReadonlyArray<TopicRowChangeBatch<object>> = [];
      const rows = new Map<string, object>([
        ["open-a", order("open-a", "open", 10, 2, "emea")],
        ["open-b", order("open-b", "open", 20, 3, "emea")],
      ]);
      const store = {
        changesSince: () => batches,
        scanRows: (visitor: (key: string, row: object) => void) => {
          for (const [key, row] of rows) {
            visitor(key, row);
          }
        },
        version: () => version,
      };
      const compiled = yield* prepareRuntimeGroupedQuery(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: {
            totalPrice: { aggFunc: "sum", field: "price" },
          },
          orderBy: [{ aggregate: "totalPrice", direction: "desc" }],
          limit: 1,
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(
        store,
        compiled,
        () => {},
        defaultGroupedIncrementalAdmissionLimits,
      );

      expect(normalizeDecimalAndBigIntFields(execution.latest().rows)).toStrictEqual([
        {
          status: "open",
          totalPrice: "30",
        },
      ]);

      const previous = order("open-b", "open", 20, 3, "emea");
      const next = order("open-b", "open", 25, 4, "emea");
      rows.set("open-b", next);
      version = 1;
      batches = [
        {
          version,
          changes: [
            {
              // @ts-expect-error manual row scans cannot forge trusted changed-field metadata.
              changedFields: new Set(["updatedAt"]),
              key: "open-b",
              previous,
              next,
            },
          ],
        },
      ];

      expect(normalizeDecimalAndBigIntFields(execution.latest().rows)).toStrictEqual([
        {
          status: "open",
          totalPrice: "35",
        },
      ]);
    }),
  );

  it.effect("keeps order-neutral grouped patches outside the visible window invisible", () =>
    Effect.gen(function* () {
      let version = 0;
      let scanCount = 0;
      let batches: ReadonlyArray<TopicRowChangeBatch<object>> = [];
      const rows = new Map<string, object>([
        ["cancelled", order("cancelled", "cancelled", 5, 1, "emea")],
        ["open-a", order("open-a", "open", 10, 2, "emea")],
        ["open-b", order("open-b", "open", 20, 3, "emea")],
      ]);
      let fullEvaluationCount = 0;
      let patchedEvaluationCount = 0;
      const store = {
        changesSince: () => batches,
        scanRows: (visitor: (key: string, row: object) => void) => {
          scanCount += 1;
          for (const [key, row] of rows) {
            visitor(key, row);
          }
        },
        version: () => version,
      };
      const compiled = yield* prepareRuntimeGroupedQuery(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: {
            maxPrice: { aggFunc: "max", field: "price" },
            rowCount: { aggFunc: "count" },
            totalPrice: { aggFunc: "sum", field: "price" },
          },
          orderBy: [{ field: "status", direction: "asc" }],
          limit: 1,
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(
        store,
        compiled,
        () => {},
        defaultGroupedIncrementalAdmissionLimits,
        {
          onFullEvaluation: () => {
            fullEvaluationCount += 1;
          },
          onPatchedEvaluation: () => {
            patchedEvaluationCount += 1;
          },
        },
      );

      expect(normalizeDecimalAndBigIntFields(execution.latest().rows)).toStrictEqual([
        {
          status: "cancelled",
          maxPrice: 5,
          rowCount: "1",
          totalPrice: "5",
        },
      ]);

      const previous = order("open-a", "open", 10, 2, "emea");
      const next = order("open-a", "open", 15, 5, "emea");
      rows.set("open-a", next);
      version = 1;
      batches = [
        {
          version,
          changes: [{ key: "open-a", previous, next }],
        },
      ];

      expect(normalizeDecimalAndBigIntFields(execution.latest().rows)).toStrictEqual([
        {
          status: "cancelled",
          maxPrice: 5,
          rowCount: "1",
          totalPrice: "5",
        },
      ]);
      expect(execution.latest().version).toBe(1);
      expect(scanCount).toBe(1);
      expect(fullEvaluationCount).toBe(0);
      expect(patchedEvaluationCount).toBe(1);
    }),
  );

  it.effect("fully re-evaluates grouped windows when patched aggregates drive ordering", () =>
    Effect.gen(function* () {
      let version = 0;
      let scanCount = 0;
      let batches: ReadonlyArray<TopicRowChangeBatch<object>> = [];
      const rows = new Map<string, object>([
        ["open", order("open", "open", 10, 1, "emea")],
        ["closed", order("closed", "closed", 20, 2, "emea")],
      ]);
      const store = {
        changesSince: () => batches,
        scanRows: (visitor: (key: string, row: object) => void) => {
          scanCount += 1;
          for (const [key, row] of rows) {
            visitor(key, row);
          }
        },
        version: () => version,
      };
      let fullEvaluationCount = 0;
      let patchedEvaluationCount = 0;
      const compiled = yield* prepareRuntimeGroupedQuery(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: {
            rowCount: { aggFunc: "count" },
            totalPrice: { aggFunc: "sum", field: "price" },
          },
          orderBy: [{ aggregate: "totalPrice", direction: "desc" }],
          limit: 2,
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(
        store,
        compiled,
        () => {},
        defaultGroupedIncrementalAdmissionLimits,
        {
          onFullEvaluation: () => {
            fullEvaluationCount += 1;
          },
          onPatchedEvaluation: () => {
            patchedEvaluationCount += 1;
          },
        },
      );

      expect(normalizeDecimalAndBigIntFields(execution.latest().rows)).toStrictEqual([
        {
          status: "closed",
          rowCount: "1",
          totalPrice: "20",
        },
        {
          status: "open",
          rowCount: "1",
          totalPrice: "10",
        },
      ]);

      const previous = order("open", "open", 10, 1, "emea");
      const next = order("open", "open", 30, 3, "emea");
      rows.set("open", next);
      version = 1;
      batches = [
        {
          version,
          changes: [{ key: "open", previous, next }],
        },
      ];

      expect(normalizeDecimalAndBigIntFields(execution.latest().rows)).toStrictEqual([
        {
          status: "open",
          rowCount: "1",
          totalPrice: "30",
        },
        {
          status: "closed",
          rowCount: "1",
          totalPrice: "20",
        },
      ]);
      expect(scanCount).toBe(1);
      expect(fullEvaluationCount).toBe(1);
      expect(patchedEvaluationCount).toBe(0);
    }),
  );

  it.effect("tracks incremental zero-limit grouped counts without aggregate windows", () =>
    Effect.gen(function* () {
      let version = 0;
      let scanCount = 0;
      let batches: ReadonlyArray<TopicRowChangeBatch<object>> = [];
      const retainedCustomer = {
        ...order("1-extra", "open", 15, 4, "emea"),
        customerId: "customer-1",
      };
      const rows = new Map<string, object>([
        ["1", order("1", "open", 10, 1, "emea")],
        ["1-extra", retainedCustomer],
        ["2", order("2", "open", 20, 2, "amer")],
      ]);
      const store = {
        changesSince: () => batches,
        scanRows: (visitor: (key: string, row: object) => void) => {
          scanCount += 1;
          for (const [key, row] of rows) {
            visitor(key, row);
          }
        },
        version: () => version,
      };
      const compiled = yield* prepareRuntimeGroupedQuery(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["customerId"],
          aggregates: {
            totalPrice: { aggFunc: "sum", field: "price" },
          },
          where: {
            region: "emea",
          },
          offset: 10_000,
          limit: 0,
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});

      expect(execution.latest().rows).toStrictEqual([]);
      expect(execution.latest().totalRows).toBe(1);
      expect(execution.diagnostics()).toStrictEqual({
        fullEvaluationCount: 0,
        patchedEvaluationCount: 0,
      });
      expect(scanCount).toBe(1);

      rows.delete("1-extra");
      version = 1;
      batches = [
        {
          version,
          changes: [{ key: "1-extra", previous: retainedCustomer, next: undefined }],
        },
      ];

      expect(execution.latest().rows).toStrictEqual([]);
      expect(execution.latest().totalRows).toBe(1);
      expect(execution.diagnostics()).toStrictEqual({
        fullEvaluationCount: 0,
        patchedEvaluationCount: 1,
      });
      expect(scanCount).toBe(1);

      const inserted = order("3", "closed", 30, 3, "emea");
      rows.set("3", inserted);
      version = 2;
      batches = [
        {
          version,
          changes: [{ key: "3", previous: undefined, next: inserted }],
        },
      ];

      expect(execution.latest().rows).toStrictEqual([]);
      expect(execution.latest().totalRows).toBe(2);
      expect(execution.diagnostics()).toStrictEqual({
        fullEvaluationCount: 0,
        patchedEvaluationCount: 2,
      });
      expect(scanCount).toBe(1);

      rows.delete("3");
      version = 3;
      batches = [
        {
          version,
          changes: [{ key: "3", previous: inserted, next: undefined }],
        },
      ];

      expect(execution.latest().rows).toStrictEqual([]);
      expect(execution.latest().totalRows).toBe(1);
      expect(execution.diagnostics()).toStrictEqual({
        fullEvaluationCount: 0,
        patchedEvaluationCount: 3,
      });
      expect(scanCount).toBe(1);

      rows.set("3", inserted);
      version = 4;
      batches = [
        {
          version,
          changes: [
            {
              key: "missing-zero-limit",
              previous: order("missing-zero-limit", "open", 1, 1, "emea"),
              next: undefined,
            },
            {
              key: "ignored-zero-limit",
              previous: undefined,
              next: order("ignored-zero-limit", "open", 1, 1, "amer"),
            },
            {
              key: "ignored-zero-limit-previous",
              previous: order("ignored-zero-limit-previous", "open", 1, 1, "amer"),
              next: undefined,
            },
          ],
        },
      ];

      expect(execution.latest().rows).toStrictEqual([]);
      expect(execution.latest().totalRows).toBe(1);
      expect(execution.diagnostics()).toStrictEqual({
        fullEvaluationCount: 0,
        patchedEvaluationCount: 4,
      });
      expect(scanCount).toBe(1);

      version = 5;
      batches = [
        {
          version,
          changes: [{ key: "3", previous: undefined, next: inserted }],
        },
      ];

      expect(execution.latest().rows).toStrictEqual([]);
      expect(execution.latest().totalRows).toBe(2);
      expect(execution.diagnostics()).toStrictEqual({
        fullEvaluationCount: 0,
        patchedEvaluationCount: 5,
      });
      expect(scanCount).toBe(1);

      const repeatedGroupChanges = Array.from({ length: 4_097 }, (_value, index) => {
        const repeated = {
          ...order(`repeat-${index}`, "open", index, index, "emea"),
          customerId: "customer-3",
        };
        rows.set(`repeat-${index}`, repeated);
        return {
          key: `repeat-${index}`,
          previous: undefined,
          next: repeated,
        };
      });
      version = 6;
      batches = [
        {
          version,
          changes: repeatedGroupChanges,
        },
      ];

      expect(execution.latest().rows).toStrictEqual([]);
      expect(execution.latest().totalRows).toBe(2);
      expect(execution.incremental).toBe(true);
      expect(execution.diagnostics()).toStrictEqual({
        fullEvaluationCount: 0,
        patchedEvaluationCount: 6,
      });
      expect(scanCount).toBe(1);

      const overflowChanges = Array.from({ length: 8_193 }, (_value, index) => {
        const overflow = order(`overflow-${index}`, "open", index, index, "emea");
        rows.set(`overflow-${index}`, overflow);
        return {
          key: `overflow-${index}`,
          previous: undefined,
          next: overflow,
        };
      });
      version = 7;
      batches = [
        {
          version,
          changes: overflowChanges,
        },
      ];

      expect(execution.latest().rows).toStrictEqual([]);
      expect(execution.latest().totalRows).toBe(8_195);
      expect(execution.incremental).toBe(false);
      expect(execution.diagnostics()).toStrictEqual({
        fullEvaluationCount: 1,
        patchedEvaluationCount: 6,
      });
      expect(scanCount).toBe(2);
    }),
  );

  it.effect("ignores malformed runtime aggregate values while removing grouped members", () =>
    Effect.gen(function* () {
      let version = 0;
      let scanCount = 0;
      let batches: ReadonlyArray<TopicRowChangeBatch<object>> = [];
      const malformedPosition = {
        id: "bad",
        accountId: "account-bad",
        symbol: "AAPL",
        active: true,
        quantity: "bad",
        price: "bad",
      };
      const validPosition = position("good", "AAPL", 10n, "2.00");
      const rows = new Map<string, object>([
        ["bad", malformedPosition],
        ["good", validPosition],
      ]);
      const store = {
        changesSince: () => batches,
        scanRows: (visitor: (key: string, row: object) => void) => {
          scanCount += 1;
          for (const [key, row] of rows) {
            visitor(key, row);
          }
        },
        version: () => version,
      };
      const compiled = yield* prepareRuntimeGroupedQuery(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          groupBy: ["symbol"],
          aggregates: {
            rowCount: { aggFunc: "count" },
            totalQuantity: { aggFunc: "sum", field: "quantity" },
            averageQuantity: { aggFunc: "avg", field: "quantity" },
            totalPrice: { aggFunc: "sum", field: "price" },
            averagePrice: { aggFunc: "avg", field: "price" },
          },
          orderBy: [{ field: "symbol", direction: "asc" }],
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});

      expect(normalizeDecimalAndBigIntFields(execution.latest().rows)).toStrictEqual([
        {
          symbol: "AAPL",
          rowCount: "2",
          totalQuantity: "10",
          averageQuantity: "10",
          totalPrice: "2",
          averagePrice: "2",
        },
      ]);
      expect(scanCount).toBe(1);

      rows.delete("bad");
      version = 1;
      batches = [
        {
          version,
          changes: [{ key: "bad", previous: malformedPosition, next: undefined }],
        },
      ];

      expect(normalizeDecimalAndBigIntFields(execution.latest().rows)).toStrictEqual([
        {
          symbol: "AAPL",
          rowCount: "1",
          totalQuantity: "10",
          averageQuantity: "10",
          totalPrice: "2",
          averagePrice: "2",
        },
      ]);
      expect(scanCount).toBe(1);
    }),
  );

  it.effect("falls back when grouped retained aggregate state exceeds admission", () =>
    Effect.gen(function* () {
      let scanCount = 0;
      const rows = new Map<string, object>(
        Array.from({ length: 4_096 }, (_value, index) => [
          `row-${index}`,
          order(`row-${index}`, "open", index, index),
        ]),
      );
      const store = {
        changesSince: () => [],
        scanRows: (visitor: (key: string, row: object) => false | void) => {
          scanCount += 1;
          for (const [key, row] of rows) {
            visitor(key, row);
          }
        },
        version: () => 0,
      };
      const retainedAggregates = Object.fromEntries(
        Array.from({ length: 17 }, (_value, index) => [
          `maxUpdatedAt${index}`,
          { aggFunc: "max", field: "updatedAt" },
        ]),
      );
      const compiled = yield* prepareRuntimeGroupedQuery(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: retainedAggregates,
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});

      expect(execution.incremental).toBe(false);
      expect(execution.latest().totalRows).toBe(1);
      expect(scanCount).toBe(2);
    }),
  );

  it.effect("falls back when materialized grouped admission is ignored by the row scanner", () =>
    Effect.gen(function* () {
      const rows = new Map<string, object>(
        Array.from({ length: 4_098 }, (_value, index) => [
          `row-${index}`,
          order(`row-${index}`, "open", index, index),
        ]),
      );
      const store = {
        changesSince: () => [],
        scanRows: (visitor: (key: string, row: object) => false | void) => {
          for (const [key, row] of rows) {
            visitor(key, row);
          }
        },
        version: () => 0,
      };
      const compiled = yield* prepareRuntimeGroupedQuery(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});

      expect(execution.incremental).toBe(false);
      expect(execution.latest().rows).toStrictEqual([{ status: "open", rowCount: 4_098n }]);
    }),
  );

  it.effect("falls back when count-only grouped admission is ignored by the row scanner", () =>
    Effect.gen(function* () {
      const rows = new Map<string, object>([
        ["ignored", order("ignored", "open", 1, 1, "amer")],
        ...Array.from({ length: 8_194 }, (_value, index): [string, object] => {
          const key = `row-${index}`;
          return [key, order(key, "open", index, index, "emea")];
        }),
      ]);
      const store = {
        changesSince: () => [],
        scanRows: (visitor: (key: string, row: object) => false | void) => {
          for (const [key, row] of rows) {
            visitor(key, row);
          }
        },
        version: () => 0,
      };
      const compiled = yield* prepareRuntimeGroupedQuery(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["customerId"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          where: {
            region: "emea",
          },
          limit: 0,
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});

      expect(execution.incremental).toBe(false);
      expect(execution.latest().rows).toStrictEqual([]);
      expect(execution.latest().totalRows).toBe(8_194);
    }),
  );

  it.effect("uses fallback grouped execution when incremental admission is too broad", () =>
    Effect.gen(function* () {
      let version = 0;
      const scanCounts: Array<number> = [];
      const rows = new Map<string, object>(
        Array.from({ length: 65_537 }, (_value, index) => [
          `row-${index}`,
          order(`row-${index}`, "open", index, index),
        ]),
      );
      const store = {
        changesSince: () => [],
        scanRows: (visitor: (key: string, row: object) => false | void) => {
          let scanCount = 0;
          for (const [key, row] of rows) {
            scanCount += 1;
            if (visitor(key, row) === false) {
              break;
            }
          }
          scanCounts.push(scanCount);
        },
        version: () => version,
      };
      const compiled = yield* prepareRuntimeGroupedQuery(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});
      expect(execution.incremental).toBe(false);
      expect(execution.latest().rows).toStrictEqual([{ status: "open", rowCount: 65_537n }]);
      expect(scanCounts).toStrictEqual([4_097, 65_537]);

      rows.set("closed", order("closed", "closed", 1, 65_538));
      version = 1;
      expect(execution.latest().rows).toStrictEqual([
        { status: "closed", rowCount: 1n },
        { status: "open", rowCount: 65_537n },
      ]);
    }),
  );

  it.effect(
    "switches grouped execution to fallback when an incremental batch exceeds admission",
    () =>
      Effect.gen(function* () {
        let version = 0;
        let batches: ReadonlyArray<TopicRowChangeBatch<object>> = [];
        const rows = new Map<string, object>();
        const store = {
          changesSince: () => batches,
          scanRows: (visitor: (key: string, row: object) => void) => {
            for (const [key, row] of rows) {
              visitor(key, row);
            }
          },
          version: () => version,
        };
        const compiled = yield* prepareRuntimeGroupedQuery(
          "orders",
          rawQueryCompilerMetadata(Order),
          {
            groupBy: ["status"],
            aggregates: {
              rowCount: { aggFunc: "count" },
            },
          },
        );
        const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});
        expect(execution.incremental).toBe(true);

        const changes = Array.from({ length: 65_537 }, (_value, index) => {
          const row = order(`row-${index}`, "open", index, index);
          rows.set(row.id, row);
          return {
            key: row.id,
            previous: undefined,
            next: row,
          };
        });
        version = 1;
        batches = [{ changes, version }];

        expect(execution.latest().rows).toStrictEqual([{ status: "open", rowCount: 65_537n }]);
        expect(execution.incremental).toBe(false);
        rows.set("closed", order("closed", "closed", 1, 65_538));
        version = 2;
        batches = [];
        expect(execution.latest().rows).toStrictEqual([
          { status: "closed", rowCount: 1n },
          { status: "open", rowCount: 65_537n },
        ]);
      }),
  );

  it.effect(
    "switches grouped execution to fallback when incremental batches exceed total admission",
    () =>
      Effect.gen(function* () {
        let version = 0;
        let batches: ReadonlyArray<TopicRowChangeBatch<object>> = [];
        const rows = new Map<string, object>();
        const store = {
          changesSince: () => batches,
          scanRows: (visitor: (key: string, row: object) => false | void) => {
            for (const [key, row] of rows) {
              if (visitor(key, row) === false) {
                break;
              }
            }
          },
          version: () => version,
        };
        const compiled = yield* prepareRuntimeGroupedQuery(
          "positions",
          rawQueryCompilerMetadata(Position),
          {
            groupBy: ["symbol"],
            aggregates: {
              rowCount: { aggFunc: "count" },
            },
          },
        );
        const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});
        expect(execution.incremental).toBe(true);

        const changes = Array.from({ length: 65_537 }, (_value, index) => {
          const row = position(`row-${index}`, `symbol-${index % 8_192}`, 1n, "1");
          rows.set(row.id, row);
          return {
            key: row.id,
            previous: undefined,
            next: row,
          };
        });
        version = 1;
        batches = [{ changes, version }];

        expect(execution.latest().totalRows).toBe(8_192);
        expect(execution.incremental).toBe(false);
      }),
  );

  it.effect(
    "switches grouped execution to fallback when incremental batches exceed group admission",
    () =>
      Effect.gen(function* () {
        let version = 0;
        let batches: ReadonlyArray<TopicRowChangeBatch<object>> = [];
        const rows = new Map<string, object>();
        const store = {
          changesSince: () => batches,
          scanRows: (visitor: (key: string, row: object) => false | void) => {
            for (const [key, row] of rows) {
              if (visitor(key, row) === false) {
                break;
              }
            }
          },
          version: () => version,
        };
        const compiled = yield* prepareRuntimeGroupedQuery(
          "positions",
          rawQueryCompilerMetadata(Position),
          {
            groupBy: ["symbol"],
            aggregates: {
              rowCount: { aggFunc: "count" },
            },
          },
        );
        const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});
        expect(execution.incremental).toBe(true);

        const changes = Array.from({ length: 8_193 }, (_value, index) => {
          const row = position(`group-row-${index}`, `group-symbol-${index}`, 1n, "1");
          rows.set(row.id, row);
          return {
            key: row.id,
            previous: undefined,
            next: row,
          };
        });
        version = 1;
        batches = [{ changes, version }];

        expect(execution.latest().totalRows).toBe(8_193);
        expect(execution.incremental).toBe(false);
      }),
  );

  it.effect("falls back to a grouped rebuild when the row-change journal is unavailable", () =>
    Effect.gen(function* () {
      let version = 0;
      let scanCount = 0;
      const rows = new Map<string, object>([["1", order("1", "open", 10, 1)]]);
      const store = {
        changesSince: () => undefined,
        scanRows: (visitor: (key: string, row: object) => void) => {
          scanCount += 1;
          for (const [key, row] of rows) {
            visitor(key, row);
          }
        },
        version: () => version,
      };
      const compiled = yield* prepareRuntimeGroupedQuery(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          orderBy: [{ field: "status", direction: "asc" }],
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});

      expect(execution.latest().rows).toStrictEqual([{ status: "open", rowCount: 1n }]);
      expect(execution.diagnostics()).toStrictEqual({
        fullEvaluationCount: 0,
        patchedEvaluationCount: 0,
      });
      rows.set("2", order("2", "closed", 20, 2));
      version = 1;

      expect(execution.latest().rows).toStrictEqual([
        { status: "closed", rowCount: 1n },
        { status: "open", rowCount: 1n },
      ]);
      expect(execution.diagnostics()).toStrictEqual({
        fullEvaluationCount: 1,
        patchedEvaluationCount: 0,
      });
      expect(scanCount).toBe(2);
    }),
  );

  it.effect("uses fallback when a grouped rebuild after a missed journal exceeds admission", () =>
    Effect.gen(function* () {
      let version = 0;
      const rows = new Map<string, object>([["initial", order("initial", "open", 1, 1)]]);
      const store = {
        changesSince: () => undefined,
        scanRows: (visitor: (key: string, row: object) => void) => {
          for (const [key, row] of rows) {
            visitor(key, row);
          }
        },
        version: () => version,
      };
      const compiled = yield* prepareRuntimeGroupedQuery(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});
      expect(execution.latest().rows).toStrictEqual([{ status: "open", rowCount: 1n }]);
      expect(execution.diagnostics()).toStrictEqual({
        fullEvaluationCount: 0,
        patchedEvaluationCount: 0,
      });

      for (let index = 0; index < 65_537; index += 1) {
        const row = order(`wide-${index}`, "closed", index, index);
        rows.set(row.id, row);
      }
      version = 1;

      expect(execution.latest().rows).toStrictEqual([
        { status: "closed", rowCount: 65_537n },
        { status: "open", rowCount: 1n },
      ]);
      expect(execution.diagnostics()).toStrictEqual({
        fullEvaluationCount: 1,
        patchedEvaluationCount: 0,
      });
      rows.set("cancelled", order("cancelled", "cancelled", 1, 65_538));
      version = 2;
      expect(execution.latest().rows).toStrictEqual([
        { status: "cancelled", rowCount: 1n },
        { status: "closed", rowCount: 65_537n },
        { status: "open", rowCount: 1n },
      ]);
      expect(execution.diagnostics()).toStrictEqual({
        fullEvaluationCount: 2,
        patchedEvaluationCount: 0,
      });
    }),
  );

  it.effect("rebuilds a real grouped execution after its row-change journal window is missed", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      yield* publishTopicStoreRow(store, order("initial", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      const readModel = activeQueryTestInterface(store);
      const compiled = yield* prepareRuntimeGroupedQuery(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          orderBy: [{ field: "status", direction: "asc" }],
        },
      );
      const execution = yield* acquireMaterializedQueryExecution(
        readModel,
        "missed-real-journal",
        compiled.plan.resultSemantics,
        () => makeIncrementalGroupedQueryExecution(readModel, compiled, () => {}),
      );
      const cursor = execution.createCursor();

      for (let index = 0; index < 1_025; index += 1) {
        yield* publishTopicStoreRow(
          store,
          order(`late-${index}`, "closed", index, index),
          (topic, message) => InvalidRowError.make({ topic, message }),
        );
      }

      const next = yield* execution.next("missed-real-journal-query", cursor);
      expect(Option.isSome(next)).toBe(true);
      expect(expectDefined(Option.getOrUndefined(next)).totalRows).toBe(2);

      yield* releaseMaterializedQueryExecution(readModel, "missed-real-journal");
    }),
  );

  it.effect("shares materialized grouped subscriptions and emits grouped deltas", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [order("1", "open", 10, 1), order("2", "closed", 5, 2)]);
      const query = {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          totalPrice: { aggFunc: "sum", field: "price" },
        },
        orderBy: [{ aggregate: "rowCount", direction: "desc" }],
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly rowCount: { readonly aggFunc: "count" };
          readonly totalPrice: { readonly aggFunc: "sum"; readonly field: "price" };
        };
        readonly orderBy: readonly [{ readonly aggregate: "rowCount"; readonly direction: "desc" }];
      };

      const first = yield* engine.subscribe("orders", query);
      const second = yield* engine.subscribe("orders", query);
      const readFirst = yield* makeEventReader(first);
      const readSecond = yield* makeEventReader(second);
      const firstSnapshot = firstEvent(yield* readFirst(1));
      const secondSnapshot = firstEvent(yield* readSecond(1));
      expectSnapshotEvent(firstSnapshot);
      expectSnapshotEvent(secondSnapshot);
      expect(normalizeDecimalFields(firstSnapshot.rows)).toStrictEqual([
        { status: "closed", rowCount: 1n, totalPrice: "5" },
        { status: "open", rowCount: 1n, totalPrice: "10" },
      ]);

      let health = yield* engine.health();
      expect(health.topics.orders.activeViews).toBe(1);
      expect(health.topics.orders.activeSubscriptions).toBe(2);

      yield* engine.publish("orders", order("3", "open", 7, 3));
      const firstDelta = firstEvent(yield* readFirst(1));
      const secondDelta = firstEvent(yield* readSecond(1));
      expectDeltaEvent(firstDelta);
      expectDeltaEvent(secondDelta);
      expect(firstDelta.totalRows).toBe(2);
      expect(secondDelta.totalRows).toBe(2);

      yield* first.close();
      health = yield* engine.health();
      expect(health.topics.orders.activeViews).toBe(1);
      expect(health.topics.orders.activeSubscriptions).toBe(1);

      yield* second.close();
      health = yield* engine.health();
      expect(health.topics.orders.activeViews).toBe(0);
      expect(health.topics.orders.activeSubscriptions).toBe(0);
    }),
  );

  it.effect("emits grouped deltas for order-neutral aggregate patches", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("cancelled", "cancelled", 5, 1, "emea"),
        order("closed", "closed", 7, 2, "emea"),
        order("open-a", "open", 10, 3, "emea"),
        order("open-b", "open", 20, 4, "emea"),
      ]);
      const query = {
        groupBy: ["status"],
        aggregates: {
          maxPrice: { aggFunc: "max", field: "price" },
          rowCount: { aggFunc: "count" },
          totalPrice: { aggFunc: "sum", field: "price" },
        },
        orderBy: [{ field: "status", direction: "asc" }],
        limit: 3,
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly maxPrice: { readonly aggFunc: "max"; readonly field: "price" };
          readonly rowCount: { readonly aggFunc: "count" };
          readonly totalPrice: { readonly aggFunc: "sum"; readonly field: "price" };
        };
        readonly orderBy: readonly [{ readonly field: "status"; readonly direction: "asc" }];
        readonly limit: 3;
      };

      const subscription = yield* engine.subscribe("orders", query);
      const read = yield* makeEventReader(subscription);
      const snapshot = firstEvent(yield* read(1));
      expectSnapshotEvent(snapshot);
      expect(snapshot.rows).toStrictEqual([
        {
          status: "cancelled",
          maxPrice: 5,
          rowCount: 1n,
          totalPrice: fromStringUnsafe("5"),
        },
        {
          status: "closed",
          maxPrice: 7,
          rowCount: 1n,
          totalPrice: fromStringUnsafe("7"),
        },
        {
          status: "open",
          maxPrice: 20,
          rowCount: 2n,
          totalPrice: fromStringUnsafe("30"),
        },
      ]);
      const openGroupKey = expectDefined(snapshot.keys[2]);
      const queryId = snapshot.queryId;

      yield* engine.patch("orders", "open-a", {
        price: 15,
        updatedAt: 5,
      });
      const visibleDelta = firstEvent(yield* read(1));
      expectDeltaEvent(visibleDelta);
      expect(visibleDelta).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId,
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "update",
            key: openGroupKey,
            row: {
              status: "open",
              maxPrice: 20,
              rowCount: 2n,
              totalPrice: fromStringUnsafe("35"),
            },
            index: 2,
          },
        ],
        totalRows: 3,
      });

      yield* engine.patch("orders", "open-b", {
        price: 12,
        updatedAt: 6,
      });
      const extremumDelta = firstEvent(yield* read(1));
      expectDeltaEvent(extremumDelta);
      expect(extremumDelta).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId,
        fromVersion: 2,
        toVersion: 3,
        operations: [
          {
            type: "update",
            key: openGroupKey,
            row: {
              status: "open",
              maxPrice: 15,
              rowCount: 2n,
              totalPrice: fromStringUnsafe("27"),
            },
            index: 2,
          },
        ],
        totalRows: 3,
      });

      yield* subscription.close();
    }),
  );

  it.effect("updates grouped subscriptions incrementally across moves and deletes", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("1", "open", 10, 1, "emea"),
        order("2", "open", 20, 2, "amer"),
        order("3", "closed", 5, 3, "emea"),
      ]);
      const query = {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          distinctRegions: { aggFunc: "countDistinct", field: "region" },
          totalPrice: { aggFunc: "sum", field: "price" },
          averagePrice: { aggFunc: "avg", field: "price" },
          minUpdatedAt: { aggFunc: "min", field: "updatedAt" },
          maxUpdatedAt: { aggFunc: "max", field: "updatedAt" },
        },
        orderBy: [{ field: "status", direction: "asc" }],
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly rowCount: { readonly aggFunc: "count" };
          readonly distinctRegions: {
            readonly aggFunc: "countDistinct";
            readonly field: "region";
          };
          readonly totalPrice: { readonly aggFunc: "sum"; readonly field: "price" };
          readonly averagePrice: { readonly aggFunc: "avg"; readonly field: "price" };
          readonly minUpdatedAt: { readonly aggFunc: "min"; readonly field: "updatedAt" };
          readonly maxUpdatedAt: { readonly aggFunc: "max"; readonly field: "updatedAt" };
        };
        readonly orderBy: readonly [{ readonly field: "status"; readonly direction: "asc" }];
      };

      const subscription = yield* engine.subscribe("orders", query);
      const read = yield* makeEventReader(subscription);
      const snapshot = firstEvent(yield* read(1));
      expectSnapshotEvent(snapshot);
      let state = stateFromSnapshot(snapshot);
      expect(normalizeDecimalFields(state.rows)).toStrictEqual([
        {
          status: "closed",
          rowCount: 1n,
          distinctRegions: 1n,
          totalPrice: "5",
          averagePrice: "5",
          minUpdatedAt: 3,
          maxUpdatedAt: 3,
        },
        {
          status: "open",
          rowCount: 2n,
          distinctRegions: 2n,
          totalPrice: "30",
          averagePrice: "15",
          minUpdatedAt: 1,
          maxUpdatedAt: 2,
        },
      ]);

      yield* engine.patch("orders", "2", {
        status: "closed",
        price: 30,
        region: "emea",
        updatedAt: 4,
      });
      const movedDelta = firstEvent(yield* read(1));
      expectDeltaEvent(movedDelta);
      state = applyDelta(state, movedDelta);
      expect(normalizeDecimalFields(state.rows)).toStrictEqual([
        {
          status: "closed",
          rowCount: 2n,
          distinctRegions: 1n,
          totalPrice: "35",
          averagePrice: "17.5",
          minUpdatedAt: 3,
          maxUpdatedAt: 4,
        },
        {
          status: "open",
          rowCount: 1n,
          distinctRegions: 1n,
          totalPrice: "10",
          averagePrice: "10",
          minUpdatedAt: 1,
          maxUpdatedAt: 1,
        },
      ]);

      yield* engine.delete("orders", "1");
      const deleteDelta = firstEvent(yield* read(1));
      expectDeltaEvent(deleteDelta);
      state = applyDelta(state, deleteDelta);
      expect(normalizeDecimalFields(state.rows)).toStrictEqual([
        {
          status: "closed",
          rowCount: 2n,
          distinctRegions: 1n,
          totalPrice: "35",
          averagePrice: "17.5",
          minUpdatedAt: 3,
          maxUpdatedAt: 4,
        },
      ]);

      yield* subscription.close();
    }),
  );

  it.effect(
    "skips grouped deltas for non-aggregate patches and preserves next delta versions",
    () =>
      Effect.gen(function* () {
        const engine = yield* makeEngine();
        yield* engine.publishMany("orders", [
          { ...order("1", "open", 10, 1, "emea"), note: "before" },
          order("2", "open", 20, 2, "emea"),
        ]);
        const query = {
          groupBy: ["status"],
          aggregates: {
            maxPrice: { aggFunc: "max", field: "price" },
            rowCount: { aggFunc: "count" },
            totalPrice: { aggFunc: "sum", field: "price" },
          },
          orderBy: [{ field: "status", direction: "asc" }],
          limit: 1,
        } satisfies {
          readonly groupBy: readonly ["status"];
          readonly aggregates: {
            readonly maxPrice: { readonly aggFunc: "max"; readonly field: "price" };
            readonly rowCount: { readonly aggFunc: "count" };
            readonly totalPrice: { readonly aggFunc: "sum"; readonly field: "price" };
          };
          readonly orderBy: readonly [{ readonly field: "status"; readonly direction: "asc" }];
          readonly limit: 1;
        };

        const subscription = yield* engine.subscribe("orders", query);
        const read = yield* makeEventReader(subscription);
        const snapshot = firstEvent(yield* read(1));
        expectSnapshotEvent(snapshot);
        const groupKey = expectDefined(snapshot.keys[0]);
        expect(snapshot).toStrictEqual({
          type: "snapshot",
          topic: "orders",
          queryId: "query-0",
          version: 1,
          rows: [
            {
              status: "open",
              maxPrice: 20,
              rowCount: 2n,
              totalPrice: fromStringUnsafe("30"),
            },
          ],
          keys: [groupKey],
          totalRows: 1,
        });

        yield* engine.patch("orders", "1", { note: "after" });
        const afterNonAggregatePatchHealth = yield* engine.health();
        expect(afterNonAggregatePatchHealth.queuedEvents).toBe(0);
        expect(afterNonAggregatePatchHealth.topics["orders"].queuedEvents).toBe(0);

        yield* engine.patch("orders", "1", { price: 15 });
        const delta = firstEvent(yield* read(1));
        expectDeltaEvent(delta);
        expect(delta).toStrictEqual({
          type: "delta",
          topic: "orders",
          queryId: "query-0",
          fromVersion: 1,
          toVersion: 3,
          operations: [
            {
              type: "update",
              key: groupKey,
              row: {
                status: "open",
                maxPrice: 20,
                rowCount: 2n,
                totalPrice: fromStringUnsafe("35"),
              },
              index: 0,
            },
          ],
          totalRows: 1,
        });

        yield* subscription.close();
      }),
  );

  it.effect("keeps grouped aggregate state exact across duplicate removals", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("1", "open", 10, 1, "emea"),
        order("2", "open", 20, 6, "emea"),
        order("3", "open", 30, 5, "amer"),
      ]);
      const query = {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          distinctRegions: { aggFunc: "countDistinct", field: "region" },
          totalPrice: { aggFunc: "sum", field: "price" },
          averagePrice: { aggFunc: "avg", field: "price" },
          minUpdatedAt: { aggFunc: "min", field: "updatedAt" },
          maxUpdatedAt: { aggFunc: "max", field: "updatedAt" },
        },
        orderBy: [{ field: "status", direction: "asc" }],
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly rowCount: { readonly aggFunc: "count" };
          readonly distinctRegions: {
            readonly aggFunc: "countDistinct";
            readonly field: "region";
          };
          readonly totalPrice: { readonly aggFunc: "sum"; readonly field: "price" };
          readonly averagePrice: { readonly aggFunc: "avg"; readonly field: "price" };
          readonly minUpdatedAt: { readonly aggFunc: "min"; readonly field: "updatedAt" };
          readonly maxUpdatedAt: { readonly aggFunc: "max"; readonly field: "updatedAt" };
        };
        readonly orderBy: readonly [{ readonly field: "status"; readonly direction: "asc" }];
      };

      const subscription = yield* engine.subscribe("orders", query);
      const read = yield* makeEventReader(subscription);
      const snapshot = firstEvent(yield* read(1));
      expectSnapshotEvent(snapshot);
      let state = stateFromSnapshot(snapshot);
      expect(normalizeDecimalFields(state.rows)).toStrictEqual([
        {
          status: "open",
          rowCount: 3n,
          distinctRegions: 2n,
          totalPrice: "60",
          averagePrice: "20",
          minUpdatedAt: 1,
          maxUpdatedAt: 6,
        },
      ]);

      yield* engine.patch("orders", "2", {
        price: 50,
      });
      const patchedDelta = firstEvent(yield* read(1));
      expectDeltaEvent(patchedDelta);
      state = applyDelta(state, patchedDelta);
      expect(normalizeDecimalFields(state.rows)).toStrictEqual([
        {
          status: "open",
          rowCount: 3n,
          distinctRegions: 2n,
          totalPrice: "90",
          averagePrice: "30",
          minUpdatedAt: 1,
          maxUpdatedAt: 6,
        },
      ]);

      yield* engine.delete("orders", "1");
      const deletedDuplicateDelta = firstEvent(yield* read(1));
      expectDeltaEvent(deletedDuplicateDelta);
      state = applyDelta(state, deletedDuplicateDelta);
      expect(normalizeDecimalFields(state.rows)).toStrictEqual([
        {
          status: "open",
          rowCount: 2n,
          distinctRegions: 2n,
          totalPrice: "80",
          averagePrice: "40",
          minUpdatedAt: 5,
          maxUpdatedAt: 6,
        },
      ]);

      yield* engine.delete("orders", "3");
      const deletedDistinctDelta = firstEvent(yield* read(1));
      expectDeltaEvent(deletedDistinctDelta);
      state = applyDelta(state, deletedDistinctDelta);
      expect(normalizeDecimalFields(state.rows)).toStrictEqual([
        {
          status: "open",
          rowCount: 1n,
          distinctRegions: 1n,
          totalPrice: "50",
          averagePrice: "50",
          minUpdatedAt: 6,
          maxUpdatedAt: 6,
        },
      ]);

      yield* engine.patch("orders", "2", {
        status: "closed",
        price: 25,
        region: "amer",
        updatedAt: 2,
      });
      const movedDelta = firstEvent(yield* read(1));
      expectDeltaEvent(movedDelta);
      state = applyDelta(state, movedDelta);
      expect(normalizeDecimalFields(state.rows)).toStrictEqual([
        {
          status: "closed",
          rowCount: 1n,
          distinctRegions: 1n,
          totalPrice: "25",
          averagePrice: "25",
          minUpdatedAt: 2,
          maxUpdatedAt: 2,
        },
      ]);

      yield* subscription.close();
    }),
  );

  it.effect("keeps duplicate grouped min max values after deleting one duplicate", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("1", "open", 10, 6, "emea"),
        order("2", "open", 20, 6, "amer"),
        order("3", "open", 30, 1, "amer"),
        order("4", "open", 40, 3, "emea"),
        order("5", "open", 50, 2, "emea"),
      ]);
      const query = {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          minUpdatedAt: { aggFunc: "min", field: "updatedAt" },
          maxUpdatedAt: { aggFunc: "max", field: "updatedAt" },
        },
        orderBy: [{ field: "status", direction: "asc" }],
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly rowCount: { readonly aggFunc: "count" };
          readonly minUpdatedAt: { readonly aggFunc: "min"; readonly field: "updatedAt" };
          readonly maxUpdatedAt: { readonly aggFunc: "max"; readonly field: "updatedAt" };
        };
        readonly orderBy: readonly [{ readonly field: "status"; readonly direction: "asc" }];
      };

      const subscription = yield* engine.subscribe("orders", query);
      const read = yield* makeEventReader(subscription);
      let state = stateFromSnapshot(firstEvent(yield* read(1)));
      expect(state.rows).toStrictEqual([
        {
          status: "open",
          rowCount: 5n,
          minUpdatedAt: 1,
          maxUpdatedAt: 6,
        },
      ]);

      yield* engine.delete("orders", "1");
      const delta = firstEvent(yield* read(1));
      expectDeltaEvent(delta);
      state = applyDelta(state, delta);
      expect(state.rows).toStrictEqual([
        {
          status: "open",
          rowCount: 4n,
          minUpdatedAt: 1,
          maxUpdatedAt: 6,
        },
      ]);

      yield* engine.delete("orders", "2");
      const maxDelta = firstEvent(yield* read(1));
      expectDeltaEvent(maxDelta);
      state = applyDelta(state, maxDelta);
      expect(state.rows).toStrictEqual([
        {
          status: "open",
          rowCount: 3n,
          minUpdatedAt: 1,
          maxUpdatedAt: 3,
        },
      ]);

      yield* subscription.close();
    }),
  );

  it.effect("converges grouped subscriptions for duplicate-key publishMany batches", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("same", "open", 1, 1, "emea"));
      const query = {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          totalPrice: { aggFunc: "sum", field: "price" },
        },
        orderBy: [{ field: "status", direction: "asc" }],
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly rowCount: { readonly aggFunc: "count" };
          readonly totalPrice: { readonly aggFunc: "sum"; readonly field: "price" };
        };
        readonly orderBy: readonly [{ readonly field: "status"; readonly direction: "asc" }];
      };

      const subscription = yield* engine.subscribe("orders", query);
      const read = yield* makeEventReader(subscription);
      let state = stateFromSnapshot(firstEvent(yield* read(1)));

      yield* engine.publishMany("orders", [
        order("same", "closed", 2, 2, "emea"),
        order("same", "open", 3, 3, "emea"),
        order("same", "closed", 4, 4, "emea"),
        order("fresh", "open", 5, 5, "amer"),
        order("fresh", "closed", 6, 6, "amer"),
        order("other", "open", 10, 10, "amer"),
      ]);

      const delta = firstEvent(yield* read(1));
      expectDeltaEvent(delta);
      state = applyDelta(state, delta);
      expect(normalizeDecimalFields(state.rows)).toStrictEqual([
        { status: "closed", rowCount: 2n, totalPrice: "10" },
        { status: "open", rowCount: 1n, totalPrice: "10" },
      ]);

      yield* subscription.close();
    }),
  );
});
