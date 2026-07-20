// Import Vitest directly so @effect/vitest's eager test-runtime module graph does not
// distort the heap, JIT, and GC behavior this benchmark is measuring.
import { beforeAll, bench, describe, expect } from "vitest";
import { Effect, Schema } from "effect";
import { compileRawPredicate, type CompiledRawPredicate } from "./raw-predicate-compiler";
import { decodeRawQuery } from "./raw-query-decoder";
import { rawQueryCompilerMetadata } from "./raw-query-metadata";

const Row = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  region: Schema.String,
});

type RowValue = typeof Row.Type;

const candidateCount = 50_000;
const partitionCount = 25;
const rowsPerPartition = 4_000;
const candidates = Array.from({ length: candidateCount }, (_value, index) => `customer-${index}`);
const partitions = Array.from({ length: partitionCount }, (_value, partition) =>
  Array.from({ length: rowsPerPartition }, (_entry, offset): RowValue => {
    const index = partition * rowsPerPartition + offset;
    return {
      id: `order-${index}`,
      customerId: `customer-${index}`,
      region: "emea",
    };
  }),
);

let compiled: CompiledRawPredicate<RowValue> | undefined;

const compiledPredicate = (): CompiledRawPredicate<RowValue> => {
  if (compiled === undefined) {
    throw new Error("Large membership benchmark is not initialized.");
  }
  return compiled;
};

beforeAll(() => {
  const metadata = rawQueryCompilerMetadata(Row);
  const query = Effect.runSync(
    decodeRawQuery("rows", metadata, {
      select: ["id"],
      where: [
        {
          type: "OR",
          conditions: [
            { field: "customerId", type: "in", filter: candidates },
            { field: "region", type: "equals", filter: "unmatched" },
          ],
        },
      ],
    }),
  );
  compiled = compileRawPredicate<RowValue>(metadata, query.where, { trustedRows: true });
  expect(compiled.plan.callbackRequired).toBe(true);
});

describe("raw nested membership callback benchmark (localhost CPU/GC partition stress)", () => {
  bench("50k candidates across 100k partitioned rows", () => {
    const predicate = compiledPredicate();
    let matches = 0;
    for (const partition of partitions) {
      for (const row of partition) {
        if (predicate.matches(row)) {
          matches += 1;
        }
      }
    }
    expect(matches).toBe(candidateCount);
  });
});
