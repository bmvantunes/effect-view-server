import { describe, expect, it } from "@effect/vitest";
import {
  collectCanonicalFilterGraphLeaves,
  compareCanonicalFilterGraphs,
  complementCanonicalFilterType,
  type CanonicalFilterGraphStep,
} from "./canonical-filter-expression";

type GraphNode =
  | {
      readonly _tag: "group";
      readonly children: ReadonlyArray<GraphNode | undefined>;
    }
  | {
      readonly _tag: "leaf";
      readonly identity: string;
    };

describe("canonical filter expression helpers", () => {
  it("expands shared graph nodes once and deduplicates structurally equal leaves", () => {
    const first = { _tag: "leaf", identity: "first" } satisfies GraphNode;
    const duplicateFirst = { _tag: "leaf", identity: "first" } satisfies GraphNode;
    const second = { _tag: "leaf", identity: "second" } satisfies GraphNode;
    const shared = {
      _tag: "group",
      children: [first, undefined, duplicateFirst, second],
    } satisfies GraphNode;
    let visits = 0;
    const leaves = collectCanonicalFilterGraphLeaves<
      GraphNode,
      Extract<GraphNode, { _tag: "leaf" }>,
      string
    >(
      [shared, undefined, shared],
      (node): CanonicalFilterGraphStep<GraphNode, Extract<GraphNode, { _tag: "leaf" }>> => {
        visits += 1;
        return node._tag === "group"
          ? { _tag: "expand", children: node.children }
          : { _tag: "leaf", leaf: node };
      },
      (leaf) => leaf.identity,
    );

    expect(leaves).toStrictEqual([first, second]);
    expect(visits).toBe(4);
  });

  it("maps only condition types with canonical complements", () => {
    expect(
      ["equals", "notEqual", "contains", "notContains", "blank", "notBlank", "in"].map(
        complementCanonicalFilterType,
      ),
    ).toStrictEqual([
      "notEqual",
      "equals",
      "notContains",
      "contains",
      "notBlank",
      "blank",
      undefined,
    ]);
  });

  it("compares shared canonical graphs without revisiting node pairs", () => {
    const leftLeaf = { _tag: "leaf", identity: "a" } satisfies GraphNode;
    const rightLeaf = { _tag: "leaf", identity: "b" } satisfies GraphNode;
    const left = { _tag: "group", children: [leftLeaf, leftLeaf] } satisfies GraphNode;
    const empty = { _tag: "group", children: [] } satisfies GraphNode;
    const oneChild = { _tag: "group", children: [leftLeaf] } satisfies GraphNode;
    const equivalentLeft = {
      _tag: "group",
      children: [leftLeaf, leftLeaf],
    } satisfies GraphNode;
    const splitEquivalent = {
      _tag: "group",
      children: [
        { _tag: "leaf", identity: "a" },
        { _tag: "leaf", identity: "a" },
      ],
    } satisfies GraphNode;
    const right = { _tag: "group", children: [rightLeaf, rightLeaf] } satisfies GraphNode;
    const leftCycleChildren: Array<GraphNode> = [];
    const rightCycleChildren: Array<GraphNode> = [];
    const leftCycle = { _tag: "group", children: leftCycleChildren } satisfies GraphNode;
    const rightCycle = { _tag: "group", children: rightCycleChildren } satisfies GraphNode;
    leftCycleChildren.push(leftCycle);
    rightCycleChildren.push(rightCycle);
    const describe = (node: GraphNode) => {
      if (node._tag === "leaf") {
        return { tag: "leaf", value: node.identity, children: [] };
      }
      const children = node.children.filter((child) => child !== undefined);
      return { tag: "group", value: String(children.length), children };
    };
    const describeWithoutArity = (node: GraphNode) => {
      if (node._tag === "leaf") {
        return { tag: "leaf", value: node.identity, children: [] };
      }
      return {
        tag: "group",
        value: "",
        children: node.children.filter((child) => child !== undefined),
      };
    };

    expect(compareCanonicalFilterGraphs(left, left, describe)).toBe(0);
    expect(compareCanonicalFilterGraphs(left, equivalentLeft, describe)).toBe(0);
    expect(compareCanonicalFilterGraphs(left, splitEquivalent, describe)).toBe(0);
    expect(compareCanonicalFilterGraphs(leftCycle, rightCycle, describe)).toBe(0);
    expect(compareCanonicalFilterGraphs(left, right, describe)).toBeLessThan(0);
    expect(compareCanonicalFilterGraphs(leftLeaf, left, describe)).toBeGreaterThan(0);
    expect(compareCanonicalFilterGraphs(empty, oneChild, describeWithoutArity)).toBeLessThan(0);
    expect(compareCanonicalFilterGraphs(oneChild, empty, describeWithoutArity)).toBeGreaterThan(0);
  });
});
