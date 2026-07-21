export type CanonicalFilterGraphStep<Node, Leaf> =
  | {
      readonly _tag: "expand";
      readonly children: ReadonlyArray<Node | undefined>;
    }
  | {
      readonly _tag: "leaf";
      readonly leaf: Leaf;
    };

export const collectCanonicalFilterGraphLeaves = <Node extends object, Leaf, Identity>(
  roots: ReadonlyArray<Node | undefined>,
  visit: (node: Node) => CanonicalFilterGraphStep<Node, Leaf>,
  identityFor: (leaf: Leaf) => Identity,
): ReadonlyArray<Leaf> => {
  const pending: Array<Node> = [];
  for (let index = roots.length - 1; index >= 0; index -= 1) {
    const root = roots[index];
    if (root !== undefined) {
      pending.push(root);
    }
  }
  const visited = new WeakSet<Node>();
  const seenLeaves = new Set<Identity>();
  const leaves: Array<Leaf> = [];
  while (pending.length > 0) {
    const node = pending.pop()!;
    if (visited.has(node)) {
      continue;
    }
    visited.add(node);
    const step = visit(node);
    if (step._tag === "expand") {
      for (let index = step.children.length - 1; index >= 0; index -= 1) {
        const child = step.children[index];
        if (child !== undefined) {
          pending.push(child);
        }
      }
      continue;
    }
    const identity = identityFor(step.leaf);
    if (!seenLeaves.has(identity)) {
      seenLeaves.add(identity);
      leaves.push(step.leaf);
    }
  }
  return leaves;
};

export type CanonicalFilterComparisonNode<Node> = {
  readonly tag: string;
  readonly value: string;
  readonly children: ReadonlyArray<Node>;
};

const compareCodeUnits = (left: string, right: string): number =>
  Number(left > right) - Number(left < right);

export const compareCanonicalFilterGraphs = <Node extends object>(
  left: Node,
  right: Node,
  describe: (node: Node) => CanonicalFilterComparisonNode<Node>,
): number => {
  const frames: Array<readonly [Node, Node]> = [[left, right]];
  const compared = new WeakMap<Node, WeakSet<Node>>();
  while (frames.length > 0) {
    const [leftNode, rightNode] = frames.pop()!;
    if (leftNode === rightNode) {
      continue;
    }
    const existing = compared.get(leftNode);
    if (existing?.has(rightNode) === true) {
      continue;
    }
    if (existing === undefined) {
      compared.set(leftNode, new WeakSet([rightNode]));
    } else {
      existing.add(rightNode);
    }
    const leftDescription = describe(leftNode);
    const rightDescription = describe(rightNode);
    const tagComparison = compareCodeUnits(leftDescription.tag, rightDescription.tag);
    if (tagComparison !== 0) {
      return tagComparison;
    }
    const valueComparison = compareCodeUnits(leftDescription.value, rightDescription.value);
    if (valueComparison !== 0) {
      return valueComparison;
    }
    const childCountComparison = leftDescription.children.length - rightDescription.children.length;
    if (childCountComparison !== 0) {
      return childCountComparison;
    }
    for (let index = leftDescription.children.length - 1; index >= 0; index -= 1) {
      frames.push([leftDescription.children[index]!, rightDescription.children[index]!]);
    }
  }
  return 0;
};

export type CanonicalComplementableFilterType =
  | "equals"
  | "notEqual"
  | "contains"
  | "notContains"
  | "blank"
  | "notBlank";

export const complementCanonicalFilterType = (
  type: string,
): CanonicalComplementableFilterType | undefined => {
  switch (type) {
    case "equals":
      return "notEqual";
    case "notEqual":
      return "equals";
    case "contains":
      return "notContains";
    case "notContains":
      return "contains";
    case "blank":
      return "notBlank";
    case "notBlank":
      return "blank";
    default:
      return undefined;
  }
};
