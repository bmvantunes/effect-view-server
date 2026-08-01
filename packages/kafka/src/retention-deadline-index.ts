export type KafkaRetentionDeadline = {
  readonly id: string;
  readonly region: string;
  readonly deadlineNanos: bigint;
  readonly generation: bigint;
};

type KafkaRetentionDeadlineNode = {
  readonly deadline: KafkaRetentionDeadline;
  readonly height: number;
  readonly left: KafkaRetentionDeadlineNode | null;
  readonly right: KafkaRetentionDeadlineNode | null;
  readonly size: number;
};

const nodeHeight = (node: KafkaRetentionDeadlineNode | null): number => node?.height ?? 0;

const nodeSize = (node: KafkaRetentionDeadlineNode | null): number => node?.size ?? 0;

const makeNode = (
  deadline: KafkaRetentionDeadline,
  left: KafkaRetentionDeadlineNode | null = null,
  right: KafkaRetentionDeadlineNode | null = null,
): KafkaRetentionDeadlineNode =>
  Object.freeze({
    deadline,
    height: Math.max(nodeHeight(left), nodeHeight(right)) + 1,
    left,
    right,
    size: nodeSize(left) + nodeSize(right) + 1,
  });

const compareDeadline = (left: KafkaRetentionDeadline, right: KafkaRetentionDeadline): number =>
  left.deadlineNanos === right.deadlineNanos
    ? left.id === right.id
      ? 0
      : left.id < right.id
        ? -1
        : 1
    : left.deadlineNanos < right.deadlineNanos
      ? -1
      : 1;

const rotateRight = (
  node: KafkaRetentionDeadlineNode,
  left: KafkaRetentionDeadlineNode,
): KafkaRetentionDeadlineNode =>
  makeNode(left.deadline, left.left, makeNode(node.deadline, left.right, node.right));

const rotateLeft = (
  node: KafkaRetentionDeadlineNode,
  right: KafkaRetentionDeadlineNode,
): KafkaRetentionDeadlineNode =>
  makeNode(right.deadline, makeNode(node.deadline, node.left, right.left), right.right);

const rebalance = (node: KafkaRetentionDeadlineNode): KafkaRetentionDeadlineNode => {
  if (node.left !== null && nodeHeight(node.left) - nodeHeight(node.right) > 1) {
    const left =
      node.left.right !== null && nodeHeight(node.left.right) > nodeHeight(node.left.left)
        ? rotateLeft(node.left, node.left.right)
        : node.left;
    return rotateRight(makeNode(node.deadline, left, node.right), left);
  }
  if (node.right !== null && nodeHeight(node.right) - nodeHeight(node.left) > 1) {
    const right =
      node.right.left !== null && nodeHeight(node.right.left) > nodeHeight(node.right.right)
        ? rotateRight(node.right, node.right.left)
        : node.right;
    return rotateLeft(makeNode(node.deadline, node.left, right), right);
  }
  return node;
};

const insertDeadline = (
  node: KafkaRetentionDeadlineNode | null,
  deadline: KafkaRetentionDeadline,
): KafkaRetentionDeadlineNode => {
  if (node === null) {
    return makeNode(deadline);
  }
  const ordering = compareDeadline(deadline, node.deadline);
  if (ordering < 0) {
    return rebalance(makeNode(node.deadline, insertDeadline(node.left, deadline), node.right));
  }
  if (ordering > 0) {
    return rebalance(makeNode(node.deadline, node.left, insertDeadline(node.right, deadline)));
  }
  return makeNode(deadline, node.left, node.right);
};

const firstDeadline = (node: KafkaRetentionDeadlineNode): KafkaRetentionDeadline =>
  node.left === null ? node.deadline : firstDeadline(node.left);

const removeDeadline = (
  node: KafkaRetentionDeadlineNode | null,
  deadline: KafkaRetentionDeadline,
): KafkaRetentionDeadlineNode | null => {
  if (node === null) {
    return null;
  }
  const ordering = compareDeadline(deadline, node.deadline);
  if (ordering < 0) {
    return rebalance(makeNode(node.deadline, removeDeadline(node.left, deadline), node.right));
  }
  if (ordering > 0) {
    return rebalance(makeNode(node.deadline, node.left, removeDeadline(node.right, deadline)));
  }
  if (node.left === null) {
    return node.right;
  }
  if (node.right === null) {
    return node.left;
  }
  const successor = firstDeadline(node.right);
  return rebalance(makeNode(successor, node.left, removeDeadline(node.right, successor)));
};

function* orderedDeadlines(
  node: KafkaRetentionDeadlineNode | null,
): IterableIterator<KafkaRetentionDeadline> {
  const stack: Array<KafkaRetentionDeadlineNode> = [];
  let current = node;
  while (true) {
    while (current !== null) {
      stack.push(current);
      current = current.left;
    }
    const next = stack.pop();
    if (next === undefined) {
      return;
    }
    yield next.deadline;
    current = next.right;
  }
}

export class KafkaRetentionDeadlineIndex implements Iterable<KafkaRetentionDeadline> {
  readonly #root: KafkaRetentionDeadlineNode | null;

  constructor(root: KafkaRetentionDeadlineNode | null = null) {
    this.#root = root;
    Object.freeze(this);
  }

  get size(): number {
    return nodeSize(this.#root);
  }

  set(deadline: KafkaRetentionDeadline): KafkaRetentionDeadlineIndex {
    return new KafkaRetentionDeadlineIndex(insertDeadline(this.#root, deadline));
  }

  remove(deadline: KafkaRetentionDeadline): KafkaRetentionDeadlineIndex {
    return new KafkaRetentionDeadlineIndex(removeDeadline(this.#root, deadline));
  }

  values(): IterableIterator<KafkaRetentionDeadline> {
    return orderedDeadlines(this.#root);
  }

  [Symbol.iterator](): IterableIterator<KafkaRetentionDeadline> {
    return this.values();
  }
}
