type KeyBranch<Value> = {
  readonly offset: number;
  readonly shift: number;
  readonly children: Map<number, KeyNode<Value>>;
};

type KeyNode<Value> = {
  readonly entries: Map<string, Value>;
  branch: KeyBranch<Value> | undefined;
};

const keyNode = <Value>(): KeyNode<Value> => ({ entries: new Map(), branch: undefined });

// Native Map has an implementation-defined entry ceiling. Leaves split long
// before it, and branch maps have at most 256 character-byte buckets.
const leafCapacity = 65_536;

/** An unordered string-key index. Values and keys retain native Map identity. */
export class StringKeyIndex<Value> {
  private root = keyNode<Value>();
  private sizeValue = 0;

  get size(): number {
    return this.sizeValue;
  }

  get(key: string): Value | undefined {
    return this.findLeaf(key)?.entries.get(key);
  }

  has(key: string): boolean {
    return this.findLeaf(key)?.entries.has(key) ?? false;
  }

  set(key: string, value: Value): void {
    let node = this.root;
    while (node.branch !== undefined) {
      const branch = node.branch;
      const symbol = ((key.charCodeAt(branch.offset) + 1) >>> branch.shift) & 255;
      let child = branch.children.get(symbol);
      if (child === undefined) {
        child = keyNode();
        branch.children.set(symbol, child);
      }
      node = child;
    }
    const previousSize = node.entries.size;
    node.entries.set(key, value);
    this.sizeValue += node.entries.size - previousSize;
    if (node.entries.size > leafCapacity) this.split(node);
  }

  delete(key: string): boolean {
    const leaf = this.findLeaf(key);
    if (leaf?.entries.delete(key) !== true) return false;
    this.sizeValue -= 1;
    if (this.sizeValue === 0) this.root = keyNode();
    else if (leaf.entries.size === 0) this.removeEmptyLeaf(key);
    return true;
  }

  clear(): void {
    this.root = keyNode();
    this.sizeValue = 0;
  }

  private findLeaf(key: string): KeyNode<Value> | undefined {
    let node = this.root;
    while (node.branch !== undefined) {
      const next = node.branch.children.get(
        ((key.charCodeAt(node.branch.offset) + 1) >>> node.branch.shift) & 255,
      );
      if (next === undefined) return undefined;
      node = next;
    }
    return node;
  }

  private removeEmptyLeaf(key: string): void {
    // Only an emptied leaf retraverses the path. Remove its entire chain of
    // single-child ancestors without allocating a stack on ordinary deletes.
    let node = this.root;
    let retainedBranch = this.root.branch!;
    let retainedSymbol = 0;
    while (node.branch !== undefined) {
      const branch = node.branch;
      const symbol = ((key.charCodeAt(branch.offset) + 1) >>> branch.shift) & 255;
      if (branch.children.size > 1) {
        retainedBranch = branch;
        retainedSymbol = symbol;
      }
      node = branch.children.get(symbol)!;
    }
    // Another live entry remains, so at least one ancestor has another child.
    retainedBranch.children.delete(retainedSymbol);
  }

  private split(node: KeyNode<Value>): void {
    // Find the first differing character. Unlike a fixed hash partition this
    // also bounds leaf size for adversarial keys with identical hash values.
    const first = node.entries.keys().next().value!;
    let offset = first.length;
    for (const key of node.entries.keys()) {
      let shared = 0;
      while (shared < offset && first[shared] === key[shared]) shared += 1;
      offset = shared;
      if (offset === 0) break;
    }
    const firstSymbol = (first.charCodeAt(offset) + 1) >>> 0;
    let differences = 0;
    for (const key of node.entries.keys())
      differences |= firstSymbol ^ ((key.charCodeAt(offset) + 1) >>> 0);
    const shift = differences >>> 16 !== 0 ? 16 : differences >>> 8 !== 0 ? 8 : 0;
    const children = new Map<number, KeyNode<Value>>();
    for (const [key, value] of node.entries) {
      // Zero denotes end-of-key; UTF-16 code units use 1..65,536.
      const symbol = ((key.charCodeAt(offset) + 1) >>> shift) & 255;
      let child = children.get(symbol);
      if (child === undefined) {
        child = keyNode();
        children.set(symbol, child);
      }
      child.entries.set(key, value);
    }
    node.entries.clear();
    node.branch = { offset, shift, children };
  }
}
