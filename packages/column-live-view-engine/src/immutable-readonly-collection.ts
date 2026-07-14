export const immutableReadonlySet = <Value>(values: Iterable<Value>): ReadonlySet<Value> => {
  const backing = new Set(values);
  let immutable: ReadonlySet<Value>;
  immutable = Object.freeze({
    get size(): number {
      return backing.size;
    },
    has: (value: Value): boolean => backing.has(value),
    union: <Other>(other: ReadonlySetLike<Other>): Set<Value | Other> => backing.union(other),
    intersection: <Other>(other: ReadonlySetLike<Other>): Set<Value & Other> =>
      backing.intersection(other),
    difference: <Other>(other: ReadonlySetLike<Other>): Set<Value> => backing.difference(other),
    symmetricDifference: <Other>(other: ReadonlySetLike<Other>): Set<Value | Other> =>
      backing.symmetricDifference(other),
    isSubsetOf: (other: ReadonlySetLike<unknown>): boolean => backing.isSubsetOf(other),
    isSupersetOf: (other: ReadonlySetLike<unknown>): boolean => backing.isSupersetOf(other),
    isDisjointFrom: (other: ReadonlySetLike<unknown>): boolean => backing.isDisjointFrom(other),
    entries: (): SetIterator<[Value, Value]> => backing.entries(),
    keys: (): SetIterator<Value> => backing.keys(),
    values: (): SetIterator<Value> => backing.values(),
    forEach: (
      callback: (value: Value, key: Value, set: ReadonlySet<Value>) => void,
      thisArg?: unknown,
    ): void => {
      for (const value of backing) {
        callback.call(thisArg, value, value, immutable);
      }
    },
    [Symbol.iterator]: (): SetIterator<Value> => backing[Symbol.iterator](),
  } satisfies ReadonlySet<Value>);
  return immutable;
};

export const immutableReadonlyMap = <Key, Value>(
  entries: Iterable<readonly [Key, Value]>,
): ReadonlyMap<Key, Value> => {
  const backing = new Map(entries);
  const immutable = Object.freeze({
    get size(): number {
      return backing.size;
    },
    get: (key: Key): Value | undefined => backing.get(key),
    has: (key: Key): boolean => backing.has(key),
    entries: (): MapIterator<[Key, Value]> => backing.entries(),
    keys: (): MapIterator<Key> => backing.keys(),
    values: (): MapIterator<Value> => backing.values(),
    forEach: (
      callback: (value: Value, key: Key, map: ReadonlyMap<Key, Value>) => void,
      thisArg?: unknown,
    ): void => {
      for (const [key, value] of backing) {
        callback.call(thisArg, value, key, immutable);
      }
    },
    [Symbol.iterator]: (): MapIterator<[Key, Value]> => backing[Symbol.iterator](),
  } satisfies ReadonlyMap<Key, Value>);
  return immutable;
};
