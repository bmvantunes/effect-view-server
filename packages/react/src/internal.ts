export const ViewServerReactConfig: unique symbol = Symbol("ViewServerReact.config");
export const ViewServerReactClientProvider: unique symbol = Symbol(
  "ViewServerReact.clientProvider",
);

export function deleteMapEntryIfCurrent<K, V>(entries: Map<K, V>, key: K, entry: V): void {
  if (entries.get(key) === entry) {
    entries.delete(key);
  }
}

export function installMapEntryIfVacant<K, V>(entries: Map<K, V>, key: K, entry: V): void {
  if (!entries.has(key)) {
    entries.set(key, entry);
  }
}
