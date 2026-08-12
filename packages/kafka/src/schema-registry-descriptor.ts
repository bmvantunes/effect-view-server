import type { DescFile, DescMessage } from "@bufbuild/protobuf";

const nestedMessageIndexes = (
  messages: DescFile["proto"]["messageType"],
  parentTypeName: string,
  typeName: string,
  skipMapEntries: boolean,
): readonly [number, ...ReadonlyArray<number>] | undefined => {
  let messageIndex = 0;
  for (const message of messages) {
    if (skipMapEntries && message.options?.mapEntry === true) {
      continue;
    }
    const candidateTypeName =
      parentTypeName.length === 0 ? message.name : `${parentTypeName}.${message.name}`;
    if (candidateTypeName === typeName) {
      return [messageIndex];
    }
    const nested = nestedMessageIndexes(
      message.nestedType,
      candidateTypeName,
      typeName,
      skipMapEntries,
    );
    if (nested !== undefined) {
      return [messageIndex, ...nested];
    }
    messageIndex += 1;
  }
  return undefined;
};

const messageIndexes = (
  root: DescFile,
  typeName: string,
  skipMapEntries: boolean,
): readonly [number, ...ReadonlyArray<number>] | undefined =>
  nestedMessageIndexes(root.proto.messageType, root.proto.package, typeName, skipMapEntries);

export const kafkaProtobufMessageIndexes = (
  root: DescFile,
  typeName: string,
): readonly [number, ...ReadonlyArray<number>] | undefined => messageIndexes(root, typeName, false);

export const kafkaProtobufNormalizedMessageIndexes = (
  root: DescFile,
  typeName: string,
): readonly [number, ...ReadonlyArray<number>] | undefined => messageIndexes(root, typeName, true);

type IndexedMessages = {
  readonly raw: ReadonlyMap<string, DescMessage>;
  readonly normalized: ReadonlyMap<string, DescMessage>;
};

const indexedMessagesByFile = new WeakMap<DescFile, IndexedMessages>();

const indexKey = (indexes: ReadonlyArray<number>): string => indexes.join(".");

const messagesByTypeName = (root: DescFile): ReadonlyMap<string, DescMessage> => {
  const messages = new Map<string, DescMessage>();
  const visit = (candidates: ReadonlyArray<DescMessage>): void => {
    for (const candidate of candidates) {
      messages.set(candidate.typeName, candidate);
      visit(candidate.nestedMessages);
    }
  };
  visit(root.messages);
  return messages;
};

const indexedMessages = (root: DescFile): IndexedMessages => {
  const cached = indexedMessagesByFile.get(root);
  if (cached !== undefined) {
    return cached;
  }
  const descriptors = messagesByTypeName(root);
  const build = (skipMapEntries: boolean): ReadonlyMap<string, DescMessage> => {
    const indexed = new Map<string, DescMessage>();
    const visit = (
      messages: DescFile["proto"]["messageType"],
      parentTypeName: string,
      parentIndexes: ReadonlyArray<number>,
    ): void => {
      const candidates = skipMapEntries
        ? messages.filter((candidate) => candidate.options?.mapEntry !== true)
        : messages;
      for (const [index, candidate] of candidates.entries()) {
        const typeName =
          parentTypeName.length === 0 ? candidate.name : `${parentTypeName}.${candidate.name}`;
        const indexes = [...parentIndexes, index];
        const descriptor = descriptors.get(typeName);
        if (descriptor !== undefined) {
          indexed.set(indexKey(indexes), descriptor);
        }
        visit(candidate.nestedType, typeName, indexes);
      }
    };
    visit(root.proto.messageType, root.proto.package, []);
    return indexed;
  };
  const result: IndexedMessages = Object.freeze({
    raw: build(false),
    normalized: build(true),
  });
  indexedMessagesByFile.set(root, result);
  return result;
};

const messageAtIndexes = (
  root: DescFile,
  indexes: readonly [number, ...ReadonlyArray<number>],
  skipMapEntries: boolean,
): DescMessage | undefined =>
  (skipMapEntries ? indexedMessages(root).normalized : indexedMessages(root).raw).get(
    indexKey(indexes),
  );

export const kafkaProtobufMessageAtIndexes = (
  root: DescFile,
  indexes: readonly [number, ...ReadonlyArray<number>],
): DescMessage | undefined => messageAtIndexes(root, indexes, false);

export const kafkaProtobufMessageAtNormalizedIndexes = (
  root: DescFile,
  indexes: readonly [number, ...ReadonlyArray<number>],
): DescMessage | undefined => messageAtIndexes(root, indexes, true);
