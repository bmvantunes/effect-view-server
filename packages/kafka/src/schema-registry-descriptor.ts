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

const messageAtIndexes = (
  root: DescFile,
  indexes: readonly [number, ...ReadonlyArray<number>],
  skipMapEntries: boolean,
): DescMessage | undefined => {
  let messages = root.proto.messageType;
  let parentTypeName = root.proto.package;
  for (const index of indexes) {
    const message = skipMapEntries
      ? messages.filter((candidate) => candidate.options?.mapEntry !== true)[index]
      : messages[index];
    if (message === undefined) {
      return undefined;
    }
    parentTypeName =
      parentTypeName.length === 0 ? message.name : `${parentTypeName}.${message.name}`;
    messages = message.nestedType;
  }
  const find = (candidates: ReadonlyArray<DescMessage>): DescMessage | undefined => {
    for (const candidate of candidates) {
      if (candidate.typeName === parentTypeName) {
        return candidate;
      }
      const nested = find(candidate.nestedMessages);
      if (nested !== undefined) {
        return nested;
      }
    }
    return undefined;
  };
  return find(root.messages);
};

export const kafkaProtobufMessageAtIndexes = (
  root: DescFile,
  indexes: readonly [number, ...ReadonlyArray<number>],
): DescMessage | undefined => messageAtIndexes(root, indexes, false);

export const kafkaProtobufMessageAtNormalizedIndexes = (
  root: DescFile,
  indexes: readonly [number, ...ReadonlyArray<number>],
): DescMessage | undefined => messageAtIndexes(root, indexes, true);
