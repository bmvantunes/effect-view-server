import {
  clone,
  create,
  createFileRegistry,
  ScalarType,
  type DescEnum,
  type DescExtension,
  type DescField,
  type DescFile,
  type DescMessage,
  type DescMethod,
  type DescService,
} from "@bufbuild/protobuf";
import {
  FieldDescriptorProto_Label,
  FileDescriptorProtoSchema,
  FileDescriptorSetSchema,
} from "@bufbuild/protobuf/wkt";
import type { DescriptorProto } from "@bufbuild/protobuf/wkt";
import { Option } from "effect";

export type KafkaProtobufWireRule =
  | "ENUM_VALUE_NO_DELETE_UNLESS_NUMBER_RESERVED"
  | "FIELD_NO_DELETE_UNLESS_NUMBER_RESERVED"
  | "FIELD_SAME_DEFAULT"
  | "FIELD_SAME_ONEOF"
  | "FIELD_WIRE_COMPATIBLE_CARDINALITY"
  | "FIELD_WIRE_COMPATIBLE_TYPE"
  | "FILE_SAME_PACKAGE"
  | "MESSAGE_SAME_REQUIRED_FIELDS"
  | "RESERVED_ENUM_NO_DELETE"
  | "RESERVED_MESSAGE_NO_DELETE"
  | "RPC_SAME_CLIENT_STREAMING"
  | "RPC_SAME_IDEMPOTENCY_LEVEL"
  | "RPC_SAME_REQUEST_TYPE"
  | "RPC_SAME_RESPONSE_TYPE"
  | "RPC_SAME_SERVER_STREAMING";

export type KafkaProtobufWireIssue = {
  readonly rule: KafkaProtobufWireRule;
  readonly path: string;
  readonly message: string;
};

type NumericRange = {
  readonly start: number;
  readonly endExclusive: number;
};

const issue = (
  rule: KafkaProtobufWireRule,
  path: string,
  message: string,
): KafkaProtobufWireIssue => ({ rule, path, message });

const descriptorFileName = (file: DescFile): string => file.proto.name;

type NormalizedDescriptorGraph = {
  readonly messages: ReadonlyMap<string, DescMessage>;
};

const normalizedDescriptorGraphs = new WeakMap<DescFile, NormalizedDescriptorGraph>();

const exposeMapEntryMessages = (messages: ReadonlyArray<DescriptorProto>): void => {
  for (const message of messages) {
    if (message.options?.mapEntry === true) {
      message.options.mapEntry = false;
    }
    exposeMapEntryMessages(message.nestedType);
  }
};

const normalizedDescriptorGraph = (root: DescFile): NormalizedDescriptorGraph => {
  const cached = normalizedDescriptorGraphs.get(root);
  if (cached !== undefined) {
    return cached;
  }
  const files = [...descriptorGraph(root).values()].reverse().map((file) => {
    const proto = clone(FileDescriptorProtoSchema, file.proto);
    exposeMapEntryMessages(proto.messageType);
    return proto;
  });
  const registry = createFileRegistry(create(FileDescriptorSetSchema, { file: files }));
  const normalized = Object.freeze({
    messages: messagesByTypeName(registry.files),
  });
  normalizedDescriptorGraphs.set(root, normalized);
  return normalized;
};

const descriptorGraph = (root: DescFile): ReadonlyMap<string, DescFile> => {
  const files = new Map<string, DescFile>();
  const visit = (file: DescFile): void => {
    const name = descriptorFileName(file);
    if (files.has(name)) {
      return;
    }
    files.set(name, file);
    for (const dependency of file.dependencies) {
      visit(dependency);
    }
  };
  visit(root);
  return files;
};

const visitMessage = (message: DescMessage, visit: (message: DescMessage) => void): void => {
  visit(message);
  for (const nested of message.nestedMessages) {
    visitMessage(nested, visit);
  }
};

const messagesByTypeName = (files: Iterable<DescFile>): ReadonlyMap<string, DescMessage> => {
  const messages = new Map<string, DescMessage>();
  for (const file of files) {
    for (const message of file.messages) {
      visitMessage(message, (candidate) => messages.set(candidate.typeName, candidate));
    }
  }
  return messages;
};

const enumsByTypeName = (files: Iterable<DescFile>): ReadonlyMap<string, DescEnum> => {
  const enums = new Map<string, DescEnum>();
  for (const file of files) {
    for (const enumeration of file.enums) {
      enums.set(enumeration.typeName, enumeration);
    }
    for (const message of file.messages) {
      visitMessage(message, (candidate) => {
        for (const enumeration of candidate.nestedEnums) {
          enums.set(enumeration.typeName, enumeration);
        }
      });
    }
  }
  return enums;
};

const messageRanges = (message: DescMessage): ReadonlyArray<NumericRange> =>
  message.proto.reservedRange.map((range) => ({
    start: range.start,
    endExclusive: range.end,
  }));

const enumRanges = (enumeration: DescEnum): ReadonlyArray<NumericRange> =>
  enumeration.proto.reservedRange.map((range) => ({
    start: range.start,
    endExclusive: range.end + 1,
  }));

const rangeContains = (ranges: ReadonlyArray<NumericRange>, number: number): boolean =>
  ranges.some((range) => range.start <= number && number < range.endExclusive);

const rangeCovered = (expected: NumericRange, candidates: ReadonlyArray<NumericRange>): boolean => {
  const ordered = [...candidates].sort((left, right) => left.start - right.start);
  let coveredUntil = expected.start;
  for (const candidate of ordered) {
    if (candidate.endExclusive <= coveredUntil) {
      continue;
    }
    if (candidate.start > coveredUntil) {
      return false;
    }
    coveredUntil = candidate.endExclusive;
    if (coveredUntil >= expected.endExclusive) {
      return true;
    }
  }
  return false;
};

const fieldCardinality = (
  field: DescField | DescExtension,
): "optional" | "repeated" | "required" => {
  if (field.proto.label === FieldDescriptorProto_Label.REQUIRED) {
    return "required";
  }
  return field.fieldKind === "list" || field.fieldKind === "map" ? "repeated" : "optional";
};

const scalarFamily = (scalar: ScalarType): string => {
  if (
    scalar === ScalarType.INT32 ||
    scalar === ScalarType.UINT32 ||
    scalar === ScalarType.INT64 ||
    scalar === ScalarType.UINT64 ||
    scalar === ScalarType.BOOL
  ) {
    return "varint";
  }
  if (scalar === ScalarType.SINT32 || scalar === ScalarType.SINT64) {
    return "zigzag";
  }
  if (scalar === ScalarType.FIXED32 || scalar === ScalarType.SFIXED32) {
    return "fixed32";
  }
  if (scalar === ScalarType.FIXED64 || scalar === ScalarType.SFIXED64) {
    return "fixed64";
  }
  return `scalar:${String(scalar)}`;
};

type NonMapFieldValueType =
  | {
      readonly _tag: "Scalar";
      readonly scalar: ScalarType;
    }
  | {
      readonly _tag: "Enum";
      readonly enumeration: DescEnum;
    }
  | {
      readonly _tag: "Message";
      readonly typeName: string;
      readonly delimitedEncoding: boolean;
    };

type FieldValueType =
  | NonMapFieldValueType
  | {
      readonly _tag: "Map";
      readonly key: ScalarType;
      readonly value: NonMapFieldValueType;
    };

type ComparableField = DescField | DescExtension;

const mapEntryTypeName = (field: Extract<DescField, { readonly fieldKind: "map" }>): string =>
  field.proto.typeName.startsWith(".") ? field.proto.typeName.slice(1) : field.proto.typeName;

const mapEntryMessage = (field: Extract<DescField, { readonly fieldKind: "map" }>): DescMessage =>
  Option.getOrThrow(
    Option.fromUndefinedOr(
      normalizedDescriptorGraph(field.parent.file).messages.get(mapEntryTypeName(field)),
    ),
  );

const fieldValueType = (field: ComparableField): FieldValueType => {
  if (field.fieldKind === "scalar") {
    return { _tag: "Scalar", scalar: field.scalar };
  }
  if (field.fieldKind === "enum") {
    return { _tag: "Enum", enumeration: field.enum };
  }
  if (field.fieldKind === "message") {
    return {
      _tag: "Message",
      typeName: field.message.typeName,
      delimitedEncoding: field.delimitedEncoding,
    };
  }
  if (field.fieldKind === "list") {
    if (field.listKind === "scalar") {
      return { _tag: "Scalar", scalar: field.scalar };
    }
    if (field.listKind === "enum") {
      return { _tag: "Enum", enumeration: field.enum };
    }
    return {
      _tag: "Message",
      typeName: field.message.typeName,
      delimitedEncoding: field.delimitedEncoding,
    };
  }
  const value =
    field.mapKind === "scalar"
      ? ({ _tag: "Scalar", scalar: field.scalar } as const)
      : field.mapKind === "enum"
        ? ({ _tag: "Enum", enumeration: field.enum } as const)
        : ({
            _tag: "Message",
            typeName: field.message.typeName,
            delimitedEncoding: field.delimitedEncoding,
          } as const);
  return {
    _tag: "Map",
    key: field.mapKey,
    value,
  };
};

const enumSubset = (previous: DescEnum, current: DescEnum): boolean => {
  if (previous.name !== current.name) {
    return false;
  }
  const currentValues = new Set(
    current.values.map((value) => JSON.stringify([value.name, value.number])),
  );
  return previous.values.every((value) =>
    currentValues.has(JSON.stringify([value.name, value.number])),
  );
};

const valueTypeCompatible = (previous: FieldValueType, current: FieldValueType): boolean => {
  if (previous._tag === "Scalar" && current._tag === "Scalar") {
    if (previous.scalar === ScalarType.STRING && current.scalar === ScalarType.BYTES) {
      return true;
    }
    return scalarFamily(previous.scalar) === scalarFamily(current.scalar);
  }
  if (previous._tag === "Enum" && current._tag === "Enum") {
    return (
      previous.enumeration.typeName === current.enumeration.typeName ||
      enumSubset(previous.enumeration, current.enumeration)
    );
  }
  if (previous._tag === "Message" && current._tag === "Message") {
    return (
      previous.typeName === current.typeName &&
      previous.delimitedEncoding === current.delimitedEncoding
    );
  }
  if (previous._tag === "Map" && current._tag === "Map") {
    return (
      scalarFamily(previous.key) === scalarFamily(current.key) &&
      valueTypeCompatible(previous.value, current.value)
    );
  }
  return false;
};

const fieldValueTypeCompatible = (previous: ComparableField, current: ComparableField): boolean => {
  // Protobuf maps are encoded as repeated synthetic entry messages. Buf WIRE therefore permits
  // toggling only the map_entry option while the repeated field keeps the same entry type.
  if (
    previous.kind === "field" &&
    previous.fieldKind === "map" &&
    current.kind === "field" &&
    current.fieldKind === "list" &&
    current.listKind === "message"
  ) {
    return mapEntryTypeName(previous) === current.message.typeName && !current.delimitedEncoding;
  }
  if (
    previous.kind === "field" &&
    previous.fieldKind === "list" &&
    previous.listKind === "message" &&
    current.kind === "field" &&
    current.fieldKind === "map"
  ) {
    return previous.message.typeName === mapEntryTypeName(current) && !previous.delimitedEncoding;
  }
  return valueTypeCompatible(fieldValueType(previous), fieldValueType(current));
};

type DefaultableField = Extract<ComparableField, { readonly fieldKind: "enum" | "scalar" }>;

const bytesDefault = (value: Uint8Array): string =>
  [...value].map((byte) => String.fromCharCode(byte)).join("");

const normalizeDeclaredDefault = (
  scalar: ScalarType,
  value: bigint | boolean | number | string | Uint8Array,
): bigint | boolean | number | string => {
  if (value instanceof Uint8Array) {
    return bytesDefault(value);
  }
  if (scalar === ScalarType.STRING && typeof value === "string") {
    return bytesDefault(new TextEncoder().encode(value));
  }
  if (scalar === ScalarType.FLOAT && typeof value === "number") {
    return Math.fround(value);
  }
  return value;
};

const fieldDefault = (field: DefaultableField): bigint | boolean | number | string => {
  if (field.fieldKind === "enum") {
    return (
      field.getDefaultValue() ??
      Option.getOrThrow(Option.fromUndefinedOr(field.enum.values[0])).number
    );
  }
  const declared = field.getDefaultValue();
  if (declared !== undefined) {
    return normalizeDeclaredDefault(field.scalar, declared);
  }
  if (field.scalar === ScalarType.STRING || field.scalar === ScalarType.BYTES) {
    return "";
  }
  if (field.scalar === ScalarType.BOOL) {
    return false;
  }
  if (
    field.scalar === ScalarType.INT64 ||
    field.scalar === ScalarType.UINT64 ||
    field.scalar === ScalarType.SINT64 ||
    field.scalar === ScalarType.FIXED64 ||
    field.scalar === ScalarType.SFIXED64
  ) {
    return 0n;
  }
  return 0;
};

const numericDefault = (value: bigint | boolean | number): number | bigint =>
  typeof value === "boolean" ? (value ? 1 : 0) : value;

const numericDefaultsEqual = (
  previous: bigint | boolean | number,
  current: bigint | boolean | number,
): boolean => {
  const previousNumeric = numericDefault(previous);
  const currentNumeric = numericDefault(current);
  if (typeof previousNumeric === "number" && Number.isNaN(previousNumeric)) {
    return typeof currentNumeric === "number" && Number.isNaN(currentNumeric);
  }
  if (typeof currentNumeric === "number" && Number.isNaN(currentNumeric)) {
    return false;
  }
  if (
    (typeof previousNumeric === "number" && !Number.isFinite(previousNumeric)) ||
    (typeof currentNumeric === "number" && !Number.isFinite(currentNumeric))
  ) {
    return previousNumeric === currentNumeric;
  }
  if (typeof previousNumeric === "bigint" && typeof currentNumeric === "bigint") {
    return previousNumeric === currentNumeric;
  }
  if (typeof previousNumeric === "bigint") {
    return Number.isInteger(currentNumeric) && BigInt(currentNumeric) === previousNumeric;
  }
  if (typeof currentNumeric === "bigint") {
    return Number.isInteger(previousNumeric) && BigInt(previousNumeric) === currentNumeric;
  }
  return previousNumeric === currentNumeric;
};

const defaultsEqual = (previous: ComparableField, current: ComparableField): boolean => {
  if (
    previous.fieldKind === "message" ||
    previous.fieldKind === "list" ||
    previous.fieldKind === "map" ||
    current.fieldKind === "message" ||
    current.fieldKind === "list" ||
    current.fieldKind === "map"
  ) {
    return true;
  }
  const previousDefault = fieldDefault(previous);
  const currentDefault = fieldDefault(current);
  if (typeof previousDefault === "string" || typeof currentDefault === "string") {
    return previousDefault === currentDefault;
  }
  if (
    previous.fieldKind === "scalar" &&
    current.fieldKind === "scalar" &&
    previous.scalar === ScalarType.FLOAT &&
    current.scalar === ScalarType.DOUBLE &&
    typeof previousDefault === "number" &&
    typeof currentDefault === "number"
  ) {
    return numericDefaultsEqual(previousDefault, Math.fround(currentDefault));
  }
  if (
    previous.fieldKind === "scalar" &&
    current.fieldKind === "scalar" &&
    previous.scalar === ScalarType.DOUBLE &&
    current.scalar === ScalarType.FLOAT &&
    typeof previousDefault === "number" &&
    typeof currentDefault === "number"
  ) {
    return numericDefaultsEqual(Math.fround(previousDefault), currentDefault);
  }
  return numericDefaultsEqual(previousDefault, currentDefault);
};

const oneofName = (field: ComparableField): string | undefined => field.oneof?.name;

const fieldCompatibilityIssues = (
  previous: ComparableField,
  current: ComparableField,
  path: string,
): ReadonlyArray<KafkaProtobufWireIssue> => {
  const issues: Array<KafkaProtobufWireIssue> = [];
  if (fieldCardinality(previous) !== fieldCardinality(current)) {
    issues.push(
      issue("FIELD_WIRE_COMPATIBLE_CARDINALITY", path, "Field cardinality is not wire-compatible."),
    );
  }
  if (oneofName(previous) !== oneofName(current)) {
    issues.push(issue("FIELD_SAME_ONEOF", path, "Field oneof membership changed."));
  }
  if (!defaultsEqual(previous, current)) {
    issues.push(issue("FIELD_SAME_DEFAULT", path, "Field default value changed."));
  }
  if (!fieldValueTypeCompatible(previous, current)) {
    issues.push(
      issue("FIELD_WIRE_COMPATIBLE_TYPE", path, "Field type is not directionally wire-compatible."),
    );
  }
  return issues;
};

const messageIssues = (
  previous: DescMessage,
  current: DescMessage,
): ReadonlyArray<KafkaProtobufWireIssue> => {
  const issues: Array<KafkaProtobufWireIssue> = [];
  const currentByNumber = new Map(current.fields.map((field) => [field.number, field]));
  const currentReservedNumbers = messageRanges(current);
  for (const previousField of previous.fields) {
    const currentField = currentByNumber.get(previousField.number);
    const path = `${previous.typeName}.${previousField.name}`;
    if (currentField === undefined) {
      if (!rangeContains(currentReservedNumbers, previousField.number)) {
        issues.push(
          issue(
            "FIELD_NO_DELETE_UNLESS_NUMBER_RESERVED",
            path,
            `Field number ${String(previousField.number)} was deleted without being reserved.`,
          ),
        );
      }
      continue;
    }
    issues.push(...fieldCompatibilityIssues(previousField, currentField, path));
  }

  const currentRanges = messageRanges(current);
  for (const range of messageRanges(previous)) {
    if (!rangeCovered(range, currentRanges)) {
      issues.push(
        issue(
          "RESERVED_MESSAGE_NO_DELETE",
          previous.typeName,
          `Reserved field-number range ${String(range.start)}-${String(range.endExclusive - 1)} was removed.`,
        ),
      );
    }
  }
  const currentReservedNames = new Set(current.proto.reservedName);
  for (const name of previous.proto.reservedName) {
    if (!currentReservedNames.has(name)) {
      issues.push(
        issue(
          "RESERVED_MESSAGE_NO_DELETE",
          previous.typeName,
          `Reserved field name ${JSON.stringify(name)} was removed.`,
        ),
      );
    }
  }

  const required = (message: DescMessage): string =>
    message.fields
      .filter((field) => field.proto.label === FieldDescriptorProto_Label.REQUIRED)
      .map((field) => field.number)
      .sort((left, right) => left - right)
      .join(",");
  if (required(previous) !== required(current)) {
    issues.push(
      issue(
        "MESSAGE_SAME_REQUIRED_FIELDS",
        previous.typeName,
        "The set of required field numbers changed.",
      ),
    );
  }
  return issues;
};

const readerMessageIssues = (
  writer: DescMessage,
  reader: DescMessage,
): ReadonlyArray<KafkaProtobufWireIssue> => {
  const issues: Array<KafkaProtobufWireIssue> = [];
  const writerByNumber = new Map(writer.fields.map((field) => [field.number, field]));
  const readerByNumber = new Map(reader.fields.map((field) => [field.number, field]));
  for (const writerField of writer.fields) {
    const readerField = readerByNumber.get(writerField.number);
    if (readerField !== undefined) {
      issues.push(
        ...fieldCompatibilityIssues(
          writerField,
          readerField,
          `${writer.typeName}.${writerField.name}`,
        ),
      );
    }
  }
  for (const readerField of reader.fields) {
    if (
      readerField.proto.label === FieldDescriptorProto_Label.REQUIRED &&
      !writerByNumber.has(readerField.number)
    ) {
      issues.push(
        issue(
          "MESSAGE_SAME_REQUIRED_FIELDS",
          `${reader.typeName}.${readerField.name}`,
          `Required reader field number ${String(readerField.number)} is absent from the writer.`,
        ),
      );
    }
  }
  return issues;
};

type MessagePair = {
  readonly previous: DescMessage;
  readonly current: DescMessage;
};

type EnumPair = {
  readonly previous: DescEnum;
  readonly current: DescEnum;
};

const referencedPairs = (
  previous: DescField,
  current: DescField,
): {
  readonly messages: ReadonlyArray<MessagePair>;
  readonly enums: ReadonlyArray<EnumPair>;
} => {
  if (
    previous.fieldKind === "map" &&
    current.fieldKind === "list" &&
    current.listKind === "message" &&
    mapEntryTypeName(previous) === current.message.typeName
  ) {
    return {
      messages: [{ previous: mapEntryMessage(previous), current: current.message }],
      enums: [],
    };
  }
  if (
    previous.fieldKind === "list" &&
    previous.listKind === "message" &&
    current.fieldKind === "map" &&
    previous.message.typeName === mapEntryTypeName(current)
  ) {
    return {
      messages: [{ previous: previous.message, current: mapEntryMessage(current) }],
      enums: [],
    };
  }
  if (previous.fieldKind === "message" && current.fieldKind === "message") {
    return {
      messages:
        previous.message.typeName === current.message.typeName
          ? [{ previous: previous.message, current: current.message }]
          : [],
      enums: [],
    };
  }
  if (
    previous.fieldKind === "list" &&
    previous.listKind === "message" &&
    current.fieldKind === "list" &&
    current.listKind === "message"
  ) {
    return {
      messages:
        previous.message.typeName === current.message.typeName
          ? [{ previous: previous.message, current: current.message }]
          : [],
      enums: [],
    };
  }
  if (
    previous.fieldKind === "map" &&
    previous.mapKind === "message" &&
    current.fieldKind === "map" &&
    current.mapKind === "message"
  ) {
    return {
      messages:
        previous.message.typeName === current.message.typeName
          ? [{ previous: previous.message, current: current.message }]
          : [],
      enums: [],
    };
  }
  if (previous.fieldKind === "enum" && current.fieldKind === "enum") {
    return {
      messages: [],
      enums:
        previous.enum.typeName === current.enum.typeName
          ? [{ previous: previous.enum, current: current.enum }]
          : [],
    };
  }
  if (
    previous.fieldKind === "list" &&
    previous.listKind === "enum" &&
    current.fieldKind === "list" &&
    current.listKind === "enum"
  ) {
    return {
      messages: [],
      enums:
        previous.enum.typeName === current.enum.typeName
          ? [{ previous: previous.enum, current: current.enum }]
          : [],
    };
  }
  if (
    previous.fieldKind === "map" &&
    previous.mapKind === "enum" &&
    current.fieldKind === "map" &&
    current.mapKind === "enum"
  ) {
    return {
      messages: [],
      enums:
        previous.enum.typeName === current.enum.typeName
          ? [{ previous: previous.enum, current: current.enum }]
          : [],
    };
  }
  return { messages: [], enums: [] };
};

const messageGraphIssues = (
  previous: DescMessage,
  current: DescMessage,
  readerCompatibility: boolean,
): ReadonlyArray<KafkaProtobufWireIssue> => {
  const issues: Array<KafkaProtobufWireIssue> = [];
  const pending: Array<MessagePair> = [{ previous, current }];
  const visitedMessages = new Set<string>();
  const visitedEnums = new Set<string>();
  while (pending.length > 0) {
    const pair = Option.getOrThrow(Option.fromUndefinedOr(pending.shift()));
    const messageIdentity = JSON.stringify([pair.previous.typeName, pair.current.typeName]);
    if (visitedMessages.has(messageIdentity)) {
      continue;
    }
    visitedMessages.add(messageIdentity);
    issues.push(
      ...(readerCompatibility
        ? readerMessageIssues(pair.previous, pair.current)
        : messageIssues(pair.previous, pair.current)),
    );
    const currentByNumber = new Map(pair.current.fields.map((field) => [field.number, field]));
    for (const previousField of pair.previous.fields) {
      const currentField = currentByNumber.get(previousField.number);
      if (currentField === undefined) {
        continue;
      }
      const referenced = referencedPairs(previousField, currentField);
      pending.push(...referenced.messages);
      if (!readerCompatibility) {
        for (const enumeration of referenced.enums) {
          const enumIdentity = JSON.stringify([
            enumeration.previous.typeName,
            enumeration.current.typeName,
          ]);
          if (!visitedEnums.has(enumIdentity)) {
            visitedEnums.add(enumIdentity);
            issues.push(...enumIssues(enumeration.previous, enumeration.current));
          }
        }
      }
    }
  }
  return Object.freeze(issues);
};

export const kafkaProtobufMessageWireCompatibilityIssues = (
  previous: DescMessage,
  current: DescMessage,
): ReadonlyArray<KafkaProtobufWireIssue> => messageGraphIssues(previous, current, false);

export const kafkaProtobufMessageReaderCompatibilityIssues = (
  writer: DescMessage,
  reader: DescMessage,
): ReadonlyArray<KafkaProtobufWireIssue> => messageGraphIssues(writer, reader, true);

const enumIssues = (
  previous: DescEnum,
  current: DescEnum,
): ReadonlyArray<KafkaProtobufWireIssue> => {
  const issues: Array<KafkaProtobufWireIssue> = [];
  const currentNumbers = new Set(current.values.map((value) => value.number));
  const currentReservedNumbers = enumRanges(current);
  for (const value of previous.values) {
    if (!currentNumbers.has(value.number) && !rangeContains(currentReservedNumbers, value.number)) {
      issues.push(
        issue(
          "ENUM_VALUE_NO_DELETE_UNLESS_NUMBER_RESERVED",
          `${previous.typeName}.${value.name}`,
          `Enum number ${String(value.number)} was deleted without being reserved.`,
        ),
      );
    }
  }
  const currentRanges = enumRanges(current);
  for (const range of enumRanges(previous)) {
    if (!rangeCovered(range, currentRanges)) {
      issues.push(
        issue(
          "RESERVED_ENUM_NO_DELETE",
          previous.typeName,
          `Reserved enum-number range ${String(range.start)}-${String(range.endExclusive - 1)} was removed.`,
        ),
      );
    }
  }
  const currentReservedNames = new Set(current.proto.reservedName);
  for (const name of previous.proto.reservedName) {
    if (!currentReservedNames.has(name)) {
      issues.push(
        issue(
          "RESERVED_ENUM_NO_DELETE",
          previous.typeName,
          `Reserved enum name ${JSON.stringify(name)} was removed.`,
        ),
      );
    }
  }
  return issues;
};

const methodIssues = (
  previous: DescMethod,
  current: DescMethod,
): ReadonlyArray<KafkaProtobufWireIssue> => {
  const issues: Array<KafkaProtobufWireIssue> = [];
  const path = `${previous.parent.typeName}.${previous.name}`;
  const previousClientStreaming =
    previous.methodKind === "client_streaming" || previous.methodKind === "bidi_streaming";
  const currentClientStreaming =
    current.methodKind === "client_streaming" || current.methodKind === "bidi_streaming";
  const previousServerStreaming =
    previous.methodKind === "server_streaming" || previous.methodKind === "bidi_streaming";
  const currentServerStreaming =
    current.methodKind === "server_streaming" || current.methodKind === "bidi_streaming";
  if (previousClientStreaming !== currentClientStreaming) {
    issues.push(issue("RPC_SAME_CLIENT_STREAMING", path, "RPC client-streaming behavior changed."));
  }
  if (previousServerStreaming !== currentServerStreaming) {
    issues.push(issue("RPC_SAME_SERVER_STREAMING", path, "RPC server-streaming behavior changed."));
  }
  if (previous.input.typeName !== current.input.typeName) {
    issues.push(issue("RPC_SAME_REQUEST_TYPE", path, "RPC request type changed."));
  }
  if (previous.output.typeName !== current.output.typeName) {
    issues.push(issue("RPC_SAME_RESPONSE_TYPE", path, "RPC response type changed."));
  }
  if (previous.idempotency !== current.idempotency) {
    issues.push(issue("RPC_SAME_IDEMPOTENCY_LEVEL", path, "RPC idempotency level changed."));
  }
  return issues;
};

const servicesByTypeName = (files: Iterable<DescFile>): ReadonlyMap<string, DescService> => {
  const services = new Map<string, DescService>();
  for (const file of files) {
    for (const service of file.services) {
      services.set(service.typeName, service);
    }
  }
  return services;
};

const extensionsByExtendeeAndNumber = (
  files: Iterable<DescFile>,
): ReadonlyMap<string, DescExtension> => {
  const extensions = new Map<string, DescExtension>();
  const add = (extension: DescExtension): void => {
    extensions.set(JSON.stringify([extension.extendee.typeName, extension.number]), extension);
  };
  for (const file of files) {
    for (const extension of file.extensions) {
      add(extension);
    }
    for (const message of file.messages) {
      visitMessage(message, (candidate) => {
        for (const extension of candidate.nestedExtensions) {
          add(extension);
        }
      });
    }
  }
  return extensions;
};

export const kafkaProtobufWireCompatibilityIssues = (
  previousRoot: DescFile,
  currentRoot: DescFile,
): ReadonlyArray<KafkaProtobufWireIssue> => {
  const issues: Array<KafkaProtobufWireIssue> = [];
  const previousFiles = descriptorGraph(previousRoot);
  const currentFiles = descriptorGraph(currentRoot);
  for (const [name, previous] of previousFiles) {
    const current = currentFiles.get(name);
    if (current !== undefined && previous.proto.package !== current.proto.package) {
      issues.push(
        issue(
          "FILE_SAME_PACKAGE",
          name,
          `File package changed from ${JSON.stringify(previous.proto.package)} to ${JSON.stringify(current.proto.package)}.`,
        ),
      );
    }
  }

  const previousEnums = enumsByTypeName(previousFiles.values());
  const currentEnums = enumsByTypeName(currentFiles.values());
  for (const [typeName, previous] of previousEnums) {
    const current = currentEnums.get(typeName);
    if (current !== undefined) {
      issues.push(...enumIssues(previous, current));
    }
  }

  // Buf exposes maps as fields and hides their synthetic entry messages from ordinary descriptor
  // traversal. Compare the normalized graph so map ↔ repeated-entry transitions validate the
  // entry's key and value fields just like any other message.
  const previousMessages = normalizedDescriptorGraph(previousRoot).messages;
  const currentMessages = normalizedDescriptorGraph(currentRoot).messages;
  for (const [typeName, previous] of previousMessages) {
    const current = currentMessages.get(typeName);
    if (current !== undefined) {
      issues.push(...messageIssues(previous, current));
    }
  }

  const previousExtensions = extensionsByExtendeeAndNumber(previousFiles.values());
  const currentExtensions = extensionsByExtendeeAndNumber(currentFiles.values());
  for (const [identity, previous] of previousExtensions) {
    const current = currentExtensions.get(identity);
    const path = `${previous.extendee.typeName}.${String(previous.number)}`;
    if (current === undefined) {
      const currentExtendee = currentMessages.get(previous.extendee.typeName);
      if (
        currentExtendee === undefined ||
        !rangeContains(messageRanges(currentExtendee), previous.number)
      ) {
        issues.push(
          issue(
            "FIELD_NO_DELETE_UNLESS_NUMBER_RESERVED",
            path,
            `Field number ${String(previous.number)} was deleted without being reserved.`,
          ),
        );
      }
      continue;
    }
    issues.push(...fieldCompatibilityIssues(previous, current, path));
  }

  const previousServices = servicesByTypeName(previousFiles.values());
  const currentServices = servicesByTypeName(currentFiles.values());
  for (const [typeName, previousService] of previousServices) {
    const currentService = currentServices.get(typeName);
    if (currentService === undefined) {
      continue;
    }
    const currentMethods = new Map(currentService.methods.map((method) => [method.name, method]));
    for (const previousMethod of previousService.methods) {
      const currentMethod = currentMethods.get(previousMethod.name);
      if (currentMethod !== undefined) {
        issues.push(...methodIssues(previousMethod, currentMethod));
      }
    }
  }
  return Object.freeze(issues);
};
