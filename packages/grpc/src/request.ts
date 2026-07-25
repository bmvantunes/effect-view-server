import {
  create,
  protoInt64,
  ScalarType,
  toBinary,
  type DescEnum,
  type DescField,
  type DescMessage,
  type DescOneof,
  type Message,
} from "@bufbuild/protobuf";
import { FeatureSet_FieldPresence, isWrapperDesc } from "@bufbuild/protobuf/wkt";

const int32Minimum = -2_147_483_648;
const int32Maximum = 2_147_483_647;
const uint32Maximum = 4_294_967_295;
const float32Maximum = 3.402_823_466_385_288_6e38;

const isPlainObject = (value: unknown): value is Readonly<Record<string, unknown>> => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
};

type DataEntry = readonly [key: string, value: unknown];
type MapKeyScalar = Extract<DescField, { readonly fieldKind: "map" }>["mapKey"];

const exactArrayValues = (value: unknown): ReadonlyArray<unknown> | undefined => {
  if (!Array.isArray(value)) {
    return undefined;
  }
  const lengthDescriptor = Object.getOwnPropertyDescriptor(value, "length");
  if (
    lengthDescriptor === undefined ||
    !("value" in lengthDescriptor) ||
    typeof lengthDescriptor.value !== "number" ||
    !Number.isSafeInteger(lengthDescriptor.value) ||
    lengthDescriptor.value < 0
  ) {
    return undefined;
  }
  const length = lengthDescriptor.value;
  const keys = Reflect.ownKeys(value);
  if (keys.length !== length + 1 || !keys.includes("length")) {
    return undefined;
  }
  const values: Array<unknown> = [];
  for (let index = 0; index < length; index++) {
    const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
    if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
      return undefined;
    }
    values.push(descriptor.value);
  }
  return values;
};

const exactDataEntries = (value: unknown): ReadonlyArray<DataEntry> | undefined => {
  if (!isPlainObject(value)) {
    return undefined;
  }
  const entries: Array<DataEntry> = [];
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key !== "string") {
      return undefined;
    }
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
      return undefined;
    }
    entries.push([key, descriptor.value]);
  }
  return entries;
};

const scalarValueIsValid = (scalar: ScalarType, longAsString: boolean, value: unknown): boolean => {
  switch (scalar) {
    case ScalarType.DOUBLE:
      return typeof value === "number";
    case ScalarType.FLOAT:
      return (
        typeof value === "number" &&
        (!Number.isFinite(value) || (value >= -float32Maximum && value <= float32Maximum))
      );
    case ScalarType.INT32:
    case ScalarType.SFIXED32:
    case ScalarType.SINT32:
      return (
        typeof value === "number" &&
        Number.isInteger(value) &&
        value >= int32Minimum &&
        value <= int32Maximum
      );
    case ScalarType.FIXED32:
    case ScalarType.UINT32:
      return (
        typeof value === "number" && Number.isInteger(value) && value >= 0 && value <= uint32Maximum
      );
    case ScalarType.INT64:
    case ScalarType.SFIXED64:
    case ScalarType.SINT64: {
      const longValue = longAsString
        ? typeof value === "string" && value.length > 0
          ? value
          : undefined
        : typeof value === "bigint"
          ? value
          : undefined;
      if (longValue === undefined) {
        return false;
      }
      try {
        protoInt64.parse(longValue);
        return true;
      } catch {
        return false;
      }
    }
    case ScalarType.UINT64:
    case ScalarType.FIXED64: {
      const longValue = longAsString
        ? typeof value === "string" && value.length > 0
          ? value
          : undefined
        : typeof value === "bigint"
          ? value
          : undefined;
      if (longValue === undefined) {
        return false;
      }
      try {
        protoInt64.uParse(longValue);
        return true;
      } catch {
        return false;
      }
    }
    case ScalarType.BOOL:
      return typeof value === "boolean";
    case ScalarType.STRING:
      return typeof value === "string";
    case ScalarType.BYTES:
      return value instanceof Uint8Array;
  }
};

const enumValueIsValid = (enumeration: DescEnum, value: unknown): boolean =>
  typeof value === "number" &&
  Number.isInteger(value) &&
  value >= int32Minimum &&
  value <= int32Maximum &&
  (enumeration.open || Object.hasOwn(enumeration.value, value));

const signedMapKeyPattern = /^-?(?:0|[1-9]\d*)$/;
const unsignedMapKeyPattern = /^(?:0|[1-9]\d*)$/;

const mapKeyIsValid = (scalar: MapKeyScalar, value: string): boolean => {
  switch (scalar) {
    case ScalarType.STRING:
      return true;
    case ScalarType.BOOL:
      return value === "true" || value === "false";
    case ScalarType.INT32:
    case ScalarType.SFIXED32:
    case ScalarType.SINT32:
      return signedMapKeyPattern.test(value) && scalarValueIsValid(scalar, false, Number(value));
    case ScalarType.FIXED32:
    case ScalarType.UINT32:
      return unsignedMapKeyPattern.test(value) && scalarValueIsValid(scalar, false, Number(value));
    case ScalarType.INT64:
    case ScalarType.SFIXED64:
    case ScalarType.SINT64:
      return signedMapKeyPattern.test(value) && scalarValueIsValid(scalar, true, value);
    case ScalarType.UINT64:
    case ScalarType.FIXED64:
      return unsignedMapKeyPattern.test(value) && scalarValueIsValid(scalar, true, value);
  }
};

const jsonValueIsValid = (value: unknown, active: WeakSet<object>): boolean => {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    typeof value === "number"
  ) {
    return true;
  }
  if (Array.isArray(value)) {
    const values = exactArrayValues(value);
    if (values === undefined || active.has(value)) {
      return false;
    }
    active.add(value);
    const valid = values.every((entry) => jsonValueIsValid(entry, active));
    active.delete(value);
    return valid;
  }
  const entries = exactDataEntries(value);
  if (entries === undefined || typeof value !== "object" || value === null || active.has(value)) {
    return false;
  }
  active.add(value);
  const valid = entries.every(([, entry]) => jsonValueIsValid(entry, active));
  active.delete(value);
  return valid;
};

const structFieldValueIsValid = (
  field: DescField,
  message: DescMessage,
  value: unknown,
  active: WeakSet<object>,
): boolean =>
  message.typeName === "google.protobuf.Struct" &&
  field.parent.typeName !== "google.protobuf.Value" &&
  exactDataEntries(value) !== undefined &&
  jsonValueIsValid(value, active);

const fieldValueIsValid = (field: DescField, value: unknown, active: WeakSet<object>): boolean => {
  if (value === undefined) {
    return true;
  }
  switch (field.fieldKind) {
    case "scalar":
      return scalarValueIsValid(field.scalar, field.longAsString, value);
    case "enum":
      return enumValueIsValid(field.enum, value);
    case "message":
      return structFieldValueIsValid(field, field.message, value, active)
        ? true
        : isWrapperDesc(field.message) && field.oneof === undefined
          ? scalarValueIsValid(
              field.message.fields[0].scalar,
              field.message.fields[0].longAsString,
              value,
            )
          : messageInitIsValid(field.message, value, active);
    case "list":
      const values = exactArrayValues(value);
      if (values === undefined) {
        return false;
      }
      return values.every((entry) => {
        if (field.listKind === "scalar") {
          return scalarValueIsValid(field.scalar, field.longAsString, entry);
        }
        if (field.listKind === "enum") {
          return enumValueIsValid(field.enum, entry);
        }
        return structFieldValueIsValid(field, field.message, entry, active)
          ? true
          : messageInitIsValid(field.message, entry, active);
      });
    case "map":
      const entries = exactDataEntries(value);
      if (entries === undefined) {
        return false;
      }
      return entries.every(([key, entry]) => {
        if (!mapKeyIsValid(field.mapKey, key)) {
          return false;
        }
        if (field.mapKind === "scalar") {
          return scalarValueIsValid(field.scalar, false, entry);
        }
        if (field.mapKind === "enum") {
          return enumValueIsValid(field.enum, entry);
        }
        return structFieldValueIsValid(field, field.message, entry, active)
          ? true
          : messageInitIsValid(field.message, entry, active);
      });
  }
};

const oneofValueIsValid = (oneof: DescOneof, value: unknown, active: WeakSet<object>): boolean => {
  if (!isPlainObject(value)) {
    return false;
  }
  const entries = exactDataEntries(value);
  if (entries === undefined) {
    return false;
  }
  const keys = entries.map(([key]) => key);
  if (keys.some((key) => key !== "case" && key !== "value") || !Object.hasOwn(value, "case")) {
    return false;
  }
  const selectedCase = value["case"];
  if (selectedCase === undefined) {
    return value["value"] === undefined;
  }
  if (typeof selectedCase !== "string" || !Object.hasOwn(value, "value")) {
    return false;
  }
  const field = oneof.fields.find((candidate) => candidate.localName === selectedCase);
  const selectedValue = value["value"];
  return (
    field !== undefined &&
    selectedValue !== undefined &&
    fieldValueIsValid(field, selectedValue, active)
  );
};

function messageInitIsValid(
  message: DescMessage,
  value: unknown,
  active: WeakSet<object>,
): value is Readonly<Record<string, unknown>> {
  if (!isPlainObject(value)) {
    return false;
  }
  const entries = exactDataEntries(value);
  if (entries === undefined || active.has(value)) {
    return false;
  }
  active.add(value);
  const supplied = new Set(entries.map(([key]) => key));
  const valid =
    message.fields.every(
      (field) =>
        field.presence !== FeatureSet_FieldPresence.LEGACY_REQUIRED ||
        supplied.has(field.localName),
    ) &&
    entries.every(([key, fieldValue]) => {
      const field = message.field[key];
      if (field !== undefined && field.oneof === undefined) {
        return fieldValueIsValid(field, fieldValue, active);
      }
      const oneof = message.oneofs.find((candidate) => candidate.localName === key);
      return oneof !== undefined && oneofValueIsValid(oneof, fieldValue, active);
    });
  active.delete(value);
  return valid;
}

function snapshotRequest(
  value: Readonly<Record<string, unknown>>,
): Readonly<Record<string, unknown>>;
function snapshotRequest(value: unknown): unknown;
function snapshotRequest(value: unknown): unknown {
  if (value instanceof Uint8Array) {
    return value.slice();
  }
  if (Array.isArray(value)) {
    const values = exactArrayValues(value);
    if (values === undefined) {
      throw new TypeError("The request-init repeated field is not an exact dense data array.");
    }
    return Object.freeze(values.map((entry) => snapshotRequest(entry)));
  }
  const entries = exactDataEntries(value);
  if (entries !== undefined) {
    const snapshot: Record<string, unknown> = {};
    for (const [key, entry] of entries) {
      Object.defineProperty(snapshot, key, {
        enumerable: true,
        value: snapshotRequest(entry),
      });
    }
    return Object.freeze(snapshot);
  }
  return value;
}

const defineMaterializedField = (target: object, key: string, value: unknown): void => {
  Object.defineProperty(target, key, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
};

type MessageBearingField =
  | Extract<DescField, { readonly fieldKind: "message" }>
  | Extract<DescField, { readonly fieldKind: "list"; readonly listKind: "message" }>
  | Extract<DescField, { readonly fieldKind: "map"; readonly mapKind: "message" }>;

const usesNativeStructRepresentation = (field: MessageBearingField): boolean =>
  field.message.typeName === "google.protobuf.Struct" &&
  field.parent.typeName !== "google.protobuf.Value";

const requireSnapshotDataEntries = (value: unknown): ReadonlyArray<DataEntry> => {
  const entries = exactDataEntries(value);
  if (entries === undefined) {
    throw new TypeError("The validated request snapshot is not an exact data object.");
  }
  return entries;
};

const requireSnapshotArrayValues = (value: unknown): ReadonlyArray<unknown> => {
  const values = exactArrayValues(value);
  if (values === undefined) {
    throw new TypeError("The validated request snapshot is not an exact dense data array.");
  }
  return values;
};

const dataObject = (entries: ReadonlyArray<DataEntry>): Readonly<Record<string, unknown>> => {
  const value: Record<string, unknown> = {};
  for (const [key, entry] of entries) {
    Object.defineProperty(value, key, {
      enumerable: true,
      value: entry,
    });
  }
  return Object.freeze(value);
};

const materializeGeneratedMessage = (
  message: DescMessage,
  init: Readonly<Record<string, unknown>>,
): Message => {
  const materialized = create(message, init);
  const entries = requireSnapshotDataEntries(init);
  const supplied = new Map(entries);

  for (const field of message.fields) {
    if (field.oneof !== undefined || !supplied.has(field.localName)) {
      continue;
    }
    const value = supplied.get(field.localName);
    if (value === undefined) {
      continue;
    }
    switch (field.fieldKind) {
      case "message":
        if (usesNativeStructRepresentation(field) || isWrapperDesc(field.message)) {
          defineMaterializedField(materialized, field.localName, value);
        } else {
          const nestedEntries = requireSnapshotDataEntries(value);
          defineMaterializedField(
            materialized,
            field.localName,
            materializeGeneratedMessage(field.message, dataObject(nestedEntries)),
          );
        }
        break;
      case "list": {
        const values = requireSnapshotArrayValues(value);
        const list =
          field.listKind === "message"
            ? values.map((entry) => {
                if (usesNativeStructRepresentation(field)) {
                  return entry;
                }
                const nestedEntries = requireSnapshotDataEntries(entry);
                return materializeGeneratedMessage(field.message, dataObject(nestedEntries));
              })
            : values;
        defineMaterializedField(materialized, field.localName, Object.freeze(list));
        break;
      }
      case "map": {
        const mapEntries = requireSnapshotDataEntries(value);
        const map: Record<string, unknown> = {};
        for (const [key, entry] of mapEntries) {
          const materializedEntry =
            field.mapKind === "message" && !usesNativeStructRepresentation(field)
              ? (() => {
                  const nestedEntries = requireSnapshotDataEntries(entry);
                  return materializeGeneratedMessage(field.message, dataObject(nestedEntries));
                })()
              : entry;
          Object.defineProperty(map, key, {
            enumerable: true,
            value: materializedEntry,
          });
        }
        defineMaterializedField(materialized, field.localName, Object.freeze(map));
        break;
      }
      case "scalar":
      case "enum":
        break;
    }
  }

  for (const oneof of message.oneofs) {
    if (!supplied.has(oneof.localName)) {
      continue;
    }
    const value = supplied.get(oneof.localName);
    const oneofEntries = requireSnapshotDataEntries(value);
    const selectedCase = oneofEntries.find(([key]) => key === "case")?.[1];
    const selectedField =
      typeof selectedCase === "string"
        ? oneof.fields.find((field) => field.localName === selectedCase)
        : undefined;
    if (selectedField?.fieldKind === "message") {
      const selectedValue = oneofEntries.find(([key]) => key === "value")?.[1];
      const materializedValue = usesNativeStructRepresentation(selectedField)
        ? selectedValue
        : (() => {
            const nestedEntries = requireSnapshotDataEntries(selectedValue);
            return materializeGeneratedMessage(selectedField.message, dataObject(nestedEntries));
          })();
      defineMaterializedField(
        materialized,
        oneof.localName,
        Object.freeze({
          case: selectedCase,
          value: materializedValue,
        }),
      );
      continue;
    }
    const normalizedOneof = Reflect.get(materialized, oneof.localName);
    defineMaterializedField(
      materialized,
      oneof.localName,
      dataObject(requireSnapshotDataEntries(normalizedOneof)),
    );
  }

  return Object.freeze(materialized);
};

export const validateAndSnapshotGrpcRequest = (message: DescMessage, value: unknown): unknown => {
  if (!messageInitIsValid(message, value, new WeakSet<object>())) {
    throw new TypeError("The request-init value does not match its generated descriptor.");
  }
  const snapshot = snapshotRequest(value);
  try {
    const materialized = materializeGeneratedMessage(message, snapshot);
    toBinary(message, materialized);
    return materialized;
  } catch {
    throw new TypeError("The request-init value does not match its generated descriptor.");
  }
};
