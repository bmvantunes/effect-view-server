import { create, createFileRegistry } from "@bufbuild/protobuf";
import type { DescFile } from "@bufbuild/protobuf";
import {
  DescriptorProtoSchema,
  FieldDescriptorProto_Label,
  FieldDescriptorProto_Type,
  FieldDescriptorProtoSchema,
  FileDescriptorProtoSchema,
  FileDescriptorSetSchema,
  MethodOptions_IdempotencyLevel,
} from "@bufbuild/protobuf/wkt";
import type { DescriptorProto } from "@bufbuild/protobuf/wkt";
import { describe, expect, it } from "@effect/vitest";
import { Option } from "effect";
import {
  kafkaProtobufMessageAtNormalizedIndexes,
  kafkaProtobufMessageAtIndexes,
  kafkaProtobufMessageIndexes,
  kafkaProtobufNormalizedMessageIndexes,
} from "./schema-registry-descriptor";
import {
  kafkaProtobufMessageReaderCompatibilityIssues,
  kafkaProtobufMessageWireCompatibilityIssues,
  kafkaProtobufWireCompatibilityIssues,
} from "./schema-registry-wire";

type FieldInput = {
  readonly name: string;
  readonly number: number;
  readonly type: FieldDescriptorProto_Type;
  readonly typeName?: string;
  readonly label?: FieldDescriptorProto_Label;
  readonly defaultValue?: string;
  readonly oneofIndex?: number;
};

type MessageInput = {
  readonly name: string;
  readonly fields?: ReadonlyArray<FieldInput>;
  readonly nested?: ReadonlyArray<MessageInput>;
  readonly enums?: ReadonlyArray<EnumInput>;
  readonly reservedNumbers?: ReadonlyArray<readonly [number, number]>;
  readonly reservedNames?: ReadonlyArray<string>;
  readonly oneofs?: ReadonlyArray<string>;
  readonly mapEntry?: boolean;
  readonly extensions?: ReadonlyArray<
    FieldInput & {
      readonly extendee: string;
    }
  >;
};

type EnumInput = {
  readonly name: string;
  readonly values: ReadonlyArray<readonly [string, number]>;
  readonly reservedNumbers?: ReadonlyArray<readonly [number, number]>;
  readonly reservedNames?: ReadonlyArray<string>;
};

type MethodInput = {
  readonly name: string;
  readonly inputType: string;
  readonly outputType: string;
  readonly clientStreaming?: boolean;
  readonly serverStreaming?: boolean;
  readonly idempotency?: MethodOptions_IdempotencyLevel;
};

const descriptorFile = (input: {
  readonly name?: string;
  readonly package?: string;
  readonly syntax?: "proto2" | "proto3";
  readonly messages: ReadonlyArray<MessageInput>;
  readonly enums?: ReadonlyArray<EnumInput>;
  readonly methods?: ReadonlyArray<MethodInput>;
  readonly extensions?: ReadonlyArray<
    FieldInput & {
      readonly extendee: string;
    }
  >;
}): DescFile => {
  const messageProto = (message: MessageInput): DescriptorProto =>
    create(DescriptorProtoSchema, {
      name: message.name,
      field: (message.fields ?? []).map((field) => ({
        name: field.name,
        number: field.number,
        type: field.type,
        label: field.label ?? FieldDescriptorProto_Label.OPTIONAL,
        ...(field.typeName === undefined ? {} : { typeName: field.typeName }),
        ...(field.defaultValue === undefined ? {} : { defaultValue: field.defaultValue }),
        ...(field.oneofIndex === undefined ? {} : { oneofIndex: field.oneofIndex }),
      })),
      nestedType: (message.nested ?? []).map(messageProto),
      enumType: (message.enums ?? []).map((enumeration) => ({
        name: enumeration.name,
        value: enumeration.values.map(([name, number]) => ({ name, number })),
        reservedRange: (enumeration.reservedNumbers ?? []).map(([start, end]) => ({ start, end })),
        reservedName: [...(enumeration.reservedNames ?? [])],
      })),
      reservedRange: (message.reservedNumbers ?? []).map(([start, end]) => ({ start, end })),
      reservedName: [...(message.reservedNames ?? [])],
      oneofDecl: (message.oneofs ?? []).map((name) => ({ name })),
      extension: (message.extensions ?? []).map((extension) => ({
        name: extension.name,
        number: extension.number,
        type: extension.type,
        label: extension.label ?? FieldDescriptorProto_Label.OPTIONAL,
        extendee: extension.extendee,
        ...(extension.typeName === undefined ? {} : { typeName: extension.typeName }),
        ...(extension.defaultValue === undefined ? {} : { defaultValue: extension.defaultValue }),
      })),
      options: { mapEntry: message.mapEntry ?? false },
    });
  const file = create(FileDescriptorProtoSchema, {
    name: input.name ?? "orders.proto",
    package: input.package ?? "example",
    syntax: input.syntax ?? "proto3",
    messageType: input.messages.map(messageProto),
    enumType: (input.enums ?? []).map((enumeration) => ({
      name: enumeration.name,
      value: enumeration.values.map(([name, number]) => ({ name, number })),
      reservedRange: (enumeration.reservedNumbers ?? []).map(([start, end]) => ({ start, end })),
      reservedName: [...(enumeration.reservedNames ?? [])],
    })),
    extension: (input.extensions ?? []).map((extension) =>
      create(FieldDescriptorProtoSchema, {
        name: extension.name,
        number: extension.number,
        type: extension.type,
        label: extension.label ?? FieldDescriptorProto_Label.OPTIONAL,
        extendee: extension.extendee,
        ...(extension.typeName === undefined ? {} : { typeName: extension.typeName }),
        ...(extension.defaultValue === undefined ? {} : { defaultValue: extension.defaultValue }),
      }),
    ),
    service:
      input.methods === undefined
        ? []
        : [
            {
              name: "Orders",
              method: input.methods.map((method) => ({
                name: method.name,
                inputType: method.inputType,
                outputType: method.outputType,
                clientStreaming: method.clientStreaming ?? false,
                serverStreaming: method.serverStreaming ?? false,
                options: {
                  idempotencyLevel:
                    method.idempotency ?? MethodOptions_IdempotencyLevel.IDEMPOTENCY_UNKNOWN,
                },
              })),
            },
          ],
  });
  const registry = createFileRegistry(create(FileDescriptorSetSchema, { file: [file] }));
  const descriptor = registry.getFile(file.name);
  if (descriptor === undefined) {
    throw new Error("test descriptor file was not created");
  }
  return descriptor;
};

const orderMessage = (
  fields: ReadonlyArray<FieldInput>,
  options: Omit<MessageInput, "name" | "fields"> = {},
): MessageInput => ({ name: "Order", fields, ...options });

const field = (
  name: string,
  number: number,
  type: FieldDescriptorProto_Type,
  options: Omit<FieldInput, "name" | "number" | "type"> = {},
): FieldInput => ({ name, number, type, ...options });

const rules = (previous: DescFile, current: DescFile): ReadonlyArray<string> =>
  kafkaProtobufWireCompatibilityIssues(previous, current).map((issue) => issue.rule);

const orderDescriptor = (file: DescFile) =>
  Option.getOrThrow(
    Option.fromUndefinedOr(file.messages.find((message) => message.typeName === "example.Order")),
  );

describe("Kafka Schema Registry Buf WIRE compatibility", () => {
  it("resolves both raw and normalized Confluent indexes around synthetic map entries", () => {
    const file = descriptorFile({
      messages: [
        { name: "First" },
        {
          name: "Order",
          nested: [
            { name: "LabelsEntry", mapEntry: true },
            { name: "Line" },
            {
              name: "Envelope",
              nested: [{ name: "Payload" }],
            },
          ],
        },
      ],
    });

    expect(kafkaProtobufMessageIndexes(file, "example.First")).toStrictEqual([0]);
    expect(kafkaProtobufMessageIndexes(file, "example.Order.Envelope.Payload")).toStrictEqual([
      1, 2, 0,
    ]);
    expect(
      kafkaProtobufNormalizedMessageIndexes(file, "example.Order.Envelope.Payload"),
    ).toStrictEqual([1, 1, 0]);
    expect(kafkaProtobufMessageAtNormalizedIndexes(file, [1, 1, 0])?.typeName).toBe(
      "example.Order.Envelope.Payload",
    );
    expect(kafkaProtobufMessageAtIndexes(file, [1, 2, 0])?.typeName).toBe(
      "example.Order.Envelope.Payload",
    );
    expect(kafkaProtobufMessageAtIndexes(file, [99])).toBeUndefined();
    expect(kafkaProtobufMessageAtNormalizedIndexes(file, [1, 99])).toBeUndefined();
    expect(
      kafkaProtobufNormalizedMessageIndexes(file, "example.Order.LabelsEntry"),
    ).toBeUndefined();
    expect(kafkaProtobufMessageIndexes(file, "example.Order.LabelsEntry")).toStrictEqual([1, 0]);
    expect(kafkaProtobufMessageIndexes(file, "example.Missing")).toBeUndefined();

    const packageLess = descriptorFile({
      package: "",
      messages: [{ name: "Outer", nested: [{ name: "Inner" }] }],
    });
    expect(kafkaProtobufMessageIndexes(packageLess, "Outer.Inner")).toStrictEqual([0, 0]);
    expect(kafkaProtobufMessageAtIndexes(packageLess, [0, 0])?.typeName).toBe("Outer.Inner");
  });

  it("covers every scalar family and reflected field container kind", () => {
    const messages: ReadonlyArray<MessageInput> = [
      { name: "Child" },
      { name: "Other" },
      orderMessage(
        [
          field("zigzag", 1, FieldDescriptorProto_Type.SINT32),
          field("fixed32", 2, FieldDescriptorProto_Type.FIXED32),
          field("fixed64", 3, FieldDescriptorProto_Type.FIXED64),
          field("floating", 4, FieldDescriptorProto_Type.FLOAT),
          field("child", 5, FieldDescriptorProto_Type.MESSAGE, {
            typeName: ".example.Child",
          }),
          field("states", 6, FieldDescriptorProto_Type.ENUM, {
            typeName: ".example.State",
            label: FieldDescriptorProto_Label.REPEATED,
          }),
          field("children", 7, FieldDescriptorProto_Type.MESSAGE, {
            typeName: ".example.Child",
            label: FieldDescriptorProto_Label.REPEATED,
          }),
          field("scalar_map", 8, FieldDescriptorProto_Type.MESSAGE, {
            typeName: ".example.Order.ScalarMapEntry",
            label: FieldDescriptorProto_Label.REPEATED,
          }),
          field("enum_map", 9, FieldDescriptorProto_Type.MESSAGE, {
            typeName: ".example.Order.EnumMapEntry",
            label: FieldDescriptorProto_Label.REPEATED,
          }),
          field("message_map", 10, FieldDescriptorProto_Type.MESSAGE, {
            typeName: ".example.Order.MessageMapEntry",
            label: FieldDescriptorProto_Label.REPEATED,
          }),
          field("local_state", 11, FieldDescriptorProto_Type.ENUM, {
            typeName: ".example.Order.LocalState",
          }),
        ],
        {
          enums: [
            {
              name: "LocalState",
              values: [["LOCAL_STATE_UNSPECIFIED", 0]],
            },
          ],
          nested: [
            {
              name: "ScalarMapEntry",
              mapEntry: true,
              fields: [
                field("key", 1, FieldDescriptorProto_Type.STRING),
                field("value", 2, FieldDescriptorProto_Type.INT32),
              ],
            },
            {
              name: "EnumMapEntry",
              mapEntry: true,
              fields: [
                field("key", 1, FieldDescriptorProto_Type.STRING),
                field("value", 2, FieldDescriptorProto_Type.ENUM, {
                  typeName: ".example.State",
                }),
              ],
            },
            {
              name: "MessageMapEntry",
              mapEntry: true,
              fields: [
                field("key", 1, FieldDescriptorProto_Type.STRING),
                field("value", 2, FieldDescriptorProto_Type.MESSAGE, {
                  typeName: ".example.Child",
                }),
              ],
            },
          ],
        },
      ),
    ];
    const previous = descriptorFile({
      enums: [
        {
          name: "State",
          values: [["STATE_UNSPECIFIED", 0]],
        },
      ],
      messages,
    });
    const current = descriptorFile({
      enums: [
        {
          name: "State",
          values: [
            ["STATE_UNSPECIFIED", 0],
            ["STATE_ACTIVE", 1],
          ],
        },
      ],
      messages: messages.map((message) => {
        if (message.name !== "Order" || message.fields === undefined) {
          return message;
        }
        return {
          ...message,
          fields: message.fields.map((candidate) =>
            candidate.name === "zigzag"
              ? { ...candidate, type: FieldDescriptorProto_Type.SINT64 }
              : candidate.name === "fixed32"
                ? { ...candidate, type: FieldDescriptorProto_Type.SFIXED32 }
                : candidate.name === "fixed64"
                  ? { ...candidate, type: FieldDescriptorProto_Type.SFIXED64 }
                  : candidate,
          ),
        };
      }),
    });

    expect(kafkaProtobufWireCompatibilityIssues(previous, current)).toStrictEqual([]);
    const previousOrder = previous.messages.find((message) => message.name === "Order");
    const currentOrder = current.messages.find((message) => message.name === "Order");
    if (previousOrder === undefined || currentOrder === undefined) {
      throw new Error("Order descriptors missing");
    }
    expect(kafkaProtobufMessageWireCompatibilityIssues(previousOrder, currentOrder)).toStrictEqual(
      [],
    );

    previous.dependencies.push(previous);
    expect(kafkaProtobufWireCompatibilityIssues(previous, previous)).toStrictEqual([]);
  });

  it("compares byte defaults and contiguous reserved-range coverage", () => {
    const previous = descriptorFile({
      syntax: "proto2",
      messages: [
        orderMessage(
          [
            field("same_bytes", 1, FieldDescriptorProto_Type.BYTES, { defaultValue: "abc" }),
            field("different_bytes", 2, FieldDescriptorProto_Type.BYTES, {
              defaultValue: "abc",
            }),
            field("longer_bytes", 3, FieldDescriptorProto_Type.BYTES, {
              defaultValue: "abc",
            }),
          ],
          { reservedNumbers: [[8, 12]] },
        ),
      ],
    });
    const covered = descriptorFile({
      syntax: "proto2",
      messages: [
        orderMessage(
          [
            field("same_bytes", 1, FieldDescriptorProto_Type.BYTES, { defaultValue: "abc" }),
            field("different_bytes", 2, FieldDescriptorProto_Type.BYTES, {
              defaultValue: "abd",
            }),
            field("longer_bytes", 3, FieldDescriptorProto_Type.BYTES, {
              defaultValue: "abcd",
            }),
          ],
          {
            reservedNumbers: [
              [8, 10],
              [8, 9],
              [10, 12],
            ],
          },
        ),
      ],
    });
    const gap = descriptorFile({
      syntax: "proto2",
      messages: [
        orderMessage(
          previous.messages[0]?.fields.map((candidate) => ({
            name: candidate.name,
            number: candidate.number,
            type: candidate.proto.type,
            defaultValue: candidate.proto.defaultValue,
          })) ?? [],
          {
            reservedNumbers: [
              [8, 9],
              [10, 12],
            ],
          },
        ),
      ],
    });

    expect(rules(previous, covered)).toStrictEqual(["FIELD_SAME_DEFAULT", "FIELD_SAME_DEFAULT"]);
    expect(rules(previous, gap)).toStrictEqual(["RESERVED_MESSAGE_NO_DELETE"]);
  });

  it("normalizes Buf WIRE-equivalent effective defaults", () => {
    const previous = descriptorFile({
      syntax: "proto2",
      messages: [
        orderMessage([
          field("integer", 1, FieldDescriptorProto_Type.INT32, { defaultValue: "1" }),
          field("text", 2, FieldDescriptorProto_Type.STRING, { defaultValue: "xyz" }),
          field("zero", 3, FieldDescriptorProto_Type.INT32),
          field("truth", 4, FieldDescriptorProto_Type.BOOL, { defaultValue: "true" }),
          field("float", 5, FieldDescriptorProto_Type.FLOAT, { defaultValue: "1.25" }),
          field("nan", 6, FieldDescriptorProto_Type.FLOAT, { defaultValue: "nan" }),
          field("infinity", 7, FieldDescriptorProto_Type.FLOAT, { defaultValue: "inf" }),
        ]),
      ],
    });
    const current = descriptorFile({
      syntax: "proto2",
      messages: [
        orderMessage([
          field("integer", 1, FieldDescriptorProto_Type.INT64, { defaultValue: "1" }),
          field("text", 2, FieldDescriptorProto_Type.BYTES, { defaultValue: "xyz" }),
          field("zero", 3, FieldDescriptorProto_Type.UINT64, { defaultValue: "0" }),
          field("truth", 4, FieldDescriptorProto_Type.INT32, { defaultValue: "1" }),
          field("float", 5, FieldDescriptorProto_Type.FLOAT, { defaultValue: "1.25" }),
          field("nan", 6, FieldDescriptorProto_Type.FLOAT, { defaultValue: "nan" }),
          field("infinity", 7, FieldDescriptorProto_Type.FLOAT, { defaultValue: "inf" }),
        ]),
      ],
    });

    expect(kafkaProtobufWireCompatibilityIssues(previous, current)).toStrictEqual([]);
  });

  it("matches Buf FIELD_SAME_DEFAULT numeric and non-default partitions", () => {
    const makeDefaults = ({
      integerType = FieldDescriptorProto_Type.INT64,
      integerDefault = "9007199254740993",
      floatType = FieldDescriptorProto_Type.FLOAT,
      floatDefault = "1.1",
      nanDefault = "nan",
      messageType = false,
      infinityDefault = "inf",
      booleanDefault = "true",
      enumDefault = "STATE_UNSPECIFIED",
    }: {
      readonly integerType?: FieldDescriptorProto_Type;
      readonly integerDefault?: string;
      readonly floatType?: FieldDescriptorProto_Type;
      readonly floatDefault?: string;
      readonly nanDefault?: string;
      readonly messageType?: boolean;
      readonly infinityDefault?: string;
      readonly booleanDefault?: string;
      readonly enumDefault?: string;
    } = {}) =>
      descriptorFile({
        syntax: "proto2",
        enums: [
          {
            name: "State",
            values: [
              ["STATE_UNSPECIFIED", 0],
              ["STATE_READY", 1],
            ],
          },
        ],
        messages: [
          { name: "Child" },
          orderMessage([
            field("integer", 1, integerType, { defaultValue: integerDefault }),
            field("float", 2, floatType, { defaultValue: floatDefault }),
            field("nan", 3, FieldDescriptorProto_Type.DOUBLE, { defaultValue: nanDefault }),
            messageType
              ? field("kind", 4, FieldDescriptorProto_Type.MESSAGE, {
                  typeName: ".example.Child",
                })
              : field("kind", 4, FieldDescriptorProto_Type.INT32),
            field("infinity", 5, FieldDescriptorProto_Type.DOUBLE, {
              defaultValue: infinityDefault,
            }),
            field("boolean", 6, FieldDescriptorProto_Type.BOOL, {
              defaultValue: booleanDefault,
            }),
            field("enumeration", 7, FieldDescriptorProto_Type.ENUM, {
              typeName: ".example.State",
              defaultValue: enumDefault,
            }),
          ]),
        ],
      });
    const previous = makeDefaults();
    const current = makeDefaults({
      integerType: FieldDescriptorProto_Type.UINT64,
      floatType: FieldDescriptorProto_Type.DOUBLE,
      floatDefault: String(Math.fround(1.1)),
      nanDefault: "1",
      messageType: true,
    });
    const reversed = makeDefaults({ nanDefault: "1" });
    const negativeInfinity = makeDefaults({ infinityDefault: "-inf" });
    const changedBooleanAndEnumDefaults = makeDefaults({
      booleanDefault: "false",
      enumDefault: "STATE_READY",
    });
    const implicit = descriptorFile({
      syntax: "proto2",
      enums: [
        {
          name: "State",
          values: [
            ["STATE_UNSPECIFIED", 0],
            ["STATE_READY", 1],
          ],
        },
      ],
      messages: [
        { name: "Child" },
        orderMessage([
          field("integer", 1, FieldDescriptorProto_Type.INT64),
          field("float", 2, FieldDescriptorProto_Type.FLOAT),
          field("nan", 3, FieldDescriptorProto_Type.DOUBLE),
          field("kind", 4, FieldDescriptorProto_Type.INT32),
          field("infinity", 5, FieldDescriptorProto_Type.DOUBLE),
          field("boolean", 6, FieldDescriptorProto_Type.BOOL),
          field("enumeration", 7, FieldDescriptorProto_Type.ENUM, {
            typeName: ".example.State",
          }),
        ]),
      ],
    });
    const messageToScalar = descriptorFile({
      syntax: "proto2",
      messages: [
        { name: "Child" },
        orderMessage([
          field("kind", 4, FieldDescriptorProto_Type.STRING),
          field("infinity", 5, FieldDescriptorProto_Type.DOUBLE),
        ]),
      ],
    });
    const rulePaths = (left: DescFile, right: DescFile) =>
      kafkaProtobufWireCompatibilityIssues(left, right).map(({ rule, path }) => ({ rule, path }));

    expect(rulePaths(previous, current)).toStrictEqual([
      { rule: "FIELD_WIRE_COMPATIBLE_TYPE", path: "example.Order.float" },
      { rule: "FIELD_SAME_DEFAULT", path: "example.Order.nan" },
      { rule: "FIELD_WIRE_COMPATIBLE_TYPE", path: "example.Order.kind" },
    ]);
    expect(rulePaths(current, reversed)).toStrictEqual([
      { rule: "FIELD_WIRE_COMPATIBLE_TYPE", path: "example.Order.float" },
      { rule: "FIELD_WIRE_COMPATIBLE_TYPE", path: "example.Order.kind" },
    ]);
    expect(rulePaths(previous, negativeInfinity)).toStrictEqual([
      { rule: "FIELD_SAME_DEFAULT", path: "example.Order.infinity" },
    ]);
    expect(rulePaths(current, changedBooleanAndEnumDefaults)).toStrictEqual([
      { rule: "FIELD_WIRE_COMPATIBLE_TYPE", path: "example.Order.float" },
      { rule: "FIELD_SAME_DEFAULT", path: "example.Order.nan" },
      { rule: "FIELD_WIRE_COMPATIBLE_TYPE", path: "example.Order.kind" },
      { rule: "FIELD_SAME_DEFAULT", path: "example.Order.boolean" },
      { rule: "FIELD_SAME_DEFAULT", path: "example.Order.enumeration" },
    ]);
    expect(rulePaths(implicit, implicit)).toStrictEqual([]);
    expect(rulePaths(current, messageToScalar)).toStrictEqual([
      { rule: "FIELD_NO_DELETE_UNLESS_NUMBER_RESERVED", path: "example.Order.integer" },
      { rule: "FIELD_NO_DELETE_UNLESS_NUMBER_RESERVED", path: "example.Order.float" },
      { rule: "FIELD_NO_DELETE_UNLESS_NUMBER_RESERVED", path: "example.Order.nan" },
      { rule: "FIELD_WIRE_COMPATIBLE_TYPE", path: "example.Order.kind" },
      { rule: "FIELD_SAME_DEFAULT", path: "example.Order.infinity" },
      { rule: "FIELD_NO_DELETE_UNLESS_NUMBER_RESERVED", path: "example.Order.boolean" },
      {
        rule: "FIELD_NO_DELETE_UNLESS_NUMBER_RESERVED",
        path: "example.Order.enumeration",
      },
    ]);
  });

  it("rejects incompatible enum, message, list, map, and cross-kind field values", () => {
    const makeKinds = (changed: boolean) =>
      descriptorFile({
        enums: [
          { name: "State", values: [["STATE_UNSPECIFIED", 0]] },
          { name: "OtherState", values: [["OTHER_STATE_UNSPECIFIED", 0]] },
        ],
        messages: [
          { name: "Child" },
          { name: "Other" },
          orderMessage(
            [
              field("state", 1, FieldDescriptorProto_Type.ENUM, {
                typeName: changed ? ".example.OtherState" : ".example.State",
              }),
              field("child", 2, FieldDescriptorProto_Type.MESSAGE, {
                typeName: changed ? ".example.Other" : ".example.Child",
              }),
              field("states", 3, FieldDescriptorProto_Type.ENUM, {
                typeName: changed ? ".example.OtherState" : ".example.State",
                label: FieldDescriptorProto_Label.REPEATED,
              }),
              field("children", 4, FieldDescriptorProto_Type.MESSAGE, {
                typeName: changed ? ".example.Other" : ".example.Child",
                label: FieldDescriptorProto_Label.REPEATED,
              }),
              field("lookup", 5, FieldDescriptorProto_Type.MESSAGE, {
                typeName: ".example.Order.LookupEntry",
                label: FieldDescriptorProto_Label.REPEATED,
              }),
              changed
                ? field("cross_kind", 6, FieldDescriptorProto_Type.MESSAGE, {
                    typeName: ".example.Child",
                  })
                : field("cross_kind", 6, FieldDescriptorProto_Type.INT32),
            ],
            {
              nested: [
                {
                  name: "LookupEntry",
                  mapEntry: true,
                  fields: [
                    field("key", 1, FieldDescriptorProto_Type.STRING),
                    field(
                      "value",
                      2,
                      changed ? FieldDescriptorProto_Type.SINT32 : FieldDescriptorProto_Type.INT32,
                    ),
                  ],
                },
              ],
            },
          ),
        ],
      });
    const previous = makeKinds(false).messages.find((message) => message.name === "Order");
    const current = makeKinds(true).messages.find((message) => message.name === "Order");
    if (previous === undefined || current === undefined) {
      throw new Error("Order descriptors missing");
    }

    expect(
      kafkaProtobufMessageWireCompatibilityIssues(previous, current).map(({ rule }) => rule),
    ).toStrictEqual([
      "FIELD_WIRE_COMPATIBLE_TYPE",
      "FIELD_WIRE_COMPATIBLE_TYPE",
      "FIELD_WIRE_COMPATIBLE_TYPE",
      "FIELD_WIRE_COMPATIBLE_TYPE",
      "FIELD_WIRE_COMPATIBLE_TYPE",
      "FIELD_WIRE_COMPATIBLE_TYPE",
    ]);
  });

  it("validates Buf WIRE-compatible map and repeated map-entry message transitions recursively", () => {
    const makeMapField = (
      mapEntry: boolean,
      qualifiedTypeName = true,
      valueType = FieldDescriptorProto_Type.INT32,
    ) =>
      descriptorFile({
        messages: [
          orderMessage(
            [
              field("labels", 1, FieldDescriptorProto_Type.MESSAGE, {
                typeName: `${qualifiedTypeName ? "." : ""}example.Order.LabelsEntry`,
                label: FieldDescriptorProto_Label.REPEATED,
              }),
            ],
            {
              nested: [
                {
                  name: "LabelsEntry",
                  mapEntry,
                  fields: [
                    field("key", 1, FieldDescriptorProto_Type.STRING),
                    field("value", 2, valueType),
                  ],
                },
              ],
            },
          ),
        ],
      });
    const mapped = makeMapField(true);
    const unqualifiedMap = makeMapField(true, false);
    const repeatedEntry = makeMapField(false);

    expect(rules(mapped, repeatedEntry)).toStrictEqual([]);
    expect(rules(repeatedEntry, mapped)).toStrictEqual([]);
    expect(rules(unqualifiedMap, repeatedEntry)).toStrictEqual([]);
    expect(
      rules(mapped, makeMapField(false, true, FieldDescriptorProto_Type.SINT32)),
    ).toStrictEqual(["FIELD_WIRE_COMPATIBLE_TYPE"]);
    expect(
      kafkaProtobufMessageReaderCompatibilityIssues(
        mapped.messages[0]!,
        makeMapField(false, true, FieldDescriptorProto_Type.SINT32).messages[0]!,
      ).map(({ rule, path }) => ({ rule, path })),
    ).toStrictEqual([
      {
        rule: "FIELD_WIRE_COMPATIBLE_TYPE",
        path: "example.Order.LabelsEntry.value",
      },
    ]);
    expect(
      kafkaProtobufMessageWireCompatibilityIssues(
        repeatedEntry.messages[0]!,
        unqualifiedMap.messages[0]!,
      ),
    ).toStrictEqual([]);
  });

  it("recurses through compatible map value graphs and skips changed map type identities", () => {
    const makeMapGraph = (input: {
      readonly childType: FieldDescriptorProto_Type;
      readonly messageTypeName: string;
      readonly enumTypeName: string;
    }) =>
      descriptorFile({
        enums: [
          { name: "State", values: [["STATE_UNSPECIFIED", 0]] },
          { name: "OtherState", values: [["OTHER_STATE_UNSPECIFIED", 0]] },
        ],
        messages: [
          {
            name: "Child",
            fields: [field("value", 1, input.childType)],
          },
          {
            name: "OtherChild",
          },
          orderMessage(
            [
              field("message_map", 1, FieldDescriptorProto_Type.MESSAGE, {
                typeName: ".example.Order.MessageMapEntry",
                label: FieldDescriptorProto_Label.REPEATED,
              }),
              field("enum_map", 2, FieldDescriptorProto_Type.MESSAGE, {
                typeName: ".example.Order.EnumMapEntry",
                label: FieldDescriptorProto_Label.REPEATED,
              }),
            ],
            {
              nested: [
                {
                  name: "MessageMapEntry",
                  mapEntry: true,
                  fields: [
                    field("key", 1, FieldDescriptorProto_Type.STRING),
                    field("value", 2, FieldDescriptorProto_Type.MESSAGE, {
                      typeName: input.messageTypeName,
                    }),
                  ],
                },
                {
                  name: "EnumMapEntry",
                  mapEntry: true,
                  fields: [
                    field("key", 1, FieldDescriptorProto_Type.STRING),
                    field("value", 2, FieldDescriptorProto_Type.ENUM, {
                      typeName: input.enumTypeName,
                    }),
                  ],
                },
              ],
            },
          ),
        ],
      });
    const previous = makeMapGraph({
      childType: FieldDescriptorProto_Type.STRING,
      messageTypeName: ".example.Child",
      enumTypeName: ".example.State",
    });
    const compatibleChild = makeMapGraph({
      childType: FieldDescriptorProto_Type.BYTES,
      messageTypeName: ".example.Child",
      enumTypeName: ".example.State",
    });
    const changedIdentities = makeMapGraph({
      childType: FieldDescriptorProto_Type.STRING,
      messageTypeName: ".example.OtherChild",
      enumTypeName: ".example.OtherState",
    });
    const previousOrder = Option.getOrThrow(
      Option.fromUndefinedOr(
        previous.messages.find((message) => message.typeName === "example.Order"),
      ),
    );
    const compatibleOrder = Option.getOrThrow(
      Option.fromUndefinedOr(
        compatibleChild.messages.find((message) => message.typeName === "example.Order"),
      ),
    );
    const changedOrder = Option.getOrThrow(
      Option.fromUndefinedOr(
        changedIdentities.messages.find((message) => message.typeName === "example.Order"),
      ),
    );

    expect(
      kafkaProtobufMessageWireCompatibilityIssues(previousOrder, compatibleOrder).map(
        ({ rule, path }) => ({ rule, path }),
      ),
    ).toStrictEqual([]);
    expect(
      kafkaProtobufMessageWireCompatibilityIssues(previousOrder, changedOrder).map(
        ({ rule }) => rule,
      ),
    ).toStrictEqual(["FIELD_WIRE_COMPATIBLE_TYPE", "FIELD_WIRE_COMPATIBLE_TYPE"]);
  });

  it("allows additive fields and deletion with a preserved numeric reservation", () => {
    const previous = descriptorFile({
      messages: [
        orderMessage([
          field("id", 1, FieldDescriptorProto_Type.STRING),
          field("legacy", 2, FieldDescriptorProto_Type.INT32),
        ]),
      ],
    });
    const added = descriptorFile({
      messages: [
        orderMessage([
          field("id", 1, FieldDescriptorProto_Type.STRING),
          field("legacy", 2, FieldDescriptorProto_Type.INT32),
          field("region", 3, FieldDescriptorProto_Type.STRING),
        ]),
      ],
    });
    const removedAndReserved = descriptorFile({
      messages: [
        orderMessage([field("id", 1, FieldDescriptorProto_Type.STRING)], {
          reservedNumbers: [[2, 3]],
        }),
      ],
    });

    expect(kafkaProtobufWireCompatibilityIssues(previous, added)).toStrictEqual([]);
    expect(kafkaProtobufWireCompatibilityIssues(previous, removedAndReserved)).toStrictEqual([]);
  });

  it("preserves required sets plus message and enum reservations", () => {
    const preserved = descriptorFile({
      syntax: "proto2",
      enums: [
        {
          name: "State",
          values: [["STATE_UNSPECIFIED", 0]],
          reservedNumbers: [[8, 9]],
          reservedNames: ["STATE_LEGACY"],
        },
      ],
      messages: [
        orderMessage(
          [
            field("second", 2, FieldDescriptorProto_Type.STRING, {
              label: FieldDescriptorProto_Label.REQUIRED,
            }),
            field("first", 1, FieldDescriptorProto_Type.STRING, {
              label: FieldDescriptorProto_Label.REQUIRED,
            }),
          ],
          {
            nested: [{ name: "Envelope", nested: [{ name: "Payload" }] }],
            reservedNumbers: [[10, 12]],
            reservedNames: ["legacy"],
          },
        ),
      ],
    });

    expect(kafkaProtobufWireCompatibilityIssues(preserved, preserved)).toStrictEqual([]);
  });

  it("rejects unreserved deletion and removal of numeric or named reservations", () => {
    const previous = descriptorFile({
      messages: [
        orderMessage([field("legacy", 2, FieldDescriptorProto_Type.INT32)], {
          reservedNumbers: [[8, 10]],
          reservedNames: ["older"],
        }),
      ],
    });
    const current = descriptorFile({ messages: [orderMessage([])] });

    expect(rules(previous, current)).toStrictEqual([
      "FIELD_NO_DELETE_UNLESS_NUMBER_RESERVED",
      "RESERVED_MESSAGE_NO_DELETE",
      "RESERVED_MESSAGE_NO_DELETE",
    ]);
  });

  it("implements scalar, cardinality, oneof, default, and required-field rules", () => {
    const previous = descriptorFile({
      syntax: "proto2",
      messages: [
        orderMessage(
          [
            field("compatible", 1, FieldDescriptorProto_Type.INT32),
            field("directional", 2, FieldDescriptorProto_Type.STRING),
            field("list", 3, FieldDescriptorProto_Type.INT32, {
              label: FieldDescriptorProto_Label.REPEATED,
            }),
            field("choice", 4, FieldDescriptorProto_Type.STRING, { oneofIndex: 0 }),
            field("defaulted", 5, FieldDescriptorProto_Type.INT32, { defaultValue: "1" }),
            field("required", 6, FieldDescriptorProto_Type.STRING, {
              label: FieldDescriptorProto_Label.REQUIRED,
            }),
          ],
          {
            oneofs: ["selection"],
          },
        ),
      ],
    });
    const current = descriptorFile({
      syntax: "proto2",
      messages: [
        orderMessage([
          field("compatible", 1, FieldDescriptorProto_Type.UINT64),
          field("directional", 2, FieldDescriptorProto_Type.BYTES),
          field("list", 3, FieldDescriptorProto_Type.INT32),
          field("choice", 4, FieldDescriptorProto_Type.STRING),
          field("defaulted", 5, FieldDescriptorProto_Type.INT32, { defaultValue: "2" }),
        ]),
      ],
    });

    expect(rules(previous, current)).toStrictEqual([
      "FIELD_WIRE_COMPATIBLE_CARDINALITY",
      "FIELD_SAME_ONEOF",
      "FIELD_SAME_DEFAULT",
      "FIELD_NO_DELETE_UNLESS_NUMBER_RESERVED",
      "MESSAGE_SAME_REQUIRED_FIELDS",
    ]);
    expect(rules(current, previous)).toStrictEqual([
      "FIELD_WIRE_COMPATIBLE_TYPE",
      "FIELD_WIRE_COMPATIBLE_CARDINALITY",
      "FIELD_SAME_ONEOF",
      "FIELD_SAME_DEFAULT",
      "MESSAGE_SAME_REQUIRED_FIELDS",
    ]);
  });

  it("matches Buf WIRE direction for string and bytes in history and generated readers", () => {
    const withScalar = (type: FieldDescriptorProto_Type) =>
      descriptorFile({ messages: [orderMessage([field("value", 1, type)])] });
    const stringFile = withScalar(FieldDescriptorProto_Type.STRING);
    const bytesFile = withScalar(FieldDescriptorProto_Type.BYTES);
    const stringMessage = orderDescriptor(stringFile);
    const bytesMessage = orderDescriptor(bytesFile);

    expect(rules(stringFile, bytesFile)).toStrictEqual([]);
    expect(rules(bytesFile, stringFile)).toStrictEqual(["FIELD_WIRE_COMPATIBLE_TYPE"]);
    expect(
      kafkaProtobufMessageReaderCompatibilityIssues(stringMessage, bytesMessage),
    ).toStrictEqual([]);
    expect(
      kafkaProtobufMessageReaderCompatibilityIssues(bytesMessage, stringMessage).map(
        ({ rule }) => rule,
      ),
    ).toStrictEqual(["FIELD_WIRE_COMPATIBLE_TYPE"]);
  });

  it("rejects enum and Buf integer types in both directions for history and generated readers", () => {
    const withType = (type: FieldDescriptorProto_Type) => {
      const options = type === FieldDescriptorProto_Type.ENUM ? { typeName: ".example.State" } : {};
      return descriptorFile({
        enums: [{ name: "State", values: [["STATE_UNSPECIFIED", 0]] }],
        messages: [orderMessage([field("value", 1, type, options)])],
      });
    };
    const enumFile = withType(FieldDescriptorProto_Type.ENUM);
    const enumMessage = orderDescriptor(enumFile);
    const integerTypes = [
      FieldDescriptorProto_Type.INT32,
      FieldDescriptorProto_Type.UINT32,
      FieldDescriptorProto_Type.INT64,
      FieldDescriptorProto_Type.UINT64,
    ];

    for (const integerType of integerTypes) {
      const integerFile = withType(integerType);
      const integerMessage = orderDescriptor(integerFile);

      expect(rules(enumFile, integerFile)).toStrictEqual(["FIELD_WIRE_COMPATIBLE_TYPE"]);
      expect(rules(integerFile, enumFile)).toStrictEqual(["FIELD_WIRE_COMPATIBLE_TYPE"]);
      expect(
        kafkaProtobufMessageReaderCompatibilityIssues(enumMessage, integerMessage).map(
          ({ rule }) => rule,
        ),
      ).toStrictEqual(["FIELD_WIRE_COMPATIBLE_TYPE"]);
      expect(
        kafkaProtobufMessageReaderCompatibilityIssues(integerMessage, enumMessage).map(
          ({ rule }) => rule,
        ),
      ).toStrictEqual(["FIELD_WIRE_COMPATIBLE_TYPE"]);
    }
  });

  it("rejects singular and repeated string, bytes, and message fields in both directions", () => {
    const withCardinality = (
      type: FieldDescriptorProto_Type,
      label: FieldDescriptorProto_Label,
    ) => {
      const options =
        type === FieldDescriptorProto_Type.MESSAGE
          ? { label, typeName: ".example.Child" }
          : { label };
      return descriptorFile({
        messages: [{ name: "Child" }, orderMessage([field("value", 1, type, options)])],
      });
    };
    const valueTypes = [
      FieldDescriptorProto_Type.STRING,
      FieldDescriptorProto_Type.BYTES,
      FieldDescriptorProto_Type.MESSAGE,
    ];

    for (const valueType of valueTypes) {
      const singularFile = withCardinality(valueType, FieldDescriptorProto_Label.OPTIONAL);
      const repeatedFile = withCardinality(valueType, FieldDescriptorProto_Label.REPEATED);
      const singularMessage = orderDescriptor(singularFile);
      const repeatedMessage = orderDescriptor(repeatedFile);

      expect(rules(singularFile, repeatedFile)).toStrictEqual([
        "FIELD_WIRE_COMPATIBLE_CARDINALITY",
      ]);
      expect(rules(repeatedFile, singularFile)).toStrictEqual([
        "FIELD_WIRE_COMPATIBLE_CARDINALITY",
      ]);
      expect(
        kafkaProtobufMessageReaderCompatibilityIssues(singularMessage, repeatedMessage).map(
          ({ rule }) => rule,
        ),
      ).toStrictEqual(["FIELD_WIRE_COMPATIBLE_CARDINALITY"]);
      expect(
        kafkaProtobufMessageReaderCompatibilityIssues(repeatedMessage, singularMessage).map(
          ({ rule }) => rule,
        ),
      ).toStrictEqual(["FIELD_WIRE_COMPATIBLE_CARDINALITY"]);
    }
  });

  it("rejects required reader fields absent from writer message graphs", () => {
    const writer = descriptorFile({
      syntax: "proto2",
      messages: [
        orderMessage([
          field("child", 1, FieldDescriptorProto_Type.MESSAGE, {
            typeName: ".example.Child",
          }),
        ]),
        { name: "Child" },
      ],
    });
    const reader = descriptorFile({
      syntax: "proto2",
      messages: [
        orderMessage([
          field("child", 1, FieldDescriptorProto_Type.MESSAGE, {
            typeName: ".example.Child",
          }),
          field("root_required", 2, FieldDescriptorProto_Type.STRING, {
            label: FieldDescriptorProto_Label.REQUIRED,
          }),
        ]),
        {
          name: "Child",
          fields: [
            field("nested_required", 2, FieldDescriptorProto_Type.STRING, {
              label: FieldDescriptorProto_Label.REQUIRED,
            }),
          ],
        },
      ],
    });
    const writerOrder = writer.messages.find((message) => message.typeName === "example.Order");
    const readerOrder = reader.messages.find((message) => message.typeName === "example.Order");
    if (writerOrder === undefined || readerOrder === undefined) {
      throw new Error("required-field graph messages missing");
    }

    expect(kafkaProtobufMessageReaderCompatibilityIssues(writerOrder, readerOrder)).toStrictEqual([
      {
        rule: "MESSAGE_SAME_REQUIRED_FIELDS",
        path: "example.Order.root_required",
        message: "Required reader field number 2 is absent from the writer.",
      },
      {
        rule: "MESSAGE_SAME_REQUIRED_FIELDS",
        path: "example.Child.nested_required",
        message: "Required reader field number 2 is absent from the writer.",
      },
    ]);
  });

  it("checks enum subsets, enum reservations, and file packages", () => {
    const previous = descriptorFile({
      enums: [
        {
          name: "State",
          values: [
            ["STATE_UNSPECIFIED", 0],
            ["STATE_ACTIVE", 1],
          ],
          reservedNumbers: [[8, 9]],
          reservedNames: ["STATE_OLD"],
        },
      ],
      messages: [
        orderMessage([
          field("state", 1, FieldDescriptorProto_Type.ENUM, {
            typeName: ".example.State",
          }),
        ]),
      ],
    });
    const current = descriptorFile({
      package: "renamed",
      enums: [
        {
          name: "State",
          values: [["STATE_UNSPECIFIED", 0]],
        },
      ],
      messages: [
        orderMessage([
          field("state", 1, FieldDescriptorProto_Type.ENUM, {
            typeName: ".renamed.State",
          }),
        ]),
      ],
    });

    expect(rules(previous, current)).toStrictEqual(["FILE_SAME_PACKAGE"]);

    const samePackage = descriptorFile({
      enums: [
        {
          name: "State",
          values: [["STATE_UNSPECIFIED", 0]],
        },
      ],
      messages: [
        orderMessage([
          field("state", 1, FieldDescriptorProto_Type.ENUM, {
            typeName: ".example.State",
          }),
        ]),
      ],
    });
    expect(rules(previous, samePackage)).toStrictEqual([
      "ENUM_VALUE_NO_DELETE_UNLESS_NUMBER_RESERVED",
      "RESERVED_ENUM_NO_DELETE",
      "RESERVED_ENUM_NO_DELETE",
    ]);
  });

  it("allows WIRE enum renames and reserved-number deletion while checking moved enum types", () => {
    const previous = descriptorFile({
      enums: [
        {
          name: "State",
          values: [
            ["STATE_UNSPECIFIED", 0],
            ["STATE_ACTIVE", 1],
          ],
        },
      ],
      messages: [
        orderMessage([
          field("state", 1, FieldDescriptorProto_Type.ENUM, {
            typeName: ".example.State",
          }),
        ]),
      ],
    });
    const renamed = descriptorFile({
      enums: [
        {
          name: "State",
          values: [
            ["STATE_UNKNOWN", 0],
            ["STATE_ENABLED", 1],
          ],
        },
      ],
      messages: [
        orderMessage([
          field("state", 1, FieldDescriptorProto_Type.ENUM, {
            typeName: ".example.State",
          }),
        ]),
      ],
    });
    const removedAndReserved = descriptorFile({
      enums: [
        {
          name: "State",
          values: [["STATE_UNSPECIFIED", 0]],
          reservedNumbers: [[1, 1]],
        },
      ],
      messages: [
        orderMessage([
          field("state", 1, FieldDescriptorProto_Type.ENUM, {
            typeName: ".example.State",
          }),
        ]),
      ],
    });
    const moved = descriptorFile({
      enums: [
        {
          name: "State",
          values: [["STATE_UNSPECIFIED", 0]],
        },
        {
          name: "Replacement",
          values: [["STATE_UNSPECIFIED", 0]],
        },
      ],
      messages: [
        orderMessage([
          field("state", 1, FieldDescriptorProto_Type.ENUM, {
            typeName: ".example.Replacement",
          }),
        ]),
      ],
    });

    expect(kafkaProtobufWireCompatibilityIssues(previous, renamed)).toStrictEqual([]);
    expect(kafkaProtobufWireCompatibilityIssues(previous, removedAndReserved)).toStrictEqual([]);
    expect(rules(previous, moved)).toStrictEqual([
      "ENUM_VALUE_NO_DELETE_UNLESS_NUMBER_RESERVED",
      "FIELD_WIRE_COMPATIBLE_TYPE",
    ]);
    const sameShortNameMoved = descriptorFile({
      package: "replacement",
      enums: [
        {
          name: "State",
          values: [
            ["STATE_UNSPECIFIED", 0],
            ["STATE_ACTIVE", 1],
          ],
        },
      ],
      messages: [
        orderMessage([
          field("state", 1, FieldDescriptorProto_Type.ENUM, {
            typeName: ".replacement.State",
          }),
        ]),
      ],
    });
    const previousMessage = previous.messages.find(
      (message) => message.typeName === "example.Order",
    );
    const movedMessage = sameShortNameMoved.messages.find(
      (message) => message.typeName === "replacement.Order",
    );
    if (previousMessage === undefined || movedMessage === undefined) {
      throw new Error("enum move messages missing");
    }
    expect(
      kafkaProtobufMessageWireCompatibilityIssues(previousMessage, movedMessage),
    ).toStrictEqual([]);
  });

  it("checks top-level and nested extensions by extendee and field number", () => {
    const previous = descriptorFile({
      syntax: "proto2",
      messages: [
        {
          name: "Extendee",
        },
      ],
      extensions: [
        {
          name: "priority",
          number: 100,
          type: FieldDescriptorProto_Type.INT32,
          extendee: ".example.Extendee",
          defaultValue: "1",
        },
      ],
    });
    const current = descriptorFile({
      syntax: "proto2",
      messages: [
        {
          name: "Extendee",
          nested: [
            {
              name: "Holder",
              extensions: [
                {
                  name: "renamed_priority",
                  number: 100,
                  type: FieldDescriptorProto_Type.SINT64,
                  extendee: ".example.Extendee",
                  label: FieldDescriptorProto_Label.REPEATED,
                },
              ],
            },
          ],
        },
      ],
    });

    expect(rules(previous, current)).toStrictEqual([
      "FIELD_WIRE_COMPATIBLE_CARDINALITY",
      "FIELD_WIRE_COMPATIBLE_TYPE",
    ]);
    const removedAndReserved = descriptorFile({
      syntax: "proto2",
      messages: [{ name: "Extendee", reservedNumbers: [[100, 101]] }],
    });
    expect(kafkaProtobufWireCompatibilityIssues(previous, removedAndReserved)).toStrictEqual([]);

    const removedWithoutReservation = descriptorFile({
      syntax: "proto2",
      messages: [{ name: "Extendee" }],
    });
    expect(kafkaProtobufWireCompatibilityIssues(previous, removedWithoutReservation)).toStrictEqual(
      [
        {
          rule: "FIELD_NO_DELETE_UNLESS_NUMBER_RESERVED",
          path: "example.Extendee.100",
          message: "Field number 100 was deleted without being reserved.",
        },
      ],
    );
  });

  it("checks the WIRE RPC signature rules for methods retained by name", () => {
    const messages = [{ name: "Request" }, { name: "Response" }, { name: "Replacement" }];
    const previous = descriptorFile({
      messages,
      methods: [
        {
          name: "Get",
          inputType: ".example.Request",
          outputType: ".example.Response",
          idempotency: MethodOptions_IdempotencyLevel.NO_SIDE_EFFECTS,
        },
      ],
    });
    const current = descriptorFile({
      messages,
      methods: [
        {
          name: "Get",
          inputType: ".example.Replacement",
          outputType: ".example.Replacement",
          clientStreaming: true,
          serverStreaming: true,
          idempotency: MethodOptions_IdempotencyLevel.IDEMPOTENT,
        },
      ],
    });

    expect(rules(previous, current)).toStrictEqual([
      "RPC_SAME_CLIENT_STREAMING",
      "RPC_SAME_SERVER_STREAMING",
      "RPC_SAME_REQUEST_TYPE",
      "RPC_SAME_RESPONSE_TYPE",
      "RPC_SAME_IDEMPOTENCY_LEVEL",
    ]);

    const bidi = descriptorFile({
      messages,
      methods: [
        {
          name: "Get",
          inputType: ".example.Request",
          outputType: ".example.Response",
          clientStreaming: true,
          serverStreaming: true,
        },
      ],
    });
    expect(rules(bidi, bidi)).toStrictEqual([]);

    const renamedFile = descriptorFile({ name: "renamed.proto", messages });
    expect(rules(previous, renamedFile)).toStrictEqual([]);
    const movedService = descriptorFile({
      name: "renamed.proto",
      messages,
      methods: [
        {
          name: "Get",
          inputType: ".example.Replacement",
          outputType: ".example.Response",
        },
      ],
    });
    expect(rules(previous, movedService)).toStrictEqual([
      "RPC_SAME_REQUEST_TYPE",
      "RPC_SAME_IDEMPOTENCY_LEVEL",
    ]);
    const serviceDeleted = descriptorFile({ messages });
    expect(rules(previous, serviceDeleted)).toStrictEqual([]);
    const methodDeleted = descriptorFile({
      messages,
      methods: [
        {
          name: "List",
          inputType: ".example.Request",
          outputType: ".example.Response",
        },
      ],
    });
    expect(rules(previous, methodDeleted)).toStrictEqual([]);
  });
});
