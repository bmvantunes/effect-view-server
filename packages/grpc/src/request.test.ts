import {
  create,
  fromJson,
  toBinary,
  type DescField,
  type DescMessage,
  type Message,
} from "@bufbuild/protobuf";
import { fileDesc, messageDesc } from "@bufbuild/protobuf/codegenv2";
import {
  FieldDescriptorProto_Label,
  FieldDescriptorProto_Type,
  FieldOptions_JSType,
  FileDescriptorProtoSchema,
  ListValueSchema,
  StringValueSchema,
  StructSchema,
  ValueSchema,
} from "@bufbuild/protobuf/wkt";
import { describe, expect, it } from "@effect/vitest";
import { Buffer } from "node:buffer";
import { validateAndSnapshotGrpcRequest } from "./request";

type RequestMessage = Message<"grpc.request.Request">;

const scalarField = (
  name: string,
  number: number,
  type: FieldDescriptorProto_Type,
  options?: {
    readonly jstype?: FieldOptions_JSType;
    readonly label?: FieldDescriptorProto_Label;
  },
) => {
  if (options?.jstype !== undefined) {
    return options.label === undefined
      ? { name, number, type, options: { jstype: options.jstype } }
      : { name, number, type, options: { jstype: options.jstype }, label: options.label };
  }
  return options?.label === undefined
    ? { name, number, type }
    : { name, number, type, label: options.label };
};

const descriptorFile = fileDesc(
  globalThis.btoa(
    Array.from(
      toBinary(
        FileDescriptorProtoSchema,
        create(FileDescriptorProtoSchema, {
          name: "grpc/request.proto",
          package: "grpc.request",
          syntax: "proto3",
          enumType: [
            {
              name: "Mode",
              value: [
                { name: "MODE_UNSPECIFIED", number: 0 },
                { name: "MODE_ACTIVE", number: 1 },
              ],
            },
          ],
          messageType: [
            {
              name: "Child",
              field: [scalarField("label", 1, FieldDescriptorProto_Type.STRING)],
            },
            {
              name: "Node",
              field: [
                {
                  name: "child",
                  number: 1,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.request.Node",
                },
              ],
            },
            {
              name: "Request",
              oneofDecl: [{ name: "selector" }],
              nestedType: [
                {
                  name: "ScalarMapEntry",
                  options: { mapEntry: true },
                  field: [
                    scalarField("key", 1, FieldDescriptorProto_Type.STRING),
                    scalarField("value", 2, FieldDescriptorProto_Type.INT32),
                  ],
                },
                {
                  name: "EnumMapEntry",
                  options: { mapEntry: true },
                  field: [
                    scalarField("key", 1, FieldDescriptorProto_Type.STRING),
                    {
                      name: "value",
                      number: 2,
                      type: FieldDescriptorProto_Type.ENUM,
                      typeName: ".grpc.request.Mode",
                    },
                  ],
                },
                {
                  name: "MessageMapEntry",
                  options: { mapEntry: true },
                  field: [
                    scalarField("key", 1, FieldDescriptorProto_Type.STRING),
                    {
                      name: "value",
                      number: 2,
                      type: FieldDescriptorProto_Type.MESSAGE,
                      typeName: ".grpc.request.Child",
                    },
                  ],
                },
                {
                  name: "Int32MapEntry",
                  options: { mapEntry: true },
                  field: [
                    scalarField("key", 1, FieldDescriptorProto_Type.INT32),
                    scalarField("value", 2, FieldDescriptorProto_Type.STRING),
                  ],
                },
                {
                  name: "BoolMapEntry",
                  options: { mapEntry: true },
                  field: [
                    scalarField("key", 1, FieldDescriptorProto_Type.BOOL),
                    scalarField("value", 2, FieldDescriptorProto_Type.STRING),
                  ],
                },
                {
                  name: "Int64MapEntry",
                  options: { mapEntry: true },
                  field: [
                    scalarField("key", 1, FieldDescriptorProto_Type.INT64),
                    scalarField("value", 2, FieldDescriptorProto_Type.STRING),
                  ],
                },
                {
                  name: "Uint64MapEntry",
                  options: { mapEntry: true },
                  field: [
                    scalarField("key", 1, FieldDescriptorProto_Type.UINT64),
                    scalarField("value", 2, FieldDescriptorProto_Type.STRING),
                  ],
                },
                {
                  name: "Uint32MapEntry",
                  options: { mapEntry: true },
                  field: [
                    scalarField("key", 1, FieldDescriptorProto_Type.UINT32),
                    scalarField("value", 2, FieldDescriptorProto_Type.STRING),
                  ],
                },
                {
                  name: "Fixed32MapEntry",
                  options: { mapEntry: true },
                  field: [
                    scalarField("key", 1, FieldDescriptorProto_Type.FIXED32),
                    scalarField("value", 2, FieldDescriptorProto_Type.STRING),
                  ],
                },
                {
                  name: "BytesMapEntry",
                  options: { mapEntry: true },
                  field: [
                    scalarField("key", 1, FieldDescriptorProto_Type.STRING),
                    scalarField("value", 2, FieldDescriptorProto_Type.BYTES),
                  ],
                },
              ],
              field: [
                scalarField("double_value", 1, FieldDescriptorProto_Type.DOUBLE),
                scalarField("float_value", 2, FieldDescriptorProto_Type.FLOAT),
                scalarField("int32_value", 3, FieldDescriptorProto_Type.INT32),
                scalarField("fixed32_value", 4, FieldDescriptorProto_Type.FIXED32),
                scalarField("uint32_value", 5, FieldDescriptorProto_Type.UINT32),
                scalarField("sfixed32_value", 6, FieldDescriptorProto_Type.SFIXED32),
                scalarField("sint32_value", 7, FieldDescriptorProto_Type.SINT32),
                scalarField("int64_value", 8, FieldDescriptorProto_Type.INT64),
                scalarField("uint64_value", 9, FieldDescriptorProto_Type.UINT64),
                scalarField("fixed64_value", 10, FieldDescriptorProto_Type.FIXED64),
                scalarField("sfixed64_value", 11, FieldDescriptorProto_Type.SFIXED64),
                scalarField("sint64_value", 12, FieldDescriptorProto_Type.SINT64),
                scalarField("string_int64_value", 13, FieldDescriptorProto_Type.INT64, {
                  jstype: FieldOptions_JSType.JS_STRING,
                }),
                scalarField("bool_value", 14, FieldDescriptorProto_Type.BOOL),
                scalarField("string_value", 15, FieldDescriptorProto_Type.STRING),
                scalarField("bytes_value", 16, FieldDescriptorProto_Type.BYTES),
                {
                  name: "mode",
                  number: 17,
                  type: FieldDescriptorProto_Type.ENUM,
                  typeName: ".grpc.request.Mode",
                },
                {
                  name: "child",
                  number: 18,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.request.Child",
                },
                scalarField("numbers", 19, FieldDescriptorProto_Type.INT32, {
                  label: FieldDescriptorProto_Label.REPEATED,
                }),
                {
                  name: "modes",
                  number: 20,
                  label: FieldDescriptorProto_Label.REPEATED,
                  type: FieldDescriptorProto_Type.ENUM,
                  typeName: ".grpc.request.Mode",
                },
                {
                  name: "children",
                  number: 21,
                  label: FieldDescriptorProto_Label.REPEATED,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.request.Child",
                },
                {
                  name: "scalar_map",
                  number: 22,
                  label: FieldDescriptorProto_Label.REPEATED,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.request.Request.ScalarMapEntry",
                },
                {
                  name: "enum_map",
                  number: 23,
                  label: FieldDescriptorProto_Label.REPEATED,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.request.Request.EnumMapEntry",
                },
                {
                  name: "message_map",
                  number: 24,
                  label: FieldDescriptorProto_Label.REPEATED,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.request.Request.MessageMapEntry",
                },
                {
                  name: "text_choice",
                  number: 25,
                  type: FieldDescriptorProto_Type.STRING,
                  oneofIndex: 0,
                },
                {
                  name: "child_choice",
                  number: 26,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.request.Child",
                  oneofIndex: 0,
                },
                {
                  name: "node",
                  number: 27,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.request.Node",
                },
                {
                  name: "int_choice",
                  number: 28,
                  type: FieldDescriptorProto_Type.INT32,
                  oneofIndex: 0,
                },
                scalarField("string_uint64_value", 29, FieldDescriptorProto_Type.UINT64, {
                  jstype: FieldOptions_JSType.JS_STRING,
                }),
                {
                  name: "int32_map",
                  number: 30,
                  label: FieldDescriptorProto_Label.REPEATED,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.request.Request.Int32MapEntry",
                },
                {
                  name: "bool_map",
                  number: 31,
                  label: FieldDescriptorProto_Label.REPEATED,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.request.Request.BoolMapEntry",
                },
                {
                  name: "int64_map",
                  number: 32,
                  label: FieldDescriptorProto_Label.REPEATED,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.request.Request.Int64MapEntry",
                },
                {
                  name: "uint64_map",
                  number: 33,
                  label: FieldDescriptorProto_Label.REPEATED,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.request.Request.Uint64MapEntry",
                },
                {
                  name: "uint32_map",
                  number: 34,
                  label: FieldDescriptorProto_Label.REPEATED,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.request.Request.Uint32MapEntry",
                },
                {
                  name: "fixed32_map",
                  number: 35,
                  label: FieldDescriptorProto_Label.REPEATED,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.request.Request.Fixed32MapEntry",
                },
                {
                  name: "mode_choice",
                  number: 36,
                  type: FieldDescriptorProto_Type.ENUM,
                  typeName: ".grpc.request.Mode",
                  oneofIndex: 0,
                },
                scalarField("bytes_values", 37, FieldDescriptorProto_Type.BYTES, {
                  label: FieldDescriptorProto_Label.REPEATED,
                }),
                {
                  name: "bytes_map",
                  number: 38,
                  label: FieldDescriptorProto_Label.REPEATED,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.request.Request.BytesMapEntry",
                },
                {
                  name: "bytes_choice",
                  number: 39,
                  type: FieldDescriptorProto_Type.BYTES,
                  oneofIndex: 0,
                },
              ],
            },
          ],
        }),
      ),
      (byte) => String.fromCharCode(byte),
    ).join(""),
  ),
);

const RequestSchema = messageDesc<RequestMessage>(descriptorFile, 2);

const closedEnumDescriptorFile = fileDesc(
  globalThis.btoa(
    Array.from(
      toBinary(
        FileDescriptorProtoSchema,
        create(FileDescriptorProtoSchema, {
          name: "grpc/closed-enum-request.proto",
          package: "grpc.closed",
          syntax: "proto2",
          enumType: [
            {
              name: "Mode",
              value: [
                { name: "MODE_UNSPECIFIED", number: 0 },
                { name: "MODE_ACTIVE", number: 1 },
              ],
            },
          ],
          messageType: [
            {
              name: "Request",
              field: [
                {
                  name: "mode",
                  number: 1,
                  type: FieldDescriptorProto_Type.ENUM,
                  typeName: ".grpc.closed.Mode",
                },
              ],
            },
          ],
        }),
      ),
      (byte) => String.fromCharCode(byte),
    ).join(""),
  ),
);

const ClosedEnumRequestSchema = messageDesc<RequestMessage>(closedEnumDescriptorFile, 0);

const requiredDescriptorFile = fileDesc(
  globalThis.btoa(
    Array.from(
      toBinary(
        FileDescriptorProtoSchema,
        create(FileDescriptorProtoSchema, {
          name: "grpc/required-request.proto",
          package: "grpc.required",
          syntax: "proto2",
          messageType: [
            {
              name: "Request",
              field: [
                scalarField("required_value", 1, FieldDescriptorProto_Type.STRING, {
                  label: FieldDescriptorProto_Label.REQUIRED,
                }),
              ],
            },
          ],
        }),
      ),
      (byte) => String.fromCharCode(byte),
    ).join(""),
  ),
);

const RequiredRequestSchema = messageDesc<RequestMessage>(requiredDescriptorFile, 0);

const wrapperDescriptorFile = fileDesc(
  globalThis.btoa(
    Array.from(
      toBinary(
        FileDescriptorProtoSchema,
        create(FileDescriptorProtoSchema, {
          name: "grpc/wrapper-request.proto",
          package: "grpc.wrapper",
          syntax: "proto3",
          dependency: ["google/protobuf/wrappers.proto"],
          messageType: [
            {
              name: "Request",
              field: [
                {
                  name: "wrapped",
                  number: 1,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".google.protobuf.StringValue",
                },
                {
                  name: "wrapped_values",
                  number: 2,
                  label: FieldDescriptorProto_Label.REPEATED,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".google.protobuf.StringValue",
                },
              ],
            },
          ],
        }),
      ),
      (byte) => String.fromCharCode(byte),
    ).join(""),
  ),
  [StringValueSchema.file],
);

const WrapperRequestSchema = messageDesc<RequestMessage>(wrapperDescriptorFile, 0);

const nativeWktDescriptorFile = fileDesc(
  globalThis.btoa(
    Array.from(
      toBinary(
        FileDescriptorProtoSchema,
        create(FileDescriptorProtoSchema, {
          name: "grpc/native-wkt-request.proto",
          package: "grpc.native_wkt",
          syntax: "proto3",
          dependency: ["google/protobuf/struct.proto", "google/protobuf/wrappers.proto"],
          messageType: [
            {
              name: "Request",
              oneofDecl: [{ name: "selection" }],
              nestedType: [
                {
                  name: "PayloadMapEntry",
                  options: { mapEntry: true },
                  field: [
                    scalarField("key", 1, FieldDescriptorProto_Type.STRING),
                    {
                      name: "value",
                      number: 2,
                      type: FieldDescriptorProto_Type.MESSAGE,
                      typeName: ".google.protobuf.Struct",
                    },
                  ],
                },
              ],
              field: [
                {
                  name: "payload",
                  number: 1,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".google.protobuf.Struct",
                },
                {
                  name: "wrapped_choice",
                  number: 2,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".google.protobuf.StringValue",
                  oneofIndex: 0,
                },
                {
                  name: "payloads",
                  number: 3,
                  label: FieldDescriptorProto_Label.REPEATED,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".google.protobuf.Struct",
                },
                {
                  name: "payload_map",
                  number: 4,
                  label: FieldDescriptorProto_Label.REPEATED,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".grpc.native_wkt.Request.PayloadMapEntry",
                },
                {
                  name: "struct_choice",
                  number: 5,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".google.protobuf.Struct",
                  oneofIndex: 0,
                },
                {
                  name: "value",
                  number: 6,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".google.protobuf.Value",
                },
                {
                  name: "list_value",
                  number: 7,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".google.protobuf.ListValue",
                },
              ],
            },
          ],
        }),
      ),
      (byte) => String.fromCharCode(byte),
    ).join(""),
  ),
  [StructSchema.file, StringValueSchema.file],
);

const NativeWktRequestSchema = messageDesc<RequestMessage>(nativeWktDescriptorFile, 0);

const validRequest = () => ({
  doubleValue: 1,
  floatValue: 2,
  int32Value: 3,
  fixed32Value: 4,
  uint32Value: 5,
  sfixed32Value: 6,
  sint32Value: 7,
  int64Value: 8n,
  uint64Value: 9n,
  fixed64Value: 10n,
  sfixed64Value: 11n,
  sint64Value: 12n,
  stringInt64Value: "13",
  stringUint64Value: "14",
  boolValue: true,
  stringValue: "value",
  bytesValue: new Uint8Array([1, 2]),
  mode: 1,
  child: { label: "child" },
  numbers: [1, 2],
  modes: [0, 1],
  children: [{ label: "one" }],
  scalarMap: { one: 1 },
  enumMap: { one: 1 },
  messageMap: { one: { label: "mapped" } },
  selector: { case: "textChoice", value: "selected" },
  node: { child: {} },
});

const isObjectRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const wireBytes = <Value>(message: DescMessage, value: Value): Uint8Array => {
  if (!isObjectRecord(value)) {
    throw new TypeError("Expected an object-valued request.");
  }
  return toBinary(message, create(message, value));
};

describe("generated gRPC request validation", () => {
  it("recursively validates, snapshots, clones bytes, and freezes request-init values", () => {
    const request = validRequest();
    const snapshot = validateAndSnapshotGrpcRequest(RequestSchema, request);
    const buffer = Buffer.from([3, 4]);
    const bufferSnapshot = validateAndSnapshotGrpcRequest(RequestSchema, {
      bytesValue: buffer,
    });
    buffer[0] = 9;
    if (typeof snapshot !== "object" || snapshot === null) {
      throw new TypeError("Expected a request snapshot.");
    }
    const snapshotBytes = Reflect.get(snapshot, "bytesValue");
    if (!(snapshotBytes instanceof Uint8Array)) {
      throw new TypeError("Expected snapshotted request bytes.");
    }
    snapshotBytes[0] = 9;
    const bytesAfterMutation = Reflect.get(snapshot, "bytesValue");
    const snapshotNumbers = Reflect.get(snapshot, "numbers");
    const snapshotChild = Reflect.get(snapshot, "child");
    const defaultSnapshot = validateAndSnapshotGrpcRequest(RequestSchema, {});
    if (typeof defaultSnapshot !== "object" || defaultSnapshot === null) {
      throw new TypeError("Expected a defaulted request snapshot.");
    }
    const nullPrototype = Object.create(null);
    nullPrototype.stringValue = "valid";
    const nullPrototypeSnapshot = validateAndSnapshotGrpcRequest(RequestSchema, nullPrototype);
    const emptyOneofSnapshot = validateAndSnapshotGrpcRequest(RequestSchema, {
      selector: { case: undefined },
    });
    const messageChoiceSnapshot = validateAndSnapshotGrpcRequest(RequestSchema, {
      selector: { case: "childChoice", value: { label: "choice" } },
    });
    if (typeof messageChoiceSnapshot !== "object" || messageChoiceSnapshot === null) {
      throw new TypeError("Expected a materialized request message.");
    }
    const messageChoice = Reflect.get(messageChoiceSnapshot, "selector");
    if (typeof messageChoice !== "object" || messageChoice === null) {
      throw new TypeError("Expected a materialized request oneof.");
    }
    const messageChoiceValue = Reflect.get(messageChoice, "value");

    expect({
      typeName: Reflect.get(snapshot, "$typeName"),
      rootFrozen: Object.isFrozen(snapshot),
      bytesCloned: snapshotBytes !== request.bytesValue,
      bytesAfterMutation,
      defaultNumbersFrozen: Object.isFrozen(Reflect.get(defaultSnapshot, "numbers")),
      defaultScalarMapFrozen: Object.isFrozen(Reflect.get(defaultSnapshot, "scalarMap")),
      listFrozen: Object.isFrozen(snapshotNumbers),
      childFrozen: Object.isFrozen(snapshotChild),
      snapshotWire: wireBytes(RequestSchema, snapshot),
      expectedWire: wireBytes(RequestSchema, request),
      nullPrototypeWire: wireBytes(RequestSchema, nullPrototypeSnapshot),
      expectedNullPrototypeWire: wireBytes(RequestSchema, nullPrototype),
      emptyOneofWire: wireBytes(RequestSchema, emptyOneofSnapshot),
      expectedEmptyOneofWire: wireBytes(RequestSchema, { selector: { case: undefined } }),
      messageChoiceWire: wireBytes(RequestSchema, messageChoiceSnapshot),
      expectedMessageChoiceWire: wireBytes(RequestSchema, {
        selector: { case: "childChoice", value: { label: "choice" } },
      }),
      messageChoiceFrozen: Object.isFrozen(messageChoice),
      messageChoiceValueFrozen: Object.isFrozen(messageChoiceValue),
      bufferBytes:
        typeof bufferSnapshot === "object" && bufferSnapshot !== null
          ? Reflect.get(bufferSnapshot, "bytesValue")
          : undefined,
    }).toStrictEqual({
      typeName: "grpc.request.Request",
      rootFrozen: true,
      bytesCloned: true,
      bytesAfterMutation: new Uint8Array([1, 2]),
      defaultNumbersFrozen: true,
      defaultScalarMapFrozen: true,
      listFrozen: true,
      childFrozen: true,
      snapshotWire: wireBytes(RequestSchema, request),
      expectedWire: wireBytes(RequestSchema, request),
      nullPrototypeWire: wireBytes(RequestSchema, nullPrototype),
      expectedNullPrototypeWire: wireBytes(RequestSchema, nullPrototype),
      emptyOneofWire: wireBytes(RequestSchema, { selector: { case: undefined } }),
      expectedEmptyOneofWire: wireBytes(RequestSchema, { selector: { case: undefined } }),
      messageChoiceWire: wireBytes(RequestSchema, {
        selector: { case: "childChoice", value: { label: "choice" } },
      }),
      expectedMessageChoiceWire: wireBytes(RequestSchema, {
        selector: { case: "childChoice", value: { label: "choice" } },
      }),
      messageChoiceFrozen: true,
      messageChoiceValueFrozen: true,
      bufferBytes: new Uint8Array([3, 4]),
    });
  });

  it("accepts every protobuf scalar boundary in singular, repeated, map, enum, and oneof fields", () => {
    const request = {
      doubleValue: Number.POSITIVE_INFINITY,
      floatValue: -3.402_823_466_385_288_6e38,
      int32Value: -2_147_483_648,
      fixed32Value: 4_294_967_295,
      uint32Value: 4_294_967_295,
      sfixed32Value: 2_147_483_647,
      sint32Value: -2_147_483_648,
      int64Value: -9_223_372_036_854_775_808n,
      uint64Value: 18_446_744_073_709_551_615n,
      fixed64Value: 18_446_744_073_709_551_615n,
      sfixed64Value: 9_223_372_036_854_775_807n,
      sint64Value: -9_223_372_036_854_775_808n,
      stringInt64Value: "9223372036854775807",
      stringUint64Value: "18446744073709551615",
      mode: 2_147_483_647,
      numbers: [-2_147_483_648, 2_147_483_647],
      modes: [-2_147_483_648, 2_147_483_647],
      scalarMap: { lower: -2_147_483_648, upper: 2_147_483_647 },
      enumMap: { future: 2_147_483_647 },
      int32Map: { "-2147483648": "lower", "2147483647": "upper" },
      boolMap: { false: "disabled", true: "enabled" },
      int64Map: {
        "-9223372036854775808": "lower",
        "9223372036854775807": "upper",
      },
      uint64Map: {
        "0": "lower",
        "18446744073709551615": "upper",
      },
      uint32Map: {
        "0": "lower",
        "4294967295": "upper",
      },
      fixed32Map: {
        "0": "lower",
        "4294967295": "upper",
      },
      selector: { case: "intChoice", value: 2_147_483_647 },
    };

    const closedEnumRequest = { mode: 1 };
    const requiredRequest = { requiredValue: "present" };
    const wrapperRequest = {
      wrapped: "unwrapped",
      wrappedValues: [{ value: "one" }, { value: "two" }],
    };
    expect({
      openEnum: wireBytes(RequestSchema, validateAndSnapshotGrpcRequest(RequestSchema, request)),
      closedEnum: wireBytes(
        ClosedEnumRequestSchema,
        validateAndSnapshotGrpcRequest(ClosedEnumRequestSchema, closedEnumRequest),
      ),
      required: wireBytes(
        RequiredRequestSchema,
        validateAndSnapshotGrpcRequest(RequiredRequestSchema, requiredRequest),
      ),
      wrapper: wireBytes(
        WrapperRequestSchema,
        validateAndSnapshotGrpcRequest(WrapperRequestSchema, wrapperRequest),
      ),
    }).toStrictEqual({
      openEnum: wireBytes(RequestSchema, request),
      closedEnum: wireBytes(ClosedEnumRequestSchema, closedEnumRequest),
      required: wireBytes(RequiredRequestSchema, requiredRequest),
      wrapper: wireBytes(WrapperRequestSchema, wrapperRequest),
    });
  });

  it("defensively freezes repeated, map, and oneof bytes without redefinition failures", () => {
    const request = {
      bytesValues: [new Uint8Array([1, 2])],
      bytesMap: {
        first: new Uint8Array([3, 4]),
      },
      selector: {
        case: "bytesChoice",
        value: new Uint8Array([5, 6]),
      },
    };
    const materialized = validateAndSnapshotGrpcRequest(RequestSchema, request);
    if (typeof materialized !== "object" || materialized === null) {
      throw new TypeError("Expected a materialized composite-bytes request.");
    }
    const bytesValues = Reflect.get(materialized, "bytesValues");
    const bytesMap = Reflect.get(materialized, "bytesMap");
    const selector = Reflect.get(materialized, "selector");
    if (
      !Array.isArray(bytesValues) ||
      typeof bytesMap !== "object" ||
      bytesMap === null ||
      typeof selector !== "object" ||
      selector === null
    ) {
      throw new TypeError("Expected materialized composite bytes containers.");
    }
    const repeatedBytes = bytesValues[0];
    const mappedBytes = Reflect.get(bytesMap, "first");
    const selectedBytes = Reflect.get(selector, "value");
    if (
      !(repeatedBytes instanceof Uint8Array) ||
      !(mappedBytes instanceof Uint8Array) ||
      !(selectedBytes instanceof Uint8Array)
    ) {
      throw new TypeError("Expected materialized composite bytes.");
    }
    repeatedBytes[0] = 9;
    mappedBytes[0] = 9;
    selectedBytes[0] = 9;

    expect({
      bytesMapFrozen: Object.isFrozen(bytesMap),
      bytesValuesFrozen: Object.isFrozen(bytesValues),
      mappedBytes: Reflect.get(bytesMap, "first"),
      oneofFrozen: Object.isFrozen(selector),
      repeatedBytes: bytesValues[0],
      selectedBytes: Reflect.get(selector, "value"),
      wire: wireBytes(RequestSchema, materialized),
    }).toStrictEqual({
      bytesMapFrozen: true,
      bytesValuesFrozen: true,
      mappedBytes: new Uint8Array([3, 4]),
      oneofFrozen: true,
      repeatedBytes: new Uint8Array([1, 2]),
      selectedBytes: new Uint8Array([5, 6]),
      wire: wireBytes(RequestSchema, request),
    });
  });

  it("preserves every admitted message-map key through Connect normalization and the wire", () => {
    const messageMap: Record<string, unknown> = {};
    Object.defineProperty(messageMap, "__proto__", {
      enumerable: true,
      value: { label: "prototype-key" },
    });
    const materialized = validateAndSnapshotGrpcRequest(RequestSchema, { messageMap });
    if (typeof materialized !== "object" || materialized === null) {
      throw new TypeError("Expected a materialized request.");
    }
    const materializedMap = Reflect.get(materialized, "messageMap");
    if (typeof materializedMap !== "object" || materializedMap === null) {
      throw new TypeError("Expected a materialized message map.");
    }
    const normalized = create(RequestSchema, materialized);
    const bytes = toBinary(RequestSchema, normalized);
    const expectedBytes = new Uint8Array([
      0xc2,
      0x01,
      0x1c,
      0x0a,
      0x09,
      ...new TextEncoder().encode("__proto__"),
      0x12,
      0x0f,
      0x0a,
      0x0d,
      ...new TextEncoder().encode("prototype-key"),
    ]);

    expect({
      connectNormalizationReusedMessage: normalized === materialized,
      materializedKeys: Object.keys(materializedMap),
      wireBytes: bytes,
      frozenMap: Object.isFrozen(materializedMap),
    }).toStrictEqual({
      connectNormalizationReusedMessage: true,
      materializedKeys: ["__proto__"],
      wireBytes: expectedBytes,
      frozenMap: true,
    });
  });

  it("accepts native Struct, Value, and ListValue JSON and keeps oneof wrappers", () => {
    const sparseJsonArray = ["value"];
    Reflect.deleteProperty(sparseJsonArray, "0");
    const cyclicJsonArray: Array<unknown> = [];
    cyclicJsonArray.push(cyclicJsonArray);
    const cyclicJsonObject: Record<string, unknown> = {};
    cyclicJsonObject["self"] = cyclicJsonObject;
    const request = {
      payload: {
        count: 1,
        enabled: true,
        nested: ["value", { child: null }],
      },
      payloadMap: {
        first: { mapped: true },
      },
      payloads: [{ listed: "value" }],
      selection: {
        case: "wrappedChoice",
        value: { value: "selected" },
      },
      value: ["native", { nested: null }, true],
      listValue: [null, "listed", { count: 1 }],
    };
    const expectedRequest = {
      ...request,
      value: fromJson(ValueSchema, request.value),
      listValue: fromJson(ListValueSchema, request.listValue),
    };

    const materialized = validateAndSnapshotGrpcRequest(NativeWktRequestSchema, request);
    const structChoiceRequest = {
      selection: {
        case: "structChoice",
        value: { selected: "json" },
      },
    };
    const materializedStructChoice = validateAndSnapshotGrpcRequest(
      NativeWktRequestSchema,
      structChoiceRequest,
    );
    if (typeof materialized !== "object" || materialized === null) {
      throw new TypeError("Expected a materialized WKT request.");
    }
    if (typeof materializedStructChoice !== "object" || materializedStructChoice === null) {
      throw new TypeError("Expected a materialized Struct oneof request.");
    }
    const selection = Reflect.get(materialized, "selection");
    if (typeof selection !== "object" || selection === null) {
      throw new TypeError("Expected a materialized WKT oneof.");
    }
    const wrappedChoice = Reflect.get(selection, "value");
    if (typeof wrappedChoice !== "object" || wrappedChoice === null) {
      throw new TypeError("Expected a materialized wrapper message.");
    }
    const nativeValue = Reflect.get(materialized, "value");
    const nativeListValue = Reflect.get(materialized, "listValue");
    if (typeof nativeValue !== "object" || nativeValue === null) {
      throw new TypeError("Expected a materialized Value message.");
    }
    if (typeof nativeListValue !== "object" || nativeListValue === null) {
      throw new TypeError("Expected a materialized ListValue message.");
    }
    const nativeValueKind = Reflect.get(nativeValue, "kind");
    const nativeValueList = Reflect.get(Object(nativeValueKind), "value");
    const nativeValueEntries = Reflect.get(Object(nativeValueList), "values");
    const nativeListEntries = Reflect.get(nativeListValue, "values");
    const structSelection = Reflect.get(materializedStructChoice, "selection");
    if (typeof structSelection !== "object" || structSelection === null) {
      throw new TypeError("Expected a materialized Struct oneof.");
    }
    expect({
      wire: wireBytes(NativeWktRequestSchema, materialized),
      structChoiceWire: wireBytes(NativeWktRequestSchema, materializedStructChoice),
      payload: Reflect.get(materialized, "payload"),
      valueTypeName: Reflect.get(nativeValue, "$typeName"),
      listValueTypeName: Reflect.get(nativeListValue, "$typeName"),
      nativeValueKindFrozen: Object.isFrozen(nativeValueKind),
      nativeValueListFrozen: Object.isFrozen(nativeValueList),
      nativeValueEntriesFrozen: Object.isFrozen(nativeValueEntries),
      nativeValueEntryFrozen: Object.isFrozen(
        Array.isArray(nativeValueEntries) ? nativeValueEntries[1] : undefined,
      ),
      nativeListEntriesFrozen: Object.isFrozen(nativeListEntries),
      nativeListEntryFrozen: Object.isFrozen(
        Array.isArray(nativeListEntries) ? nativeListEntries[2] : undefined,
      ),
      structChoice: Reflect.get(structSelection, "value"),
      wrappedChoiceTypeName: Reflect.get(wrappedChoice, "$typeName"),
      frozen: Object.isFrozen(materialized),
    }).toStrictEqual({
      wire: wireBytes(NativeWktRequestSchema, expectedRequest),
      structChoiceWire: wireBytes(NativeWktRequestSchema, structChoiceRequest),
      payload: request.payload,
      valueTypeName: "google.protobuf.Value",
      listValueTypeName: "google.protobuf.ListValue",
      nativeValueKindFrozen: true,
      nativeValueListFrozen: true,
      nativeValueEntriesFrozen: true,
      nativeValueEntryFrozen: true,
      nativeListEntriesFrozen: true,
      nativeListEntryFrozen: true,
      structChoice: { selected: "json" },
      wrappedChoiceTypeName: "google.protobuf.StringValue",
      frozen: true,
    });
    expect(() =>
      validateAndSnapshotGrpcRequest(NativeWktRequestSchema, {
        payload: { unsupported: 1n },
      }),
    ).toThrow("request-init value does not match");
    expect(() =>
      validateAndSnapshotGrpcRequest(NativeWktRequestSchema, {
        value: 1n,
      }),
    ).toThrow("request-init value does not match");
    expect(() =>
      validateAndSnapshotGrpcRequest(NativeWktRequestSchema, {
        listValue: { invalid: true },
      }),
    ).toThrow("request-init value does not match");
    let changingJsonReads = 0;
    const changingJson = new Proxy(
      {},
      {
        getPrototypeOf: () => Object.prototype,
        ownKeys: () => {
          changingJsonReads += 1;
          return changingJsonReads === 1 ? ["valid"] : [Symbol("invalid")];
        },
        getOwnPropertyDescriptor: (_target, key) =>
          key === "valid"
            ? {
                configurable: true,
                enumerable: true,
                value: "value",
              }
            : undefined,
      },
    );
    expect(() =>
      validateAndSnapshotGrpcRequest(NativeWktRequestSchema, {
        value: changingJson,
      }),
    ).toThrow("request-init value does not match");
    expect(changingJsonReads).toBe(3);
    for (const invalidArray of [sparseJsonArray, cyclicJsonArray]) {
      expect(() =>
        validateAndSnapshotGrpcRequest(NativeWktRequestSchema, {
          payload: { invalidArray },
        }),
      ).toThrow("request-init value does not match");
    }
    expect(() =>
      validateAndSnapshotGrpcRequest(NativeWktRequestSchema, {
        payload: cyclicJsonObject,
      }),
    ).toThrow("request-init value does not match");
    expect(() =>
      validateAndSnapshotGrpcRequest(NativeWktRequestSchema, {
        selection: {
          case: "wrappedChoice",
          value: "selected",
        },
      }),
    ).toThrow("request-init value does not match");
  });

  it("rejects every malformed scalar, aggregate, oneof, extra, and cyclic value", () => {
    const cycle: Record<string, unknown> = {};
    cycle["child"] = cycle;
    const symbolExtra = { stringValue: "visible" };
    Object.defineProperty(symbolExtra, Symbol("hidden"), {
      enumerable: true,
      value: "extra",
    });
    const nonEnumerableExtra = { stringValue: "visible" };
    Object.defineProperty(nonEnumerableExtra, "hidden", {
      value: "extra",
    });
    let accessorCalls = 0;
    const accessor = {};
    Object.defineProperty(accessor, "stringValue", {
      enumerable: true,
      get: () => {
        accessorCalls += 1;
        return "must-not-run";
      },
    });
    const oneofAccessor = {};
    Object.defineProperty(oneofAccessor, "case", {
      enumerable: true,
      get: () => {
        accessorCalls += 1;
        return "textChoice";
      },
    });
    const nestedSymbolExtra = { label: "visible" };
    Object.defineProperty(nestedSymbolExtra, Symbol("hidden"), {
      enumerable: true,
      value: "extra",
    });
    const sparseNumbers = [1, 2];
    Reflect.deleteProperty(sparseNumbers, "0");
    const symbolNumbers = [1];
    Object.defineProperty(symbolNumbers, Symbol("hidden"), {
      enumerable: true,
      value: 2,
    });
    const extraNumbers = [1];
    Object.defineProperty(extraNumbers, "extra", {
      enumerable: true,
      value: 2,
    });
    const nonEnumerableNumbers = [1];
    Object.defineProperty(nonEnumerableNumbers, "hidden", {
      value: 2,
    });
    const accessorNumbers: Array<number> = [];
    Object.defineProperty(accessorNumbers, "0", {
      enumerable: true,
      get: () => {
        accessorCalls += 1;
        return 1;
      },
    });
    Object.defineProperty(accessorNumbers, "length", {
      value: 1,
    });
    const invalidLengthNumbers = new Proxy([1], {
      getOwnPropertyDescriptor: (target, key) =>
        key === "length"
          ? {
              configurable: false,
              enumerable: false,
              value: 1.5,
              writable: true,
            }
          : Reflect.getOwnPropertyDescriptor(target, key),
    });
    let repeatedOwnKeyReads = 0;
    const changingNumbers = new Proxy([1], {
      ownKeys: (target) => {
        repeatedOwnKeyReads += 1;
        return repeatedOwnKeyReads === 1 ? Reflect.ownKeys(target) : ["0", "extra", "length"];
      },
      getOwnPropertyDescriptor: (target, key) =>
        key === "extra"
          ? {
              configurable: true,
              enumerable: true,
              value: 2,
              writable: true,
            }
          : Reflect.getOwnPropertyDescriptor(target, key),
    });
    let descriptorFieldReads = 0;
    const encodingFailureSchema = new Proxy(RequestSchema, {
      get: (target, key, receiver) => {
        if (key === "fields") {
          descriptorFieldReads += 1;
          if (descriptorFieldReads > 1) {
            throw new Error("planned protobuf encoding failure");
          }
        }
        return Reflect.get(target, key, receiver);
      },
    });
    const descriptorChangedAfterCreate = (replacement: DescField) => {
      let creating = false;
      const remapped = new Proxy(replacement, {
        get: (target, key, receiver) =>
          key === "localName" ? "stringValue" : Reflect.get(target, key, receiver),
      });
      return new Proxy(RequestSchema, {
        get: (target, key, receiver) => {
          if (key === "members") {
            creating = true;
          }
          return key === "fields" && creating ? [remapped] : Reflect.get(target, key, receiver);
        },
      });
    };
    const childField = RequestSchema.fields.find((field) => field.localName === "child");
    const numbersField = RequestSchema.fields.find((field) => field.localName === "numbers");
    if (childField === undefined || numbersField === undefined) {
      throw new TypeError("Expected request fixture fields.");
    }
    const messageChangedAfterValidation = descriptorChangedAfterCreate(childField);
    const listChangedAfterValidation = descriptorChangedAfterCreate(numbersField);
    const invalid: ReadonlyArray<unknown> = [
      null,
      [],
      new (class Request {})(),
      { extra: true },
      symbolExtra,
      nonEnumerableExtra,
      accessor,
      { doubleValue: "1" },
      { floatValue: "1" },
      { floatValue: Number.MAX_VALUE },
      { int32Value: 1.5 },
      { int32Value: -2_147_483_649 },
      { int32Value: 2_147_483_648 },
      { fixed32Value: -1 },
      { fixed32Value: 4_294_967_296 },
      { uint32Value: -1 },
      { uint32Value: 4_294_967_296 },
      { sfixed32Value: -2_147_483_649 },
      { sint32Value: 2_147_483_648 },
      { int64Value: 1 },
      { uint64Value: 1 },
      { int64Value: -9_223_372_036_854_775_809n },
      { int64Value: 9_223_372_036_854_775_808n },
      { uint64Value: -1n },
      { uint64Value: 18_446_744_073_709_551_616n },
      { fixed64Value: -1n },
      { sfixed64Value: 9_223_372_036_854_775_808n },
      { sint64Value: -9_223_372_036_854_775_809n },
      { stringInt64Value: 1n },
      { stringInt64Value: "" },
      { stringInt64Value: "not-an-integer" },
      { stringInt64Value: "9223372036854775808" },
      { stringUint64Value: 1n },
      { stringUint64Value: "" },
      { stringUint64Value: "not-an-integer" },
      { stringUint64Value: "18446744073709551616" },
      { boolValue: 1 },
      { stringValue: 1 },
      { stringValue: undefined },
      { bytesValue: [1, 2] },
      { mode: "active" },
      { mode: 1.5 },
      { mode: 2_147_483_648 },
      { child: "child" },
      { child: undefined },
      { child: { extra: true } },
      { child: nestedSymbolExtra },
      { numbers: "numbers" },
      { numbers: undefined },
      { numbers: [1, "2"] },
      { numbers: [undefined] },
      { numbers: [2_147_483_648] },
      { numbers: sparseNumbers },
      { numbers: symbolNumbers },
      { numbers: extraNumbers },
      { numbers: nonEnumerableNumbers },
      { numbers: accessorNumbers },
      { numbers: invalidLengthNumbers },
      { modes: [0, "active"] },
      { modes: [1.5] },
      { children: [{ label: 1 }] },
      { children: [undefined] },
      { scalarMap: [] },
      { scalarMap: undefined },
      { scalarMap: { one: "1" } },
      { scalarMap: { one: undefined } },
      { scalarMap: { one: 2_147_483_648 } },
      { enumMap: { one: "active" } },
      { enumMap: { one: 2_147_483_648 } },
      { messageMap: { one: { label: 1 } } },
      { messageMap: { one: undefined } },
      { int32Map: { notANumber: "value" } },
      { int32Map: { "2147483648": "value" } },
      { boolMap: { yes: "value" } },
      { int64Map: { "9223372036854775808": "value" } },
      { uint64Map: { "-1": "value" } },
      { uint32Map: { notANumber: "value" } },
      { uint32Map: { "-1": "value" } },
      { uint32Map: { "4294967296": "value" } },
      { selector: "text" },
      { selector: oneofAccessor },
      { selector: {} },
      { selector: { case: "textChoice", value: "text", extra: true } },
      { selector: { case: undefined, value: "text" } },
      { selector: { case: 1, value: "text" } },
      { selector: { case: "textChoice" } },
      { selector: { case: "textChoice", value: undefined } },
      { selector: { case: "modeChoice", value: undefined } },
      { selector: { case: "missing", value: "text" } },
      { selector: { case: "childChoice", value: "child" } },
      { selector: { case: "intChoice", value: 1.5 } },
      { node: cycle },
    ];

    for (const value of invalid) {
      expect(() => validateAndSnapshotGrpcRequest(RequestSchema, value)).toThrow(
        "request-init value does not match",
      );
    }
    expect(() =>
      validateAndSnapshotGrpcRequest(ClosedEnumRequestSchema, {
        mode: 2,
      }),
    ).toThrow("request-init value does not match");
    expect(() => validateAndSnapshotGrpcRequest(RequiredRequestSchema, {})).toThrow(
      "request-init value does not match",
    );
    expect(() =>
      validateAndSnapshotGrpcRequest(RequiredRequestSchema, {
        requiredValue: undefined,
      }),
    ).toThrow("request-init value does not match");
    expect(() =>
      validateAndSnapshotGrpcRequest(WrapperRequestSchema, {
        wrapped: 1,
      }),
    ).toThrow("request-init value does not match");
    expect(() =>
      validateAndSnapshotGrpcRequest(WrapperRequestSchema, {
        wrappedValues: ["one"],
      }),
    ).toThrow("request-init value does not match");
    expect(() =>
      validateAndSnapshotGrpcRequest(RequestSchema, {
        numbers: changingNumbers,
      }),
    ).toThrow("repeated field is not an exact dense data array");
    expect(() =>
      validateAndSnapshotGrpcRequest(encodingFailureSchema, {
        stringValue: "valid",
      }),
    ).toThrow("request-init value does not match");
    expect(() =>
      validateAndSnapshotGrpcRequest(messageChangedAfterValidation, {
        stringValue: "valid",
      }),
    ).toThrow("request-init value does not match");
    expect(() =>
      validateAndSnapshotGrpcRequest(listChangedAfterValidation, {
        stringValue: "valid",
      }),
    ).toThrow("request-init value does not match");
    expect(accessorCalls).toBe(0);
  });
});
