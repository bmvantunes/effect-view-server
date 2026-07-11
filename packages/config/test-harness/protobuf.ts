import { create, toBinary } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import type { Message } from "@bufbuild/protobuf";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";

export type OrdersValueMessage = Message<"viewserver.test.OrderValue"> & {
  readonly customerId: string;
  readonly status: "open" | "closed" | "cancelled";
  readonly price: number;
  readonly updatedAt: number;
};

export type OrdersKeyMessage = Message<"viewserver.test.OrderKey"> & {
  readonly orderId: string;
};

export type WrappedOrdersKeyMessage = Message<"viewserver.test.WrappedOrdersKey"> & {
  readonly order?: OrdersKeyMessage;
};

export type TradesValueMessage = Message<"viewserver.test.TradeValue"> & {
  readonly symbol: string;
  readonly quantity: number;
  readonly price: number;
};

export const base64FromBytes = (bytes: Uint8Array) =>
  globalThis.btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(""));

export const testProtoFile = fileDesc(
  base64FromBytes(
    toBinary(
      FileDescriptorProtoSchema,
      create(FileDescriptorProtoSchema, {
        name: "viewserver/test.proto",
        package: "viewserver.test",
        syntax: "proto3",
        messageType: [
          {
            name: "OrderValue",
            field: [
              { name: "customer_id", number: 1, type: FieldDescriptorProto_Type.STRING },
              { name: "status", number: 2, type: FieldDescriptorProto_Type.STRING },
              { name: "price", number: 3, type: FieldDescriptorProto_Type.DOUBLE },
              { name: "updated_at", number: 4, type: FieldDescriptorProto_Type.DOUBLE },
            ],
          },
          {
            name: "OrderKey",
            field: [{ name: "order_id", number: 1, type: FieldDescriptorProto_Type.STRING }],
          },
          {
            name: "WrappedOrdersKey",
            field: [
              {
                name: "order",
                number: 1,
                type: FieldDescriptorProto_Type.MESSAGE,
                typeName: ".viewserver.test.OrderKey",
              },
            ],
          },
          {
            name: "TradeValue",
            field: [
              { name: "symbol", number: 1, type: FieldDescriptorProto_Type.STRING },
              { name: "quantity", number: 2, type: FieldDescriptorProto_Type.DOUBLE },
              { name: "price", number: 3, type: FieldDescriptorProto_Type.DOUBLE },
            ],
          },
        ],
        service: [
          {
            name: "OrdersService",
            method: [
              {
                name: "StreamOrders",
                inputType: ".viewserver.test.OrderKey",
                outputType: ".viewserver.test.OrderValue",
                serverStreaming: true,
              },
              {
                name: "StreamTrades",
                inputType: ".viewserver.test.OrderKey",
                outputType: ".viewserver.test.TradeValue",
                serverStreaming: true,
              },
              {
                name: "StreamWrappedOrders",
                inputType: ".viewserver.test.WrappedOrdersKey",
                outputType: ".viewserver.test.OrderValue",
                serverStreaming: true,
              },
              {
                name: "GetOrder",
                inputType: ".viewserver.test.OrderKey",
                outputType: ".viewserver.test.OrderValue",
              },
            ],
          },
        ],
      }),
    ),
  ),
);

export const ordersValueSchema = messageDesc<OrdersValueMessage>(testProtoFile, 0);
export const ordersKeySchema = messageDesc<OrdersKeyMessage>(testProtoFile, 1);
export const wrappedOrdersKeySchema = messageDesc<WrappedOrdersKeyMessage>(testProtoFile, 2);
export const tradesValueSchema = messageDesc<TradesValueMessage>(testProtoFile, 3);

export const ordersService = serviceDesc<{
  readonly streamOrders: {
    readonly input: typeof ordersKeySchema;
    readonly output: typeof ordersValueSchema;
    readonly methodKind: "server_streaming";
  };
  readonly streamTrades: {
    readonly input: typeof ordersKeySchema;
    readonly output: typeof tradesValueSchema;
    readonly methodKind: "server_streaming";
  };
  readonly streamWrappedOrders: {
    readonly input: typeof wrappedOrdersKeySchema;
    readonly output: typeof ordersValueSchema;
    readonly methodKind: "server_streaming";
  };
  readonly getOrder: {
    readonly input: typeof ordersKeySchema;
    readonly output: typeof ordersValueSchema;
    readonly methodKind: "unary";
  };
}>(testProtoFile, 0);

export const tradesOnlyService = serviceDesc<{
  readonly streamTrades: {
    readonly input: typeof ordersKeySchema;
    readonly output: typeof tradesValueSchema;
    readonly methodKind: "server_streaming";
  };
}>(testProtoFile, 0);
