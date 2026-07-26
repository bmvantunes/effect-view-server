// Generated-style protobuf fixture for Kafka integration tests.
import type { Message } from "@bufbuild/protobuf";
import { fileDesc, messageDesc } from "@bufbuild/protobuf/codegenv2";
import type { GenFile, GenMessage } from "@bufbuild/protobuf/codegenv2";

export const file_effect_view_server_kafka_test: GenFile = fileDesc(
  "Ch12aWV3c2VydmVyL3J1bnRpbWUvdGVzdC5wcm90bxIXdmlld3NlcnZlci5ydW50aW1lLnRlc3QiLAoKT3JkZXJWYWx1ZRIRCgtjdXN0b21lcl9pZBgBKAkSCwoFcHJpY2UYAigBIhoKCE9yZGVyS2V5Eg4KCG9yZGVyX2lkGAEoCWIGcHJvdG8z",
);

export type OrderValue = Message<"viewserver.runtime.test.OrderValue"> & {
  readonly customerId: string;
  readonly price: number;
};

export type OrderValueJson = {
  readonly customerId?: string;
  readonly price?: number;
};

export const OrderValueSchema: GenMessage<OrderValue, { jsonType: OrderValueJson }> = messageDesc(
  file_effect_view_server_kafka_test,
  0,
);

export type OrderKey = Message<"viewserver.runtime.test.OrderKey"> & {
  readonly orderId: string;
};

export type OrderKeyJson = {
  readonly orderId?: string;
};

export const OrderKeySchema: GenMessage<OrderKey, { jsonType: OrderKeyJson }> = messageDesc(
  file_effect_view_server_kafka_test,
  1,
);
