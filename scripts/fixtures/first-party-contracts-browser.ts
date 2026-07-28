import { create, toBinary, type Message } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import {
  FieldDescriptorProto_Type,
  FileDescriptorProtoSchema,
} from "@bufbuild/protobuf/wkt";
import { Schema } from "effect";
import { ViewServerId, defineViewServerConfig } from "effect-view-server/config";
import { grpc } from "effect-view-server/grpc/contract";
import { kafka } from "effect-view-server/kafka/contract";

type Request = Message<"browser.combined.Request"> & {
  readonly region: string;
};

type Event = Message<"browser.combined.Event"> & {
  readonly id: string;
  readonly region: string;
};

const descriptorFile = fileDesc(
  globalThis.btoa(
    Array.from(
      toBinary(
        FileDescriptorProtoSchema,
        create(FileDescriptorProtoSchema, {
          name: "browser/combined.proto",
          package: "browser.combined",
          syntax: "proto3",
          messageType: [
            {
              name: "Request",
              field: [
                {
                  name: "region",
                  number: 1,
                  type: FieldDescriptorProto_Type.STRING,
                },
              ],
            },
            {
              name: "Event",
              field: [
                {
                  name: "id",
                  number: 1,
                  type: FieldDescriptorProto_Type.STRING,
                },
                {
                  name: "region",
                  number: 2,
                  type: FieldDescriptorProto_Type.STRING,
                },
              ],
            },
          ],
          service: [
            {
              name: "Rows",
              method: [
                {
                  name: "Stream",
                  inputType: ".browser.combined.Request",
                  outputType: ".browser.combined.Event",
                  serverStreaming: true,
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

const RequestSchema = messageDesc<Request>(descriptorFile, 0);
const EventSchema = messageDesc<Event>(descriptorFile, 1);
const RowsService = serviceDesc<{
  readonly stream: {
    readonly input: typeof RequestSchema;
    readonly output: typeof EventSchema;
    readonly methodKind: "server_streaming";
  };
}>(descriptorFile, 0);

const Row = Schema.Struct({
  id: ViewServerId,
  region: Schema.String,
});
const KafkaValue = Schema.Struct({
  region: Schema.String,
});
const grpcSources = grpc.topicSources({
  rows: RowsService,
});

export const firstPartyBrowserConfig = defineViewServerConfig({
  topics: {
    kafkaRows: {
      schema: Row,
      source: kafka.source({
        topic: "browser-rows",
        regions: ["primary"],
        key: kafka.string(),
        value: kafka.json(() => Schema.toCodecJson(KafkaValue)),
        localRowKey: ({ key }) => key,
        map: ({ value }) => ({ region: value.region }),
        startFrom: "earliest",
      }),
    },
    grpcRows: {
      schema: Row,
      source: grpcSources.materialized({
        client: "rows",
        method: "stream",
        request: () => ({ region: "all" }),
        map: ({ value }) => ({
          id: value.id,
          region: value.region,
        }),
      }),
    },
  },
});
