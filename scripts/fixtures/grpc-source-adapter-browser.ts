import { create, toBinary, type Message } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import {
  FieldDescriptorProto_Type,
  FileDescriptorProtoSchema,
} from "@bufbuild/protobuf/wkt";
import {
  GrpcAdapterFailure,
  GrpcLeasedMetrics,
  GrpcMaterializedMetrics,
  GrpcRejectionLocation,
  grpc,
} from "effect-view-server/grpc/contract";

type BrowserRequest = Message<"browser.grpc.Request"> & {
  readonly region: string;
};

type BrowserEvent = Message<"browser.grpc.Event"> & {
  readonly id: string;
  readonly region: string;
};

const descriptorFile = fileDesc(
  globalThis.btoa(
    Array.from(
      toBinary(
        FileDescriptorProtoSchema,
        create(FileDescriptorProtoSchema, {
          name: "browser/grpc.proto",
          package: "browser.grpc",
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
                  inputType: ".browser.grpc.Request",
                  outputType: ".browser.grpc.Event",
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

const RequestSchema = messageDesc<BrowserRequest>(descriptorFile, 0);
const EventSchema = messageDesc<BrowserEvent>(descriptorFile, 1);
const RowsService = serviceDesc<{
  readonly stream: {
    readonly input: typeof RequestSchema;
    readonly output: typeof EventSchema;
    readonly methodKind: "server_streaming";
  };
}>(descriptorFile, 0);

export const browserGrpcContract = {
  failures: GrpcAdapterFailure,
  leasedMetrics: GrpcLeasedMetrics,
  materializedMetrics: GrpcMaterializedMetrics,
  rejectionLocation: GrpcRejectionLocation,
  sources: grpc.topicSources({
    rows: RowsService,
  }),
};
