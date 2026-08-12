import { fromBinary } from "@bufbuild/protobuf";
import type { DescMessage, MessageShape } from "@bufbuild/protobuf";
import { Effect } from "effect";

export type KafkaSchemaRegistryProtobufPayloadDecodeError = {
  readonly _tag: "KafkaCodecError";
  readonly message: string;
};

export const decodeKafkaSchemaRegistryProtobufPayload = <Descriptor extends DescMessage>(
  descriptor: Descriptor,
  payload: Uint8Array,
): Effect.Effect<MessageShape<Descriptor>, KafkaSchemaRegistryProtobufPayloadDecodeError> =>
  Effect.try({
    try: () => fromBinary(descriptor, payload),
    catch: () => ({
      _tag: "KafkaCodecError",
      message: "Kafka Schema Registry protobuf payload could not be decoded.",
    }),
  });
