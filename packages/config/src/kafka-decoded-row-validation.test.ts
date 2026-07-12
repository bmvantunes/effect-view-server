import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { defineViewServerConfig, kafka, kafkaErrorIsMapping, viewSchema } from "./index";
import { decodeKafkaTopicMessage } from "./kafka-contract";
import { makeKafkaSourceTopicsForConfig } from "./internal";
import { kafkaRegions, kafkaTestMetadata, textEncoder } from "../test-harness/kafka";

const IncomingQuantity = Schema.Struct({
  quantity: Schema.BigIntFromString,
});

class RootClassKafkaRow extends Schema.Class<RootClassKafkaRow>("RootClassKafkaRow")({
  id: Schema.String,
  quantity: Schema.BigIntFromString,
}) {}
viewSchema.admitClass(RootClassKafkaRow);

class NormalizingKafkaKeyRow extends Schema.Class<NormalizingKafkaKeyRow>("NormalizingKafkaKeyRow")(
  {
    id: Schema.String,
    quantity: Schema.BigIntFromString,
  },
) {}
viewSchema.admitClass(NormalizingKafkaKeyRow);
const normalizingKafkaKeyRowMakeEffect =
  NormalizingKafkaKeyRow.makeEffect.bind(NormalizingKafkaKeyRow);
const makeNormalizingKafkaKeyRow: typeof NormalizingKafkaKeyRow.makeEffect = (input, options) =>
  normalizingKafkaKeyRowMakeEffect(input, options).pipe(
    Effect.map(
      (row) => new NormalizingKafkaKeyRow({ id: row.id.toUpperCase(), quantity: row.quantity }),
    ),
  );
Object.defineProperty(NormalizingKafkaKeyRow, "makeEffect", {
  value: makeNormalizingKafkaKeyRow,
});

class ThrowingKafkaKeyRow extends Schema.Class<ThrowingKafkaKeyRow>("ThrowingKafkaKeyRow")({
  id: Schema.String,
  quantity: Schema.BigIntFromString,
}) {}
viewSchema.admitClass(ThrowingKafkaKeyRow);
const throwingKafkaKeyRowMakeEffect = ThrowingKafkaKeyRow.makeEffect.bind(ThrowingKafkaKeyRow);
const makeThrowingKafkaKeyRow: typeof ThrowingKafkaKeyRow.makeEffect = (input, options) =>
  throwingKafkaKeyRowMakeEffect(input, options).pipe(
    Effect.map(
      (row) =>
        new Proxy(row, {
          getOwnPropertyDescriptor: (target, property) => {
            if (property === "id") {
              throw new Error("key descriptor exploded");
            }
            return Reflect.getOwnPropertyDescriptor(target, property);
          },
        }),
    ),
  );
Object.defineProperty(ThrowingKafkaKeyRow, "makeEffect", {
  value: makeThrowingKafkaKeyRow,
});

let accessorKeyReads = 0;
class AccessorKafkaKeyRow extends Schema.Class<AccessorKafkaKeyRow>("AccessorKafkaKeyRow")({
  id: Schema.String,
  quantity: Schema.BigIntFromString,
}) {}
viewSchema.admitClass(AccessorKafkaKeyRow);
const accessorKafkaKeyRowMakeEffect = AccessorKafkaKeyRow.makeEffect.bind(AccessorKafkaKeyRow);
const makeAccessorKafkaKeyRow: typeof AccessorKafkaKeyRow.makeEffect = (input, options) =>
  accessorKafkaKeyRowMakeEffect(input, options).pipe(
    Effect.map(
      (row) =>
        new Proxy(row, {
          getOwnPropertyDescriptor: (target, property) =>
            property === "id"
              ? {
                  configurable: true,
                  enumerable: true,
                  get: () => {
                    accessorKeyReads += 1;
                    return "quantity-1";
                  },
                }
              : Reflect.getOwnPropertyDescriptor(target, property),
        }),
    ),
  );
Object.defineProperty(AccessorKafkaKeyRow, "makeEffect", {
  value: makeAccessorKafkaKeyRow,
});

class MissingKafkaKeyRow extends Schema.Class<MissingKafkaKeyRow>("MissingKafkaKeyRow")({
  id: Schema.String,
  quantity: Schema.BigIntFromString,
}) {}
viewSchema.admitClass(MissingKafkaKeyRow);
const missingKafkaKeyRowMakeEffect = MissingKafkaKeyRow.makeEffect.bind(MissingKafkaKeyRow);
const makeMissingKafkaKeyRow: typeof MissingKafkaKeyRow.makeEffect = (input, options) =>
  missingKafkaKeyRowMakeEffect(input, options).pipe(
    Effect.map(
      (row) =>
        new Proxy(row, {
          getOwnPropertyDescriptor: (target, property) =>
            property === "id" ? undefined : Reflect.getOwnPropertyDescriptor(target, property),
        }),
    ),
  );
Object.defineProperty(MissingKafkaKeyRow, "makeEffect", {
  value: makeMissingKafkaKeyRow,
});

class HiddenKafkaKeyRow extends Schema.Class<HiddenKafkaKeyRow>("HiddenKafkaKeyRow")({
  id: Schema.String,
  quantity: Schema.BigIntFromString,
}) {}
viewSchema.admitClass(HiddenKafkaKeyRow);
const hiddenKafkaKeyRowMakeEffect = HiddenKafkaKeyRow.makeEffect.bind(HiddenKafkaKeyRow);
const makeHiddenKafkaKeyRow: typeof HiddenKafkaKeyRow.makeEffect = (input, options) =>
  hiddenKafkaKeyRowMakeEffect(input, options).pipe(
    Effect.map(
      (row) =>
        new Proxy(row, {
          getOwnPropertyDescriptor: (target, property) => {
            const descriptor = Reflect.getOwnPropertyDescriptor(target, property);
            return property === "id" && descriptor !== undefined
              ? { ...descriptor, enumerable: false }
              : descriptor;
          },
        }),
    ),
  );
Object.defineProperty(HiddenKafkaKeyRow, "makeEffect", {
  value: makeHiddenKafkaKeyRow,
});

describe("Kafka decoded row validation", () => {
  it.effect("accepts a plain decoded mapper row for an admitted root Class topic", () =>
    Effect.gen(function* () {
      const config = defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          quantities: {
            schema: RootClassKafkaRow,
            key: "id",
            kafkaSource: kafka.source({
              topic: "quantities-source",
              regions: ["usa"],
              value: kafka.json(() => Schema.toCodecJson(IncomingQuantity)),
              rowKey: ({ key }) => key,
              map: ({ value }) => ({ quantity: value.quantity }),
            }),
          },
        },
      });
      const source = makeKafkaSourceTopicsForConfig(config)[0]!;

      const decoded = yield* decodeKafkaTopicMessage(source, {
        keyBytes: textEncoder.encode("quantity-1"),
        valueBytes: textEncoder.encode('{"quantity":"42"}'),
        region: "usa",
        metadata: kafkaTestMetadata("usa"),
        rowKeyField: "id",
        schema: RootClassKafkaRow,
        viewServerTopic: "quantities",
      });

      expect(decoded).toStrictEqual({
        row: new RootClassKafkaRow({ id: "quantity-1", quantity: 42n }),
        rowKey: "quantity-1",
        viewServerTopic: "quantities",
      });
    }),
  );

  it.effect("rejects root Class normalization that changes the configured Kafka row key", () =>
    Effect.gen(function* () {
      const config = defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          quantities: {
            schema: NormalizingKafkaKeyRow,
            key: "id",
            kafkaSource: kafka.source({
              topic: "quantities-source",
              regions: ["usa"],
              value: kafka.json(() => Schema.toCodecJson(IncomingQuantity)),
              rowKey: ({ key }) => key,
              map: ({ value }) => ({ quantity: value.quantity }),
            }),
          },
        },
      });
      const source = makeKafkaSourceTopicsForConfig(config)[0]!;

      const error = yield* Effect.flip(
        decodeKafkaTopicMessage(source, {
          keyBytes: textEncoder.encode("quantity-1"),
          valueBytes: textEncoder.encode('{"quantity":"42"}'),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
          rowKeyField: "id",
          schema: NormalizingKafkaKeyRow,
          viewServerTopic: "quantities",
        }),
      );

      expect(kafkaErrorIsMapping(error)).toBe(true);
      expect(Reflect.get(Object(error), "message")).toBe(
        "Kafka mapped row changed the configured row key",
      );
    }),
  );

  it.effect("returns a mapping error when the constructed Kafka row key cannot be read", () =>
    Effect.gen(function* () {
      const config = defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          quantities: {
            schema: ThrowingKafkaKeyRow,
            key: "id",
            kafkaSource: kafka.source({
              topic: "quantities-source",
              regions: ["usa"],
              value: kafka.json(() => Schema.toCodecJson(IncomingQuantity)),
              rowKey: ({ key }) => key,
              map: ({ value }) => ({ quantity: value.quantity }),
            }),
          },
        },
      });
      const source = makeKafkaSourceTopicsForConfig(config)[0]!;

      const error = yield* Effect.flip(
        decodeKafkaTopicMessage(source, {
          keyBytes: textEncoder.encode("quantity-1"),
          valueBytes: textEncoder.encode('{"quantity":"42"}'),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
          rowKeyField: "id",
          schema: ThrowingKafkaKeyRow,
          viewServerTopic: "quantities",
        }),
      );

      expect(kafkaErrorIsMapping(error)).toBe(true);
      expect(Reflect.get(Object(error), "message")).toBe(
        "Kafka mapped row key could not be inspected",
      );
    }),
  );

  it.effect("rejects an accessor Kafka row key without invoking its getter", () =>
    Effect.gen(function* () {
      accessorKeyReads = 0;
      const config = defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          quantities: {
            schema: AccessorKafkaKeyRow,
            key: "id",
            kafkaSource: kafka.source({
              topic: "quantities-source",
              regions: ["usa"],
              value: kafka.json(() => Schema.toCodecJson(IncomingQuantity)),
              rowKey: ({ key }) => key,
              map: ({ value }) => ({ quantity: value.quantity }),
            }),
          },
        },
      });
      const source = makeKafkaSourceTopicsForConfig(config)[0]!;

      const error = yield* Effect.flip(
        decodeKafkaTopicMessage(source, {
          keyBytes: textEncoder.encode("quantity-1"),
          valueBytes: textEncoder.encode('{"quantity":"42"}'),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
          rowKeyField: "id",
          schema: AccessorKafkaKeyRow,
          viewServerTopic: "quantities",
        }),
      );

      expect(accessorKeyReads).toBe(0);
      expect(kafkaErrorIsMapping(error)).toBe(true);
      expect(Reflect.get(Object(error), "message")).toBe(
        "Kafka mapped row key must be an enumerable own data property",
      );
    }),
  );

  it.effect("rejects a missing own Kafka row key", () =>
    Effect.gen(function* () {
      const config = defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          quantities: {
            schema: MissingKafkaKeyRow,
            key: "id",
            kafkaSource: kafka.source({
              topic: "quantities-source",
              regions: ["usa"],
              value: kafka.json(() => Schema.toCodecJson(IncomingQuantity)),
              rowKey: ({ key }) => key,
              map: ({ value }) => ({ quantity: value.quantity }),
            }),
          },
        },
      });
      const source = makeKafkaSourceTopicsForConfig(config)[0]!;

      const error = yield* Effect.flip(
        decodeKafkaTopicMessage(source, {
          keyBytes: textEncoder.encode("quantity-1"),
          valueBytes: textEncoder.encode('{"quantity":"42"}'),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
          rowKeyField: "id",
          schema: MissingKafkaKeyRow,
          viewServerTopic: "quantities",
        }),
      );

      expect(kafkaErrorIsMapping(error)).toBe(true);
      expect(Reflect.get(Object(error), "message")).toBe(
        "Kafka mapped row key must be an enumerable own data property",
      );
    }),
  );

  it.effect("rejects a non-enumerable own Kafka row key", () =>
    Effect.gen(function* () {
      const config = defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          quantities: {
            schema: HiddenKafkaKeyRow,
            key: "id",
            kafkaSource: kafka.source({
              topic: "quantities-source",
              regions: ["usa"],
              value: kafka.json(() => Schema.toCodecJson(IncomingQuantity)),
              rowKey: ({ key }) => key,
              map: ({ value }) => ({ quantity: value.quantity }),
            }),
          },
        },
      });
      const source = makeKafkaSourceTopicsForConfig(config)[0]!;

      const error = yield* Effect.flip(
        decodeKafkaTopicMessage(source, {
          keyBytes: textEncoder.encode("quantity-1"),
          valueBytes: textEncoder.encode('{"quantity":"42"}'),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
          rowKeyField: "id",
          schema: HiddenKafkaKeyRow,
          viewServerTopic: "quantities",
        }),
      );

      expect(kafkaErrorIsMapping(error)).toBe(true);
      expect(Reflect.get(Object(error), "message")).toBe(
        "Kafka mapped row key must be an enumerable own data property",
      );
    }),
  );
});
