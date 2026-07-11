import { expect, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import * as HashMap from "effect/HashMap";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import { Cause, Effect, Result, Schema, SchemaTransformation } from "effect";
import { Duration } from "effect";
import { decodeKafkaCodec, kafka } from "./index";

const textEncoder = new TextEncoder();

const metadata = {
  sourceTopic: "trades-source",
  sourceRegion: "usa",
  partition: 0,
  offset: "1",
  timestamp: null,
  headers: {},
};

const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
});

class Venue extends Schema.Class<Venue>("Venue")({
  code: Schema.String,
}) {}

type TradeTree = {
  readonly label: string;
  readonly children: ReadonlyArray<TradeTree>;
};

const TradeTree: Schema.Codec<TradeTree> = Schema.Struct({
  label: Schema.String,
  children: Schema.Array(Schema.suspend((): Schema.Codec<TradeTree> => TradeTree)),
});

const DeclaredTradeId = Schema.declare<string>(
  (value): value is string => typeof value === "string",
  {
    toCodecJson: () => Schema.link<string>()(Schema.String, SchemaTransformation.passthrough()),
  },
);

const CompositeTrade = Schema.Struct({
  id: DeclaredTradeId,
  venue: Venue,
  note: Schema.optionalKey(Schema.String),
  legs: Schema.Array(Schema.Number),
  tuple: Schema.TupleWithRest(Schema.Tuple([Schema.String]), [Schema.Number]),
  scalar: Schema.Union([Schema.String, Schema.Number]),
  event: Schema.TaggedUnion({
    quantity: { amount: Schema.BigInt },
    note: { text: Schema.String },
  }),
  amounts: Schema.Record(Schema.String, Schema.BigInt),
  tree: TradeTree,
  renamed: Schema.Struct({ quantity: Schema.BigInt }).pipe(Schema.encodeKeys({ quantity: "qty" })),
});

const EffectValueTrade = Schema.Struct({
  id: Schema.String,
  quantity: Schema.BigInt,
  price: Schema.BigDecimal,
  latency: Schema.Duration,
  optionalQuantity: Schema.Option(Schema.BigInt),
  tags: Schema.Chunk(Schema.String),
  readonlyMap: Schema.ReadonlyMap(Schema.String, Schema.BigInt),
  hashMap: Schema.HashMap(Schema.String, Schema.BigInt),
  readonlySet: Schema.ReadonlySet(Schema.String),
  hashSet: Schema.HashSet(Schema.String),
  secret: Schema.Redacted(Schema.String),
  json: Schema.Json,
  mutableJson: Schema.MutableJson,
});

it.effect("constructs the canonical JSON codec once and decodes its wire value", () =>
  Effect.gen(function* () {
    let factoryCalls = 0;
    const codec = kafka.json(() => {
      factoryCalls += 1;
      return Schema.toCodecJson(Trade);
    });

    expect(factoryCalls).toBe(1);
    expect(
      yield* decodeKafkaCodec(codec, {
        bytes: textEncoder.encode('{"id":"trade-1","symbol":"AAPL"}'),
        metadata,
      }),
    ).toStrictEqual({ id: "trade-1", symbol: "AAPL" });
    expect(factoryCalls).toBe(1);
  }),
);

it.effect("preserves a synchronous canonical-codec factory failure as KafkaDecodeError", () =>
  Effect.gen(function* () {
    const cause = new Error("codec construction failed");
    let factoryCalls = 0;
    const codec = kafka.json((): Schema.toCodecJson<typeof Trade> => {
      factoryCalls += 1;
      throw cause;
    });

    expect(factoryCalls).toBe(1);
    expect(
      yield* decodeKafkaCodec(codec, {
        bytes: textEncoder.encode('{"id":"trade-1","symbol":"AAPL"}'),
        metadata,
      }).pipe(Effect.flip),
    ).toStrictEqual({
      _tag: "KafkaDecodeError",
      message: "Kafka JSON schema is not JSON-compatible",
      cause,
    });
    expect(factoryCalls).toBe(1);
  }),
);

it.effect("distinguishes malformed JSON from canonical codec mismatches", () =>
  Effect.gen(function* () {
    const codec = kafka.json(() => Schema.toCodecJson(Trade));
    const parseError = yield* decodeKafkaCodec(codec, {
      bytes: textEncoder.encode("{"),
      metadata,
    }).pipe(Effect.flip);
    const schemaError = yield* decodeKafkaCodec(codec, {
      bytes: textEncoder.encode('{"id":"trade-1","symbol":42}'),
      metadata,
    }).pipe(Effect.flip);

    expect(parseError._tag).toBe("KafkaDecodeError");
    expect(parseError.message).toBe("Failed to parse Kafka JSON payload");
    expect(parseError.cause).toBeInstanceOf(SyntaxError);
    expect(schemaError._tag).toBe("KafkaDecodeError");
    expect(schemaError.message).toBe("Failed to decode Kafka JSON payload");
    expect(Schema.isSchemaError(schemaError.cause)).toBe(true);
  }),
);

it.effect("leaves canonical codec defects in the defect channel", () =>
  Effect.gen(function* () {
    const defect = new Error("unexpected decoder defect");
    const DefectTrade = Schema.Struct({
      id: Schema.String.pipe(Schema.middlewareDecoding(() => Effect.die(defect))),
      symbol: Schema.String,
    });
    const codec = kafka.json(() => Schema.toCodecJson(DefectTrade));
    const cause = yield* decodeKafkaCodec(codec, {
      bytes: textEncoder.encode('{"id":"trade-1","symbol":"AAPL"}'),
      metadata,
    }).pipe(Effect.sandbox, Effect.flip);

    expect(Cause.hasDies(cause)).toBe(true);
    expect(Cause.findDefect(cause)).toStrictEqual(Result.succeed(defect));
  }),
);

it.effect("decodes the stable canonical JSON composition fixture", () =>
  Effect.gen(function* () {
    const codec = kafka.json(() => Schema.toCodecJson(CompositeTrade));
    const decoded = yield* decodeKafkaCodec(codec, {
      bytes: textEncoder.encode(
        '{"id":"trade-1","venue":{"code":"XNAS"},"legs":[1,2],"tuple":["open",3,4],"scalar":5,"event":{"_tag":"quantity","amount":"9007199254740993"},"amounts":{"desk-a":"9007199254740995"},"tree":{"label":"root","children":[{"label":"leaf","children":[]}]},"renamed":{"qty":"9007199254740997"}}',
      ),
      metadata,
    });

    expect(decoded).toStrictEqual({
      id: "trade-1",
      venue: Venue.make({ code: "XNAS" }),
      legs: [1, 2],
      tuple: ["open", 3, 4],
      scalar: 5,
      event: { _tag: "quantity", amount: 9007199254740993n },
      amounts: { "desk-a": 9007199254740995n },
      tree: {
        label: "root",
        children: [{ label: "leaf", children: [] }],
      },
      renamed: { quantity: 9007199254740997n },
    });
    expect(
      yield* Schema.encodeUnknownEffect(Schema.toCodecJson(CompositeTrade))(decoded),
    ).toStrictEqual({
      id: "trade-1",
      venue: { code: "XNAS" },
      legs: [1, 2],
      tuple: ["open", 3, 4],
      scalar: 5,
      event: { _tag: "quantity", amount: "9007199254740993" },
      amounts: { "desk-a": "9007199254740995" },
      tree: {
        label: "root",
        children: [{ label: "leaf", children: [] }],
      },
      renamed: { qty: "9007199254740997" },
    });
  }),
);

it.effect("decodes the stable canonical JSON public Effect-value fixture", () =>
  Effect.gen(function* () {
    const codec = kafka.json(() => Schema.toCodecJson(EffectValueTrade));
    const decoded = yield* decodeKafkaCodec(codec, {
      bytes: textEncoder.encode(
        '{"id":"trade-2","quantity":"9007199254740993","price":"1234567890.123456789","latency":{"_tag":"Millis","value":42},"optionalQuantity":{"_tag":"Some","value":"9007199254740995"},"tags":["fast","typed"],"readonlyMap":[["desk-a","9007199254740997"]],"hashMap":[["desk-b","9007199254740999"]],"readonlySet":["equities"],"hashSet":["live"],"secret":"classified","json":{"nested":[1,true,null]},"mutableJson":{"status":"ready"}}',
      ),
      metadata,
    });

    expect({
      id: decoded.id,
      quantity: decoded.quantity,
      price: BigDecimal.format(decoded.price),
      latencyMillis: Duration.toMillis(decoded.latency),
      optionalQuantity: Option.getOrElse(decoded.optionalQuantity, () => 0n),
      tags: Array.from(decoded.tags),
      readonlyMap: Array.from(decoded.readonlyMap.entries()),
      hashMap: HashMap.toEntries(decoded.hashMap),
      readonlySet: Array.from(decoded.readonlySet),
      hashSet: Array.from(decoded.hashSet),
      secret: Redacted.value(decoded.secret),
      json: decoded.json,
      mutableJson: decoded.mutableJson,
    }).toStrictEqual({
      id: "trade-2",
      quantity: 9007199254740993n,
      price: "1234567890.123456789",
      latencyMillis: 42,
      optionalQuantity: 9007199254740995n,
      tags: ["fast", "typed"],
      readonlyMap: [["desk-a", 9007199254740997n]],
      hashMap: [["desk-b", 9007199254740999n]],
      readonlySet: ["equities"],
      hashSet: ["live"],
      secret: "classified",
      json: { nested: [1, true, null] },
      mutableJson: { status: "ready" },
    });
    expect(
      yield* Schema.encodeUnknownEffect(Schema.toCodecJson(EffectValueTrade))(decoded),
    ).toStrictEqual({
      id: "trade-2",
      quantity: "9007199254740993",
      price: "1234567890.123456789",
      latency: { _tag: "Millis", value: 42 },
      optionalQuantity: { _tag: "Some", value: "9007199254740995" },
      tags: ["fast", "typed"],
      readonlyMap: [["desk-a", "9007199254740997"]],
      hashMap: [["desk-b", "9007199254740999"]],
      readonlySet: ["equities"],
      hashSet: ["live"],
      secret: "classified",
      json: { nested: [1, true, null] },
      mutableJson: { status: "ready" },
    });
  }),
);
