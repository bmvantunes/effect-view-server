import { describe, expectTypeOf, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  kafka as kafkaFromConfig,
  viewSchema,
} from "effect-view-server/config";
import { decodeKafkaCodec as decodeKafkaCodecFromConfig } from "effect-view-server/config";
import { decodeKafkaCodec, kafka } from "effect-view-server/config/kafka";
import type { KafkaCodecType, LiveQueryResult, RowFromSchema } from "effect-view-server/config";
import { createViewServerReact } from "effect-view-server/react";
import { createInMemoryViewServerReact } from "effect-view-server/react/testing";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { createViewServerWebSocketServer } from "effect-view-server/server";
import { createInMemoryViewServer } from "effect-view-server/in-memory";
import { Schema } from "effect";
import type * as EffectOption from "effect/Option";
import type { ViewServerLiveClient } from "effect-view-server/client";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed"]),
  price: Schema.Number,
  region: Schema.String,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

const react = createViewServerReact(viewServer);
const kafkaOrderCodec = kafka.json(() => Schema.toCodecJson(Order));
const kafkaOrderCodecFromConfig = kafkaFromConfig.json(() => Schema.toCodecJson(Order));

class PublicProfile extends Schema.Class<PublicProfile>("PublicProfile")({
  id: Schema.String,
  score: Schema.NumberFromString,
  backup: viewSchema.Option(Schema.String),
  nickname: Schema.optionalKey(Schema.String),
}) {
  upperId(): string {
    return this.id.toUpperCase();
  }
}
viewSchema.admitClass(PublicProfile);

const publicProfileViewServer = defineViewServerConfig({
  topics: {
    profiles: {
      schema: PublicProfile,
      key: "id",
    },
  },
});
const publicProfileReact = createViewServerReact(publicProfileViewServer);
const publicProfileInMemory = createInMemoryViewServer(publicProfileViewServer);

describe("public effect-view-server subpath type contracts", () => {
  it("rejects bare root package imports", () => {
    // @ts-expect-error effect-view-server intentionally has no root export.
    expectTypeOf<typeof import("effect-view-server")>().not.toBeAny();
  });

  it("exposes public package subpaths", () => {
    expectTypeOf(defineViewServerConfig).not.toBeAny();
    expectTypeOf(createViewServerReact).not.toBeAny();
    expectTypeOf(createInMemoryViewServerReact).not.toBeAny();
    expectTypeOf(runViewServerRuntime).not.toBeAny();
    expectTypeOf(createViewServerWebSocketServer).not.toBeAny();
    expectTypeOf(createInMemoryViewServer).not.toBeAny();
    expectTypeOf(decodeKafkaCodec).not.toBeAny();
    expectTypeOf(decodeKafkaCodecFromConfig).not.toBeAny();
    expectTypeOf(viewSchema).not.toBeAny();
    expectTypeOf<typeof PublicProfile.Type>().toEqualTypeOf<PublicProfile>();
    expectTypeOf<typeof PublicProfile.Encoded>().toEqualTypeOf<{
      readonly id: string;
      readonly score: string;
      readonly backup: EffectOption.Option<string>;
      readonly nickname?: string;
    }>();
    expectTypeOf<RowFromSchema<typeof PublicProfile>>().toEqualTypeOf<{
      readonly id: string;
      readonly score: number;
      readonly backup: EffectOption.Option<string>;
      readonly nickname?: string;
    }>();
    expectTypeOf(publicProfileViewServer.topics.profiles.schema).toEqualTypeOf<
      typeof PublicProfile
    >();
    expectTypeOf<KafkaCodecType<typeof kafkaOrderCodec>>().toEqualTypeOf<typeof Order.Type>();
    expectTypeOf<KafkaCodecType<typeof kafkaOrderCodecFromConfig>>().toEqualTypeOf<
      typeof Order.Type
    >();
    expectTypeOf(kafkaOrderCodec).not.toHaveProperty("schema");
    expectTypeOf<ViewServerLiveClient<typeof viewServer.topics>>().not.toBeAny();
  });

  it("preserves query result inference through public subpaths", () => {
    const result = react.useLiveQuery("orders", {
      select: ["id", "price"],
      where: [{ field: "status", type: "equals", filter: "open" }],
      orderBy: [{ field: "price", direction: "desc" }],
      limit: 10,
    });

    expectTypeOf(result).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly price: number;
      }>
    >();

    const profileResult = publicProfileReact.useLiveQuery("profiles", {
      select: ["id", "score"],
      where: [{ field: "score", type: "greaterThanOrEqual", filter: 10 }],
      orderBy: [{ field: "score", direction: "desc" }],
    });
    expectTypeOf(profileResult).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly score: number;
      }>
    >();
  });

  it("rejects invalid query and config contracts through public subpaths", () => {
    // @ts-expect-error raw queries must explicitly select columns.
    react.useLiveQuery("orders", { where: [{ field: "status", type: "equals", filter: "open" }] });

    const unknownProjectedFieldQuery = {
      select: ["missing"],
    } satisfies {
      readonly select: readonly ["missing"];
    };
    // @ts-expect-error projected fields must be topic field names.
    react.useLiveQuery("orders", unknownProjectedFieldQuery);

    const encodedProfileScoreQuery = {
      select: ["id"],
      where: [{ field: "score", type: "greaterThanOrEqual", filter: "10" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        {
          readonly field: "score";
          readonly type: "greaterThanOrEqual";
          readonly filter: "10";
        },
      ];
    };
    // @ts-expect-error transformed query operands must use the decoded number type.
    publicProfileReact.useLiveQuery("profiles", encodedProfileScoreQuery);

    const stringRangeFilterQuery = {
      select: ["id"],
      where: [{ field: "status", type: "greaterThanOrEqual", filter: "open" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        {
          readonly field: "status";
          readonly type: "greaterThanOrEqual";
          readonly filter: "open";
        },
      ];
    };
    // @ts-expect-error string fields do not accept range predicates.
    react.useLiveQuery("orders", stringRangeFilterQuery);

    // @ts-expect-error topic keys must reference fields on the topic schema.
    defineViewServerConfig({
      topics: {
        invalidOrders: {
          schema: Order,
          key: "missing",
        },
      },
    });

    // @ts-expect-error Schema.Class methods are not Topic Row key fields.
    defineViewServerConfig({
      topics: {
        invalidProfiles: {
          schema: PublicProfile,
          key: "upperId",
        },
      },
    });

    // @ts-expect-error public config/kafka rejects raw Row Schemas
    kafka.json(Order);
    // @ts-expect-error public config rejects direct canonical codecs
    kafkaFromConfig.json(Schema.toCodecJson(Order));

    const methodSelectQuery = {
      select: ["upperId"],
    } satisfies { readonly select: readonly ["upperId"] };
    // @ts-expect-error Schema.Class methods are not Topic Row select fields.
    publicProfileReact.useLiveQuery("profiles", methodSelectQuery);

    const methodWhereQuery = {
      select: ["id"],
      where: [{ field: "upperId", type: "equals", filter: () => "PROFILE" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        {
          readonly field: "upperId";
          readonly type: "equals";
          readonly filter: () => string;
        },
      ];
    };
    // @ts-expect-error Schema.Class methods are not Topic Row where fields.
    publicProfileReact.useLiveQuery("profiles", methodWhereQuery);

    const methodOrderQuery = {
      select: ["id"],
      orderBy: [{ field: "upperId", direction: "asc" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly orderBy: readonly [{ readonly field: "upperId"; readonly direction: "asc" }];
    };
    // @ts-expect-error Schema.Class methods are not Topic Row orderBy fields.
    publicProfileReact.useLiveQuery("profiles", methodOrderQuery);

    const methodGroupQuery = {
      groupBy: ["upperId"],
      aggregates: { rowCount: { aggFunc: "count" } },
    } satisfies {
      readonly groupBy: readonly ["upperId"];
      readonly aggregates: { readonly rowCount: { readonly aggFunc: "count" } };
    };
    // @ts-expect-error Schema.Class methods are not Topic Row groupBy fields.
    publicProfileReact.useLiveQuery("profiles", methodGroupQuery);

    const methodAggregateQuery = {
      groupBy: ["id"],
      aggregates: {
        distinctMethods: { aggFunc: "countDistinct", field: "upperId" },
      },
    } satisfies {
      readonly groupBy: readonly ["id"];
      readonly aggregates: {
        readonly distinctMethods: {
          readonly aggFunc: "countDistinct";
          readonly field: "upperId";
        };
      };
    };
    // @ts-expect-error Schema.Class methods are not Topic Row aggregate fields.
    publicProfileReact.useLiveQuery("profiles", methodAggregateQuery);

    const methodPatch = { upperId: () => "PROFILE" };
    const invalidMethodPatch = publicProfileInMemory.client.patch(
      "profiles",
      "profile-1",
      // @ts-expect-error Schema.Class methods are not Topic Row patch fields.
      methodPatch,
    );
    expectTypeOf(invalidMethodPatch).not.toBeAny();
  });

  it("preserves public in-memory client and React testing provider types", () => {
    const runtime = createInMemoryViewServer(viewServer);
    const testReact = createInMemoryViewServerReact(react);

    expectTypeOf(runtime.client.publish).parameter(0).toEqualTypeOf<"orders">();
    expectTypeOf(runtime.client.publish).parameter(1).toEqualTypeOf<typeof Order.Type>();
    expectTypeOf(testReact.client.publish).parameter(0).toEqualTypeOf<"orders">();
    expectTypeOf(testReact.ViewServerInMemoryProvider).not.toBeAny();
  });
});
