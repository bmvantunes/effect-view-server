import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, kafka } from "@effect-view-server/config";
import { grpcSourceMarkers } from "@effect-view-server/config/internal";
import { Schema } from "effect";
import {
  makeTopicSourceBindings,
  topicGrpcSourceMetadataFromUnknown,
} from "./source-binding-resolution";

const Row = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
  region: Schema.String,
});

const viewServer = defineViewServerConfig({
  kafka: {
    usa: "localhost:9092",
  },
  topics: {
    externalOrders: {
      schema: Row,
      key: "id",
    },
    kafkaOrders: {
      schema: Row,
      key: "id",
      kafkaSource: kafka.source({
        topic: "orders-source",
        regions: ["usa"],
        value: kafka.json(() => Schema.toCodecJson(Row)),
        key: kafka.stringKey(),
        rowKey: ({ key }) => key,
        map: ({ value }) => ({
          price: value.price,
          region: value.region,
        }),
      }),
    },
    leasedOrders: {
      schema: Row,
      key: "id",
      grpcSource: grpcSourceMarkers.leased({
        routeBy: ["region"],
      }),
    },
    materializedOrders: {
      schema: Row,
      key: "id",
      grpcSource: grpcSourceMarkers.materialized(),
    },
  },
});

describe("source binding resolution", () => {
  it("derives canonical source bindings from topic-owned config", () => {
    const bindings = makeTopicSourceBindings(viewServer);

    expect(
      [...bindings].map(([topic, binding]) => ({
        grpcLeased: binding.grpcLeased,
        grpcMetadata: binding.grpcMetadata,
        owners: binding.owners,
        sourceOwned: binding.sourceOwned,
        topic,
      })),
    ).toStrictEqual([
      {
        grpcLeased: false,
        grpcMetadata: { _tag: "absent" },
        owners: [],
        sourceOwned: false,
        topic: "externalOrders",
      },
      {
        grpcLeased: false,
        grpcMetadata: { _tag: "absent" },
        owners: [{ _tag: "kafka" }],
        sourceOwned: true,
        topic: "kafkaOrders",
      },
      {
        grpcLeased: true,
        grpcMetadata: { _tag: "valid", lifecycle: "leased", routeBy: ["region"] },
        owners: [{ _tag: "grpc", lifecycle: "leased" }],
        sourceOwned: true,
        topic: "leasedOrders",
      },
      {
        grpcLeased: false,
        grpcMetadata: { _tag: "valid", lifecycle: "materialized" },
        owners: [{ _tag: "grpc", lifecycle: "materialized" }],
        sourceOwned: true,
        topic: "materializedOrders",
      },
    ]);
  });

  it("keeps malformed gRPC source metadata local to the binding", () => {
    const malformedOrders: {
      schema: typeof Row;
      key: "id";
    } = {
      schema: Row,
      key: "id",
    };
    const malformedSource = { kind: "grpc", lifecycle: "wat" };
    Object.defineProperty(malformedOrders, "grpcSource", {
      value: malformedSource,
    });
    const malformedViewServer = defineViewServerConfig({
      topics: {
        malformedOrders,
      },
    });

    expect(
      [...makeTopicSourceBindings(malformedViewServer)].map(([topic, binding]) => ({
        grpcMetadata: binding.grpcMetadata,
        owners: binding.owners,
        sourceOwned: binding.sourceOwned,
        topic,
      })),
    ).toStrictEqual([
      {
        grpcMetadata: { _tag: "invalid", cause: malformedSource },
        owners: [{ _tag: "grpc", lifecycle: "unknown" }],
        sourceOwned: true,
        topic: "malformedOrders",
      },
    ]);
  });

  it("classifies hostile gRPC source shapes without caller reflection", () => {
    const topicDefinition = (): {
      schema: typeof Row;
      key: "id";
    } => ({
      schema: Row,
      key: "id",
    });
    const topicDefinitions = () => ({
      incompleteConcreteLeased: topicDefinition(),
      incompleteConcreteMaterialized: topicDefinition(),
      invalidKind: topicDefinition(),
      invalidLeasedRouteBy: topicDefinition(),
      invalidLifecycle: topicDefinition(),
      materializedExtraKey: topicDefinition(),
      materializedWrongTag: topicDefinition(),
      nonStringLeasedRouteBy: topicDefinition(),
      primitiveSource: topicDefinition(),
      validConcreteLeased: topicDefinition(),
      validConcreteMaterialized: topicDefinition(),
      leasedExtraKey: topicDefinition(),
      leasedWrongTag: topicDefinition(),
      releaseIsNotCallable: topicDefinition(),
    });
    const hostileTopics = topicDefinitions();
    const request = () => undefined;
    const acquire = () => undefined;
    const release = () => undefined;
    const map = () => undefined;
    Object.defineProperty(hostileTopics.incompleteConcreteLeased, "grpcSource", {
      value: {
        _tag: "GrpcLeasedTopicSource",
        kind: "grpc",
        lifecycle: "leased",
        routeBy: ["region"],
        client: "orders",
      },
    });
    Object.defineProperty(hostileTopics.incompleteConcreteMaterialized, "grpcSource", {
      value: {
        _tag: "GrpcMaterializedTopicSource",
        kind: "grpc",
        lifecycle: "materialized",
        client: "orders",
      },
    });
    Object.defineProperty(hostileTopics.invalidKind, "grpcSource", {
      value: { kind: "not-grpc", lifecycle: "leased" },
    });
    Object.defineProperty(hostileTopics.invalidLeasedRouteBy, "grpcSource", {
      value: {
        _tag: "GrpcLeasedTopicSource",
        kind: "grpc",
        lifecycle: "leased",
        routeBy: [],
      },
    });
    Object.defineProperty(hostileTopics.invalidLifecycle, "grpcSource", {
      value: { kind: "grpc", lifecycle: "wat" },
    });
    Object.defineProperty(hostileTopics.materializedExtraKey, "grpcSource", {
      value: {
        _tag: "GrpcMaterializedTopicSource",
        extra: true,
        kind: "grpc",
        lifecycle: "materialized",
      },
    });
    Object.defineProperty(hostileTopics.materializedWrongTag, "grpcSource", {
      value: { _tag: "Wrong", kind: "grpc", lifecycle: "materialized" },
    });
    Object.defineProperty(hostileTopics.nonStringLeasedRouteBy, "grpcSource", {
      value: {
        _tag: "GrpcLeasedTopicSource",
        kind: "grpc",
        lifecycle: "leased",
        routeBy: ["region", 1],
      },
    });
    Object.defineProperty(hostileTopics.primitiveSource, "grpcSource", {
      value: "not-an-object",
    });
    Object.defineProperty(hostileTopics.validConcreteLeased, "grpcSource", {
      value: {
        _tag: "GrpcLeasedTopicSource",
        kind: "grpc",
        lifecycle: "leased",
        routeBy: ["region"],
        client: "orders",
        method: "stream",
        request,
        acquire,
        release,
        map,
      },
    });
    Object.defineProperty(hostileTopics.validConcreteMaterialized, "grpcSource", {
      value: {
        _tag: "GrpcMaterializedTopicSource",
        kind: "grpc",
        lifecycle: "materialized",
        client: "orders",
        method: "stream",
        request,
        acquire,
        map,
      },
    });
    Object.defineProperty(hostileTopics.leasedExtraKey, "grpcSource", {
      value: {
        _tag: "GrpcLeasedTopicSource",
        extra: true,
        kind: "grpc",
        lifecycle: "leased",
        routeBy: ["region"],
      },
    });
    Object.defineProperty(hostileTopics.leasedWrongTag, "grpcSource", {
      value: {
        _tag: "Wrong",
        kind: "grpc",
        lifecycle: "leased",
        routeBy: ["region"],
      },
    });
    Object.defineProperty(hostileTopics.releaseIsNotCallable, "grpcSource", {
      value: {
        _tag: "GrpcMaterializedTopicSource",
        kind: "grpc",
        lifecycle: "materialized",
        client: "orders",
        method: "stream",
        request,
        acquire,
        release: "nope",
        map,
      },
    });
    const hostileViewServer = defineViewServerConfig({
      topics: topicDefinitions(),
    });
    const hostileTopicsWithPrimitive = {
      ...hostileTopics,
    };
    Object.defineProperty(hostileTopicsWithPrimitive, "primitiveTopic", {
      enumerable: true,
      value: null,
    });

    expect(
      [
        ...makeTopicSourceBindings({
          ...hostileViewServer,
          topics: hostileTopicsWithPrimitive,
        }),
      ].map(([topic, binding]) => ({
        grpcMetadataTag: binding.grpcMetadata._tag,
        owners: binding.owners,
        topic,
      })),
    ).toStrictEqual([
      {
        grpcMetadataTag: "invalid",
        owners: [{ _tag: "grpc", lifecycle: "leased" }],
        topic: "incompleteConcreteLeased",
      },
      {
        grpcMetadataTag: "invalid",
        owners: [{ _tag: "grpc", lifecycle: "materialized" }],
        topic: "incompleteConcreteMaterialized",
      },
      {
        grpcMetadataTag: "invalid",
        owners: [{ _tag: "grpc", lifecycle: "unknown" }],
        topic: "invalidKind",
      },
      {
        grpcMetadataTag: "invalid",
        owners: [{ _tag: "grpc", lifecycle: "leased" }],
        topic: "invalidLeasedRouteBy",
      },
      {
        grpcMetadataTag: "invalid",
        owners: [{ _tag: "grpc", lifecycle: "unknown" }],
        topic: "invalidLifecycle",
      },
      {
        grpcMetadataTag: "invalid",
        owners: [{ _tag: "grpc", lifecycle: "leased" }],
        topic: "leasedExtraKey",
      },
      {
        grpcMetadataTag: "invalid",
        owners: [{ _tag: "grpc", lifecycle: "leased" }],
        topic: "leasedWrongTag",
      },
      {
        grpcMetadataTag: "invalid",
        owners: [{ _tag: "grpc", lifecycle: "materialized" }],
        topic: "materializedExtraKey",
      },
      {
        grpcMetadataTag: "invalid",
        owners: [{ _tag: "grpc", lifecycle: "materialized" }],
        topic: "materializedWrongTag",
      },
      {
        grpcMetadataTag: "invalid",
        owners: [{ _tag: "grpc", lifecycle: "leased" }],
        topic: "nonStringLeasedRouteBy",
      },
      {
        grpcMetadataTag: "invalid",
        owners: [{ _tag: "grpc", lifecycle: "unknown" }],
        topic: "primitiveSource",
      },
      {
        grpcMetadataTag: "absent",
        owners: [],
        topic: "primitiveTopic",
      },
      {
        grpcMetadataTag: "invalid",
        owners: [{ _tag: "grpc", lifecycle: "materialized" }],
        topic: "releaseIsNotCallable",
      },
      {
        grpcMetadataTag: "valid",
        owners: [{ _tag: "grpc", lifecycle: "leased" }],
        topic: "validConcreteLeased",
      },
      {
        grpcMetadataTag: "valid",
        owners: [{ _tag: "grpc", lifecycle: "materialized" }],
        topic: "validConcreteMaterialized",
      },
    ]);
  });

  it("treats primitive topic definitions as absent gRPC source metadata", () => {
    expect(topicGrpcSourceMetadataFromUnknown(undefined)).toStrictEqual({ _tag: "absent" });
    expect(topicGrpcSourceMetadataFromUnknown(null)).toStrictEqual({ _tag: "absent" });
  });
});
