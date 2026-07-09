import type { DecodableTopicDefinitions } from "@effect-view-server/column-live-view-engine";
import type { ViewServerTopicConfig } from "@effect-view-server/config";

export type TopicDefinitionHasRequiredDefinedObjectProperty<
  Definition,
  Key extends string,
> = Key extends keyof Definition
  ? undefined extends Definition[Key]
    ? false
    : Exclude<Definition[Key], undefined> extends object
      ? true
      : false
  : false;

export type TopicDefinitionHasSourceOwner<Definition> = true extends
  | TopicDefinitionHasRequiredDefinedObjectProperty<Definition, "kafkaSource">
  | TopicDefinitionHasRequiredDefinedObjectProperty<Definition, "grpcSource">
  ? true
  : false;

export type TopicGrpcSourceLifecycle = "leased" | "materialized" | "unknown";

export type TopicGrpcSourceMetadata =
  | {
      readonly _tag: "absent";
    }
  | {
      readonly _tag: "invalid";
      readonly cause: unknown;
    }
  | {
      readonly _tag: "valid";
      readonly lifecycle: "materialized";
    }
  | {
      readonly _tag: "valid";
      readonly lifecycle: "leased";
      readonly routeBy: ReadonlyArray<string>;
    };

export type TopicGrpcSourceValidMetadata = Extract<
  TopicGrpcSourceMetadata,
  { readonly _tag: "valid" }
>;

export type TopicSourceOwner =
  | {
      readonly _tag: "kafka";
    }
  | {
      readonly _tag: "grpc";
      readonly lifecycle: TopicGrpcSourceLifecycle;
    };

export type TopicSourceBinding = {
  readonly grpcLeased: boolean;
  readonly grpcMetadata: TopicGrpcSourceMetadata;
  readonly grpcSource: unknown;
  readonly kafkaSource: unknown;
  readonly owners: ReadonlyArray<TopicSourceOwner>;
  readonly sourceOwned: boolean;
  readonly topic: string;
};

const hasDefinedOwnProperty = (value: object, key: string): boolean =>
  Object.prototype.hasOwnProperty.call(value, key) && Reflect.get(value, key) !== undefined;

const hasOnlyOwnStringKeys = (value: object, allowedKeys: ReadonlyArray<string>): boolean =>
  Object.getOwnPropertyNames(value).every((key) => allowedKeys.includes(key));

const hasCallableOwnProperty = (value: object, key: string): boolean =>
  typeof Reflect.get(value, key) === "function";

const hasConcreteGrpcBinding = (source: object): boolean =>
  ["client", "method", "request", "acquire", "release", "map"].some((key) =>
    hasDefinedOwnProperty(source, key),
  );

const hasCompleteConcreteGrpcBinding = (source: object): boolean =>
  hasDefinedOwnProperty(source, "client") &&
  hasDefinedOwnProperty(source, "method") &&
  hasCallableOwnProperty(source, "request") &&
  hasCallableOwnProperty(source, "acquire") &&
  hasCallableOwnProperty(source, "map") &&
  (!hasDefinedOwnProperty(source, "release") || hasCallableOwnProperty(source, "release"));

const grpcTopicSourceFromUnknown = (source: unknown): TopicGrpcSourceMetadata => {
  if (typeof source !== "object" || source === null) {
    return { _tag: "invalid", cause: source };
  }
  if (Reflect.get(source, "kind") !== "grpc") {
    return { _tag: "invalid", cause: source };
  }
  const lifecycle = Reflect.get(source, "lifecycle");
  if (lifecycle !== "leased" && lifecycle !== "materialized") {
    return { _tag: "invalid", cause: source };
  }
  const sourceTag = Reflect.get(source, "_tag");
  if (lifecycle === "materialized") {
    if (
      !hasOnlyOwnStringKeys(source, [
        "_tag",
        "kind",
        "lifecycle",
        "client",
        "method",
        "request",
        "acquire",
        "release",
        "map",
      ])
    ) {
      return { _tag: "invalid", cause: source };
    }
    if (sourceTag !== "GrpcMaterializedTopicSource") {
      return { _tag: "invalid", cause: source };
    }
    if (hasConcreteGrpcBinding(source) && !hasCompleteConcreteGrpcBinding(source)) {
      return { _tag: "invalid", cause: source };
    }
    return { _tag: "valid", lifecycle };
  }
  if (
    !hasOnlyOwnStringKeys(source, [
      "_tag",
      "kind",
      "lifecycle",
      "routeBy",
      "client",
      "method",
      "request",
      "acquire",
      "release",
      "map",
    ])
  ) {
    return { _tag: "invalid", cause: source };
  }
  if (sourceTag !== "GrpcLeasedTopicSource") {
    return { _tag: "invalid", cause: source };
  }
  if (hasConcreteGrpcBinding(source) && !hasCompleteConcreteGrpcBinding(source)) {
    return { _tag: "invalid", cause: source };
  }
  const routeBy = Reflect.get(source, "routeBy");
  if (
    !Array.isArray(routeBy) ||
    routeBy.length === 0 ||
    !routeBy.every((field) => typeof field === "string")
  ) {
    return { _tag: "invalid", cause: source };
  }
  return { _tag: "valid", lifecycle, routeBy };
};

export const topicGrpcSourceMetadataFromUnknown = (
  topicDefinition: unknown,
): TopicGrpcSourceMetadata => {
  if (typeof topicDefinition !== "object" || topicDefinition === null) {
    return { _tag: "absent" };
  }
  if (!hasDefinedOwnProperty(topicDefinition, "grpcSource")) {
    return { _tag: "absent" };
  }
  return grpcTopicSourceFromUnknown(Reflect.get(topicDefinition, "grpcSource"));
};

const topicKafkaSourceFromUnknown = (topicDefinition: unknown): unknown => {
  if (typeof topicDefinition !== "object" || topicDefinition === null) {
    return undefined;
  }
  return hasDefinedOwnProperty(topicDefinition, "kafkaSource")
    ? Reflect.get(topicDefinition, "kafkaSource")
    : undefined;
};

const topicGrpcSourceFromUnknown = (topicDefinition: unknown): unknown => {
  if (typeof topicDefinition !== "object" || topicDefinition === null) {
    return undefined;
  }
  return hasDefinedOwnProperty(topicDefinition, "grpcSource")
    ? Reflect.get(topicDefinition, "grpcSource")
    : undefined;
};

const grpcDeclaredLifecycleFromUnknown = (source: unknown): TopicGrpcSourceLifecycle => {
  if (typeof source !== "object" || source === null || Reflect.get(source, "kind") !== "grpc") {
    return "unknown";
  }
  const lifecycle = Reflect.get(source, "lifecycle");
  return lifecycle === "leased" || lifecycle === "materialized" ? lifecycle : "unknown";
};

const topicSourceBinding = (topic: string, definition: unknown): TopicSourceBinding => {
  const kafkaSource = topicKafkaSourceFromUnknown(definition);
  const grpcSource = topicGrpcSourceFromUnknown(definition);
  const grpcMetadata = topicGrpcSourceMetadataFromUnknown(definition);
  const owners: Array<TopicSourceOwner> = [];
  if (kafkaSource !== undefined) {
    owners.push({ _tag: "kafka" });
  }
  if (grpcSource !== undefined) {
    owners.push({
      _tag: "grpc",
      lifecycle: grpcDeclaredLifecycleFromUnknown(grpcSource),
    });
  }
  return {
    grpcLeased: owners.some((owner) => owner._tag === "grpc" && owner.lifecycle === "leased"),
    grpcMetadata,
    grpcSource,
    kafkaSource,
    owners,
    sourceOwned: owners.length > 0,
    topic,
  };
};

export const makeTopicSourceBindings = <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
): ReadonlyMap<string, TopicSourceBinding> =>
  new Map(
    Object.entries(config.topics)
      .map(
        ([topic, definition]) =>
          [topic, topicSourceBinding(topic, definition)] satisfies [string, TopicSourceBinding],
      )
      .sort(([left], [right]) => left.localeCompare(right)),
  );
