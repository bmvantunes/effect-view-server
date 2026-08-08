import type {
  RowSchema,
  SourceDefinitionAny,
  TopicDefinitions,
  ViewServerTopicConfig,
} from "@effect-view-server/config";
import { isSourceDefinition } from "@effect-view-server/source-adapter/internal";
import { Schema } from "effect";

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

export type TopicDefinitionHasSourceOwner<Definition> =
  TopicDefinitionHasRequiredDefinedObjectProperty<Definition, "source">;

export type TopicSourceOwner = {
  readonly _tag: "source";
  readonly lifecycle: "materialized" | "leased" | "unknown";
};

export type TopicSourceBinding = {
  readonly schema: RowSchema | undefined;
  readonly source: SourceDefinitionAny | undefined;
  readonly sourceLifecycle: "materialized" | "leased" | "unknown";
  readonly sourceLeased: boolean;
  readonly owners: ReadonlyArray<TopicSourceOwner>;
  readonly sourceOwned: boolean;
  readonly topic: string;
};

const hasDefinedOwnProperty = (value: object, key: string): boolean =>
  Object.prototype.hasOwnProperty.call(value, key) && Reflect.get(value, key) !== undefined;

const isRowSchema = (value: unknown): value is RowSchema =>
  Schema.isSchema(value) &&
  (typeof value === "object" || typeof value === "function") &&
  value !== null &&
  "fields" in value;

const topicCanonicalSourceFromUnknown = (
  topicDefinition: unknown,
): SourceDefinitionAny | undefined => {
  if (
    typeof topicDefinition !== "object" ||
    topicDefinition === null ||
    !hasDefinedOwnProperty(topicDefinition, "source")
  ) {
    return undefined;
  }
  const source = Reflect.get(topicDefinition, "source");
  return isSourceDefinition(source) ? source : undefined;
};

const topicSourceBinding = (topic: string, definition: unknown): TopicSourceBinding => {
  const sourceDeclared =
    typeof definition === "object" &&
    definition !== null &&
    hasDefinedOwnProperty(definition, "source");
  const source = topicCanonicalSourceFromUnknown(definition);
  const owners: ReadonlyArray<TopicSourceOwner> = sourceDeclared
    ? [
        {
          _tag: "source",
          lifecycle: source?.lifecycle ?? "unknown",
        },
      ]
    : [];
  const schema =
    typeof definition === "object" && definition !== null
      ? Reflect.get(definition, "schema")
      : undefined;
  return {
    schema: isRowSchema(schema) ? schema : undefined,
    source,
    sourceLifecycle: source?.lifecycle ?? "unknown",
    sourceLeased: source?.lifecycle === "leased",
    owners,
    sourceOwned: sourceDeclared,
    topic,
  };
};

export const makeTopicSourceBindings = <const Topics extends TopicDefinitions>(
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
