import { Result, Schedule } from "effect";
import type {
  SourceAdapterHandle,
  SourceDefinition,
  SourceDefinitionOptionsFamily,
  SourceLifecycle,
  SourceLifecycleDeclaration,
} from "./model";
import { isSourceAdapterHandle } from "./adapter-brand";

type SourceLifecycleDeclarationAny = SourceLifecycleDeclaration<
  unknown,
  unknown,
  unknown,
  SourceDefinitionOptionsFamily
>;

export const SourceDefinitionTypeId: unique symbol = Symbol(
  "@effect-view-server/source-adapter/SourceDefinition",
);
export const SourceDefinitionTypesTypeId: unique symbol = Symbol(
  "@effect-view-server/source-adapter/SourceDefinitionTypes",
);

const sourceDefinitions = new WeakSet<object>();

export const registerSourceDefinition = <Definition extends object>(
  definition: Definition,
): Definition => {
  sourceDefinitions.add(definition);
  return definition;
};

const hasSourceModelSelfBrand = (value: object, key: symbol): boolean => {
  const inspected = Result.try(() => Reflect.get(value, key));
  if (Result.isFailure(inspected) || typeof inspected.success !== "function") {
    return false;
  }
  const branded = Result.try(() => Reflect.apply(inspected.success, undefined, []));
  return Result.isSuccess(branded) && branded.success === value;
};

const hasExactDefinitionDataKeys = (
  value: object,
  expectedKeys: ReadonlyArray<PropertyKey>,
): boolean => {
  const keys = Result.try(() => Reflect.ownKeys(value));
  if (
    Result.isFailure(keys) ||
    keys.success.length !== expectedKeys.length ||
    keys.success.some((key) => !expectedKeys.includes(key))
  ) {
    return false;
  }
  return expectedKeys.every((key) => {
    const descriptor = Result.try(() => Object.getOwnPropertyDescriptor(value, key));
    return (
      Result.isSuccess(descriptor) &&
      descriptor.success !== undefined &&
      descriptor.success.enumerable === true &&
      "value" in descriptor.success
    );
  });
};

const validateSourceDefinitionEnvelope = (value: unknown): boolean => {
  if (
    typeof value !== "object" ||
    value === null ||
    !sourceDefinitions.has(value) ||
    !hasSourceModelSelfBrand(value, SourceDefinitionTypeId) ||
    !hasExactDefinitionDataKeys(value, [
      "adapter",
      "identity",
      "lifecycle",
      "options",
      "routeBy",
      "retry",
      SourceDefinitionTypeId,
      SourceDefinitionTypesTypeId,
    ]) ||
    !Object.isFrozen(value)
  ) {
    return false;
  }
  const adapter = Reflect.get(value, "adapter");
  const identity = Reflect.get(value, "identity");
  const lifecycle = Reflect.get(value, "lifecycle");
  const options = Reflect.get(value, "options");
  const routeBy = Reflect.get(value, "routeBy");
  const retry = Reflect.get(value, "retry");
  const types = Reflect.get(value, SourceDefinitionTypesTypeId);
  if (
    typeof adapter !== "object" ||
    adapter === null ||
    !isSourceAdapterHandle(adapter) ||
    !Object.isFrozen(adapter) ||
    Reflect.get(adapter, "identity") !== identity ||
    (lifecycle !== "materialized" && lifecycle !== "leased") ||
    !Array.isArray(routeBy) ||
    !Object.isFrozen(routeBy) ||
    routeBy.some((field) => typeof field !== "string" || field.length === 0) ||
    new Set(routeBy).size !== routeBy.length ||
    (lifecycle === "materialized" ? routeBy.length !== 0 : routeBy.length === 0) ||
    Reflect.get(adapter, lifecycle) === undefined ||
    typeof retry !== "object" ||
    retry === null ||
    !Object.isFrozen(retry) ||
    typeof types !== "object" ||
    types === null ||
    !hasExactDefinitionDataKeys(types, ["adapter", "lifecycle", "options", "routeFields"]) ||
    !Object.isFrozen(types) ||
    Reflect.get(types, "adapter") !== adapter ||
    Reflect.get(types, "lifecycle") !== lifecycle ||
    Reflect.get(types, "options") !== options ||
    Reflect.get(types, "routeFields") !== routeBy
  ) {
    return false;
  }
  const retryTag = Reflect.get(retry, "_tag");
  return retryTag === "UseAdapterDefault"
    ? hasExactDefinitionDataKeys(retry, ["_tag"])
    : retryTag === "Override" &&
        hasExactDefinitionDataKeys(retry, ["_tag", "policy"]) &&
        Schedule.isSchedule(Reflect.get(retry, "policy"));
};

export const validateSourceDefinition = (value: unknown): boolean => {
  const validation = Result.try(() => validateSourceDefinitionEnvelope(value));
  return Result.isSuccess(validation) && validation.success;
};

export const isSourceDefinition = (
  value: unknown,
): value is SourceDefinition<
  SourceAdapterHandle<
    string,
    string | undefined,
    unknown,
    SourceLifecycleDeclarationAny | undefined,
    SourceLifecycleDeclarationAny | undefined
  >,
  SourceLifecycle,
  unknown,
  ReadonlyArray<string>,
  never
> => validateSourceDefinition(value);
