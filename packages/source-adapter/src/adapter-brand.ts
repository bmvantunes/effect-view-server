import { Result } from "effect";
import type {
  SourceAdapterDescriptor,
  SourceLifecycleDeclaration,
  SourceDefinitionOptionsFamily,
} from "./model";

type SourceLifecycleDeclarationAny = SourceLifecycleDeclaration<
  unknown,
  unknown,
  unknown,
  SourceDefinitionOptionsFamily
>;

export const SourceAdapterTypeId: unique symbol = Symbol(
  "@effect-view-server/source-adapter/SourceAdapter",
);
export const sourceAdapterHandles = new WeakMap<object, unknown>();
export const sourceAdapterDescriptors = new WeakMap<object, unknown>();

export const hasSourceModelSelfBrand = (value: unknown, key: symbol): boolean => {
  if ((typeof value !== "object" && typeof value !== "function") || value === null) {
    return false;
  }
  const inspected = Result.try(() => Reflect.get(value, key));
  if (Result.isFailure(inspected) || typeof inspected.success !== "function") {
    return false;
  }
  const branded = Result.try(() => Reflect.apply(inspected.success, undefined, []));
  return Result.isSuccess(branded) && branded.success === value;
};

export function isSourceAdapterHandle<
  Name extends string,
  Version extends string | undefined,
  AdapterFailure,
  Materialized extends SourceLifecycleDeclarationAny | undefined,
  Leased extends SourceLifecycleDeclarationAny | undefined,
>(value: SourceAdapterDescriptor<Name, Version, AdapterFailure, Materialized, Leased>): boolean;
export function isSourceAdapterHandle(
  value: unknown,
): value is SourceAdapterDescriptor<
  string,
  string | undefined,
  unknown,
  SourceLifecycleDeclarationAny | undefined,
  SourceLifecycleDeclarationAny | undefined
>;
export function isSourceAdapterHandle(value: unknown): boolean {
  return (
    typeof value === "object" &&
    value !== null &&
    sourceAdapterHandles.has(value) &&
    Reflect.get(value, SourceAdapterTypeId) === SourceAdapterTypeId
  );
}
