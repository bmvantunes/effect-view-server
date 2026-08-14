type OptionalFieldRecord<Value extends object> = Partial<Value>;

type DefinedFieldRecord<Value extends object> = {
  [Key in keyof Value]?: Exclude<Value[Key], undefined>;
};

/** Build fields from a value only when that value is defined, preserving narrowing in the factory. */
export const definedFields = <Value, Fields extends object>(
  value: Value | undefined,
  fields: (value: Value) => Fields,
): DefinedFieldRecord<Fields> => {
  if (value === undefined) return {};
  const source = fields(value);
  const output: DefinedFieldRecord<Fields> = {};
  for (const key of Reflect.ownKeys(source)) {
    const descriptor = Object.getOwnPropertyDescriptor(source, key);
    if (descriptor?.enumerable !== true || !("value" in descriptor)) continue;
    const fieldValue = descriptor.value;
    if (fieldValue !== undefined) {
      Object.defineProperty(output, key, {
        configurable: true,
        enumerable: true,
        value: fieldValue,
        writable: true,
      });
    }
  }
  return output;
};

/** Build a partial object only when its condition is true. */
export const conditionalFields = <Value extends object>(
  condition: boolean,
  fields: () => Value,
): OptionalFieldRecord<Value> => (condition ? fields() : {});
