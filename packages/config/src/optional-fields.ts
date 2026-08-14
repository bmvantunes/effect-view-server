type DefinedFieldRecord<Value extends object> = {
  [Key in keyof Value]?: Exclude<Value[Key], undefined>;
};

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

export const conditionalFields = <Value extends object>(
  condition: boolean,
  fields: () => Value,
): Partial<Value> => (condition ? fields() : {});
