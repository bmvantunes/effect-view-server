export type SchemaValuePresenceToken =
  | readonly ["missing"]
  | readonly ["present", canonicalKey: string];

export const missingSchemaValuePresenceToken: SchemaValuePresenceToken = Object.freeze(["missing"]);

export const presentSchemaValuePresenceToken = (canonicalKey: string): SchemaValuePresenceToken => [
  "present",
  canonicalKey,
];

export const schemaValuePresenceKey = (token: SchemaValuePresenceToken): string =>
  JSON.stringify(token);
