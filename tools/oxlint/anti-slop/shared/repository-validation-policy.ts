/*
 * Repository adapter for the generic anti-slop analyzer.
 *
 * The analyzer only knows how to consume this policy shape. Keeping the
 * project-specific validation exports here makes the seam explicit and keeps
 * the AST analysis independent from this repository's module layout.
 */

import type { RuntimeLiteralValue } from "./dictionary-types.ts";

export type ReviewedImportedTypeContract = {
	readonly tags: ReadonlySet<string>;
	readonly structural: boolean;
	readonly properties: ReadonlyMap<string, ReadonlySet<string>>;
	readonly literalProperties?: ReadonlyMap<string, ReadonlySet<RuntimeLiteralValue>>;
};

export const reviewedImportedTypeContracts = new Map<string, ReviewedImportedTypeContract>([
	[
		"@bufbuild/protobuf:DescService",
		{
			tags: new Set(["object"]),
			structural: true,
			properties: new Map([
				["kind", new Set(["string"])],
				["name", new Set(["string"])],
				["typeName", new Set(["string"])],
				["toString", new Set(["function"])],
				["file", new Set(["object"])],
				["file.kind", new Set(["string"])],
				["file.services", new Set(["object"])],
				["methods", new Set(["object"])],
				["method", new Set(["object"])],
			]),
			literalProperties: new Map(),
		},
	],
	[
		"@bufbuild/protobuf:DescMethodServerStreaming",
		{
			tags: new Set(["object"]),
			structural: true,
			properties: new Map([
				["kind", new Set(["string"])],
				["methodKind", new Set(["string"])],
				["parent", new Set(["object"])],
				["input", new Set(["object"])],
				["output", new Set(["object"])],
				["name", new Set(["string"])],
				["localName", new Set(["string"])],
				["toString", new Set(["function"])],
			]),
			literalProperties: new Map([["methodKind", new Set(["server_streaming"])]])
		},
	],
	[
		"@bufbuild/protobuf:JsonValue",
		{
			tags: new Set(["object", "string", "boolean", "number"]),
			structural: false,
			properties: new Map(),
			literalProperties: new Map(),
		},
	],
	[
		"@effect-view-server/config:RowSchema",
		{
			tags: new Set(["object"]),
			structural: true,
			properties: new Map([
				["fields", new Set(["object"])],
				["ast", new Set(["object"])],
			]),
			literalProperties: new Map(),
		},
	],
	[
		"./topic-contract:RowSchema",
		{
			tags: new Set(["object"]),
			structural: true,
			properties: new Map([
				["fields", new Set(["object"])],
				["ast", new Set(["object"])],
			]),
			literalProperties: new Map(),
		},
	],
	[
		"@effect-view-server/config:ViewServerRuntimeError",
		{
			tags: new Set(["object"]),
			structural: true,
			properties: new Map([
				["_tag", new Set(["string"])],
				["code", new Set(["string"])],
				["message", new Set(["string"])],
				["topic", new Set(["string"])],
				["queryId", new Set(["string"])],
				["queuedEvents", new Set(["number"])],
				["maxQueueDepth", new Set(["number"])],
			]),
			literalProperties: new Map([
				["_tag", new Set(["ViewServerRuntimeError", "ViewServerBackpressureError"])],
				[
					"code",
					new Set([
						"BackpressureExceeded",
						"InvalidTopic",
						"InvalidRow",
						"InvalidQuery",
						"UnsupportedQuery",
						"SnapshotStale",
						"RuntimeUnavailable",
						"RuntimeResetFailed",
					]),
				],
			]),
		},
	],
	[
		"effect-view-server/source-adapter:SourceRetryPolicy",
		{
			tags: new Set(["object"]),
			structural: false,
			properties: new Map(),
			literalProperties: new Map(),
		},
	],
]);

export const namedPredicateContracts = new Map<string, ReadonlySet<string>>([
	["BigDecimal.isBigDecimal", new Set(["BigDecimal"])],
	["isBigDecimal", new Set(["BigDecimal"])],
	["inspectWireSafeBigDecimal", new Set(["BigDecimal", "WireSafeBigDecimal"])],
	["isTrustedWireSafeBigDecimal", new Set(["BigDecimal", "WireSafeBigDecimal"])],
	["isWireSafeBigDecimal", new Set(["BigDecimal", "WireSafeBigDecimal"])],
	["Layer.isLayer", new Set(["Layer"])],
	["isBenchmarkEngineHealth", new Set(["BenchmarkEngineHealth"])],
	["isSourceAdapterHandle", new Set(["SourceAdapterHandle"])],
	["registeredDescriptorMatchesHandle", new Set(["SourceAdapterDescriptor"])],
	["isSourceDefinition", new Set(["SourceDefinition", "AnySourceDefinition"])],
	["Schedule.isSchedule", new Set(["SourceRetryPolicy"])],
	["exactDataEntries", new Set(["GrpcNodeClientOptions"])],
	["hasPlainRecordPrototype", new Set(["Record"])],
	["inspectPlainRecordData", new Set(["Record"])],
	["inspectPlainRecordShape", new Set(["Record"])],
	["isPlainRecord", new Set(["Record"])],
	["plainRecordSnapshot", new Set(["Record"])],
]);

export const trustedSchemaUnknownTypePaths = new Set(["Schema.Schema.Type", "Schema.Type"]);
export const trustedSchemaUnknownValuePaths = new Set(["Schema.Unknown", "Unknown"]);

export const knownValidationCalls = new Set([
  "accepts",
  "canonicalWhereKey",
  "captureOwnDataValues",
  "compareOrderedRangeValue",
  "compareOrderedSlotRangeValue",
  "decodeJsonFieldValue",
  "decodeGroupedQuery",
  "decodeKafkaDurationInput",
  "decodeRawQuery",
  "decodeTypedGroupedQuery",
  "denseArraySnapshot",
  "denseArrayValues",
  "exactArrayValues",
  "exactDataEntries",
  "encodeJsonFieldValue",
  "hasPlainRecordPrototype",
  "inspectArrayData",
  "inspectDenseArrayData",
  "inspectPlainRecordData",
  "inspectPlainRecordShape",
  "inspectWireSafeBigDecimal",
  "inspect",
  "isBenchmarkEngineHealth",
  "invokeLayerConstructor",
  "isBigDecimal",
  "isTrustedWireSafeBigDecimal",
  "isWireSafeBigDecimal",
  "normalizeWhere",
  "normalizeFilterBigDecimal",
  "ownScalar",
  "plainRecordSnapshot",
  "protocolDenseArray",
  "protocolRecordSnapshot",
  "readOwnDataProperty",
  "readProperty",
  "recordValues",
  "Reflect.apply",
  "schemaAst",
  "stableQueryValueString",
  "validateDecodedRow",
  "validateExactLeasedHealthRoutes",
  "validateExactRoute",
  "validateExactSourceHealth",
  "jsonObject",
  "jsonResponse",
  "isPlainRecord",
  "materializeJsonFieldValue",
  "materializeStrictJson",
  "viewServerFilterFieldContracts",
]);

export const validationCallbackWrappers = new Set([
  "Effect.try",
  "Effect.tryPromise",
  "Result.try",
  "inspect",
]);

export const validationFactoryCalls = new Set([
  "Schema.decodeUnknownEffect",
  "Schema.decodeUnknownExit",
  "Schema.decodeUnknownResult",
  "Schema.decodeUnknownSync",
  "Schema.encodeUnknownEffect",
  "Schema.encodeUnknownExit",
  "Schema.encodeUnknownResult",
  "Schema.encodeUnknownSync",
  "Schema.is",
]);

// Imported names are trusted only when both the module specifier and export
// have been reviewed as validation infrastructure. A local alias with a
// familiar name must not make an unrelated function count as evidence.
export const trustedValidationImportExports = new Map<string, ReadonlySet<string>>([
  [
    "effect",
    new Set(["BigDecimal", "Cause", "Effect", "Exit", "Layer", "Option", "Result", "Schedule", "Schema"]),
  ],
  ["effect/BigDecimal", new Set(["isBigDecimal"])],
  [
    "@effect-view-server/config/internal",
    new Set(["validateDecodedRow", "viewServerFilterFieldContracts"]),
  ],
  [
    "@effect-view-server/effect-utils",
    new Set([
      "hasPlainRecordPrototype",
      "inspectArrayData",
      "inspectDenseArrayData",
      "inspectPlainRecordData",
      "inspectPlainRecordShape",
      "inspectWireSafeBigDecimal",
      "isTrustedWireSafeBigDecimal",
      "isWireSafeBigDecimal",
    ]),
  ],
  ["@effect-view-server/source-adapter/internal", new Set(["isSourceAdapterHandle", "isSourceDefinition"])],
  ["./adapter-brand", new Set(["hasSourceModelSelfBrand", "isSourceAdapterHandle"])],
  ["./definition-brand", new Set(["isSourceDefinition", "validateSourceDefinition"])],
  [
    "effect-view-server/value-semantics",
    new Set([
      "inspectWireSafeBigDecimal",
      "isTrustedWireSafeBigDecimal",
      "isWireSafeBigDecimal",
    ]),
  ],
  ["./benchmark-artifact", new Set(["isBenchmarkEngineHealth"])],
  ["./contract", new Set(["decodeKafkaDurationInput"])],
  ["./decoded-row-validation", new Set(["validateDecodedRow"])],
  ["./exact-shape", new Set(["exactArrayValues", "exactDataEntries"])],
  ["./filter-expression", new Set(["normalizeWhere"])],
  [
    "./protocol-json-field-codec",
    new Set(["decodeJsonFieldValue", "encodeJsonFieldValue", "materializeJsonFieldValue"]),
  ],
  ["./protocol-structural-value", new Set(["protocolDenseArray", "protocolRecordSnapshot"])],
  ["./query-structural-data", new Set(["denseArrayValues", "plainRecordSnapshot"])],
  ["./query-value", new Set(["stableQueryValueString"])],
  ["./query-where-key", new Set(["canonicalWhereKey"])],
  ["./grouped-query-decoder", new Set(["decodeGroupedQuery", "decodeTypedGroupedQuery"])],
  ["./grouped-query-decoder.ts", new Set(["decodeGroupedQuery", "decodeTypedGroupedQuery"])],
  ["./raw-query-decoder", new Set(["decodeRawQuery", "denseArraySnapshot", "validateRuntimeQuery"])],
  ["./raw-query-decoder.ts", new Set(["decodeRawQuery", "denseArraySnapshot", "validateRuntimeQuery"])],
  ["./row-values", new Set(["isPlainRecord"])],
  [
    "./source-health-wire",
    new Set(["validateExactLeasedHealthRoutes", "validateExactRoute", "validateExactSourceHealth"]),
  ],
  ["./strict-json-materialization", new Set(["materializeStrictJson"])],
  [
    "./structural-data",
    new Set([
      "hasPlainRecordPrototype",
      "inspectArrayData",
      "inspectDenseArrayData",
      "inspectPlainRecordData",
      "inspectPlainRecordShape",
    ]),
  ],
  [
    "./wire-safe-big-decimal",
    new Set(["inspectWireSafeBigDecimal", "isTrustedWireSafeBigDecimal", "isWireSafeBigDecimal"]),
  ],
]);

export const trustedValidationNamespaceImports = new Map<string, ReadonlySet<string>>([
  [
    "effect",
    new Set(["BigDecimal", "Cause", "Effect", "Exit", "Layer", "Option", "Result", "Schedule", "Schema"]),
  ],
  ["effect/BigDecimal", new Set(["BigDecimal"])],
]);

export const trustedValidationNamespaceRoots = new Set([
  "BigDecimal",
  "Cause",
  "Effect",
  "Exit",
  "Layer",
  "Option",
  "Result",
  "Schedule",
  "Schema",
]);
