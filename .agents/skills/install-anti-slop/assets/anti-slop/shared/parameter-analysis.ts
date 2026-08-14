import type { ESTree, Scope, SourceCode, Variable } from "@oxlint/plugins";
import {
	typeResolvesToRuntimeTypeofTags,
	type TypeEnvironment,
} from "./dictionary-types.ts";

export type Parameter = ESTree.ParamPattern;
type IdentifierNode = Extract<ESTree.Node, { type: "Identifier"; name: string }>;

export type ParameterOwner =
	| ESTree.ArrowFunctionExpression
	| ESTree.Function
	| ESTree.TSCallSignatureDeclaration
	| ESTree.TSConstructSignatureDeclaration
	| ESTree.TSConstructorType
	| ESTree.TSFunctionType
	| ESTree.TSMethodSignature;

export function isRuntimeParameterOwner(
	owner: ParameterOwner,
): owner is ESTree.ArrowFunctionExpression | ESTree.Function {
	return (
		owner.type === "ArrowFunctionExpression" ||
		owner.type === "FunctionDeclaration" ||
		owner.type === "FunctionExpression"
	);
}

const knownValidationCalls = new Set([
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

const validationCallbackWrappers = new Set(["Effect.try", "Effect.tryPromise", "Result.try", "inspect"]);
const validationFactoryCalls = new Set([
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

// Imported names are trusted only when both the module specifier and export have
// been reviewed as validation infrastructure. A local alias with a familiar
// name must not make an unrelated function count as validation evidence.
const trustedValidationImportExports = new Map<string, ReadonlySet<string>>([
	["effect", new Set(["BigDecimal", "Cause", "Effect", "Exit", "Layer", "Option", "Result", "Schema"])],
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

const trustedValidationNamespaceImports = new Map<string, ReadonlySet<string>>([
	["effect", new Set(["BigDecimal", "Cause", "Effect", "Exit", "Layer", "Option", "Result", "Schema"])],
	["effect/BigDecimal", new Set(["BigDecimal"])],
]);

const trustedValidationNamespaceRoots = new Set([
	"BigDecimal",
	"Cause",
	"Effect",
	"Exit",
	"Layer",
	"Option",
	"Result",
	"Schema",
]);

const typeofTags = new Set([
	"bigint",
	"boolean",
	"function",
	"number",
	"object",
	"string",
	"symbol",
	"undefined",
]);

function isNode(value: unknown): value is ESTree.Node {
	return typeof value === "object" && value !== null && "type" in value && typeof value.type === "string";
}

function validationCallName(node: ESTree.CallExpression): string | null {
	if (node.callee.type === "Identifier") return node.callee.name;
	if (
		node.callee.type !== "MemberExpression" ||
		node.callee.computed ||
		node.callee.property.type !== "Identifier"
	)
		return null;
	const objectName =
		node.callee.object.type === "Identifier" ? `${node.callee.object.name}.` : null;
	if (objectName === null) return null;
	return `${objectName}${node.callee.property.name}`;
}

type TypeofUnaryExpression = ESTree.UnaryExpression & {
	readonly parent: ESTree.BinaryExpression;
};

type TypeofComparison = {
	readonly node: ESTree.Node;
	readonly operator: string;
	readonly tag: string;
};

function isTypeofComparison(node: ESTree.UnaryExpression): node is TypeofUnaryExpression {
	const parent = node.parent;
	if (parent.type !== "BinaryExpression" || !["==", "===", "!=", "!=="].includes(parent.operator))
		return false;
	const other = parent.left === node ? parent.right : parent.left;
	return other.type === "Literal" && typeof other.value === "string" && typeofTags.has(other.value);
}

function typeofComparisonForNode(node: ESTree.Node): TypeofComparison | undefined {
	if (node.type === "UnaryExpression") {
		if (node.operator !== "typeof" || !isTypeofComparison(node)) return undefined;
		const other = node.parent.left === node ? node.parent.right : node.parent.left;
		return other.type === "Literal" && typeof other.value === "string"
			? { node: node.parent, operator: node.parent.operator, tag: other.value }
			: undefined;
	}
	if (node.type !== "BinaryExpression") return undefined;
	const typeofNode =
		node.left.type === "UnaryExpression" && isTypeofComparison(node.left)
			? node.left
			: node.right.type === "UnaryExpression" && isTypeofComparison(node.right)
				? node.right
				: undefined;
	if (typeofNode === undefined) return undefined;
	const other = node.left === typeofNode ? node.right : node.left;
	return other.type === "Literal" && typeof other.value === "string"
		? { node, operator: node.operator, tag: other.value }
		: undefined;
}

type ValidationResolution = ReadonlySet<Variable> | undefined;

const builtinValidationCalls = new Set([
	"Array.isArray",
	"BigDecimal.isBigDecimal",
	"Cause.hasDies",
	"Cause.hasFails",
	"Cause.hasInterrupts",
	"Cause.hasInterruptsOnly",
	"Cause.isCause",
	"Cause.isDieReason",
	"Cause.isFailReason",
	"Cause.isInterruptReason",
	"Cause.isReason",
	"Effect.isEffect",
	"Exit.hasDies",
	"Exit.hasFails",
	"Exit.hasInterrupts",
	"Exit.isExit",
	"Exit.isFailure",
	"Exit.isSuccess",
	"Layer.isLayer",
	"Option.isNone",
	"Option.isOption",
	"Option.isSome",
	"Result.isFailure",
	"Result.isResult",
	"Result.isSuccess",
	"Schema.decodeEffect",
	"Schema.decodeExit",
	"Schema.decodeOption",
	"Schema.decodePromise",
	"Schema.decodeResult",
	"Schema.decodeSync",
	"Schema.decodeUnknownEffect",
	"Schema.decodeUnknownExit",
	"Schema.decodeUnknownOption",
	"Schema.decodeUnknownPromise",
	"Schema.decodeUnknownResult",
	"Schema.decodeUnknownSync",
	"Schema.encodeEffect",
	"Schema.encodeExit",
	"Schema.encodeOption",
	"Schema.encodePromise",
	"Schema.encodeResult",
	"Schema.encodeSync",
	"Schema.encodeUnknownEffect",
	"Schema.encodeUnknownExit",
	"Schema.encodeUnknownOption",
	"Schema.encodeUnknownPromise",
	"Schema.encodeUnknownResult",
	"Schema.encodeUnknownSync",
	"Schema.is",
	"Schema.isSchema",
	"Schema.isSchemaError",
	"Effect.try",
	"Effect.tryPromise",
	"Result.try",
]);

function isBuiltinValidationName(name: string): boolean {
	return builtinValidationCalls.has(name);
}

function isValidationOperation(
	node: ESTree.Node,
	sourceCode?: SourceCode,
	resolving?: ValidationResolution,
	environment?: TypeEnvironment,
): boolean {
	if (node.type === "UnaryExpression") return node.operator === "typeof" && isTypeofComparison(node);
	if (node.type === "BinaryExpression") {
		if (!["===", "!==", "==", "!="].includes(node.operator)) return false;
		const left = node.left.type === "UnaryExpression" && isTypeofComparison(node.left);
		const right = node.right.type === "UnaryExpression" && isTypeofComparison(node.right);
		return left || right;
	}
	if (node.type !== "CallExpression") return false;
	const name = validationCallName(node);
	if (name === null) {
		if (node.callee.type !== "CallExpression") return false;
		const factoryName = validationCallName(node.callee);
		return (
			factoryName !== null &&
			validationFactoryCalls.has(factoryName) &&
			!isShadowedValidationCall(node.callee, sourceCode, resolving, environment)
		);
	}
	if (isBuiltinValidationName(name)) {
		return !isShadowedValidationCall(node, sourceCode, resolving, environment);
	}
	const localValidation = isLocallyTrustedValidationCall(node, sourceCode, resolving, environment);
	const trustedImportedValidation = isTrustedValidationCall(node, sourceCode, resolving, environment);
	if (!knownValidationCalls.has(name) && !localValidation && !trustedImportedValidation) return false;
	if (isShadowedValidationCall(node, sourceCode, resolving, environment)) return false;
	return knownValidationCalls.has(name) || localValidation || trustedImportedValidation;
}

function parameterBindingIdentifiers(parameter: Parameter): readonly IdentifierNode[] {
	const identifiers: IdentifierNode[] = [];
	const collect = (pattern: Parameter): void => {
		switch (pattern.type) {
			case "Identifier":
				identifiers.push(pattern);
				return;
			case "AssignmentPattern":
				collect(pattern.left);
				return;
			case "RestElement":
				collect(pattern.argument);
				return;
			case "TSParameterProperty":
				collect(pattern.parameter);
				return;
			case "ArrayPattern":
				for (const element of pattern.elements) {
					if (element !== null) collect(element);
				}
				return;
			case "ObjectPattern":
				for (const property of pattern.properties) {
					if (property.type === "Property") collect(property.value);
					else collect(property.argument);
				}
				return;
		}
	};
	collect(parameter);
	return identifiers;
}

export function parameterBindingNames(parameter: Parameter): ReadonlySet<string> {
	const names = new Set<string>();
	for (const identifier of parameterBindingIdentifiers(parameter)) names.add(identifier.name);
	return names;
}

function isFunctionNode(
	node: ESTree.Node,
): node is ESTree.ArrowFunctionExpression | ESTree.Function {
	return (
		node.type === "ArrowFunctionExpression" ||
		node.type === "FunctionDeclaration" ||
		node.type === "FunctionExpression"
	);
}

function functionShadowsParameterNames(
	node: ESTree.ArrowFunctionExpression | ESTree.Function,
	names: ReadonlySet<string>,
): boolean {
	return node.params.some((parameter) =>
		Array.from(parameterBindingNames(parameter)).some((name) => names.has(name)),
	);
}

function isValidationCallback(
	node: ESTree.Node,
	sourceCode?: SourceCode,
	environment?: TypeEnvironment,
): boolean {
	let current = node.parent;
	while (current !== null && !isFunctionNode(current)) {
		if (current.type === "CallExpression") {
			const name = validationCallName(current);
			return (
				name !== null &&
				validationCallbackWrappers.has(name) &&
				!isShadowedValidationCall(current, sourceCode, undefined, environment)
			);
		}
		current = current.parent;
	}
	return false;
}

type ParameterBindings = {
	readonly names: ReadonlySet<string>;
	readonly variables: ReadonlySet<Variable> | undefined;
};

function isParameterReference(
	node: ESTree.Node,
	bindings: ParameterBindings,
	sourceCode?: SourceCode,
): boolean {
	if (node.type !== "Identifier" || !bindings.names.has(node.name)) return false;
	if (bindings.variables === undefined || sourceCode === undefined) return true;
	const variable = resolvedVariable(sourceCode, node);
	return variable !== undefined && bindings.variables.has(variable);
}

function isReferenceIdentifier(node: IdentifierNode): boolean {
	const parent = node.parent;
	if (parent.type === "MemberExpression") {
		return parent.object === node || parent.computed;
	}
	if (parent.type === "Property") {
		return parent.key !== node || parent.computed || parent.value === node;
	}
	if (parent.type === "MethodDefinition" || parent.type === "PropertyDefinition") {
		return parent.key !== node || parent.computed;
	}
	if (parent.type === "LabeledStatement" || parent.type === "BreakStatement" || parent.type === "ContinueStatement") {
		return false;
	}
	return true;
}

function containsParameterReference(
	node: ESTree.Node,
	bindings: ParameterBindings,
	sourceCode?: SourceCode,
): boolean {
	if (isFunctionNode(node)) return false;
	if (node.type === "Identifier" && isReferenceIdentifier(node) && isParameterReference(node, bindings, sourceCode))
		return true;
	for (const [key, value] of Object.entries(node)) {
		if (key === "parent") continue;
		if (Array.isArray(value)) {
			for (const entry of value) {
				if (isNode(entry) && containsParameterReference(entry, bindings, sourceCode)) return true;
			}
		} else if (isNode(value) && containsParameterReference(value, bindings, sourceCode)) {
			return true;
		}
	}
	return false;
}

function containsParameterReferenceInCallback(
	node: ESTree.Node,
	bindings: ParameterBindings,
	sourceCode?: SourceCode,
): boolean {
	if (isFunctionNode(node)) {
		return isNode(node.body) && containsParameterReference(node.body, bindings, sourceCode);
	}
	if (node.type === "Identifier" && isReferenceIdentifier(node) && isParameterReference(node, bindings, sourceCode))
		return true;
	for (const [key, value] of Object.entries(node)) {
		if (key === "parent") continue;
		if (Array.isArray(value)) {
			for (const entry of value) {
				if (isNode(entry) && containsParameterReferenceInCallback(entry, bindings, sourceCode))
					return true;
			}
		} else if (isNode(value) && containsParameterReferenceInCallback(value, bindings, sourceCode)) {
			return true;
		}
	}
	return false;
}

export function resolvedVariable(
	sourceCode: SourceCode,
	node: IdentifierNode,
): Variable | undefined {
	let scope: Scope | null = sourceCode.getScope(node);
	while (scope !== null) {
		const variable = scope.set.get(node.name);
		if (variable !== undefined) return variable;
		scope = scope.upper;
	}
	return undefined;
}

type ImportBindingDetails = Readonly<{
	source: string;
	imported: string | "*";
}>;

function importBindingDetails(
	definition: Variable["defs"][number],
): ImportBindingDetails | undefined {
	if (definition.type !== "ImportBinding" || definition.parent?.type !== "ImportDeclaration") return undefined;
	const source = definition.parent.source.value;
	if (typeof source !== "string") return undefined;
	if (definition.node.type === "ImportSpecifier") {
		const imported = definition.node.imported;
		if (imported.type === "Identifier") return { source, imported: imported.name };
		if (imported.type === "Literal" && typeof imported.value === "string") {
			return { source, imported: imported.value };
		}
		return undefined;
	}
	if (definition.node.type === "ImportNamespaceSpecifier") return { source, imported: "*" };
	if (definition.node.type === "ImportDefaultSpecifier") return { source, imported: "default" };
	return undefined;
}

function isTrustedImportedValidation(variable: Variable, name: string): boolean {
	const rootName = name.split(".")[0] ?? name;
	return variable.defs.every((definition) => {
		const details = importBindingDetails(definition);
		if (details === undefined) return false;
		if (details.imported === "*") {
			return (
				trustedValidationNamespaceImports.get(details.source)?.has(rootName) === true &&
				trustedValidationNamespaceRoots.has(rootName) &&
				isBuiltinValidationName(name)
			);
		}
		const trusted = trustedValidationImportExports.get(details.source)?.has(details.imported) ?? false;
		return (
			trusted &&
			(!trustedValidationNamespaceRoots.has(details.imported) || isBuiltinValidationName(name))
		);
	});
}

function isTrustedValidationCall(
	node: ESTree.CallExpression,
	sourceCode?: SourceCode,
	resolving?: ValidationResolution,
	environment?: TypeEnvironment,
): boolean {
	const name = validationCallName(node);
	if (name === null) return false;
	const recognizedValidationName =
		isBuiltinValidationName(name) ||
		knownValidationCalls.has(name) ||
		validationCallbackWrappers.has(name);
	if (sourceCode === undefined) return true;
	const identifier =
		node.callee.type === "Identifier"
			? node.callee
			: node.callee.type === "MemberExpression" &&
				  !node.callee.computed &&
				  node.callee.object.type === "Identifier"
				? node.callee.object
				: undefined;
	if (identifier === undefined) return false;
	const variable = resolvedVariable(sourceCode, identifier);
	if (variable === undefined) return isBuiltinValidationName(name);
	if (variable.defs.some((definition) => definition.type === "ImportBinding")) {
		return (
			variable.defs.every((definition) => definition.type === "ImportBinding") &&
			isTrustedImportedValidation(variable, name)
		);
	}
	if (!recognizedValidationName) return false;
	if (isBuiltinValidationName(name)) {
		return variable.defs.every((definition) => definition.type === "ImplicitGlobalVariable");
	}
	return isTrustedLocalValidation(variable, sourceCode, resolving, environment);
}

function isShadowedValidationCall(
	node: ESTree.CallExpression,
	sourceCode: SourceCode | undefined,
	resolving?: ValidationResolution,
	environment?: TypeEnvironment,
): boolean {
	return (
		!isTrustedValidationCall(node, sourceCode, resolving, environment) &&
		!isLocallyTrustedValidationCall(node, sourceCode, resolving, environment)
	);
}

function functionValueFromDefinition(
	definition: Variable["defs"][number],
): ESTree.ArrowFunctionExpression | ESTree.Function | undefined {
	if (definition.type === "FunctionName" && isFunctionNode(definition.node)) return definition.node;
	if (definition.type !== "Variable" || definition.node.type !== "VariableDeclarator") return undefined;
	if (definition.node.init === null) return undefined;
	if (isFunctionNode(definition.node.init)) return definition.node.init;
	if (definition.node.init.type !== "CallExpression") return undefined;
	return definition.node.init.arguments.find(isFunctionNode);
}

function isTrustedLocalValidation(
	variable: Variable,
	sourceCode: SourceCode,
	resolving?: ValidationResolution,
	environment?: TypeEnvironment,
): boolean {
	const current = resolving ?? new Set<Variable>();
	if (current.has(variable)) return false;
	const next = new Set(current);
	next.add(variable);
	for (const definition of variable.defs) {
		const functionValue = functionValueFromDefinition(definition);
		if (
			functionValue !== undefined &&
			functionValue.params.some((parameter) =>
				hasValidationEvidence(
					functionValue,
					parameterBindings(parameter, sourceCode),
					sourceCode,
					next,
					true,
					environment,
				),
			)
		)
			return true;
	}
	return false;
}

function isLocallyTrustedValidationCall(
	node: ESTree.CallExpression,
	sourceCode: SourceCode | undefined,
	resolving?: ValidationResolution,
	environment?: TypeEnvironment,
): boolean {
	if (sourceCode === undefined || node.callee.type !== "Identifier") return false;
	const variable = resolvedVariable(sourceCode, node.callee);
	if (variable === undefined || variable.defs.some((definition) => definition.type === "ImportBinding"))
		return false;
	return isTrustedLocalValidation(variable, sourceCode, resolving, environment);
}

function validationReferencesParameter(
	node: ESTree.Node,
	bindings: ParameterBindings,
	sourceCode?: SourceCode,
	resolving?: ValidationResolution,
	environment?: TypeEnvironment,
): boolean {
	if (
		(node.type === "UnaryExpression" || node.type === "BinaryExpression" || node.type === "CallExpression") &&
		!validationResultIsConsumed(node, sourceCode)
	)
		return false;
	if (node.type === "UnaryExpression")
		return containsParameterReference(node.argument, bindings, sourceCode);
	if (node.type === "BinaryExpression") {
		return (
			containsParameterReference(node.left, bindings, sourceCode) ||
			containsParameterReference(node.right, bindings, sourceCode)
		);
	}
	if (node.type === "CallExpression") {
		const references = node.arguments.filter((argument) =>
			containsParameterReference(argument, bindings, sourceCode),
		);
		if (references.length > 0) {
			if (isValidationOperation(node, sourceCode, resolving, environment)) return true;
			return false;
		}
		if (!isValidationOperation(node, sourceCode, resolving, environment)) return false;
		return node.arguments.some((argument) =>
			containsParameterReferenceInCallback(argument, bindings, sourceCode),
		);
	}
	return false;
}

function validationResultIsConsumed(
	node: ESTree.Node,
	sourceCode?: SourceCode,
	seenVariables = new Set<Variable>(),
): boolean {
	let current = node;
	while (current.parent !== null) {
		const parent = current.parent;
		if (
			parent.type === "ParenthesizedExpression" ||
			parent.type === "TSAsExpression" ||
			parent.type === "TSSatisfiesExpression" ||
			parent.type === "TSTypeAssertion" ||
			parent.type === "TSNonNullExpression" ||
			parent.type === "MemberExpression" ||
			parent.type === "ChainExpression" ||
			parent.type === "UnaryExpression" ||
			parent.type === "BinaryExpression" ||
			parent.type === "LogicalExpression" ||
			parent.type === "ConditionalExpression" ||
			parent.type === "CallExpression" ||
			parent.type === "NewExpression" ||
			parent.type === "AwaitExpression"
		) {
			current = parent;
			continue;
		}
		if (parent.type === "ReturnStatement") return parent.argument === current;
		if (parent.type === "ArrowFunctionExpression" && parent.body === current) return true;
		if (
			(parent.type === "IfStatement" && parent.test === current) ||
			(parent.type === "WhileStatement" && parent.test === current) ||
			(parent.type === "DoWhileStatement" && parent.test === current) ||
			(parent.type === "ForStatement" && parent.test === current)
		)
			return true;
		if (parent.type === "VariableDeclarator" && parent.init === current) {
			if (sourceCode === undefined || parent.id.type !== "Identifier") return false;
			const variable = resolvedVariable(sourceCode, parent.id);
			if (variable === undefined || seenVariables.has(variable)) return false;
			seenVariables.add(variable);
			return variable.references.some(
				(reference) =>
					reference.isRead() &&
					validationResultIsConsumed(reference.identifier, sourceCode, seenVariables),
			);
		}
		if (
			parent.type === "AssignmentExpression" &&
			parent.right === current &&
			parent.left.type === "Identifier"
		) {
			if (sourceCode === undefined) return false;
			const variable = resolvedVariable(sourceCode, parent.left);
			if (variable === undefined || seenVariables.has(variable)) return false;
			seenVariables.add(variable);
			return variable.references.some(
				(reference) =>
					reference.isRead() &&
					validationResultIsConsumed(reference.identifier, sourceCode, seenVariables),
			);
		}
		return false;
	}
	return false;
}

function booleanExpressionValue(node: ESTree.Node): boolean | undefined {
	let current = node;
	while (
		current.type === "ParenthesizedExpression" ||
		current.type === "TSAsExpression" ||
		current.type === "TSSatisfiesExpression" ||
		current.type === "TSTypeAssertion" ||
		current.type === "TSNonNullExpression"
	) {
		current = current.expression;
	}
	return current.type === "Literal" && typeof current.value === "boolean"
		? current.value
		: undefined;
}

function booleanReturnValue(node: ESTree.Node): boolean | undefined {
	if (node.type === "ReturnStatement" && node.argument !== null) {
		return booleanExpressionValue(node.argument);
	}
	if (node.type === "BlockStatement" && node.body.length === 1) {
		return booleanReturnValue(node.body[0]);
	}
	return undefined;
}

function nextStatement(node: ESTree.IfStatement): ESTree.Statement | undefined {
	if (node.parent.type !== "BlockStatement") return undefined;
	const index = node.parent.body.indexOf(node);
	return index === -1 ? undefined : node.parent.body[index + 1];
}

function isParameterGuardRejection(
	node: ESTree.Node,
	bindings: ParameterBindings,
	sourceCode?: SourceCode,
): boolean {
	if (node.type === "UnaryExpression" && node.operator === "!")
		return containsParameterReference(node.argument, bindings, sourceCode);
	if (node.type !== "BinaryExpression" || !["==", "==="].includes(node.operator)) return false;
	const other = node.left.type === "Literal" ? node.left : node.right;
	return (
		(other.type === "Literal" && other.value === null) ||
		(other.type === "Identifier" && other.name === "undefined")
	) && containsParameterReference(node, bindings, sourceCode);
}

/**
 * Returns whether a validation operation is known to constrain the value
 * returned by a type predicate. `typeof value !== ...` is validation, but it
 * proves the opposite of a predicate such as `value is string` and must not
 * create an exemption by itself.
 */
function typePredicateEvidencePolarity(
	node: ESTree.Node,
	expectedTypeofTags: ReadonlySet<string> | undefined,
	bindings: ParameterBindings,
	sourceCode?: SourceCode,
): boolean | undefined {
	const comparison = typeofComparisonForNode(node);
	const typeofTagMatches = (tag: string): boolean =>
		expectedTypeofTags !== undefined && expectedTypeofTags.has(tag);
	if (comparison !== undefined && !typeofTagMatches(comparison.tag))
		return undefined;
	let current: ESTree.Node = comparison?.node ?? node;
	let polarity = comparison === undefined || comparison.operator === "==" || comparison.operator === "===";
	if (comparison === undefined && node.type !== "CallExpression") return undefined;

	while (current.parent !== null) {
		const parent: ESTree.Node = current.parent;
		if (
			parent.type === "ParenthesizedExpression" ||
			parent.type === "TSAsExpression" ||
			parent.type === "TSSatisfiesExpression" ||
			parent.type === "TSTypeAssertion" ||
			parent.type === "TSNonNullExpression"
		) {
			current = parent;
			continue;
		}
		if (parent.type === "UnaryExpression" && parent.operator === "!") {
			polarity = !polarity;
			current = parent;
			continue;
		}
		if (parent.type === "LogicalExpression") {
			if (parent.operator === "||") {
				const other = parent.left === current ? parent.right : parent.left;
				const otherComparison = typeofComparisonForNode(other);
				if (
					otherComparison !== undefined &&
					otherComparison.operator !== "!=" &&
					otherComparison.operator !== "!==" &&
					typeofTagMatches(otherComparison.tag)
				) {
					current = parent;
					continue;
				}
				if (
					comparison !== undefined &&
					comparison.operator !== "==" &&
					comparison.operator !== "===" &&
					isParameterGuardRejection(other, bindings, sourceCode)
				) {
					current = parent;
					continue;
				}
				return undefined;
			}
			if (parent.operator !== "&&") return undefined;
			current = parent;
			continue;
		}
		if (parent.type === "ConditionalExpression" && parent.test === current) {
			const consequent = booleanExpressionValue(parent.consequent);
			const alternate = booleanExpressionValue(parent.alternate);
			if (consequent === true && alternate === false) {
				current = parent;
				continue;
			}
			if (consequent === false && alternate === true) {
				polarity = !polarity;
				current = parent;
				continue;
			}
			return undefined;
		}
		if (parent.type === "IfStatement" && parent.test === current) {
			const following = parent.alternate === null ? nextStatement(parent) : undefined;
			const consequent = booleanReturnValue(parent.consequent);
			const alternate =
				parent.alternate === null
					? following === undefined
						? undefined
						: booleanReturnValue(following)
					: booleanReturnValue(parent.alternate);
			if (consequent === true && alternate === false) return polarity;
			if (consequent === false && alternate === true) return !polarity;
			if (parent.alternate === null && consequent === false && nextStatement(parent) !== undefined)
				return !polarity;
			return undefined;
		}
		if (parent.type === "ReturnStatement" && parent.argument === current) return polarity;
		if (parent.type === "ArrowFunctionExpression" && parent.body === current) return polarity;
		return undefined;
	}
	return undefined;
}

function hasValidationEvidence(
	owner: ParameterOwner,
	bindings: ParameterBindings,
	sourceCode?: SourceCode,
	resolving?: ValidationResolution,
	allowTypeof = true,
	environment?: TypeEnvironment,
): boolean {
	if (!("body" in owner) || !isNode(owner.body)) return false;
	const isTypePredicateOwner =
		"returnType" in owner &&
		owner.returnType !== null &&
		owner.returnType !== undefined &&
		owner.returnType.typeAnnotation.type === "TSTypePredicate";
	const predicateType = isTypePredicateOwner
		? owner.returnType.typeAnnotation.typeAnnotation?.typeAnnotation
		: undefined;
	const expectedTypeofTags =
		predicateType === undefined || environment === undefined
			? undefined
			: typeResolvesToRuntimeTypeofTags(predicateType, environment);
	if (
		isTypePredicateOwner
	) {
		const pendingPredicateNodes: ESTree.Node[] = [owner.body];
		let hasOppositeTypeofEvidence = false;
		while (pendingPredicateNodes.length > 0) {
			const current = pendingPredicateNodes.pop();
			if (current === undefined) continue;
			if (
				isValidationOperation(current, sourceCode, resolving, environment) &&
				validationReferencesParameter(current, bindings, sourceCode, resolving, environment)
			) {
				const polarity = typePredicateEvidencePolarity(
					current,
					expectedTypeofTags,
					bindings,
					sourceCode,
				);
				if (polarity === true) return true;
				if (polarity === false) hasOppositeTypeofEvidence = true;
			}
			for (const [key, value] of Object.entries(current)) {
				if (key === "parent") continue;
				if (Array.isArray(value)) {
					for (const entry of value) if (isNode(entry)) pendingPredicateNodes.push(entry);
				} else if (isNode(value)) {
					pendingPredicateNodes.push(value);
				}
			}
		}
		if (hasOppositeTypeofEvidence) return false;
	}
	const pending: ESTree.Node[] = [owner.body];
	while (pending.length > 0) {
		const current = pending.pop();
		if (current === undefined) continue;
		const isTypeofOperation = current.type === "UnaryExpression" || current.type === "BinaryExpression";
		const isStrictTypeofPredicate = isTypePredicateOwner;
		if (
			(!isTypeofOperation || (allowTypeof && !isStrictTypeofPredicate)) &&
			isValidationOperation(current, sourceCode, resolving, environment) &&
			validationReferencesParameter(current, bindings, sourceCode, resolving, environment)
		)
			return true;
		for (const [key, value] of Object.entries(current)) {
			if (key === "parent") continue;
			if (Array.isArray(value)) {
				for (const entry of value) {
					if (!isNode(entry)) continue;
					if (isFunctionNode(entry)) {
						if (
								!functionShadowsParameterNames(entry, bindings.names) &&
								isValidationCallback(entry, sourceCode, environment) &&
							isNode(entry.body) &&
								hasValidationEvidence(
									entry,
									bindings,
									sourceCode,
									resolving,
									allowTypeof,
									environment,
								)
						)
							return true;
						continue;
					}
					pending.push(entry);
				}
			} else if (isNode(value)) {
				if (isFunctionNode(value)) {
					if (
						!functionShadowsParameterNames(value, bindings.names) &&
						isValidationCallback(value, sourceCode, environment) &&
						isNode(value.body) &&
						hasValidationEvidence(
							value,
							bindings,
							sourceCode,
							resolving,
							allowTypeof,
							environment,
						)
					)
						return true;
					continue;
				}
				pending.push(value);
			}
		}
	}
	return false;
}

function parameterBindings(parameter: Parameter, sourceCode?: SourceCode): ParameterBindings {
	const names = parameterBindingNames(parameter);
	if (sourceCode === undefined)
		return { names, variables: undefined };
	const variables = new Set<Variable>();
	for (const identifier of parameterBindingIdentifiers(parameter)) {
		const variable = resolvedVariable(sourceCode, identifier);
		if (variable !== undefined) variables.add(variable);
	}
	return { names, variables };
}

export function isValidationOwner(
	owner: ParameterOwner,
	sourceCode?: SourceCode,
	environment?: TypeEnvironment,
): boolean {
	return owner.params.some((parameter) =>
		isValidationParameter(owner, parameter, sourceCode, environment),
	);
}

export function isValidationParameter(
	owner: ParameterOwner,
	parameter: Parameter,
	sourceCode?: SourceCode,
	environment?: TypeEnvironment,
): boolean {
	const bindings = parameterBindings(parameter, sourceCode);
	if (bindings.names.size === 0) return false;
	if (
		"returnType" in owner &&
		owner.returnType !== null &&
		owner.returnType !== undefined &&
		owner.returnType.typeAnnotation.type === "TSTypePredicate" &&
		owner.returnType.typeAnnotation.parameterName.type === "Identifier" &&
		!bindings.names.has(owner.returnType.typeAnnotation.parameterName.name)
	)
		return false;
	if (hasValidationEvidence(owner, bindings, sourceCode, undefined, true, environment)) return true;
	return false;
}

export function isValidationCallParameter(
	owner: ParameterOwner,
	parameter: Parameter,
	sourceCode?: SourceCode,
	environment?: TypeEnvironment,
): boolean {
	const bindings = parameterBindings(parameter, sourceCode);
	return (
		bindings.names.size > 0 &&
		hasValidationEvidence(owner, bindings, sourceCode, undefined, false, environment)
	);
}

export function parameterAnnotation(
	parameter: Parameter,
): ESTree.TSTypeAnnotation | null | undefined {
	if (parameter.type === "TSParameterProperty") return parameterAnnotation(parameter.parameter);
	if (parameter.type === "RestElement") {
		return parameter.typeAnnotation ?? parameterAnnotation(parameter.argument);
	}
	if (parameter.type === "AssignmentPattern") {
		return parameter.typeAnnotation ?? parameter.left.typeAnnotation;
	}
	return parameter.typeAnnotation;
}

export function parameterName(parameter: Parameter, sourceCode: SourceCode): string {
	if (parameter.type === "Identifier") return parameter.name;
	if (parameter.type === "TSParameterProperty") {
		return parameterName(parameter.parameter, sourceCode);
	}
	if (parameter.type === "AssignmentPattern") return parameterName(parameter.left, sourceCode);
	if (parameter.type === "RestElement") return parameterName(parameter.argument, sourceCode);
	const typeAnnotation = parameter.typeAnnotation;
	if (typeAnnotation === null || typeAnnotation === undefined) return sourceCode.getText(parameter);
	return sourceCode
		.getText(parameter)
		.slice(0, typeAnnotation.start - parameter.start)
		.trimEnd();
}
