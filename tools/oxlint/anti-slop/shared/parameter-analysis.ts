import type { ESTree, Scope, SourceCode, Variable } from "@oxlint/plugins";
import {
	typeRequiresStructuralRuntimeEvidence,
	typeResolvesToRuntimeTypeofTags,
	typeResolvesToRuntimeTypeofTagsForPropertyPath,
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
	readonly operand: ESTree.Node;
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
			? {
					 node: node.parent,
					 operator: node.parent.operator,
					 tag: other.value,
					 operand: node.argument,
				  }
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
		? { node, operator: node.operator, tag: other.value, operand: typeofNode.argument }
		: undefined;
}

type ValidationResolution = ReadonlySet<Variable> | undefined;

const builtinValidationCalls = new Set([
	"Array.isArray",
	"Object.hasOwn",
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
		if (isPropertyPresenceOperation(node)) return true;
		if (!["===", "!==", "==", "!="].includes(node.operator)) return false;
		if (
			isIdentityLookupShape(node) ||
			isReflectiveBrandShape(node) ||
			reflectedPropertyComparisonForNode(node) !== undefined
		)
			return true;
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

function unwrapRuntimeTypeofOperand(node: ESTree.Node): ESTree.Node {
	let current = node;
	while (
		current.type === "ChainExpression" ||
		current.type === "ParenthesizedExpression" ||
		current.type === "TSAsExpression" ||
		current.type === "TSSatisfiesExpression" ||
		current.type === "TSTypeAssertion" ||
		current.type === "TSNonNullExpression"
	) {
		current = current.expression;
	}
	return current;
}

function parameterPropertyPath(
	node: ESTree.Node,
	bindings: ParameterBindings,
	sourceCode?: SourceCode,
): readonly string[] | undefined {
	const current = unwrapRuntimeTypeofOperand(node);
	if (
		current.type === "Identifier" &&
		isReferenceIdentifier(current) &&
		isParameterReference(current, bindings, sourceCode)
	)
		return [];
	if (current.type !== "MemberExpression" || current.object.type === "Super") return undefined;
	const propertyName = current.computed
		? current.property.type === "Literal" && typeof current.property.value === "string"
			? current.property.value
			: undefined
		: current.property.type === "Identifier"
			? current.property.name
			: undefined;
	if (propertyName === undefined) return undefined;
	const basePath = parameterPropertyPath(current.object, bindings, sourceCode);
	return basePath === undefined ? undefined : [...basePath, propertyName];
}

function reflectedPropertyPath(
	node: ESTree.Node,
	bindings: ParameterBindings,
	sourceCode?: SourceCode,
): readonly string[] | undefined {
	const current = unwrapRuntimeTypeofOperand(node);
	if (current.type !== "CallExpression" || validationCallName(current) !== "Reflect.get") return undefined;
	const object = current.arguments[0];
	const property = current.arguments[1];
	if (
		object === undefined ||
		property === undefined ||
		property.type === "SpreadElement" ||
		property.type !== "Literal" ||
		typeof property.value !== "string"
	)
		return undefined;
	return parameterPropertyPath(object, bindings, sourceCode) === undefined
		? undefined
		: [...(parameterPropertyPath(object, bindings, sourceCode) ?? []), property.value];
}

function observedPropertyPath(
	node: ESTree.Node,
	bindings: ParameterBindings,
	sourceCode?: SourceCode,
): readonly string[] | undefined {
	return parameterPropertyPath(node, bindings, sourceCode) ?? reflectedPropertyPath(node, bindings, sourceCode);
}

function isImportedPredicateType(
	type: ESTree.TSType,
	environment: TypeEnvironment,
): boolean {
	const current = unwrapRuntimeTypeofOperand(type);
	return (
		current.type === "TSTypeReference" &&
		current.typeName.type === "Identifier" &&
		environment.resolveImportedType(current, current.typeName.name) !== undefined
	);
}

function isSchemaType(
	type: ESTree.TSType,
	environment: TypeEnvironment,
	resolving: ReadonlySet<string> = new Set(),
): boolean {
	const current = unwrapRuntimeTypeofOperand(type);
	if (current.type !== "TSTypeReference") return false;
	if (
		current.typeName.type === "TSQualifiedName" &&
		current.typeName.left.type === "Identifier" &&
		current.typeName.left.name === "Schema" &&
		current.typeName.right.type === "Identifier" &&
		(current.typeName.right.name === "Codec" || current.typeName.right.name === "Schema")
	)
		return true;
	if (current.typeName.type !== "Identifier" || resolving.has(current.typeName.name)) return false;
	const alias = environment.resolveAlias(current, current.typeName.name);
	if (alias !== undefined) {
		return isSchemaType(alias.typeAnnotation, environment, new Set([...resolving, current.typeName.name]));
	}
	return environment.resolveInterfaces(current, current.typeName.name).some((declaration) =>
		declaration.extends.some(
			(heritage) =>
				heritage.expression.type === "MemberExpression" &&
				!heritage.expression.computed &&
				heritage.expression.object.type === "Identifier" &&
				heritage.expression.object.name === "Schema" &&
				heritage.expression.property.type === "Identifier" &&
				heritage.expression.property.name === "Codec",
		),
	);
}

function isPropertyPresenceOperation(node: ESTree.Node): node is ESTree.BinaryExpression {
	return (
		node.type === "BinaryExpression" &&
		node.operator === "in" &&
		node.left.type === "Literal" &&
		typeof node.left.value === "string"
	);
}

function reflectedPropertyComparisonForNode(
	node: ESTree.Node,
): { readonly node: ESTree.BinaryExpression; readonly operator: string; readonly operand: ESTree.Node } | undefined {
	if (node.type !== "BinaryExpression" || !["==", "===", "!=", "!=="].includes(node.operator))
		return undefined;
	const reflected = [node.left, node.right].find(
		(candidate): candidate is ESTree.CallExpression =>
			candidate.type === "CallExpression" && validationCallName(candidate) === "Reflect.get",
	);
	return reflected === undefined
		? undefined
		: { node, operator: node.operator, operand: reflected };
}

function isIdentityLookupShape(node: ESTree.Node): boolean {
	if (node.type !== "BinaryExpression" || !["==", "==="].includes(node.operator)) return false;
	return [node.left, node.right].some(
		(candidate) =>
			candidate.type === "CallExpression" &&
			candidate.callee.type === "MemberExpression" &&
			!candidate.callee.computed &&
			candidate.callee.property.type === "Identifier" &&
			candidate.callee.property.name === "get",
	);
}

function isReflectiveBrandShape(node: ESTree.Node): boolean {
	if (node.type !== "BinaryExpression" || !["==", "==="].includes(node.operator)) return false;
	const names = [node.left, node.right].map((candidate) =>
		candidate.type === "CallExpression" ? validationCallName(candidate) : null,
	);
	return names.includes("Reflect.apply") && names.includes("Reflect.get");
}

function propertyPresenceMatchesPredicate(
	node: ESTree.Node,
	predicateType: ESTree.TSType | undefined,
	bindings: ParameterBindings,
	sourceCode: SourceCode | undefined,
	environment: TypeEnvironment | undefined,
): boolean {
	let current = unwrapRuntimeTypeofOperand(node);
	if (current.type === "UnaryExpression" && current.operator === "!") {
		current = unwrapRuntimeTypeofOperand(current.argument);
	}
	if (!isPropertyPresenceOperation(current)) return false;
	const path = parameterPropertyPath(current.right, bindings, sourceCode);
	return (
		path !== undefined &&
		predicateType !== undefined &&
		environment !== undefined &&
		(typeResolvesToRuntimeTypeofTagsForPropertyPath(
			predicateType,
			[...path, current.left.value],
			environment,
		) !== undefined || isImportedPredicateType(predicateType, environment))
	);
}

function reflectedPropertyMatchesPredicate(
	node: ESTree.Node,
	predicateType: ESTree.TSType | undefined,
	bindings: ParameterBindings,
	sourceCode: SourceCode | undefined,
	environment: TypeEnvironment | undefined,
): boolean {
	const comparison = reflectedPropertyComparisonForNode(node);
	if (comparison === undefined || predicateType === undefined || environment === undefined) return false;
	const path = reflectedPropertyPath(comparison.operand, bindings, sourceCode);
	return (
		path !== undefined &&
		(typeResolvesToRuntimeTypeofTagsForPropertyPath(predicateType, path, environment) !== undefined ||
			isImportedPredicateType(predicateType, environment))
	);
}

function parameterGuardAcceptsValue(
	node: ESTree.Node,
	bindings: ParameterBindings,
	sourceCode?: SourceCode,
): boolean {
	if (node.type !== "BinaryExpression" || !["!=", "!=="].includes(node.operator)) return false;
	const other = node.left.type === "Literal" ? node.left : node.right;
	return (
		(other.type === "Literal" && other.value === null) ||
		(other.type === "Identifier" && other.name === "undefined")
	) && containsParameterReference(node, bindings, sourceCode);
}

type SchemaEvidence = {
	readonly tags: ReadonlySet<string>;
	readonly structural: boolean;
	readonly arrayLike: boolean;
	readonly broadObject?: boolean;
};

function schemaEvidence(node: ESTree.Node): SchemaEvidence | undefined {
	const current = unwrapRuntimeTypeofOperand(node);
	if (
		current.type === "MemberExpression" &&
		!current.computed &&
		current.object.type === "Identifier" &&
		current.object.name === "Schema" &&
		current.property.type === "Identifier"
	) {
		switch (current.property.name) {
			case "BigInt":
				return { tags: new Set(["bigint"]), structural: false, arrayLike: false };
			case "Boolean":
				return { tags: new Set(["boolean"]), structural: false, arrayLike: false };
			case "Number":
				return { tags: new Set(["number"]), structural: false, arrayLike: false };
			case "String":
				return { tags: new Set(["string"]), structural: false, arrayLike: false };
			case "Symbol":
				return { tags: new Set(["symbol"]), structural: false, arrayLike: false };
			case "Undefined":
				return { tags: new Set(["undefined"]), structural: false, arrayLike: false };
			case "Array":
			case "ReadonlyArray":
				return { tags: new Set(["object"]), structural: false, arrayLike: true };
			case "Object":
				return { tags: new Set(["object"]), structural: false, arrayLike: false, broadObject: true };
			default:
				return undefined;
		}
	}
	if (current.type !== "CallExpression") return undefined;
	const name = validationCallName(current);
	if (name === "Schema.Array" || name === "Schema.Tuple") {
		return { tags: new Set(["object"]), structural: false, arrayLike: true };
	}
	if (name === "Schema.Struct" || name === "Schema.Record") {
		return { tags: new Set(["object"]), structural: true, arrayLike: false };
	}
	return undefined;
}

function isArrayPredicateType(type: ESTree.TSType): boolean {
	const current = unwrapRuntimeTypeofOperand(type);
	return (
		current.type === "TSArrayType" ||
		current.type === "TSTupleType" ||
		(current.type === "TSTypeReference" &&
			current.typeName.type === "Identifier" &&
			(current.typeName.name === "Array" || current.typeName.name === "ReadonlyArray"))
	);
}

function localPredicateTypeMatches(
	node: ESTree.CallExpression,
	predicateType: ESTree.TSType,
	sourceCode: SourceCode | undefined,
): boolean {
	if (sourceCode === undefined || node.callee.type !== "Identifier") return false;
	const variable = resolvedVariable(sourceCode, node.callee);
	if (variable === undefined) return false;
	const expected = sourceCode.getText(predicateType);
	return variable.defs.some((definition) => {
		const functionValue = functionValueFromDefinition(definition);
		if (functionValue === undefined || functionValue.returnType === null || functionValue.returnType === undefined)
			return false;
		const returnType = functionValue.returnType.typeAnnotation;
		return (
			returnType.type === "TSTypePredicate" &&
			returnType.typeAnnotation?.typeAnnotation !== undefined &&
			sourceCode.getText(returnType.typeAnnotation.typeAnnotation) === expected
		);
	});
}

function typePredicateCallEvidenceMatches(
	node: ESTree.CallExpression,
	predicateType: ESTree.TSType,
	expectedTypeofTags: ReadonlySet<string> | undefined,
	sourceCode: SourceCode | undefined,
	environment: TypeEnvironment | undefined,
	resolving: ValidationResolution,
): boolean {
	const name = validationCallName(node);
	if (
		name === "Result.isSuccess" ||
		name === "Result.isFailure" ||
		name === "Result.isResult" ||
		name === "Option.isSome" ||
		name === "Option.isNone" ||
		name === "Option.isOption" ||
		name === "Cause.isCause" ||
		name === "Exit.isExit"
	)
		return false;
	if (name === "Array.isArray") return isArrayPredicateType(predicateType);
	if (node.callee.type === "CallExpression" && validationCallName(node.callee) === "Schema.is") {
		const schemaNode = node.callee.arguments[0];
		if (schemaNode === undefined || schemaNode.type === "SpreadElement") return false;
		if (
			schemaNode.type === "MemberExpression" &&
			!schemaNode.computed &&
			schemaNode.object.type === "Identifier" &&
			schemaNode.object.name === "Schema" &&
			schemaNode.property.type === "Identifier" &&
			schemaNode.property.name === "Unknown"
		)
			return false;
		const evidence = schemaEvidence(schemaNode);
		if (evidence === undefined) {
			return (
				schemaNode.type === "Identifier" &&
				(expectedTypeofTags === undefined ||
					expectedTypeofTags.has("object") ||
					expectedTypeofTags.has("function"))
			);
		}
		if (evidence.arrayLike) return isArrayPredicateType(predicateType);
		if (evidence.broadObject) {
			return (
				expectedTypeofTags?.has("object") === true &&
				environment !== undefined &&
				!typeRequiresStructuralRuntimeEvidence(predicateType, environment)
			);
		}
		if (evidence.structural) {
			return (
				expectedTypeofTags?.has("object") === true &&
				environment !== undefined &&
				typeRequiresStructuralRuntimeEvidence(predicateType, environment)
			);
		}
		return expectedTypeofTags !== undefined && Array.from(evidence.tags).every((tag) => expectedTypeofTags.has(tag));
	}
	if (name === "Schema.isSchema") {
		return environment !== undefined && isSchemaType(predicateType, environment);
	}
	if (name === "Object.hasOwn") {
		return environment !== undefined && typeRequiresStructuralRuntimeEvidence(predicateType, environment);
	}
	if (isLocallyTrustedValidationCall(node, sourceCode, resolving, environment)) {
		return localPredicateTypeMatches(node, predicateType, sourceCode);
	}
	if (expectedTypeofTags === undefined || environment === undefined) {
		return (
			(name !== null && isBuiltinValidationName(name)) ||
			(name !== null && knownValidationCalls.has(name)) ||
			isTrustedValidationCall(node, sourceCode, resolving, environment)
		);
	}
	return (
		(name !== null && isBuiltinValidationName(name)) ||
		(name !== null && knownValidationCalls.has(name)) ||
		isTrustedValidationCall(node, sourceCode, resolving, environment)
	);
}

function isIdentityLookupEvidence(
	node: ESTree.Node,
	bindings: ParameterBindings,
	sourceCode: SourceCode | undefined,
): boolean {
	const current = unwrapRuntimeTypeofOperand(node);
	if (current.type !== "BinaryExpression" || !["==", "==="].includes(current.operator)) return false;
	const isLookup = (candidate: ESTree.Node): boolean => {
		const expression = unwrapRuntimeTypeofOperand(candidate);
		return (
			expression.type === "CallExpression" &&
			expression.callee.type === "MemberExpression" &&
			!expression.callee.computed &&
			expression.callee.property.type === "Identifier" &&
			expression.callee.property.name === "get"
		);
	};
	return (
		(isLookup(current.left) &&
			(containsParameterReference(current.left, bindings, sourceCode) ||
				containsParameterReference(current.right, bindings, sourceCode))) ||
		(isLookup(current.right) &&
			(containsParameterReference(current.right, bindings, sourceCode) ||
				containsParameterReference(current.left, bindings, sourceCode)))
	);
}

function isReflectiveBrandEvidence(
	node: ESTree.Node,
	bindings: ParameterBindings,
	sourceCode: SourceCode | undefined,
): boolean {
	const current = unwrapRuntimeTypeofOperand(node);
	if (current.type !== "BinaryExpression" || !["==", "==="].includes(current.operator)) return false;
	const isReflectApply = (candidate: ESTree.Node): boolean => {
		const expression = unwrapRuntimeTypeofOperand(candidate);
		return (
			expression.type === "CallExpression" &&
			validationCallName(expression) === "Reflect.apply" &&
			expression.arguments[1] !== undefined &&
			containsParameterReference(expression.arguments[1], bindings, sourceCode)
		);
	};
	const isReflectGet = (candidate: ESTree.Node): boolean => {
		const expression = unwrapRuntimeTypeofOperand(candidate);
		return reflectedPropertyPath(expression, bindings, sourceCode) !== undefined;
	};
	return (
		(isReflectApply(current.left) && isReflectGet(current.right)) ||
		(isReflectApply(current.right) && isReflectGet(current.left))
	);
}

function hasStructuralRuntimeEvidence(
	node: ESTree.Node,
	bindings: ParameterBindings,
	sourceCode: SourceCode | undefined,
	typeofTagMatches: (comparison: TypeofComparison) => boolean,
	predicateType: ESTree.TSType | undefined,
	environment: TypeEnvironment | undefined,
	resolving: ValidationResolution | undefined,
): boolean {
	const current = unwrapRuntimeTypeofOperand(node);
	if (predicateType === undefined) return false;
	const comparison = typeofComparisonForNode(current);
	if (comparison !== undefined) {
		const path = observedPropertyPath(comparison.operand, bindings, sourceCode);
		return path !== undefined && path.length > 0 && typeofTagMatches(comparison);
	}
	if (reflectedPropertyComparisonForNode(current) !== undefined) {
		return reflectedPropertyMatchesPredicate(current, predicateType, bindings, sourceCode, environment);
	}
	if (isPropertyPresenceOperation(current)) {
		return propertyPresenceMatchesPredicate(current, predicateType, bindings, sourceCode, environment);
	}
	if (isIdentityLookupEvidence(current, bindings, sourceCode) || isReflectiveBrandEvidence(current, bindings, sourceCode)) {
		return true;
	}
	if (current.type === "CallExpression") {
		const name = validationCallName(current);
		if (
			name === "Reflect.apply" &&
			!isReflectiveBrandEvidence(current.parent, bindings, sourceCode)
		)
			return false;
		return (
			validationReferencesParameter(current, bindings, sourceCode, resolving, environment) &&
			typePredicateCallEvidenceMatches(
				current,
				predicateType,
				undefined,
				sourceCode,
				environment,
				resolving,
			)
		);
	}
	if (current.type === "LogicalExpression" && current.operator === "&&") {
		return (
			hasStructuralRuntimeEvidence(
				current.left,
				bindings,
				sourceCode,
				typeofTagMatches,
				predicateType,
				environment,
				resolving,
			) ||
			hasStructuralRuntimeEvidence(
				current.right,
				bindings,
				sourceCode,
				typeofTagMatches,
				predicateType,
				environment,
				resolving,
			)
		);
	}
	return false;
}

function isPositivePredicateCondition(
	node: ESTree.Node,
	bindings: ParameterBindings,
	sourceCode: SourceCode | undefined,
	typeofTagMatches: (comparison: TypeofComparison) => boolean,
	predicateType: ESTree.TSType | undefined,
	environment: TypeEnvironment | undefined,
): boolean {
	const current = unwrapRuntimeTypeofOperand(node);
	const comparison = typeofComparisonForNode(current);
	if (comparison !== undefined) {
		if (comparison.operator !== "==" && comparison.operator !== "===") return false;
		const propertyPath = parameterPropertyPath(comparison.operand, bindings, sourceCode);
		if (propertyPath === undefined) return true;
		if (typeofTagMatches(comparison)) return true;
		return (
			comparison.tag === "object" &&
			propertyPath?.length === 0 &&
			predicateType !== undefined &&
			environment !== undefined &&
			 typeRequiresStructuralRuntimeEvidence(predicateType, environment)
		);
	}
	const reflectedComparison = reflectedPropertyComparisonForNode(current);
	if (reflectedComparison !== undefined) {
		return (
			(reflectedComparison.operator === "==" || reflectedComparison.operator === "===") &&
			reflectedPropertyMatchesPredicate(current, predicateType, bindings, sourceCode, environment)
		);
	}
	if (isPropertyPresenceOperation(current)) {
		return propertyPresenceMatchesPredicate(current, predicateType, bindings, sourceCode, environment);
	}
	if (current.type === "BinaryExpression" && isParameterGuardRejection(current, bindings, sourceCode)) {
		return false;
	}
	if (parameterGuardAcceptsValue(current, bindings, sourceCode)) return true;
	if (current.type === "LogicalExpression" && current.operator === "&&") {
		return (
			isPositivePredicateCondition(
				current.left,
				bindings,
				sourceCode,
				typeofTagMatches,
				predicateType,
				environment,
			) &&
			isPositivePredicateCondition(
				current.right,
				bindings,
				sourceCode,
								typeofTagMatches,
								predicateType,
								environment,
							)
		);
	}
	return true;
}

function containsConsumedValidationEvidence(
	node: ESTree.Node,
	bindings: ParameterBindings,
	sourceCode: SourceCode | undefined,
	resolving: ValidationResolution | undefined,
	environment: TypeEnvironment | undefined,
): boolean {
	const pending: ESTree.Node[] = [node];
	while (pending.length > 0) {
		const current = pending.pop();
		if (current === undefined) continue;
		if (
			isValidationOperation(current, sourceCode, resolving, environment) &&
			validationReferencesParameter(current, bindings, sourceCode, resolving, environment)
		)
			return true;
		for (const [key, value] of Object.entries(current)) {
			if (key === "parent") continue;
			if (Array.isArray(value)) {
				for (const entry of value) if (isNode(entry)) pending.push(entry);
			} else if (isNode(value)) {
				pending.push(value);
			}
		}
	}
	return false;
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
	predicateType: ESTree.TSType | undefined,
	bindings: ParameterBindings,
	sourceCode?: SourceCode,
	environment?: TypeEnvironment,
	resolving?: ValidationResolution,
): boolean | undefined {
	const comparison = typeofComparisonForNode(node);
	const reflectedComparison = reflectedPropertyComparisonForNode(node);
	const identityLookupShape = isIdentityLookupShape(node) || isReflectiveBrandShape(node);
	const structuralIdentityEvidence =
		identityLookupShape &&
		predicateType !== undefined &&
		environment !== undefined &&
		typeRequiresStructuralRuntimeEvidence(predicateType, environment);
	const comparisonPropertyPath =
		comparison === undefined ? undefined : observedPropertyPath(comparison.operand, bindings, sourceCode);
	const requiresStructuralEvidence =
		comparison !== undefined &&
		comparison.tag === "object" &&
		comparisonPropertyPath?.length === 0 &&
		predicateType !== undefined &&
		environment !== undefined &&
		typeRequiresStructuralRuntimeEvidence(predicateType, environment);
	const typeofTagMatches = (candidate: TypeofComparison): boolean => {
		const propertyPath = observedPropertyPath(candidate.operand, bindings, sourceCode);
		if (propertyPath === undefined) return false;
		if (propertyPath.length === 0) {
			if (expectedTypeofTags?.has(candidate.tag) !== true) return false;
			return !(candidate.tag === "object" && requiresStructuralEvidence);
		}
		const propertyTags =
			predicateType === undefined || environment === undefined
				? undefined
				: typeResolvesToRuntimeTypeofTagsForPropertyPath(predicateType, propertyPath, environment);
		return (
			propertyTags?.has(candidate.tag) === true ||
			(propertyTags === undefined &&
				predicateType !== undefined &&
				environment !== undefined &&
				isImportedPredicateType(predicateType, environment) &&
				expectedTypeofTags?.has(candidate.tag) === true)
		);
	};
	const propertyPresence = isPropertyPresenceOperation(node)
		? {
			propertyName: node.left.value,
			propertyPath: parameterPropertyPath(node.right, bindings, sourceCode),
		}
		: undefined;
	if (
		comparison !== undefined &&
		!typeofTagMatches(comparison) &&
		!(requiresStructuralEvidence && comparisonPropertyPath?.length === 0 && comparison.tag === "object")
	)
		return undefined;
	if (
		comparison === undefined &&
		reflectedComparison !== undefined &&
		!reflectedPropertyMatchesPredicate(node, predicateType, bindings, sourceCode, environment)
	)
		return undefined;
	if (identityLookupShape && !structuralIdentityEvidence) return undefined;
	if (
		comparison === undefined &&
		node.type === "CallExpression" &&
		(predicateType === undefined ||
			!typePredicateCallEvidenceMatches(
				node,
				predicateType,
				expectedTypeofTags,
				sourceCode,
				environment,
				resolving,
			))
	)
		return undefined;
	if (
		comparison === undefined &&
		(propertyPresence === undefined ||
			propertyPresence.propertyPath === undefined ||
			predicateType === undefined ||
			environment === undefined ||
			!propertyPresenceMatchesPredicate(node, predicateType, bindings, sourceCode, environment)) &&
		reflectedComparison === undefined &&
		node.type !== "CallExpression"
	)
		return undefined;
	let current: ESTree.Node = comparison?.node ?? node;
	let polarity =
		comparison === undefined
			? reflectedComparison === undefined ||
				reflectedComparison.operator === "==" ||
				reflectedComparison.operator === "==="
			: comparison.operator === "==" || comparison.operator === "===";
	let hasStructuralEvidence = !requiresStructuralEvidence;
	const finish = (value: boolean): boolean | undefined =>
		!requiresStructuralEvidence || hasStructuralEvidence ? value : undefined;

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
					propertyPresence !== undefined &&
					!polarity &&
					propertyPresenceMatchesPredicate(
						node,
						predicateType,
						bindings,
						 sourceCode,
						environment,
					)
				) {
					if (
						requiresStructuralEvidence &&
						hasStructuralRuntimeEvidence(
							node,
							bindings,
							sourceCode,
							typeofTagMatches,
							predicateType,
							environment,
							resolving,
						)
					)
						hasStructuralEvidence = true;
					current = parent;
					continue;
				}
				if (
					otherComparison !== undefined &&
					otherComparison.operator !== "!=" &&
					otherComparison.operator !== "!==" &&
					typeofTagMatches(otherComparison)
				) {
					if (
						requiresStructuralEvidence &&
						hasStructuralRuntimeEvidence(
							other,
							bindings,
							sourceCode,
							typeofTagMatches,
							predicateType,
							environment,
							resolving,
						)
					)
						hasStructuralEvidence = true;
					current = parent;
					continue;
				}
				if (
					!polarity &&
					reflectedPropertyMatchesPredicate(other, predicateType, bindings, sourceCode, environment)
				) {
					if (
						requiresStructuralEvidence &&
						hasStructuralRuntimeEvidence(
							other,
							bindings,
							sourceCode,
							typeofTagMatches,
							predicateType,
							environment,
							resolving,
						)
					)
						hasStructuralEvidence = true;
					current = parent;
					continue;
				}
				if (
					!polarity &&
					isParameterGuardRejection(other, bindings, sourceCode)
				) {
					current = parent;
					continue;
				}
				if (
					!polarity &&
					other.type === "UnaryExpression" &&
					other.operator === "!" &&
					propertyPresenceMatchesPredicate(
						other,
						predicateType,
						bindings,
						 sourceCode,
						environment,
					)
				) {
					if (
						requiresStructuralEvidence &&
						hasStructuralRuntimeEvidence(
							other,
							bindings,
							sourceCode,
							typeofTagMatches,
							predicateType,
							environment,
							resolving,
						)
					)
						hasStructuralEvidence = true;
					current = parent;
					continue;
				}
				if (
					!polarity &&
					other.type === "CallExpression" &&
					["Array.isArray", "Object.hasOwn"].includes(validationCallName(other) ?? "") &&
					validationReferencesParameter(other, bindings, sourceCode, resolving, environment)
				) {
					if (
						requiresStructuralEvidence &&
						hasStructuralRuntimeEvidence(
							other,
							bindings,
							sourceCode,
							typeofTagMatches,
							predicateType,
							environment,
							resolving,
						)
					)
						hasStructuralEvidence = true;
					current = parent;
					continue;
				}
				if (
					!polarity &&
					containsConsumedValidationEvidence(other, bindings, sourceCode, resolving, environment)
				) {
					if (
						requiresStructuralEvidence &&
						hasStructuralRuntimeEvidence(
							other,
							bindings,
							sourceCode,
							typeofTagMatches,
							predicateType,
							environment,
							resolving,
						)
					)
						hasStructuralEvidence = true;
					current = parent;
					continue;
				}
				return undefined;
			}
			if (parent.operator !== "&&") return undefined;
			const other = parent.left === current ? parent.right : parent.left;
			if (
				!isPositivePredicateCondition(
					other,
					bindings,
					sourceCode,
					typeofTagMatches,
					predicateType,
					environment,
				)
			)
				return undefined;
			if (
				requiresStructuralEvidence &&
				hasStructuralRuntimeEvidence(
					other,
					bindings,
					sourceCode,
					typeofTagMatches,
					predicateType,
					environment,
					resolving,
				)
			)
				hasStructuralEvidence = true;
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
			if (consequent === true && alternate === false) return finish(polarity);
			if (consequent === false && alternate === true) return finish(!polarity);
			if (parent.alternate === null && consequent === false && nextStatement(parent) !== undefined)
				return finish(!polarity);
			return undefined;
		}
		if (parent.type === "ReturnStatement" && parent.argument === current) return finish(polarity);
		if (parent.type === "ArrowFunctionExpression" && parent.body === current) return finish(polarity);
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
					predicateType,
					bindings,
					sourceCode,
					environment,
					resolving,
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
		const isUnverifiedTypePredicateCall =
			isTypePredicateOwner &&
			current.type === "CallExpression" &&
			(predicateType === undefined ||
				!typePredicateCallEvidenceMatches(
					current,
					predicateType,
					expectedTypeofTags,
					sourceCode,
					environment,
					resolving,
				));
		if (
			(!isTypeofOperation || (allowTypeof && !isStrictTypeofPredicate)) &&
			!isUnverifiedTypePredicateCall &&
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
