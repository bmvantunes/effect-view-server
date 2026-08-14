import type { ESTree, Scope, SourceCode, Variable } from "@oxlint/plugins";
import {
	typeRequiresStructuralRuntimeEvidence,
	typeRequiresExactRuntimeValueEvidenceForPropertyPath,
	typeAtRuntimePropertyPath,
	typeAcceptsRuntimeLiteralValueForPropertyPath,
	typeResolvesToRuntimeTypeofTags,
	typeResolvesToRuntimeTypeofTagsForPropertyPath,
	type RuntimeLiteralValue,
	type TypeEnvironment,
} from "./dictionary-types.ts";
import {
	knownValidationCalls,
	trustedValidationImportExports,
	trustedValidationNamespaceImports,
	trustedValidationNamespaceRoots,
	validationCallbackWrappers,
	validationFactoryCalls,
	namedPredicateContracts,
	reviewedImportedTypeContracts,
	trustedSchemaUnknownValuePaths,
	type ReviewedImportedTypeContract,
} from "./repository-validation-policy.ts";

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

function reviewedImportedTypeContract(
	type: ESTree.TSType,
	environment: TypeEnvironment,
	resolving: ReadonlySet<string> = new Set(),
): ReviewedImportedTypeContract | undefined {
	const current = unwrapRuntimeTypeofOperand(type);
	if (current.type !== "TSTypeReference" || current.typeName.type !== "Identifier") return undefined;
	const name = current.typeName.name;
	const binding = environment.resolveImportedType(current, name);
	if (binding !== undefined) {
		return reviewedImportedTypeContracts.get(`${binding.source}:${binding.imported}`);
	}
	if (resolving.has(name)) return undefined;
	const alias = environment.resolveAlias(current, name);
	return alias === undefined
		? undefined
		: reviewedImportedTypeContract(alias.typeAnnotation, environment, new Set([...resolving, name]));
}

function reviewedImportedLiteralValues(
	type: ESTree.TSType | undefined,
	propertyPath: readonly string[],
	environment: TypeEnvironment | undefined,
): ReadonlySet<RuntimeLiteralValue> | undefined {
	if (type === undefined || environment === undefined) return undefined;
	return reviewedImportedTypeContract(type, environment)?.literalProperties?.get(propertyPath.join("."));
}

function predicateRuntimeTypeofTags(
	type: ESTree.TSType,
	propertyPath: readonly string[],
	environment: TypeEnvironment,
): ReadonlySet<string> | undefined {
	const tags = typeResolvesToRuntimeTypeofTagsForPropertyPath(type, propertyPath, environment);
	if (tags !== undefined) return tags;
	const contract = reviewedImportedTypeContract(type, environment);
	const directProperty = contract?.properties.get(propertyPath.join("."));
	if (directProperty !== undefined) return directProperty;
	if (propertyPath.length === 0) return contract?.tags;
	let current = type;
	for (const [index, propertyName] of propertyPath.entries()) {
		const propertyType = typeAtRuntimePropertyPath(current, [propertyName], environment);
		if (propertyType !== undefined) {
			current = propertyType;
			continue;
		}
		const nestedContract = reviewedImportedTypeContract(current, environment);
		const nestedTags = nestedContract?.properties.get(propertyName);
		return index === propertyPath.length - 1 ? nestedTags : undefined;
	}
	return typeResolvesToRuntimeTypeofTags(current, environment) ?? reviewedImportedTypeContract(current, environment)?.tags;
}

function predicateRequiresStructuralEvidence(
	type: ESTree.TSType,
	environment: TypeEnvironment,
): boolean {
	return (
		typeRequiresStructuralRuntimeEvidence(type, environment) ||
		reviewedImportedTypeContract(type, environment)?.structural === true
	);
}

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

function memberExpressionPath(node: ESTree.Node): string | null {
	if (node.type === "Identifier") return node.name;
	if (
		node.type !== "MemberExpression" ||
		node.computed ||
		node.property.type !== "Identifier"
	)
		return null;
	const objectPath = memberExpressionPath(node.object);
	return objectPath === null ? null : `${objectPath}.${node.property.name}`;
}

function canonicalImportedValidationName(
	name: string,
	source: string,
	imported: string,
): string | undefined {
	if (source !== "effect") return undefined;
	const parts = name.split(".");
	if (imported === "*") {
		const namespaceRoot = parts[1];
		return namespaceRoot !== undefined &&
			trustedValidationNamespaceImports.get(source)?.has(namespaceRoot) === true &&
			trustedValidationNamespaceRoots.has(namespaceRoot)
			? parts.slice(1).join(".")
			: undefined;
	}
	if (!trustedValidationNamespaceRoots.has(imported)) return undefined;
	const member = parts.slice(1).join(".");
	return member.length === 0 ? imported : `${imported}.${member}`;
}

function canonicalValidationName(
	node: ESTree.Node,
	name: string,
	environment: TypeEnvironment | undefined,
): string {
	if (environment === undefined) return name;
	const root = name.split(".")[0];
	if (root === undefined) return name;
	const binding = environment.resolveImportedType(node, root);
	return binding === undefined
		? name
		: canonicalImportedValidationName(name, binding.source, binding.imported) ?? name;
}

function validationCallName(
	node: ESTree.CallExpression,
	environment?: TypeEnvironment,
): string | null {
	const name = memberExpressionPath(node.callee);
	return name === null ? null : canonicalValidationName(node, name, environment);
}

function validationRootIdentifier(node: ESTree.Node): IdentifierNode | undefined {
	if (node.type === "Identifier") return node;
	return node.type === "MemberExpression" && !node.computed
		? validationRootIdentifier(node.object)
		: undefined;
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
	"Reflect.apply",
	"Reflect.get",
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
	"Schedule.isSchedule",
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
		if (propertyValueComparisonShape(node)) return true;
		if (isIdentityLookupShape(node, sourceCode, environment)) return true;
		const reflected = reflectedPropertyComparisonForNode(node);
		if (reflected !== undefined) {
			if (isShadowedValidationCall(reflected.operand, sourceCode, resolving, environment)) return false;
			return true;
		}
		const left = node.left.type === "UnaryExpression" && isTypeofComparison(node.left);
		const right = node.right.type === "UnaryExpression" && isTypeofComparison(node.right);
		return left || right;
	}
	if (node.type !== "CallExpression") return false;
	const name = validationCallName(node, environment);
	if (name === null) {
		if (node.callee.type !== "CallExpression") return false;
		const factoryName = validationCallName(node.callee, environment);
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
			const name = validationCallName(current, environment);
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
	return variable.defs.every((definition) => {
		const details = importBindingDetails(definition);
		if (details === undefined) return false;
		const canonicalName = canonicalImportedValidationName(name, details.source, details.imported);
		const builtinName =
			isBuiltinValidationName(name) ||
			(canonicalName !== undefined && isBuiltinValidationName(canonicalName));
		if (details.imported === "*") {
			const namespaceRoot = name.split(".")[1];
			return (
				namespaceRoot !== undefined &&
				trustedValidationNamespaceImports.get(details.source)?.has(namespaceRoot) === true &&
				trustedValidationNamespaceRoots.has(namespaceRoot) &&
				builtinName
			);
		}
		const trusted = trustedValidationImportExports.get(details.source)?.has(details.imported) ?? false;
		return (
			trusted &&
			(!trustedValidationNamespaceRoots.has(details.imported) || builtinName)
		);
	});
}

function isTrustedValidationCall(
	node: ESTree.CallExpression,
	sourceCode?: SourceCode,
	resolving?: ValidationResolution,
	environment?: TypeEnvironment,
): boolean {
	const sourceName = validationCallName(node);
	const name = validationCallName(node, environment);
	if (name === null) return false;
	const recognizedValidationName =
		isBuiltinValidationName(name) ||
		knownValidationCalls.has(name) ||
		validationCallbackWrappers.has(name);
	if (sourceCode === undefined) return true;
	const identifier = validationRootIdentifier(node.callee);
	if (identifier === undefined) return false;
	const variable = resolvedVariable(sourceCode, identifier);
	if (variable === undefined) return isBuiltinValidationName(name);
	if (variable.defs.some((definition) => definition.type === "ImportBinding")) {
		return (
			variable.defs.every((definition) => definition.type === "ImportBinding") &&
			isTrustedImportedValidation(variable, sourceName ?? name)
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

function propertyValueComparisonShape(node: ESTree.Node): node is ESTree.BinaryExpression {
	if (node.type !== "BinaryExpression" || !["==", "===", "!=", "!=="].includes(node.operator)) return false;
	return (
		(node.left.type === "Literal" && node.right.type === "MemberExpression") ||
		(node.right.type === "Literal" && node.left.type === "MemberExpression")
	);
}

function runtimeLiteralValue(node: ESTree.Literal): RuntimeLiteralValue | undefined {
	if (node.value === null) return null;
	return typeof node.value === "bigint" ||
		typeof node.value === "boolean" ||
		typeof node.value === "number" ||
		typeof node.value === "string"
		? node.value
		: undefined;
}

function runtimeTagForLiteral(node: ESTree.Literal): string | undefined {
	const value = runtimeLiteralValue(node);
	if (value === null || value === undefined) return undefined;
	if (typeof value === "bigint") return "bigint";
	if (typeof value === "boolean") return "boolean";
	if (typeof value === "number") return "number";
	if (typeof value === "string") return "string";
	return undefined;
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

function isSourceAdapterHandlesBinding(
	node: ESTree.Node,
	sourceCode: SourceCode | undefined,
	environment: TypeEnvironment | undefined,
): boolean {
	if (sourceCode === undefined || environment === undefined || node.type !== "Identifier") return false;
	const binding = environment.resolveImportedType(node, node.name);
	return binding?.source === "./adapter-brand" && binding.imported === "sourceAdapterHandles";
}

function isIdentityLookupShape(
	node: ESTree.Node,
	sourceCode?: SourceCode,
	environment?: TypeEnvironment,
): boolean {
	if (node.type !== "BinaryExpression" || !["==", "==="].includes(node.operator)) return false;
	return [node.left, node.right].some(
		(candidate) =>
			candidate.type === "CallExpression" &&
			candidate.callee.type === "MemberExpression" &&
			!candidate.callee.computed &&
				candidate.callee.object.type === "Identifier" &&
				isSourceAdapterHandlesBinding(candidate.callee.object, sourceCode, environment) &&
			candidate.callee.property.type === "Identifier" &&
			candidate.callee.property.name === "get",
	);
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
		predicateRuntimeTypeofTags(
			predicateType,
			[...path, current.left.value],
			environment,
		) !== undefined
	);
}

function propertyValueMatchesPredicate(
	node: ESTree.Node,
	predicateType: ESTree.TSType | undefined,
	bindings: ParameterBindings,
	sourceCode: SourceCode | undefined,
	environment: TypeEnvironment | undefined,
): boolean {
	if (predicateType === undefined || environment === undefined || !propertyValueComparisonShape(node)) return false;
	const literal = node.left.type === "Literal" ? node.left : node.right;
	const property = node.left.type === "MemberExpression" ? node.left : node.right;
	const path = parameterPropertyPath(property, bindings, sourceCode);
	const value = runtimeLiteralValue(literal);
	const tag = runtimeTagForLiteral(literal);
	if (path === undefined || path.length === 0 || value === undefined) return false;
	const exactType = typeAtRuntimePropertyPath(predicateType, path, environment);
	if (
		exactType !== undefined &&
		!typeAcceptsRuntimeLiteralValueForPropertyPath(predicateType, path, value, environment)
	)
		return false;
	const reviewedValues = reviewedImportedLiteralValues(predicateType, path, environment);
	if (reviewedValues !== undefined && !reviewedValues.has(value)) return false;
	return tag === undefined || predicateRuntimeTypeofTags(predicateType, path, environment)?.has(tag) === true;
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
	if (isShadowedValidationCall(comparison.operand, sourceCode, undefined, environment)) return false;
	const path = reflectedPropertyPath(comparison.operand, bindings, sourceCode);
	const literal =
		comparison.node.left.type === "Literal"
			? comparison.node.left
			: comparison.node.right.type === "Literal"
				? comparison.node.right
				: undefined;
	const value = literal === undefined ? undefined : runtimeLiteralValue(literal);
	return (
		path !== undefined &&
		value !== undefined &&
		(typeAtRuntimePropertyPath(predicateType, path, environment) === undefined ||
			typeAcceptsRuntimeLiteralValueForPropertyPath(predicateType, path, value, environment)) &&
		(reviewedImportedLiteralValues(predicateType, path, environment)?.has(value) ?? true) &&
		predicateRuntimeTypeofTags(predicateType, path, environment) !== undefined
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

function schemaEvidence(
	node: ESTree.Node,
	environment?: TypeEnvironment,
): SchemaEvidence | undefined {
	const current = unwrapRuntimeTypeofOperand(node);
	if (
		current.type === "MemberExpression" &&
		!current.computed &&
		current.property.type === "Identifier"
	) {
		const path = memberExpressionPath(current);
		const name = path === null ? undefined : canonicalValidationName(current, path, environment);
		const propertyName = name?.startsWith("Schema.") ? name.slice("Schema.".length) : undefined;
		switch (propertyName) {
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
	const name = validationCallName(current, environment);
	if (name === "Schema.Array" || name === "Schema.Tuple") {
		return { tags: new Set(["object"]), structural: false, arrayLike: true };
	}
	if (name === "Schema.Struct" || name === "Schema.Record") {
		return { tags: new Set(["object"]), structural: true, arrayLike: false };
	}
	return undefined;
}

type PredicateField = {
	readonly optional: boolean;
	readonly type: ESTree.TSType;
};

type PredicateIndex = {
	readonly key: ESTree.TSType | undefined;
	readonly value: ESTree.TSType;
};

type PredicateTypeSubstitutions = ReadonlyMap<string, ESTree.TSType>;

function resolvePredicateType(
	type: ESTree.TSType,
	substitutions: PredicateTypeSubstitutions,
	resolving: ReadonlySet<string> = new Set(),
): ESTree.TSType {
	const current = unwrapRuntimeTypeofOperand(type);
	if (current.type !== "TSTypeReference" || current.typeName.type !== "Identifier") return type;
	const substitution = substitutions.get(current.typeName.name);
	if (substitution === undefined || resolving.has(current.typeName.name)) return type;
	return resolvePredicateType(
		substitution,
		new Map([...substitutions].filter(([name]) => name !== current.typeName.name)),
		new Set([...resolving, current.typeName.name]),
	);
}

function predicateInterfaceSubstitutions(
	declaration: ESTree.TSInterfaceDeclaration,
	arguments_: readonly ESTree.TSType[],
	base: PredicateTypeSubstitutions,
): PredicateTypeSubstitutions | undefined {
	const parameters = declaration.typeParameters?.params ?? [];
	const next = new Map(base);
	for (const [index, parameter] of parameters.entries()) {
		const argument = arguments_[index] ?? parameter.default;
		if (argument === undefined) return undefined;
		next.set(parameter.name.name, resolvePredicateType(argument, base));
	}
	return next;
}

function propertyNameFromKey(key: ESTree.Node): string | undefined {
	return key.type === "Identifier"
		? key.name
		: key.type === "Literal" && typeof key.value === "string"
			? key.value
			: undefined;
}

function heritageTypeName(node: ESTree.Node): string | null {
	if (node.type === "Identifier") return node.name;
	if (
		node.type === "MemberExpression" &&
		!node.computed &&
		node.object.type === "Identifier" &&
		node.property.type === "Identifier"
	)
		return `${node.object.name}.${node.property.name}`;
	if (
		node.type === "TSQualifiedName" &&
		node.left.type === "Identifier" &&
		node.right.type === "Identifier"
	)
		return `${node.left.name}.${node.right.name}`;
	return null;
}

function mergePredicateField(
	fields: Map<string, PredicateField>,
	name: string,
	field: PredicateField,
): void {
	const existing = fields.get(name);
	fields.set(
		name,
		existing === undefined
			? field
			: { optional: existing.optional && field.optional, type: existing.type },
	);
}

function predicateFieldsFromInterface(
	declaration: ESTree.TSInterfaceDeclaration,
	environment: TypeEnvironment,
	resolving: ReadonlySet<ESTree.TSInterfaceDeclaration>,
	substitutions: PredicateTypeSubstitutions = new Map(),
): ReadonlyMap<string, PredicateField> | undefined {
	if (resolving.has(declaration)) return undefined;
	const nextResolving = new Set(resolving);
	nextResolving.add(declaration);
	const fields = new Map<string, PredicateField>();
	for (const member of declaration.body.body) {
		if (member.type === "TSIndexSignature") return undefined;
		if (member.type !== "TSPropertySignature" || member.typeAnnotation === null) continue;
		const name = propertyNameFromKey(member.key);
		if (name === undefined) return undefined;
		mergePredicateField(fields, name, {
			optional: member.optional === true,
			type: resolvePredicateType(member.typeAnnotation.typeAnnotation, substitutions),
		});
	}
	for (const heritage of declaration.extends) {
		const name = heritageTypeName(heritage.expression);
		if (name === null) return undefined;
		for (const parent of environment.resolveInterfaces(heritage.expression, name)) {
			const parentSubstitutions = predicateInterfaceSubstitutions(
				parent,
				heritage.typeArguments?.params ?? [],
				substitutions,
			);
			if (parentSubstitutions === undefined) return undefined;
			const parentFields = predicateFieldsFromInterface(
				parent,
				environment,
				nextResolving,
				parentSubstitutions,
			);
			if (parentFields === undefined) return undefined;
			for (const [fieldName, field] of parentFields) mergePredicateField(fields, fieldName, field);
		}
	}
	return fields;
}

function predicateTypeFields(
	type: ESTree.TSType,
	environment: TypeEnvironment,
	resolving: ReadonlySet<string> = new Set(),
): ReadonlyMap<string, PredicateField> | undefined {
	const current = unwrapRuntimeTypeofOperand(type);
	if (current.type === "TSTypeLiteral") {
		const fields = new Map<string, PredicateField>();
		for (const member of current.members) {
			if (member.type === "TSIndexSignature") return undefined;
			if (member.type !== "TSPropertySignature" || member.typeAnnotation === null) continue;
			const name = propertyNameFromKey(member.key);
			if (name === undefined) return undefined;
			mergePredicateField(fields, name, {
				optional: member.optional === true,
				type: member.typeAnnotation.typeAnnotation,
			});
		}
		return fields;
	}
	if (current.type === "TSIntersectionType") {
		const fields = new Map<string, PredicateField>();
		for (const member of current.types) {
			const memberFields = predicateTypeFields(member, environment, resolving);
			if (memberFields === undefined) return undefined;
			for (const [fieldName, field] of memberFields) mergePredicateField(fields, fieldName, field);
		}
		return fields;
	}
	if (current.type !== "TSTypeReference" || current.typeName.type !== "Identifier") return undefined;
	const name = current.typeName.name;
	if (resolving.has(name)) return undefined;
	const alias = environment.resolveAlias(current, name);
	if (alias !== undefined) {
		return predicateTypeFields(alias.typeAnnotation, environment, new Set([...resolving, name]));
	}
	const declarations = environment.resolveInterfaces(current, name);
	if (declarations.length === 0) return undefined;
	const fields = new Map<string, PredicateField>();
	for (const declaration of declarations) {
			const declarationFields = predicateFieldsFromInterface(declaration, environment, new Set());
		if (declarationFields === undefined) return undefined;
		for (const [fieldName, field] of declarationFields) mergePredicateField(fields, fieldName, field);
	}
	return fields;
}

function predicateTypeIndex(type: ESTree.TSType, environment: TypeEnvironment): PredicateIndex | undefined {
	const current = unwrapRuntimeTypeofOperand(type);
	if (current.type === "TSTypeLiteral") {
		const index = current.members.find((member) => member.type === "TSIndexSignature");
		if (index === undefined || index.typeAnnotation === null) return undefined;
		const key = index.parameters[0];
		return {
			key:
				key?.type === "Identifier" && key.typeAnnotation !== null && key.typeAnnotation !== undefined
					? key.typeAnnotation.typeAnnotation
					: undefined,
			value: index.typeAnnotation.typeAnnotation,
		};
	}
	if (current.type !== "TSTypeReference" || current.typeName.type !== "Identifier") return undefined;
	if (current.typeName.name === "Record") {
		const params = current.typeArguments?.params ?? [];
		return params[1] === undefined
			? undefined
			: { key: params[0], value: params[1] };
	}
	const alias = environment.resolveAlias(current, current.typeName.name);
	return alias === undefined ? undefined : predicateTypeIndex(alias.typeAnnotation, environment);
}

function predicateArrayElementTypes(
	type: ESTree.TSType,
	environment: TypeEnvironment,
): readonly ESTree.TSType[] | undefined {
	const current = unwrapRuntimeTypeofOperand(type);
	if (current.type === "TSArrayType") return [current.elementType];
	if (current.type === "TSTupleType") return current.elementTypes;
	if (
		current.type === "TSTypeReference" &&
		current.typeName.type === "Identifier" &&
		(current.typeName.name === "Array" || current.typeName.name === "ReadonlyArray")
	) {
		const element = current.typeArguments?.params[0];
		return element === undefined ? undefined : [element];
	}
	const alias =
		current.type === "TSTypeReference" && current.typeName.type === "Identifier"
			? environment.resolveAlias(current, current.typeName.name)
			: undefined;
	return alias === undefined ? undefined : predicateArrayElementTypes(alias.typeAnnotation, environment);
}

function schemaUnionMembers(
	node: ESTree.Node,
	environment?: TypeEnvironment,
): readonly ESTree.Node[] | undefined {
	const current = unwrapRuntimeTypeofOperand(node);
	if (
		current.type !== "CallExpression" ||
		validationCallName(current, environment) !== "Schema.Union"
	)
		return undefined;
	const arguments_ = current.arguments.filter(
		(argument): argument is ESTree.Expression => argument.type !== "SpreadElement",
	);
	const members =
		arguments_.length === 1 && arguments_[0]?.type === "ArrayExpression"
			? arguments_[0].elements
			: arguments_;
	return members.every(
		(member): member is ESTree.Node => member !== null && member.type !== "SpreadElement",
	)
		? members
		: undefined;
}

function schemaInitializer(node: ESTree.Node, sourceCode?: SourceCode): ESTree.Node {
	if (node.type !== "Identifier" || sourceCode === undefined) return node;
	const variable = resolvedVariable(sourceCode, node);
	const initializer = variable?.defs
		.map((definition) =>
			definition.type === "Variable" && definition.node.type === "VariableDeclarator"
				? definition.node.init
				: null,
		)
		.find((candidate): candidate is ESTree.Expression => candidate !== null);
	return initializer ?? node;
}

function schemaMatchesPredicate(
	schemaNode: ESTree.Node,
	predicateType: ESTree.TSType,
	environment: TypeEnvironment,
	sourceCode?: SourceCode,
	resolvingSchemas: ReadonlySet<ESTree.Node> = new Set(),
): boolean {
	const current = schemaInitializer(schemaNode, sourceCode);
	if (resolvingSchemas.has(current)) return false;
	const predicate = unwrapRuntimeTypeofOperand(predicateType);
	if (predicate.type === "TSUnionType") {
		const schemaMembers = schemaUnionMembers(current, environment);
		if (schemaMembers !== undefined) {
			return (
				schemaMembers.length > 0 &&
				schemaMembers.every((member) =>
					predicate.types.some((memberType) =>
						schemaMatchesPredicate(member, memberType, environment, sourceCode, resolvingSchemas),
					),
				)
			);
		}
		return predicate.types.some((memberType) =>
			schemaMatchesPredicate(current, memberType, environment, sourceCode, resolvingSchemas),
		);
	}
	const nextResolvingSchemas = new Set(resolvingSchemas);
	nextResolvingSchemas.add(current);
	const directSchemaName =
		current.type === "MemberExpression" && !current.computed
			? (() => {
					const path = memberExpressionPath(current);
					return path === null ? undefined : canonicalValidationName(current, path, environment);
				})()
			: undefined;
	if (directSchemaName?.startsWith("Schema.") === true) {
		const evidence = schemaEvidence(current, environment);
		if (evidence === undefined) {
			return trustedSchemaUnknownValuePaths.has(directSchemaName)
				? predicate.type === "TSUnknownKeyword"
				: directSchemaName === "Schema.Null" &&
						typeAcceptsRuntimeLiteralValueForPropertyPath(predicateType, [], null, environment);
		}
		if (evidence.broadObject) {
			return predicateRuntimeTypeofTags(predicateType, [], environment)?.has("object") === true &&
				!predicateRequiresStructuralEvidence(predicateType, environment);
		}
		if (evidence.arrayLike) return predicateArrayElementTypes(predicateType, environment) !== undefined;
		return Array.from(evidence.tags).every((tag) => predicateRuntimeTypeofTags(predicateType, [], environment)?.has(tag) === true);
	}
	if (current.type !== "CallExpression") return false;
	const name = validationCallName(current, environment);
	const arguments_ = current.arguments.filter((argument): argument is ESTree.Expression => argument.type !== "SpreadElement");
	if (name === "Schema.optionalKey" || name === "Schema.optional") {
		const inner = arguments_[0];
		return inner !== undefined && schemaMatchesPredicate(inner, predicateType, environment, sourceCode, nextResolvingSchemas);
	}
	if (name === "Schema.Literal") {
		const literal = arguments_[0];
		if (literal?.type !== "Literal") return false;
		const value = runtimeLiteralValue(literal);
		return value !== undefined &&
			typeAcceptsRuntimeLiteralValueForPropertyPath(predicateType, [], value, environment);
	}
	if (name === "Schema.Literals") {
		const literals = arguments_[0];
		if (literals?.type !== "ArrayExpression") return false;
		return literals.elements.every((element) => {
			if (element === null || element.type === "SpreadElement" || element.type !== "Literal") return false;
			const value = runtimeLiteralValue(element);
			return value !== undefined &&
				typeAcceptsRuntimeLiteralValueForPropertyPath(predicateType, [], value, environment);
		});
	}
	if (name === "Schema.Array" || name === "Schema.ReadonlyArray") {
		const schemaElement = arguments_[0];
		const predicateElements = predicateArrayElementTypes(predicateType, environment);
		return schemaElement !== undefined && predicateElements !== undefined && predicateElements.every((element) =>
			schemaMatchesPredicate(schemaElement, element, environment, sourceCode, nextResolvingSchemas),
		);
	}
	if (name === "Schema.Tuple") {
		const predicate = unwrapRuntimeTypeofOperand(predicateType);
		const schemaElements =
			arguments_.length === 1 && arguments_[0]?.type === "ArrayExpression"
				? arguments_[0].elements
				: arguments_;
		if (
			predicate.type !== "TSTupleType" ||
			predicate.elementTypes.length !== schemaElements.length ||
			schemaElements.some((element) => element === null || element.type === "SpreadElement")
		)
			return false;
		return schemaElements.every((schemaElement, index) =>
			schemaElement !== null &&
			schemaElement.type !== "SpreadElement" &&
			schemaMatchesPredicate(schemaElement, predicate.elementTypes[index]!, environment, sourceCode, nextResolvingSchemas),
		);
	}
	if (name === "Schema.Record") {
		const index = predicateTypeIndex(predicateType, environment);
		return (
			index !== undefined &&
			arguments_[0] !== undefined &&
			arguments_[1] !== undefined &&
			(index.key === undefined || schemaMatchesPredicate(arguments_[0], index.key, environment, sourceCode, nextResolvingSchemas)) &&
			schemaMatchesPredicate(arguments_[1], index.value, environment, sourceCode, nextResolvingSchemas)
		);
	}
	if (name === "Schema.Union") {
		const members = arguments_[0]?.type === "ArrayExpression" ? arguments_[0].elements : arguments_;
		return members.length > 0 && members.every((member) =>
			member !== null && member.type !== "SpreadElement" && schemaMatchesPredicate(member, predicateType, environment, sourceCode, nextResolvingSchemas),
		);
	}
	if (name !== "Schema.Struct") return false;
	const fields = arguments_[0];
	const predicateFields = predicateTypeFields(predicateType, environment);
	if (fields?.type !== "ObjectExpression" || predicateFields === undefined) return false;
	const schemaFields = new Map<string, ESTree.Node>();
	for (const property of fields.properties) {
		if (property.type !== "Property" || property.computed || property.key.type === "PrivateIdentifier" || property.value.type === "SpreadElement") return false;
		const fieldName = propertyNameFromKey(property.key);
		if (fieldName === undefined) return false;
		schemaFields.set(fieldName, property.value);
	}
	for (const [fieldName, field] of predicateFields) {
		const schemaField = schemaFields.get(fieldName);
		if (schemaField === undefined) {
			if (field.optional) continue;
			return false;
		}
		const schemaFieldNode = schemaInitializer(schemaField, sourceCode);
		if (
			schemaFieldNode.type === "CallExpression" &&
			schemaFieldNode.callee.type === "MemberExpression" &&
			!schemaFieldNode.callee.computed &&
			schemaFieldNode.callee.object.type === "Identifier" &&
			schemaFieldNode.callee.object.name === "Schema" &&
			schemaFieldNode.callee.property.type === "Identifier" &&
			(schemaFieldNode.callee.property.name === "optionalKey" ||
				schemaFieldNode.callee.property.name === "optional") &&
			!field.optional
		)
			return false;
		if (!schemaMatchesPredicate(schemaField, field.type, environment, sourceCode, nextResolvingSchemas)) return false;
	}
	return true;
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

function typeContainsNamedReference(
	type: ESTree.Node,
	names: ReadonlySet<string>,
	environment?: TypeEnvironment,
	seen: ReadonlySet<ESTree.Node> = new Set(),
): boolean {
	if (seen.has(type)) return false;
	const nextSeen = new Set(seen);
	nextSeen.add(type);
	if (type.type === "TSTypeReference") {
		const name =
			type.typeName.type === "Identifier"
				? type.typeName.name
				: type.typeName.type === "TSQualifiedName" && type.typeName.right.type === "Identifier"
					? type.typeName.right.name
					: undefined;
		if (
			name !== undefined &&
			names.has(name) &&
			environment !== undefined &&
			environment.resolveAlias(type, name) === undefined &&
			environment.resolveInterfaces(type, name).length === 0
		)
			return true;
		if (
			environment !== undefined &&
			type.typeName.type === "Identifier" &&
			name !== undefined
		) {
			const alias = environment.resolveAlias(type, name);
			if (
				alias !== undefined &&
				typeContainsNamedReference(alias.typeAnnotation, names, environment, nextSeen)
			)
				return true;
		}
	}
	for (const [key, value] of Object.entries(type)) {
		if (key === "parent") continue;
		if (Array.isArray(value)) {
			if (
				value.some(
					(entry) =>
						isNode(entry) && typeContainsNamedReference(entry, names, environment, nextSeen),
				)
			)
				return true;
		} else if (isNode(value) && typeContainsNamedReference(value, names, environment, nextSeen)) {
			return true;
		}
	}
	return false;
}

function schemaStructMatchesPredicate(
	schemaNode: ESTree.Node,
	predicateType: ESTree.TSType,
	environment: TypeEnvironment,
	sourceCode?: SourceCode,
): boolean {
	return schemaMatchesPredicate(schemaNode, predicateType, environment, sourceCode);
}

function typePredicateCallEvidenceMatches(
	node: ESTree.CallExpression,
	predicateType: ESTree.TSType,
	expectedTypeofTags: ReadonlySet<string> | undefined,
	sourceCode: SourceCode | undefined,
	environment: TypeEnvironment | undefined,
	resolving: ValidationResolution,
): boolean {
	const name = validationCallName(node, environment);
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
	if (
		node.callee.type === "CallExpression" &&
		validationCallName(node.callee, environment) === "Schema.is"
	) {
		const schemaNode = node.callee.arguments[0];
		if (schemaNode === undefined || schemaNode.type === "SpreadElement") return false;
		if (
			schemaNode.type === "MemberExpression" &&
			!schemaNode.computed &&
			memberExpressionPath(schemaNode) !== null &&
					trustedSchemaUnknownValuePaths.has(
						canonicalValidationName(schemaNode, memberExpressionPath(schemaNode) ?? "", environment) ??
							"",
					)
		)
			return false;
		const resolvedSchemaNode =
			schemaNode.type === "Identifier" && sourceCode !== undefined
				? (() => {
						const variable = resolvedVariable(sourceCode, schemaNode);
						const initializer = variable?.defs
							.map((definition) =>
								definition.type === "Variable" && definition.node.type === "VariableDeclarator"
									? definition.node.init
									: null,
							)
							.find((candidate): candidate is ESTree.Expression => candidate !== null);
						return initializer;
					})()
				: schemaNode;
		const resolvedEvidence =
			resolvedSchemaNode === undefined ? undefined : schemaEvidence(resolvedSchemaNode, environment);
		if (resolvedEvidence === undefined) return false;
		if (resolvedEvidence.arrayLike) return isArrayPredicateType(predicateType);
		if (resolvedEvidence.broadObject) {
			return (
				expectedTypeofTags?.has("object") === true &&
				environment !== undefined &&
				!predicateRequiresStructuralEvidence(predicateType, environment)
			);
		}
		if (resolvedEvidence.structural) {
			return (
				expectedTypeofTags?.has("object") === true &&
				environment !== undefined &&
				schemaStructMatchesPredicate(resolvedSchemaNode, predicateType, environment, sourceCode)
			);
		}
		return (
			expectedTypeofTags !== undefined &&
			Array.from(resolvedEvidence.tags).every((tag) => expectedTypeofTags.has(tag))
		);
	}
	if (name === "Schema.isSchema") {
		return environment !== undefined && isSchemaType(predicateType, environment);
	}
	if (name === "Object.hasOwn") {
		return false;
	}
	if (isLocallyTrustedValidationCall(node, sourceCode, resolving, environment)) {
		return localPredicateTypeMatches(node, predicateType, sourceCode);
	}
	if (name === null || environment === undefined) return false;
	const contract = namedPredicateContracts.get(name);
	return contract !== undefined && typeContainsNamedReference(predicateType, contract, environment);
}

function isIdentityLookupEvidence(
	node: ESTree.Node,
	bindings: ParameterBindings,
	sourceCode: SourceCode | undefined,
	environment: TypeEnvironment | undefined,
	predicateType: ESTree.TSType | undefined,
): boolean {
	const current = unwrapRuntimeTypeofOperand(node);
	if (
		current.type !== "BinaryExpression" ||
		!["==", "==="].includes(current.operator) ||
		environment === undefined ||
		predicateType === undefined ||
		!typeContainsNamedReference(
			predicateType,
			new Set(["SourceAdapterHandle", "SourceAdapterDescriptor"]),
			environment,
		)
	)
		return false;
	const isLookup = (candidate: ESTree.Node): boolean => {
		const expression = unwrapRuntimeTypeofOperand(candidate);
		return (
			expression.type === "CallExpression" &&
			expression.callee.type === "MemberExpression" &&
			!expression.callee.computed &&
				expression.callee.object.type === "Identifier" &&
				isSourceAdapterHandlesBinding(expression.callee.object, sourceCode, environment) &&
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
	if (propertyValueComparisonShape(current)) {
		return (
			(current.operator === "==" || current.operator === "===") &&
			propertyValueMatchesPredicate(current, predicateType, bindings, sourceCode, environment)
		);
	}
	if (isPropertyPresenceOperation(current)) {
		return false;
	}
	if (
		isIdentityLookupEvidence(current, bindings, sourceCode, environment, predicateType)
	) {
		return true;
	}
	if (current.type === "CallExpression") {
		const name = validationCallName(current);
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
				 predicateRequiresStructuralEvidence(predicateType, environment)
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
	const propertyValueComparison = propertyValueComparisonShape(node) ? node : undefined;
	const identityLookupShape = isIdentityLookupShape(node, sourceCode, environment);
	const structuralIdentityEvidence =
		identityLookupShape &&
		predicateType !== undefined &&
		environment !== undefined &&
		(predicateRequiresStructuralEvidence(predicateType, environment) ||
			(isIdentityLookupShape(node, sourceCode, environment) &&
				isIdentityLookupEvidence(node, bindings, sourceCode, environment, predicateType)));
	const comparisonPropertyPath =
		comparison === undefined ? undefined : observedPropertyPath(comparison.operand, bindings, sourceCode);
	const requiresStructuralEvidence =
		comparison !== undefined &&
		comparison.tag === "object" &&
		comparisonPropertyPath?.length === 0 &&
		predicateType !== undefined &&
		environment !== undefined &&
		predicateRequiresStructuralEvidence(predicateType, environment);
	const typeofTagMatches = (candidate: TypeofComparison): boolean => {
		const propertyPath = observedPropertyPath(candidate.operand, bindings, sourceCode);
		if (propertyPath === undefined) return false;
		const observedOperand = unwrapRuntimeTypeofOperand(candidate.operand);
		if (
			observedOperand.type === "CallExpression" &&
			validationCallName(observedOperand) === "Reflect.get" &&
			isShadowedValidationCall(observedOperand, sourceCode, resolving, environment)
		)
			return false;
		if (propertyPath.length === 0) {
			if (
				predicateType !== undefined &&
				environment !== undefined &&
				typeRequiresExactRuntimeValueEvidenceForPropertyPath(predicateType, [], environment)
			)
				return false;
			if (expectedTypeofTags?.has(candidate.tag) !== true) return false;
			return !(candidate.tag === "object" && requiresStructuralEvidence);
		}
		if (
			predicateType !== undefined &&
			environment !== undefined &&
			typeRequiresExactRuntimeValueEvidenceForPropertyPath(predicateType, propertyPath, environment)
		)
			return false;
		const propertyTags =
			predicateType === undefined || environment === undefined
				? undefined
				: predicateRuntimeTypeofTags(predicateType, propertyPath, environment);
		return (
			propertyTags?.has(candidate.tag) === true
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
	if (
		propertyValueComparisonShape(node) &&
		!propertyValueMatchesPredicate(node, predicateType, bindings, sourceCode, environment)
	)
		return undefined;
	if (propertyPresence !== undefined) return undefined;
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
	let current: ESTree.Node = comparison?.node ?? propertyValueComparison?.node ?? node;
	let polarity =
		propertyValueComparison !== undefined
			? propertyValueComparison.operator === "==" || propertyValueComparison.operator === "==="
			: comparison === undefined
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
			: predicateRuntimeTypeofTags(predicateType, [], environment);
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
					for (const entry of value) {
						if (isNode(entry) && !isFunctionNode(entry)) pendingPredicateNodes.push(entry);
					}
				} else if (isNode(value)) {
					if (!isFunctionNode(value)) pendingPredicateNodes.push(value);
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
