import { defineRule } from "@oxlint/plugins";

import type { ESTree, Scope, SourceCode, Variable } from "@oxlint/plugins";
import {
	isValidationCallParameter,
	isValidationParameter,
	parameterAnnotation,
	parameterBindingNames,
	resolvedVariable,
} from "../shared/parameter-analysis.ts";
import {
	createTypeEnvironment,
	typeResolvesToBroadType,
	type BroadType,
	type TypeEnvironment,
} from "../shared/dictionary-types.ts";

type FunctionOwner = ESTree.ArrowFunctionExpression | ESTree.Function;
type IdentifierNode = Extract<ESTree.Node, { type: "Identifier"; name: string }>;

function enclosingFunction(node: ESTree.Node): FunctionOwner | null {
	let current: ESTree.Node | null = node.parent;
	while (current !== null && current.type !== "Program") {
		if (
			current.type === "ArrowFunctionExpression" ||
			current.type === "FunctionDeclaration" ||
			current.type === "FunctionExpression"
		)
			return current;
		current = current.parent;
	}
	return null;
}

function shouldReportParameterTypeof(
	node: ESTree.UnaryExpression,
	owner: FunctionOwner | null,
	sourceCode: SourceCode,
	environment: TypeEnvironment,
	broadVariables: ReadonlyMap<Variable, BroadType>,
): boolean {
	if (owner === null) {
		if (isGlobalThisMemberExpression(node.argument, sourceCode)) return false;
		const identifier = baseIdentifierForTypeofOperand(node.argument);
		if (identifier !== null) {
			const variable = resolvedVariable(sourceCode, identifier);
			return variable !== undefined && broadVariables.has(variable);
		}
		return true;
	}
	const parameter = parameterForTypeofOperand(node.argument, owner, sourceCode);
	if (parameter === undefined) return false;
	const annotation = parameterAnnotation(parameter);
	const type = annotation?.typeAnnotation;
	if (
		type === undefined ||
		(!typeResolvesToBroadType(type, environment, "unknown") &&
			!typeResolvesToBroadType(type, environment, "object"))
	) {
		return false;
	}
	return !isRuntimeValidationParameter(owner, parameter, sourceCode, environment);
}

function isTypePredicateOwner(owner: FunctionOwner): boolean {
	return (
		"returnType" in owner &&
		owner.returnType !== null &&
		owner.returnType !== undefined &&
		owner.returnType.typeAnnotation.type === "TSTypePredicate"
	);
}

function hasBooleanReturnContract(owner: FunctionOwner): boolean {
	if (owner.returnType === null || owner.returnType === undefined) return false;
	const type = owner.returnType.typeAnnotation;
	return type.type === "TSBooleanKeyword" || type.type === "TSTypePredicate";
}

function hasMeaningfulReturnContract(owner: FunctionOwner): boolean {
	if (owner.returnType === null || owner.returnType === undefined) return false;
	const isMeaningful = (type: ESTree.TSType): boolean => {
		switch (type.type) {
			case "TSAnyKeyword":
			case "TSUnknownKeyword":
			case "TSVoidKeyword":
			case "TSUndefinedKeyword":
			case "TSNullKeyword":
			case "TSStringKeyword":
				return false;
			case "TSUnionType":
				return type.types.some(isMeaningful);
			case "TSParenthesizedType":
				return isMeaningful(type.typeAnnotation);
			default:
				return true;
		}
	};
	return isMeaningful(owner.returnType.typeAnnotation);
}

function isRuntimeValidationParameter(
	owner: FunctionOwner,
	parameter: ESTree.ParamPattern,
	sourceCode: SourceCode,
	environment: TypeEnvironment,
): boolean {
	if (!isValidationParameter(owner, parameter, sourceCode, environment)) return false;
	if (isTypePredicateOwner(owner) || hasBooleanReturnContract(owner)) return true;
	return (
		isValidationCallParameter(owner, parameter, sourceCode, environment) ||
		hasMeaningfulReturnContract(owner)
	);
}

function parameterForIdentifier(
	identifier: IdentifierNode,
	owner: FunctionOwner,
	sourceCode: SourceCode,
): ESTree.ParamPattern | undefined {
	const argumentVariable = resolvedVariable(sourceCode, identifier);
	return owner.params.find(
		(candidate) =>
			Array.from(parameterBindingNames(candidate)).some((name) => {
				let scope: Scope | null = sourceCode.getScope(owner);
				while (scope !== null) {
					const variable = scope.set.get(name);
					if (variable !== undefined) return variable === argumentVariable;
					scope = scope.upper;
				}
				return false;
			}),
	);
}

function unwrapExpression(expression: ESTree.Expression): ESTree.Expression {
	let current = expression;
	while (
		current.type === "ParenthesizedExpression" ||
		current.type === "ChainExpression" ||
		current.type === "TSAsExpression" ||
		current.type === "TSTypeAssertion" ||
		current.type === "TSNonNullExpression" ||
		current.type === "TSSatisfiesExpression"
	) {
		current = current.expression;
	}
	return current;
}

function baseIdentifierForTypeofOperand(expression: ESTree.Expression): IdentifierNode | null {
	let current: ESTree.Expression | ESTree.Super = unwrapExpression(expression);
	while (current.type === "MemberExpression") {
		current = current.object;
		if (current.type === "Super") return null;
		current = unwrapExpression(current);
	}
	return current.type === "Identifier" ? current : null;
}

function identifiersForTypeofOperand(expression: ESTree.Expression): ReadonlyArray<IdentifierNode> {
	const unwrapped = unwrapExpression(expression);
	if (unwrapped.type === "Identifier") return [unwrapped];
	if (unwrapped.type === "MemberExpression" && unwrapped.object.type !== "Super") {
		return identifiersForTypeofOperand(unwrapped.object);
	}
	return [];
}

function parameterForTypeofOperand(
	expression: ESTree.Expression,
	owner: FunctionOwner,
	sourceCode: SourceCode,
): ESTree.ParamPattern | undefined {
	const allowNameFallback = expression.type !== "Identifier";
	for (const identifier of identifiersForTypeofOperand(expression)) {
		const parameter = parameterForIdentifier(identifier, owner, sourceCode);
		if (parameter !== undefined) return parameter;
		if (allowNameFallback) {
			const namedParameter = owner.params.find((candidate) =>
				parameterBindingNames(candidate).has(identifier.name),
			);
			if (namedParameter !== undefined) return namedParameter;
		}
	}
	return undefined;
}

function isGlobalThisMemberExpression(expression: ESTree.Expression, sourceCode: SourceCode): boolean {
	const unwrapped = unwrapExpression(expression);
	const identifier =
		unwrapped.type === "MemberExpression" ? baseIdentifierForTypeofOperand(unwrapped) : null;
	return identifier?.name === "globalThis" && resolvedVariable(sourceCode, identifier) === undefined;
}

/** Disallow runtime typeof checks that narrow unparsed values instead of decoding them. */
export const noRuntimeTypeofRule = defineRule({
  meta: {
    type: "problem",
    docs: {
      description:
        "Disallow broad runtime typeof checks outside validation and type-guard functions; external values must be decoded into meaningful types at their I/O boundary.",
    },
    messages: {
      runtimeTypeof:
        "A runtime `typeof` check only narrows an unparsed representation; it does not establish the expected contract. Parse the value into a strongly typed domain type at the earliest possible point, as close as possible to the I/O boundary where the data originated.",
    },
  },
  create(context) {
		let environment: TypeEnvironment | null = null;
		const broadVariables = new Map<Variable, BroadType>();

		const variableForName = (node: ESTree.Node, name: string): Variable | undefined => {
			let scope: Scope | null = context.sourceCode.getScope(node);
			while (scope !== null) {
				const variable = scope.set.get(name);
				if (variable !== undefined) return variable;
				scope = scope.upper;
			}
			return undefined;
		};

		const broadTypeFromExpression = (expression: ESTree.Expression): BroadType | null => {
			const unwrapped = unwrapExpression(expression);
			if (unwrapped.type !== "Identifier") return null;
			const variable = resolvedVariable(context.sourceCode, unwrapped);
			return variable === undefined ? null : broadVariables.get(variable) ?? null;
		};

		const registerFunctionParameters = (node: FunctionOwner) => {
			if (environment === null) return;
			for (const parameter of node.params) {
				const annotation = parameterAnnotation(parameter);
				if (annotation === null || annotation === undefined) continue;
				const broadType = typeResolvesToBroadType(annotation.typeAnnotation, environment, "unknown")
					? "unknown"
					: typeResolvesToBroadType(annotation.typeAnnotation, environment, "object")
						? "object"
						: null;
				if (broadType === null) continue;
				for (const name of parameterBindingNames(parameter)) {
					const variable = variableForName(node, name);
					if (variable !== undefined) broadVariables.set(variable, broadType);
				}
			}
		};

		return {
			Program(node) {
				environment = createTypeEnvironment(node);
			},
			ArrowFunctionExpression: registerFunctionParameters,
			FunctionDeclaration: registerFunctionParameters,
			FunctionExpression: registerFunctionParameters,
			VariableDeclarator(node) {
				if (environment === null || node.id.type !== "Identifier") return;
				const variable = resolvedVariable(context.sourceCode, node.id);
				if (variable === undefined) return;
				const annotation = node.id.typeAnnotation;
				const broadType =
					annotation !== null &&
					annotation !== undefined &&
					typeResolvesToBroadType(annotation.typeAnnotation, environment, "unknown")
						? "unknown"
						: annotation !== null &&
								annotation !== undefined &&
								typeResolvesToBroadType(annotation.typeAnnotation, environment, "object")
							? "object"
							: node.init === null
								? null
								: broadTypeFromExpression(node.init);
				if (broadType !== null) broadVariables.set(variable, broadType);
			},
			UnaryExpression(node) {
				if (node.operator !== "typeof")
					return;
				const owner = enclosingFunction(node);
				const identifier = baseIdentifierForTypeofOperand(node.argument);
				if (owner !== null) {
					const parameter = parameterForTypeofOperand(node.argument, owner, context.sourceCode);
					if (parameter !== undefined) {
						if (
							environment !== null &&
							shouldReportParameterTypeof(
								node,
								owner,
								context.sourceCode,
								environment,
								broadVariables,
							)
						) {
							context.report({ node, messageId: "runtimeTypeof" });
						}
						return;
					}
				}
				if (
					environment !== null &&
					shouldReportParameterTypeof(node, owner, context.sourceCode, environment, broadVariables)
				) {
					context.report({ node, messageId: "runtimeTypeof" });
					return;
				}
				if (identifier === null) return;
				const variable = resolvedVariable(context.sourceCode, identifier);
				if (variable !== undefined && broadVariables.has(variable))
					context.report({ node, messageId: "runtimeTypeof" });
			},
		};
  },
});
