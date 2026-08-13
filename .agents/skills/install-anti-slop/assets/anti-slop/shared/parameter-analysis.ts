import type { ESTree, SourceCode } from "@oxlint/plugins";

export type Parameter = ESTree.ParamPattern;

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

function ownerName(owner: ParameterOwner): string | null {
	if (
		"id" in owner &&
		owner.id !== null &&
		owner.id !== undefined &&
		owner.id.type === "Identifier"
	)
		return owner.id.name;

	let current: ESTree.Node | null = owner.parent;
	while (current !== null && current.type !== "Program") {
		if (current.type === "VariableDeclarator" && current.id.type === "Identifier") {
			return current.id.name;
		}
		if (current.type === "Property") {
			if (current.key.type === "Identifier") return current.key.name;
			if (current.key.type === "Literal" && typeof current.key.value === "string") {
				return current.key.value;
			}
		}
		if (current.type === "MethodDefinition") {
			if (current.key.type === "Identifier") return current.key.name;
			if (current.key.type === "Literal" && typeof current.key.value === "string") {
				return current.key.value;
			}
		}
		current = current.parent;
	}
	return null;
}

export function isValidationOwner(owner: ParameterOwner): boolean {
	if (
		"returnType" in owner &&
		owner.returnType !== null &&
		owner.returnType !== undefined &&
		owner.returnType.typeAnnotation.type === "TSTypePredicate"
	)
		return true;
	const name = ownerName(owner);
	return (
		name !== null &&
    /^(?:acquire|assert|capture|check|coerce|compare|decode|encode|guard|has|inspect|is|materialize|normalize|parse|prepare|read|snapshot|subscribeRuntime|supports|validate|viewServerDecode|viewServerEncode)/iu.test(
			name,
		)
	);
}

export function hasTypedReturnContract(owner: ParameterOwner): boolean {
	if (!isRuntimeParameterOwner(owner) || owner.returnType === null || owner.returnType === undefined) {
		return false;
	}
	const returnType = owner.returnType.typeAnnotation;
	return returnType.type !== "TSUnknownKeyword" && returnType.type !== "TSVoidKeyword";
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
