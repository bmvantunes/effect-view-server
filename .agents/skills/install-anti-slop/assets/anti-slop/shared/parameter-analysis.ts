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
