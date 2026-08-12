import { defineRule } from "@oxlint/plugins";

import type { ESTree, SourceCode } from "@oxlint/plugins";

type Parameter = ESTree.ParamPattern;
type ParameterOwner =
	| ESTree.ArrowFunctionExpression
	| ESTree.Function
	| ESTree.TSCallSignatureDeclaration
	| ESTree.TSConstructSignatureDeclaration
	| ESTree.TSConstructorType
	| ESTree.TSFunctionType
	| ESTree.TSMethodSignature;

function parameterAnnotation(parameter: Parameter): ESTree.TSTypeAnnotation | null | undefined {
	if (parameter.type === "TSParameterProperty") {
		return parameterAnnotation(parameter.parameter);
	}
	if (parameter.type === "RestElement") {
		return parameter.typeAnnotation ?? parameterAnnotation(parameter.argument);
	}
	if (parameter.type === "AssignmentPattern") {
		return parameter.typeAnnotation ?? parameter.left.typeAnnotation;
	}
	return parameter.typeAnnotation;
}

function parameterName(parameter: Parameter, sourceCode: SourceCode): string {
	return parameter.type === "Identifier"
		? parameter.name
		: sourceCode.getText(parameter).replace(/\s*:\s*object\s*$/u, "");
}

function lexicalTypeParameterNames(node: ESTree.Node): ReadonlySet<string> {
	const names = new Set<string>();
	let current: ESTree.Node | null = node;
	while (current !== null && current.type !== "Program") {
		if ("typeParameters" in current) {
			for (const parameter of current.typeParameters?.params ?? []) {
				names.add(parameter.name.name);
			}
		}
		if (current.type === "TSMappedType") names.add(current.key.name);
		if (current.type === "TSInferType") names.add(current.typeParameter.name.name);
		current = current.parent;
	}
	return names;
}

function isLexicalScope(node: ESTree.Node): boolean {
	return (
		node.type === "Program" ||
		node.type === "BlockStatement" ||
		node.type === "StaticBlock" ||
		node.type === "TSModuleBlock"
	);
}

function lexicalScopes(node: ESTree.Node): readonly ESTree.Node[] {
	const scopes: ESTree.Node[] = [];
	let current: ESTree.Node | null = node.parent;
	while (current !== null) {
		if (isLexicalScope(current)) scopes.push(current);
		current = current.parent;
	}
	return scopes;
}

function lexicalScope(node: ESTree.Node): ESTree.Node {
	return lexicalScopes(node)[0] ?? node;
}

/** Ban the broad object type on function inputs, including local aliases to object. */
export const noObjectParametersRule = defineRule({
	meta: {
		type: "problem",
		docs: {
			description:
				"Disallow object function parameters; inputs must use an owner-provided type and be parsed at their boundary.",
		},
		messages: {
			objectParameter:
				"Parameter `{{parameter}}` accepts the broad `object` type. Use the expected owner type or decode the external input at its boundary.",
		},
	},
	create(context) {
		const aliasesByScope = new Map<ESTree.Node, Map<string, ESTree.TSType>>();
		const parameterOwners: ParameterOwner[] = [];

		const visibleAliases = (node: ESTree.Node): ReadonlyMap<string, ESTree.TSType> => {
			const aliases = new Map<string, ESTree.TSType>();
			for (const scope of lexicalScopes(node)) {
				for (const [name, type] of aliasesByScope.get(scope) ?? []) {
					if (!aliases.has(name)) aliases.set(name, type);
				}
			}
			return aliases;
		};

		const resolvesToObject = (
			type: ESTree.TSType,
			shadowedAliases: ReadonlySet<string>,
			aliases: ReadonlyMap<string, ESTree.TSType>,
			visited = new Set<string>(),
		): boolean => {
			if (type.type === "TSObjectKeyword") return true;
			if (type.type === "TSParenthesizedType")
				return resolvesToObject(type.typeAnnotation, shadowedAliases, aliases, visited);
			if (type.type === "TSUnionType") {
				return type.types.some((member) =>
					resolvesToObject(member, shadowedAliases, aliases, visited),
				);
			}
			if (
				type.type !== "TSTypeReference" ||
				type.typeName.type !== "Identifier" ||
				(type.typeArguments !== null &&
					type.typeArguments !== undefined &&
					type.typeArguments.params.length > 0) ||
				visited.has(type.typeName.name) ||
				shadowedAliases.has(type.typeName.name)
			) {
				return false;
			}
			const alias = aliases.get(type.typeName.name);
			if (alias === undefined) return false;
			const nextVisited = new Set(visited);
			nextVisited.add(type.typeName.name);
			return resolvesToObject(alias, shadowedAliases, aliases, nextVisited);
		};

		const checkParameters = (node: ParameterOwner) => {
			const shadowedAliases = lexicalTypeParameterNames(node);
			const aliases = visibleAliases(node);
			for (const parameter of node.params) {
				const annotation = parameterAnnotation(parameter);
				if (annotation === null || annotation === undefined) continue;
				if (!resolvesToObject(annotation.typeAnnotation, shadowedAliases, aliases)) continue;
				context.report({
					node: annotation.typeAnnotation,
					messageId: "objectParameter",
					data: { parameter: parameterName(parameter, context.sourceCode) },
				});
			}
		};

		return {
			"Program:exit"() {
				for (const node of parameterOwners) checkParameters(node);
			},
			TSTypeAliasDeclaration(node) {
				if (node.typeParameters !== null && node.typeParameters !== undefined) return;
				const scope = lexicalScope(node);
				const aliases = aliasesByScope.get(scope) ?? new Map<string, ESTree.TSType>();
				aliases.set(node.id.name, node.typeAnnotation);
				aliasesByScope.set(scope, aliases);
			},
			ArrowFunctionExpression: (node) => parameterOwners.push(node),
			FunctionDeclaration: (node) => parameterOwners.push(node),
			FunctionExpression: (node) => parameterOwners.push(node),
			TSCallSignatureDeclaration: (node) => parameterOwners.push(node),
			TSConstructSignatureDeclaration: (node) => parameterOwners.push(node),
			TSConstructorType: (node) => parameterOwners.push(node),
			TSDeclareFunction: (node) => parameterOwners.push(node),
			TSEmptyBodyFunctionExpression: (node) => parameterOwners.push(node),
			TSFunctionType: (node) => parameterOwners.push(node),
			TSMethodSignature: (node) => parameterOwners.push(node),
		};
	},
});
