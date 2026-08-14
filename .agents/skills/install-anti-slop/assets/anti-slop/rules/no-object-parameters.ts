import { defineRule } from "@oxlint/plugins";

import type { ESTree } from "@oxlint/plugins";

import {
	createTypeEnvironment,
	typeResolvesToBroadType,
	type TypeEnvironment,
} from "../shared/dictionary-types.ts";

import {
	isValidationParameter,
	parameterAnnotation,
	parameterName,
	type Parameter,
	type ParameterOwner,
} from "../shared/parameter-analysis.ts";

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
		let environment: TypeEnvironment | null = null;

		const checkParameters = (node: ParameterOwner) => {
			if (environment === null) return;
			const shadowedAliases = lexicalTypeParameterNames(node);
			for (const parameter of node.params) {
				const annotation = parameterAnnotation(parameter);
				if (annotation === null || annotation === undefined) continue;
				if (!typeResolvesToBroadType(annotation.typeAnnotation, environment, "object", shadowedAliases)) continue;
				if (isValidationParameter(node, parameter, context.sourceCode)) continue;
				context.report({
					node: annotation.typeAnnotation,
					messageId: "objectParameter",
					data: { parameter: parameterName(parameter, context.sourceCode) },
				});
			}
		};

		return {
			Program(node) {
				environment = createTypeEnvironment(node);
			},
			ArrowFunctionExpression: checkParameters,
			FunctionDeclaration: checkParameters,
			FunctionExpression: checkParameters,
			TSCallSignatureDeclaration: checkParameters,
			TSConstructSignatureDeclaration: checkParameters,
			TSConstructorType: checkParameters,
			TSDeclareFunction: checkParameters,
			TSEmptyBodyFunctionExpression: checkParameters,
			TSFunctionType: checkParameters,
			TSMethodSignature: checkParameters,
		};
	},
});
