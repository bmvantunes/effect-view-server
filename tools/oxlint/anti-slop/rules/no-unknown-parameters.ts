import { defineRule } from "@oxlint/plugins";
import type { ESTree } from "@oxlint/plugins";
import {
	createTypeEnvironment,
	typeResolvesToBroadType,
	typeResolvesToSchemaUnknownBoundary,
	type TypeEnvironment,
} from "../shared/dictionary-types.ts";
import {
	isValidationParameter,
	parameterAnnotation,
	parameterName,
	type ParameterOwner,
} from "../shared/parameter-analysis.ts";

/** Disallow unknown inputs except explicitly named error-cause enrichment. */
export const noUnknownParametersRule = defineRule({
  meta: {
    type: "problem",
    docs: {
      description:
        "Disallow explicitly unknown function parameters except `cause`; decode unknown input at its I/O boundary instead.",
    },
    messages: {
      unknownParameter:
        "Parameter `{{parameter}}` accepts `unknown` without establishing its contract. Define the expected schema or parser so the value becomes a strongly typed domain type at the earliest possible point, as close as possible to the I/O boundary where the data originated.",
    },
	},
	create(context) {
		let environment: TypeEnvironment | null = null;
		const checkParameters = (node: ParameterOwner) => {
			if (environment === null) return;
      for (const parameter of node.params) {
        const annotation = parameterAnnotation(parameter);
        if (
				annotation === null ||
				annotation === undefined ||
				!typeResolvesToBroadType(annotation.typeAnnotation, environment, "unknown") ||
				typeResolvesToSchemaUnknownBoundary(annotation.typeAnnotation, environment)
			)
			continue;
        const name = parameterName(parameter, context.sourceCode);
        if (name === "cause") continue;
		if (isValidationParameter(node, parameter, context.sourceCode, environment)) continue;
        context.report({
          node: annotation.typeAnnotation,
          messageId: "unknownParameter",
          data: { parameter: name },
        });
      }
		};

		return {
			Program(node: ESTree.Program) {
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
