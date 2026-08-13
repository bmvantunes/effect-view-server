import { defineRule } from "@oxlint/plugins";

import type { ESTree } from "@oxlint/plugins";
import { isValidationOwner } from "../shared/parameter-analysis.ts";

type FunctionOwner = ESTree.ArrowFunctionExpression | ESTree.Function;

const typeofTags = new Set([
	"bigint",
	"boolean",
	"function",
	"number",
	"object",
	"string",
	"symbol",
]);

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

function isValidationFunction(owner: FunctionOwner | null): boolean {
	return owner !== null && isValidationOwner(owner);
}

function isTypeofComparison(node: ESTree.UnaryExpression): boolean {
	const parent = node.parent;
	if (parent.type !== "BinaryExpression" || !["==", "===", "!=", "!=="].includes(parent.operator))
		return false;
	const other = parent.left === node ? parent.right : parent.left;
	return (
		(other.type === "Literal" && typeof other.value === "string" && typeofTags.has(other.value)) ||
		(other.type === "UnaryExpression" && other.operator === "typeof")
	);
}

function isTypeofObservation(node: ESTree.UnaryExpression): boolean {
	const parent = node.parent;
	if (parent.type === "Property" && parent.value === node) return true;
	if (parent.type === "ArrayExpression") return true;
	if (parent.type !== "CallExpression") return false;
	return parent.callee.type === "Identifier" && parent.callee.name === "expect";
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
    return {
      UnaryExpression(node) {
		if (
				node.operator !== "typeof" ||
				isTypeofComparison(node) ||
				isTypeofObservation(node) ||
				isValidationFunction(enclosingFunction(node))
			)
				return;
			context.report({ node, messageId: "runtimeTypeof" });
      },
    };
  },
});
