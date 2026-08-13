import { defineRule } from "@oxlint/plugins";
import type { ESTree } from "@oxlint/plugins";

function unwrapParentheses(node: ESTree.Expression): ESTree.Expression {
  let current = node;
  while (current.type === "ParenthesizedExpression") {
    current = current.expression;
  }
  return current;
}

function isEmptyObjectExpression(node: ESTree.Expression): boolean {
  return node.type === "ObjectExpression" && node.properties.length === 0;
}

function isNullishComparison(node: ESTree.Expression): boolean {
	if (node.type !== "BinaryExpression" || !["==", "===", "!=", "!=="].includes(node.operator))
		return false;
	return [node.left, node.right].some(
		(operand) =>
			(operand.type === "Identifier" && operand.name === "undefined") ||
			(operand.type === "Literal" && operand.value === null),
	);
}

function isSimpleOptionalCondition(node: ESTree.Expression): boolean {
	const condition = unwrapParentheses(node);
	return (
		condition.type === "Identifier" ||
		condition.type === "CallExpression" ||
		condition.type === "BinaryExpression" ||
		condition.type === "UnaryExpression"
	);
}

function isConditionalEmptyObjectSpread(node: ESTree.Expression): boolean {
  const conditional = unwrapParentheses(node);
  return (
    conditional.type === "ConditionalExpression" &&
		!isNullishComparison(conditional.test) &&
		!isSimpleOptionalCondition(conditional.test) &&
    (isEmptyObjectExpression(conditional.consequent) ||
      isEmptyObjectExpression(conditional.alternate))
  );
}

/** Ban conditional empty-object spreads without changing their omission semantics. */
export const noConditionalEmptyObjectSpreadRule = defineRule({
  meta: {
    type: "suggestion",
    docs: {
      description:
        "Disallow object spreads that conditionally spread an empty object to omit fields.",
    },
    messages: {
      avoid:
        "Do not use conditional empty-object spreads. Prefer a direct property or build the object in separate statements.",
    },
  },
  create(context) {
    return {
      SpreadElement(node) {
        if (node.parent.type !== "ObjectExpression") return;

        if (isConditionalEmptyObjectSpread(node.argument)) {
          context.report({ node, messageId: "avoid" });
        }
      },
    };
  },
});
