import { defineRule } from "@oxlint/plugins";
import type { ESTree, Scope, SourceCode, Variable } from "@oxlint/plugins";

type IdentifierNode = Extract<ESTree.Node, { type: "Identifier"; name: string }>;

function unwrapExpression(node: ESTree.Expression): ESTree.Expression {
	let current = node;
	while (true) {
		switch (current.type) {
			case "ParenthesizedExpression":
			case "TSAsExpression":
			case "TSSatisfiesExpression":
			case "TSTypeAssertion":
			case "TSNonNullExpression":
				current = current.expression;
				continue;
			default:
				return current;
		}
	}
}

function isEmptyObjectExpression(
	node: ESTree.Expression,
	emptyObjectVariables: ReadonlySet<Variable>,
	sourceCode: SourceCode,
): boolean {
	const expression = unwrapExpression(node);
	const variable = expression.type === "Identifier" ? resolveVariable(sourceCode, expression) : undefined;
	return (
		(expression.type === "ObjectExpression" && expression.properties.length === 0) ||
		(expression.type === "Identifier" &&
			variable !== undefined &&
			emptyObjectVariables.has(variable))
	);
}

function isConditionalEmptyObjectSpread(
	node: ESTree.Expression,
	emptyObjectVariables: ReadonlySet<Variable>,
	sourceCode: SourceCode,
): boolean {
	const conditional = unwrapExpression(node);
	return (
		conditional.type === "ConditionalExpression" &&
		(isEmptyObjectExpression(conditional.consequent, emptyObjectVariables, sourceCode) ||
			isEmptyObjectExpression(conditional.alternate, emptyObjectVariables, sourceCode))
	);
}

function resolveVariable(sourceCode: SourceCode, node: IdentifierNode): Variable | undefined {
	let scope: Scope | null = sourceCode.getScope(node);
	while (scope !== null) {
		const variable = scope.set.get(node.name);
		if (variable !== undefined) return variable;
		scope = scope.upper;
	}
	return undefined;
}

function mutationTargetVariable(
	node: ESTree.Node,
	sourceCode: SourceCode,
): Variable | undefined {
	if (node.type === "Identifier") return resolveVariable(sourceCode, node);
	if (node.type === "MemberExpression" && node.object.type === "Identifier") {
		return resolveVariable(sourceCode, node.object);
	}
	return undefined;
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
			const emptyObjectVariables = new Set<Variable>();
			const emptyObjectAliases = new Map<Variable, Set<Variable>>();
			const invalidateMutatedEmptyObjects = (target: ESTree.Node): void => {
				const variable = mutationTargetVariable(target, context.sourceCode);
				if (variable === undefined) return;
				const aliases = emptyObjectAliases.get(variable);
				if (aliases === undefined) return;
				for (const alias of aliases) {
					emptyObjectVariables.delete(alias);
					emptyObjectAliases.delete(alias);
				}
			};
			const registerEmptyObjectVariable = (variable: Variable, init: ESTree.Expression): void => {
				const expression = unwrapExpression(init);
				if (expression.type === "Identifier") {
					const sourceVariable = resolveVariable(context.sourceCode, expression);
					const aliases = sourceVariable === undefined ? undefined : emptyObjectAliases.get(sourceVariable);
					if (aliases !== undefined) {
						aliases.add(variable);
						emptyObjectAliases.set(variable, aliases);
						emptyObjectVariables.add(variable);
						return;
					}
				}
				emptyObjectAliases.set(variable, new Set([variable]));
				emptyObjectVariables.add(variable);
			};

		return {
			AssignmentExpression(node) {
				invalidateMutatedEmptyObjects(node.left);
			},
			UpdateExpression(node) {
				invalidateMutatedEmptyObjects(node.argument);
			},
			CallExpression() {
				// A call may mutate an alias directly, through Object.assign/defineProperty,
				// or through a closure. Do not retain an empty-object fact across it.
				emptyObjectVariables.clear();
				emptyObjectAliases.clear();
			},
			NewExpression() {
				// Constructors can mutate arguments or shared state before the next spread.
				emptyObjectVariables.clear();
				emptyObjectAliases.clear();
			},
			UnaryExpression(node) {
				if (node.operator === "delete") invalidateMutatedEmptyObjects(node.argument);
			},
			VariableDeclarator(node) {
        if (
          node.parent.type !== "VariableDeclaration" ||
          node.parent.kind !== "const" ||
				node.id.type !== "Identifier" ||
					node.init === null ||
				!isEmptyObjectExpression(node.init, emptyObjectVariables, context.sourceCode)
				)
					return;
				const variable = resolveVariable(context.sourceCode, node.id);
				if (variable !== undefined) registerEmptyObjectVariable(variable, node.init);
			},
			SpreadElement(node) {
				if (node.parent.type !== "ObjectExpression") return;

				if (isConditionalEmptyObjectSpread(node.argument, emptyObjectVariables, context.sourceCode)) {
          context.report({ node, messageId: "avoid" });
        }
      },
    };
  },
});
