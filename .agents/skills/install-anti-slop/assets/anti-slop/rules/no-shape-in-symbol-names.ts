import { defineRule } from "@oxlint/plugins";
import type { ESTree } from "@oxlint/plugins";

const FORBIDDEN_SYMBOL_NAME = "shape";

function containsForbiddenSymbolName(name: string): boolean {
	return new RegExp(`\\b${FORBIDDEN_SYMBOL_NAME}\\b`, "iu").test(name);
}

/** Ban the case-insensitive whole word "shape" in locally declared symbol names. */
export const noForbiddenTermInSymbolNamesRule = defineRule({
  meta: {
    type: "problem",
		docs: {
			description:
				'Disallow the case-insensitive whole word "shape" in locally declared JavaScript and TypeScript symbol names.',
    },
    messages: {
      forbiddenSymbolName:
        'Do not use the case-insensitive whole word "shape" in locally declared symbol names (found "{{name}}").',
    },
	},
	create(context) {
		const reportForbiddenSymbolName = (node: ESTree.Node & { name: string }) => {
			if (!containsForbiddenSymbolName(node.name)) return;
      context.report({
        node,
        messageId: "forbiddenSymbolName",
        data: { name: node.name },
			});
		};
		const isDeclarationIdentifier = (node: ESTree.Node & { name: string }): boolean => {
			const parent = node.parent;
			if (parent === null) return false;
			if (
				parent.type === "ImportSpecifier" ||
				parent.type === "ImportDefaultSpecifier" ||
				parent.type === "ImportNamespaceSpecifier" ||
				parent.type === "ExportSpecifier"
			)
				return false;
			if (
				(parent.type === "ClassDeclaration" ||
					parent.type === "ClassExpression" ||
					parent.type === "FunctionDeclaration" ||
					parent.type === "FunctionExpression" ||
					parent.type === "TSEnumDeclaration" ||
					parent.type === "TSInterfaceDeclaration" ||
					parent.type === "TSTypeAliasDeclaration") &&
				parent.id === node
			)
				return true;
			if (parent.type === "TSTypeParameter") return parent.name === node;
			if (parent.type === "VariableDeclarator" && parent.id === node) return true;
			return context.sourceCode.getDeclaredVariables(node).length > 0;
		};

		return {
			Identifier(node) {
				if (isDeclarationIdentifier(node)) reportForbiddenSymbolName(node);
			},
			PrivateIdentifier(node) {
				if (
					(node.parent.type === "MethodDefinition" || node.parent.type === "PropertyDefinition") &&
					node.parent.key === node
				)
					reportForbiddenSymbolName(node);
			},
		};
  },
});
