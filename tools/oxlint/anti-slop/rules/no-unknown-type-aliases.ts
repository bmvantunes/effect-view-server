import { defineRule } from "@oxlint/plugins";

import type { ESTree } from "@oxlint/plugins";

type AliasDeclaration = {
	readonly name: string;
	readonly type: ESTree.TSType;
	readonly scope: ESTree.Node;
	readonly node: ESTree.TSTypeAliasDeclaration;
};

function referencedAliasName(type: ESTree.TSType): string | null {
	if (type.type === "TSParenthesizedType") return referencedAliasName(type.typeAnnotation);
	if (type.type !== "TSTypeReference" || type.typeName.type !== "Identifier") return null;
	return type.typeArguments === null ||
		type.typeArguments === undefined ||
		type.typeArguments.params.length === 0
		? type.typeName.name
		: null;
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

function visibleAliases(
	node: ESTree.Node,
	aliases: readonly AliasDeclaration[],
): ReadonlyMap<string, ESTree.TSType> {
	const visible = new Map<string, ESTree.TSType>();
	for (const scope of lexicalScopes(node)) {
		for (const alias of aliases) {
			if (alias.scope === scope && !visible.has(alias.name)) {
				visible.set(alias.name, alias.type);
			}
		}
	}
	return visible;
}

/** Ban named aliases that merely conceal TypeScript's unknown top type. */
export const noUnknownTypeAliasesRule = defineRule({
	meta: {
		type: "problem",
		docs: {
			description:
				"Disallow type aliases whose resolved type is unknown; unknown must remain visible at an allowed boundary.",
		},
		messages: {
			unknownAlias:
				"Type alias `{{alias}}` only renames `unknown`. Keep `unknown` explicit on an allowed `cause` field or replace it with the parsed owner type.",
		},
	},
	create(context) {
		const aliases: AliasDeclaration[] = [];

		const resolvesToUnknown = (
			type: ESTree.TSType,
			visible: ReadonlyMap<string, ESTree.TSType>,
			visited = new Set<string>(),
		): boolean => {
			if (type.type === "TSUnknownKeyword") return true;
			if (type.type === "TSParenthesizedType") {
				return resolvesToUnknown(type.typeAnnotation, visible, visited);
			}
			if (type.type === "TSUnionType") {
				return type.types.some((member) => resolvesToUnknown(member, visible, visited));
			}
			const name = referencedAliasName(type);
			if (name === null || visited.has(name)) return false;
			const alias = visible.get(name);
			if (alias === undefined) return false;
			const nextVisited = new Set(visited);
			nextVisited.add(name);
			return resolvesToUnknown(alias, visible, nextVisited);
		};

		return {
			TSTypeAliasDeclaration(node) {
				if (node.typeParameters !== null && node.typeParameters !== undefined) return;
				aliases.push({
					name: node.id.name,
					type: node.typeAnnotation,
					scope: lexicalScopes(node)[0] ?? node,
					node,
				});
			},
			"Program:exit"() {
				for (const alias of aliases) {
					const visible = visibleAliases(alias.node, aliases);
					if (!resolvesToUnknown(alias.type, visible, new Set([alias.name]))) continue;
					context.report({
						node: alias.node.id,
						messageId: "unknownAlias",
						data: { alias: alias.name },
					});
				}
			},
		};
	},
});
