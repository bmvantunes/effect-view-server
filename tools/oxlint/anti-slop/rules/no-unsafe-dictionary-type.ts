import { defineRule } from "@oxlint/plugins";

import {
	classifyUnsafeDictionary,
	classifyUnsafeDictionaryInterfaceValue,
	classifyUnsafeDictionaryInterfaceReference,
	classifyUnsafeDictionaryValue,
	createTypeEnvironment,
	type TypeEnvironment,
	type UnsafeDictionary,
} from "../shared/dictionary-types.ts";

import type { ESTree } from "@oxlint/plugins";

const typeNodeKinds: ReadonlySet<string> = new Set([
	"JSDocNonNullableType",
	"JSDocNullableType",
	"JSDocUnknownType",
	"TSAnyKeyword",
	"TSArrayType",
	"TSBigIntKeyword",
	"TSBooleanKeyword",
	"TSConditionalType",
	"TSConstructorType",
	"TSFunctionType",
	"TSImportType",
	"TSIndexedAccessType",
	"TSInferType",
	"TSIntersectionType",
	"TSIntrinsicKeyword",
	"TSLiteralType",
	"TSMappedType",
	"TSNamedTupleMember",
	"TSNeverKeyword",
	"TSNullKeyword",
	"TSNumberKeyword",
	"TSObjectKeyword",
	"TSParenthesizedType",
	"TSStringKeyword",
	"TSSymbolKeyword",
	"TSTemplateLiteralType",
	"TSThisType",
	"TSTupleType",
	"TSTypeLiteral",
	"TSTypeOperator",
	"TSTypePredicate",
	"TSTypeQuery",
	"TSTypeReference",
	"TSUndefinedKeyword",
	"TSUnionType",
	"TSUnknownKeyword",
	"TSVoidKeyword",
]);

function isTypeNode(node: ESTree.Node): node is ESTree.TSType {
	return typeNodeKinds.has(node.type);
}

function isExportedTypeDeclaration(
	declaration: ESTree.TSTypeAliasDeclaration | ESTree.TSInterfaceDeclaration,
): boolean {
	if (
		declaration.parent.type === "ExportNamedDeclaration" ||
		declaration.parent.type === "ExportDefaultDeclaration"
	)
		return true;
	let current: ESTree.Node | null = declaration.parent;
	while (current !== null && current.type !== "Program") current = current.parent;
	if (current === null || current.type !== "Program") return false;
	return current.body.some(
		(statement) =>
			(statement.type === "ExportNamedDeclaration" &&
				statement.source === null &&
				statement.specifiers.some(
					(specifier) =>
						specifier.type === "ExportSpecifier" &&
						specifier.local.type === "Identifier" &&
						specifier.local.name === declaration.id.name,
				)) ||
			(statement.type === "TSExportAssignment" &&
				statement.expression.type === "Identifier" &&
				statement.expression.name === declaration.id.name),
	);
}

function isDirectExportedDictionaryContractType(node: ESTree.Node): boolean {
	let current: ESTree.Node | null = node.parent;
	while (current !== null && current.type !== "Program") {
		if (
			current.type === "TSTypePredicate" ||
			current.type === "TSTypeParameter" ||
			current.type === "TSPropertySignature" ||
			current.type === "TSMethodSignature" ||
			current.type === "TSCallSignatureDeclaration" ||
			current.type === "TSConstructSignatureDeclaration" ||
			current.type === "TSFunctionType" ||
			current.type === "TSConstructorType"
		)
			return false;
		if (current.type === "TSTypeAliasDeclaration" || current.type === "TSInterfaceDeclaration") {
			return isExportedTypeDeclaration(current);
		}
		current = current.parent;
	}
	return false;
}

function directUnsafeDictionary(
	node: ESTree.TSType,
	classify: (node: ESTree.TSType) => UnsafeDictionary | null,
): UnsafeDictionary | null {
	if (!isDirectExportedDictionaryContractType(node)) return null;
	const unsafe = classify(node);
	if (unsafe === null) return null;
	let current: ESTree.Node | null = node.parent;
	while (current !== null && current.type !== "Program") {
		if (isTypeNode(current) && classify(current) !== null) return null;
		current = current.parent;
	}
	return unsafe;
}

function exportedContractName(node: ESTree.Node): string {
	let current: ESTree.Node | null = node.parent;
	while (current !== null && current.type !== "Program") {
		if (current.type === "TSTypeAliasDeclaration" || current.type === "TSInterfaceDeclaration")
			return current.id.name;
		current = current.parent;
	}
	return "<anonymous>";
}

/** Disallow direct exported object-dictionary contracts whose value type is an unsafe escape hatch. */
export const noUnsafeDictionaryTypeRule = defineRule({
	meta: {
		type: "problem",
		docs: {
			description:
				"Disallow object-dictionary contracts whose direct value type is unknown, any, object, {}, or a union/alias containing one of those escape hatches.",
		},
		messages: {
			unsafeDictionary:
				'The exported object dictionary contract "{{name}}" has an unsafe {{value}} direct value type. Replace it with a concrete owner/schema-derived value type and parse external data at its boundary.',
		},
	},
	createOnce(context) {
		let environment: TypeEnvironment | null = null;
		let cache = new WeakMap<ESTree.TSType, UnsafeDictionary | null>();
		const classify = (node: ESTree.TSType): UnsafeDictionary | null => {
			if (cache.has(node)) return cache.get(node) ?? null;
			if (environment === null) return null;
			const unsafe = classifyUnsafeDictionary(node, environment);
			cache.set(node, unsafe);
			return unsafe;
		};
		const report = (node: ESTree.Node, value: string) => {
			context.report({
				node,
				messageId: "unsafeDictionary",
				data: { name: exportedContractName(node), value },
			});
		};
		const reportIfUnsafe = (node: ESTree.TSType) => {
			const unsafe = directUnsafeDictionary(node, classify);
			if (unsafe === null) return;
			report(node, unsafe.unsafeValue);
		};

		return {
			Program(node) {
				environment = createTypeEnvironment(node);
				cache = new WeakMap();
			},
			TSTypeReference: reportIfUnsafe,
			TSTypeLiteral: reportIfUnsafe,
			TSMappedType: reportIfUnsafe,
			TSParenthesizedType: reportIfUnsafe,
			TSUnionType: reportIfUnsafe,
			TSIntersectionType: reportIfUnsafe,
			TSInterfaceHeritage(node) {
				if (environment === null || !isDirectExportedDictionaryContractType(node)) return;
				const unsafe = classifyUnsafeDictionaryInterfaceReference(node, environment);
				if (unsafe !== null) report(node, unsafe.unsafeValue);
			},
			TSIndexSignature(node) {
				if (
					environment === null ||
					!isDirectExportedDictionaryContractType(node) ||
					node.typeAnnotation === null ||
					node.typeAnnotation === undefined ||
					node.parent.type === "TSTypeLiteral"
				)
					return;
				const declaration =
					node.parent.type === "TSInterfaceBody" &&
					node.parent.parent.type === "TSInterfaceDeclaration"
						? node.parent.parent
						: null;
				const unsafe =
					declaration === null
						? classifyUnsafeDictionaryValue(node.typeAnnotation.typeAnnotation, environment)
						: classifyUnsafeDictionaryInterfaceValue(
								declaration,
								node.typeAnnotation.typeAnnotation,
								environment,
							);
				if (unsafe !== null) report(node, unsafe.unsafeValue);
			},
		};
	},
});
