import { defineRule } from "@oxlint/plugins";

import {
	classifyUnsafeDictionary,
	classifyUnsafeDictionaryValue,
	createTypeEnvironment,
	type TypeEnvironment,
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

function typeReferenceName(type: ESTree.TSTypeReference): string | null {
	return type.typeName.type === "Identifier" ? type.typeName.name : null;
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
			statement.type === "ExportNamedDeclaration" &&
			statement.specifiers.some(
				(specifier) =>
					specifier.type === "ExportSpecifier" &&
					specifier.local.type === "Identifier" &&
					specifier.local.name === declaration.id.name,
			),
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

function isPlainAliasConsumerUse(node: ESTree.TSType, environment: TypeEnvironment): boolean {
	if (node.type !== "TSTypeReference" || node.typeArguments?.params.length) return false;
	const name = typeReferenceName(node);
	return (
		name !== null &&
		environment.aliases.has(name) &&
		node.parent.type !== "TSTypeAliasDeclaration"
	);
}

function shouldReportType(node: ESTree.TSType, environment: TypeEnvironment): boolean {
	if (!isDirectExportedDictionaryContractType(node)) return false;
	if (isPlainAliasConsumerUse(node, environment)) return false;
	if (classifyUnsafeDictionary(node, environment) === null) return false;
	let current: ESTree.Node | null = node.parent;
	while (current !== null && current.type !== "Program") {
		if (isTypeNode(current) && classifyUnsafeDictionary(current, environment) !== null)
			return false;
		current = current.parent;
	}
	return true;
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
				"This object dictionary's direct value type is an unsafe {{value}} escape hatch. Replace it with a concrete owner/schema-derived value type and parse external data at its boundary.",
		},
	},
	createOnce(context) {
		let environment: TypeEnvironment | null = null;
		const report = (node: ESTree.Node, value: string) => {
			context.report({ node, messageId: "unsafeDictionary", data: { value } });
		};
		const reportIfUnsafe = (node: ESTree.TSType) => {
			if (environment === null || !shouldReportType(node, environment)) return;
			const unsafe = classifyUnsafeDictionary(node, environment);
			if (unsafe === null) return;
			report(node, unsafe.unsafeValue);
		};

		return {
			Program(node) {
				environment = createTypeEnvironment(node);
			},
			TSTypeReference: reportIfUnsafe,
			TSTypeLiteral: reportIfUnsafe,
			TSMappedType: reportIfUnsafe,
			TSIndexSignature(node) {
				if (
					environment === null ||
					!isDirectExportedDictionaryContractType(node) ||
					node.typeAnnotation === null ||
					node.parent.type === "TSTypeLiteral"
				)
					return;
				const unsafe = classifyUnsafeDictionaryValue(
					node.typeAnnotation.typeAnnotation,
					environment,
				);
				if (unsafe !== null) report(node, unsafe.unsafeValue);
			},
		};
	},
});
