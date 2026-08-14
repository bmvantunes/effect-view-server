import type { ESTree } from "@oxlint/plugins";

const BUILT_INS = new Set([
	"Record",
	"Readonly",
	"Partial",
	"Required",
	"Pick",
	"Omit",
	"PropertyKey",
	"NonNullable",
]);
const TRANSPARENT_WRAPPERS = new Set(["Readonly", "Partial", "Required", "NonNullable"]);

type TypeAliasEnvironment = ReadonlyMap<string, ESTree.TSType>;

type ImportedTypeBinding = {
	readonly local: string;
	readonly source: string;
	readonly imported: string;
	readonly scope: readonly ESTree.Node[];
};

const trustedBroadTypeImports = new Map<string, ReadonlySet<string>>();

type ResolvedType = {
	readonly type: ESTree.TSType;
	readonly substitutions: TypeAliasEnvironment;
};

export type UnsafeDictionary = {
	readonly kind: "unsafe-dictionary";
	readonly unsafeValue: "any" | "empty-object" | "object" | "union" | "unknown";
};

export type WideningTargetKind =
	| "anonymous object"
	| "generic container"
	| "object"
	| "open dictionary"
	| "unknown";

export type WideningTarget = {
	readonly kind: WideningTargetKind;
};

export type TypeEnvironment = {
	readonly aliases: ReadonlyMap<string, readonly ESTree.TSTypeAliasDeclaration[]>;
	readonly interfaces: ReadonlyMap<string, readonly ESTree.TSInterfaceDeclaration[]>;
	readonly shadowedBuiltIns: ReadonlyMap<string, readonly ESTree.Node[]>;
	readonly resolveAlias: (
		reference: ESTree.Node,
		name: string,
	) => ESTree.TSTypeAliasDeclaration | undefined;
	readonly resolveInterfaces: (
		reference: ESTree.Node,
		name: string,
	) => readonly ESTree.TSInterfaceDeclaration[];
	readonly resolveImportedType: (
		reference: ESTree.Node,
		name: string,
	) => ImportedTypeBinding | undefined;
};

export type BroadType = "unknown" | "object";

export type RuntimeTypeofTag =
	| "bigint"
	| "boolean"
	| "function"
	| "number"
	| "object"
	| "string"
	| "symbol"
	| "undefined";

function isNode(value: unknown): value is ESTree.Node {
	return typeof value === "object" && value !== null && "type" in value && typeof value.type === "string";
}

function isLexicalScope(node: ESTree.Node): boolean {
	return (
		node.type === "Program" ||
		node.type === "BlockStatement" ||
		node.type === "CatchClause" ||
		node.type === "ForStatement" ||
		node.type === "ForInStatement" ||
		node.type === "ForOfStatement" ||
		node.type === "SwitchStatement" ||
		node.type === "TSModuleBlock" ||
		node.type === "FunctionDeclaration" ||
		node.type === "FunctionExpression" ||
		node.type === "ArrowFunctionExpression"
	);
}

function lexicalScopeChain(node: ESTree.Node): readonly ESTree.Node[] {
	const scopes: ESTree.Node[] = [];
	let current: ESTree.Node | null | undefined = node;
	while (current !== null && current !== undefined) {
		if (isLexicalScope(current)) scopes.unshift(current);
		current = current.parent;
	}
	return scopes;
}

type ScopedNode = {
	readonly node: ESTree.Node;
	readonly scope: readonly ESTree.Node[];
};

function isScopePrefix(
	candidate: readonly ESTree.Node[],
	actual: readonly ESTree.Node[],
): boolean {
	return candidate.length <= actual.length && candidate.every((scope, index) => scope === actual[index]);
}

function visibleDeclarations<T extends ESTree.Node>(
	declarations: readonly (ScopedNode & { readonly node: T })[],
	reference: ESTree.Node,
): readonly T[] {
	const referenceScope = lexicalScopeChain(reference);
	let nearestScopeLength = -1;
	const visible: T[] = [];
	for (const declaration of declarations) {
		if (!isScopePrefix(declaration.scope, referenceScope)) continue;
		if (declaration.scope.length > nearestScopeLength) {
			nearestScopeLength = declaration.scope.length;
			visible.length = 0;
		}
		if (declaration.scope.length === nearestScopeLength) visible.push(declaration.node);
	}
	return visible;
}

function collectTypeDeclarations(
	node: ESTree.Node,
	aliases: Map<string, Array<ScopedNode & { readonly node: ESTree.TSTypeAliasDeclaration }>>,
	interfaces: Map<string, Array<ScopedNode & { readonly node: ESTree.TSInterfaceDeclaration }>>,
	shadowedBuiltIns: Map<string, Array<ScopedNode>>,
	importedTypes: Map<string, ImportedTypeBinding[]>,
): void {
	const scope = lexicalScopeChain(node);
	const addShadow = (name: string, declaration: ESTree.Node): void => {
		if (!BUILT_INS.has(name)) return;
		const declarations = shadowedBuiltIns.get(name) ?? [];
		declarations.push({ node: declaration, scope: lexicalScopeChain(declaration) });
		shadowedBuiltIns.set(name, declarations);
	};

	if (node.type === "TSTypeAliasDeclaration") {
		const declarations = aliases.get(node.id.name) ?? [];
		declarations.push({ node, scope });
		aliases.set(node.id.name, declarations);
		addShadow(node.id.name, node);
	} else if (node.type === "TSInterfaceDeclaration") {
		const declarations = interfaces.get(node.id.name) ?? [];
		declarations.push({ node, scope });
		interfaces.set(node.id.name, declarations);
		addShadow(node.id.name, node);
	} else if (node.type === "TSEnumDeclaration") {
		addShadow(node.id.name, node);
	} else if (node.type === "ClassDeclaration" && node.id !== null) {
		addShadow(node.id.name, node);
	} else if (node.type === "ImportDeclaration") {
		const source = node.source.value;
		if (typeof source === "string") {
			for (const specifier of node.specifiers) {
				addShadow(specifier.local.name, specifier.local);
				const imported =
					specifier.type === "ImportSpecifier"
						? specifier.imported.type === "Identifier"
							? specifier.imported.name
							: typeof specifier.imported.value === "string"
								? specifier.imported.value
								: null
						: specifier.type === "ImportNamespaceSpecifier"
							? "*"
							: "default";
				if (imported === null) continue;
				const bindings = importedTypes.get(specifier.local.name) ?? [];
				bindings.push({
					local: specifier.local.name,
					source,
					imported,
					scope: lexicalScopeChain(specifier),
				});
				importedTypes.set(specifier.local.name, bindings);
			}
		}
	}

	for (const [key, value] of Object.entries(node)) {
		if (key === "parent") continue;
		if (Array.isArray(value)) {
			for (const entry of value) {
				if (isNode(entry)) collectTypeDeclarations(entry, aliases, interfaces, shadowedBuiltIns, importedTypes);
			}
		} else if (isNode(value)) {
			collectTypeDeclarations(value, aliases, interfaces, shadowedBuiltIns, importedTypes);
		}
	}
}

export function createTypeEnvironment(program: ESTree.Program): TypeEnvironment {
	const aliases = new Map<string, Array<ScopedNode & { readonly node: ESTree.TSTypeAliasDeclaration }>>();
	const interfaces = new Map<string, Array<ScopedNode & { readonly node: ESTree.TSInterfaceDeclaration }>>();
	const shadowedBuiltIns = new Map<string, Array<ScopedNode>>();
	const importedTypes = new Map<string, ImportedTypeBinding[]>();
	collectTypeDeclarations(program, aliases, interfaces, shadowedBuiltIns, importedTypes);

	const aliasNodes = new Map<string, readonly ESTree.TSTypeAliasDeclaration[]>();
	for (const [name, declarations] of aliases) {
		aliasNodes.set(name, declarations.map(({ node }) => node));
	}
	const interfaceNodes = new Map<string, readonly ESTree.TSInterfaceDeclaration[]>();
	for (const [name, declarations] of interfaces) {
		interfaceNodes.set(name, declarations.map(({ node }) => node));
	}
	const shadowNodes = new Map<string, readonly ESTree.Node[]>();
	for (const [name, declarations] of shadowedBuiltIns) {
		shadowNodes.set(name, declarations.map(({ node }) => node));
	}

	const resolveAlias = (reference: ESTree.Node, name: string): ESTree.TSTypeAliasDeclaration | undefined => {
		const declarations = aliases.get(name);
		if (declarations === undefined) return undefined;
		const visible = visibleDeclarations(declarations, reference);
		return visible.length === 1 ? visible[0] : undefined;
	};
	const resolveInterfaces = (
		reference: ESTree.Node,
		name: string,
	): readonly ESTree.TSInterfaceDeclaration[] => {
		const declarations = interfaces.get(name);
		return declarations === undefined ? [] : visibleDeclarations(declarations, reference);
	};
	const resolveImportedType = (
		reference: ESTree.Node,
		name: string,
	): ImportedTypeBinding | undefined => {
		const declarations = importedTypes.get(name);
		if (declarations === undefined) return undefined;
		const visible = declarations.filter((declaration) =>
			isScopePrefix(declaration.scope, lexicalScopeChain(reference)),
		);
		const nearestScopeLength = Math.max(...visible.map((declaration) => declaration.scope.length), -1);
		const nearest = visible.filter((declaration) => declaration.scope.length === nearestScopeLength);
		return nearest.length === 1 ? nearest[0] : undefined;
	};

	return {
		aliases: aliasNodes,
		interfaces: interfaceNodes,
		shadowedBuiltIns: shadowNodes,
		resolveAlias,
		resolveInterfaces,
		resolveImportedType,
	};
}

function typeReferenceName(type: ESTree.TSTypeReference): string | null {
	if (type.typeName.type === "Identifier") return type.typeName.name;
	return type.typeName.type === "TSQualifiedName" &&
		type.typeName.left.type === "Identifier" &&
		type.typeName.left.name === "globalThis" &&
		type.typeName.right.type === "Identifier"
		? type.typeName.right.name
		: null;
}

function typeReferencePath(type: ESTree.TSTypeReference): string | null {
	const parts: string[] = [];
	let current = type.typeName;
	while (current.type === "TSQualifiedName") {
		if (current.right.type !== "Identifier") return null;
		parts.unshift(current.right.name);
		current = current.left;
	}
	if (current.type !== "Identifier") return null;
	parts.unshift(current.name);
	return parts.join(".");
}

function typeQueryPath(type: ESTree.TSTypeQuery): string | null {
	const parts: string[] = [];
	let current = type.exprName;
	while (current.type === "TSQualifiedName") {
		if (current.right.type !== "Identifier") return null;
		parts.unshift(current.right.name);
		current = current.left;
	}
	if (current.type !== "Identifier") return null;
	parts.unshift(current.name);
	return parts.join(".");
}

function importedTypeResolvesToUnknown(
	type: ESTree.TSTypeReference,
	environment: TypeEnvironment,
): boolean {
	const path = typeReferencePath(type);
	if (path === null) return false;
	const direct = environment.resolveImportedType(type, path);
	if (
		direct !== undefined &&
		trustedBroadTypeImports.get(direct.source)?.has(direct.imported) === true
	)
		return true;
	const separator = path.indexOf(".");
	if (separator === -1) return false;
	const namespace = environment.resolveImportedType(type, path.slice(0, separator));
	if (namespace === undefined || namespace.imported !== "*") return false;
	return trustedBroadTypeImports.get(namespace.source)?.has(path.slice(separator + 1)) === true;
}

function schemaTypeResolvesToUnknown(
	type: ESTree.TSTypeReference,
	environment: TypeEnvironment,
): boolean {
	if (typeReferencePath(type) !== "Schema.Schema.Type") return false;
	const schema = environment.resolveImportedType(type, "Schema");
	const typeArgument = type.typeArguments?.params[0];
	return (
		schema?.source === "effect" &&
		schema.imported === "Schema" &&
		typeArgument?.type === "TSTypeQuery" &&
		typeQueryPath(typeArgument) === "Schema.Unknown"
	);
}

function isGlobalThisQualified(type: ESTree.TSTypeReference): boolean {
	return (
		type.typeName.type === "TSQualifiedName" &&
		type.typeName.left.type === "Identifier" &&
		type.typeName.left.name === "globalThis"
	);
}

function isBuiltInReference(
	type: ESTree.TSTypeReference,
	name: string,
	environment: TypeEnvironment,
): boolean {
	return BUILT_INS.has(name) &&
		(isGlobalThisQualified(type) ||
			environment.shadowedBuiltIns.get(name) === undefined ||
			visibleDeclarations(
				(environment.shadowedBuiltIns.get(name) ?? []).map((declaration) => ({
					node: declaration,
					scope: lexicalScopeChain(declaration),
				})),
				type,
			).length === 0);
}

function isUnappliedReferenceTo(type: ESTree.TSType, name: string): boolean {
	const unwrapped = unwrapTransparentType(type);
	return (
		unwrapped.type === "TSTypeReference" &&
		typeReferenceName(unwrapped) === name &&
		(unwrapped.typeArguments === null ||
			unwrapped.typeArguments === undefined ||
			unwrapped.typeArguments.params.length === 0)
	);
}

function unwrapTransparentType(type: ESTree.TSType): ESTree.TSType {
	let current = type;
	while (
		current.type === "TSParenthesizedType" ||
		(current.type === "TSTypeOperator" && current.operator === "readonly")
	) {
		current = current.typeAnnotation;
	}
	return current;
}

function isNeverType(type: ESTree.TSType): boolean {
	return unwrapTransparentType(type).type === "TSNeverKeyword";
}

function isEffectivelyEmptyMember(member: ESTree.TSSignature): boolean {
	return (
		member.type === "TSPropertySignature" &&
		member.optional === true &&
		member.typeAnnotation !== null &&
		member.typeAnnotation !== undefined &&
		isNeverType(member.typeAnnotation.typeAnnotation)
	);
}

function isEffectivelyEmptyTypeLiteral(type: ESTree.TSTypeLiteral): boolean {
	return type.members.length === 0 || type.members.every(isEffectivelyEmptyMember);
}

function isEffectivelyEmptyInterface(
	declarations: readonly ESTree.TSInterfaceDeclaration[],
): boolean {
	return declarations.length > 0 &&
		declarations.every(
			(type) =>
				type.extends.length === 0 &&
				(type.body.body.length === 0 || type.body.body.every(isEffectivelyEmptyMember)),
		);
}

function resolvedSubstitutionArgument(
	type: ESTree.TSType,
	base: TypeAliasEnvironment,
	resolving: ReadonlySet<string> = new Set(),
): ESTree.TSType {
	const unwrapped = unwrapTransparentType(type);
	if (unwrapped.type !== "TSTypeReference") return type;
	const name = typeReferenceName(unwrapped);
	if (name === null || resolving.has(name)) return type;
	const substitution = base.get(name);
	if (substitution === undefined) return type;
	const nextResolving = new Set(resolving);
	nextResolving.add(name);
	return resolvedSubstitutionArgument(substitution, base, nextResolving);
}

function aliasSubstitutionArguments(
	alias: ESTree.TSTypeAliasDeclaration,
	arguments_: readonly ESTree.TSType[],
	base: TypeAliasEnvironment,
): TypeAliasEnvironment | null {
	const parameters = alias.typeParameters?.params ?? [];
	const next = new Map(base);
	for (const [index, parameter] of parameters.entries()) {
		const argument = arguments_[index] ?? parameter.default;
		if (argument === null || argument === undefined) return null;
		next.set(parameter.name.name, resolvedSubstitutionArgument(argument, next));
	}
	return next;
}

function aliasSubstitution(
	alias: ESTree.TSTypeAliasDeclaration,
	type: ESTree.TSTypeReference,
	base: TypeAliasEnvironment,
): TypeAliasEnvironment | null {
	return aliasSubstitutionArguments(alias, type.typeArguments?.params ?? [], base);
}

function runtimePropertyType(
	type: ESTree.TSType,
	propertyName: string,
	environment: TypeEnvironment,
	substitutions: TypeAliasEnvironment,
	resolvingAliases: ReadonlySet<string>,
): ESTree.TSType | undefined {
	const unwrapped = unwrapTransparentType(type);
	const memberType = (members: readonly ESTree.TSSignature[]): ESTree.TSType | undefined => {
		for (const member of members) {
			if (member.type !== "TSPropertySignature" || member.typeAnnotation === null) continue;
			const name =
				member.key.type === "Identifier"
					? member.key.name
					: member.key.type === "Literal" && typeof member.key.value === "string"
						? member.key.value
						: undefined;
			if (name === propertyName) return member.typeAnnotation.typeAnnotation;
		}
		return undefined;
	};
	if (unwrapped.type === "TSTypeLiteral") return memberType(unwrapped.members);
	if (unwrapped.type !== "TSTypeReference") return undefined;
	const name = typeReferenceName(unwrapped);
	if (name === null || resolvingAliases.has(name)) return undefined;
	const substitution = substitutions.get(name);
	if (substitution !== undefined && !isUnappliedReferenceTo(substitution, name)) {
		return runtimePropertyType(substitution, propertyName, environment, substitutions, resolvingAliases);
	}
	const alias = environment.resolveAlias(unwrapped, name);
	if (alias !== undefined) {
		const nextSubstitutions = aliasSubstitution(alias, unwrapped, substitutions);
		if (nextSubstitutions === null) return undefined;
		const nextResolving = new Set(resolvingAliases);
		nextResolving.add(name);
		return runtimePropertyType(
			alias.typeAnnotation,
			propertyName,
			environment,
			nextSubstitutions,
			nextResolving,
		);
	}
	for (const declaration of environment.resolveInterfaces(unwrapped, name)) {
		const property = memberType(declaration.body.body);
		if (property !== undefined) return property;
	}
	return undefined;
}

function typeHasRuntimeObjectStructure(
	type: ESTree.TSType,
	environment: TypeEnvironment,
	substitutions: TypeAliasEnvironment,
	resolvingAliases: ReadonlySet<string>,
): boolean {
	const unwrapped = unwrapTransparentType(type);
	switch (unwrapped.type) {
		case "TSTypeLiteral":
			return unwrapped.members.length > 0;
		case "TSMappedType":
			return true;
		case "TSIntersectionType":
			return unwrapped.types.some((member) =>
				typeHasRuntimeObjectStructure(member, environment, substitutions, resolvingAliases),
			);
		case "TSUnionType":
			return unwrapped.types.some((member) =>
				typeHasRuntimeObjectStructure(member, environment, substitutions, resolvingAliases),
			);
		case "TSTypeReference": {
			const name = typeReferenceName(unwrapped);
			if (name === null) return false;
			const substitution = substitutions.get(name);
			if (substitution !== undefined && !isUnappliedReferenceTo(substitution, name)) {
				return typeHasRuntimeObjectStructure(
					substitution,
					environment,
					substitutions,
					resolvingAliases,
				);
			}
			if (resolvingAliases.has(name)) return false;
			const alias = environment.resolveAlias(unwrapped, name);
			if (alias !== undefined) {
				const nextSubstitutions = aliasSubstitution(alias, unwrapped, substitutions);
				if (nextSubstitutions === null) return false;
				const nextResolving = new Set(resolvingAliases);
				nextResolving.add(name);
				return typeHasRuntimeObjectStructure(
					alias.typeAnnotation,
					environment,
					nextSubstitutions,
					nextResolving,
				);
			}
			return environment.resolveInterfaces(unwrapped, name).some((declaration) =>
				declaration.body.body.length > 0,
			);
		}
		default:
			return false;
	}
}

export function typeRequiresStructuralRuntimeEvidence(
	type: ESTree.TSType,
	environment: TypeEnvironment,
): boolean {
	return typeHasRuntimeObjectStructure(type, environment, new Map(), new Set());
}

export function typeResolvesToRuntimeTypeofTagsForPropertyPath(
	type: ESTree.TSType,
	propertyPath: readonly string[],
	environment: TypeEnvironment,
): ReadonlySet<RuntimeTypeofTag> | undefined {
	let current = type;
	for (const propertyName of propertyPath) {
		const propertyType = runtimePropertyType(current, propertyName, environment, new Map(), new Set());
		if (propertyType === undefined) return undefined;
		current = propertyType;
	}
	return runtimeTypeofTagsForType(current, environment, new Map(), new Set());
}

function runtimeTypeofTagsForType(
	type: ESTree.TSType,
	environment: TypeEnvironment,
	substitutions: TypeAliasEnvironment,
	resolvingAliases: ReadonlySet<string>,
): ReadonlySet<RuntimeTypeofTag> | undefined {
	const unwrapped = unwrapTransparentType(type);
	const tags = (...values: RuntimeTypeofTag[]): ReadonlySet<RuntimeTypeofTag> => new Set(values);

	switch (unwrapped.type) {
		case "TSBigIntKeyword":
			return tags("bigint");
		case "TSBooleanKeyword":
			return tags("boolean");
		case "TSFunctionType":
		case "TSConstructorType":
			return tags("function");
		case "TSNumberKeyword":
			return tags("number");
		case "TSObjectKeyword":
			return tags("object", "function");
		case "TSStringKeyword":
			return tags("string");
		case "TSSymbolKeyword":
			return tags("symbol");
		case "TSUndefinedKeyword":
			return tags("undefined");
		case "TSNullKeyword":
			return tags("object");
		case "TSArrayType":
		case "TSTupleType":
		case "TSTypeLiteral":
		case "TSMappedType":
			return tags("object");
		case "TSNeverKeyword":
			return tags();
		case "TSLiteralType": {
			if (unwrapped.literal.type !== "Literal") return undefined;
			if (unwrapped.literal.value === null) return tags("object");
			switch (typeof unwrapped.literal.value) {
				case "bigint":
					return tags("bigint");
				case "boolean":
					return tags("boolean");
				case "number":
					return tags("number");
				case "string":
					return tags("string");
				default:
					return undefined;
			}
		}
		case "TSIndexedAccessType": {
			const indexType = unwrapTransparentType(unwrapped.indexType);
			if (indexType.type !== "TSLiteralType" || indexType.literal.type !== "Literal") return undefined;
			if (typeof indexType.literal.value !== "string") return undefined;
			const propertyType = runtimePropertyType(
				unwrapped.objectType,
				indexType.literal.value,
				environment,
				substitutions,
				resolvingAliases,
			);
			return propertyType === undefined
				? undefined
				: runtimeTypeofTagsForType(propertyType, environment, substitutions, resolvingAliases);
		}
		case "TSConditionalType": {
			const trueTags = runtimeTypeofTagsForType(
				unwrapped.trueType,
				environment,
				substitutions,
				resolvingAliases,
			);
			const falseTags = runtimeTypeofTagsForType(
				unwrapped.falseType,
				environment,
				substitutions,
				resolvingAliases,
			);
			if (trueTags === undefined && falseTags === undefined) return undefined;
			const conditional = new Set<RuntimeTypeofTag>();
			for (const tag of trueTags ?? []) conditional.add(tag);
			for (const tag of falseTags ?? []) conditional.add(tag);
			return conditional;
		}
		case "TSUnionType": {
			const union = new Set<RuntimeTypeofTag>();
			let hasKnownMember = false;
			for (const member of unwrapped.types) {
				const memberTags = runtimeTypeofTagsForType(
					member,
					environment,
					substitutions,
					resolvingAliases,
				);
				if (memberTags === undefined) continue;
				hasKnownMember = true;
				for (const tag of memberTags) union.add(tag);
			}
			return hasKnownMember ? union : undefined;
		}
		case "TSIntersectionType": {
			let intersection: Set<RuntimeTypeofTag> | undefined;
			for (const member of unwrapped.types) {
				const memberTags = runtimeTypeofTagsForType(
					member,
					environment,
					substitutions,
					resolvingAliases,
				);
				if (memberTags === undefined) continue;
				if (intersection === undefined) {
					intersection = new Set(memberTags);
					continue;
				}
				for (const tag of intersection) {
					if (!memberTags.has(tag)) intersection.delete(tag);
				}
			}
			return intersection;
		}
		case "TSTypeReference": {
			const name = typeReferenceName(unwrapped);
			if (name === null) return undefined;
			const substitution = substitutions.get(name);
			if (substitution !== undefined && !isUnappliedReferenceTo(substitution, name)) {
				return runtimeTypeofTagsForType(
					substitution,
					environment,
					substitutions,
					resolvingAliases,
				);
			}
			if (resolvingAliases.has(name)) return undefined;
			const alias = environment.resolveAlias(unwrapped, name);
			if (alias !== undefined) {
				const nextSubstitutions = aliasSubstitution(alias, unwrapped, substitutions);
				if (nextSubstitutions === null) return undefined;
				const nextResolving = new Set(resolvingAliases);
				nextResolving.add(name);
				return runtimeTypeofTagsForType(
					alias.typeAnnotation,
					environment,
					nextSubstitutions,
					nextResolving,
				);
			}
			if (environment.resolveInterfaces(unwrapped, name).length > 0) return tags("object", "function");
			if (name === "Function") return tags("function");
			if (
				name === "Iterator" ||
				name === "AsyncIterator" ||
				name === "Iterable" ||
				name === "AsyncIterable" ||
				name === "Generator" ||
				name === "AsyncGenerator"
			)
				return tags("object", "function");
			const typeArguments = unwrapped.typeArguments?.params ?? [];
			if (name === "Extract") {
				const extracted = typeArguments[1];
				return extracted === undefined
					? undefined
					: runtimeTypeofTagsForType(extracted, environment, substitutions, resolvingAliases);
			}
			if (name === "NonNullable") {
				const nonNullable = typeArguments[0];
				return nonNullable === undefined
					? undefined
					: runtimeTypeofTagsForType(nonNullable, environment, substitutions, resolvingAliases);
			}
			if (
				name === "Array" ||
				name === "ReadonlyArray" ||
				name === "Map" ||
				name === "ReadonlyMap" ||
				name === "Set" ||
				name === "ReadonlySet" ||
				name === "WeakMap" ||
				name === "WeakSet" ||
				name === "Promise" ||
				name === "Date" ||
				name === "RegExp" ||
				name === "Error" ||
				name === "Object" ||
				name === "Record" ||
				name === "Readonly" ||
				name === "Partial" ||
				name === "Required" ||
				name === "Pick" ||
				name === "Omit"
			)
				return tags("object", "function");
			// Unresolved named types are conservatively treated as object-shaped.
			// This keeps external nominal contracts usable while still rejecting
			// primitive typeof evidence that cannot establish a named domain type.
			return tags("object", "function");
		}
		default:
			return undefined;
	}
}

export function typeResolvesToRuntimeTypeofTags(
	type: ESTree.TSType,
	environment: TypeEnvironment,
): ReadonlySet<RuntimeTypeofTag> | undefined {
	return runtimeTypeofTagsForType(type, environment, new Map(), new Set());
}

function interfaceSubstitution(
	declaration: ESTree.TSInterfaceDeclaration,
	arguments_: readonly ESTree.TSType[],
	base: TypeAliasEnvironment,
): TypeAliasEnvironment | null {
	const parameters = declaration.typeParameters?.params ?? [];
	const next = new Map(base);
	for (const [index, parameter] of parameters.entries()) {
		const argument = arguments_[index] ?? parameter.default;
		if (argument === null || argument === undefined) return null;
		next.set(parameter.name.name, resolvedSubstitutionArgument(argument, next));
	}
	return next;
}

function unsafeDirectValue(
	type: ESTree.TSType,
	environment: TypeEnvironment,
	substitutions: TypeAliasEnvironment,
	resolvingAliases: ReadonlySet<string>,
): UnsafeDictionary["unsafeValue"] | null {
	const unwrapped = unwrapTransparentType(type);
	if (unwrapped.type === "TSUnknownKeyword") return "unknown";
	if (unwrapped.type === "TSAnyKeyword") return "any";
	if (unwrapped.type === "TSObjectKeyword") return "object";
	if (unwrapped.type === "TSTypeLiteral" && isEffectivelyEmptyTypeLiteral(unwrapped))
		return "empty-object";
	if (unwrapped.type === "TSUnionType") {
		return unwrapped.types.some(
			(member) => unsafeDirectValue(member, environment, substitutions, resolvingAliases) !== null,
		)
			? "union"
			: null;
	}
	if (unwrapped.type === "TSIntersectionType") {
		const unsafeMembers = unwrapped.types.map((member) =>
			unsafeDirectValue(member, environment, substitutions, resolvingAliases),
		);
		if (unsafeMembers.includes("any")) return "any";
		return unsafeMembers.length > 0 && unsafeMembers.every((member) => member !== null)
			? unsafeMembers[0]
			: null;
	}
	if (unwrapped.type !== "TSTypeReference") return null;
	const name = typeReferenceName(unwrapped);
	if (name === null) return null;
	if (TRANSPARENT_WRAPPERS.has(name) && isBuiltInReference(unwrapped, name, environment)) {
		const wrapped = unwrapped.typeArguments?.params[0];
		return wrapped === undefined
			? null
			: unsafeDirectValue(wrapped, environment, substitutions, resolvingAliases);
	}
	const substitution = substitutions.get(name);
	if (substitution !== undefined) {
		return isUnappliedReferenceTo(substitution, name)
			? null
			: unsafeDirectValue(substitution, environment, substitutions, resolvingAliases);
	}
	const interfaceDeclarations = environment.resolveInterfaces(unwrapped, name);
	if (interfaceDeclarations.length > 0) {
		return isEffectivelyEmptyInterface(interfaceDeclarations) ? "empty-object" : null;
	}
	const alias = environment.resolveAlias(unwrapped, name);
	if (alias === undefined || resolvingAliases.has(name)) return null;
	const nextSubstitutions = aliasSubstitution(alias, unwrapped, substitutions);
	if (nextSubstitutions === null) return null;
	const nextResolving = new Set(resolvingAliases);
	nextResolving.add(name);
	return unsafeDirectValue(alias.typeAnnotation, environment, nextSubstitutions, nextResolving);
}

function dictionaryValueTypes(
	type: ESTree.TSType,
	environment: TypeEnvironment,
	substitutions: TypeAliasEnvironment,
	resolvingAliases: ReadonlySet<string>,
): readonly ResolvedType[] {
	const unwrapped = unwrapTransparentType(type);

	if (unwrapped.type === "TSUnionType" || unwrapped.type === "TSIntersectionType") {
		return unwrapped.types.flatMap((member) =>
			dictionaryValueTypes(member, environment, substitutions, resolvingAliases),
		);
	}

	if (unwrapped.type === "TSTypeLiteral") {
		return unwrapped.members.flatMap((member): readonly ResolvedType[] =>
			member.type === "TSIndexSignature" && member.typeAnnotation !== null
				? [{ type: member.typeAnnotation.typeAnnotation, substitutions }]
				: [],
		);
	}

	if (unwrapped.type === "TSMappedType") {
		return unwrapped.typeAnnotation === null
			? []
			: [{ type: unwrapped.typeAnnotation, substitutions }];
	}

	if (unwrapped.type !== "TSTypeReference") return [];
	const name = typeReferenceName(unwrapped);
	if (name === null) return [];

	const substitution = substitutions.get(name);
	if (substitution !== undefined) {
		return isUnappliedReferenceTo(substitution, name)
			? []
			: dictionaryValueTypes(substitution, environment, substitutions, resolvingAliases);
	}

	if (TRANSPARENT_WRAPPERS.has(name) && isBuiltInReference(unwrapped, name, environment)) {
		const wrapped = unwrapped.typeArguments?.params[0];
		return wrapped === undefined
			? []
			: dictionaryValueTypes(wrapped, environment, substitutions, resolvingAliases);
	}

	if (name === "Record" && isBuiltInReference(unwrapped, name, environment)) {
		const value = unwrapped.typeArguments?.params[1] ?? null;
		return value === null ? [] : [{ type: value, substitutions }];
	}

	if ((name === "Pick" || name === "Omit") && isBuiltInReference(unwrapped, name, environment)) {
		const source = unwrapped.typeArguments?.params[0];
		return source === undefined
			? []
			: dictionaryValueTypes(source, environment, substitutions, resolvingAliases);
	}

	if (environment.interfaces.has(name) || environment.aliases.has(name)) {
		return dictionaryValueTypesForReference(
			unwrapped,
			name,
			unwrapped.typeArguments?.params ?? [],
			environment,
			substitutions,
			resolvingAliases,
		);
	}

	return [];
}

function dictionaryValueTypesForReference(
	reference: ESTree.Node,
	name: string,
	arguments_: readonly ESTree.TSType[],
	environment: TypeEnvironment,
	substitutions: TypeAliasEnvironment,
	resolvingAliases: ReadonlySet<string>,
): readonly ResolvedType[] {
	if (environment.resolveInterfaces(reference, name).length > 0) {
		return dictionaryValueTypesForInterfaceReference(
			reference,
			name,
			arguments_,
			environment,
			substitutions,
			resolvingAliases,
		);
	}

	const alias = environment.resolveAlias(reference, name);
	if (alias === undefined || resolvingAliases.has(name)) return [];
	const nextSubstitutions = aliasSubstitutionArguments(alias, arguments_, substitutions);
	if (nextSubstitutions === null) return [];
	const nextResolving = new Set(resolvingAliases);
	nextResolving.add(name);
	return dictionaryValueTypes(alias.typeAnnotation, environment, nextSubstitutions, nextResolving);
}

function dictionaryValueTypesForInterfaceReference(
	reference: ESTree.Node,
	name: string,
	arguments_: readonly ESTree.TSType[],
	environment: TypeEnvironment,
	substitutions: TypeAliasEnvironment,
	resolvingAliases: ReadonlySet<string>,
): readonly ResolvedType[] {
	const interfaceDeclarations = environment.resolveInterfaces(reference, name);
	if (interfaceDeclarations.length === 0 || resolvingAliases.has(name)) return [];
	const nextResolving = new Set(resolvingAliases);
	nextResolving.add(name);
	return interfaceDeclarations.flatMap((declaration) => {
		const nextSubstitutions = interfaceSubstitution(declaration, arguments_, substitutions);
		return nextSubstitutions === null
			? []
			: dictionaryValueTypesFromInterface(
					declaration,
					environment,
					nextSubstitutions,
					nextResolving,
			  );
	});
}

function dictionaryValueTypesFromInterface(
	declaration: ESTree.TSInterfaceDeclaration,
	environment: TypeEnvironment,
	substitutions: TypeAliasEnvironment,
	resolvingAliases: ReadonlySet<string>,
): readonly ResolvedType[] {
	const ownValues = declaration.body.body.flatMap((member): readonly ResolvedType[] =>
		member.type === "TSIndexSignature" && member.typeAnnotation !== null
			? [{ type: member.typeAnnotation.typeAnnotation, substitutions }]
			: [],
	);
	const inheritedValues = declaration.extends.flatMap((heritage): readonly ResolvedType[] => {
		const name = heritage.expression.type === "Identifier" ? heritage.expression.name : null;
		return name === null
			? []
			: dictionaryValueTypesForReference(
					heritage,
					name,
					heritage.typeArguments?.params ?? [],
					environment,
					substitutions,
					resolvingAliases,
			  );
	});
	return [...ownValues, ...inheritedValues];
}

export function classifyUnsafeDictionaryValue(
	valueType: ESTree.TSType,
	environment: TypeEnvironment,
	substitutions: TypeAliasEnvironment = new Map(),
): UnsafeDictionary | null {
	const unsafeValue = unsafeDirectValue(valueType, environment, substitutions, new Set());
	return unsafeValue === null ? null : { kind: "unsafe-dictionary", unsafeValue };
}

export function classifyUnsafeDictionaryInterfaceValue(
	declaration: ESTree.TSInterfaceDeclaration,
	valueType: ESTree.TSType,
	environment: TypeEnvironment,
): UnsafeDictionary | null {
	const substitutions = interfaceSubstitution(declaration, [], new Map());
	return substitutions === null
		? null
		: classifyUnsafeDictionaryValue(valueType, environment, substitutions);
}

export function classifyUnsafeDictionary(
	type: ESTree.TSType,
	environment: TypeEnvironment,
): UnsafeDictionary | null {
	const unwrapped = unwrapTransparentType(type);
	if (unwrapped.type === "TSUnionType" || unwrapped.type === "TSIntersectionType") {
		for (const member of unwrapped.types) {
			const unsafe = classifyUnsafeDictionary(member, environment);
			if (unsafe !== null) return { kind: "unsafe-dictionary", unsafeValue: "union" };
		}
		return null;
	}
	return classifyUnsafeDictionaryValues(
		dictionaryValueTypes(type, environment, new Map(), new Set()),
		environment,
	);
}

function resolvesToBroadType(
	type: ESTree.TSType,
	environment: TypeEnvironment,
	target: BroadType,
	substitutions: TypeAliasEnvironment,
	resolvingAliases: ReadonlySet<string>,
	shadowedAliases: ReadonlySet<string>,
): boolean {
	const unwrapped = unwrapTransparentType(type);
	if (target === "unknown" && unwrapped.type === "TSUnknownKeyword") return true;
	if (target === "object" && unwrapped.type === "TSObjectKeyword") return true;
	if (unwrapped.type === "TSUnionType") {
		return unwrapped.types.some((member) =>
			resolvesToBroadType(
				member,
				environment,
				target,
				substitutions,
				resolvingAliases,
				shadowedAliases,
			),
		);
	}
	if (unwrapped.type === "TSIntersectionType") {
		const members = unwrapped.types.map((member) =>
			resolvesToBroadType(
				member,
				environment,
				target,
				substitutions,
				resolvingAliases,
				shadowedAliases,
			),
		);
		return target === "object" ? members.some(Boolean) : members.every(Boolean);
	}
	if (unwrapped.type !== "TSTypeReference") return false;
	if (
		target === "unknown" &&
		(importedTypeResolvesToUnknown(unwrapped, environment) ||
			schemaTypeResolvesToUnknown(unwrapped, environment))
	)
		return true;
	const name = typeReferenceName(unwrapped);
	if (
		name === null ||
		shadowedAliases.has(name) ||
		resolvingAliases.has(name)
	)
		return false;
	const substitution = substitutions.get(name);
	if (substitution !== undefined && !isUnappliedReferenceTo(substitution, name)) {
		return resolvesToBroadType(
			substitution,
			environment,
			target,
			substitutions,
			resolvingAliases,
			shadowedAliases,
		);
	}
	if (TRANSPARENT_WRAPPERS.has(name) && isBuiltInReference(unwrapped, name, environment)) {
		const wrapped = unwrapped.typeArguments?.params[0];
		return wrapped !== undefined &&
			resolvesToBroadType(
				wrapped,
				environment,
				target,
				substitutions,
				resolvingAliases,
				shadowedAliases,
			);
	}
	const alias = environment.resolveAlias(unwrapped, name);
	if (alias === undefined) return false;
	const nextSubstitutions = aliasSubstitution(alias, unwrapped, substitutions);
	if (nextSubstitutions === null) return false;
	const nextResolving = new Set(resolvingAliases);
	nextResolving.add(name);
	return resolvesToBroadType(
		alias.typeAnnotation,
		environment,
		target,
		nextSubstitutions,
		nextResolving,
		shadowedAliases,
	);
}

export function typeResolvesToBroadType(
	type: ESTree.TSType,
	environment: TypeEnvironment,
	target: BroadType,
	shadowedAliases: ReadonlySet<string> = new Set(),
): boolean {
	return resolvesToBroadType(type, environment, target, new Map(), new Set(), shadowedAliases);
}

function resolvesToSchemaUnknownBoundary(
	type: ESTree.TSType,
	environment: TypeEnvironment,
	resolvingAliases: ReadonlySet<string>,
): boolean {
	const unwrapped = unwrapTransparentType(type);
	if (unwrapped.type !== "TSTypeReference") return false;
	if (schemaTypeResolvesToUnknown(unwrapped, environment)) return true;
	const name = typeReferenceName(unwrapped);
	if (name === null || resolvingAliases.has(name)) return false;
	const alias = environment.resolveAlias(unwrapped, name);
	if (alias === undefined) return false;
	const next = new Set(resolvingAliases);
	next.add(name);
	return resolvesToSchemaUnknownBoundary(alias.typeAnnotation, environment, next);
}

export function typeResolvesToSchemaUnknownBoundary(
	type: ESTree.TSType,
	environment: TypeEnvironment,
): boolean {
	return resolvesToSchemaUnknownBoundary(type, environment, new Set());
}

export function classifyUnsafeDictionaryInterfaceReference(
	heritage: ESTree.TSInterfaceHeritage,
	environment: TypeEnvironment,
): UnsafeDictionary | null {
	if (heritage.expression.type !== "Identifier") return null;
	return classifyUnsafeDictionaryValues(
		dictionaryValueTypesForReference(
			heritage,
			heritage.expression.name,
			heritage.typeArguments?.params ?? [],
			environment,
			new Map(),
			new Set(),
		),
		environment,
	);
}

function classifyUnsafeDictionaryValues(
	valueTypes: readonly ResolvedType[],
	environment: TypeEnvironment,
): UnsafeDictionary | null {
	for (const valueType of valueTypes) {
		const unsafeValue = unsafeDirectValue(
			valueType.type,
			environment,
			valueType.substitutions,
			new Set(),
		);
		if (unsafeValue !== null) return { kind: "unsafe-dictionary", unsafeValue };
	}
	return null;
}

function resolvesToDictionary(
	type: ESTree.TSType,
	environment: TypeEnvironment,
	substitutions: TypeAliasEnvironment,
	resolvingAliases: ReadonlySet<string>,
): boolean {
	return dictionaryValueTypes(type, environment, substitutions, resolvingAliases).length > 0;
}

export function classifyWideningTarget(
	type: ESTree.TSType,
	environment: TypeEnvironment,
): WideningTarget | null {
	const unwrapped = unwrapTransparentType(type);
	if (unwrapped.type === "TSUnknownKeyword") return { kind: "unknown" };
	if (unwrapped.type === "TSObjectKeyword") return { kind: "object" };
	if (unwrapped.type === "TSTypeLiteral") {
		const index = unwrapped.members.find((member) => member.type === "TSIndexSignature");
		if (index?.type === "TSIndexSignature") {
			return index.typeAnnotation !== null &&
				classifyUnsafeDictionaryValue(index.typeAnnotation.typeAnnotation, environment) !== null
				? { kind: "open dictionary" }
				: null;
		}
		return { kind: "anonymous object" };
	}
	if (
		unwrapped.type === "TSMappedType" &&
		isBroadMappedKey(unwrapped.constraint, environment, new Map()) &&
		unwrapped.typeAnnotation !== null &&
		classifyUnsafeDictionaryValue(unwrapped.typeAnnotation, environment) !== null
	)
		return { kind: "open dictionary" };
	if (unwrapped.type !== "TSTypeReference") return null;
	const name = typeReferenceName(unwrapped);
	if (name === null) return null;
	if (TRANSPARENT_WRAPPERS.has(name) && isBuiltInReference(unwrapped, name, environment)) {
		const wrapped = unwrapped.typeArguments?.params[0];
		return wrapped === undefined ? null : classifyWideningTarget(wrapped, environment);
	}
	if (name === "Record" && isBuiltInReference(unwrapped, name, environment)) {
		const value = unwrapped.typeArguments?.params[1];
		return value !== undefined &&
			isBroadRecordKey(unwrapped.typeArguments?.params[0], environment) &&
			classifyUnsafeDictionaryValue(value, environment) !== null
			? { kind: "open dictionary" }
			: null;
	}
	const alias = environment.resolveAlias(unwrapped, name);
	if (alias === undefined) return null;
	if ((alias.typeParameters?.params.length ?? 0) > 0) {
		const substitutions = aliasSubstitution(alias, unwrapped, new Map());
		return substitutions !== null &&
			resolvesToDictionary(alias.typeAnnotation, environment, substitutions, new Set([name]))
			? { kind: "generic container" }
			: null;
	}
	const substitutions = aliasSubstitution(alias, unwrapped, new Map());
	if (substitutions === null) return null;
	const resolved = classifyAliasBroadTarget(
		alias.typeAnnotation,
		environment,
		substitutions,
		new Set([name]),
	);
	return resolved;
}

function isBroadMappedKey(
	type: ESTree.TSType,
	environment: TypeEnvironment,
	substitutions: TypeAliasEnvironment,
): boolean {
	const unwrapped = unwrapTransparentType(type);
	if (
		unwrapped.type === "TSStringKeyword" ||
		unwrapped.type === "TSNumberKeyword" ||
		unwrapped.type === "TSSymbolKeyword"
	) {
		return true;
	}
	if (unwrapped.type === "TSUnionType") {
		return unwrapped.types.every((member) =>
			isBroadMappedKey(member, environment, substitutions),
		);
	}
	if (unwrapped.type !== "TSTypeReference") return false;
	const name = typeReferenceName(unwrapped);
	if (name === null) return false;
	const substitution = substitutions.get(name);
	if (substitution !== undefined && !isUnappliedReferenceTo(substitution, name)) {
		return isBroadMappedKey(substitution, environment, substitutions);
	}
	return isBuiltInReference(unwrapped, name, environment) && name === "PropertyKey";
}

function isBroadRecordKey(
	type: ESTree.TSType | undefined,
	environment: TypeEnvironment,
): boolean {
	if (type === undefined) return false;
	const unwrapped = unwrapTransparentType(type);
	if (
		unwrapped.type === "TSStringKeyword" ||
		unwrapped.type === "TSNumberKeyword" ||
		unwrapped.type === "TSSymbolKeyword"
	)
		return true;
	if (unwrapped.type === "TSUnionType") {
		return unwrapped.types.some((member) => isBroadRecordKey(member, environment));
	}
	if (unwrapped.type !== "TSTypeReference") return false;
	const name = typeReferenceName(unwrapped);
	return name !== null && isBuiltInReference(unwrapped, name, environment) && name === "PropertyKey";
}

function classifyAliasBroadTarget(
	type: ESTree.TSType,
	environment: TypeEnvironment,
	substitutions: TypeAliasEnvironment,
	resolvingAliases: ReadonlySet<string>,
): WideningTarget | null {
	const unwrapped = unwrapTransparentType(type);
	if (unwrapped.type === "TSUnknownKeyword") return { kind: "unknown" };
	if (unwrapped.type === "TSObjectKeyword") return { kind: "object" };
	if (unwrapped.type === "TSTypeLiteral") {
		const index = unwrapped.members.find((member) => member.type === "TSIndexSignature");
		return index?.type === "TSIndexSignature" &&
			index.typeAnnotation !== null &&
			classifyUnsafeDictionaryValue(index.typeAnnotation.typeAnnotation, environment) !== null
			? { kind: "open dictionary" }
			: null;
	}
	if (unwrapped.type === "TSMappedType") {
		return isBroadMappedKey(unwrapped.constraint, environment, substitutions) &&
			unwrapped.typeAnnotation !== null &&
			classifyUnsafeDictionaryValue(unwrapped.typeAnnotation, environment, substitutions) !== null
			? { kind: "open dictionary" }
			: null;
	}
	if (unwrapped.type !== "TSTypeReference") return null;
	const name = typeReferenceName(unwrapped);
	if (name === null) return null;
	const substitution = substitutions.get(name);
	if (substitution !== undefined) {
		return isUnappliedReferenceTo(substitution, name)
			? null
			: classifyAliasBroadTarget(
					substitution,
					environment,
					substitutions,
					resolvingAliases,
				);
	}
	if (TRANSPARENT_WRAPPERS.has(name) && isBuiltInReference(unwrapped, name, environment)) {
		const wrapped = unwrapped.typeArguments?.params[0];
		return wrapped === undefined
			? null
			: classifyAliasBroadTarget(wrapped, environment, substitutions, resolvingAliases);
	}
	if (name === "Record" && isBuiltInReference(unwrapped, name, environment)) {
		const value = unwrapped.typeArguments?.params[1];
		return value !== undefined &&
			isBroadRecordKey(unwrapped.typeArguments?.params[0], environment) &&
			classifyUnsafeDictionaryValue(value, environment, substitutions) !== null
			? { kind: "open dictionary" }
			: null;
	}
	const alias = environment.resolveAlias(unwrapped, name);
	if (alias === undefined || resolvingAliases.has(name)) return null;
	const nextSubstitutions = aliasSubstitution(alias, unwrapped, substitutions);
	if (nextSubstitutions === null) return null;
	const nextResolving = new Set(resolvingAliases);
	nextResolving.add(name);
	return classifyAliasBroadTarget(
		alias.typeAnnotation,
		environment,
		nextSubstitutions,
		nextResolving,
	);
}

export function isPopulatedObjectExpression(expression: ESTree.Expression): boolean {
	let current = expression;
	while (
		current.type === "ParenthesizedExpression" ||
		current.type === "TSAsExpression" ||
		current.type === "TSTypeAssertion" ||
		current.type === "TSNonNullExpression"
	) {
		current = current.expression;
	}
	return current.type === "ObjectExpression" && current.properties.length > 0;
}

export function isKnownEvidenceExpression(expression: ESTree.Expression): boolean {
	let current = expression;
	while (
		current.type === "ParenthesizedExpression" ||
		current.type === "TSAsExpression" ||
		current.type === "TSTypeAssertion" ||
		current.type === "TSNonNullExpression" ||
		current.type === "TSSatisfiesExpression"
	) {
		current = current.expression;
	}
	if (current.type === "ObjectExpression") return true;
	return (
		current.type === "ArrayExpression" ||
		current.type === "ArrowFunctionExpression" ||
		current.type === "ClassExpression" ||
		current.type === "FunctionExpression" ||
		current.type === "NewExpression" ||
		current.type === "Literal" ||
		current.type === "TemplateLiteral" ||
		current.type === "UnaryExpression"
	);
}
