/*
 * Target-repository policy for the generic anti-slop analyzer.
 *
 * The installer ships this conservative empty policy. A target repository
 * may add reviewed validation contracts after installing the skill; the
 * bundled asset must never trust another repository's module names or types.
 */

export type ReviewedImportedTypeContract = {
	readonly tags: ReadonlySet<string>;
	readonly structural: boolean;
	readonly properties: ReadonlyMap<string, ReadonlySet<string>>;
	readonly literalProperties?: ReadonlyMap<
		string,
		ReadonlySet<string | number | boolean | bigint | null>
	>;
};

export const reviewedImportedTypeContracts = new Map<string, ReviewedImportedTypeContract>();
export const namedPredicateContracts = new Map<string, ReadonlySet<string>>();
export const knownValidationCalls = new Set<string>();
export const validationCallbackWrappers = new Set<string>();
export const validationFactoryCalls = new Set<string>();
export const trustedValidationImportExports = new Map<string, ReadonlySet<string>>();
export const trustedValidationNamespaceImports = new Map<string, ReadonlySet<string>>();
export const trustedValidationNamespaceRoots = new Set<string>();
export const trustedSchemaUnknownTypePaths = new Set<string>();
export const trustedSchemaUnknownValuePaths = new Set<string>();
