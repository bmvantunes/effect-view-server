import { defineRule } from "@oxlint/plugins";

import {
	createTypeEnvironment,
	typeResolvesToBroadType,
	typeResolvesToSchemaUnknownBoundary,
	type TypeEnvironment,
} from "../shared/dictionary-types.ts";

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
		let environment: TypeEnvironment | null = null;

		return {
			Program(node) {
				environment = createTypeEnvironment(node);
				for (const aliases of environment.aliases.values()) {
					for (const alias of aliases) {
						if (
							!typeResolvesToBroadType(alias.typeAnnotation, environment, "unknown") ||
							typeResolvesToSchemaUnknownBoundary(alias.typeAnnotation, environment)
						)
							continue;
					context.report({
						node: alias.id,
						messageId: "unknownAlias",
						data: { alias: alias.id.name },
					});
					}
				}
			},
		};
	},
});
