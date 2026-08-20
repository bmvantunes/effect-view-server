import type { OxlintConfig } from "oxlint";

export const strictLintOptions = {
  denyWarnings: true,
  typeAware: true,
  typeCheck: true,
} satisfies NonNullable<OxlintConfig["options"]>;
