import type { OxlintConfig } from "vite-plus/lint";

export const strictLintOptions = {
  denyWarnings: true,
  typeAware: true,
  typeCheck: true,
} satisfies NonNullable<OxlintConfig["options"]>;
