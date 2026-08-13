---
name: install-anti-slop
description: Install and configure the anti-slop Oxlint plugin in a local TypeScript or JavaScript repository. Use whenever a user asks to add anti-slop lint rules, copy the anti-slop plugin, configure opinionated Oxlint rules, or migrate an existing local anti-slop setup.
---

# Install anti-slop

Install the bundled Oxlint plugin into the current repository and integrate it with the repository's existing lint setup. Preserve unrelated work and adapt to the project's package manager and configuration style.

## Procedure

1. Inspect the repository before changing it:
   - Read its agent instructions.
   - Check `git status` and preserve unrelated changes.
   - Identify the package manager from `packageManager` and lockfiles.
   - Find Oxlint configuration (`oxlint.config.*`, `.oxlintrc*`, or a Vite+ config).
   - Check whether anti-slop files or rules already exist. Do not overwrite them without reviewing the diff.

2. Copy the bundled plugin from this skill. Run from the target repository:

   ```bash
   node <skill-directory>/scripts/install.mjs
   ```

   This creates `tools/oxlint/anti-slop/`. Pass another relative destination as the first argument when the repository has an established tooling layout. The script refuses to replace an existing destination; only use `--force` after backing up and reviewing existing files.

3. Install current compatible dependencies rather than trusting versions remembered by the agent:
   - Query `npm view oxlint version` and `npm view @oxlint/plugins version`.
   - Install the same current version of both packages with the repository's package manager.
   - `oxlint` is a development dependency. The copied source imports `@oxlint/plugins`, so install it as a development dependency for a local-only plugin.
   - Do not replace the package manager or rewrite unrelated dependency ranges.

4. Register the plugin and enable the reviewed rules. For `oxlint.config.ts`, add:

   ```ts
   jsPlugins: [
     { name: "anti-slop", specifier: "./tools/oxlint/anti-slop/index.ts" },
   ],
   ```

   For `.oxlintrc.json`, use the equivalent JSON form:

   ```json
   {
     "jsPlugins": [
       { "name": "anti-slop", "specifier": "./tools/oxlint/anti-slop/index.ts" }
     ]
   }
   ```

   For Vite+, add that same entry to `lint.jsPlugins`. Merge it with existing entries instead of replacing them.

   Enable the rules that have been reviewed for the target repository at `"error"` severity. Keep
   noisy or intentionally deferred rules disabled until their findings are addressed; do not lower
   the severity of an enabled rule just to make the lint pass. For a focused rollout, the reviewed
   configuration can be as small as:

   ```json
   {
     "anti-slop/no-unsafe-dictionary-type": "error"
   }
   ```

   The other bundled rule IDs can be enabled individually after their findings have been reviewed.

5. Run the repository's lint command and typecheck. If findings appear, report them and fix them only when the user asked for migration/cleanup. Do not suppress enabled rules, weaken enabled rule severity, add unsafe casts, or mechanically launder types to make lint pass.

6. Review the final diff and clearly report:
   - copied path,
   - dependency versions installed,
   - configuration changed,
   - checks run and any remaining findings.

## Migration guidance

When replacing an older local copy, compare its rules and diagnostics before overwriting. Keep project-specific rules in their own plugin; anti-slop is intentionally generic. Prefer inference, `as const`, `satisfies`, named owner contracts, and boundary parsing when resolving findings.
