# Continuous npm publishing with Trusted Publishing

Research date: 2026-07-31

## Executive conclusion

npm does not require Trusted Publishing releases to pass through staging. A
GitHub Actions trusted publisher can be authorized for `npm publish`,
`npm stage publish`, or both. Direct `npm publish` uses the same short-lived
OIDC authentication and, for a public package built from a public GitHub
repository on a GitHub-hosted runner, npm automatically publishes provenance.
That direct release therefore retains the green provenance check mark shown in
the supplied npm screenshot.

The repository was stage-only because it deliberately chose that policy, not
because npm imposes it. The release workflow now uses direct publishing after
CI succeeds; the npm trusted publisher still needs its one-time permission
change to allow `npm publish`.

## Direct publishing versus staged publishing

| Property                                 | Direct trusted publish             | Staged trusted publish                                  |
| ---------------------------------------- | ---------------------------------- | ------------------------------------------------------- |
| CI command                               | `npm publish`                      | `npm stage publish`                                     |
| Authentication                           | Short-lived GitHub OIDC credential | Short-lived GitHub OIDC credential                      |
| Human action before going live           | None                               | Maintainer approval with 2FA                            |
| Provenance on an eligible public package | Automatic                          | Trusted-publishing provenance is retained when approved |
| npm trusted-publisher permission         | Allow `npm publish`                | Allow `npm stage publish`                               |
| Best fit                                 | Continuous delivery                | Human-reviewed releases / maximum npm-side protection   |

npm describes staging as an optional extra approval step: "instead of"
publishing directly, CI submits the package to a staging area, after which a
maintainer approves it with 2FA. npm separately documents `npm publish` as a
supported OIDC action and even shows it in the canonical GitHub Actions
example. Sources:

- [npm Trusted Publishing](https://docs.npmjs.com/trusted-publishers/)
- [npm staged publishing](https://docs.npmjs.com/staged-publishing/)
- [`npm trust` permissions](https://docs.npmjs.com/cli/v11/commands/npm-trust/)

For a direct release, edit the existing trusted publisher on npmjs.com and
allow `npm publish` (either alone or as well as staging). Keep the exact GitHub
repository and workflow filename binding. The publish job needs
`id-token: write`, must use a supported GitHub-hosted runner, and must run a
supported toolchain. npm's current minimum for Trusted Publishing is npm
11.5.1 with Node 22.14.0; this repository's Node 26 and npm 11.17.0 already meet
that requirement. npm permits only one trusted-publisher configuration per
package.

After trusted publishing works, npm recommends setting package Publishing
access to **Require two-factor authentication and disallow tokens**, then
revoking unused automation tokens. That setting does not block the trusted
publisher because OIDC does not use a traditional npm token. Stage-only
permission adds the strongest npm-side posture, but it is not necessary for
tokenless publishing or provenance.

## What the green check mark means

The green symbol next to each version in the screenshot is npm's provenance
indicator. npm says that it links the package to its source repository, source
commit, build workflow, build environment, and public transparency-log entry.
It is available for both direct and staged releases that meet the trusted
publishing provenance conditions.

- [Viewing package provenance](https://docs.npmjs.com/viewing-package-provenance/)
- [Generating provenance statements](https://docs.npmjs.com/generating-provenance-statements/)

Provenance is not a malware or correctness certificate. npm explicitly says it
does not guarantee that a package contains no malicious code; it gives consumers
a verifiable origin and build trail. Consequently, moving from stage-only to
direct publishing preserves the green checks but removes the independent human
inspection/2FA gate.

## Can ten commits to `main` produce ten npm versions?

Yes. When each commit has enough time to finish its release, every run can
receive a unique version and publish it. npm rejects an already-existing
`name@version`, and that exact combination can never be reused even after
unpublishing. Publishing without an explicit dist-tag moves `latest` to the
published version. Sources:

- [`npm publish`](https://docs.npmjs.com/cli/publish/)
- [npm unpublish policy](https://docs.npmjs.com/policies/unpublish/)
- [npm dist-tags](https://docs.npmjs.com/cli/v11/commands/npm-dist-tag/)

Two separate decisions are required:

1. **Version identity.** Each commit needs a deterministic, unique npm version.
   For stable releases, that normally means a version already committed on the
   exact source commit. For continuous snapshots, a version derived from a
   monotonic workflow number or commit identity can be written only into the
   staged artifact and published under a `canary`/`next` dist-tag.
2. **Release semantics.** Automatically putting every commit on `latest` turns
   every merge, including documentation or CI-only changes, into a stable
   semver release. A canary dist-tag is safer when "every commit" is literal;
   `latest` is reasonable only if `main` is always releasable and the team
   accepts a patch release for every merge.

The clarified requirement deliberately allows rapid commits to coalesce into
one release. That matches GitHub's default one-running/one-pending concurrency
behavior: a newer pending run replaces an older pending run. `queue: max` is
therefore unnecessary.

Do not, however, cancel a job that may already be executing `npm publish`.
Registry publication is irreversible for that version, so cancellation can
leave npm published while later Git tagging or reporting steps did not run. A
safer workflow uses two concurrency lanes:

1. a preparation/readiness lane with `cancel-in-progress: true`, so a newer
   commit supersedes an older commit that has not reached publication; and
2. a publish lane with `cancel-in-progress: false`, so a publish that has begun
   always finishes while the default single pending slot coalesces any burst to
   the newest ready commit.

If a newer commit arrives after the older commit has crossed the publication
boundary, both versions may publish. No cancellation policy can retract an npm
version that the registry has already accepted. This is the safe interpretation
of "rapid commits may become one publish; otherwise each commit publishes."

- [GitHub Actions concurrency concept](https://docs.github.com/en/actions/concepts/workflows-and-actions/concurrency)
- [Controlling concurrency and `queue: max`](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency)

CI cancels obsolete preparation work, while the reusable publication job keeps
an in-flight publish alive and coalesces only its pending work. This gives the
desired latest-wins behavior without interrupting an irreversible publish. It
also keeps npm's provenance `GITHUB_SHA` aligned with the tested push commit;
using a separate `workflow_run` publisher would need an explicit provenance
identity override because that event's default SHA is the default branch tip.

## Changesets and `AGENTS.md`

An `AGENTS.md` instruction is useful guidance, but it is not enforcement. If a
changeset is required for releaseable changes, CI should verify it as well.
Changesets' own workflow is:

1. add a changeset with a contribution;
2. run `changeset version` when a release is ready; and
3. run `changeset publish` afterward.

Changesets also says not every repository change needs a changeset. Its GitHub
Action normally opens or updates a Version Packages PR while unreleased
changesets exist, then publishes after that version PR is merged. Ten ordinary
commits can therefore be batched into one version PR and one release; adding a
changeset to each commit does **not** by itself guarantee ten publishes.

- [Changesets usage](https://github.com/changesets/changesets/blob/main/docs/intro-to-using-changesets.md)
- [Changesets Action](https://github.com/changesets/action)

The implementation keeps Changesets as optional release intent: agents should
add one when a public change needs a deliberate major/minor/patch bump, while a
main commit without a changeset receives a patch bump automatically. This
means correctness does not depend on agents remembering a changeset and still
preserves meaningful release-type control.

## What TanStack Config is doing

TanStack Config is package-maintenance tooling, not a special npm registry
feature. Its Publish docs expose an ESM `publish(...)` function and explain how
to set up npm Trusted Publishing, but explicitly note that trusted publishing
still has to be configured for each npm package. TanStack Config is also
pnpm-only, while this repository's non-negotiable interface is Vite+ (`vp`).

- [TanStack Config Publish docs](https://tanstack.com/config/latest/docs/publish)
- [TanStack Config overview](https://tanstack.com/config/latest/docs/overview)

TanStack's current documented CI convention uses Changesets for versioning and
publishing. The TanStack Config repository's own workflow runs on pushes to
release branches, grants `id-token: write`, and invokes `changesets/action` with
`changeset:version` and `changeset:publish`; its package scripts resolve those to
`changeset version` and `changeset publish`. TanStack Router follows the same
"Create Release Pull Request or Publish" pattern. Those workflows publish
directly through Trusted Publishing; they do not use npm staged publishing.

- [TanStack Config CI/CD docs](https://tanstack.com/config/latest/docs/ci-cd)
- [TanStack Config release workflow](https://github.com/TanStack/config/blob/main/.github/workflows/release.yml)
- [TanStack Config package scripts](https://github.com/TanStack/config/blob/main/package.json)
- [TanStack Router release workflow](https://github.com/TanStack/router/blob/main/.github/workflows/release.yml)

Therefore, adopting `@tanstack/publish-config` is not required to solve this
repository's problem. The existing release module already has valuable
artifact-sanitization and private-workspace-leak checks. The smaller and safer
change is to retain that artifact preparation, change only the release
orchestration to direct OIDC `npm publish`, and choose an explicit versioning
policy. TanStack's relevant example is the release shape—Changesets plus direct
Trusted Publishing—not a hidden way around npm staging.

## Recommended direction for `effect-view-server`

For the user's exact goal, use direct npm Trusted Publishing from `ci.yml`,
retain the sanitized artifact, automatic provenance, OIDC checks,
and no-cache release build, and allow `npm publish` in the npm trusted
publisher. The implementation moves `latest` on every non-superseded main
commit, gives Changesets control over the bump type, defaults to patch, and
makes retries idempotent.

Security hardening remains important because direct publishing makes the
workflow the final authority: bind npm to the exact workflow (and optionally a
protected GitHub environment), disallow traditional npm tokens, use minimal
job permissions, avoid release caches, and pin third-party Actions to full
commit SHAs. GitHub documents environment branch restrictions and recommends
full-SHA action pinning; TanStack adopted no-cache releases and SHA-pinned
Actions after its own npm supply-chain incident.

- [GitHub deployment environments](https://docs.github.com/en/actions/reference/workflows-and-actions/deployments-and-environments)
- [GitHub Actions policy for full-SHA pinning](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/enabling-features-for-your-repository/managing-github-actions-settings-for-a-repository)
- [TanStack hardening follow-up](https://tanstack.com/blog/incident-followup)
