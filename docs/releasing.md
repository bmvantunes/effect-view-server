# Releasing

The publishable npm package is `effect-view-server`. Workspace packages under
`@effect-view-server/*` are internal implementation packages and must stay
private.

Releases use Changesets for release intent and npm Trusted Publishing for the
actual publish. Every successful, non-superseded push to `main` gets one new
version. Do not add an `NPM_TOKEN`; the release workflow uses GitHub OIDC with
`id-token: write`.

## One-time npm setup

Before the first publish, configure npm Trusted Publishing for the
`effect-view-server` package:

- Package name: `effect-view-server`
- Repository: `bmvantunes/effect-view-server`
- Workflow file: `ci.yml`
- Allowed action: `npm publish`
- Environment: leave unset unless the workflow is also changed to use a GitHub
  environment

The public package manifest uses npm's normalized repository URL
`git+https://github.com/bmvantunes/effect-view-server.git`; keep that aligned
with the npm trusted publisher repository instead of using an SSH URL. The
publish job installs a known npm CLI version because trusted publishing
requires modern npm OIDC support.

The package sets `publishConfig.provenance: true`, and the release workflow has
`id-token: write`, so npm attaches provenance to every direct publish. The
package was bootstrapped with a one-time manual `effect-view-server@0.0.1`
publish before trusted publishing was enabled.

## Contributor flow

For any PR that should control the release type, add a changeset:

```sh
vp run -w changeset
```

Choose the release type for `effect-view-server`:

- `patch`: bug fixes and documentation-safe package changes
- `minor`: new backwards-compatible API or runtime features
- `major`: breaking public API/runtime behavior

Internal `@effect-view-server/*` packages are not published. If a change only
touches tests, CI, docs, benchmarks, or private internals and should not
control the release type, do not add a versioned changeset. The continuous
release still creates a patch version for that main commit.

## Main branch flow

On every push to `main`, `CI` runs the readiness and benchmark gates. Its
publish job downloads the exact `effect-view-server/dist` artifact produced by
that same workflow and publishes it directly to npm with trusted publishing.
Keeping publication in the same push workflow means npm provenance points at
the exact `GITHUB_SHA` that was tested. There is no version PR, manual stage
approval, or second finalize job.

The release script finds the current npm version and its matching public release
tag. The existing `-staged` tag is accepted as a one-time migration baseline.
It then inspects only `.changeset/*.md` files added since that tag, increments
the npm version using the strongest changeset type, and defaults to a patch
bump when the commit has no changeset. Ten main commits that finish CI
separately therefore produce ten npm publishes. If newer commits arrive while
older CI is still running, GitHub may cancel stale preparation and keep only
the newest pending release.

The workspace `package.json` keeps its development version; only the sanitized
publish artifact receives the computed npm version.

Before publishing, CI reserves an `effect-view-server@<version>-pending` tag at
the exact tested commit. This closes the small race where npm accepts a publish
before a runner can push the public tag: a retry can repair the public tag, while
a later commit can safely reserve the next version. Pending tags are bookkeeping,
not additional npm releases.

The published artifact intentionally excludes source maps, source-map
references, scripts, dev dependencies, internal `@effect-view-server/*`
workspace metadata, and internal workspace import specifiers. The release
script refuses placeholder `effect-view-server@0.0.0`, untrusted contexts, and
public internal workspace packages. If a retry races an existing npm version,
it verifies the version and repairs the corresponding git tag instead of
publishing a duplicate.

`scripts/release-publish.mjs` is only the process Adapter. Release Publish
Orchestration owns the temporary artifact, version calculation, npm state
decisions, public release tag, and cleanup behind an injected command Adapter,
so repository tests exercise the real file staging without invoking npm or Git.

## Manual checks

Useful local checks before merging release-sensitive changes:

```sh
vp run -w ready
vp run effect-view-server#build
vp run effect-view-server#test
```

For PRs that should control a package release type, also verify the changeset
state:

```sh
vp exec changeset status --since main
```

Use the heavier capacity gate only when promoting production-like runtime
readiness:

```sh
vp run -w release-candidate:capacity
```
