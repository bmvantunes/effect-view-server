# PRD #292 implementation-loop handoff

Updated: 2026-07-15 (Europe/London)

This document is a live execution handoff. It does not restate the product requirements or issue acceptance criteria. Read the linked PRD, issue bodies, repository instructions, domain context, ADRs, and plans before changing code.

## Goal and authoritative state

The active objective is to complete [PRD #292](https://github.com/bmvantunes/effect-view-server/issues/292) through one reviewed, validated, merged pull request per child issue, then satisfy [final convergence issue #307](https://github.com/bmvantunes/effect-view-server/issues/307) and close #292.

The goal is **not complete**.

- Original remediation issues [#293 through #306](https://github.com/bmvantunes/effect-view-server/issues/292) are closed through merged work.
- Follow-up issue [#356](https://github.com/bmvantunes/effect-view-server/issues/356) is closed by merged [PR #357](https://github.com/bmvantunes/effect-view-server/pull/357), merge commit `6327e6e8b9da4386a2bd0b58387b296fe361a7e7`.
- Parent-linked follow-ups #327 through #348 remain open and carry `ready-for-agent`.
- #307 and parent #292 remain open.
- The in-product goal ledger still says `blocked` from an old approval-limit pause and its objective predates #327 through #348. Treat the live GitHub parent/child issue state as authoritative; do not mark the goal complete until #327 through #348 and #307 are closed through clean merged work and #292 is actually satisfied.

Suggested implementation order is #327, #328 through #335, #336 through #340, #341 through #348, then #307. Follow dependencies in each issue body; in particular, #335 depends on #327.

## Current issue: #327

Issue: [Make the TCP publisher example command interruption-safe](https://github.com/bmvantunes/effect-view-server/issues/327)

Live work is uncommitted in:

- Worktree: `/private/tmp/view-server-327-tcp-interruption`
- Branch: `codex/issue-327-tcp-interruption`
- Base: `origin/main` at `6327e6e8b9da4386a2bd0b58387b296fe361a7e7`
- Intended modified files only:
  - `examples/tcp-publisher-react/src/tcp-client.ts`
  - `examples/tcp-publisher-react/src/tcp-client.test.ts`
- The branch is not committed, not pushed, and has no pull request yet.

The current implementation:

- makes `writeCommand` a named `Effect.fn` workflow;
- owns the socket with `Effect.scoped` plus `Effect.acquireRelease`;
- bridges Node callbacks through one Effect `Deferred`;
- removes all installed listeners and destroys the socket in one finalizer;
- uses `Effect.timeoutOrElse` and `TestClock`, with no raw timer or nested `Effect.run*` boundary;
- maps synchronous connect failures, asynchronous transport failures, early close, timeout, malformed JSON, and command-encoding failures into typed failures;
- keeps the response-line helper and five-second acknowledgement timeout private;
- preserves defaults when callers explicitly pass `host: undefined` or `port: undefined`;
- covers interruption with a withholding peer and proves an interrupt-only exit, one peer close, and zero remaining sockets.

The original regression-first interruption test failed because the socket remained open and the test hit a 250 ms timeout. The scoped implementation made it pass.

### Review history and current evidence

First-round architecture review found a test-only exported parser seam, an unnecessary public timeout option, and a shallow one-caller connect helper. All were removed or folded into the owning workflow.

First-round Effect review found direct `JSON.stringify` defects and generated route-tree cast noise. Encoding is now wrapped in `Effect.try` with a bigint regression, and generated route trees are clean.

First-round Vitest review found nondeterministic coverage of the no-newline branch and incorrect optional-default resolution. The early-close case now sends an unterminated partial acknowledgement, and each option resolves with `??` before encoding.

Latest evidence after those fixes:

- Focused tests: 10/10 pass.
- Exact Istanbul coverage of `tcp-client.ts`: 100% statements (53/53), branches (7/7), functions (21/21), and lines (53/53).
- Package test passes: Chromium, WebKit, Firefox, 10 TCP tests, and typechecking (12 total tests, no type errors).
- Fresh Vitest re-review: 0 blocking, 1 non-blocking generated-route hygiene note; the route-tree note was cleaned immediately afterward, so the worktree again contains only the two intended TCP files.
- `git diff --check` passes and the changed files contain no forbidden timers, nested runtimes, casts, assertion styles, or coverage ignores.

Do not overstate validation: the prior repository-wide `vp check`, strict Effect diagnostics, and `vp run -w ready` passed in the isolated worktree before the final encoding/default/review fixes, so they are stale and must be rerun. The serial smoke benchmark has not run for the final diff. A complete second review round with all three independent reviewers has not run yet.

## Exact resume sequence for #327

1. Read root `AGENTS.md`, [CONTEXT.md](../../CONTEXT.md), relevant [ADRs](../adr/), [plans](../../plans/), parent #292, and issue #327.
2. Verify the root checkout still contains only the protected user TCP edit and that its SHA-256 is unchanged (see the protected-state section). Never validate in the root checkout.
3. In `/private/tmp/view-server-327-tcp-interruption`, inspect the full diff and confirm only the two intended TCP files are modified.
4. Rerun the focused exact-coverage command and `vp run @effect-view-server/example-tcp-publisher-react#test`.
5. Run `vp check`, strict Effect diagnostics (`vp run -w check:effect`), and `vp run -w ready` from the isolated #327 worktree.
6. Clean any TanStack generated route-tree drift with `scripts/clean-tanstack-route-tree.mjs` followed by `vp check --fix` on the generated route files. Confirm the diff returns to the two intended files.
7. Run `vp run -w bench:baseline:smoke` serially. Do not run competing benchmark suites concurrently.
8. Spawn three read-only reviewers in parallel: Effect, Vitest/type safety, and architecture/maintainability. Require explicit `BLOCKING` and `NON-BLOCKING` counts. Fix every blocker, rerun affected gates, then repeat all three reviewers until all report zero blockers.
9. No changeset is expected: this is a private example/internal correctness fix, not a publishable public-package change.
10. Commit intentionally, push `codex/issue-327-tcp-interruption`, open a ready PR that closes #327, and include red/green, exact-coverage, browser/type, readiness, smoke, and three-reviewer evidence.
11. Monitor GitHub Actions and Codex Cloud review. Fix all actionable feedback, repeat the three-reviewer loop after code changes, merge only when clean, verify #327 closes, and reverify the protected root file.
12. Continue the remaining ready issues one at a time in dependency order, then run #307's final convergence/capacity gates and close #292 only when every stopping condition is true.

## Protected root checkout

The root checkout is `/Users/bruno/projects/view-server-smart`. It contains user work that must not be touched, staged, discarded, overwritten, or included in any issue commit:

- `M packages/runtime/src/tcp-publish-socket-runtime.ts`
- Expected SHA-256: `9b16f805e66b1e43a924b301fc39a7ae95bd2e76bbe63b1ba78201e377467b30`

Use isolated worktrees for implementation and validation. Never run validations in the root checkout. Recheck the hash before and after Git operations.

## Loop discipline

- Use one implementation branch/worktree per issue. Do not launch multiple overlapping implementation agents against shared files or serial performance gates.
- Parallelize the three read-only review roles after each implementation is locally green.
- Every issue follows regression-first implementation, focused tests, changed-package exact 100% coverage, `vp check`, strict Effect diagnostics, `ready`, relevant serial benchmark gates, three-reviewer convergence, PR/CI/Codex review, merge, and issue-close verification.
- Preserve Effect v4 patterns, typed errors, package direction, NDJSON transport, type-level contracts, and benchmark honesty defined in `AGENTS.md`.
- Do not confuse local validation with merge state. Verify repository identity, PR state, merge commit, and issue closure live.

## Recommended skills for the next session

- `.agents/skills/effect-ts/SKILL.md` for all implementation and Effect review work.
- `.agents/skills/vitest/SKILL.md` for tests, exact coverage, browser/type tests, and fixtures.
- `.agents/skills/improve-codebase-architecture/SKILL.md` for the architecture reviewer.
- `.agents/skills/thermo-nuclear-code-quality-review/SKILL.md` for strict maintainability checks where an issue changes deep runtime code.
- `.agents/skills/tdd/SKILL.md` for each regression-first issue slice.
- GitHub PR comment/CI skills when review threads or Actions fail.

## Authoritative references

- [PRD #292](https://github.com/bmvantunes/effect-view-server/issues/292)
- [Current issue #327](https://github.com/bmvantunes/effect-view-server/issues/327)
- [Final convergence #307](https://github.com/bmvantunes/effect-view-server/issues/307)
- [Repository instructions](../../AGENTS.md)
- [Domain context](../../CONTEXT.md)
- [ADRs](../adr/)
- [Remaining-roadmap audit](../../plans/remaining-roadmap-audit.md)
- [Issue tracker conventions](../agents/issue-tracker.md)
- [Triage labels](../agents/triage-labels.md)

## Paste-ready resume prompt

> Resume PRD #292 from `docs/handoffs/prd-292-issue-loop.md`. Start with the uncommitted #327 worktree at `/private/tmp/view-server-327-tcp-interruption`, preserve the protected root TCP file and its exact hash, rerun the stale final gates, repeat the three-reviewer loop until zero blockers, publish/merge #327, then continue #328 through #348 and finally #307 one issue at a time under the repository rules. Do not validate in the root checkout and do not mark #292 complete until every linked stopping condition is actually satisfied.
