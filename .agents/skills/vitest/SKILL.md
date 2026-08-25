---
name: vitest
description: Use for tests, type tests, browser tests, mocking, fixtures, coverage, benchmarks, and Vitest or Vite+ test configuration in this repository.
---

# Vitest

Start with the repository's test rules and existing nearby tests. For the installed
test runner and configuration, read the relevant pages under
`node_modules/vite-plus/docs`, especially `guide/test.md` and `config/test.md`.

Use installed-version evidence for details:

- Inspect `node_modules/vitest` declarations and source for exact Vitest APIs.
- For Effect tests, read `node_modules/effect/AGENTS.md` completely and follow the relevant testing links; inspect `node_modules/@effect/vitest` when signatures matter.
- For browser tests, inspect the installed `@vitest/browser*` packages and existing project browser tests.
- For public generic APIs, follow existing `.test-d.ts` and `expectTypeOf` patterns and cover both accepted and rejected calls.

Use `vp test` and repository tasks rather than invoking a different package manager.
Repository instructions override general package guidance.
