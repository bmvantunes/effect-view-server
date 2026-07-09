# Domain documentation

This is a single-context repository.

## Required reading

Before planning, reviewing, or implementing repository work:

1. Read the root `CONTEXT.md` and use its domain vocabulary.
2. Read the ADRs in `docs/adr/` that affect the work.
3. Read relevant active material in `plans/`, while treating accepted ADRs and current implementation as authoritative when plan text is stale.

Do not silently contradict an accepted ADR. If a proposed change conflicts with one, mark the issue `ready-for-human` and explain which decision must be reopened.

When an issue title, PRD, test, or refactoring proposal names a domain concept, use the term defined in `CONTEXT.md`. Avoid inventing synonyms that weaken the project language.

If a required domain concept is genuinely missing, record that gap and use the `grill-with-docs` workflow before finalizing the design.
