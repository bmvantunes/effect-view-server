# Issue tracker: GitHub

Issues and PRDs for this repository live in GitHub Issues at `bmvantunes/effect-view-server`.

Use the connected GitHub integration when available. The `gh` CLI is the local fallback and infers the repository from `git remote -v`.

## Conventions

- Create issues with a concise title, a complete Markdown body, and exactly one triage-state label from `docs/agents/triage-labels.md`.
- Read an issue and its discussion before implementing it.
- Publish PRDs as parent GitHub issues.
- Reference the parent PRD from every implementation issue.
- Publish implementation issues in dependency order so `Blocked by` can reference real issue numbers.
- Select a `ready-for-agent` issue only after every issue referenced in its `Blocked by` section is closed.
- Do not close or rewrite a parent PRD while creating or completing child issues.
- Use one implementation issue per independently verifiable vertical slice.
- Link the implementation PR from its issue and include the validation evidence in the PR body.

## Common CLI operations

- Create: `gh issue create --title "..." --body-file <path> --label "<triage-label>"`
- Read: `gh issue view <number> --comments`
- List ready work: `gh issue list --state open --label "ready-for-agent"`
- Comment: `gh issue comment <number> --body "..."`
- Add a label: `gh issue edit <number> --add-label "..."`
- Remove a label: `gh issue edit <number> --remove-label "..."`
- Close after the linked work is merged: `gh issue close <number> --comment "..."`

When a skill says to publish to the issue tracker, create a GitHub issue in this repository.
