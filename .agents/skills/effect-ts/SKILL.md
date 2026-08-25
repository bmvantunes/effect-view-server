---
name: effect-ts
description: Use whenever working in a repository that uses Effect, including implementation, review, architecture, services, layers, schemas, streams, runtimes, testing, and typed error handling.
---

# Effect

Before writing or reviewing Effect code, read `node_modules/effect/AGENTS.md`
completely. Follow only the links relevant to the task, resolving relative paths
from `node_modules/effect`.

Use the installed packages as the source of truth because they match the version
used by the project:

- Search `node_modules/effect/src` for exact Effect APIs and implementation details.
- Search the corresponding installed `node_modules/@effect/*` package for integration-specific APIs.
- Check existing project code for local conventions and established module seams.

If dependencies are missing, use the repository's normal install command. Do not
require a separate Effect checkout.

Repository instructions override general package guidance.
