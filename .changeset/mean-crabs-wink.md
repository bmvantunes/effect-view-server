---
"effect-view-server": patch
---

Move React-only packages out of hard npm dependencies. The React subpath now declares @effect/atom-react and scheduler as optional peers, keeping non-React installs smaller while preserving an explicit React peer contract.
