---
"effect-view-server": major
---

Replace the hook-valued whole-result viewport property with a source-owned React Renderer Adapter. The Renderer owns the live query subscription and returns its result through a render callback, so consumers no longer need to invoke a hook discovered from the source object.
