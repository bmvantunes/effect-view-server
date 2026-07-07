# effect-view-server

Typed Effect View Server for live query snapshots, deltas, React bindings, and runtime ingress adapters.

This package intentionally has no root export. Import from explicit subpaths so applications only load the surface they use:

```ts
import { defineViewServerConfig } from "effect-view-server/config";
import { createViewServerReact } from "effect-view-server/react";
import { runViewServerRuntime } from "effect-view-server/runtime";
```

React applications should install compatible peer dependencies:

```sh
vp add effect react react-dom @effect/atom-react
```

See the repository README and examples for Kafka, gRPC, TCP publishing, in-memory testing, and React usage.
