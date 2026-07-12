# effect-view-server

Typed Effect View Server for live query snapshots, deltas, React bindings, and runtime ingress adapters.

This package intentionally has no root export. Import from explicit subpaths so applications only load the surface they use:

```ts
import { defineViewServerConfig } from "effect-view-server/config";
import { createViewServerReact } from "effect-view-server/react";
import { runViewServerRuntime } from "effect-view-server/runtime";
```

Topic schemas that use Effect `Option`, `Chunk`, `HashMap`, or `HashSet` values
use the corresponding `viewSchema` factory from `effect-view-server/config`.
`viewSchema.BigDecimal` is the admitted `Schema.BigDecimal` declaration, so
either spelling is valid. Admit each concrete schema class explicitly after
definition:

```ts
import { viewSchema } from "effect-view-server/config";
import { Schema } from "effect";

class Profile extends Schema.Class<Profile>("Profile")({ id: Schema.String }) {}
viewSchema.admitClass(Profile);
```

Class methods remain domain behavior and are not exposed as Topic Row columns.

React applications should install the package and compatible peer dependencies:

```sh
npm install effect-view-server effect react react-dom @effect/atom-react
```

See the repository README and Public API guide for schema admission, Kafka,
gRPC, TCP publishing, in-memory testing, and React usage.
