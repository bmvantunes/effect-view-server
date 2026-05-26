# view-server-smart

## Remote React provider

Server code exposes a runtime through Effect RPC WebSocket:

```ts
import { createInMemoryViewServer } from "@view-server/in-memory";
import { createViewServerWebSocketServer } from "@view-server/server";
import { Effect } from "effect";
import { viewServer } from "./view-server-config";

const runtime = createInMemoryViewServer(viewServer);

const server = await Effect.runPromise(
  createViewServerWebSocketServer(viewServer, {
    liveClient: runtime.liveClient,
    runtime: runtime.client,
  }),
);

console.log(server.url);
```

Browser React code keeps using the normal provider and hooks:

```tsx
import { createViewServerReact } from "@view-server/react";
import { viewServer } from "./view-server-config";

const react = createViewServerReact(viewServer);

export function App() {
  return (
    <react.ViewServerProvider url={window.__APP_CONFIG__.VIEW_SERVER_URL}>
      <Orders />
    </react.ViewServerProvider>
  );
}

function Orders() {
  const orders = react.useLiveQuery("orders", {
    select: ["id", "price"],
    orderBy: [{ field: "price", direction: "asc" }],
    limit: 20,
  });

  return <pre>{JSON.stringify(orders.rows, null, 2)}</pre>;
}
```
