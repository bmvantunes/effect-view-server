# In-Memory Browser Testing

Production React imports from `@effect-view-server/react`. Browser tests import the
testing helper from `@effect-view-server/react/testing`.

```tsx
import { createInMemoryViewServerReact } from "@effect-view-server/react/testing";
import { viewServerReact } from "./view-server.config";

export const createInMemoryExampleViewServer = () => createInMemoryViewServerReact(viewServerReact);
```

The testing helper returns a provider plus a typed client. The provider uses the
same hook binding object as production, but transport is in-memory instead of
Effect RPC WebSocket.

```tsx
import { Effect } from "effect";
import { expect, it } from "@effect/vitest";
import { render } from "vitest-browser-react";
import { createInMemoryExampleViewServer } from "./testing";
import { OrdersGrid } from "./orders-grid";

it("renders pushed orders", async () => {
  const { ViewServerInMemoryProvider, client } = createInMemoryExampleViewServer();
  const screen = await render(
    <ViewServerInMemoryProvider>
      <OrdersGrid />
    </ViewServerInMemoryProvider>,
  );

  await Effect.runPromise(
    client.publish("orders", {
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 10,
      region: "usa",
      updatedAt: 1,
    }),
  );

  await expect.element(screen.getByText("order-1")).toBeVisible();
});
```

## Rules

- Do not change application components for tests. Swap only the provider.
- Do not seed providers. Publish through the returned client like any other
  ingress path.
- Do not use Testing Library, `act`, `flushSync`, or `data-testid`.
- Use Vitest Browser Mode locators from `vitest-browser-react`.
- Runtime-level tests should use Effect-based tests where possible.

The in-memory provider exercises the same Runtime Core and engine code as the
remote runtime. Only external ingress and transport are replaced.
