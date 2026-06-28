import { useLiveQuery, useViewServerHealthSummary, ViewServerProvider } from "./view-server.config";

export function SsrExampleApp() {
  return (
    <main className="example-shell">
      <header>
        <p className="eyebrow">SSR shell</p>
        <h1>TanStack Start shell with client-only live data</h1>
        <p>
          The page shell is safe to server-render. The View Server WebSocket provider only mounts in
          the browser.
        </p>
      </header>
      <ClientOnlyLivePanel />
    </main>
  );
}

function ClientOnlyLivePanel() {
  if (globalThis.document === undefined) {
    return (
      <section className="panel" aria-label="ssr placeholder">
        <h2>Live data</h2>
        <p>Live queries hydrate in the browser.</p>
      </section>
    );
  }

  return (
    <ViewServerProvider url="ws://127.0.0.1:8080/rpc">
      <LiveOrdersPanel />
    </ViewServerProvider>
  );
}

function LiveOrdersPanel() {
  const health = useViewServerHealthSummary();
  const orders = useLiveQuery("orders", {
    select: ["id", "customerId", "price"],
    orderBy: [{ field: "updatedAt", direction: "desc" }],
    limit: 10,
  });

  return (
    <section className="panel" aria-label="hydrated live orders">
      <h2>Live orders</h2>
      <p role="status">Runtime status: {health.status}</p>
      <p>Total rows: {orders.totalRows}</p>
      <ul>
        {orders.rows.map((order) => (
          <li key={order.id}>
            {order.id} / {order.customerId} / {order.price}
          </li>
        ))}
      </ul>
    </section>
  );
}
