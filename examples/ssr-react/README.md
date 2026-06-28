# SSR React Example

TanStack Start app showing the current SSR-safe contract.

Run:

```bash
vp run @view-server/example-ssr-react#build
vp run @view-server/example-ssr-react#dev
```

This example demonstrates:

- The page shell can be server-rendered.
- The View Server WebSocket provider is mounted only in the browser.
- Live query hooks stay behind the browser-only provider boundary.
