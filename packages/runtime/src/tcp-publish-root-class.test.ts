import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, viewSchema } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import { sendTcpPublishCommand } from "../test-harness/runtime";
import { makeViewServerRuntime } from "./index";

class RootClassTcpOrder extends Schema.Class<RootClassTcpOrder>("RootClassTcpOrder")({
  id: Schema.String,
  quantity: Schema.BigInt,
  status: Schema.String,
}) {}
viewSchema.admitClass(RootClassTcpOrder);

const rootClassTcpViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: RootClassTcpOrder,
      key: "id",
    },
  },
});

describe("TCP publish root Class rows", () => {
  it.live("publishes, publishes many, and patches through the TCP command path", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(rootClassTcpViewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const responses = [
        yield* sendTcpPublishCommand(tcpUrl, {
          op: "publish",
          topic: "orders",
          row: { id: "a", quantity: "1", status: "published" },
        }),
        yield* sendTcpPublishCommand(tcpUrl, {
          op: "publishMany",
          topic: "orders",
          rows: [
            { id: "b", quantity: "2", status: "published-many" },
            { id: "c", quantity: "3", status: "published-many" },
          ],
        }),
        yield* sendTcpPublishCommand(tcpUrl, {
          op: "patch",
          topic: "orders",
          key: "a",
          patch: { quantity: "10", status: "patched" },
        }),
      ];
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "quantity", "status"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(responses).toStrictEqual([{ ok: true }, { ok: true }, { ok: true }]);
      expect(snapshot).toStrictEqual({
        rows: [
          { id: "a", quantity: 10n, status: "patched" },
          { id: "b", quantity: 2n, status: "published-many" },
          { id: "c", quantity: 3n, status: "published-many" },
        ],
        status: "ready",
        statusCode: "Ready",
        totalRows: 3,
        version: 3,
      });
      yield* runtime.close;
    }),
  );
});
