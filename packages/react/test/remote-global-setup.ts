import { env } from "node:process";

type Project = {
  readonly provide: (
    key: "viewServerRemoteUrl" | "viewServerSourceRemoteUrl",
    value: string,
  ) => void;
};

const skipRemoteGlobalSetup = (): boolean =>
  env["VIEW_SERVER_REACT_SKIP_REMOTE_GLOBAL_SETUP"] === "1";

export const setup = async (project: Project) => {
  if (skipRemoteGlobalSetup()) {
    project.provide("viewServerRemoteUrl", "ws://127.0.0.1:0/rpc");
    project.provide("viewServerSourceRemoteUrl", "ws://127.0.0.1:0/rpc");
    return () => Promise.resolve();
  }

  const { ViewServerId, defineViewServerConfig } = await import("@effect-view-server/config");
  const { createInMemoryViewServerTesting, makeInMemoryViewServerTesting } =
    await import("@effect-view-server/in-memory/testing");
  const { makeViewServerWebSocketServer } = await import("@effect-view-server/server");
  const { SourceAdapter } = await import("@effect-view-server/source-adapter");
  const { SourceAdapterServer } = await import("@effect-view-server/source-adapter/server");
  const { Effect, Schedule, Schema, Stream } = await import("effect");

  const Order = Schema.Struct({
    id: ViewServerId,
    customerId: Schema.String,
    status: Schema.Literals(["open", "closed", "cancelled"]),
    price: Schema.Number,
    region: Schema.String,
    updatedAt: Schema.Number,
  });

  const Trade = Schema.Struct({
    id: ViewServerId,
    symbol: Schema.String,
    quantity: Schema.BigInt,
    price: Schema.Number,
    region: Schema.String,
  });

  const viewServer = defineViewServerConfig({
    topics: {
      orders: {
        schema: Order,
      },
      trades: {
        schema: Trade,
      },
    },
  });
  const SourceHealthOrder = Schema.Struct({
    id: ViewServerId,
    region: Schema.String,
  });
  const sourceAdapter = SourceAdapter.make({
    identity: { name: "react-browser-source" },
    failure: Schema.Never,
    materialized: {
      metrics: Schema.Struct({ observed: Schema.BigInt }),
      rejectionLocation: Schema.Struct({ offset: Schema.BigInt }),
      definitionOptions: SourceAdapter.definitionOptions<undefined>(),
    },
    leased: {
      metrics: Schema.Struct({ observed: Schema.BigInt }),
      rejectionLocation: Schema.Struct({ offset: Schema.BigInt }),
      definitionOptions: SourceAdapter.definitionOptions<undefined>(),
    },
  });
  const sourceAdapterLayer = SourceAdapterServer.make(sourceAdapter, {
    materialized: {
      acquire: () =>
        Effect.succeed(
          SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "react-browser",
              events: Stream.never,
            }),
          ]),
        ),
      metrics: () => Effect.succeed({ observed: 1n }),
      retry: Schedule.recurs(0),
    },
    leased: {
      acquire: () =>
        Effect.succeed(
          SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "react-browser",
              events: Stream.never,
            }),
          ]),
        ),
      metrics: () => Effect.succeed({ observed: 1n }),
      retry: Schedule.recurs(0),
    },
  });
  const sourceHealthViewServer = defineViewServerConfig({
    topics: {
      orders: {
        schema: SourceHealthOrder,
        source: sourceAdapter.leasedSource(["region"], undefined),
      },
    },
  });

  const runtimeCore = createInMemoryViewServerTesting(viewServer);
  const server = await Effect.runPromise(
    makeViewServerWebSocketServer(viewServer, {
      liveClient: runtimeCore.serverLiveClient,
      runtime: runtimeCore.client,
    }),
  );
  project.provide("viewServerRemoteUrl", server.url);
  const sourceRuntimeCore = await Effect.runPromise(
    makeInMemoryViewServerTesting(sourceHealthViewServer, {}).pipe(
      Effect.provide(sourceAdapterLayer),
    ),
  );
  const sourceServer = await Effect.runPromise(
    makeViewServerWebSocketServer(sourceHealthViewServer, {
      liveClient: sourceRuntimeCore.serverLiveClient,
      runtime: sourceRuntimeCore.client,
    }),
  );
  project.provide("viewServerSourceRemoteUrl", sourceServer.url);

  return async () => {
    await Effect.runPromise(
      server.close.pipe(
        Effect.andThen(runtimeCore.close),
        Effect.andThen(sourceServer.close),
        Effect.andThen(sourceRuntimeCore.close),
      ),
    );
  };
};
