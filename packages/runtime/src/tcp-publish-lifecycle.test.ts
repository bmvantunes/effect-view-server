import { describe, expect, it } from "@effect/vitest";
import { type ViewServerRuntimeError } from "@effect-view-server/config";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import type { ViewServerRuntimeCoreInternalClient } from "@effect-view-server/runtime-core/internal";
import { Cause, Deferred, Effect, Exit, Fiber, Option, Schema } from "effect";
import { TestClock } from "effect/testing";
import type { ViewServerRuntimeDependencies } from "./internal";
import { makeDefaultRuntimeDependencies, makeViewServerRuntimeWithDependencies } from "./internal";
import { makeViewServerRuntime } from "./index";
import {
  makeViewServerTcpPublishIngress,
  makeViewServerTcpPublishIngressWithServerFactory,
  ViewServerTcpPublishIngressError,
  writeTcpJsonLine,
} from "./tcp-publish-ingress";
import {
  closeTestTcpServer,
  connectTcpPublishSocket,
  readTcpPublishResponse,
  readTcpPublishResponses,
  reserveTcpPort,
  RuntimeTestFailure,
  sendTcpPublishCommand,
} from "../test-harness/runtime";
import * as Net from "node:net";

import { order, Order, viewServer } from "../test-harness/runtime-config";

describe("TCP publish lifecycle ownership", () => {
  it.effect("owns accepted TCP socket deadlines with Effect time", () =>
    Effect.acquireUseRelease(
      makeViewServerRuntimeCoreInternal(viewServer, {}),
      (runtimeCore) =>
        Effect.acquireUseRelease(
          makeViewServerTcpPublishIngressWithServerFactory(
            viewServer,
            runtimeCore.internalClient,
            { port: 0 },
            (connectionListener) => Net.createServer(connectionListener),
          ),
          (ingress) =>
            Effect.acquireUseRelease(
              connectTcpPublishSocket(ingress.url),
              (socket) =>
                Effect.gen(function* () {
                  const closed = yield* Deferred.make<void>();
                  socket.once("close", () => Deferred.doneUnsafe(closed, Effect.void));
                  socket.write(
                    `${JSON.stringify({
                      op: "publish",
                      topic: "orders",
                      row: order("effect-clock-deadline", 10),
                    })}\n`,
                  );
                  const response = yield* readTcpPublishResponse(socket);
                  yield* Effect.yieldNow;
                  yield* TestClock.adjust("30 seconds");
                  yield* Deferred.await(closed);

                  expect({
                    destroyed: socket.destroyed,
                    response,
                  }).toStrictEqual({
                    destroyed: true,
                    response: { ok: true },
                  });
                }),
              (socket) => Effect.sync(() => socket.destroy()),
            ),
          (ingress) => ingress.close,
        ),
      (runtimeCore) => runtimeCore.close,
    ),
  );

  it.live("joins accepted TCP socket command finalizers before ingress close completes", () =>
    Effect.acquireUseRelease(
      makeViewServerRuntimeCoreInternal(viewServer, {}),
      (runtimeCore) =>
        Effect.gen(function* () {
          const commandStarted = yield* Deferred.make<void>();
          const commandFinalizerStarted = yield* Deferred.make<void>();
          const allowCommandFinalizer = yield* Deferred.make<void>();
          const commandFinalized = yield* Deferred.make<void>();
          const closeCompleted = yield* Deferred.make<void>();
          const client: ViewServerRuntimeCoreInternalClient<typeof viewServer.topics> = {
            ...runtimeCore.internalClient,
            publishManyDecodedRows: () =>
              Effect.acquireUseRelease(
                Deferred.succeed(commandStarted, undefined),
                () => Effect.never,
                () =>
                  Deferred.succeed(commandFinalizerStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(allowCommandFinalizer)),
                    Effect.andThen(Deferred.succeed(commandFinalized, undefined)),
                  ),
              ),
          };

          yield* Effect.acquireUseRelease(
            makeViewServerTcpPublishIngress(viewServer, client, { port: 0 }),
            (ingress) =>
              Effect.acquireUseRelease(
                connectTcpPublishSocket(ingress.url),
                (socket) =>
                  Effect.gen(function* () {
                    const clientClosed = yield* Deferred.make<void>();
                    socket.once("close", () => Deferred.doneUnsafe(clientClosed, Effect.void));
                    socket.write(
                      `${JSON.stringify({
                        op: "publish",
                        topic: "orders",
                        row: order("socket-scope-close", 10),
                      })}\n`,
                    );
                    yield* Deferred.await(commandStarted).pipe(Effect.timeout("1 second"));
                    const closeFiber = yield* ingress.close.pipe(
                      Effect.ensuring(Deferred.succeed(closeCompleted, undefined)),
                      Effect.forkChild({ startImmediately: true }),
                    );
                    yield* Deferred.await(commandFinalizerStarted).pipe(Effect.timeout("1 second"));
                    yield* Deferred.await(clientClosed).pipe(Effect.timeout("1 second"));

                    expect({
                      closeCompleted: yield* Deferred.isDone(closeCompleted),
                      commandFinalized: yield* Deferred.isDone(commandFinalized),
                      socketDestroyed: socket.destroyed,
                    }).toStrictEqual({
                      closeCompleted: false,
                      commandFinalized: false,
                      socketDestroyed: true,
                    });

                    yield* Deferred.succeed(allowCommandFinalizer, undefined);
                    yield* Fiber.join(closeFiber).pipe(Effect.timeout("1 second"));
                    expect({
                      closeCompleted: yield* Deferred.isDone(closeCompleted),
                      commandFinalized: yield* Deferred.isDone(commandFinalized),
                      socketDestroyed: socket.destroyed,
                    }).toStrictEqual({
                      closeCompleted: true,
                      commandFinalized: true,
                      socketDestroyed: true,
                    });
                  }).pipe(Effect.ensuring(Deferred.succeed(allowCommandFinalizer, undefined))),
                (socket) => Effect.sync(() => socket.destroy()),
              ),
            (ingress) => ingress.close,
          );
        }),
      (runtimeCore) => runtimeCore.close,
    ),
  );

  it.live("sends terminal TCP queue errors before joining command finalizers", () =>
    Effect.acquireUseRelease(
      makeViewServerRuntimeCoreInternal(viewServer, {}),
      (runtimeCore) =>
        Effect.gen(function* () {
          const commandStarted = yield* Deferred.make<void>();
          const commandFinalizerStarted = yield* Deferred.make<void>();
          const allowCommandFinalizer = yield* Deferred.make<void>();
          const commandFinalized = yield* Deferred.make<void>();
          const client: ViewServerRuntimeCoreInternalClient<typeof viewServer.topics> = {
            ...runtimeCore.internalClient,
            publishManyDecodedRows: () =>
              Effect.acquireUseRelease(
                Deferred.succeed(commandStarted, undefined),
                () => Effect.never,
                () =>
                  Deferred.succeed(commandFinalizerStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(allowCommandFinalizer)),
                    Effect.andThen(Deferred.succeed(commandFinalized, undefined)),
                  ),
              ),
          };

          yield* Effect.acquireUseRelease(
            makeViewServerTcpPublishIngress(viewServer, client, {
              maxQueuedCommands: 1,
              port: 0,
            }),
            (ingress) =>
              Effect.acquireUseRelease(
                connectTcpPublishSocket(ingress.url),
                (socket) =>
                  Effect.gen(function* () {
                    const commandLine = `${JSON.stringify({
                      op: "publish",
                      topic: "orders",
                      row: order("terminal-before-finalizer", 10),
                    })}\n`;
                    socket.write(commandLine);
                    yield* Deferred.await(commandStarted).pipe(Effect.timeout("1 second"));
                    socket.write(commandLine);

                    const response = yield* readTcpPublishResponse(socket).pipe(
                      Effect.timeout("1 second"),
                    );
                    expect({
                      commandFinalized: yield* Deferred.isDone(commandFinalized),
                      response,
                    }).toStrictEqual({
                      commandFinalized: false,
                      response: {
                        ok: false,
                        error: {
                          _tag: "ViewServerTcpPublishIngressError",
                          message: "TCP publish command queue exceeded 1 commands.",
                          phase: "backpressure",
                        },
                      },
                    });

                    yield* Deferred.await(commandFinalizerStarted).pipe(Effect.timeout("1 second"));
                    expect(yield* Deferred.isDone(commandFinalized)).toBe(false);
                    yield* Deferred.succeed(allowCommandFinalizer, undefined);
                    yield* Deferred.await(commandFinalized).pipe(Effect.timeout("1 second"));
                  }).pipe(Effect.ensuring(Deferred.succeed(allowCommandFinalizer, undefined))),
                (socket) => Effect.sync(() => socket.destroy()),
              ),
            (ingress) => ingress.close,
          );
        }),
      (runtimeCore) => runtimeCore.close,
    ),
  );

  it.live("releases TCP connection admission before joining disconnect finalizers", () =>
    Effect.acquireUseRelease(
      makeViewServerRuntimeCoreInternal(viewServer, {}),
      (runtimeCore) =>
        Effect.gen(function* () {
          const commandStarted = yield* Deferred.make<void>();
          const commandFinalizerStarted = yield* Deferred.make<void>();
          const allowCommandFinalizer = yield* Deferred.make<void>();
          const commandFinalized = yield* Deferred.make<void>();
          let publishCalls = 0;
          const client: ViewServerRuntimeCoreInternalClient<typeof viewServer.topics> = {
            ...runtimeCore.internalClient,
            publishManyDecodedRows: () => {
              publishCalls += 1;
              if (publishCalls > 1) {
                return Effect.void;
              }
              return Effect.acquireUseRelease(
                Deferred.succeed(commandStarted, undefined),
                () => Effect.never,
                () =>
                  Deferred.succeed(commandFinalizerStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(allowCommandFinalizer)),
                    Effect.andThen(Deferred.succeed(commandFinalized, undefined)),
                  ),
              );
            },
          };

          yield* Effect.acquireUseRelease(
            makeViewServerTcpPublishIngress(viewServer, client, {
              maxConnections: 1,
              port: 0,
            }),
            (ingress) =>
              Effect.acquireUseRelease(
                connectTcpPublishSocket(ingress.url),
                (firstSocket) =>
                  Effect.gen(function* () {
                    firstSocket.write(
                      `${JSON.stringify({
                        op: "publish",
                        topic: "orders",
                        row: order("disconnect-with-finalizer", 10),
                      })}\n`,
                    );
                    yield* Deferred.await(commandStarted).pipe(Effect.timeout("1 second"));
                    firstSocket.destroy();
                    yield* Deferred.await(commandFinalizerStarted).pipe(Effect.timeout("1 second"));

                    const secondResponse = yield* sendTcpPublishCommand(ingress.url, {
                      op: "publish",
                      topic: "orders",
                      row: order("admitted-before-finalizer", 20),
                    });
                    expect({
                      commandFinalized: yield* Deferred.isDone(commandFinalized),
                      secondResponse,
                    }).toStrictEqual({
                      commandFinalized: false,
                      secondResponse: { ok: true },
                    });

                    yield* Deferred.succeed(allowCommandFinalizer, undefined);
                    yield* Deferred.await(commandFinalized).pipe(Effect.timeout("1 second"));
                  }).pipe(Effect.ensuring(Deferred.succeed(allowCommandFinalizer, undefined))),
                (socket) => Effect.sync(() => socket.destroy()),
              ),
            (ingress) => ingress.close,
          );
        }),
      (runtimeCore) => runtimeCore.close,
    ),
  );

  it.live("bounds TCP publish line size and command queue", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      const slowPublishStarted = yield* Deferred.make<void>();
      const slowPublishInterrupted = yield* Deferred.make<void>();
      const recoveryPublishStarted = yield* Deferred.make<void>();
      const allowRecoveryPublish = yield* Deferred.make<void>();
      const recoveryPublishFinalized = yield* Deferred.make<void>();
      const globalPublishStarted = yield* Deferred.make<void>();
      const globalPublishInterrupted = yield* Deferred.make<void>();
      const disconnectedPublishStarted = yield* Deferred.make<void>();
      const disconnectedPublishInterrupted = yield* Deferred.make<void>();
      const closedQueuePublishStarted = yield* Deferred.make<void>();
      const closedQueuePublishInterrupted = yield* Deferred.make<void>();
      const fifoIds: Array<string> = [];
      let slowPublishCalls = 0;
      const slowPublishClient: ViewServerRuntimeCoreInternalClient<typeof viewServer.topics> = {
        ...runtimeCore.internalClient,
        publishManyDecodedRows: () => {
          slowPublishCalls += 1;
          if (slowPublishCalls === 1) {
            return Deferred.succeed(slowPublishStarted, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(Deferred.succeed(slowPublishInterrupted, undefined)),
            );
          }
          if (slowPublishCalls === 2) {
            return Effect.acquireUseRelease(
              Deferred.succeed(recoveryPublishStarted, undefined),
              () => Deferred.await(allowRecoveryPublish),
              () => Deferred.succeed(recoveryPublishFinalized, undefined),
            );
          }
          return Effect.void;
        },
      };
      const fifoPublishClient: ViewServerRuntimeCoreInternalClient<typeof viewServer.topics> = {
        ...runtimeCore.internalClient,
        publishManyDecodedRows: (_topic, rows) =>
          Effect.sync(() => {
            fifoIds.push(...rows.map((row) => Schema.decodeUnknownSync(Order)(row).id));
          }),
      };
      const globalPublishClient: ViewServerRuntimeCoreInternalClient<typeof viewServer.topics> = {
        ...runtimeCore.internalClient,
        publishManyDecodedRows: () =>
          Deferred.succeed(globalPublishStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(globalPublishInterrupted, undefined)),
          ),
      };
      const disconnectedPublishClient: ViewServerRuntimeCoreInternalClient<
        typeof viewServer.topics
      > = {
        ...runtimeCore.internalClient,
        publishManyDecodedRows: () =>
          Deferred.succeed(disconnectedPublishStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(disconnectedPublishInterrupted, undefined)),
          ),
      };
      const closedQueuePublishClient: ViewServerRuntimeCoreInternalClient<
        typeof viewServer.topics
      > = {
        ...runtimeCore.internalClient,
        publishManyDecodedRows: () =>
          Deferred.succeed(closedQueuePublishStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(closedQueuePublishInterrupted, undefined)),
          ),
      };
      const fifoCommandLines = ["fifo-1", "fifo-2"].map(
        (id) =>
          `${JSON.stringify({
            op: "publish",
            topic: "orders",
            row: order(id, 10),
          })}\n`,
      );
      const commandLine = `${JSON.stringify({
        op: "publish",
        topic: "orders",
        row: order("a", 10),
      })}\n`;
      const oversizedIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        runtimeCore.internalClient,
        {
          maxLineBytes: 8,
          port: 0,
        },
      );
      const oversizedCompleteLineIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        runtimeCore.internalClient,
        {
          maxLineBytes: 8,
          port: 0,
        },
      );
      const coalescedIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        fifoPublishClient,
        {
          maxLineBytes: Math.max(
            ...fifoCommandLines.map((line) => Buffer.byteLength(line, "utf8")),
          ),
          port: 0,
        },
      );
      const queuedIngress = yield* makeViewServerTcpPublishIngress(viewServer, slowPublishClient, {
        maxGlobalQueuedCommands: 2,
        maxQueuedCommands: 2,
        port: 0,
      });
      const globalQueuedIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        globalPublishClient,
        {
          maxGlobalQueuedCommands: 1,
          maxQueuedCommands: 2,
          port: 0,
        },
      );
      const disconnectedIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        disconnectedPublishClient,
        { port: 0 },
      );
      const connectionCappedIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        runtimeCore.internalClient,
        {
          maxConnections: 1,
          port: 0,
        },
      );
      const closedQueueIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        closedQueuePublishClient,
        {
          maxLineBytes: Buffer.byteLength(commandLine, "utf8"),
          maxQueuedCommands: 1,
          port: 0,
        },
      );
      const oversizedSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(oversizedIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      oversizedSocket.write("this-line-is-too-large");
      const oversizedResponse = yield* readTcpPublishResponse(oversizedSocket).pipe(
        Effect.timeout("1 second"),
      );

      const oversizedCompleteLineSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(oversizedCompleteLineIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      oversizedCompleteLineSocket.write("this-line-is-too-large\n");
      const oversizedCompleteLineResponse = yield* readTcpPublishResponse(
        oversizedCompleteLineSocket,
      ).pipe(Effect.timeout("1 second"));

      const coalescedSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(coalescedIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      coalescedSocket.write(fifoCommandLines.join(""));
      const coalescedResponses = yield* readTcpPublishResponses(coalescedSocket, 2).pipe(
        Effect.timeout("1 second"),
      );

      const queuedSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(queuedIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      queuedSocket.write(commandLine);
      yield* Deferred.await(slowPublishStarted);
      queuedSocket.write(commandLine);
      queuedSocket.write(commandLine);
      const queuedResponse = yield* readTcpPublishResponse(queuedSocket).pipe(
        Effect.timeout("1 second"),
      );
      yield* Deferred.await(slowPublishInterrupted).pipe(Effect.timeout("1 second"));
      const queueRecovery = yield* Effect.acquireUseRelease(
        sendTcpPublishCommand(queuedIngress.url, {
          op: "publish",
          topic: "orders",
          row: order("queue-recovery-held", 10),
        }).pipe(Effect.forkChild({ startImmediately: true })),
        (heldRecoveryFiber) =>
          Effect.gen(function* () {
            yield* Deferred.await(recoveryPublishStarted).pipe(Effect.timeout("1 second"));
            const recoveredQueuedResponse = yield* sendTcpPublishCommand(queuedIngress.url, {
              op: "publish",
              topic: "orders",
              row: order("queue-recovered", 10),
            });
            yield* Deferred.succeed(allowRecoveryPublish, undefined);
            const heldRecoveryResponse = yield* Fiber.join(heldRecoveryFiber).pipe(
              Effect.timeout("1 second"),
            );
            return {
              heldRecoveryResponse,
              recoveredQueuedResponse,
              recoveryPublishFinalized: yield* Deferred.isDone(recoveryPublishFinalized),
            };
          }),
        (heldRecoveryFiber) =>
          Deferred.succeed(allowRecoveryPublish, undefined).pipe(
            Effect.andThen(Fiber.interrupt(heldRecoveryFiber)),
            Effect.asVoid,
          ),
      );

      const globalQueuedFirstSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(globalQueuedIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      const globalQueuedSecondSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(globalQueuedIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      globalQueuedFirstSocket.write(commandLine);
      yield* Deferred.await(globalPublishStarted);
      globalQueuedSecondSocket.write(commandLine);
      const globalQueuedResponse = yield* readTcpPublishResponse(globalQueuedSecondSocket).pipe(
        Effect.timeout("1 second"),
      );

      const disconnectedSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(disconnectedIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      disconnectedSocket.write(commandLine);
      yield* Deferred.await(disconnectedPublishStarted);
      disconnectedSocket.destroy();
      yield* Deferred.await(disconnectedPublishInterrupted).pipe(Effect.timeout("1 second"));

      const closedQueueSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(closedQueueIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      const oversizedPartialLine = "x".repeat(Buffer.byteLength(commandLine, "utf8") + 1);
      const closedQueueResponseText = yield* Effect.callback<string>((resume) => {
        let responseText = "";
        const onData = (chunk: string): void => {
          responseText += chunk;
        };
        const onClose = (): void => {
          closedQueueSocket.off("data", onData);
          resume(Effect.succeed(responseText));
        };
        closedQueueSocket.on("data", onData);
        closedQueueSocket.once("close", onClose);
        closedQueueSocket.write(
          `${commandLine}${commandLine}${commandLine}${oversizedPartialLine}`,
        );
        return Effect.sync(() => {
          closedQueueSocket.off("data", onData);
          closedQueueSocket.off("close", onClose);
        });
      }).pipe(Effect.timeout("1 second"));
      const closedQueueResponses = yield* Effect.forEach(
        closedQueueResponseText.trimEnd().split("\n"),
        (line) => Effect.sync((): unknown => JSON.parse(line)),
      );

      const heldConnectionCappedSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(connectionCappedIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      const rejectedConnectionCappedSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(connectionCappedIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      const connectionCappedResponse = yield* readTcpPublishResponse(
        rejectedConnectionCappedSocket,
      ).pipe(Effect.timeout("1 second"));
      yield* Effect.sleep("10 millis");

      expect(oversizedResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish command exceeded 8 bytes without a newline.",
          phase: "backpressure",
        },
      });
      expect(oversizedCompleteLineResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish command exceeded 8 bytes.",
          phase: "backpressure",
        },
      });
      expect({ coalescedResponses, fifoIds }).toStrictEqual({
        coalescedResponses: [{ ok: true }, { ok: true }],
        fifoIds: ["fifo-1", "fifo-2"],
      });
      expect(queuedResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish command queue exceeded 2 commands.",
          phase: "backpressure",
        },
      });
      expect(queueRecovery).toStrictEqual({
        heldRecoveryResponse: { ok: true },
        recoveredQueuedResponse: { ok: true },
        recoveryPublishFinalized: true,
      });
      expect(globalQueuedResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish global command queue exceeded 1 commands.",
          phase: "backpressure",
        },
      });
      expect(closedQueueResponses).toStrictEqual([
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command queue exceeded 1 commands.",
            phase: "backpressure",
          },
        },
      ]);
      expect(connectionCappedResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish connection count exceeded 1 sockets.",
          phase: "backpressure",
        },
      });
      expect(oversizedSocket.destroyed).toBe(true);
      expect(oversizedCompleteLineSocket.destroyed).toBe(true);
      expect(rejectedConnectionCappedSocket.destroyed).toBe(true);

      heldConnectionCappedSocket.destroy();
      yield* connectionCappedIngress.close;
      yield* closedQueueIngress.close.pipe(Effect.timeout("1 second"));
      yield* disconnectedIngress.close;
      yield* globalQueuedIngress.close.pipe(Effect.timeout("1 second"));
      yield* Deferred.await(globalPublishInterrupted);
      yield* queuedIngress.close.pipe(Effect.timeout("1 second"));
      yield* queuedIngress.close.pipe(Effect.timeout("1 second"));
      yield* Deferred.await(slowPublishInterrupted);
      yield* coalescedIngress.close;
      yield* oversizedCompleteLineIngress.close;
      yield* oversizedIngress.close;
      yield* oversizedIngress.close;
      yield* runtimeCore.close;
    }),
  );

  it.effect("keeps TCP response backpressure non-fatal through deterministic internals", () =>
    Effect.gen(function* () {
      const writtenChunks: Array<string> = [];
      const listeners: Partial<Record<"close" | "error", () => void>> = {};
      let writeCallback: () => void = () => undefined;
      const state = { closed: false };

      const pendingWrite = yield* writeTcpJsonLine(
        {
          destroyed: false,
          off: (event) => {
            delete listeners[event];
            return new Net.Socket();
          },
          once: (event, listener) => {
            listeners[event] = () => listener();
            return new Net.Socket();
          },
          write: (chunk, callback) => {
            writtenChunks.push(chunk);
            writeCallback = callback;
            return false;
          },
        },
        state,
        { ok: true },
      ).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Effect.yieldNow;
      expect({
        listenerNames: Object.keys(listeners),
        stateClosed: state.closed,
        writtenChunks,
      }).toStrictEqual({
        listenerNames: ["close", "error"],
        stateClosed: false,
        writtenChunks: ['{"ok":true}\n'],
      });
      writeCallback();
      writeCallback();
      yield* Fiber.join(pendingWrite);
      const cancelledChunks: Array<string> = [];
      const cancelledListeners: Partial<Record<"close" | "error", () => void>> = {};
      const cancelledWrite = yield* writeTcpJsonLine(
        {
          destroyed: false,
          off: (event) => {
            delete cancelledListeners[event];
            return new Net.Socket();
          },
          once: (event, listener) => {
            cancelledListeners[event] = () => listener();
            return new Net.Socket();
          },
          write: (chunk) => {
            cancelledChunks.push(chunk);
            return false;
          },
        },
        state,
        { ok: "cancelled" },
      ).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Effect.yieldNow;
      expect(Object.keys(cancelledListeners)).toStrictEqual(["close", "error"]);
      yield* Fiber.interrupt(cancelledWrite);
      expect({
        cancelledChunks,
        cancelledListeners: Object.keys(cancelledListeners),
      }).toStrictEqual({
        cancelledChunks: ['{"ok":"cancelled"}\n'],
        cancelledListeners: [],
      });
      yield* writeTcpJsonLine(
        {
          destroyed: true,
          off: () => new Net.Socket(),
          once: () => new Net.Socket(),
          write: (chunk, callback) => {
            writtenChunks.push(chunk);
            callback();
            return true;
          },
        },
        state,
        { ok: false },
      );
      state.closed = true;
      yield* writeTcpJsonLine(
        {
          destroyed: false,
          off: () => new Net.Socket(),
          once: () => new Net.Socket(),
          write: (chunk, callback) => {
            writtenChunks.push(chunk);
            callback();
            return true;
          },
        },
        state,
        { ok: false },
      );

      expect({
        listenerNames: Object.keys(listeners),
        stateClosed: state.closed,
        writtenChunks,
      }).toStrictEqual({
        listenerNames: [],
        stateClosed: true,
        writtenChunks: ['{"ok":true}\n'],
      });
    }),
  );

  it.live("returns typed TCP publish errors for runtime mutation failures", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      const defectPublishClient: ViewServerRuntimeCoreInternalClient<typeof viewServer.topics> = {
        ...runtimeCore.internalClient,
        publishManyDecodedRows: () => Effect.die("tcp publish defect"),
      };
      const unavailablePublishClient: ViewServerRuntimeCoreInternalClient<
        typeof viewServer.topics
      > = {
        ...runtimeCore.internalClient,
        publishManyDecodedRows: () =>
          Effect.fail({
            _tag: "ViewServerRuntimeError",
            code: "RuntimeUnavailable",
            message: "runtime unavailable for tcp test",
          } satisfies ViewServerRuntimeError),
      };
      const typedFailurePublishClient: ViewServerRuntimeCoreInternalClient<
        typeof viewServer.topics
      > = {
        ...runtimeCore.internalClient,
      };
      Object.defineProperty(typedFailurePublishClient, "publishManyDecodedRows", {
        value: () => Effect.fail(new RuntimeTestFailure({ message: "runtime typed failure" })),
      });
      const defectPublishIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        defectPublishClient,
        { port: 0 },
      );
      const unavailablePublishIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        unavailablePublishClient,
        { port: 0 },
      );
      const typedFailurePublishIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        typedFailurePublishClient,
        { port: 0 },
      );

      const responses = [
        yield* sendTcpPublishCommand(defectPublishIngress.url, {
          op: "publish",
          topic: "orders",
          row: order("a", 10),
        }),
        yield* sendTcpPublishCommand(unavailablePublishIngress.url, {
          op: "publish",
          topic: "orders",
          row: order("a", 10),
        }),
        yield* sendTcpPublishCommand(typedFailurePublishIngress.url, {
          op: "publish",
          topic: "orders",
          row: order("a", 10),
        }),
      ];

      expect(responses).toStrictEqual([
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command failed with an untyped cause.",
            phase: "runtime",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerRuntimeError",
            code: "RuntimeUnavailable",
            message: "runtime unavailable for tcp test",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish runtime publish failed for topic orders.",
            phase: "runtime",
            topic: "orders",
          },
        },
      ]);

      yield* typedFailurePublishIngress.close;
      yield* unavailablePublishIngress.close;
      yield* defectPublishIngress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("closes TCP publish servers on steady-state server errors", () =>
    Effect.acquireUseRelease(
      makeViewServerRuntimeCoreInternal(viewServer, {}),
      (runtimeCore) =>
        Effect.gen(function* () {
          const closeEntered = yield* Deferred.make<void>();
          let emitLateStartupError = (): void => undefined;
          let releaseServerClose = (): void => undefined;
          let server: Net.Server | undefined;
          let listenerCountBefore = -1;
          let listenerCountWhenStartupListenerRemoved = -1;
          yield* Effect.acquireUseRelease(
            makeViewServerTcpPublishIngressWithServerFactory(
              viewServer,
              runtimeCore.internalClient,
              { port: 0 },
              (connectionListener) => {
                const created = Net.createServer(connectionListener);
                const closeServer = created.close.bind(created);
                const offServer = created.off.bind(created);
                const onceServer = created.once.bind(created);
                server = created;
                listenerCountBefore = created.listenerCount("error");
                const observeStartupError = (
                  eventName: string | symbol,
                  listener: (...args: ReadonlyArray<unknown>) => void,
                ): Net.Server => {
                  if (eventName === "error") {
                    emitLateStartupError = () => listener(new Error("late startup error"));
                  }
                  return onceServer(eventName, listener);
                };
                const observeStartupListenerRemoval = (
                  eventName: string | symbol,
                  listener: (...args: ReadonlyArray<unknown>) => void,
                ): Net.Server => {
                  const result = offServer(eventName, listener);
                  if (eventName === "error" && listenerCountWhenStartupListenerRemoved === -1) {
                    listenerCountWhenStartupListenerRemoved = created.listenerCount("error");
                  }
                  return result;
                };
                Object.defineProperty(created, "off", {
                  value: observeStartupListenerRemoval,
                });
                Object.defineProperty(created, "once", {
                  value: observeStartupError,
                });
                Object.defineProperty(created, "close", {
                  value: (callback?: (error?: Error) => void) => {
                    Deferred.doneUnsafe(closeEntered, Effect.void);
                    let released = false;
                    releaseServerClose = () => {
                      if (released) {
                        return;
                      }
                      released = true;
                      closeServer(callback);
                    };
                    return created;
                  },
                });
                return created;
              },
            ),
            (ingress) =>
              Effect.gen(function* () {
                const ownedServer = yield* Effect.fromNullishOr(server);
                const listenerCountAfter = ownedServer.listenerCount("error");
                yield* Effect.sync(emitLateStartupError);
                const listenerCountAfterLateStartupError = ownedServer.listenerCount("error");

                ownedServer.emit("error", new Error("tcp test steady-state failure"));
                yield* Deferred.await(closeEntered).pipe(Effect.timeout("1 second"));
                const listenerCountDuringDrain = ownedServer.listenerCount("error");
                expect(() =>
                  ownedServer.emit("error", new Error("tcp test error during drain")),
                ).not.toThrow();
                const listenerCountAfterSecondError = ownedServer.listenerCount("error");
                yield* Effect.sync(releaseServerClose);
                yield* ingress.close.pipe(Effect.timeout("1 second"));

                expect({
                  listenerCountAfter,
                  listenerCountAfterClose: ownedServer.listenerCount("error"),
                  listenerCountAfterLateStartupError,
                  listenerCountAfterSecondError,
                  listenerCountBefore,
                  listenerCountDuringDrain,
                  listenerCountWhenStartupListenerRemoved,
                }).toStrictEqual({
                  listenerCountAfter: 1,
                  listenerCountAfterClose: 0,
                  listenerCountAfterLateStartupError: 1,
                  listenerCountAfterSecondError: 1,
                  listenerCountBefore: 0,
                  listenerCountDuringDrain: 1,
                  listenerCountWhenStartupListenerRemoved: 1,
                });
              }),
            (ingress) =>
              Effect.sync(releaseServerClose).pipe(Effect.andThen(ingress.close), Effect.asVoid),
          );
        }),
      (runtimeCore) => runtimeCore.close,
    ),
  );

  it.live("guards accepted TCP sockets before Effect ownership transfer", () =>
    Effect.acquireUseRelease(
      makeViewServerRuntimeCoreInternal(viewServer, {}),
      (runtimeCore) =>
        Effect.gen(function* () {
          let acceptSocket: ((socket: Net.Socket) => void) | undefined;
          yield* Effect.acquireUseRelease(
            makeViewServerTcpPublishIngressWithServerFactory(
              viewServer,
              runtimeCore.internalClient,
              { maxConnections: 1, port: 0 },
              (connectionListener) => {
                acceptSocket = connectionListener;
                return Net.createServer(connectionListener);
              },
            ),
            () =>
              Effect.gen(function* () {
                const accept = yield* Effect.fromNullishOr(acceptSocket);
                const closedBeforeEffectOwnership = new Net.Socket();
                let closedBeforeEffectOwnershipReads = 0;
                let closedBeforeEffectOwnershipDestroying = false;
                let closedBeforeEffectOwnershipErrorListeners = -1;
                let closedBeforeEffectOwnershipTouches = 0;
                Object.defineProperty(closedBeforeEffectOwnership, "destroyed", {
                  get: () => {
                    closedBeforeEffectOwnershipReads += 1;
                    return closedBeforeEffectOwnershipReads >= 2;
                  },
                });
                Object.defineProperty(closedBeforeEffectOwnership, "setEncoding", {
                  value: (_encoding: BufferEncoding) => {
                    closedBeforeEffectOwnershipTouches += 1;
                    return closedBeforeEffectOwnership;
                  },
                });
                closedBeforeEffectOwnership.destroy = () => {
                  if (closedBeforeEffectOwnershipDestroying) {
                    return closedBeforeEffectOwnership;
                  }
                  closedBeforeEffectOwnershipDestroying = true;
                  closedBeforeEffectOwnershipErrorListeners =
                    closedBeforeEffectOwnership.listenerCount("error");
                  closedBeforeEffectOwnership.emit(
                    "error",
                    new Error("destroyed before Effect ownership"),
                  );
                  closedBeforeEffectOwnership.emit("close");
                  return closedBeforeEffectOwnership;
                };
                accept(closedBeforeEffectOwnership);

                const closedBeforeTransfer = new Net.Socket();
                let closedBeforeTransferReads = 0;
                let closedBeforeTransferDestroying = false;
                let closedBeforeTransferErrorListeners = -1;
                let closedBeforeTransferTouches = 0;
                Object.defineProperty(closedBeforeTransfer, "destroyed", {
                  get: () => {
                    closedBeforeTransferReads += 1;
                    return closedBeforeTransferReads >= 3;
                  },
                });
                Object.defineProperty(closedBeforeTransfer, "setEncoding", {
                  value: (_encoding: BufferEncoding) => {
                    closedBeforeTransferTouches += 1;
                    return closedBeforeTransfer;
                  },
                });
                closedBeforeTransfer.destroy = () => {
                  if (closedBeforeTransferDestroying) {
                    return closedBeforeTransfer;
                  }
                  closedBeforeTransferDestroying = true;
                  closedBeforeTransferErrorListeners = closedBeforeTransfer.listenerCount("error");
                  closedBeforeTransfer.emit("error", new Error("destroyed before active transfer"));
                  closedBeforeTransfer.emit("close");
                  return closedBeforeTransfer;
                };
                accept(closedBeforeTransfer);

                const resetBeforeTransfer = new Net.Socket();
                const resetSetEncoding = resetBeforeTransfer.setEncoding.bind(resetBeforeTransfer);
                let errorListenersAtReset = -1;
                Object.defineProperty(resetBeforeTransfer, "setEncoding", {
                  value: (encoding: BufferEncoding) => {
                    errorListenersAtReset = resetBeforeTransfer.listenerCount("error");
                    resetBeforeTransfer.emit("error", new Error("reset before ownership transfer"));
                    resetBeforeTransfer.emit("close");
                    return resetSetEncoding(encoding);
                  },
                });
                accept(resetBeforeTransfer);

                expect(errorListenersAtReset).toBe(1);

                const acceptedTouched = yield* Deferred.make<void>();
                const accepted = new Net.Socket();
                const setEncoding = accepted.setEncoding.bind(accepted);
                let errorListenersAtFirstEffectTouch = -1;
                Object.defineProperty(accepted, "setEncoding", {
                  value: (encoding: BufferEncoding) => {
                    errorListenersAtFirstEffectTouch = accepted.listenerCount("error");
                    const result = setEncoding(encoding);
                    Deferred.doneUnsafe(acceptedTouched, Effect.void);
                    return result;
                  },
                });

                accept(accepted);
                yield* Deferred.await(acceptedTouched).pipe(Effect.timeout("1 second"));
                yield* Effect.yieldNow;

                expect({
                  closedBeforeEffectOwnershipErrorListeners,
                  closedBeforeEffectOwnershipListeners:
                    closedBeforeEffectOwnership.listenerCount("close") +
                    closedBeforeEffectOwnership.listenerCount("error"),
                  closedBeforeEffectOwnershipTouches,
                  closedBeforeTransferErrorListeners,
                  closedBeforeTransferListeners:
                    closedBeforeTransfer.listenerCount("close") +
                    closedBeforeTransfer.listenerCount("error"),
                  closedBeforeTransferTouches,
                  destroyed: accepted.destroyed,
                  errorListenersAfterTransfer: accepted.listenerCount("error"),
                  errorListenersAtFirstEffectTouch,
                }).toStrictEqual({
                  closedBeforeEffectOwnershipErrorListeners: 1,
                  closedBeforeEffectOwnershipListeners: 0,
                  closedBeforeEffectOwnershipTouches: 0,
                  closedBeforeTransferErrorListeners: 1,
                  closedBeforeTransferListeners: 0,
                  closedBeforeTransferTouches: 0,
                  destroyed: false,
                  errorListenersAfterTransfer: 1,
                  errorListenersAtFirstEffectTouch: 1,
                });
                accepted.destroy();
              }),
            (ingress) => ingress.close,
          );
        }),
      (runtimeCore) => runtimeCore.close,
    ),
  );

  it.live("owns accepted socket errors and rejects late connections after close", () =>
    Effect.acquireUseRelease(
      makeViewServerRuntimeCoreInternal(viewServer, {}),
      (runtimeCore) =>
        Effect.gen(function* () {
          const acceptedSocketReady = yield* Deferred.make<void>();
          let acceptSocket: ((socket: Net.Socket) => void) | undefined;
          let acceptedSocket: Net.Socket | undefined;
          const ingress = yield* makeViewServerTcpPublishIngressWithServerFactory(
            viewServer,
            runtimeCore.internalClient,
            { maxConnections: 1, port: 0 },
            (connectionListener) => {
              acceptSocket = connectionListener;
              const server = Net.createServer(connectionListener);
              server.on("connection", (socket) => {
                acceptedSocket = socket;
                Deferred.doneUnsafe(acceptedSocketReady, Effect.void);
              });
              return server;
            },
          );
          const clientSocket = yield* connectTcpPublishSocket(ingress.url);
          const clientClosed = yield* Deferred.make<void>();
          clientSocket.once("close", () => Deferred.doneUnsafe(clientClosed, Effect.void));
          yield* Deferred.await(acceptedSocketReady).pipe(Effect.timeout("1 second"));
          const ownedAcceptedSocket = yield* Effect.fromNullishOr(acceptedSocket);
          yield* Effect.gen(function* () {
            while (ownedAcceptedSocket.listenerCount("error") === 0) {
              yield* Effect.sleep("1 millis");
            }
          }).pipe(Effect.timeout("1 second"));

          const pendingRejectedClosed = yield* Deferred.make<void>();
          const pendingRejectedSocket = new Net.Socket();
          let pendingRejectedDestroying = false;
          const pendingRejectedErrorListenersDuringDestroy: Array<number> = [];
          Object.defineProperty(pendingRejectedSocket, "end", {
            value: (_chunk: string, callback: () => void) => {
              callback();
              return pendingRejectedSocket;
            },
          });
          pendingRejectedSocket.destroy = () => {
            if (pendingRejectedDestroying) {
              return pendingRejectedSocket;
            }
            pendingRejectedErrorListenersDuringDestroy.push(
              pendingRejectedSocket.listenerCount("error"),
            );
            if (pendingRejectedErrorListenersDuringDestroy.length === 2) {
              pendingRejectedDestroying = true;
              pendingRejectedSocket.emit("error", new Error("error while rejection closes"));
              pendingRejectedSocket.emit("close");
              Deferred.doneUnsafe(pendingRejectedClosed, Effect.void);
            }
            return pendingRejectedSocket;
          };
          const acceptPendingSocket = yield* Effect.fromNullishOr(acceptSocket);
          acceptPendingSocket(pendingRejectedSocket);
          yield* Deferred.await(pendingRejectedClosed).pipe(Effect.timeout("1 second"));

          const pendingCloseStarted = yield* Deferred.make<void>();
          const pendingDuringCloseSocket = new Net.Socket();
          let pendingDuringCloseDestroying = false;
          let pendingDuringCloseErrorListeners = -1;
          Object.defineProperty(pendingDuringCloseSocket, "end", {
            value: (_chunk: string, _callback: () => void) => {
              Deferred.doneUnsafe(pendingCloseStarted, Effect.void);
              return pendingDuringCloseSocket;
            },
          });
          pendingDuringCloseSocket.destroy = () => {
            if (pendingDuringCloseDestroying) {
              return pendingDuringCloseSocket;
            }
            pendingDuringCloseDestroying = true;
            pendingDuringCloseErrorListeners = pendingDuringCloseSocket.listenerCount("error");
            pendingDuringCloseSocket.emit("error", new Error("error while ingress closes pending"));
            pendingDuringCloseSocket.emit("close");
            return pendingDuringCloseSocket;
          };
          acceptPendingSocket(pendingDuringCloseSocket);
          yield* Deferred.await(pendingCloseStarted).pipe(Effect.timeout("1 second"));

          ownedAcceptedSocket.emit("error", new Error("accepted socket test error"));
          yield* Deferred.await(clientClosed).pipe(Effect.timeout("1 second"));
          yield* ingress.close;

          const lateSocket = new Net.Socket();
          let lateSocketDestroying = false;
          const lateSocketDestroyed: Array<"socket"> = [];
          let lateSocketErrorListenersDuringDestroy = -1;
          lateSocket.destroy = () => {
            if (lateSocketDestroying) {
              return lateSocket;
            }
            lateSocketDestroying = true;
            lateSocketDestroyed.push("socket");
            lateSocketErrorListenersDuringDestroy = lateSocket.listenerCount("error");
            lateSocket.emit("error", new Error("error while late socket closes"));
            lateSocket.emit("close");
            return lateSocket;
          };
          const acceptLateSocket = yield* Effect.fromNullishOr(acceptSocket);
          acceptLateSocket(lateSocket);

          expect({
            clientSocketDestroyed: clientSocket.destroyed,
            lateSocketErrorListenersDuringDestroy,
            lateSocketListeners:
              lateSocket.listenerCount("close") + lateSocket.listenerCount("error"),
            lateSocketDestroyed,
            pendingRejectedErrorListenersDuringDestroy,
            pendingRejectedSocketListeners:
              pendingRejectedSocket.listenerCount("close") +
              pendingRejectedSocket.listenerCount("error"),
            pendingDuringCloseErrorListeners,
            pendingDuringCloseSocketListeners:
              pendingDuringCloseSocket.listenerCount("close") +
              pendingDuringCloseSocket.listenerCount("error"),
          }).toStrictEqual({
            clientSocketDestroyed: true,
            lateSocketErrorListenersDuringDestroy: 1,
            lateSocketListeners: 0,
            lateSocketDestroyed: ["socket"],
            pendingRejectedErrorListenersDuringDestroy: [1, 1],
            pendingRejectedSocketListeners: 0,
            pendingDuringCloseErrorListeners: 2,
            pendingDuringCloseSocketListeners: 0,
          });
          clientSocket.destroy();
        }),
      (runtimeCore) => runtimeCore.close,
    ),
  );

  it.live(
    "joins listener, explicit ingress, and runtime shutdown before runtime-core teardown",
    () =>
      Effect.gen(function* () {
        const commandStarted = yield* Deferred.make<void>();
        const commandInterruptStarted = yield* Deferred.make<void>();
        const allowCommandFinalize = yield* Deferred.make<void>();
        const commandFinalized = yield* Deferred.make<void>();
        const explicitIngressCloseCompleted = yield* Deferred.make<void>();
        const runtimeIngressCloseEntered = yield* Deferred.make<void>();
        const runtimeCoreCloseStarted = yield* Deferred.make<void>();
        const runtimeCloseCompleted = yield* Deferred.make<void>();
        let tcpServerCloseCount = 0;
        let tcpServer: Net.Server | undefined;
        let explicitIngressClose: Effect.Effect<void> | undefined;
        const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
          ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
          makeRuntimeCore: (config, options) =>
            makeViewServerRuntimeCoreInternal(config, options).pipe(
              Effect.map((runtimeCore) => ({
                ...runtimeCore,
                internalClient: {
                  ...runtimeCore.internalClient,
                  publishManyDecodedRows: () =>
                    Deferred.succeed(commandStarted, undefined).pipe(
                      Effect.andThen(Effect.never),
                      Effect.ensuring(
                        Deferred.succeed(commandInterruptStarted, undefined).pipe(
                          Effect.andThen(Deferred.await(allowCommandFinalize)),
                          Effect.andThen(Deferred.succeed(commandFinalized, undefined)),
                        ),
                      ),
                    ),
                },
                close: Deferred.succeed(runtimeCoreCloseStarted, undefined).pipe(
                  Effect.andThen(runtimeCore.close),
                ),
              })),
            ),
          makeServer: () =>
            Effect.succeed({
              url: "ws://127.0.0.1:0/rpc",
              healthUrl: "http://127.0.0.1:0/health",
              metricsUrl: "http://127.0.0.1:0/metrics",
              close: Effect.void,
            }),
          makeTcpPublishIngress: (config, client, options) =>
            makeViewServerTcpPublishIngressWithServerFactory(
              config,
              client,
              options,
              (connectionListener) => {
                const server = Net.createServer(connectionListener);
                const closeServer = server.close.bind(server);
                const countedClose: Net.Server["close"] = (callback) => {
                  tcpServerCloseCount += 1;
                  return closeServer(callback);
                };
                Object.defineProperty(server, "close", {
                  value: countedClose,
                });
                tcpServer = server;
                return server;
              },
            ).pipe(
              Effect.map((ingress) => {
                explicitIngressClose = ingress.close;
                return {
                  ...ingress,
                  close: Deferred.succeed(runtimeIngressCloseEntered, undefined).pipe(
                    Effect.andThen(ingress.close),
                  ),
                };
              }),
            ),
        };
        yield* Effect.acquireUseRelease(
          makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
            tcpPublishPort: 0,
            websocketPort: 0,
          }),
          (runtime) =>
            Effect.gen(function* () {
              const tcpPublishUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);
              const server = yield* Effect.fromNullishOr(tcpServer);
              const ingressClose = yield* Effect.fromNullishOr(explicitIngressClose);
              yield* Effect.acquireUseRelease(
                connectTcpPublishSocket(tcpPublishUrl),
                (socket) =>
                  Effect.gen(function* () {
                    socket.write(
                      `${JSON.stringify({
                        op: "publish",
                        topic: "orders",
                        row: order("shutdown-overlap", 10),
                      })}\n`,
                    );
                    yield* Deferred.await(commandStarted).pipe(Effect.timeout("1 second"));

                    server.emit("error", new Error("tcp shutdown overlap"));
                    yield* Deferred.await(commandInterruptStarted).pipe(Effect.timeout("1 second"));
                    const explicitIngressCloseFiber = yield* ingressClose.pipe(
                      Effect.ensuring(Deferred.succeed(explicitIngressCloseCompleted, undefined)),
                      Effect.forkChild({ startImmediately: true }),
                    );
                    const runtimeCloseFiber = yield* runtime.close.pipe(
                      Effect.ensuring(Deferred.succeed(runtimeCloseCompleted, undefined)),
                      Effect.forkChild({ startImmediately: true }),
                    );
                    yield* Deferred.await(runtimeIngressCloseEntered).pipe(
                      Effect.timeout("1 second"),
                    );

                    expect({
                      commandFinalized: yield* Deferred.isDone(commandFinalized),
                      explicitIngressCloseCompleted: yield* Deferred.isDone(
                        explicitIngressCloseCompleted,
                      ),
                      runtimeCloseCompleted: yield* Deferred.isDone(runtimeCloseCompleted),
                      runtimeCoreCloseStarted: yield* Deferred.isDone(runtimeCoreCloseStarted),
                    }).toStrictEqual({
                      commandFinalized: false,
                      explicitIngressCloseCompleted: false,
                      runtimeCloseCompleted: false,
                      runtimeCoreCloseStarted: false,
                    });

                    yield* Deferred.succeed(allowCommandFinalize, undefined);
                    yield* Fiber.join(explicitIngressCloseFiber).pipe(Effect.timeout("1 second"));
                    yield* Fiber.join(runtimeCloseFiber).pipe(Effect.timeout("1 second"));
                    const priorCloseExit = Effect.runSyncExit(ingressClose);

                    expect({
                      commandFinalized: yield* Deferred.isDone(commandFinalized),
                      explicitIngressCloseCompleted: yield* Deferred.isDone(
                        explicitIngressCloseCompleted,
                      ),
                      priorCloseSucceeded: Exit.isSuccess(priorCloseExit),
                      runtimeCloseCompleted: yield* Deferred.isDone(runtimeCloseCompleted),
                      runtimeCoreCloseStarted: yield* Deferred.isDone(runtimeCoreCloseStarted),
                      tcpServerCloseCount,
                    }).toStrictEqual({
                      commandFinalized: true,
                      explicitIngressCloseCompleted: true,
                      priorCloseSucceeded: true,
                      runtimeCloseCompleted: true,
                      runtimeCoreCloseStarted: true,
                      tcpServerCloseCount: 1,
                    });
                  }).pipe(Effect.ensuring(Deferred.succeed(allowCommandFinalize, undefined))),
                (socket) => Effect.sync(() => socket.destroy()),
              );
            }),
          (runtime) =>
            Deferred.succeed(allowCommandFinalize, undefined).pipe(Effect.andThen(runtime.close)),
        );
      }),
  );

  it.live("closes TCP publish startup interrupted before listen completes", () =>
    Effect.acquireUseRelease(
      makeViewServerRuntimeCoreInternal(viewServer, {}),
      (runtimeCore) =>
        Effect.gen(function* () {
          const listenStarted = yield* Deferred.make<void>();
          const serverClosed = yield* Deferred.make<void>();
          let completeListen = (): void => undefined;
          let server: Net.Server | undefined;
          const startupFiber = yield* makeViewServerTcpPublishIngressWithServerFactory(
            viewServer,
            runtimeCore.internalClient,
            { port: 0 },
            (connectionListener) => {
              const createdServer = Net.createServer(connectionListener);
              server = createdServer;
              Object.defineProperty(createdServer, "listen", {
                value: (_options: Net.ListenOptions, callback: () => void) => {
                  completeListen = callback;
                  Deferred.doneUnsafe(listenStarted, Effect.void);
                  return createdServer;
                },
              });
              Object.defineProperty(createdServer, "close", {
                value: (callback?: (error?: Error) => void) => {
                  Deferred.doneUnsafe(serverClosed, Effect.void);
                  callback?.();
                  return createdServer;
                },
              });
              return createdServer;
            },
          ).pipe(Effect.forkChild({ startImmediately: true }));

          yield* Effect.gen(function* () {
            yield* Deferred.await(listenStarted).pipe(Effect.timeout("1 second"));
            yield* Fiber.interrupt(startupFiber);
            yield* Effect.sync(completeListen);
            const ownedServer = yield* Effect.fromNullishOr(server);
            expect({
              errorListenerCount: ownedServer.listenerCount("error"),
              serverClosed: yield* Deferred.isDone(serverClosed),
            }).toStrictEqual({
              errorListenerCount: 0,
              serverClosed: true,
            });
          }).pipe(
            Effect.ensuring(
              Fiber.interrupt(startupFiber).pipe(
                Effect.andThen(Effect.sync(() => server?.removeAllListeners())),
              ),
            ),
          );
        }),
      (runtimeCore) => runtimeCore.close,
    ),
  );

  it.live("rolls back a listening TCP publish server when post-listen setup fails", () =>
    Effect.acquireUseRelease(
      makeViewServerRuntimeCoreInternal(viewServer, {}),
      (runtimeCore) =>
        Effect.gen(function* () {
          const serverClosed = yield* Deferred.make<void>();
          let server: Net.Server | undefined;
          const exit = yield* makeViewServerTcpPublishIngressWithServerFactory(
            viewServer,
            runtimeCore.internalClient,
            { port: 0 },
            (connectionListener) => {
              const createdServer = Net.createServer(connectionListener);
              server = createdServer;
              createdServer.once("close", () => Deferred.doneUnsafe(serverClosed, Effect.void));
              Object.defineProperty(createdServer, "address", {
                value: () => null,
              });
              return createdServer;
            },
          ).pipe(Effect.exit);
          yield* Deferred.await(serverClosed).pipe(Effect.timeout("1 second"));
          const ownedServer = yield* Effect.fromNullishOr(server);

          expect({
            errorListenerCount: ownedServer.listenerCount("error"),
            failed: Exit.isFailure(exit),
            listening: ownedServer.listening,
          }).toStrictEqual({
            errorListenerCount: 0,
            failed: true,
            listening: false,
          });
        }),
      (runtimeCore) => runtimeCore.close,
    ),
  );

  it.live("fails startup when the TCP publish port is already bound", () =>
    Effect.gen(function* () {
      const reserved = yield* Effect.acquireRelease(reserveTcpPort(), ({ server }) =>
        closeTestTcpServer(server),
      );
      const exit = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        tcpPublishPort: reserved.port,
        websocketPort: 0,
      }).pipe(Effect.exit);

      expect(
        Exit.isFailure(exit)
          ? Option.match(Cause.findErrorOption(exit.cause), {
              onNone: () => null,
              onSome: (error) => ({
                message: error instanceof Error ? error.message : undefined,
                phase: error instanceof ViewServerTcpPublishIngressError ? error.phase : undefined,
                tag: error instanceof ViewServerTcpPublishIngressError ? error._tag : undefined,
              }),
            })
          : null,
      ).toStrictEqual({
        message: "TCP publish server failed to listen.",
        phase: "listen",
        tag: "ViewServerTcpPublishIngressError",
      });
    }),
  );

  it.live("closes the TCP publish endpoint with runtime shutdown", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpPublishUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);
      const response = yield* sendTcpPublishCommand(tcpPublishUrl, {
        op: "publish",
        topic: "orders",
        row: order("a", 10),
      });

      expect(response).toStrictEqual({ ok: true });

      const heldSocket = yield* connectTcpPublishSocket(tcpPublishUrl);
      yield* runtime.close;
      heldSocket.destroy();
      const connectExit = yield* connectTcpPublishSocket(tcpPublishUrl).pipe(Effect.exit);
      expect(Exit.isFailure(connectExit)).toBe(true);
    }),
  );
});
