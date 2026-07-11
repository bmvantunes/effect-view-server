import { describe, expect, it } from "@effect/vitest";
import { Effect, Stream } from "effect";
import {
  callLeasedGrpcSourceAcquire,
  callLeasedGrpcSourceRelease,
  callLeasedGrpcSourceRequest,
  callMaterializedGrpcSourceAcquire,
  callMaterializedGrpcSourceRelease,
  callMaterializedGrpcSourceRequest,
  makeGrpcSourceInput,
  makeViewServerGrpcSourceError,
} from "./grpc-source-lifecycle";

const sourceInput = <Route>(request: unknown, route: Route) =>
  makeGrpcSourceInput({ name: "orders-client" }, request, route);

describe("gRPC source lifecycle", () => {
  it.effect("calls request, acquire, and release through one source lifecycle seam", () =>
    Effect.gen(function* () {
      const operations: Array<string> = [];
      const feed = {
        topic: "orders",
        request: () => {
          operations.push("request");
          return "orders-request";
        },
        acquire: (input: ReturnType<typeof sourceInput>) => {
          expect(input.request).toBe("orders-request");
          operations.push("acquire");
          return Stream.make("row-1", "row-2");
        },
        release: (input: ReturnType<typeof sourceInput>) =>
          Effect.sync(() => {
            expect(input.request).toBe("orders-request");
            operations.push("release");
          }),
      };

      const request = yield* callMaterializedGrpcSourceRequest("orders", feed);
      const stream = yield* callMaterializedGrpcSourceAcquire(
        "orders",
        feed,
        sourceInput(request, undefined),
      );
      const rows = yield* Stream.runCollect(stream);
      yield* callMaterializedGrpcSourceRelease("orders", feed, sourceInput(request, undefined));

      expect({
        operations,
        rows: Array.from(rows),
      }).toStrictEqual({
        operations: ["request", "acquire", "release"],
        rows: ["row-1", "row-2"],
      });
    }),
  );

  it.effect("normalizes request construction failures with source context", () =>
    Effect.gen(function* () {
      const feed = {
        topic: "orders",
        request: () => {
          throw "request exploded";
        },
        acquire: () => Stream.never,
      };

      const error = yield* Effect.flip(
        callLeasedGrpcSourceRequest("orders", feed, { region: "usa" }),
      );
      expect({
        _tag: error._tag,
        cause: error.cause,
        feedName: error.feedName,
        message: error.message,
        phase: error.phase,
        topic: error.topic,
      }).toStrictEqual({
        _tag: "ViewServerGrpcIngressError",
        cause: "request exploded",
        feedName: "orders",
        message: "gRPC leased feed request creation failed for orders",
        phase: "request",
        topic: "orders",
      });
    }),
  );

  it.effect("rejects acquire callbacks that do not return a stream", () =>
    Effect.gen(function* () {
      const feed = {
        topic: "orders",
        request: () => undefined,
        acquire: () => "not-a-stream",
      };

      const error = yield* Effect.flip(
        callMaterializedGrpcSourceAcquire("orders", feed, sourceInput(undefined, undefined)),
      );

      expect({
        _tag: error._tag,
        cause: error.cause,
        feedName: error.feedName,
        message: error.message,
        phase: error.phase,
        topic: error.topic,
      }).toStrictEqual({
        _tag: "ViewServerGrpcIngressError",
        cause: "not-a-stream",
        feedName: "orders",
        message: "gRPC feed acquire did not return a Stream for orders",
        phase: "acquire",
        topic: "orders",
      });
    }),
  );

  it.effect("normalizes acquire callback defects with source context", () =>
    Effect.gen(function* () {
      const feed = {
        topic: "orders",
        request: () => undefined,
        acquire: () => {
          throw "acquire exploded";
        },
      };

      const error = yield* Effect.flip(
        callLeasedGrpcSourceAcquire("orders", feed, sourceInput(undefined, { region: "usa" })),
      );

      expect({
        _tag: error._tag,
        cause: error.cause,
        feedName: error.feedName,
        message: error.message,
        phase: error.phase,
        topic: error.topic,
      }).toStrictEqual({
        _tag: "ViewServerGrpcIngressError",
        cause: "acquire exploded",
        feedName: "orders",
        message: "gRPC leased feed acquire failed for orders",
        phase: "acquire",
        topic: "orders",
      });
    }),
  );

  it.effect("treats missing release callbacks as a no-op", () =>
    Effect.gen(function* () {
      const feed = {
        topic: "orders",
        request: () => undefined,
        acquire: () => Stream.never,
      };

      const result = yield* callMaterializedGrpcSourceRelease(
        "orders",
        feed,
        sourceInput(undefined, undefined),
      );

      expect(result).toBeUndefined();
    }),
  );

  it.effect("normalizes release callback defects with source context", () =>
    Effect.gen(function* () {
      const feed = {
        topic: "orders",
        request: () => undefined,
        acquire: () => Stream.never,
        release: () => {
          throw "release exploded";
        },
      };

      const error = yield* Effect.flip(
        callMaterializedGrpcSourceRelease("orders", feed, sourceInput(undefined, undefined)),
      );

      expect({
        _tag: error._tag,
        cause: error.cause,
        feedName: error.feedName,
        message: error.message,
        phase: error.phase,
        topic: error.topic,
      }).toStrictEqual({
        _tag: "ViewServerGrpcIngressError",
        cause: "release exploded",
        feedName: "orders",
        message: "gRPC feed release failed for orders",
        phase: "release",
        topic: "orders",
      });
    }),
  );

  it.effect("rejects release callbacks that do not return an Effect", () =>
    Effect.gen(function* () {
      const feed = {
        topic: "orders",
        request: () => undefined,
        acquire: () => Stream.never,
        release: () => "not-an-effect",
      };

      const error = yield* Effect.flip(
        callLeasedGrpcSourceRelease("orders", feed, sourceInput(undefined, { region: "usa" })),
      );

      expect({
        _tag: error._tag,
        cause: error.cause,
        feedName: error.feedName,
        message: error.message,
        phase: error.phase,
        topic: error.topic,
      }).toStrictEqual({
        _tag: "ViewServerGrpcIngressError",
        cause: "not-an-effect",
        feedName: "orders",
        message: "gRPC leased feed release did not return an Effect for orders",
        phase: "release",
        topic: "orders",
      });
    }),
  );

  it.effect("normalizes release effect failures with source context", () =>
    Effect.gen(function* () {
      const feed = {
        topic: "orders",
        request: () => undefined,
        acquire: () => Stream.never,
        release: () => Effect.fail("release effect failed"),
      };

      const error = yield* Effect.flip(
        callLeasedGrpcSourceRelease("orders", feed, sourceInput(undefined, { region: "usa" })),
      );

      expect({
        _tag: error._tag,
        cause: error.cause,
        feedName: error.feedName,
        message: error.message,
        phase: error.phase,
        topic: error.topic,
      }).toStrictEqual({
        _tag: "ViewServerGrpcIngressError",
        cause: "release effect failed",
        feedName: "orders",
        message: "gRPC leased feed release failed for orders",
        phase: "release",
        topic: "orders",
      });
    }),
  );

  it("constructs source errors without a phase when the caller has no phase to report", () => {
    const error = makeViewServerGrpcSourceError({
      cause: "route",
      feedName: "orders",
      message: "leased route failed",
      topic: "orders",
    });

    expect({
      _tag: error._tag,
      cause: error.cause,
      feedName: error.feedName,
      message: error.message,
      phase: error.phase,
      topic: error.topic,
    }).toStrictEqual({
      _tag: "ViewServerGrpcIngressError",
      cause: "route",
      feedName: "orders",
      message: "leased route failed",
      phase: undefined,
      topic: "orders",
    });
  });

  it("creates isolated immutable session objects for each source input", () => {
    const firstInput = sourceInput({ requestId: "first" }, undefined);
    const secondInput = sourceInput({ requestId: "second" }, undefined);

    expect(firstInput.session).not.toBe(secondInput.session);
    expect(firstInput.session.forwardedHeaders).not.toBe(secondInput.session.forwardedHeaders);
    expect(firstInput.session.systemHeaders).not.toBe(secondInput.session.systemHeaders);
    expect({
      firstSessionFrozen: Object.isFrozen(firstInput.session),
      firstForwardedHeadersFrozen: Object.isFrozen(firstInput.session.forwardedHeaders),
      firstSystemHeadersFrozen: Object.isFrozen(firstInput.session.systemHeaders),
      secondSessionFrozen: Object.isFrozen(secondInput.session),
      secondForwardedHeadersFrozen: Object.isFrozen(secondInput.session.forwardedHeaders),
      secondSystemHeadersFrozen: Object.isFrozen(secondInput.session.systemHeaders),
    }).toStrictEqual({
      firstSessionFrozen: true,
      firstForwardedHeadersFrozen: true,
      firstSystemHeadersFrozen: true,
      secondSessionFrozen: true,
      secondForwardedHeadersFrozen: true,
      secondSystemHeadersFrozen: true,
    });
  });
});
