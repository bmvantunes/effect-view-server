import { Context, Effect, Option, Stream } from "effect";

export type HostHealthSnapshot = {
  readonly statusTag: string;
  readonly attempt: bigint | undefined;
  readonly retryAtNanos: bigint | undefined;
  readonly adapterMetrics: unknown;
  readonly rejectedItemCount: bigint;
  readonly failedSettlementCount: bigint;
  readonly lastRuntimeFailureTag: string | undefined;
  readonly lastExecutionFailure: unknown;
  readonly latestRejectionFailureTag: string | undefined;
  readonly latestRejectionFailure: unknown;
  readonly latestRejectionLocation: unknown;
  readonly latestRejectedAtNanos: bigint | undefined;
  readonly bufferHighWaterMark: number;
  readonly bufferOverflowCount: bigint;
};

const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null;

const propertyRecord = (
  value: Readonly<Record<string, unknown>>,
  property: string,
): Readonly<Record<string, unknown>> => {
  const candidate = value[property];
  if (!isRecord(candidate)) {
    throw new TypeError(`Conformance host expected ${property} to be an object.`);
  }
  return candidate;
};

const propertyString = (value: Readonly<Record<string, unknown>>, property: string): string => {
  const candidate = value[property];
  if (typeof candidate !== "string") {
    throw new TypeError(`Conformance host expected ${property} to be a string.`);
  }
  return candidate;
};

const propertyBigInt = (value: Readonly<Record<string, unknown>>, property: string): bigint => {
  const candidate = value[property];
  if (typeof candidate !== "bigint") {
    throw new TypeError(`Conformance host expected ${property} to be a bigint.`);
  }
  return candidate;
};

const optionalBigInt = (
  value: Readonly<Record<string, unknown>>,
  property: string,
): bigint | undefined => {
  const candidate = value[property];
  return typeof candidate === "bigint" ? candidate : undefined;
};

export const snapshotHealth = (value: unknown): HostHealthSnapshot => {
  if (!isRecord(value)) {
    throw new TypeError("Conformance host expected Source Health to be an object.");
  }
  if (value["_tag"] === "Inactive") {
    return {
      statusTag: "Inactive",
      attempt: undefined,
      retryAtNanos: undefined,
      adapterMetrics: undefined,
      rejectedItemCount: 0n,
      failedSettlementCount: 0n,
      lastRuntimeFailureTag: undefined,
      lastExecutionFailure: undefined,
      latestRejectionFailureTag: undefined,
      latestRejectionFailure: undefined,
      latestRejectionLocation: undefined,
      latestRejectedAtNanos: undefined,
      bufferHighWaterMark: 0,
      bufferOverflowCount: 0n,
    };
  }
  if (value["_tag"] === "Active") {
    return snapshotHealth(value["health"]);
  }
  const status = propertyRecord(value, "status");
  const metrics = propertyRecord(value, "metrics");
  const runtimeMetrics = propertyRecord(metrics, "runtime");
  const lanes = Array.isArray(runtimeMetrics["lanes"]) ? runtimeMetrics["lanes"] : [];
  let bufferHighWaterMark = 0;
  let bufferOverflowCount = 0n;
  for (const lane of lanes) {
    if (!isRecord(lane) || !isRecord(lane["buffer"])) {
      continue;
    }
    const highWaterMark = lane["buffer"]["highWaterMark"];
    const overflowCount = lane["buffer"]["overflowCount"];
    if (typeof highWaterMark === "number") {
      bufferHighWaterMark = Math.max(bufferHighWaterMark, highWaterMark);
    }
    if (typeof overflowCount === "bigint") {
      bufferOverflowCount += overflowCount;
    }
  }
  const exhaustion = isRecord(status["exhaustion"]) ? status["exhaustion"] : undefined;
  const termination = isRecord(status["termination"])
    ? status["termination"]
    : isRecord(status["previousTermination"])
      ? status["previousTermination"]
      : exhaustion === undefined || !isRecord(exhaustion["lastTermination"])
        ? undefined
        : exhaustion["lastTermination"];
  const failure =
    termination === undefined || !isRecord(termination["failure"])
      ? undefined
      : termination["failure"];
  const runtimeFailure =
    failure === undefined || failure["_tag"] !== "RuntimeFailure" || !isRecord(failure["failure"])
      ? undefined
      : failure["failure"];
  const latestRejection = isRecord(status["latestRejection"])
    ? status["latestRejection"]
    : undefined;
  const latestRejectionFailure =
    latestRejection === undefined || !isRecord(latestRejection["failure"])
      ? undefined
      : latestRejection["failure"];
  const latestRejectionLocation =
    latestRejection === undefined ? undefined : latestRejection["location"];
  return {
    statusTag: propertyString(status, "_tag"),
    attempt: optionalBigInt(status, "attempt"),
    retryAtNanos: optionalBigInt(status, "retryAtNanos"),
    adapterMetrics: metrics["adapter"],
    rejectedItemCount: propertyBigInt(runtimeMetrics, "rejectedItemCount"),
    failedSettlementCount: propertyBigInt(runtimeMetrics, "failedSettlementCount"),
    lastRuntimeFailureTag:
      runtimeFailure === undefined ? undefined : propertyString(runtimeFailure, "_tag"),
    lastExecutionFailure: failure,
    latestRejectionFailureTag:
      latestRejectionFailure === undefined
        ? undefined
        : propertyString(latestRejectionFailure, "_tag"),
    latestRejectionFailure,
    latestRejectionLocation,
    latestRejectedAtNanos:
      latestRejection === undefined
        ? undefined
        : optionalBigInt(latestRejection, "rejectedAtNanos"),
    bufferHighWaterMark,
    bufferOverflowCount,
  };
};

const emptyUnknownContext = Context.makeUnsafe<unknown>(new Map());

export const invokeEffect = (
  owner: object,
  methodName: string,
  arguments_: ReadonlyArray<unknown>,
): Effect.Effect<unknown, unknown> =>
  Effect.suspend(() => {
    const method = Reflect.get(owner, methodName);
    if (typeof method !== "function") {
      return Effect.die(new TypeError(`Conformance host expected ${methodName} to be a function.`));
    }
    const result: unknown = Reflect.apply(method, owner, arguments_);
    return Effect.isEffect(result)
      ? result.pipe(Effect.provideContext(emptyUnknownContext))
      : Effect.die(new TypeError(`Conformance host expected ${methodName} to return an Effect.`));
  });

export const openHealth = Effect.fn("SourceAdapterConformanceHost.health.open")(function* (
  liveClient: object,
  route?: Readonly<Record<string, unknown>>,
) {
  const acquired = yield* invokeEffect(liveClient, "subscribeSourceHealth", [
    {
      topic: "rows",
      ...(route === undefined ? {} : { routeBy: route }),
    },
  ]);
  if (!isRecord(acquired) || !Stream.isStream(acquired["events"])) {
    return yield* Effect.die(
      new TypeError("Conformance host expected a Source Health subscription."),
    );
  }
  const events = acquired["events"].pipe(
    Stream.provideContext(emptyUnknownContext),
    Stream.map(snapshotHealth),
  );
  return {
    events,
    close: () => invokeEffect(acquired, "close", []).pipe(Effect.asVoid),
  };
});

export const rows = Effect.fn("SourceAdapterConformanceHost.runtime.rows")(function* (
  runtime: {
    readonly liveClient: object;
  },
  route?: Readonly<Record<string, unknown>>,
) {
  const acquired = yield* invokeEffect(runtime.liveClient, "subscribe", [
    "rows",
    {
      select: ["id"],
      orderBy: [{ field: "id", direction: "asc" }],
      ...(route === undefined ? {} : { routeBy: route }),
    },
  ]);
  if (!isRecord(acquired) || !Stream.isStream(acquired["events"])) {
    return yield* Effect.die(new TypeError("Conformance host expected a live subscription."));
  }
  const snapshot = yield* acquired["events"].pipe(
    Stream.provideContext(emptyUnknownContext),
    Stream.filter((event) => isRecord(event) && event["type"] === "snapshot"),
    Stream.take(1),
    Stream.runHead,
    Effect.map(Option.getOrThrow),
  );
  yield* invokeEffect(acquired, "close", []);
  if (!isRecord(snapshot) || !Array.isArray(snapshot["rows"])) {
    return yield* Effect.die(new TypeError("Conformance host expected a snapshot event."));
  }
  return snapshot["rows"].map((row) => {
    if (!isRecord(row)) {
      throw new TypeError("Conformance host expected snapshot rows to be objects.");
    }
    return propertyString(row, "id");
  });
});

export const openQuery = Effect.fn("SourceAdapterConformanceHost.query.open")(function* (
  liveClient: object,
  query: Readonly<Record<string, unknown>>,
) {
  const acquired = yield* invokeEffect(liveClient, "subscribe", ["rows", query]);
  if (!isRecord(acquired) || !Stream.isStream(acquired["events"])) {
    return yield* Effect.die(new TypeError("Conformance host expected a live subscription."));
  }
  return {
    close: () => invokeEffect(acquired, "close", []).pipe(Effect.asVoid),
  };
});

export const requireMaterialized = <Materialized extends object>(driver: {
  readonly materialized: Materialized | undefined;
}): NonNullable<Materialized> => {
  if (driver.materialized === undefined) {
    throw new Error("The Source Adapter conformance Driver omitted Materialized support.");
  }
  return driver.materialized;
};

export const requireLeased = <Leased extends object>(driver: {
  readonly leased: Leased | undefined;
}): NonNullable<Leased> => {
  if (driver.leased === undefined) {
    throw new Error("The Source Adapter conformance Driver omitted Leased support.");
  }
  return driver.leased;
};

export const requireCallbackBridge = <CallbackBridge extends object>(driver: {
  readonly callbackBridge: CallbackBridge | undefined;
}): NonNullable<CallbackBridge> => {
  if (driver.callbackBridge === undefined) {
    throw new Error("The Source Adapter conformance Driver omitted its callback bridge.");
  }
  return driver.callbackBridge;
};
