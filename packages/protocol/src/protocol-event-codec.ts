import type {
  DeltaEvent,
  ExactLiveQueryInputForTopic,
  LiveQuery,
  LiveQueryRow,
  SnapshotEvent,
  StatusEvent,
  TopicDefinitions,
  TopicRow,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import {
  ViewServerTrustedWireEventSchema,
  type ViewServerTrustedWireEvent,
  ViewServerWireEventSchema,
  type ViewServerWireEvent,
} from "./protocol-event-schema";
import { materializeJsonFieldValue } from "./protocol-json-field-codec";
import type { ViewServerEventQuery } from "./protocol-query-schema";
import {
  compileViewServerEventRowPlan,
  compileViewServerRuntimeEventRowPlan,
  decodeSystemRow,
  encodeSystemRow,
  type ViewServerEventRowPlan,
} from "./protocol-row-codec";

export {
  ViewServerTrustedWireEventSchema,
  ViewServerWireEventSchema,
  ViewServerWireRowSchema,
} from "./protocol-event-schema";
export type {
  ViewServerTrustedWireEvent,
  ViewServerWireEvent,
  ViewServerWireRow,
} from "./protocol-event-schema";

export type ViewServerProtocolEvent<Row> = SnapshotEvent<Row> | DeltaEvent<Row> | StatusEvent;

export type ViewServerLiveEventCodec<Row> = {
  readonly decode: (
    event: ViewServerWireEvent,
  ) => Effect.Effect<ViewServerProtocolEvent<Row>, ViewServerRuntimeError>;
  readonly decodeTrusted: (
    event: ViewServerTrustedWireEvent,
  ) => Effect.Effect<ViewServerProtocolEvent<Row>, ViewServerRuntimeError>;
  readonly encode: (
    event: ViewServerProtocolEvent<Row>,
  ) => Effect.Effect<ViewServerTrustedWireEvent, ViewServerRuntimeError>;
};

export type ViewServerRuntimeLiveEventEncoder = {
  readonly encode: (
    event: ViewServerProtocolEvent<object>,
  ) => Effect.Effect<ViewServerTrustedWireEvent, ViewServerRuntimeError>;
};

const invalidRow = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidRow",
  message,
  topic,
});

const validateTrustedWireEvent = Effect.fn("ViewServerProtocol.event.trusted.validate")(function* (
  topic: string,
  event: ViewServerWireEvent,
) {
  return yield* Schema.decodeUnknownEffect(ViewServerTrustedWireEventSchema)(event).pipe(
    Effect.mapError((error) => invalidRow(topic, `Invalid event: ${error.message}`)),
  );
});

const encodeStatusEvent = Effect.fn("ViewServerProtocol.event.status.encode")(function* (
  topic: string,
  event: StatusEvent,
) {
  const wireEvent = yield* Schema.decodeUnknownEffect(ViewServerWireEventSchema)(event).pipe(
    Effect.mapError((error) => invalidRow(topic, `Invalid event: ${error.message}`)),
  );
  return yield* validateTrustedWireEvent(topic, wireEvent);
});

const encodeLiveEventWithPlan = Effect.fn("ViewServerProtocol.event.encode")(function* <
  const Topics extends TopicDefinitions,
  Row extends object,
>(
  config: { readonly topics: Topics },
  expectedTopic: Extract<keyof Topics, string>,
  rowPlan: ViewServerEventRowPlan<Row>,
  event: ViewServerProtocolEvent<Row>,
) {
  if (event.topic !== expectedTopic) {
    return yield* Effect.fail(
      invalidRow(
        expectedTopic,
        `Received event for ${event.topic} while subscribed to ${expectedTopic}`,
      ),
    );
  }
  if (event.type === "status") {
    return yield* encodeStatusEvent(expectedTopic, event);
  }
  if (event.type === "snapshot") {
    const rows = yield* Effect.forEach(event.rows, rowPlan.encode);
    const wireEvent = {
      ...event,
      rows,
    };
    return yield* validateTrustedWireEvent(expectedTopic, wireEvent);
  }
  type WireDeltaOperation = Extract<
    ViewServerWireEvent,
    { readonly type: "delta" }
  >["operations"][number];
  const operations: Array<WireDeltaOperation> = [];
  for (const operation of event.operations) {
    if (operation.type === "insert" || operation.type === "update") {
      const row = yield* rowPlan.encode(operation.row);
      operations.push({
        ...operation,
        row,
      });
    } else {
      operations.push(operation);
    }
  }
  const wireEvent = {
    ...event,
    operations,
  };
  return yield* validateTrustedWireEvent(expectedTopic, wireEvent);
});

const decodeValidatedLiveEvent = Effect.fn("ViewServerProtocol.event.decodeValidated")(function* <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Row extends object,
>(
  expectedTopic: Topic,
  rowPlan: ViewServerEventRowPlan<Row>,
  wireEvent: ViewServerWireEvent,
): Effect.fn.Return<ViewServerProtocolEvent<Row>, ViewServerRuntimeError> {
  if (wireEvent.topic !== expectedTopic) {
    return yield* Effect.fail(
      invalidRow(
        expectedTopic,
        `Received event for ${wireEvent.topic} while subscribed to ${expectedTopic}`,
      ),
    );
  }
  if (wireEvent.type === "status") {
    return wireEvent;
  }
  if (wireEvent.type === "snapshot") {
    const rows = yield* Effect.forEach(wireEvent.rows, rowPlan.decodeMaterialized);
    return {
      ...wireEvent,
      rows,
    };
  }
  type DecodedDeltaOperation = Extract<
    ViewServerProtocolEvent<Row>,
    { readonly type: "delta" }
  >["operations"][number];
  const operations: Array<DecodedDeltaOperation> = [];
  for (const operation of wireEvent.operations) {
    if (operation.type === "insert" || operation.type === "update") {
      const row = yield* rowPlan.decodeMaterialized(operation.row);
      operations.push({
        ...operation,
        row,
      });
    } else {
      operations.push(operation);
    }
  }
  return {
    ...wireEvent,
    operations,
  };
});

const decodeLiveEventWithContract = Effect.fn("ViewServerProtocol.event.decode")(function* <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Row extends object,
>(expectedTopic: Topic, rowPlan: ViewServerEventRowPlan<Row>, event: ViewServerWireEvent) {
  const materializedEvent = yield* materializeJsonFieldValue(event, (message) =>
    invalidRow(expectedTopic, `Invalid event: ${message}`),
  );
  const wireEvent = yield* Schema.decodeUnknownEffect(ViewServerWireEventSchema)(
    materializedEvent,
  ).pipe(Effect.mapError((error) => invalidRow(expectedTopic, `Invalid event: ${error.message}`)));
  return yield* decodeValidatedLiveEvent<Topics, Topic, Row>(expectedTopic, rowPlan, wireEvent);
});

export const defineViewServerLiveEventQuery = <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  const Query extends LiveQuery<TopicRow<Topics, Topic>>,
>(
  config: { readonly topics: Topics },
  expectedTopic: Topic,
  query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
): Query => {
  void config;
  void expectedTopic;
  return query;
};

export const compileViewServerLiveEventCodec = <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  const Query extends LiveQuery<TopicRow<Topics, Topic>>,
>(
  config: { readonly topics: Topics },
  expectedTopic: Topic,
  query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
): ViewServerLiveEventCodec<LiveQueryRow<TopicRow<Topics, Topic>, Query>> => {
  type Row = LiveQueryRow<TopicRow<Topics, Topic>, Query>;
  const rowPlan = compileViewServerEventRowPlan(config, expectedTopic, query);
  const encode: ViewServerLiveEventCodec<Row>["encode"] = (event) =>
    encodeLiveEventWithPlan(config, expectedTopic, rowPlan, event);
  const decode: ViewServerLiveEventCodec<Row>["decode"] = (event) =>
    decodeLiveEventWithContract<Topics, Topic, Row>(expectedTopic, rowPlan, event);
  const decodeTrusted: ViewServerLiveEventCodec<Row>["decodeTrusted"] = (event) =>
    decodeValidatedLiveEvent<Topics, Topic, Row>(expectedTopic, rowPlan, event);
  return Object.freeze({ decode, decodeTrusted, encode });
};

export const compileViewServerRuntimeLiveEventEncoder = (
  config: { readonly topics: TopicDefinitions },
  expectedTopic: string,
  query: ViewServerEventQuery,
): ViewServerRuntimeLiveEventEncoder => {
  const rowPlan = compileViewServerRuntimeEventRowPlan(config, expectedTopic, query);
  const encode: ViewServerRuntimeLiveEventEncoder["encode"] = (event) =>
    encodeLiveEventWithPlan(config, expectedTopic, rowPlan, event);
  return Object.freeze({ encode });
};

export const viewServerEncodeLiveEvent = <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  const Query extends LiveQuery<TopicRow<Topics, Topic>>,
>(
  config: { readonly topics: Topics },
  expectedTopic: Topic,
  query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  event: ViewServerProtocolEvent<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
): Effect.Effect<ViewServerTrustedWireEvent, ViewServerRuntimeError> =>
  compileViewServerLiveEventCodec(config, expectedTopic, query).encode(event);

export const viewServerDecodeLiveEvent = <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  const Query extends LiveQuery<TopicRow<Topics, Topic>>,
>(
  config: { readonly topics: Topics },
  expectedTopic: Topic,
  query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  event: ViewServerWireEvent,
): Effect.Effect<
  ViewServerProtocolEvent<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
  ViewServerRuntimeError
> => compileViewServerLiveEventCodec(config, expectedTopic, query).decode(event);

export const viewServerDecodeTrustedLiveEvent = <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  const Query extends LiveQuery<TopicRow<Topics, Topic>>,
>(
  config: { readonly topics: Topics },
  expectedTopic: Topic,
  query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  event: ViewServerTrustedWireEvent,
): Effect.Effect<
  ViewServerProtocolEvent<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
  ViewServerRuntimeError
> => compileViewServerLiveEventCodec(config, expectedTopic, query).decodeTrusted(event);

export const encodeSystemLiveEvent = Effect.fn("ViewServerProtocol.system.event.encode")(function* <
  Row,
>(
  expectedTopic: string,
  schema: Schema.Codec<Row, unknown, never, never>,
  event: ViewServerProtocolEvent<Row>,
) {
  if (event.topic !== expectedTopic) {
    return yield* Effect.fail(
      invalidRow(
        expectedTopic,
        `Received event for ${event.topic} while subscribed to ${expectedTopic}`,
      ),
    );
  }
  if (event.type === "status") {
    return yield* encodeStatusEvent(expectedTopic, event);
  }
  if (event.type === "snapshot") {
    const rows = yield* Effect.forEach(event.rows, (row) =>
      encodeSystemRow(expectedTopic, schema, row),
    );
    const wireEvent = {
      ...event,
      rows,
    };
    return yield* validateTrustedWireEvent(expectedTopic, wireEvent);
  }
  type WireDeltaOperation = Extract<
    ViewServerWireEvent,
    { readonly type: "delta" }
  >["operations"][number];
  const operations: Array<WireDeltaOperation> = [];
  for (const operation of event.operations) {
    if (operation.type === "insert" || operation.type === "update") {
      const row = yield* encodeSystemRow(expectedTopic, schema, operation.row);
      operations.push({
        ...operation,
        row,
      });
    } else {
      operations.push(operation);
    }
  }
  const wireEvent = {
    ...event,
    operations,
  };
  return yield* validateTrustedWireEvent(expectedTopic, wireEvent);
});

export const decodeSystemLiveEvent = Effect.fn("ViewServerProtocol.system.event.decode")(function* <
  Row,
>(
  expectedTopic: string,
  schema: Schema.Codec<Row, unknown, never, never>,
  event: ViewServerWireEvent,
) {
  if (event.topic !== expectedTopic) {
    return yield* Effect.fail(
      invalidRow(
        expectedTopic,
        `Received event for ${event.topic} while subscribed to ${expectedTopic}`,
      ),
    );
  }
  if (event.type === "status") {
    return event;
  }
  if (event.type === "snapshot") {
    const rows = yield* Effect.forEach(event.rows, (row) =>
      decodeSystemRow(expectedTopic, schema, row),
    );
    return {
      ...event,
      rows,
    };
  }
  type DecodedDeltaOperation = Extract<
    ViewServerProtocolEvent<Row>,
    { readonly type: "delta" }
  >["operations"][number];
  const operations: Array<DecodedDeltaOperation> = [];
  for (const operation of event.operations) {
    if (operation.type === "insert" || operation.type === "update") {
      const row = yield* decodeSystemRow(expectedTopic, schema, operation.row);
      operations.push({
        ...operation,
        row,
      });
    } else {
      operations.push(operation);
    }
  }
  return {
    ...event,
    operations,
  };
});
