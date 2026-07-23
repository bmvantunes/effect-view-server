import type {
  DeltaEvent,
  SnapshotEvent,
  StatusEvent,
  TopicDefinitions,
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
  compileViewServerGroupedRowContract,
  decodeMaterializedGroupedRow,
  decodeMaterializedProjectedRow,
  decodeSystemRow,
  encodeGroupedRow,
  encodeProjectedRow,
  encodeSystemRow,
  isViewServerEventGroupedQuery,
  type ViewServerGroupedRowContract,
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

type ViewServerEventRowContract =
  | {
      readonly _tag: "grouped";
      readonly grouped: ViewServerGroupedRowContract;
    }
  | {
      readonly _tag: "raw";
      readonly selectedFields: ReadonlySet<string>;
    };

export type ViewServerLiveEventCodec<Row> = {
  readonly decode: (
    event: ViewServerWireEvent,
  ) => Effect.Effect<ViewServerProtocolEvent<Row>, ViewServerRuntimeError>;
  readonly decodeTrusted: (
    event: ViewServerTrustedWireEvent,
  ) => Effect.Effect<ViewServerProtocolEvent<Row>, ViewServerRuntimeError>;
  readonly encode: (
    event: ViewServerProtocolEvent<object>,
  ) => Effect.Effect<ViewServerTrustedWireEvent, ViewServerRuntimeError>;
};

const compileViewServerEventRowContract = (
  query: ViewServerEventQuery,
): ViewServerEventRowContract =>
  isViewServerEventGroupedQuery(query)
    ? {
        _tag: "grouped",
        grouped: compileViewServerGroupedRowContract(query),
      }
    : {
        _tag: "raw",
        selectedFields: new Set(query.select),
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

const encodeLiveEventWithContract = Effect.fn("ViewServerProtocol.event.encode")(function* <
  const Topics extends TopicDefinitions,
>(
  config: { readonly topics: Topics },
  expectedTopic: Extract<keyof Topics, string>,
  rowContract: ViewServerEventRowContract,
  event: ViewServerProtocolEvent<object>,
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
    const rows = yield* Effect.forEach(event.rows, (row) => {
      if (rowContract._tag === "raw") {
        return encodeProjectedRow(config, expectedTopic, rowContract.selectedFields, row);
      }
      return encodeGroupedRow(config, expectedTopic, rowContract.grouped, row);
    });
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
      const row = yield* rowContract._tag === "raw"
        ? encodeProjectedRow(config, expectedTopic, rowContract.selectedFields, operation.row)
        : encodeGroupedRow(config, expectedTopic, rowContract.grouped, operation.row);
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

function typedLiveEvent<Row>(
  event: ViewServerProtocolEvent<Record<string, unknown>>,
): ViewServerProtocolEvent<Row>;
function typedLiveEvent(
  event: ViewServerProtocolEvent<Record<string, unknown>>,
): ViewServerProtocolEvent<Record<string, unknown>> {
  return event;
}

const decodeValidatedLiveEvent = Effect.fn("ViewServerProtocol.event.decodeValidated")(function* <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Row,
>(
  config: { readonly topics: Topics },
  expectedTopic: Topic,
  rowContract: ViewServerEventRowContract,
  wireEvent: ViewServerWireEvent,
) {
  if (wireEvent.topic !== expectedTopic) {
    return yield* Effect.fail(
      invalidRow(
        expectedTopic,
        `Received event for ${wireEvent.topic} while subscribed to ${expectedTopic}`,
      ),
    );
  }
  if (wireEvent.type === "status") {
    return typedLiveEvent<Row>(wireEvent);
  }
  if (wireEvent.type === "snapshot") {
    const rows = yield* Effect.forEach(wireEvent.rows, (row) => {
      if (rowContract._tag === "raw") {
        return decodeMaterializedProjectedRow(
          config,
          expectedTopic,
          rowContract.selectedFields,
          row,
        );
      }
      return decodeMaterializedGroupedRow(config, expectedTopic, rowContract.grouped, row);
    });
    return typedLiveEvent<Row>({
      ...wireEvent,
      rows,
    });
  }
  type DecodedDeltaOperation = Extract<
    ViewServerProtocolEvent<Record<string, unknown>>,
    { readonly type: "delta" }
  >["operations"][number];
  const operations: Array<DecodedDeltaOperation> = [];
  for (const operation of wireEvent.operations) {
    if (operation.type === "insert" || operation.type === "update") {
      const row = yield* rowContract._tag === "raw"
        ? decodeMaterializedProjectedRow(
            config,
            expectedTopic,
            rowContract.selectedFields,
            operation.row,
          )
        : decodeMaterializedGroupedRow(config, expectedTopic, rowContract.grouped, operation.row);
      operations.push({
        ...operation,
        row,
      });
    } else {
      operations.push(operation);
    }
  }
  return typedLiveEvent<Row>({
    ...wireEvent,
    operations,
  });
});

const decodeLiveEventWithContract = Effect.fn("ViewServerProtocol.event.decode")(function* <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Row,
>(
  config: { readonly topics: Topics },
  expectedTopic: Topic,
  rowContract: ViewServerEventRowContract,
  event: ViewServerWireEvent,
) {
  const materializedEvent = yield* materializeJsonFieldValue(event, (message) =>
    invalidRow(expectedTopic, `Invalid event: ${message}`),
  );
  const wireEvent = yield* Schema.decodeUnknownEffect(ViewServerWireEventSchema)(
    materializedEvent,
  ).pipe(Effect.mapError((error) => invalidRow(expectedTopic, `Invalid event: ${error.message}`)));
  return yield* decodeValidatedLiveEvent<Topics, Topic, Row>(
    config,
    expectedTopic,
    rowContract,
    wireEvent,
  );
});

export const compileViewServerLiveEventCodec = <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Row = object,
>(
  config: { readonly topics: Topics },
  expectedTopic: Topic,
  query: ViewServerEventQuery,
): ViewServerLiveEventCodec<Row> => {
  const rowContract = compileViewServerEventRowContract(query);
  const encode: ViewServerLiveEventCodec<Row>["encode"] = (event) =>
    encodeLiveEventWithContract(config, expectedTopic, rowContract, event);
  const decode: ViewServerLiveEventCodec<Row>["decode"] = (event) =>
    decodeLiveEventWithContract<Topics, Topic, Row>(config, expectedTopic, rowContract, event);
  const decodeTrusted: ViewServerLiveEventCodec<Row>["decodeTrusted"] = (event) =>
    decodeValidatedLiveEvent<Topics, Topic, Row>(config, expectedTopic, rowContract, event);
  return Object.freeze({ decode, decodeTrusted, encode });
};

export const viewServerEncodeLiveEvent = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  expectedTopic: Extract<keyof Topics, string>,
  query: ViewServerEventQuery,
  event: ViewServerProtocolEvent<object>,
): Effect.Effect<ViewServerTrustedWireEvent, ViewServerRuntimeError> =>
  compileViewServerLiveEventCodec(config, expectedTopic, query).encode(event);

export const viewServerDecodeLiveEvent = <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Row,
>(
  config: { readonly topics: Topics },
  expectedTopic: Topic,
  query: ViewServerEventQuery,
  event: ViewServerWireEvent,
): Effect.Effect<ViewServerProtocolEvent<Row>, ViewServerRuntimeError> =>
  compileViewServerLiveEventCodec<Topics, Topic, Row>(config, expectedTopic, query).decode(event);

export const viewServerDecodeTrustedLiveEvent = <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Row,
>(
  config: { readonly topics: Topics },
  expectedTopic: Topic,
  query: ViewServerEventQuery,
  event: ViewServerTrustedWireEvent,
): Effect.Effect<ViewServerProtocolEvent<Row>, ViewServerRuntimeError> =>
  compileViewServerLiveEventCodec<Topics, Topic, Row>(config, expectedTopic, query).decodeTrusted(
    event,
  );

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
