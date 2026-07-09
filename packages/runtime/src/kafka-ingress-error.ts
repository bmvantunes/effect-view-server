import { Cause, Option, Schema } from "effect";

export class ViewServerKafkaIngressError extends Schema.TaggedErrorClass<ViewServerKafkaIngressError>()(
  "ViewServerKafkaIngressError",
  {
    message: Schema.String,
    cause: Schema.Unknown,
    region: Schema.optionalKey(Schema.String),
    sourceTopic: Schema.optionalKey(Schema.String),
  },
) {}

export const messageFromUnknown = (error: unknown): string => {
  if (error instanceof Error) {
    return error.message;
  }
  if (typeof error === "object" && error !== null && "message" in error) {
    const message = Reflect.get(error, "message");
    if (typeof message === "string") {
      return message;
    }
  }
  return String(error);
};

export const kafkaConsumerStartError = (
  region: string,
  cause: unknown,
): ViewServerKafkaIngressError =>
  new ViewServerKafkaIngressError({
    message: `Failed to start Kafka consumer for region ${region}`,
    cause,
    region,
  });

export const mapKafkaConsumerStartError =
  (region: string) =>
  (cause: unknown): ViewServerKafkaIngressError =>
    kafkaConsumerStartError(region, cause);

export const kafkaStreamError = (region: string, cause: unknown): ViewServerKafkaIngressError =>
  new ViewServerKafkaIngressError({
    message: `Kafka stream failed for region ${region}`,
    cause,
    region,
  });

export const mapKafkaStreamError =
  (region: string) =>
  (cause: unknown): ViewServerKafkaIngressError =>
    kafkaStreamError(region, cause);

export const kafkaStreamCloseError = (cause: unknown): ViewServerKafkaIngressError =>
  new ViewServerKafkaIngressError({
    message: "Failed to close Kafka stream",
    cause,
  });

export const kafkaConsumerCloseError = (cause: unknown): ViewServerKafkaIngressError =>
  new ViewServerKafkaIngressError({
    message: "Failed to close Kafka consumer",
    cause,
  });

export const kafkaMessageCommitError = (
  region: string,
  sourceTopic: string,
  cause: unknown,
): ViewServerKafkaIngressError =>
  new ViewServerKafkaIngressError({
    message: `Failed to commit Kafka message for source topic ${sourceTopic}`,
    cause,
    region,
    sourceTopic,
  });

export const kafkaMessageDecodeError = (
  region: string,
  sourceTopic: string,
  cause: unknown,
): ViewServerKafkaIngressError =>
  new ViewServerKafkaIngressError({
    message: `Failed to decode Kafka message for source topic ${sourceTopic}`,
    cause,
    region,
    sourceTopic,
  });

export const kafkaMessageMappingError = (
  region: string,
  sourceTopic: string,
  cause: unknown,
): ViewServerKafkaIngressError =>
  new ViewServerKafkaIngressError({
    message: `Failed to map Kafka message for source topic ${sourceTopic}`,
    cause,
    region,
    sourceTopic,
  });

export const kafkaMessageProcessingError = (
  region: string,
  sourceTopic: string,
  cause: unknown,
): ViewServerKafkaIngressError =>
  new ViewServerKafkaIngressError({
    message: `Failed to process Kafka message for source topic ${sourceTopic}`,
    cause,
    region,
    sourceTopic,
  });

const isNonFailureKafkaCauseReason = (
  reason: Cause.Reason<unknown>,
): reason is Cause.Die | Cause.Interrupt =>
  Cause.isDieReason(reason) || Cause.isInterruptReason(reason);

export const kafkaFailureCause = (error: unknown, cause: Cause.Cause<unknown>): unknown =>
  cause.reasons.length > 1 || Cause.hasDies(cause) || Cause.hasInterrupts(cause) ? cause : error;

const preservedKafkaFailureError = (
  primaryError: ViewServerKafkaIngressError,
  error: unknown,
): ViewServerKafkaIngressError =>
  error instanceof ViewServerKafkaIngressError
    ? error
    : new ViewServerKafkaIngressError({
        cause: error,
        message: messageFromUnknown(error),
        ...(primaryError.region === undefined ? {} : { region: primaryError.region }),
        ...(primaryError.sourceTopic === undefined
          ? {}
          : { sourceTopic: primaryError.sourceTopic }),
      });

export const kafkaIngressFailureCause = (
  error: ViewServerKafkaIngressError,
  cause: Cause.Cause<unknown>,
): Cause.Cause<ViewServerKafkaIngressError> => {
  const failureReasons = cause.reasons
    .filter(Cause.isFailReason)
    .map((reason) => Cause.makeFailReason(preservedKafkaFailureError(error, reason.error)));
  const nonFailureReasons = cause.reasons.filter(isNonFailureKafkaCauseReason);
  const hasPrimaryFailure = failureReasons.some((reason) => reason.error === error);
  return Cause.fromReasons([
    ...(hasPrimaryFailure ? [] : [Cause.makeFailReason(error)]),
    ...failureReasons,
    ...nonFailureReasons,
  ]);
};

export const kafkaNonFailureCause = (cause: Cause.Cause<unknown>): Cause.Cause<never> =>
  Cause.fromReasons(cause.reasons.filter(isNonFailureKafkaCauseReason));

export const kafkaIngressErrorSourceTopic = (error: unknown): Option.Option<string> =>
  error instanceof ViewServerKafkaIngressError
    ? Option.fromUndefinedOr(error.sourceTopic)
    : Option.none();
