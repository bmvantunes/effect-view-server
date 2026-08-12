import { Schema } from "effect";
import type { InvalidQueryError } from "./raw-query-compiler";

export class InvalidTopicError extends Schema.TaggedError<InvalidTopicError>()(
  "InvalidTopicError",
  {
    topic: Schema.String,
    message: Schema.String,
  },
) {}

export class InvalidRowError extends Schema.TaggedError<InvalidRowError>()("InvalidRowError", {
  topic: Schema.String,
  message: Schema.String,
}) {}

export class EngineClosedError extends Schema.TaggedError<EngineClosedError>()(
  "EngineClosedError",
  {
    message: Schema.String,
  },
) {}

export type ColumnLiveViewEngineError =
  | InvalidTopicError
  | InvalidRowError
  | InvalidQueryError
  | EngineClosedError;
