import type {
  EngineClosedError,
  InvalidQueryError,
  InvalidRowError,
  InvalidTopicError,
} from "@effect-view-server/column-live-view-engine";
import type { ViewServerRuntimeError } from "@effect-view-server/config";

type EngineRuntimeError =
  | InvalidTopicError
  | InvalidRowError
  | InvalidQueryError
  | EngineClosedError;

export const engineErrorToRuntimeError = (error: EngineRuntimeError): ViewServerRuntimeError => {
  switch (error._tag) {
    case "InvalidTopicError": {
      return {
        _tag: "ViewServerRuntimeError",
        code: "InvalidTopic",
        topic: error.topic,
        message: error.message,
      };
    }
    case "InvalidRowError": {
      return {
        _tag: "ViewServerRuntimeError",
        code: "InvalidRow",
        topic: error.topic,
        message: error.message,
      };
    }
    case "InvalidQueryError": {
      return {
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: error.topic,
        message: error.message,
      };
    }
    case "EngineClosedError": {
      return {
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        message: error.message,
      };
    }
  }
};

export const invalidRuntimeQueryError = (
  topic: string,
  message: string,
): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidQuery",
  topic,
  message,
});

export const sourceLeasedRuntimeReadError = (topic: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "UnsupportedQuery",
  topic,
  message:
    "Leased Source topics do not support one-shot snapshots; use a live subscription so Runtime Core owns the source lease lifecycle.",
});

export const sourceOwnedRuntimeMutationError = (topic: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "UnsupportedQuery",
  topic,
  message:
    "Source-owned topics do not support direct runtime mutations; publish through the configured Source Adapter or use an externally-published topic.",
});

export const sourceOwnedRuntimeResetError: ViewServerRuntimeError = {
  _tag: "ViewServerRuntimeError",
  code: "UnsupportedQuery",
  message:
    "Source-owned topics do not support direct runtime reset; close the runtime or reset source-free topics through their owner.",
};
