import {
  topicRowValueSemanticsShareSchema,
  type TopicRowValueSemantics,
} from "./topic-row-value-semantics";

type RowObject = object;

const queryResultTopicStorageProjectionProofConstructionToken = Object.freeze({});

type QueryResultTopicStorageProjectionProofMetadata = {
  readonly selectedFields: ReadonlyArray<string>;
  readonly topicRow: TopicRowValueSemantics;
};

const queryResultTopicStorageProjectionProofMetadata = new WeakMap<
  object,
  QueryResultTopicStorageProjectionProofMetadata
>();

class AuthenticQueryResultTopicStorageProjectionProof<ResultRow extends RowObject> {
  declare private readonly output: ResultRow;

  constructor(
    constructionToken: object,
    topicRow: TopicRowValueSemantics,
    selectedFields: ReadonlyArray<string>,
  ) {
    if (constructionToken !== queryResultTopicStorageProjectionProofConstructionToken) {
      throw new TypeError("Query Result Topic Storage projection proof construction is private.");
    }
    queryResultTopicStorageProjectionProofMetadata.set(this, {
      selectedFields: Object.freeze([...selectedFields]),
      topicRow,
    });
    Object.freeze(this);
  }
}

Object.freeze(AuthenticQueryResultTopicStorageProjectionProof.prototype);

export type QueryResultTopicStorageProjectionProof<ResultRow extends RowObject> =
  AuthenticQueryResultTopicStorageProjectionProof<ResultRow>;

export const makeQueryResultTopicStorageProjectionProof = <ResultRow extends RowObject>(
  topicRow: TopicRowValueSemantics,
  selectedFields: ReadonlyArray<string>,
): QueryResultTopicStorageProjectionProof<ResultRow> =>
  new AuthenticQueryResultTopicStorageProjectionProof(
    queryResultTopicStorageProjectionProofConstructionToken,
    topicRow,
    selectedFields,
  );

export const bindQueryResultTopicStorageProjectionProof = <ResultRow extends RowObject>(
  proof: QueryResultTopicStorageProjectionProof<ResultRow>,
  valueSemantics: TopicRowValueSemantics,
): ReadonlyArray<string> => {
  const metadata = queryResultTopicStorageProjectionProofMetadata.get(proof);
  if (metadata === undefined) {
    throw new TypeError("Query Result Topic Storage projection proof is not authentic.");
  }
  if (!topicRowValueSemanticsShareSchema(valueSemantics, metadata.topicRow)) {
    throw new TypeError("Topic Storage projection schema does not match its compiled proof.");
  }
  return metadata.selectedFields;
};
