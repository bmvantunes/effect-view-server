import {
  topicRowValueSemanticsShareSchema,
  type TopicRowValueSemantics,
} from "./topic-row-value-semantics";

type RowObject = object;

type QueryResultRowNarrower<ResultRow extends RowObject> = (row: RowObject) => ResultRow;

export type BoundQueryResultTopicStorageProjectionProof<ResultRow extends RowObject> = {
  readonly narrowProjectedRow: QueryResultRowNarrower<ResultRow>;
  readonly ownProjectedRow: QueryResultRowNarrower<ResultRow>;
  readonly selectedFields: ReadonlyArray<string>;
};

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
  readonly #narrowProjectedRow: QueryResultRowNarrower<ResultRow>;
  readonly #ownProjectedRow: QueryResultRowNarrower<ResultRow>;

  declare private readonly output: ResultRow;

  constructor(
    constructionToken: object,
    topicRow: TopicRowValueSemantics,
    selectedFields: ReadonlyArray<string>,
    narrowProjectedRow: QueryResultRowNarrower<ResultRow>,
    ownProjectedRow: QueryResultRowNarrower<ResultRow>,
  ) {
    if (constructionToken !== queryResultTopicStorageProjectionProofConstructionToken) {
      throw new TypeError("Query Result Topic Storage projection proof construction is private.");
    }
    queryResultTopicStorageProjectionProofMetadata.set(this, {
      selectedFields: Object.freeze([...selectedFields]),
      topicRow,
    });
    this.#narrowProjectedRow = narrowProjectedRow;
    this.#ownProjectedRow = ownProjectedRow;
    Object.freeze(this);
  }

  bind(
    constructionToken: object,
    selectedFields: ReadonlyArray<string>,
  ): BoundQueryResultTopicStorageProjectionProof<ResultRow> {
    if (constructionToken !== queryResultTopicStorageProjectionProofConstructionToken) {
      throw new TypeError("Query Result Topic Storage projection proof binding is private.");
    }
    return Object.freeze({
      narrowProjectedRow: (row) => this.#narrowProjectedRow(row),
      ownProjectedRow: (row) => this.#ownProjectedRow(row),
      selectedFields,
    });
  }
}

Object.freeze(AuthenticQueryResultTopicStorageProjectionProof.prototype);

export type QueryResultTopicStorageProjectionProof<ResultRow extends RowObject> =
  AuthenticQueryResultTopicStorageProjectionProof<ResultRow>;

export const makeQueryResultTopicStorageProjectionProof = <ResultRow extends RowObject>(
  topicRow: TopicRowValueSemantics,
  selectedFields: ReadonlyArray<string>,
  narrowProjectedRow: QueryResultRowNarrower<ResultRow>,
  ownProjectedRow: QueryResultRowNarrower<ResultRow>,
): QueryResultTopicStorageProjectionProof<ResultRow> =>
  new AuthenticQueryResultTopicStorageProjectionProof(
    queryResultTopicStorageProjectionProofConstructionToken,
    topicRow,
    selectedFields,
    narrowProjectedRow,
    ownProjectedRow,
  );

export const bindQueryResultTopicStorageProjectionProof = <ResultRow extends RowObject>(
  proof: QueryResultTopicStorageProjectionProof<ResultRow>,
  valueSemantics: TopicRowValueSemantics,
): BoundQueryResultTopicStorageProjectionProof<ResultRow> => {
  const metadata = queryResultTopicStorageProjectionProofMetadata.get(proof);
  if (metadata === undefined) {
    throw new TypeError("Query Result Topic Storage projection proof is not authentic.");
  }
  if (!topicRowValueSemanticsShareSchema(valueSemantics, metadata.topicRow)) {
    throw new TypeError("Topic Storage projection schema does not match its compiled proof.");
  }
  return proof.bind(
    queryResultTopicStorageProjectionProofConstructionToken,
    metadata.selectedFields,
  );
};
