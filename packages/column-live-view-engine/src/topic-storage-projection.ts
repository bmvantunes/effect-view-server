import {
  bindQueryResultTopicStorageProjectionProof,
  type BoundQueryResultTopicStorageProjectionProof,
  type QueryResultTopicStorageProjectionProof,
} from "./query-result-topic-storage-proof";
import type { TopicRowValueSemantics } from "./topic-row-value-semantics";

type RowObject = object;

type TopicStorageProjector = (slot: number) => RowObject;

type TopicStorageProjectionBinder = (
  selectedFields: ReadonlyArray<string>,
) => TopicStorageProjector;

const topicStorageProjectionConstructionToken = Object.freeze({});

const assertTopicStorageProjectionConstruction = (constructionToken: object): void => {
  if (constructionToken !== topicStorageProjectionConstructionToken) {
    throw new TypeError("Topic Storage projection construction is private.");
  }
};

class AuthenticTopicStorageProjectionCapability {
  readonly #bindProjectRow: TopicStorageProjectionBinder;
  readonly #valueSemantics: TopicRowValueSemantics;

  constructor(
    constructionToken: object,
    valueSemantics: TopicRowValueSemantics,
    bindProjectRow: TopicStorageProjectionBinder,
  ) {
    assertTopicStorageProjectionConstruction(constructionToken);
    this.#bindProjectRow = bindProjectRow;
    this.#valueSemantics = valueSemantics;
    Object.freeze(this);
  }

  bind<ResultRow extends RowObject>(
    proof: QueryResultTopicStorageProjectionProof<ResultRow>,
  ): TopicStorageProjectionSession<ResultRow> {
    const boundProof = bindQueryResultTopicStorageProjectionProof(proof, this.#valueSemantics);
    return new AuthenticTopicStorageProjectionSession<ResultRow>(
      topicStorageProjectionConstructionToken,
      this.#bindProjectRow(boundProof.selectedFields),
      boundProof,
    );
  }
}

Object.freeze(AuthenticTopicStorageProjectionCapability.prototype);

export type TopicStorageProjectionCapability = AuthenticTopicStorageProjectionCapability;

export const makeTopicStorageProjectionCapability = (
  valueSemantics: TopicRowValueSemantics,
  bindProjectRow: TopicStorageProjectionBinder,
): TopicStorageProjectionCapability =>
  new AuthenticTopicStorageProjectionCapability(
    topicStorageProjectionConstructionToken,
    valueSemantics,
    bindProjectRow,
  );

export const bindTopicStorageProjection = <ResultRow extends RowObject>(
  capability: TopicStorageProjectionCapability,
  proof: QueryResultTopicStorageProjectionProof<ResultRow>,
): TopicStorageProjectionSession<ResultRow> => {
  if (!(capability instanceof AuthenticTopicStorageProjectionCapability)) {
    throw new TypeError("Topic Storage projection capability is not authentic.");
  }
  return capability.bind(proof);
};

class AuthenticTopicStorageProjectionSession<ResultRow extends RowObject> {
  readonly #projectRow: TopicStorageProjector;
  readonly projectOwnedResultRow: (slot: number) => ResultRow;
  readonly projectResultRow: (slot: number) => ResultRow;

  constructor(
    constructionToken: object,
    projectRow: TopicStorageProjector,
    proof: BoundQueryResultTopicStorageProjectionProof<ResultRow>,
  ) {
    assertTopicStorageProjectionConstruction(constructionToken);
    this.#projectRow = projectRow;
    this.projectOwnedResultRow = (slot) => proof.ownProjectedRow(this.#projectRow(slot));
    this.projectResultRow = (slot) => proof.narrowProjectedRow(this.#projectRow(slot));
    Object.freeze(this);
  }
}

Object.freeze(AuthenticTopicStorageProjectionSession.prototype);

export type TopicStorageProjectionSession<ResultRow extends RowObject = RowObject> =
  AuthenticTopicStorageProjectionSession<ResultRow>;
