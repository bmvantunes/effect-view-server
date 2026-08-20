import { createFileRegistry, toBinary, type DescFile, type DescMessage } from "@bufbuild/protobuf";
import type { FileDescriptorProto } from "@bufbuild/protobuf/wkt";
import { FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { Effect, Exit, Option, Schema } from "effect";
import {
  kafkaProtobufMessageAtIndexes,
  kafkaProtobufMessageIndexes,
  kafkaProtobufNormalizedMessageIndexes,
} from "./schema-registry-descriptor";
import { parseKafkaSchemaRegistryProtobufFrame } from "./schema-registry-frame";
import {
  kafkaProtobufMessageReaderCompatibilityIssues,
  kafkaProtobufMessageWireCompatibilityIssues,
  kafkaProtobufWireCompatibilityIssues,
} from "./schema-registry-wire";

export type KafkaSchemaRegistrySide = "key" | "value";

export const KafkaSchemaRegistryContractIssueCode = Schema.Literals([
  "RegistryUnavailable",
  "CompatibilityPolicyMismatch",
  "MalformedVersionHistory",
  "SoftDeletedVersion",
  "HardDeletedVersionGap",
  "SchemaTypeMismatch",
  "MalformedSchema",
  "MissingSchemaReference",
  "WireHistoryMismatch",
  "GeneratedSchemaMismatch",
]);
export type KafkaSchemaRegistryContractIssueCode = typeof KafkaSchemaRegistryContractIssueCode.Type;

export const KafkaSchemaRegistryContractIssue = Schema.TaggedStruct(
  "KafkaSchemaRegistryContractIssue",
  {
    region: Schema.NonEmptyString,
    viewServerTopic: Schema.NonEmptyString,
    sourceTopic: Schema.NonEmptyString,
    side: Schema.Literals(["key", "value"]),
    subject: Schema.NonEmptyString,
    code: KafkaSchemaRegistryContractIssueCode,
    version: Schema.NullOr(Schema.Int),
    schemaId: Schema.NullOr(Schema.Int),
    message: Schema.String,
  },
);
export type KafkaSchemaRegistryContractIssue = typeof KafkaSchemaRegistryContractIssue.Type;

export const KafkaSchemaRegistryContractValidationFailure = Schema.TaggedStruct(
  "KafkaSchemaRegistryContractValidationFailure",
  {
    message: Schema.Literal(
      "Kafka Schema Registry Protobuf validation failed before runtime startup.",
    ),
    issues: Schema.NonEmptyArray(KafkaSchemaRegistryContractIssue),
  },
);
export type KafkaSchemaRegistryContractValidationFailure =
  typeof KafkaSchemaRegistryContractValidationFailure.Type;

export type KafkaSchemaRegistryDeclaration = {
  readonly region: string;
  readonly viewServerTopic: string;
  readonly sourceTopic: string;
  readonly side: KafkaSchemaRegistrySide;
  readonly subject: string;
  readonly descriptor: DescMessage;
};

export type KafkaSchemaRegistryReference = {
  readonly name: string;
  readonly subject: string;
  readonly version: number;
};

export type KafkaSchemaRegistrySchemaVersion = {
  readonly subject: string;
  readonly version: number;
  readonly id: number;
  readonly schemaType: string;
  readonly references: ReadonlyArray<KafkaSchemaRegistryReference>;
  readonly descriptor: FileDescriptorProto;
};

export type KafkaSchemaRegistryRequestFailure = {
  readonly message: string;
};

export type KafkaSchemaRegistryReader = {
  readonly effectiveCompatibility: (
    subject: string,
  ) => Effect.Effect<string, KafkaSchemaRegistryRequestFailure>;
  readonly versions: (
    subject: string,
    includeDeleted: boolean,
  ) => Effect.Effect<ReadonlyArray<number>, KafkaSchemaRegistryRequestFailure>;
  readonly schema: (
    subject: string,
    version: number,
  ) => Effect.Effect<KafkaSchemaRegistrySchemaVersion, KafkaSchemaRegistryRequestFailure>;
};

export type KafkaResolvedSchemaRegistryVersion = {
  readonly id: number;
  readonly version: number;
  readonly root: DescFile;
  readonly messageIndexes: readonly [number, ...ReadonlyArray<number>];
};

export type KafkaResolvedSchemaRegistryContract = KafkaSchemaRegistryDeclaration & {
  readonly dependencySubjects: ReadonlyArray<string>;
  readonly versions: ReadonlyArray<KafkaResolvedSchemaRegistryVersion>;
  readonly schemaIds: ReadonlyMap<
    number,
    ReadonlyArray<readonly [number, ...ReadonlyArray<number>]>
  >;
};

type LoadedGraph = {
  readonly id: number;
  readonly version: number;
  readonly root: DescFile;
  readonly subjects: ReadonlySet<string>;
};

type LoadedHistory = {
  readonly subject: string;
  readonly versions: ReadonlyArray<LoadedGraph>;
  readonly referencedSubjects: ReadonlySet<string>;
};

const issue = (
  declaration: KafkaSchemaRegistryDeclaration,
  subject: string,
  code: KafkaSchemaRegistryContractIssueCode,
  message: string,
  version: number | null = null,
  schemaId: number | null = null,
): KafkaSchemaRegistryContractIssue => ({
  _tag: "KafkaSchemaRegistryContractIssue",
  region: declaration.region,
  viewServerTopic: declaration.viewServerTopic,
  sourceTopic: declaration.sourceTopic,
  side: declaration.side,
  subject,
  code,
  version,
  schemaId,
  message,
});

const unavailable = (
  declaration: KafkaSchemaRegistryDeclaration,
  subject: string,
  failure: KafkaSchemaRegistryRequestFailure,
): KafkaSchemaRegistryContractIssue =>
  issue(
    declaration,
    subject,
    "RegistryUnavailable",
    `Schema Registry request for subject ${JSON.stringify(subject)} failed: ${failure.message}`,
  );

const canonicalWellKnownTypes = new Set([
  "google/protobuf/compiler/plugin.proto",
  "google/protobuf/any.proto",
  "google/protobuf/api.proto",
  "google/protobuf/cpp_features.proto",
  "google/protobuf/descriptor.proto",
  "google/protobuf/duration.proto",
  "google/protobuf/empty.proto",
  "google/protobuf/field_mask.proto",
  "google/protobuf/go_features.proto",
  "google/protobuf/java_features.proto",
  "google/protobuf/source_context.proto",
  "google/protobuf/struct.proto",
  "google/protobuf/timestamp.proto",
  "google/protobuf/type.proto",
  "google/protobuf/wrappers.proto",
]);

const canonicalWellKnownType = (name: string): boolean => canonicalWellKnownTypes.has(name);

const descriptorGraph = (root: DescFile): ReadonlyMap<string, DescFile> => {
  const files = new Map<string, DescFile>();
  const visit = (file: DescFile): void => {
    if (files.has(file.proto.name)) {
      return;
    }
    files.set(file.proto.name, file);
    for (const dependency of file.dependencies) {
      visit(dependency);
    }
  };
  visit(root);
  return files;
};

const bytesEqual = (left: Uint8Array, right: Uint8Array): boolean =>
  left.byteLength === right.byteLength && left.every((byte, index) => byte === right[index]);

const descriptorsEqual = (left: FileDescriptorProto, right: FileDescriptorProto): boolean =>
  bytesEqual(toBinary(FileDescriptorProtoSchema, left), toBinary(FileDescriptorProtoSchema, right));

const validVersions = (versions: ReadonlyArray<number>): boolean =>
  versions.every((version) => Number.isSafeInteger(version) && version > 0) &&
  new Set(versions).size === versions.length;

const sameIndexes = (left: ReadonlyArray<number>, right: ReadonlyArray<number>): boolean =>
  left.length === right.length && left.every((index, position) => index === right[position]);

const sharedSchemaRegistryReader = (
  reader: KafkaSchemaRegistryReader,
): KafkaSchemaRegistryReader => {
  const compatibilities = new Map<string, Exit.Exit<string, KafkaSchemaRegistryRequestFailure>>();
  const versionHistories = new Map<
    string,
    Exit.Exit<ReadonlyArray<number>, KafkaSchemaRegistryRequestFailure>
  >();
  const schemas = new Map<
    string,
    Exit.Exit<KafkaSchemaRegistrySchemaVersion, KafkaSchemaRegistryRequestFailure>
  >();
  const cached = <A>(
    cache: Map<string, Exit.Exit<A, KafkaSchemaRegistryRequestFailure>>,
    key: string,
    read: () => Effect.Effect<A, KafkaSchemaRegistryRequestFailure>,
  ): Effect.Effect<A, KafkaSchemaRegistryRequestFailure> => {
    const result = cache.get(key);
    if (result !== undefined) {
      return result._tag === "Success"
        ? Effect.succeed(result.value)
        : Effect.failCause(result.cause);
    }
    return read().pipe(
      Effect.matchEffect({
        onFailure: (failure) =>
          Effect.sync(() => cache.set(key, Exit.fail(failure))).pipe(
            Effect.andThen(Effect.fail(failure)),
          ),
        onSuccess: (value) =>
          Effect.sync(() => cache.set(key, Exit.succeed(value))).pipe(
            Effect.andThen(Effect.succeed(value)),
          ),
      }),
    );
  };
  return {
    effectiveCompatibility: (subject) =>
      cached(compatibilities, subject, () => reader.effectiveCompatibility(subject)),
    versions: (subject, includeDeleted) =>
      cached(versionHistories, JSON.stringify([subject, includeDeleted]), () =>
        reader.versions(subject, includeDeleted),
      ),
    schema: (subject, version) =>
      cached(schemas, JSON.stringify([subject, version]), () => reader.schema(subject, version)),
  };
};

const validateDeclaration = Effect.fn("KafkaSchemaRegistry.contract.validateDeclaration")(
  function* (
    declaration: KafkaSchemaRegistryDeclaration,
    reader: KafkaSchemaRegistryReader,
  ): Effect.fn.Return<KafkaResolvedSchemaRegistryContract, KafkaSchemaRegistryContractIssue> {
    const generatedFiles = descriptorGraph(declaration.descriptor.file);
    const loadedGraphs = new Map<string, LoadedGraph>();
    const loadingGraphs = new Set<string>();

    const readCompatibility = (subject: string) =>
      reader
        .effectiveCompatibility(subject)
        .pipe(Effect.mapError((failure) => unavailable(declaration, subject, failure)));
    const readVersions = (subject: string, includeDeleted: boolean) =>
      reader
        .versions(subject, includeDeleted)
        .pipe(Effect.mapError((failure) => unavailable(declaration, subject, failure)));
    const readSchema = (subject: string, version: number) => {
      return reader.schema(subject, version).pipe(
        Effect.mapError((failure) => unavailable(declaration, subject, failure)),
        Effect.flatMap((record) => {
          if (
            record.subject !== subject ||
            record.version !== version ||
            !Number.isSafeInteger(record.id) ||
            record.id <= 0
          ) {
            return Effect.fail(
              issue(
                declaration,
                subject,
                "MalformedSchema",
                `Schema Registry returned malformed metadata for subject ${JSON.stringify(subject)} version ${String(version)}.`,
                version,
              ),
            );
          }
          return Effect.succeed(record);
        }),
      );
    };

    const loadGraph = (
      subject: string,
      version: number,
    ): Effect.Effect<LoadedGraph, KafkaSchemaRegistryContractIssue> => {
      const graphKey = JSON.stringify([subject, version]);
      const cached = loadedGraphs.get(graphKey);
      if (cached !== undefined) {
        return Effect.succeed(cached);
      }
      if (loadingGraphs.has(graphKey)) {
        return Effect.fail(
          issue(
            declaration,
            subject,
            "MalformedSchema",
            `Schema Registry references contain a cycle at subject ${JSON.stringify(subject)} version ${String(version)}.`,
            version,
          ),
        );
      }
      loadingGraphs.add(graphKey);
      return Effect.gen(function* () {
        const record = yield* readSchema(subject, version);
        if (record.schemaType !== "PROTOBUF") {
          return yield* Effect.fail(
            issue(
              declaration,
              subject,
              "SchemaTypeMismatch",
              `Subject ${JSON.stringify(subject)} version ${String(version)} is ${JSON.stringify(record.schemaType)}, not PROTOBUF.`,
              version,
              record.id,
            ),
          );
        }
        if (record.descriptor.name.length === 0) {
          return yield* Effect.fail(
            issue(
              declaration,
              subject,
              "MalformedSchema",
              `Subject ${JSON.stringify(subject)} version ${String(version)} has no protobuf file name.`,
              version,
              record.id,
            ),
          );
        }
        const references = new Map<string, KafkaSchemaRegistryReference>();
        for (const reference of record.references) {
          if (
            reference.name.length === 0 ||
            reference.subject.length === 0 ||
            !Number.isSafeInteger(reference.version) ||
            reference.version <= 0 ||
            references.has(reference.name)
          ) {
            return yield* Effect.fail(
              issue(
                declaration,
                subject,
                "MalformedSchema",
                `Subject ${JSON.stringify(subject)} version ${String(version)} has malformed protobuf references.`,
                version,
                record.id,
              ),
            );
          }
          references.set(reference.name, reference);
        }

        const files = new Map<string, FileDescriptorProto>([
          [record.descriptor.name, record.descriptor],
        ]);
        const subjects = new Set<string>([subject]);
        const mergeFile = (
          name: string,
          descriptor: FileDescriptorProto,
        ): KafkaSchemaRegistryContractIssue | undefined => {
          const existing = files.get(name);
          if (existing !== undefined && !descriptorsEqual(existing, descriptor)) {
            return issue(
              declaration,
              subject,
              "MalformedSchema",
              `Subject ${JSON.stringify(subject)} version ${String(version)} resolves conflicting descriptors for ${JSON.stringify(name)}.`,
              version,
              record.id,
            );
          }
          files.set(name, descriptor);
          return undefined;
        };
        const consumedReferences = new Set<string>();
        for (const dependency of record.descriptor.dependency) {
          const generated = generatedFiles.get(dependency);
          if (canonicalWellKnownType(dependency) && generated !== undefined) {
            if (references.has(dependency)) {
              consumedReferences.add(dependency);
            }
            for (const file of descriptorGraph(generated).values()) {
              const conflict = mergeFile(file.proto.name, file.proto);
              if (conflict !== undefined) {
                return yield* Effect.fail(conflict);
              }
            }
            continue;
          }
          const reference = references.get(dependency);
          if (reference !== undefined) {
            consumedReferences.add(dependency);
            const referenced = yield* loadGraph(reference.subject, reference.version);
            if (referenced.root.proto.name !== dependency) {
              return yield* Effect.fail(
                issue(
                  declaration,
                  subject,
                  "MissingSchemaReference",
                  `Reference ${JSON.stringify(dependency)} resolves to protobuf file ${JSON.stringify(referenced.root.proto.name)}.`,
                  version,
                  record.id,
                ),
              );
            }
            for (const file of descriptorGraph(referenced.root).values()) {
              const conflict = mergeFile(file.proto.name, file.proto);
              if (conflict !== undefined) {
                return yield* Effect.fail(conflict);
              }
            }
            for (const referencedSubject of referenced.subjects) {
              subjects.add(referencedSubject);
            }
            continue;
          }
          return yield* Effect.fail(
            issue(
              declaration,
              subject,
              "MissingSchemaReference",
              `Subject ${JSON.stringify(subject)} version ${String(version)} does not resolve import ${JSON.stringify(dependency)}.`,
              version,
              record.id,
            ),
          );
        }
        if (consumedReferences.size !== references.size) {
          return yield* Effect.fail(
            issue(
              declaration,
              subject,
              "MalformedSchema",
              `Subject ${JSON.stringify(subject)} version ${String(version)} declares references that are not protobuf imports.`,
              version,
              record.id,
            ),
          );
        }
        const root = yield* Effect.try({
          try: () => {
            const registry = createFileRegistry(record.descriptor, (name) => files.get(name));
            return Option.getOrThrow(
              Option.fromUndefinedOr(registry.getFile(record.descriptor.name)),
            );
          },
          catch: () =>
            issue(
              declaration,
              subject,
              "MalformedSchema",
              `Subject ${JSON.stringify(subject)} version ${String(version)} does not contain a valid serialized protobuf descriptor graph.`,
              version,
              record.id,
            ),
        });
        const graph: LoadedGraph = Object.freeze({
          id: record.id,
          version,
          root,
          subjects,
        });
        loadingGraphs.delete(graphKey);
        loadedGraphs.set(graphKey, graph);
        return graph;
      });
    };

    const loadHistory = (
      subject: string,
    ): Effect.Effect<LoadedHistory, KafkaSchemaRegistryContractIssue> => {
      return Effect.gen(function* () {
        const compatibility = yield* readCompatibility(subject);
        if (compatibility !== "FULL_TRANSITIVE") {
          return yield* Effect.fail(
            issue(
              declaration,
              subject,
              "CompatibilityPolicyMismatch",
              `Subject ${JSON.stringify(subject)} requires effective FULL_TRANSITIVE compatibility; observed ${JSON.stringify(compatibility)}.`,
            ),
          );
        }
        const active = yield* readVersions(subject, false);
        const all = yield* readVersions(subject, true);
        if (!validVersions(active) || !validVersions(all) || active.length === 0) {
          return yield* Effect.fail(
            issue(
              declaration,
              subject,
              "MalformedVersionHistory",
              `Subject ${JSON.stringify(subject)} returned an invalid version history.`,
            ),
          );
        }
        const activeSet = new Set(active);
        const allSet = new Set(all);
        if (active.some((version) => !allSet.has(version))) {
          return yield* Effect.fail(
            issue(
              declaration,
              subject,
              "MalformedVersionHistory",
              `Subject ${JSON.stringify(subject)} returned inconsistent active and deleted version histories.`,
            ),
          );
        }
        const softDeleted = [...all]
          .sort((left, right) => left - right)
          .find((version) => !activeSet.has(version));
        if (softDeleted !== undefined) {
          return yield* Effect.fail(
            issue(
              declaration,
              subject,
              "SoftDeletedVersion",
              `Subject ${JSON.stringify(subject)} version ${String(softDeleted)} is soft-deleted.`,
              softDeleted,
            ),
          );
        }
        const ordered = [...active].sort((left, right) => left - right);
        for (let index = 0; index < ordered.length; index += 1) {
          const expected = index + 1;
          if (ordered[index] !== expected) {
            return yield* Effect.fail(
              issue(
                declaration,
                subject,
                "HardDeletedVersionGap",
                `Subject ${JSON.stringify(subject)} has a detectable hard-deleted version gap at ${String(expected)}.`,
                expected,
              ),
            );
          }
        }
        const versions = yield* Effect.forEach(ordered, (version) => loadGraph(subject, version));
        for (let currentIndex = 1; currentIndex < versions.length; currentIndex += 1) {
          const current = Option.getOrThrow(Option.fromUndefinedOr(versions[currentIndex]));
          for (let previousIndex = 0; previousIndex < currentIndex; previousIndex += 1) {
            const previous = Option.getOrThrow(Option.fromUndefinedOr(versions[previousIndex]));
            const mismatch = kafkaProtobufWireCompatibilityIssues(previous.root, current.root)[0];
            if (mismatch !== undefined) {
              return yield* Effect.fail(
                issue(
                  declaration,
                  subject,
                  "WireHistoryMismatch",
                  `Subject ${JSON.stringify(subject)} versions ${String(previous.version)} and ${String(current.version)} violate Buf WIRE rule ${mismatch.rule} at ${mismatch.path}: ${mismatch.message}`,
                  current.version,
                  current.id,
                ),
              );
            }
          }
        }
        const referencedSubjects = new Set<string>();
        for (const version of versions) {
          for (const referencedSubject of version.subjects) {
            if (referencedSubject !== subject) {
              referencedSubjects.add(referencedSubject);
            }
          }
        }
        const history: LoadedHistory = Object.freeze({
          subject,
          versions: Object.freeze(versions),
          referencedSubjects,
        });
        return history;
      });
    };

    const rootHistory = yield* loadHistory(declaration.subject);
    const pending = [...rootHistory.referencedSubjects];
    const dependencySubjects = new Set<string>([declaration.subject]);
    while (pending.length > 0) {
      const subject = Option.getOrThrow(Option.fromUndefinedOr(pending.shift()));
      if (dependencySubjects.has(subject)) {
        continue;
      }
      dependencySubjects.add(subject);
      const history = yield* loadHistory(subject);
      for (const referencedSubject of history.referencedSubjects) {
        pending.push(referencedSubject);
      }
    }

    let anchored = false;
    const resolvedVersions: Array<KafkaResolvedSchemaRegistryVersion> = [];
    const schemaIds = new Map<number, Array<readonly [number, ...ReadonlyArray<number>]>>();
    for (const version of rootHistory.versions) {
      const messageIndexes = kafkaProtobufMessageIndexes(
        version.root,
        declaration.descriptor.typeName,
      );
      const normalizedMessageIndexes = kafkaProtobufNormalizedMessageIndexes(
        version.root,
        declaration.descriptor.typeName,
      );
      if (messageIndexes === undefined) {
        return yield* Effect.fail(
          issue(
            declaration,
            declaration.subject,
            "GeneratedSchemaMismatch",
            `Subject ${JSON.stringify(declaration.subject)} version ${String(version.version)} does not contain generated message ${JSON.stringify(declaration.descriptor.typeName)}.`,
            version.version,
            version.id,
          ),
        );
      }
      const registered = Option.getOrThrow(
        Option.fromUndefinedOr(kafkaProtobufMessageAtIndexes(version.root, messageIndexes)),
      );
      const generatedToRegistered = kafkaProtobufMessageWireCompatibilityIssues(
        declaration.descriptor,
        registered,
      );
      const registeredToGenerated = kafkaProtobufMessageWireCompatibilityIssues(
        registered,
        declaration.descriptor,
      );
      anchored ||= generatedToRegistered.length === 0 && registeredToGenerated.length === 0;
      const mismatch = kafkaProtobufMessageReaderCompatibilityIssues(
        registered,
        declaration.descriptor,
      )[0];
      if (mismatch !== undefined) {
        return yield* Effect.fail(
          issue(
            declaration,
            declaration.subject,
            "GeneratedSchemaMismatch",
            `Subject ${JSON.stringify(declaration.subject)} version ${String(version.version)} is not decodable by generated message ${JSON.stringify(declaration.descriptor.typeName)}: Buf WIRE rule ${mismatch.rule} at ${mismatch.path}: ${mismatch.message}`,
            version.version,
            version.id,
          ),
        );
      }
      resolvedVersions.push(
        Object.freeze({
          id: version.id,
          version: version.version,
          root: version.root,
          messageIndexes,
        }),
      );
      const allowed = schemaIds.get(version.id) ?? [];
      if (!allowed.some((indexes) => sameIndexes(indexes, messageIndexes))) {
        allowed.push(messageIndexes);
      }
      if (
        normalizedMessageIndexes !== undefined &&
        !allowed.some((indexes) => sameIndexes(indexes, normalizedMessageIndexes))
      ) {
        allowed.push(normalizedMessageIndexes);
      }
      schemaIds.set(version.id, allowed);
    }
    if (!anchored) {
      return yield* Effect.fail(
        issue(
          declaration,
          declaration.subject,
          "GeneratedSchemaMismatch",
          `Generated message ${JSON.stringify(declaration.descriptor.typeName)} is not mutually Buf WIRE-compatible with any active version of subject ${JSON.stringify(declaration.subject)}.`,
        ),
      );
    }
    return Object.freeze({
      ...declaration,
      dependencySubjects: Object.freeze([...dependencySubjects].sort()),
      versions: Object.freeze(resolvedVersions),
      schemaIds: new Map(
        [...schemaIds].map(([schemaId, indexes]) => [schemaId, Object.freeze(indexes)]),
      ),
    });
  },
);

export type KafkaSchemaRegistryContractResolution = {
  readonly contracts: ReadonlyArray<KafkaResolvedSchemaRegistryContract>;
  readonly issues: ReadonlyArray<KafkaSchemaRegistryContractIssue>;
};

export const inspectKafkaSchemaRegistryContracts = Effect.fn(
  "KafkaSchemaRegistry.contract.inspect",
)(function* (
  declarations: ReadonlyArray<KafkaSchemaRegistryDeclaration>,
  reader: KafkaSchemaRegistryReader,
) {
  const sharedReader = sharedSchemaRegistryReader(reader);
  const results = yield* Effect.forEach(declarations, (declaration) =>
    validateDeclaration(declaration, sharedReader).pipe(
      Effect.match({
        onFailure: (contractIssue) => ({ contractIssue }),
        onSuccess: (contract) => ({ contract }),
      }),
    ),
  );
  const issues = results.flatMap((result) =>
    "contractIssue" in result ? [result.contractIssue] : [],
  );
  const contracts = results.flatMap((result) => ("contract" in result ? [result.contract] : []));
  return Object.freeze({
    contracts: Object.freeze(contracts),
    issues: Object.freeze(issues),
  });
});

export const resolveKafkaSchemaRegistryContracts = Effect.fn(
  "KafkaSchemaRegistry.contract.resolve",
)(function* (
  declarations: ReadonlyArray<KafkaSchemaRegistryDeclaration>,
  reader: KafkaSchemaRegistryReader,
): Effect.fn.Return<
  ReadonlyArray<KafkaResolvedSchemaRegistryContract>,
  KafkaSchemaRegistryContractValidationFailure
> {
  const resolution = yield* inspectKafkaSchemaRegistryContracts(declarations, reader);
  const issues = resolution.issues;
  const firstIssue = issues[0];
  if (firstIssue !== undefined) {
    const failure: KafkaSchemaRegistryContractValidationFailure = {
      _tag: "KafkaSchemaRegistryContractValidationFailure",
      message: "Kafka Schema Registry Protobuf validation failed before runtime startup.",
      issues: [firstIssue, ...issues.slice(1)],
    };
    return yield* Effect.fail(failure);
  }
  return resolution.contracts;
});

export type KafkaSchemaRegistryFrameMismatch = {
  readonly _tag: "Mismatch";
  readonly reason: "frame" | "schema";
  readonly schemaId: number | null;
  readonly message: string;
};

export type KafkaSchemaRegistryValidatedFrame = {
  readonly _tag: "Valid";
  readonly schemaId: number;
  readonly payload: Uint8Array;
};

export const validateKafkaSchemaRegistryFrame = (
  contract: KafkaResolvedSchemaRegistryContract,
  bytes: Uint8Array,
): KafkaSchemaRegistryFrameMismatch | KafkaSchemaRegistryValidatedFrame => {
  const frame = parseKafkaSchemaRegistryProtobufFrame(bytes);
  if (frame._tag === "KafkaSchemaRegistryFrameParseFailure") {
    if (frame.schemaId !== null && !contract.schemaIds.has(frame.schemaId)) {
      return {
        _tag: "Mismatch",
        reason: "schema",
        schemaId: frame.schemaId,
        message: `Schema ID ${String(frame.schemaId)} is not an active validated version of subject ${JSON.stringify(contract.subject)}.`,
      };
    }
    return {
      _tag: "Mismatch",
      reason: "frame",
      schemaId: frame.schemaId,
      message: frame.message,
    };
  }
  const allowed = contract.schemaIds.get(frame.schemaId);
  if (allowed === undefined) {
    return {
      _tag: "Mismatch",
      reason: "schema",
      schemaId: frame.schemaId,
      message: `Schema ID ${String(frame.schemaId)} is not an active validated version of subject ${JSON.stringify(contract.subject)}.`,
    };
  }
  if (!allowed.some((indexes) => sameIndexes(indexes, frame.messageIndexes))) {
    return {
      _tag: "Mismatch",
      reason: "schema",
      schemaId: frame.schemaId,
      message: `Schema ID ${String(frame.schemaId)} selected a protobuf message that does not match ${JSON.stringify(contract.descriptor.typeName)}.`,
    };
  }
  return {
    _tag: "Valid",
    schemaId: frame.schemaId,
    payload: frame.payload,
  };
};
