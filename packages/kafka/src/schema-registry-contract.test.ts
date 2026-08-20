import { clone, create, createFileRegistry, type DescMessage } from "@bufbuild/protobuf";
import {
  DescriptorProtoSchema,
  FieldDescriptorProtoSchema,
  FieldDescriptorProto_Label,
  FieldDescriptorProto_Type,
  FileDescriptorProtoSchema,
  FileDescriptorSetSchema,
  TimestampSchema,
} from "@bufbuild/protobuf/wkt";
import type { FileDescriptorProto } from "@bufbuild/protobuf/wkt";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Option } from "effect";
import { OrderValueSchema } from "./test-fixtures/orders_pb";
import {
  resolveKafkaSchemaRegistryContracts,
  validateKafkaSchemaRegistryFrame,
  type KafkaSchemaRegistryContractIssue,
  type KafkaSchemaRegistryDeclaration,
  type KafkaSchemaRegistryReader,
  type KafkaSchemaRegistrySchemaVersion,
} from "./schema-registry-contract";

type RegistryFixture = {
  readonly compatibility?: Readonly<Record<string, string>>;
  readonly active?: Readonly<Record<string, ReadonlyArray<number>>>;
  readonly all?: Readonly<Record<string, ReadonlyArray<number>>>;
  readonly schemas?: Readonly<Record<string, KafkaSchemaRegistrySchemaVersion>>;
};

const fixtureKey = (subject: string, version: number): string => `${subject}:${String(version)}`;

const reader = (fixture: RegistryFixture): KafkaSchemaRegistryReader => ({
  effectiveCompatibility: (subject) => {
    const compatibility = fixture.compatibility?.[subject];
    return compatibility === undefined
      ? Effect.fail({ message: "compatibility unavailable" })
      : Effect.succeed(compatibility);
  },
  versions: (subject, includeDeleted) => {
    const versions = (includeDeleted ? fixture.all : fixture.active)?.[subject];
    return versions === undefined
      ? Effect.fail({ message: "versions unavailable" })
      : Effect.succeed(versions);
  },
  schema: (subject, version) => {
    const schema = fixture.schemas?.[fixtureKey(subject, version)];
    return schema === undefined
      ? Effect.fail({ message: "schema unavailable" })
      : Effect.succeed(schema);
  },
});

const declaration = (subject = "orders-value"): KafkaSchemaRegistryDeclaration => ({
  region: "eu-west-1",
  viewServerTopic: "orders",
  sourceTopic: "source-orders",
  side: "value",
  subject,
  descriptor: OrderValueSchema,
});

const expectedIssue = (
  code: KafkaSchemaRegistryContractIssue["code"],
  message: string,
  overrides: Partial<
    Pick<KafkaSchemaRegistryContractIssue, "schemaId" | "subject" | "version" | "viewServerTopic">
  > = {},
): KafkaSchemaRegistryContractIssue => ({
  _tag: "KafkaSchemaRegistryContractIssue",
  region: "eu-west-1",
  viewServerTopic: "orders",
  sourceTopic: "source-orders",
  side: "value",
  subject: "orders-value",
  code,
  version: null,
  schemaId: null,
  message,
  ...overrides,
});

const schemaVersion = (
  version: number,
  id: number,
  descriptor = OrderValueSchema.file.proto,
): KafkaSchemaRegistrySchemaVersion => ({
  subject: "orders-value",
  version,
  id,
  schemaType: "PROTOBUF",
  references: [],
  descriptor,
});

const referencedSchemaVersion = (
  subject: string,
  version: number,
  id: number,
  descriptor: FileDescriptorProto,
  references: KafkaSchemaRegistrySchemaVersion["references"] = [],
): KafkaSchemaRegistrySchemaVersion => ({
  subject,
  version,
  id,
  schemaType: "PROTOBUF",
  references,
  descriptor,
});

const frame = (schemaId: number, messageIndex: number): Uint8Array => {
  const bytes = new Uint8Array(7);
  const view = new DataView(bytes.buffer);
  bytes[0] = 0;
  view.setUint32(1, schemaId, false);
  bytes[5] = 2;
  bytes[6] = messageIndex * 2;
  return bytes;
};

const currentDescriptor = () => {
  const descriptor = clone(FileDescriptorProtoSchema, OrderValueSchema.file.proto);
  const order = descriptor.messageType[0];
  if (order === undefined) {
    throw new Error("OrderValue descriptor missing");
  }
  order.field.push(
    create(FieldDescriptorProtoSchema, {
      name: "region",
      number: 3,
      label: FieldDescriptorProto_Label.OPTIONAL,
      type: FieldDescriptorProto_Type.STRING,
    }),
  );
  return descriptor;
};

const resolveFailure = (
  fixture: RegistryFixture,
  declarations: ReadonlyArray<KafkaSchemaRegistryDeclaration> = [declaration()],
) => resolveKafkaSchemaRegistryContracts(declarations, reader(fixture)).pipe(Effect.flip);

const generatedMessage = (
  files: ReadonlyArray<FileDescriptorProto>,
  rootName: string,
  typeName: string,
): DescMessage => {
  const registry = createFileRegistry(create(FileDescriptorSetSchema, { file: [...files] }));
  const root = registry.getFile(rootName);
  if (root === undefined) {
    throw new Error("generated root descriptor missing");
  }
  const pending = [...root.messages];
  while (pending.length > 0) {
    const candidate = Option.getOrThrow(Option.fromUndefinedOr(pending.shift()));
    if (candidate.typeName === typeName) {
      return candidate;
    }
    pending.push(...candidate.nestedMessages);
  }
  throw new Error(`generated message ${typeName} missing`);
};

describe("Kafka Schema Registry Protobuf contracts", () => {
  it.effect("accepts FULL_TRANSITIVE WIRE history, anchors generated code, and caches IDs", () =>
    Effect.gen(function* () {
      const contracts = yield* resolveKafkaSchemaRegistryContracts(
        [declaration()],
        reader({
          compatibility: { "orders-value": "FULL_TRANSITIVE" },
          active: { "orders-value": [1, 2] },
          all: { "orders-value": [1, 2] },
          schemas: {
            "orders-value:1": schemaVersion(1, 41),
            "orders-value:2": schemaVersion(2, 42, currentDescriptor()),
          },
        }),
      );

      expect(contracts).toHaveLength(1);
      const contract = contracts[0];
      if (contract === undefined) {
        throw new Error("resolved contract missing");
      }
      expect(contract.dependencySubjects).toStrictEqual(["orders-value"]);
      expect([...contract.schemaIds.keys()]).toStrictEqual([41, 42]);
      expect(validateKafkaSchemaRegistryFrame(contract, frame(41, 0))).toStrictEqual({
        _tag: "Valid",
        schemaId: 41,
        payload: Uint8Array.from([]),
      });
      expect(validateKafkaSchemaRegistryFrame(contract, frame(999, 0))).toStrictEqual({
        _tag: "Mismatch",
        reason: "schema",
        schemaId: 999,
        message: 'Schema ID 999 is not an active validated version of subject "orders-value".',
      });
      expect(validateKafkaSchemaRegistryFrame(contract, frame(41, 1))).toStrictEqual({
        _tag: "Mismatch",
        reason: "schema",
        schemaId: 41,
        message:
          'Schema ID 41 selected a protobuf message that does not match "viewserver.runtime.test.OrderValue".',
      });
    }),
  );

  it.effect("rejects any effective policy weaker than FULL_TRANSITIVE", () =>
    Effect.gen(function* () {
      const failure = yield* Effect.flip(
        resolveKafkaSchemaRegistryContracts(
          [declaration()],
          reader({
            compatibility: { "orders-value": "BACKWARD_TRANSITIVE" },
            active: { "orders-value": [1] },
            all: { "orders-value": [1] },
            schemas: { "orders-value:1": schemaVersion(1, 41) },
          }),
        ),
      );

      expect(failure.issues[0]).toStrictEqual({
        _tag: "KafkaSchemaRegistryContractIssue",
        region: "eu-west-1",
        viewServerTopic: "orders",
        sourceTopic: "source-orders",
        side: "value",
        subject: "orders-value",
        code: "CompatibilityPolicyMismatch",
        version: null,
        schemaId: null,
        message:
          'Subject "orders-value" requires effective FULL_TRANSITIVE compatibility; observed "BACKWARD_TRANSITIVE".',
      });
    }),
  );

  it.effect("turns every Registry read boundary failure into structured unavailability", () =>
    Effect.gen(function* () {
      const compatibility = yield* resolveFailure({});
      const activeVersions = yield* resolveFailure({
        compatibility: { "orders-value": "FULL_TRANSITIVE" },
      });
      const allVersions = yield* resolveFailure({
        compatibility: { "orders-value": "FULL_TRANSITIVE" },
        active: { "orders-value": [1] },
      });
      const schema = yield* resolveFailure({
        compatibility: { "orders-value": "FULL_TRANSITIVE" },
        active: { "orders-value": [1] },
        all: { "orders-value": [1] },
      });

      expect([
        compatibility.issues,
        activeVersions.issues,
        allVersions.issues,
        schema.issues,
      ]).toStrictEqual([
        [
          expectedIssue(
            "RegistryUnavailable",
            'Schema Registry request for subject "orders-value" failed: compatibility unavailable',
          ),
        ],
        [
          expectedIssue(
            "RegistryUnavailable",
            'Schema Registry request for subject "orders-value" failed: versions unavailable',
          ),
        ],
        [
          expectedIssue(
            "RegistryUnavailable",
            'Schema Registry request for subject "orders-value" failed: versions unavailable',
          ),
        ],
        [
          expectedIssue(
            "RegistryUnavailable",
            'Schema Registry request for subject "orders-value" failed: schema unavailable',
          ),
        ],
      ]);
    }),
  );

  it.effect("rejects malformed active and deleted version histories", () =>
    Effect.gen(function* () {
      const invalidActive = yield* resolveFailure({
        compatibility: { "orders-value": "FULL_TRANSITIVE" },
        active: { "orders-value": [0] },
        all: { "orders-value": [1] },
      });
      const nonIntegerActive = yield* resolveFailure({
        compatibility: { "orders-value": "FULL_TRANSITIVE" },
        active: { "orders-value": [1.5] },
        all: { "orders-value": [1] },
      });
      const duplicateActive = yield* resolveFailure({
        compatibility: { "orders-value": "FULL_TRANSITIVE" },
        active: { "orders-value": [1, 1] },
        all: { "orders-value": [1] },
      });
      const emptyActive = yield* resolveFailure({
        compatibility: { "orders-value": "FULL_TRANSITIVE" },
        active: { "orders-value": [] },
        all: { "orders-value": [] },
      });
      const invalidDeleted = yield* resolveFailure({
        compatibility: { "orders-value": "FULL_TRANSITIVE" },
        active: { "orders-value": [1] },
        all: { "orders-value": [-1] },
      });
      const inconsistent = yield* resolveFailure({
        compatibility: { "orders-value": "FULL_TRANSITIVE" },
        active: { "orders-value": [1] },
        all: { "orders-value": [2] },
      });

      const invalidHistory = [
        expectedIssue(
          "MalformedVersionHistory",
          'Subject "orders-value" returned an invalid version history.',
        ),
      ];
      expect([
        invalidActive.issues,
        nonIntegerActive.issues,
        duplicateActive.issues,
        emptyActive.issues,
        invalidDeleted.issues,
        inconsistent.issues,
      ]).toStrictEqual([
        invalidHistory,
        invalidHistory,
        invalidHistory,
        invalidHistory,
        invalidHistory,
        [
          expectedIssue(
            "MalformedVersionHistory",
            'Subject "orders-value" returned inconsistent active and deleted version histories.',
          ),
        ],
      ]);
    }),
  );

  it.effect("rejects malformed Registry schema metadata, types, names, and references", () =>
    Effect.gen(function* () {
      const base = schemaVersion(1, 41);
      const malformedRecords: ReadonlyArray<KafkaSchemaRegistrySchemaVersion> = [
        { ...base, subject: "wrong-subject" },
        { ...base, version: 2 },
        { ...base, id: 0 },
        { ...base, id: 1.5 },
        { ...base, schemaType: "AVRO" },
        {
          ...base,
          descriptor: create(FileDescriptorProtoSchema, {
            ...base.descriptor,
            name: "",
          }),
        },
        { ...base, references: [{ name: "", subject: "shared", version: 1 }] },
        { ...base, references: [{ name: "shared.proto", subject: "", version: 1 }] },
        { ...base, references: [{ name: "shared.proto", subject: "shared", version: 1.5 }] },
        { ...base, references: [{ name: "shared.proto", subject: "shared", version: 0 }] },
        {
          ...base,
          references: [
            { name: "shared.proto", subject: "shared", version: 1 },
            { name: "shared.proto", subject: "shared-copy", version: 1 },
          ],
        },
      ];
      const failures = yield* Effect.forEach(malformedRecords, (record) =>
        resolveFailure({
          compatibility: { "orders-value": "FULL_TRANSITIVE" },
          active: { "orders-value": [1] },
          all: { "orders-value": [1] },
          schemas: { "orders-value:1": record },
        }),
      );

      expect(failures.map((failure) => failure.issues)).toStrictEqual([
        [
          expectedIssue(
            "MalformedSchema",
            'Schema Registry returned malformed metadata for subject "orders-value" version 1.',
            { version: 1 },
          ),
        ],
        [
          expectedIssue(
            "MalformedSchema",
            'Schema Registry returned malformed metadata for subject "orders-value" version 1.',
            { version: 1 },
          ),
        ],
        [
          expectedIssue(
            "MalformedSchema",
            'Schema Registry returned malformed metadata for subject "orders-value" version 1.',
            { version: 1 },
          ),
        ],
        [
          expectedIssue(
            "MalformedSchema",
            'Schema Registry returned malformed metadata for subject "orders-value" version 1.',
            { version: 1 },
          ),
        ],
        [
          expectedIssue(
            "SchemaTypeMismatch",
            'Subject "orders-value" version 1 is "AVRO", not PROTOBUF.',
            { version: 1, schemaId: 41 },
          ),
        ],
        [
          expectedIssue(
            "MalformedSchema",
            'Subject "orders-value" version 1 has no protobuf file name.',
            { version: 1, schemaId: 41 },
          ),
        ],
        ...Array.from({ length: 5 }, () => [
          expectedIssue(
            "MalformedSchema",
            'Subject "orders-value" version 1 has malformed protobuf references.',
            { version: 1, schemaId: 41 },
          ),
        ]),
      ]);
    }),
  );

  it.effect("rejects soft-deleted and detectably hard-deleted history", () =>
    Effect.gen(function* () {
      const softDeleted = yield* Effect.flip(
        resolveKafkaSchemaRegistryContracts(
          [declaration()],
          reader({
            compatibility: { "orders-value": "FULL_TRANSITIVE" },
            active: { "orders-value": [1, 3] },
            all: { "orders-value": [1, 2, 3] },
          }),
        ),
      );
      expect(softDeleted.issues).toStrictEqual([
        expectedIssue("SoftDeletedVersion", 'Subject "orders-value" version 2 is soft-deleted.', {
          version: 2,
        }),
      ]);

      const hardDeleted = yield* Effect.flip(
        resolveKafkaSchemaRegistryContracts(
          [declaration()],
          reader({
            compatibility: { "orders-value": "FULL_TRANSITIVE" },
            active: { "orders-value": [1, 3] },
            all: { "orders-value": [1, 3] },
          }),
        ),
      );
      expect(hardDeleted.issues).toStrictEqual([
        expectedIssue(
          "HardDeletedVersionGap",
          'Subject "orders-value" has a detectable hard-deleted version gap at 2.',
          { version: 2 },
        ),
      ]);
    }),
  );

  it.effect("validates recursively referenced custom subjects and their policies", () =>
    Effect.gen(function* () {
      const child = create(FileDescriptorProtoSchema, {
        name: "shared.proto",
        package: "viewserver.runtime.test",
        syntax: "proto3",
        messageType: [{ name: "Shared", field: [] }],
      });
      const root = clone(FileDescriptorProtoSchema, OrderValueSchema.file.proto);
      root.dependency.push("shared.proto");
      const registry = createFileRegistry(create(FileDescriptorSetSchema, { file: [child, root] }));
      const rootFile = registry.getFile(root.name);
      if (rootFile === undefined) {
        throw new Error("root descriptor missing");
      }
      const generated = rootFile.messages[0];
      if (generated === undefined) {
        throw new Error("generated message missing");
      }
      const rootRecord: KafkaSchemaRegistrySchemaVersion = {
        ...schemaVersion(1, 41, root),
        references: [{ name: "shared.proto", subject: "shared", version: 1 }],
      };
      const childRecord: KafkaSchemaRegistrySchemaVersion = {
        subject: "shared",
        version: 1,
        id: 50,
        schemaType: "PROTOBUF",
        references: [],
        descriptor: child,
      };
      const contracts = yield* resolveKafkaSchemaRegistryContracts(
        [{ ...declaration(), descriptor: generated }],
        reader({
          compatibility: {
            "orders-value": "FULL_TRANSITIVE",
            shared: "FULL_TRANSITIVE",
          },
          active: { "orders-value": [1], shared: [1] },
          all: { "orders-value": [1], shared: [1] },
          schemas: {
            "orders-value:1": rootRecord,
            "shared:1": childRecord,
          },
        }),
      );
      expect(contracts[0]?.dependencySubjects).toStrictEqual(["orders-value", "shared"]);

      const failure = yield* Effect.flip(
        resolveKafkaSchemaRegistryContracts(
          [{ ...declaration(), descriptor: generated }],
          reader({
            compatibility: {
              "orders-value": "FULL_TRANSITIVE",
              shared: "BACKWARD",
            },
            active: { "orders-value": [1], shared: [1] },
            all: { "orders-value": [1], shared: [1] },
            schemas: {
              "orders-value:1": rootRecord,
              "shared:1": childRecord,
            },
          }),
        ),
      );
      expect(failure.issues).toStrictEqual([
        expectedIssue(
          "CompatibilityPolicyMismatch",
          'Subject "shared" requires effective FULL_TRANSITIVE compatibility; observed "BACKWARD".',
          { subject: "shared" },
        ),
      ]);
    }),
  );

  it.effect("deduplicates diamond dependencies and rejects conflicting transitive files", () =>
    Effect.gen(function* () {
      const common = create(FileDescriptorProtoSchema, {
        name: "common.proto",
        package: "shared",
        syntax: "proto3",
        messageType: [{ name: "Common" }],
      });
      const left = create(FileDescriptorProtoSchema, {
        name: "left.proto",
        package: "shared",
        syntax: "proto3",
        dependency: [common.name],
        messageType: [{ name: "Left" }],
      });
      const right = create(FileDescriptorProtoSchema, {
        name: "right.proto",
        package: "shared",
        syntax: "proto3",
        dependency: [common.name],
        messageType: [{ name: "Right" }],
      });
      const root = clone(FileDescriptorProtoSchema, OrderValueSchema.file.proto);
      root.dependency.push(left.name, right.name);
      const generated = generatedMessage(
        [common, left, right, root],
        root.name,
        OrderValueSchema.typeName,
      );
      const rootRecord = referencedSchemaVersion("orders-value", 1, 41, root, [
        { name: left.name, subject: "left", version: 1 },
        { name: right.name, subject: "right", version: 1 },
      ]);
      const leftRecord = referencedSchemaVersion("left", 1, 51, left, [
        { name: common.name, subject: "common", version: 1 },
      ]);
      const rightRecord = referencedSchemaVersion("right", 1, 52, right, [
        { name: common.name, subject: "common", version: 1 },
      ]);
      const commonRecord = referencedSchemaVersion("common", 1, 53, common);
      const contracts = yield* resolveKafkaSchemaRegistryContracts(
        [{ ...declaration(), descriptor: generated }],
        reader({
          compatibility: {
            "orders-value": "FULL_TRANSITIVE",
            left: "FULL_TRANSITIVE",
            right: "FULL_TRANSITIVE",
            common: "FULL_TRANSITIVE",
          },
          active: { "orders-value": [1], left: [1], right: [1], common: [1] },
          all: { "orders-value": [1], left: [1], right: [1], common: [1] },
          schemas: {
            "orders-value:1": rootRecord,
            "left:1": leftRecord,
            "right:1": rightRecord,
            "common:1": commonRecord,
          },
        }),
      );
      expect(contracts[0]?.dependencySubjects).toStrictEqual([
        "common",
        "left",
        "orders-value",
        "right",
      ]);

      const conflictingCommon = clone(FileDescriptorProtoSchema, common);
      conflictingCommon.package = "conflicting";
      const conflict = yield* resolveFailure(
        {
          compatibility: { "orders-value": "FULL_TRANSITIVE" },
          active: { "orders-value": [1] },
          all: { "orders-value": [1] },
          schemas: {
            "orders-value:1": rootRecord,
            "left:1": {
              ...leftRecord,
              references: [{ name: common.name, subject: "common-left", version: 1 }],
            },
            "right:1": {
              ...rightRecord,
              references: [{ name: common.name, subject: "common-right", version: 1 }],
            },
            "common-left:1": referencedSchemaVersion("common-left", 1, 54, common),
            "common-right:1": referencedSchemaVersion("common-right", 1, 55, conflictingCommon),
          },
        },
        [{ ...declaration(), descriptor: generated }],
      );
      expect(conflict.issues).toStrictEqual([
        expectedIssue(
          "MalformedSchema",
          'Subject "orders-value" version 1 resolves conflicting descriptors for "common.proto".',
          { version: 1, schemaId: 41 },
        ),
      ]);
    }),
  );

  it.effect("rejects unresolved, unused, misnamed, and cyclic custom references", () =>
    Effect.gen(function* () {
      const root = clone(FileDescriptorProtoSchema, OrderValueSchema.file.proto);
      root.dependency.push("shared.proto");
      const shared = create(FileDescriptorProtoSchema, {
        name: "shared.proto",
        package: "viewserver.runtime.test",
        syntax: "proto3",
        messageType: [{ name: "Shared" }],
      });
      const wrong = create(FileDescriptorProtoSchema, {
        name: "wrong.proto",
        package: "viewserver.runtime.test",
        syntax: "proto3",
        messageType: [{ name: "Shared" }],
      });
      const rootRecord: KafkaSchemaRegistrySchemaVersion = {
        ...schemaVersion(1, 41, root),
        references: [{ name: "shared.proto", subject: "shared", version: 1 }],
      };

      const unresolved = yield* resolveFailure({
        compatibility: { "orders-value": "FULL_TRANSITIVE" },
        active: { "orders-value": [1] },
        all: { "orders-value": [1] },
        schemas: {
          "orders-value:1": { ...rootRecord, references: [] },
        },
      });
      const unused = yield* resolveFailure({
        compatibility: { "orders-value": "FULL_TRANSITIVE" },
        active: { "orders-value": [1] },
        all: { "orders-value": [1] },
        schemas: {
          "orders-value:1": {
            ...schemaVersion(1, 41),
            references: [{ name: "shared.proto", subject: "shared", version: 1 }],
          },
        },
      });
      const misnamed = yield* resolveFailure({
        compatibility: { "orders-value": "FULL_TRANSITIVE" },
        active: { "orders-value": [1] },
        all: { "orders-value": [1] },
        schemas: {
          "orders-value:1": rootRecord,
          "shared:1": {
            subject: "shared",
            version: 1,
            id: 50,
            schemaType: "PROTOBUF",
            references: [],
            descriptor: wrong,
          },
        },
      });

      const cyclicShared = clone(FileDescriptorProtoSchema, shared);
      cyclicShared.dependency.push(root.name);
      const cycle = yield* resolveFailure({
        compatibility: { "orders-value": "FULL_TRANSITIVE" },
        active: { "orders-value": [1] },
        all: { "orders-value": [1] },
        schemas: {
          "orders-value:1": rootRecord,
          "shared:1": {
            subject: "shared",
            version: 1,
            id: 50,
            schemaType: "PROTOBUF",
            references: [{ name: root.name, subject: "orders-value", version: 1 }],
            descriptor: cyclicShared,
          },
        },
      });

      expect([unresolved.issues, unused.issues, misnamed.issues, cycle.issues]).toStrictEqual([
        [
          expectedIssue(
            "MissingSchemaReference",
            'Subject "orders-value" version 1 does not resolve import "shared.proto".',
            { version: 1, schemaId: 41 },
          ),
        ],
        [
          expectedIssue(
            "MalformedSchema",
            'Subject "orders-value" version 1 declares references that are not protobuf imports.',
            { version: 1, schemaId: 41 },
          ),
        ],
        [
          expectedIssue(
            "MissingSchemaReference",
            'Reference "shared.proto" resolves to protobuf file "wrong.proto".',
            { version: 1, schemaId: 41 },
          ),
        ],
        [
          expectedIssue(
            "MalformedSchema",
            'Schema Registry references contain a cycle at subject "orders-value" version 1.',
            { version: 1 },
          ),
        ],
      ]);
    }),
  );

  it.effect("resolves canonical well-known imports from the Buf-generated graph", () =>
    Effect.gen(function* () {
      const root = create(FileDescriptorProtoSchema, {
        name: "events.proto",
        package: "events",
        syntax: "proto3",
        dependency: [TimestampSchema.file.proto.name],
        messageType: [
          {
            name: "Event",
            field: [
              {
                name: "occurred_at",
                number: 1,
                label: FieldDescriptorProto_Label.OPTIONAL,
                type: FieldDescriptorProto_Type.MESSAGE,
                typeName: ".google.protobuf.Timestamp",
              },
            ],
          },
        ],
      });
      const generated = generatedMessage(
        [TimestampSchema.file.proto, root],
        root.name,
        "events.Event",
      );
      const contracts = yield* resolveKafkaSchemaRegistryContracts(
        [{ ...declaration(), descriptor: generated }],
        reader({
          compatibility: { "orders-value": "FULL_TRANSITIVE" },
          active: { "orders-value": [1] },
          all: { "orders-value": [1] },
          schemas: {
            "orders-value:1": referencedSchemaVersion("orders-value", 1, 41, root, [
              {
                name: TimestampSchema.file.proto.name,
                subject: "registry-owned-timestamp",
                version: 1,
              },
            ]),
          },
        }),
      );

      expect(contracts[0]?.dependencySubjects).toStrictEqual(["orders-value"]);
      expect([...(contracts[0]?.schemaIds.keys() ?? [])]).toStrictEqual([41]);

      const conflictingWellKnownType = create(FileDescriptorProtoSchema, {
        name: TimestampSchema.file.proto.name,
        package: "conflicting",
        syntax: "proto3",
        dependency: [TimestampSchema.file.proto.name],
        messageType: [{ name: "Timestamp" }],
      });
      const conflict = yield* resolveFailure(
        {
          compatibility: { "orders-value": "FULL_TRANSITIVE" },
          active: { "orders-value": [1] },
          all: { "orders-value": [1] },
          schemas: {
            "orders-value:1": schemaVersion(1, 41, conflictingWellKnownType),
          },
        },
        [{ ...declaration(), descriptor: generated }],
      );
      expect(conflict.issues).toStrictEqual([
        expectedIssue(
          "MalformedSchema",
          'Subject "orders-value" version 1 resolves conflicting descriptors for "google/protobuf/timestamp.proto".',
          { version: 1, schemaId: 41 },
        ),
      ]);

      const customGoogleFile = create(FileDescriptorProtoSchema, {
        name: "google/protobuf/acme.proto",
        package: "google.protobuf.acme",
        syntax: "proto3",
        messageType: [{ name: "Custom" }],
      });
      const customRoot = create(FileDescriptorProtoSchema, {
        name: "custom-events.proto",
        package: "events",
        syntax: "proto3",
        dependency: [customGoogleFile.name],
        messageType: [{ name: "CustomEvent" }],
      });
      const customGenerated = generatedMessage(
        [customGoogleFile, customRoot],
        customRoot.name,
        "events.CustomEvent",
      );
      const customFailure = yield* resolveFailure(
        {
          compatibility: {
            "orders-value": "FULL_TRANSITIVE",
            "registry-owned-acme": "BACKWARD",
          },
          active: { "orders-value": [1], "registry-owned-acme": [1] },
          all: { "orders-value": [1], "registry-owned-acme": [1] },
          schemas: {
            "orders-value:1": referencedSchemaVersion("orders-value", 1, 41, customRoot, [
              {
                name: customGoogleFile.name,
                subject: "registry-owned-acme",
                version: 1,
              },
            ]),
            "registry-owned-acme:1": referencedSchemaVersion(
              "registry-owned-acme",
              1,
              51,
              customGoogleFile,
            ),
          },
        },
        [{ ...declaration(), descriptor: customGenerated }],
      );
      expect(customFailure.issues).toStrictEqual([
        expectedIssue(
          "CompatibilityPolicyMismatch",
          'Subject "registry-owned-acme" requires effective FULL_TRANSITIVE compatibility; observed "BACKWARD".',
          { subject: "registry-owned-acme" },
        ),
      ]);
    }),
  );

  it.effect("accepts both raw and normalized indexes after synthetic map entries", () =>
    Effect.gen(function* () {
      const root = create(FileDescriptorProtoSchema, {
        name: "mapped.proto",
        package: "mapped",
        syntax: "proto3",
        messageType: [
          { name: "First" },
          {
            name: "Container",
            nestedType: [{ name: "LabelsEntry", options: { mapEntry: true } }, { name: "Payload" }],
          },
        ],
      });
      const generated = generatedMessage([root], root.name, "mapped.Container.Payload");
      const contracts = yield* resolveKafkaSchemaRegistryContracts(
        [{ ...declaration(), descriptor: generated }],
        reader({
          compatibility: { "orders-value": "FULL_TRANSITIVE" },
          active: { "orders-value": [1] },
          all: { "orders-value": [1] },
          schemas: { "orders-value:1": schemaVersion(1, 41, root) },
        }),
      );

      expect(contracts[0]?.schemaIds.get(41)).toStrictEqual([
        [1, 1],
        [1, 0],
      ]);
    }),
  );

  it.effect("resolves message indexes from each registered schema version", () =>
    Effect.gen(function* () {
      const generatedRoot = OrderValueSchema.file.proto;
      const shiftedRoot = clone(FileDescriptorProtoSchema, generatedRoot);
      shiftedRoot.messageType.unshift(create(DescriptorProtoSchema, { name: "Unrelated" }));
      const contracts = yield* resolveKafkaSchemaRegistryContracts(
        [declaration()],
        reader({
          compatibility: { "orders-value": "FULL_TRANSITIVE" },
          active: { "orders-value": [1, 2] },
          all: { "orders-value": [1, 2] },
          schemas: {
            "orders-value:1": schemaVersion(1, 41, generatedRoot),
            "orders-value:2": schemaVersion(2, 42, shiftedRoot),
          },
        }),
      );
      const contract = Option.getOrThrow(Option.fromUndefinedOr(contracts[0]));

      expect([...contract.schemaIds]).toStrictEqual([
        [41, [[0]]],
        [42, [[1]]],
      ]);
      expect(validateKafkaSchemaRegistryFrame(contract, frame(42, 1))).toStrictEqual({
        _tag: "Valid",
        schemaId: 42,
        payload: Uint8Array.from([]),
      });
      expect(validateKafkaSchemaRegistryFrame(contract, frame(42, 0))).toStrictEqual({
        _tag: "Mismatch",
        reason: "schema",
        schemaId: 42,
        message:
          'Schema ID 42 selected a protobuf message that does not match "viewserver.runtime.test.OrderValue".',
      });
    }),
  );

  it.effect("rejects invalid descriptor graphs and incompatible active history", () =>
    Effect.gen(function* () {
      const malformed = clone(FileDescriptorProtoSchema, OrderValueSchema.file.proto);
      const malformedMessage = malformed.messageType[0];
      if (malformedMessage === undefined) {
        throw new Error("OrderValue descriptor missing");
      }
      malformedMessage.field.push(
        create(FieldDescriptorProtoSchema, {
          name: "missing",
          number: 99,
          label: FieldDescriptorProto_Label.OPTIONAL,
          type: FieldDescriptorProto_Type.MESSAGE,
          typeName: ".missing.Type",
        }),
      );
      const malformedFailure = yield* resolveFailure({
        compatibility: { "orders-value": "FULL_TRANSITIVE" },
        active: { "orders-value": [1] },
        all: { "orders-value": [1] },
        schemas: { "orders-value:1": schemaVersion(1, 41, malformed) },
      });

      const incompatible = clone(FileDescriptorProtoSchema, OrderValueSchema.file.proto);
      const incompatibleMessage = incompatible.messageType[0];
      if (incompatibleMessage === undefined) {
        throw new Error("OrderValue descriptor missing");
      }
      incompatibleMessage.field.splice(1, 1);
      const historyFailure = yield* resolveFailure({
        compatibility: { "orders-value": "FULL_TRANSITIVE" },
        active: { "orders-value": [1, 2] },
        all: { "orders-value": [1, 2] },
        schemas: {
          "orders-value:1": schemaVersion(1, 41),
          "orders-value:2": schemaVersion(2, 42, incompatible),
        },
      });

      expect(malformedFailure.issues).toStrictEqual([
        expectedIssue(
          "MalformedSchema",
          'Subject "orders-value" version 1 does not contain a valid serialized protobuf descriptor graph.',
          { version: 1, schemaId: 41 },
        ),
      ]);
      expect(historyFailure.issues).toStrictEqual([
        expectedIssue(
          "WireHistoryMismatch",
          'Subject "orders-value" versions 1 and 2 violate Buf WIRE rule FIELD_NO_DELETE_UNLESS_NUMBER_RESERVED at viewserver.runtime.test.OrderValue.price: Field number 2 was deleted without being reserved.',
          { version: 2, schemaId: 42 },
        ),
      ]);
    }),
  );

  it.effect("rejects missing generated messages and histories without a mutual WIRE anchor", () =>
    Effect.gen(function* () {
      const missing = clone(FileDescriptorProtoSchema, OrderValueSchema.file.proto);
      const missingMessage = missing.messageType[0];
      if (missingMessage === undefined) {
        throw new Error("OrderValue descriptor missing");
      }
      missingMessage.name = "Replacement";
      const missingFailure = yield* resolveFailure({
        compatibility: { "orders-value": "FULL_TRANSITIVE" },
        active: { "orders-value": [1] },
        all: { "orders-value": [1] },
        schemas: { "orders-value:1": schemaVersion(1, 41, missing) },
      });

      const incompatible = clone(FileDescriptorProtoSchema, OrderValueSchema.file.proto);
      const incompatibleMessage = incompatible.messageType[0];
      const incompatibleField = incompatibleMessage?.field[0];
      if (incompatibleField === undefined) {
        throw new Error("OrderValue field missing");
      }
      incompatibleField.type = FieldDescriptorProto_Type.BYTES;
      const anchorFailure = yield* resolveFailure({
        compatibility: { "orders-value": "FULL_TRANSITIVE" },
        active: { "orders-value": [1] },
        all: { "orders-value": [1] },
        schemas: { "orders-value:1": schemaVersion(1, 41, incompatible) },
      });

      const generatedWriterOnly = clone(FileDescriptorProtoSchema, OrderValueSchema.file.proto);
      const generatedWriterOnlyMessage = generatedWriterOnly.messageType[0];
      if (generatedWriterOnlyMessage === undefined) {
        throw new Error("generated writer-only message missing");
      }
      generatedWriterOnlyMessage.field.push(
        create(FieldDescriptorProtoSchema, {
          name: "generated_only",
          number: 3,
          label: FieldDescriptorProto_Label.OPTIONAL,
          type: FieldDescriptorProto_Type.STRING,
        }),
      );
      const generatedWriterOnlyDescriptor = generatedMessage(
        [generatedWriterOnly],
        generatedWriterOnly.name,
        OrderValueSchema.typeName,
      );
      const noMutualAnchorFailure = yield* resolveFailure(
        {
          compatibility: { "orders-value": "FULL_TRANSITIVE" },
          active: { "orders-value": [1] },
          all: { "orders-value": [1] },
          schemas: { "orders-value:1": schemaVersion(1, 41) },
        },
        [{ ...declaration(), descriptor: generatedWriterOnlyDescriptor }],
      );

      const registeredWriterOnly = clone(FileDescriptorProtoSchema, OrderValueSchema.file.proto);
      const registeredWriterOnlyMessage = registeredWriterOnly.messageType[0];
      if (registeredWriterOnlyMessage === undefined) {
        throw new Error("registered writer-only message missing");
      }
      registeredWriterOnlyMessage.field.push(
        create(FieldDescriptorProtoSchema, {
          name: "registered_only",
          number: 3,
          label: FieldDescriptorProto_Label.OPTIONAL,
          type: FieldDescriptorProto_Type.STRING,
        }),
      );
      const reverseNoMutualAnchorFailure = yield* resolveFailure({
        compatibility: { "orders-value": "FULL_TRANSITIVE" },
        active: { "orders-value": [1] },
        all: { "orders-value": [1] },
        schemas: {
          "orders-value:1": schemaVersion(1, 41, registeredWriterOnly),
        },
      });

      expect(missingFailure.issues).toStrictEqual([
        expectedIssue(
          "GeneratedSchemaMismatch",
          'Subject "orders-value" version 1 does not contain generated message "viewserver.runtime.test.OrderValue".',
          { version: 1, schemaId: 41 },
        ),
      ]);
      expect(anchorFailure.issues).toStrictEqual([
        expectedIssue(
          "GeneratedSchemaMismatch",
          'Subject "orders-value" version 1 is not decodable by generated message "viewserver.runtime.test.OrderValue": Buf WIRE rule FIELD_WIRE_COMPATIBLE_TYPE at viewserver.runtime.test.OrderValue.customer_id: Field type is not directionally wire-compatible.',
          { version: 1, schemaId: 41 },
        ),
      ]);
      expect(noMutualAnchorFailure.issues).toStrictEqual([
        expectedIssue(
          "GeneratedSchemaMismatch",
          'Generated message "viewserver.runtime.test.OrderValue" is not mutually Buf WIRE-compatible with any active version of subject "orders-value".',
        ),
      ]);
      expect(reverseNoMutualAnchorFailure.issues).toStrictEqual([
        expectedIssue(
          "GeneratedSchemaMismatch",
          'Generated message "viewserver.runtime.test.OrderValue" is not mutually Buf WIRE-compatible with any active version of subject "orders-value".',
        ),
      ]);
    }),
  );

  it.effect("rejects every active schema that generated code cannot read recursively", () =>
    Effect.gen(function* () {
      const generatedChild = create(FileDescriptorProtoSchema, {
        name: "child.proto",
        package: "example",
        syntax: "proto2",
        messageType: [
          {
            name: "Child",
            field: [
              {
                name: "value",
                number: 1,
                label: FieldDescriptorProto_Label.OPTIONAL,
                type: FieldDescriptorProto_Type.STRING,
              },
              {
                name: "must_exist",
                number: 2,
                label: FieldDescriptorProto_Label.REQUIRED,
                type: FieldDescriptorProto_Type.STRING,
              },
            ],
          },
        ],
      });
      const generatedRoot = create(FileDescriptorProtoSchema, {
        name: "root.proto",
        package: "example",
        syntax: "proto3",
        dependency: [generatedChild.name],
        messageType: [
          {
            name: "Root",
            field: [
              {
                name: "child",
                number: 1,
                label: FieldDescriptorProto_Label.OPTIONAL,
                type: FieldDescriptorProto_Type.MESSAGE,
                typeName: ".example.Child",
              },
            ],
          },
        ],
      });
      const generated = generatedMessage(
        [generatedChild, generatedRoot],
        generatedRoot.name,
        "example.Root",
      );
      const incompatibleChild = clone(FileDescriptorProtoSchema, generatedChild);
      const nestedValue = incompatibleChild.messageType[0]?.field[0];
      if (nestedValue === undefined) {
        throw new Error("nested generated field missing");
      }
      nestedValue.type = FieldDescriptorProto_Type.BYTES;
      const rootRecord = referencedSchemaVersion("orders-value", 1, 41, generatedRoot, [
        { name: generatedChild.name, subject: "child", version: 1 },
      ]);

      const failure = yield* resolveFailure(
        {
          compatibility: {
            "orders-value": "FULL_TRANSITIVE",
            child: "FULL_TRANSITIVE",
          },
          active: { "orders-value": [1], child: [1] },
          all: { "orders-value": [1], child: [1] },
          schemas: {
            "orders-value:1": rootRecord,
            "child:1": referencedSchemaVersion("child", 1, 51, incompatibleChild),
          },
        },
        [{ ...declaration(), descriptor: generated }],
      );
      const missingRequiredChild = clone(FileDescriptorProtoSchema, generatedChild);
      missingRequiredChild.messageType[0]?.field.splice(1, 1);
      const requiredFailure = yield* resolveFailure(
        {
          compatibility: {
            "orders-value": "FULL_TRANSITIVE",
            child: "FULL_TRANSITIVE",
          },
          active: { "orders-value": [1], child: [1] },
          all: { "orders-value": [1], child: [1] },
          schemas: {
            "orders-value:1": rootRecord,
            "child:1": referencedSchemaVersion("child", 1, 51, missingRequiredChild),
          },
        },
        [{ ...declaration(), descriptor: generated }],
      );

      expect([failure.issues, requiredFailure.issues]).toStrictEqual([
        [
          expectedIssue(
            "GeneratedSchemaMismatch",
            'Subject "orders-value" version 1 is not decodable by generated message "example.Root": Buf WIRE rule FIELD_WIRE_COMPATIBLE_TYPE at example.Child.value: Field type is not directionally wire-compatible.',
            { version: 1, schemaId: 41 },
          ),
        ],
        [
          expectedIssue(
            "GeneratedSchemaMismatch",
            'Subject "orders-value" version 1 is not decodable by generated message "example.Root": Buf WIRE rule MESSAGE_SAME_REQUIRED_FIELDS at example.Child.must_exist: Required reader field number 2 is absent from the writer.',
            { version: 1, schemaId: 41 },
          ),
        ],
      ]);
    }),
  );

  it.effect("rejects a directional active ID even when an older version is a mutual anchor", () =>
    Effect.gen(function* () {
      const anchor = create(FileDescriptorProtoSchema, {
        name: "directional.proto",
        package: "example",
        syntax: "proto3",
        messageType: [
          {
            name: "Directional",
            field: [
              {
                name: "value",
                number: 1,
                label: FieldDescriptorProto_Label.OPTIONAL,
                type: FieldDescriptorProto_Type.STRING,
              },
            ],
          },
        ],
      });
      const generated = generatedMessage([anchor], anchor.name, "example.Directional");
      const newer = clone(FileDescriptorProtoSchema, anchor);
      const newerValue = newer.messageType[0]?.field[0];
      if (newerValue === undefined) {
        throw new Error("directional field missing");
      }
      newerValue.type = FieldDescriptorProto_Type.BYTES;

      const failure = yield* resolveFailure(
        {
          compatibility: { "orders-value": "FULL_TRANSITIVE" },
          active: { "orders-value": [1, 2] },
          all: { "orders-value": [1, 2] },
          schemas: {
            "orders-value:1": schemaVersion(1, 41, anchor),
            "orders-value:2": schemaVersion(2, 42, newer),
          },
        },
        [{ ...declaration(), descriptor: generated }],
      );

      expect(failure.issues).toStrictEqual([
        expectedIssue(
          "GeneratedSchemaMismatch",
          'Subject "orders-value" version 2 is not decodable by generated message "example.Directional": Buf WIRE rule FIELD_WIRE_COMPATIBLE_TYPE at example.Directional.value: Field type is not directionally wire-compatible.',
          { version: 2, schemaId: 42 },
        ),
      ]);
    }),
  );

  it.effect("deduplicates repeated schema IDs and accumulates declaration failures", () =>
    Effect.gen(function* () {
      const contracts = yield* resolveKafkaSchemaRegistryContracts(
        [declaration()],
        reader({
          compatibility: { "orders-value": "FULL_TRANSITIVE" },
          active: { "orders-value": [1, 2] },
          all: { "orders-value": [1, 2] },
          schemas: {
            "orders-value:1": schemaVersion(1, 41),
            "orders-value:2": schemaVersion(2, 41, currentDescriptor()),
          },
        }),
      );
      expect(contracts[0]?.schemaIds.get(41)).toStrictEqual([[0]]);

      const otherDeclaration = declaration("other-value");
      const accumulated = yield* resolveFailure(
        {
          compatibility: {
            "orders-value": "BACKWARD",
            "other-value": "FORWARD",
          },
        },
        [declaration(), otherDeclaration],
      );
      expect(accumulated.issues).toStrictEqual([
        expectedIssue(
          "CompatibilityPolicyMismatch",
          'Subject "orders-value" requires effective FULL_TRANSITIVE compatibility; observed "BACKWARD".',
        ),
        expectedIssue(
          "CompatibilityPolicyMismatch",
          'Subject "other-value" requires effective FULL_TRANSITIVE compatibility; observed "FORWARD".',
          { subject: "other-value" },
        ),
      ]);
    }),
  );

  it.effect("shares identical Registry reads across declarations in one inspection", () =>
    Effect.gen(function* () {
      const calls = { compatibility: 0, versions: 0, schema: 0 };
      const base = reader({
        compatibility: { "orders-value": "FULL_TRANSITIVE" },
        active: { "orders-value": [1] },
        all: { "orders-value": [1] },
        schemas: { "orders-value:1": schemaVersion(1, 41) },
      });
      const counted: KafkaSchemaRegistryReader = {
        effectiveCompatibility: (subject) => {
          calls.compatibility += 1;
          return base.effectiveCompatibility(subject);
        },
        versions: (subject, includeDeleted) => {
          calls.versions += 1;
          return base.versions(subject, includeDeleted);
        },
        schema: (subject, version) => {
          calls.schema += 1;
          return base.schema(subject, version);
        },
      };
      const contracts = yield* resolveKafkaSchemaRegistryContracts(
        [declaration(), { ...declaration(), viewServerTopic: "orders-copy" }],
        counted,
      );

      expect(
        contracts.map(({ viewServerTopic, subject, schemaIds }) => ({
          viewServerTopic,
          subject,
          schemaIds: [...schemaIds],
        })),
      ).toStrictEqual([
        {
          viewServerTopic: "orders",
          subject: "orders-value",
          schemaIds: [[41, [[0]]]],
        },
        {
          viewServerTopic: "orders-copy",
          subject: "orders-value",
          schemaIds: [[41, [[0]]]],
        },
      ]);
      expect(calls).toStrictEqual({ compatibility: 1, versions: 2, schema: 1 });
    }),
  );

  it.effect("shares identical Registry failures across declarations", () =>
    Effect.gen(function* () {
      let compatibilityCalls = 0;
      const unavailable: KafkaSchemaRegistryReader = {
        effectiveCompatibility: () => {
          compatibilityCalls += 1;
          return Effect.fail({ message: "compatibility unavailable" });
        },
        versions: () => Effect.die(new Error("versions must not be read")),
        schema: () => Effect.die(new Error("schema must not be read")),
      };
      const failure = yield* resolveKafkaSchemaRegistryContracts(
        [declaration(), { ...declaration(), viewServerTopic: "orders-copy" }],
        unavailable,
      ).pipe(Effect.flip);

      expect(compatibilityCalls).toBe(1);
      expect(failure.issues).toStrictEqual([
        expectedIssue(
          "RegistryUnavailable",
          'Schema Registry request for subject "orders-value" failed: compatibility unavailable',
        ),
        expectedIssue(
          "RegistryUnavailable",
          'Schema Registry request for subject "orders-value" failed: compatibility unavailable',
          { viewServerTopic: "orders-copy" },
        ),
      ]);
    }),
  );

  it.effect("returns malformed frame mismatches without a schema ID", () =>
    Effect.gen(function* () {
      const contracts = yield* resolveKafkaSchemaRegistryContracts(
        [declaration()],
        reader({
          compatibility: { "orders-value": "FULL_TRANSITIVE" },
          active: { "orders-value": [1] },
          all: { "orders-value": [1] },
          schemas: { "orders-value:1": schemaVersion(1, 41) },
        }),
      );
      const contract = contracts[0];
      if (contract === undefined) {
        throw new Error("resolved contract missing");
      }

      expect(validateKafkaSchemaRegistryFrame(contract, Uint8Array.from([]))).toStrictEqual({
        _tag: "Mismatch",
        reason: "frame",
        schemaId: null,
        message:
          "Confluent Schema Registry Protobuf frame is shorter than its six-byte minimum prefix.",
      });
    }),
  );
});
