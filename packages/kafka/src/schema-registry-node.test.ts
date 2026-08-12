import { toBinary } from "@bufbuild/protobuf";
import { FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { afterEach, beforeEach, describe, expect, it } from "@effect/vitest";
// Vitest's mock transform requires this API to come directly from "vitest";
// the @effect/vitest re-export cannot be hoisted.
import { vi } from "vitest";
import { Duration, Effect, Fiber } from "effect";
import { TestClock } from "effect/testing";
import {
  kafkaSchemaRegistryHttpDefaults,
  makeKafkaSchemaRegistryReader,
  type KafkaSchemaRegistryHttpOptions,
} from "./schema-registry-node";
import { OrderValueSchema } from "./test-fixtures/orders_pb";

const platformatic = vi.hoisted(() => {
  type Mode = "normal" | "throw" | "invalid" | "missing-close" | "reject-close";
  const calls: Array<object> = [];
  let closes = 0;
  let mode: Mode = "normal";
  const createUndiciAgent = (options: object) => {
    calls.push(options);
    if (mode === "throw") {
      throw new Error("dispatcher creation failed");
    }
    if (mode === "invalid") {
      return "invalid";
    }
    if (mode === "missing-close") {
      return {};
    }
    return {
      close: () => {
        closes += 1;
        return mode === "reject-close"
          ? Promise.reject(new Error("dispatcher close failed"))
          : Promise.resolve();
      },
    };
  };
  const reset = () => {
    calls.splice(0);
    closes = 0;
    mode = "normal";
  };
  return {
    calls,
    createUndiciAgent,
    get closes() {
      return closes;
    },
    reset,
    setMode: (next: Mode) => {
      mode = next;
    },
  };
});

vi.mock("@platformatic/kafka", () => ({
  createUndiciAgent: platformatic.createUndiciAgent,
}));

type CapturedRequest = {
  readonly url: string;
  readonly init: RequestInit | undefined;
};

const serializedDescriptor = Buffer.from(
  toBinary(FileDescriptorProtoSchema, OrderValueSchema.file.proto),
).toString("base64");

const options = (
  overrides: Partial<KafkaSchemaRegistryHttpOptions> = {},
): KafkaSchemaRegistryHttpOptions => ({
  url: "https://registry.example.com/",
  headers: {},
  timeout: 5_000,
  retries: 0,
  retryDelay: 0,
  ...overrides,
});

const jsonResponse = (value: unknown, status = 200): Response =>
  new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" },
  });

const validSchemaResponse = (subject: string, version: number, id: number) => ({
  subject,
  version,
  id,
  schemaType: "PROTOBUF",
  references: [],
  schema: serializedDescriptor,
});

beforeEach(() => {
  platformatic.reset();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("Kafka Schema Registry HTTP reader", () => {
  it.effect("preserves base paths, applies Basic auth and TLS, and closes its dispatcher", () => {
    const requests: Array<CapturedRequest> = [];
    vi.stubGlobal("fetch", (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
      requests.push({ url, init });
      if (init?.method !== "GET" || init.body !== undefined) {
        return Promise.reject(new Error("Schema Registry reader attempted a mutation."));
      }
      if (url.includes("/config/")) {
        return Promise.resolve(jsonResponse({ compatibilityLevel: "FULL_TRANSITIVE" }));
      }
      if (url.includes("format=serialized")) {
        return Promise.resolve(jsonResponse(validSchemaResponse("orders/eu-value", 1, 41)));
      }
      return Promise.resolve(jsonResponse([1]));
    });

    return Effect.scoped(
      Effect.gen(function* () {
        const reader = yield* makeKafkaSchemaRegistryReader(
          options({
            url: "https://registry.example.com/schema-registry/",
            auth: { _tag: "Basic", username: "orders", password: "secret" },
            headers: { "x-tenant": "commerce" },
            tls: { servername: "registry.internal" },
          }),
        );
        expect(yield* reader.effectiveCompatibility("orders/eu-value")).toBe("FULL_TRANSITIVE");
        expect(yield* reader.versions("orders/eu-value", false)).toStrictEqual([1]);
        expect(yield* reader.versions("orders/eu-value", true)).toStrictEqual([1]);
        const schema = yield* reader.schema("orders/eu-value", 1);
        expect({
          descriptor: schema.descriptor.name,
          requests: requests.map((request) => ({
            url: request.url,
            headers: request.init?.headers,
            hasDispatcher: Reflect.get(request.init ?? {}, "dispatcher") !== undefined,
            method: request.init?.method,
            body: request.init?.body ?? null,
          })),
          agentCalls: platformatic.calls,
        }).toStrictEqual({
          descriptor: "viewserver/runtime/test.proto",
          requests: [
            {
              url: "https://registry.example.com/schema-registry/config/orders%2Feu-value?defaultToGlobal=true",
              headers: {
                accept: "application/vnd.schemaregistry.v1+json",
                authorization: "Basic b3JkZXJzOnNlY3JldA==",
                "x-tenant": "commerce",
              },
              hasDispatcher: true,
              method: "GET",
              body: null,
            },
            {
              url: "https://registry.example.com/schema-registry/subjects/orders%2Feu-value/versions",
              headers: {
                accept: "application/vnd.schemaregistry.v1+json",
                authorization: "Basic b3JkZXJzOnNlY3JldA==",
                "x-tenant": "commerce",
              },
              hasDispatcher: true,
              method: "GET",
              body: null,
            },
            {
              url: "https://registry.example.com/schema-registry/subjects/orders%2Feu-value/versions?deleted=true",
              headers: {
                accept: "application/vnd.schemaregistry.v1+json",
                authorization: "Basic b3JkZXJzOnNlY3JldA==",
                "x-tenant": "commerce",
              },
              hasDispatcher: true,
              method: "GET",
              body: null,
            },
            {
              url: "https://registry.example.com/schema-registry/subjects/orders%2Feu-value/versions/1?deleted=true&format=serialized",
              headers: {
                accept: "application/vnd.schemaregistry.v1+json",
                authorization: "Basic b3JkZXJzOnNlY3JldA==",
                "x-tenant": "commerce",
              },
              hasDispatcher: true,
              method: "GET",
              body: null,
            },
          ],
          agentCalls: [{ connect: { servername: "registry.internal" } }],
        });
      }),
    ).pipe(
      Effect.tap(() =>
        Effect.sync(() => {
          expect(platformatic.closes).toBe(1);
        }),
      ),
    );
  });

  it.effect("retries network and retryable HTTP failures but not terminal statuses", () =>
    Effect.gen(function* () {
      let attempts = 0;
      vi.stubGlobal("fetch", () => {
        attempts += 1;
        if (attempts === 1) {
          return Promise.reject(new Error("network unavailable"));
        }
        return Promise.resolve(
          attempts === 2
            ? jsonResponse({ message: "overloaded" }, 503)
            : jsonResponse({ compatibilityLevel: "FULL_TRANSITIVE" }),
        );
      });
      const compatibility = yield* Effect.scoped(
        makeKafkaSchemaRegistryReader(
          options({
            auth: { _tag: "Bearer", token: "registry-token" },
            retries: 2,
          }),
        ).pipe(Effect.flatMap((reader) => reader.effectiveCompatibility("orders-value"))),
      );
      expect({ attempts, compatibility }).toStrictEqual({
        attempts: 3,
        compatibility: "FULL_TRANSITIVE",
      });

      attempts = 0;
      vi.stubGlobal("fetch", () => {
        attempts += 1;
        return Promise.resolve(jsonResponse({ message: "missing" }, 404));
      });
      const failure = yield* Effect.scoped(
        makeKafkaSchemaRegistryReader(options({ retries: 3 })).pipe(
          Effect.flatMap((reader) => reader.effectiveCompatibility("orders-value")),
        ),
      ).pipe(Effect.flip);
      expect({ attempts, failure }).toStrictEqual({
        attempts: 1,
        failure: { message: "Schema Registry request failed with HTTP 404." },
      });
    }),
  );

  it.effect("turns request timeouts and malformed response bodies into typed failures", () =>
    Effect.gen(function* () {
      let stalledBodyCancellations = 0;
      vi.stubGlobal("fetch", (_input: string | URL | Request, init?: RequestInit) =>
        Promise.resolve({
          ok: true,
          status: 200,
          text: () =>
            new Promise<string>((_resolve, reject) => {
              init?.signal?.addEventListener(
                "abort",
                () => {
                  stalledBodyCancellations += 1;
                  reject(new Error("body cancelled"));
                },
                { once: true },
              );
            }),
        }),
      );
      const timeoutFiber = yield* Effect.scoped(
        makeKafkaSchemaRegistryReader(options({ timeout: 1 })).pipe(
          Effect.flatMap((reader) => reader.effectiveCompatibility("orders-value")),
        ),
      ).pipe(Effect.flip, Effect.forkChild({ startImmediately: true }));
      yield* TestClock.adjust(Duration.millis(1));
      expect(yield* Fiber.join(timeoutFiber)).toStrictEqual({
        message: "Schema Registry request timed out.",
      });
      expect(stalledBodyCancellations).toBe(1);

      let retryAttempts = 0;
      let retriedBodyCancellations = 0;
      vi.stubGlobal("fetch", (_input: string | URL | Request, init?: RequestInit) => {
        retryAttempts += 1;
        if (retryAttempts === 2) {
          return Promise.resolve(jsonResponse({ compatibilityLevel: "FULL_TRANSITIVE" }));
        }
        return Promise.resolve({
          ok: false,
          status: 503,
          text: () =>
            new Promise<string>((_resolve, reject) => {
              init?.signal?.addEventListener(
                "abort",
                () => {
                  retriedBodyCancellations += 1;
                  reject(new Error("retry body cancelled"));
                },
                { once: true },
              );
            }),
        });
      });
      const retriedFiber = yield* Effect.scoped(
        makeKafkaSchemaRegistryReader(options({ timeout: 1, retries: 1 })).pipe(
          Effect.flatMap((reader) => reader.effectiveCompatibility("orders-value")),
        ),
      ).pipe(Effect.forkChild({ startImmediately: true }));
      yield* TestClock.adjust(Duration.millis(1));
      expect({
        compatibility: yield* Fiber.join(retriedFiber),
        retriedBodyCancellations,
        retryAttempts,
      }).toStrictEqual({
        compatibility: "FULL_TRANSITIVE",
        retriedBodyCancellations: 1,
        retryAttempts: 2,
      });

      let bodyAttempts = 0;
      vi.stubGlobal("fetch", () => {
        bodyAttempts += 1;
        return bodyAttempts === 2
          ? Promise.resolve(jsonResponse({ compatibilityLevel: "FULL_TRANSITIVE" }))
          : Promise.resolve({
              ok: true,
              status: 200,
              text: () => Promise.reject(new Error("body failed")),
            });
      });
      const recoveredBody = yield* Effect.scoped(
        makeKafkaSchemaRegistryReader(options({ retries: 1 })).pipe(
          Effect.flatMap((reader) => reader.effectiveCompatibility("orders-value")),
        ),
      );
      expect({ bodyAttempts, recoveredBody }).toStrictEqual({
        bodyAttempts: 2,
        recoveredBody: "FULL_TRANSITIVE",
      });

      vi.stubGlobal("fetch", () =>
        Promise.resolve({
          ok: true,
          status: 200,
          text: () => Promise.reject(new Error("body failed")),
        }),
      );
      const bodyFailure = yield* Effect.scoped(
        makeKafkaSchemaRegistryReader(options()).pipe(
          Effect.flatMap((reader) => reader.effectiveCompatibility("orders-value")),
        ),
      ).pipe(Effect.flip);
      expect(bodyFailure).toStrictEqual({
        message: "Schema Registry response body could not be read.",
      });

      vi.stubGlobal("fetch", () => Promise.resolve(new Response("{")));
      const jsonFailure = yield* Effect.scoped(
        makeKafkaSchemaRegistryReader(options()).pipe(
          Effect.flatMap((reader) => reader.effectiveCompatibility("orders-value")),
        ),
      ).pipe(Effect.flip);
      expect(jsonFailure).toStrictEqual({
        message: "Schema Registry returned malformed compatibility JSON.",
      });
    }),
  );

  it.effect(
    "rejects malformed compatibility, version, schema, reference, and descriptor JSON",
    () =>
      Effect.gen(function* () {
        const readFailure = (body: unknown, operation: "compatibility" | "versions" | "schema") => {
          vi.stubGlobal("fetch", () => Promise.resolve(jsonResponse(body)));
          return Effect.scoped(
            makeKafkaSchemaRegistryReader(options()).pipe(
              Effect.flatMap((reader) =>
                operation === "compatibility"
                  ? reader.effectiveCompatibility("orders-value").pipe(Effect.asVoid)
                  : operation === "versions"
                    ? reader.versions("orders-value", false).pipe(Effect.asVoid)
                    : reader.schema("orders-value", 1).pipe(Effect.asVoid),
              ),
            ),
          ).pipe(Effect.flip);
        };

        expect(yield* readFailure({}, "compatibility")).toStrictEqual({
          message: "Schema Registry returned malformed compatibility JSON.",
        });
        expect(yield* readFailure([], "compatibility")).toStrictEqual({
          message: "Schema Registry returned malformed compatibility JSON.",
        });
        expect(yield* readFailure([0], "versions")).toStrictEqual({
          message: "Schema Registry returned malformed versions JSON.",
        });
        expect(yield* readFailure([1.5], "versions")).toStrictEqual({
          message: "Schema Registry returned malformed versions JSON.",
        });
        expect(yield* readFailure({}, "versions")).toStrictEqual({
          message: "Schema Registry returned malformed versions JSON.",
        });
        expect(yield* readFailure([], "schema")).toStrictEqual({
          message: "Schema Registry returned malformed schema JSON.",
        });
        expect(yield* readFailure({}, "schema")).toStrictEqual({
          message: "Schema Registry returned malformed schema JSON.",
        });
        const validSchema = validSchemaResponse("orders-value", 1, 41);
        const malformedSchemaMetadata = [
          { ...validSchema, subject: "wrong" },
          { ...validSchema, version: 2 },
          { ...validSchema, id: "41" },
          { ...validSchema, id: 1.5 },
          { ...validSchema, id: 0 },
          { ...validSchema, schemaType: 1 },
          { ...validSchema, schema: 1 },
          { ...validSchema, references: {} },
        ];
        const metadataFailures = yield* Effect.forEach(malformedSchemaMetadata, (body) =>
          readFailure(body, "schema"),
        );
        expect(metadataFailures).toStrictEqual([
          { message: "Schema Registry returned malformed schema JSON." },
          { message: "Schema Registry returned malformed schema JSON." },
          { message: "Schema Registry returned malformed schema JSON." },
          { message: "Schema Registry returned malformed schema JSON." },
          { message: "Schema Registry returned malformed schema JSON." },
          { message: "Schema Registry returned malformed schema JSON." },
          { message: "Schema Registry returned malformed schema JSON." },
          { message: "Schema Registry returned malformed schema JSON." },
        ]);
        expect(
          yield* readFailure(
            {
              ...validSchemaResponse("orders-value", 1, 41),
              references: [{ name: "child.proto", subject: "", version: 1 }],
            },
            "schema",
          ),
        ).toStrictEqual({ message: "Schema Registry returned malformed schema JSON." });
        const malformedReferences = [
          null,
          { name: 1, subject: "shared", version: 1 },
          { name: "", subject: "shared", version: 1 },
          { name: "shared.proto", subject: 1, version: 1 },
          { name: "shared.proto", subject: "", version: 1 },
          { name: "shared.proto", subject: "shared", version: "1" },
          { name: "shared.proto", subject: "shared", version: 1.5 },
          { name: "shared.proto", subject: "shared", version: 0 },
        ];
        const referenceFailures = yield* Effect.forEach(malformedReferences, (reference) =>
          readFailure(
            {
              ...validSchemaResponse("orders-value", 1, 41),
              references: [reference],
            },
            "schema",
          ),
        );
        expect(referenceFailures).toStrictEqual([
          { message: "Schema Registry returned malformed schema JSON." },
          { message: "Schema Registry returned malformed schema JSON." },
          { message: "Schema Registry returned malformed schema JSON." },
          { message: "Schema Registry returned malformed schema JSON." },
          { message: "Schema Registry returned malformed schema JSON." },
          { message: "Schema Registry returned malformed schema JSON." },
          { message: "Schema Registry returned malformed schema JSON." },
          { message: "Schema Registry returned malformed schema JSON." },
        ]);
        expect(
          yield* readFailure(
            { ...validSchemaResponse("orders-value", 1, 41), schema: "not-base64" },
            "schema",
          ),
        ).toStrictEqual({
          message: "Schema Registry returned malformed serialized protobuf.",
        });
        expect(
          yield* readFailure(
            { ...validSchemaResponse("orders-value", 1, 41), schema: "" },
            "schema",
          ),
        ).toStrictEqual({
          message: "Schema Registry returned malformed serialized protobuf.",
        });
        expect(
          yield* readFailure(
            { ...validSchemaResponse("orders-value", 1, 41), schema: "@@@@" },
            "schema",
          ),
        ).toStrictEqual({
          message: "Schema Registry returned malformed serialized protobuf.",
        });
        expect(
          yield* readFailure(
            { ...validSchemaResponse("orders-value", 1, 41), schema: "////" },
            "schema",
          ),
        ).toStrictEqual({
          message: "Schema Registry returned malformed serialized protobuf.",
        });

        vi.stubGlobal("fetch", () =>
          Promise.resolve(
            jsonResponse({
              ...validSchemaResponse("orders-value", 1, 41),
              references: [{ name: "shared.proto", subject: "shared", version: 1 }],
            }),
          ),
        );
        const referenced = yield* Effect.scoped(
          makeKafkaSchemaRegistryReader(options()).pipe(
            Effect.flatMap((registryReader) => registryReader.schema("orders-value", 1)),
          ),
        );
        expect(referenced.references).toStrictEqual([
          { name: "shared.proto", subject: "shared", version: 1 },
        ]);

        vi.stubGlobal("fetch", () =>
          Promise.resolve(
            jsonResponse({
              subject: "orders-value",
              version: 1,
              id: 41,
              schemaType: "PROTOBUF",
              schema: serializedDescriptor,
            }),
          ),
        );
        const withoutReferences = yield* Effect.scoped(
          makeKafkaSchemaRegistryReader(options()).pipe(
            Effect.flatMap((registryReader) => registryReader.schema("orders-value", 1)),
          ),
        );
        expect(withoutReferences.references).toStrictEqual([]);
      }),
  );

  it.effect("contains dispatcher creation and cleanup failures", () =>
    Effect.gen(function* () {
      platformatic.setMode("throw");
      expect(
        yield* Effect.scoped(
          makeKafkaSchemaRegistryReader(options({ tls: { servername: "registry" } })),
        ).pipe(Effect.flip),
      ).toStrictEqual({ message: "Schema Registry TLS dispatcher creation failed." });

      platformatic.setMode("invalid");
      expect(
        yield* Effect.scoped(
          makeKafkaSchemaRegistryReader(options({ tls: { servername: "registry" } })),
        ).pipe(Effect.flip),
      ).toStrictEqual({ message: "Schema Registry TLS dispatcher creation failed." });

      platformatic.setMode("missing-close");
      yield* Effect.scoped(
        makeKafkaSchemaRegistryReader(options({ tls: { servername: "registry" } })),
      );

      platformatic.setMode("reject-close");
      yield* Effect.scoped(
        makeKafkaSchemaRegistryReader(options({ tls: { servername: "registry" } })),
      );
      expect(platformatic.closes).toBe(1);
      expect(kafkaSchemaRegistryHttpDefaults).toStrictEqual({
        timeout: 5_000,
        retries: 3,
        retryDelay: 1_000,
      });
    }),
  );
});
