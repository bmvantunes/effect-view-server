import { execFileSync } from "node:child_process";
import {
  existsSync,
  mkdtempSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "@effect/vitest";
import { inspectTypeScriptModule } from "./typescript-module-inspection";

const repositoryRoot = fileURLToPath(new URL("../", import.meta.url));
const commandTimeoutMilliseconds = 60_000;
const setupTimeoutMilliseconds = 360_000;
const scenarioTimeoutMilliseconds = 200_000;

const writeJson = (path: string, value: unknown): void => {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
};

const installStrictly = (directory: string): void => {
  writeFileSync(
    join(directory, "pnpm-workspace.yaml"),
    [
      "packages:",
      "  - .",
      "",
      "autoInstallPeers: false",
      "strictPeerDependencies: true",
      "",
      "onlyBuiltDependencies:",
      "  - msgpackr-extract",
      "",
      "allowBuilds:",
      "  msgpackr-extract: true",
      "",
    ].join("\n"),
  );
  execFileSync("vp", ["install"], {
    cwd: directory,
    killSignal: "SIGTERM",
    stdio: "inherit",
    timeout: commandTimeoutMilliseconds,
  });
};

const run = (command: string, args: ReadonlyArray<string>, directory: string): void => {
  execFileSync(command, args, {
    cwd: directory,
    killSignal: "SIGTERM",
    stdio: "inherit",
    timeout: commandTimeoutMilliseconds,
  });
};

const pack = (directory: string, destination: string): string => {
  const output = execFileSync(
    "npm",
    [
      "pack",
      directory,
      "--json",
      "--pack-destination",
      destination,
      "--cache",
      join(destination, ".npm-cache"),
    ],
    {
      cwd: repositoryRoot,
      encoding: "utf8",
      killSignal: "SIGTERM",
      timeout: commandTimeoutMilliseconds,
    },
  );
  const result: unknown = JSON.parse(output);
  if (
    !Array.isArray(result) ||
    result.length !== 1 ||
    typeof result[0] !== "object" ||
    result[0] === null ||
    !("filename" in result[0]) ||
    typeof result[0].filename !== "string"
  ) {
    throw new Error("npm pack returned an unexpected result.");
  }
  return join(destination, result[0].filename);
};

type DeclarationClosureFile = {
  readonly path: string;
  readonly source: string;
};

const declarationPathForSpecifier = (sourcePath: string, specifier: string): string => {
  const resolved = resolve(dirname(sourcePath), specifier);
  const candidates = [
    ...(resolved.endsWith(".mjs") ? [resolved.replace(/\.mjs$/, ".d.mts")] : []),
    ...(resolved.endsWith(".js") ? [resolved.replace(/\.js$/, ".d.ts")] : []),
    `${resolved}.d.mts`,
    `${resolved}.d.ts`,
    resolved,
  ];
  const path = candidates.find((candidate) => existsSync(candidate));
  if (path === undefined) {
    throw new Error(`Cannot resolve declaration ${specifier} from ${sourcePath}.`);
  }
  return path;
};

const collectDeclarationClosure = (entryPath: string): ReadonlyArray<DeclarationClosureFile> => {
  const files = new Map<string, DeclarationClosureFile>();
  const pending = [entryPath];
  while (pending.length > 0) {
    const path = pending.pop();
    if (path === undefined || files.has(path)) {
      continue;
    }
    const source = readFileSync(path, "utf8");
    files.set(path, { path, source });
    const inspection = inspectTypeScriptModule({ fileName: path, source });
    for (const specifier of inspection.moduleSpecifiers) {
      if (specifier.startsWith(".")) {
        pending.push(declarationPathForSpecifier(path, specifier));
      }
    }
  }
  return Array.from(files.values());
};

const selectViewServerTarball = (options: {
  readonly requestedTarball: string | undefined;
  readonly artifactsReady: boolean;
  readonly buildLocal: () => void;
  readonly packLocal: () => string;
}): string => {
  if (options.requestedTarball !== undefined) {
    return resolve(options.requestedTarball);
  }
  if (!options.artifactsReady) {
    options.buildLocal();
  }
  return options.packLocal();
};

describe("downstream viewport declaration bundle", () => {
  const temporaryRoot = mkdtempSync(join(tmpdir(), "view-server-viewport-bundle-"));
  const downstreamDirectory = join(temporaryRoot, "downstream");
  let viewServerTarball = "";
  let downstreamTarball = "";

  beforeAll(() => {
    viewServerTarball = selectViewServerTarball({
      requestedTarball: process.env.VIEW_SERVER_VIEWPORT_CONSUMER_TARBALL,
      artifactsReady: process.env.VIEW_SERVER_REPOSITORY_TEST_ARTIFACTS_READY === "1",
      buildLocal: () =>
        run("vp", ["run", "effect-view-server#build"], repositoryRoot),
      packLocal: () =>
        pack(join(repositoryRoot, "packages", "effect-view-server"), temporaryRoot),
    });

    mkdirSync(join(downstreamDirectory, "src"), { recursive: true });
    writeJson(join(downstreamDirectory, "package.json"), {
      name: "downstream-viewport-adapter",
      version: "1.0.0",
      type: "module",
      packageManager: "pnpm@11.9.0",
      files: ["dist"],
      exports: {
        ".": {
          types: "./dist/index.d.mts",
          import: "./dist/index.mjs",
        },
        "./effect": {
          types: "./dist/effect.d.mts",
          import: "./dist/effect.mjs",
        },
      },
      dependencies: {},
      peerDependencies: {
        effect: "4.0.0-rc.111",
      },
      peerDependenciesMeta: {
        effect: { optional: true },
      },
      devDependencies: {
        "@emnapi/core": "1.7.1",
        "@emnapi/runtime": "1.7.1",
        "@effect/atom-react": "4.0.0-rc.111",
        "@types/node": "26.2.0",
        "@types/react": "19.2.18",
        "@types/react-dom": "19.2.4",
        effect: "4.0.0-rc.111",
        "effect-view-server": `file:${viewServerTarball}`,
        react: "19.2.8",
        "react-dom": "19.2.8",
        redis: "6.2.1",
        typescript: "7.0.2",
        vite: "8.0.0",
        "vite-plus": "0.2.8",
      },
    });
    writeFileSync(
      join(downstreamDirectory, "vite.config.ts"),
      [
        'import { defineConfig } from "vite-plus";',
        "",
        "export default defineConfig({",
        "  pack: {",
        '    entry: { effect: "src/effect.ts", index: "src/index.ts" },',
        "    dts: { tsgo: true },",
        "  },",
        "});",
        "",
      ].join("\n"),
    );
    writeJson(join(downstreamDirectory, "tsconfig.json"), {
      compilerOptions: {
        declaration: true,
        exactOptionalPropertyTypes: true,
        module: "preserve",
        moduleResolution: "bundler",
        strict: true,
      },
      include: ["src"],
    });
    writeFileSync(
      join(downstreamDirectory, "src", "index.ts"),
      [
        'import type { LiveQueryViewportBaseRow, LiveQueryViewportCompleteRawSelect } from "effect-view-server/react/viewport-base-row";',
        "",
        "export type BundledViewportBaseRow<Viewport> = LiveQueryViewportBaseRow<Viewport>;",
        "export type BundledViewportCompleteRawSelect<Viewport> = LiveQueryViewportCompleteRawSelect<Viewport>;",
        "export const clientReady = true;",
        "",
      ].join("\n"),
    );
    writeFileSync(
      join(downstreamDirectory, "src", "effect.ts"),
      [
        'import type { Effect } from "effect";',
        "",
        "export type OptionalEffect = Effect<void>;",
        "export const effectReady = true;",
        "",
      ].join("\n"),
    );
    installStrictly(downstreamDirectory);
    run("vp", ["pack"], downstreamDirectory);
    downstreamTarball = pack(downstreamDirectory, temporaryRoot);
  }, setupTimeoutMilliseconds);

  afterAll(() => rmSync(temporaryRoot, { force: true, recursive: true }));

  it("selects local and configured View Server tarballs", () => {
    let buildCount = 0;
    let packCount = 0;
    const localTarball = selectViewServerTarball({
      requestedTarball: undefined,
      artifactsReady: false,
      buildLocal: () => {
        buildCount += 1;
      },
      packLocal: () => {
        packCount += 1;
        return "/local/effect-view-server.tgz";
      },
    });
    expect(localTarball).toBe("/local/effect-view-server.tgz");
    expect(buildCount).toBe(1);
    expect(packCount).toBe(1);

    const readyTarball = selectViewServerTarball({
      requestedTarball: undefined,
      artifactsReady: true,
      buildLocal: () => {
        buildCount += 1;
      },
      packLocal: () => {
        packCount += 1;
        return "/ready/effect-view-server.tgz";
      },
    });
    expect(readyTarball).toBe("/ready/effect-view-server.tgz");
    expect(buildCount).toBe(1);
    expect(packCount).toBe(2);

    const configuredTarball = selectViewServerTarball({
      requestedTarball: "/registry/effect-view-server.tgz",
      artifactsReady: false,
      buildLocal: () => {
        buildCount += 1;
      },
      packLocal: () => {
        packCount += 1;
        return "/unused/effect-view-server.tgz";
      },
    });
    expect(configuredTarball).toBe("/registry/effect-view-server.tgz");
    expect(buildCount).toBe(1);
    expect(packCount).toBe(2);
  });

  it("prefers declaration siblings when runtime and declaration files both exist", () => {
    for (const [runtimeExtension, declarationExtension] of [
      ["mjs", "d.mts"],
      ["js", "d.ts"],
    ] as const) {
      const fixture = mkdtempSync(join(tmpdir(), "view-server-declaration-closure-"));
      const entryPath = join(fixture, "index.d.mts");
      const declarationPath = join(fixture, `shared.${declarationExtension}`);
      const runtimePath = join(fixture, `shared.${runtimeExtension}`);
      writeFileSync(entryPath, `export * from "./shared.${runtimeExtension}";\n`);
      writeFileSync(
        declarationPath,
        "declare global { interface LeakedAmbient {} }\nexport {};\n",
      );
      writeFileSync(runtimePath, "export const runtimeOnly = true;\n");

      const closure = collectDeclarationClosure(entryPath);

      expect(closure.map(({ path }) => path)).toStrictEqual([entryPath, declarationPath]);
      expect(closure.map(({ source }) => source).join("\n")).toMatch(/\bdeclare\s+global\b/);
      rmSync(fixture, { force: true, recursive: true });
    }
  });

  it("isolates the complete Client root declaration closure from the optional Effect entry", () => {
    const declarationClosure = collectDeclarationClosure(
      join(downstreamDirectory, "dist", "index.d.mts"),
    );
    const downstreamDeclaration = declarationClosure.map(({ source }) => source).join("\n");
    const downstreamRuntime = readFileSync(
      join(downstreamDirectory, "dist", "index.mjs"),
      "utf8",
    );
    expect(downstreamDeclaration).toContain("BundledViewportBaseRow");
    expect(downstreamDeclaration).toContain("BundledViewportCompleteRawSelect");
    expect(
      declarationClosure.flatMap(({ path, source }) =>
        inspectTypeScriptModule({ fileName: path, source }).moduleSpecifiers.filter(
          (specifier) =>
            specifier === "effect" ||
            specifier.startsWith("effect/") ||
            specifier === "effect-view-server" ||
            specifier.startsWith("effect-view-server/"),
        ),
      ),
    ).toStrictEqual([]);
    expect(declarationClosure.map(({ source }) => source).join("\n")).not.toMatch(
      /\bdeclare\s+(?:global|module)\b|stackTraceLimit|\bReact(?:Node|Element|Portal|HTML)?\b/,
    );
    expect(downstreamRuntime).not.toContain("effect-view-server");
    expect(downstreamRuntime).not.toMatch(/(?:from|import\()[^\n]*["']effect(?:\/|["'])/);
  });

  it("packs a source-owned pure helper declaration and an empty runtime module", () => {
    const installedPackage = join(downstreamDirectory, "node_modules", "effect-view-server");
    const declarationPath = join(installedPackage, "dist", "react-viewport-base-row.d.ts");
    const runtimePath = join(installedPackage, "dist", "react-viewport-base-row.js");
    const declaration = readFileSync(declarationPath, "utf8");
    const inspection = inspectTypeScriptModule({ fileName: declarationPath, source: declaration });

    expect(declaration).toContain("type LiveQueryViewportBaseRow<Viewport>");
    expect(declaration).toContain("type LiveQueryViewportCompleteRawSelect<Viewport>");
    expect(inspection.moduleSpecifiers).toStrictEqual([]);
    expect(declaration).not.toMatch(
      /\bdeclare\s+(?:global|module)\b|stackTraceLimit|\bReact(?:Node|Element|Portal|HTML)?\b|\bEffect\b/,
    );
    expect(readFileSync(runtimePath, "utf8").trim()).toBe("export {};");
  });

  it(
    "accepts a real matching viewport and rejects unsafe sources",
    () => {
      const integrationDirectory = join(temporaryRoot, "integration-consumer");
      mkdirSync(integrationDirectory, { recursive: true });
      writeJson(join(integrationDirectory, "package.json"), {
        name: "viewport-integration-consumer",
        private: true,
        type: "module",
        packageManager: "pnpm@11.9.0",
        dependencies: {
          "@emnapi/core": "1.7.1",
          "@emnapi/runtime": "1.7.1",
          "@effect/atom-react": "4.0.0-rc.111",
          "@types/node": "26.2.0",
          "@types/react": "19.2.18",
          "@types/react-dom": "19.2.4",
          "downstream-viewport-adapter": `file:${downstreamTarball}`,
          effect: "4.0.0-rc.111",
          "effect-view-server": `file:${viewServerTarball}`,
          react: "19.2.8",
          "react-dom": "19.2.8",
          redis: "6.2.1",
          typescript: "7.0.2",
          vite: "8.0.0",
        },
      });
      writeJson(join(integrationDirectory, "tsconfig.json"), {
        compilerOptions: {
          exactOptionalPropertyTypes: true,
          lib: ["ESNext", "DOM"],
          module: "preserve",
          moduleResolution: "bundler",
          noEmit: true,
          skipLibCheck: false,
          strict: true,
          types: ["node"],
        },
        include: ["consumer.ts"],
      });
      writeFileSync(
        join(integrationDirectory, "consumer.ts"),
        [
          'import type { BundledViewportBaseRow, BundledViewportCompleteRawSelect } from "downstream-viewport-adapter";',
          'import { ViewServerId, defineViewServerConfig } from "effect-view-server/config";',
          'import type { ExactRawQuery, LiveQueryRow } from "effect-view-server/config";',
          'import type * as PublicConfig from "effect-view-server/config";',
          'import type { LiveQueryViewport, UseLiveQueryViewportResult } from "effect-view-server/react";',
          'import { Schema } from "effect";',
          "",
          "const orderConfig = defineViewServerConfig({",
          "  topics: { orders: { schema: Schema.Struct({ id: ViewServerId, total: Schema.Number }) } },",
          "});",
          "const positionConfig = defineViewServerConfig({",
          "  topics: { positions: { schema: Schema.Struct({ id: ViewServerId, quantity: Schema.Number }) } },",
          "});",
          "const optionalOrderConfig = defineViewServerConfig({",
          "  topics: { orders: { schema: Schema.Struct({ id: ViewServerId, total: Schema.Number, note: Schema.optionalKey(Schema.String) }) } },",
          "});",
          'declare const orders: LiveQueryViewport<typeof orderConfig.topics, "orders">;',
          'declare const orderSource: UseLiveQueryViewportResult<typeof orderConfig.topics, "orders">;',
          'declare const positions: LiveQueryViewport<typeof positionConfig.topics, "positions">;',
          'declare const optionalOrders: LiveQueryViewport<typeof optionalOrderConfig.topics, "orders">;',
          "type Order = typeof orderConfig.topics.orders.schema.Type;",
          "type RequireNever<Value extends never> = Value;",
          "// @ts-expect-error row-only complete-projection authority is not public.",
          "type _NoPublicCompleteSelect = PublicConfig.LiveQueryViewportCompleteRawSelectForRow;",
          "const forgedCompleteSelect = Object.assign([\"id\"] as const, {",
          '  "__effect-view-server/LiveQueryViewportCompleteRawSelect@v1": (row: Order): Order => row,',
          "});",
          "type RejectForgedCompleteSelect = RequireNever<",
          "  ExactRawQuery<Order, { readonly select: typeof forgedCompleteSelect }>",
          ">;",
          "declare const forgedRow: LiveQueryRow<Order, { readonly select: typeof forgedCompleteSelect }> ;",
          "const forgedRowIdOnly: { readonly id: string } = forgedRow;",
          "// @ts-expect-error a copied structural witness cannot expose an unselected field.",
          "forgedRow.total;",
          "type ExactSource<Row, Viewport> =",
          "  [BundledViewportBaseRow<Viewport>] extends [Row]",
          "    ? [Row] extends [BundledViewportBaseRow<Viewport>] ? Viewport : never",
          "    : never;",
          "declare const extractedOrder: BundledViewportBaseRow<typeof orders>;",
          "declare const expectedOrder: Order;",
          "const exactForward: Order = extractedOrder;",
          "const exactBackward: BundledViewportBaseRow<typeof orders> = expectedOrder;",
          "const matching: ExactSource<Order, typeof orders> = orders;",
          "const completeSelect: BundledViewportCompleteRawSelect<typeof orders> = orderSource.completeRawSelect;",
          'const completeField: "id" | "total" = completeSelect[0];',
          "const detachedCompleteField = [completeSelect[0]] as const;",
          "// @ts-expect-error detaching a field cannot retain complete-projection authority.",
          'detachedCompleteField["__effect-view-server/LiveQueryViewportCompleteRawSelect@v1"];',
          'type SameRowViewportUnion = (typeof orders & { readonly source: "left" }) | (typeof orders & { readonly source: "right" });',
          "declare const sameRowUnion: SameRowViewportUnion;",
          "declare const extractedSameRowUnion: BundledViewportBaseRow<SameRowViewportUnion>;",
          "const sameRowUnionForward: Order = extractedSameRowUnion;",
          "const sameRowUnionBackward: BundledViewportBaseRow<SameRowViewportUnion> = expectedOrder;",
          "// @ts-expect-error a viewport for another Topic Row is rejected invariantly.",
          "const wrong: ExactSource<Order, typeof positions> = positions;",
          "type RejectAny = RequireNever<BundledViewportBaseRow<any>>;",
          "type RejectCompleteAny = RequireNever<BundledViewportCompleteRawSelect<any>>;",
          "type RejectCompleteUnknown = RequireNever<BundledViewportCompleteRawSelect<unknown>>;",
          "type RejectCompleteUnwitnessed = RequireNever<BundledViewportCompleteRawSelect<{ readonly destroy: () => void }>>;",
          "type RejectUnknown = RequireNever<BundledViewportBaseRow<unknown>>;",
          "type RejectErasedViewport = RequireNever<BundledViewportBaseRow<LiveQueryViewport<any, string>>>;",
          "type RejectStringIndex = RequireNever<BundledViewportBaseRow<Readonly<Record<string, (_row: Order) => Order>>>>;",
          "type RejectPatternIndex = RequireNever<BundledViewportBaseRow<Readonly<Record<`__effect-view-server/${string}`, (_row: Order) => Order>>>>;",
          "type RejectUnsafeUnion = RequireNever<BundledViewportBaseRow<LiveQueryViewport<typeof orderConfig.topics, \"orders\"> | Readonly<Record<string, (_row: Order) => Order>>>>;",
          "type RejectMixedViewportUnion = RequireNever<BundledViewportBaseRow<typeof orders | typeof positions>>;",
          "type RejectIntersectedViewports = RequireNever<BundledViewportBaseRow<typeof orders & typeof positions>>;",
          "type RejectEquivalentViewportUnion = RequireNever<BundledViewportBaseRow<typeof orders | typeof optionalOrders>>;",
          "type RejectNonInvariantWitness = RequireNever<BundledViewportBaseRow<{ readonly \"__effect-view-server/LiveQueryViewportBaseRow@v1\"?: (_row: Order) => Order & { readonly note?: string } }>>;",
          "type RejectPartlyCallableWitness = RequireNever<BundledViewportBaseRow<{ readonly \"__effect-view-server/LiveQueryViewportBaseRow@v1\"?: ((_row: Order) => Order) | 0 }>>;",
          "type RejectMixedCallableWitness = RequireNever<BundledViewportBaseRow<{ readonly \"__effect-view-server/LiveQueryViewportBaseRow@v1\"?: ((_row: Order) => Order) | ((_row: typeof positionConfig.topics.positions.schema.Type) => typeof positionConfig.topics.positions.schema.Type) }>>;",
          "type RejectOverloadedWitness = RequireNever<BundledViewportBaseRow<{ readonly \"__effect-view-server/LiveQueryViewportBaseRow@v1\"?: ((_row: Order) => Order) & ((_row: typeof positionConfig.topics.positions.schema.Type) => typeof positionConfig.topics.positions.schema.Type) }>>;",
          "type RejectUnwitnessed = RequireNever<BundledViewportBaseRow<{ readonly destroy: () => void }>>;",
          "void exactForward;",
          "void exactBackward;",
          "void matching;",
          "void completeSelect;",
          "void completeField;",
          "void detachedCompleteField;",
          "void sameRowUnion;",
          "void sameRowUnionForward;",
          "void sameRowUnionBackward;",
          "void wrong;",
          "type Rejected = RejectAny | RejectCompleteAny | RejectCompleteUnknown | RejectCompleteUnwitnessed | RejectUnknown | RejectErasedViewport | RejectStringIndex | RejectPatternIndex | RejectUnsafeUnion | RejectMixedViewportUnion | RejectIntersectedViewports | RejectEquivalentViewportUnion | RejectNonInvariantWitness | RejectPartlyCallableWitness | RejectMixedCallableWitness | RejectOverloadedWitness | RejectUnwitnessed;",
          "",
        ].join("\n"),
      );
      installStrictly(integrationDirectory);
      run("vp", ["exec", "tsc"], integrationDirectory);
      run(
        process.execPath,
        [
          "--input-type=module",
          "--eval",
          [
            'const facade = await import("effect-view-server/react");',
            "const keys = Reflect.ownKeys(facade);",
            'if (keys.length !== 2 || keys[0] !== "createViewServerReact" || keys[1] !== Symbol.toStringTag) {',
            '  throw new Error(`Unexpected React facade keys: ${keys.map(String).join(", ")}`);',
            "}",
            'const helper = await import("effect-view-server/react/viewport-base-row");',
            "const helperKeys = Reflect.ownKeys(helper);",
            "if (helperKeys.length !== 1 || helperKeys[0] !== Symbol.toStringTag) {",
            '  throw new Error(`Unexpected pure helper keys: ${helperKeys.map(String).join(", ")}`);',
            "}",
          ].join("\n"),
        ],
        integrationDirectory,
      );
    },
    scenarioTimeoutMilliseconds,
  );

  it(
    "installs and imports the downstream artifact without Effect",
    () => {
      const clientDirectory = join(temporaryRoot, "client-only-consumer");
      mkdirSync(clientDirectory, { recursive: true });
      writeJson(join(clientDirectory, "package.json"), {
        name: "viewport-client-only-consumer",
        private: true,
        type: "module",
        packageManager: "pnpm@11.9.0",
        dependencies: {
          "downstream-viewport-adapter": `file:${downstreamTarball}`,
          typescript: "7.0.2",
        },
      });
      writeJson(join(clientDirectory, "tsconfig.json"), {
        compilerOptions: {
          module: "preserve",
          moduleResolution: "bundler",
          noEmit: true,
          skipLibCheck: false,
          strict: true,
        },
        include: ["consumer.ts"],
      });
      writeFileSync(
        join(clientDirectory, "consumer.ts"),
        [
          'import { clientReady, type BundledViewportBaseRow, type BundledViewportCompleteRawSelect } from "downstream-viewport-adapter";',
          "type RequireNever<Value extends never> = Value;",
          "type ClientRow = { readonly id: string };",
          "type ClientWitness = { readonly \"__effect-view-server/LiveQueryViewportBaseRow@v1\"?: (_row: ClientRow) => ClientRow };",
          "type CompleteClientSelect = BundledViewportCompleteRawSelect<ClientWitness>;",
          "declare const completeClientSelect: CompleteClientSelect;",
          'const completeClientField: "id" = completeClientSelect[0];',
          "type CompleteAny = RequireNever<BundledViewportCompleteRawSelect<any>>;",
          "type CompleteUnknown = RequireNever<BundledViewportCompleteRawSelect<unknown>>;",
          "type OtherClientRow = { readonly key: number };",
          "type OtherClientWitness = { readonly \"__effect-view-server/LiveQueryViewportBaseRow@v1\"?: (_row: OtherClientRow) => OtherClientRow };",
          "type OptionalClientRow = ClientRow & { readonly note?: string };",
          "type OptionalClientWitness = { readonly \"__effect-view-server/LiveQueryViewportBaseRow@v1\"?: (_row: OptionalClientRow) => OptionalClientRow };",
          "type StringIndex = RequireNever<BundledViewportBaseRow<Readonly<Record<string, (_row: ClientRow) => ClientRow>>>>;",
          "type PatternIndex = RequireNever<BundledViewportBaseRow<Readonly<Record<`__effect-view-server/${string}`, (_row: ClientRow) => ClientRow>>>>;",
          "type UnsafeUnion = RequireNever<BundledViewportBaseRow<ClientWitness | Readonly<Record<string, (_row: ClientRow) => ClientRow>>>>;",
          "type MixedWitnessUnion = RequireNever<BundledViewportBaseRow<ClientWitness | OtherClientWitness>>;",
          "type IntersectedWitnesses = RequireNever<BundledViewportBaseRow<ClientWitness & OtherClientWitness>>;",
          "type EquivalentWitnessUnion = RequireNever<BundledViewportBaseRow<ClientWitness | OptionalClientWitness>>;",
          "type NonInvariantWitness = RequireNever<BundledViewportBaseRow<{ readonly \"__effect-view-server/LiveQueryViewportBaseRow@v1\"?: (_row: ClientRow) => OptionalClientRow }>>;",
          "type PartlyCallableWitness = RequireNever<BundledViewportBaseRow<{ readonly \"__effect-view-server/LiveQueryViewportBaseRow@v1\"?: ((_row: ClientRow) => ClientRow) | 0 }>>;",
          "type MixedCallableWitness = RequireNever<BundledViewportBaseRow<{ readonly \"__effect-view-server/LiveQueryViewportBaseRow@v1\"?: ((_row: ClientRow) => ClientRow) | ((_row: OtherClientRow) => OtherClientRow) }>>;",
          "type OverloadedWitness = RequireNever<BundledViewportBaseRow<{ readonly \"__effect-view-server/LiveQueryViewportBaseRow@v1\"?: ((_row: ClientRow) => ClientRow) & ((_row: OtherClientRow) => OtherClientRow) }>>;",
          "type Unwitnessed = RequireNever<BundledViewportBaseRow<{ readonly ready: true }>>;",
          "if (!clientReady) throw new Error(\"downstream runtime was not ready\");",
          "void completeClientSelect;",
          "void completeClientField;",
          "type ClientRejection = CompleteAny | CompleteUnknown | StringIndex | PatternIndex | UnsafeUnion | MixedWitnessUnion | IntersectedWitnesses | EquivalentWitnessUnion | NonInvariantWitness | PartlyCallableWitness | MixedCallableWitness | OverloadedWitness | Unwitnessed;",
          "",
        ].join("\n"),
      );
      installStrictly(clientDirectory);
      const installedPackageEntries = readdirSync(
        join(clientDirectory, "node_modules", ".pnpm"),
      );
      expect(
        installedPackageEntries.filter(
          (entry) => entry.startsWith("effect@") || entry.startsWith("effect-view-server@"),
        ),
      ).toStrictEqual([]);
      const clientLockfile = readFileSync(join(clientDirectory, "pnpm-lock.yaml"), "utf8");
      expect(clientLockfile).not.toMatch(/^\s{2}(?:effect|effect-view-server)@/m);
      run("vp", ["exec", "tsc"], clientDirectory);
      run(
        process.execPath,
        ["--input-type=module", "--eval", 'await import("downstream-viewport-adapter")'],
        clientDirectory,
      );
    },
    scenarioTimeoutMilliseconds,
  );
});
