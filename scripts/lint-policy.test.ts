import { describe, expect, it } from "@effect/vitest";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import ts from "typescript-compiler-api";
import { strictLintOptions } from "../tools/vite/lint-policy";

const lintConfigPaths = [
  "vite.config.ts",
  "apps/example/vite.config.ts",
  "examples/vite.config.shared.ts",
  ...readdirSync("packages")
    .map((directory) => `packages/${directory}/vite.config.ts`)
    .filter(existsSync),
].sort();

const propertyName = (name: ts.PropertyName): string | undefined =>
  ts.isIdentifier(name) || ts.isStringLiteral(name) ? name.text : undefined;

const unwrapParentheses = (expression: ts.Expression): ts.Expression =>
  ts.isParenthesizedExpression(expression) ? unwrapParentheses(expression.expression) : expression;

const configObjectFromDefineConfig = (
  call: ts.CallExpression,
): ts.ObjectLiteralExpression | undefined => {
  const argument = call.arguments[0];
  if (argument === undefined) {
    return undefined;
  }
  const unwrappedArgument = unwrapParentheses(argument);
  if (ts.isObjectLiteralExpression(unwrappedArgument)) {
    return unwrappedArgument;
  }
  if (
    !ts.isArrowFunction(unwrappedArgument) ||
    ts.isBlock(unwrappedArgument.body)
  ) {
    return undefined;
  }
  const body = unwrapParentheses(unwrappedArgument.body);
  return ts.isObjectLiteralExpression(body) ? body : undefined;
};

const inspectLintPolicy = (path: string) => {
  const sourceFile = ts.createSourceFile(
    path,
    readFileSync(path, "utf8"),
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TS,
  );
  const importsStrictLintOptions = sourceFile.statements.some(
    (statement) =>
      ts.isImportDeclaration(statement) &&
      ts.isStringLiteral(statement.moduleSpecifier) &&
      statement.moduleSpecifier.text.endsWith("tools/vite/lint-policy") &&
      statement.importClause?.namedBindings !== undefined &&
      ts.isNamedImports(statement.importClause.namedBindings) &&
      statement.importClause.namedBindings.elements.some(
        (element) => element.name.text === "strictLintOptions",
      ),
  );
  const defineConfigCalls: Array<ts.CallExpression> = [];
  const visit = (node: ts.Node): void => {
    if (
      ts.isCallExpression(node) &&
      ts.isIdentifier(node.expression) &&
      node.expression.text === "defineConfig"
    ) {
      defineConfigCalls.push(node);
    }
    ts.forEachChild(node, visit);
  };
  visit(sourceFile);
  const configObject =
    defineConfigCalls.length === 1
      ? configObjectFromDefineConfig(defineConfigCalls[0])
      : undefined;
  const lintProperty = configObject?.properties.find(
    (property) => ts.isPropertyAssignment(property) && propertyName(property.name) === "lint",
  );
  const usesStrictLintOptions =
    lintProperty !== undefined &&
    ts.isPropertyAssignment(lintProperty) &&
    ts.isObjectLiteralExpression(lintProperty.initializer) &&
    lintProperty.initializer.properties.some(
      (property) =>
        ts.isPropertyAssignment(property) &&
        propertyName(property.name) === "options" &&
        ts.isIdentifier(property.initializer) &&
        property.initializer.text === "strictLintOptions",
    );
  return {
    defineConfigCallCount: defineConfigCalls.length,
    importsStrictLintOptions,
    path,
    usesStrictLintOptions,
  };
};

describe("repository lint policy", () => {
  it("makes every warning fatal through one shared Vite+ interface", () => {
    expect(strictLintOptions).toStrictEqual({
      denyWarnings: true,
      typeAware: true,
      typeCheck: true,
    });
    expect(lintConfigPaths.map(inspectLintPolicy)).toStrictEqual(
      lintConfigPaths.map((path) => ({
        defineConfigCallCount: 1,
        importsStrictLintOptions: true,
        path,
        usesStrictLintOptions: true,
      })),
    );
  });

  it("hard-fails native React compiler and Hooks diagnostics", () => {
    const rootPackage = JSON.parse(readFileSync("package.json", "utf8"));

    expect(rootPackage.scripts["check:react-compiler"]).toBe(
      "vp exec oxlint --deny react/react-compiler --deny react/rules-of-hooks --deny react/exhaustive-deps --react-plugin --deny-warnings examples",
    );
  });
});
