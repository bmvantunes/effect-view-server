import ts from "typescript";

export type ModuleReexport =
  | {
      readonly kind: "all";
      readonly moduleSpecifier: string;
      readonly typeOnly: boolean;
    }
  | {
      readonly exportedName: string;
      readonly kind: "namespace";
      readonly moduleSpecifier: string;
      readonly typeOnly: boolean;
    }
  | {
      readonly exports: ReadonlyArray<{
        readonly exportedName: string;
        readonly sourceName: string;
        readonly typeOnly: boolean;
      }>;
      readonly kind: "named";
      readonly moduleSpecifier: string | undefined;
    };

export type TypeScriptModuleInspection = {
  readonly moduleSpecifiers: ReadonlyArray<string>;
  readonly reexports: ReadonlyArray<ModuleReexport>;
  readonly violations: ReadonlyArray<UnsupportedModuleLoad>;
};

export type ModuleLoader =
  | "create-require"
  | "create-require-factory"
  | "create-require-resolve"
  | "dynamic-import"
  | "import-equals"
  | "import-type"
  | "import-meta-resolve"
  | "module-require"
  | "require"
  | "require-resolve"
  | "static-export"
  | "static-import";

export type UnsupportedModuleLoad = {
  readonly column: number;
  readonly kind:
    | "ambiguous-loader-call"
    | "computed-loader"
    | "constructed-loader"
    | "controlled-loader"
    | "interpolated-specifier"
    | "loader-alias"
    | "loader-wrapper"
    | "malformed-literal"
    | "non-literal-specifier"
    | "tagged-loader";
  readonly line: number;
  readonly loader: ModuleLoader;
};

export type LibraryPackViolation = {
  readonly column: number;
  readonly kind:
    | "ambiguous-library-pack-override"
    | "duplicate-library-pack"
    | "missing-library-pack"
    | "non-literal-library-pack";
  readonly line: number;
};

const scriptKindForFileName = (fileName: string): ts.ScriptKind => {
  if (fileName.endsWith(".tsx")) {
    return ts.ScriptKind.TSX;
  }
  if (fileName.endsWith(".jsx")) {
    return ts.ScriptKind.JSX;
  }
  if (
    fileName.endsWith(".js") ||
    fileName.endsWith(".mjs") ||
    fileName.endsWith(".cjs")
  ) {
    return ts.ScriptKind.JS;
  }
  return ts.ScriptKind.TS;
};

const createSourceFileForInspection = (fileName: string, source: string): ts.SourceFile =>
  ts.createSourceFile(
    fileName,
    source,
    ts.ScriptTarget.Latest,
    true,
    scriptKindForFileName(fileName),
  );

const unwrapExpression = (expression: ts.Expression): ts.Expression => {
  let current = expression;
  while (
    ts.isParenthesizedExpression(current) ||
    ts.isAsExpression(current) ||
    ts.isTypeAssertionExpression(current) ||
    ts.isNonNullExpression(current) ||
    ts.isSatisfiesExpression(current)
  ) {
    current = current.expression;
  }
  return current;
};

const literalModuleSpecifier = (expression: ts.Expression): string | undefined => {
  const unwrapped = unwrapExpression(expression);
  return ts.isStringLiteralLike(unwrapped) && unwrapped.isUnterminated !== true
    ? unwrapped.text
    : undefined;
};

const unsupportedSpecifierKind = (
  expression: ts.Expression,
): "malformed-literal" | "non-literal-specifier" => {
  const unwrapped = unwrapExpression(expression);
  return ts.isStringLiteralLike(unwrapped) && unwrapped.isUnterminated === true
    ? "malformed-literal"
    : "non-literal-specifier";
};

const isIdentifierNamed = (expression: ts.Expression, name: string): boolean => {
  const unwrapped = unwrapExpression(expression);
  return ts.isIdentifier(unwrapped) && unwrapped.text === name;
};

const isImportMeta = (expression: ts.Expression): boolean => {
  const unwrapped = unwrapExpression(expression);
  return (
    ts.isMetaProperty(unwrapped) &&
    unwrapped.keywordToken === ts.SyntaxKind.ImportKeyword &&
    unwrapped.name.text === "meta"
  );
};

const isDirectPropertyAccess = (
  expression: ts.Expression,
  owner: (expression: ts.Expression) => boolean,
  property: string,
): boolean => {
  const unwrapped = unwrapExpression(expression);
  if (ts.isPropertyAccessExpression(unwrapped)) {
    return owner(unwrapped.expression) && unwrapped.name.text === property;
  }
  if (ts.isElementAccessExpression(unwrapped)) {
    const argument = unwrapped.argumentExpression;
    return (
      argument !== undefined &&
      owner(unwrapped.expression) &&
      literalModuleSpecifier(argument) === property
    );
  }
  return false;
};

const isCreateRequireFactoryCall = (expression: ts.Expression): boolean => {
  const unwrapped = unwrapExpression(expression);
  return (
    ts.isCallExpression(unwrapped) &&
    isIdentifierNamed(unwrapped.expression, "createRequire")
  );
};

const moduleLoaderForExpression = (expression: ts.Expression): ModuleLoader | undefined => {
  const unwrapped = unwrapExpression(expression);
  if (unwrapped.kind === ts.SyntaxKind.ImportKeyword) {
    return "dynamic-import";
  }
  if (isIdentifierNamed(unwrapped, "require")) {
    return "require";
  }
  if (
    isDirectPropertyAccess(
      unwrapped,
      (owner) => isIdentifierNamed(owner, "require"),
      "resolve",
    )
  ) {
    return "require-resolve";
  }
  if (
    isDirectPropertyAccess(
      unwrapped,
      (owner) => isIdentifierNamed(owner, "module"),
      "require",
    )
  ) {
    return "module-require";
  }
  if (isDirectPropertyAccess(unwrapped, isImportMeta, "resolve")) {
    return "import-meta-resolve";
  }
  if (isCreateRequireFactoryCall(unwrapped)) {
    return "create-require";
  }
  if (isDirectPropertyAccess(unwrapped, isCreateRequireFactoryCall, "resolve")) {
    return "create-require-resolve";
  }
  return undefined;
};

const aliasableModuleLoaderForExpression = (
  expression: ts.Expression,
): ModuleLoader | undefined =>
  isIdentifierNamed(expression, "createRequire")
    ? "create-require-factory"
    : moduleLoaderForExpression(expression);

const nodeModuleCapabilityAcquisition = (expression: ts.Expression): ModuleLoader | undefined => {
  const unwrapped = unwrapExpression(expression);
  const acquisition = ts.isAwaitExpression(unwrapped)
    ? unwrapExpression(unwrapped.expression)
    : unwrapped;
  if (!ts.isCallExpression(acquisition)) {
    return undefined;
  }
  const loader = moduleLoaderForExpression(acquisition.expression);
  const moduleSpecifier = acquisition.arguments[0];
  return (loader === "require" || loader === "dynamic-import") &&
    moduleSpecifier !== undefined &&
    (literalModuleSpecifier(moduleSpecifier) === "node:module" ||
      literalModuleSpecifier(moduleSpecifier) === "module")
    ? "create-require-factory"
    : undefined;
};

const escapedModuleLoaderForExpression = (expression: ts.Expression): ModuleLoader | undefined =>
  aliasableModuleLoaderForExpression(expression) ?? nodeModuleCapabilityAcquisition(expression);

const directNodeModuleCreateRequireAccess = (
  expression: ts.Expression,
): "computed" | "direct" | undefined => {
  const access = propertyAccessParts(expression);
  if (access === undefined || nodeModuleCapabilityAcquisition(access.owner) === undefined) {
    return undefined;
  }
  if (access.name === "createRequire") {
    return "direct";
  }
  return access.name === undefined ? "computed" : undefined;
};

const supportedModuleLoadCall = (
  call: ts.CallExpression,
): { readonly argument: ts.Expression | undefined; readonly loader: ModuleLoader } | undefined => {
  if (isIdentifierNamed(call.expression, "createRequire")) {
    return undefined;
  }
  const loader = moduleLoaderForExpression(call.expression);
  return loader === undefined ? undefined : { argument: call.arguments[0], loader };
};

const propertyAccessParts = (
  expression: ts.Expression,
): { readonly name: string | undefined; readonly owner: ts.Expression } | undefined => {
  const unwrapped = unwrapExpression(expression);
  if (ts.isPropertyAccessExpression(unwrapped)) {
    return { name: unwrapped.name.text, owner: unwrapped.expression };
  }
  if (ts.isElementAccessExpression(unwrapped)) {
    return {
      name: literalModuleSpecifier(unwrapped.argumentExpression),
      owner: unwrapped.expression,
    };
  }
  return undefined;
};

const computedModuleLoaderForExpression = (
  expression: ts.Expression,
): ModuleLoader | undefined => {
  const unwrapped = unwrapExpression(expression);
  if (!ts.isElementAccessExpression(unwrapped)) {
    return undefined;
  }
  const argument = unwrapped.argumentExpression;
  if (argument !== undefined && literalModuleSpecifier(argument) !== undefined) {
    return undefined;
  }
  const owner = unwrapExpression(unwrapped.expression);
  if (isIdentifierNamed(owner, "require")) {
    return "require-resolve";
  }
  if (isIdentifierNamed(owner, "module")) {
    return "module-require";
  }
  if (isImportMeta(owner)) {
    return "import-meta-resolve";
  }
  return isCreateRequireFactoryCall(owner) ? "create-require-resolve" : undefined;
};

const wrappedModuleLoaderForExpression = (
  expression: ts.Expression,
): ModuleLoader | undefined => {
  const access = propertyAccessParts(expression);
  if (
    access === undefined ||
    (access.name !== "bind" && access.name !== "call" && access.name !== "apply")
  ) {
    return undefined;
  }
  return aliasableModuleLoaderForExpression(access.owner);
};

const containedModuleLoader = (node: ts.Node): ModuleLoader | undefined => {
  if (ts.isExpression(node)) {
    const loader = aliasableModuleLoaderForExpression(node);
    if (loader !== undefined) {
      return loader;
    }
  }
  let found: ModuleLoader | undefined;
  ts.forEachChild(node, (child) => {
    if (found === undefined) {
      found = containedModuleLoader(child);
    }
  });
  return found;
};

const controlledModuleLoaderForExpression = (
  expression: ts.Expression,
): ModuleLoader | undefined => {
  const unwrapped = unwrapExpression(expression);
  return ts.isBinaryExpression(unwrapped) || ts.isConditionalExpression(unwrapped)
    ? containedModuleLoader(unwrapped)
    : undefined;
};

const importTypeSpecifier = (node: ts.ImportTypeNode): string | undefined => {
  if (!ts.isLiteralTypeNode(node.argument)) {
    return undefined;
  }
  return literalModuleSpecifier(node.argument.literal);
};

const importEqualsSpecifier = (node: ts.ImportEqualsDeclaration): string | undefined => {
  if (!ts.isExternalModuleReference(node.moduleReference)) {
    return undefined;
  }
  return literalModuleSpecifier(node.moduleReference.expression);
};

const namedExports = (
  exportClause: ts.NamedExports,
  declarationIsTypeOnly: boolean,
): ReadonlyArray<{
  readonly exportedName: string;
  readonly sourceName: string;
  readonly typeOnly: boolean;
}> => {
  return exportClause.elements.map((element) => ({
    exportedName: element.name.text,
    sourceName: element.propertyName?.text ?? element.name.text,
    typeOnly: declarationIsTypeOnly || element.isTypeOnly,
  }));
};

const importsAliasedCreateRequireCapability = (
  declaration: ts.ImportDeclaration,
  moduleSpecifier: string,
): boolean => {
  if (
    (moduleSpecifier !== "node:module" && moduleSpecifier !== "module") ||
    declaration.importClause === undefined ||
    declaration.importClause.isTypeOnly
  ) {
    return false;
  }
  if (declaration.importClause.name !== undefined) {
    return true;
  }
  const clauseChildren = declaration.importClause.getChildren();
  if (clauseChildren.some(ts.isNamespaceImport)) {
    return true;
  }
  return clauseChildren
    .filter(ts.isNamedImports)
    .flatMap((bindings) => bindings.elements)
    .some(
      (element) =>
        !element.isTypeOnly &&
        (element.propertyName?.text ?? element.name.text) === "createRequire" &&
        element.name.text !== "createRequire",
    );
};

export const inspectTypeScriptModule = ({
  fileName,
  source,
}: {
  readonly fileName: string;
  readonly source: string;
}): TypeScriptModuleInspection => {
  const sourceFile = createSourceFileForInspection(fileName, source);
  const moduleSpecifiers: Array<string> = [];
  const reexports: Array<ModuleReexport> = [];
  const violations: Array<UnsupportedModuleLoad> = [];

  const addViolation = (
    node: ts.Node,
    kind: UnsupportedModuleLoad["kind"],
    loader: ModuleLoader,
  ): void => {
    const location = sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile));
    violations.push({ column: location.character + 1, kind, line: location.line + 1, loader });
  };

  const visit = (node: ts.Node): void => {
    if (ts.isImportDeclaration(node)) {
      const moduleSpecifier = literalModuleSpecifier(node.moduleSpecifier);
      if (moduleSpecifier === undefined) {
        addViolation(node, unsupportedSpecifierKind(node.moduleSpecifier), "static-import");
      } else {
        moduleSpecifiers.push(moduleSpecifier);
        if (importsAliasedCreateRequireCapability(node, moduleSpecifier)) {
          addViolation(node, "loader-alias", "create-require-factory");
        }
      }
    } else if (ts.isExportDeclaration(node)) {
      const moduleSpecifier =
        node.moduleSpecifier === undefined
          ? undefined
          : literalModuleSpecifier(node.moduleSpecifier);
      if (moduleSpecifier !== undefined) {
        moduleSpecifiers.push(moduleSpecifier);
      } else if (node.moduleSpecifier !== undefined) {
        addViolation(node, unsupportedSpecifierKind(node.moduleSpecifier), "static-export");
      }
      if (node.exportClause === undefined && moduleSpecifier !== undefined) {
        reexports.push({ kind: "all", moduleSpecifier, typeOnly: node.isTypeOnly });
      } else if (node.exportClause !== undefined && ts.isNamespaceExport(node.exportClause)) {
        if (moduleSpecifier !== undefined) {
          reexports.push({
            exportedName: node.exportClause.name.text,
            kind: "namespace",
            moduleSpecifier,
            typeOnly: node.isTypeOnly,
          });
        }
      } else if (node.exportClause !== undefined && ts.isNamedExports(node.exportClause)) {
        reexports.push({
          exports: namedExports(node.exportClause, node.isTypeOnly),
          kind: "named",
          moduleSpecifier,
        });
      }
    } else if (ts.isImportTypeNode(node)) {
      const moduleSpecifier = importTypeSpecifier(node);
      if (moduleSpecifier !== undefined) {
        moduleSpecifiers.push(moduleSpecifier);
      } else {
        addViolation(node, "non-literal-specifier", "import-type");
      }
    } else if (ts.isImportEqualsDeclaration(node)) {
      const moduleSpecifier = importEqualsSpecifier(node);
      if (moduleSpecifier !== undefined) {
        moduleSpecifiers.push(moduleSpecifier);
      } else if (ts.isExternalModuleReference(node.moduleReference)) {
        addViolation(
          node,
          unsupportedSpecifierKind(node.moduleReference.expression),
          "import-equals",
        );
      }
    } else if (ts.isPropertyAccessExpression(node) || ts.isElementAccessExpression(node)) {
      const acquisition = directNodeModuleCreateRequireAccess(node);
      if (acquisition !== undefined) {
        addViolation(
          node,
          acquisition === "computed" ? "computed-loader" : "loader-alias",
          "create-require-factory",
        );
      }
    } else if (ts.isVariableDeclaration(node) && node.initializer !== undefined) {
      const loader = escapedModuleLoaderForExpression(node.initializer);
      if (loader !== undefined) {
        addViolation(node, "loader-alias", loader);
      }
    } else if (
      ts.isBinaryExpression(node) &&
      node.operatorToken.kind === ts.SyntaxKind.EqualsToken
    ) {
      const loader = escapedModuleLoaderForExpression(node.right);
      if (loader !== undefined) {
        addViolation(node, "loader-alias", loader);
      }
    } else if (ts.isPropertyAssignment(node)) {
      const loader = escapedModuleLoaderForExpression(node.initializer);
      if (loader !== undefined) {
        addViolation(node, "loader-alias", loader);
      }
    } else if (ts.isReturnStatement(node) && node.expression !== undefined) {
      const loader = escapedModuleLoaderForExpression(node.expression);
      if (loader !== undefined) {
        addViolation(node, "loader-alias", loader);
      }
    } else if (ts.isTaggedTemplateExpression(node)) {
      const loader = aliasableModuleLoaderForExpression(node.tag);
      if (loader !== undefined) {
        addViolation(node, "tagged-loader", loader);
      }
    } else if (ts.isNewExpression(node)) {
      const loader = aliasableModuleLoaderForExpression(node.expression);
      if (loader !== undefined) {
        addViolation(node, "constructed-loader", loader);
      }
    } else if (ts.isCallExpression(node)) {
      const computedLoader = computedModuleLoaderForExpression(node.expression);
      const wrappedLoader = wrappedModuleLoaderForExpression(node.expression);
      const controlledLoader = controlledModuleLoaderForExpression(node.expression);
      const supportedLoad = supportedModuleLoadCall(node);
      if (computedLoader !== undefined) {
        addViolation(node, "computed-loader", computedLoader);
      } else if (wrappedLoader !== undefined) {
        addViolation(node, "loader-wrapper", wrappedLoader);
      } else if (controlledLoader !== undefined) {
        addViolation(node, "controlled-loader", controlledLoader);
      } else if (supportedLoad !== undefined) {
        const maximumArguments =
          supportedLoad.loader === "dynamic-import" ||
          supportedLoad.loader === "require-resolve" ||
          supportedLoad.loader === "create-require-resolve"
            ? 2
            : 1;
        if (node.arguments.length > maximumArguments) {
          addViolation(node, "ambiguous-loader-call", supportedLoad.loader);
        } else if (supportedLoad.argument === undefined) {
          addViolation(node, "non-literal-specifier", supportedLoad.loader);
        } else {
          const moduleSpecifier = literalModuleSpecifier(supportedLoad.argument);
          if (moduleSpecifier !== undefined) {
            moduleSpecifiers.push(moduleSpecifier);
          } else {
            const unwrappedArgument = unwrapExpression(supportedLoad.argument);
            addViolation(
              node,
              ts.isStringLiteralLike(unwrappedArgument) && unwrappedArgument.isUnterminated === true
                ? "malformed-literal"
                : ts.isTemplateExpression(unwrappedArgument)
                  ? "interpolated-specifier"
                  : "non-literal-specifier",
              supportedLoad.loader,
            );
          }
        }
      } else {
        const aliasedArgument = node.arguments
          .map(escapedModuleLoaderForExpression)
          .find((loader) => loader !== undefined);
        if (aliasedArgument !== undefined) {
          addViolation(node, "loader-alias", aliasedArgument);
        }
      }
    }
    ts.forEachChild(node, visit);
  };

  visit(sourceFile);
  return { moduleSpecifiers, reexports, violations };
};

export type NonReexportStatement = {
  readonly column: number;
  readonly line: number;
};

export const inspectReexportModule = ({
  fileName,
  source,
}: {
  readonly fileName: string;
  readonly source: string;
}): TypeScriptModuleInspection & {
  readonly nonReexportStatements: ReadonlyArray<NonReexportStatement>;
} => {
  const inspection = inspectTypeScriptModule({ fileName, source });
  const sourceFile = createSourceFileForInspection(fileName, source);
  const nonReexportStatements = sourceFile.statements.flatMap((statement) => {
    if (ts.isExportDeclaration(statement) && statement.moduleSpecifier !== undefined) {
      return [];
    }
    const location = sourceFile.getLineAndCharacterOfPosition(statement.getStart(sourceFile));
    return [{ column: location.character + 1, line: location.line + 1 }];
  });
  return { ...inspection, nonReexportStatements };
};

export const identifierNamesFromTypeScript = ({
  fileName,
  source,
}: {
  readonly fileName: string;
  readonly source: string;
}): ReadonlyArray<string> => {
  const sourceFile = createSourceFileForInspection(fileName, source);
  const identifiers: Array<string> = [];
  const visit = (node: ts.Node): void => {
    if (ts.isIdentifier(node)) {
      identifiers.push(node.text);
    } else if (ts.isElementAccessExpression(node) && node.argumentExpression !== undefined) {
      const propertyName = literalModuleSpecifier(node.argumentExpression);
      if (propertyName !== undefined) {
        identifiers.push(propertyName);
      }
    } else if (ts.isImportSpecifier(node) && node.propertyName !== undefined) {
      if (ts.isStringLiteralLike(node.propertyName)) {
        identifiers.push(node.propertyName.text);
      }
    }
    ts.forEachChild(node, visit);
  };
  visit(sourceFile);
  return identifiers;
};

export const namespaceImportsFromTypeScript = ({
  fileName,
  source,
}: {
  readonly fileName: string;
  readonly source: string;
}): ReadonlyArray<{ readonly localName: string; readonly moduleSpecifier: string }> => {
  const sourceFile = createSourceFileForInspection(fileName, source);
  return sourceFile.statements.flatMap((statement) => {
    if (
      !ts.isImportDeclaration(statement) ||
      statement.importClause?.namedBindings === undefined ||
      !ts.isNamespaceImport(statement.importClause.namedBindings)
    ) {
      return [];
    }
    const moduleSpecifier = literalModuleSpecifier(statement.moduleSpecifier);
    return moduleSpecifier === undefined
      ? []
      : [
          {
            localName: statement.importClause.namedBindings.name.text,
            moduleSpecifier,
          },
        ];
  });
};

export const inspectPrivateWorkspaceLeaks = ({
  fileName,
  privateScope,
  source,
}: {
  readonly fileName: string;
  readonly privateScope: string;
  readonly source: string;
}): {
  readonly privateSpecifiers: ReadonlyArray<string>;
  readonly violations: ReadonlyArray<UnsupportedModuleLoad>;
} => {
  const inspection = inspectTypeScriptModule({ fileName, source });
  return {
    privateSpecifiers: inspection.moduleSpecifiers.filter(
      (specifier) => specifier === privateScope || specifier.startsWith(`${privateScope}/`),
    ),
    violations: inspection.violations,
  };
};

const propertyNameText = (name: ts.PropertyName): string | undefined => {
  if (ts.isIdentifier(name) || ts.isStringLiteralLike(name)) {
    return name.text;
  }
  return undefined;
};

const objectLiteralElementName = (
  element: ts.ObjectLiteralElementLike,
): ts.PropertyName | undefined => {
  if (
    ts.isPropertyAssignment(element) ||
    ts.isShorthandPropertyAssignment(element) ||
    ts.isMethodDeclaration(element) ||
    ts.isGetAccessorDeclaration(element) ||
    ts.isSetAccessorDeclaration(element)
  ) {
    return element.name;
  }
  return undefined;
};

const defaultConfigObject = (expression: ts.Expression): ts.ObjectLiteralExpression | undefined => {
  const unwrapped = unwrapExpression(expression);
  if (ts.isObjectLiteralExpression(unwrapped)) {
    return unwrapped;
  }
  if (
    !ts.isCallExpression(unwrapped) ||
    !isIdentifierNamed(unwrapped.expression, "defineConfig") ||
    unwrapped.arguments.length !== 1
  ) {
    return undefined;
  }
  const argument = unwrapExpression(unwrapped.arguments[0]);
  return ts.isObjectLiteralExpression(argument) ? argument : undefined;
};

export const inspectLibraryPack = ({
  fileName,
  source,
}: {
  readonly fileName: string;
  readonly source: string;
}): {
  readonly entrypoints: ReadonlyArray<string>;
  readonly violations: ReadonlyArray<LibraryPackViolation>;
} => {
  const sourceFile = createSourceFileForInspection(fileName, source);
  const entrypoints: Array<string> = [];
  const violations: Array<LibraryPackViolation> = [];

  const addViolation = (node: ts.Node, kind: LibraryPackViolation["kind"]): void => {
    const location = sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile));
    violations.push({
      column: location.character + 1,
      kind,
      line: location.line + 1,
    });
  };

  const defaultExports = sourceFile.statements.flatMap((statement) =>
    ts.isExportAssignment(statement) && !statement.isExportEquals ? [statement] : [],
  );
  const defaultExport = defaultExports[0];
  if (defaultExport === undefined) {
    addViolation(sourceFile, "missing-library-pack");
    return { entrypoints, violations };
  }
  for (const duplicateDefaultExport of defaultExports.slice(1)) {
    addViolation(duplicateDefaultExport, "non-literal-library-pack");
  }
  const configObject = defaultConfigObject(defaultExport.expression);
  if (configObject === undefined) {
    addViolation(defaultExport, "non-literal-library-pack");
    return { entrypoints, violations };
  }
  for (const property of configObject.properties) {
    const name = objectLiteralElementName(property);
    if (ts.isSpreadAssignment(property) || (name !== undefined && ts.isComputedPropertyName(name))) {
      addViolation(property, "ambiguous-library-pack-override");
    }
  }
  const packDeclarations = configObject.properties.filter((property) => {
    const name = objectLiteralElementName(property);
    return (
      name !== undefined &&
      !ts.isComputedPropertyName(name) &&
      propertyNameText(name) === "pack"
    );
  });
  const packDeclaration = packDeclarations[0];
  if (packDeclaration === undefined) {
    addViolation(configObject, "missing-library-pack");
    return { entrypoints, violations };
  }
  for (const duplicatePackDeclaration of packDeclarations.slice(1)) {
    addViolation(duplicatePackDeclaration, "duplicate-library-pack");
  }
  if (!ts.isPropertyAssignment(packDeclaration)) {
    addViolation(packDeclaration, "non-literal-library-pack");
    return { entrypoints, violations };
  }
  const initializer = unwrapExpression(packDeclaration.initializer);
  if (
    !ts.isCallExpression(initializer) ||
    !isIdentifierNamed(initializer.expression, "libraryPack") ||
    initializer.arguments.length !== 1
  ) {
    addViolation(packDeclaration, "non-literal-library-pack");
    return { entrypoints, violations };
  }
  const argument = initializer.arguments[0];
  const literal = literalModuleSpecifier(argument);
  if (literal !== undefined) {
    entrypoints.push(literal);
    return { entrypoints, violations };
  }
  const unwrappedArgument = unwrapExpression(argument);
  if (!ts.isArrayLiteralExpression(unwrappedArgument)) {
    addViolation(argument, "non-literal-library-pack");
    return { entrypoints, violations };
  }
  const literals = unwrappedArgument.elements.map(literalModuleSpecifier);
  if (literals.every((entrypoint) => entrypoint !== undefined)) {
    entrypoints.push(...literals);
  } else {
    addViolation(argument, "non-literal-library-pack");
  }
  return { entrypoints, violations };
};
