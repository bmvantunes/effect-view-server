import ts from "typescript-compiler-api";
import { publishedFileViolations } from "./release-publish-policy.mjs";

const sourceMapReferenceDirective =
  /^(?:\/\/[#@][\t ]*sourceMappingURL=[^\r\n]*|\/\*[#@][\t ]*sourceMappingURL=[^\r\n]*\*\/)$/;

const sourceMapReferenceRanges = (contents) => {
  const sourceFile = ts.createSourceFile(
    "published.ts",
    contents,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TS,
  );
  const protectedRanges = [];
  const visit = (node) => {
    if (
      node.kind === ts.SyntaxKind.StringLiteral ||
      node.kind === ts.SyntaxKind.NoSubstitutionTemplateLiteral ||
      node.kind === ts.SyntaxKind.TemplateHead ||
      node.kind === ts.SyntaxKind.TemplateMiddle ||
      node.kind === ts.SyntaxKind.TemplateTail ||
      node.kind === ts.SyntaxKind.RegularExpressionLiteral
    ) {
      protectedRanges.push({ pos: node.getStart(sourceFile), end: node.getEnd() });
    }
    ts.forEachChild(node, visit);
  };
  visit(sourceFile);
  protectedRanges.sort((left, right) => left.pos - right.pos);

  const scanner = ts.createScanner(
    ts.ScriptTarget.Latest,
    false,
    ts.LanguageVariant.Standard,
    contents,
  );
  const ranges = [];
  let protectedIndex = 0;
  for (let token = scanner.scan(); token !== ts.SyntaxKind.EndOfFileToken; token = scanner.scan()) {
    const tokenStart = scanner.getTokenPos();
    const tokenEnd = scanner.getTextPos();
    while (protectedRanges[protectedIndex]?.end <= tokenStart) {
      protectedIndex += 1;
    }
    const protectedRange = protectedRanges[protectedIndex];
    if (
      (token === ts.SyntaxKind.SingleLineCommentTrivia ||
        token === ts.SyntaxKind.MultiLineCommentTrivia) &&
      !(protectedRange?.pos <= tokenStart && tokenStart < protectedRange.end) &&
      sourceMapReferenceDirective.test(scanner.getTokenText())
    ) {
      ranges.push({ pos: tokenStart, end: tokenEnd });
    }
  }
  return ranges;
};

export const stripSourceMapReference = (contents) => {
  let sanitized = contents;
  for (const range of sourceMapReferenceRanges(contents).toReversed()) {
    const lineStart = contents.lastIndexOf("\n", range.pos - 1) + 1;
    const ownsLine = contents.slice(lineStart, range.pos).trim().length === 0;
    const start = ownsLine ? lineStart : range.pos;
    let end = range.end;
    if (ownsLine && contents.startsWith("\r\n", end)) {
      end += 2;
    } else if (ownsLine && contents[end] === "\n") {
      end += 1;
    }
    const replacement =
      !ownsLine && contents.startsWith("/*", range.pos) && /\S/.test(contents[end] ?? "")
        ? " "
        : "";
    sanitized = `${sanitized.slice(0, start)}${replacement}${sanitized.slice(end)}`;
  }
  return sanitized;
};

export const validatedPublishedFileViolations = (files) =>
  files.flatMap((file) => [
    ...publishedFileViolations([file]),
    ...(sourceMapReferenceRanges(file.contents).length > 0
      ? [`${file.relativePath} references a source map`]
      : []),
  ]);
