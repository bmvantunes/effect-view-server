import ts from "typescript-compiler-api";
import { publishedFileViolations } from "./release-publish-policy.mjs";

const sourceMapReferenceDirective =
  /^(?:\/\/[#@][\t ]*sourceMappingURL=[^\r\n]*|\/\*[#@][\t ]*sourceMappingURL=[^\r\n]*\*\/)$/;

const sourceMapReferenceRanges = (contents) => {
  const ranges = new Map();
  const sourceFile = ts.createSourceFile(
    "published.ts",
    contents,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TS,
  );
  const addRanges = (candidates) => {
    for (const candidate of candidates ?? []) {
      if (sourceMapReferenceDirective.test(contents.slice(candidate.pos, candidate.end))) {
        ranges.set(`${candidate.pos}:${candidate.end}`, candidate);
      }
    }
  };
  const visit = (node) => {
    addRanges(ts.getLeadingCommentRanges(contents, node.pos));
    addRanges(ts.getTrailingCommentRanges(contents, node.end));
    ts.forEachChild(node, visit);
  };
  visit(sourceFile);
  return [...ranges.values()].sort((left, right) => left.pos - right.pos);
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
    sanitized = `${sanitized.slice(0, start)}${sanitized.slice(end)}`;
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
