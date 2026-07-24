import { describe, expect, it } from "@effect/vitest";
import { browserBuildChunks } from "./browser-build-output";

const chunk = {
  type: "chunk",
  code: "export {};",
  modules: {},
};

describe("browser build output normalization", () => {
  it("collects chunks from one or many build outputs and ignores assets", () => {
    expect(
      Reflect.apply(browserBuildChunks, undefined, [
        {
          output: [
            chunk,
            {
              type: "asset",
              fileName: "style.css",
              source: "",
              names: [],
              originalFileNames: [],
            },
          ],
        },
      ]),
    ).toStrictEqual([chunk]);
    expect(
      Reflect.apply(browserBuildChunks, undefined, [
        [
          {
            output: [chunk],
          },
          {
            output: [chunk],
          },
        ],
      ]),
    ).toStrictEqual([chunk, chunk]);
  });

  it("rejects watcher and asset-only results", () => {
    expect(() => Reflect.apply(browserBuildChunks, undefined, [{}])).toThrow(
      "emitted no JavaScript chunk",
    );
    expect(() =>
      Reflect.apply(browserBuildChunks, undefined, [
        {
          output: [
            {
              type: "asset",
              fileName: "style.css",
              source: "",
              names: [],
              originalFileNames: [],
            },
          ],
        },
      ]),
    ).toThrow("emitted no JavaScript chunk");
  });
});
