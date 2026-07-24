import type { build } from "vite";

type ViteBuildResult = Awaited<ReturnType<typeof build>>;

export const browserBuildChunks = (result: ViteBuildResult) => {
  const outputs = Array.isArray(result) ? result : [result];
  const chunks = outputs.flatMap((output) =>
    "output" in output ? output.output.filter((entry) => entry.type === "chunk") : [],
  );
  if (chunks.length === 0) {
    throw new Error("The Source Adapter browser fixture emitted no JavaScript chunk.");
  }
  return chunks;
};
