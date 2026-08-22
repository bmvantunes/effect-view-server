import type { PackUserConfig } from "vite-plus/pack";

type LibraryPackEntry = string | Array<string>;

type LibraryPackOptions = Pick<PackUserConfig, "alias" | "deps" | "shims" | "tsconfig">;

export const libraryPack = (entry: LibraryPackEntry, options: LibraryPackOptions = {}) => ({
  ...options,
  entry,
  dts: true,
  fixedExtension: false,
  exports: false,
});
