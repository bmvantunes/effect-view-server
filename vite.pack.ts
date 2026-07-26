type LibraryPackEntry = string | Array<string>;

type LibraryPackOptions = {
  readonly alias?: Readonly<Record<string, string>>;
  readonly tsconfig?: string;
};

export const libraryPack = (entry: LibraryPackEntry, options: LibraryPackOptions = {}) => ({
  ...options,
  entry,
  dts: true,
  fixedExtension: false,
  exports: false,
});
