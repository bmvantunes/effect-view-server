export const importPackageExportModule = (moduleUrl: URL): Promise<object> =>
  import(moduleUrl.href).then((module) => Object.fromEntries(Object.entries(module)));
