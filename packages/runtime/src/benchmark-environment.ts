export const booleanFromBenchmarkEnvironment = (
  raw: string | undefined,
  name: string,
  fallback: boolean,
): boolean => {
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const trimmed = raw.trim();
  if (trimmed === "true") {
    return true;
  }
  if (trimmed === "false") {
    return false;
  }
  throw new Error(`${name} must be true or false.`);
};
