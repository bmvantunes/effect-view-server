import { defineConfig } from "vite-plus";

export default defineConfig({
  staged: {
    "*": "vp check --fix",
  },
  fmt: {
    ignorePatterns: [".repos/**"],
  },
  lint: {
    ignorePatterns: [".repos/**"],
    options: { typeAware: true, typeCheck: true },
  },
  run: {
    cache: true,
  },
});
