import "vitest";

declare module "vitest" {
  interface ProvidedContext {
    readonly viewServerRemoteUrl: string;
    readonly viewServerSourceRemoteUrl: string;
    readonly viewServerDiagnosticRemoteUrl: string;
  }
}
