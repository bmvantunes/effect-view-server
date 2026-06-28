import { mountExampleApp, resolveExampleRuntimeConfig } from "./main";

mountExampleApp(document.getElementById("root"), resolveExampleRuntimeConfig(window));
