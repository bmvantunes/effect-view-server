import { source as contractSource } from "../contract.js";

contractSource({ stream: /[// @ts-expect-error]/ });
contractSource({ stream: /[/* @ts-expect-error */]/ });
