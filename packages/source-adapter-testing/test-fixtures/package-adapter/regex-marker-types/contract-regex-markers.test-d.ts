import { source as contractSource } from "../contract.js";

contractSource({ stream: /[// @ts-expect-error]/ as unknown as string });
contractSource({ stream: /[/* @ts-expect-error */]/ as unknown as string });
