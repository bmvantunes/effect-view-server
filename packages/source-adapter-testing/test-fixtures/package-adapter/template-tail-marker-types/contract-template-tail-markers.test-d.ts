import { source as contractSource } from "../contract.js";

contractSource({ stream: `x${1}// @ts-expect-error` });
contractSource({ stream: `x${1}/* @ts-expect-error */` });
