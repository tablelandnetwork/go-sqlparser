// Need to optionally shim `crypto.getRandomValues` and esbuild needs the
// import to come before importing `wasm_exec.js`
import "./polyfills/crypto.js";

// @ts-check
/* global Go */
import "./wasm_exec.js";
import mainWasm from "./main.wasm";

// @ts-ignore
const go = new Go();
// Bit of a hack for this: https://github.com/tinygo-org/tinygo/issues/1140
go.importObject.env["syscall/js.finalizeRef"] = () => {};

/** @type {WebAssembly.Exports | undefined} */
let wasm;

const cachedTextDecoder = new TextDecoder("utf-8", {
  ignoreBOM: true,
  fatal: true,
});

cachedTextDecoder.decode();

/**
 * @param {WebAssembly.Imports} imports
 * @returns {Promise<any>}
 */
async function load(imports) {
  const { instance } = await WebAssembly.instantiate(mainWasm, imports);
  return instance;
}

/**
 * @returns {WebAssembly.Imports}
 */
function getImports() {
  return go.importObject;
}

/**
 * @param {WebAssembly.Imports} imports
 * @param {WebAssembly.Memory} [maybeMemory]
 */
function initMemory(imports, maybeMemory) {}

/**
 * @param {WebAssembly.Instance} instance
 * @returns {WebAssembly.Exports}
 */
function finalizeInit(instance) {
  wasm = instance.exports;
  // cachedUint8Memory0 = new Uint8Array(wasm.memory.buffer);

  return wasm;
}

/** @typedef {Promise<T> | T} PromiseOrValue<T> @template T */

/**
 * @typedef {{ __wbindgen_wasm_module?: WebAssembly.Module, (
 * input?: PromiseOrValue<string | Response | URL | BufferSource>
 * ): Promise<WebAssembly.Exports> }} InitFunction
 */

/**
 * @type {InitFunction}
 * @returns {Promise<WebAssembly.Exports>}
 */
const init = async () => {
  const imports = getImports();

  initMemory(imports);

  const instance = await load(imports);

  go.run(instance);

  return finalizeInit(instance);
};

export { wasm as __wasm, init };
export default init;

