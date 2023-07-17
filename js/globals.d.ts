declare global {
  // Needed for tinygo's wasm_exec.js module
  // eslint-disable-next-line no-var, no-unused-vars
  var Go: { new (...args: any[]): any };
}

export {};
