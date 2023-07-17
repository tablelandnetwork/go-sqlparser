import * as esbuild from "esbuild"
import watPlugin from "esbuild-plugin-wat";

// cjs
await esbuild.build({
  platform: "node",
  entryPoints: ["main.js"],
  bundle: true,
  outfile: "cjs/out.js",
  plugins: [watPlugin()],
});
