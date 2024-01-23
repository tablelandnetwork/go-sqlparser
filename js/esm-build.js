import * as esbuild from "esbuild"
import watPlugin from "esbuild-plugin-wat";

// esm
await esbuild.build({
  platform: "neutral",
  entryPoints: ["main.js"],
  bundle: true,
  outfile: "esm/out.js",
  plugins: [watPlugin()],
});
