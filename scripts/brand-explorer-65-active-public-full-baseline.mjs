#!/usr/bin/env node
/**
 * Freeze protected 65 Active/Live baseline (runs post-release validate + freeze).
 */
import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const script = path.join(__dirname, "brand-explorer-wave16a-low-risk-post-release-validate.mjs");
const child = spawn(process.execPath, [script, "--freeze-baseline-65", ...process.argv.slice(2)], {
  stdio: "inherit",
  cwd: path.resolve(__dirname, ".."),
});
child.on("exit", (code) => process.exit(code || 0));
