#!/usr/bin/env node
/**
 * Alias — exports enrichment queue (invokes data-readiness).
 *   node scripts/operator-fit-enrichment-queue.mjs
 */
import { spawnSync } from "child_process";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const r = spawnSync(process.execPath, [join(__dirname, "operator-fit-data-readiness.mjs")], {
  stdio: "inherit",
  env: process.env,
});
process.exit(r.status || 0);
