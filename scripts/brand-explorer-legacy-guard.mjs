#!/usr/bin/env node
import { spawnSync } from "child_process";
import { resolve } from "path";
import { legacyApplyRequested } from "../lib/brand-explorer/canonical/legacy-apply-guard.js";

const target = process.argv[2];
const args = process.argv.slice(3);
if (!target) {
  console.error("Usage: node scripts/brand-explorer-legacy-guard.mjs <script> [args]");
  process.exit(2);
}
if (legacyApplyRequested(["node", target, ...args])) {
  if (process.env.BRAND_EXPLORER_ALLOW_LEGACY_APPLY !== "1") {
    console.error(`[brand-explorer] Legacy apply blocked for ${target}.`);
    console.error("Use the canonical pipeline. Break-glass: BRAND_EXPLORER_ALLOW_LEGACY_APPLY=1");
    process.exit(2);
  }
  console.error(`[brand-explorer] legacy apply break-glass enabled for ${target}`);
}
const res = spawnSync(process.execPath, [resolve(target), ...args], { stdio: "inherit" });
process.exit(res.status == null ? 1 : res.status);
