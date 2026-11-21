#!/usr/bin/env node
/**
 * Hotel Intelligence Factory CLI — Node execution engine entry.
 *
 *   node scripts/hotel-intelligence-factory-run.mjs --mode=Research --dry-run
 *   node scripts/hotel-intelligence-factory-run.mjs --mode=CanonicalPromotion --dry-run
 *
 * PowerShell must call this script; do not reimplement stages in PS.
 */
import path from "node:path";
import { fileURLToPath } from "node:url";
import { runHotelIntelligenceFactory } from "../lib/hotel-intelligence/hotel-intelligence-factory/engine-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function arg(name, fallback = null) {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  if (!hit) return fallback;
  return hit.slice(name.length + 3);
}
function flag(name) {
  return process.argv.includes(`--${name}`);
}

const mode = arg("mode", "Research");
const result = await runHotelIntelligenceFactory({
  cwd: ROOT,
  mode,
  configPath: arg("config") || path.join(ROOT, "configs/hotel-intelligence-factory/default.v4.json"),
  runId: arg("run-id"),
  stages: arg("stages"),
  dryRun: !flag("apply-research") && !flag("apply-promotion"),
  applyResearch: flag("apply-research"),
  applyPromotion: flag("apply-promotion"),
  manifestPath: arg("manifest"),
  resume: flag("resume"),
});

console.log(JSON.stringify(result, null, 2));
if (!result.ok) process.exitCode = Number(result.code) || 2;
