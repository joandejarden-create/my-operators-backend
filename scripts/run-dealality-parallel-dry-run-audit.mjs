#!/usr/bin/env node
/**
 * Experiment #1: run two read-only audits in parallel (brand explorer gaps + one CALA TI audit).
 *
 *   node scripts/run-dealality-parallel-dry-run-audit.mjs
 *   node scripts/run-dealality-parallel-dry-run-audit.mjs --country "Dominican Republic" --market "Dominican Republic Countrywide"
 *
 * No Airtable writes. See docs/dealality-parallel-dry-run-experiment.md
 */
import { spawn } from "child_process";
import { fileURLToPath } from "url";
import path from "path";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const npmCmd = process.platform === "win32" ? "npm.cmd" : "npm";

function getArg(name, fallback) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : fallback;
}

function slugify(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

const country = getArg("--country", "Dominican Republic");
const market = getArg("--market", "Dominican Republic Countrywide");
const tiOutput = path.join(
  ROOT,
  "data",
  `${slugify(country)}-travel-infrastructure-audit.json`
);

function runNode(script, args, label) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [script, ...args], {
      cwd: ROOT,
      stdio: "inherit",
      env: process.env,
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) resolve({ label, code });
      else reject(new Error(`${label} exited with code ${code}`));
    });
  });
}

function runNpm(script, label) {
  return new Promise((resolve, reject) => {
    const child = spawn(npmCmd, ["run", script], {
      cwd: ROOT,
      stdio: "inherit",
      env: process.env,
      shell: process.platform === "win32",
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) resolve({ label, code });
      else reject(new Error(`${label} exited with code ${code}`));
    });
  });
}

console.log("[dealality] Parallel dry-run audit — read-only\n");
console.log("  A: audit-choice-explorer-presentation-gaps");
console.log(`  B: TI audit — ${country} / ${market}`);
console.log(`      → ${path.relative(ROOT, tiOutput)}\n`);

const tiScript = path.join(ROOT, "scripts", "audit-market-travel-infrastructure.mjs");
const tiArgs = [
  "--country",
  country,
  "--market",
  market,
  "--output",
  path.relative(ROOT, tiOutput).replace(/\\/g, "/"),
];

const started = Date.now();

try {
  const results = await Promise.all([
    runNpm("audit-choice-explorer-presentation-gaps", "Brand explorer gap audit"),
    runNode(tiScript, tiArgs, "Travel infrastructure audit"),
  ]);
  const elapsed = ((Date.now() - started) / 1000).toFixed(1);
  console.log(`\n[dealality] Both audits passed in ${elapsed}s`);
  for (const r of results) console.log(`  ✓ ${r.label}`);
  console.log("\nNext: review docs/choice-explorer-presentation-gap-audit.md and", tiOutput);
  console.log("See docs/dealality-parallel-dry-run-experiment.md for PR checklist.");
} catch (err) {
  console.error("\n[dealality] Parallel audit failed:", err.message || err);
  process.exit(1);
}
