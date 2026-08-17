#!/usr/bin/env node
/**
 * AUDIT-ONLY wrapper — Comparable Relevance Index simulation.
 * Delegates to operator-fit-differentiation-audit.mjs (writes full audit JSON).
 * Does not modify production scoring.
 *
 *   node scripts/operator-fit-comparable-relevance-simulation.mjs
 */
import { spawnSync } from "child_process";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { readFileSync, writeFileSync, existsSync } from "fs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const auditScript = join(root, "scripts/operator-fit-differentiation-audit.mjs");
const r = spawnSync(process.execPath, [auditScript], { cwd: root, encoding: "utf8", env: process.env });
if (r.stdout) process.stdout.write(r.stdout);
if (r.stderr) process.stderr.write(r.stderr);
if (r.status) process.exit(r.status);

const auditPath = join(root, "reports/operator-fit-differentiation-audit.json");
if (!existsSync(auditPath)) {
  console.error("Missing audit JSON");
  process.exit(1);
}
const audit = JSON.parse(readFileSync(auditPath, "utf8"));
const sims = audit.dealC?.comparableSims || {};
const md = [
  "# Operator Fit — Comparable Relevance Simulation (Audit Only)",
  "",
  `Generated: ${audit.generatedAt}`,
  "",
  "Not production. Does not change engine weights.",
  "",
  "## Variant summary (Deal C Santa Fe vs Highgate)",
  "",
  "| Variant | SF Displayed (sim) | HG Displayed (sim) | Diff | Directionally defensible? |",
  "| ------- | -----------------: | -----------------: | ---: | ------------------------- |",
];
for (const [id, v] of Object.entries(sims)) {
  md.push(
    `| ${v.label || id} | ${v.santaFe?.displayed} | ${v.highgate?.displayed} | ${v.differenceDisplayed} | See notes |`
  );
}
md.push("", "## Detail", "", "```json", JSON.stringify(sims, null, 2), "```", "");
writeFileSync(join(root, "reports/operator-fit-comparable-relevance-simulation.md"), md.join("\n"));
console.log("Wrote reports/operator-fit-comparable-relevance-simulation.md");
