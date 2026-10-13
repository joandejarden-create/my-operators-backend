#!/usr/bin/env node
/**
 * Smoke tests — GDI Success-Source Forensics V1 helpers (inline re-exports via running patterns).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/group-demand-intelligence/success-source-forensics-v1");

assert.ok(fs.existsSync(path.join(OUT, "FOUNDER_REPORT.md")), "founder report exists");
assert.ok(fs.existsSync(path.join(OUT, "LINEAGE_ROWS.json")), "lineage rows exist");

const summary = JSON.parse(fs.readFileSync(path.join(OUT, "RUN_SUMMARY.json"), "utf8"));
assert.ok(summary.counts?.total >= 50, "audited ready count roughly Bethesda+Renaissance+Waterstone");
assert.ok(
  [
    "WEBHOUND WAS MATERIAL — RUN BOUNDED CANARY BEFORE SOURCE EXPANSION",
    "WEBHOUND HELPED BUT WAS NOT REQUIRED — TEST SOURCE FAMILIES PROVIDER-AGNOSTICALLY",
    "WEBHOUND WAS NOT MATERIAL — CONTINUE SOURCE-EXPANSION V2 WITHOUT DEPENDENCY",
    "HISTORICAL LINEAGE INCOMPLETE — CANNOT ATTRIBUTE SUCCESS RELIABLY",
  ].includes(summary.finalVerdict),
  "verdict is one of allowed finals"
);

const rows = JSON.parse(fs.readFileSync(path.join(OUT, "LINEAGE_ROWS.json"), "utf8"));
const roles = new Set(rows.map((r) => r.webhoundRole));
for (const r of roles) {
  assert.ok(
    [
      "PRIMARY_DISCOVERY",
      "MATERIAL_SUPPORT",
      "ENRICHMENT_ONLY",
      "ROUTING_ONLY",
      "NO_MATERIAL_ROLE",
      "UNKNOWN",
    ].includes(r),
    `valid role ${r}`
  );
}

// Contamination guard documented: evidence.researchProvider alone must not force PRIMARY
const report = fs.readFileSync(path.join(OUT, "FOUNDER_REPORT.md"), "utf8");
assert.match(report, /CURRENT GDI REQUIRES WEBHOUND:\*\* \*\*NO\*\*/);
assert.match(report, /OPTION B/);

console.log("test-gdi-success-source-forensics-v1: PASS");
