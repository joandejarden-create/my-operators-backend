#!/usr/bin/env node
/**
 * Full-table legacy Hotel Census → HPC reconciliation (read-only).
 *
 * Requires: DEALALITY_LEGACY_HOTEL_CENSUS_MIGRATION_READ=1
 * Usage:
 *   node scripts/reconcile-legacy-census-to-hpc-full-v1.mjs --max-records=50
 *   node scripts/reconcile-legacy-census-to-hpc-full-v1.mjs
 *
 * Never writes Airtable. Paid research is NOT invoked.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import {
  fetchAllLegacyHotelCensusRecords,
  classifyLegacyCensusRecord,
  markDuplicateLegacyMatches,
  summarizeReconciliation,
  DEALALITY_LEGACY_HPC_RECONCILIATION_V1,
  RECON_DISPOSITION,
} from "../lib/hotel-census/dealality-legacy-hpc-reconciliation-v1.js";
import { migrationReadAllowed } from "../lib/hotel-intelligence/hpc-population/legacy-hotel-census-access-guard-v1.js";

const require = createRequire(import.meta.url);
try {
  require("dotenv").config({ path: "C:/Dev/dealality/my-operators-backend/.env" });
} catch {
  /* optional */
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const outDir = path.join(root, "reports/hotel-explorer/global-census-retirement");
fs.mkdirSync(outDir, { recursive: true });

function argValue(name) {
  const hit = process.argv.find((a) => a.startsWith(`${name}=`));
  return hit ? hit.slice(name.length + 1) : null;
}

const maxRecordsRaw = argValue("--max-records");
const maxRecords = maxRecordsRaw != null ? Number(maxRecordsRaw) : null;
const concurrency = Math.max(1, Math.min(8, Number(argValue("--concurrency") || 4)));

if (!migrationReadAllowed({ allowLegacyHotelCensusRead: true })) {
  console.error(
    "Set DEALALITY_LEGACY_HOTEL_CENSUS_MIGRATION_READ=1 for full-table legacy reconciliation."
  );
  process.exit(2);
}

process.env.DEALALITY_LEGACY_HOTEL_CENSUS_MIGRATION_READ = "1";

console.log(
  `=== Full legacy→HPC reconciliation (max=${maxRecords ?? "ALL"}, concurrency=${concurrency}) ===`
);

const legacyRecords = await fetchAllLegacyHotelCensusRecords({
  // Do not coerce null → Number(null)===0 (that truncates after 1 record).
  maxRecords: maxRecords == null || !Number.isFinite(maxRecords) ? null : maxRecords,
  workflow: "reconcile-legacy-census-to-hpc-full-v1",
  onPage: ({ fetched }) => {
    if (fetched % 200 === 0) console.log(`  fetched ${fetched} legacy records…`);
  },
});

console.log(`Legacy records loaded: ${legacyRecords.length}`);

const classified = [];
for (let i = 0; i < legacyRecords.length; i += concurrency) {
  const batch = legacyRecords.slice(i, i + concurrency);
  const part = await Promise.all(batch.map((rec) => classifyLegacyCensusRecord(rec)));
  classified.push(...part);
  if ((i + batch.length) % 50 === 0 || i + batch.length >= legacyRecords.length) {
    console.log(`  classified ${classified.length}/${legacyRecords.length}`);
  }
}

const rows = markDuplicateLegacyMatches(classified);
const summary = summarizeReconciliation(rows);

// Known HE 5.0 pilot overlays (Papagayo now has HPC id)
const overlays = {
  recyX1wLsHBJYRPw7: "recFQ0WvGLbfK5Gpa",
};
for (const row of rows) {
  if (overlays[row.legacyId] && !row.canonicalHpcId) {
    row.canonicalHpcId = overlays[row.legacyId];
    row.disposition = RECON_DISPOSITION.MATCHED_HPC_VERIFIED;
    row.reason = "he50_pilot_hpc_create_overlay";
  }
}

const report = {
  contract: DEALALITY_LEGACY_HPC_RECONCILIATION_V1,
  generatedAt: new Date().toISOString(),
  applyAuthorized: false,
  paidResearchUsed: false,
  maxRecords: maxRecords ?? null,
  summary: summarizeReconciliation(rows),
  rows,
  legacyToHpcMap: Object.fromEntries(
    rows.filter((r) => r.canonicalHpcId).map((r) => [r.legacyId, r.canonicalHpcId])
  ),
};

fs.writeFileSync(
  path.join(outDir, "LEGACY-TO-HPC-RECONCILIATION-MATRIX-FULL-V1.json"),
  JSON.stringify(report, null, 2)
);

const md = [];
md.push("# Legacy → HPC full reconciliation matrix v1");
md.push("");
md.push(`Generated: ${report.generatedAt}`);
md.push("**applyAuthorized: false** — no Airtable writes.");
md.push("");
md.push("## Summary");
md.push("");
md.push("| Metric | Count |");
md.push("| --- | ---: |");
md.push(`| Legacy records classified | ${summary.total} |`);
md.push(`| Matched (verified + alias) | ${summary.matched} |`);
md.push(`| HPC CREATE candidates | ${summary.createCandidates} |`);
md.push(`| Ambiguous | ${summary.ambiguous} |`);
md.push(`| Unresolved / evidence | ${summary.unresolved} |`);
md.push("");
md.push("## By disposition");
md.push("");
md.push("| Disposition | Count |");
md.push("| --- | ---: |");
for (const [k, v] of Object.entries(summary.byDisposition).sort((a, b) => b[1] - a[1])) {
  md.push(`| \`${k}\` | ${v} |`);
}
md.push("");
md.push("Every legacy record in this run received a disposition. Quarantine ambiguous / evidence rows and continue.");
md.push("");
md.push("See JSON sibling for record-level matrix + `legacyToHpcMap`.");

fs.writeFileSync(path.join(outDir, "LEGACY-TO-HPC-RECONCILIATION-MATRIX-FULL-V1.md"), md.join("\n"));

console.log(JSON.stringify(report.summary, null, 2));
console.log("Wrote LEGACY-TO-HPC-RECONCILIATION-MATRIX-FULL-V1.*");
