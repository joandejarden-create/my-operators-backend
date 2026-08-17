#!/usr/bin/env node
/**
 * Audit demand anchor names + coordinates vs Google Maps (or OSM fallback).
 *
 *   node scripts/audit-demand-anchors-against-maps.mjs --country "Dominican Republic"
 *   node scripts/audit-demand-anchors-against-maps.mjs --country "Dominican Republic" --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { DEMAND_ANCHORS_FIELDS as F } from "../lib/demand-anchors/airtable-demand-anchors-fields.js";
import { getDemandAnchorsAirtableConfig, resolveDemandAnchorsTableName } from "../lib/demand-anchors/demand-anchors-base.js";
import { fetchDemandAnchorRecords } from "../lib/demand-anchors/airtable-demand-anchors-io.js";
import {
  auditDemandAnchorAgainstMaps,
  buildAutoApplyPatch,
  mapsAuditProviderLabel,
  shouldAutoApply,
} from "../lib/demand-anchors/google-maps-place-audit.js";
import { todayIsoDate } from "../lib/demand-anchors/coordinate-verification.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const APPLY = process.argv.includes("--apply");
const countryArg = process.argv.find((a, i) => process.argv[i - 1] === "--country");
const COUNTRY = countryArg || "Dominican Republic";
const DELAY_MS = Number(process.env.MAPS_AUDIT_DELAY_MS) || (process.env.GOOGLE_MAPS_API_KEY ? 200 : 2000);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

console.log(APPLY ? "=== APPLY ===" : "=== AUDIT ===");
console.log("Country:", COUNTRY);
console.log("Provider:", mapsAuditProviderLabel());

const { allPoints, error } = await fetchDemandAnchorRecords({
  country: COUNTRY,
  includeHidden: true,
});
if (error) {
  console.error(error);
  process.exit(1);
}

const points = (allPoints || []).sort((a, b) => a.name.localeCompare(b.name));
const results = [];

for (let i = 0; i < points.length; i++) {
  const p = points[i];
  process.stdout.write(`[${i + 1}/${points.length}] ${p.name.slice(0, 50)}...\r`);
  const audit = await auditDemandAnchorAgainstMaps(p);
  results.push(audit);
  await sleep(DELAY_MS);
}
console.log("");

const flagged = results.filter((r) => r.status !== "ok");
const autoApply = results.filter(shouldAutoApply);

console.log("\nTotal:", results.length);
console.log("OK:", results.filter((r) => r.status === "ok").length);
console.log("Flagged:", flagged.length);
console.log("Auto-apply candidates:", autoApply.length);

if (flagged.length) {
  console.log("\n--- Flagged ---");
  for (const r of flagged.sort((a, b) => (b.driftKm || 0) - (a.driftKm || 0))) {
    console.log(`\n${r.name}`);
    console.log(`  status: ${r.status} | action: ${r.action} | drift: ${r.driftKm ?? "?"} km | nameSim: ${r.nameSimilarity ?? "?"}`);
    if (r.referenceName) console.log(`  maps name: ${r.referenceName}`);
    if (r.referenceLat) console.log(`  maps coords: ${r.referenceLat}, ${r.referenceLng}`);
    if (r.suggestedName) console.log(`  suggested rename: ${r.suggestedName}`);
  }
}

const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
const reportDir = join(__dirname, "..", "reports");
mkdirSync(reportDir, { recursive: true });
const reportPath = join(
  reportDir,
  `dr-demand-anchors-maps-audit-${stamp}.json`
);
writeFileSync(
  reportPath,
  JSON.stringify({ country: COUNTRY, provider: mapsAuditProviderLabel(), results }, null, 2)
);
console.log("\nReport:", reportPath);

if (APPLY && autoApply.length) {
  const cfg = getDemandAnchorsAirtableConfig();
  const table = await resolveDemandAnchorsTableName(cfg.baseId, cfg.apiKey);
  let updated = 0;
  let failed = 0;
  for (const audit of autoApply) {
    const patch = buildAutoApplyPatch(audit, F);
    if (!Object.keys(patch).length) continue;
    patch[F.lastVerified] = todayIsoDate();
    patch[F.includeOnRadarMap] = true;
    try {
      await cfg.base(table).update(audit.id, patch, { typecast: true });
      updated += 1;
      console.log("Applied:", audit.name, "→", Object.keys(patch).join(", "));
    } catch (err) {
      failed += 1;
      console.error("FAIL", audit.name, err?.message || err);
    }
    await sleep(Number(process.env.AIRTABLE_WRITE_DELAY_MS) || 220);
  }
  console.log("\nApply: updated=", updated, "failed=", failed);
} else if (APPLY) {
  console.log("\nNo auto-apply candidates.");
} else {
  console.log("\nRe-run with --apply to auto-fix location drift ≥0.5 km (review flagged names manually).");
}

process.exit(flagged.length && !APPLY ? 1 : 0);
