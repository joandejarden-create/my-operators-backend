/**
 * Audit Company Profile brand links — verified sources only (no writes).
 *
 *   node scripts/audit-company-profile-brands.mjs
 *   node scripts/audit-company-profile-brands.mjs --json=reports/company-profile-brands-audit.json
 */
import "../load-env.js";
import { writeFileSync } from "fs";
import {
  BRAND_BACKFILL_TABLES,
  buildBrandBasicsIndex,
  buildCompanyProfileBrandsBackfillPlan,
  indexOperatorRowsByCompanyName,
} from "../lib/company-profile-brands-backfill.js";

const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;

const jsonOut = (() => {
  const idx = process.argv.indexOf("--json");
  return idx >= 0 ? process.argv[idx + 1] : "";
})();

async function fetchAll(tableIdOrName) {
  const records = [];
  let offset;
  for (;;) {
    const qs = new URLSearchParams({ pageSize: "100" });
    if (offset) qs.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableIdOrName)}?${qs}`,
      { headers: { Authorization: `Bearer ${apiKey}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json));
    records.push(...(json.records || []));
    offset = json.offset;
    if (!offset) break;
  }
  return records;
}

if (!baseId || !apiKey) {
  console.error("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY");
  process.exit(1);
}

const [companies, brandBasics, operatorBasics, operatorProfiles] = await Promise.all([
  fetchAll(BRAND_BACKFILL_TABLES.companyProfile),
  fetchAll(BRAND_BACKFILL_TABLES.brandBasics),
  fetchAll(BRAND_BACKFILL_TABLES.operatorBasics).catch(() => []),
  fetchAll(BRAND_BACKFILL_TABLES.operatorProfile).catch(() => []),
]);

const brandIndex = buildBrandBasicsIndex(brandBasics);
const ctx = {
  ...brandIndex,
  operatorBasicsByCompanyKey: indexOperatorRowsByCompanyName(operatorBasics),
  operatorProfileByCompanyKey: indexOperatorRowsByCompanyName(operatorProfiles),
};

const plans = companies.map((rec) => buildCompanyProfileBrandsBackfillPlan(rec, ctx));

const withBrands = plans.filter((p) => p.mergedIds.length > 0);
const needsWrite = plans.filter((p) => p.hasChange);
const emptyAfter = plans.filter((p) => p.mergedIds.length === 0);

console.log("Company Profile — Brands You Operate / Support audit");
console.log(`Companies: ${companies.length}`);
console.log(`Brand Basics catalog: ${brandBasics.length}`);
console.log(`With verified brands (after merge): ${withBrands.length}`);
console.log(`Would update link field: ${needsWrite.length}`);
console.log(`Still empty (no verified source): ${emptyAfter.length}\n`);

for (const p of needsWrite.sort((a, b) => a.companyName.localeCompare(b.companyName))) {
  console.log(`${p.companyName} (${p.companyId})`);
  console.log(`  was: ${p.existingIds.length} link(s)`);
  console.log(`  now: ${p.mergedIds.length} — ${p.brandLabels.slice(0, 8).join(", ")}${p.brandLabels.length > 8 ? "…" : ""}`);
  for (const s of p.sources) {
    console.log(`  + ${s.source}${s.count != null ? ` (${s.count})` : ""}`);
    if (s.unresolved?.length) console.log(`    unresolved lookup names: ${s.unresolved.join(", ")}`);
    if (s.ambiguous?.length) {
      console.log(
        `    ambiguous (skipped): ${s.ambiguous.map((a) => a.name).join(", ")}`
      );
    }
  }
  console.log("");
}

if (emptyAfter.length) {
  console.log("--- No verified brands (manual research needed) ---");
  for (const p of emptyAfter.sort((a, b) => a.companyName.localeCompare(b.companyName))) {
    console.log(`  ${p.companyName} (${p.companyId}) — type: ${p.companyType || "—"}`);
  }
}

const report = {
  generatedAt: new Date().toISOString(),
  totals: {
    companies: companies.length,
    withVerifiedBrands: withBrands.length,
    needsWrite: needsWrite.length,
    stillEmpty: emptyAfter.length,
  },
  updates: needsWrite,
  stillEmpty: emptyAfter.map((p) => ({
    companyId: p.companyId,
    companyName: p.companyName,
    companyType: p.companyType,
  })),
};

if (jsonOut) {
  writeFileSync(jsonOut, JSON.stringify(report, null, 2), "utf8");
  console.log(`\nWrote ${jsonOut}`);
}
