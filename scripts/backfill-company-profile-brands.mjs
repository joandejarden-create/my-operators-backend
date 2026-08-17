/**
 * Backfill Company Profile "Brands You Operate / Support" from verified Airtable sources only.
 *
 *   node scripts/backfill-company-profile-brands.mjs
 *   node scripts/backfill-company-profile-brands.mjs --apply
 *   node scripts/backfill-company-profile-brands.mjs --apply --id recXXXXXXXX
 */
import "../load-env.js";
import { writeFileSync } from "fs";
import { join } from "path";
import {
  BRAND_BACKFILL_TABLES,
  MAP_CP_BRANDS_AIRTABLE,
  buildBrandBasicsIndex,
  buildCompanyProfileBrandsBackfillPlan,
  indexOperatorRowsByCompanyName,
} from "../lib/company-profile-brands-backfill.js";

const TABLE = BRAND_BACKFILL_TABLES.companyProfile;
const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const apply = process.argv.includes("--apply");
const onlyId = (() => {
  const idx = process.argv.indexOf("--id");
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

async function patchRecord(id, fields) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TABLE)}/${id}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields, typecast: false }),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  return json;
}

if (!baseId || !apiKey) {
  console.error("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY");
  process.exit(1);
}

const [companies, brandBasics, operatorBasics, operatorProfiles] = await Promise.all([
  fetchAll(TABLE),
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

const planned = [];
for (const rec of companies) {
  if (onlyId && rec.id !== onlyId) continue;
  const plan = buildCompanyProfileBrandsBackfillPlan(rec, ctx);
  if (!plan.hasChange) continue;
  planned.push(plan);
}

console.log(apply ? "APPLY mode (typecast: false)" : "DRY-RUN — verified sources only");
console.log(`Planned updates: ${planned.length} of ${companies.length} companies\n`);

for (const p of planned.sort((a, b) => a.companyName.localeCompare(b.companyName))) {
  console.log(`${p.companyName} (${p.companyId})`);
  console.log(`  ${MAP_CP_BRANDS_AIRTABLE.brandLinkField}: ${p.mergedIds.length} brands`);
  console.log(`  ${p.brandLabels.join(", ")}`);
  console.log(`  sources: ${p.sources.map((s) => s.source).join(", ")}`);
}

const log = {
  generatedAt: new Date().toISOString(),
  mode: apply ? "apply" : "dry-run",
  plannedCount: planned.length,
  updates: planned,
};

if (apply && planned.length) {
  const applied = [];
  const failed = [];
  for (const p of planned) {
    try {
      await patchRecord(p.companyId, p.patch);
      applied.push({ id: p.companyId, name: p.companyName, brandCount: p.mergedIds.length });
      console.log(`\nOK ${p.companyName}`);
    } catch (err) {
      failed.push({ id: p.companyId, name: p.companyName, error: String(err.message || err) });
      console.error(`\nFAIL ${p.companyName}:`, err.message || err);
    }
  }
  log.applied = applied;
  log.failed = failed;
}

const logPath = join("reports", "company-profile-brands-backfill-log.json");
writeFileSync(logPath, JSON.stringify(log, null, 2), "utf8");
console.log(`\nLog: ${logPath}`);
