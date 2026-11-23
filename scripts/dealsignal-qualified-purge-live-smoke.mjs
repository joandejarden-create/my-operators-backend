/**
 * Live Airtable smoke for Qualified purge V2. Read-only. CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadDealOpportunities } from "../lib/hotel-intelligence/deal-opportunities/hif-deal-opportunities-reader-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function loadEnvFile(filePath) {
  if (!fs.existsSync(filePath)) return;
  for (const line of fs.readFileSync(filePath, "utf8").split(/\r?\n/)) {
    const m = line.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
    if (!m) continue;
    if (process.env[m[1]]) continue;
    let v = m[2];
    if (
      (v.startsWith('"') && v.endsWith('"')) ||
      (v.startsWith("'") && v.endsWith("'"))
    ) {
      v = v.slice(1, -1);
    }
    process.env[m[1]] = v;
  }
}

loadEnvFile(path.join(ROOT, ".env"));
loadEnvFile("C:/Dev/dealality/my-operators-backend/.env");

if (!process.env.AIRTABLE_API_KEY) {
  console.error("AIRTABLE_API_KEY_MISSING");
  process.exit(2);
}

const data = await loadDealOpportunities();
const q = (data.opportunities || []).filter((o) => o.group === "qualified");
const watch = (data.opportunities || []).filter((o) => o.group === "watch");
const defects = [
  "find_ds_v4_sundancer_ac_cartagena_bct_2026",
  "find_ds_v4_ghl_sonesta_retention_2025",
  "find_ds_v3_hist_finn_hi_mty_franchise_retention_2025",
  "find_ds_v3_posadas_devossion_isla_mujeres_2026",
  "find_ds_cala_v1_finn_courtyard_pdc_committed_2026",
  "find_tb_cala_fu_hi_puebla_hma_nonrenew_20250724",
  "find_tb_v4_drh_the_gwen_hotel_hma_202606",
  "find_ds_v3_hist_finn_courtyard_gdl_2025",
];
const defectRows = defects.map((id) => {
  const o = (data.opportunities || []).find((x) => x.id === id);
  if (!o) return { id, missing: true };
  return {
    id,
    group: o.group,
    status: o.status,
    reason: o.classifyReason,
    dest: o.commercialDestination,
    sources: (o.sources || []).filter((s) => s.url).length,
    engagement: o.drawerV2?.engagementPriority,
    confirmed: String(o.drawerV2?.whyExists?.confirmedProspectiveDecision || "").slice(0, 160),
  };
});

const outDir = path.join(ROOT, "reports/dealsignal-qualified-executed-purge-v2");
fs.mkdirSync(outDir, { recursive: true });
const out = {
  asOf: data.asOf,
  metrics: data.metrics,
  qualifiedCount: q.length,
  watchCount: watch.length,
  qualifiedIds: q.map((o) => o.id),
  qualifiedSample: q.map((o) => ({
    id: o.id,
    name: o.name,
    status: o.status,
    reason: o.classifyReason,
  })),
  defectRows,
};
fs.writeFileSync(path.join(outDir, "LIVE_READER_SMOKE.json"), JSON.stringify(out, null, 2));
console.log(
  JSON.stringify(
    {
      qualified: q.length,
      watch: watch.length,
      metrics: data.metrics,
      defectRows,
      qualifiedSample: out.qualifiedSample.slice(0, 20),
    },
    null,
    2,
  ),
);
