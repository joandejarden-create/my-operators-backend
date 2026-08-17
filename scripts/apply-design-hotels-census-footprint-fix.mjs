/**
 * Design Hotels footprint fix: census alias + CALA footprint rows (replaces empty Brand Footprint form).
 *
 *   node scripts/apply-design-hotels-census-footprint-fix.mjs --dry-run
 *   node scripts/apply-design-hotels-census-footprint-fix.mjs
 */
import "../load-env.js";
import Airtable from "airtable";
import { spawnSync } from "child_process";
import path from "path";
import { fileURLToPath } from "url";
import { aggregateCensusPresenceSummary } from "../lib/hotel-census/aggregate-presence-summary.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BASICS_ID = "rec02zPClpWUTCyXM";
const dryRun = process.argv.includes("--dry-run");

async function writeWithFieldPruning(base, table, recordId, fields) {
  let payload = { ...fields };
  const removed = [];
  for (let attempt = 0; attempt < 80; attempt++) {
    if (!Object.keys(payload).length) return { removed };
    try {
      await base(table).update(recordId, payload, { typecast: true });
      return { removed };
    } catch (err) {
      const msg = String(err?.message || err);
      const m =
        msg.match(/Unknown field name:\s*['"](.+?)['"]/i) ||
        msg.match(/Field\s+['"](.+?)['"]\s+cannot accept/i);
      const bad = m ? m[1].trim() : null;
      if (bad && Object.prototype.hasOwnProperty.call(payload, bad)) {
        delete payload[bad];
        removed.push(bad);
        continue;
      }
      throw err;
    }
  }
  throw new Error("writeWithFieldPruning exceeded retries");
}

function footprintPatchFromCensus(summary) {
  const openH = summary.metrics.totalOpenHotels;
  const openR = summary.metrics.totalOpenKeys;
  const pipeH = summary.metrics.totalPipelineHotels;
  const pipeR = summary.metrics.totalPipelineKeys;

  const patch = {
    "Footprint Figures As Of": new Date().toISOString().slice(0, 10),
    "Footprint Data Source": "Dealality Hotel Census — CALA (Affiliation: Design Hotels)",
    "Footprint Data Status": "Estimated",
    "Footprint Notes":
      "CALA footprint from Hotel Census (open + pipeline). Explorer Footprint tab uses census for country/archetype breakdowns when alias is active. Global region cards remain directional directory narrative.",
    "Figures as of": new Date().toISOString().slice(0, 10),
    "Number of Markets Operated In": summary.metrics.countryCount || 0,
    "CALA Existing Hotel": openH,
    "CALA Existing Rooms": openR,
    "CALA Pipeline Hotel": pipeH,
    "CALA Pipeline Rooms": pipeR,
  };

  return { patch, openH, openR, pipeH, pipeR };
}

async function main() {
  const summary = await aggregateCensusPresenceSummary({
    affiliationMatchers: ["Design Hotels"],
    parentCompany: null,
  });
  if (!summary.ok) throw new Error(summary.error || "Census aggregation failed");

  const { patch, openH, openR, pipeH, pipeR } = footprintPatchFromCensus(summary);
  console.log("Census CALA rollup:", { openH, openR, pipeH, pipeR });
  console.log("Countries:", (summary.countryBreakdown || []).map((r) => r.label).join(", "));

  console.log("\n=== 1/2 Brand Alias Mapping ===");
  const aliasArgs = [
    path.join(ROOT, "scripts/seed-reviewed-brand-aliases.mjs"),
    "--file=fixtures/design-hotels-brand-alias-reviewed.json",
  ];
  if (dryRun) aliasArgs.push("--dry-run");
  const aliasRun = spawnSync(process.execPath, aliasArgs, { cwd: ROOT, stdio: "inherit" });
  if (aliasRun.status !== 0) process.exit(aliasRun.status ?? 1);

  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  console.log("\n=== 2/2 Brand Footprint (CALA census rows) ===");
  const base = new Airtable({ apiKey: key }).base(baseId);
  const basics = await base("Brand Setup - Brand Basics").find(BASICS_ID);
  const link = basics.fields["Brand Setup - Brand Footprint"];
  const footprintId = Array.isArray(link) ? link[0] : null;
  if (!footprintId) throw new Error("Design Hotels Brand Footprint link missing on Basics");

  if (dryRun) {
    console.log("[dry-run] would patch footprint", footprintId, patch);
    return;
  }

  const { removed } = await writeWithFieldPruning(
    base,
    "Brand Setup - Brand Footprint",
    footprintId,
    patch
  );
  console.log("Updated footprint", footprintId, removed.length ? `(skipped: ${removed.join(", ")})` : "");

  const { buildBrandCensusSummary } = await import("../lib/hotel-census/build-brand-census-summary.js");
  const cs = await buildBrandCensusSummary("Design Hotels", "Marriott International");
  console.log("\nPost-fix censusSummary:", {
    fallbackRecommended: cs.fallbackRecommended,
    metrics: cs.metrics,
    countries: cs.breakdowns?.country?.length,
    locationTypes: cs.breakdowns?.locationType?.length,
  });
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
