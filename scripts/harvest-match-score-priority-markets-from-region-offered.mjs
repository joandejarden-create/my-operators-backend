#!/usr/bin/env node
/**
 * Harvest Priority Markets worksheet fills from Brand Basics "Region Offered"
 * (existing Airtable data — source B). Does not invent markets.
 *
 * Writes an updated worksheet JSON, then optionally dry-run/apply via apply script.
 *
 * Usage:
 *   node scripts/harvest-match-score-priority-markets-from-region-offered.mjs
 *   node scripts/harvest-match-score-priority-markets-from-region-offered.mjs --apply
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadActiveUniverse } from "../lib/partner-intelligence/brand-explorer-active-universe.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BASICS = "Brand Setup - Brand Basics";

async function atFetch(baseId, apiKey, urlPath) {
  const url = `https://api.airtable.com/v0/${baseId}/${urlPath}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const data = await res.json();
  if (!res.ok || data.error) throw new Error(data.error?.message || res.statusText);
  return data;
}

function regionToPriorityList(regionOffered) {
  if (regionOffered == null) return [];
  const arr = Array.isArray(regionOffered) ? regionOffered : [regionOffered];
  const regions = arr
    .map((v) => String(typeof v === "object" && v?.name != null ? v.name : v).trim())
    .filter(Boolean);

  /** Map Brand Basics Region Offered → Project Fit Priority Markets allowed options (schema meta). */
  const REGION_TO_PRIORITY = {
    "North America": ["United States (Broad)", "Canada"],
    "Caribbean & Latin America": ["Caribbean", "Latin America (Broad)", "Mexico", "Central America", "South America"],
    Europe: ["Western Europe", "Eastern Europe", "Southern Europe", "Northern Europe", "United Kingdom", "Nordic Countries"],
    "Middle East & Africa": ["Middle East"],
    // No Asia Pacific option on Priority Markets — leave unmapped rather than invent
  };

  const out = new Set();
  // If brand is offered in every primary region bucket, prefer Global (valid select option).
  const primary = ["North America", "Caribbean & Latin America", "Europe", "Middle East & Africa", "Asia Pacific"];
  const hasAllPrimary = primary.every((p) => regions.some((r) => r === p));
  if (hasAllPrimary || regions.length >= 4) {
    return ["Global"];
  }
  for (const r of regions) {
    for (const opt of REGION_TO_PRIORITY[r] || []) out.add(opt);
  }
  return [...out];
}

async function main() {
  const doApply = process.argv.includes("--apply");
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  const worksheetPath = path.join(ROOT, "reports", "match-score-brand-setup-founder-worksheet.json");
  if (!fs.existsSync(worksheetPath)) {
    throw new Error("Run npm run audit-match-score-brand-setup-gaps first");
  }
  const worksheet = JSON.parse(fs.readFileSync(worksheetPath, "utf8"));
  const universe = await loadActiveUniverse({ includeDetails: false });
  const byId = new Map(universe.brands.map((b) => [b.recordId, b]));

  let filled = 0;
  const today = new Date().toISOString().slice(0, 10);
  for (const row of worksheet.rows || []) {
    if (row.fieldKey !== "priorityMarkets") continue;
    // Re-harvest even if a previous failed attempt left a proposedValue
    const brand = byId.get(row.brandRecordId);
    if (!brand) continue;
    const rec = await atFetch(baseId, apiKey, `${encodeURIComponent(BASICS)}/${encodeURIComponent(row.brandRecordId)}`);
    const regions = regionToPriorityList(rec.fields?.["Region Offered"]);
    if (!regions.length) {
      row.proposedValue = "";
      row.sourceType = "";
      row.sourceRef = "";
      row.notes = "No mappable Priority Markets options from Region Offered (left blank).";
      continue;
    }
    // Store as JSON array so apply path does not re-split on commas inside labels
    row.proposedValue = JSON.stringify(regions);
    row.sourceType = "B";
    row.sourceRef = `Brand Setup - Brand Basics · Region Offered (${row.brandName}) → Priority Markets schema map`;
    row.filledBy = "harvest-match-score-priority-markets-from-region-offered.mjs";
    row.filledDate = today;
    row.notes =
      "Mapped Region Offered to existing Priority Markets select options. Global used when brand is offered across most regions. Not invented markets.";
    filled += 1;
  }

  const outPath = path.join(ROOT, "reports", "match-score-brand-setup-founder-worksheet-harvested.json");
  fs.writeFileSync(outPath, JSON.stringify(worksheet, null, 2), "utf8");
  // Also update primary worksheet so apply default path works
  fs.writeFileSync(worksheetPath, JSON.stringify(worksheet, null, 2), "utf8");

  console.log(`Harvested priorityMarkets fills: ${filled}`);
  console.log(`Wrote ${outPath}`);
  console.log(`Updated ${worksheetPath}`);

  // Chain dry-run / apply
  const { spawnSync } = await import("child_process");
  const args = ["scripts/apply-match-score-brand-setup-fills.mjs", "--worksheet", worksheetPath];
  if (doApply) args.push("--apply");
  else args.push("--dry-run");
  const r = spawnSync(process.execPath, args, { cwd: ROOT, stdio: "inherit" });
  process.exit(r.status || 0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
