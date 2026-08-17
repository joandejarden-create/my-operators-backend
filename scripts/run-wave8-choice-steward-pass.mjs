#!/usr/bin/env node
/**
 * Wave 8: build Choice-family steward subset from Wave 7 worklist + opener HTML,
 * then optionally harvest Wayback for missing amenity HTML.
 *
 *   node scripts/run-wave8-choice-steward-pass.mjs
 *   node scripts/run-wave8-choice-steward-pass.mjs --harvest --limit=40
 *   node scripts/run-wave8-choice-steward-pass.mjs --harvest --apply
 */
import "../load-env.js";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  readdirSync,
} from "node:fs";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { parseChoiceAmenitiesFromHtml } from "../lib/choice-hotel-content-fetch.js";
import { writeCsv } from "../lib/str-census-import/report-utils.mjs";

const WAVE7 = "reports/active-brand-cala-steward-worklist-wave7.json";
const HTML_DIR = "reports/choice-amenity-html";
const CHOICE_CSV = "reports/choice-wave8-steward-worklist.csv";
const OPENER = "reports/choice-wave8-steward-opener.html";
const HARVEST_CSV = "reports/choice-wave8-wayback-harvest-input.csv";

const CHOICE_BRANDS = new Set([
  "Ascend Hotel Collection",
  "Comfort Inn & Suites",
  "Quality Inn",
  "Radisson by Choice",
  "Radisson Blu by Choice",
  "Radisson Individuals by Choice",
  "Radisson RED by Choice",
  "Country Inn & Suites by Choice",
]);

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    harvest: args.includes("--harvest"),
    apply: args.includes("--apply"),
    limit: Number(args.find((a) => a.startsWith("--limit="))?.split("=")[1] || 40),
  };
}

function hasUsableHtml(pid) {
  const path = join(HTML_DIR, `${String(pid).toLowerCase()}.html`);
  if (!existsSync(path)) return false;
  try {
    const parsed = parseChoiceAmenitiesFromHtml(readFileSync(path, "utf8"));
    return Boolean(parsed.hasAmenityMarkers && parsed.amenities?.length >= 3);
  } catch {
    return false;
  }
}

function escapeHtml(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function writeOpener(rows) {
  const body = rows
    .map(
      (r, i) => `<tr>
  <td>${i + 1}</td>
  <td>${escapeHtml(r.brandName)}</td>
  <td>${escapeHtml(r.censusName)}</td>
  <td>${escapeHtml(r.propertyId)}</td>
  <td>${escapeHtml(r.blankFieldsJoined)}</td>
  <td>${r.hasUsableHtml ? "yes" : "no"}</td>
  <td><a href="${escapeHtml(r.website)}" target="_blank" rel="noopener">Open</a></td>
  <td><code>reports/choice-amenity-html/${escapeHtml(String(r.propertyId).toLowerCase())}.html</code></td>
</tr>`
    )
    .join("\n");
  const html = `<!doctype html>
<html><head><meta charset="utf-8"/><title>Choice Wave 8 steward opener</title>
<style>
body{font-family:Segoe UI,system-ui,sans-serif;margin:24px;color:#1a1a1a}
table{border-collapse:collapse;width:100%;font-size:13px}
th,td{border:1px solid #ddd;padding:6px 8px;text-align:left;vertical-align:top}
th{background:#f4f4f4}
code{font-size:12px}
.note{max-width:900px;line-height:1.45;margin-bottom:16px}
</style></head><body>
<h1>Choice Wave 8 steward opener</h1>
<div class="note">
<p>Save each property page as <strong>Webpage, Complete</strong> to the path in the last column.
Then run <code>node scripts/backfill-choice-wave4-from-html.mjs --apply</code>.</p>
<p>Rows: ${rows.length}. Missing usable HTML: ${rows.filter((r) => !r.hasUsableHtml).length}.</p>
</div>
<table>
<thead><tr><th>#</th><th>Brand</th><th>Hotel</th><th>PID</th><th>Blanks</th><th>HTML?</th><th>Open</th><th>Save as</th></tr></thead>
<tbody>
${body}
</tbody></table>
</body></html>`;
  writeFileSync(OPENER, html);
}

async function main() {
  mkdirSync("reports", { recursive: true });
  if (!existsSync(WAVE7)) {
    throw new Error(`Missing ${WAVE7} — run export-active-brand-cala-steward-worklist.mjs first`);
  }
  const wave7 = JSON.parse(readFileSync(WAVE7, "utf8"));
  const choice = (wave7.rows || [])
    .filter((r) => r.stewardPath === "browser_save_property_page")
    .filter((r) => CHOICE_BRANDS.has(r.brandName))
    .filter((r) => r.website && /choicehotels\.com/i.test(r.website))
    .map((r) => ({
      ...r,
      propertyId: String(r.propertyId || "").trim().toUpperCase(),
      hasUsableHtml: hasUsableHtml(r.propertyId),
    }))
    .filter((r) => r.propertyId)
    .sort((a, b) => Number(a.hasUsableHtml) - Number(b.hasUsableHtml) || a.brandName.localeCompare(b.brandName));

  writeCsv(
    CHOICE_CSV,
    choice.map((r) => ({
      brandName: r.brandName,
      censusRecordId: r.censusRecordId,
      censusName: r.censusName,
      country: r.country,
      propertyId: r.propertyId,
      website: r.website,
      blankFieldsJoined: r.blankFieldsJoined,
      hasUsableHtml: r.hasUsableHtml ? "yes" : "no",
      htmlFile: `reports/choice-amenity-html/${r.propertyId.toLowerCase()}.html`,
    }))
  );
  writeOpener(choice);

  const missing = choice.filter((r) => !r.hasUsableHtml);
  console.log(`Choice browser-save rows: ${choice.length}`);
  console.log(`Missing usable HTML: ${missing.length}`);
  console.log(`Opener: ${OPENER}`);
  console.log(`CSV: ${CHOICE_CSV}`);

  const opts = parseArgs();
  if (!opts.harvest) {
    console.log("Pass --harvest to Wayback-fetch missing HTML (limit default 40).");
    return;
  }

  const harvestRows = missing.slice(0, opts.limit);
  writeCsv(
    HARVEST_CSV,
    harvestRows.map((r) => ({
      censusRecordId: r.censusRecordId,
      censusName: r.censusName,
      city: r.city || "",
      country: r.country,
      propertyId: r.propertyId,
      propertyUrl: r.website,
      htmlFile: `reports/choice-amenity-html/${r.propertyId.toLowerCase()}.html`,
      instruction: "wayback harvest",
    }))
  );

  console.log(`\nHarvesting Wayback for ${harvestRows.length} missing Choice PIDs…`);
  const harvest = spawnSync(
    process.execPath,
    [
      "scripts/harvest-choice-amenity-wayback-steward.mjs",
      `--input=${HARVEST_CSV}`,
      "--delay-ms=700",
    ],
    { stdio: "inherit" }
  );
  if (harvest.status) process.exit(harvest.status || 1);

  console.log("\nApplying Choice fills from HTML (dry-run unless --apply)…");
  const applyArgs = ["scripts/backfill-choice-wave4-from-html.mjs"];
  if (opts.apply) applyArgs.push("--apply");
  const apply = spawnSync(process.execPath, applyArgs, { stdio: "inherit" });
  process.exit(apply.status || 0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
