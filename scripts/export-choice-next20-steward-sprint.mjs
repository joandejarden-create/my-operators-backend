#!/usr/bin/env node
/**
 * Wave 9A: scored next-20 Choice steward saves (highest impact browser-save sprint).
 *
 * Scoring (higher = sooner):
 *  +40 both Amenities + Hotel Description blank
 *  +20 Amenities blank only / +15 Description blank only
 *  +15 Ascend / Country Inn (lowest amenity coverage)
 *  +10 Comfort / Quality / Radisson family
 *  +10 Property ID present (known save path)
 *  -20 duplicate Property ID (keep first)
 *
 *   node scripts/export-choice-next20-steward-sprint.mjs
 */
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { parseChoiceAmenitiesFromHtml } from "../lib/choice-hotel-content-fetch.js";
import { writeCsv } from "../lib/str-census-import/report-utils.mjs";

const WAVE7 = "reports/active-brand-cala-steward-worklist-wave7.json";
const HTML_DIR = "reports/choice-amenity-html";
const OUT_CSV = "reports/choice-next20-steward-sprint.csv";
const OUT_MD = "reports/choice-next20-steward-sprint.md";
const OUT_HTML = "reports/choice-next20-steward-opener.html";
const OUT_JSON = "reports/choice-next20-steward-sprint.json";

const CHOICE = new Set([
  "Ascend Hotel Collection",
  "Comfort Inn & Suites",
  "Quality Inn",
  "Radisson by Choice",
  "Radisson Blu by Choice",
  "Radisson Individuals by Choice",
  "Radisson RED by Choice",
  "Country Inn & Suites by Choice",
]);

function hasUsableHtml(pid) {
  const path = join(HTML_DIR, `${String(pid || "").toLowerCase()}.html`);
  if (!existsSync(path)) return false;
  try {
    const parsed = parseChoiceAmenitiesFromHtml(readFileSync(path, "utf8"));
    return Boolean(parsed.hasAmenityMarkers && parsed.amenities?.length >= 3);
  } catch {
    return false;
  }
}

function scoreRow(r) {
  let score = 0;
  const blanks = r.blankFields || [];
  const hasAmen = blanks.includes("Amenities");
  const hasDesc = blanks.includes("Hotel Description");
  if (hasAmen && hasDesc) score += 40;
  else if (hasAmen) score += 20;
  else if (hasDesc) score += 15;
  if (r.brandName === "Ascend Hotel Collection" || r.brandName === "Country Inn & Suites by Choice") {
    score += 15;
  } else if (/Comfort|Quality|Radisson/i.test(r.brandName)) {
    score += 10;
  }
  if (r.propertyId) score += 10;
  if (/mexico|brazil|dominican|ecuador|chile/i.test(String(r.country || ""))) score += 5;
  return score;
}

function escapeHtml(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function main() {
  mkdirSync("reports", { recursive: true });
  if (!existsSync(WAVE7)) throw new Error(`Missing ${WAVE7}`);
  const wave7 = JSON.parse(readFileSync(WAVE7, "utf8"));

  const seenPid = new Set();
  /** @type {object[]} */
  const scored = [];
  for (const r of wave7.rows || []) {
    if (r.stewardPath !== "browser_save_property_page") continue;
    if (!CHOICE.has(r.brandName)) continue;
    if (!/choicehotels\.com/i.test(r.website || "")) continue;
    const pid = String(r.propertyId || "").trim().toUpperCase();
    if (!pid) continue;
    if (hasUsableHtml(pid)) continue;
    if (seenPid.has(pid)) continue;
    seenPid.add(pid);
    scored.push({
      ...r,
      propertyId: pid,
      score: scoreRow(r),
      htmlFile: `reports/choice-amenity-html/${pid.toLowerCase()}.html`,
    });
  }

  scored.sort((a, b) => b.score - a.score || a.brandName.localeCompare(b.brandName));
  const top20 = scored.slice(0, 20);

  writeCsv(
    OUT_CSV,
    top20.map((r, i) => ({
      rank: i + 1,
      score: r.score,
      brandName: r.brandName,
      censusRecordId: r.censusRecordId,
      censusName: r.censusName,
      country: r.country,
      propertyId: r.propertyId,
      blankFieldsJoined: r.blankFieldsJoined,
      website: r.website,
      htmlFile: r.htmlFile,
    }))
  );

  writeFileSync(
    OUT_JSON,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        poolMissingHtml: scored.length,
        top20Count: top20.length,
        top20,
      },
      null,
      2
    )
  );

  const md = [
    "# Choice next-20 steward sprint",
    "",
    `**Generated:** ${new Date().toISOString().slice(0, 10)}`,
    `**Pool (missing usable HTML, deduped PID):** ${scored.length}`,
    `**This sprint:** ${top20.length} highest-score saves`,
    "",
    "## Steps (≈30–60 min)",
    "",
    "1. Open `reports/choice-next20-steward-opener.html`",
    "2. For each row: **Open** → Ctrl+S → **Webpage, Complete** → save exactly to `htmlFile`",
    "3. Dry-run: `node scripts/backfill-choice-wave4-from-html.mjs`",
    "4. Apply: `node scripts/backfill-choice-wave4-from-html.mjs --apply`",
    "5. Re-export coverage: `node scripts/export-active-brand-cala-enrichment-coverage.mjs --tag=after-choice20`",
    "",
    "| Rank | Score | Brand | Hotel | PID | Blanks |",
    "|-----:|------:|-------|-------|-----|--------|",
    ...top20.map(
      (r, i) =>
        `| ${i + 1} | ${r.score} | ${r.brandName} | ${String(r.censusName).replace(/\|/g, "/")} | ${r.propertyId} | ${r.blankFieldsJoined} |`
    ),
    "",
    `CSV: \`${OUT_CSV}\` · Opener: \`${OUT_HTML}\``,
    "",
  ].join("\n");
  writeFileSync(OUT_MD, md);

  const rowsHtml = top20
    .map(
      (r, i) => `<tr>
  <td>${i + 1}</td>
  <td>${r.score}</td>
  <td>${escapeHtml(r.brandName)}</td>
  <td>${escapeHtml(r.censusName)}</td>
  <td><code>${escapeHtml(r.propertyId)}</code></td>
  <td>${escapeHtml(r.blankFieldsJoined)}</td>
  <td><a href="${escapeHtml(r.website)}" target="_blank" rel="noopener">Open</a></td>
  <td><code>${escapeHtml(r.htmlFile)}</code></td>
</tr>`
    )
    .join("\n");

  writeFileSync(
    OUT_HTML,
    `<!doctype html>
<html><head><meta charset="utf-8"/><title>Choice next-20 steward sprint</title>
<style>
body{font-family:Segoe UI,system-ui,sans-serif;margin:24px;color:#1a1a1a}
.note{background:#fff8e6;border:1px solid #e6d9a8;padding:12px 14px;max-width:960px;line-height:1.45}
table{border-collapse:collapse;width:100%;font-size:13px;margin-top:16px}
th,td{border:1px solid #ddd;padding:6px 8px;text-align:left;vertical-align:top}
th{background:#f4f4f4}
code{font-size:12px}
</style></head><body>
<h1>Choice next-20 steward sprint</h1>
<div class="note">
<p><strong>Goal:</strong> save 20 property pages as Webpage Complete, then run Choice HTML apply.</p>
<p>Save path must match the last column exactly (lowercase property id).</p>
<p>After saves: <code>node scripts/backfill-choice-wave4-from-html.mjs</code> then <code>--apply</code>.</p>
</div>
<table>
<thead><tr><th>#</th><th>Score</th><th>Brand</th><th>Hotel</th><th>PID</th><th>Blanks</th><th>Open</th><th>Save as</th></tr></thead>
<tbody>
${rowsHtml}
</tbody></table>
</body></html>`
  );

  console.log(`Pool missing HTML: ${scored.length}`);
  console.log(`Top 20 written → ${OUT_HTML}`);
  for (const r of top20.slice(0, 8)) {
    console.log(`  ${r.score} ${r.propertyId} ${r.brandName} | ${r.censusName}`);
  }
  if (top20.length > 8) console.log(`  … +${top20.length - 8} more`);
}

main();
