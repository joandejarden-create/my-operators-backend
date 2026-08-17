#!/usr/bin/env node
/**
 * Wave 8: opener HTML worklists for non-Choice browser-save residuals
 * (BWH, Wyndham, Marriott Autograph/Tribute/Design, IHG soft-blocks).
 *
 *   node scripts/generate-wave8-nonchoice-steward-openers.mjs
 */
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";

const WAVE7 = "reports/active-brand-cala-steward-worklist-wave7.json";

const GROUPS = {
  choice: [
    "Ascend Hotel Collection",
    "Comfort Inn & Suites",
    "Quality Inn",
    "Radisson by Choice",
    "Radisson Blu by Choice",
    "Radisson Individuals by Choice",
    "Radisson RED by Choice",
    "Country Inn & Suites by Choice",
  ],
  bwh: ["BW Premier Collection", "BW Signature Collection"],
  wyndham: ["Dazzler by Wyndham", "Trademark Collection by Wyndham"],
  marriott: ["Autograph Collection", "Tribute Portfolio", "Design Hotels"],
  ihg: ["Kimpton Hotels", "Hotel Indigo", "Vignette Collection"],
  hilton: ["Curio Collection by Hilton", "Tapestry Collection by Hilton"],
  other: [],
};

function escapeHtml(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function openerHtml(title, note, rows) {
  const body = rows
    .map(
      (r, i) => `<tr>
  <td>${i + 1}</td>
  <td>${escapeHtml(r.brandName)}</td>
  <td>${escapeHtml(r.censusName)}</td>
  <td>${escapeHtml(r.country)}</td>
  <td>${escapeHtml(r.blankFieldsJoined)}</td>
  <td><a href="${escapeHtml(r.website)}" target="_blank" rel="noopener">Open</a></td>
</tr>`
    )
    .join("\n");
  return `<!doctype html>
<html><head><meta charset="utf-8"/><title>${escapeHtml(title)}</title>
<style>
body{font-family:Segoe UI,system-ui,sans-serif;margin:24px}
table{border-collapse:collapse;width:100%;font-size:13px}
th,td{border:1px solid #ddd;padding:6px 8px;text-align:left}
th{background:#f4f4f4}
.note{max-width:920px;line-height:1.45;margin-bottom:16px}
</style></head><body>
<h1>${escapeHtml(title)}</h1>
<div class="note"><p>${escapeHtml(note)}</p><p>Rows: ${rows.length}</p></div>
<table><thead><tr><th>#</th><th>Brand</th><th>Hotel</th><th>Country</th><th>Blanks</th><th>Open</th></tr></thead>
<tbody>${body}</tbody></table>
</body></html>`;
}

function main() {
  mkdirSync("reports", { recursive: true });
  if (!existsSync(WAVE7)) throw new Error(`Missing ${WAVE7}`);
  const wave7 = JSON.parse(readFileSync(WAVE7, "utf8"));
  const browser = (wave7.rows || []).filter((r) => r.stewardPath === "browser_save_property_page");
  const choiceSet = new Set(GROUPS.choice);
  const assigned = new Set([...GROUPS.bwh, ...GROUPS.wyndham, ...GROUPS.marriott, ...GROUPS.ihg, ...GROUPS.hilton, ...GROUPS.choice]);

  const buckets = {
    bwh: browser.filter((r) => GROUPS.bwh.includes(r.brandName)),
    wyndham: browser.filter((r) => GROUPS.wyndham.includes(r.brandName)),
    marriott: browser.filter((r) => GROUPS.marriott.includes(r.brandName)),
    ihg: browser.filter((r) => GROUPS.ihg.includes(r.brandName)),
    hilton: browser.filter((r) => GROUPS.hilton.includes(r.brandName)),
    other: browser.filter((r) => !assigned.has(r.brandName) && !choiceSet.has(r.brandName)),
  };

  const outs = [];
  for (const [key, rows] of Object.entries(buckets)) {
    if (!rows.length) continue;
    const path = `reports/wave8-${key}-steward-opener.html`;
    const notes = {
      bwh: "bestwestern.com often captcha-blocks automation. Save property page HTML manually; do not invent amenities.",
      wyndham: "Avoid nav breadcrumbs. Prefer /services-amenities when real amenity chips render.",
      marriott: "Overview is Akamai-blocked for bots. Save overview HTML in a normal browser for description/amenities.",
      ihg: "Soft-blocked hoteldetail shells return no amenity markers. Save full property page from a browser session.",
      hilton: "Prefer Hilton GraphQL when Property ID exists; otherwise save hilton.com property page.",
      other: "Official brand property page only; fill-blank; no invent.",
    };
    writeFileSync(
      path,
      openerHtml(`Wave 8 ${key} steward opener`, notes[key] || notes.other, rows)
    );
    writeFileSync(
      `reports/wave8-${key}-steward-worklist.json`,
      JSON.stringify({ generatedAt: new Date().toISOString(), group: key, count: rows.length, rows }, null, 2)
    );
    outs.push({ key, path, count: rows.length });
    console.log(`${key}: ${rows.length} → ${path}`);
  }
  writeFileSync(
    "reports/wave8-nonchoice-steward-openers-summary.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), outs }, null, 2)
  );
}

main();
