/**
 * Align Everhome Suites Brand Explorer + Basics with Choice reference materials.
 * Updates text fields in place — never touches Image attachments.
 *
 *   node scripts/patch-everhome-reference-alignment.mjs --dry-run
 *   node scripts/patch-everhome-reference-alignment.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";
import { sanitizeExternalCopy } from "../lib/external-owner-copy.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BRAND = "Everhome Suites";
const TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS = "Brand Setup - Brand Basics";
const FIXTURE = path.join(ROOT, "fixtures/brand-explorer-presentation-everhome-reference-alignment.json");

const TEXT_FIELDS = [
  "Title",
  "Body",
  "Case Summary Overview",
  "Case Summary Owner Objective",
  "Case Summary Brand Relevance",
  "Case Summary Interpretation",
  "Case Summary Tags",
];

/** Extra Everhome-specific cleanup beyond sanitizeExternalCopy. */
const EXTRA_PATTERNS = [
  [/extended-stay \(newer system\)/gi, "midscale extended-stay"],
  [/\ba extended-stay\b/gi, "a midscale extended-stay"],
  [/Residential extended-stay for longer-term guests\.?/gi, "A fresh take on midscale extended stay."],
  [/Residential extended-stay for longer-term guests/gi, "A fresh take on midscale extended stay"],
  [/Item 19[^.\n;]*/gi, ""],
  [/Item 20[^.\n;]*/gi, ""],
  [/No Item 19[^.\n.]*/gi, ""],
  [/FDD Item 19[^.\n.]*/gi, ""],
  [/without system Item 19[^.\n.]*/gi, ""],
  [/without system Item 19 tables[^.\n.]*/gi, ""],
  [/Diligence-Heavy Underwriting/gi, "Developer-Friendly Prototype"],
  [/Feasibility-Led Underwriting/gi, "Extended-Stay Expert Bench"],
  [/No Item 19 Averages/gi, "2020 Midscale Launch"],
  [/7,100\+ hotels/gi, "7,400+ hotels"],
  [/Owners willing to diligence newer FDD without Item 19 performance tables\.?/gi, "Owners targeting newest midscale extended-stay prototype with professional third-party management."],
  [/Operators comparing Choice extended-stay brands on royalty and prototype fit\.?/gi, "Operators comparing Everhome prototype economics versus MainStay and Suburban."],
];

function parseArgs(argv) {
  return { dryRun: argv.includes("--dry-run") };
}

function cleanText(value) {
  let s = sanitizeExternalCopy(String(value ?? "").trim());
  for (const [re, rep] of EXTRA_PATTERNS) {
    s = s.replace(re, rep);
  }
  return s
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/\s+\./g, ".")
    .replace(/\.\s*\./g, ".")
    .replace(/;\s*;/g, ";")
    .trim();
}

function patchFieldsFromRecord(rec) {
  const fields = {};
  let changed = false;
  for (const f of TEXT_FIELDS) {
    const raw = rec.get(f);
    if (raw == null || raw === "") continue;
    const clean = cleanText(raw);
    if (clean !== String(raw).trim()) {
      fields[f] = clean;
      changed = true;
    }
  }
  return changed ? fields : null;
}

async function selectPresentation(base) {
  const esc = BRAND.replace(/"/g, '\\"');
  return base(TABLE)
    .select({
      filterByFormula: `OR({Brand Name} = "${esc}", {Brand} = "${esc}")`,
      maxRecords: 500,
    })
    .all();
}

async function main() {
  const { dryRun } = parseArgs(process.argv);
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const data = JSON.parse(fs.readFileSync(FIXTURE, "utf8"));
  const explicitBySlot = new Map();
  for (const row of data.rows || []) {
    const sk = String(row.slotKey || "").trim();
    if (!sk) continue;
    if (!explicitBySlot.has(sk)) explicitBySlot.set(sk, []);
    explicitBySlot.get(sk).push(row);
  }

  const base = new Airtable({ apiKey: key }).base(baseId);
  const records = await selectPresentation(base);
  const bySlot = new Map();
  for (const rec of records) {
    const sk = String(rec.get("Slot Key") || "").trim();
    if (!bySlot.has(sk)) bySlot.set(sk, []);
    bySlot.get(sk).push(rec);
  }

  const updates = [];

  // Explicit fixture patches — match by title when multiple rows share a slot key
  for (const [slotKey, patches] of explicitBySlot) {
    const existing = bySlot.get(slotKey) || [];
    for (const patch of patches) {
      const patchTitle = patch.title != null ? cleanText(patch.title) : "";
      let rec = null;
      if (patchTitle) {
        rec = existing.find((r) => cleanText(r.get("Title")) === patchTitle);
      }
      if (!rec && existing.length === 1) rec = existing[0];
      if (!rec && !patchTitle && patches.length === 1) rec = existing[0];
      if (!rec) {
        console.warn(`Missing row for explicit patch: ${slotKey}${patchTitle ? ` title="${patchTitle}"` : ""}`);
        continue;
      }
      const fields = {};
      if (patch.title != null) {
        const t = cleanText(patch.title);
        if (t !== String(rec.get("Title") || "").trim()) fields.Title = t;
      }
      if (patch.body != null) {
        const b = cleanText(patch.body);
        if (b !== String(rec.get("Body") || "").trim()) fields.Body = b;
      }
      if (!Object.keys(fields).length) continue;
      updates.push({ id: rec.id, slotKey, fields, kind: "explicit" });
    }
  }

  // Global sanitize pass on all rows (skip if already in explicit batch)
  const explicitIds = new Set(updates.map((u) => u.id));
  for (const rec of records) {
    if (explicitIds.has(rec.id)) continue;
    const fields = patchFieldsFromRecord(rec);
    if (!fields) continue;
    updates.push({
      id: rec.id,
      slotKey: String(rec.get("Slot Key") || "").trim(),
      fields,
      kind: "sanitize",
    });
  }

  // Brand Basics alignment
  const basicsFields = data.brandBasicsFields || {};
  const basicsRows = await base(BASICS)
    .select({ filterByFormula: `{Brand Name} = "${BRAND.replace(/"/g, '\\"')}"`, maxRecords: 1 })
    .all();
  const basicsUpdates = {};
  for (const [field, value] of Object.entries(basicsFields)) {
    const rec = basicsRows[0];
    if (!rec) break;
    const clean = cleanText(value);
    if (clean !== String(rec.get(field) || "").trim()) basicsUpdates[field] = clean;
  }

  console.log(`${BRAND}: ${updates.length} presentation update(s)${Object.keys(basicsUpdates).length ? `; ${Object.keys(basicsUpdates).length} basics field(s)` : ""}`);
  for (const u of updates.filter((x) => x.kind === "explicit").slice(0, 15)) {
    console.log(`  explicit ${u.slotKey}`);
  }
  const sanitizeCount = updates.filter((x) => x.kind === "sanitize").length;
  if (sanitizeCount) console.log(`  + ${sanitizeCount} sanitize-only row(s)`);

  if (dryRun) {
    console.log("\nDry run — no writes.");
    if (Object.keys(basicsUpdates).length) console.log("Basics would update:", Object.keys(basicsUpdates).join(", "));
    return;
  }

  for (let i = 0; i < updates.length; i += 10) {
    const chunk = updates.slice(i, i + 10).map((u) => ({ id: u.id, fields: u.fields }));
    await base(TABLE).update(chunk);
  }

  if (Object.keys(basicsUpdates).length && basicsRows[0]) {
    await base(BASICS).update(basicsRows[0].id, basicsUpdates);
    console.log(`Updated Brand Basics: ${Object.keys(basicsUpdates).join(", ")}`);
  }

  console.log(`Updated ${updates.length} presentation row(s).`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
