#!/usr/bin/env node
/**
 * Restore brand-card description prose after Brand Positioning enrichment.
 *
 * Brand Positioning is now the concise Title Case attribute (Product & Segment).
 * Prior long prose that lived in Brand Positioning is restored into
 * Brand Value Proposition when that field is empty — so library cards can
 * show the original description again without undoing Positioning.
 *
 * Usage:
 *   node scripts/be-restore-brand-card-description-v1.mjs
 *   node scripts/be-restore-brand-card-description-v1.mjs --apply
 */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import Airtable from "airtable";

const APPLY = process.argv.includes("--apply");
const ROOT = process.cwd();
const REPORT = join(ROOT, "reports/brand-explorer/brand-positioning-v1");
const APPLY_REPORT = join(REPORT, "01-enrichment-APPLY.json");
const TABLE = "tbl1x6S7I7JwTcRdV";
const FIELD_VP = "Brand Value Proposition";
const FIELD_POS = "Brand Positioning";

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function isShortPositioning(s) {
  const t = nz(s);
  if (!t) return true;
  if (t.length <= 60 && !/[.!?…]/.test(t)) return true;
  // Title Case short phrases like "Upscale Select Service"
  if (t.length <= 80 && !/\bis a\b|\bfor owners\b|\bwithin\b/i.test(t) && (t.match(/\s/g) || []).length <= 6) {
    return true;
  }
  return false;
}

async function main() {
  const report = JSON.parse(readFileSync(APPLY_REPORT, "utf8"));
  const matrix = Array.isArray(report.matrix) ? report.matrix : [];
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID || "appvtnDurnMSjINP6";
  if (!key) throw new Error("AIRTABLE_API_KEY required");
  const base = new Airtable({ apiKey: key }).base(baseId);

  const planned = [];
  const skipped = [];

  for (const row of matrix) {
    const id = nz(row.brandId);
    const prev = nz(row.previousPositioning);
    if (!id || !prev) {
      skipped.push({ brandId: id, brandName: row.brandName, reason: "no_previous" });
      continue;
    }
    if (isShortPositioning(prev)) {
      skipped.push({ brandId: id, brandName: row.brandName, reason: "previous_was_short", prev });
      continue;
    }
    let rec;
    try {
      rec = await base(TABLE).find(id);
    } catch (err) {
      skipped.push({ brandId: id, brandName: row.brandName, reason: "find_failed", error: String(err.message || err) });
      continue;
    }
    const vp = nz(rec.get(FIELD_VP));
    const pos = nz(rec.get(FIELD_POS));
    if (vp) {
      skipped.push({ brandId: id, brandName: row.brandName, reason: "value_proposition_already_set" });
      continue;
    }
    planned.push({
      brandId: id,
      brandName: row.brandName || nz(rec.get("Brand Name")),
      previousPositioning: prev,
      currentPositioning: pos,
      action: "restore_to_value_proposition",
    });
  }

  let written = 0;
  let readbackOk = 0;
  if (APPLY) {
    for (const row of planned) {
      await base(TABLE).update(row.brandId, { [FIELD_VP]: row.previousPositioning });
      written += 1;
      const back = await base(TABLE).find(row.brandId);
      if (nz(back.get(FIELD_VP)) === row.previousPositioning) readbackOk += 1;
      await new Promise((r) => setTimeout(r, 120));
    }
  }

  mkdirSync(REPORT, { recursive: true });
  const out = {
    contract: "BE_RESTORE_BRAND_CARD_DESCRIPTION_V1",
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    planned: planned.length,
    written,
    readbackOk,
    skipped: skipped.length,
    rows: planned,
    skipSample: skipped.slice(0, 40),
  };
  const name = APPLY ? "02-restore-card-description-APPLY.json" : "02-restore-card-description-DRY_RUN.json";
  writeFileSync(join(REPORT, name), JSON.stringify(out, null, 2));
  console.log(JSON.stringify({ mode: out.mode, planned: planned.length, written, readbackOk, skipped: skipped.length }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
