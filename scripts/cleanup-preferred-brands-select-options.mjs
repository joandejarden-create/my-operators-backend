#!/usr/bin/env node
/**
 * Clean Strategic Intent "Preferred Brands" multi-select options.
 *
 * Keeps: options used on any SI record + options that exact-match a Brand Basics Brand Name.
 * Removes: unused options that are not a Brand Basics Brand Name (obsolete aliases / typos / rec* junk).
 *
 * NOTE: Airtable Meta API currently returns 422 when PATCHing select `choices` on this base
 * (even no-op). Script always writes a report + manual checklist; --apply attempts the API and
 * falls back to reporting if write fails.
 *
 * Usage:
 *   node scripts/cleanup-preferred-brands-select-options.mjs --dry-run
 *   node scripts/cleanup-preferred-brands-select-options.mjs --apply
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { STRATEGIC_INTENT_TABLE } from "../api/schemas/deal-setup-fields.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const stamp = APPLY ? "apply" : "dry-run";
const OUT_JSON = path.join(ROOT, "reports", `preferred-brands-select-cleanup-${stamp}.json`);
const OUT_MD = path.join(ROOT, "reports", `preferred-brands-select-cleanup-${stamp}.md`);

const SI_TABLE = STRATEGIC_INTENT_TABLE || "Strategic Intent - Operational - Key Challenges";
const BB_TABLE = "Brand Setup - Brand Basics";
const FIELD_NAME = "Preferred Brands";

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

async function metaTables(baseId, token) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data?.error?.message || JSON.stringify(data.error || data));
  return data.tables || [];
}

async function listAll(baseId, token, table, fields) {
  const out = [];
  let offset = null;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const data = await res.json();
    if (!res.ok) throw new Error(`${table}: ${data?.error?.message || res.status}`);
    out.push(...(data.records || []));
    offset = data.offset || null;
    await new Promise((r) => setTimeout(r, 220));
  } while (offset);
  return out;
}

async function patchFieldChoices(baseId, token, tableId, fieldId, choices) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}/tables/${tableId}/fields/${fieldId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      options: {
        choices: choices.map((c) => ({
          id: c.id,
          name: c.name,
          ...(c.color ? { color: c.color } : {}),
        })),
      },
    }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(data?.error?.message || JSON.stringify(data.error || data));
    err.status = res.status;
    err.body = data;
    throw err;
  }
  return data;
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const token = process.env.AIRTABLE_API_KEY;
  if (!baseId || !token) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  console.log(`mode=${APPLY ? "APPLY" : "dry-run"} field=${FIELD_NAME}`);

  const tables = await metaTables(baseId, token);
  const siTable = tables.find((t) => t.name === SI_TABLE);
  if (!siTable) throw new Error(`Table not found: ${SI_TABLE}`);
  const field = (siTable.fields || []).find((f) => f.name === FIELD_NAME);
  if (!field) throw new Error(`Field not found: ${FIELD_NAME}`);
  if (field.type !== "multipleSelects") {
    throw new Error(`Expected multipleSelects, got ${field.type}`);
  }

  const choices = field.options?.choices || [];
  const basics = await listAll(baseId, token, BB_TABLE, ["Brand Name"]);
  const brandNames = new Set(
    basics.map((r) => String(r.fields?.["Brand Name"] || "").trim()).filter(Boolean)
  );
  const brandNorm = new Set([...brandNames].map(norm));

  const siRows = await listAll(baseId, token, SI_TABLE, [FIELD_NAME]);
  const used = new Set();
  for (const rec of siRows) {
    const val = rec.fields?.[FIELD_NAME];
    const arr = Array.isArray(val) ? val : val != null ? [val] : [];
    for (const v of arr) {
      const name = typeof v === "string" ? v.trim() : String(v?.name || "").trim();
      if (name) used.add(name);
    }
  }

  const keep = [];
  const remove = [];
  for (const choice of choices) {
    const name = String(choice.name || "").trim();
    const inUse = used.has(name);
    const isBrandBasics = brandNames.has(name) || brandNorm.has(norm(name));
    const isRecJunk = /^rec[a-zA-Z0-9]{10,}$/.test(name);
    if ((inUse || isBrandBasics) && !isRecJunk) {
      keep.push({
        id: choice.id,
        name,
        color: choice.color,
        reason: inUse ? (isBrandBasics ? "used+basics" : "used") : "brand_basics",
      });
    } else {
      remove.push({
        id: choice.id,
        name,
        color: choice.color,
        reason: isRecJunk ? "rec_id_junk" : "unused_not_brand_basics",
      });
    }
  }

  const obsoleteAliases = remove.filter((r) =>
    [
      "moxy",
      "kimpton hotels and restaurants",
      "mgallery hotel collection",
      "unbound collection",
      "hyatt unbound collection",
      "tapestry collection",
      "the luxury collection",
      "the unbound collection",
    ].includes(norm(r.name))
  );

  const report = {
    generatedAt: new Date().toISOString(),
    dryRun: !APPLY,
    tableId: siTable.id,
    tableName: SI_TABLE,
    fieldId: field.id,
    fieldName: FIELD_NAME,
    choiceCountBefore: choices.length,
    choiceCountAfter: keep.length,
    usedOnRecords: used.size,
    brandBasicsNames: brandNames.size,
    removeCount: remove.length,
    obsoleteAliasCount: obsoleteAliases.length,
    removeNames: remove.map((r) => r.name),
    obsoleteAliases: obsoleteAliases.map((r) => r.name),
    keepCount: keep.length,
    apiNote:
      "Meta API PATCH of select choices may return 422 on this base; use Airtable UI checklist if apply fails.",
  };

  if (APPLY) {
    try {
      await patchFieldChoices(
        baseId,
        token,
        siTable.id,
        field.id,
        keep.map((c) => ({ id: c.id, name: c.name, color: c.color }))
      );
      report.applied = true;
      report.applyMethod = "meta_api";
      console.log(`API removed ${remove.length} options; kept ${keep.length}.`);
    } catch (err) {
      report.applied = false;
      report.applyMethod = "manual_required";
      report.applyError = err.message || String(err);
      console.warn("Meta API could not update choices:", err.message || err);
      console.warn("Use the markdown checklist in Airtable UI (Customize field → Options).");
    }
  } else {
    console.log(
      `Would remove ${remove.length} / ${choices.length} options; keep ${keep.length} (used=${used.size}).`
    );
  }

  const md = [
    `# Preferred Brands select cleanup (${stamp})`,
    "",
    `Generated: ${report.generatedAt}`,
    "",
    `Field: **${SI_TABLE} → ${FIELD_NAME}**`,
    "",
    `- Choices now: **${choices.length}**`,
    `- Would keep: **${keep.length}**`,
    `- Would remove: **${remove.length}**`,
    `- Obsolete renamed aliases in remove list: **${obsoleteAliases.length}**`,
    "",
    "## Manual steps (Airtable UI)",
    "",
    "1. Open base → table **Strategic Intent - Operational - Key Challenges**.",
    "2. Customize field **Preferred Brands** → edit Options.",
    "3. Delete each option listed below (safe: unused and not a Brand Basics Brand Name).",
    "",
    "### Priority — obsolete after preferred-name normalize",
    "",
    ...obsoleteAliases.map((r) => `- [ ] ${r.name}`),
    "",
    "### All options to remove",
    "",
    ...remove.map((r) => `- [ ] ${r.name} _(${r.reason})_`),
    "",
  ].join("\n");

  fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));
  fs.writeFileSync(OUT_MD, md);
  console.log(JSON.stringify({ outJson: OUT_JSON, outMd: OUT_MD, removeCount: remove.length }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
