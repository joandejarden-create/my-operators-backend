/**
 * Read-only Active/Live Brand Setup - Deal Terms gap audit + Meta vs form select diff.
 *
 *   node scripts/audit-active-live-deal-terms-gaps.mjs
 *
 * Writes:
 *   reports/deal-terms-active-live-gap-audit.json
 *   reports/deal-terms-active-live-gap-audit.md
 *   reports/deal-terms-select-option-proposals.md
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { BRAND_STATUS_ACTIVE_FORMULA } from "../lib/brand-status-active.js";
import {
  TABLE_BASICS,
  TABLE_DEAL,
  LINK_FIELD_DEAL,
  DEAL_TERMS_WRITE_COLUMNS,
  DEAL_TERMS_SELECT_COLUMNS,
  DEAL_TERMS_FORM_SELECT_OPTIONS,
  isEmptyDealValue,
} from "./lib/deal-terms-field-contract.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_JSON = path.join(ROOT, "reports", "deal-terms-active-live-gap-audit.json");
const OUT_MD = path.join(ROOT, "reports", "deal-terms-active-live-gap-audit.md");
const OUT_PROPOSALS = path.join(ROOT, "reports", "deal-terms-select-option-proposals.md");

async function getMetaTable(baseId, apiKey, tableName) {
  const r = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  if (!r.ok) throw new Error(`Meta API ${r.status}: ${await r.text()}`);
  const j = await r.json();
  const t = j.tables.find((x) => x.name === tableName);
  if (!t) throw new Error(`Table not found: ${tableName}`);
  return t;
}

function fieldChoices(metaTable, fieldName) {
  const f = metaTable.fields.find((x) => x.name === fieldName);
  if (!f) return { exists: false, type: null, choices: [] };
  const choices = (f.options?.choices || []).map((c) => c.name).filter((c) => String(c).trim() !== "");
  return { exists: true, type: f.type, choices };
}

async function loadActiveBasics(base) {
  const rows = [];
  await base(TABLE_BASICS)
    .select({
      filterByFormula: BRAND_STATUS_ACTIVE_FORMULA,
      fields: ["Brand Name", "Brand Status", LINK_FIELD_DEAL],
    })
    .eachPage((page, next) => {
      rows.push(...page);
      next();
    });
  return rows;
}

function brandSlug(basics) {
  return String(basics.fields["Brand Name"] || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  const base = new Airtable({ apiKey }).base(baseId);
  const metaDeal = await getMetaTable(baseId, apiKey, TABLE_DEAL);
  const basicsRows = await loadActiveBasics(base);

  const metaSelect = {};
  const selectDiffs = [];
  for (const col of DEAL_TERMS_SELECT_COLUMNS) {
    const meta = fieldChoices(metaDeal, col);
    metaSelect[col] = meta;
    const formOpts = DEAL_TERMS_FORM_SELECT_OPTIONS[col] || [];
    const metaSet = new Set(meta.choices.map((c) => c.toLowerCase()));
    const formSet = new Set(formOpts.map((c) => c.toLowerCase()));
    const inFormNotMeta = formOpts.filter((c) => !metaSet.has(c.toLowerCase()));
    const inMetaNotForm = meta.choices.filter((c) => !formSet.has(c.toLowerCase()));
    if (!meta.exists || inFormNotMeta.length || inMetaNotForm.length) {
      selectDiffs.push({
        column: col,
        metaExists: meta.exists,
        metaType: meta.type,
        inFormNotMeta,
        inMetaNotForm,
        formOptions: formOpts,
        metaChoices: meta.choices,
      });
    }
  }

  const missingWriteColumns = DEAL_TERMS_WRITE_COLUMNS.filter(
    (c) => !metaDeal.fields.some((f) => f.name === c)
  );

  const brands = [];
  for (const row of basicsRows) {
    const name = String(row.fields["Brand Name"] || "").trim();
    const link = row.fields[LINK_FIELD_DEAL];
    const dealId = Array.isArray(link) && link[0] ? link[0] : null;
    const entry = {
      basicsRecordId: row.id,
      name,
      slug: brandSlug(row),
      status: row.fields["Brand Status"] || null,
      dealTermsRecordId: dealId,
      linked: Boolean(dealId),
      emptyColumns: [],
      filledColumns: [],
      filledCount: 0,
      emptyCount: 0,
      completenessPct: 0,
    };

    if (!dealId) {
      entry.emptyColumns = [...DEAL_TERMS_WRITE_COLUMNS];
      entry.emptyCount = DEAL_TERMS_WRITE_COLUMNS.length;
      brands.push(entry);
      continue;
    }

    const deal = await base(TABLE_DEAL).find(dealId);
    for (const col of DEAL_TERMS_WRITE_COLUMNS) {
      const v = deal.fields[col];
      if (isEmptyDealValue(v)) entry.emptyColumns.push(col);
      else {
        entry.filledColumns.push(col);
        entry.filledCount += 1;
      }
    }
    entry.emptyCount = entry.emptyColumns.length;
    entry.completenessPct = Math.round(
      (100 * entry.filledCount) / DEAL_TERMS_WRITE_COLUMNS.length
    );
    brands.push(entry);
  }

  brands.sort((a, b) => a.name.localeCompare(b.name));

  const summary = {
    generatedAt: new Date().toISOString(),
    activeLiveCount: brands.length,
    writeColumnCount: DEAL_TERMS_WRITE_COLUMNS.length,
    brandsWithDealLink: brands.filter((b) => b.linked).length,
    brandsMissingDealLink: brands.filter((b) => !b.linked).map((b) => b.name),
    fullyEmpty: brands.filter((b) => b.filledCount === 0).map((b) => b.name),
    fullyComplete: brands.filter((b) => b.emptyCount === 0).map((b) => b.name),
    avgCompletenessPct: brands.length
      ? Math.round(
          brands.reduce((s, b) => s + b.completenessPct, 0) / brands.length
        )
      : 0,
    missingWriteColumnsInMeta: missingWriteColumns,
    selectDiffCount: selectDiffs.length,
  };

  const report = {
    summary,
    writeColumns: DEAL_TERMS_WRITE_COLUMNS,
    metaSelect,
    selectDiffs,
    brands,
  };

  fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
  fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));

  const md = [];
  md.push(`# Deal Terms — Active/Live Gap Audit`);
  md.push("");
  md.push(`Generated: ${summary.generatedAt}`);
  md.push("");
  md.push(`| Metric | Value |`);
  md.push(`|--------|-------|`);
  md.push(`| Active/Live brands | ${summary.activeLiveCount} |`);
  md.push(`| Linked Deal Terms rows | ${summary.brandsWithDealLink} |`);
  md.push(`| Avg completeness | ${summary.avgCompletenessPct}% |`);
  md.push(`| Fully empty | ${summary.fullyEmpty.length} |`);
  md.push(`| Fully complete | ${summary.fullyComplete.length} |`);
  md.push(`| Meta↔form select diffs | ${summary.selectDiffCount} |`);
  md.push("");
  md.push(`## Per brand`);
  md.push("");
  md.push(`| Brand | Completeness | Empty | Linked |`);
  md.push(`|-------|--------------|-------|--------|`);
  for (const b of brands) {
    md.push(
      `| ${b.name} | ${b.completenessPct}% (${b.filledCount}/${DEAL_TERMS_WRITE_COLUMNS.length}) | ${b.emptyCount} | ${b.linked ? "yes" : "**NO**"} |`
    );
  }
  if (missingWriteColumns.length) {
    md.push("");
    md.push(`## Missing columns in Meta`);
    md.push("");
    for (const c of missingWriteColumns) md.push(`- ${c}`);
  }
  if (selectDiffs.length) {
    md.push("");
    md.push(`## Select Meta ↔ form diffs`);
    md.push("");
    for (const d of selectDiffs) {
      md.push(`### ${d.column}`);
      md.push(`- Meta exists: ${d.metaExists} (${d.metaType || "n/a"})`);
      if (d.inFormNotMeta.length) md.push(`- In form, not Meta: ${d.inFormNotMeta.join("; ")}`);
      if (d.inMetaNotForm.length) md.push(`- In Meta, not form: ${d.inMetaNotForm.join("; ")}`);
    }
  }
  fs.writeFileSync(OUT_MD, md.join("\n") + "\n");

  const prop = [];
  prop.push(`# Deal Terms — Select Option Proposals`);
  prop.push("");
  prop.push(`**Status:** proposals only — do **not** add to Airtable Meta or Brand Setup HTML until founder approval, then update every required surface.`);
  prop.push("");
  prop.push(`Generated: ${summary.generatedAt}`);
  prop.push("");
  prop.push(`## Observed Meta ↔ form diffs`);
  prop.push("");
  if (!selectDiffs.length) {
    prop.push(`No Meta↔form mismatches for mapped Deal Terms selects in this snapshot.`);
  } else {
    for (const d of selectDiffs) {
      prop.push(`### ${d.column}`);
      prop.push(`- Form options: ${(d.formOptions || []).join(" | ") || "(none)"}`);
      prop.push(`- Meta choices: ${(d.metaChoices || []).join(" | ") || "(none / missing field)"}`);
      if (d.inFormNotMeta.length) {
        prop.push(`- **Propose adding to Meta:** ${d.inFormNotMeta.map((x) => `\`${x}\``).join(", ")}`);
      }
      if (d.inMetaNotForm.length) {
        prop.push(
          `- **Propose adding to Brand Setup HTML (and mirrors):** ${d.inMetaNotForm.map((x) => `\`${x}\``).join(", ")}`
        );
      }
      prop.push("");
    }
  }
  prop.push(`## Termination fields missing from Deal Terms table`);
  prop.push("");
  prop.push(
    `These columns are mapped in \`DEAL_TERMS_FORM_TO_AIRTABLE\` / Brand Setup form but **do not exist** on Airtable \`Brand Setup - Deal Terms\` (writes are skipped). They may already live on **Fee Structure** — do not invent Deal Terms columns without approval.`
  );
  prop.push("");
  prop.push(`- \`Typical Termination Fee Structure (if any)\``);
  prop.push(`- \`Typical Termination Fee Structure (if any) Text\``);
  prop.push(`- \`Who Can Exercise Termination Right After Failed Test?\``);
  prop.push("");
  prop.push(`## Suggested new options (not present in form today)`);
  prop.push("");
  prop.push(`These map real franchise language better than current choices. **Do not add** until approved.`);
  prop.push("");
  prop.push(`| Field | Proposed option | Why | Interim fill (existing option) |`);
  prop.push(`|-------|-----------------|-----|--------------------------------|`);
  prop.push(
    `| Renewal Structure | \`No contractual renewal / re-license only\` | Matches Kimpton/IHG/Hilton FDD Item 17 (no automatic renewal; re-licensing) | \`Renewal by Mutual Agreement Only\` + nuance in Typical Renewal Conditions |`
  );
  prop.push(
    `| Who Can Exercise Termination Right After Failed Test? | \`Either party (with cure)\` | Common FDD framing | \`Mutual\` |`
  );
  prop.push(
    `| Typical Termination Fee Structure (if any) | \`Liquidated damages / lost future fees\` | Common franchise exit economics | \`Allowed With X Months Fees\` + notes text |`
  );
  prop.push("");
  prop.push(`## Surfaces to update if any proposal is approved`);
  prop.push("");
  prop.push(`1. Airtable Meta — \`Brand Setup - Deal Terms\` single-select choices`);
  prop.push(`2. [\`public/brand-setup.html\`](../public/brand-setup.html) Deal Terms / Fee Structure selects`);
  prop.push(`3. Operator intake mirrors if they share the same option lists`);
  prop.push(`4. Writers: \`scripts/lib/deal-terms-field-contract.mjs\`, Kimpton/Choice/Active-Live apply profiles`);
  prop.push(`5. Tests / dry-run fixtures that assert option strings`);
  prop.push("");
  fs.writeFileSync(OUT_PROPOSALS, prop.join("\n"));

  console.log(`Active/Live=${summary.activeLiveCount} avgCompleteness=${summary.avgCompletenessPct}%`);
  console.log(`Wrote ${OUT_JSON}`);
  console.log(`Wrote ${OUT_MD}`);
  console.log(`Wrote ${OUT_PROPOSALS}`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
