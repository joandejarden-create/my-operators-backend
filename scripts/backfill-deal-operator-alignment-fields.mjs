#!/usr/bin/env node
/**
 * Backfill Phase 5B operator-alignment deal fields (Strategic Intent + Deals).
 *
 *   node scripts/backfill-deal-operator-alignment-fields.mjs --deal-id recIeGRZP21udmTnt
 *   node scripts/backfill-deal-operator-alignment-fields.mjs --deal-id recIeGRZP21udmTnt --apply
 *   node scripts/backfill-deal-operator-alignment-fields.mjs --sample-deals --apply
 *   node scripts/backfill-deal-operator-alignment-fields.mjs --sample-deals --apply --overwrite
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  OAS_DEAL_SI_FIELD_NAMES as SI,
  OAS_DEAL_DEALS_FIELD_NAMES as DEALS,
} from "../lib/operator-alignment-field-options.js";
import {
  loadAllSamplePlans,
  planAeropuertoCancun,
  buildBackfillPlanFromMerged,
  mergeFixtureFields,
  validateProposalValue,
  normalizeProposalForWrite,
  OAS_SI_BACKFILL_COLUMNS,
  OAS_DEALS_BACKFILL_COLUMNS,
} from "../lib/operator-alignment-deal-backfill-plans.js";
import { getLiveOperatorAlignmentOptions } from "../lib/operator-alignment-airtable-options-loader.js";
import { fetchDealScoringContext } from "../api/my-deals.js";
import { STRATEGIC_INTENT_LINK_FIELD, DEALS_TABLE } from "../api/schemas/deal-setup-fields.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const args = process.argv.slice(2);
const APPLY = args.includes("--apply");
const OVERWRITE = args.includes("--overwrite");
const SAMPLE_DEALS = args.includes("--sample-deals");
const dealIdArg = (() => {
  const i = args.indexOf("--deal-id");
  return i >= 0 ? args[i + 1] : null;
})();

function isEmpty(v) {
  if (v == null) return true;
  if (Array.isArray(v)) return v.length === 0;
  return String(v).trim() === "";
}

function enc(t) {
  return encodeURIComponent(String(t));
}

async function fetchRecord(table, recordId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const url = `https://api.airtable.com/v0/${baseId}/${enc(table)}/${enc(recordId)}`;
  const r = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const j = await r.json();
  if (!r.ok) throw new Error(j.error?.message || `fetch ${table} ${r.status}`);
  return j;
}

async function patchRecord(table, recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const url = `https://api.airtable.com/v0/${baseId}/${enc(table)}/${enc(recordId)}`;
  const r = await fetch(url, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields }),
  });
  const j = await r.json();
  if (!r.ok) throw new Error(j.error?.message || `patch ${table} ${r.status}`);
  return j;
}

function getSiId(dealFields) {
  const raw = dealFields[STRATEGIC_INTENT_LINK_FIELD];
  return Array.isArray(raw) && raw[0] ? raw[0] : null;
}

function planToPayload(plan, liveIndex, optionValidation) {
  const si = {};
  const deals = {};
  for (const [col, prop] of Object.entries(plan.fields || {})) {
    const val = validateProposalValue(col, prop.value, liveIndex);
    optionValidation.push({
      field: col,
      proposed: prop.value,
      ok: val.ok,
      source: val.source,
      bad: val.bad,
      normalized: val.normalized,
    });
    if (!val.ok) throw new Error(`Invalid option for ${col}: ${(val.bad || []).join(", ")}`);
    const writeVal = liveIndex ? normalizeProposalForWrite(col, prop.value, liveIndex) : prop.value;
    if (OAS_SI_BACKFILL_COLUMNS.includes(col)) si[col] = writeVal;
    if (OAS_DEALS_BACKFILL_COLUMNS.includes(col)) deals[col] = writeVal;
  }
  return { si, deals };
}

async function processDeal(dealId, plan, report, liveIndex) {
  const ctx = await fetchDealScoringContext(process.env.AIRTABLE_BASE_ID, process.env.AIRTABLE_API_KEY, dealId);
  if (!ctx) {
    report.rows.push({ dealId, status: "error", error: "Deal not found" });
    return;
  }
  const siId = getSiId(ctx.dealFields);
  if (!siId) {
    report.rows.push({ dealId, status: "error", error: "No Strategic Intent link" });
    return;
  }

  const siBefore = (await fetchRecord("Strategic Intent - Operational - Key Challenges", siId)).fields || {};
  const dealBefore = (await fetchRecord(DEALS_TABLE, dealId)).fields || {};

  if (plan.skip) {
    report.rows.push({
      dealId,
      projectName: plan.projectName,
      status: "skipped",
      reason: plan.skipReason,
    });
    return;
  }

  const optionValidation = [];
  const { si: siPayload, deals: dealsPayload } = planToPayload(plan, liveIndex, optionValidation);
  report.optionValidation = report.optionValidation || [];
  report.optionValidation.push({ dealId, fields: optionValidation });
  const changes = [];

  for (const [col, val] of Object.entries(siPayload)) {
    const before = siBefore[col];
    if (!OVERWRITE && !isEmpty(before)) {
      changes.push({ table: "SI", field: col, action: "skip_existing", before, after: val });
      continue;
    }
    changes.push({ table: "SI", field: col, action: isEmpty(before) ? "set" : "overwrite", before, after: val });
  }
  for (const [col, val] of Object.entries(dealsPayload)) {
    const before = dealBefore[col];
    if (!OVERWRITE && !isEmpty(before)) {
      changes.push({ table: "Deals", field: col, action: "skip_existing", before, after: val });
      continue;
    }
    changes.push({ table: "Deals", field: col, action: isEmpty(before) ? "set" : "overwrite", before, after: val });
  }

  const row = {
    dealId,
    projectName: plan.projectName || ctx.dealFields?.["Property Name"] || dealId,
    slug: plan.slug,
    status: APPLY ? "applied" : "dry_run",
    changes,
    proposals: Object.fromEntries(
      Object.entries(plan.fields).map(([k, v]) => [
        k,
        { value: v.value, source: v.source, confidence: v.confidence, note: v.note },
      ])
    ),
    notes: plan.notes,
  };
  report.rows.push(row);

  if (!APPLY) return;

  const siPatch = {};
  const dealPatch = {};
  for (const c of changes) {
    if (c.action === "skip_existing") continue;
    if (c.table === "SI") siPatch[c.field] = c.after;
    if (c.table === "Deals") dealPatch[c.field] = c.after;
  }
  if (Object.keys(siPatch).length) await patchRecord("Strategic Intent - Operational - Key Challenges", siId, siPatch);
  if (Object.keys(dealPatch).length) await patchRecord(DEALS_TABLE, dealId, dealPatch);
}

async function main() {
  if (!process.env.AIRTABLE_BASE_ID || !process.env.AIRTABLE_API_KEY) {
    throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");
  }

  const liveIndex = await getLiveOperatorAlignmentOptions({ refresh: true });
  const plans = new Map();
  if (SAMPLE_DEALS) {
    for (const [id, p] of loadAllSamplePlans(ROOT)) plans.set(id, p);
  } else if (dealIdArg) {
    if (dealIdArg === "recIeGRZP21udmTnt") plans.set(dealIdArg, planAeropuertoCancun());
    else {
      const ctx = await fetchDealScoringContext(process.env.AIRTABLE_BASE_ID, process.env.AIRTABLE_API_KEY, dealIdArg);
      const merged = {
        ...(ctx?.locationData || {}),
        ...(ctx?.dealFields || {}),
        ...(ctx?.mpData || {}),
        ...(ctx?.siData || {}),
      };
      plans.set(
        dealIdArg,
        buildBackfillPlanFromMerged(merged, { dealId: dealIdArg, projectName: merged["Property Name"] })
      );
    }
  } else {
    console.error("Use --deal-id <rec...> or --sample-deals");
    process.exit(1);
  }

  const report = {
    mode: APPLY ? "apply" : "dry_run",
    overwrite: OVERWRITE,
    at: new Date().toISOString(),
    rows: [],
  };

  for (const [dealId, plan] of plans) {
    console.log("\n---", dealId, plan.projectName || plan.slug || "", "---");
    if (plan.skip) {
      console.log("SKIP:", plan.skipReason);
    }
    await processDeal(dealId, plan, report, liveIndex);
    const last = report.rows[report.rows.length - 1];
    if (last?.changes) {
      for (const c of last.changes) {
        console.log(`  ${c.action}\t${c.table}\t${c.field}`);
      }
    }
  }

  const stamp = new Date().toISOString().slice(0, 19).replace(/:/g, "");
  const reportPath = path.join(ROOT, "reports", `deal-operator-alignment-backfill-${stamp}.json`);
  const validationPath = path.join(
    ROOT,
    "reports",
    `deal-operator-alignment-backfill-option-validation-${stamp}.json`
  );
  fs.mkdirSync(path.join(ROOT, "reports"), { recursive: true });
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
  fs.writeFileSync(
    validationPath,
    JSON.stringify(
      {
        at: report.at,
        mode: report.mode,
        liveExportedAt: liveIndex.exportedAt,
        optionValidation: report.optionValidation || [],
      },
      null,
      2
    )
  );
  console.log("\nReport:", reportPath);
  console.log("Option validation:", validationPath);
  if (!APPLY) console.log("Dry run only. Re-run with --apply to write.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
