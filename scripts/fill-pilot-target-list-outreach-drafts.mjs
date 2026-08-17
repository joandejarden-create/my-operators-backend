/**
 * Fill missing outreach draft fields on Pilot Target List rows (GTM base).
 *
 *   node scripts/fill-pilot-target-list-outreach-drafts.mjs --dry-run
 *   node scripts/fill-pilot-target-list-outreach-drafts.mjs --execute
 *   node scripts/fill-pilot-target-list-outreach-drafts.mjs --dry-run --record-id recXXX
 *   node scripts/fill-pilot-target-list-outreach-drafts.mjs --execute --limit 10
 *   node scripts/fill-pilot-target-list-outreach-drafts.mjs --execute --overwrite
 *
 * Reports:
 *   reports/pilot-target-list-draft-fill-report.json
 *   reports/pilot-target-list-draft-fill-report.md
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  GTM_PILOT_TARGET_LIST_TABLE,
  MAP_PILOT_TARGET_LIST,
} from "../lib/gtm-owner-target/pilot-target-list-field-map.js";
import {
  DEFAULT_ELIGIBLE_OUTREACH_STATUSES,
  buildDraftFillMarkdown,
  buildDraftFillPlan,
  hasMinimumContext,
  hasText,
  summarizePilotTargetRows,
} from "../lib/gtm-owner-target/pilot-target-list-draft-fill.js";
import {
  assertGtmBaseConfigured,
  assertNotProductBase,
  getGtmAirtableBase,
} from "../lib/gtm-owner-target/platform-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORT_JSON = path.join(ROOT, "reports", "pilot-target-list-draft-fill-report.json");
const REPORT_MD = path.join(ROOT, "reports", "pilot-target-list-draft-fill-report.md");

const GTM_COMPANIES_TABLE = process.env.AIRTABLE_GTM_COMPANIES_TABLE || "Companies";
const GTM_COMPANY_NAME_FIELD = process.env.AIRTABLE_GTM_COMPANY_NAME_FIELD || "Company";

function argValue(flag, fallback = "") {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return fallback;
  return process.argv[idx + 1] || fallback;
}

const EXECUTE = process.argv.includes("--execute");
const DRY_RUN = process.argv.includes("--dry-run") || !EXECUTE;
const OVERWRITE = process.argv.includes("--overwrite");
const RECORD_ID = argValue("--record-id", "");
const LIMIT = Number(argValue("--limit", "0")) || 0;
const SEGMENT_FILTER = argValue("--segment", "");
const STATUS_FILTER = argValue("--status", "");

async function buildCompanyNameMap(base, linkedCompanyIds) {
  const ids = [...new Set(linkedCompanyIds.filter(Boolean))];
  const map = new Map();
  if (!ids.length) return map;

  const chunkSize = 50;
  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
    const formula = `OR(${chunk.map((id) => `RECORD_ID()='${id}'`).join(",")})`;
    const records = await base(GTM_COMPANIES_TABLE)
      .select({ filterByFormula: formula, fields: [GTM_COMPANY_NAME_FIELD] })
      .all();
    for (const rec of records) {
      map.set(rec.id, String(rec.get(GTM_COMPANY_NAME_FIELD) || "").trim());
    }
  }
  return map;
}

function buildEligibleStatuses() {
  if (STATUS_FILTER) {
    return new Set(STATUS_FILTER.split(",").map((s) => s.trim()).filter(Boolean));
  }
  return DEFAULT_ELIGIBLE_OUTREACH_STATUSES;
}

async function fetchRecords(base) {
  if (RECORD_ID) {
    const rec = await base(GTM_PILOT_TARGET_LIST_TABLE).find(RECORD_ID);
    return [rec];
  }
  const rows = [];
  await base(GTM_PILOT_TARGET_LIST_TABLE)
    .select({})
    .eachPage((page, next) => {
      rows.push(...page);
      next();
    });
  return rows;
}

async function main() {
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();

  const allRecords = await fetchRecords(base);
  const inspectionSummary = summarizePilotTargetRows(allRecords.map((r) => ({ fields: r.fields })));

  const linkedCompanyIds = allRecords.flatMap((r) => {
    const company = r.get(MAP_PILOT_TARGET_LIST.company);
    return Array.isArray(company) ? company : [];
  });
  const companyNameById = await buildCompanyNameMap(base, linkedCompanyIds);
  const eligibleStatuses = buildEligibleStatuses();

  let candidates = allRecords.filter((r) => hasMinimumContext(r.fields));
  if (SEGMENT_FILTER) {
    candidates = candidates.filter(
      (r) => String(r.get(MAP_PILOT_TARGET_LIST.outreachSegment) || "") === SEGMENT_FILTER
    );
  }
  if (LIMIT > 0) candidates = candidates.slice(0, LIMIT);

  const plans = [];
  for (const rec of candidates) {
    const plan = buildDraftFillPlan({
      recordId: rec.id,
      fields: rec.fields,
      companyNameById,
      overwrite: OVERWRITE,
      eligibleStatuses,
    });
    if (SEGMENT_FILTER && plan.patch[MAP_PILOT_TARGET_LIST.outreachSegment] !== SEGMENT_FILTER) {
      if (!plan.skipped && !String(rec.get(MAP_PILOT_TARGET_LIST.outreachSegment) || "").includes(SEGMENT_FILTER)) {
        // After fill plan, segment might be inferred — filter post-plan for execute list
      }
    }
    plans.push(plan);
  }

  const updates = plans.filter((p) => !p.skipped && Object.keys(p.patch).length);
  const skipped = plans.filter((p) => p.skipped || Object.keys(p.patch).length === 0);
  const requiringReview = plans.filter((p) => !p.skipped && p.reviewReasons.length);

  console.log("Pilot Target List draft fill");
  console.log(`Mode: ${DRY_RUN ? "dry-run" : "execute"}`);
  console.log(`Total rows in table: ${inspectionSummary.totalRows}`);
  console.log(`Rows with context: ${inspectionSummary.eligibleContextRows}`);
  console.log(`Candidates processed: ${candidates.length}`);
  console.log(`Would update / updated: ${updates.length}`);
  console.log(`Skipped: ${skipped.length}`);
  console.log(`Requiring review: ${requiringReview.length}`);

  for (const plan of updates) {
    console.log(`\n${plan.name || plan.recordId} (${plan.recordId})`);
    for (const [field, change] of Object.entries(plan.changes)) {
      const afterPreview = String(change.after).slice(0, 80).replace(/\n/g, " ");
      console.log(`  ${field}: ${JSON.stringify(change.before)} → ${afterPreview}${String(change.after).length > 80 ? "…" : ""}`);
    }
  }

  const recordsUpdated = [];
  if (!DRY_RUN && updates.length) {
    for (const plan of updates) {
      const safePatch = { ...plan.patch };
      delete safePatch[MAP_PILOT_TARGET_LIST.finalApprovedEmail];
      delete safePatch[MAP_PILOT_TARGET_LIST.readyForMailMerge];
      if (safePatch[MAP_PILOT_TARGET_LIST.outreachStatus] === "Approved") {
        delete safePatch[MAP_PILOT_TARGET_LIST.outreachStatus];
      }

      if (!Object.keys(safePatch).length) continue;

      await base(GTM_PILOT_TARGET_LIST_TABLE).update(plan.recordId, safePatch);
      recordsUpdated.push({
        recordId: plan.recordId,
        name: plan.name,
        fieldsFilled: Object.keys(safePatch),
        changes: plan.changes,
        reviewReasons: plan.reviewReasons,
      });
      console.log("UPDATED", plan.recordId, Object.keys(safePatch).join(", "));
    }
  }

  const fieldsFilledCount = {};
  for (const plan of updates) {
    for (const field of Object.keys(plan.patch)) {
      fieldsFilledCount[field] = (fieldsFilledCount[field] || 0) + 1;
    }
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "execute",
    overwrite: OVERWRITE,
    baseId,
    tableName: GTM_PILOT_TARGET_LIST_TABLE,
    filters: {
      recordId: RECORD_ID || null,
      limit: LIMIT || null,
      segment: SEGMENT_FILTER || null,
      status: STATUS_FILTER || [...DEFAULT_ELIGIBLE_OUTREACH_STATUSES],
    },
    inspectionSummary,
    recordsInspected: candidates.length,
    recordsUpdated: DRY_RUN ? updates.length : recordsUpdated.length,
    recordsSkipped: skipped.map((p) => ({
      recordId: p.recordId,
      name: p.name,
      skipReason: p.skipReason || "no_changes",
    })),
    recordsRequiringReview: requiringReview.map((p) => ({
      recordId: p.recordId,
      name: p.name,
      reviewReasons: p.reviewReasons,
      segmentConfidence: p.segmentConfidence,
      fitConfidence: p.fitConfidence,
    })),
    rowsMissingEmail: inspectionSummary.missingEmail,
    rowsMissingLinkedInUrl: inspectionSummary.missingLinkedInUrl,
    doNotContactRows: inspectionSummary.doNotContact,
    segmentOrFitUncertain: requiringReview.filter(
      (p) => p.reviewReasons.includes("segment_unclear") || p.reviewReasons.includes("pilot_fit_unclear")
    ).length,
    fieldsFilled: fieldsFilledCount,
    updates: updates.map((p) => ({
      recordId: p.recordId,
      name: p.name,
      changes: p.changes,
      reviewReasons: p.reviewReasons,
    })),
    appliedUpdates: recordsUpdated,
  };

  fs.mkdirSync(path.dirname(REPORT_JSON), { recursive: true });
  fs.writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));
  fs.writeFileSync(REPORT_MD, buildDraftFillMarkdown(report), "utf8");

  console.log("\nWrote", REPORT_JSON);
  console.log("Wrote", REPORT_MD);

  if (DRY_RUN) {
    console.log("\nNo Airtable changes made (dry-run). Use --execute to apply.");
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
