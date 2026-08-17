#!/usr/bin/env node
/**
 * Plan / apply Dealality corridor backfill into Hotel Census `Submarket`.
 *
 * Usage:
 *   node scripts/plan-hotel-census-dealality-submarket.mjs
 *   node scripts/plan-hotel-census-dealality-submarket.mjs --overwrite-regional
 *   node scripts/plan-hotel-census-dealality-submarket.mjs --normalize-labels
 *   node scripts/plan-hotel-census-dealality-submarket.mjs --apply --overwrite-regional
 *   node scripts/plan-hotel-census-dealality-submarket.mjs --apply --full-census
 */

import fs from "node:fs";
import path from "node:path";
import "../load-env.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import {
  CENSUS_SUBMARKET_BACKFILL_FIELD,
  isCalaCensusCountry,
  isStrRegionalSubmarket,
  proposeCensusSubmarketCorridor,
  validateSubmarketCorridorProposal,
} from "../lib/hotel-census/census-dealality-submarket.js";

const APPLY = process.argv.includes("--apply");
const FULL_CENSUS = process.argv.includes("--full-census");
const OVERWRITE_REGIONAL =
  process.argv.includes("--overwrite-regional") || FULL_CENSUS;
const NORMALIZE_LABELS =
  process.argv.includes("--normalize-labels") || FULL_CENSUS;
const FORCE = process.argv.includes("--force");
const ASSIGN_OTHER =
  process.argv.includes("--assign-unmapped-other") || FULL_CENSUS;
const CALA_ONLY = !process.argv.includes("--all-countries") && !FULL_CENSUS;
const minConfidenceArg = process.argv.find((a) => a.startsWith("--min-confidence="));
const MIN_CONFIDENCE = minConfidenceArg
  ? minConfidenceArg.split("=")[1]
  : FULL_CENSUS
    ? "Low"
    : "Medium";

const REPORT_DIR = path.join(process.cwd(), "reports");
const STAMP = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
const BATCH = 10;

const SELECT_FIELDS = [
  CENSUS_FIELDS.name,
  CENSUS_FIELDS.city,
  CENSUS_FIELDS.country,
  CENSUS_FIELDS.market,
  CENSUS_FIELDS.submarket,
];

function csvEscape(value) {
  const s = String(value ?? "");
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

async function main() {
  const base = getPlatformBase();
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({ fields: SELECT_FIELDS, pageSize: 100 })
    .all();

  const plans = [];
  const summary = {
    total: records.length,
    inScope: 0,
    propose: 0,
    skipped: 0,
    validationErrors: 0,
    applied: 0,
    applyErrors: 0,
    bySource: {},
    byCountry: {},
  };

  for (const rec of records) {
    const row = rec.fields;
    const country = String(row[CENSUS_FIELDS.country] || "").trim();
    if (CALA_ONLY && !isCalaCensusCountry(country)) continue;
    summary.inScope += 1;

    const proposal = proposeCensusSubmarketCorridor(
      { ...row, id: rec.id },
      {
        force: FORCE,
        overwriteRegional: OVERWRITE_REGIONAL,
        normalizeLabels: NORMALIZE_LABELS,
        minConfidence: MIN_CONFIDENCE,
        assignUnmappedOther: ASSIGN_OTHER,
      }
    );

    if (proposal.skipped || !proposal.submarket) {
      summary.skipped += 1;
      continue;
    }

    const fields = { [CENSUS_SUBMARKET_BACKFILL_FIELD]: proposal.submarket };
    const validation = validateSubmarketCorridorProposal(fields, country);
    if (!validation.pass) {
      summary.validationErrors += 1;
      plans.push({
        recordId: rec.id,
        name: row[CENSUS_FIELDS.name],
        country,
        city: row[CENSUS_FIELDS.city],
        currentSubmarket: row[CENSUS_FIELDS.submarket],
        status: "validation_error",
        errors: validation.errors.join("; "),
        proposed: fields,
      });
      continue;
    }

    summary.propose += 1;
    summary.bySource[proposal.source] = (summary.bySource[proposal.source] || 0) + 1;
    summary.byCountry[country] = (summary.byCountry[country] || 0) + 1;

    plans.push({
      recordId: rec.id,
      name: row[CENSUS_FIELDS.name],
      country,
      city: row[CENSUS_FIELDS.city],
      currentSubmarket: row[CENSUS_FIELDS.submarket],
      strRegional: isStrRegionalSubmarket(row[CENSUS_FIELDS.submarket]),
      proposed: fields,
      confidence: proposal.confidence,
      reason: proposal.reason,
      source: proposal.source,
      status: "ready",
    });
  }

  fs.mkdirSync(REPORT_DIR, { recursive: true });
  const planPath = path.join(REPORT_DIR, `hotel-census-submarket-corridor-plan-${STAMP}.json`);
  fs.writeFileSync(planPath, JSON.stringify({ summary, plans }, null, 2));

  const csvPath = path.join(REPORT_DIR, `hotel-census-submarket-corridor-plan-${STAMP}.csv`);
  const header = [
    "status",
    "recordId",
    "name",
    "country",
    "city",
    "currentSubmarket",
    "strRegional",
    "proposedSubmarket",
    "confidence",
    "source",
    "reason",
  ];
  const lines = [header.join(",")];
  for (const p of plans) {
    lines.push(
      [
        p.status,
        p.recordId,
        csvEscape(p.name),
        csvEscape(p.country),
        csvEscape(p.city),
        csvEscape(p.currentSubmarket),
        p.strRegional ? "yes" : "no",
        csvEscape(p.proposed?.[CENSUS_SUBMARKET_BACKFILL_FIELD]),
        p.confidence,
        p.source,
        p.reason,
      ].join(",")
    );
  }
  fs.writeFileSync(csvPath, lines.join("\n"));

  console.log("Hotel Census Submarket corridor backfill plan");
  console.log("  Target field:", CENSUS_SUBMARKET_BACKFILL_FIELD);
  console.log("  In scope:", summary.inScope);
  console.log("  Proposed:", summary.propose);
  console.log("  Skipped:", summary.skipped);
  console.log("  Validation errors:", summary.validationErrors);
  console.log("  Plan JSON:", planPath);
  console.log("  Plan CSV:", csvPath);

  if (!APPLY) {
    console.log("\nDry run — pass --apply to write proposals to Submarket in Airtable.");
    return;
  }

  const ready = plans.filter((p) => p.status === "ready");
  for (let i = 0; i < ready.length; i += BATCH) {
    const chunk = ready.slice(i, i + BATCH);
    try {
      await base(HOTEL_CENSUS_TABLE).update(
        chunk.map((p) => ({
          id: p.recordId,
          fields: p.proposed,
        }))
      );
      summary.applied += chunk.length;
    } catch (err) {
      summary.applyErrors += chunk.length;
      console.error("[apply] batch failed:", err.message || err);
    }
  }

  console.log("\nApply complete");
  console.log("  Applied:", summary.applied);
  console.log("  Apply errors:", summary.applyErrors);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
