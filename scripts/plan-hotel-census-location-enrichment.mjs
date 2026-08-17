#!/usr/bin/env node
/**
 * Plan (and optionally apply) fill-blank Location (STR location type) enrichment.
 *
 * Usage:
 *   node scripts/plan-hotel-census-location-enrichment.mjs
 *   node scripts/plan-hotel-census-location-enrichment.mjs --apply
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import {
  buildLocationPeerIndex,
  proposeLocationType,
  validateLocationProposal,
  locationProposalToAirtableFields,
  isValidLocationType,
} from "../lib/hotel-census/location-enrichment-contract.js";

const APPLY = process.argv.includes("--apply");
const REPORT_DIR = path.join(process.cwd(), "reports");
const STAMP = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

const SELECT_FIELDS = [
  CENSUS_FIELDS.name,
  CENSUS_FIELDS.city,
  CENSUS_FIELDS.country,
  CENSUS_FIELDS.location,
  CENSUS_FIELDS.market,
  "Submarket",
  "Resort (Y/N)",
  CENSUS_FIELDS.chainScale,
  CENSUS_FIELDS.affiliation,
];

async function main() {
  const base = getPlatformBase();
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({ fields: SELECT_FIELDS, pageSize: 100 })
    .all();

  const peerIndex = buildLocationPeerIndex(records);
  const plans = [];
  const summary = {
    total: records.length,
    alreadyValid: 0,
    alreadyInvalid: 0,
    proposeFill: 0,
    noProposal: 0,
    bySource: {},
    byConfidence: {},
    applied: 0,
    applyErrors: 0,
  };

  for (const rec of records) {
    const row = rec.fields;
    const existing = String(row[CENSUS_FIELDS.location] ?? "").trim();

    if (existing && isValidLocationType(existing)) {
      summary.alreadyValid++;
      continue;
    }
    if (existing) summary.alreadyInvalid++;

    const proposal = proposeLocationType(row, peerIndex);
    if (!proposal.location) {
      summary.noProposal++;
      plans.push({
        recordId: rec.id,
        name: row[CENSUS_FIELDS.name],
        city: row[CENSUS_FIELDS.city],
        country: row[CENSUS_FIELDS.country],
        existingLocation: existing,
        status: "needs_review",
        skipped: proposal.skipped,
      });
      continue;
    }

    const validation = validateLocationProposal(proposal.location);
    if (!validation.pass) {
      summary.noProposal++;
      plans.push({
        recordId: rec.id,
        name: row[CENSUS_FIELDS.name],
        status: "validation_error",
        errors: validation.errors,
      });
      continue;
    }

    summary.proposeFill++;
    summary.bySource[proposal.source] = (summary.bySource[proposal.source] || 0) + 1;
    summary.byConfidence[proposal.confidence] =
      (summary.byConfidence[proposal.confidence] || 0) + 1;

    plans.push({
      recordId: rec.id,
      name: row[CENSUS_FIELDS.name],
      city: row[CENSUS_FIELDS.city],
      country: row[CENSUS_FIELDS.country],
      existingLocation: existing,
      proposedLocation: proposal.location,
      source: proposal.source,
      confidence: proposal.confidence,
      status: "ready",
      fields: locationProposalToAirtableFields(proposal),
    });
  }

  fs.mkdirSync(REPORT_DIR, { recursive: true });
  const jsonPath = path.join(REPORT_DIR, `hotel-census-location-plan-${STAMP}.json`);
  const csvPath = path.join(REPORT_DIR, `hotel-census-location-plan-${STAMP}.csv`);

  fs.writeFileSync(
    jsonPath,
    JSON.stringify({ generatedAt: new Date().toISOString(), apply: APPLY, summary, plans }, null, 2)
  );

  const csvHeader =
    "recordId,name,city,country,existingLocation,proposedLocation,source,confidence,status";
  const csvRows = plans.map((p) => {
    const esc = (v) => `"${String(v ?? "").replace(/"/g, '""')}"`;
    return [
      p.recordId,
      esc(p.name),
      esc(p.city),
      esc(p.country),
      esc(p.existingLocation),
      esc(p.proposedLocation),
      esc(p.source),
      esc(p.confidence),
      p.status,
    ].join(",");
  });
  fs.writeFileSync(csvPath, [csvHeader, ...csvRows].join("\n"));

  console.log("Hotel Census Location (STR location type) plan");
  console.log("  Total rows:", summary.total);
  console.log("  Already valid:", summary.alreadyValid);
  console.log("  Invalid existing:", summary.alreadyInvalid);
  console.log("  Proposals:", summary.proposeFill);
  console.log("  Needs review:", summary.noProposal);
  console.log("  By source:", summary.bySource);
  console.log("  By confidence:", summary.byConfidence);
  console.log("  Reports:", jsonPath, csvPath);

  if (!APPLY) {
    console.log("\nDry run. Pass --apply to write fill-blank Location values.");
    return;
  }

  const ready = plans.filter((p) => p.status === "ready");
  const BATCH = 10;
  for (let i = 0; i < ready.length; i += BATCH) {
    const chunk = ready.slice(i, i + BATCH);
    try {
      await base(HOTEL_CENSUS_TABLE).update(
        chunk.map((p) => ({ id: p.recordId, fields: p.fields }))
      );
      summary.applied += chunk.length;
    } catch (err) {
      summary.applyErrors += chunk.length;
      console.error("Apply batch failed:", err?.message || err);
    }
  }

  console.log("\nApply complete:", summary.applied, "updated,", summary.applyErrors, "errors");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
