#!/usr/bin/env node
/**
 * Reprocess persisted Bethesda/DMV opportunities through qualification-precision gates.
 * Usage:
 *   node scripts/gdi-apply-qualification-precision.mjs --dry-run
 *   node scripts/gdi-apply-qualification-precision.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { applyQualificationPrecisionPass } from "../lib/group-demand-intelligence/qualification-precision-pass.js";
import { PILOT_HOTEL_ID } from "../lib/group-demand-intelligence/hotel-profile.js";
import { createId } from "../lib/group-demand-intelligence/repository.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const hotelId = PILOT_HOTEL_ID;
const oppPath = path.join(
  root,
  "data/group-demand-intelligence/hotels",
  hotelId,
  "opportunities.json"
);
const apply = process.argv.includes("--apply");
const dryRun = !apply || process.argv.includes("--dry-run");

const doc = JSON.parse(fs.readFileSync(oppPath, "utf8"));
const beforeOps = doc.opportunities || [];
const beforeCounts = {
  HIGH_PRIORITY: beforeOps.filter((o) => o.priority === "HIGH_PRIORITY").length,
  MEDIUM_PRIORITY: beforeOps.filter((o) => o.priority === "MEDIUM_PRIORITY").length,
  WATCHLIST: beforeOps.filter((o) => o.priority === "WATCHLIST").length,
  DISQUALIFIED: beforeOps.filter((o) => o.priority === "DISQUALIFIED").length,
};

const result = applyQualificationPrecisionPass(beforeOps);
const afterCounts = result.enrichment.priorityCounts;

const snapshotDir = path.join(root, "reports/group-demand-intelligence");
fs.mkdirSync(snapshotDir, { recursive: true });
const snapshotPath = path.join(
  snapshotDir,
  "bethesda-qualification-precision-before-after.json"
);
fs.writeFileSync(
  snapshotPath,
  JSON.stringify(
    {
      hotelId,
      generatedAt: new Date().toISOString(),
      webhoundSpentUsd: 0,
      beforeCounts,
      afterCounts,
      beforeAfter: result.beforeAfter,
      highAfter: result.opportunities
        .filter((o) => o.priority === "HIGH_PRIORITY")
        .map((o) => ({
          id: o.id,
          title: o.title,
          opportunityType: o.opportunityType,
          venueSourcingStatus: o.venueSourcingStatus,
          roomDemandStatus: o.roomDemandStatus,
          opportunityQualification: o.opportunityQualification,
          hotelFitScore: o.hotelFitScore,
          evidenceConfidence: o.evidenceConfidence,
          contactQuality: o.contactQuality,
          whyNow: o.whyNow,
          recommendedAction: o.recommendedAction,
          hotelOpportunityThesis: o.hotelOpportunityThesis,
        })),
    },
    null,
    2
  )
);

console.log(JSON.stringify({ dryRun: dryRun && !apply, beforeCounts, afterCounts, snapshotPath }, null, 2));

if (apply) {
  const runId = createId("gdi_run");
  const next = {
    ...doc,
    hotelId,
    runId,
    updatedAt: new Date().toISOString(),
    opportunities: result.opportunities,
    enrichment: {
      ...(doc.enrichment || {}),
      qualificationPrecision: result.enrichment,
    },
  };
  fs.writeFileSync(oppPath, JSON.stringify(next, null, 2));
  const runDir = path.join(
    root,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "runs",
    runId
  );
  fs.mkdirSync(runDir, { recursive: true });
  fs.writeFileSync(
    path.join(runDir, "run.json"),
    JSON.stringify(
      {
        runId,
        hotelId,
        kind: "qualification_precision_reprocess",
        webhoundSpentUsd: 0,
        beforeCounts,
        afterCounts,
        opportunityCount: result.opportunities.length,
        createdAt: new Date().toISOString(),
      },
      null,
      2
    )
  );
  console.log(`Applied run ${runId} → ${oppPath}`);
} else {
  console.log("Dry-run only. Pass --apply to persist.");
}
