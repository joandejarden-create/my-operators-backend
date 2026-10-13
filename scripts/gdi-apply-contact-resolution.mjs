#!/usr/bin/env node
/**
 * Post-qualification contact resolution for Bethesda/DMV qualified opportunities.
 * Usage:
 *   node scripts/gdi-apply-contact-resolution.mjs --dry-run
 *   node scripts/gdi-apply-contact-resolution.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { applyContactResolutionPass } from "../lib/group-demand-intelligence/contact-resolution-pass.js";
import { PILOT_HOTEL_ID } from "../lib/group-demand-intelligence/hotel-profile.js";
import { createId } from "../lib/group-demand-intelligence/repository.js";
import { PRIORITY } from "../lib/group-demand-intelligence/claim-types.js";
import { shouldEnrichContact } from "../lib/group-demand-intelligence/contact-resolution.js";

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
const result = applyContactResolutionPass(beforeOps);

const snapshotDir = path.join(root, "reports/group-demand-intelligence");
fs.mkdirSync(snapshotDir, { recursive: true });
const snapshotPath = path.join(
  snapshotDir,
  "bethesda-contact-resolution-before-after.json"
);

const highAfter = result.opportunities
  .filter((o) => o.priority === PRIORITY.HIGH)
  .map((o) => ({
    id: o.id,
    title: o.title,
    contact: o.primaryContact?.name,
    titleRole: o.primaryContact?.role,
    relationship: o.primaryContact?.relationshipToEvent,
    email: o.primaryContact?.email,
    emailVerification: o.primaryContact?.emailVerificationStatusLabel,
    phone: o.primaryContact?.phone,
    phoneType: o.primaryContact?.phoneTypeLabel,
    contactGrade: o.contactGrade,
    contactConfidence: o.contactConfidence,
    whyThisContact: o.whyThisContact,
    source: o.primaryContact?.sourceUrl || o.primaryContact?.source,
  }));

const strongMedium = result.opportunities
  .filter(
    (o) =>
      o.priority === PRIORITY.MEDIUM &&
      shouldEnrichContact(o) &&
      (o.opportunityQualification === "VERIFIED_OPEN" ||
        o.opportunityQualification === "STRONG" ||
        o.opportunityQualification === "MODERATE")
  )
  .sort((a, b) => (b.hotelFitScore || 0) - (a.hotelFitScore || 0))
  .slice(0, 10)
  .map((o) => ({
    id: o.id,
    title: o.title,
    qualification: o.opportunityQualification,
    contact: o.primaryContact?.name,
    role: o.primaryContact?.role,
    email: o.primaryContact?.email,
    emailVerification: o.primaryContact?.emailVerificationStatusLabel,
    phone: o.primaryContact?.phone,
    phoneType: o.primaryContact?.phoneTypeLabel,
    contactGrade: o.contactGrade,
    contactConfidence: o.contactConfidence,
    whyThisContact: o.whyThisContact,
  }));

fs.writeFileSync(
  snapshotPath,
  JSON.stringify(
    {
      hotelId,
      generatedAt: new Date().toISOString(),
      enrichment: result.enrichment,
      beforeAfter: result.beforeAfter,
      highAfter,
      strongMedium,
    },
    null,
    2
  )
);

console.log(
  JSON.stringify(
    {
      dryRun: dryRun && !apply,
      priorityCounts: result.enrichment.priorityCounts,
      metricsBefore: result.enrichment.metricsBefore,
      metricsAfter: result.enrichment.metricsAfter,
      contactResearchCostUsd: result.enrichment.contactResearchCostUsd,
      snapshotPath,
    },
    null,
    2
  )
);

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
      contactResolution: result.enrichment,
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
        kind: "contact_resolution_v1",
        webhoundSpentUsd: 0,
        contactResearchCostUsd: result.enrichment.contactResearchCostUsd,
        metricsBefore: result.enrichment.metricsBefore,
        metricsAfter: result.enrichment.metricsAfter,
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
