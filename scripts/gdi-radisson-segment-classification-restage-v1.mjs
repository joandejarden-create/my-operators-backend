#!/usr/bin/env node
/**
 * Reclassify Radisson Blu Toronto staged opportunities by primary demand
 * mechanism and write a NEW immutable STAGED package. Preserves prior package.
 *
 * No Airtable writes. No CUSTOMER_PUBLISHED. No paid research.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { applyPrimaryDemandSegmentClassification } from "../lib/group-demand-intelligence/demand-segments/primary-demand-segment-classification-v1.js";
import { classifyOpportunityTypeFilter } from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";
import {
  buildCustomerPublicationPackage,
} from "../lib/group-demand-intelligence/publication-package/gdi-customer-publication-package-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/gdi-publication-package-store-v1.js";
import { GDI_PUBLICATION_LIFECYCLE_STATE } from "../lib/group-demand-intelligence/publication-package/gdi-publication-lifecycle-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recH9h3McgMNFQEla";
const PRIOR_FP =
  "ac55190dd783fe6eb5f3ec60d65fd2ffdd9c7f3e3aba7eb35645197e96979c86";
const PRIOR_VERSION =
  "gdi_pub_recH9h3McgMNFQEla_20261116_radisson_toronto_six_missing_segments_exa_30usd_v1";
const NEW_VERSION =
  "gdi_pub_recH9h3McgMNFQEla_20261116_segment_classification_remediation_v1";
const OUT = path.join(
  ROOT,
  "reports/gdi-segment-classification-and-preview-ui-remediation-v1",
);

const DRY = process.argv.includes("--dry-run");

function writeJson(name, obj) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
}

const priorPath = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
  HOTEL_ID,
  PRIOR_VERSION,
  "package.json",
);
const prior = JSON.parse(fs.readFileSync(priorPath, "utf8"));
if (prior.contentFingerprintSha256 !== PRIOR_FP) {
  throw new Error(
    `Prior fingerprint mismatch: ${prior.contentFingerprintSha256} !== ${PRIOR_FP}`,
  );
}

const mapping = [];
const nextOpps = (prior.opportunities || []).map((o) => {
  const before = {
    id: o.id,
    title: o.displayTitle || o.title,
    opportunityType: o.opportunityType || null,
    filter: o.opportunityTypeFilterCategory || null,
    label: o.segmentDisplayLabel || null,
    runtimeBefore: classifyOpportunityTypeFilter(o),
  };
  const applied = applyPrimaryDemandSegmentClassification(o);
  const decision = applied.classificationDecision || {};
  const claimedScaling =
    /scaling/i.test(String(o.opportunityType || "")) ||
    String(o.opportunityTypeFilterCategory || "").toUpperCase() ===
      "SCALING_COMPANY";
  const mustFix =
    (claimedScaling &&
      decision.demandSegmentId !== "corporate_accounts_scaling") ||
    decision.reason === "conference_not_planner_partnership" ||
    decision.reason === "planner_listing_without_group_program" ||
    decision.reason === "conference_not_travel_trade" ||
    decision.reason === "conference_not_local_catering" ||
    (String(o.opportunityType || "").includes("travel_trade") &&
      decision.demandSegmentId === "associations_conferences_events");

  if (!mustFix) {
    return o;
  }

  const next = {
    ...o,
    opportunityType:
      applied.opportunityType || o.opportunityType || null,
    opportunityTypeFilterCategory:
      applied.opportunityTypeFilterCategory ||
      o.opportunityTypeFilterCategory ||
      null,
    segmentDisplayLabel:
      applied.segmentDisplayLabel || o.segmentDisplayLabel || null,
    demandSegmentId: applied.demandSegmentId || o.demandSegmentId || null,
    customerDemandSegmentId:
      applied.customerDemandSegmentId || o.customerDemandSegmentId || null,
    priorOpportunityType: o.opportunityType || null,
    priorOpportunityTypeFilterCategory:
      o.opportunityTypeFilterCategory || null,
  };
  const after = {
    id: next.id,
    title: next.displayTitle || next.title,
    opportunityType: next.opportunityType || null,
    filter: next.opportunityTypeFilterCategory || null,
    label: next.segmentDisplayLabel || null,
    runtimeAfter: classifyOpportunityTypeFilter(next),
    reason: decision.reason || null,
    changed: true,
  };
  mapping.push({ before, after, classificationDecision: decision });
  return next;
});

const pkg = buildCustomerPublicationPackage({
  hotelId: HOTEL_ID,
  hotelName: prior.hotelName || "Radisson Blu Toronto Downtown",
  publicationVersion: NEW_VERSION,
  publicationId: `gdi_pubpkg_${HOTEL_ID}_segclass_${Date.now().toString(16).slice(-8)}`,
  opportunities: nextOpps,
  sortOrder: prior.sortOrder || nextOpps.map((o) => o.id),
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  approvalStatus: "CANDIDATE",
  createdAt: new Date().toISOString(),
  notes: {
    remediation: "gdi-segment-classification-and-preview-ui-remediation-v1",
    preservesPriorFingerprint: PRIOR_FP,
    preservesPriorVersion: PRIOR_VERSION,
  },
});

const filterCounts = {};
for (const o of pkg.opportunities) {
  const f = classifyOpportunityTypeFilter(o) || "OTHER";
  filterCounts[f] = (filterCounts[f] || 0) + 1;
}

const summary = {
  hotelId: HOTEL_ID,
  dryRun: DRY,
  priorFingerprint: PRIOR_FP,
  priorVersion: PRIOR_VERSION,
  newFingerprint: pkg.contentFingerprintSha256,
  newVersion: pkg.publicationVersion,
  opportunityCount: pkg.opportunityCount,
  inventoryCounts: pkg.inventoryCounts,
  changedRows: mapping.length,
  filterCounts,
  scalingCompanyCount: filterCounts.SCALING_COMPANY || 0,
  localCateringCount: filterCounts.LOCAL_CATERING_DAY_MEETINGS || 0,
  mapping,
};

writeJson("01-CLASSIFICATION_MAPPING.json", summary);
writeJson("02-SEGMENT_COUNTS.json", {
  inventoryCounts: pkg.inventoryCounts,
  filterCounts,
  scalingCompanyCount: filterCounts.SCALING_COMPANY || 0,
  localCateringCount: filterCounts.LOCAL_CATERING_DAY_MEETINGS || 0,
});

if (DRY) {
  console.log(JSON.stringify({ ok: true, dryRun: true, ...summary }, null, 2));
  process.exit(0);
}

const store = createGdiPublicationPackageStore({
  root: path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
  ),
});
const written = store.writePackage({
  ...pkg,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
});
if (!written.ok) {
  console.error(written);
  process.exit(1);
}
store.writePointer(HOTEL_ID, "STAGED", {
  publicationId: pkg.publicationId,
  publicationVersion: pkg.publicationVersion,
  contentFingerprintSha256: pkg.contentFingerprintSha256,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
  supersedes: {
    publicationVersion: PRIOR_VERSION,
    contentFingerprintSha256: PRIOR_FP,
  },
});

// Confirm prior package bytes untouched.
const priorAfter = JSON.parse(fs.readFileSync(priorPath, "utf8"));
if (priorAfter.contentFingerprintSha256 !== PRIOR_FP) {
  throw new Error("Prior package was mutated — abort");
}

writeJson("03-STAGED_POINTER.json", store.readPointer(HOTEL_ID, "STAGED"));
console.log(
  JSON.stringify(
    {
      ok: true,
      priorFingerprint: PRIOR_FP,
      newFingerprint: pkg.contentFingerprintSha256,
      changedRows: mapping.length,
      path: written.path,
    },
    null,
    2,
  ),
);
