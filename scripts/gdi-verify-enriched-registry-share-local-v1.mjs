#!/usr/bin/env node
/**
 * Production-like local verification: registry-only merge (no local-shadow),
 * then customer presentation + share sanitize for all seven Spice IDs.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createGdiPublishedInventoryRegistry,
  mergePublishedInventoryIntoOpportunities,
} from "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js";
import { sanitizeOpportunityForCustomerPresentation } from "../lib/group-demand-intelligence/customer-presentation-sanitize-v1.js";
import { sanitizeOpportunityForShare } from "../lib/group-demand-intelligence/share/gdi-signed-share-capability-v1.js";
import { prepareCustomerFacingOpportunity } from "../lib/group-demand-intelligence/share/gdi-customer-facing-opportunity-contract-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL = "recKRJjcPnb4tVDDS";
const STAGING = path.join(
  ROOT,
  "reports/gdi-spice-island-production-activation-v1/staging-published-inventory",
);
const OUT = path.join(
  ROOT,
  "reports/gdi-production-publication-full-content-parity-v1/07-registry-only-share-verify.json",
);

const registry = createGdiPublishedInventoryRegistry({
  root: STAGING,
  env: { GDI_PUBLISHED_INVENTORY_ROOT: STAGING },
});

// Empty research base — mirrors production opportunities.json (IDs absent).
const merged = mergePublishedInventoryIntoOpportunities(HOTEL, [], {
  registry,
  strictPublishedOnly: true,
});

const rows = [];
let allOk = true;
for (const o of merged) {
  const prepared = prepareCustomerFacingOpportunity(o);
  const presented = sanitizeOpportunityForCustomerPresentation(prepared);
  const share = sanitizeOpportunityForShare(presented);
  const drawer =
    share.eventsDrawerModel?.sections ||
    share.scalingDrawerModel?.sections ||
    share.sharedTenSectionDrawer?.sections ||
    null;
  const checks = {
    whatIsHappening: Boolean(
      share.whatIsHappening &&
        share.organizationName &&
        share.whatIsHappening.trim().toLowerCase() !==
          share.organizationName.trim().toLowerCase() &&
        share.whatIsHappening.trim().toLowerCase() !==
          String(share.title || "").trim().toLowerCase(),
    ),
    potentialHotelBusiness: Boolean(
      share.potentialHotelBusiness ||
        share.salesBrief?.potentialBusinessForThisHotel ||
        drawer?.potential_business?.body,
    ),
    whyThisHotel: Boolean(share.whyThisHotel || drawer?.why_this_hotel?.body),
    whyActNow: Boolean(
      share.whyActNow || share.whyNow || drawer?.why_act_now?.body,
    ),
    facts: (share.establishedFacts || drawer?.facts_and_uncertainties?.facts || [])
      .length >= 1,
    uncertainties: (
      share.uncertainties ||
      drawer?.facts_and_uncertainties?.uncertainties ||
      []
    ).length >= 1,
    nextStep: Boolean(
      share.recommendedNextStep || drawer?.recommended_next_step?.body,
    ),
    primaryContact: Boolean(share.primaryContact?.name),
    sources: (share.sources || []).length >= 1,
    drawerPresent: Boolean(drawer),
    classification: o.commercialInventoryClass || null,
  };
  const ok = Object.entries(checks)
    .filter(([k]) => k !== "classification")
    .every(([, v]) => v === true);
  if (!ok) allOk = false;
  rows.push({
    opportunityId: o.id,
    title: share.title || o.title,
    ok,
    checks,
    previews: {
      what: String(share.whatIsHappening || "").slice(0, 120),
      potential: String(
        share.potentialHotelBusiness ||
          share.salesBrief?.potentialBusinessForThisHotel ||
          "",
      ).slice(0, 120),
      whyNow: String(share.whyActNow || share.whyNow || "").slice(0, 120),
      facts: (share.establishedFacts || []).length,
      qq: (share.qualificationQuestions || []).length,
      secondary: share.secondaryContact?.name || null,
      next: String(share.recommendedNextStep || "").slice(0, 120),
    },
  });
}

const classCounts = merged.reduce((acc, o) => {
  const c = o.commercialInventoryClass;
  acc[c] = (acc[c] || 0) + 1;
  return acc;
}, {});

const report = {
  ok: allOk && merged.length === 7,
  mode: "registry_only_no_local_shadow",
  count: merged.length,
  classCounts,
  expected: { total: 7, SALES_READY: 1, ACCOUNT_DEVELOPMENT: 6 },
  classOk:
    classCounts.SALES_READY === 1 && classCounts.ACCOUNT_DEVELOPMENT === 6,
  rows,
};
fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify({ ok: report.ok, classCounts, failing: rows.filter((r) => !r.ok).map((r) => r.opportunityId) }, null, 2));
process.exit(report.ok ? 0 : 1);
