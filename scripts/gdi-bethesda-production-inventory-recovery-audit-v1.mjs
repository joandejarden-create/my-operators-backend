#!/usr/bin/env node
/**
 * Read-only Bethesda production inventory recovery audit.
 * Zero paid. No writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { applyApprovedBethesdaCustomerProjection } from "../lib/group-demand-intelligence/workflow/gdi-bethesda-publication-v1.js";
import {
  loadBethesda67SalesBaselineManifest,
  isBethesda67CustomerPublicationApproved,
  selectBethesdaSalesOpportunityInventory,
  annotateInventoryPopulations,
  buildInventoryPopulationCounts,
  isHotelMarketScopeEligible,
  isCompetitiveMarketScopeEligible,
  classifyCompetitiveMarketIntelligence,
  GDI_INVENTORY_AUDIENCE,
  BETHESDA_HOTEL_ID,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-bethesda-production-inventory-recovery-v1"
);
fs.mkdirSync(OUT, { recursive: true });

const hotelId = BETHESDA_HOTEL_ID;
const prodPath = path.join(OUT, "00-prod-share-opportunities.json");
if (!fs.existsSync(prodPath)) {
  console.error("Missing production capture:", prodPath);
  process.exit(2);
}
const prod = JSON.parse(fs.readFileSync(prodPath, "utf8"));
const prodIds = new Set((prod.opportunities || []).map((o) => o.id));
const prodById = new Map((prod.opportunities || []).map((o) => [o.id, o]));

const doc = await loadOpportunitiesCanonical(hotelId, { bypassCache: true });
const store = doc.opportunities || [];
const union = mergeBethesdaInternalResearchOpportunities(hotelId, store);
const storeIds = new Set(store.map((o) => o.id));
const unionIds = new Set(union.opportunities.map((o) => o.id));

const manifest = loadBethesda67SalesBaselineManifest({ forceReload: true });
const baselineIds = new Set(manifest.baselineIds || []);
const publishedIds = new Set(
  manifest.publishedIds || manifest.published19 || []
);
const stagedIds = new Set(manifest.stagedIds || manifest.staged48 || []);

const facingNoApproval = applyApprovedBethesdaCustomerProjection(
  filterCustomerFacingOpportunities(union.opportunities, { hotelId }),
  { hotelId }
);

const facingWithApproval = applyApprovedBethesdaCustomerProjection(
  filterCustomerFacingOpportunities(union.opportunities, {
    hotelId,
    env: {
      ...process.env,
      GDI_BETHESDA_67_CUSTOMER_PUBLICATION_APPROVED: "1",
    },
  }),
  { hotelId }
);

const salesSelected = selectBethesdaSalesOpportunityInventory(
  union.opportunities,
  {
    hotelId,
    audience: GDI_INVENTORY_AUDIENCE.SALES_TEAM_INVENTORY,
  }
);
const salesRows = Array.isArray(salesSelected)
  ? salesSelected
  : salesSelected.opportunities || [];
const annotated = annotateInventoryPopulations(salesRows);
const popCounts = buildInventoryPopulationCounts(annotated);

let hotelScope = 0;
let competitiveScope = 0;
for (const o of annotated) {
  const cmi = classifyCompetitiveMarketIntelligence(o);
  if (isHotelMarketScopeEligible(o, cmi)) hotelScope += 1;
  if (isCompetitiveMarketScopeEligible(o, cmi)) competitiveScope += 1;
}

function gapReason(id, local) {
  if (!local && prodIds.has(id)) return "PROD_ONLY";
  if (local && prodIds.has(id)) return "PRESENT_BOTH";
  if (!baselineIds.has(id)) return "LOCAL_NOT_ON_67_BASELINE";
  if (publishedIds.has(id)) return "PUBLISHED19_MISSING_FROM_PROD_SHARE";
  if (stagedIds.has(id)) {
    return "STAGED48_REQUIRES_PUBLICATION_APPROVAL_FOR_SHARE";
  }
  return "LOCAL_NOT_IN_PROD_SHARE";
}

function rowMeta(o, id) {
  const cmi = o ? classifyCompetitiveMarketIntelligence(o) : null;
  return {
    id,
    title: o ? o.displayTitle || o.title || null : prodById.get(id)?.title || null,
    customerVisible: o?.customerVisible ?? null,
    publicationState: o?.publicationState || null,
    commercialInventoryClass: o?.commercialInventoryClass || null,
    opportunityType: o?.opportunityType || null,
    demandSegment:
      o?.demandSegmentId || o?.researchSegment || o?.segment || null,
    priority: o?.priority || null,
    onBaseline67: baselineIds.has(id),
    onPublished19: publishedIds.has(id),
    onStaged48: stagedIds.has(id),
    hotelScopeEligible: o ? isHotelMarketScopeEligible(o, cmi) : null,
    competitiveScopeEligible: o
      ? isCompetitiveMarketScopeEligible(o, cmi)
      : null,
    cmiClass: cmi?.class || cmi?.cmiClass || null,
    hasContacts: !!(
      o?.primaryContactName ||
      o?.contacts?.length ||
      o?.whoPathClass
    ),
    updatedAt: o?.updatedAt || o?.lastResearchedAt || null,
    inLocalStore: storeIds.has(id),
    inUnion: unionIds.has(id),
    inProdShare: prodIds.has(id),
  };
}

const allIds = new Set([
  ...baselineIds,
  ...storeIds,
  ...unionIds,
  ...prodIds,
]);

const reconciliation = [...allIds].sort().map((id) => {
  const local =
    union.opportunities.find((o) => o.id === id) ||
    store.find((o) => o.id === id) ||
    null;
  return {
    ...rowMeta(local, id),
    existsLocally: !!local,
    existsProdShare: prodIds.has(id),
    gapReason: gapReason(id, local),
  };
});

const summary = {
  asOf: new Date().toISOString(),
  hotelId,
  rootCauseHypothesis: [
    "Production share uses published/customer-facing audience, not sales-team inventory.",
    "Bethesda 67 customer publication approval receipt is ABSENT — share must not expand to 67 without founder approval.",
    "Local authenticated GDI with sales-team capability shows 67/61/10; external share correctly stays on published/customer-facing set unless approved.",
    "Production currently serves 27 share rows (broader than frozen published-19 allowlist) from Airtable/customerVisible path — not the Sep-13-only story alone, and not the sales-team 67.",
  ],
  approval: {
    receiptPath:
      "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/bethesda-67-customer-publication-approval.json",
    receiptExists: fs.existsSync(
      path.join(
        ROOT,
        "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/bethesda-67-customer-publication-approval.json"
      )
    ),
    envApproved: isBethesda67CustomerPublicationApproved(),
    gate:
      "Customer/share expansion to 67 requires receipt approved:true OR GDI_BETHESDA_67_CUSTOMER_PUBLICATION_APPROVED=1",
  },
  persistence: {
    mode: doc.persistence,
    source: doc.source,
    updatedAt: doc.updatedAt,
    storeCount: store.length,
    unionCount: union.opportunities.length,
  },
  baseline: {
    baseline67: baselineIds.size,
    published19: publishedIds.size,
    staged48: stagedIds.size,
  },
  localSurfaces: {
    customerFacingWithoutApproval: facingNoApproval.length,
    customerFacingWithApprovalSim: facingWithApproval.length,
    salesTeamInventory: salesRows.length,
    salesTeamMarketScope: {
      all: annotated.length,
      hotel: hotelScope,
      competitive: competitiveScope,
      populationCounts: popCounts,
    },
  },
  prodShare: {
    count: prod.count,
    inPublished19: [...prodIds].filter((id) => publishedIds.has(id)).length,
    inStaged48: [...prodIds].filter((id) => stagedIds.has(id)).length,
    inBaseline67: [...prodIds].filter((id) => baselineIds.has(id)).length,
    outsideBaseline67: [...prodIds]
      .filter((id) => !baselineIds.has(id))
      .sort(),
    ids: [...prodIds].sort(),
  },
  displayedVsApi:
    "UI 'Showing 26 of 27' is an active browse/filter exclusion on one of the 27 API rows — not a second inventory source.",
  gapBuckets: reconciliation.reduce((acc, r) => {
    acc[r.gapReason] = (acc[r.gapReason] || 0) + 1;
    return acc;
  }, {}),
  outstandingPr235:
    "Guided-tour drawer Next/Back fix remains on unmerged PR #235 — not part of this inventory recovery.",
  nextActions: {
    safeNow: [
      "Backup production feedback + share inventory snapshot (done in this report folder).",
      "Do NOT write GDI_BETHESDA_67_CUSTOMER_PUBLICATION_APPROVED or invent approval receipt without founder decision.",
      "Do NOT force-push local 59/67 file over Airtable.",
    ],
    requiresFounderDecision: [
      "Authorize Bethesda 67 customer publication (approval receipt) if external share must show 67/61/10.",
      "OR confirm external share should remain published/customer-facing (~19–27) while sales-team auth UI shows 67.",
      "Decide whether production's extra share rows beyond published-19 should be trimmed to the frozen 19 or retained.",
    ],
  },
};

fs.writeFileSync(
  path.join(OUT, "01-reconciliation-summary.json"),
  JSON.stringify(summary, null, 2) + "\n"
);
fs.writeFileSync(
  path.join(OUT, "02-record-level-reconciliation.json"),
  JSON.stringify(reconciliation, null, 2) + "\n"
);

const stagedMissingFromProd = reconciliation.filter(
  (r) => r.gapReason === "STAGED48_REQUIRES_PUBLICATION_APPROVAL_FOR_SHARE"
);
const publishedMissing = reconciliation.filter(
  (r) => r.gapReason === "PUBLISHED19_MISSING_FROM_PROD_SHARE"
);
fs.writeFileSync(
  path.join(OUT, "03-staged48-missing-from-prod-share.json"),
  JSON.stringify(stagedMissingFromProd, null, 2) + "\n"
);
fs.writeFileSync(
  path.join(OUT, "04-published19-missing-from-prod-share.json"),
  JSON.stringify(publishedMissing, null, 2) + "\n"
);

console.log(JSON.stringify(summary, null, 2));
