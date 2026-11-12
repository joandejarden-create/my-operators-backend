/**
 * Regression: Bethesda 29 commercial category cleanup + quality gates.
 */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { BETHESDA_29_ATTESTED_IDS } from "../lib/group-demand-intelligence/research/bethesda-historical-attestation-bag-v1.js";
import {
  buildBethesda29CommercialQualityCleanup,
  stageBethesda29CommercialQualityCleanup,
} from "../lib/group-demand-intelligence/research/bethesda-29-commercial-quality-cleanup-v1.js";
import {
  mergeBethesdaInternalResearchOpportunities,
  projectHistoricalRowForInternalList,
} from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import {
  classifyOpportunityTypeFilter,
  OPPORTUNITY_TYPE_FILTER,
} from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { mapOpportunitiesToPresentationDtos } from "../lib/group-demand-intelligence/presentation/gdi-universal-presentation-pipeline-v1.js";
import { GDI_PRESENTATION_SURFACES } from "../lib/group-demand-intelligence/contracts/gdi-presentation-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recLuxvwwxID7U2B8";
const ACCEPTABLE_STORE_SHAS = [
  "fd438549ff030ffc6f219469a526ec9b88ad8a704871fa01784d5650468226ad",
  "fd4539d9dd850c12a3093dd7b4e75039e5db67fadcd5e2c5ae5de09c68d00e44",
];

const checks = [];
function check(name, fn) {
  try {
    fn();
    checks.push({ name, ok: true });
  } catch (err) {
    checks.push({ name, ok: false, error: String(err?.message || err) });
  }
}

const oppPath = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL,
  "opportunities.json"
);
const beforeHash = crypto.createHash("sha256").update(fs.readFileSync(oppPath)).digest("hex");

// Apply cleanup (idempotent merge into research-stage).
const staged = stageBethesda29CommercialQualityCleanup({
  root: ROOT,
  writeFixture: true,
  writeReports: true,
});
const doc = staged.doc;

check("cleanup_29_rows", () => {
  assert.equal(doc.opportunities.length, 29);
  assert.equal(doc.counts.cleaned, 29);
});

check("zero_other_unclassified_after", () => {
  assert.equal(doc.counts.otherUnclassifiedAfter, 0);
  for (const row of doc.opportunities) {
    const f = classifyOpportunityTypeFilter(row);
    assert.notEqual(
      f,
      OPPORTUNITY_TYPE_FILTER.OTHER_UNCLASSIFIED,
      `${row.id} still Other/Unclassified`
    );
  }
});

check("no_sales_ready_promotions", () => {
  assert.equal(doc.counts.salesReadyEligible, 0);
  for (const row of doc.opportunities) {
    assert.equal(row.salesReady, false);
    assert.notEqual(row.commercialMaturity, "SALES_READY");
    assert.equal(row.customerFacing, false);
  }
});

const canonicalDoc = await loadOpportunitiesCanonical(HOTEL);
const canonical = canonicalDoc.opportunities || [];
const union = mergeBethesdaInternalResearchOpportunities(HOTEL, canonical, {
  root: ROOT,
});

check("union_still_88_with_taxonomy", () => {
  assert.equal(union.opportunities.length, 88);
  const hist = union.opportunities.filter((o) =>
    BETHESDA_29_ATTESTED_IDS.includes(o.id || o.opportunityId)
  );
  assert.equal(hist.length, 29);
  for (const row of hist) {
    const f = classifyOpportunityTypeFilter(row);
    assert.notEqual(f, OPPORTUNITY_TYPE_FILTER.OTHER_UNCLASSIFIED, row.id);
    assert.equal(row.salesReady, false);
  }
  const facing = filterCustomerFacingOpportunities(union.opportunities, {
    hotelId: HOTEL,
  });
  assert.equal(
    facing.filter((o) => BETHESDA_29_ATTESTED_IDS.includes(o.id || o.opportunityId))
      .length,
    0
  );
});

check("projector_maps_partnerships", () => {
  const plp = projectHistoricalRowForInternalList({
    id: "gdi_plp_live_fa2063dc1195",
    organizationName: "Rock Creek Mansion",
    researchCategory: "POTENTIAL_PARTNERSHIPS",
    researchSegment: "preferred_lodging",
    demandSegment: "preferred_lodging",
    historicalRecoveryDisposition: "PARTNERSHIP_WATCH",
    whatIsHappening: "Venue partnership watch",
  });
  assert.equal(
    classifyOpportunityTypeFilter(plp),
    OPPORTUNITY_TYPE_FILTER.PREFERRED_LODGING_PARTNERSHIP
  );
  const pep = projectHistoricalRowForInternalList({
    id: "gdi_pep_live_af0711e711ba",
    organizationName: "Howerton+Wooten Events, LLC",
    researchCategory: "POTENTIAL_PARTNERSHIPS",
    researchSegment: "professional_event_planners",
    demandSegment: "professional_event_planners",
    historicalRecoveryDisposition: "PARTNERSHIP_WATCH",
  });
  assert.equal(
    classifyOpportunityTypeFilter(pep),
    OPPORTUNITY_TYPE_FILTER.PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP
  );
  const lmc = projectHistoricalRowForInternalList({
    id: "disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce",
    organizationName: "Greater Bethesda Chamber of Commerce",
    researchCategory: "LOCAL_CATERING",
    researchSegment: "local_meetings_catering",
    demandSegment: "local_meetings_catering",
  });
  assert.equal(
    classifyOpportunityTypeFilter(lmc),
    OPPORTUNITY_TYPE_FILTER.LOCAL_CATERING_DAY_MEETINGS
  );
});

check("store_hash_unchanged", () => {
  const after = crypto.createHash("sha256").update(fs.readFileSync(oppPath)).digest("hex");
  assert.equal(after, beforeHash);
  assert.ok(ACCEPTABLE_STORE_SHAS.includes(after), `unexpected store sha ${after}`);
  assert.equal(doc.opportunityStore.untouched, true);
});

check("idempotent_rebuild", () => {
  const again = buildBethesda29CommercialQualityCleanup({ root: ROOT });
  assert.equal(again.opportunities.length, 29);
  assert.equal(again.counts.otherUnclassifiedAfter, 0);
  assert.equal(again.counts.salesReadyEligible, 0);
});

check("presentation_pipeline_preserves_taxonomy", () => {
  const dtos = mapOpportunitiesToPresentationDtos(union.opportunities, {
    hotelId: HOTEL,
    surface: GDI_PRESENTATION_SURFACES.LOCAL_AUTH,
  });
  const histDtos = dtos.filter((o) =>
    BETHESDA_29_ATTESTED_IDS.includes(o.id || o.opportunityId)
  );
  assert.equal(histDtos.length, 29);
  const other = histDtos.filter(
    (o) => o.opportunityTypeFilterCategory === OPPORTUNITY_TYPE_FILTER.OTHER_UNCLASSIFIED
  );
  assert.equal(other.length, 0);
  const celebrations = histDtos.filter(
    (o) =>
      o.opportunityTypeFilterCategory ===
      OPPORTUNITY_TYPE_FILTER.CORPORATE_CELEBRATIONS_PRIVATE_EVENTS
  );
  assert.equal(celebrations.length, 2);
  const plp = histDtos.filter(
    (o) =>
      o.opportunityTypeFilterCategory ===
      OPPORTUNITY_TYPE_FILTER.PREFERRED_LODGING_PARTNERSHIP
  );
  assert.equal(plp.length, 4);
});

const failed = checks.filter((c) => !c.ok);
console.log(
  JSON.stringify(
    {
      ok: failed.length === 0,
      beforeHash,
      counts: doc.counts,
      beforeFilterDistribution: doc.beforeFilterDistribution,
      afterFilterDistribution: doc.afterFilterDistribution,
      checks,
    },
    null,
    2
  )
);
if (failed.length) process.exit(1);
