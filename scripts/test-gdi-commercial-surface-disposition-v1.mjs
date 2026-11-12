#!/usr/bin/env node
/**
 * Adversarial acceptance fixtures for GDI commercial surface disposition.
 * Fails if known founder-visible defects remain on the primary commercial list.
 */
import assert from "node:assert/strict";
import {
  classifyCommercialSurfaceDisposition,
  filterOpportunitiesByCommercialSurface,
  isMarketIntelligenceResearchTheme,
  primaryCommercialForbiddenFixtures,
  COMMERCIAL_SURFACE,
  GDI_LIST_SURFACE_VIEW,
} from "../lib/group-demand-intelligence/gdi-commercial-surface-disposition-v1.js";
import { applyBethesdaCommercialEventFamilyPresentation } from "../lib/group-demand-intelligence/research/bethesda-88-commercial-event-family-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import { loadHotelOpportunityDoc } from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import { resolveCustomerSalesAction } from "../lib/group-demand-intelligence/customer-sales-action-resolver-v1.js";
import { buildCommercialSurfaceReconciliationManifest } from "../lib/group-demand-intelligence/research/bethesda-88-commercial-surface-reconciliation-v1.js";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}:`, err.message || err);
  }
}

check("marriott_hq_is_market_intelligence", () => {
  const o = {
    id: "gdi_opp_marriott_hq_adjacent_corporate_watch",
    title: "Corporate meetings near Marriott HQ",
    priority: "WATCHLIST",
    hotelFitHonesty: "CATEGORY_WATCH_NO_SINGLE_BUYER",
  };
  assert.equal(isMarketIntelligenceResearchTheme(o), true);
  const c = classifyCommercialSurfaceDisposition(o);
  assert.equal(c.surface, COMMERCIAL_SURFACE.MARKET_INTELLIGENCE);
  assert.equal(c.primaryCommercialEligible, false);
  const action = resolveCustomerSalesAction(o);
  assert.equal(action.key, "MONITOR");
  assert.notEqual(action.key, "BUILD_ACCOUNT");
});

check("aao_outside_is_archive", () => {
  const o = {
    id: "gdi_opp_aao_2027_annual_session_9",
    priority: "DISQUALIFIED",
    hotelFitHonesty: "DESTINATION_OUTSIDE_LAS_VEGAS",
    eventLocation: "Las Vegas, NV",
  };
  assert.equal(
    classifyCommercialSurfaceDisposition(o).surface,
    COMMERCIAL_SURFACE.ARCHIVE_DISQUALIFIED,
  );
});

check("iscb_rotterdam_is_archive", () => {
  const o = {
    id: "gdi_opp_48th_iscb_annual_conference_2027_12",
    priority: "DISQUALIFIED",
    hotelFitHonesty: "DESTINATION_OUTSIDE_ROTTERDAM",
    eventLocation: "Rotterdam, The Netherlands",
  };
  assert.equal(
    classifyCommercialSurfaceDisposition(o).primaryCommercialEligible,
    false,
  );
});

check("genuine_account_development_stays_commercial", () => {
  const o = {
    id: "gdi_sc_b36b66547864e8ce",
    priority: "MEDIUM_PRIORITY",
    category: "CORPORATE_ACCOUNT_RELATIONSHIP",
    title: "Matthews — Scaling Corporate Account",
    organizationName: "Matthews",
    primaryContactName: "Someone Real",
    hotelOpportunityThesis: "Corporate travel lodging demand for Bethesda corridor.",
  };
  assert.equal(
    classifyCommercialSurfaceDisposition(o).surface,
    COMMERCIAL_SURFACE.COMMERCIAL_OPPORTUNITY,
  );
});

check("bethesda_live_primary_list_excludes_known_defects", () => {
  const store = loadHotelOpportunityDoc("recLuxvwwxID7U2B8");
  const union = mergeBethesdaInternalResearchOpportunities(
    "recLuxvwwxID7U2B8",
    store.opportunities,
  );
  // Apply factual patches in-memory the same way reconciliation does
  const patched = union.opportunities.map((o) => {
    if (o.id === "gdi_opp_marriott_hq_adjacent_corporate_watch") {
      return {
        ...o,
        hotelFitHonesty: "CATEGORY_WATCH_NO_SINGLE_BUYER",
        commercialSurfaceDisposition: COMMERCIAL_SURFACE.MARKET_INTELLIGENCE,
        classification: "MARKET_INTELLIGENCE",
      };
    }
    if (o.id === "gdi_opp_2027_nci_rna_biology_symposium_7") {
      return {
        ...o,
        commercialSurfaceDisposition: COMMERCIAL_SURFACE.RESEARCH_PIPELINE,
      };
    }
    return o;
  });
  const family = applyBethesdaCommercialEventFamilyPresentation(patched, {
    hideLinkedFromList: true,
  });
  const commercial = filterOpportunitiesByCommercialSurface(
    family.opportunities,
    GDI_LIST_SURFACE_VIEW.COMMERCIAL,
  );
  const ids = new Set(commercial.map((o) => o.id));
  const fixtures = primaryCommercialForbiddenFixtures();
  for (const id of fixtures.marketIntelligenceIds) {
    assert.equal(ids.has(id), false, `market intelligence still visible: ${id}`);
  }
  for (const id of fixtures.disqualifiedIds) {
    assert.equal(ids.has(id), false, `disqualified still visible: ${id}`);
  }
  for (const id of fixtures.researchPipelinePreferredIds) {
    assert.equal(ids.has(id), false, `research pipeline still on commercial: ${id}`);
  }
  const ife = fixtures.duplicateFamilyPrimaryOnly.ife.filter((id) => ids.has(id));
  const gbcc = fixtures.duplicateFamilyPrimaryOnly.gbcc.filter((id) => ids.has(id));
  assert.ok(ife.length <= 1, `IFE duplicates on commercial: ${ife.join(",")}`);
  assert.ok(gbcc.length <= 1, `GBCC duplicates on commercial: ${gbcc.join(",")}`);
  assert.ok(commercial.length > 0, "commercial list empty");
  assert.ok(commercial.length < 83, `commercial list still inflated: ${commercial.length}`);
});

check("reconciliation_manifest_acceptance", () => {
  const m = buildCommercialSurfaceReconciliationManifest();
  assert.equal(m.unionCount, 88);
  assert.equal(m.acceptance.marriottExcluded, true);
  assert.equal(m.acceptance.noForbiddenVisible, true);
  assert.equal(m.acceptance.duplicatesCollapsed, true);
  assert.equal(m.acceptance.rawIdsPreserved, true);
  assert.ok(m.after.commercialListCount < m.before.listCount);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nAll commercial surface disposition gates passed.");
