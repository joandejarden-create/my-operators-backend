#!/usr/bin/env node
/**
 * GDI — Confirmed competitor booking vs future-cycle opportunity presentation.
 * Preserves Market Scope counts; blocks false RFP / Pursue Now on contracted editions.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import {
  annotateInventoryPopulations,
  BETHESDA_HOTEL_ID,
  selectBethesdaSalesOpportunityInventory,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import {
  filterOpportunitiesByCommercialSurface,
  GDI_LIST_SURFACE_VIEW,
} from "../lib/group-demand-intelligence/gdi-commercial-surface-disposition-v1.js";
import { applyBethesdaCommercialEventFamilyPresentation } from "../lib/group-demand-intelligence/research/bethesda-88-commercial-event-family-v1.js";
import {
  applyCompetitiveFutureCyclePresentation,
  buildCompetitiveFutureCyclePresentation,
  COMPETITIVE_COMMERCIAL_ACTION,
  COMPETITIVE_COMMERCIAL_ACTION_LABEL,
} from "../lib/group-demand-intelligence/commercial-consistency/competitive-future-cycle-presentation-v1.js";
import {
  COMPETITOR_VENUE_STATUS,
  TARGET_HOTEL_SOURCING_STATUS,
} from "../lib/group-demand-intelligence/commercial-consistency/commercial-status-dimensions-v1.js";
import { BETHESDA_CMI_EVIDENCE_LEDGER } from "../lib/group-demand-intelligence/commercial-consistency/cmi-evidence-ledger-v1.js";
import {
  buildCustomerExportCsv,
  GDI_CUSTOMER_CSV_HEADERS,
} from "../lib/group-demand-intelligence/customer-csv-export.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

let passed = 0;
let failed = 0;
function check(name, fn) {
  try {
    fn();
    passed += 1;
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

function loadUi() {
  const src = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8"
  );
  const sandbox = { window: {}, globalThis: {}, console, URL, URLSearchParams };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  vm.runInNewContext(src, sandbox, { filename: "dealality-gdi-ui.js" });
  return sandbox.DealalityGdiUi;
}

const UI = loadUi();

const doc = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      `data/group-demand-intelligence/hotels/${BETHESDA_HOTEL_ID}/opportunities.json`
    ),
    "utf8"
  )
);
const union = mergeBethesdaInternalResearchOpportunities(
  BETHESDA_HOTEL_ID,
  doc.opportunities || []
);
const family = applyBethesdaCommercialEventFamilyPresentation(
  union.opportunities,
  { hideLinkedFromList: true }
);
const surface = filterOpportunitiesByCommercialSurface(
  family.opportunities,
  GDI_LIST_SURFACE_VIEW.COMMERCIAL
);
const selected = selectBethesdaSalesOpportunityInventory(surface, {
  salesTeamInventory: true,
  founderReview: true,
}).selected;
const annotated = annotateInventoryPopulations(selected, {
  hotelId: BETHESDA_HOTEL_ID,
});
const counts = UI.countByMarketScope(annotated);

check("market_scope_counts_preserved_all_67_hotel_61_competitive_10", () => {
  assert.equal(annotated.length, 67);
  assert.equal(counts.ALL, 67);
  assert.equal(counts.HOTEL, 61);
  assert.equal(counts.COMPETITIVE, 10);
});

function findOpp(...patterns) {
  return annotated.find((o) => {
    const blob = `${o.id} ${o.title} ${o.opportunityName} ${o.organizationName}`;
    return patterns.every((p) =>
      p instanceof RegExp ? p.test(blob) : blob.includes(p)
    );
  });
}

const nado2028 =
  findOpp(/nado/i, /2028/) ||
  annotated.find((o) => o.id === "gdi_opp_nado_ddaa_washcon_2028");
const nado2027 =
  findOpp(/nado/i, /2027/) ||
  annotated.find((o) => o.id === "gdi_opp_nado_ddaa_washcon_2027_overflow");
const afcea =
  findOpp(/afcea/i) ||
  annotated.find((o) => o.id === "gdi_opp_afcea_hits_2027");

check("nado_2028_confirmed_competitor_not_open_host", () => {
  assert.ok(nado2028, "NADO 2028 present");
  const p = nado2028.competitiveFutureCyclePresentation;
  assert.ok(p?.showSplitPresentation);
  assert.equal(p.confirmedCompetitorBusiness?.applicable, true);
  assert.match(
    p.confirmedCompetitorBusiness.headline,
    /Crystal Gateway Marriott/i
  );
  assert.match(
    p.confirmedCompetitorBusiness.headline,
    /not currently available/i
  );
  assert.equal(
    nado2028.competitiveCommercialActionKey,
    COMPETITIVE_COMMERCIAL_ACTION.QUALIFY_OVERFLOW
  );
  assert.equal(
    nado2028.competitiveCommercialActionLabel,
    COMPETITIVE_COMMERCIAL_ACTION_LABEL.QUALIFY_OVERFLOW
  );
  assert.doesNotMatch(String(nado2028.recommendedAction || ""), /Pursue Now/i);
  assert.match(
    String(nado2028.recommendedAction || ""),
    /do not request an RFP as host/i
  );
  assert.notEqual(nado2028.salesActionKey, "PURSUE_NOW");
  assert.notEqual(nado2028.salesActionPillLabel, "Pursue Now");
  // Competitive-only unless independent target demand
  assert.equal(nado2028.competitiveMarketScopeEligible, true);
  assert.equal(nado2028.hotelMarketScopeEligible, false);
});

check("nado_2027_confirmed_competitor_qualify_overflow", () => {
  assert.ok(nado2027, "NADO 2027 present");
  const p = nado2027.competitiveFutureCyclePresentation;
  assert.equal(p?.confirmedCompetitorBusiness?.applicable, true);
  assert.match(
    p.confirmedCompetitorBusiness.headline,
    /Crystal Gateway Marriott/i
  );
  assert.equal(
    nado2027.competitiveCommercialActionKey,
    COMPETITIVE_COMMERCIAL_ACTION.QUALIFY_OVERFLOW
  );
  assert.equal(nado2027.competitiveMarketScopeEligible, true);
});

check("afcea_future_cycle_monitor_not_pursue", () => {
  assert.ok(afcea, "AFCEA present");
  const p = afcea.competitiveFutureCyclePresentation;
  assert.ok(p?.showSplitPresentation);
  assert.equal(
    afcea.competitiveCommercialActionKey,
    COMPETITIVE_COMMERCIAL_ACTION.MONITOR_FUTURE_CYCLE
  );
  assert.equal(
    afcea.competitiveCommercialActionLabel,
    "Monitor Future Cycle"
  );
  assert.equal(p.potentialFutureOpportunity?.applicable, true);
  assert.match(
    p.potentialFutureOpportunity.headline,
    /Monitor future event editions|Future sourcing has not been confirmed/i
  );
  assert.equal(p.potentialFutureOpportunity.futureHotelSelectionKnown, false);
  assert.equal(p.potentialFutureOpportunity.expectedTiming, null);
  // Must not invent next-edition year as available
  assert.doesNotMatch(
    String(p.potentialFutureOpportunity.nextEditionHint || ""),
    /2028 is open|RFP open/i
  );
  assert.doesNotMatch(String(afcea.recommendedAction || ""), /Pursue Now/i);
});

check("ledger_afcea_future_cycle_status", () => {
  const led = BETHESDA_CMI_EVIDENCE_LEDGER.gdi_opp_afcea_hits_2027;
  assert.equal(
    led.targetHotelSourcingStatus,
    TARGET_HOTEL_SOURCING_STATUS.FUTURE_CYCLE_TO_MONITOR
  );
  assert.equal(
    led.competitorVenueStatus,
    COMPETITOR_VENUE_STATUS.VERIFIED_HISTORICAL_VENUE
  );
});

check("synthetic_open_confirmed_allows_pursue_open_business", () => {
  const open = applyCompetitiveFutureCyclePresentation({
    title: "Open housing test",
    competitiveStatus: "VERIFIED",
    cmiVerified: true,
    cmiCompetitorHotel: "Rival Hotel",
    competitorVenueStatus: COMPETITOR_VENUE_STATUS.CONFIRMED_EVENT_VENUE,
    targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.OPEN_CONFIRMED,
    targetHotelName: "Bethesda Marriott",
    competitiveMarketScopeEligible: true,
  });
  assert.equal(
    open.competitiveCommercialActionKey,
    COMPETITIVE_COMMERCIAL_ACTION.PURSUE_OPEN_BUSINESS
  );
});

check("no_invented_future_dates_in_presentation_builder", () => {
  const p = buildCompetitiveFutureCyclePresentation({
    title: "Some Event 2027",
    competitiveStatus: "HISTORICAL",
    cmiCompetitorHotel: "Other Hotel",
    competitorVenueStatus: COMPETITOR_VENUE_STATUS.VERIFIED_HISTORICAL_VENUE,
    targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.FUTURE_CYCLE_TO_MONITOR,
    targetHotelName: "Bethesda Marriott",
  });
  assert.equal(p.potentialFutureOpportunity.expectedTiming, null);
  assert.equal(p.potentialFutureOpportunity.futureHotelSelectionKnown, false);
});

check("drawer_renders_split_sections_for_nado", () => {
  assert.ok(UI?.intelligenceDetailHtml);
  const html = UI.intelligenceDetailHtml(nado2028, null, {});
  assert.match(html, /Confirmed Competitor Business/);
  assert.match(html, /Potential Future Opportunity|Qualify Overflow/i);
  assert.match(html, /Crystal Gateway Marriott/);
  assert.match(html, /not currently available/i);
  assert.doesNotMatch(html, /Request RFP/);
});

check("drawer_renders_monitor_future_cycle_for_afcea", () => {
  const html = UI.intelligenceDetailHtml(afcea, null, {});
  assert.match(html, /Confirmed Competitor Business|Potential Future Opportunity/);
  assert.match(html, /Monitor Future Cycle/);
  assert.doesNotMatch(html, /Pursue Now/);
});

check("list_card_shows_competitive_action_pill", () => {
  assert.ok(UI?.opportunityTileHtml);
  const tile = UI.opportunityTileHtml(nado2028);
  assert.match(tile, /Qualify Overflow|Verified(?: Competitive)?/);
});

check("csv_exports_competitive_action_columns", () => {
  assert.ok(
    GDI_CUSTOMER_CSV_HEADERS.includes("Competitive Commercial Action")
  );
  assert.ok(
    GDI_CUSTOMER_CSV_HEADERS.includes("Confirmed Competitor Business")
  );
  assert.ok(
    GDI_CUSTOMER_CSV_HEADERS.includes("Potential Future Opportunity")
  );
  const csv = buildCustomerExportCsv([nado2028, afcea]);
  assert.match(csv, /Qualify Overflow|Monitor Future Cycle/);
  assert.match(csv, /Crystal Gateway Marriott|not currently available/i);
});

check("all_competitive_rows_have_safe_actions", () => {
  const competitive = annotated.filter(
    (o) => o.competitiveMarketScopeEligible === true
  );
  assert.equal(competitive.length, 10);
  for (const o of competitive) {
    const label = o.competitiveCommercialActionLabel || "";
    if (o.competitiveFutureCyclePresentation?.showSplitPresentation) {
      assert.ok(
        Object.values(COMPETITIVE_COMMERCIAL_ACTION_LABEL).includes(label) ||
          !label,
        `${o.id} unexpected action ${label}`
      );
    }
    if (
      o.targetHotelSourcingStatus !==
        TARGET_HOTEL_SOURCING_STATUS.OPEN_CONFIRMED &&
      o.competitiveFutureCyclePresentation?.confirmedCompetitorBusiness
        ?.applicable
    ) {
      assert.notEqual(o.salesActionPillLabel, "Pursue Now", o.id);
      assert.doesNotMatch(
        String(o.recommendedAction || ""),
        /Pursue Now/i,
        o.id
      );
    }
  }
});

const reportDir = path.join(
  ROOT,
  "reports/gdi-competitive-future-cycle-presentation-v1"
);
fs.mkdirSync(reportDir, { recursive: true });

const competitiveRows = annotated
  .filter((o) => o.competitiveMarketScopeEligible === true)
  .map((o) => ({
    id: o.id,
    title: o.title || o.opportunityName,
    competitorHotel: o.cmiCompetitorHotel || o.competitorHotel || null,
    targetHotelSourcingStatus: o.targetHotelSourcingStatus || null,
    competitiveStatus: o.competitiveStatus || null,
    commercialAction: o.competitiveCommercialActionLabel || null,
    hotelEligible: o.hotelMarketScopeEligible === true,
    competitiveEligible: o.competitiveMarketScopeEligible === true,
    confirmedHeadline:
      o.competitiveFutureCyclePresentation?.confirmedCompetitorBusiness
        ?.headline || null,
    futureHeadline:
      o.competitiveFutureCyclePresentation?.potentialFutureOpportunity
        ?.headline || null,
  }));

fs.writeFileSync(
  path.join(reportDir, "00-bethesda-summary.json"),
  JSON.stringify(
    {
      hotelId: BETHESDA_HOTEL_ID,
      marketScope: {
        all: 67,
        hotel: 61,
        competitive: 10,
      },
      focus: {
        nado2028: competitiveRows.find((r) => /2028/i.test(r.id + r.title)),
        nado2027: competitiveRows.find((r) => /2027/i.test(r.id + r.title) && /nado/i.test(r.id + r.title)),
        afcea: competitiveRows.find((r) => /afcea/i.test(r.id + r.title)),
      },
      competitiveInventory: competitiveRows,
      passed,
      failed,
    },
    null,
    2
  )
);

console.log(`\n${passed} passed, ${failed} failed`);
if (failed) process.exit(1);
