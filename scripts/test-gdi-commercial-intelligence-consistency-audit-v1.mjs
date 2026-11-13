#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_HOTEL_ID,
  classifyCompetitiveMarketIntelligence,
  applyCmiLedgerRemediation,
  annotateInventoryPopulations,
  loadBethesda67SalesBaselineManifest,
  BETHESDA_CMI_EVIDENCE_LEDGER,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import {
  COMPETITOR_VENUE_STATUS,
  TARGET_HOTEL_SOURCING_STATUS,
  recommendCommercialAction,
} from "../lib/group-demand-intelligence/commercial-consistency/commercial-status-dimensions-v1.js";
import {
  detectOpportunityContradictions,
  CONTRADICTION_CODE,
} from "../lib/group-demand-intelligence/commercial-consistency/commercial-contradiction-detector-v1.js";
import { GDI_CUSTOMER_CSV_HEADERS } from "../lib/group-demand-intelligence/customer-csv-export.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

const cases = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "fixtures/gdi/commercial-consistency/contradiction-cases-v1.json"),
    "utf8"
  )
).cases;

check("acvnu_confirmed_block_sourcing_unknown", () => {
  const rem = applyCmiLedgerRemediation({
    id: "gdi_opp_acvnu_renal_week_20270412",
    recommendedAction: "request RFP",
    attendance: "UNKNOWN",
  });
  const cmi = classifyCompetitiveMarketIntelligence(rem);
  assert.equal(cmi.verified, true);
  assert.equal(cmi.competitorVenueStatus, COMPETITOR_VENUE_STATUS.CONFIRMED_CONTRACTED_BLOCK);
  assert.equal(cmi.targetHotelSourcingStatus, TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED);
  assert.equal(rem.attendance, "250+");
  assert.doesNotMatch(rem.recommendedAction, /request rfp/i);
});

check("afcea_mismatched_competitor_demoted", () => {
  const cmi = classifyCompetitiveMarketIntelligence({
    id: "gdi_opp_afcea_hits_2027",
  });
  assert.equal(cmi.verified, false);
  assert.notEqual(cmi.competitorHotel, "Hyatt Regency Bethesda");
  assert.match(cmi.competitorHotel || "", /Bethesda North Marriott/i);
});

check("ecs_confirmed_marquis_not_bethesda_north", () => {
  const cmi = classifyCompetitiveMarketIntelligence({
    id: "gdi_opp_ecs_251_2027_overflow",
  });
  assert.equal(cmi.verified, true);
  assert.match(cmi.competitorHotel || "", /Marriott Marquis/i);
});

check("biomaterials_without_hotel_block_demoted", () => {
  const cmi = classifyCompetitiveMarketIntelligence({
    id: "gdi_opp_world_biomaterials_2028_overflow",
  });
  assert.equal(cmi.verified, false);
});

check("aad_without_hotel_block_demoted", () => {
  const cmi = classifyCompetitiveMarketIntelligence({
    id: "gdi_opp_aad_2028_overflow",
  });
  assert.equal(cmi.verified, false);
});

check("confirmed_competitor_no_overflow_action", () => {
  const action = recommendCommercialAction({
    competitorVenueStatus: COMPETITOR_VENUE_STATUS.CONFIRMED_CONTRACTED_BLOCK,
    targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED,
    commercialActionability: "COMPETITIVE_MARKET_INTELLIGENCE",
    competitorHotel: "The Bethesdan",
  });
  assert.match(action, /do not request an rfp as host/i);
  assert.match(action, /overflow|secondary|qualify/i);
  assert.doesNotMatch(action, /request rfp or housing inclusion/i);
});

check("confirmed_overflow_with_evidence_allows_overflow_action", () => {
  const action = recommendCommercialAction({
    competitorVenueStatus: COMPETITOR_VENUE_STATUS.CONFIRMED_EVENT_VENUE,
    targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.OVERFLOW_CONFIRMED,
    competitorHotel: "Crystal Gateway Marriott",
    contactHint: "housing desk",
  });
  assert.match(action, /overflow/i);
});

check("activity_connected_contact_note_on_acvnu", () => {
  const rem = applyCmiLedgerRemediation({
    id: "gdi_opp_acvnu_renal_week_20270412",
  });
  assert.match(rem.contactRoleNote || "", /not verified hotel-booking/i);
});

check("seed_alone_is_not_proof_for_unknown_id", () => {
  const cmi = classifyCompetitiveMarketIntelligence({
    id: "gdi_opp_not_in_ledger",
    competitorHotel: "Some Hotel",
    venueSourcingStatus: "PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE",
    title: "Something overflow",
  });
  assert.equal(cmi.verified, false);
});

check("legacy_booking_flag_plus_generic_sources_not_verified", () => {
  const cmi = classifyCompetitiveMarketIntelligence({
    id: "gdi_opp_generic_competitor_2028",
    competitorHotel: "Hyatt Regency Bethesda",
    competitorVenueConfirmed: true,
    sources: ["https://www.example.org/"],
    title: "Some 2028 event",
  });
  assert.equal(cmi.verified, false);
  assert.match(cmi.reason || "", /edition-specific|generic sources/i);
});

check("nado_2028_uses_edition_specific_washcon_evidence", () => {
  const rem = applyCmiLedgerRemediation({
    id: "gdi_opp_nado_ddaa_washcon_2028",
  });
  const cmi = classifyCompetitiveMarketIntelligence(rem);
  assert.equal(cmi.verified, true);
  assert.equal(
    cmi.competitorVenueStatus,
    COMPETITOR_VENUE_STATUS.CONFIRMED_CONTRACTED_BLOCK
  );
  assert.ok((cmi.evidenceUrls || []).some((u) => /2028washcon/i.test(u)));
  assert.match(rem.recommendedAction || "", /do not request an rfp as host/i);
  assert.doesNotMatch(
    rem.recommendedAction || "",
    /(?<!do not )request an? rfp(?:\s|\/|$)/i
  );
});

check("open_rfp_action_sanitized_when_sourcing_unknown", () => {
  const annotated = annotateInventoryPopulations(
    [
      {
        id: "gdi_opp_amwa_2027_annual",
        hotelId: BETHESDA_HOTEL_ID,
        recommendedAction:
          "Contact associatedirector@amwa-doc.org to confirm venue/sourcing status is still open, introduce the hotel, and request RFP / site-visit inclusion.",
        venueSourcingStatus: "UNKNOWN",
        primaryContact: { email: "associatedirector@amwa-doc.org" },
      },
    ],
    { hotelId: BETHESDA_HOTEL_ID }
  );
  assert.doesNotMatch(annotated[0].recommendedAction || "", /request rfp/i);
  const d = detectOpportunityContradictions(annotated[0]);
  assert.ok(
    !d.findings.some(
      (f) => f.code === CONTRADICTION_CODE.OPEN_RFP_WITHOUT_SOURCING_EVIDENCE
    )
  );
});

check("detector_flags_open_rfp_without_sourcing", () => {
  const d = detectOpportunityContradictions({
    id: "gdi_opp_acvnu_renal_week_20270412",
    recommendedAction: "request RFP for next open cycle",
    venueSourcingStatus: "PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE",
    competitorHotel: "The Bethesdan",
  });
  assert.ok(
    d.findings.some(
      (f) => f.code === CONTRADICTION_CODE.OPEN_RFP_WITHOUT_SOURCING_EVIDENCE
    )
  );
});

check("dual_class_unique_id_not_inflated", () => {
  const manifest = loadBethesda67SalesBaselineManifest({ forceReload: true });
  const annotated = annotateInventoryPopulations(
    Object.keys(BETHESDA_CMI_EVIDENCE_LEDGER).map((id) => ({
      id,
      hotelId: BETHESDA_HOTEL_ID,
    })),
    { hotelId: BETHESDA_HOTEL_ID }
  );
  const verified = annotated.filter((o) => o.cmiVerified);
  assert.ok(verified.length >= 3);
  assert.ok(verified.length <= 7);
  for (const o of verified) {
    assert.ok(manifest.baselineIds.includes(o.id));
    assert.ok(
      o.inventoryPopulations.includes("COMPETITIVE_MARKET_INTELLIGENCE")
    );
    // Confirmed competitor bookings without independent target-hotel demand
    // stay Competitive-only (not Hotel / SALES_OPPORTUNITIES).
    if (o.hotelMarketScopeEligible === false) {
      assert.ok(
        !o.inventoryPopulations.includes("SALES_OPPORTUNITIES"),
        `${o.id} must not inflate Hotel scope`
      );
    }
  }
  assert.equal(
    new Set(verified.map((o) => o.id)).size,
    verified.length,
    "no duplicate cards"
  );
});

check("csv_includes_competitive_future_cycle_columns", () => {
  // Prior lock was 64; +3 columns for competitive/future-cycle presentation parity.
  assert.equal(GDI_CUSTOMER_CSV_HEADERS.length, 67);
  assert.ok(
    GDI_CUSTOMER_CSV_HEADERS.includes("Competitive Commercial Action")
  );
  assert.ok(
    GDI_CUSTOMER_CSV_HEADERS.includes("Confirmed Competitor Business")
  );
  assert.ok(
    GDI_CUSTOMER_CSV_HEADERS.includes("Potential Future Opportunity")
  );
});

check("fixture_cases_cover_required_scenarios", () => {
  assert.ok(cases.length >= 7);
  assert.ok(cases.some((c) => c.id.includes("acvnu")));
  assert.ok(cases.some((c) => c.id.includes("afcea")));
  assert.ok(cases.some((c) => c.id.includes("ecs")));
});

check("cross_hotel_ledger_does_not_apply", () => {
  const annotated = annotateInventoryPopulations(
    [{ id: "gdi_opp_acvnu_renal_week_20270412", title: "x" }],
    { hotelId: "recnPB3Q02NO8Ma1o" }
  );
  assert.equal(annotated[0].commercialConsistencyRemediatedAt == null, true);
});

console.log(
  JSON.stringify(
    {
      suite: "gdi-commercial-intelligence-consistency-audit-v1",
      failed,
      ledgerSize: Object.keys(BETHESDA_CMI_EVIDENCE_LEDGER).length,
    },
    null,
    2
  )
);
process.exit(failed ? 1 : 0);
