#!/usr/bin/env node
/**
 * Positive + negative regression tests for permanent commercial-actionability gate.
 */
import assert from "node:assert/strict";
import {
  COMMERCIAL_ACTIONABILITY_STATE,
  classifyCommercialActionability,
  applyCommercialActionabilityGate,
  assertCustomerInventoryPassesActionabilityGate,
} from "../lib/group-demand-intelligence/commercial-actionability-gate-v1.js";
import { runPublicationQualityGate } from "../lib/group-demand-intelligence/publication-package/gdi-publication-quality-gate-v1.js";
import {
  TEMPLATE_E2E_STAGES,
  COMMERCIAL_ELIGIBILITY_POLICY_V1,
  GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
} from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";

const REF = "2026-11-14";
const results = [];

function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err.message || err) });
    throw err;
  }
}

check("completed_one_time_event_not_hotel", () => {
  const r = classifyCommercialActionability(
    {
      id: "exp1",
      title: "XXXVII Congreso Nacional del Notariado Mexicano 2026",
      eventStartDate: "2026-11-05",
      eventEndDate: "2026-11-07",
      whatIsHappening: "Celebró su XXXVII Congreso Nacional en Guadalajara",
    },
    {
      referenceDate: REF,
      actualEventStart: "2026-11-05",
      actualEventEnd: "2026-11-07",
      dispositionOverride: "EXPIRED_ONE_TIME_EVENT",
      actionabilityOverride:
        COMMERCIAL_ACTIONABILITY_STATE.EXPIRED_ONE_TIME_EVENT,
    },
  );
  assert.equal(r.state, COMMERCIAL_ACTIONABILITY_STATE.EXPIRED_ONE_TIME_EVENT);
  assert.equal(r.activeHotelMarketEligible, false);
});

check("upcoming_open_sourcing_actionable", () => {
  const r = classifyCommercialActionability(
    {
      id: "fut1",
      title: "Ecomondo Mexico 2027",
      eventStartDate: "2027-04-20",
      eventEndDate: "2027-04-22",
      hotelSourcingStatusLabel: "Hotel Sourcing Unconfirmed",
      whatIsHappening: "Confirmed April 2027 at Expo Guadalajara",
    },
    { referenceDate: REF },
  );
  assert.equal(
    r.state,
    COMMERCIAL_ACTIONABILITY_STATE.ACTIONABLE_CURRENT_OR_FUTURE,
  );
  assert.equal(r.activeHotelMarketEligible, true);
});

check("upcoming_closed_sourcing_not_hotel_open", () => {
  const r = classifyCommercialActionability(
    {
      id: "cmi1",
      title: "Semana IMIN 2026",
      eventStartDate: "2026-11-17",
      eventEndDate: "2026-11-21",
      cmiVerified: true,
      hotelSourcingStatusLabel:
        "Competitor venue confirmed (Hard Rock Hotel Guadalajara)",
    },
    { referenceDate: REF, cmiVerified: true, competitorCommitted: true },
  );
  assert.equal(
    r.state,
    COMMERCIAL_ACTIONABILITY_STATE.SOURCING_CLOSED_OR_COMMITTED,
  );
  assert.equal(r.activeHotelMarketEligible, false);
  assert.equal(r.activeCompetitiveEligible, true);
});

check("historical_with_verified_future_cycle", () => {
  const r = classifyCommercialActionability(
    {
      id: "fc1",
      title: "Annual Industry Congress — 2027 edition verified",
      eventStartDate: "2027-09-01",
      whatIsHappening: "Verified 2027 edition dates published",
    },
    {
      referenceDate: REF,
      dispositionOverride: "FUTURE_CYCLE_VERIFIED",
      actionabilityOverride: COMMERCIAL_ACTIONABILITY_STATE.FUTURE_CYCLE_VERIFIED,
    },
  );
  assert.equal(r.state, COMMERCIAL_ACTIONABILITY_STATE.FUTURE_CYCLE_VERIFIED);
  assert.equal(r.activeHotelMarketEligible, true);
});

check("historical_with_unverified_future_cycle_not_hotel", () => {
  const r = classifyCommercialActionability(
    {
      id: "fcr1",
      title: "Annual Industry Congress 2026",
      eventStartDate: "2026-03-01",
      whatIsHappening: "Annual congress completed; next edition unverified",
    },
    {
      referenceDate: REF,
      dispositionOverride: "FUTURE_CYCLE_RESEARCH_REQUIRED",
      actionabilityOverride:
        COMMERCIAL_ACTIONABILITY_STATE.FUTURE_CYCLE_RESEARCH_REQUIRED,
    },
  );
  assert.equal(
    r.state,
    COMMERCIAL_ACTIONABILITY_STATE.FUTURE_CYCLE_RESEARCH_REQUIRED,
  );
  assert.equal(r.activeHotelMarketEligible, false);
});

check("multiyear_construction_project_ongoing", () => {
  const r = classifyCommercialActionability(
    {
      id: "proj1",
      title: "Foxconn AI-Server Manufacturing Megafactory",
      datePillDisplayLabel: "Ongoing",
      whatIsHappening:
        "Multiyear Guadalajara-area manufacturing project with contractor travel demand",
    },
    {
      referenceDate: REF,
      dispositionOverride: "ACTIVE_ONGOING_DEMAND",
      actionabilityOverride:
        COMMERCIAL_ACTIONABILITY_STATE.ONGOING_ACCOUNT_DEVELOPMENT,
    },
  );
  assert.equal(
    r.state,
    COMMERCIAL_ACTIONABILITY_STATE.ONGOING_ACCOUNT_DEVELOPMENT,
  );
  assert.equal(r.activeHotelMarketEligible, true);
});

check("continuing_corporate_travel_ongoing", () => {
  const r = classifyCommercialActionability(
    {
      id: "corp1",
      title: "Mexico Relocation Services",
      datePillDisplayLabel: "Dates TBD",
      whatIsHappening: "Standing corporate relocation and temporary lodging demand",
    },
    {
      referenceDate: REF,
      dispositionOverride: "ACTIVE_ONGOING_DEMAND",
      actionabilityOverride:
        COMMERCIAL_ACTIONABILITY_STATE.ONGOING_ACCOUNT_DEVELOPMENT,
    },
  );
  assert.equal(r.activeHotelMarketEligible, true);
});

check("recurring_training_program_ongoing", () => {
  const r = classifyCommercialActionability(
    {
      id: "train1",
      title: "Convenio de Formación Continua Para El Legislativo",
      datePillDisplayLabel: "Ongoing",
      whatIsHappening: "Continuing education agreement with recurring sessions",
    },
    {
      referenceDate: REF,
      dispositionOverride: "ACTIVE_ONGOING_DEMAND",
      actionabilityOverride:
        COMMERCIAL_ACTIONABILITY_STATE.ONGOING_ACCOUNT_DEVELOPMENT,
    },
  );
  assert.equal(r.activeHotelMarketEligible, true);
});

check("article_date_not_event_date_contamination", () => {
  const r = classifyCommercialActionability(
    {
      id: "contam1",
      title: "Infrastructure project announcement",
      datePillDisplayLabel: "Dates TBD",
      eventStartDate: "2026-11-10",
      tileEventStartDate: "2026-11-10",
      whatIsHappening: "Original public date: 2026-11-10 is not the activity date",
    },
    { referenceDate: REF, storedIsResearchTimestamp: true },
  );
  assert.equal(r.researchTimestampContamination, true);
});

check("future_event_missing_date_tbd", () => {
  const r = classifyCommercialActionability(
    {
      id: "tbd1",
      title: "XLI Medio Maratón Internacional Guadalajara Electrolit 2027",
      datePillDisplayLabel: "Dates TBD",
      whatIsHappening: "2027 race edition announced; exact race day TBD",
    },
    {
      referenceDate: REF,
      dispositionOverride: "ACTIVE_FUTURE_EVENT",
      actionabilityOverride:
        COMMERCIAL_ACTIONABILITY_STATE.ACTIONABLE_CURRENT_OR_FUTURE,
      clearContaminatedEventDates: true,
    },
  );
  assert.equal(r.activeHotelMarketEligible, true);
});

check("confirmed_competitor_booked_competitive_only", () => {
  const applied = applyCommercialActionabilityGate(
    [
      {
        id: "cmi2",
        title: "Congreso APIEJ 2027",
        eventStartDate: "2027-10-13",
        cmiVerified: true,
        hotelSourcingStatusLabel:
          "Competitor venue confirmed (Hyatt Regency Andares Guadalajara)",
      },
    ],
    {
      referenceDate: REF,
      hintsById: {
        cmi2: { cmiVerified: true, competitorCommitted: true },
      },
    },
  );
  assert.equal(applied.counts.activeHotelMarket, 0);
  assert.equal(applied.counts.activeCompetitive, 1);
});

check("historical_account_no_current_hotel_opportunity", () => {
  const r = classifyCommercialActionability(
    {
      id: "hist1",
      title: "CEO Summit 2026",
      eventStartDate: "2026-11-05",
      whatIsHappening: "Completed one-day summit; no verified future edition",
    },
    {
      referenceDate: REF,
      actionabilityOverride:
        COMMERCIAL_ACTIONABILITY_STATE.HISTORICAL_RESEARCH_ONLY,
      dispositionOverride: "HISTORICAL_ACCOUNT_DEVELOPMENT",
    },
  );
  assert.equal(
    r.state,
    COMMERCIAL_ACTIONABILITY_STATE.HISTORICAL_RESEARCH_ONLY,
  );
  assert.equal(r.activeHotelMarketEligible, false);
});

check("certification_fails_expired_in_hotel_market", () => {
  const gate = assertCustomerInventoryPassesActionabilityGate(
    [
      {
        id: "bad1",
        title: "Completed congress",
        hotelMarketScopeEligible: true,
        commercialActionabilityStateV1:
          COMMERCIAL_ACTIONABILITY_STATE.EXPIRED_ONE_TIME_EVENT,
        eventStartDate: "2026-11-05",
        eventEndDate: "2026-11-07",
      },
    ],
    {
      referenceDate: REF,
      hintsById: {
        bad1: {
          actionabilityOverride:
            COMMERCIAL_ACTIONABILITY_STATE.EXPIRED_ONE_TIME_EVENT,
          dispositionOverride: "EXPIRED_ONE_TIME_EVENT",
        },
      },
    },
  );
  assert.equal(gate.ok, false);
  assert.ok(gate.blockers.length >= 1);

  const q = runPublicationQualityGate({
    opportunities: [
      {
        id: "bad1",
        displayTitle: "Completed congress",
        hotelMarketScopeEligible: true,
        whatIsHappening: "Event completed",
        commercialActionabilityStateV1:
          COMMERCIAL_ACTIONABILITY_STATE.EXPIRED_ONE_TIME_EVENT,
        commercialTemporalDisposition: "EXPIRED_ONE_TIME_EVENT",
      },
    ],
    referenceDate: REF,
  });
  assert.equal(q.ok, false);
  assert.ok(
    q.blocking.some(
      (f) =>
        f.code === "EXPIRED_ONE_TIME_IN_HOTEL_MARKET" ||
        f.code === "COMMERCIAL_ACTIONABILITY_GATE_BLOCK" ||
        f.code === "HOTEL_MARKET_CONTAINS_NON_ACTIONABLE" ||
        f.code === "COMMERCIAL_EXPIRATION_GATE_BLOCK",
    ),
  );
});

check("template_stages_include_actionability_gate", () => {
  const ids = TEMPLATE_E2E_STAGES.map((s) => String(s.id));
  assert.ok(ids.includes("4b"));
  assert.ok(
    TEMPLATE_E2E_STAGES.some(
      (s) => s.actionabilityGate === GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
    ),
  );
  assert.equal(
    COMMERCIAL_ELIGIBILITY_POLICY_V1.commercialActionability.contractId,
    GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  );
  assert.deepEqual(
    COMMERCIAL_ELIGIBILITY_POLICY_V1.commercialActionability.appliesAt,
    [
      "discovery_qualification",
      "commercial_research_review",
      "customer_visible_eligibility",
      "publication_staging",
      "weekly_refresh",
      "founder_publication_certification",
    ],
  );
});

check("near_term_sourcing_closed_not_hotel", () => {
  const r = classifyCommercialActionability(
    {
      id: "near1",
      title: "Smart Technology Expo 2026",
      eventStartDate: "2026-11-18",
      whatIsHappening: "Expo in four days; main hotel RFP window closed",
    },
    {
      referenceDate: REF,
      nearTermSourcingClosed: true,
      actionabilityOverride:
        COMMERCIAL_ACTIONABILITY_STATE.NOT_CURRENTLY_ACTIONABLE,
    },
  );
  assert.equal(
    r.state,
    COMMERCIAL_ACTIONABILITY_STATE.NOT_CURRENTLY_ACTIONABLE,
  );
  assert.equal(r.activeHotelMarketEligible, false);
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      ok: failed.length === 0,
      passed: results.filter((r) => r.ok).length,
      failed: failed.length,
      results,
    },
    null,
    2,
  ),
);
if (failed.length) process.exit(1);
