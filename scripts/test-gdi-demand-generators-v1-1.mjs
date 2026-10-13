/**
 * Demand Generator V1.1 — lodging-evidence harvest fixture tests.
 */
import assert from "node:assert/strict";
import {
  classifyWatchFailureReasons,
  buildLodgingEvidenceQueries,
  extractLodgingEvidenceFromText,
  evidenceToQualificationHints,
  classifyAgainstExistingGdi,
  HARVEST_CLASSIFICATION,
  LODGING_EVIDENCE_STRENGTH,
  WATCH_FAIL_REASON,
} from "../lib/group-demand-intelligence/demand-generators/lodging-evidence.js";
import {
  buildDemandGeneratorSignal,
} from "../lib/group-demand-intelligence/demand-generators/qualify-promote.js";
import { auditDemandGeneratorHotelSpecificLogic } from "../lib/group-demand-intelligence/demand-generators/hotel-specific-audit.js";

let passed = 0;
let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS  ${name}`);
    passed += 1;
  } catch (err) {
    console.error(`FAIL  ${name}`);
    console.error(err);
    failed += 1;
  }
}

check("recurring_program_plus_future_hotel_page_true_candidate", () => {
  const ev = extractLodgingEvidenceFromText({
    title: "2027 Annual Meeting — Hotel Block",
    snippet: "Official hotel block now open for out-of-town attendees",
    text: "Book your hotel room block for the 2027 annual conference. Registration open.",
    url: "https://example-assoc.org/2027-housing",
  });
  assert.equal(ev.futureCycle, true);
  assert.equal(ev.lodging, true);
  assert.equal(ev.strength, LODGING_EVIDENCE_STRENGTH.STRONG);
  const hints = evidenceToQualificationHints(ev, {
    organizationName: "Example Association",
    programName: "Annual Meeting",
  });
  const signal = buildDemandGeneratorSignal({
    demandGeneratorId: "dg_1",
    programId: "dgp_1",
    triggerType: "HOUSING_INFO_POSTED",
    title: "Annual Meeting 2027",
    sourceUrl: "https://example-assoc.org/2027-housing",
    marketRelevance: "CORE",
    productFit: "HIGH",
    hotelFitOk: true,
    ...hints,
  });
  assert.equal(signal.trueActionable, true);
});

check("recurring_program_historical_dates_only_future_watch", () => {
  const reasons = classifyWatchFailureReasons({
    failReasons: ["NO_FUTURE_TIMING", "NO_LODGING_THESIS"],
  });
  assert.ok(reasons.includes(WATCH_FAIL_REASON.NO_FUTURE_DATE));
  const cls = classifyAgainstExistingGdi({
    generator: { organizationName: "Org" },
    program: { recurrenceStatus: "RECURRING_CONFIRMED", programName: "Annual" },
    evidence: { futureCycle: false, lodging: false },
    trueActionable: false,
    existingOpps: [],
  });
  assert.equal(cls, HARVEST_CLASSIFICATION.FUTURE_WATCH);
});

check("contract_award_only_not_true", () => {
  const ev = extractLodgingEvidenceFromText({
    title: "Leidos awarded $50M task order",
    snippet: "Contract award announced for federal program support",
    text: "The company received a contract award. No travel or lodging details.",
    url: "https://leidos.com/newsroom/award",
  });
  const hints = evidenceToQualificationHints(ev, { organizationName: "Leidos" });
  assert.equal(hints.lodgingDemandThesis, false);
  const signal = buildDemandGeneratorSignal({
    demandGeneratorId: "dg_c",
    triggerType: "CONTRACT_AWARDED",
    title: "Contract award",
    sourceUrl: "https://leidos.com/newsroom/award",
    marketRelevance: "COMPETITIVE",
    hotelFitOk: true,
    ...hints,
  });
  assert.equal(signal.trueActionable, false);
});

check("contract_plus_mobilization_multi_day_travel_eligible", () => {
  const ev = extractLodgingEvidenceFromText({
    title: "Project kickoff and onsite mobilization 2027",
    snippet: "Multi-day onsite implementation; travel and accommodations for deployed team",
    text: "Kickoff workshop requires overnight travel. Nearby hotels recommended for out-of-town staff.",
    url: "https://contractor.example/mobilization-2027",
  });
  assert.ok(
    ev.strength === LODGING_EVIDENCE_STRENGTH.STRONG ||
      ev.strength === LODGING_EVIDENCE_STRENGTH.MODERATE
  );
  const hints = evidenceToQualificationHints(ev, { organizationName: "Contractor" });
  assert.equal(hints.lodgingDemandThesis, true);
});

check("training_program_repeat_cohorts_hotel_instructions_eligible", () => {
  const ev = extractLodgingEvidenceFromText({
    title: "Leadership Academy 2027 cohort schedule",
    snippet: "In-person residential training — accommodation instructions for participants",
    url: "https://training.example/academy-2027",
  });
  assert.equal(ev.lodging, true);
  assert.equal(ev.futureCycle, true);
});

check("sports_tournament_stay_to_play_eligible", () => {
  const ev = extractLodgingEvidenceFromText({
    title: "Premier Cup 2027 Stay-to-Play",
    snippet: "All teams must book through official stay-to-play hotel portal",
    url: "https://bethesdasoccer.example/premier-cup-2027-housing",
  });
  assert.equal(ev.strength, LODGING_EVIDENCE_STRENGTH.STRONG);
  assert.equal(ev.housing, true);
});

check("local_one_day_event_no_lodging", () => {
  const ev = extractLodgingEvidenceFromText({
    title: "Local lunch meeting",
    snippet: "Single-day local meeting for nearby members",
    text: "One-day session at HQ. Virtual option available.",
  });
  const hints = evidenceToQualificationHints(ev, { organizationName: "Local Org" });
  assert.equal(hints.lodgingDemandThesis, false);
});

check("future_program_duplicate_existing_gdi_overlap", () => {
  const cls = classifyAgainstExistingGdi({
    generator: {
      organizationName: "Bethesda Soccer Club",
      organizationAliases: ["BSC"],
    },
    program: { programName: "Bethesda Premier Cup" },
    evidence: {
      futureCycle: true,
      lodging: true,
      matchedYears: ["2026"],
      strength: LODGING_EVIDENCE_STRENGTH.STRONG,
    },
    trueActionable: true,
    existingOpps: [
      {
        id: "gdi_existing_1",
        organizationName: "Bethesda Soccer Club",
        title: "Bethesda Premier Cup — stay-to-play weekend hotel demand",
        eventStartDate: "2026-11-13",
      },
    ],
  });
  assert.ok(
    cls === HARVEST_CLASSIFICATION.EXISTING_UPDATE ||
      cls === HARVEST_CLASSIFICATION.GENERIC_DISCOVERY_OVERLAP
  );
});

check("playbook_queries_driven_by_missing_evidence", () => {
  const qs = buildLodgingEvidenceQueries({
    generator: {
      organizationName: "Example Assoc",
      officialDomain: "example.org",
      organizationType: "ASSOCIATION",
    },
    program: {
      programName: "Annual Conference",
      programType: "ANNUAL_CONFERENCE",
    },
    missingReasons: [
      WATCH_FAIL_REASON.NO_FUTURE_DATE,
      WATCH_FAIL_REASON.NO_LODGING_SIGNAL,
    ],
  });
  assert.ok(qs.some((q) => q.query.includes("site:example.org")));
  assert.ok(qs.some((q) => /hotel|lodging|housing/i.test(q.query)));
});

check("hotel_specific_audit_still_clean", () => {
  const audit = auditDemandGeneratorHotelSpecificLogic();
  assert.equal(audit.ok, true, JSON.stringify(audit.findings));
});

console.log(`\nDemand Generator V1.1 tests: ${passed} pass, ${failed} fail`);
if (failed) process.exit(1);
