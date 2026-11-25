#!/usr/bin/env node
/**
 * CAD Phase 3A PR-A regressions.
 * No Airtable / STAGED / CURRENT / paid providers.
 */
import assert from "node:assert/strict";
import {
  GDI_COMPETITIVE_ACCOUNT_DISCOVERY_CONTRACT_V1,
  CAD_CLASS,
  CAD_ATTAINABILITY,
  CAD_INCUMBENT_RELATIONSHIP,
  CAD_SOURCING_WINDOW_STATE,
  buildCadProgramKey,
  sanitizeSourcingWindow,
  validateCompetitiveAccountDiscovery,
  resolveCadDedupeAction,
  emptyCompetitiveAccountDiscovery,
} from "../lib/group-demand-intelligence/workflow/gdi-competitive-account-discovery-contract-v1.js";
import {
  classifyCompetitiveAccountDiscovery,
  attachCompetitiveAccountDiscovery,
  GDI_COMPETITIVE_ACCOUNT_DISCOVERY_CLASSIFIER_V1,
} from "../lib/group-demand-intelligence/workflow/gdi-competitive-account-discovery-classifier-v1.js";

function check(name, fn) {
  try {
    fn();
    console.log(`  PASS ${name}`);
  } catch (err) {
    console.error(`  FAIL ${name}`);
    throw err;
  }
}

check("contract_versions", () => {
  assert.equal(
    GDI_COMPETITIVE_ACCOUNT_DISCOVERY_CONTRACT_V1,
    "gdi-competitive-account-discovery-contract-v1",
  );
  assert.equal(
    GDI_COMPETITIVE_ACCOUNT_DISCOVERY_CLASSIFIER_V1,
    "gdi-competitive-account-discovery-classifier-v1",
  );
});

check("program_key_stable_for_dedupe", () => {
  const a = buildCadProgramKey({
    organizationName: "NADO / DDAA",
    title: "Washington Conference 2028",
    eventStartDate: "2028-03-01",
  });
  const b = buildCadProgramKey({
    organizationName: "NADO  DDAA",
    title: "Washington Conference 2028",
    eventStartDate: "2028-03-01",
  });
  assert.equal(a, b);
  assert.match(a, /nado/);
});

check("dedupe_prefers_enrich_existing_without_transferring_qualification", () => {
  const key = buildCadProgramKey({
    organizationName: "Example Assoc",
    title: "Annual 2027",
    eventStartDate: "2027-06-01",
  });
  const r = resolveCadDedupeAction({
    programKey: key,
    existingByProgramKey: new Map([[key, "opp_existing_1"]]),
  });
  assert.equal(r.action, "ENRICH_EXISTING");
  assert.equal(r.opportunityId, "opp_existing_1");
  assert.equal(r.qualificationTransferred, false);
});

check("sourcing_window_never_keeps_fabricated_confirmed_dates_without_evidence", () => {
  const sw = sanitizeSourcingWindow({
    state: CAD_SOURCING_WINDOW_STATE.CONFIRMED,
    start: "2027-01-01",
    end: "2027-01-05",
    basis: null,
    evidenceUrls: [],
  });
  assert.equal(sw.state, CAD_SOURCING_WINDOW_STATE.UNKNOWN);
  assert.equal(sw.start, null);
  assert.equal(sw.end, null);
});

check("venue_only_is_not_verified_incumbent_or_fqo_candidate", () => {
  const r = classifyCompetitiveAccountDiscovery({
    title: "City Association Gala",
    organizationName: "City Association",
    venue: "San Diego Convention Center",
    eventVenue: "San Diego Convention Center",
    likelyCompetitor: "San Diego Convention Center",
    whatIsHappening: "Gala at the San Diego Convention Center",
    whyThisHotel: "Nearby hotel option",
    recommendedNextStep: "Monitor",
    sourceUrls: ["https://example.org/gala"],
  });
  assert.equal(r.fqoPass, false);
  assert.equal(r.persistenceAuthorized, false);
  assert.ok(
    r.cad.incumbent.venueOnlyRejected === true ||
      r.cad.cadClass == null ||
      r.cad.incumbent.relationship === CAD_INCUMBENT_RELATIONSHIP.UNKNOWN,
  );
  assert.notEqual(r.cad.cadClass, CAD_CLASS.FQO_CANDIDATE);
});

check("historical_competitor_without_pursuit_is_historical_intel", () => {
  const r = classifyCompetitiveAccountDiscovery({
    title: "Past Annual Meeting 2024",
    organizationName: "Example Society",
    likelyCompetitor: "Hilton San Diego Bayfront",
    eventStartDate: "2024-05-01",
    lodgingEvidence:
      "Official room block at Hilton San Diego Bayfront for attendees.",
    lodgingEvidenceUrl: "https://example.org/housing",
    sourceUrls: ["https://example.org/housing"],
    whyThisHotel: "Previously hosted nearby; no current open pathway evidenced.",
    recommendedNextStep: "Archive learnings.",
    futureDemandPathway: false,
  });
  assert.equal(r.cad.cadClass, CAD_CLASS.HISTORICAL_COMPETITIVE_INTEL);
  assert.ok(
    r.cad.incumbent.relationship === CAD_INCUMBENT_RELATIONSHIP.VERIFIED ||
      r.cad.incumbent.relationship === CAD_INCUMBENT_RELATIONSHIP.INFERRED,
  );
  assert.equal(r.fqoPass, false);
});

check("emerging_overflow_without_hotel_action_does_not_auto_pass_fqo", () => {
  const r = classifyCompetitiveAccountDiscovery({
    title: "BIO International Convention 2028 lodging/overflow",
    organizationName: "BIO",
    likelyCompetitor: "Marriott Marquis San Diego Marina",
    competitiveContext: {
      incumbentHotel: "Marriott Marquis San Diego Marina",
      pathway: "OVERFLOW_OR_FUTURE_CYCLE",
    },
    eventStartDate: "2028-06-01",
    lodgingEvidence:
      "Official housing partner pages list Marriott Marquis as a primary hotel.",
    sourceUrls: ["https://example.org/bio-housing"],
    whyThisHotel:
      "This hotel has no verified current host or overflow role for BIO yet.",
    recommendedNextStep: "",
    primaryContact: { name: "BIO International Convention" },
  });
  assert.notEqual(r.cad.cadClass, CAD_CLASS.FQO_CANDIDATE);
  assert.ok(
    r.cad.cadClass === CAD_CLASS.EMERGING_COMPETITIVE_OPPORTUNITY ||
      r.cad.cadClass === CAD_CLASS.HISTORICAL_COMPETITIVE_INTEL,
  );
  assert.equal(r.fqoPass, false);
  assert.equal(r.persistenceAuthorized, false);
});

check("fqo_candidate_is_not_fqo_pass_or_persistence_auth", () => {
  const r = classifyCompetitiveAccountDiscovery({
    title: "NADO WashCon 2028",
    organizationName: "NADO",
    likelyCompetitor: "Bethesda North Marriott Hotel & Conference Center",
    competitiveContext: {
      incumbentHotel: "Bethesda North Marriott Hotel & Conference Center",
      pathway: "OVERFLOW_OR_FUTURE_CYCLE",
    },
    eventStartDate: "2028-03-15",
    lodgingEvidence:
      "Official room block / host hotel evidence at Bethesda North Marriott.",
    sourceUrls: ["https://example.org/nado-housing"],
    whyThisHotel:
      "Bethesda Marriott can pursue overflow / alternate housing for WashCon attendees.",
    recommendedNextStep:
      "Ask NADO whether 2028 hotel selection is open or overflow list will exist.",
    primaryContact: {
      name: "Jamie McCormick",
      email: "jmccormick@nado.org",
      role: "Meetings",
    },
    futureDemandPathway: true,
  });
  assert.equal(r.cad.cadClass, CAD_CLASS.FQO_CANDIDATE);
  assert.equal(r.cad.attainability, CAD_ATTAINABILITY.OVERFLOW_ONLY);
  assert.equal(r.fqoPass, false);
  assert.equal(r.persistenceAuthorized, false);
  assert.equal(r.publicationAuthorized, false);
  assert.ok(
    r.cad.risks.includes("FQO_CANDIDATE_REQUIRES_SIX_GATE_EVALUATION"),
  );
});

check("unsupported_inferred_dates_without_basis_downgrade", () => {
  const sw = sanitizeSourcingWindow({
    state: "INFERRED",
    start: "2027-09-01",
    evidenceUrls: [],
    basis: null,
  });
  assert.equal(sw.state, CAD_SOURCING_WINDOW_STATE.UNKNOWN);
  assert.equal(sw.start, null);
});

check("validate_rejects_fqo_candidate_unattainable", () => {
  const bad = emptyCompetitiveAccountDiscovery({
    cadClass: CAD_CLASS.FQO_CANDIDATE,
    attainability: CAD_ATTAINABILITY.UNATTAINABLE,
  });
  const v = validateCompetitiveAccountDiscovery(bad);
  assert.equal(v.ok, false);
  assert.ok(v.errors.includes("FQO_CANDIDATE_CANNOT_BE_UNATTAINABLE"));
});

check("attach_does_not_mutate_input_and_sets_sot_field", () => {
  const input = {
    title: "Test",
    organizationName: "Org",
    likelyCompetitor: "Hyatt Regency Bethesda",
    competitiveContext: { pathway: "FUTURE_CYCLE" },
    lodgingEvidence: "Host hotel Hyatt Regency Bethesda with official housing.",
    sourceUrls: ["https://example.org/x"],
    whyThisHotel: "Overflow pathway for local attendees.",
    recommendedNextStep: "Qualify overflow.",
    eventStartDate: "2027-10-01",
  };
  const frozen = JSON.stringify(input);
  const out = attachCompetitiveAccountDiscovery(input);
  assert.equal(JSON.stringify(input), frozen);
  assert.ok(out.competitiveAccountDiscovery);
  assert.equal(out.__cadClassification.fqoPass, false);
  assert.equal(out.__cadClassification.qualificationTransferred, false);
});

console.log("\nOK test-gdi-competitive-account-discovery-v1");
