#!/usr/bin/env node
/**
 * Unit tests: buying-authority annotation, ledger → channel flag recovery,
 * Findymail identity normalization.
 *   npm run test:gdi-close-e2e-cert-gaps-v1
 */
import assert from "node:assert/strict";
import {
  annotateOpportunityBuyingAuthority,
  annotateHotelOpportunitySet,
  BUYING_AUTHORITY_STATUS,
  ALTERNATIVE_ENTRY_RESEARCH_STATUS,
} from "../lib/group-demand-intelligence/workflow/gdi-buying-authority-annotation-v1.js";
import {
  recoverContactChannelResearchFlags,
  applyEvidenceRecoveryToOpportunities,
} from "../lib/group-demand-intelligence/workflow/gdi-contact-channel-evidence-recovery-v1.js";
import {
  looksLikeFullPersonName,
  normalizePersonNameForEnrichment,
  assessFindymailEmailEligibility,
} from "../lib/group-demand-intelligence/workflow/gdi-findymail-stage4-enrichment-v1.js";
import {
  buildHotelContactBestEffortLedger,
  evaluateContactBestEffortCertificationGate,
} from "../lib/group-demand-intelligence/workflow/gdi-contact-research-best-effort-v1.js";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err?.message || err}`);
  }
}

const S = BUYING_AUTHORITY_STATUS;
const ALT = ALTERNATIVE_ENTRY_RESEARCH_STATUS;

const opp = (over = {}) => ({
  id: "opp1",
  organizationName: "Acme Corp",
  primaryContact: {
    name: "Pat Jones",
    title: "Director of Travel and Lodging Procurement",
    buyerVsEntry: "DIRECT_BUYER",
  },
  ...over,
});

// ── Buying authority annotation ─────────────────────────────────────────────
check("title_alone_never_verified_direct", () => {
  const { opportunity, report } = annotateOpportunityBuyingAuthority(opp());
  assert.equal(opportunity.primaryContact.buyingAuthorityStatus, S.BUYING_AUTHORITY_NOT_VERIFIED);
  assert.equal(opportunity.requiresAlternativeEntryPoints, true);
  assert.equal(opportunity.alternativeEntryResearchStatus, ALT.REQUIRED_NOT_COMPLETED);
  assert.ok(report.reasons.includes("TITLE_OR_HEURISTIC_ROLE_ONLY_NOT_AUTHORITY_EVIDENCE"));
});

check("who_how_direct_without_lodging_verification_stays_not_verified", () => {
  const { opportunity } = annotateOpportunityBuyingAuthority(opp(), {
    whoHowStatus: "VERIFIED_DIRECT_BUYER",
  });
  assert.equal(opportunity.primaryContact.buyingAuthorityStatus, S.BUYING_AUTHORITY_NOT_VERIFIED);
});

check("who_how_direct_plus_lodging_verified_is_direct", () => {
  const o = opp();
  o.primaryContact.hotelBuyingAuthorityVerified = true;
  const { opportunity } = annotateOpportunityBuyingAuthority(o, {
    whoHowStatus: "VERIFIED_DIRECT_BUYER",
  });
  assert.equal(
    opportunity.primaryContact.buyingAuthorityStatus,
    S.VERIFIED_DIRECT_LODGING_DECISION_MAKER,
  );
  assert.equal(opportunity.requiresAlternativeEntryPoints, false);
  assert.equal(opportunity.alternativeEntryResearchStatus, ALT.NOT_APPLICABLE);
});

check("lodging_flag_without_who_how_direct_is_not_direct", () => {
  const o = opp();
  o.primaryContact.hotelBuyingAuthorityVerified = true;
  const { opportunity } = annotateOpportunityBuyingAuthority(o);
  assert.notEqual(
    opportunity.primaryContact.buyingAuthorityStatus,
    S.VERIFIED_DIRECT_LODGING_DECISION_MAKER,
  );
});

check("who_how_buyer_influencer", () => {
  const { opportunity } = annotateOpportunityBuyingAuthority(opp(), {
    whoHowStatus: "VERIFIED_BUYER_INFLUENCER",
  });
  assert.equal(opportunity.primaryContact.buyingAuthorityStatus, S.VERIFIED_BUYER_INFLUENCER);
  assert.equal(opportunity.requiresAlternativeEntryPoints, true);
});

check("alt_entry_with_commercial_relevance_claims_no_authority", () => {
  const o = opp();
  o.primaryContact.commercialClass = "commercial_influencer_or_referral";
  o.primaryContact.contactRelevanceLabel = "CONTACT_RELEVANT";
  const { opportunity } = annotateOpportunityBuyingAuthority(o);
  assert.equal(
    opportunity.primaryContact.buyingAuthorityStatus,
    S.VERIFIED_ALTERNATIVE_ENTRY_POINT,
  );
  assert.equal(opportunity.requiresAlternativeEntryPoints, true);
});

check("abbreviated_identity_requires_further_research", () => {
  const o = opp({ primaryContact: { name: "Suzanne K.", title: "Travel Manager" } });
  const { opportunity } = annotateOpportunityBuyingAuthority(o);
  assert.equal(
    opportunity.primaryContact.buyingAuthorityStatus,
    S.IDENTITY_OR_RELEVANCE_REQUIRES_FURTHER_RESEARCH,
  );
});

check("alt_research_completed_when_alt_or_secondary_present", () => {
  const withAlt = annotateOpportunityBuyingAuthority(
    opp({ alternativeEntryPoints: [{ name: "Sam Lee", title: "Ops" }] }),
  ).opportunity;
  assert.equal(withAlt.alternativeEntryResearchStatus, ALT.COMPLETED);
  const withSecondary = annotateOpportunityBuyingAuthority(
    opp({ secondaryContact: { name: "Sam Lee" } }),
  ).opportunity;
  assert.equal(withSecondary.alternativeEntryResearchStatus, ALT.COMPLETED);
  const viaWhoHow = annotateOpportunityBuyingAuthority(opp(), {
    alternativeEntryResearchCompleted: true,
  }).opportunity;
  assert.equal(viaWhoHow.alternativeEntryResearchStatus, ALT.COMPLETED);
});

check("never_invents_people_or_channels_and_does_not_mutate_input", () => {
  const input = opp();
  const snapshot = JSON.stringify(input);
  const { opportunity } = annotateOpportunityBuyingAuthority(input);
  assert.equal(JSON.stringify(input), snapshot);
  assert.equal(opportunity.alternativeEntryPoints, undefined);
  assert.equal(opportunity.secondaryContact, undefined);
  assert.equal(opportunity.primaryContact.email, undefined);
  assert.equal(opportunity.primaryContact.name, "Pat Jones");
});

check("legacy_side_contact_status_preserved", () => {
  const { opportunity } = annotateOpportunityBuyingAuthority(
    opp({
      alternativeEntryPoints: [
        { name: "Sam Lee", buyingAuthorityStatus: "NOT_VERIFIED_LODGING_DECISION_MAKER" },
      ],
    }),
  );
  assert.equal(
    opportunity.alternativeEntryPoints[0].buyingAuthorityStatus,
    "NOT_VERIFIED_LODGING_DECISION_MAKER",
  );
});

check("hotel_set_report_and_gate_annotation_cleared", () => {
  const set = annotateHotelOpportunitySet([opp(), opp({ id: "opp2" })]);
  assert.equal(set.opportunities.length, 2);
  assert.equal(set.report.opportunityCount, 2);
  assert.equal(set.report.verifiedDirectLodgingDecisionMakers, 0);
  const ledger = buildHotelContactBestEffortLedger(set.opportunities, {
    findymailLedger: [],
  });
  assert.equal(ledger.scorecard.opportunitiesMissingAuthorityAnnotation, 0);
  const gate = evaluateContactBestEffortCertificationGate(ledger);
  assert.ok(!gate.failures.includes("BUYING_AUTHORITY_ANNOTATION_INCOMPLETE"));
});

// ── Findymail ledger → email researched flags ───────────────────────────────
const ledgerNotFound = [
  {
    personKey: "jackson mitz|modern railway systems",
    operation: "FIND_FROM_NAME",
    resultDisposition: "NOT_FOUND",
    emailFound: false,
    at: "2026-11-27T22:47:41.997Z",
  },
];

check("ledger_not_found_sets_email_researched_flags", () => {
  const r = recoverContactChannelResearchFlags(
    { name: "Jackson Mitz", title: "PM" },
    { organizationName: "Modern Railway Systems", findymailLedger: ledgerNotFound },
  );
  const cr = r.person.contactChannelResearch;
  assert.equal(cr.professionalEmail, "NOT_FOUND_OR_UNAVAILABLE");
  assert.equal(cr.emailResearched, true);
  assert.equal(cr.findymailUsed, true);
  assert.equal(r.person.email, undefined);
  assert.equal(r.person.phone, undefined);
  assert.equal(r.person.linkedinUrl, undefined);
});

check("ledger_found_without_persisted_email_marks_researched_without_inventing", () => {
  const r = recoverContactChannelResearchFlags(
    { name: "Steve Fry" },
    {
      organizationName: "FlatironDragados",
      findymailLedger: [
        {
          personKey: "steve fry|flatirondragados",
          operation: "FIND_FROM_NAME",
          resultDisposition: "FOUND",
          emailFound: true,
        },
      ],
    },
  );
  const cr = r.person.contactChannelResearch;
  assert.equal(cr.emailResearched, true);
  assert.notEqual(cr.professionalEmail, "NOT_FOUND_OR_UNAVAILABLE");
  assert.equal(cr.emailRecoveryNote, "PROVIDER_FOUND_EMAIL_NOT_PERSISTED_IN_PACKAGE");
  assert.equal(r.person.email, undefined);
});

check("recovery_clears_email_not_yet_in_gate_scorecard", () => {
  const o = {
    id: "o1",
    organizationName: "Modern Railway Systems",
    primaryContact: { name: "Jackson Mitz", title: "PM" },
  };
  const before = buildHotelContactBestEffortLedger([o], { findymailLedger: ledgerNotFound });
  assert.equal(before.scorecard.emailNotYetResearched, 1);
  const rec = applyEvidenceRecoveryToOpportunities([o], { findymailLedger: ledgerNotFound });
  const after = buildHotelContactBestEffortLedger(rec.opportunities, {
    findymailLedger: ledgerNotFound,
  });
  assert.equal(after.scorecard.emailNotYetResearched, 0);
  // telephone / linkedin untouched (no evidence)
  assert.equal(after.scorecard.telephoneNotYetResearched, 1);
  assert.equal(after.scorecard.linkedinNotYetResearched, 1);
});

check("prior_artifact_hydrates_flags_only", () => {
  const r = recoverContactChannelResearchFlags(
    { name: "Ann Smith", organization: "Acme" },
    {
      findymailLedger: [],
      priorResearchArtifacts: [
        {
          name: "Ann Smith",
          organization: "Acme",
          channelsResearched: { linkedin: true },
          source: "artifact-1",
        },
      ],
    },
  );
  assert.equal(r.person.contactChannelResearch.linkedinResearched, true);
  assert.equal(r.person.linkedinUrl, undefined);
});

check("display_name_not_mutated_without_flag", () => {
  const person = { name: "Alejandra (Alex) Gutierrez, PhD", organization: "Grifols" };
  const r = recoverContactChannelResearchFlags(person, {
    findymailLedger: [],
    normalizeIdentity: true,
  });
  assert.equal(r.person.name, person.name);
  assert.equal(r.person.enrichmentName, "Alejandra Gutierrez");
  const m = recoverContactChannelResearchFlags(person, {
    findymailLedger: [],
    mutateDisplayName: true,
  });
  assert.equal(m.person.name, "Alejandra Gutierrez");
});

// ── looksLikeFullPersonName / normalization ─────────────────────────────────
check("full_name_allows_parentheses_and_credentials", () => {
  assert.equal(looksLikeFullPersonName("Alejandra (Alex) Gutierrez, PhD"), true);
  assert.equal(looksLikeFullPersonName("Jane Doe, PhD, MBA"), true);
  assert.equal(looksLikeFullPersonName("John R. Mosack"), true);
  assert.equal(looksLikeFullPersonName("Sandra Sifuentes-Rovello"), true);
});

check("full_name_still_rejects_abbreviated_surnames", () => {
  assert.equal(looksLikeFullPersonName("Lily Y."), false);
  assert.equal(looksLikeFullPersonName("Suzanne K"), false);
  assert.equal(looksLikeFullPersonName("Suzanne K."), false);
  assert.equal(looksLikeFullPersonName("Lily Y., PhD"), false);
  assert.equal(looksLikeFullPersonName("Madonna"), false);
  assert.equal(looksLikeFullPersonName("(Alex) PhD"), false);
});

check("normalization_does_not_strip_surname_like_tokens", () => {
  assert.equal(normalizePersonNameForEnrichment("Mary Jane Ma").normalized, "Mary Jane Ma");
  assert.equal(
    normalizePersonNameForEnrichment("Alejandra (Alex) Gutierrez, PhD").normalized,
    "Alejandra Gutierrez",
  );
});

check("findymail_eligibility_after_identity_fix", () => {
  const e = assessFindymailEmailEligibility(
    { name: "Alejandra (Alex) Gutierrez, PhD", organization: "Grifols" },
    { organizationName: "Grifols" },
    { ledger: [] },
  );
  assert.equal(e.eligible, true);
  assert.equal(e.enrichmentName, "Alejandra Gutierrez");
  const abbreviated = assessFindymailEmailEligibility(
    { name: "Lily Y.", organization: "Bora" },
    { organizationName: "Bora" },
    { ledger: [] },
  );
  assert.equal(abbreviated.reason, "IDENTITY_INCOMPLETE");
});

if (failed) {
  console.error(`${failed} test(s) failed`);
  process.exit(1);
}
console.log("OK test:gdi-close-e2e-cert-gaps-v1");
