#!/usr/bin/env node
/**
 * Regressions: invalid domain + wrong-person LinkedIn must not pass provider submission gate.
 */
import assert from "node:assert/strict";
import {
  validateProviderSubmissionInput,
  validateLinkedInIdentifier,
  linkedInNameTokenAgreement,
  classifyInvalidInputEvaluation,
  SUBMISSION_REJECT_REASON,
} from "../lib/hotel-intelligence/contact-intelligence/provider-submission-gate.js";
import { gateProviderCandidates } from "../lib/hotel-intelligence/contact-intelligence/fullenrich-gated-submit.js";

// --- Wrong LinkedIn person (Carlos Justo ← roccobova) ---
const wrongLi = validateLinkedInIdentifier({
  personName: "Carlos Justo",
  linkedinUrl: "https://mx.linkedin.com/in/roccobova",
  orgName: "Alliance Hotel Management",
  roleTitle: "Named buyer principal",
  independentlyConfirmed: false,
});
assert.equal(wrongLi.decision, "OMIT_IDENTIFIER");
assert.equal(wrongLi.reason, SUBMISSION_REJECT_REASON.LINKEDIN_WRONG_PERSON);
assert.equal(wrongLi.linkedin_url, null);

const nameScreen = linkedInNameTokenAgreement("Carlos Justo", "https://mx.linkedin.com/in/roccobova");
assert.equal(nameScreen.ok, false);

// Independently confirmed Rolf LinkedIn passes name screen + confirmation flag
const rolfLi = validateLinkedInIdentifier({
  personName: "Rolf Tweeten",
  linkedinUrl: "https://www.linkedin.com/in/rolftweeten",
  orgName: "Alliance Hospitality Management LLC",
  roleTitle: "Chairman / Investor",
  independentlyConfirmed: true,
  evidenceNote: "Deep-research LinkedIn for Alliance Hospitality Management LLC",
});
assert.equal(rolfLi.decision, "ALLOW");
assert.ok(rolfLi.linkedin_url);

// Name tokens alone without org context → omit
const nameOnly = validateLinkedInIdentifier({
  personName: "Rolf Tweeten",
  linkedinUrl: "https://www.linkedin.com/in/rolftweeten",
  orgName: "Alliance Hotel Management",
  independentlyConfirmed: false,
  evidenceNote: null,
  profileCompany: null,
  profileHeadline: null,
});
assert.equal(nameOnly.decision, "OMIT_IDENTIFIER");
assert.equal(nameOnly.reason, SUBMISSION_REJECT_REASON.LINKEDIN_ORG_CONTEXT_MISSING);

// --- Rejected domain (alliancehm.com Morocco car rental) ---
const rejectedDomain = validateProviderSubmissionInput({
  provider: "fullenrich",
  person: {
    display_name: "Carlos Justo",
    title: "Named buyer principal",
    publication_label: "EVIDENCED_ORG_PERSON_CANDIDATE",
    identity_supported: true,
  },
  organization: {
    name: "Alliance Hotel Management",
    entity_id: "ent_alliance-hotel-management",
    relationship_supported: true,
  },
  identifiers: {
    domain: { value: "alliancehm.com", status: "REJECTED" },
    linkedin_url: {
      value: "https://mx.linkedin.com/in/carlos-justo-2ab2a2177",
      independently_confirmed: true,
      evidence_note: "Deep-research Alliance LinkedIn",
    },
  },
  rejected_domains: ["alliancehm.com"],
});
assert.equal(rejectedDomain.ok, false);
assert.ok(
  rejectedDomain.violations.some((v) => v.code === SUBMISSION_REJECT_REASON.DOMAIN_REJECTED),
  "rejected domain must violate"
);

// Rejected domain + wrong LI cannot be salvaged by dropping LI alone if domain still forced
const carlosInvalid = validateProviderSubmissionInput({
  provider: "fullenrich",
  person: {
    display_name: "Carlos Justo",
    identity_supported: true,
    publication_label: "EVIDENCED_ORG_PERSON_CANDIDATE",
  },
  organization: {
    name: "Alliance Hotel Management",
    relationship_supported: true,
  },
  identifiers: {
    domain: { value: "alliancehm.com", status: "REJECTED" },
    linkedin_url: { value: "https://mx.linkedin.com/in/roccobova" },
  },
  rejected_domains: ["alliancehm.com"],
});
assert.equal(carlosInvalid.ok, false);

// LinkedIn-only alternative chain (no corporate website) when LI independently confirmed
const liOnly = validateProviderSubmissionInput({
  provider: "fullenrich",
  person: {
    display_name: "Rolf Tweeten",
    title: "Chairman / Investor, Alliance Hospitality Management LLC",
    identity_supported: true,
    publication_label: "EVIDENCED_ORG_PERSON_CANDIDATE",
    why_relevant: "Alliance Hospitality Management LLC chairman",
  },
  organization: {
    name: "Alliance Hospitality Management LLC",
    entity_id: "ent_alliance-hotel-management",
    relationship_supported: true,
  },
  identifiers: {
    domain: { status: "ABSENT" },
    linkedin_url: {
      value: "https://www.linkedin.com/in/rolftweeten",
      independently_confirmed: true,
      evidence_note: "Alliance Hospitality Management LLC",
      profile_company: "Alliance Hospitality Management LLC",
    },
  },
  rejected_domains: ["alliancehm.com"],
});
assert.equal(liOnly.ok, true, JSON.stringify(liOnly.violations));
assert.equal(liOnly.sanitized_identifiers.domain, null);
assert.equal(liOnly.sanitized_identifiers.linkedin_url, "https://www.linkedin.com/in/rolftweeten");
assert.equal(liOnly.alternative_chain_allowed, true);

// Hotel/operator domain must not become owner domain
const hotelAsOwner = validateProviderSubmissionInput({
  provider: "fullenrich",
  person: {
    display_name: "Rolf Tweeten",
    identity_supported: true,
    publication_label: "EVIDENCED_ORG_PERSON_CANDIDATE",
  },
  organization: { name: "Alliance Hotel Management", relationship_supported: true },
  identifiers: {
    domain: { value: "ihg.com", status: "CONFIRMED", independently_supported: true },
    linkedin_url: {
      value: "https://www.linkedin.com/in/rolftweeten",
      independently_confirmed: true,
    },
  },
  forbidden_owner_domain_hosts: ["ihg.com", "aimbridgelatam.com", "marriott.com"],
});
assert.equal(hotelAsOwner.ok, false);
assert.ok(
  hotelAsOwner.violations.some((v) => v.code === SUBMISSION_REJECT_REASON.HOTEL_OR_OPERATOR_DOMAIN_AS_OWNER)
);

// gateProviderCandidates drops invalid rows
const gated = gateProviderCandidates([
  {
    id: "bad_carlos",
    person: {
      display_name: "Carlos Justo",
      identity_supported: true,
      publication_label: "EVIDENCED_ORG_PERSON_CANDIDATE",
    },
    organization: { name: "Alliance Hotel Management", relationship_supported: true },
    identifiers: {
      domain: { value: "alliancehm.com", status: "REJECTED" },
      linkedin_url: { value: "https://mx.linkedin.com/in/roccobova" },
    },
    rejected_domains: ["alliancehm.com"],
  },
  {
    id: "good_rolf",
    person: {
      display_name: "Rolf Tweeten",
      identity_supported: true,
      publication_label: "EVIDENCED_ORG_PERSON_CANDIDATE",
      title: "Chairman / Investor",
    },
    organization: {
      name: "Alliance Hospitality Management LLC",
      relationship_supported: true,
    },
    identifiers: {
      linkedin_url: {
        value: "https://www.linkedin.com/in/rolftweeten",
        independently_confirmed: true,
        profile_company: "Alliance Hospitality Management LLC",
      },
    },
    rejected_domains: ["alliancehm.com"],
  },
], { workflow: "owner_person_search" });
assert.equal(gated.allowed.length, 1);
assert.equal(gated.allowed[0].id, "good_rolf");
assert.equal(gated.rejected.length, 1);
assert.equal(gated.rejected[0].id, "bad_carlos");

const invalidClass = classifyInvalidInputEvaluation({
  subject_id: "recTYaiA4S6fR6ixx__Carlos_Justo",
  person: "Carlos Justo",
  submitted_identifiers: {
    domain: "alliancehm.com",
    linkedin_url: "https://mx.linkedin.com/in/roccobova",
  },
  rejection_reasons: ["DOMAIN_REJECTED", "LINKEDIN_WRONG_PERSON"],
  raw_provider_response: { preserved: true },
});
assert.equal(invalidClass.classification, "INVALID_INPUT_EVALUATION");
assert.equal(invalidClass.exclude_from_provider_coverage_metrics, true);

// Host without owner token must not confirm (neuron.com ≠ Alliance)
assert.equal(
  validateProviderSubmissionInput({
    provider: "fullenrich",
    person: {
      display_name: "Rolf Tweeten",
      identity_supported: true,
      publication_label: "EVIDENCED_ORG_PERSON_CANDIDATE",
    },
    organization: {
      name: "Alliance Hospitality Management LLC",
      relationship_supported: true,
    },
    identifiers: {
      domain: { value: "neuron.com", status: "CONFIRMED", independently_supported: true },
      linkedin_url: {
        value: "https://www.linkedin.com/in/rolftweeten",
        independently_confirmed: true,
        profile_company: "Alliance Hospitality Management LLC",
      },
    },
    forbidden_owner_domain_hosts: ["neuron.com", "hcareers.com"],
  }).ok,
  false,
  "neuron.com forbidden as owner domain"
);

console.log("test-contact-intelligence-provider-submission-gate: PASS");
