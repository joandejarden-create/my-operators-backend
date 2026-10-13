#!/usr/bin/env node
/**
 * Focused regressions for owner-person enrichment gate bypasses observed in
 * the 20-hotel DEVELOPMENT experiment (Surfe-only affiliation submitted).
 * Synthetic fixtures only — never mixed into live coverage metrics.
 */
import assert from "node:assert/strict";
import {
  validateProviderSubmissionInput,
} from "../lib/hotel-intelligence/contact-intelligence/provider-submission-gate.js";
import { gateProviderCandidates } from "../lib/hotel-intelligence/contact-intelligence/fullenrich-gated-submit.js";
import {
  OWNER_PERSON_ENRICHMENT_WORKFLOW,
  OWNER_PERSON_REJECT,
  AFFILIATION_SOURCE_CLASS,
  validateOwnerPersonEnrichmentSubmission,
  evaluateOwnerPersonReturnedContact,
  sponsorQualifiesWithoutDeed,
  isAffiliationCorroborated,
} from "../lib/hotel-intelligence/contact-intelligence/owner-person-enrichment-gate.js";

const SYNTH_HOTEL = "synth_hotel_rec_test_only";
const SYNTH_ORG = "Synth Portfolio Holdings LLC";
const SYNTH_DOMAIN = "synth-portfolio-holdings.example";

function baseOwnerPersonInput(over = {}) {
  return {
    hotel_name: "Synth Bay Hotel",
    workflow: OWNER_PERSON_ENRICHMENT_WORKFLOW,
    provider: "surfe",
    hotel_to_owner: {
      supported: true,
      relationship_class: "ECONOMIC_OWNER_OR_SPONSOR",
      evidence_refs: [{source_url:"https://example.test/synth-acquisition-pr", excerpt:`${SYNTH_ORG} acquired Synth Bay Hotel.`, source_type:"COMPANY_ANNOUNCEMENT"}],
      ...(over.hotel_to_owner || {}),
    },
    affiliation_corroboration: {
      status: "CORROBORATED",
      source_class: AFFILIATION_SOURCE_CLASS.COMPANY_ANNOUNCEMENT,
      independently_corroborated: true,
      evidence_refs: [{source_url:"https://example.test/synth-leadership", excerpt:`Alex Rivera is Director of Acquisitions at ${SYNTH_ORG}.`, source_type:"COMPANY_ANNOUNCEMENT"}],
      ...(over.affiliation_corroboration || {}),
    },
    person: {
      display_name: "Alex Rivera",
      full_name: "Alex Rivera",
      first_name: "Alex",
      last_name: "Rivera",
      identity_supported: true,
      title: "Director of Acquisitions",
      publication_label: "EVIDENCED",
      ...(over.person || {}),
    },
    organization: {
      name: SYNTH_ORG,
      relationship_supported: true,
      ...(over.organization || {}),
    },
    identifiers: {
      domain: {
        value: SYNTH_DOMAIN,
        evidence_refs:[{source_url:`https://${SYNTH_DOMAIN}/about`,excerpt:`Welcome to ${SYNTH_ORG}, our official corporate website.`}],
        status: "CONFIRMED_FIRST_PARTY",
        independently_supported: true,
      },
      ...(over.identifiers || {}),
    },
    ...over.rest,
  };
}

// 1) Surfe-only affiliation cannot pass owner-person enrichment
{
  const surfeOnly = validateOwnerPersonEnrichmentSubmission(
    baseOwnerPersonInput({
      affiliation_corroboration: {
        status: "SURFE_ONLY",
        source_class: AFFILIATION_SOURCE_CLASS.SURFE_ONLY,
        independently_corroborated: false,
        evidence_refs: [],
      },
    })
  );
  assert.equal(surfeOnly.ok, false);
  assert.ok(
    surfeOnly.violations.some((v) => v.code === OWNER_PERSON_REJECT.AFFILIATION_SURFE_ONLY),
    "Surfe-only must be rejected"
  );
}

// 2) Related domain alone cannot establish person attribution
{
  const related = evaluateOwnerPersonReturnedContact({
    personName: "Alex Rivera",
    targetOrganization: "Synth Iberostar Group",
    targetDomain: "grupoiberostar.example",
    providerEmail: "alex.rivera@iberostar.example",
    providerValidationStatus: "VALID",
    relatedDomainAllowedWithOrgSupport: false,
    organizationRelationshipSupported: false,
    personAttributionSupported: false,
  });
  assert.equal(related.accept_person_attributed_email, false);
  assert.equal(related.contact_class, "RELATED_DOMAIN_UNATTRIBUTED");
}

// 3) Returned wrong-person identity is rejected
{
  const wrong = evaluateOwnerPersonReturnedContact({
    personName: "Alex Rivera",
    targetDomain: SYNTH_DOMAIN,
    providerEmail: "alex.rivera@synth-portfolio-holdings.example",
    providerFullName: "Jordan Blake",
    providerValidationStatus: "VALID",
  });
  assert.equal(wrong.accept_person_attributed_email, false);
  assert.equal(wrong.contact_class, "RETURNED_WRONG_PERSON");
}

// 4) Supported sponsor can qualify despite unresolved deed/UBO
{
  const sponsor = sponsorQualifiesWithoutDeed({
    relationshipClass: "ECONOMIC_OWNER_OR_SPONSOR",
    evidenceSupported: true,
  });
  assert.equal(sponsor.qualifies, true);
  assert.equal(sponsor.deed_ubo_required, false);
}

// 5) Candidate discovery allowed before person corroboration
//    (base provider gate without owner_person_enrichment workflow)
{
  const discovery = validateProviderSubmissionInput({
    provider: "surfe",
    person: {
      display_name: "Alex Rivera",
      identity_supported: true,
      publication_label: "SURFE_CANDIDATE",
      title: "Director of Acquisitions",
    },
    organization: { name: SYNTH_ORG, relationship_supported: true },
    identifiers: {
      domain: {
        value: SYNTH_DOMAIN,
        evidence_refs:[{source_url:`https://${SYNTH_DOMAIN}/about`,excerpt:`Welcome to ${SYNTH_ORG}, our official corporate website.`}],
        status: "CONFIRMED_FIRST_PARTY",
        independently_supported: true,
      },
    },
  });
  assert.equal(discovery.ok, true, "people search / domain lookup must still work without affiliation");

  const gatedDiscovery = gateProviderCandidates(
    [
      {
        id: `${SYNTH_HOTEL}_discovery`,
        person: {
          display_name: "Alex Rivera",
          identity_supported: true,
          publication_label: "SURFE_CANDIDATE",
        },
        organization: { name: SYNTH_ORG, relationship_supported: true },
        identifiers: {
          domain: {
            value: SYNTH_DOMAIN,
            status: "CONFIRMED_FIRST_PARTY",
            independently_supported: true,
          },
        },
      },
    ],
    { provider: "surfe", workflow: "owner_person_search" } // explicit discovery scope
  );
  assert.equal(gatedDiscovery.allowed.length, 1);
}

// 6) Public-source affiliation + matched provider email qualifies internally
{
  const publicAff = validateOwnerPersonEnrichmentSubmission(
    baseOwnerPersonInput({
      affiliation_corroboration: {
        status: "CORROBORATED",
        source_class: AFFILIATION_SOURCE_CLASS.LINKEDIN_SELF,
        independently_corroborated: true,
        evidence_refs: [{source_url:"https://www.linkedin.com/in/synth-alex-rivera-announcement",excerpt:`Alex Rivera is Director of Acquisitions at ${SYNTH_ORG}.`,source_type:"LINKEDIN_SELF"}],
      },
    })
  );
  assert.equal(publicAff.ok, true, JSON.stringify(publicAff.violations));
  assert.equal(
    isAffiliationCorroborated({
      source_class: AFFILIATION_SOURCE_CLASS.LINKEDIN_SELF,
      independently_corroborated: true,
      evidence_refs: [{source_url:"https://www.linkedin.com/in/synth-alex-rivera-announcement",excerpt:`Alex Rivera is Director of Acquisitions at ${SYNTH_ORG}.`,source_type:"LINKEDIN_SELF"}],
    }, {personName:"Alex Rivera",organizationName:SYNTH_ORG,title:"Director of Acquisitions"}),
    true
  );
  const returned = evaluateOwnerPersonReturnedContact({
    personName: "Alex Rivera",
    targetOrganization: SYNTH_ORG,
    targetDomain: SYNTH_DOMAIN,
    providerEmail: "alex.rivera@synth-portfolio-holdings.example",
    qualificationInput: baseOwnerPersonInput(),
    providerFullName: "Alex Rivera",
    providerValidationStatus: "VALID",
  });
  assert.equal(returned.accept_person_attributed_email, true);
  assert.equal(returned.contact_class, "CORROBORATED_PERSON_PROVIDER_ATTRIBUTED_EMAIL");
  assert.equal(returned.labels.freshness, "UNKNOWN");
  assert.match(returned.labels.usage_rights, /LICENSING_PENDING/);
}

// Hotel/GDI path must not inherit hotel-ownership requirements
{
  const hotelContact = gateProviderCandidates(
    [
      {
        id: "hotel_switchboard",
        person: {
          display_name: "Front Desk",
          identity_supported: true,
          publication_label: "EVIDENCED",
        },
        organization: { name: "Synth Hotel", relationship_supported: true },
        identifiers: {
          domain: {
            value: "synthhotel.example",
            status: "CONFIRMED_FIRST_PARTY",
            independently_supported: true,
          },
        },
      },
    ],
    { provider: "surfe", workflow: "hotel_contact" }
  );
  assert.equal(hotelContact.allowed.length, 1, "hotel-contact workflow must not require owner affiliation");
}

// Observed bypass pattern: forging EVIDENCED + confirmed domain without affiliation refs
{
  const bypass = gateProviderCandidates(
    [
      {
        id: "bypass_surfe_only",
        workflow: OWNER_PERSON_ENRICHMENT_WORKFLOW,
        hotel_to_owner: {
          supported: true,
          relationship_class: "ECONOMIC_OWNER_OR_SPONSOR",
          evidence_refs: ["https://example.test/owner"],
        },
        affiliation_corroboration: {
          status: "SURFE_ONLY",
          source_class: "SURFE_ONLY",
          independently_corroborated: false,
          evidence_refs: [],
        },
        person: {
          display_name: "Candice Synth",
          identity_supported: true,
          title: "Training and Development Manager",
          publication_label: "EVIDENCED",
        },
        organization: { name: SYNTH_ORG, relationship_supported: true },
        identifiers: {
          domain: {
            value: SYNTH_DOMAIN,
            status: "CONFIRMED_FIRST_PARTY",
            independently_supported: true,
          },
        },
      },
    ],
    { provider: "surfe", workflow: OWNER_PERSON_ENRICHMENT_WORKFLOW }
  );
  assert.equal(bypass.allowed.length, 0);
  assert.ok(
    bypass.rejected[0].gate.violations.some(
      (v) =>
        v.code === OWNER_PERSON_REJECT.AFFILIATION_SURFE_ONLY ||
        v.code === OWNER_PERSON_REJECT.ROLE_NOT_RELEVANT ||
        v.code === OWNER_PERSON_REJECT.EVIDENCE_REFS_MISSING
    )
  );
}

console.log("test-owner-person-enrichment-gate: PASS");
