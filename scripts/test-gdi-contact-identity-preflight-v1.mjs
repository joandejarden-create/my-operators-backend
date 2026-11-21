/**
 * Offline gate — Contact Identity Preflight V1
 */
import assert from "node:assert/strict";
import {
  CONTACT_IDENTITY_READINESS,
  LINKEDIN_PROFILE_MATCH,
  EMPLOYER_RELATIONSHIP,
  splitContactPersonName,
  evaluatePersonalLinkedInUrl,
  computeContactIdentityReadiness,
  buildContactIdentityRecord,
  buildSurfeEnrichmentIdentityInput,
  runContactIdentityPreflight,
  buildLinkedInIdentityQueries,
} from "../lib/group-demand-intelligence/presentation/gdi-contact-identity-preflight-v1.js";
import {
  buildGoogleLinkedInIdentityPrompt,
} from "../lib/group-demand-intelligence/presentation/gdi-google-grounded-contact-discovery-v1.js";
import {
  SURFE_SEARCH_PEOPLE_SCHEMA,
  assessSurfeSearchPeopleReadiness,
} from "../lib/group-demand-intelligence/presentation/gdi-surfe-search-people-readiness-v1.js";

function testNameSplit() {
  const a = splitContactPersonName("Abbas Zavar");
  assert.equal(a.firstName, "Abbas");
  assert.equal(a.lastName, "Zavar");
  const b = splitContactPersonName("Dra. Carmen Encarnación");
  assert.ok(b.nameVariants.some((v) => /Carmen/i.test(v)));
}

function testRejectCompanyLinkedIn() {
  const r = evaluatePersonalLinkedInUrl({
    url: "https://www.linkedin.com/company/pda",
    personName: "Laura Keeley",
    employerName: "PDA",
  });
  assert.equal(r.status, LINKEDIN_PROFILE_MATCH.NO_PROFILE_FOUND);
  assert.equal(r.rejectReason, "COMPANY_LINKEDIN_PAGE_NOT_PERSONAL");
  assert.equal(r.trustedForSurfe, false);
}

function testVerifiedLinkedInReady() {
  const identity = buildContactIdentityRecord({
    opportunity: {
      id: "rx_d_2400ebe4c2bb",
      organizationName: "University of Toronto",
      organizationDomain: "utoronto.ca",
    },
    contact: {
      name: "Abbas Zavar",
      title: "Researcher",
      employerName: "University of Toronto",
      employerDomain: "utoronto.ca",
      linkedinUrl: "https://www.linkedin.com/in/abbaszavar/",
      linkedinEvidenceText:
        "Abbas Zavar University of Toronto Institute of Health Policy",
    },
    assumeDemandOrgIsEmployer: true,
  });
  assert.equal(identity.linkedinTrustedForSurfe, true);
  assert.equal(identity.readiness, CONTACT_IDENTITY_READINESS.READY);
  const surfe = buildSurfeEnrichmentIdentityInput(identity);
  assert.equal(surfe.surfeCallExecuted, false);
  assert.equal(surfe.paidCallAuthorized, false);
  assert.ok(surfe.person.linkedInUrl);
  assert.equal(surfe.person.firstName, "Abbas");
  assert.equal(surfe.person.companyDomain, "utoronto.ca");
}

function testReadyWithoutDomainStillReady() {
  const identity = buildContactIdentityRecord({
    contact: {
      name: "Abbas Zavar",
      employerName: "University of Toronto",
      linkedinUrl: "https://www.linkedin.com/in/abbaszavar/",
      linkedinEvidenceText: "Abbas Zavar University of Toronto researcher",
    },
  });
  assert.equal(identity.linkedinTrustedForSurfe, true);
  assert.equal(identity.readiness, CONTACT_IDENTITY_READINESS.READY);
}

function testPartialWithoutLinkedIn() {
  const identity = buildContactIdentityRecord({
    contact: {
      name: "Alejandra Vázquez",
      title: "Sales Manager",
      employerName: "IGECO",
      email: "alejandra.vazquez@igeco.mx",
    },
  });
  assert.equal(identity.employerDomain, "igeco.mx");
  assert.equal(identity.employerDomainVerified, true);
  assert.equal(identity.readiness, CONTACT_IDENTITY_READINESS.PARTIAL);
  assert.ok(identity.readinessGaps.includes("LINKEDIN_MISSING"));
  const surfe = buildSurfeEnrichmentIdentityInput(identity);
  assert.equal(surfe.person.linkedInUrl, undefined);
  assert.equal(surfe.person.companyDomain, "igeco.mx");
  assert.equal(surfe.sufficient, true);
}

function testUnresolvedNoEmployer() {
  const identity = buildContactIdentityRecord({
    opportunity: { organizationName: "Big Event Organizer Inc" },
    contact: { name: "Jane Doe", title: "Speaker" },
    // Do not assume demand org is employer
    assumeDemandOrgIsEmployer: false,
  });
  assert.equal(identity.readiness, CONTACT_IDENTITY_READINESS.UNRESOLVED);
  assert.ok(identity.readinessGaps.includes("EMPLOYER_UNRESOLVED"));
}

function testEmployerOrganizerMismatch() {
  const identity = buildContactIdentityRecord({
    opportunity: {
      organizationName: "Parenteral Drug Association",
      organizationDomain: "pda.org",
    },
    contact: {
      name: "Bacilio Sánchez",
      employerName: "SPA Consultores",
      email: "bsanchez@spaconsultores.com.do",
      title: "Advisor",
    },
  });
  assert.equal(identity.demandOrganizationDistinct, true);
  assert.equal(identity.employerDomain, "spaconsultores.com.do");
  assert.notEqual(identity.employerDomain, "pda.org");
  assert.equal(
    identity.employerRelationship,
    EMPLOYER_RELATIONSHIP.DIRECT_EMPLOYER,
  );
}

function testUnverifiedLinkedInOmittedFromSurfe() {
  const identity = buildContactIdentityRecord({
    contact: {
      name: "Wrong Person",
      employerName: "Acme Corp",
      employerDomain: "acme.com",
      linkedinUrl: "https://www.linkedin.com/in/totally-different-slug",
      linkedinEvidenceText: "unrelated bio",
    },
  });
  assert.notEqual(
    identity.linkedinStatus,
    LINKEDIN_PROFILE_MATCH.VERIFIED_LINKEDIN_PROFILE,
  );
  assert.equal(identity.linkedinTrustedForSurfe, false);
  const surfe = buildSurfeEnrichmentIdentityInput(identity);
  assert.equal(surfe.person.linkedInUrl, undefined);
  assert.equal(surfe.omittedUnverifiedLinkedIn, true);
}

function testQueries() {
  const q = buildLinkedInIdentityQueries({
    fullName: "Amy Drow",
    employerName: "National Down Syndrome Society",
    title: "Director",
    domain: "ndss.org",
  });
  assert.ok(q.some((x) => /site:linkedin.com\/in/.test(x)));
  assert.ok(q.some((x) => /ndss\.org/.test(x)));
}

function testReadinessEdges() {
  const r = computeContactIdentityReadiness({
    firstName: "A",
    lastName: "B",
    employerName: "Org",
    employerDomain: "org.com",
    employerDomainVerified: true,
    linkedinTrustedForSurfe: false,
    linkedinStatus: LINKEDIN_PROFILE_MATCH.NO_PROFILE_FOUND,
  });
  assert.equal(r.status, CONTACT_IDENTITY_READINESS.PARTIAL);
}

function testRun() {
  const out = runContactIdentityPreflight({
    contact: {
      name: "Adam Jordan",
      employerName: "InsuranceERM",
      email: "adam.jordan@insuranceerm.com",
    },
  });
  assert.equal(out.surfeInput.surfeCallExecuted, false);
  assert.ok(out.researchContinuation);
}

function testLinkedInIdentityPrompt() {
  const p = buildGoogleLinkedInIdentityPrompt(
    { name: "Olivia Jallits", title: "Event Director" },
    { name: "IAADFS", domain: "iaadfs.org" },
  );
  assert.match(p.text, /linkedin\.com\/in/);
  assert.match(p.text, /Olivia Jallits/);
  assert.equal(p.promptId, "gdi_google_linkedin_identity_v1");
}

function testSurfeSearchSchema() {
  assert.equal(
    SURFE_SEARCH_PEOPLE_SCHEMA.matchingMode,
    "ORGANIZATION_AND_ROLE_DISCOVERY",
  );
  assert.ok(
    SURFE_SEARCH_PEOPLE_SCHEMA.requestFields.peopleFiltersMissing.includes(
      "firstName",
    ),
  );
  assert.equal(typeof assessSurfeSearchPeopleReadiness, "function");
}

testNameSplit();
testRejectCompanyLinkedIn();
testVerifiedLinkedInReady();
testReadyWithoutDomainStillReady();
testPartialWithoutLinkedIn();
testUnresolvedNoEmployer();
testEmployerOrganizerMismatch();
testUnverifiedLinkedInOmittedFromSurfe();
testQueries();
testReadinessEdges();
testRun();
testLinkedInIdentityPrompt();
testSurfeSearchSchema();
console.log("test-gdi-contact-identity-preflight-v1: PASS");
