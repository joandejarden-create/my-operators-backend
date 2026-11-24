#!/usr/bin/env node
/**
 * Permanent CI — Agent recovery hardening ($0).
 *   node scripts/test-gdi-agent-recovery-hardening-v1.mjs
 */
import assert from "node:assert/strict";
import {
  classifyPublicContactChannel,
  assessPublicContactVerification,
  PUBLIC_CONTACT_CHANNEL_KIND,
  PUBLIC_CONTACT_VERIFICATION_STATUS,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-verification-v1.js";
import { assessWhoHowCompletenessGates } from "../lib/group-demand-intelligence/workflow/gdi-who-how-contact-intelligence-restore-v1.js";
import {
  classifyVerifiedAgentCandidate,
  evaluateExaAgentContactEscalationEligibility,
  AGENT_CANDIDATE_TYPE,
} from "../lib/group-demand-intelligence/workflow/gdi-exa-agent-contact-escalation-v1.js";

let passed = 0;
function check(name, fn) {
  fn();
  passed += 1;
  console.log(`  PASS ${name}`);
}

check("contacto@ shared inbox cannot be DIRECT_WORK_EMAIL", () => {
  const c = classifyPublicContactChannel({
    channel: "email",
    value: "contacto@amegendoscopia.org.mx",
    personName: "Mayra Membrillo Garduño",
    organizationName: "AMEG",
    contact: {
      channelSourceUrl: "https://amegendoscopia.org.mx/contact",
      contactDataOrigin: "PUBLIC_SOURCE",
      channelLastVerifiedAt: "2026-11-24",
    },
  });
  assert.equal(c.kind, PUBLIC_CONTACT_CHANNEL_KIND.ORGANIZATION_EMAIL_ROUTE);
  assert.notEqual(c.kind, PUBLIC_CONTACT_CHANNEL_KIND.DIRECT_WORK_EMAIL);
});

check("Larry Lawrence person email remains DIRECT_WORK_EMAIL", () => {
  const c = classifyPublicContactChannel({
    channel: "email",
    value: "llawrence@grenadaco-opbank.com",
    personName: "Larry Lawrence",
    organizationName: "Grenada Co-operative Bank",
    contact: {
      channelSourceUrl: "https://imrtgrenada.com/",
      contactDataOrigin: "PUBLIC_SOURCE",
      channelLastVerifiedAt: "2026-11-24",
    },
  });
  assert.equal(c.kind, PUBLIC_CONTACT_CHANNEL_KIND.DIRECT_WORK_EMAIL);
  assert.equal(c.status, PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_DIRECT);
});

check("switchboard phone cannot be verified personal direct", () => {
  const c = classifyPublicContactChannel({
    channel: "phone",
    value: "+1 473 440 2111",
    personName: "Larry Lawrence",
    organizationName: "Bank",
    contact: {
      phoneType: "main line / switchboard",
      channelSourceUrl: "https://example.com/contact",
      contactDataOrigin: "PUBLIC_SOURCE",
      channelLastVerifiedAt: "2026-11-24",
    },
  });
  assert.equal(c.kind, PUBLIC_CONTACT_CHANNEL_KIND.ORGANIZATION_PHONE_ROUTE);
});

check("missing email keeps WHO_COMPLETE; Sales Ready stays false", () => {
  const g = assessWhoHowCompletenessGates({
    organizationName: "Advantage Travel Partnership",
    primaryContact: {
      name: "Pelin Dede",
      role: "Head of Supplier Partnerships, Lodging",
      organization: "Advantage Travel Partnership",
    },
    howContactResearchAttempted: true,
    howContactResearchStatus: "HOW_ATTEMPTED_NO_CHANNEL",
  });
  assert.equal(g.whoComplete, true);
  assert.equal(g.salesReady, false);
});

check("viable alternative classification preserved (seniority ≠ direct buyer)", () => {
  const cls = classifyVerifiedAgentCandidate(
    {
      name: "Jeroen Posma",
      title: "Executive Director / Managing Director",
      currentOrganization: "Mexico Business Events",
      sourceUrl: "https://mexicobusiness.com/about/leadership",
      relevanceEvidence: "Founder and ED; lodging authority not evidenced",
    },
    { organizationName: "Mexico Business Events" },
  );
  assert.equal(cls.type, AGENT_CANDIDATE_TYPE.VIABLE_ALTERNATIVE);
  assert.notEqual(cls.type, AGENT_CANDIDATE_TYPE.DIRECT_BUYER);
});

check("Agent escalation remains OFF by default", () => {
  const elig = evaluateExaAgentContactEscalationEligibility({
    opportunity: {
      opportunityId: "disc_test",
      organizationName: "Org",
      demandValidated: true,
    },
    ladderExhausted: true,
    opts: {
      demandVerified: true,
      exaAgentContactOpportunityAllowlist: ["disc_test"],
      exaAgentContactRemainingBudgetUsd: 1,
    },
  });
  assert.equal(elig.eligible, false);
  assert.match(String(elig.reason), /FLAG_OFF|CONTACT_ESCALATION_FLAG_OFF/i);
});

check("Agent candidate with wrong employer is rejected", () => {
  const cls = classifyVerifiedAgentCandidate(
    {
      name: "Monica Ribeiro",
      title: "Partner",
      currentOrganization: "Unrelated Venue Partner",
      sourceUrl: "https://linkedin.com/in/monica-ribeiro-2b404b69",
    },
    { organizationName: "AFCONA Additives" },
  );
  assert.equal(cls.type, AGENT_CANDIDATE_TYPE.REJECTED);
});

check("org inbox assessment never sets hasVerifiedDirectEmail", () => {
  const a = assessPublicContactVerification({
    organizationName: "AMEG",
    primaryContact: {
      name: "Mayra Membrillo Garduño",
      email: "contacto@amegendoscopia.org.mx",
      channelSourceUrl: "https://amegendoscopia.org.mx/contact",
      contactDataOrigin: "PUBLIC_SOURCE",
      channelLastVerifiedAt: "2026-11-24",
    },
  });
  assert.equal(a.hasVerifiedDirectEmail, false);
  assert.equal(a.hasVerifiedOrgEmailRoute, true);
});

console.log(`\ntest-gdi-agent-recovery-hardening-v1: PASS (${passed})`);
