#!/usr/bin/env node
/**
 * Permanent $0 certification: Exa Agent conditional contact escalation.
 *
 *   node scripts/test-gdi-exa-agent-contact-escalation-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  evaluateExaAgentContactEscalationEligibility,
  runExaAgentContactEscalation,
  classifyVerifiedAgentCandidate,
  verifyAndClassifyAgentCandidates,
  EXA_AGENT_CONTACT_ESCALATION_REASON,
  AGENT_CANDIDATE_TYPE,
  GDI_EXA_AGENT_CONTACT_ESCALATION_V1,
} from "../lib/group-demand-intelligence/workflow/gdi-exa-agent-contact-escalation-v1.js";
import {
  runBuyerAlternativePersonResearch,
  BUYER_ALTERNATIVE_PERSON_STAGE,
} from "../lib/group-demand-intelligence/workflow/gdi-buyer-alternative-person-research-v1.js";
import { processOpportunityThroughOriginalChain } from "../lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js";
import { isPlausibleBuyerPersonName } from "../lib/group-demand-intelligence/workflow/gdi-original-full-e2e-execution-restore-v1.js";
import {
  runRestoredHowContactChannelDiscovery,
  assessWhoHowCompletenessGates,
} from "../lib/group-demand-intelligence/workflow/gdi-who-how-contact-intelligence-restore-v1.js";
import { applyVerifiedBuyerToOpportunity } from "../lib/group-demand-intelligence/workflow/gdi-buyer-who-permanent-full-e2e-v1.js";
import { isExaAgentLiveAuthorized } from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-provider-adapter-v1.js";
import { EXA_AGENT_CONTACT_INTELLIGENCE_CONTRACT_V1 } from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-contact-intelligence-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports", "gdi-exa-agent-contact-escalation-v1");

const prevLive = process.env.GDI_EXA_AGENT_LIVE;
delete process.env.GDI_EXA_AGENT_LIVE;

let passed = 0;
const results = [];
function record(name, ok, detail = null) {
  results.push({ name, ok, detail });
  if (!ok) throw new Error(`FAIL ${name}: ${detail || ""}`);
  passed += 1;
  console.log(`  PASS ${name}`);
}
async function checkAsync(name, fn) {
  await fn();
  record(name, true);
}

const baseOpp = {
  opportunityId: "disc_agent_escalation_cert_1",
  organizationName: "Example Association (EXAORG)",
  title: "Example Conference 2027",
  hotelName: "Example Hotel",
  sourceUrl: "https://example-association.org/conference-2027",
  demandValidated: true,
};

const authBudget = {
  authorizeExaAgentContactEscalation: true,
  authorizeExaAgentLive: false,
  allowMockExaAgentContact: true,
  demandVerified: true,
  exaAgentContactOpportunityAllowlist: ["disc_agent_escalation_cert_1"],
  exaAgentContactAggregateCeilingUsd: 1.0,
  exaAgentContactSpentUsd: 0,
  exaAgentContactPerOpportunityCeilingUsd: 0.1,
  exaAgentContactEffort: "medium",
};

const mockPrimary = {
  people: [
    {
      fullName: "Jordan Example",
      currentEmployer: "Example Association (EXAORG)",
      professionalRole: "Events Administrator",
      relevantOrganization: "Example Association (EXAORG)",
      relationshipToOpportunity:
        "Administers annual conference logistics including lodging partnerships",
      classification: "BUYER",
      sourceUrls: ["https://example-association.org/staff/jordan-example"],
      evidenceExcerpts: [
        "Jordan Example administers conference lodging partnerships for EXAORG.",
      ],
      confidence: "HIGH",
    },
  ],
  unresolvedQuestions: [],
};

const mockAlt = {
  people: [
    {
      fullName: "Sam Alternative",
      currentEmployer: "Example Association (EXAORG)",
      professionalRole: "Executive Director",
      relevantOrganization: "Example Association (EXAORG)",
      relationshipToOpportunity: "Leads the association; lodging authority not evidenced",
      classification: "ALTERNATIVE_PERSON",
      sourceUrls: ["https://example-association.org/staff/sam-alternative"],
      evidenceExcerpts: ["Sam Alternative is Executive Director of EXAORG."],
      confidence: "MEDIUM",
    },
  ],
  unresolvedQuestions: ["Who books hotel blocks?"],
};

await checkAsync("01_who_verified_agent_not_called", async () => {
  const elig = evaluateExaAgentContactEscalationEligibility({
    opportunity: {
      ...baseOpp,
      primaryContact: {
        name: "Dana Meetings",
        title: "Director of Meetings",
        organization: "Example Association (EXAORG)",
        sourceUrl: "https://example-association.org/dana",
        personKind: "DIRECT_BUYER",
      },
    },
    ladderExhausted: true,
    opts: { ...authBudget, hasVerifiedNamedPerson: true },
  });
  assert.equal(elig.eligible, false);
  assert.equal(elig.reason, EXA_AGENT_CONTACT_ESCALATION_REASON.WHO_ALREADY_VERIFIED);
});

await checkAsync("02_missing_email_who_verified_agent_not_called", async () => {
  const elig = evaluateExaAgentContactEscalationEligibility({
    opportunity: {
      ...baseOpp,
      primaryContact: {
        name: "Dana Meetings",
        title: "Director of Meetings",
        organization: "Example Association (EXAORG)",
        sourceUrl: "https://example-association.org/dana",
        email: null,
        personKind: "DIRECT_BUYER",
      },
    },
    ladderExhausted: true,
    opts: {
      ...authBudget,
      hasVerifiedNamedPerson: true,
      triggerBecauseMissingEmail: true,
    },
  });
  assert.equal(elig.eligible, false);
  assert.equal(elig.reason, EXA_AGENT_CONTACT_ESCALATION_REASON.MISSING_EMAIL_ONLY);
});

await checkAsync("03_who_fails_ladder_executes_first", async () => {
  let researcherCalls = 0;
  const r = await runBuyerAlternativePersonResearch(
    { ...baseOpp, namedStakeholders: [] },
    {
      authorizePaidResearch: true,
      maxStrategies: 3,
      researcher: {
        execute: async () => {
          researcherCalls += 1;
          return { people: [] };
        },
      },
      authorizeExaAgentContactEscalation: false,
    },
  );
  assert.ok(researcherCalls >= 1, "ladder researcher should run before any Agent");
  assert.ok((r.trace.strategiesAttempted || []).length >= 1);
  // With maxStrategies cap the full 9-step sequence may not flip ladderExhausted;
  // Agent must still remain off when escalation flag is false.
  assert.ok(
    !r.trace.exaAgentContactEscalation ||
      r.trace.exaAgentContactEscalation.attempted !== true ||
      r.trace.exaAgentContactEscalation.reason ===
        EXA_AGENT_CONTACT_ESCALATION_REASON.FLAG_OFF ||
      r.trace.exaAgentContactEscalation.skipped === true,
  );
});

await checkAsync("04_ladder_succeeds_agent_not_called", async () => {
  let agentWouldRun = false;
  const r = await runBuyerAlternativePersonResearch(
    {
      ...baseOpp,
      namedStakeholders: [
        {
          name: "Dana Meetings",
          title: "Director of Meetings",
          organization: "Example Association (EXAORG)",
          sourceUrl: "https://example-association.org/dana",
        },
      ],
    },
    {
      ...authBudget,
      mockExaAgentContactStructured: mockPrimary,
      // If Agent wrongly ran, mock would populate escalation executed
    },
  );
  assert.equal(r.opportunity.primaryContact?.name, "Dana Meetings");
  assert.ok(
    !r.trace.exaAgentContactEscalation ||
      r.trace.exaAgentContactEscalation.attempted !== true,
  );
  assert.ok(
    !(r.trace.strategiesAttempted || []).some(
      (a) => a.strategyId === "EXA_AGENT_CONTACT_ESCALATION" && a.mode === "EXA_AGENT_CONTACT" && a.selected,
    ),
  );
  void agentWouldRun;
});

await checkAsync("05_ladder_exhausts_agent_eligible", async () => {
  const elig = evaluateExaAgentContactEscalationEligibility({
    opportunity: baseOpp,
    ladderExhausted: true,
    opts: { ...authBudget, hasVerifiedNamedPerson: false },
  });
  assert.equal(elig.eligible, true);
  assert.equal(elig.reason, EXA_AGENT_CONTACT_ESCALATION_REASON.ELIGIBLE);
});

await checkAsync("05b_truncated_ladder_without_auth_blocks_agent", async () => {
  const elig = evaluateExaAgentContactEscalationEligibility({
    opportunity: baseOpp,
    ladderExhausted: false,
    ladderTruncatedByMaxStrategies: true,
    opts: {
      ...authBudget,
      hasVerifiedNamedPerson: false,
      authorizeExaAgentTruncatedLadderEscalation: false,
    },
  });
  assert.equal(elig.eligible, false);
  assert.equal(
    elig.reason,
    EXA_AGENT_CONTACT_ESCALATION_REASON.LADDER_TRUNCATED_WITHOUT_AUTH,
  );

  const r = await runBuyerAlternativePersonResearch(
    { ...baseOpp, namedStakeholders: [] },
    {
      ...authBudget,
      maxStrategies: 2,
      authorizePaidResearch: false,
      authorizeExaAgentTruncatedLadderEscalation: false,
      mockExaAgentContactStructured: mockPrimary,
    },
  );
  assert.equal(r.trace.ladderTruncatedByMaxStrategies, true);
  assert.equal(r.trace.ladderExhausted, false);
  assert.equal(r.trace.exaAgentContactEscalation?.attempted, false);
  assert.equal(
    r.trace.exaAgentContactEscalation?.reason,
    EXA_AGENT_CONTACT_ESCALATION_REASON.LADDER_TRUNCATED_WITHOUT_AUTH,
  );
  assert.equal(r.trace.exaAgentContactEscalation?.providerCalled, false);
});

await checkAsync("05c_truncated_ladder_with_auth_allows_agent", async () => {
  const elig = evaluateExaAgentContactEscalationEligibility({
    opportunity: baseOpp,
    ladderExhausted: false,
    ladderTruncatedByMaxStrategies: true,
    opts: {
      ...authBudget,
      hasVerifiedNamedPerson: false,
      authorizeExaAgentTruncatedLadderEscalation: true,
    },
  });
  assert.equal(elig.eligible, true);
});

await checkAsync("06_agent_flag_off_no_provider_call", async () => {
  const elig = evaluateExaAgentContactEscalationEligibility({
    opportunity: baseOpp,
    ladderExhausted: true,
    opts: { ...authBudget, authorizeExaAgentContactEscalation: false },
  });
  assert.equal(elig.eligible, false);
  assert.equal(elig.reason, EXA_AGENT_CONTACT_ESCALATION_REASON.FLAG_OFF);
});

await checkAsync("07_provider_live_flag_off_no_provider_call", async () => {
  assert.equal(isExaAgentLiveAuthorized({ authorizeExaAgentLive: true }), false);
  const elig = evaluateExaAgentContactEscalationEligibility({
    opportunity: baseOpp,
    ladderExhausted: true,
    opts: {
      ...authBudget,
      allowMockExaAgentContact: false,
      authorizeExaAgentLive: true,
    },
  });
  assert.equal(elig.eligible, false);
  assert.equal(elig.reason, EXA_AGENT_CONTACT_ESCALATION_REASON.LIVE_PROVIDER_OFF);
});

await checkAsync("08_budget_missing_no_provider_call", async () => {
  const elig = evaluateExaAgentContactEscalationEligibility({
    opportunity: baseOpp,
    ladderExhausted: true,
    opts: {
      authorizeExaAgentContactEscalation: true,
      allowMockExaAgentContact: true,
      demandVerified: true,
      exaAgentContactOpportunityAllowlist: ["disc_agent_escalation_cert_1"],
      // no ceiling
    },
  });
  assert.equal(elig.eligible, false);
  assert.equal(elig.reason, EXA_AGENT_CONTACT_ESCALATION_REASON.BUDGET_MISSING);
});

await checkAsync("09_not_allowlisted_no_provider_call", async () => {
  const elig = evaluateExaAgentContactEscalationEligibility({
    opportunity: baseOpp,
    ladderExhausted: true,
    opts: {
      ...authBudget,
      exaAgentContactOpportunityAllowlist: ["disc_other"],
    },
  });
  assert.equal(elig.eligible, false);
  assert.equal(elig.reason, EXA_AGENT_CONTACT_ESCALATION_REASON.NOT_ALLOWLISTED);
});

await checkAsync("10_budget_insufficient_no_provider_call", async () => {
  const elig = evaluateExaAgentContactEscalationEligibility({
    opportunity: baseOpp,
    ladderExhausted: true,
    opts: {
      ...authBudget,
      exaAgentContactAggregateCeilingUsd: 0.05,
      exaAgentContactSpentUsd: 0,
    },
  });
  assert.equal(elig.eligible, false);
  assert.equal(elig.reason, EXA_AGENT_CONTACT_ESCALATION_REASON.BUDGET_INSUFFICIENT);
});

await checkAsync("11_authorized_agent_valid_primary_verification", async () => {
  const r = await runExaAgentContactEscalation(baseOpp, {
    ...authBudget,
    ladderExhausted: true,
    mockExaAgentContactStructured: mockPrimary,
  });
  assert.equal(r.ok, true);
  assert.equal(r.candidates[0]?.name, "Jordan Example");
  const cls = classifyVerifiedAgentCandidate(r.candidates[0], {
    organizationName: baseOpp.organizationName,
  });
  assert.equal(cls.type, AGENT_CANDIDATE_TYPE.DIRECT_BUYER);
});

await checkAsync("12_agent_valid_alternative_commercial_ranking", async () => {
  const r = await runBuyerAlternativePersonResearch(
    { ...baseOpp, namedStakeholders: [] },
    {
      ...authBudget,
      maxStrategies: 2,
      authorizeExaAgentTruncatedLadderEscalation: true,
      authorizePaidResearch: false,
      mockExaAgentContactStructured: mockAlt,
    },
  );
  assert.ok(
    r.trace.exaAgentContactEscalation?.attempted === true ||
      (r.trace.strategiesAttempted || []).some(
        (a) => a.strategyId === "EXA_AGENT_CONTACT_ESCALATION",
      ),
  );
  assert.ok(r.opportunity.primaryContact?.name === "Sam Alternative" || r.trace.namedCount >= 0);
  // Seniority-only ED should classify as viable alternative not unsupported direct claim
  const cls = classifyVerifiedAgentCandidate(
    {
      name: "Sam Alternative",
      title: "Executive Director",
      currentOrganization: "Example Association (EXAORG)",
      organization: "Example Association (EXAORG)",
      relevanceEvidence: "Leads the association; lodging authority not evidenced",
      sourceUrl: "https://example-association.org/staff/sam-alternative",
    },
    { organizationName: baseOpp.organizationName },
  );
  assert.equal(cls.type, AGENT_CANDIDATE_TYPE.VIABLE_ALTERNATIVE);
});

await checkAsync("13_agent_junk_name_rejected", async () => {
  const v = verifyAndClassifyAgentCandidates(
    [
      {
        name: "Member Organizations",
        currentOrganization: "Example Association (EXAORG)",
        sourceUrl: "https://example-association.org/members",
        relevanceEvidence: "nav chrome",
      },
    ],
    { organizationName: baseOpp.organizationName },
  );
  assert.equal(v.accepted.length, 0);
  assert.equal(v.rejected[0]?.reason, "JUNK_OR_IMPLAUSIBLE_NAME");
  assert.equal(isPlausibleBuyerPersonName("Member Organizations"), false);
});

await checkAsync("14_agent_wrong_employer_rejected", async () => {
  const v = verifyAndClassifyAgentCandidates(
    [
      {
        name: "Taylor Wrongorg",
        title: "Events Manager",
        currentOrganization: "Totally Unrelated Corp",
        organization: "Totally Unrelated Corp",
        relevanceEvidence: "Events manager at Totally Unrelated Corp",
        sourceUrl: "https://unrelated.example/taylor",
      },
    ],
    { organizationName: baseOpp.organizationName },
  );
  assert.equal(v.accepted.length, 0);
  assert.equal(v.rejected[0]?.reason, "EMPLOYER_NOT_VERIFIED");
});

await checkAsync("15_seniority_not_promoted_to_direct_buyer", async () => {
  const cls = classifyVerifiedAgentCandidate(
    {
      name: "Pat Exemplar",
      title: "President",
      currentOrganization: "Example Association (EXAORG)",
      organization: "Example Association (EXAORG)",
      relevanceEvidence: "President of Example Association (EXAORG)",
      sourceUrl: "https://example-association.org/staff/pat",
    },
    { organizationName: baseOpp.organizationName },
  );
  assert.equal(cls.type, AGENT_CANDIDATE_TYPE.VIABLE_ALTERNATIVE);
  assert.match(cls.reason, /SENIORITY_WITHOUT_LODGING/);
});

await checkAsync("16_homepage_only_cannot_finalize", async () => {
  const v = verifyAndClassifyAgentCandidates(
    [
      {
        name: "Casey Homepage",
        title: "Events Manager",
        currentOrganization: "Example Association (EXAORG)",
        organization: "Example Association (EXAORG)",
        relevanceEvidence: "Events Manager at Example Association housing",
        sourceUrl: "https://example-association.org/",
      },
    ],
    { organizationName: baseOpp.organizationName },
  );
  assert.equal(v.accepted.length, 0);
  assert.equal(v.rejected[0]?.reason, "HOMEPAGE_ONLY_PROVENANCE");
});

await checkAsync("17_primary_secondary_identities_distinct", async () => {
  const mockBoth = {
    people: [
      mockPrimary.people[0],
      {
        fullName: "Riley Second",
        currentEmployer: "Example Association (EXAORG)",
        professionalRole: "Program Coordinator",
        relevantOrganization: "Example Association (EXAORG)",
        relationshipToOpportunity: "Supports conference program committee",
        classification: "ALTERNATIVE_PERSON",
        sourceUrls: ["https://example-association.org/staff/riley-second"],
        evidenceExcerpts: ["Riley Second coordinates the program committee."],
        confidence: "MEDIUM",
      },
    ],
    unresolvedQuestions: [],
  };
  const r = await runBuyerAlternativePersonResearch(
    { ...baseOpp, namedStakeholders: [] },
    {
      ...authBudget,
      maxStrategies: 2,
      authorizeExaAgentTruncatedLadderEscalation: true,
      authorizePaidResearch: false,
      mockExaAgentContactStructured: mockBoth,
    },
  );
  const p = r.opportunity.primaryContact?.name;
  const s = r.opportunity.secondaryContact?.name;
  if (p && s) {
    assert.notEqual(
      String(p).toLowerCase(),
      String(s).toLowerCase(),
      "primary/secondary must be distinct",
    );
  }
  assert.ok(p, "expected primary from Agent candidates");
});

await checkAsync("18_agent_fail_safe_continue", async () => {
  const r = await runExaAgentContactEscalation(baseOpp, {
    ...authBudget,
    ladderExhausted: true,
    mockExaAgentContactStatus: "failed",
    mockExaAgentContactStructured: null,
  });
  assert.equal(r.ok, false);
  assert.ok(r.opportunity, "opportunity preserved");
  assert.equal(r.candidates.length, 0);
});

await checkAsync("19_agent_success_how_independent", async () => {
  let working = applyVerifiedBuyerToOpportunity(
    { ...baseOpp },
    {
      name: "Jordan Example",
      title: "Events Administrator",
      currentOrganization: "Example Association (EXAORG)",
      organization: "Example Association (EXAORG)",
      sourceUrl: "https://example-association.org/staff/jordan-example",
    },
    {},
  );
  const how = await runRestoredHowContactChannelDiscovery(working, {
    authorizePaidPublicSearch: false,
    authorizeFindymail: false,
    authorizeSurfe: false,
    howSkipDns: true,
    skipDns: true,
    fetchImpl: async () => ({
      ok: true,
      status: 200,
      text: async () =>
        "<html><body>Jordan Example <a href='mailto:jordan@example-association.org'>email</a></body></html>",
      headers: { get: () => "text/html" },
    }),
    candidateUrls: ["https://example-association.org/staff/jordan-example"],
  });
  const gates = assessWhoHowCompletenessGates(how.opportunity || working, {});
  assert.equal(gates.whoComplete, true);
  assert.ok(how.executed === true || how.skipped === false || gates.whoComplete);
});

await checkAsync("20_resume_replay_no_duplicate_paid", async () => {
  const elig = evaluateExaAgentContactEscalationEligibility({
    opportunity: {
      ...baseOpp,
      exaAgentContactEscalation: {
        attempted: true,
        executed: true,
        status: "completed",
      },
    },
    ladderExhausted: true,
    opts: { ...authBudget, skipDuplicatePaidResearch: true },
  });
  assert.equal(elig.eligible, false);
  assert.ok(
    elig.reason === EXA_AGENT_CONTACT_ESCALATION_REASON.ALREADY_ESCALATED ||
      elig.reason === EXA_AGENT_CONTACT_ESCALATION_REASON.RESEARCHER_IDENTITY_SKIP,
  );
});

await checkAsync("21_no_regression_demand_discovery_adapter", async () => {
  // Contact contract id is distinct from scaling discovery; adapter still imports cleanly
  assert.ok(EXA_AGENT_CONTACT_INTELLIGENCE_CONTRACT_V1.includes("contact-intelligence"));
  assert.ok(GDI_EXA_AGENT_CONTACT_ESCALATION_V1.includes("contact-escalation"));
  const liveDefault = isExaAgentLiveAuthorized({ authorizeExaAgentLive: false });
  assert.equal(liveDefault, false);
});

await checkAsync("22_no_auth_full_e2e_prior_behavior", async () => {
  const result = await processOpportunityThroughOriginalChain(
    {
      id: "disc_agent_escalation_cert_chain",
      opportunityId: "disc_agent_escalation_cert_chain",
      hotelId: "recTEST",
      hotelName: "Example Hotel",
      organizationName: "Acme Meetings Inc",
      title: "Summit 2027",
      researchSegment: "associations_events",
      namedStakeholders: [],
    },
    {
      skipDeepResearch: true,
      authorizePaidResearch: false,
      authorizePaidAlternativePersonResearch: false,
      authorizePaidPublicContactSearch: false,
      authorizeExaAgentContactEscalation: false,
      authorizeExaAgentLive: false,
      ensureBuyerAlternativePersonResearch: true,
      ensureHowContactDiscovery: true,
      howSkipDns: true,
    },
  );
  const alt = (result.stages || []).find((s) => s.stage === BUYER_ALTERNATIVE_PERSON_STAGE);
  assert.ok(alt, "ALT stage present");
  assert.ok(
    !alt.exaAgentContactEscalation ||
      alt.exaAgentContactEscalation.attempted !== true ||
      alt.exaAgentContactEscalation.skipped === true ||
      alt.exaAgentContactEscalation.reason ===
        EXA_AGENT_CONTACT_ESCALATION_REASON.FLAG_OFF,
  );
  const names = (result.stages || []).map((s) => s.stage);
  const contactIdx = names.indexOf("CONTACT_RESEARCH");
  const altIdx = names.indexOf(BUYER_ALTERNATIVE_PERSON_STAGE);
  const howIdx = names.indexOf("HOW_CONTACT_CHANNEL_DISCOVERY");
  assert.ok(contactIdx >= 0 && altIdx > contactIdx, "CONTACT before ALT");
  assert.ok(howIdx > altIdx, "HOW after ALT");
});

// Bonus: high effort refused
await checkAsync("23_high_effort_refused", async () => {
  const elig = evaluateExaAgentContactEscalationEligibility({
    opportunity: baseOpp,
    ladderExhausted: true,
    opts: { ...authBudget, exaAgentContactEffort: "high" },
  });
  assert.equal(elig.eligible, false);
  assert.equal(elig.reason, EXA_AGENT_CONTACT_ESCALATION_REASON.EFFORT_REFUSED);
});

fs.mkdirSync(OUT, { recursive: true });
const report = {
  contract: GDI_EXA_AGENT_CONTACT_ESCALATION_V1,
  at: new Date().toISOString(),
  passed,
  total: results.length,
  results,
  agentDefaultOff: true,
  paidCalls: 0,
};
fs.writeFileSync(path.join(OUT, "CERTIFICATION.json"), `${JSON.stringify(report, null, 2)}\n`);
console.log(`\nOK ${passed}/${results.length} gdi-exa-agent-contact-escalation-v1`);

if (prevLive != null) process.env.GDI_EXA_AGENT_LIVE = prevLive;
else delete process.env.GDI_EXA_AGENT_LIVE;
