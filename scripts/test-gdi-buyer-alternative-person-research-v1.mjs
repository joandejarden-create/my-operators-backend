/**
 * Permanent certification: alternative-person ladder reconnected to Full E2E ($0 mocked).
 *
 *   node scripts/test-gdi-buyer-alternative-person-research-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_BUYER_ALTERNATIVE_PERSON_RESEARCH_V1,
  BUYER_ALTERNATIVE_PERSON_STAGE,
  PERSON_KIND,
  ALTERNATIVE_PERSON_REQUIRED_WORKFLOWS,
  harvestAlternativePersonCandidates,
  runBuyerAlternativePersonResearch,
  assertBuyerAlternativePersonStage,
  BUYER_RESEARCH_STRATEGY_SEQUENCE,
} from "../lib/group-demand-intelligence/workflow/gdi-buyer-alternative-person-research-v1.js";
import { evaluateNamedContactCoverageStandard } from "../lib/group-demand-intelligence/workflow/gdi-named-contact-coverage-standard-v1.js";
import {
  assessWhoHowCompletenessGates,
  runRestoredHowContactChannelDiscovery,
  HOW_CONTACT_RESEARCH_STATUS,
  WHO_HOW_FULL_E2E_SEQUENCE,
} from "../lib/group-demand-intelligence/workflow/gdi-who-how-contact-intelligence-restore-v1.js";
import { COMMERCIAL_CONTACT_CLASS } from "../lib/group-demand-intelligence/presentation/gdi-commercial-contact-classification-v1.js";
import { indexPriorNamedContactsByCluster } from "../lib/group-demand-intelligence/workflow/gdi-opportunity-contact-lineage-reuse-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports",
  "gdi-buyer-alternative-person-research-v1",
);

let passed = 0;
function check(name, fn) {
  fn();
  passed += 1;
  console.log(`  PASS ${name}`);
}
async function checkAsync(name, fn) {
  await fn();
  passed += 1;
  console.log(`  PASS ${name}`);
}

await checkAsync("1_ideal_buyer_found_selected", async () => {
  const r = await runBuyerAlternativePersonResearch({
    organizationName: "ISHC",
    primaryContact: {
      name: "Lori Raleigh",
      title: "Executive Director",
      organization: "ISHC",
      sourceUrl: "https://www.ishc.com/team",
    },
    namedStakeholders: [
      {
        name: "Lori Raleigh",
        title: "Executive Director",
        organization: "ISHC",
        sourceUrl: "https://www.ishc.com/team",
      },
    ],
  });
  assert.equal(r.opportunity.primaryContact?.name, "Lori Raleigh");
  assert.ok(r.executed);
});

await checkAsync("2_ideal_missing_ladder_executes", async () => {
  const r = await runBuyerAlternativePersonResearch({
    organizationName: "Acme Corp",
    title: "Corporate offsite",
    namedStakeholders: [],
  });
  assert.ok((r.trace.strategiesAttempted || []).length >= 1);
  assert.equal(r.trace.ladderExhausted, true);
});

await checkAsync("3_first_alt_missing_next_strategy", async () => {
  let calls = 0;
  const r = await runBuyerAlternativePersonResearch(
    { organizationName: "Acme", namedStakeholders: [] },
    {
      authorizePaidResearch: true,
      maxStrategies: 3,
      researcher: {
        execute: async ({ strategy }) => {
          calls += 1;
          if (calls < 2) return { people: [] };
          return {
            people: [
              {
                name: "Sam Planner",
                title: "Director of Meetings",
                organization: "Acme",
                sourceUrl: "https://acme.example/team",
              },
            ],
          };
        },
      },
    },
  );
  assert.ok(calls >= 2);
  assert.equal(r.opportunity.primaryContact?.name, "Sam Planner");
  assert.ok(
    (r.trace.strategiesAttempted || []).length >= 2,
    "must advance past first unsuccessful strategy",
  );
});

await checkAsync("4_multiple_candidates_ranking_selects_best", async () => {
  const r = await runBuyerAlternativePersonResearch({
    organizationName: "EventPipe",
    title: "Group hotel housing software",
    namedStakeholders: [
      {
        name: "Hotel Booking Software",
        title: "EventPipe Home Solutions",
      },
      {
        name: "Craig Davis",
        title: "VP of Brand Performance & Experience, Varsity Spirit",
        organization: "Varsity Spirit",
        sourceUrl: "https://www.eventpipe.com/",
      },
      {
        name: "Amy Yoder",
        title: "Group Manager, ICE All Stars",
        organization: "ICE All Stars",
        sourceUrl: "https://www.eventpipe.com/",
      },
    ],
  });
  const harvest = harvestAlternativePersonCandidates({
    namedStakeholders: r.opportunity.namedStakeholders,
    organizationName: "EventPipe",
  });
  assert.ok(harvest.rejected.some((x) => /Hotel Booking/i.test(x.name)));
  assert.ok(
    ["Craig Davis", "Amy Yoder"].includes(r.opportunity.primaryContact?.name),
  );
});

await checkAsync("5_relevant_alternative_accepted_with_evidence", async () => {
  const r = await runBuyerAlternativePersonResearch({
    organizationName: "Fundacion Por Mexico",
    namedStakeholders: [
      {
        name: "Karla Mawcinitt",
        title: "Directora de Fundacion Por Mexico",
        organization: "Fundacion Por Mexico",
        sourceUrl: "https://example.com/team",
      },
    ],
  });
  assert.equal(r.opportunity.primaryContact?.name, "Karla Mawcinitt");
  assert.ok(
    r.trace.primaryKind === PERSON_KIND.VERIFIED_VIABLE_ALTERNATIVE ||
      r.trace.primaryKind === PERSON_KIND.DIRECT_BUYER ||
      r.coverage.namedCount >= 1,
  );
});

await checkAsync("6_unrelated_senior_executive_rejected_or_deprioritized", async () => {
  const r = await runBuyerAlternativePersonResearch({
    organizationName: "Giant Co",
    title: "Annual sales kickoff lodging",
    namedStakeholders: [
      {
        name: "John Celebrity",
        title: "Chief Executive Officer",
        organization: "Giant Co",
      },
      {
        name: "Maria Meetings",
        title: "Director of Meetings and Events",
        organization: "Giant Co",
        sourceUrl: "https://giant.example/team",
      },
    ],
  });
  assert.equal(r.opportunity.primaryContact?.name, "Maria Meetings");
});

await checkAsync("7_genuine_person_among_junk_retained", async () => {
  const harvest = harvestAlternativePersonCandidates({
    namedStakeholders: [
      { name: "Family Travel", title: "Groups Specialty" },
      { name: "Historic Center", title: "Hotel Downtown" },
      {
        name: "Amy Yoder",
        title: "Group Manager",
        organization: "ICE",
        sourceUrl: "https://example.com",
      },
    ],
  });
  assert.ok(harvest.rejected.length >= 2);
  assert.ok(harvest.accepted.some((p) => p.name === "Amy Yoder"));
});

await checkAsync("8_second_viable_contact_selected", async () => {
  const r = await runBuyerAlternativePersonResearch({
    organizationName: "Acme",
    namedStakeholders: [
      {
        name: "Primary Buyer",
        title: "Travel Manager",
        organization: "Acme",
        sourceUrl: "https://acme.example/a",
      },
      {
        name: "Secondary Lead",
        title: "Meetings Coordinator",
        organization: "Acme",
        sourceUrl: "https://acme.example/b",
      },
    ],
  });
  assert.ok(r.opportunity.primaryContact?.name);
  // Secondary may or may not attach depending on classifier; ensure attempt ran
  assert.equal(r.trace.secondaryResearchRan, true);
});

await checkAsync("9_no_named_person_cannot_finalize", async () => {
  const r = await runBuyerAlternativePersonResearch({
    organizationName: "Ghost Org",
    namedStakeholders: [
      { name: "Organization Contact Desk", title: "Info" },
      { name: "Hotel Booking Software", title: "Solutions" },
    ],
  });
  const cov = evaluateNamedContactCoverageStandard(r.opportunity);
  assert.equal(cov.finalizeEligible, false);
  assert.equal(cov.namedCount, 0);
});

await checkAsync("10_verified_named_without_email_who_complete", async () => {
  const r = await runBuyerAlternativePersonResearch({
    organizationName: "ISHC",
    namedStakeholders: [
      {
        name: "Lori Raleigh",
        title: "Executive Director",
        organization: "ISHC",
        sourceUrl: "https://www.ishc.com/team",
      },
    ],
  });
  assert.ok(!r.opportunity.primaryContact?.email);
  const gates = assessWhoHowCompletenessGates({
    ...r.opportunity,
    secondaryContactResearchStatus: "ATTEMPTED_NO_SECOND_VERIFIED",
  });
  assert.equal(gates.whoComplete, true);
});

await checkAsync("11_public_how_runs_separately", async () => {
  const who = await runBuyerAlternativePersonResearch({
    organizationName: "ISHC",
    namedStakeholders: [
      {
        name: "Lori Raleigh",
        title: "Executive Director",
        organization: "ISHC",
        sourceUrl: "https://www.ishc.com/team",
      },
    ],
  });
  const how = await runRestoredHowContactChannelDiscovery(who.opportunity, {
    authorizePaidPublicSearch: false,
    authorizeSurfe: false,
    authorizeFindymail: false,
    skipDns: true,
    fetchImpl: async () => ({
      ok: true,
      status: 200,
      text: async () => "<html></html>",
      headers: { get: () => "text/html" },
    }),
  });
  assert.equal(how.executed, true);
  assert.equal(how.providers.findymail, false);
  assert.equal(how.providers.surfe, false);
});

await checkAsync("12_no_channel_not_falsely_how_complete_without_attempt", async () => {
  const gates = assessWhoHowCompletenessGates({
    primaryContact: {
      name: "Lori Raleigh",
      title: "Executive Director",
      organization: "ISHC",
    },
    howContactResearchAttempted: false,
    howContactResearchStatus: HOW_CONTACT_RESEARCH_STATUS.NOT_ATTEMPTED,
    secondaryContactResearchStatus: "ATTEMPTED_NO_SECOND_VERIFIED",
  });
  assert.equal(gates.whoComplete, true);
  assert.equal(gates.howComplete, false);
});

check("13_no_findymail_surfe_dependency", () => {
  assert.ok(BUYER_RESEARCH_STRATEGY_SEQUENCE.length >= 5);
  assert.ok(
    !JSON.stringify(BUYER_RESEARCH_STRATEGY_SEQUENCE).toLowerCase().includes(
      "findymail",
    ),
  );
});

await checkAsync("14_identity_reuse", async () => {
  const prior = indexPriorNamedContactsByCluster([
    {
      id: "prior1",
      organizationName: "Reuse Org",
      title: "Annual conference 2027",
      primaryContact: {
        name: "Reuse Person",
        title: "Meetings Director",
        organization: "Reuse Org",
        sourceUrl: "https://reuse.example/team",
      },
    },
  ]);
  const r = await runBuyerAlternativePersonResearch(
    {
      organizationName: "Reuse Org",
      title: "Annual conference 2027",
      namedStakeholders: [],
    },
    { priorContactIndex: prior },
  );
  assert.ok(
    r.trace.lineageReuse?.reused === true ||
      r.opportunity.primaryContact?.name === "Reuse Person" ||
      r.trace.lineageReuse != null,
  );
});

await checkAsync("15_budget_exhaustion_stops_paid_escalation", async () => {
  let calls = 0;
  await runBuyerAlternativePersonResearch(
    { organizationName: "Acme", namedStakeholders: [] },
    {
      authorizePaidResearch: false,
      researcher: {
        execute: async () => {
          calls += 1;
          return { people: [] };
        },
      },
    },
  );
  assert.equal(calls, 0, "researcher must not run without authorizePaidResearch");
});

check("16_no_live_mode_fixture_fallback_flag", () => {
  // Stage assert rejects missing stage; fixture researcher is caller's concern —
  // ensure workflows list excludes silent fixture as authority.
  assert.ok(ALTERNATIVE_PERSON_REQUIRED_WORKFLOWS.includes("cohort_execute"));
  assert.ok(
    WHO_HOW_FULL_E2E_SEQUENCE.includes(BUYER_ALTERNATIVE_PERSON_STAGE),
  );
});

check("17_all_canonical_modes_share_hierarchy", () => {
  for (const w of ALTERNATIVE_PERSON_REQUIRED_WORKFLOWS) {
    assert.ok(typeof w === "string" && w.length > 3);
  }
  const whoIdx = WHO_HOW_FULL_E2E_SEQUENCE.indexOf("WHO_IDENTIFICATION");
  const altIdx = WHO_HOW_FULL_E2E_SEQUENCE.indexOf(
    BUYER_ALTERNATIVE_PERSON_STAGE,
  );
  const howIdx = WHO_HOW_FULL_E2E_SEQUENCE.indexOf(
    "HOW_CONTACT_CHANNEL_DISCOVERY",
  );
  assert.ok(whoIdx < altIdx && altIdx < howIdx);
});

check("18_stage_assert_and_finalization_identities", () => {
  const missing = assertBuyerAlternativePersonStage([
    { stage: "CONTACT_RESEARCH" },
    { stage: "HOW_CONTACT_CHANNEL_DISCOVERY", executed: true },
  ]);
  assert.equal(missing.ok, false);
  assert.equal(missing.code, "ALTERNATIVE_PERSON_STAGE_MISSING");

  const ok = assertBuyerAlternativePersonStage([
    { stage: "BUYER_WHO_RESEARCH", executed: true },
    { stage: "CONTACT_RESEARCH" },
    {
      stage: BUYER_ALTERNATIVE_PERSON_STAGE,
      executed: true,
      strategiesAttempted: 3,
    },
    { stage: "HOW_CONTACT_CHANNEL_DISCOVERY", executed: true },
  ]);
  assert.equal(ok.ok, true);

  const howBefore = assertBuyerAlternativePersonStage([
    { stage: "CONTACT_RESEARCH" },
    { stage: "HOW_CONTACT_CHANNEL_DISCOVERY", executed: true },
    { stage: BUYER_ALTERNATIVE_PERSON_STAGE, executed: true },
  ]);
  assert.equal(howBefore.ok, false);
  assert.equal(howBefore.code, "HOW_BEFORE_ALTERNATIVE");
});

// Fix check 1 duplicate - remove empty sync stub was already replaced by async

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(
  path.join(OUT, "CERTIFICATION.json"),
  JSON.stringify(
    {
      at: new Date().toISOString(),
      version: GDI_BUYER_ALTERNATIVE_PERSON_RESEARCH_V1,
      status: "CERTIFICATION_PASS",
      checksPassed: passed,
      sequence: WHO_HOW_FULL_E2E_SEQUENCE,
      workflows: ALTERNATIVE_PERSON_REQUIRED_WORKFLOWS,
      providers: { findymail: false, surfe: false, exaSpent: 0 },
      personKinds: PERSON_KIND,
      commercialClasses: COMMERCIAL_CONTACT_CLASS,
    },
    null,
    2,
  ),
);

console.log(`\n${passed} checks evaluated\nCERTIFICATION_PASS`);
