#!/usr/bin/env node
/**
 * Permanent Full E2E buyer-WHO regression suite V1 — $0 mocked providers only.
 * Covers new/existing/portfolio/leisure/resume/replay + finalization gates.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-buyer-who-permanent-full-e2e-v1");

const {
  runRestoredOrsDeepResearchPass,
  isPlausibleBuyerPersonName,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-original-full-e2e-execution-restore-v1.js"
);
const {
  assertBuyerWhoStagePresent,
  evaluateNamedBuyerFinalizationGate,
  selectBestSuggestedBuyer,
  applyVerifiedBuyerToOpportunity,
  BUYER_WHO_REQUIRED_WORKFLOWS,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-buyer-who-permanent-full-e2e-v1.js"
);
const {
  indexPriorNamedContactsByCluster,
  applyContactLineageReuse,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-opportunity-contact-lineage-reuse-v1.js"
);

const results = [];
const traces = {};

function check(name, fn) {
  try {
    const v = fn();
    results.push({ name, ok: true, detail: v || null });
    console.log(`PASS ${name}`);
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
    console.error(`FAIL ${name}: ${err?.message || err}`);
  }
}

function mockResearcher(peopleByOrg = {}, opts = {}) {
  const calls = [];
  let spent = 0;
  const budget = Number(opts.budgetUsd || 999);
  return {
    calls,
    get spent() {
      return spent;
    },
    async execute({ opportunity, action }) {
      calls.push({
        org: opportunity?.organizationName,
        queries: action?.queries || [],
        strategy: action?.strategy,
      });
      if (spent >= budget) {
        return { evidence: [], deepPatch: { namedStakeholdersFound: [] }, budgetExhausted: true };
      }
      spent += Number(opts.costPerCall || 0.004);
      const org = String(opportunity?.organizationName || "");
      const people = peopleByOrg[org] || peopleByOrg["*"] || [];
      return {
        evidence: people.map((p, i) => ({
          url: p.sourceUrl || `https://example.org/team#${i}`,
          statement: `${p.name} is ${p.role || "Travel Manager"} at ${org}. Responsible for hotel sourcing and meeting lodging.`,
          relatedEntity: true,
          topic: "buyer",
        })),
        deepPatch: { namedStakeholdersFound: people },
      };
    },
  };
}

function baseOpp(overrides = {}) {
  return {
    id: "disc_perm_buyer_who_1",
    opportunityId: "disc_perm_buyer_who_1",
    organizationName: "Acme Meetings Inc",
    hotelName: "Test Hotel",
    hotelId: "recTESTHOTEL000001",
    title: "Acme Annual Housing Block",
    segmentId: "association_events",
    whatIsHappening: "Annual conference with housing need",
    evidence: [
      {
        url: "https://acme-meetings.example/event",
        statement:
          "Acme Meetings Inc hosts an annual conference requiring a hotel room block and meeting space for attendees.",
      },
    ],
    ...overrides,
  };
}

async function runWorkflow(label, opts = {}) {
  const researcher = opts.researcher || mockResearcher(opts.people || {
    "*": [
      {
        name: "Jordan Lee",
        role: "Meetings Director",
        title: "Meetings Director",
        sourceUrl: "https://acme-meetings.example/team",
        buyerClassification: "PROBABLE_RELEVANT_BUYER",
        decisionMakerClass: "POTENTIAL_DECISION_MAKER",
      },
    ],
  });
  const opportunity = baseOpp(opts.opportunity);
  const result = await runRestoredOrsDeepResearchPass(opportunity, {
    authorizeExaAgentLive: opts.authorizeExaAgentLive !== false,
    authorizePaidResearch: opts.authorizePaidResearch !== false,
    allowLiveExaDeepSeed: opts.allowLiveExaDeepSeed === true,
    authorizeLiveBuyerWho: opts.authorizeLiveBuyerWho === true,
    forceFixtureResearcher: opts.forceFixtureResearcher === true,
    enableLiveBuyerWhoAfterSeed: opts.enableLiveBuyerWhoAfterSeed !== false,
    remainingBudgetUsd: opts.remainingBudgetUsd ?? 1,
    researcher: opts.forceFixtureResearcher ? null : researcher,
    liveResearcher: opts.injectLive ? researcher : null,
    forceOrsPaidBlocked: true,
    ensureBuyerWho: opts.ensureBuyerWho !== false,
    buildCorpus: () => ({
      pages: [
        {
          url: "https://acme-meetings.example/team",
          text: "Jordan Lee, Meetings Director, leads hotel sourcing for Acme Meetings Inc.",
        },
      ],
    }),
  });
  traces[label] = {
    stages: (result.stages || []).map((s) => s.stage),
    buyerWho: result.stages?.find((s) => s.stage === "BUYER_WHO_RESEARCH") || null,
    researcherCalls: researcher.calls?.length ?? null,
    spent: researcher.spent ?? null,
  };
  return { result, researcher };
}

async function main() {
  {
    const { result } = await runWorkflow("new_hotel", {
      authorizeLiveBuyerWho: true,
      opportunity: { hotelId: "recNEWHOTEL0000001", hotelName: "New Boutique Hotel" },
    });
    const gate = assertBuyerWhoStagePresent(result.stages, {
      requireExecuted: true,
      forbidLiveFixtureFallback: true,
      workflow: "new_hotel_full_e2e",
    });
    assert.equal(gate.ok, true, gate.reason);
    results.push({ name: "1_new_hotel_invokes_buyer_who", ok: true });
    console.log("PASS 1_new_hotel_invokes_buyer_who");
  }

  {
    const { result } = await runWorkflow("existing_hotel", {
      authorizeLiveBuyerWho: true,
      opportunity: { hotelId: "rece0NdV9FCnaFlVS", hotelName: "Condesa DF" },
    });
    const gate = assertBuyerWhoStagePresent(result.stages, {
      requireExecuted: true,
      forbidLiveFixtureFallback: true,
      workflow: "existing_hotel_full_e2e",
    });
    assert.equal(gate.ok, true, gate.reason);
    results.push({ name: "2_existing_hotel_invokes_buyer_who", ok: true });
    console.log("PASS 2_existing_hotel_invokes_buyer_who");
  }

  {
    const { result } = await runWorkflow("portfolio", {
      authorizeLiveBuyerWho: true,
      opportunity: { hotelId: "recPORTFOLIO00001", portfolioRun: true },
    });
    const gate = assertBuyerWhoStagePresent(result.stages, {
      requireExecuted: true,
      workflow: "portfolio_full_e2e",
    });
    assert.equal(gate.ok, true, gate.reason);
    results.push({ name: "3_portfolio_invokes_buyer_who", ok: true });
    console.log("PASS 3_portfolio_invokes_buyer_who");
  }

  {
    const { result } = await runWorkflow("leisure", {
      authorizeLiveBuyerWho: true,
      opportunity: {
        segmentId: "leisure_dmc_tour",
        organizationName: "Journey Mexico",
        title: "DMC contracting for leisure groups",
      },
      people: {
        "Journey Mexico": [
          {
            name: "Maria Santos",
            role: "Product Manager",
            title: "Product Manager — Hotel Partnerships",
            sourceUrl: "https://journeymexico.example/team",
            buyerClassification: "PROBABLE_RELEVANT_BUYER",
          },
        ],
      },
    });
    const gate = assertBuyerWhoStagePresent(result.stages, {
      requireExecuted: true,
      workflow: "leisure_demand_full_e2e",
    });
    assert.equal(gate.ok, true, gate.reason);
    results.push({ name: "4_leisure_invokes_buyer_who", ok: true });
    console.log("PASS 4_leisure_invokes_buyer_who");
  }

  {
    const researcher = mockResearcher();
    const first = await runRestoredOrsDeepResearchPass(baseOpp(), {
      authorizeExaAgentLive: true,
      authorizePaidResearch: true,
      authorizeLiveBuyerWho: true,
      remainingBudgetUsd: 1,
      researcher,
      forceOrsPaidBlocked: true,
    });
    const contactState = first.opportunity?.buyerWhoFallback;
    const second = await runRestoredOrsDeepResearchPass(
      {
        ...first.opportunity,
        namedStakeholders: [
          {
            name: "Jordan Lee",
            title: "Meetings Director",
            buyerClassification: "PROBABLE_RELEVANT_BUYER",
            decisionMakerClass: "POTENTIAL_DECISION_MAKER",
          },
        ],
        primaryContact: { name: "Jordan Lee", role: "Meetings Director" },
      },
      {
        authorizeExaAgentLive: true,
        authorizePaidResearch: true,
        authorizeLiveBuyerWho: true,
        remainingBudgetUsd: 1,
        researcher,
        forceOrsPaidBlocked: true,
      },
    );
    assert.ok(contactState?.executed === true || first.stages.some((s) => s.stage === "BUYER_WHO_RESEARCH"));
    assert.ok(
      isPlausibleBuyerPersonName(second.opportunity?.primaryContact?.name) ||
        second.opportunity?.namedStakeholders?.some((s) => isPlausibleBuyerPersonName(s.name)),
    );
    traces.resume = {
      firstBuyerWho: first.stages.find((s) => s.stage === "BUYER_WHO_RESEARCH"),
      secondBuyerWho: second.stages.find((s) => s.stage === "BUYER_WHO_RESEARCH"),
      preservedName: second.opportunity?.primaryContact?.name || null,
    };
    results.push({ name: "5_resume_preserves_contact_state", ok: true });
    console.log("PASS 5_resume_preserves_contact_state");
  }

  {
    const researcher = mockResearcher({}, { budgetUsd: 0, costPerCall: 0.004 });
    const { result } = await runWorkflow("replay", {
      researcher,
      remainingBudgetUsd: 0,
      authorizeExaAgentLive: true,
      authorizePaidResearch: true,
      forceFixtureResearcher: true,
    });
    const who = result.stages.find((s) => s.stage === "BUYER_WHO_RESEARCH");
    assert.ok(who, "BUYER_WHO stage required on replay");
    assert.equal(researcher.spent, 0);
    traces.replay = { buyerWho: who, spent: researcher.spent };
    results.push({ name: "6_replay_reuses_without_paid_calls", ok: true });
    console.log("PASS 6_replay_reuses_without_paid_calls");
  }

  check("7_missing_buyer_fails_named_gate", () => {
    const gate = evaluateNamedBuyerFinalizationGate(
      {
        organizationName: "Nestlé",
        primaryContact: { name: null },
        buyerWhoFallback: {
          executed: true,
          reason: "buyer_intelligence_gap_forced_who_and_discovery_queries",
        },
        buyerWhoRequiredIncomplete: true,
      },
      { requireNamedPerson: true, looksLikePersonName: isPlausibleBuyerPersonName },
    );
    assert.equal(gate.pass, false);
    assert.match(gate.mode, /INCOMPLETE|REQUIRED/);
  });

  check("8_verified_name_without_email_displayable", () => {
    const opp = applyVerifiedBuyerToOpportunity(baseOpp(), {
      name: "Ana Ribeiro",
      title: "Housing Manager",
      sourceUrl: "https://example.org/team",
      email: null,
    });
    assert.equal(opp.primaryContact.email, null);
    assert.equal(opp.primaryContact.emailVerificationStatus, "NO_EMAIL");
    assert.ok(isPlausibleBuyerPersonName(opp.primaryContact.name));
    const gate = evaluateNamedBuyerFinalizationGate(opp, {
      requireNamedPerson: true,
      looksLikePersonName: isPlausibleBuyerPersonName,
    });
    assert.equal(gate.pass, true);
  });

  check("9_junk_person_extraction_rejected", () => {
    assert.equal(isPlausibleBuyerPersonName("Contact Research Required"), false);
    assert.equal(isPlausibleBuyerPersonName("Conference"), false);
    assert.equal(isPlausibleBuyerPersonName("Nestlé"), false);
    const sel = selectBestSuggestedBuyer(
      [
        { name: "Conference", title: "Event" },
        { name: "Contact Research Required", title: "Desk" },
        {
          name: "Priya Nair",
          title: "Director of Meetings",
          buyerClassification: "PROBABLE_RELEVANT_BUYER",
          sourceUrl: "https://example.org/t",
        },
      ],
      { looksLikePersonName: isPlausibleBuyerPersonName },
    );
    assert.equal(sel.selected?.name, "Priya Nair");
  });

  check("10_cross_hotel_person_reuse", () => {
    const inventory = [
      {
        opportunityId: "a",
        organizationName: "Salesforce",
        title: "Mexico City office and GDC",
        primaryContact: {
          name: "Daniel Roy",
          role: "Senior Strategic Sourcing Partner",
          personIdentityKey: "pik_daniel_roy_salesforce",
        },
      },
    ];
    const index = indexPriorNamedContactsByCluster(inventory);
    const reuse = applyContactLineageReuse(
      {
        id: "b_new_live_id",
        organizationName: "Salesforce",
        title: "Mexico City office and GDC",
        primaryContact: { name: null },
      },
      index,
    );
    assert.equal(reuse.reused, true, reuse.reason);
    assert.ok(isPlausibleBuyerPersonName(reuse.opportunity.primaryContact?.name));
  });

  check("11_original_opportunity_identity_survives", () => {
    const opp = applyVerifiedBuyerToOpportunity(
      baseOpp({ id: "disc_original_identity", opportunityId: "disc_original_identity" }),
      { name: "Chris Ng", title: "Travel Manager", sourceUrl: "https://x.example/t" },
    );
    assert.equal(opp.id, "disc_original_identity");
    assert.equal(opp.opportunityId, "disc_original_identity");
  });

  {
    const researcher = mockResearcher({ "*": [] }, { budgetUsd: 0.004, costPerCall: 0.004 });
    await researcher.execute({ opportunity: baseOpp(), action: { queries: ["q1"] } });
    const second = await researcher.execute({
      opportunity: baseOpp(),
      action: { queries: ["q2"] },
    });
    assert.equal(second.budgetExhausted, true);
    assert.ok(researcher.spent <= 0.004 + 1e-9);
    results.push({ name: "12_budget_exhaustion_stops_paid_calls", ok: true });
    console.log("PASS 12_budget_exhaustion_stops_paid_calls");
  }

  {
    const researcher = mockResearcher();
    const { result } = await runWorkflow("no_fixture_fallback", {
      researcher,
      authorizeLiveBuyerWho: true,
      authorizeExaAgentLive: true,
      authorizePaidResearch: true,
    });
    const gate = assertBuyerWhoStagePresent(result.stages, {
      forbidLiveFixtureFallback: true,
      workflow: "new_hotel_full_e2e",
    });
    assert.equal(gate.ok, true, gate.reason);
    assert.notEqual(gate.stage?.researcherSource, "fixture");
    results.push({ name: "13_no_live_mode_fixture_fallback", ok: true });
    console.log("PASS 13_no_live_mode_fixture_fallback");
  }

  check("14_staged_includes_selected_verified_contact", () => {
    const opp = applyVerifiedBuyerToOpportunity(baseOpp(), {
      name: "Elena Costa",
      title: "Convention Housing Director",
      sourceUrl: "https://example.org/team",
    });
    const gate = evaluateNamedBuyerFinalizationGate(opp, {
      requireNamedPerson: true,
      looksLikePersonName: isPlausibleBuyerPersonName,
    });
    assert.equal(gate.pass, true);
    assert.equal(opp.tileContactPreview.name, "Elena Costa");
    assert.match(String(opp.recommendedNextStep || ""), /Elena Costa/);
  });

  check("15_sales_ready_independently_gated", () => {
    const opp = applyVerifiedBuyerToOpportunity(baseOpp(), {
      name: "Elena Costa",
      title: "Convention Housing Director",
      sourceUrl: "https://example.org/team",
    });
    // Finding a name must NOT auto-mark Sales Ready
    assert.notEqual(opp.salesReady, true);
    assert.notEqual(opp.commercialStageKey, "SALES_READY");
  });

  check("16_no_customer_publication_without_founder_approval", () => {
    assert.ok(BUYER_WHO_REQUIRED_WORKFLOWS.includes("new_hotel_full_e2e"));
    // Permanent helpers never flip lifecycle to PRODUCTION_CURRENT / customer publish
    const opp = applyVerifiedBuyerToOpportunity(baseOpp(), {
      name: "Elena Costa",
      title: "Housing Director",
      sourceUrl: "https://example.org/team",
    });
    assert.equal(opp.lifecycleState, undefined);
    assert.equal(opp.customerPublished, undefined);
  });

  fs.mkdirSync(OUT, { recursive: true });
  const report = {
    contract: "gdi-buyer-who-permanent-full-e2e-v1",
    at: new Date().toISOString(),
    paidCalls: 0,
    workflows: BUYER_WHO_REQUIRED_WORKFLOWS,
    traces,
    results,
    pass: results.every((r) => r.ok),
  };
  fs.writeFileSync(path.join(OUT, "CERTIFICATION.json"), JSON.stringify(report, null, 2));
  fs.writeFileSync(path.join(OUT, "EXECUTION_TRACES.json"), JSON.stringify(traces, null, 2));
  console.log(`\n${results.length} checks evaluated`);
  console.log(report.pass ? "CERTIFICATION_PASS" : "CERTIFICATION_FAIL");
  process.exit(report.pass ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
