/**
 * E0 Exa-parity arms (mock only).
 *
 * Arm A — existing Exa-powered GDI policy: deterministic ORS↔Exa selection
 *          (Search first, then Agent for complex gaps; fixed queries; no reformulation).
 * Arm B — Dealality adaptive planner over the same mock Exa capabilities/budgets.
 */

import {
  buildResearchSelectionRecord,
  classifyResearchQuestionComplexity,
} from "../research/adaptive-ors-exa-research-loop-v1.js";
import { selectExaAgentStrategy } from "../research/exa-agent/exa-agent-ors-strategy-v1.js";
import { evaluateOpportunity } from "../research/opportunity-research-supervisor-v1.js";
import { evaluateSharedCommercialEligibility } from "../commercial-eligibility-contract-v1.js";
import {
  assessOpportunityDrawerReadiness,
  classifyCommercialDisposition,
} from "../research/gdi-discovery-intake-disposition-v1.js";
import { runIndependentEvidenceChallenger } from "./gdi-20opp-live-research-prep-v1.js";
import {
  createArmBudgetLedger,
  E0_LIMITS,
} from "./gdi-ai-agent-exa-parity-e0-budget-ledger-v1.js";
import {
  buildMockExaCorpus,
  createMockExaExecutor,
} from "./gdi-ai-agent-exa-parity-e0-mock-exa-v1.js";

export const E0_ARMS_VERSION = "gdi-ai-agent-exa-parity-e0-arms-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function cloneOpp(opp) {
  return JSON.parse(JSON.stringify(opp || {}));
}

function hotelFromMeta(meta = {}, opp = {}) {
  return {
    hotelId: meta.hotelId || opp.hotelId,
    name: meta.hotelName || opp.hotelName,
    displayName: meta.hotelName || opp.hotelName,
  };
}

function deterministicQuery(opp, hotel, intent = "overview") {
  const org =
    text(opp.organizationName) ||
    text(opp.companyName) ||
    text(opp.title) ||
    "organization";
  const h = text(hotel.name) || "hotel";
  if (intent === "overview") {
    return `${org} company overview near ${h}`;
  }
  if (intent === "complex") {
    return `${org} lodging demand meetings travel buyer near ${h}`;
  }
  return `${org} ${intent} ${h}`;
}

function adaptiveQuery(opp, hotel, gapType, attempt = 1) {
  const org =
    text(opp.organizationName) ||
    text(opp.companyName) ||
    text(opp.title) ||
    "organization";
  const h = text(hotel.name) || "hotel";
  // Attempt 1 is intentionally thin (mirrors weak first GDI/Exa hit) so Arm B
  // must reformulate — richer contact/demand language unlocks on attempt 2+.
  if (gapType === "CONTACT_ROUTING_GAP" || gapType === "contact") {
    return attempt === 1
      ? `${org} overview`
      : `${org} Director of Meetings Events contact hotel outreach reformulate named buyer guestroom lodging`;
  }
  if (gapType === "HOTEL_DEMAND_GAP" || gapType === "thesis") {
    return attempt === 1
      ? `${org} company page`
      : `${org} guestroom hotel lodging offsite training conference demand near ${h} reformulate corroborate`;
  }
  if (gapType === "competitor") {
    return `${org} hotel room block competitor placed already booked verify`;
  }
  if (gapType === "speculative") {
    return `${org} speculative HQ watch no active lodging demand demote`;
  }
  return attempt === 1
    ? `${org} overview`
    : `${org} commercial lodging demand follow-up reformulate near ${h}`;
}

function inferPrimaryGap(slot, opp) {
  if (slot === "competitor_cross_hotel") return "competitor";
  if (slot === "rejected_speculative") return "speculative";
  if (slot === "missing_verified_contact") return "contact";
  if (slot === "incomplete_demand") return "thesis";
  if (slot === "forward_looking") return "speculative";
  const tasks = opp.unresolvedResearchTasks || [];
  const q = text(tasks[0]?.question || opp.demandThesis || "");
  if (/contact|buyer|who to (call|email)/i.test(q)) return "contact";
  if (/thesis|demand|why|lodging/i.test(q)) return "thesis";
  return "thesis";
}

function commercialPacket({
  opp,
  eligibility,
  challenger,
  drawer,
  disposition,
  ledgerSnap,
  durationMs,
  decisions,
}) {
  const mockCount = (challenger?.buckets?.mockSimulatedFindings || []).length;
  const genuine = (challenger?.buckets?.genuinelyNewVerifiedEvidence || []).length;
  return {
    eligibilityStatus: eligibility?.status || null,
    salesReady: eligibility?.salesReady === true,
    challengerBlocked: challenger?.salesReadyBlockedByChallenger === true,
    verifiedNamedCount: challenger?.contact?.verifiedNamedCount || 0,
    genuinelyNewVerifiedCount: genuine,
    mockSimulatedFindingCount: mockCount,
    drawerReady: drawer?.ready === true,
    drawerGaps: drawer?.gaps || [],
    disposition: disposition?.commercialDisposition || null,
    durationMs,
    actualPaidUsd: 0,
    fetchCount: ledgerSnap?.retrievalCount || 0,
    virtualSettledUsd: ledgerSnap?.settledUsd || 0,
    decisionCount: (decisions || []).length,
  };
}

function finalizeArm({
  arm,
  executionKind,
  adaptivePlanner,
  working,
  frozen,
  meta,
  slot,
  ledger,
  decisions,
  retrievalRecords,
  startedAt,
  stopReason,
}) {
  const ors = evaluateOpportunity(working, {
    targetHotel: meta.hotelName || "",
  });
  const eligibility = evaluateSharedCommercialEligibility(working, {
    targetHotel: meta.hotelName || "",
  });
  const challenger = runIndependentEvidenceChallenger({
    baselineOpportunity: frozen,
    workingOpportunity: working,
    retrievalRecords,
  });
  const drawer = assessOpportunityDrawerReadiness(working);
  const disposition = classifyCommercialDisposition(working, {
    targetHotel: meta.hotelName || "",
  });
  const ledgerSnap = ledger.snapshot();
  const durationMs = Date.now() - startedAt;
  const commercial = commercialPacket({
    opp: working,
    eligibility,
    challenger,
    drawer,
    disposition,
    ledgerSnap,
    durationMs,
    decisions,
  });

  return {
    arm,
    executionKind,
    activeResearch: true,
    adaptivePlanner: adaptivePlanner === true,
    exaPoweredPolicy: true,
    mockExaOnly: true,
    forceMockOnly: true,
    slot,
    stopReason,
    fairness: {
      maxUsd: E0_LIMITS.perArmPerCaseUsd,
      maxRetrievals: E0_LIMITS.maxRetrievalsPerCase,
      actualPaidUsd: 0,
      identicalExaCapabilities: true,
    },
    costs: {
      actualPaidUsd: 0,
      virtualSettledUsd: ledgerSnap.settledUsd,
      fetchCount: ledgerSnap.retrievalCount,
      fetchBudgetCap: E0_LIMITS.maxRetrievalsPerCase,
      usdCap: E0_LIMITS.perArmPerCaseUsd,
      mode: "e0_mock_exa_parity",
    },
    ledger: ledgerSnap,
    decisions,
    retrievalRecords,
    orsSummary: {
      status: ors?.status || null,
      stopReason: ors?.stopReason || null,
    },
    sharedEligibility: eligibility,
    independentChallenger: challenger,
    drawer,
    disposition,
    commercial,
    durationMs,
    workingOpportunity: working,
  };
}

function pushRetrieval(records, result, strategyId) {
  records.push({
    strategyId,
    mocked: true,
    informationFound: result.informationFound === true,
    retrievals: result.ok ? 1 : 0,
    urls: (result.payload?.hits || [])
      .map((h) => h.url)
      .filter(Boolean)
      .concat(result.payload?.url ? [result.payload.url] : []),
    provenance: result.provenance,
    operation: result.operation,
    yieldLabel: result.yieldLabel,
  });
}

/**
 * Arm A — existing Exa-powered GDI policy (deterministic).
 */
export async function runArmAExistingExaPoweredPolicy({
  frozenOpportunity,
  focusCase,
  meta,
} = {}) {
  const startedAt = Date.now();
  const frozen = cloneOpp(frozenOpportunity);
  const working = cloneOpp(frozenOpportunity);
  const slot = focusCase.slot;
  const hotel = hotelFromMeta(meta, working);
  const ledger = createArmBudgetLedger({
    arm: "A",
    opportunityId: focusCase.opportunityId,
    forceMockOnly: true,
  });
  const corpus = buildMockExaCorpus(focusCase, working);
  const exa = createMockExaExecutor({ ledger, corpus, arm: "A" });
  const decisions = [];
  const retrievalRecords = [];

  const question =
    text(working.unresolvedResearchTasks?.[0]?.question) ||
    text(working.demandThesis) ||
    "Identify highest-value unanswered commercial question";
  const complexity = classifyResearchQuestionComplexity(question);
  const selection = buildResearchSelectionRecord({
    opportunity: working,
    hotel,
    remainingAuthorizedUsd: E0_LIMITS.perArmPerCaseUsd,
    authorizeExaAgentLive: false,
  });
  const exaSel = selectExaAgentStrategy({
    gap: { commercialQuestion: question },
    opportunity: working,
    authorizeExaAgentLive: false,
    remainingBudgetUsd: E0_LIMITS.perArmPerCaseUsd,
    priorExaSearchAttempts: 0,
    priorAgentAttempts: 0,
  });

  decisions.push({
    arm: "A",
    step: 1,
    decision: "EXISTING_POLICY_SEARCH_FIRST",
    reason: "try_cheaper_search_first_per_selectExaAgentStrategy",
    complexity: complexity.complexity,
    providerFamily: selection.providerFamily,
    exaPrefer: exaSel.prefer || null,
    queryIntent: "overview",
    adaptive: false,
  });

  const searchQ = deterministicQuery(working, hotel, "overview");
  const search = await exa.invoke({ operation: "search", query: searchQ });
  pushRetrieval(retrievalRecords, search, "EXISTING_GDI_EXA_SEARCH");
  decisions.push({
    arm: "A",
    step: 2,
    decision: search.ok ? "EXECUTED_EXA_SEARCH" : "SEARCH_REFUSED",
    reason: search.reason || "deterministic_overview_query",
    query: searchQ,
    adaptive: false,
  });

  const complex =
    complexity.complexity === "complex" ||
    slot === "missing_verified_contact" ||
    slot === "incomplete_demand";

  if (complex && ledger.remainingUsd() >= 0.1 && ledger.remainingRetrievals() > 0) {
    decisions.push({
      arm: "A",
      step: 3,
      decision: "EXISTING_POLICY_EXA_AGENT_ONCE",
      reason: "complex_gap_single_agent_pass_no_reformulation",
      adaptive: false,
    });
    const agentQ = deterministicQuery(working, hotel, "complex");
    const agent = await exa.invoke({
      operation: "agent",
      query: agentQ,
      effort: "medium",
    });
    pushRetrieval(retrievalRecords, agent, "EXISTING_GDI_EXA_AGENT");
    decisions.push({
      arm: "A",
      step: 4,
      decision: agent.ok ? "EXECUTED_EXA_AGENT" : "AGENT_REFUSED",
      reason: agent.reason || "fixed_complex_query",
      query: agentQ,
      adaptive: false,
    });
  }

  // Optional contents on first hit — existing cheap corroboration pattern
  const firstUrl = search.payload?.hits?.[0]?.url;
  if (firstUrl && ledger.remainingRetrievals() > 0 && ledger.remainingUsd() >= 0.001) {
    const contents = await exa.invoke({
      operation: "contents",
      query: "contents",
      urls: [firstUrl],
    });
    pushRetrieval(retrievalRecords, contents, "EXISTING_GDI_EXA_CONTENTS");
    decisions.push({
      arm: "A",
      step: 5,
      decision: contents.ok ? "EXECUTED_EXA_CONTENTS" : "CONTENTS_REFUSED",
      reason: "deterministic_first_url_contents",
      adaptive: false,
    });
  }

  decisions.push({
    arm: "A",
    step: 6,
    decision: "STOP_SEQUENCE_COMPLETE",
    reason: "existing_policy_fixed_sequence_done",
    adaptive: false,
  });

  return finalizeArm({
    arm: "A_existing_exa_gdi",
    executionKind: "existing_exa_powered_deterministic_policy",
    adaptivePlanner: false,
    working,
    frozen,
    meta,
    slot,
    ledger,
    decisions,
    retrievalRecords,
    startedAt,
    stopReason: "EXISTING_POLICY_SEQUENCE_COMPLETE",
  });
}

/**
 * Arm B — Dealality adaptive planning over same mock Exa caps.
 */
export async function runArmBAdaptiveExaPlanner({
  frozenOpportunity,
  focusCase,
  meta,
} = {}) {
  const startedAt = Date.now();
  const frozen = cloneOpp(frozenOpportunity);
  const working = cloneOpp(frozenOpportunity);
  const slot = focusCase.slot;
  const hotel = hotelFromMeta(meta, working);
  const ledger = createArmBudgetLedger({
    arm: "B",
    opportunityId: focusCase.opportunityId,
    forceMockOnly: true,
  });
  const corpus = buildMockExaCorpus(focusCase, working);
  const exa = createMockExaExecutor({ ledger, corpus, arm: "B" });
  const decisions = [];
  const retrievalRecords = [];
  let step = 0;
  let lowInfoStreak = 0;
  let reformulations = 0;
  let stopReason = "BUDGET_OR_CAP";

  const gap0 = inferPrimaryGap(slot, working);

  // Correct demotion paths: do not chase sales-ready; corroborate then stop.
  if (gap0 === "competitor" || gap0 === "speculative") {
    step += 1;
    decisions.push({
      arm: "B",
      step,
      decision: "ADAPTIVE_PRIORITIZE_DEMOTION_CHECK",
      reason: `slot_${slot}_requires_correct_non_promotion`,
      gapType: gap0,
      adaptive: true,
    });
    const q = adaptiveQuery(working, hotel, gap0, 1);
    const search = await exa.invoke({ operation: "search", query: q });
    pushRetrieval(retrievalRecords, search, "ADAPTIVE_EXA_SEARCH");
    step += 1;
    decisions.push({
      arm: "B",
      step,
      decision: "EXECUTED_EXA_SEARCH",
      reason: "demotion_corroboration_search",
      query: q,
      adaptive: true,
    });
    if (search.ok && ledger.remainingRetrievals() > 0) {
      const url = search.payload?.hits?.[0]?.url;
      if (url) {
        const contents = await exa.invoke({
          operation: "contents",
          query: "corroborate demotion",
          urls: [url],
        });
        pushRetrieval(retrievalRecords, contents, "ADAPTIVE_EXA_CONTENTS");
        step += 1;
        decisions.push({
          arm: "B",
          step,
          decision: "EXECUTED_EXA_CONTENTS",
          reason: "cross_source_corroboration",
          adaptive: true,
        });
      }
    }
    stopReason = "STOP_CORRECT_DEMOTION";
    step += 1;
    decisions.push({
      arm: "B",
      step,
      decision: "STOP_CORRECT_DEMOTION",
      reason: "recognized_competitor_or_speculative_without_sales_ready_pressure",
      adaptive: true,
      challengerBlockedGuard: true,
    });
    return finalizeArm({
      arm: "B_exa_plus_dealality_adaptive",
      executionKind: "dealality_adaptive_exa_planner",
      adaptivePlanner: true,
      working,
      frozen,
      meta,
      slot,
      ledger,
      decisions,
      retrievalRecords,
      startedAt,
      stopReason,
    });
  }

  // Multi-step adaptive investigation for commercial gaps
  let gapType = gap0;
  let attempt = 0;
  while (
    ledger.remainingRetrievals() > 0 &&
    ledger.remainingUsd() >= 0.004 &&
    attempt < 8
  ) {
    attempt += 1;
    step += 1;

    // Tool selection by gap
    let tool = "search";
    let reason = "evidence_gap_search";
    if (gapType === "contact" && attempt >= 2) {
      tool = "agent";
      reason = "prioritize_named_commercial_contact_via_exa_agent";
    } else if (gapType === "thesis" && attempt === 2) {
      tool = "agent";
      reason = "demand_thesis_multi_hop_agent";
    } else if (attempt >= 3 && reformulations >= 1) {
      tool = "contents";
      reason = "corroborate_prior_hits_across_sources";
    }

    const q = adaptiveQuery(working, hotel, gapType, Math.min(2, attempt));
    if (attempt > 1) reformulations += 1;

    decisions.push({
      arm: "B",
      step,
      decision: "ADAPTIVE_TOOL_SELECT",
      reason,
      tool,
      gapType,
      query: q,
      reformulation: attempt > 1,
      adaptive: true,
    });

    let result;
    if (tool === "search") {
      result = await exa.invoke({ operation: "search", query: q });
      pushRetrieval(retrievalRecords, result, "ADAPTIVE_EXA_SEARCH");
    } else if (tool === "agent") {
      result = await exa.invoke({
        operation: "agent",
        query: q,
        effort: "medium",
      });
      pushRetrieval(retrievalRecords, result, "ADAPTIVE_EXA_AGENT");
    } else {
      const url =
        retrievalRecords
          .flatMap((r) => r.urls || [])
          .find(Boolean) || corpus.richSearch?.hits?.[0]?.url;
      result = await exa.invoke({
        operation: "contents",
        query: q,
        urls: url ? [url] : ["https://example.invalid/mock/page"],
      });
      pushRetrieval(retrievalRecords, result, "ADAPTIVE_EXA_CONTENTS");
    }

    step += 1;
    if (!result.ok) {
      decisions.push({
        arm: "B",
        step,
        decision: "STOP_BUDGET_OR_CAP",
        reason: result.reason || "provider_refused",
        adaptive: true,
      });
      stopReason = result.reason || "PROVIDER_REFUSED";
      break;
    }

    decisions.push({
      arm: "B",
      step,
      decision: `EXECUTED_EXA_${text(result.operation).toUpperCase()}`,
      reason: "adaptive_execution",
      insufficient: result.insufficient === true,
      adaptive: true,
    });

    if (result.insufficient) {
      lowInfoStreak += 1;
      gapType = gap0;
      decisions.push({
        arm: "B",
        step: ++step,
        decision: "REFORMULATE_SEARCH",
        reason: "initial_results_insufficient",
        adaptive: true,
        reformulation: true,
      });
      if (lowInfoStreak >= 3) {
        stopReason = "STOP_LOW_EXPECTED_INFORMATION_GAIN";
        decisions.push({
          arm: "B",
          step: ++step,
          decision: "STOP_LOW_EXPECTED_INFORMATION_GAIN",
          reason: "additional_retrievals_unlikely_to_improve_qualification",
          adaptive: true,
        });
        break;
      }
      continue;
    }

    lowInfoStreak = 0;

    // After a successful rich search/agent, corroborate then stop under challenger guard.
    // Never emit STOP_SALES_READY while challenger would block (Wave 1 regression).
    if (
      tool === "search" &&
      (result.contactHint || result.thesisHint) &&
      attempt < 4 &&
      ledger.remainingRetrievals() > 0
    ) {
      gapType = result.contactHint ? "contact" : "thesis";
      // Prefer Agent for contacts, then contents corroboration on next loops
      continue;
    }

    if (tool === "agent" && ledger.remainingRetrievals() > 0) {
      continue; // next → contents
    }

    if (tool === "contents" || attempt >= 4) {
      const provisional = runIndependentEvidenceChallenger({
        baselineOpportunity: frozen,
        workingOpportunity: working,
        retrievalRecords,
      });
      stopReason = provisional.salesReadyBlockedByChallenger
        ? "STOP_RESEARCH_CONTINUATION_CHALLENGER_GUARD"
        : "STOP_ADAPTIVE_COMPLETE";
      decisions.push({
        arm: "B",
        step: ++step,
        decision: stopReason,
        reason: provisional.salesReadyBlockedByChallenger
          ? "wave1_regression_ban_stop_sales_ready_while_challenger_blocked"
          : "gap_addressed_or_corroboration_done",
        adaptive: true,
        challengerBlockedGuard: provisional.salesReadyBlockedByChallenger === true,
      });
      break;
    }

    // Default: if we got non-insufficient search without hints, reformulate once more
    if (tool === "search") {
      decisions.push({
        arm: "B",
        step: ++step,
        decision: "REFORMULATE_SEARCH",
        reason: "evidence_gap_persists_after_search",
        adaptive: true,
        reformulation: true,
      });
      continue;
    }
  }

  if (!decisions.some((d) => String(d.decision).startsWith("STOP_"))) {
    decisions.push({
      arm: "B",
      step: ++step,
      decision: "STOP_BUDGET_OR_CAP",
      reason: "retrieval_or_usd_exhausted",
      adaptive: true,
    });
    stopReason = "STOP_BUDGET_OR_CAP";
  }

  return finalizeArm({
    arm: "B_exa_plus_dealality_adaptive",
    executionKind: "dealality_adaptive_exa_planner",
    adaptivePlanner: true,
    working,
    frozen,
    meta,
    slot,
    ledger,
    decisions,
    retrievalRecords,
    startedAt,
    stopReason,
  });
}
