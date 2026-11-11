/**
 * Adaptive Deep Research prioritization V1 (zero-paid).
 *
 * Uniform $1.10/workflow allocation is NOT evidence-driven.
 * Discovery may stay broad; Deep Research should prefer:
 *   - strongest demand relevance
 *   - buyer discoverability
 *   - hotel suitability
 *   - commercial timing
 *   - differentiation vs existing inventory
 *   - unresolved high-value gaps
 *   - hotel commercial strategy profile weights (V30)
 *   - likelihood additional research resolves a material blocker (V30)
 *
 * Does not authorize spend or make paid calls.
 */
import { applyHotelStrategyScoreAdjustment } from "./hotel-commercial-strategy-profiles-v1.js";

export const ADAPTIVE_DEEP_RESEARCH_PRIORITIZATION_V1 =
  "gdi-adaptive-deep-research-prioritization-v1";

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

function clamp(n, lo, hi) {
  return Math.max(lo, Math.min(hi, n));
}

/**
 * Score a discovery candidate for Deep Research value (0–100).
 */
export function scoreDeepResearchCandidate(candidate = {}, ctx = {}) {
  const name = text(
    candidate.name ||
      candidate.organizationName ||
      candidate.organizingEntity ||
      candidate.eventIdentity ||
      candidate.verifiedEventName,
  );
  const reasons = [];
  let score = 0;

  // Demand relevance — prefer recoverable hotel revenue mechanisms over fame.
  const demand = text(
    candidate.demandMechanismSummary ||
      candidate.evidenceOfHospitalityDemand ||
      candidate.hotelDemandThesis ||
      candidate.preliminaryHotelFit ||
      "",
  );
  const blob = `${name} ${demand} ${text(candidate.programmeName)} ${text(candidate.activityGeography)}`;
  if (
    /hotel|room|lodging|delegate|speaker|overflow|meeting|catering|banquet|incentive|retreat|buyout|wedding|gala|preferred.?lodging|training.?group|board.?offsite|executive.?offsite/i.test(
      demand,
    )
  ) {
    score += 18;
    reasons.push("demand_language_present");
  } else if (demand) {
    score += 6;
    reasons.push("generic_demand_text");
  } else {
    reasons.push("missing_demand_mechanism");
  }

  // High-yield commercial mechanisms that historically convert under shared acceptance
  if (
    /\b(incentive|executive retreat|leadership offsite|destination wedding|group buyout|preferred lodging|corporate training|recurring (?:meeting|programme|program)|client event|board retreat)\b/i.test(
      blob,
    )
  ) {
    score += 10;
    reasons.push("high_yield_commercial_mechanism");
  }

  // Event / programme identity (not venue/hotel page)
  if (
    /\b(congress|conference|summit|symposium|forum|championship|gala|showcase|incentive)\b/i.test(
      name,
    )
  ) {
    score += 14;
    reasons.push("named_programme_identity");
  }
  if (/\b(hotel|novotel|hilton|marriott|accommodation list|hotel list)\b/i.test(name)) {
    score -= 25;
    reasons.push("venue_or_hotel_page_penalty");
  }

  // V26→V27 yield lesson: competitor construction / infrastructure workforce rarely
  // become Sales Ready for boutique or meetings hotels — deprioritize for deep spend.
  if (
    /topping[- ]out|first hotel tower|national resort|developer reported|water supply expansion|sewerage|workforce housing|construction project\b/i.test(
      blob,
    )
  ) {
    score -= 18;
    reasons.push("low_conversion_development_or_infrastructure_penalty");
  }
  // Auction/gallery openings without lodging pattern — Watch-grade, not deep-first.
  if (
    /auction|gallery opening|espace .{0,20}opened|expert days/i.test(blob) &&
    !/room block|delegates?|overnight|incentive|symposium|congress/i.test(blob)
  ) {
    score -= 12;
    reasons.push("thin_cultural_presence_penalty");
  }
  // Famous org alone must not outrank concrete lodging/meeting demand.
  if (
    /\b(united nations|unesco|who\b|olympic|fifa)\b/i.test(name) &&
    !/hotel|room|lodging|delegate|meeting|accommodation|overflow/i.test(demand)
  ) {
    score -= 8;
    reasons.push("fame_without_lodging_mechanism_penalty");
  }

  // Timing
  const dates = text(
    candidate.verifiedDates ||
      candidate.futureEditionDate ||
      candidate.verifiedStartDate ||
      candidate.eventStartDate ||
      "",
  );
  if (/202[6-9]|2030/i.test(dates) || candidate.futureEditionStatus === "confirmed") {
    score += 12;
    reasons.push("upcoming_or_confirmed_edition");
  } else if (/historical|past|completed/i.test(text(candidate.eventStatus))) {
    score -= 8;
    reasons.push("historical_timing_penalty");
  }

  // Buyer discoverability
  const contacts = candidate.organizerContactCandidates || candidate.namedContacts || [];
  const named = contacts.filter((c) => text(c?.name));
  if (named.length) {
    score += 14;
    reasons.push("named_contact_candidate");
  } else if (contacts.length) {
    score += 4;
    reasons.push("role_only_contact");
  } else {
    reasons.push("no_buyer_seed");
  }

  // Hotel suitability (generic fit language — strategy profile adds geography)
  const fit = text(candidate.preliminaryHotelFit || candidate.hotelMarketRelevance || "");
  if (/specific property|this hotel|overflow|ancillary meeting|airport corridor|grand anse|founex/i.test(fit)) {
    score += 12;
    reasons.push("hotel_specific_fit_language");
  } else if (fit) {
    score += 5;
    reasons.push("generic_fit");
  }

  // Evidence quality
  const sources = candidate.supportingSources || candidate.evidenceUrls || [];
  if (sources.length >= 2) {
    score += 8;
    reasons.push("multi_source");
  } else if (sources.length === 1) {
    score += 4;
    reasons.push("single_source");
  }

  // Inventory overlap
  const key = text(candidate.candidateKey || name).toLowerCase();
  const existing = new Set(
    (ctx.existingKeys || []).map((k) => String(k || "").toLowerCase()),
  );
  if (key && existing.has(key)) {
    score -= 20;
    reasons.push("existing_inventory_overlap");
  }

  // Differentiation bonus for high commercial track
  if (
    /TRACK_A|upcoming_venue|confirmed_upcoming/i.test(
      text(candidate.commercialOpportunityTrack || candidate.demandCycleStatus),
    )
  ) {
    score += 8;
    reasons.push("confirmed_upcoming_track");
  }

  // Speculative overnight / competitor-committed without accessible alternative
  if (
    /speculative overnight|might stay|could overnight/i.test(blob) &&
    !/room block|preferred|housing partner|delegates?/i.test(blob)
  ) {
    score -= 10;
    reasons.push("speculative_overnight_penalty");
  }
  if (
    /confirmed (?:host|venue)|competitor.?commit|main venue (?:is|already)/i.test(blob) &&
    !/overflow|satellite|ancillary|preferred lodging|alternative/i.test(blob)
  ) {
    score -= 8;
    reasons.push("competitor_committed_without_accessible_alt_penalty");
  }

  // V30: likelihood additional research resolves a material gap
  const unresolved = text(
    (candidate.uncertaintyRegister || []).join(" ") ||
      candidate.unresolvedCommercialQuestion ||
      "",
  );
  if (
    unresolved &&
    /contact|buyer|room block|housing|sourcing|planner|dates?|edition/i.test(unresolved)
  ) {
    score += 6;
    reasons.push("recoverable_unresolved_gap");
  }

  // V30: hotel commercial strategy profile (config weights, no org-name hardcoding)
  const hotelId = text(ctx.hotelId || candidate.hotelId || "");
  if (hotelId) {
    const adj = applyHotelStrategyScoreAdjustment(
      { ...candidate, workflowId: candidate.workflowId || ctx.workflowId },
      hotelId,
    );
    if (adj.delta) {
      score += adj.delta;
      reasons.push(...adj.reasons.map((r) => `strategy:${r}`));
    }
  }

  score = clamp(score, 0, 100);
  return {
    version: ADAPTIVE_DEEP_RESEARCH_PRIORITIZATION_V1,
    score,
    reasons,
    name: name || null,
    workflowId: candidate.workflowId || ctx.workflowId || null,
    hotelId: hotelId || null,
  };
}

/**
 * Continuation eligibility for undeepened / partial candidates (zero-paid planning).
 * Does not authorize spend.
 */
export function assessContinuationEligibility(candidate = {}, ctx = {}) {
  const scored = scoreDeepResearchCandidate(candidate, ctx);
  const minScore = Number(ctx.minContinuationScore ?? 36);
  const highPriorityFloor = Number(ctx.highPriorityScore ?? 48);
  const unsuitable =
    (scored.reasons || []).includes("low_conversion_development_or_infrastructure_penalty") ||
    (scored.reasons || []).includes("venue_or_hotel_page_penalty") ||
    scored.score < 18;
  const historical = (scored.reasons || []).includes("historical_timing_penalty");
  if (unsuitable) {
    return {
      eligible: false,
      class: "COMMERCIALLY_UNSUITABLE",
      score: scored.score,
      reasons: scored.reasons,
    };
  }
  if (historical && scored.score < minScore) {
    return {
      eligible: false,
      class: "HISTORICAL_COMPLETED",
      score: scored.score,
      reasons: scored.reasons,
    };
  }
  if (scored.score >= minScore) {
    return {
      eligible: true,
      class: "ELIGIBLE_FOR_CONTINUATION",
      score: scored.score,
      reasons: scored.reasons,
      highPriority: scored.score >= highPriorityFloor,
    };
  }
  return {
    eligible: false,
    class: "INSUFFICIENT_EVIDENCE_TO_PRIORITIZE",
    score: scored.score,
    reasons: scored.reasons,
  };
}

/**
 * Allocate Deep Research slots across workflows by candidate strength,
 * not uniform per-workflow spend.
 *
 * @param {object[]} candidates scored or raw
 * @param {{ maxDeepTotal?: number, maxPerWorkflow?: number, minScore?: number }} opts
 */
export function allocateAdaptiveDeepResearchSlots(candidates = [], opts = {}) {
  const maxDeepTotal = Number(opts.maxDeepTotal || 28);
  const maxPerWorkflow = Number(opts.maxPerWorkflow || 4);
  const minScore = Number(opts.minScore ?? 28);
  const existingKeys = opts.existingKeys || [];
  /**
   * First-time Full E2E depth floor (hotel-generic):
   * If a workflow produced unique post-dedup candidates, ensure at least N deep
   * slots for that workflow (unless global/workflow caps block it).
   * Prevents "LIVE_COMPLETED" with discovery-only when demand was found.
   */
  const minDeepPerWorkflowWithCandidates = Number(
    opts.minDeepPerWorkflowWithCandidates ??
      (opts.firstTimeFullE2e === true ? 1 : 0),
  );
  const floorMinScore = Number(
    opts.floorMinScore ?? Math.min(minScore, 12),
  );

  const hotelId = opts.hotelId || null;
  const scored = (candidates || []).map((c) => {
    const s =
      typeof c.score === "number" && c.reasons
        ? c
        : scoreDeepResearchCandidate(c, {
            existingKeys,
            workflowId: c.workflowId,
            hotelId: hotelId || c.hotelId,
          });
    return { ...c, ...s };
  });

  scored.sort((a, b) => (b.score || 0) - (a.score || 0));

  const selected = [];
  const perWf = new Map();
  const deferred = [];

  for (const row of scored) {
    if ((row.score || 0) < minScore) {
      deferred.push({ ...row, disposition: "below_min_score" });
      continue;
    }
    const wf = String(row.workflowId || "unknown");
    const used = perWf.get(wf) || 0;
    if (used >= maxPerWorkflow) {
      deferred.push({ ...row, disposition: "workflow_cap" });
      continue;
    }
    if (selected.length >= maxDeepTotal) {
      deferred.push({ ...row, disposition: "global_cap" });
      continue;
    }
    selected.push({ ...row, disposition: "selected_for_deep_research" });
    perWf.set(wf, used + 1);
  }

  // First-time depth floor: promote top deferred/below-min candidates per under-covered workflow
  const floorPromotions = [];
  if (minDeepPerWorkflowWithCandidates > 0) {
    const byWf = new Map();
    for (const row of scored) {
      const wf = String(row.workflowId || "unknown");
      if (!byWf.has(wf)) byWf.set(wf, []);
      byWf.get(wf).push(row);
    }
    for (const [wf, rows] of byWf) {
      const used = perWf.get(wf) || 0;
      const need = minDeepPerWorkflowWithCandidates - used;
      if (need <= 0) continue;
      const selectedKeys = new Set(
        selected.map((s) =>
          text(s.candidateKey || s.name || s.organizationName).toLowerCase(),
        ),
      );
      const pool = rows
        .filter((r) => (r.score || 0) >= floorMinScore)
        .filter((r) => {
          const k = text(r.candidateKey || r.name || r.organizationName).toLowerCase();
          return k && !selectedKeys.has(k);
        });
      for (let i = 0; i < need && i < pool.length; i++) {
        if (selected.length >= maxDeepTotal) break;
        if ((perWf.get(wf) || 0) >= maxPerWorkflow) break;
        const row = {
          ...pool[i],
          disposition: "selected_for_deep_research_first_time_floor",
          firstTimeDepthFloor: true,
        };
        selected.push(row);
        floorPromotions.push({
          workflowId: wf,
          name: row.name || row.organizationName || null,
          score: row.score,
        });
        perWf.set(wf, (perWf.get(wf) || 0) + 1);
        // remove matching deferred entries
        const k = text(row.candidateKey || row.name || row.organizationName).toLowerCase();
        for (let d = deferred.length - 1; d >= 0; d--) {
          const dk = text(
            deferred[d].candidateKey || deferred[d].name || deferred[d].organizationName,
          ).toLowerCase();
          if (dk && dk === k) deferred.splice(d, 1);
        }
      }
    }
  }

  const capDeferred = deferred.filter((d) =>
    /workflow_cap|global_cap/i.test(String(d.disposition || "")),
  );
  const unfinishedEligible = deferred.filter((d) => {
    const el = assessContinuationEligibility(d, {
      hotelId: hotelId || d.hotelId,
      minContinuationScore: minScore,
    });
    return el.eligible === true;
  });
  const unfinishedHighPriority = unfinishedEligible.filter((d) => {
    const el = assessContinuationEligibility(d, {
      hotelId: hotelId || d.hotelId,
      minContinuationScore: minScore,
    });
    return el.highPriority === true;
  });

  // Caps protect resources but MUST NOT be reported as commercial research complete.
  let researchCompletenessStatus = "COMPLETE";
  if (unfinishedHighPriority.length > 0 && capDeferred.length > 0) {
    researchCompletenessStatus = "PARTIAL_CANDIDATE_CAP";
  } else if (unfinishedHighPriority.length > 0 && opts.budgetCapHit === true) {
    researchCompletenessStatus = "PARTIAL_BUDGET_LIMIT";
  } else if (unfinishedHighPriority.length > 0) {
    researchCompletenessStatus =
      capDeferred.length > 0 ? "PARTIAL_CANDIDATE_CAP" : "PARTIAL_BUDGET_LIMIT";
  } else if (selected.length === 0 && scored.length === 0) {
    researchCompletenessStatus = "NO_ELIGIBLE_DEMAND";
  }

  return {
    version: ADAPTIVE_DEEP_RESEARCH_PRIORITIZATION_V1,
    maxDeepTotal,
    maxPerWorkflow,
    minScore,
    minDeepPerWorkflowWithCandidates,
    floorMinScore,
    firstTimeFullE2e: opts.firstTimeFullE2e === true,
    hotelId: hotelId || null,
    selected,
    deferred,
    floorPromotions,
    workflowCounts: Object.fromEntries(perWf),
    researchCompletenessStatus,
    commercialResearchComplete: researchCompletenessStatus === "COMPLETE",
    preserveResearchQueue:
      unfinishedEligible.length > 0 || unfinishedHighPriority.length > 0,
    // Full unfinished queue retained; preview is a DISPLAY batch only.
    unfinishedEligibleQueue: unfinishedEligible,
    unfinishedEligibleQueuePreview: unfinishedEligible.slice(0, 120),
    unfinishedEligibleCount: unfinishedEligible.length,
    unfinishedHighPriorityCount: unfinishedHighPriority.length,
    continueBeyondMinimumTarget:
      unfinishedHighPriority.length > 0 && researchCompletenessStatus !== "COMPLETE",
    note:
      "Zero-paid planning only — does not authorize or execute Exa spend. maxDeepTotal/maxPerWorkflow are RESEARCH_BATCH_LIMIT wave knobs, not inventory ceilings. PARTIAL_* preserves unfinished queues; never treat candidate caps or ≥25 Sales Ready as COMPLETE.",
  };
}

/**
 * Contrast uniform $1.10/workflow vs adaptive selection.
 */
export function auditUniformVsAdaptiveAllocation(uniformSelected = [], adaptivePlan = {}) {
  const uniformKeys = new Set(
    (uniformSelected || []).map((c) =>
      text(c.candidateKey || c.name || c.candidateName).toLowerCase(),
    ),
  );
  const adaptiveKeys = new Set(
    (adaptivePlan.selected || []).map((c) =>
      text(c.candidateKey || c.name || c.candidateName).toLowerCase(),
    ),
  );
  const onlyUniform = [...uniformKeys].filter((k) => k && !adaptiveKeys.has(k));
  const onlyAdaptive = [...adaptiveKeys].filter((k) => k && !uniformKeys.has(k));
  return {
    version: ADAPTIVE_DEEP_RESEARCH_PRIORITIZATION_V1,
    uniformCount: uniformKeys.size,
    adaptiveCount: adaptiveKeys.size,
    onlyInUniformAllocation: onlyUniform,
    onlyInAdaptiveAllocation: onlyAdaptive,
    uniformWasEqualSpendPerWorkflow: true,
    adaptiveIsEvidenceWeighted: true,
  };
}
