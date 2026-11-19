/**
 * Offline research-priority evaluation V1.
 *
 * Compares selection policies on frozen inventories WITHOUT enabling a new
 * production selection policy. Weights here are evaluation hypotheses only.
 */

import { listFourteenWorkflowSegmentIds } from "./universal-contact-research-contract-v1.js";
import {
  selectCandidatesForDeepResearch,
  scoreSelectionDimensions,
} from "./gdi-deep-research-selection-register-v1.js";
import {
  resolveLegacyResearchSegmentAlias,
  inferDemandSegmentIdFromResearchEvidence,
} from "../legacy-demand-segment-alias-map-v1.js";

export const GDI_RESEARCH_PRIORITY_EVALUATION_V1 =
  "gdi-research-priority-evaluation-v1";

/** Evaluation-only rubric — NOT production weights. */
export const PROPOSED_RESEARCH_PRIORITY_WEIGHTS_EVAL = Object.freeze({
  demandCredibility: 0.22,
  hotelMarketRelevance: 0.2,
  futureTimingAndSourcingPotential: 0.18,
  commercialAccessibility: 0.12,
  potentialEconomicRelevance: 0.14,
  noveltyAndExpectedValueOfAdditionalResearch: 0.14,
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function clamp01(n) {
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(1, n));
}

export function resolveCandidateSegmentId(row = {}) {
  const raw =
    row.researchSegment ||
    row.demandSegmentId ||
    row.segmentId ||
    row.segment ||
    row.opportunityType ||
    "";
  const alias = resolveLegacyResearchSegmentAlias(raw);
  if (alias) return alias;
  const inferred = inferDemandSegmentIdFromResearchEvidence(row);
  if (inferred?.demandSegmentId) return inferred.demandSegmentId;
  if (typeof inferred === "string" && inferred) return inferred;
  const lower = text(raw).toLowerCase();
  const ids = listFourteenWorkflowSegmentIds();
  if (ids.includes(lower)) return lower;
  if (/scaling|emerging corporate|corporate account/i.test(raw)) {
    return "corporate_accounts_scaling";
  }
  if (/medical|scientific|healthcare|university|education/i.test(raw)) {
    return "education_scientific";
  }
  if (/government|public sector|contractor/i.test(raw)) {
    return "government_public_sector";
  }
  if (/association|conference|advocacy/i.test(raw)) {
    return "associations_conferences_events";
  }
  if (/sport/i.test(raw)) return "sports_entertainment";
  if (/reloc|extended stay/i.test(raw)) return "relocation_extended_stay";
  if (/preferred.?lodging|venue partnership/i.test(raw)) {
    return "preferred_lodging_partnerships";
  }
  if (/planner|dmc/i.test(raw)) return "professional_event_planners";
  if (/catering|local meeting/i.test(raw)) return "local_catering_day_meetings";
  if (/celebration|private event|wedding/i.test(raw)) {
    return "corporate_celebrations_private_events";
  }
  if (/meeting|training|incentive/i.test(raw)) {
    return "corporate_meetings_incentives";
  }
  if (/project|infrastructure|construction/i.test(raw)) {
    return "projects_infrastructure";
  }
  return "associations_conferences_events";
}

/**
 * Normalize a frozen inventory row into a selection candidate shape.
 */
export function inventoryRowToSelectionCandidate(row = {}, hotelMeta = {}) {
  const segmentId = resolveCandidateSegmentId(row);
  return {
    id: row.id || row.opportunityId,
    opportunityId: row.id || row.opportunityId,
    hotelId: row.hotelId || hotelMeta.hotelId || null,
    hotelName: hotelMeta.hotelName || null,
    organizationName: row.organizationName || row.company || null,
    title: row.title || row.displayTitle || row.programmeName || null,
    researchSegment: segmentId,
    segmentId,
    hotelOpportunityThesis:
      row.hotelOpportunityThesis || row.summaryWhyHotel || row.whyThisHotel || "",
    evidence: Array.isArray(row.evidence) ? row.evidence : [],
    seedUrls: [
      ...(Array.isArray(row.evidence)
        ? row.evidence.map((e) => e?.url).filter(Boolean)
        : []),
      row.sourceUrl,
    ].filter(Boolean),
    priority: row.priority || null,
    demandStatus: row.demandStatus || null,
    venueSourcingStatus: row.venueSourcingStatus || null,
    hotelFitScore: row.hotelFitScore,
    primaryContactName: row.primaryContactName || row.primaryContact?.name,
    qualificationFailureReason: row.qualificationFailureReason || null,
    eventStartDate: row.eventStartDate || null,
    preDeepScreen: { outcome: "ADVANCE_TO_DEEP_RESEARCH" },
  };
}

/**
 * Proposed research-priority score (evaluation only).
 * Does not penalize missing named buyer / room nights alone.
 */
export function scoreProposedResearchPriority(candidate = {}, context = {}) {
  const blob = [
    candidate.hotelOpportunityThesis,
    candidate.title,
    candidate.organizationName,
    candidate.demandStatus,
    candidate.venueSourcingStatus,
    candidate.qualificationFailureReason,
  ]
    .map(text)
    .join(" ");

  const demandCredibility = clamp01(
    (/CONFIRMED|RELIABLE|qualified|housing|room block|overflow|rfp|sourcing/i.test(
      blob,
    )
      ? 0.55
      : 0.3) +
      ((candidate.evidence || []).length || (candidate.seedUrls || []).length
        ? 0.25
        : 0) +
      (/SPECULATIVE|unsupported|junk/i.test(blob) ? -0.25 : 0.1),
  );

  const hotelMarketRelevance = clamp01(
    Number.isFinite(Number(candidate.hotelFitScore))
      ? Number(candidate.hotelFitScore) / 100
      : /OUT_OF_MARKET|MISMATCH|elsewhere|chicago|national harbor/i.test(blob)
        ? 0.15
        : /bethesda|rockville|montgomery|dmv|local|same market/i.test(blob)
          ? 0.7
          : 0.45,
  );

  const futureTimingAndSourcingPotential = clamp01(
    /202[7-9]|TOO_EARLY|monitor|future cycle|rotational|shortlist/i.test(blob)
      ? 0.75
      : candidate.eventStartDate
        ? 0.55
        : 0.35,
  );

  const commercialAccessibility = clamp01(
    candidate.primaryContactName || candidate.primaryContact?.name
      ? 0.7
      : /contact|planner|procurement|housing bureau|rfp/i.test(blob)
        ? 0.55
        : 0.35,
  );

  const potentialEconomicRelevance = clamp01(
    /peak rooms|room night|attendance|block|multi-day|training|workforce|preferred/i.test(
      blob,
    )
      ? 0.7
      : Number(candidate.priority === "HIGH" || candidate.priority === "MEDIUM")
        ? 0.55
        : 0.4,
  );

  const noveltyAndExpectedValueOfAdditionalResearch = clamp01(
    /WATCHLIST|TOO_EARLY|unpublished|lodging not|hotel tba|open sourcing/i.test(
      blob,
    )
      ? 0.8
      : candidate.preDeepScreen?.outcome === "DUPLICATE_NO_NEW_EVIDENCE"
        ? 0.1
        : 0.45,
  );

  const dims = {
    demandCredibility: Number(demandCredibility.toFixed(3)),
    hotelMarketRelevance: Number(hotelMarketRelevance.toFixed(3)),
    futureTimingAndSourcingPotential: Number(
      futureTimingAndSourcingPotential.toFixed(3),
    ),
    commercialAccessibility: Number(commercialAccessibility.toFixed(3)),
    potentialEconomicRelevance: Number(potentialEconomicRelevance.toFixed(3)),
    noveltyAndExpectedValueOfAdditionalResearch: Number(
      noveltyAndExpectedValueOfAdditionalResearch.toFixed(3),
    ),
  };

  let weighted = 0;
  for (const [k, w] of Object.entries(PROPOSED_RESEARCH_PRIORITY_WEIGHTS_EVAL)) {
    weighted += (dims[k] || 0) * w;
  }

  // Existing production score for side-by-side comparison only.
  const existing = scoreSelectionDimensions(candidate, context);

  return {
    version: GDI_RESEARCH_PRIORITY_EVALUATION_V1,
    evaluationOnly: true,
    dimensions: dims,
    weights: PROPOSED_RESEARCH_PRIORITY_WEIGHTS_EVAL,
    proposedScore: Number((weighted * 100).toFixed(2)),
    existingCombinedScore: existing.combinedScore,
    existingDimensions: existing.dimensions,
    note: "Proposed weights are offline hypotheses — not production policy.",
  };
}

function selectFirstInList(candidates, maxCandidates) {
  const selected = candidates.slice(0, maxCandidates);
  return {
    policy: "FIRST_IN_LIST",
    selectedIds: selected.map((c) => c.id || c.opportunityId),
    selected,
  };
}

function selectPureScore(candidates, maxCandidates, context) {
  const scored = candidates
    .map((c, idx) => ({
      c,
      idx,
      score: scoreProposedResearchPriority(c, context).proposedScore,
    }))
    .sort((a, b) => b.score - a.score || a.idx - b.idx);
  const selected = scored.slice(0, maxCandidates).map((r) => r.c);
  return {
    policy: "PROPOSED_PURE_SCORE_EVAL",
    selectedIds: selected.map((c) => c.id || c.opportunityId),
    selected,
    scored: scored.map((r) => ({
      id: r.c.id || r.c.opportunityId,
      score: r.score,
      segmentId: r.c.researchSegment,
    })),
  };
}

/**
 * Offline comparison of selection policies on a candidate pool.
 */
export function compareResearchPriorityPolicies({
  candidates = [],
  maxCandidates = 8,
  fairMinPerSegment = 1,
  hotelProfile = null,
} = {}) {
  const context = { hotelProfile };
  const planned = listFourteenWorkflowSegmentIds();

  const first = selectFirstInList(candidates, maxCandidates);
  const current = selectCandidatesForDeepResearch(candidates, {
    maxCandidates,
    fairMinPerSegment,
    hotelProfile,
    segmentIds: planned,
  });
  const proposed = selectPureScore(candidates, maxCandidates, context);

  const currentIds = new Set(
    (current.selected || []).map((c) => c.id || c.opportunityId),
  );
  const proposedIds = new Set(proposed.selectedIds);
  const firstIds = new Set(first.selectedIds);

  const overlapCurrentProposed = [...currentIds].filter((id) =>
    proposedIds.has(id),
  ).length;
  const proposedNotInCurrent = proposed.selected.filter(
    (c) => !currentIds.has(c.id || c.opportunityId),
  );
  const currentNotInProposed = (current.selected || []).filter(
    (c) => !proposedIds.has(c.id || c.opportunityId),
  );

  const segmentsSelectedCurrent = { ...(current.segmentSelected || {}) };
  const segmentsSelectedProposed = {};
  for (const c of proposed.selected) {
    const sid = c.researchSegment || c.segmentId;
    if (!sid) continue;
    segmentsSelectedProposed[sid] = (segmentsSelectedProposed[sid] || 0) + 1;
  }

  const fairPassConsumedEntireCap =
    fairMinPerSegment > 0 &&
    maxCandidates > 0 &&
    maxCandidates < planned.length * fairMinPerSegment;

  return {
    version: GDI_RESEARCH_PRIORITY_EVALUATION_V1,
    evaluationOnly: true,
    productionPolicyChanged: false,
    maxCandidates,
    fairMinPerSegment,
    candidateCount: candidates.length,
    policies: {
      firstInList: {
        selectedCount: first.selectedIds.length,
        selectedIds: first.selectedIds,
      },
      currentProductionFairFloorThenScore: {
        selectedCount: current.selectedCount,
        deferredCount: current.deferredCount,
        segmentSelected: segmentsSelectedCurrent,
        selectedIds: [...currentIds],
        registerSample: (current.register || []).slice(0, 12).map((r) => ({
          id: r.candidateId,
          segmentId: r.segmentId,
          score: r.preliminaryScore,
          decision: r.selectionDecision,
          reason: r.decisionReason,
        })),
      },
      proposedPureScoreEval: {
        selectedCount: proposed.selectedIds.length,
        segmentSelected: segmentsSelectedProposed,
        selectedIds: proposed.selectedIds,
        topScores: (proposed.scored || []).slice(0, 12),
      },
    },
    overlap: {
      currentVsProposed: overlapCurrentProposed,
      firstVsCurrent: [...firstIds].filter((id) => currentIds.has(id)).length,
      firstVsProposed: [...firstIds].filter((id) => proposedIds.has(id)).length,
    },
    valuableOverlookedByCurrentCap: proposedNotInCurrent.map((c) => ({
      id: c.id || c.opportunityId,
      organizationName: c.organizationName,
      title: c.title,
      segmentId: c.researchSegment,
      proposedScore: scoreProposedResearchPriority(c, context).proposedScore,
      existingScore: scoreSelectionDimensions(c, context).combinedScore,
      priority: c.priority || null,
    })),
    currentSelectedNotInProposedTop: currentNotInProposed.map((c) => ({
      id: c.id || c.opportunityId,
      organizationName: c.organizationName,
      segmentId: c.researchSegment,
    })),
    structuralFinding: {
      fairPassCanConsumeEntireCap: fairPassConsumedEntireCap,
      note: fairPassConsumedEntireCap
        ? "With fairMinPerSegment=1 and maxCandidates < planned segments, commercial score fill never runs; late taxonomy segments are systematically deferred."
        : "Cap is large enough for fair floor + commercial fill to coexist.",
    },
    recommendation:
      "Do not enable proposed weights in production until founder review. Consider raising default deep cap to plannedSegmentCount OR reserving commercial-fill slots after a bounded fair floor.",
  };
}

export default {
  GDI_RESEARCH_PRIORITY_EVALUATION_V1,
  PROPOSED_RESEARCH_PRIORITY_WEIGHTS_EVAL,
  resolveCandidateSegmentId,
  inventoryRowToSelectionCandidate,
  scoreProposedResearchPriority,
  compareResearchPriorityPolicies,
};
