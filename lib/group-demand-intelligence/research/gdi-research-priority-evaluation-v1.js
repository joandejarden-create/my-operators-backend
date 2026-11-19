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

/** Founder Option 2 evaluation arms — offline only; not production weights. */
export const OPTION2_FAIRNESS_COMMERCIAL_ARMS_EVAL = Object.freeze([
  { id: "fair25_commercial75", fairnessRatio: 0.25, commercialRatio: 0.75 },
  { id: "fair40_commercial60", fairnessRatio: 0.4, commercialRatio: 0.6 },
  { id: "fair50_commercial50", fairnessRatio: 0.5, commercialRatio: 0.5 },
]);

function normalizeOrgKey(name) {
  return text(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/**
 * Edition key: same org + same calendar year/edition may collapse;
 * distinct future editions of a recurring org stay separate.
 */
export function opportunityEditionKey(candidate = {}) {
  const org = normalizeOrgKey(candidate.organizationName || candidate.company);
  const title = text(candidate.title || candidate.programmeName || "");
  const yearMatch =
    String(candidate.eventStartDate || title).match(/\b(20[2-9]\d)\b/) || [];
  const year = yearMatch[1] || "undated";
  const editionHint = title
    .toLowerCase()
    .replace(org, "")
    .replace(/\b(20[2-9]\d)\b/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .slice(0, 48);
  return `${org || "unknown"}::${year}::${editionHint || "edition"}`;
}

/**
 * Deduplicate identical org+edition rows before allocating deep slots.
 * Keeps the higher-scoring / earlier row. Does NOT collapse distinct editions.
 */
export function dedupeCandidatesByOrganizationEdition(
  candidates = [],
  context = {},
) {
  const bestByKey = new Map();
  const droppedDuplicates = [];
  candidates.forEach((c, idx) => {
    const key = opportunityEditionKey(c);
    const score = scoreProposedResearchPriority(c, context).proposedScore;
    const prev = bestByKey.get(key);
    if (!prev) {
      bestByKey.set(key, { c, idx, score, key });
      return;
    }
    if (score > prev.score || (score === prev.score && idx < prev.idx)) {
      droppedDuplicates.push({
        keptId: c.id || c.opportunityId,
        droppedId: prev.c.id || prev.c.opportunityId,
        editionKey: key,
        reason: "same_org_same_edition",
      });
      bestByKey.set(key, { c, idx, score, key });
    } else {
      droppedDuplicates.push({
        keptId: prev.c.id || prev.c.opportunityId,
        droppedId: c.id || c.opportunityId,
        editionKey: key,
        reason: "same_org_same_edition",
      });
    }
  });
  return {
    candidates: [...bestByKey.values()]
      .sort((a, b) => a.idx - b.idx)
      .map((r) => r.c),
    droppedDuplicates,
    distinctEditionCount: bestByKey.size,
  };
}

function summarizeSelectionQuality(selected = [], context = {}) {
  const rows = selected.map((c) => {
    const scored = scoreProposedResearchPriority(c, context);
    return {
      id: c.id || c.opportunityId,
      organizationName: c.organizationName,
      segmentId: c.researchSegment || c.segmentId,
      editionKey: opportunityEditionKey(c),
      proposedScore: scored.proposedScore,
      dimensions: scored.dimensions,
      hasEvidence: Boolean(
        (c.evidence && c.evidence.length) || (c.seedUrls && c.seedUrls.length),
      ),
      hasVerifiedContactName: Boolean(
        c.primaryContactName || c.primaryContact?.name,
      ),
    };
  });
  const segmentSelected = {};
  for (const r of rows) {
    if (!r.segmentId) continue;
    segmentSelected[r.segmentId] = (segmentSelected[r.segmentId] || 0) + 1;
  }
  const dimKeys = Object.keys(PROPOSED_RESEARCH_PRIORITY_WEIGHTS_EVAL);
  const avgDims = {};
  for (const k of dimKeys) {
    const vals = rows.map((r) => r.dimensions?.[k]).filter(Number.isFinite);
    avgDims[k] = vals.length
      ? Number((vals.reduce((a, b) => a + b, 0) / vals.length).toFixed(3))
      : 0;
  }
  return {
    selectedCount: rows.length,
    segmentSelected,
    segmentDiversity: Object.keys(segmentSelected).length,
    withEvidenceCount: rows.filter((r) => r.hasEvidence).length,
    withContactNameCount: rows.filter((r) => r.hasVerifiedContactName).length,
    meanProposedScore: rows.length
      ? Number(
          (
            rows.reduce((a, r) => a + (r.proposedScore || 0), 0) / rows.length
          ).toFixed(2),
        )
      : 0,
    averageDimensions: avgDims,
    selectedIds: rows.map((r) => r.id),
    sample: rows.slice(0, 12),
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
 * Option 2 evaluation arm: bounded fairness slots then commercial fill.
 * fairnessRatio of cap is reserved for round-robin segment coverage;
 * remaining slots fill by proposed commercial score.
 * Evaluation-only — does not mutate production selectCandidatesForDeepResearch.
 */
export function selectBoundedFairnessThenCommercial(
  candidates = [],
  {
    maxCandidates = 8,
    fairnessRatio = 0.4,
    hotelProfile = null,
    segmentIds = null,
    cycleOffset = 0,
  } = {},
) {
  const context = { hotelProfile };
  const planned = segmentIds || listFourteenWorkflowSegmentIds();
  const rotated = [
    ...planned.slice(cycleOffset % planned.length),
    ...planned.slice(0, cycleOffset % planned.length),
  ];
  const fairSlots = Math.max(
    0,
    Math.min(maxCandidates, Math.round(maxCandidates * fairnessRatio)),
  );
  const commercialSlots = Math.max(0, maxCandidates - fairSlots);

  const scored = candidates
    .map((c, idx) => ({
      c,
      idx,
      score: scoreProposedResearchPriority(c, context).proposedScore,
      segmentId: c.researchSegment || c.segmentId,
    }))
    .sort((a, b) => b.score - a.score || a.idx - b.idx);

  const selected = [];
  const selectedIds = new Set();
  const segmentSelected = Object.fromEntries(planned.map((s) => [s, 0]));

  function tryPick(row, reason) {
    const id = row.c.id || row.c.opportunityId;
    if (!id || selectedIds.has(id)) return false;
    if (selected.length >= maxCandidates) return false;
    selectedIds.add(id);
    selected.push({ ...row.c, _selectionReason: reason, _evalScore: row.score });
    if (row.segmentId && segmentSelected[row.segmentId] != null) {
      segmentSelected[row.segmentId] += 1;
    }
    return true;
  }

  // Fairness pass: round-robin across rotated segment order (anti-starvation).
  let fairTaken = 0;
  let progress = true;
  while (fairTaken < fairSlots && progress) {
    progress = false;
    for (const seg of rotated) {
      if (fairTaken >= fairSlots) break;
      const row = scored.find(
        (r) =>
          r.segmentId === seg &&
          !selectedIds.has(r.c.id || r.c.opportunityId),
      );
      if (!row) continue;
      if (tryPick(row, `bounded_fair:${seg}:score=${row.score}`)) {
        fairTaken += 1;
        progress = true;
      }
    }
  }

  // Commercial fill by score.
  let commercialTaken = 0;
  for (const row of scored) {
    if (commercialTaken >= commercialSlots && selected.length >= maxCandidates) {
      break;
    }
    if (selected.length >= maxCandidates) break;
    if (tryPick(row, `commercial_fill:score=${row.score}`)) {
      commercialTaken += 1;
    }
  }

  return {
    policy: "BOUNDED_FAIRNESS_THEN_COMMERCIAL_EVAL",
    evaluationOnly: true,
    fairnessRatio,
    commercialRatio: Number((1 - fairnessRatio).toFixed(2)),
    fairSlots,
    commercialSlots,
    cycleOffset,
    segmentOrderStart: rotated[0] || null,
    selectedIds: selected.map((c) => c.id || c.opportunityId),
    selected,
    segmentSelected,
    quality: summarizeSelectionQuality(selected, context),
  };
}

/**
 * Simulate fair segment rotation across repeated research cycles.
 */
export function simulateFairSegmentRotationAcrossCycles({
  candidates = [],
  maxCandidates = 8,
  fairnessRatio = 0.4,
  cycles = 4,
  hotelProfile = null,
} = {}) {
  const planned = listFourteenWorkflowSegmentIds();
  const cycleResults = [];
  const cumulativeSegmentHits = Object.fromEntries(planned.map((s) => [s, 0]));
  for (let cycle = 0; cycle < cycles; cycle += 1) {
    const arm = selectBoundedFairnessThenCommercial(candidates, {
      maxCandidates,
      fairnessRatio,
      hotelProfile,
      cycleOffset: cycle,
    });
    for (const [seg, n] of Object.entries(arm.segmentSelected || {})) {
      cumulativeSegmentHits[seg] = (cumulativeSegmentHits[seg] || 0) + n;
    }
    cycleResults.push({
      cycle,
      cycleOffset: cycle,
      segmentOrderStart: arm.segmentOrderStart,
      selectedIds: arm.selectedIds,
      segmentSelected: arm.segmentSelected,
      segmentDiversity: arm.quality.segmentDiversity,
    });
  }
  const starvedAfterCycles = planned.filter(
    (s) => (cumulativeSegmentHits[s] || 0) === 0,
  );
  return {
    evaluationOnly: true,
    fairnessRatio,
    cycles,
    cumulativeSegmentHits,
    starvedAfterCycles,
    lateSegmentsRecovered: starvedAfterCycles.length === 0,
    cycleResults,
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

  const deduped = dedupeCandidatesByOrganizationEdition(candidates, context);
  const pool = deduped.candidates;

  const first = selectFirstInList(pool, maxCandidates);
  const current = selectCandidatesForDeepResearch(pool, {
    maxCandidates,
    fairMinPerSegment,
    hotelProfile,
    segmentIds: planned,
  });
  const proposed = selectPureScore(pool, maxCandidates, context);

  const option2Arms = {};
  for (const arm of OPTION2_FAIRNESS_COMMERCIAL_ARMS_EVAL) {
    option2Arms[arm.id] = selectBoundedFairnessThenCommercial(pool, {
      maxCandidates,
      fairnessRatio: arm.fairnessRatio,
      hotelProfile,
      cycleOffset: 0,
    });
  }

  const rotation40 = simulateFairSegmentRotationAcrossCycles({
    candidates: pool,
    maxCandidates,
    fairnessRatio: 0.4,
    cycles: 4,
    hotelProfile,
  });

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

  const qualityByPolicy = {
    currentFairFloor: summarizeSelectionQuality(current.selected || [], context),
    pureCommercial: summarizeSelectionQuality(proposed.selected || [], context),
  };
  for (const [id, arm] of Object.entries(option2Arms)) {
    qualityByPolicy[id] = arm.quality;
  }

  return {
    version: GDI_RESEARCH_PRIORITY_EVALUATION_V1,
    evaluationOnly: true,
    productionPolicyChanged: false,
    founderOption2Selected: true,
    productionWeightsUnchanged: true,
    maxCandidates,
    fairMinPerSegment,
    candidateCount: candidates.length,
    afterEditionDedupeCount: pool.length,
    droppedSameEditionDuplicates: deduped.droppedDuplicates.length,
    coverageDistinction: {
      shallowDiscoveryCoverage:
        "Segment presence in discovery inventory / fair rotation hits",
      deepResearchCoverage:
        "Slots allocated under maxCandidates for STAGE 1+ research",
      note: "Low research expenditure is not treated as superior quality without commercial outcome evidence.",
    },
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
        quality: qualityByPolicy.currentFairFloor,
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
        quality: qualityByPolicy.pureCommercial,
        topScores: (proposed.scored || []).slice(0, 12),
      },
      option2BoundedFairnessThenCommercialEval: option2Arms,
    },
    qualityByPolicy,
    fairRotationSimulation: {
      fair40_commercial60: rotation40,
    },
    overlap: {
      currentVsProposed: overlapCurrentProposed,
      firstVsCurrent: [...firstIds].filter((id) => currentIds.has(id)).length,
      firstVsProposed: [...firstIds].filter((id) => proposedIds.has(id)).length,
      option2_40_vs_current: (option2Arms.fair40_commercial60?.selectedIds || []).filter(
        (id) => currentIds.has(id),
      ).length,
      option2_40_vs_pure: (option2Arms.fair40_commercial60?.selectedIds || []).filter(
        (id) => proposedIds.has(id),
      ).length,
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
      "Founder selected Option 2 (bounded fairness then commercial priority). Keep production weights unchanged until this offline evaluation is reviewed. Prefer fair40/commercial60 or fair25/commercial75 so commercial fill can run inside the deep cap; enable multi-cycle segment rotation so late taxonomy segments are not permanently starved. Do not treat low spend as quality.",
  };
}

export default {
  GDI_RESEARCH_PRIORITY_EVALUATION_V1,
  PROPOSED_RESEARCH_PRIORITY_WEIGHTS_EVAL,
  OPTION2_FAIRNESS_COMMERCIAL_ARMS_EVAL,
  resolveCandidateSegmentId,
  inventoryRowToSelectionCandidate,
  scoreProposedResearchPriority,
  opportunityEditionKey,
  dedupeCandidatesByOrganizationEdition,
  selectBoundedFairnessThenCommercial,
  simulateFairSegmentRotationAcrossCycles,
  compareResearchPriorityPolicies,
};
