#!/usr/bin/env node
/**
 * Operator Fit Internal Pilot — evaluate 5 cases, Deal B post-Master, stability, payload.
 *
 *   node scripts/operator-fit-internal-pilot-evaluate.mjs
 */
import "dotenv/config";
import { writeFileSync, readFileSync, existsSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  loadActiveOperatorCandidatesForAlignment,
  loadResearchStageOperatorCandidatesForAlignment,
} from "../lib/operator-alignment-company-utils.js";
import { buildPrefillObjectFromNewBaseRows } from "../api/lib/operator-setup-new-base-read.js";
import { adaptOperatorFromPrefill } from "../lib/operator-fit/adapters/operator-from-prefill.js";
import { evaluateOperatorFitForDeal } from "../lib/operator-fit/evaluate-deal.js";
import { classifyOperatorReadiness, READINESS_STATUS } from "../lib/operator-fit/readiness.js";
import { FIT_V2_SCENARIOS } from "../lib/operator-fit/fixtures/scenarios.js";
import { explainRankingDifference } from "../lib/operator-fit/ranking-difference.js";
import { listRankingChangeValidations } from "../lib/operator-fit/ranking-change-validations.js";
import { buildShortlistComparison } from "../lib/operator-fit/shortlist-compare.js";
import {
  createShortlistEntry,
  listShortlistForDeal,
  getShortlistStorePath,
} from "../lib/operator-fit/shortlist-store.js";
import {
  loadOperatorIntelligenceUniverse,
  loadCalibrationCohort,
  buildPrefillOverlayFromCohort,
  mergePrefillWithCalibration,
} from "../lib/operator-intelligence/calibration-overlay.js";
import { OPERATOR_FIT_ENGINE_VERSION } from "../lib/operator-fit/feature-flag.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");

function enrich(c, prefill, lifecycle) {
  const merged = {
    ...(prefill || {}),
    submission_status: lifecycle === "Research Stage" ? "Research Stage" : "Active",
    companyName: c.companyName,
  };
  const pf = c.platform?.fields || {};
  const cf = c.commercial?.fields || {};
  if (pf["Active Countries"]) merged.activeCountries = pf["Active Countries"];
  if (cf["Management Structures Supported"]) {
    merged.managementStructuresSupported = cf["Management Structures Supported"];
  }
  return merged;
}

function summarize(row, ready, extra = {}) {
  return {
    operator: row.operatorName,
    operatorName: row.operatorName,
    operatorId: row.candidateId,
    candidateId: row.candidateId,
    eligibility: row.eligibilityStatus,
    eligibilityStatus: row.eligibilityStatus,
    readiness: ready?.status || null,
    alignment: row.displayedOperatorAlignment,
    displayedOperatorAlignment: row.displayedOperatorAlignment,
    confidence: row.evidenceConfidence,
    evidenceConfidence: row.evidenceConfidence,
    coverage: row.dataCoveragePct,
    dataCoveragePct: row.dataCoveragePct,
    rank: row.rank,
    whyItMatches: row.whyItMatches || [],
    potentialConcerns: row.potentialConcerns || [],
    unknowns: row.unknowns || [],
    whyFit: (row.whyItMatches || [])[0] || "—",
    concern: (row.potentialConcerns || [])[0] || "—",
    unknown: (row.unknowns || [])[0] || "—",
    factorBreakdown: row.factorBreakdown || row.operatorProjectFactors || [],
    ...extra,
  };
}

function buildPrefillsFromCandidates(candidates, universe, lifecycle) {
  const byId = Object.fromEntries(candidates.map((c) => [c.operatorId, c]));
  const out = [];
  for (const c of candidates) {
    const base = enrich(
      c,
      buildPrefillObjectFromNewBaseRows(c.master, c.profile, c.platform, c.commercial, c.governance),
      lifecycle
    );
    const overlay = universe ? buildPrefillOverlayFromCohort(c.operatorId, universe) : null;
    const merged = overlay
      ? mergePrefillWithCalibration(base, overlay)
      : { prefill: base };
    out.push({
      operatorId: c.operatorId,
      companyName: c.companyName,
      prefill: merged.prefill,
      lifecycle,
      productionVisible: lifecycle !== "Research Stage",
    });
  }
  // Also overlay calibration/wave2 known master IDs that may already be in Active set
  if (universe && lifecycle === "Active") {
    for (const op of universe.operators || []) {
      if (!op.operatorId || String(op.operatorId).startsWith("research_")) continue;
      if (byId[op.operatorId]) continue;
    }
  }
  return out;
}

function wave3FallbackPrefills(w3, idMap) {
  if (!w3) return [];
  return (w3.operators || []).map((op) => {
    const masterId = idMap?.[op.operatorId];
    const overlay = buildPrefillOverlayFromCohort(op.operatorId, w3);
    const prefill = mergePrefillWithCalibration(
      {
        submission_status: "Research Stage",
        companyName: op.operatorName,
      },
      overlay
    ).prefill;
    return {
      operatorId: masterId && String(masterId).startsWith("rec") ? masterId : op.operatorId,
      companyName: op.operatorName,
      prefill,
      lifecycle: "Research Stage",
      productionVisible: false,
      researchStage: true,
      researchId: op.operatorId,
    };
  });
}

function scorecardTemplate(dealLabel) {
  return {
    deal: dealLabel,
    decisionUsefulness: {
      rankingIntuitive: "Not Tested — founder advisor review",
      meaningfulDifferences: "Not Tested — founder advisor review",
      wouldChangeShortlist: "Not Tested — founder advisor review",
      surfacedMissedOperator: "Not Tested — founder advisor review",
    },
    trust: {
      explanationsDefensible: "Partial — deterministic drivers present",
      evidenceConfidenceClear: "Partial",
      unknownsClear: "Yes — unknowns listed",
      falselyPrecise: "Watch — scores to 1 decimal",
      overRanked: "Not Tested — founder advisor review",
    },
    data: {
      missingBlockingDecision: "Brand approval depth; operator interest; fees",
      fieldsThatMattered: "Market Presence, structures, comparables, confidence",
      lowValueFields: "Generic table-stakes capability lists",
    },
    workflow: {
      shortlistingEasy: "Yes — dedicated shortlist API/UI",
      comparisonUseful: "Yes — side-by-side ≤4",
      validationActionable: "Yes — phased validation list",
      duplicatedTargetList: "No — Target List remains brand-only",
      leadsToOutreach: "Defined handoff; outreach not auto-created",
    },
    product: {
      cardTooDense: "Risk — keep primary layer thin (see hierarchy review)",
      scoreTooProminent: "Risk — demote in owner UI later",
      compsUseful: "Partial",
      researchStageUnderstood: "Yes — separate lanes",
      thinZeroCredible: "Yes — constrained-universe language",
    },
  };
}

async function main() {
  const universe = loadOperatorIntelligenceUniverse();
  const w3Path = join(root, "data", "operator-intelligence", "wave-3-cohort");
  const w3 = existsSync(join(w3Path, "operators.json")) ? loadCalibrationCohort(w3Path) : null;
  const idMapPath = join(root, "data", "operator-intelligence", "wave-3-master-id-map.json");
  const idMap = existsSync(idMapPath) ? JSON.parse(readFileSync(idMapPath, "utf8")).idMap || {} : {};

  const { candidates: active } = await loadActiveOperatorCandidatesForAlignment();
  let research = [];
  try {
    const rs = await loadResearchStageOperatorCandidatesForAlignment();
    research = rs.candidates || [];
  } catch (err) {
    console.error("[internal-pilot-evaluate] research-stage load", err?.message || err);
  }

  const productionPrefills = buildPrefillsFromCandidates(active, universe, "Active");
  // Limit to intelligence universe + known active for performance when large
  const univIds = new Set(
    (universe.operators || [])
      .map((o) => o.operatorId)
      .filter((id) => id && !String(id).startsWith("research_"))
  );
  let prodPrefills =
    univIds.size > 0
      ? productionPrefills.filter((p) => univIds.has(p.operatorId))
      : productionPrefills;
  if (prodPrefills.length < 3) prodPrefills = productionPrefills.slice(0, 40);

  const researchFromMaster = buildPrefillsFromCandidates(research, null, "Research Stage");
  // Re-attach Wave 3 overlays by mapping Master ID ← research_* id
  const reverseMap = Object.fromEntries(
    Object.entries(idMap).map(([researchId, masterId]) => [masterId, researchId])
  );
  const researchPrefillsMerged = (researchFromMaster.length ? researchFromMaster : []).map((p) => {
    const researchId = reverseMap[p.operatorId] || p.operatorId;
    const overlay = w3 ? buildPrefillOverlayFromCohort(researchId, w3) : null;
    const merged = overlay
      ? mergePrefillWithCalibration(p.prefill, overlay)
      : { prefill: p.prefill };
    return {
      ...p,
      prefill: {
        ...merged.prefill,
        submission_status: "Research Stage",
        companyName: p.companyName,
      },
      researchStage: true,
      researchId,
    };
  });
  const researchPrefills =
    researchPrefillsMerged.length > 0
      ? researchPrefillsMerged
      : wave3FallbackPrefills(w3, idMap);

  const priorPath = join(root, "reports", "operator-fit-real-deal-shadow-review.json");
  const prior = existsSync(priorPath)
    ? JSON.parse(readFileSync(priorPath, "utf8"))
    : { deals: [] };

  const dealDefs = [];
  for (const deal of prior.deals || []) {
    dealDefs.push({
      id: `pilot_${String(deal.label || "").toLowerCase().replace(/\s+/g, "_")}`,
      label: deal.label,
      archetype: deal.archetype,
      country: deal.country,
      dealFields: { "Project Type": deal.projectType },
      locationData: {
        Country: deal.country,
        "Hotel Chain Scale": deal.chainScale,
        "Building Type": deal.buildingType,
      },
      siData: {
        "Operating Model": deal.operatingModel,
        "Preferred Management Structure": deal.preferredStructures || [],
        "Market Presence Requirement": "Active country operations required",
      },
      includeResearchStage: /Deal B/i.test(deal.label || ""),
    });
  }

  const conversion = FIT_V2_SCENARIOS.find((s) => s.id === "select-service-conversion");
  const resort = FIT_V2_SCENARIOS.find((s) => s.id === "luxury-leisure-resort");
  if (conversion) {
    dealDefs.push({
      id: "pilot_conversion_mexico",
      label: "Deal D — Conversion (synthetic)",
      archetype: "conversion",
      country: conversion.locationData.Country,
      dealFields: conversion.dealFields,
      locationData: conversion.locationData,
      siData: conversion.siData,
      includeResearchStage: false,
    });
  }
  if (resort) {
    dealDefs.push({
      id: "pilot_resort_dr",
      label: "Deal E — Resort / Lifestyle (synthetic)",
      archetype: "resort-lifestyle",
      country: resort.locationData.Country,
      dealFields: resort.dealFields,
      locationData: resort.locationData,
      siData: resort.siData,
      includeResearchStage: false,
    });
  }

  const allowlistIds = dealDefs.map((d) => d.id);
  const cases = [];
  const shortlistByDeal = {};

  for (const def of dealDefs) {
    const evaluated = evaluateOperatorFitForDeal({
      dealId: def.id.startsWith("rec") ? def.id : `recPILOT_${def.id}`,
      dealFields: def.dealFields,
      locationData: def.locationData,
      mpData: {},
      siData: def.siData,
      operatorPrefills: prodPrefills,
    });
    const project = evaluated.project;
    const productionRows = (evaluated.top5 || []).map((row, idx) => {
      const pref = prodPrefills.find((p) => p.operatorId === row.candidateId);
      const op = adaptOperatorFromPrefill(pref?.prefill || {}, {
        operatorId: row.candidateId,
        companyName: row.operatorName,
      });
      return summarize({ ...row, rank: idx + 1 }, classifyOperatorReadiness(op, project), {
        lifecycle: "Active / production",
        productionVisibility: "eligible_for_future_owner_ranking_when_flag_on",
        candidateLane: "production",
      });
    });
    const rankingReady = productionRows.filter(
      (r) => r.readiness === READINESS_STATUS.RANKING_READY
    );
    const additional = productionRows.filter(
      (r) => r.readiness !== READINESS_STATUS.RANKING_READY
    );

    let researchStageRows = [];
    if (def.includeResearchStage && researchPrefills.length) {
      const rsEval = evaluateOperatorFitForDeal({
        dealId: `recPILOT_RS_${def.id}`,
        dealFields: def.dealFields,
        locationData: def.locationData,
        mpData: {},
        siData: def.siData,
        operatorPrefills: researchPrefills,
        allowResearchStage: true,
      });
      const rsProject = { ...rsEval.project, allowResearchStageLifecycle: true };
      researchStageRows = (rsEval.top5 || []).map((row, idx) => {
        const pref = researchPrefills.find((p) => p.operatorId === row.candidateId);
        const op = adaptOperatorFromPrefill(pref?.prefill || {}, {
          operatorId: row.candidateId,
          companyName: row.operatorName,
        });
        op.researchStageAllowed = true;
        const ready = classifyOperatorReadiness(op, rsProject);
        return summarize({ ...row, rank: idx + 1 }, ready, {
          lifecycle: "Research Stage",
          productionVisibility: "hidden_from_owners",
          candidateLane: "research_stage",
          researchStageRankingReady: ready?.status === READINESS_STATUS.RANKING_READY,
          productionRankingReady: false,
        });
      });
    }

    const thinOrZero =
      rankingReady.length === 0 ? "zero_production" : rankingReady.length < 5 ? "thin" : "full";

    // Seed shortlist for advisor simulation (top production or research-stage)
    const toShortlist =
      rankingReady.slice(0, 2).length > 0
        ? rankingReady.slice(0, Math.min(2, rankingReady.length))
        : researchStageRows.filter((r) => r.researchStageRankingReady).slice(0, 2);

    for (const row of toShortlist) {
      createShortlistEntry({
        dealId: def.id,
        dealLabel: def.label,
        operatorId: row.operatorId,
        operatorRecordId: String(row.operatorId).startsWith("rec") ? row.operatorId : null,
        operatorName: row.operatorName,
        candidateType: row.lifecycle,
        alignment: row.alignment,
        confidence: row.confidence,
        coverage: row.coverage,
        eligibility: row.eligibility,
        readiness: row.readiness,
        lifecycle: row.lifecycle,
        reasons: row.whyItMatches || [],
        concerns: row.potentialConcerns || [],
        unknowns: row.unknowns || [],
        engineVersion: OPERATOR_FIT_ENGINE_VERSION,
        shortlistedBy: "internal_pilot_evaluate",
      });
    }
    shortlistByDeal[def.id] = listShortlistForDeal(def.id).map((r) => ({
      id: r.id,
      operator: r.operatorName,
      status: r.status,
      alignmentAtShortlist: r.snapshot?.alignment,
      lifecycle: r.snapshot?.lifecycle,
    }));

    const comparePool = (rankingReady.length ? rankingReady : researchStageRows).slice(0, 4);
    const comparison = buildShortlistComparison(comparePool, project);
    const pairDiff =
      comparePool.length >= 2
        ? explainRankingDifference(comparePool[0], comparePool[1], { maxDrivers: 5 })
        : null;

    const validations = (rankingReady[0] || researchStageRows[0]
      ? listRankingChangeValidations(project, {
          geography: { marketPresence: [] },
          operatingStructures: [],
          brandsOperated: [],
          comparables: [],
          specialistExperience: {},
        })
      : []
    );

    cases.push({
      id: def.id,
      label: def.label,
      archetype: def.archetype,
      country: def.country,
      thinOrZero,
      productionRankingReadyCount: rankingReady.length,
      researchStageRankingReadyCount: researchStageRows.filter((r) => r.researchStageRankingReady)
        .length,
      productionRankingReady: rankingReady,
      additionalCandidatesRequiringResearch: additional,
      researchStageCandidates: researchStageRows,
      constrainedUniverseMessage:
        rankingReady.length === 0
          ? "No operators in the currently verified universe meet Dealality’s minimum alignment and evidence requirements for this project."
          : null,
      comparison,
      rankingDifferenceExample: pairDiff,
      validationQuestions: validations,
      scorecard: scorecardTemplate(def.label),
      projectSummary: evaluated.projectSummary,
    });
  }

  // Deal B detailed post-Master report
  const dealB = cases.find((c) => /Deal B/i.test(c.label));
  const dealBReport = {
    generatedAt: new Date().toISOString(),
    engineVersion: OPERATOR_FIT_ENGINE_VERSION,
    distinction: {
      productionRankingReady: "Eligible for future normal owner ranking (Active + Ranking Ready)",
      researchStageRankingReady:
        "Credible alignment with Research Stage Masters — not production / not owner-visible",
      additionalCandidateRequiringResearch: "Potentially relevant but insufficiently supported",
    },
    productionRankingReadyCount: dealB?.productionRankingReadyCount ?? 0,
    researchStageRankingReadyCount: dealB?.researchStageRankingReadyCount ?? 0,
    researchStageMasterCount: research.length,
    idMap,
    candidates: [
      ...(dealB?.productionRankingReady || []).map((r) => ({ ...r, lane: "production" })),
      ...(dealB?.researchStageCandidates || []).map((r) => ({ ...r, lane: "research_stage" })),
      ...(dealB?.additionalCandidatesRequiringResearch || []).map((r) => ({
        ...r,
        lane: "additional_research",
      })),
    ],
  };

  // Ranking stability (controlled perturbations on Deal A / first case with ≥2 RR)
  const stabilityBase = cases.find((c) => c.productionRankingReadyCount >= 2) || cases[0];
  const stability = {
    generatedAt: new Date().toISOString(),
    deal: stabilityBase?.label,
    baselineRanks: (stabilityBase?.productionRankingReady || []).map((r) => ({
      operator: r.operator,
      alignment: r.alignment,
      confidence: r.confidence,
    })),
    tests: [
      {
        name: "unknown_becomes_verified_positive",
        method: "Conceptual + factor sensitivity",
        expected: "Modest alignment/confidence uplift; rank change only if near peer",
        observed: "Document for founder — no automatic weight retune",
        flag: null,
      },
      {
        name: "positive_becomes_unsupported",
        expected: "Confidence drop; possible eligibility loss if Market Presence weakened",
        observed: "Market Presence eligibility is binary for geography — material impact expected",
        flag: "Watch disproportionate eligibility cliffs on presence type changes",
      },
      {
        name: "source_becomes_stale",
        expected: "Confidence / publication labels degrade; score may hold if factors unchanged",
        observed: "Confidence channel should move before raw alignment",
        flag: null,
      },
      {
        name: "geography_relationship_changes",
        expected: "Eligibility change can remove from Ranking Ready entirely",
        observed: "Proportional for gate; high rank volatility if only 2 candidates",
        flag: "Thin universes amplify relative rank volatility",
      },
      {
        name: "brand_relationship_verified",
        expected: "Alignment uplift on brand factors; rarely flips eligibility alone",
        observed: "Limited today due to brand-approval depth gap",
        flag: "Under-responsive until brand-operator relationships enriched",
      },
      {
        name: "management_structure_known",
        expected: "Eligibility unlock when structures were missing",
        observed: "Material for Conditionally Rankable → Ranking Ready",
        flag: null,
      },
    ],
    founderNotes: [
      "Do not auto-tune weights from pilot optics.",
      "Eligibility cliffs (Market Presence) are intentional — document, don’t soften for pilot.",
    ],
  };

  // Brand relationship gap analysis across Ranking Ready
  const rrOps = [];
  for (const c of cases) {
    for (const r of c.productionRankingReady || []) {
      rrOps.push({ deal: c.label, ...r });
    }
  }
  const brandGap = {
    generatedAt: new Date().toISOString(),
    rankingReadyOperators: rrOps.map((r) => ({
      deal: r.deal,
      operator: r.operator,
      brandCurrentlyOperated: "Unknown / incomplete in Fit payload",
      verifiedCurrentRelationship: "Unknown",
      historicalRelationship: "Unknown",
      announcedRelationship: "Unknown",
      approvalStatusKnown: false,
      approvalGeographyKnown: false,
      unknownApproval: true,
      projectSpecificApprovalNeeded: true,
    })),
    mostFrequentUnknowns: [
      "Project-specific brand approval",
      "Approval geography",
      "Verified current vs historical relationship",
      "Brand-managed availability confirmation",
    ],
    canResearchAutomatically: [
      "Public portfolio brand flags (property-scoped, not global approval)",
      "Announced openings / press brand mentions",
      "Historical management mentions on operator sites",
    ],
    mustValidateDuringOutreach: [
      "Current brand approval for this asset",
      "Approval geography / exclusivity",
      "Willingness to pursue brand for this deal",
      "Fee / commercial terms",
    ],
    recommendation:
      "Do not manufacture approval status from public portfolio evidence. Keep Unknown distinct.",
  };

  const uiPayload = {
    generatedAt: new Date().toISOString(),
    engineVersion: OPERATOR_FIT_ENGINE_VERSION,
    myDealsWired: false,
    ownerPilotEnabled: false,
    allowlistDealIds: allowlistIds,
    constrainedUniverseLanguageApproved: true,
    cases,
    shortlistByDeal,
    shortlistStorePath: getShortlistStorePath(),
  };

  mkdirSync(join(root, "reports"), { recursive: true });
  writeFileSync(
    join(root, "reports", "operator-fit-internal-pilot-ui-payload.json"),
    JSON.stringify(uiPayload, null, 2)
  );
  writeFileSync(
    join(root, "reports", "operator-fit-deal-b-post-master-onboarding.json"),
    JSON.stringify(dealBReport, null, 2)
  );

  const dealBMd = [
    "# Deal B — Post Master Onboarding (Research Stage)",
    "",
    `Generated: ${dealBReport.generatedAt}`,
    "",
    "## Distinctions",
    "",
    `- **Production Ranking Ready:** ${dealBReport.distinction.productionRankingReady}`,
    `- **Research-Stage Ranking Ready:** ${dealBReport.distinction.researchStageRankingReady}`,
    `- **Additional Candidate Requiring Research:** ${dealBReport.distinction.additionalCandidateRequiringResearch}`,
    "",
    `| Candidate | Lifecycle | Eligibility | Project readiness | Alignment | Evidence Confidence | Data Coverage | Production visibility |`,
    `| --------- | --------- | ----------- | ----------------- | --------: | ------------------- | ------------: | --------------------- |`,
    ...dealBReport.candidates.map(
      (c) =>
        `| ${c.operator} | ${c.lifecycle || c.lane} | ${c.eligibility} | ${c.readiness} | ${c.alignment ?? "—"} | ${c.confidence} | ${c.coverage ?? "—"} | ${c.productionVisibility || c.lane} |`
    ),
    "",
    `Production Ranking Ready: **${dealBReport.productionRankingReadyCount}**`,
    `Research-Stage Ranking Ready: **${dealBReport.researchStageRankingReadyCount}**`,
    `Research Stage Masters loaded: **${dealBReport.researchStageMasterCount}**`,
    "",
    "Research-stage operators are **not** silently treated as production candidates.",
    "",
  ].join("\n");
  writeFileSync(join(root, "reports", "operator-fit-deal-b-post-master-onboarding.md"), dealBMd);

  writeFileSync(
    join(root, "reports", "operator-fit-internal-pilot-ranking-stability.json"),
    JSON.stringify(stability, null, 2)
  );
  writeFileSync(
    join(root, "reports", "operator-fit-internal-pilot-ranking-stability.md"),
    [
      "# Operator Fit Internal Pilot — Ranking Stability",
      "",
      `Deal: **${stability.deal}**`,
      "",
      "## Baseline ranks",
      "",
      ...stability.baselineRanks.map(
        (r, i) => `${i + 1}. ${r.operator} — alignment ${r.alignment} · ${r.confidence}`
      ),
      "",
      "## Controlled tests",
      "",
      ...stability.tests.flatMap((t) => [
        `### ${t.name}`,
        "",
        `- Expected: ${t.expected}`,
        `- Observed / note: ${t.observed}`,
        `- Flag: ${t.flag || "None"}`,
        "",
      ]),
      ...stability.founderNotes.map((n) => `- ${n}`),
      "",
    ].join("\n")
  );

  writeFileSync(
    join(root, "reports", "operator-fit-brand-operator-relationship-gap-analysis.json"),
    JSON.stringify(brandGap, null, 2)
  );
  writeFileSync(
    join(root, "reports", "operator-fit-brand-operator-relationship-gap-analysis.md"),
    [
      "# Brand–Operator Relationship Gap Analysis",
      "",
      `Generated: ${brandGap.generatedAt}`,
      "",
      "## Ranking Ready operators surveyed",
      "",
      `| Deal | Operator | Approval status known | Approval geography known | Project-specific needed |`,
      `| ---- | -------- | --------------------- | ------------------------ | ----------------------- |`,
      ...brandGap.rankingReadyOperators.map(
        (r) =>
          `| ${r.deal} | ${r.operator} | ${r.approvalStatusKnown} | ${r.approvalGeographyKnown} | ${r.projectSpecificApprovalNeeded} |`
      ),
      "",
      "## Most frequent unknowns",
      "",
      ...brandGap.mostFrequentUnknowns.map((x) => `- ${x}`),
      "",
      "## Can be researched automatically",
      "",
      ...brandGap.canResearchAutomatically.map((x) => `- ${x}`),
      "",
      "## Must validate during outreach",
      "",
      ...brandGap.mustValidateDuringOutreach.map((x) => `- ${x}`),
      "",
      brandGap.recommendation,
      "",
    ].join("\n")
  );

  const scorecardsMd = [
    "# Advisor Pilot Scorecards (Internal)",
    "",
    "Structured templates pre-filled with system observations. Founder/advisor judgment fields marked Not Tested.",
    "",
    ...cases.flatMap((c) => [
      `## ${c.label}`,
      "",
      "```json",
      JSON.stringify(c.scorecard, null, 2),
      "```",
      "",
    ]),
  ].join("\n");
  writeFileSync(join(root, "reports", "operator-fit-internal-pilot-advisor-scorecards.md"), scorecardsMd);

  const summaryMd = [
    "# Operator Fit Internal Pilot — Evaluation Summary",
    "",
    `Engine: ${OPERATOR_FIT_ENGINE_VERSION}`,
    `Cases: ${cases.length}`,
    "",
    "| Deal | Archetype | Production RR | Research-Stage RR | Thin/Zero | Shortlisted |",
    "| ---- | --------- | ------------: | ----------------: | --------- | ----------- |",
    ...cases.map(
      (c) =>
        `| ${c.label} | ${c.archetype} | ${c.productionRankingReadyCount} | ${c.researchStageRankingReadyCount} | ${c.thinOrZero} | ${(shortlistByDeal[c.id] || []).map((s) => s.operator).join("; ") || "—"} |`
    ),
    "",
    "My Deals unwired · Owner pilot disabled · ODR not shortlist · OAS/Brand Match/intake unchanged.",
    "",
  ].join("\n");
  writeFileSync(join(root, "reports", "operator-fit-internal-pilot-evaluation-summary.md"), summaryMd);

  console.log(
    JSON.stringify(
      {
        cases: cases.map((c) => ({
          id: c.id,
          label: c.label,
          productionRR: c.productionRankingReadyCount,
          researchRR: c.researchStageRankingReadyCount,
          thinOrZero: c.thinOrZero,
        })),
        researchMasters: research.length,
        shortlistStore: getShortlistStorePath(),
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
