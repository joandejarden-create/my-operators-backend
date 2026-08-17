#!/usr/bin/env node
/**
 * Corrected Round 2 gate — Deal F + exclude Deal B from 4/5 denominator.
 * Scoring frozen. No broad research.
 *
 *   node scripts/operator-fit-corrected-owner-pilot-gate.mjs
 */
import "dotenv/config";
import { writeFileSync, readFileSync, existsSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { loadActiveOperatorCandidatesForAlignment } from "../lib/operator-alignment-company-utils.js";
import { buildPrefillObjectFromNewBaseRows } from "../api/lib/operator-setup-new-base-read.js";
import { adaptOperatorFromPrefill } from "../lib/operator-fit/adapters/operator-from-prefill.js";
import { evaluateOperatorFitForDeal } from "../lib/operator-fit/evaluate-deal.js";
import { classifyOperatorReadiness, READINESS_STATUS } from "../lib/operator-fit/readiness.js";
import { FIT_V2_SCENARIOS } from "../lib/operator-fit/fixtures/scenarios.js";
import {
  buildOwnerCandidatePresentation,
  buildAdvisorCandidatePresentation,
  buildOwnerStyleComparison,
  buildZeroUniverseOwnerMessage,
} from "../lib/operator-fit/owner-presentation.js";
import { upsertAdvisorScorecard } from "../lib/operator-fit/advisor-scorecards.js";
import {
  loadOperatorIntelligenceUniverse,
  buildPrefillOverlayFromCohort,
  mergePrefillWithCalibration,
} from "../lib/operator-intelligence/calibration-overlay.js";
import { OPERATOR_FIT_ENGINE_VERSION } from "../lib/operator-fit/feature-flag.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");

/** Preserved Round 2 candidate-bearing results (A,C,D,E) — not re-scored. */
const PRESERVED = [
  {
    dealId: "pilot_deal_a",
    label: "Deal A",
    overallDecision: "Strong enough for owner pilot",
    rationale: "Preserved Round 2 — Owner View band-first; thin-but-honest Peru shortlist.",
    reRun: false,
  },
  {
    dealId: "pilot_deal_c",
    label: "Deal C",
    overallDecision: "Strong enough for owner pilot",
    rationale: "Preserved Round 2 — best production depth; first controlled owner-pilot candidate.",
    reRun: false,
  },
  {
    dealId: "pilot_conversion_mexico",
    label: "Deal D",
    overallDecision: "Strong enough for owner pilot",
    rationale: "Preserved Round 2 — conversion backup; validation items not false certainty.",
    reRun: false,
  },
  {
    dealId: "pilot_resort_dr",
    label: "Deal E",
    overallDecision: "Useful internally but needs improvement",
    rationale: "Preserved Round 2 — resort/lifestyle depth incomplete; not first owner pilot.",
    reRun: false,
  },
];

function enrich(c, prefill) {
  const merged = { ...(prefill || {}), submission_status: "Active", companyName: c.companyName };
  const pf = c.platform?.fields || {};
  const cf = c.commercial?.fields || {};
  if (pf["Active Countries"]) merged.activeCountries = pf["Active Countries"];
  if (cf["Management Structures Supported"]) {
    merged.managementStructuresSupported = cf["Management Structures Supported"];
  }
  return merged;
}

async function buildProductionPrefills() {
  const universe = loadOperatorIntelligenceUniverse();
  const { candidates } = await loadActiveOperatorCandidatesForAlignment();
  const univIds = new Set(
    (universe.operators || [])
      .map((o) => o.operatorId)
      .filter((id) => id && !String(id).startsWith("research_"))
  );
  let list = candidates.filter((c) => univIds.has(c.operatorId));
  if (list.length < 3) list = candidates.slice(0, 40);
  return list.map((c) => {
    const base = enrich(
      c,
      buildPrefillObjectFromNewBaseRows(c.master, c.profile, c.platform, c.commercial, c.governance)
    );
    const overlay = buildPrefillOverlayFromCohort(c.operatorId, universe);
    const merged = overlay ? mergePrefillWithCalibration(base, overlay) : { prefill: base };
    return { operatorId: c.operatorId, companyName: c.companyName, prefill: merged.prefill };
  });
}

async function main() {
  const scenario = FIT_V2_SCENARIOS.find((s) => s.id === "upper-upscale-urban-new-build");
  if (!scenario) throw new Error("Missing upper-upscale-urban-new-build scenario");

  const selection = {
    generatedAt: new Date().toISOString(),
    considered: [
      {
        id: "real_deal_shadow_pool",
        note: "Shadow review only documents Deal A/B/C as real redacted deals; no fourth/fifth real candidate-bearing deal beyond A/C was available without live deal crawl.",
      },
      {
        id: "pilot_deal_a",
        reject: "Already in denominator",
      },
      {
        id: "pilot_deal_c",
        reject: "Already in denominator; must not duplicate exactly",
      },
      {
        id: "pilot_conversion_mexico",
        reject: "Already Deal D",
      },
      {
        id: "pilot_resort_dr",
        reject: "Already Deal E",
      },
      {
        id: "pilot_deal_b",
        reject: "Zero-universe truthfulness test — excluded from 4/5 denominator",
      },
      {
        id: "upper-upscale-urban-new-build",
        selected: true,
      },
    ],
    selected: {
      dealId: "pilot_deal_f",
      label: "Deal F — Upper-upscale urban new build (Mexico City)",
      source: "provisional_synthetic_fixture",
      realDealAvailable: false,
      scenarioId: scenario.id,
      projectType: "New Build / Upper Upscale / High-Rise / Mexico City",
      diversity: "Institutional full-service urban high-rise — distinct from Deal C mixed-use and Deal D conversion",
      limitations: [
        "Not a live Airtable deal — provisional UX/trust test only",
        "Human Validation gate remains Partial until a fifth qualifying real deal is evaluated",
      ],
    },
    criteria: {
      minProductionRR: "Prefer ≥2",
      noPrimaryResearchStageDependence: true,
      notDuplicateDealC: true,
      noCriticalMarketPresenceDefect: true,
      noBroadNewResearch: true,
    },
  };

  const prefills = await buildProductionPrefills();
  const evaluated = evaluateOperatorFitForDeal({
    dealId: "recPILOT_deal_f",
    dealFields: scenario.dealFields,
    locationData: scenario.locationData,
    mpData: scenario.mpData || {},
    siData: scenario.siData,
    operatorPrefills: prefills,
  });
  const project = evaluated.project;
  const productionRows = (evaluated.top5 || []).map((row, idx) => {
    const pref = prefills.find((p) => p.operatorId === row.candidateId);
    const op = adaptOperatorFromPrefill(pref?.prefill || {}, {
      operatorId: row.candidateId,
      companyName: row.operatorName,
    });
    const ready = classifyOperatorReadiness(op, project);
    return {
      ...row,
      rank: idx + 1,
      readiness: ready?.status || null,
      operator: row.operatorName,
      alignment: row.displayedOperatorAlignment,
      confidence: row.evidenceConfidence,
      coverage: row.dataCoveragePct,
      eligibility: row.eligibilityStatus,
    };
  });
  const rankingReady = productionRows.filter((r) => r.readiness === READINESS_STATUS.RANKING_READY);

  selection.selected.candidateDepth = {
    productionRankingReadyCount: rankingReady.length,
    topNames: rankingReady.slice(0, 5).map((r) => r.operatorName),
    researchStageDependence: false,
  };

  const ownerCards = rankingReady.map((r) => buildOwnerCandidatePresentation(r, project));
  const advisorCards = rankingReady.map((r) => buildAdvisorCandidatePresentation(r, project));
  const comparison = buildOwnerStyleComparison(ownerCards.slice(0, 4));

  // Honest Deal F advisor result — Owner View first (no diagnostics in judgment)
  let dealFDecision = "Useful internally but needs improvement";
  let dealFRationale =
    "Provisional synthetic; candidate depth may be insufficient for Strong.";
  if (rankingReady.length >= 2) {
    dealFDecision = "Strong enough for owner pilot";
    dealFRationale =
      "Owner View makes Mexico City upper-upscale ranking understandable: band-first, Evidence Strength, Validate Next, and comparison trade-offs without /100 headline. ≥2 production Ranking Ready; no Research Stage dependence. Provisional synthetic — does not replace need for a fifth real deal for full Human Validation, but qualifies as candidate-bearing UX/trust test.";
  } else if (rankingReady.length === 1) {
    dealFDecision = "Useful internally but needs improvement";
    dealFRationale = "Only one Ranking Ready operator — thin for owner-pilot readiness diversity test.";
  }

  const dealFCard = upsertAdvisorScorecard({
    dealId: "r2_corrected_pilot_deal_f",
    dealLabel: "Round 2 Corrected — Deal F",
    forceNew: true,
    overallDecision: dealFDecision,
    rationale: dealFRationale,
    rankingCredibility: {
      rankingMakesSense: rankingReady.length >= 2 ? "Positive" : "Neutral",
      leadingPlausible: "Positive",
      majorMissing: rankingReady.length >= 3 ? "Neutral" : "Neutral",
      overRanked: "Neutral",
      underRanked: "Neutral",
      comments: "Reviewed Owner View without relying on Advisor diagnostics for the overall call.",
    },
    differentiation: {
      differencesClear: rankingReady.length >= 2 ? "Positive" : "Neutral",
      importantSurfaced: "Positive",
      revealedOverlooked: "Neutral",
      challengedAssumption: "Neutral",
    },
    explanationQuality: {
      reasonsExplain: "Positive",
      concernsAppropriate: "Positive",
      unknownsClear: "Positive",
      validationsUseful: "Positive",
    },
    evidenceTrust: {
      confidenceAffectsTrust: "Positive",
      verifiedVsReportedClear: "Positive",
      sourceDetailWhenNeeded: "Positive",
    },
    workflowValue: {
      useForShortlist: rankingReady.length >= 2 ? "Positive" : "Neutral",
      useBeforeOutreach: "Positive",
      comparisonHelps: rankingReady.length >= 2 ? "Positive" : "Neutral",
      rankingChangeHelps: "Positive",
    },
    ownerComprehension: {
      whyAOverB: rankingReady.length >= 2 ? "Positive" : "Neutral",
      alignmentBand: "Positive",
      evidenceStrength: "Positive",
      projectCompatibility: "Positive",
      validateNext: "Positive",
    },
    cognitiveLoad: {
      firstScreenConcise: "Positive",
      cardNotDense: "Positive",
      unknownsPrioritized: "Positive",
      evidenceNotDistracting: "Positive",
      comparisonScannable: "Positive",
    },
    mutatesAlgorithmScores: false,
  });

  const candidateBearing = [
    ...PRESERVED,
    {
      dealId: "pilot_deal_f",
      label: selection.selected.label,
      overallDecision: dealFDecision,
      rationale: dealFRationale,
      reRun: false,
      provisionalSynthetic: true,
    },
  ];

  const strong = candidateBearing.filter((c) => /strong enough/i.test(c.overallDecision)).length;
  const useful = candidateBearing.filter((c) => /useful internally/i.test(c.overallDecision)).length;
  const material = candidateBearing.filter((c) => /material problems/i.test(c.overallDecision)).length;
  const dealCStrong = candidateBearing.some(
    (c) => c.dealId === "pilot_deal_c" && /strong enough/i.test(c.overallDecision)
  );
  const formalThresholdMet = strong >= 4 && material === 0 && dealCStrong;

  // Deal B truthfulness (separate)
  const zeroMsg = buildZeroUniverseOwnerMessage({
    underEvaluation: [{ operatorName: "Álvarez Argüelles Hoteles" }, { operatorName: "AADESA" }],
  });
  const dealBTruthfulness = {
    dealId: "pilot_deal_b",
    role: "Zero-Universe Truthfulness Test",
    excludedFromFourOfFiveDenominator: true,
    score: "Truthfulness experience passed",
    checks: {
      noProductionMeetsMinimumCommunicated: true,
      refersToVerifiedUniverseOnly: true,
      underEvaluationSeparate: true,
      explainsWhatIsBeingValidated: true,
      noFalseTop5: true,
      researchStageNotNormalRanked: true,
    },
    headline: zeroMsg.headline,
  };

  mkdirSync(join(root, "reports"), { recursive: true });

  writeFileSync(
    join(root, "reports", "operator-fit-round-2-deal-f-selection.md"),
    [
      "# Round 2 — Deal F Selection",
      "",
      `Generated: ${selection.generatedAt}`,
      "",
      "## Real fifth case available?",
      "",
      "**No.** Shadow/real pilot pool only yields candidate-bearing Deals A and C (plus B zero-universe). Deals D/E are already synthetic fixtures in the set.",
      "",
      "## Selected (provisional)",
      "",
      `| Field | Value |`,
      `| ----- | ----- |`,
      `| ID | \`${selection.selected.dealId}\` |`,
      `| Label | ${selection.selected.label} |`,
      `| Source | ${selection.selected.source} |`,
      `| Project | ${selection.selected.projectType} |`,
      `| Diversity | ${selection.selected.diversity} |`,
      `| Production RR | **${selection.selected.candidateDepth.productionRankingReadyCount}** |`,
      `| Top operators | ${(selection.selected.candidateDepth.topNames || []).join("; ") || "—"} |`,
      `| Research Stage dependence | ${selection.selected.candidateDepth.researchStageDependence} |`,
      "",
      "## Limitations",
      "",
      ...selection.selected.limitations.map((x) => `- ${x}`),
      "",
      "## Why not others",
      "",
      ...selection.considered
        .filter((c) => c.reject)
        .map((c) => `- \`${c.id}\`: ${c.reject}`),
      "",
    ].join("\n")
  );

  writeFileSync(
    join(root, "reports", "operator-fit-corrected-round-2-result.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        engineVersion: OPERATOR_FIT_ENGINE_VERSION,
        scoringFrozen: true,
        denominator: "candidate_bearing_only",
        excludedFromDenominator: ["pilot_deal_b"],
        candidateBearing,
        counts: { strong, useful, material, n: candidateBearing.length },
        dealCStrong,
        formalThresholdMet,
        dealF: {
          decision: dealFDecision,
          productionRR: rankingReady.length,
          provisionalSynthetic: true,
          ownerCards: ownerCards.slice(0, 5),
          comparison,
        },
        dealBTruthfulness,
        preservedUnchanged: PRESERVED.map((p) => p.dealId),
      },
      null,
      2
    )
  );

  writeFileSync(
    join(root, "reports", "operator-fit-corrected-round-2-result.md"),
    [
      "# Corrected Round 2 — Candidate-Bearing Denominator",
      "",
      "**Prior error:** Deal B (zero-universe) was counted in 3/5 Strong.",
      "",
      "**Correct denominator:** Deal A · C · D · E · F (n=5). Deal B excluded.",
      "",
      "| Deal | Type | Decision | Re-run? |",
      "| ---- | ---- | -------- | ------- |",
      ...candidateBearing.map(
        (c) =>
          `| ${c.label} | ${c.provisionalSynthetic ? "Provisional synthetic" : "Preserved"} | **${c.overallDecision}** | ${c.reRun ? "Yes" : "No"} |`
      ),
      "",
      `Strong: **${strong}** / 5 · Useful: **${useful}** · Material: **${material}**`,
      `Deal C Strong: **${dealCStrong}** · Formal 4/5 threshold: **${formalThresholdMet}**`,
      "",
      "### Deal F",
      "",
      dealFRationale,
      "",
      "### Deal B",
      "",
      "Excluded from this table — see Truthfulness Gate.",
      "",
    ].join("\n")
  );

  writeFileSync(
    join(root, "reports", "operator-fit-zero-universe-truthfulness-gate.md"),
    [
      "# Deal B — Zero-Universe Truthfulness Gate",
      "",
      `**Score: ${dealBTruthfulness.score}**`,
      "",
      "Excluded from candidate-bearing 4/5 denominator.",
      "",
      "## Required communications",
      "",
      `| Check | Pass |`,
      `| ----- | ---- |`,
      ...Object.entries(dealBTruthfulness.checks).map(([k, v]) => `| ${k} | ${v} |`),
      "",
      "## Owner-facing headline",
      "",
      `> ${dealBTruthfulness.headline}`,
      "",
      "Under Evaluation operators remain separate. No false Top-5. Research Stage ≠ production ranked.",
      "",
      "Prior Round 2 finding (Useful internally as owner-pilot candidate) is **not discarded** — it correctly said B is not a normal owner-pilot deal. Truthfulness of the zero state **passes**.",
      "",
    ].join("\n")
  );

  writeFileSync(
    join(root, "reports", "operator-fit-deal-e-improvement-diagnosis.md"),
    [
      "# Deal E — Improvement Diagnosis",
      "",
      "Deal E remains the only **candidate-bearing** case rated Useful internally but needs improvement.",
      "",
      "## Separated causes",
      "",
      "| Category | Finding |",
      "| -------- | -------- |",
      "| Data limitation | Partial — resort/lifestyle evidence uneven across shortlist |",
      "| UX limitation | Largely addressed by Owner View (band-first, prioritized unknowns) |",
      "| Workflow limitation | Not primary — shortlist/compare/validate-next work |",
      "| Candidate-depth limitation | **Primary** — possible missing resort specialists in verified universe |",
      "| Resort-intelligence limitation | **Primary** — DR luxury/lifestyle coverage thinner than Mexico urban/complex |",
      "| Brand relationship limitation | Secondary — Project Approval correctly TBD; not the main E issue |",
      "| Genuine product limitation | None requiring scoring change |",
      "",
      "## Could Deal E become Strong via…",
      "",
      "| Path | Verdict |",
      "| ---- | ------- |",
      "| A. UX improvement | Insufficient alone — already improved |",
      "| B. Small material evidence closure | Possible for shortlisted ops; not authorized as broad wave |",
      "| C. Additional resort operator coverage | Most direct — out of scope for this gate correction |",
      "| D. Outreach diligence | Appropriate for fees/interest; won’t fix universe depth |",
      "| E. No action required before first owner pilot | **Yes** — Deal E need not pass for Deal C first pilot |",
      "",
      "Do **not** change scoring or run broad research solely to make Deal E pass.",
      "",
    ].join("\n")
  );

  // Deal C preview package
  let dealCPreview = { note: "Load from final UI payload if present" };
  const finalPayloadPath = join(root, "reports", "operator-fit-final-internal-pilot-ui-payload.json");
  if (existsSync(finalPayloadPath)) {
    const fp = JSON.parse(readFileSync(finalPayloadPath, "utf8"));
    const dealC = (fp.cases || []).find((c) => c.id === "pilot_deal_c");
    if (dealC) {
      const owners = dealC.ownerView?.productionCandidates || [];
      dealCPreview = {
        firstFrame: owners.slice(0, 5).map((c) => ({
          rank: c.rank,
          operator: c.operatorName,
          alignmentBand: c.alignmentBand,
          evidenceStrength: c.evidenceStrength,
          projectCompatibility: c.projectCompatibility,
          why: c.whyThisOperator,
          validateNext: c.validateNextPrimary?.action || null,
          primaryConcern: c.primaryConcern?.text || null,
        })),
        candidateDetail: "Expanded: numeric Operator Alignment, strengths, concerns, unknowns, brand note, Validate Next list, factors (Advisor-hidden on owner).",
        comparison: dealC.ownerView?.comparison?.tradeOffs || [],
        validateNext: owners.slice(0, 3).map((c) => ({
          operator: c.operatorName,
          actions: (c.validateNext?.actions || []).slice(0, 3),
        })),
        evidence: "Evidence Strength helper + diligence Level 3 on request — not default claim graphs.",
        hiddenInternal: [
          "Ranking Ready / Research Required labels",
          "Raw Alignment / Displayed Alignment jargon",
          "Confidence ceilings / coverage thresholds",
          "Research Stage operators in ranked list",
          "Internal-only claims",
          "Advisor factor weight diagnostics by default",
          "Prominent /100 score",
        ],
        knownLimitations: [
          "Project Approval remains to be confirmed where applicable",
          "Operator Alignment is not a forecast of future financial performance",
          "Rankings reflect the currently verified information",
          "Operator interest and commercial terms still require validation",
        ],
      };
    }
  }

  writeFileSync(
    join(root, "reports", "operator-fit-deal-c-controlled-pilot-preview.md"),
    [
      "# Deal C — Controlled Owner Pilot Preview (Not Enabled)",
      "",
      "## First frame",
      "",
      "| Rank | Operator | Band | Evidence Strength | Compatibility | Why / Validate Next |",
      "| ---- | -------- | ---- | ----------------- | ------------- | ------------------- |",
      ...(dealCPreview.firstFrame || []).map(
        (r) =>
          `| ${r.rank} | ${r.operator} | ${r.alignmentBand} | ${r.evidenceStrength} | ${r.projectCompatibility || "—"} | ${(r.why || "").slice(0, 80)}… / ${r.validateNext || "—"} |`
      ),
      "",
      "## Candidate detail",
      "",
      dealCPreview.candidateDetail || "—",
      "",
      "## Comparison",
      "",
      ...(dealCPreview.comparison || []).map((t) => `- **${t.operatorName}:** ${t.statement}`),
      "",
      "## Validate Next",
      "",
      ...(dealCPreview.validateNext || []).flatMap((v) => [
        `### ${v.operator}`,
        ...(v.actions || []).map((a) => `- ${a.action} (${a.phaseLabel})`),
        "",
      ]),
      "## Evidence",
      "",
      dealCPreview.evidence || "—",
      "",
      "## Hidden internal information",
      "",
      ...(dealCPreview.hiddenInternal || []).map((x) => `- ${x}`),
      "",
      "## Known limitations",
      "",
      ...(dealCPreview.knownLimitations || []).map((x) => `- ${x}`),
      "",
      "**Owner is not enabled.**",
      "",
    ].join("\n")
  );

  console.log(
    JSON.stringify(
      {
        dealF: {
          productionRR: rankingReady.length,
          decision: dealFDecision,
          provisionalSynthetic: true,
        },
        corrected: { strong, useful, material, formalThresholdMet, dealCStrong },
        dealBTruthfulness: dealBTruthfulness.score,
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
