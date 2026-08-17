/**
 * Owner/Advisor presentation layer for Operator Fit.
 * Does NOT change scoring, weights, eligibility logic, or readiness thresholds.
 * Presentation-only mapping of evaluated candidates.
 */

import { classifyRankChangeList } from "./rank-change-actionability.js";
import { listRankingChangeValidations } from "./ranking-change-validations.js";
import { EVIDENCE_CONFIDENCE } from "./config.js";

/** Presentation bands — map displayed alignment; not new scoring thresholds. */
export const OWNER_ALIGNMENT_BANDS = Object.freeze([
  { id: "strong", label: "Strong Alignment", minInclusive: 70 },
  { id: "good", label: "Good Alignment", minInclusive: 55 },
  { id: "potential", label: "Potential Alignment", minInclusive: 40 },
  { id: "limited", label: "Limited Alignment", minInclusive: 0 },
]);

export const EVIDENCE_STRENGTH_HELPER = Object.freeze({
  Strong: "Supported by multiple relevant and independently verifiable sources.",
  Moderate: "Supported by credible information, but some important details remain operator-reported or incomplete.",
  Limited: "Material information remains unverified or unavailable.",
});

export const UNKNOWN_PRIORITY = Object.freeze({
  CRITICAL: "Critical",
  MATERIAL: "Material",
  DILIGENCE: "Diligence",
  INFORMATIONAL: "Informational",
});

export const CONCERN_KIND = Object.freeze({
  CONFIRMED_LIMITATION: "Confirmed Limitation",
  POTENTIAL_CONSTRAINT: "Potential Constraint",
  VALIDATION_NEEDED: "Validation Needed",
  INFORMATION_NOT_YET_AVAILABLE: "Information Not Yet Available",
});

export const OWNER_TERMS = Object.freeze({
  evidenceStrength: "Evidence Strength",
  potentialFitValidationNeeded: "Potential Fit — Validation Needed",
  underEvaluation: "Under Evaluation",
  validateNext: "Validate Next",
  projectCompatibility: "Project Compatibility",
});

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function asList(v) {
  if (Array.isArray(v)) return v.filter(Boolean).map(String);
  if (v == null || v === "") return [];
  return [String(v)];
}

/**
 * Map displayed alignment → band. Hard eligibility failure cannot show Strong/Good.
 */
export function mapAlignmentBand(displayedAlignment, eligibilityStatus) {
  const score = num(displayedAlignment);
  const elig = String(eligibilityStatus || "");
  const hardFail = /not currently eligible|ineligible/i.test(elig);

  if (hardFail) {
    return {
      id: "limited",
      label: "Limited Alignment",
      numeric: score,
      contradictedByEligibility: true,
      note: "Alignment band capped because of a hard eligibility conflict.",
    };
  }

  let band = OWNER_ALIGNMENT_BANDS[OWNER_ALIGNMENT_BANDS.length - 1];
  if (score != null) {
    for (const b of OWNER_ALIGNMENT_BANDS) {
      if (score >= b.minInclusive) {
        band = b;
        break;
      }
    }
  }

  // Eligible With Conditions → prefer Potential label when would otherwise say Strong
  if (/eligible with conditions/i.test(elig) && band.id === "strong") {
    return {
      id: "good",
      label: "Good Alignment",
      numeric: score,
      conditionVisible: true,
      projectCompatibility: OWNER_TERMS.potentialFitValidationNeeded,
    };
  }

  return {
    id: band.id,
    label: band.label,
    numeric: score,
    conditionVisible: /eligible with conditions/i.test(elig),
    projectCompatibility: /eligible with conditions/i.test(elig)
      ? OWNER_TERMS.potentialFitValidationNeeded
      : /preferred/i.test(elig)
        ? "Compatible"
        : /eligible/i.test(elig)
          ? "Compatible"
          : elig || null,
  };
}

export function mapEvidenceStrength(evidenceConfidence) {
  const raw = String(evidenceConfidence || "Limited").trim();
  const label =
    /strong/i.test(raw) ? "Strong" : /moderate/i.test(raw) ? "Moderate" : "Limited";
  return {
    label,
    helperText: EVIDENCE_STRENGTH_HELPER[label],
    internalConfidenceLabel: raw,
  };
}

/**
 * Prioritize unknowns for owner card vs detail.
 */
export function prioritizeUnknowns(unknowns = []) {
  const classified = asList(unknowns).map((text) => {
    let priority = UNKNOWN_PRIORITY.INFORMATIONAL;
    if (/eligib|market presence|not currently|hard conflict|cannot enter/i.test(text)) {
      priority = UNKNOWN_PRIORITY.CRITICAL;
    } else if (
      /brand approval|project-specific|management structure|regional|conversion|comparable|operating.structure/i.test(
        text
      )
    ) {
      priority = UNKNOWN_PRIORITY.MATERIAL;
    } else if (/fee|commercial|proposal|final|contract|leadership|pre-opening/i.test(text)) {
      priority = UNKNOWN_PRIORITY.DILIGENCE;
    }
    return { text, priority };
  });

  const order = [
    UNKNOWN_PRIORITY.CRITICAL,
    UNKNOWN_PRIORITY.MATERIAL,
    UNKNOWN_PRIORITY.DILIGENCE,
    UNKNOWN_PRIORITY.INFORMATIONAL,
  ];
  classified.sort((a, b) => order.indexOf(a.priority) - order.indexOf(b.priority));

  const primary = [];
  const critical = classified.filter((x) => x.priority === UNKNOWN_PRIORITY.CRITICAL);
  const material = classified.filter((x) => x.priority === UNKNOWN_PRIORITY.MATERIAL);
  if (critical.length) primary.push(critical[0]);
  else primary.push(...material.slice(0, 2));

  return {
    primary: primary.filter(Boolean),
    all: classified,
    hiddenCount: Math.max(0, classified.length - primary.length),
  };
}

/**
 * Soften unknown-as-negative; preserve confirmed limitations.
 */
export function classifyConcern(text) {
  const t = String(text || "");
  if (!t || /^—$/.test(t)) {
    return null;
  }
  if (/not yet|unconfirm|unknown|not confirmed|has not|unavailable|missing:/i.test(t)) {
    let ownerText = t;
    if (/weak regional|limited regional/i.test(t) && /unconfirm|unknown|not/i.test(t)) {
      ownerText = "Regional support for this project has not yet been confirmed.";
    } else if (/regional/i.test(t) && /unconfirm|unknown|not confirmed|not yet/i.test(t)) {
      ownerText = "Regional support for this project has not yet been confirmed.";
    } else if (/brand/i.test(t) && /unconfirm|unknown|not|approval/i.test(t)) {
      ownerText = "Project-specific brand approval has not yet been confirmed.";
    } else if (/fee|commercial/i.test(t) && /unknown|missing/i.test(t)) {
      ownerText = "Project-specific commercial terms are not yet available.";
    }
    return {
      kind: /validation|confirm/i.test(t)
        ? CONCERN_KIND.VALIDATION_NEEDED
        : CONCERN_KIND.INFORMATION_NOT_YET_AVAILABLE,
      text: ownerText,
      original: t,
    };
  }
  if (/limited asset|limited overlap|no clear evidence|unsupported|conflict/i.test(t)) {
    return { kind: CONCERN_KIND.POTENTIAL_CONSTRAINT, text: t, original: t };
  }
  return { kind: CONCERN_KIND.CONFIRMED_LIMITATION, text: t, original: t };
}

export function buildWhyThisOperator(candidate = {}) {
  const reasons = asList(candidate.whyItMatches || candidate.reasons)
    .filter((r) => !/table.?stakes|generic|operator is active\.?$/i.test(r))
    .slice(0, 3);
  const name = candidate.operatorName || candidate.operator || "This operator";
  if (!reasons.length) {
    return {
      summary: `${name} shows project-relevant alignment signals based on available verified information.`,
      reasons: [],
    };
  }
  const joined =
    reasons.length === 1
      ? reasons[0].replace(/\.$/, "")
      : reasons.length === 2
        ? `${reasons[0].replace(/\.$/, "")} and ${reasons[1].replace(/\.$/, "").toLowerCase()}`
        : `${reasons[0].replace(/\.$/, "")}, ${reasons[1].replace(/\.$/, "").toLowerCase()}, and ${reasons[2].replace(/\.$/, "").toLowerCase()}`;
  return {
    summary: `Aligned because of ${joined}.`,
    reasons,
  };
}

export function buildValidateNext(candidate = {}, project = null, { max = 3 } = {}) {
  const fromCandidate = asList(candidate.validationQuestions);
  const fromEngine = classifyRankChangeList(
    listRankingChangeValidations(project || { geography: {} }, {
      geography: { marketPresence: [] },
      operatingStructures: [],
      brandsOperated: [],
      comparables: [],
      specialistExperience: {},
    })
  );

  const merged = [];
  const seen = new Set();
  for (const q of fromCandidate) {
    const key = q.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    merged.push({
      question: q,
      sensitivity: /brand|eligib|presence/i.test(q)
        ? "Eligibility-sensitive"
        : /fee|proposal|final/i.test(q)
          ? "Final-selection-sensitive"
          : "Rank-sensitive",
      phase: /fee|proposal/i.test(q)
        ? "before_proposal"
        : /interest|confirm operator/i.test(q)
          ? "during_outreach"
          : "before_outreach",
      ownerAdvisorWording: q,
    });
  }
  for (const item of fromEngine) {
    const key = String(item.question || "").toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    merged.push(item);
  }

  const phaseRank = { before_outreach: 0, during_outreach: 1, before_proposal: 2, before_final: 3 };
  const sensRank = {
    "Eligibility-sensitive": 0,
    "Rank-sensitive": 1,
    "Confidence-sensitive": 2,
    "Final-selection-sensitive": 3,
    "Informational only": 4,
  };
  merged.sort(
    (a, b) =>
      (sensRank[a.sensitivity] ?? 9) - (sensRank[b.sensitivity] ?? 9) ||
      (phaseRank[a.phase] ?? 9) - (phaseRank[b.phase] ?? 9)
  );

  const top = merged.slice(0, max).map((m) => ({
    action: m.ownerAdvisorWording || m.question,
    phase: m.phase || "before_outreach",
    phaseLabel:
      m.phase === "during_outreach"
        ? "During Outreach"
        : m.phase === "before_proposal" || m.phase === "before_final"
          ? "Before Final Decision"
          : "Before Outreach",
    sensitivity: m.sensitivity,
  }));

  return {
    primary: top[0] || null,
    actions: top,
    beforeOutreach: top.filter((a) => a.phaseLabel === "Before Outreach"),
    duringOutreach: top.filter((a) => a.phaseLabel === "During Outreach"),
    beforeFinal: top.filter((a) => a.phaseLabel === "Before Final Decision"),
  };
}

/**
 * Build Level-1 owner card + Level-2/3 payloads from one evaluated candidate.
 */
export function buildOwnerCandidatePresentation(candidate = {}, project = null, opts = {}) {
  const hideScore = opts.hideNumericScore === true;
  const lifecycle = candidate.lifecycle || candidate.candidateLane || "";
  const research =
    /research/i.test(lifecycle) || candidate.researchStage === true || candidate.researchStageRankingReady;

  const band = mapAlignmentBand(
    candidate.displayedOperatorAlignment ?? candidate.alignment,
    candidate.eligibilityStatus || candidate.eligibility
  );
  const strength = mapEvidenceStrength(candidate.evidenceConfidence || candidate.confidence);
  const why = buildWhyThisOperator(candidate);
  const unknowns = prioritizeUnknowns(candidate.unknowns || []);
  const concerns = asList(candidate.potentialConcerns || candidate.concerns)
    .map(classifyConcern)
    .filter(Boolean);
  const validateNext = buildValidateNext(candidate, project);

  const primaryConcern = concerns.find((c) => c.kind === CONCERN_KIND.CONFIRMED_LIMITATION) ||
    concerns.find((c) => c.kind === CONCERN_KIND.POTENTIAL_CONSTRAINT) ||
    concerns[0] ||
    null;

  return {
    view: "owner",
    rank: candidate.rank || null,
    operatorId: candidate.operatorId || candidate.candidateId || null,
    operatorName: candidate.operatorName || candidate.operator || "—",
    underEvaluation: Boolean(research),
    underEvaluationLabel: research ? OWNER_TERMS.underEvaluation : null,
    /** Production ranked list should exclude research — flag for UI filter */
    includeInProductionRanking: !research,
    alignmentBand: band.label,
    alignmentBandId: band.id,
    projectCompatibility: band.projectCompatibility,
    evidenceStrength: strength.label,
    evidenceStrengthHelper: strength.helperText,
    whyThisOperator: why.summary,
    whyReasons: why.reasons,
    primaryConcern: primaryConcern
      ? { kind: primaryConcern.kind, text: primaryConcern.text }
      : null,
    primaryUnknown: unknowns.primary[0] || null,
    primaryUnknowns: unknowns.primary,
    validateNextPrimary: validateNext.primary,
    validateNext: validateNext,
    shortlistEnabled: true,
    /** Numeric hidden on Level 1 */
    numericAlignment: hideScore ? null : null,
    level2: {
      numericOperatorAlignment: candidate.displayedOperatorAlignment ?? candidate.alignment ?? null,
      rawOperatorAlignment: candidate.rawOperatorAlignment ?? null,
      dataCoveragePct: candidate.dataCoveragePct ?? candidate.coverage ?? null,
      eligibilityStatus: candidate.eligibilityStatus || candidate.eligibility,
      eligibilityInternal: true,
      strengths: asList(candidate.whyItMatches).slice(0, 6),
      concerns: concerns,
      unknowns: unknowns.all,
      comparables: asList(candidate.comparables).slice(0, 5),
      operatingStructure: candidate.operatingStructure || null,
      brandRelationshipNote:
        "Brand relationship is not project approval. Project-specific approval remains to be confirmed unless explicitly verified.",
      factorBreakdown: candidate.factorBreakdown || candidate.operatorProjectFactors || [],
      validateNext: validateNext.actions,
    },
    level3: {
      note: "Evidence detail — claims/sources/verification. Internal / diligence only.",
      evidenceConfidenceInternal: candidate.evidenceConfidence || candidate.confidence,
      readinessInternal: candidate.readiness || null,
    },
    evidenceCeilingsRef: EVIDENCE_CONFIDENCE,
  };
}

export function buildAdvisorCandidatePresentation(candidate = {}, project = null) {
  const owner = buildOwnerCandidatePresentation(candidate, project, { hideNumericScore: false });
  return {
    ...owner,
    view: "advisor",
    numericOperatorAlignment: candidate.displayedOperatorAlignment ?? candidate.alignment ?? null,
    rawOperatorAlignment: candidate.rawOperatorAlignment ?? null,
    dataCoveragePct: candidate.dataCoveragePct ?? candidate.coverage ?? null,
    eligibilityStatus: candidate.eligibilityStatus || candidate.eligibility,
    readinessInternal: candidate.readiness || null,
    lifecycle: candidate.lifecycle || null,
    evidenceConfidenceInternal: candidate.evidenceConfidence || candidate.confidence,
    factorBreakdown: candidate.factorBreakdown || [],
    allUnknowns: candidate.unknowns || [],
    allConcerns: candidate.potentialConcerns || [],
    diagnosticsOpen: true,
  };
}

/**
 * Difference-only comparison rows for up to 4 candidates (owner-style).
 */
export function buildOwnerStyleComparison(candidates = []) {
  const list = (candidates || []).slice(0, 4).map((c) => {
    const p = typeof c.alignmentBand === "string" ? c : buildOwnerCandidatePresentation(c);
    return p;
  });

  const fieldDefs = [
    { key: "alignmentBand", label: "Alignment" },
    { key: "evidenceStrength", label: "Evidence Strength" },
    { key: "projectCompatibility", label: "Project compatibility" },
    {
      key: "whyThisOperator",
      label: "Why this operator",
    },
  ];

  const rows = fieldDefs
    .map((f) => {
      const values = list.map((c) => c[f.key] || "—");
      const unique = new Set(values.map((v) => String(v).toLowerCase()));
      return {
        label: f.label,
        values,
        highlight: unique.size > 1,
        hide: unique.size === 1,
      };
    })
    .filter((r) => !r.hide || r.label === "Alignment");

  const tradeOffs = list.map((c) => {
    const strength = (c.whyReasons && c.whyReasons[0]) || c.whyThisOperator || "Relevant alignment signals";
    const gap =
      (c.primaryConcern && c.primaryConcern.text) ||
      (c.validateNextPrimary && c.validateNextPrimary.action) ||
      "Further validation still required";
    return {
      operatorName: c.operatorName,
      statement: `${String(strength).replace(/\.$/, "")}, but ${String(gap).charAt(0).toLowerCase()}${String(gap).slice(1)}`.replace(
        /\.\.$/,
        "."
      ),
    };
  });

  return { operators: list, rows, tradeOffs, maxCompared: 4 };
}

/**
 * Zero-production-universe owner messaging (constrained verified universe).
 */
export function buildZeroUniverseOwnerMessage({ underEvaluation = [] } = {}) {
  return {
    headline:
      "No operators in the currently verified universe meet Dealality’s minimum alignment and evidence requirements for this project.",
    clarifying: [
      "This applies to Dealality’s currently verified operator universe — not a claim that no suitable operator exists in the real market.",
      "Additional operators may be under evaluation.",
      "Some candidates require further validation before they can be ranked.",
    ],
    underEvaluation: underEvaluation.map((c) => ({
      operatorName: c.operatorName || c.operator,
      label: OWNER_TERMS.underEvaluation,
      whyNotRanked:
        "Not yet in the production Active / Ranking Ready universe for owner-facing ranking.",
      validating: c.validateNextPrimary?.action || "Confirm evidence and production readiness requirements.",
    })),
  };
}
