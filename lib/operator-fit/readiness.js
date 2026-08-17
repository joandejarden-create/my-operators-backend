/**
 * Operator Fit — production ranking readiness + enrichment field catalog.
 * Founder rule 2.5: Ranking Ready requires critical fields + ≥50% project coverage.
 * @see docs/architecture/decisions/operator-fit-enrichment-founder-decisions.md
 */

import { isKnownPositive, listValue, scalarValue } from "./adapters/field-state.js";
import { EVIDENCE_CLASSES, EVIDENCE_CLASS_RANK, ELIGIBILITY_STATUS } from "./config.js";
import { evaluateCandidate } from "./evaluate-candidate.js";
import { evaluateEligibility } from "./eligibility.js";
import { isTableStakesToken } from "./adapters/operator-from-prefill.js";

export const READINESS_STATUS = Object.freeze({
  RANKING_READY: "Ranking Ready",
  CONDITIONALLY_RANKABLE: "Conditionally Rankable",
  RESEARCH_REQUIRED: "Research Required",
  OUT_OF_SCOPE: "Out of Current Scope",
  ADDITIONAL_RESEARCH: "Additional Candidate Requiring Research",
});

export const RESEARCH_PRIORITY = Object.freeze({
  CRITICAL: "Critical",
  HIGH: "High",
  MEDIUM: "Medium",
  LOW: "Low",
});

/** Minimum project-specific data coverage for Ranking Ready (founder 2.5). */
export const PRODUCTION_COVERAGE_THRESHOLD_PCT = 50;

/**
 * Enrichment field catalog used for coverage / impact / MVP profile.
 * Keys map to assessment functions over adapted operator (+ optional project).
 */
export const ENRICHMENT_FIELD_CATALOG = Object.freeze([
  {
    id: "activeStatus",
    label: "Active status",
    level: "A",
    requiredForRanking: true,
    eligibilityImpact: true,
    alignmentImpact: false,
    confidenceImpact: false,
    differentiationValue: "none",
    tableStakes: false,
    sourcingDifficulty: "easy",
    priority: RESEARCH_PRIORITY.CRITICAL,
    airtableHint: "Operator Setup - Master.submission_status",
  },
  {
    id: "geography",
    label: "Active countries (structured)",
    level: "A",
    requiredForRanking: true,
    eligibilityImpact: true,
    alignmentImpact: true,
    confidenceImpact: false,
    differentiationValue: "high",
    tableStakes: false,
    sourcingDifficulty: "medium",
    priority: RESEARCH_PRIORITY.CRITICAL,
    airtableHint: "Platform.Active Countries (structured multi-select; prose markets do not qualify)",
  },
  {
    id: "operatingStructures",
    label: "Operating structures supported",
    level: "A",
    requiredForRanking: true,
    eligibilityImpact: true,
    alignmentImpact: true,
    confidenceImpact: false,
    differentiationValue: "high",
    tableStakes: false,
    sourcingDifficulty: "medium",
    priority: RESEARCH_PRIORITY.CRITICAL,
    airtableHint: "Commercial.Management Structures Supported",
  },
  {
    id: "chainScales",
    label: "Hotel segments / chain scales",
    level: "A",
    requiredForRanking: true,
    eligibilityImpact: true,
    alignmentImpact: true,
    confidenceImpact: false,
    differentiationValue: "high",
    tableStakes: false,
    sourcingDifficulty: "easy",
    priority: RESEARCH_PRIORITY.CRITICAL,
    airtableHint: "Profile.chainScalesSupported",
  },
  {
    id: "projectExperience",
    label: "Meaningful project-experience dimension",
    level: "B",
    requiredForRanking: true,
    eligibilityImpact: false,
    alignmentImpact: true,
    confidenceImpact: false,
    differentiationValue: "high",
    tableStakes: false,
    sourcingDifficulty: "hard",
    priority: RESEARCH_PRIORITY.CRITICAL,
    airtableHint: "Commercial asset/situation + Case Studies",
  },
  {
    id: "evidenceSource",
    label: "Identified evidence source for material claims",
    level: "D",
    requiredForRanking: true,
    eligibilityImpact: false,
    alignmentImpact: false,
    confidenceImpact: true,
    differentiationValue: "medium",
    tableStakes: false,
    sourcingDifficulty: "hard",
    priority: RESEARCH_PRIORITY.HIGH,
    airtableHint: "PI Source Library / Case Studies / Master Source Type",
  },
  {
    id: "brandsOperated",
    label: "Brands currently operated",
    level: "C",
    requiredForRanking: false,
    eligibilityImpact: false,
    alignmentImpact: true,
    confidenceImpact: false,
    differentiationValue: "high",
    tableStakes: false,
    sourcingDifficulty: "medium",
    priority: RESEARCH_PRIORITY.HIGH,
    airtableHint: "Profile.brands / Brand Relationships",
  },
  {
    id: "brandApprovals",
    label: "Brand approval / relationship status",
    level: "C",
    requiredForRanking: false,
    eligibilityImpact: true,
    alignmentImpact: true,
    confidenceImpact: true,
    differentiationValue: "high",
    tableStakes: false,
    sourcingDifficulty: "hard",
    priority: RESEARCH_PRIORITY.HIGH,
    airtableHint: "Operator Setup - Brand Relationships (proposed)",
  },
  {
    id: "comparables",
    label: "Comparable operator assignments",
    level: "D",
    requiredForRanking: false,
    eligibilityImpact: false,
    alignmentImpact: true,
    confidenceImpact: true,
    differentiationValue: "very_high",
    tableStakes: false,
    sourcingDifficulty: "hard",
    priority: RESEARCH_PRIORITY.HIGH,
    airtableHint: "Operator Setup - Case Studies",
  },
  {
    id: "conversionExperience",
    label: "Conversion / reflag experience",
    level: "B",
    requiredForRanking: false,
    eligibilityImpact: false,
    alignmentImpact: true,
    confidenceImpact: false,
    differentiationValue: "very_high",
    tableStakes: false,
    sourcingDifficulty: "medium",
    priority: RESEARCH_PRIORITY.HIGH,
    airtableHint: "Commercial.Conversion / Reflag Experience",
  },
  {
    id: "governanceReporting",
    label: "Owner reporting / governance level",
    level: "B",
    requiredForRanking: false,
    eligibilityImpact: false,
    alignmentImpact: true,
    confidenceImpact: false,
    differentiationValue: "medium",
    tableStakes: false,
    sourcingDifficulty: "medium",
    priority: RESEARCH_PRIORITY.MEDIUM,
    airtableHint: "Governance.Owner Reporting Level",
  },
  {
    id: "regionalResources",
    label: "Regional resources / capacity",
    level: "B",
    requiredForRanking: false,
    eligibilityImpact: false,
    alignmentImpact: true,
    confidenceImpact: false,
    differentiationValue: "medium",
    tableStakes: false,
    sourcingDifficulty: "hard",
    priority: RESEARCH_PRIORITY.MEDIUM,
    airtableHint: "Platform regional fields (proposed)",
  },
  {
    id: "genericOfferedServices",
    label: "Generic offered-services checklist",
    level: "B",
    requiredForRanking: false,
    eligibilityImpact: false,
    alignmentImpact: false,
    confidenceImpact: false,
    differentiationValue: "none",
    tableStakes: true,
    sourcingDifficulty: "easy",
    priority: RESEARCH_PRIORITY.LOW,
    airtableHint: "Governance.Offered Services (table-stakes subset)",
  },
  {
    id: "economics",
    label: "Fee / commercial economics",
    level: "E",
    requiredForRanking: false,
    eligibilityImpact: false,
    alignmentImpact: false,
    confidenceImpact: false,
    differentiationValue: "low",
    tableStakes: false,
    sourcingDifficulty: "hard",
    priority: RESEARCH_PRIORITY.LOW,
    airtableHint: "Outreach / project-specific (Level E)",
  },
]);

/** Structured Active Countries only — prose market blurbs do not satisfy Ranking Ready. */
function hasStructuredGeography(op) {
  return listValue(op.geography?.countries).length > 0;
}

/** Narrative markets / regions may support Conditional eligibility research only. */
function hasProseGeographyOnly(op) {
  return (
    !hasStructuredGeography(op) && listValue(op.geography?.markets).length > 0
  );
}

function hasStructures(op) {
  return listValue(op.operatingStructures).length > 0;
}

function hasScales(op) {
  return listValue(op.chainScales).length > 0;
}

function hasProjectExperience(op) {
  return (
    listValue(op.assetExperience).length > 0 ||
    listValue(op.developmentExperience).length > 0 ||
    isKnownPositive(op.comparables) ||
    isKnownPositive(op.specialistExperience?.conversion) ||
    isKnownPositive(op.specialistExperience?.newBuild) ||
    listValue(op.specialistExperience?.differentiators).length > 0
  );
}

function hasEvidenceSource(op) {
  if (isKnownPositive(op.sources) && (op.sources.value || []).length) return true;
  if (isKnownPositive(op.comparables)) {
    const comps = op.comparables.value || [];
    if (comps.some((c) => c && (c.verified || c.referenced || c.source))) return true;
  }
  const classes = listValue(op.evidenceClasses);
  const best = Math.max(
    0,
    ...classes.map((c) => EVIDENCE_CLASS_RANK[c] ?? 0)
  );
  // Require detailed operator-provided or better — not portfolio/general alone,
  // and not Master "Source Type" metadata by itself (enrichment founder 2.1).
  if (best >= EVIDENCE_CLASS_RANK[EVIDENCE_CLASSES.DETAILED_OPERATOR_PROVIDED]) return true;
  return false;
}

/**
 * Field presence map for an adapted operator.
 * @returns {Record<string, boolean>}
 */
export function assessFieldPresence(operator) {
  const tableStakesOnly =
    listValue(operator.specialistExperience?.tableStakesClaimed).length > 0 &&
    listValue(operator.specialistExperience?.differentiators).length === 0;

  return {
    activeStatus:
      /^active$/i.test(String(scalarValue(operator.activeStatus) || "").trim()) ||
      (Boolean(operator.researchStageAllowed) &&
        /^research[\s_-]*stage$/i.test(String(scalarValue(operator.activeStatus) || "").trim())),
    geography: hasStructuredGeography(operator),
    geographyProseOnly: hasProseGeographyOnly(operator),
    operatingStructures: hasStructures(operator),
    chainScales: hasScales(operator),
    projectExperience: hasProjectExperience(operator),
    evidenceSource: hasEvidenceSource(operator),
    brandsOperated:
      listValue(operator.brandsOperated).length > 0 ||
      listValue(operator.brandFamilies).length > 0,
    brandApprovals: Array.isArray(operator.brandApprovals) && operator.brandApprovals.length > 0,
    comparables: isKnownPositive(operator.comparables),
    conversionExperience: isKnownPositive(operator.specialistExperience?.conversion),
    governanceReporting: isKnownPositive(operator.ownershipGovernance?.reportingLevel),
    regionalResources: listValue(operator.regionalResources).length > 0,
    genericOfferedServices: tableStakesOnly || listValue(operator.specialistExperience?.tableStakesClaimed).length > 0,
    economics: isKnownPositive(operator.commercial?.feeEconomics),
  };
}

function coveragePct(presence, fieldIds) {
  if (!fieldIds.length) return 0;
  const hits = fieldIds.filter((id) => presence[id]).length;
  return Math.round((hits / fieldIds.length) * 1000) / 10;
}

const ELIGIBILITY_FIELDS = [
  "activeStatus",
  "geography",
  "operatingStructures",
  "chainScales",
];
const DIFFERENTIATION_FIELDS = [
  "projectExperience",
  "brandsOperated",
  "comparables",
  "conversionExperience",
  "governanceReporting",
  "regionalResources",
];
const EVIDENCE_FIELDS = ["evidenceSource", "comparables", "brandApprovals"];
const BRAND_REL_FIELDS = ["brandsOperated", "brandApprovals"];

/**
 * Critical eligibility gates for Ranking Ready (independent of coverage %).
 */
export function missingCriticalRankingFields(presence) {
  const missing = [];
  if (!presence.activeStatus) missing.push("activeStatus");
  if (!presence.geography) missing.push("geography");
  if (!presence.operatingStructures) missing.push("operatingStructures");
  if (!presence.chainScales) missing.push("chainScales");
  if (!presence.projectExperience) missing.push("projectExperience");
  if (!presence.evidenceSource) missing.push("evidenceSource");
  return missing;
}

/**
 * Project-applicable coverage: uses evaluation coverage when project provided;
 * otherwise baseline eligibility+differentiation+evidence field coverage.
 */
export function calculateProjectApplicableCoverage(operator, project = null) {
  const presence = assessFieldPresence(operator);
  if (!project) {
    const ids = [
      ...ELIGIBILITY_FIELDS,
      "projectExperience",
      "evidenceSource",
      "brandsOperated",
      "comparables",
    ];
    return {
      coveragePct: coveragePct(presence, ids),
      mode: "baseline_profile",
      presence,
    };
  }
  const evaluated = evaluateCandidate(project, operator);
  return {
    coveragePct: evaluated.dataCoveragePct ?? 0,
    mode: "project_specific",
    presence,
    evaluated,
  };
}

/**
 * Classify operator readiness for a project (or baseline if project null).
 */
export function classifyOperatorReadiness(operator, project = null, opts = {}) {
  const outOfScope = opts.outOfScope === true;
  if (outOfScope) {
    return {
      status: READINESS_STATUS.OUT_OF_SCOPE,
      researchPriority: RESEARCH_PRIORITY.LOW,
      reason: opts.outOfScopeReason || "Outside current active operator universe.",
      coveragePct: 0,
      missingCritical: [],
      presence: assessFieldPresence(operator),
    };
  }

  const cov = calculateProjectApplicableCoverage(operator, project);
  const presence = cov.presence;
  const missingCritical = missingCriticalRankingFields(presence);
  const eligibilityCoverage = coveragePct(presence, ELIGIBILITY_FIELDS);
  const differentiationCoverage = coveragePct(presence, DIFFERENTIATION_FIELDS);
  const evidenceCoverage = coveragePct(presence, EVIDENCE_FIELDS);
  const brandRelCoverage = coveragePct(presence, BRAND_REL_FIELDS);

  let status = READINESS_STATUS.RESEARCH_REQUIRED;
  let reason = "Missing critical eligibility or differentiation information.";

  const meetsCritical = missingCritical.length === 0;
  const meetsCoverage = cov.coveragePct >= PRODUCTION_COVERAGE_THRESHOLD_PCT;

  let eligibilityConflict = false;
  let eligibilityStatus = null;
  if (project) {
    const elig = evaluateEligibility(project, operator);
    eligibilityStatus = elig.status;
    if (elig.status === ELIGIBILITY_STATUS.NOT_ELIGIBLE || (elig.hardConflicts || []).length) {
      eligibilityConflict = true;
    }
  }

  // High coverage of unsupported/generic claims alone is not Ranking Ready
  const onlyGenericClaims =
    presence.genericOfferedServices &&
    !presence.comparables &&
    !presence.conversionExperience &&
    listValue(operator.specialistExperience?.differentiators).length === 0;

  if (eligibilityConflict) {
    status = READINESS_STATUS.RESEARCH_REQUIRED;
    reason =
      "Critical eligibility conflict for this project — cannot enter production Top-5 (founder 2.5).";
  } else if (meetsCritical && meetsCoverage && !onlyGenericClaims) {
    status = READINESS_STATUS.RANKING_READY;
    reason =
      "Critical ranking fields present and project-applicable coverage meets the 50% threshold.";
  } else if (
    presence.activeStatus &&
    (presence.geography ||
      presence.geographyProseOnly ||
      presence.operatingStructures ||
      presence.chainScales) &&
    missingCritical.length <= 2
  ) {
    status = READINESS_STATUS.CONDITIONALLY_RANKABLE;
    reason =
      "Core identity present but missing differentiators, evidence, or full eligibility set.";
  } else if (meetsCoverage && !meetsCritical) {
    status = READINESS_STATUS.RESEARCH_REQUIRED;
    reason =
      "Coverage may meet 50% but critical eligibility/evidence fields are missing — not Ranking Ready.";
  }

  // Brand-managed special: unconfirmed availability → conditional at best
  if (operator.candidateType === "Brand Managed") {
    const meta = operator.brandManagedMeta || {};
    if (!meta.offersBrandManagementConfirmed && !meta.offersBrandManagementVerified) {
      if (status === READINESS_STATUS.RANKING_READY) {
        status = READINESS_STATUS.CONDITIONALLY_RANKABLE;
      }
      reason =
        "Brand-managed availability is not independently confirmed — Eligible With Conditions / Conditionally Rankable.";
    }
  }

  const missingFields = ENRICHMENT_FIELD_CATALOG.filter((f) => !presence[f.id] && !f.tableStakes)
    .sort((a, b) => priorityRank(a.priority) - priorityRank(b.priority))
    .map((f) => f.id);

  const researchPriority =
    status === READINESS_STATUS.RESEARCH_REQUIRED
      ? RESEARCH_PRIORITY.CRITICAL
      : status === READINESS_STATUS.CONDITIONALLY_RANKABLE
        ? RESEARCH_PRIORITY.HIGH
        : RESEARCH_PRIORITY.LOW;

  return {
    status,
    researchPriority,
    reason,
    coveragePct: cov.coveragePct,
    coverageMode: cov.mode,
    eligibilityCoverage,
    differentiationCoverage,
    evidenceCoverage,
    brandRelCoverage,
    missingCritical,
    missingFields,
    presence,
    evaluated: cov.evaluated || null,
    eligibilityStatus,
    eligibilityConflict,
    thresholdPct: PRODUCTION_COVERAGE_THRESHOLD_PCT,
  };
}

function priorityRank(p) {
  if (p === RESEARCH_PRIORITY.CRITICAL) return 0;
  if (p === RESEARCH_PRIORITY.HIGH) return 1;
  if (p === RESEARCH_PRIORITY.MEDIUM) return 2;
  return 3;
}

/**
 * Production Top-5 gate: Ranking Ready only (founder 2.5).
 * Others may be surfaced as Additional Candidate Requiring Research.
 */
export function filterProductionTop5Candidates(evaluatedRows, readinessById) {
  const ranked = [];
  const additional = [];
  for (const row of evaluatedRows || []) {
    const ready = readinessById[row.candidateId];
    if (!ready) {
      additional.push({ ...row, productionLabel: READINESS_STATUS.ADDITIONAL_RESEARCH });
      continue;
    }
    if (ready.status === READINESS_STATUS.RANKING_READY) {
      ranked.push(row);
    } else if (
      ready.status === READINESS_STATUS.CONDITIONALLY_RANKABLE ||
      ready.status === READINESS_STATUS.RESEARCH_REQUIRED
    ) {
      additional.push({
        ...row,
        productionLabel: READINESS_STATUS.ADDITIONAL_RESEARCH,
        readinessStatus: ready.status,
      });
    }
  }
  return { productionTop5Pool: ranked, additionalResearch: additional };
}

/**
 * Brand-managed confirmation helper (founder 2.3).
 */
export function classifyBrandManagedAvailability({
  strategicPreference = false,
  officialBrandInfo = false,
  verifiedDealalityRelationship = false,
  directBrandConfirmation = false,
  documentedComparables = false,
}) {
  const confirmed =
    officialBrandInfo ||
    verifiedDealalityRelationship ||
    directBrandConfirmation ||
    documentedComparables;
  if (confirmed) {
    return {
      confirmed: true,
      eligibilityHint: "Eligible",
      positiveCompatibilityPointsAllowed: true,
      validationItem: null,
    };
  }
  if (strategicPreference) {
    return {
      confirmed: false,
      eligibilityHint: "Eligible With Conditions",
      positiveCompatibilityPointsAllowed: false,
      validationItem:
        "Confirm whether the brand will offer direct management for this project.",
    };
  }
  return {
    confirmed: false,
    eligibilityHint: "Eligible With Conditions",
    positiveCompatibilityPointsAllowed: false,
    validationItem:
      "Confirm whether the brand will offer direct management for this project.",
  };
}

export function buildEnrichmentQueueRow(operator, readiness, opts = {}) {
  const name = operator.identity?.value?.name || operator.operatorId || "Unknown";
  const missing = readiness.missingFields || [];
  const catalogById = Object.fromEntries(ENRICHMENT_FIELD_CATALOG.map((f) => [f.id, f]));
  const topMissing = missing.slice(0, 3).map((id) => catalogById[id]?.label || id);
  const topField = catalogById[missing[0]];
  return {
    operatorId: operator.operatorId,
    operatorName: name,
    readinessStatus: readiness.status,
    overallCoverage: readiness.coveragePct,
    eligibilityCoverage: readiness.eligibilityCoverage,
    differentiationCoverage: readiness.differentiationCoverage,
    evidenceCoverage: readiness.evidenceCoverage,
    brandRelationshipCoverage: readiness.brandRelCoverage,
    highestPriorityMissingField: topMissing[0] || "",
    secondPriorityMissingField: topMissing[1] || "",
    thirdPriorityMissingField: topMissing[2] || "",
    recommendedSourceType: topField
      ? topField.level === "D"
        ? "Dealality research + independent source"
        : topField.level === "C"
          ? "Brand confirmation / Dealality relationship"
          : "Dealality baseline research"
      : "Dealality baseline research",
    expectedScoringImpact: topField?.differentiationValue || "medium",
    researchPriority: readiness.researchPriority,
    notes: readiness.reason,
    regionHint: listValue(operator.geography?.countries).slice(0, 3).join("; "),
    operatorType: operator.candidateType || "Third-Party Operator",
    ...opts,
  };
}

/** Detect taxonomy issues in raw prefill arrays. */
export function validateOperatorTaxonomy(prefill = {}) {
  const issues = [];
  const scales = [].concat(prefill.chainScalesSupported || prefill.chainScale || []);
  for (const s of scales) {
    if (typeof s !== "string" || !s.trim()) {
      issues.push({ field: "chainScalesSupported", issue: "empty_or_invalid", value: s });
    }
  }
  const countries = [].concat(prefill.activeCountries || []);
  for (const c of countries) {
    if (typeof c === "string" && /uruguay/i.test(c)) {
      issues.push({
        field: "activeCountries",
        issue: "known_option_gap_risk",
        value: c,
        note: "Historically not in allowed options — validate against live registry",
      });
    }
  }
  const services = [].concat(prefill.offeredServices || []);
  const onlyStakes = services.length > 0 && services.every((s) => isTableStakesToken(s));
  if (onlyStakes) {
    issues.push({
      field: "offeredServices",
      issue: "table_stakes_only",
      value: services.slice(0, 5),
      note: "Does not create positive differentiation in Operator Fit v2",
    });
  }
  return issues;
}

/** Detect claims without evidence. */
export function validateOperatorEvidence(operator) {
  const issues = [];
  const presence = assessFieldPresence(operator);
  if (presence.projectExperience && !presence.evidenceSource) {
    issues.push({
      issue: "experience_without_source",
      message: "Project experience signals present without identified evidence source.",
    });
  }
  if (presence.brandsOperated && !presence.brandApprovals) {
    issues.push({
      issue: "brands_without_verification",
      message: "Brand portfolio listed without approval/verification records.",
    });
  }
  if (presence.comparables) {
    for (const c of operator.comparables.value || []) {
      if (!c) continue;
      const missingMeta = [];
      if (!c.propertyName && !c.name) missingMeta.push("name");
      if (!c.region && !c.country && !c.market) missingMeta.push("geography");
      if (!c.situation && !c.hotelType) missingMeta.push("project_type");
      if (missingMeta.length) {
        issues.push({
          issue: "comparable_incomplete",
          message: `Comparable missing: ${missingMeta.join(", ")}`,
        });
      }
    }
  }
  const classes = listValue(operator.evidenceClasses);
  if (
    classes.includes(EVIDENCE_CLASSES.GENERAL_CLAIM) &&
    !classes.some(
      (c) =>
        EVIDENCE_CLASS_RANK[c] >= EVIDENCE_CLASS_RANK[EVIDENCE_CLASSES.INDEPENDENT_REFERENCED]
    )
  ) {
    issues.push({
      issue: "unsupported_claims_only",
      message: "Evidence is general operator claim only — cannot produce Strong confidence.",
    });
  }
  return issues;
}
