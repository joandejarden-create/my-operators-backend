/**
 * Operator Capability Snapshot v1 — shared build + access assessment.
 */

import {
  DEALS_FIELDS,
  LOCATION_FIELDS,
  SI_FIELDS,
  NEEDS_REVIEW,
  OCS_INPUTS_VERSION,
  strVal,
  listVal,
  isOperatorInScopeFromFields,
} from "./operator-capability-inputs.js";
import {
  PROJECT_TYPE_CANONICAL_OPTIONS,
  normalizeProjectTypeLabel,
  resolveProjectTypeKind,
  isDeprecatedProjectTypeWriteValue,
} from "./project-type.js";
import { detectOperatingModelConflicts } from "./operator-capability-backfill.js";
import {
  deriveCapabilityAreas,
  buildClarifications,
  buildOperatingContext,
  buildReportingSummary,
  capabilityIdsForProjectTypeKind,
} from "./operator-capability-rules.js";
import { OCS_DISCLAIMER_V1 } from "./operator-capability-copy.js";
import {
  buildOperatorCapabilityNarrative,
  neutralizeSnapshotAccessReasons,
} from "./operator-capability-narrative.js";

export const SNAPSHOT_STATUS = {
  ALLOWED: "allowed",
  LIMITED: "limited",
  BLOCKED: "blocked",
};

const REVIEW_LABELS = {
  allowed: "Ready for owner/advisor review",
  limited: "Limited draft — review before external use",
  blocked: "More deal information required",
};

const MANUAL_REVIEW_LABEL = "Manual Review Required";

/** @param {Record<string, unknown>} fields */
function isEmptyField(fields, key) {
  const v = fields[key];
  if (v == null || v === "") return true;
  if (Array.isArray(v)) return v.length === 0;
  return strVal(v) === "";
}

/** @param {Record<string, unknown>} fields @param {boolean} inScope */
export function collectMissingP0(fields, inScope) {
  const missing = [];
  if (isEmptyField(fields, DEALS_FIELDS.projectType)) missing.push("Project Type");
  if (isEmptyField(fields, DEALS_FIELDS.currentOperatingModel)) {
    missing.push("Current Operating Model");
  }
  if (isEmptyField(fields, LOCATION_FIELDS.primaryMarketRegion)) {
    missing.push("Primary Market Region");
  }
  if (inScope) {
    if (isEmptyField(fields, SI_FIELDS.preferredFutureOperatingModel)) {
      missing.push("Preferred Future Operating Model");
    }
    if (isEmptyField(fields, SI_FIELDS.operatorCapabilityPriorities)) {
      missing.push("Operator Capability Priorities");
    }
    const kind = resolveProjectTypeKind(fields[DEALS_FIELDS.projectType]);
    if (
      (kind === "conversion_reflag" || kind === "renovation_repositioning") &&
      isEmptyField(fields, DEALS_FIELDS.openingTransitionPhase)
    ) {
      missing.push("Opening / Transition Phase");
    }
    if (
      /third.party|brand \+ third/i.test(strVal(fields[SI_FIELDS.preferredFutureOperatingModel])) &&
      isEmptyField(fields, SI_FIELDS.ownerReportingFrequency)
    ) {
      missing.push("Owner Reporting Frequency");
    }
  }
  return missing;
}

/** @param {{ id: string, label: string, sources: string[], strength: string }[]} capabilityAreas */
export function extractRuleTriggers(capabilityAreas) {
  const rules = new Set();
  for (const row of capabilityAreas) {
    const src = (row.sources && row.sources[0]) || "";
    if (src.startsWith("Project Type")) rules.add("project_type_kind");
    else if (src === "Operator Capability Priorities") rules.add("stated_priorities");
    else if (src === "Deal context inference") rules.add("generic_context_blob");
    else if (src) rules.add(src);
  }
  const ptRow = capabilityAreas.find((r) => (r.sources?.[0] || "").startsWith("Project Type"));
  if (ptRow) {
    const m = ptRow.sources[0].match(/Project Type \((.+)\)/);
    if (m) rules.add(`project_type:${m[1]}`);
  }
  return [...rules].sort();
}

/**
 * @param {Record<string, unknown>} fields
 * @param {boolean} backfillUncertain
 * @param {string[]} clarifications
 * @param {{ id: string }[]} capabilityAreas
 */
export function assessSnapshotStatus(fields, backfillUncertain, clarifications, capabilityAreas) {
  const reasons = [];
  const rawPt = strVal(fields[DEALS_FIELDS.projectType]);
  const inScope = isOperatorInScopeFromFields(fields);
  const acquisitionInPt = /acquisition\s+of\s+operating/i.test(rawPt);
  const deprecatedPt = isDeprecatedProjectTypeWriteValue(rawPt);
  const nonCanonicalStored =
    rawPt && !PROJECT_TYPE_CANONICAL_OPTIONS.includes(rawPt) && rawPt !== normalizeProjectTypeLabel(rawPt);

  if (acquisitionInPt) reasons.push("Project Type contains acquisition language (invalid)");
  if (deprecatedPt) reasons.push("Deprecated Project Type value");
  if (!rawPt) reasons.push("Missing Project Type");

  const missing = collectMissingP0(fields, inScope);
  if (missing.length) reasons.push(`Missing: ${missing.join(", ")}`);

  if (strVal(fields[DEALS_FIELDS.currentOperatingModel]) === NEEDS_REVIEW) {
    reasons.push("Current Operating Model = Needs Review");
  }
  if (strVal(fields[DEALS_FIELDS.openingTransitionPhase]) === NEEDS_REVIEW) {
    reasons.push("Opening / Transition Phase = Needs Review");
  }
  if (strVal(fields[SI_FIELDS.preferredFutureOperatingModel]) === NEEDS_REVIEW) {
    reasons.push("Preferred Future Operating Model = Needs Review");
  }
  if (backfillUncertain) reasons.push("Uncertain operating field inference");

  const kind = resolveProjectTypeKind(rawPt);
  if (kind === "other_tbc") reasons.push("Project Type is Other / To Be Confirmed");

  const conflicts = detectOperatingModelConflicts(fields);
  if (conflicts.length) reasons.push(`Operating model conflicts: ${conflicts.join("; ")}`);

  if (!inScope) reasons.push("Third-party operator path not in scope");

  if (acquisitionInPt || !rawPt || deprecatedPt || nonCanonicalStored) {
    return { status: SNAPSHOT_STATUS.BLOCKED, reasons, missing, conflicts };
  }
  if (!inScope) {
    return { status: SNAPSHOT_STATUS.LIMITED, reasons, missing, conflicts };
  }

  const needsReviewStored =
    strVal(fields[DEALS_FIELDS.currentOperatingModel]) === NEEDS_REVIEW ||
    strVal(fields[DEALS_FIELDS.openingTransitionPhase]) === NEEDS_REVIEW ||
    strVal(fields[SI_FIELDS.preferredFutureOperatingModel]) === NEEDS_REVIEW;

  if (
    backfillUncertain ||
    needsReviewStored ||
    missing.length >= 2 ||
    clarifications.length >= 3 ||
    kind === "other_tbc"
  ) {
    return { status: SNAPSHOT_STATUS.LIMITED, reasons, missing, conflicts };
  }
  if (missing.length === 1 || clarifications.length > 0 || conflicts.length > 0) {
    return { status: SNAPSHOT_STATUS.LIMITED, reasons, missing, conflicts };
  }
  return {
    status: SNAPSHOT_STATUS.ALLOWED,
    reasons: ["P0 operator fields populated"],
    missing,
    conflicts,
  };
}

/** @param {import('./project-type.js').ProjectTypeKind} kind */
function buildReviewProfiles(kind, ctx) {
  const base = {
    id: "operating_context",
    label: "Operating context",
    summary: `${ctx.currentOperatingModel} → ${ctx.preferredFutureOperatingModel} (${ctx.projectType})`,
    focusAreas: ["Current Operating Model", "Preferred Future Operating Model", "Opening / Transition Phase"],
  };
  const byKind = {
    new_build: {
      id: "new_build",
      label: "New build / opening",
      summary: "Ground-up development, pre-opening, and development complexity.",
      focusAreas: ["Pre-opening support", "Development & permitting", "Opening / Transition Phase"],
    },
    conversion_reflag: {
      id: "conversion_reflag",
      label: "Conversion / reflag",
      summary: "Brand change, PIP, standards alignment, and operator transition.",
      focusAreas: ["Conversion & PIP", "Brand standards", "Operator transition"],
    },
    renovation_repositioning: {
      id: "renovation_repositioning",
      label: "Renovation / repositioning",
      summary: "Capex, commercial repositioning, and operating-while-renovating coordination.",
      focusAreas: ["PIP / capex", "Commercial repositioning", "Revenue management"],
    },
    existing_operating: {
      id: "existing_operating",
      label: "Existing operating hotel",
      summary: "Current performance, reporting, and management model review.",
      focusAreas: ["Full management", "Owner reporting", "Revenue management"],
    },
    adaptive_reuse: {
      id: "adaptive_reuse",
      label: "Adaptive reuse",
      summary: "Conversion execution, permitting, and pre-opening ramp.",
      focusAreas: ["Development complexity", "Conversion execution", "Pre-opening"],
    },
    mixed_use: {
      id: "mixed_use",
      label: "Mixed-use hospitality",
      summary: "Governance, amenity complexity, and brand/operator coordination.",
      focusAreas: ["Governance", "F&B / amenities", "Multi-stakeholder coordination"],
    },
    other_tbc: {
      id: "other_tbc",
      label: "Project type to be confirmed",
      summary: "Confirm project type before relying on capability themes.",
      focusAreas: ["Project Type confirmation"],
    },
  };
  const specific = byKind[kind];
  return specific ? [base, specific] : [base];
}

/** @param {string} status @param {boolean} requiresManualReview */
function buildConfidence(status, requiresManualReview) {
  if (status === SNAPSHOT_STATUS.BLOCKED) return "low";
  if (status === SNAPSHOT_STATUS.LIMITED || requiresManualReview) return "medium";
  return "high";
}

/**
 * @param {Record<string, unknown>} mergedFields
 * @param {string} dealId
 * @param {{ normalized?: { keyCount?: unknown } }} [meta]
 */
export function buildOperatorCapabilitySnapshotV1(mergedFields, dealId, meta = {}) {
  const fields = mergedFields || {};
  const dealName =
    strVal(fields["Property Name"]) ||
    strVal(fields["Project Name"]) ||
    strVal(fields["Name"]) ||
    "Deal";

  const operatingContext = buildOperatingContext(fields);
  const allClarifications = buildClarifications(fields);
  const reporting = buildReportingSummary(fields);
  const kind = resolveProjectTypeKind(fields[DEALS_FIELDS.projectType]);

  const needsReviewCurrent = strVal(fields[DEALS_FIELDS.currentOperatingModel]) === NEEDS_REVIEW;
  const needsReviewOpening = strVal(fields[DEALS_FIELDS.openingTransitionPhase]) === NEEDS_REVIEW;
  const needsReviewPreferred =
    strVal(fields[SI_FIELDS.preferredFutureOperatingModel]) === NEEDS_REVIEW;
  /** Post-backfill: only stored Needs Review flags drive manual review (not re-inference). */
  const requiresManualReview =
    needsReviewCurrent || needsReviewOpening || needsReviewPreferred;

  let capabilityAreas = deriveCapabilityAreas(fields);
  const access = assessSnapshotStatus(fields, false, allClarifications, capabilityAreas);

  if (access.status === SNAPSHOT_STATUS.BLOCKED) {
    capabilityAreas = capabilityAreas.filter((c) => c.strength === "stated");
  }

  const ruleTriggers = extractRuleTriggers(capabilityAreas);
  const missing = access.missing || collectMissingP0(fields, operatingContext.operatorInScope);
  const reviewProfiles = buildReviewProfiles(kind, operatingContext);

  const narrative = buildOperatorCapabilityNarrative({
    fields,
    operatingContext,
    projectTypeKind: kind,
    capabilityAreas,
    clarifications: allClarifications,
    missingInputs: missing,
    requiresManualReview,
    snapshotStatus: access.status,
    operatingModelConflicts: access.conflicts || [],
  });
  capabilityAreas = narrative.capabilityAreas;
  const diligenceQuestions = narrative.diligenceQuestions;

  let reviewLabel = REVIEW_LABELS[access.status] || REVIEW_LABELS.limited;
  if (requiresManualReview && access.status !== SNAPSHOT_STATUS.BLOCKED) {
    reviewLabel = MANUAL_REVIEW_LABEL;
  }

  const reviewContextParts = [];
  if (access.status === SNAPSHOT_STATUS.BLOCKED) {
    reviewContextParts.push(
      "Core deal inputs are insufficient or invalid for a capability snapshot. Resolve missing fields before using inferred themes."
    );
  } else if (requiresManualReview) {
    reviewContextParts.push(
      "One or more operating fields need manual validation (Needs Review). Capability themes below are draft-only until confirmed."
    );
  } else if (access.status === SNAPSHOT_STATUS.LIMITED) {
    reviewContextParts.push(
      "Limited internal draft. Resolve the clarification below before external sharing."
    );
  } else {
    reviewContextParts.push(
      "Ready for structured owner/advisor review based on current deal inputs."
    );
  }

  const generatedAt = new Date().toISOString();
  const city = strVal(fields["City & State"] || fields["City"]);
  const country = strVal(fields["Country"]);

  return {
    success: true,
    version: OCS_INPUTS_VERSION,
    dealId,
    dealName,
    snapshotStatus: access.status,
    requiresManualReview,
    reviewLabel,
    reviewContext: reviewContextParts.join(" "),
    capabilityAreas,
    reviewProfiles,
    clarifications: narrative.displayClarifications || allClarifications,
    diligenceQuestions,
    confidence: buildConfidence(access.status, requiresManualReview),
    ruleTriggers,
    generatedAt,
    disclaimer: OCS_DISCLAIMER_V1,
    operatingContext,
    reporting,
    missingInputs: missing,
    snapshotAccessReasons: neutralizeSnapshotAccessReasons(access.reasons),
    operatingModelConflicts: narrative.operatingModelTransitionsToValidate,
    operatingModelTransitionsToValidate: narrative.operatingModelTransitionsToValidate,
    operatingModelTransitionSummary: narrative.operatingModelTransitionSummary,
    projectTypeKind: kind,
    projectTypeCanonical: normalizeProjectTypeLabel(fields[DEALS_FIELDS.projectType]),
    executiveSummary: narrative.executiveSummary,
    ownerAdvisorReviewTakeaway: narrative.ownerAdvisorReviewTakeaway,
    whyOperatorStrategyMatters: narrative.whyOperatorStrategyMatters,
    operatingPathways: narrative.operatingPathways,
    capabilityImplications: narrative.capabilityImplications,
    decisionPointsBeforeOutreach: narrative.decisionPointsBeforeOutreach,
    knownGapsClarifications: narrative.knownGapsClarifications,
    newBuildGuidance: narrative.newBuildGuidance,
    brandManagedGuidance: narrative.brandManagedGuidance,
    deal: {
      id: dealId,
      name: dealName,
      projectType: strVal(fields[DEALS_FIELDS.projectType]) || "—",
      keyCount: (meta.normalized?.keyCount ?? strVal(fields["Total Number of Rooms/Keys"])) || "—",
      market: [city, country].filter(Boolean).join(", ") || "—",
      country: country || "—",
    },
    expectedProjectTypeCapabilityIds: capabilityIdsForProjectTypeKind(kind).map((r) => r.id),
  };
}
