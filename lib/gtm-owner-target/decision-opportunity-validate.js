/**
 * Validation for Dealality Decision Opportunity + Evidence writes (GTM internal).
 *
 * Returns structured validation results — never silently accept invalid payloads.
 * Stage 1: encodes lifecycle contracts; does not auto-transition status.
 */

import {
  MAP_DECISION_OPPORTUNITY,
  MAP_DECISION_OPPORTUNITY_EVIDENCE,
  VAL_DECISION_PROJECT_TYPE,
  VAL_DECISION_LIKELY_TYPE,
  VAL_DECISION_STAGE,
  VAL_DECISION_WINDOW,
  VAL_DECISION_BRAND_STATUS,
  VAL_DECISION_OPERATOR_STATUS,
  VAL_DECISION_EXCLUSIVITY_STATUS,
  VAL_DECISION_STILL_OPEN,
  VAL_DECISION_OPEN_CONFIDENCE,
  VAL_DECISION_TRIGGER,
  VAL_DECISION_WARM_PATH_TYPE,
  VAL_DECISION_STATUS,
  VAL_DECISION_RECOMMENDED_ACTION,
  VAL_DECISION_SCORE_BAND,
  VAL_DECISION_VISIBILITY,
  VAL_DECISION_DATA_SOURCE,
  VAL_DECISION_STATUS_REQUIRES_QUALIFICATION,
  VAL_DECISION_STATUS_OUTREACH_READY_CONTRACT,
  VAL_EVIDENCE_CONFIDENCE,
  VAL_EVIDENCE_DIRECTION,
  VAL_EVIDENCE_SUPPORTS_FIELD,
  VAL_EVIDENCE_SOURCE_TYPE,
} from "./decision-opportunity-field-map.js";

function isBlank(value) {
  if (value == null) return true;
  if (Array.isArray(value)) return value.length === 0;
  return String(value).trim() === "";
}

function isAllowed(value, allowed) {
  return value == null || value === "" || allowed.includes(value);
}

function field(fields, key) {
  return fields?.[key];
}

/**
 * @param {object} fields Airtable field payload keyed by Airtable column names
 * @param {object} [options]
 * @param {object[]} [options.evidenceRecords] Evidence rows (Airtable column names) for this opportunity
 * @param {boolean} [options.requireEvidenceForQualification=true]
 * @returns {{
 *   ok: boolean,
 *   failures: string[],
 *   challenges: string[],
 *   fieldMapping: Record<string, string>,
 *   sanitizedPreview: object,
 *   lifecycle: { status: string, qualificationRequired: boolean, outreachReadyContract: boolean }
 * }}
 */
export function validateDecisionOpportunityWrite(fields = {}, options = {}) {
  const failures = [];
  const challenges = [];
  const M = MAP_DECISION_OPPORTUNITY;
  const status = String(field(fields, M.status) || "Discovered").trim() || "Discovered";
  const qualificationRequired = VAL_DECISION_STATUS_REQUIRES_QUALIFICATION.includes(status);
  const outreachReadyContract = VAL_DECISION_STATUS_OUTREACH_READY_CONTRACT.includes(status);
  const evidenceRecords = Array.isArray(options.evidenceRecords)
    ? options.evidenceRecords
    : [];
  const requireEvidence =
    options.requireEvidenceForQualification !== false && qualificationRequired;

  // --- Identity (always) ---
  if (isBlank(field(fields, M.opportunityName)) && isBlank(field(fields, M.projectHotelName))) {
    failures.push(
      "Opportunity Name or Project / Hotel Name is required (identifiable project/hotel)."
    );
  }

  // --- Enum validation ---
  const enumChecks = [
    [M.projectType, VAL_DECISION_PROJECT_TYPE, "Project Type"],
    [M.likelyDecisionType, VAL_DECISION_LIKELY_TYPE, "Likely Decision Type"],
    [M.decisionStage, VAL_DECISION_STAGE, "Decision Stage"],
    [M.decisionWindow, VAL_DECISION_WINDOW, "Decision Window"],
    [M.decisionStillOpen, VAL_DECISION_STILL_OPEN, "Decision Still Open"],
    [M.decisionOpenConfidence, VAL_DECISION_OPEN_CONFIDENCE, "Decision Open Confidence"],
    [M.brandStatus, VAL_DECISION_BRAND_STATUS, "Brand Status"],
    [M.operatorStatus, VAL_DECISION_OPERATOR_STATUS, "Operator Status"],
    [M.exclusivityStatus, VAL_DECISION_EXCLUSIVITY_STATUS, "Exclusivity Status"],
    [M.trigger, VAL_DECISION_TRIGGER, "Trigger"],
    [M.warmPathType, VAL_DECISION_WARM_PATH_TYPE, "Warm Path Type"],
    [M.status, VAL_DECISION_STATUS, "Status"],
    [M.recommendedAction, VAL_DECISION_RECOMMENDED_ACTION, "Recommended Action"],
    [M.scoreBand, VAL_DECISION_SCORE_BAND, "Score Band"],
    [M.visibility, VAL_DECISION_VISIBILITY, "Visibility"],
    [M.dataSource, VAL_DECISION_DATA_SOURCE, "Data Source"],
  ];

  for (const [col, allowed, label] of enumChecks) {
    const value = field(fields, col);
    if (!isAllowed(value, allowed)) {
      failures.push(`Invalid ${label}: ${value}`);
    }
  }

  if (field(fields, M.visibility) && field(fields, M.visibility) !== "internal_only") {
    failures.push("Visibility must be internal_only for Decision Opportunities.");
  }

  // --- Qualified / Founder Review / Outreach Ready (shared qualification contract) ---
  const requiresQualificationFields = qualificationRequired || outreachReadyContract;
  if (requiresQualificationFields) {
    if (isBlank(field(fields, M.projectHotelName)) && isBlank(field(fields, M.opportunityName))) {
      failures.push(
        `${status} requires an identifiable Project / Hotel Name (or Opportunity Name).`
      );
    }
    if (isBlank(field(fields, M.country))) {
      failures.push(`${status} requires Country.`);
    }
    if (isBlank(field(fields, M.trigger))) {
      failures.push(`${status} requires Trigger.`);
    }
    if (isBlank(field(fields, M.likelyDecisionType))) {
      failures.push(`${status} requires Likely Decision Type.`);
    }
    if (
      field(fields, M.likelyDecisionType) === "Unknown" &&
      isBlank(field(fields, M.whyNow)) &&
      isBlank(field(fields, M.founderNotes)) &&
      isBlank(field(fields, M.internalNotes))
    ) {
      failures.push(
        `${status} with Likely Decision Type = Unknown requires Why Now or notes with rationale.`
      );
    }
    if (isBlank(field(fields, M.whyNow))) {
      failures.push(`${status} requires Why Now.`);
    }
    if (isBlank(field(fields, M.decisionStillOpen))) {
      failures.push(`${status} requires Decision Still Open.`);
    }
    if (isBlank(field(fields, M.decisionOpenConfidence))) {
      failures.push(`${status} requires Decision Open Confidence.`);
    }
    if (
      (requireEvidence || outreachReadyContract) &&
      evidenceRecords.length === 0 &&
      options.requireEvidenceForQualification !== false
    ) {
      failures.push(
        `${status} requires at least one Decision Opportunity Evidence record.`
      );
    }
  }

  // Founder Review is stricter than Qualified on founder review checkbox
  if (status === "Founder Review") {
    // evidence already required via qualificationRequired
    if (isBlank(field(fields, M.decisionStage))) {
      failures.push("Founder Review requires Decision Stage.");
    }
  }

  // --- Outreach Ready contract (encoded; no auto-transition) ---
  if (outreachReadyContract) {
    if (isBlank(field(fields, M.ownerTarget))) {
      failures.push(
        "Outreach Ready contract: Owner Target (owner/developer) must be identified."
      );
    }
    const hasDecisionMaker = !isBlank(field(fields, M.decisionMakers));
    const hasWarmPath =
      !isBlank(field(fields, M.warmPathContactSource)) ||
      (!isBlank(field(fields, M.warmPathType)) &&
        field(fields, M.warmPathType) !== "Unknown" &&
        field(fields, M.warmPathType) !== "Cold");
    if (!hasDecisionMaker && !hasWarmPath) {
      failures.push(
        "Outreach Ready contract: Decision Makers or actionable Warm Path required."
      );
    }
    if (field(fields, M.decisionStillOpen) === "No") {
      failures.push("Outreach Ready contract: Decision Still Open must not be No.");
    }
    // Evidence already enforced in shared qualification block; keep explicit contract message if somehow skipped.
    if (
      evidenceRecords.length === 0 &&
      options.requireEvidenceForQualification !== false &&
      !failures.some((f) => /Evidence record/i.test(f))
    ) {
      failures.push("Outreach Ready contract: at least one evidence record required.");
    }
    if (!field(fields, M.founderReviewed)) {
      failures.push("Outreach Ready contract: Founder Reviewed must be set.");
    }
  }

  // --- High-risk semantic challenges (fail soft as challenges + hard fail when Confirmed misuse) ---
  const brandStatus = field(fields, M.brandStatus);
  const operatorStatus = field(fields, M.operatorStatus);
  const stillOpen = field(fields, M.decisionStillOpen);
  const openConfidence = field(fields, M.decisionOpenConfidence);
  const likelyType = field(fields, M.likelyDecisionType);
  const exclusivity = field(fields, M.exclusivityStatus);
  const decisionStage = field(fields, M.decisionStage);

  // NOT_PUBLICLY_IDENTIFIED ≠ Confirmed open
  if (
    (brandStatus === "Not Publicly Identified" ||
      operatorStatus === "Not Publicly Identified") &&
    stillOpen === "Yes" &&
    openConfidence === "Confirmed"
  ) {
    failures.push(
      "Decision Open Confidence cannot be Confirmed solely because Brand/Operator Status is Not Publicly Identified. Absence of public identification is not confirmation the decision remains open."
    );
  }

  if (
    brandStatus === "Not Publicly Identified" &&
    stillOpen === "Yes" &&
    (openConfidence === "Probable" || openConfidence === "Inferred" || !openConfidence)
  ) {
    challenges.push(
      "Brand Status = Not Publicly Identified with Decision Still Open = Yes should usually be Probable/Inferred/Unknown confidence, with Why Now explaining further qualification is required."
    );
  }

  // Signed/exclusive vs brand selection open
  const brandClosed =
    brandStatus === "Signed / Exclusive" || exclusivity === "Exclusive" || exclusivity === "Signed";
  const stageClosed = decisionStage === "Exclusive / Signed";
  if (
    stillOpen === "Yes" &&
    brandClosed &&
    (likelyType === "Brand Selection" || likelyType === "Reflag")
  ) {
    failures.push(
      "Contradictory decision state: Decision Still Open = Yes with Brand Status/Exclusivity indicating Signed/Exclusive and decision type Brand Selection/Reflag. Provide rationale in Why Now / Founder Notes or set Decision Still Open to No/Uncertain."
    );
  } else if (stillOpen === "Yes" && (brandClosed || stageClosed)) {
    challenges.push(
      "Decision Still Open = Yes while Brand Status, Exclusivity, or Decision Stage indicates exclusive/signed — verify too-late evidence before founder action."
    );
  }

  if (stillOpen === "No" && status === "Founder Review") {
    challenges.push(
      "Founder Review with Decision Still Open = No — prefer Disqualified / Archived / Monitor unless reviewing a too-late learning case."
    );
  }

  // Warm path must not be invented as Direct without notes
  if (
    field(fields, M.warmPathType) === "Direct" &&
    isBlank(field(fields, M.warmPathContactSource)) &&
    isBlank(field(fields, M.warmPathNotes))
  ) {
    challenges.push(
      "Warm Path Type = Direct without Warm Path Contact / Source or notes — confirm evidence-backed or manually entered path (never infer)."
    );
  }

  const sanitizedPreview = {
    [M.opportunityId]: field(fields, M.opportunityId) || null,
    [M.opportunityName]: field(fields, M.opportunityName) || null,
    [M.projectHotelName]: field(fields, M.projectHotelName) || null,
    [M.country]: field(fields, M.country) || null,
    [M.projectType]: field(fields, M.projectType) || null,
    [M.likelyDecisionType]: likelyType || null,
    [M.decisionStage]: decisionStage || null,
    [M.decisionStillOpen]: stillOpen || null,
    [M.decisionOpenConfidence]: openConfidence || null,
    [M.brandStatus]: brandStatus || null,
    [M.operatorStatus]: operatorStatus || null,
    [M.trigger]: field(fields, M.trigger) || null,
    [M.status]: status,
    [M.visibility]: field(fields, M.visibility) || "internal_only",
    evidenceCount: evidenceRecords.length,
  };

  return {
    ok: failures.length === 0,
    failures,
    challenges,
    fieldMapping: { ...M },
    sanitizedPreview,
    lifecycle: {
      status,
      qualificationRequired,
      outreachReadyContract,
    },
  };
}

/**
 * @param {object} fields Evidence Airtable field payload
 */
export function validateDecisionOpportunityEvidenceWrite(fields = {}) {
  const failures = [];
  const challenges = [];
  const E = MAP_DECISION_OPPORTUNITY_EVIDENCE;

  if (isBlank(field(fields, E.evidenceId)) && isBlank(field(fields, E.sourceName))) {
    failures.push("Evidence ID or Source Name is required.");
  }
  if (isBlank(field(fields, E.decisionOpportunity))) {
    failures.push("Decision Opportunity link is required.");
  }
  if (isBlank(field(fields, E.sourceUrl)) && isBlank(field(fields, E.sourceName))) {
    failures.push("Source URL or Source Name is required for provenance.");
  }

  if (!isAllowed(field(fields, E.sourceType), VAL_EVIDENCE_SOURCE_TYPE)) {
    failures.push(`Invalid Source Type: ${field(fields, E.sourceType)}`);
  }
  if (!isAllowed(field(fields, E.supportsField), VAL_EVIDENCE_SUPPORTS_FIELD)) {
    failures.push(`Invalid Supports Field: ${field(fields, E.supportsField)}`);
  }
  if (!isAllowed(field(fields, E.evidenceConfidence), VAL_EVIDENCE_CONFIDENCE)) {
    failures.push(`Invalid Evidence Confidence: ${field(fields, E.evidenceConfidence)}`);
  }
  if (!isAllowed(field(fields, E.evidenceDirection), VAL_EVIDENCE_DIRECTION)) {
    failures.push(`Invalid Evidence Direction: ${field(fields, E.evidenceDirection)}`);
  }

  const url = field(fields, E.sourceUrl);
  if (url && !/^https?:\/\//i.test(String(url).trim())) {
    failures.push("Source URL must be an http(s) URL when provided.");
  }

  if (
    field(fields, E.evidenceDirection) === "Supports Closed / Too Late" &&
    field(fields, E.supportsField) === "Decision Still Open"
  ) {
    // Valid combination — too-late evidence for openness claims
  }

  if (
    field(fields, E.evidenceConfidence) === "High" &&
    isBlank(field(fields, E.sourceUrl)) &&
    field(fields, E.sourceType) !== "Manual Note" &&
    field(fields, E.sourceType) !== "CoStar Internal"
  ) {
    challenges.push(
      "High evidence confidence without Source URL — prefer attaching a primary URL except Manual Note / CoStar Internal."
    );
  }

  return {
    ok: failures.length === 0,
    failures,
    challenges,
    fieldMapping: { ...E },
    sanitizedPreview: {
      [E.evidenceId]: field(fields, E.evidenceId) || null,
      [E.sourceName]: field(fields, E.sourceName) || null,
      [E.sourceUrl]: field(fields, E.sourceUrl) || null,
      [E.supportsField]: field(fields, E.supportsField) || null,
      [E.evidenceDirection]: field(fields, E.evidenceDirection) || null,
      [E.evidenceConfidence]: field(fields, E.evidenceConfidence) || null,
    },
  };
}

/**
 * Helpers for tests / seed previews — build Airtable-keyed payloads from camelCase.
 * @param {object} input
 */
export function toDecisionOpportunityAirtableFields(input = {}) {
  const M = MAP_DECISION_OPPORTUNITY;
  const out = {};
  for (const [key, col] of Object.entries(M)) {
    if (input[key] !== undefined) out[col] = input[key];
  }
  return out;
}

/**
 * @param {object} input
 */
export function toDecisionOpportunityEvidenceAirtableFields(input = {}) {
  const E = MAP_DECISION_OPPORTUNITY_EVIDENCE;
  const out = {};
  for (const [key, col] of Object.entries(E)) {
    if (input[key] !== undefined) out[col] = input[key];
  }
  return out;
}
