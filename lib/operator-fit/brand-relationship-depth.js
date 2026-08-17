/**
 * Brand Relationship Depth — diagnostics (does not invent Project Approval).
 */

export const BRAND_EXPERIENCE = Object.freeze({
  REPEATED_VERIFIED: "Repeated Verified Experience",
  VERIFIED_CURRENT: "Verified Current Experience",
  VERIFIED_HISTORICAL: "Verified Historical Experience",
  ANNOUNCED: "Announced Experience",
  OPERATOR_REPORTED: "Operator Reported",
  NO_EVIDENCE: "No Evidence Found",
  UNKNOWN: "Unknown",
});

export const PARENT_RELATIONSHIP = Object.freeze({
  STRONG: "Strong Documented Relationship",
  DOCUMENTED: "Documented Relationship",
  LIMITED: "Limited Documented Relationship",
  HISTORICAL: "Historical Relationship",
  UNKNOWN: "Unknown",
});

export const APPROVAL_STATUS = Object.freeze({
  APPROVED: "Approved",
  APPROVED_WITH_CONDITIONS: "Approved With Conditions",
  HISTORICALLY_APPROVED: "Historically Approved",
  UNKNOWN: "Approval Unknown",
  UNSUPPORTED: "Unsupported",
});

export const PROJECT_APPROVAL = Object.freeze({
  CONFIRMED: "Confirmed",
  BRAND_CONFIRMATION_REQUIRED: "Brand Confirmation Required",
  OPERATOR_CONFIRMATION_REQUIRED: "Operator Confirmation Required",
  BOTH_MUST_CONFIRM: "Both Parties Must Confirm",
  KNOWN_CONFLICT: "Known Conflict",
  NOT_APPLICABLE: "Not Applicable",
});

export const VALIDATION_TIMING = Object.freeze({
  ALREADY_SUFFICIENT: "Already sufficient",
  BEFORE_SHORTLIST: "Validate before shortlist",
  BEFORE_OUTREACH: "Validate before outreach",
  DURING_OUTREACH: "Validate during outreach",
  BEFORE_PROPOSAL: "Validate before proposal comparison",
  BEFORE_FINAL: "Validate before final recommendation",
});

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/** True only for explicit approval statuses — not "approval" substrings in limitations. */
export function isExplicitApprovalStatus(status) {
  const s = String(status || "").trim();
  return /^(approved|approved with conditions|historically approved)$/i.test(s);
}

/** Never treat portfolio notes as project approval. */
export function projectApprovalFromEvidence({ knownConflict = false, confirmed = false, brandPreferred = true } = {}) {
  if (!brandPreferred) return PROJECT_APPROVAL.NOT_APPLICABLE;
  if (knownConflict) return PROJECT_APPROVAL.KNOWN_CONFLICT;
  if (confirmed) return PROJECT_APPROVAL.CONFIRMED;
  return PROJECT_APPROVAL.BOTH_MUST_CONFIRM;
}

/**
 * Classify depth layers from a relationship row + optional brand target.
 */
export function classifyBrandRelationshipDepth(rel = {}, { targetBrand = null } = {}) {
  const brand = rel.brand || targetBrand || "—";
  const status = String(rel.relationshipStatus || rel.status || "");
  const currentOrHistorical = String(rel.currentOrHistorical || "");
  const hist =
    /historical/i.test(status) ||
    /historical/i.test(currentOrHistorical) ||
    /historical/i.test(String(rel.relationshipGeography || rel.geography || ""));
  const announced = /announc/i.test(status) || /announc/i.test(String(rel.operatingOrAnnounced || ""));
  const verifiedCurrent =
    /verified current/i.test(status) ||
    (/current/i.test(currentOrHistorical) && /verif/i.test(status)) ||
    (/operating/i.test(String(rel.operatingOrAnnounced || "")) && /verif|current/i.test(status));
  const operatorReported = /operator.?reported|self.?report/i.test(status);

  let brandExperience = BRAND_EXPERIENCE.UNKNOWN;
  if (verifiedCurrent && !hist) brandExperience = BRAND_EXPERIENCE.VERIFIED_CURRENT;
  else if (hist) brandExperience = BRAND_EXPERIENCE.VERIFIED_HISTORICAL;
  else if (announced) brandExperience = BRAND_EXPERIENCE.ANNOUNCED;
  else if (operatorReported) brandExperience = BRAND_EXPERIENCE.OPERATOR_REPORTED;
  else if (!status) brandExperience = BRAND_EXPERIENCE.NO_EVIDENCE;

  let parentRelationship = PARENT_RELATIONSHIP.UNKNOWN;
  if (verifiedCurrent && !hist) parentRelationship = PARENT_RELATIONSHIP.DOCUMENTED;
  else if (hist) parentRelationship = PARENT_RELATIONSHIP.HISTORICAL;
  else if (announced) parentRelationship = PARENT_RELATIONSHIP.LIMITED;

  let approvalStatus = APPROVAL_STATUS.UNKNOWN;
  const rawApproval = rel.approvalStatus || "";
  if (isExplicitApprovalStatus(rawApproval)) {
    if (/conditions/i.test(rawApproval)) approvalStatus = APPROVAL_STATUS.APPROVED_WITH_CONDITIONS;
    else if (/historically/i.test(rawApproval)) approvalStatus = APPROVAL_STATUS.HISTORICALLY_APPROVED;
    else approvalStatus = APPROVAL_STATUS.APPROVED;
  } else if (/unsupported|incompat|conflict/i.test(rawApproval)) {
    approvalStatus = APPROVAL_STATUS.UNSUPPORTED;
  }
  // "Property-scoped — not global approval" → Approval Unknown (not Approved)

  const projectApproval = projectApprovalFromEvidence({
    knownConflict: approvalStatus === APPROVAL_STATUS.UNSUPPORTED,
    confirmed: false,
    brandPreferred: true,
  });

  let timing = VALIDATION_TIMING.DURING_OUTREACH;
  if (projectApproval === PROJECT_APPROVAL.NOT_APPLICABLE) timing = VALIDATION_TIMING.ALREADY_SUFFICIENT;
  else if (projectApproval === PROJECT_APPROVAL.KNOWN_CONFLICT) timing = VALIDATION_TIMING.BEFORE_SHORTLIST;
  else if (brandExperience === BRAND_EXPERIENCE.NO_EVIDENCE || brandExperience === BRAND_EXPERIENCE.UNKNOWN) {
    timing = VALIDATION_TIMING.BEFORE_OUTREACH;
  } else {
    timing = VALIDATION_TIMING.DURING_OUTREACH;
  }

  return {
    brand,
    parentCompany: rel.parentCompany || null,
    brandExperience,
    parentRelationship,
    approvalStatus,
    projectApproval,
    geography: rel.relationshipGeography || rel.geography || null,
    evidence: rel.evidence || rel.limitations || "",
    sourceIds: rel.sourceIds || [],
    limitations: rel.limitations || "",
    validationTiming: timing,
    action:
      projectApproval === PROJECT_APPROVAL.CONFIRMED
        ? "None — project approval confirmed"
        : "Confirm project-specific brand approval during outreach (do not infer from portfolio)",
  };
}

export function brandsMatch(a, b) {
  const na = norm(a);
  const nb = norm(b);
  if (!na || !nb) return false;
  return na === nb || na.includes(nb) || nb.includes(na);
}
