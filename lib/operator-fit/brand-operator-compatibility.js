/**
 * Brand–Operator Compatibility layer (categorical; limited numeric for composition).
 * Brand Relationship ≠ Project Approval (evidence-closure ADR 1.2–1.3).
 */

import { BRAND_OPERATOR_COMPAT, CANDIDATE_TYPE } from "./config.js";
import { listValue } from "./adapters/field-state.js";
import { isExplicitApprovalStatus } from "./brand-relationship-depth.js";

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function projectApprovalValidation(preferred) {
  if (!preferred.length) return [];
  return [
    "Confirm project-specific brand approval for preferred brand(s) — portfolio relationship is not project approval.",
  ];
}

/**
 * @returns {{
 *   category: string,
 *   numericForComposition: number|null,
 *   state: 'known'|'unknown'|'not_applicable',
 *   rationale: string,
 *   validationItems: string[],
 *   projectApproval: string,
 * }}
 */
export function evaluateBrandOperatorCompatibility(project, operator) {
  const preferred = listValue(project.selectedOrEvaluatedBrands);

  if (operator.candidateType === CANDIDATE_TYPE.BRAND_MANAGED) {
    const meta = operator.brandManagedMeta || {};
    if (!meta.brandName) {
      return {
        category: BRAND_OPERATOR_COMPAT.UNKNOWN,
        numericForComposition: 0,
        state: "unknown",
        rationale: "Brand-managed candidate missing brand identity.",
        validationItems: ["Confirm brand identity for brand-managed path."],
        projectApproval: "Brand Confirmation Required",
      };
    }
    const confirmed =
      meta.offersBrandManagementConfirmed || meta.offersBrandManagementVerified;
    if (confirmed) {
      return {
        category: BRAND_OPERATOR_COMPAT.SUPPORTED,
        numericForComposition: 90,
        state: "known",
        rationale: `${meta.brandName} brand management is independently supported.`,
        validationItems: projectApprovalValidation([meta.brandName]),
        projectApproval: "Both Parties Must Confirm",
      };
    }
    return {
      category: BRAND_OPERATOR_COMPAT.UNKNOWN,
      numericForComposition: 0,
      state: "unknown",
      rationale: "Brand-management availability is not independently confirmed.",
      validationItems: [
        "Confirm whether the brand will offer direct management for this project.",
        ...projectApprovalValidation([meta.brandName]),
      ],
      projectApproval: "Brand Confirmation Required",
    };
  }

  if (!preferred.length) {
    return {
      category: BRAND_OPERATOR_COMPAT.NOT_APPLICABLE,
      numericForComposition: null,
      state: "not_applicable",
      rationale: "No preferred/evaluated brand on the project.",
      validationItems: [],
      projectApproval: "Not Applicable",
    };
  }

  const opBrands = [
    ...listValue(operator.brandsOperated),
    ...listValue(operator.brandFamilies),
  ];
  if (!opBrands.length) {
    return {
      category: BRAND_OPERATOR_COMPAT.UNKNOWN,
      numericForComposition: 0,
      state: "unknown",
      rationale: "Operator brand relationships are unknown.",
      validationItems: [
        "Confirm brand operating experience for preferred brands.",
        ...projectApprovalValidation(preferred),
      ],
      projectApproval: "Both Parties Must Confirm",
    };
  }

  const hits = preferred.filter((b) =>
    opBrands.some((o) => norm(o) === norm(b) || norm(o).includes(norm(b)) || norm(b).includes(norm(o)))
  );

  const approvals = Array.isArray(operator.brandApprovals) ? operator.brandApprovals : [];
  const approvedHits = hits.filter((b) =>
    approvals.some(
      (a) =>
        norm(a.brand) === norm(b) &&
        isExplicitApprovalStatus(a.status) &&
        !/historical/i.test(String(a.status || ""))
    )
  );
  const historicalHits = hits.filter((b) =>
    approvals.some(
      (a) =>
        norm(a.brand) === norm(b) &&
        (/historical/i.test(String(a.status || "")) ||
          /historical/i.test(String(a.currentOrHistorical || "")))
    )
  );

  if (approvedHits.length) {
    return {
      category: BRAND_OPERATOR_COMPAT.SUPPORTED,
      numericForComposition: 88,
      state: "known",
      rationale: `Verified current brand relationship for: ${approvedHits.join(", ")} (not project approval).`,
      validationItems: projectApprovalValidation(preferred),
      projectApproval: "Both Parties Must Confirm",
    };
  }
  if (hits.length === preferred.length) {
    return {
      category: BRAND_OPERATOR_COMPAT.PARTIALLY_SUPPORTED,
      numericForComposition: historicalHits.length === hits.length ? 48 : 70,
      state: "known",
      rationale: historicalHits.length
        ? "Preferred brands appear with historical emphasis; lower confidence than verified current."
        : "Preferred brands appear in portfolio; formal project approval not confirmed.",
      validationItems: projectApprovalValidation(preferred),
      projectApproval: "Both Parties Must Confirm",
    };
  }
  if (hits.length > 0) {
    return {
      category: BRAND_OPERATOR_COMPAT.PARTIALLY_SUPPORTED,
      numericForComposition: 55,
      state: "known",
      rationale: `Partial brand portfolio overlap (${hits.join(", ")}).`,
      validationItems: [
        "Validate brand relationship for non-overlapping preferred brands.",
        ...projectApprovalValidation(preferred),
      ],
      projectApproval: "Both Parties Must Confirm",
    };
  }
  return {
    category: BRAND_OPERATOR_COMPAT.UNSUPPORTED,
    numericForComposition: 20,
    state: "known",
    rationale: "No documented overlap with preferred brands.",
    validationItems: [
      "Confirm whether a brand relationship and project approval can be obtained.",
      ...projectApprovalValidation(preferred),
    ],
    projectApproval: "Both Parties Must Confirm",
  };
}
