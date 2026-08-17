/**
 * Operating Structure Alignment layer.
 */

import { CANDIDATE_TYPE } from "./config.js";
import { listValue } from "./adapters/field-state.js";
import {
  mapOperatingStructureList,
  ownerStructureKeysFromProject,
} from "./structure-mapping.js";

/**
 * @returns {{
 *   score: number|null,
 *   state: 'known'|'unknown'|'not_applicable',
 *   rationale: string,
 *   ownerKeys: string[],
 *   operatorKeys: string[],
 *   validationItems: string[],
 * }}
 */
export function evaluateOperatingStructureAlignment(project, operator) {
  const ownerKeys = ownerStructureKeysFromProject(project);

  if (operator.candidateType === CANDIDATE_TYPE.BRAND_MANAGED) {
    if (!ownerKeys.length) {
      return {
        score: null,
        state: "unknown",
        rationale: "Owner structure preference unknown; brand-managed path needs confirmation.",
        ownerKeys,
        operatorKeys: ["brand_managed"],
        validationItems: ["Confirm owner openness to brand management."],
      };
    }
    if (ownerKeys.includes("brand_managed") || ownerKeys.includes("third_party_management")) {
      return {
        score: 88,
        state: "known",
        rationale: "Owner preferences are compatible with a brand-managed path.",
        ownerKeys,
        operatorKeys: ["brand_managed"],
        validationItems: [],
      };
    }
    if (ownerKeys.includes("to_be_confirmed")) {
      return {
        score: 55,
        state: "known",
        rationale: "Owner structure is to be confirmed — brand-managed remains possible.",
        ownerKeys,
        operatorKeys: ["brand_managed"],
        validationItems: ["Confirm whether brand management is acceptable."],
      };
    }
    return {
      score: 15,
      state: "known",
      rationale: "Owner preferences do not currently indicate brand management.",
      ownerKeys,
      operatorKeys: ["brand_managed"],
      validationItems: ["Confirm structure path before pursuing brand management."],
    };
  }

  const opKeys =
    operator.operatingStructures?.canonicalKeys ||
    mapOperatingStructureList(listValue(operator.operatingStructures));

  if (!ownerKeys.length) {
    return {
      score: null,
      state: "unknown",
      rationale: "Owner operating-structure preference is unknown.",
      ownerKeys,
      operatorKeys: opKeys,
      validationItems: ["Capture preferred operating structure on the deal."],
    };
  }
  if (!opKeys.length) {
    return {
      score: null,
      state: "unknown",
      rationale: "Operator supported structures are unknown.",
      ownerKeys,
      operatorKeys: opKeys,
      validationItems: ["Confirm management structures the operator supports."],
    };
  }

  const exact = ownerKeys.filter((k) => opKeys.includes(k));
  if (exact.length) {
    return {
      score: 100,
      state: "known",
      rationale: `Structure alignment on: ${exact.join(", ")}.`,
      ownerKeys,
      operatorKeys: opKeys,
      validationItems: [],
    };
  }

  // Franchise brand agreement + third-party operator path
  if (
    (ownerKeys.includes("franchise_plus_operator") || ownerKeys.includes("franchise_only")) &&
    (opKeys.includes("third_party_management") || opKeys.includes("franchise_plus_operator"))
  ) {
    return {
      score: 72,
      state: "known",
      rationale:
        "Franchise brand path with third-party operating support appears directionally aligned — validate role split.",
      ownerKeys,
      operatorKeys: opKeys,
      validationItems: ["Confirm franchise vs operator responsibilities."],
    };
  }

  return {
    score: 22,
    state: "known",
    rationale: "Limited overlap between owner structure preferences and operator-supported structures.",
    ownerKeys,
    operatorKeys: opKeys,
    validationItems: ["Validate operating structure feasibility."],
  };
}
