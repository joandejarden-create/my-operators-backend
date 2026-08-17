/**
 * Eligibility layer — runs before alignment. Pure functions.
 */

import {
  ELIGIBILITY_STATUS,
  CANDIDATE_TYPE,
} from "./config.js";
import { isKnownPositive, listValue, scalarValue } from "./adapters/field-state.js";
import { mapOperatingStructureList, ownerStructureKeysFromProject } from "./structure-mapping.js";
import { evaluateGeographicEligibilityFromPresence } from "../operator-intelligence/market-presence.js";

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function structureOverlap(ownerKeys, opKeys) {
  if (!ownerKeys.length) return "unknown_project";
  if (!opKeys.length) return "unknown_operator";
  if (ownerKeys.some((k) => opKeys.includes(k))) return "match";
  if (
    ownerKeys.includes("franchise_only") &&
    (opKeys.includes("third_party_management") || opKeys.includes("franchise_plus_operator"))
  ) {
    return "conditional";
  }
  if (ownerKeys.includes("to_be_confirmed")) return "conditional";
  return "conflict";
}

function scaleOverlap(projectScale, opScales) {
  const s = norm(projectScale);
  if (!s) return "unknown_project";
  if (!opScales.length) return "unknown_operator";
  if (opScales.some((x) => norm(x) === s)) return "match";
  if (opScales.some((x) => norm(x).includes(s) || s.includes(norm(x)))) return "partial";
  return "conflict";
}

/**
 * @returns {{
 *   status: string,
 *   reasons: string[],
 *   conditions: string[],
 *   hardConflicts: string[],
 *   unknowns: string[],
 *   geoEval?: object,
 * }}
 */
export function evaluateEligibility(project, operator) {
  const reasons = [];
  const conditions = [];
  const hardConflicts = [];
  const unknowns = [];

  const active = scalarValue(operator.activeStatus);
  const allowResearchStage =
    Boolean(project?.allowResearchStageLifecycle) || Boolean(operator?.researchStageAllowed);
  const isActiveStatus = /^active$/i.test(String(active || "").trim());
  const isResearchStageStatus = /^research[\s_-]*stage$/i.test(String(active || "").trim());
  if (active && !isActiveStatus) {
    if (allowResearchStage && isResearchStageStatus) {
      conditions.push("Research Stage — internal pilot lane only; not production Active.");
      reasons.push("Research Stage lifecycle accepted for internal ranking lane.");
    } else {
      hardConflicts.push(`Operator status is ${active}, not Active.`);
    }
  } else if (!isKnownPositive(operator.activeStatus) || !isActiveStatus) {
    if (!(allowResearchStage && isResearchStageStatus)) {
      unknowns.push("Operator active status is not confirmed.");
      conditions.push("Confirm operator Active status before outreach.");
    }
  } else {
    reasons.push("Operator is Active.");
  }

  if (operator.candidateType === CANDIDATE_TYPE.BRAND_MANAGED) {
    const excl = project.knownExclusions?.excludesBrandManaged;
    if (excl && excl.value === true) {
      hardConflicts.push("Owner structure preferences exclude brand management.");
    }
    const meta = operator.brandManagedMeta || {};
    const confirmed =
      meta.offersBrandManagementConfirmed || meta.offersBrandManagementVerified;
    if (confirmed) {
      reasons.push("Brand management is independently confirmed as available.");
    } else {
      unknowns.push("Brand-management availability is not independently confirmed.");
      conditions.push(
        "Confirm whether the brand will offer direct management for this project."
      );
    }
  }

  // Geography — Market Presence preferred over raw Active Countries
  const country = scalarValue(project.geography?.country);
  const presenceRecords =
    operator.geography?.marketPresence ||
    operator.geography?.presenceRecords ||
    operator.marketPresence ||
    [];
  const opCountries = listValue(operator.geography?.countries);
  const geoEval = evaluateGeographicEligibilityFromPresence(
    country,
    presenceRecords,
    opCountries
  );
  reasons.push(...(geoEval.reasons || []));
  conditions.push(...(geoEval.conditions || []));
  hardConflicts.push(...(geoEval.hardConflicts || []));
  unknowns.push(...(geoEval.unknowns || []));
  const geo = geoEval.status;

  const mpr = scalarValue(project.geography?.marketPresenceRequirement);
  if (/active country/i.test(mpr)) {
    if (geo === "conflict") {
      if (!hardConflicts.some((h) => /Market presence requirement/i.test(h))) {
        hardConflicts.push(
          "Market presence requirement not met (qualifying current presence required)."
        );
      }
    } else if (geo === "conditional") {
      // Claimed Capability / Historical / Strategic Interest or Active Development — not Ranking Ready geography
      hardConflicts.push(
        "Market presence requirement not met — qualifying Current Managed / Operating / Regional Office presence required."
      );
    } else if (geo === "unknown_operator") {
      unknowns.push("Active country operations required but operator presence unknown.");
      conditions.push(
        "Confirm qualifying Market Presence (Current Managed / Operating / Regional Office)."
      );
    }
  }

  const ownerKeys = ownerStructureKeysFromProject(project);
  const opStructKeys =
    operator.operatingStructures?.canonicalKeys ||
    mapOperatingStructureList(listValue(operator.operatingStructures));
  const st = structureOverlap(ownerKeys, opStructKeys);
  if (st === "conflict") {
    hardConflicts.push("Documented operating structures conflict with owner preferences.");
  } else if (st === "unknown_operator") {
    unknowns.push("Operator supported management structures are unknown.");
    conditions.push("Confirm operating-structure support.");
  } else if (st === "unknown_project") {
    unknowns.push("Owner operating-structure preference is unknown.");
  } else if (st === "conditional") {
    conditions.push("Operating-structure path needs validation (franchise vs management roles).");
  } else {
    reasons.push("Operating-structure preferences overlap with operator-supported structures.");
  }

  const scale = scalarValue(project.hotelSegment);
  const opScales = listValue(operator.chainScales);
  const sc = scaleOverlap(scale, opScales);
  if (sc === "conflict") {
    hardConflicts.push(`Chain-scale mismatch vs project scale (${scale}).`);
  } else if (sc === "unknown_operator") {
    unknowns.push("Operator chain-scale coverage is unknown.");
    conditions.push("Confirm chain-scale experience.");
  } else if (sc === "partial") {
    conditions.push("Chain-scale overlap is partial — validate positioning.");
  } else if (sc === "match") {
    reasons.push(`Supports project chain scale (${scale}).`);
  }

  const dev = scalarValue(project.developmentType);
  const situations = listValue(operator.developmentExperience);
  if (dev && situations.length) {
    const hit = situations.some(
      (s) =>
        norm(s).includes(norm(dev).slice(0, 8)) ||
        norm(dev).includes(norm(s).slice(0, 8)) ||
        (/new build/i.test(dev) && /new build/i.test(s)) ||
        (/conversion|reflag/i.test(dev) && /conversion|reflag/i.test(s)) ||
        (/renovation|reposition|turnaround/i.test(dev) &&
          /renovation|reposition|turnaround/i.test(s))
    );
    if (!hit) {
      conditions.push(`Limited documented overlap with development type (${dev}).`);
    } else {
      reasons.push(`Development experience overlaps (${dev}).`);
    }
  } else if (dev && !situations.length) {
    unknowns.push("Operator development/situation experience is unknown.");
  }

  const breakers = listValue(project.knownExclusions?.dealBreakers);
  const lessIdeal = scalarValue(operator.risksAndConcerns);
  if (breakers.length && lessIdeal) {
    const hit = breakers.some((b) => norm(lessIdeal).includes(norm(b)));
    if (hit) {
      hardConflicts.push("Deal breaker overlaps operator less-ideal situations.");
    }
  }

  let status = ELIGIBILITY_STATUS.ELIGIBLE;
  if (hardConflicts.length) {
    status = ELIGIBILITY_STATUS.NOT_ELIGIBLE;
  } else if (conditions.length || unknowns.length) {
    status = ELIGIBILITY_STATUS.WITH_CONDITIONS;
  } else if (reasons.length >= 3 && geo === "match" && (sc === "match" || sc === "partial")) {
    status = ELIGIBILITY_STATUS.PREFERRED;
  }

  return {
    status,
    reasons,
    conditions,
    hardConflicts,
    unknowns,
    geoEval,
  };
}

export function isOwnerFacingEligible(status) {
  return (
    status === ELIGIBILITY_STATUS.ELIGIBLE ||
    status === ELIGIBILITY_STATUS.PREFERRED ||
    status === ELIGIBILITY_STATUS.WITH_CONDITIONS
  );
}
