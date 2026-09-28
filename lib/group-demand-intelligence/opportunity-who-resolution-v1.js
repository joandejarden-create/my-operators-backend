/**
 * Global GDI WHO / HOW resolution helpers (hotel-agnostic).
 * WHO = stakeholder identity + relevance. HOW = reachability.
 * Does not invent people; Surfe AUTO remains off.
 */

import {
  gradeContactCompleteness,
  preferredRolesForOpportunity,
  classifyCommercialMotion,
  PUBLIC_CONTACT_CEILING_REASON,
  computeNextContactResearchAt,
} from "./contact-completeness-v1.js";

export const WHO_PATH_CLASS = Object.freeze({
  NAMED_DIRECT: "NAMED_DIRECT",
  NAMED_PARTIAL: "NAMED_PARTIAL",
  FUNCTIONAL: "FUNCTIONAL",
  ORG_PATH: "ORG_PATH",
  NO_CONTACT_AFTER_RESEARCH: "NO_CONTACT_AFTER_RESEARCH",
  NOT_RESEARCHED: "NOT_RESEARCHED",
});

export const CONTACT_RESEARCH_STATE = Object.freeze({
  ATTEMPTED: "ATTEMPTED",
  NOT_RESEARCHED: "NOT_RESEARCHED",
  PUBLIC_DATA_CEILING: "PUBLIC_DATA_CEILING",
});

function looksLikeNamedPerson(name = "") {
  const n = String(name || "").trim();
  if (!n || n === "UNKNOWN") return false;
  if (/^(booth|press|event|survey|interested|powered|exhibitor|contact us)\b/i.test(n)) {
    return false;
  }
  return n.split(/\s+/).length >= 2;
}

/**
 * Classify WHO/HOW path from existing opportunity contact fields.
 */
export function classifyWhoHowPath(opp = {}) {
  const c = opp.primaryContact && typeof opp.primaryContact === "object" ? opp.primaryContact : null;
  const name = c?.name || c?.fullName || "";
  const email = c?.email || "";
  const phone = c?.phone || "";
  const functional = opp.functionalContactEmail || opp.functionalContact || "";
  const gradeInfo = gradeContactCompleteness(opp);
  const roles = preferredRolesForOpportunity(opp);

  if (looksLikeNamedPerson(name) && (email || phone)) {
    return {
      pathClass: WHO_PATH_CLASS.NAMED_DIRECT,
      grade: gradeInfo.grade || "A",
      commercialMotion: roles.commercialMotion,
      preferredRoles: roles.preferredRoles,
      researchState: CONTACT_RESEARCH_STATE.ATTEMPTED,
    };
  }
  if (looksLikeNamedPerson(name)) {
    return {
      pathClass: WHO_PATH_CLASS.NAMED_PARTIAL,
      grade: gradeInfo.grade || "B",
      commercialMotion: roles.commercialMotion,
      preferredRoles: roles.preferredRoles,
      researchState: CONTACT_RESEARCH_STATE.ATTEMPTED,
    };
  }
  if (functional || gradeInfo.tier === "FUNCTIONAL_CONTACT" || gradeInfo.tier === "FUNCTIONAL") {
    return {
      pathClass: WHO_PATH_CLASS.FUNCTIONAL,
      grade: gradeInfo.grade || "C",
      commercialMotion: roles.commercialMotion,
      preferredRoles: roles.preferredRoles,
      researchState: CONTACT_RESEARCH_STATE.ATTEMPTED,
    };
  }
  // ORG_PATH requires an explicit org/contact path — not merely officialSource discovery URL
  if (
    opp.organizationContactUrl ||
    opp.officialContactPath ||
    opp.contactOfficialUrl ||
    opp.orgContactPath ||
    opp.organizationContactPath
  ) {
    return {
      pathClass: WHO_PATH_CLASS.ORG_PATH,
      grade: gradeInfo.grade || "D",
      commercialMotion: roles.commercialMotion,
      preferredRoles: roles.preferredRoles,
      researchState: CONTACT_RESEARCH_STATE.ATTEMPTED,
    };
  }

  if (
    opp.contactResearchAttempted === true ||
    opp.contactResearchState === CONTACT_RESEARCH_STATE.ATTEMPTED ||
    opp.contactResearchState === CONTACT_RESEARCH_STATE.PUBLIC_DATA_CEILING ||
    opp.publicContactCeilingReason
  ) {
    return {
      pathClass: WHO_PATH_CLASS.NO_CONTACT_AFTER_RESEARCH,
      grade: "E",
      commercialMotion: roles.commercialMotion,
      preferredRoles: roles.preferredRoles,
      researchState: opp.publicContactCeilingReason
        ? CONTACT_RESEARCH_STATE.PUBLIC_DATA_CEILING
        : CONTACT_RESEARCH_STATE.ATTEMPTED,
      ceilingReason: opp.publicContactCeilingReason || null,
    };
  }

  return {
    pathClass: WHO_PATH_CLASS.NOT_RESEARCHED,
    grade: "E",
    commercialMotion: roles.commercialMotion,
    preferredRoles: roles.preferredRoles,
    researchState: CONTACT_RESEARCH_STATE.NOT_RESEARCHED,
  };
}

/**
 * Mark WHO research as attempted without inventing a person.
 * Used when public sources were checked and nothing usable was found.
 */
export function applyPublicDataCeiling(opp = {}, reason = "PUBLIC_DATA_CEILING") {
  const ceiling =
    PUBLIC_CONTACT_CEILING_REASON[reason] || reason || "PUBLIC_DATA_CEILING";
  const nextAt = computeNextContactResearchAt(opp, ceiling);
  return {
    ...opp,
    contactResearchAttempted: true,
    contactResearchState: CONTACT_RESEARCH_STATE.PUBLIC_DATA_CEILING,
    publicContactCeilingReason: ceiling,
    whoPathClass: WHO_PATH_CLASS.NO_CONTACT_AFTER_RESEARCH,
    nextContactResearchAt: nextAt || opp.nextContactResearchAt || null,
    nextContactResearchReason: ceiling,
  };
}

/**
 * Stamp WHO/HOW classification onto opportunity from existing contact evidence.
 * Does not invent named stakeholders (JEV PERSON APPLY = NO).
 */
export function applyGdiWhoHowResolution(opp = {}, opts = {}) {
  const classified = classifyWhoHowPath(opp);
  const next = {
    ...opp,
    whoPathClass: classified.pathClass,
    contactPathClass: opp.contactPathClass || classified.pathClass,
    contactGrade: classified.grade,
    commercialMotionForWho: classified.commercialMotion,
    preferredWhoRoles: classified.preferredRoles,
    contactResearchState: classified.researchState,
  };

  if (classified.researchState !== CONTACT_RESEARCH_STATE.NOT_RESEARCHED) {
    next.contactResearchAttempted = true;
  } else if (opts.markAttempted === true) {
    // Explicit research pass with no named WHO found
    const ceilingOpp = applyPublicDataCeiling(next, opts.ceilingReason || "PUBLIC_DATA_CEILING");
    return {
      opportunity: ceilingOpp,
      classified: {
        pathClass: WHO_PATH_CLASS.NO_CONTACT_AFTER_RESEARCH,
        grade: "E",
        commercialMotion: classified.commercialMotion,
        preferredRoles: classified.preferredRoles,
        researchState: CONTACT_RESEARCH_STATE.PUBLIC_DATA_CEILING,
        ceilingReason: ceilingOpp.publicContactCeilingReason,
      },
    };
  }

  if (classified.ceilingReason) {
    next.publicContactCeilingReason = classified.ceilingReason;
  }

  return { opportunity: next, classified };
}

export function whoResearchAttempted(opp = {}) {
  const c = classifyWhoHowPath(opp);
  return c.researchState !== CONTACT_RESEARCH_STATE.NOT_RESEARCHED;
}

export { classifyCommercialMotion, preferredRolesForOpportunity };
