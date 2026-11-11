/**
 * Commercial contact role classifier V1 (hotel-generic).
 *
 * Distinguishes buyer vs entry-point vs intermediary vs unrelated contact
 * without weakening Sales Ready eligibility.
 *
 * Zero-paid. No hotel-specific organization hardcoding.
 */
export const COMMERCIAL_CONTACT_ROLE_CLASSIFIER_V1 =
  "gdi-commercial-contact-role-classifier-v1";

export const COMMERCIAL_CONTACT_ROLE = Object.freeze({
  DIRECT_BUYER: "DIRECT_BUYER",
  RELEVANT_COMMERCIAL_ENTRY_POINT: "RELEVANT_COMMERCIAL_ENTRY_POINT",
  PARTNERSHIP_INTERMEDIARY: "PARTNERSHIP_INTERMEDIARY",
  UNRELATED_CONTACT: "UNRELATED_CONTACT",
  UNKNOWN: "UNKNOWN",
});

export const COMMERCIAL_OPPORTUNITY_CLASS = Object.freeze({
  DIRECT_DEMAND: "DIRECT_DEMAND",
  RELATIONSHIP_DEVELOPMENT: "RELATIONSHIP_DEVELOPMENT",
  PREFERRED_LODGING_PARTNERSHIP: "PREFERRED_LODGING_PARTNERSHIP",
  INTERMEDIARY_FEEDER: "INTERMEDIARY_FEEDER",
  SCALING_CORPORATE_ACCOUNT: "SCALING_CORPORATE_ACCOUNT",
});

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Classify a named contact's commercial role relative to the demand thesis.
 */
export function classifyCommercialContactRole(opportunity = {}, contact = null) {
  const c = contact || opportunity.primaryContact || {};
  const name = text(c.name || c.fullName);
  const title = text(c.title || c.role || c.jobTitle);
  const blob = [
    title,
    opportunity.hotelOpportunityThesis,
    opportunity.summaryWhat,
    opportunity.demandMechanismSummary,
    opportunity.organizationName,
    opportunity.programmeName,
    opportunity.demandSegmentId,
    opportunity.researchSegment,
  ]
    .map(text)
    .join(" ");

  if (!name) {
    return {
      version: COMMERCIAL_CONTACT_ROLE_CLASSIFIER_V1,
      role: COMMERCIAL_CONTACT_ROLE.UNKNOWN,
      eligibleAsSalesReadyContact: false,
      reasons: ["no_named_contact"],
    };
  }

  const intermediaryTitle =
    /\b(dmc|destination management|wedding planner|incentive|tour operator|travel (?:agency|agent|manager)|group travel|event (?:agency|planner)|PCO|professional congress)\b/i.test(
      `${title} ${blob}`,
    );
  const buyerTitle =
    /\b(procurement|buyer|sourcing|meeting(s)? manager|event(s)? manager|travel manager|housing|room block|catering manager|director of (?:events|meetings|sales)|VP .{0,20}(events|meetings|travel))\b/i.test(
      title,
    );
  const seg = text(opportunity.demandSegmentId || opportunity.researchSegment);
  const isScalingSeg = /scaling/i.test(seg);
  const scalingEntry =
    isScalingSeg &&
    /\b(operations|office manager|facilities|HR|people|talent|general manager|managing director|founder|CEO|COO|site lead|lab manager|plant manager)\b/i.test(
      title,
    ) &&
    /scaling|facility|office|expansion|hiring|relocati|training|visiting/i.test(blob);
  const intermediarySeg =
    /travel_trade|professional_event|wedding|incentive|dmc/i.test(
      `${seg} ${text(opportunity.organizationName)}`,
    );

  if (
    /press|media|spokesperson|investor relations|careers@|info@only/i.test(
      `${title} ${c.email || ""}`,
    ) &&
    !buyerTitle
  ) {
    return {
      version: COMMERCIAL_CONTACT_ROLE_CLASSIFIER_V1,
      role: COMMERCIAL_CONTACT_ROLE.UNRELATED_CONTACT,
      eligibleAsSalesReadyContact: false,
      reasons: ["press_or_generic_unrelated"],
    };
  }

  if (intermediaryTitle || intermediarySeg) {
    return {
      version: COMMERCIAL_CONTACT_ROLE_CLASSIFIER_V1,
      role: COMMERCIAL_CONTACT_ROLE.PARTNERSHIP_INTERMEDIARY,
      eligibleAsSalesReadyContact: true,
      reasons: intermediaryTitle
        ? ["intermediary_planner_or_dmc_title"]
        : ["intermediary_segment"],
      commercialOpportunityClass: COMMERCIAL_OPPORTUNITY_CLASS.INTERMEDIARY_FEEDER,
    };
  }

  if (buyerTitle) {
    return {
      version: COMMERCIAL_CONTACT_ROLE_CLASSIFIER_V1,
      role: COMMERCIAL_CONTACT_ROLE.DIRECT_BUYER,
      eligibleAsSalesReadyContact: true,
      reasons: ["buyer_or_meetings_title"],
      commercialOpportunityClass: COMMERCIAL_OPPORTUNITY_CLASS.DIRECT_DEMAND,
    };
  }

  if (scalingEntry) {
    return {
      version: COMMERCIAL_CONTACT_ROLE_CLASSIFIER_V1,
      role: COMMERCIAL_CONTACT_ROLE.RELEVANT_COMMERCIAL_ENTRY_POINT,
      eligibleAsSalesReadyContact: true,
      reasons: ["scaling_activity_connected_named_contact"],
      commercialOpportunityClass:
        COMMERCIAL_OPPORTUNITY_CLASS.SCALING_CORPORATE_ACCOUNT,
    };
  }

  if (title) {
    return {
      version: COMMERCIAL_CONTACT_ROLE_CLASSIFIER_V1,
      role: COMMERCIAL_CONTACT_ROLE.RELEVANT_COMMERCIAL_ENTRY_POINT,
      eligibleAsSalesReadyContact: true,
      reasons: ["named_role_connected_to_organisation_activity"],
      commercialOpportunityClass: isScalingSeg
        ? COMMERCIAL_OPPORTUNITY_CLASS.SCALING_CORPORATE_ACCOUNT
        : COMMERCIAL_OPPORTUNITY_CLASS.RELATIONSHIP_DEVELOPMENT,
    };
  }

  return {
    version: COMMERCIAL_CONTACT_ROLE_CLASSIFIER_V1,
    role: COMMERCIAL_CONTACT_ROLE.UNKNOWN,
    eligibleAsSalesReadyContact: Boolean(name),
    reasons: ["named_without_role_context"],
    commercialOpportunityClass: COMMERCIAL_OPPORTUNITY_CLASS.RELATIONSHIP_DEVELOPMENT,
  };
}

/**
 * Classify opportunity commercial posture for sales labeling (not an eligibility gate).
 */
export function classifyCommercialOpportunityClass(opportunity = {}) {
  const contactRole = classifyCommercialContactRole(opportunity);
  if (contactRole.commercialOpportunityClass) {
    return {
      version: COMMERCIAL_CONTACT_ROLE_CLASSIFIER_V1,
      class: contactRole.commercialOpportunityClass,
      contactRole: contactRole.role,
    };
  }
  const seg = text(opportunity.demandSegmentId || opportunity.researchSegment);
  if (/preferred_lodging/i.test(seg)) {
    return {
      version: COMMERCIAL_CONTACT_ROLE_CLASSIFIER_V1,
      class: COMMERCIAL_OPPORTUNITY_CLASS.PREFERRED_LODGING_PARTNERSHIP,
      contactRole: contactRole.role,
    };
  }
  if (/travel_trade|professional_event|wedding|incentive|dmc/i.test(seg + text(opportunity.organizationName))) {
    return {
      version: COMMERCIAL_CONTACT_ROLE_CLASSIFIER_V1,
      class: COMMERCIAL_OPPORTUNITY_CLASS.INTERMEDIARY_FEEDER,
      contactRole: contactRole.role,
    };
  }
  if (/scaling/i.test(seg)) {
    return {
      version: COMMERCIAL_CONTACT_ROLE_CLASSIFIER_V1,
      class: COMMERCIAL_OPPORTUNITY_CLASS.SCALING_CORPORATE_ACCOUNT,
      contactRole: contactRole.role,
    };
  }
  return {
    version: COMMERCIAL_CONTACT_ROLE_CLASSIFIER_V1,
    class: COMMERCIAL_OPPORTUNITY_CLASS.DIRECT_DEMAND,
    contactRole: contactRole.role,
  };
}
