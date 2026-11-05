/**
 * Canonical identity for Professional Event Planner Partnership opportunities.
 * Primary display title = verified planner / agency name only.
 */
import {
  PROFESSIONAL_EVENT_PLANNER_CLASSIFICATION_LINE,
  PROFESSIONAL_EVENT_PLANNER_SEGMENT_ID,
  PROFESSIONAL_EVENT_PLANNER_SUBTYPE,
  PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP_V1,
  text,
} from "./professional-event-planner-contract-v1.js";

export const PROFESSIONAL_EVENT_PLANNER_IDENTITY_V1 =
  "gdi-professional-event-planner-identity-v1";

/**
 * Strip legacy subtype suffixes and research prose from display titles.
 */
export function normalizePlannerDisplayTitle(rawTitle, plannerName) {
  const planner = text(plannerName);
  if (
    planner &&
    planner.length <= 80 &&
    !/professional event planner|partnership/i.test(planner)
  ) {
    return planner;
  }
  const raw = text(rawTitle);
  if (!raw) return planner || "Event Planner";
  const withoutSuffix = raw
    .replace(/\s*[—–-]\s*Professional Event Planner Partnership.*$/i, "")
    .replace(/\s*[—–-]\s*Wedding and Corporate Events.*$/i, "")
    .trim();
  const left = withoutSuffix.split(/\s*[—–]\s*/)[0].trim();
  if (
    left &&
    left.length <= 80 &&
    !/wedding and corporate|in mexico city|research continuation/i.test(left)
  ) {
    return left;
  }
  return planner || left || "Event Planner";
}

export function buildProfessionalEventPlannerIdentity({
  planner = {},
  hotel = {},
  opportunity = {},
} = {}) {
  const plannerName = text(
    planner.name ||
      planner.companyName ||
      planner.organizationName ||
      opportunity.organizationName ||
      opportunity.plannerName,
  );
  const displayTitle = normalizePlannerDisplayTitle(
    opportunity.displayTitle || opportunity.title,
    plannerName,
  );
  const plannerKey = displayTitle
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
  const hotelId = text(hotel.hotelId || opportunity.hotelId) || "unknown_hotel";
  const plannerOrgId =
    text(
      opportunity.plannerOrgId ||
        planner.plannerOrgId ||
        planner.organizationId,
    ) || null;

  return {
    version: PROFESSIONAL_EVENT_PLANNER_IDENTITY_V1,
    contract: PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP_V1,
    displayTitle,
    primaryOrganizationName: displayTitle,
    plannerName: displayTitle,
    plannerAliases: [plannerName, opportunity.organizationName]
      .map(text)
      .filter((x, i, a) => x && a.indexOf(x) === i && x !== displayTitle),
    officeLocation:
      text(planner.officeLocation || opportunity.officeLocation) || null,
    verifiedServiceArea:
      text(planner.verifiedServiceArea || opportunity.verifiedServiceArea) ||
      null,
    hotelId,
    demandSegmentId: PROFESSIONAL_EVENT_PLANNER_SEGMENT_ID,
    demandOpportunitySubtype: PROFESSIONAL_EVENT_PLANNER_SUBTYPE,
    classificationLine: PROFESSIONAL_EVENT_PLANNER_CLASSIFICATION_LINE,
    plannerOrgId,
    /** Stable within hotel — one planner account opportunity per agency. */
    plannerPartnershipFingerprint: `pep_${hotelId}_${plannerOrgId || plannerKey}`,
    namingBasis: "verified_planner_agency_name",
    opportunityLevel: "account",
    researchTitlePreserved: text(opportunity.title) || null,
  };
}
