/**
 * Hotel revenue-mechanism taxonomy V1
 *
 * Guestrooms are NOT a universal commercial eligibility requirement.
 * A single opportunity may carry multiple revenue types.
 * Absence of evidence ≠ proof of zero room demand.
 */

export const HOTEL_REVENUE_MECHANISM_V1 = "gdi-hotel-revenue-mechanism-v1";

export const HOTEL_REVENUE_TYPE = Object.freeze({
  GUESTROOMS: "guestrooms",
  GROUP_GUESTROOMS: "group_guestrooms",
  EXTENDED_STAY: "extended_stay",
  MEETING_SPACE_RENTAL: "meeting_space_rental",
  LOCAL_CATERING: "local_catering",
  BANQUETS: "banquets",
  PRIVATE_DINING: "private_dining",
  RESORT_ALL_INCLUSIVE_GROUP: "resort_all_inclusive_group",
  AV_EVENT_ANCILLARY: "av_event_ancillary",
  OTHER_VERIFIED_HOTEL_SERVICE: "other_verified_hotel_service",
});

export const GUESTROOM_DEMAND_STATUS = Object.freeze({
  NONE_CONFIRMED: "NONE_CONFIRMED",
  UNKNOWN: "UNKNOWN",
  EVIDENCE_SUPPORTED: "EVIDENCE_SUPPORTED",
});

export const APPROVED_REVENUE_TYPES = Object.freeze(
  Object.values(HOTEL_REVENUE_TYPE),
);

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Normalize revenue type tags — dedupe, allowlist only.
 * @returns {string[]}
 */
export function normalizeRevenueTypeTags(tags = []) {
  const out = [];
  const seen = new Set();
  for (const raw of Array.isArray(tags) ? tags : []) {
    const t = text(raw).toLowerCase().replace(/\s+/g, "_");
    if (!APPROVED_REVENUE_TYPES.includes(t)) continue;
    if (seen.has(t)) continue;
    seen.add(t);
    out.push(t);
  }
  return out;
}

/**
 * Infer conservative guestroomDemand status from evidence blob / tags.
 * Never invents room nights. UNKNOWN when silence.
 */
export function resolveGuestroomDemandStatus(input = {}) {
  const explicit = text(input.guestroomDemandStatus || input.guestroomDemand);
  if (Object.values(GUESTROOM_DEMAND_STATUS).includes(explicit)) return explicit;

  const tags = normalizeRevenueTypeTags(input.revenueTypes || input.revenueTypeTags);
  const blob = [
    input.title,
    input.summaryWhat,
    input.whatIsHappening,
    input.potentialBusiness,
    input.demandMechanism,
  ]
    .map(text)
    .join(" ")
    .toLowerCase();

  const roomEvidence =
    tags.includes(HOTEL_REVENUE_TYPE.GUESTROOMS) ||
    tags.includes(HOTEL_REVENUE_TYPE.GROUP_GUESTROOMS) ||
    tags.includes(HOTEL_REVENUE_TYPE.EXTENDED_STAY) ||
    tags.includes(HOTEL_REVENUE_TYPE.RESORT_ALL_INCLUSIVE_GROUP) ||
    /\b(overnight|room block|guestrooms?|extended stay|accommodation nights?)\b/i.test(
      blob,
    );

  const localOnly =
    (tags.includes(HOTEL_REVENUE_TYPE.LOCAL_CATERING) ||
      tags.includes(HOTEL_REVENUE_TYPE.MEETING_SPACE_RENTAL) ||
      tags.includes(HOTEL_REVENUE_TYPE.BANQUETS) ||
      tags.includes(HOTEL_REVENUE_TYPE.PRIVATE_DINING) ||
      /\b(day meeting|local catering|desayuno empresarial|capacitaci[oó]n|coffee break|private dining|banquet)\b/i.test(
        blob,
      )) &&
    !roomEvidence &&
    (input.localAttendeesOnly === true ||
      /\b(local attendees?|no overnight|zero room|day[- ]use only|sin hospedaje)\b/i.test(
        blob,
      ));

  if (roomEvidence) return GUESTROOM_DEMAND_STATUS.EVIDENCE_SUPPORTED;
  if (localOnly || input.guestroomRequired === false) {
    return GUESTROOM_DEMAND_STATUS.NONE_CONFIRMED;
  }
  return GUESTROOM_DEMAND_STATUS.UNKNOWN;
}

/**
 * Build revenue taxonomy stamp for CIB / DTO (presentation + qualification).
 */
export function buildRevenueMechanismStamp(input = {}) {
  const revenueTypes = normalizeRevenueTypeTags(
    input.revenueTypes || input.revenueTypeTags || [],
  );
  const guestroomDemandStatus = resolveGuestroomDemandStatus({
    ...input,
    revenueTypes,
  });
  return {
    version: HOTEL_REVENUE_MECHANISM_V1,
    revenueTypes,
    guestroomDemandStatus,
    guestroomRequired: false,
    note:
      "At least one credible hotel revenue mechanism is required — guestrooms are optional.",
  };
}
