/**
 * Repair empty hotelOpportunityThesis for discovery-stage candidates that already
 * have organization identity + evidence URLs but were mapped without a title.
 *
 * Does NOT invent demand activity. Uses only existing org/segment/hotel labels
 * so deep-research eligibility can evaluate hotel relevance honestly.
 */
export const GDI_SEGMENT_DISCOVERY_THESIS_REPAIR_V1 =
  "gdi-segment-discovery-thesis-repair-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

const SEGMENT_ACTIVITY_LABEL = Object.freeze({
  preferred_lodging_partnerships:
    "Independent event venue — preferred lodging partnership / overnight guest referral prospect",
  professional_event_planners:
    "Professional event planner — hotel lodging referral and group accommodation pathway",
  local_catering_day_meetings:
    "Local catering / day-meeting demand channel — hotel F&B and meeting-space prospect",
  corporate_celebrations_private_events:
    "Corporate celebration / private event — hotel banquet and lodging prospect",
  travel_trade_group_intermediaries:
    "Travel-trade / group intermediary — hotel group contracting pathway",
  relocation_extended_stay:
    "Relocation / extended-stay mobility — hotel extended-stay prospect",
});

/**
 * Ensure discovery candidates have a segment-honest thesis when missing.
 * Idempotent; never overwrites an existing non-empty thesis.
 */
export function ensureSegmentDiscoveryThesisForEligibility(opportunity = {}) {
  const next = { ...opportunity };
  const existing = text(next.hotelOpportunityThesis || next.whyMyHotel || "");
  if (existing.length >= 40) return next;

  const org = text(
    next.organizationName || next.venueName || next.companyName || "",
  );
  if (!org) return next;

  const segmentId = text(
    next.researchSegment || next.segmentId || next.demandType || "",
  );
  const title =
    text(next.title || next.programmeName || next.eventName || "") ||
    SEGMENT_ACTIVITY_LABEL[segmentId] ||
    null;
  if (!title) return next;

  const hotel =
    text(next.hotelName || next.targetHotelName || "") || "target hotel";
  const evidenceCount = Array.isArray(next.evidence)
    ? next.evidence.length
    : 0;
  const seedCount = Array.isArray(next.seedUrls) ? next.seedUrls.length : 0;
  if (evidenceCount + seedCount < 1) return next;

  const thesis = `${org} — ${title}: investigate hotel-relevant lodging / meetings / catering demand for ${hotel}.`;
  next.hotelOpportunityThesis = thesis;
  if (!text(next.title) && SEGMENT_ACTIVITY_LABEL[segmentId]) {
    next.title = SEGMENT_ACTIVITY_LABEL[segmentId];
    next.programmeName = next.programmeName || next.title;
  }
  next.discoveryThesisRepair = {
    version: GDI_SEGMENT_DISCOVERY_THESIS_REPAIR_V1,
    repaired: true,
    segmentId: segmentId || null,
    priorThesisEmpty: true,
    at: new Date().toISOString(),
    note: "Filled empty thesis from org + segment activity label + existing evidence; no invented demand claims.",
  };
  return next;
}
