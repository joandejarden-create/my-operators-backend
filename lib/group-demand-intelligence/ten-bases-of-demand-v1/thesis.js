/**
 * Hotel opportunity thesis — FACT / INFERENCE / UNKNOWN separation.
 */

export function buildHotelOpportunityThesis(record = {}, hotel = {}, packetEval = {}) {
  const org = record.organizationName || record.organization || record.lookalikeAccount || "UNKNOWN";
  const motion = record.groupMotion || record.role || record.meetingType || record.groupType || "GROUP_MOTION";
  const buyer =
    record.buyerEntity ||
    record.organizer ||
    record.buyer ||
    record.buyerFunction ||
    record.buyerRoleHint ||
    "UNKNOWN";
  const when =
    record.eventStartDate ||
    record.nextConfirmedCycle ||
    record.nextPredictedCycle ||
    record.decisionWindow ||
    record.futureTiming ||
    "UNKNOWN";

  const facts = [];
  const inferences = [];
  const unknowns = [];

  if (org && org !== "UNKNOWN") facts.push(`Named entity: ${org}`);
  if (record.officialSource || record.source) facts.push(`Source: ${record.officialSource || record.source}`);
  if (record.competitorHotel && /DIRECT|STRONG/i.test(String(record.evidenceClass || ""))) {
    facts.push(`Competitor lodging evidence: ${record.competitorHotel} (${record.evidenceClass})`);
  } else if (record.competitorHotel) {
    inferences.push(`Competitor association (not proof alone): ${record.competitorHotel}`);
  }
  if (record.eventStartDate) facts.push(`Confirmed timing: ${record.eventStartDate}`);
  else if (record.isPredicted || record.predictedLabel) {
    inferences.push(`Predicted / inferred timing: ${when}`);
  } else if (when !== "UNKNOWN") {
    inferences.push(`Timing cue: ${when}`);
  }

  if (!record.publicContactPath && !record.organizationContactUrl) {
    unknowns.push("public_contact_path");
  }
  if (!record.lodgingEvidence && !/DIRECT|STRONG/i.test(String(record.evidenceClass || ""))) {
    unknowns.push("lodging_evidence");
  }
  if (record.groupSize === "UNKNOWN" || !record.groupSize) unknowns.push("group_size");
  if (buyer === "UNKNOWN") unknowns.push("buyer_entity");

  const winAngle =
    /overflow|housing/i.test(`${motion} ${record.role || ""}`)
      ? "overflow_or_secondary_delegation"
      : /crew|production|workforce/i.test(`${motion} ${record.role || ""}`)
        ? "crew_or_workforce"
        : /lookalike|predicted/i.test(`${record.inferenceLabel || ""} ${record.maturity || ""}`)
          ? "next_rotating_cycle_or_lookalike"
          : "full_group_or_primary_block";

  return {
    hotelKey: hotel.hotelKey,
    opportunityId: record.id,
    organization: org,
    baseOfDemand: record.baseOfDemand || "",
    whyThisGroup: `${org} shows ${motion} relevant to ${hotel.destinationMarket || hotel.market}.`,
    whyThisMarket: hotel.market || hotel.destinationMarket || "",
    whyThisHotel: hotel.fitLine || record.hotelThesis || "",
    whyNow: when,
    whoBuys: buyer,
    whatHotelDemandExists: record.hotelMotionHypothesis || record.lodgingContext || motion,
    whatEvidenceSupports: facts.join(" | ") || "limited",
    whatIsFact: facts.join(" | "),
    whatIsInference: inferences.join(" | "),
    whatIsUnknown: unknowns.join("|"),
    whatHotelCouldWin: winAngle,
    whatCouldPreventWin: unknowns.length
      ? `Missing: ${unknowns.join(", ")}`
      : "competition_or_timing_slip",
    nextSalesAction:
      record.maturity === "PREDICTED_OPPORTUNITY"
        ? "Monitor decision window; do not sell as confirmed. Seek buyer confirmation."
        : record.publicContactPath
          ? "Outreach via public contact with lodging thesis"
          : "Resolve buyer/contact path before outreach",
    packetQuality: packetEval.quality || record.packetQuality || "",
    maturity: record.maturity || "",
    predictedLabel: record.predictedLabel || "",
  };
}
