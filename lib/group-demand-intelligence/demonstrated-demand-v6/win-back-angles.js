/**
 * Competitor win-back / next-cycle sales angles — hypotheses, not displacement assumptions.
 */

export function buildCompetitorWinBackAngles(lead = {}, thesis = {}, targetHotel = {}) {
  if (thesis.fitClass === "WEAK_FIT" || thesis.fitClass === "NO_FIT") return [];
  const angles = [];
  const push = (angle, rationale) => {
    angles.push({
      targetHotelKey: targetHotel.hotelKey || lead.hotelKey,
      organization: lead.organizationName || lead.organization,
      competitorHotel: lead.competitorHotel || "",
      winAngle: angle,
      hypothesis: true,
      rationale,
    });
  };

  push("overflow", "Hypothesis: capture overflow when primary competitor sells out.");
  push("secondary_delegation", "Hypothesis: secondary delegation / staff lodging.");
  push("smaller_leadership_subgroup", "Hypothesis: leadership/offsite subset at target hotel.");
  push("pre_post_event_stay", "Hypothesis: pre/post program nights.");

  const product = targetHotel.productFit || "";
  if (/airport/i.test(product) || /YOTEL/i.test(targetHotel.hotelKey || "")) {
    push("crew_or_staff", "Hypothesis: crew/staff near airport corridor.");
    push("full_group_value_alternative", "Hypothesis: value alternative — unproven.");
  }
  if (/resort|cottage|beach/i.test(product)) {
    push("next_rotating_cycle", "Hypothesis: next rotating/destination cycle if cadence supports.");
  }
  if (/urban/i.test(product)) {
    push("full_group", "Hypothesis: full boutique group only if size fits — do not assume displacement.");
  }

  return angles.slice(0, 6);
}
