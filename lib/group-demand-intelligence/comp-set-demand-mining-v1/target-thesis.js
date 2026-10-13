/**
 * Target-hotel competitive thesis + win angles.
 * Separate FACT / INFERENCE / UNKNOWN. Do not overstate competitor-selection reasons.
 */

export const FIT_CLASS = Object.freeze({
  STRONG_FIT: "STRONG_FIT",
  PLAUSIBLE_FIT: "PLAUSIBLE_FIT",
  WEAK_FIT: "WEAK_FIT",
  NO_FIT: "NO_FIT",
});

const WIN_ANGLES = Object.freeze([
  "lower_value_alternative",
  "overflow_solution",
  "secondary_delegation_hotel",
  "airport_access_alternative",
  "smaller_leadership_offsite_venue",
  "exclusive_resort_alternative",
  "extended_stay_crew_fit",
  "better_group_size_fit",
  "meeting_lodging_package",
  "pre_post_event_option",
]);

/**
 * Build Hotel Opportunity Thesis for a competitive demand pattern / strong trace.
 */
export function buildTargetHotelThesis(pattern = {}, targetHotel = {}, opts = {}) {
  const product = targetHotel.productFit || "";
  const rooms = Number(targetHotel.rooms || targetHotel.targetRooms || 0);
  const engine = pattern.demandEngine || "";
  const cadence = pattern.repeatCadence || pattern.cadence || "UNKNOWN";
  const historicHotels = Array.isArray(pattern.historicHotels)
    ? pattern.historicHotels
    : String(pattern.historicHotels || "")
        .split("|")
        .filter(Boolean);

  const facts = [];
  const inferences = [];
  const unknowns = [];

  facts.push(
    `Public traces associate ${pattern.organization || "organization"} with competitor hotel(s): ${historicHotels.join(", ") || "UNKNOWN"}.`
  );
  if (pattern.nextExpectedCycle || pattern.nextKnownCycle) {
    facts.push(
      `Future/next cycle signal: ${pattern.nextKnownCycle || pattern.nextExpectedCycle}.`
    );
  } else {
    unknowns.push("Next confirmed cycle date");
  }
  if (cadence && cadence !== "UNKNOWN") {
    inferences.push(`Repeat cadence appears ${cadence} based on public language/years — not guaranteed.`);
  } else {
    unknowns.push("Whether demand is recurring");
  }
  unknowns.push("Exact room block size at competitor");
  unknowns.push("Why competitor was selected (rate, location, relationship) — not evidenced");

  // Fit evaluation (deterministic heuristics, labeled)
  let fit = FIT_CLASS.PLAUSIBLE_FIT;
  const constraints = [];
  const advantages = [];

  if (product.includes("resort") || product.includes("cottage") || product.includes("beach")) {
    if (/SPORTS|SOCIAL|TOUR_DMC|CORPORATE|ASSOCIATION/i.test(engine)) {
      advantages.push("destination_resort_fit_for_incentive_or_retreat");
      fit = FIT_CLASS.PLAUSIBLE_FIT;
    }
    if (/PHARMA|TECH_FINANCIAL|GOVERNMENT/i.test(engine) && /NYC|Geneva|urban/i.test(targetHotel.market || "")) {
      constraints.push("resort_vs_urban_meeting_mismatch_risk");
    }
  }
  if (product.includes("airport") || /YOTEL/i.test(targetHotel.hotelKey || "")) {
    advantages.push("airport_corridor_overflow_or_value_alternative");
    if (/ASSOCIATION|CORPORATE|GOVERNMENT|PHARMA/i.test(engine)) fit = FIT_CLASS.PLAUSIBLE_FIT;
  }
  if (product.includes("urban")) {
    advantages.push("urban_meeting_access");
    if (/TOUR_DMC|incentive beach/i.test(engine)) {
      constraints.push("urban_vs_leisure_destination_mismatch");
      fit = FIT_CLASS.WEAK_FIT;
    }
  }
  if (rooms > 0 && rooms < 80) {
    advantages.push("better_fit_for_smaller_leadership_or_delegation");
    constraints.push("inventory_may_not_support_large_congress_peak");
  }
  if (rooms >= 150) {
    advantages.push("inventory_for_medium_group_peak");
  }
  if (!pattern.organization) {
    fit = FIT_CLASS.NO_FIT;
    constraints.push("organization_unresolved");
  }
  if (
    !pattern.nextExpectedCycle &&
    !pattern.nextKnownCycle &&
    cadence === "ONE_OFF"
  ) {
    fit = FIT_CLASS.WEAK_FIT;
    constraints.push("no_future_cycle_evidence");
  }
  if (historicHotels.length >= 2 && /ANNUAL|MULTI_YEAR|REPEAT/i.test(cadence)) {
    fit = fit === FIT_CLASS.WEAK_FIT ? FIT_CLASS.PLAUSIBLE_FIT : FIT_CLASS.STRONG_FIT;
    advantages.push("multi_hotel_or_multi_year_repeat_supports_future_compete");
  }

  // Cap Maison (St Lucia) vs Spice (Grenada) — geography constraint if markets diverge
  const targetMarket = String(targetHotel.market || "").toLowerCase();
  for (const h of historicHotels) {
    if (/cap maison/i.test(h) && /grenada/i.test(targetMarket)) {
      constraints.push("historic_hotel_in_different_island_market");
      if (fit === FIT_CLASS.STRONG_FIT) fit = FIT_CLASS.PLAUSIBLE_FIT;
    }
  }

  const whatCompetitorAppearedToProvide = historicHotels.length
    ? `Public sources name ${historicHotels.join(" / ")} in lodging/event context — exact package UNKNOWN.`
    : "UNKNOWN";

  const whatTargetCouldWin =
    fit === FIT_CLASS.NO_FIT || fit === FIT_CLASS.WEAK_FIT
      ? "Unclear — insufficient future lodging compete thesis."
      : "Potential primary, overflow, or secondary delegation lodging on next cycle if buyer open and fit holds.";

  const nextAction =
    fit === FIT_CLASS.STRONG_FIT || fit === FIT_CLASS.PLAUSIBLE_FIT
      ? "Confirm next cycle + housing buyer/organizer path; do not sell until canonical gates pass."
      : "Park as research signal; do not pursue as opportunity yet.";

  return {
    targetHotelKey: targetHotel.hotelKey || targetHotel.targetHotelKey,
    organization: pattern.organization,
    patternId: pattern.patternId,
    fitClass: fit,
    whyRelevant: `${pattern.organization || "Group"} shows public competitor-hotel lodging/event association in ${targetHotel.market || "market"}.`,
    repeatFutureEvidence: pattern.nextKnownCycle || pattern.nextExpectedCycle || cadence || "UNKNOWN",
    whatCompetitorAppearedToProvide,
    couldTargetServe: fit === FIT_CLASS.STRONG_FIT || fit === FIT_CLASS.PLAUSIBLE_FIT ? "YES_PLAUSIBLE" : "NO_OR_WEAK",
    whatTargetCouldWin,
    majorDisadvantages: constraints.join("|") || "UNKNOWN",
    advantages: advantages.join("|") || "",
    nextSalesResearchAction: nextAction,
    fact: facts.join(" "),
    inference: inferences.join(" "),
    unknown: unknowns.join(" "),
    geographyOk: !constraints.some((c) => /different_island|mismatch/i.test(c)),
    rooms: rooms || null,
  };
}

/**
 * Derive commercial win-angle hypotheses for STRONG/PLAUSIBLE fit.
 */
export function deriveWinAngles(thesis = {}, targetHotel = {}) {
  if (thesis.fitClass !== FIT_CLASS.STRONG_FIT && thesis.fitClass !== FIT_CLASS.PLAUSIBLE_FIT) {
    return [];
  }
  const angles = [];
  const product = targetHotel.productFit || "";
  const adv = String(thesis.advantages || "");

  const push = (angle, rationale) => {
    angles.push({
      targetHotelKey: thesis.targetHotelKey,
      organization: thesis.organization,
      patternId: thesis.patternId,
      winAngle: angle,
      hypothesis: true,
      rationale,
      notFact: true,
    });
  };

  if (/airport/i.test(product) || /airport/i.test(adv)) {
    push("airport_access_alternative", "Hypothesis: airport-corridor access vs city/center competitor.");
    push("overflow_solution", "Hypothesis: overflow when primary host hotel sells out.");
  }
  if (/resort|cottage|beach/i.test(product)) {
    push("exclusive_resort_alternative", "Hypothesis: resort/exclusive alternative to urban or other resort comps.");
    push("pre_post_event_option", "Hypothesis: pre/post program lodging.");
  }
  if (/urban/i.test(product)) {
    push("smaller_leadership_offsite_venue", "Hypothesis: boutique/urban leadership or offsite fit.");
    push("secondary_delegation_hotel", "Hypothesis: secondary delegation housing.");
  }
  if (Number(targetHotel.rooms || targetHotel.targetRooms || 0) < 100) {
    push("better_group_size_fit", "Hypothesis: better fit for mid/small peak than large convention hotel.");
  }
  push("lower_value_alternative", "Hypothesis: value positioning vs higher-rate competitor — unproven.");
  push("meeting_lodging_package", "Hypothesis: meeting + lodging package if inventory/space allow.");

  // Keep unique angles from allowed set
  return angles.filter((a) => WIN_ANGLES.includes(a.winAngle)).slice(0, 6);
}
