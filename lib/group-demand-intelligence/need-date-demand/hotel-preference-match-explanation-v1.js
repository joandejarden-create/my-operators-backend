/**
 * Soft hotel-preference match explanations for GDI ranking / presentation.
 * Preferences influence priority messaging only — never invent demand or
 * reject opportunities solely for falling outside a need window.
 */

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function asId(v) {
  if (v == null) return "";
  if (typeof v === "string") return v;
  return text(v.id || v.strategyId || "");
}

/**
 * @returns {string[]} Human-readable soft-match lines (may be empty).
 */
export function buildHotelPreferenceMatchExplanations({
  commercialProfile = null,
  needPeriod = null,
  opportunity = null,
  timingMatch = null,
} = {}) {
  const lines = [];
  const profile = commercialProfile && typeof commercialProfile === "object"
    ? commercialProfile
    : null;
  const period = needPeriod && typeof needPeriod === "object" ? needPeriod : null;
  const opp = opportunity && typeof opportunity === "object" ? opportunity : {};

  const revenueIds = new Set(
    (profile?.revenuePriorities || []).map(asId).filter(Boolean),
  );
  const periodRevenue = new Set((period?.revenueNeed || []).map(asId).filter(Boolean));
  const segments = new Set((profile?.prioritySegments || []).map(asId).filter(Boolean));
  const dayPref = text(profile?.dayPreference || "");
  const specificDays = (profile?.specificDays || []).map(text).filter(Boolean);

  const oppSegment = text(
    opp.demandSegmentId ||
      opp.segmentId ||
      opp.workflowId ||
      opp.researchWorkflowId ||
      "",
  );
  const oppTitle = text(opp.title || opp.name || "");
  const oppText = `${oppTitle} ${text(opp.summary || "")} ${text(
    opp.commercialType || "",
  )}`.toLowerCase();

  if (
    (revenueIds.has("LOCAL_CATERING") || periodRevenue.has("LOCAL_CATERING")) &&
    /cater|banquet|meeting|event|fnc|f&amp;b|food/.test(oppText)
  ) {
    lines.push("Matches hotel's stated local catering priority.");
  }
  if (
    (revenueIds.has("GROUP_ROOM_NIGHTS") ||
      periodRevenue.has("GROUP_GUESTROOMS") ||
      revenueIds.has("ASSOCIATION_BUSINESS")) &&
    /group|room block|peak|association|conference/.test(oppText)
  ) {
    lines.push("Matches hotel's stated group-room priority.");
  }
  if (
    (dayPref === "WEEKDAYS" ||
      specificDays.some((d) => ["Mon", "Tue", "Wed", "Thu"].includes(d))) &&
    /weekday|tue|wed|midweek|monday|tuesday|wednesday|thursday/.test(oppText)
  ) {
    lines.push("Matches hotel's stated weekday group-room priority.");
  }
  if (oppSegment && segments.has(oppSegment)) {
    lines.push(`Matches hotel's stated ${oppSegment.replace(/_/g, " ")} priority.`);
  }
  if (
    timingMatch?.overlapStatus &&
    /CONFIRMED_PERIOD_OVERLAP|SUPPORTED_PROBABLE_OVERLAP/.test(
      String(timingMatch.overlapStatus),
    ) &&
    period?.periodLabel
  ) {
    lines.push(`Aligns with hotel need period ${period.periodLabel}.`);
  }

  // Deduplicate while preserving order
  return [...new Set(lines)];
}

/**
 * Attach soft preference explanations onto an opportunity DTO without mutating demand claims.
 */
export function attachHotelPreferenceMatchExplanations(opportunity = {}, opts = {}) {
  const explanations = buildHotelPreferenceMatchExplanations({
    commercialProfile: opts.commercialProfile || null,
    needPeriod: opts.needPeriod || opportunity.needPeriod || null,
    opportunity,
    timingMatch:
      opts.timingMatch ||
      (opportunity.needDateMatchV1?.timingOverlapStatus
        ? { overlapStatus: opportunity.needDateMatchV1.timingOverlapStatus }
        : null),
  });
  if (!explanations.length) return opportunity;
  return {
    ...opportunity,
    hotelPreferenceMatchExplanations: explanations,
    hotelPreferenceMatchLine: explanations[0],
  };
}
