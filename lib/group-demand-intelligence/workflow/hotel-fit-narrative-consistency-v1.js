/**
 * Shared narrative ↔ hotel-fit consistency checks.
 * Used during research, import preparation, and evidence-driven refresh.
 *
 * Distinguishes:
 *   not_applicable | applicable_unknown | research_not_performed | conflicting_evidence
 */

export const HOTEL_FIT_NARRATIVE_CONSISTENCY_VERSION =
  "gdi-hotel-fit-narrative-consistency-v1";

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

function blob(opp = {}) {
  return [
    opp.hotelOpportunityThesis,
    opp.summaryWhyMatters,
    opp.summaryWhyHotel,
    opp.fitExplanation,
    opp.hotelFitRationale,
    opp.whyThisHotel,
    opp.proposedHotelUse,
    opp.competitiveContextNote,
    opp.venueHistoryNote,
    opp.localConnection,
  ]
    .map(text)
    .filter(Boolean)
    .join(" ");
}

/**
 * Detect unsupported claim patterns that contradict assessment rules.
 */
export function detectNarrativeConsistencyIssues(opp = {}, assessment = null) {
  const issues = [];
  const prose = blob(opp);
  const dims = assessment?.dimensions || opp.hotelFitDimensionAssessments || {};
  const score = assessment?.hotelFitScore ?? opp.hotelFitScore;
  const provisional =
    assessment?.overallStatus === "provisional" ||
    opp.hotelFitOverallStatus === "provisional" ||
    opp.hotelFitProvisional === true ||
    assessment?.calculationBasis?.provisional === true;

  // A. Hotel-block implication without evidence
  if (
    /wonk\s*week/i.test(prose) &&
    /hotel\s*block|room\s*block|official\s*hotel/i.test(prose) &&
    !/without assuming|not evidence|do not assume|not available|unconfirmed/i.test(
      prose
    )
  ) {
    issues.push({
      code: "hotel_block_implied_without_evidence",
      severity: "high",
      message:
        "Narrative implies a hotel block or official hotel role without sourced confirmation.",
      suggestion:
        "Separate possible business uses from any named program; state that no hotel block is assumed.",
    });
  }

  // B. "Not named" ≠ partnership vacancy / high capture
  if (
    /not named|is not named|hotel not named/i.test(prose) &&
    /referral opening|partnership opening|strongest|high (partnership|capture)|vacancy/i.test(
      prose
    )
  ) {
    issues.push({
      code: "not_named_treated_as_vacancy",
      severity: "high",
      message:
        "Absence from a nearby-hotel list is treated as a partnership vacancy or high-capture opening.",
      suggestion:
        "Replace with a bounded sales hypothesis; confirm referral flow before claiming opening.",
    });
  }

  if (
    (dims.historicalFit?.status === "insufficient_evidence" ||
      dims.historicalFit?.status === "not_applicable") &&
    /preferred partner|partnership vacancy|competitive referral opening|preferred-partner opening/i.test(
      prose
    ) &&
    !/bounded sales hypothesis|not proof of a partnership vacancy/i.test(prose)
  ) {
    issues.push({
      code: "partnership_claim_without_history",
      severity: "high",
      message: "Partnership-opening claim without verified partnership history.",
    });
  }

  // C. Provisional mismatch — stretch/gap narrative with non-provisional high score
  if (
    provisional !== true &&
    score != null &&
    Number(score) >= 55 &&
    (/stretch|exploratory|no supported primary-host|evidence gap|not quantified|peak rooms unknown/i.test(
      prose
    ) ||
      dims.physicalFit?.status === "insufficient_evidence" ||
      dims.commercialValue?.status === "insufficient_evidence")
  ) {
    issues.push({
      code: "provisional_score_mismatch",
      severity: "medium",
      message:
        "Material evidence gaps or stretch geography coexist with a non-provisional overall score.",
      suggestion: "Mark overall provisional when applicable dimensions lack evidence.",
    });
  }

  // D. History claim vs historicalFit N/A or gap
  if (
    /proven .+ (banquet|gala|history)|already selected .+ for|held .+ at the mayflower|held .+ at .+ hotel/i.test(
      prose
    ) &&
    (dims.historicalFit?.status === "not_applicable" ||
      dims.historicalFit?.status === "insufficient_evidence" ||
      !dims.historicalFit)
  ) {
    issues.push({
      code: "history_claim_not_in_assessment",
      severity: "high",
      message:
        "Narrative asserts hotel banquet/host history but historicalFit is N/A or insufficient.",
      suggestion:
        "Feed sourced hotel history into historicalFit as assessed, or qualify the prose.",
    });
  }

  // Historical marked N/A solely for missing history (wrong)
  if (
    dims.historicalFit?.status === "not_applicable" &&
    /not found|no history|unknown history|not researched/i.test(
      String(dims.historicalFit?.rationale || "")
    )
  ) {
    issues.push({
      code: "history_na_for_unknown",
      severity: "medium",
      message:
        "historicalFit marked N/A because history was not found — should be insufficient_evidence (applicable but unknown).",
    });
  }

  return {
    version: HOTEL_FIT_NARRATIVE_CONSISTENCY_VERSION,
    ok: issues.length === 0,
    issueCount: issues.length,
    issues,
  };
}

/**
 * Bounded narrative repairs for known contradiction patterns.
 * Does not invent scores — only qualifies unsupported prose.
 */
export function repairNarrativeForConsistency(opp = {}, opts = {}) {
  const next = { ...opp };
  const hotelName = text(opts.hotelName) || "this hotel";
  const changes = [];

  const fields = [
    "hotelOpportunityThesis",
    "summaryWhyMatters",
    "summaryWhyHotel",
    "fitExplanation",
    "fitRationale",
    "whyThisHotel",
    "proposedHotelUse",
  ];

  for (const f of fields) {
    let v = text(next[f]);
    if (!v) continue;
    const before = v;

    // Anderson / PE: not-named ≠ vacancy — replace the whole claim, do not splice mid-sentence
    if (
      /not named/i.test(v) &&
      /referral opening|competitive referral|partnership opening/i.test(v)
    ) {
      const venueHint = text(next.venueName || next.organizationName);
      v = `${venueHint ? venueHint + " is nearby. " : ""}${hotelName} is not listed among nearby lodging references on the venue's public materials. That absence is not proof of a partnership vacancy — it is a bounded sales hypothesis to explore whether overnight guests are referred and whether a complementary lodging conversation is welcome. Confirm traveling-guest volume and any existing referral habits before treating this as a preferred-partner opening.`;
    }

    // Soften absolute "strongest possible" without sourced exclusivity
    if (/strongest possible hotel-specific fit/i.test(v)) {
      v = v.replace(
        /strongest possible hotel-specific fit/gi,
        "strong hotel-specific fit based on prior hosted activity"
      );
    }

    // Wonk Week — ensure non-assumption of block
    if (/wonk\s*week/i.test(v) && !/without assuming|do not assume|not evidence/i.test(v)) {
      v = `${v.replace(/\s+$/, "")} Explore lodging or meeting needs associated with customer events without assuming any Wonk Week hotel block is available.`;
    }

    if (v !== before) {
      next[f] = v;
      changes.push({ field: f, before: before.slice(0, 160), after: v.slice(0, 160) });
    }
  }

  // Classify history evidence status for callers
  const prose = blob(next);
  let historyEvidenceClass = "research_not_performed";
  if (
    /held .+ at the mayflower|selected mayflower|mayflower for .+gala|at the mayflower hotel/i.test(
      prose
    )
  ) {
    historyEvidenceClass = "sourced_hotel_history";
  } else if (/not researched|history unknown|no prior host/i.test(prose)) {
    historyEvidenceClass = "applicable_unknown";
  }

  return {
    opportunity: next,
    changes,
    historyEvidenceClass,
    check: detectNarrativeConsistencyIssues(next, opts.assessment || null),
  };
}

export default {
  HOTEL_FIT_NARRATIVE_CONSISTENCY_VERSION,
  detectNarrativeConsistencyIssues,
  repairNarrativeForConsistency,
};
