/**
 * run-052 Issue 6 (RC-07): do not present empty hotel-fit dimension shells
 * as a completed "Not established" scorecard.
 */

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

const GENERIC_FACILITIES_RE =
  /seven meeting rooms|ballroom capacity up to|executive facilities|chapultepec\s*\/\s*polanco\s*\/\s*reforma location/i;

/**
 * True when at least one dimension carries a real assessment status or score.
 */
export function hasRenderableHotelFitDimensions(opportunity = {}, audit = {}) {
  const dimAssess =
    opportunity.hotelFitDimensionAssessments ||
    audit?.hotelFit?.dimensionAssessments ||
    null;
  const qual = opportunity.hotelFitQualitativeComponents || null;
  const src =
    dimAssess && typeof dimAssess === "object" && Object.keys(dimAssess).length
      ? dimAssess
      : qual && typeof qual === "object" && Object.keys(qual).length
        ? qual
        : null;
  if (!src) return false;
  return Object.values(src).some((d) => {
    if (!d || typeof d !== "object") return false;
    const status = text(d.status).toLowerCase();
    if (
      status === "assessed" ||
      status === "insufficient_evidence" ||
      status === "not_applicable" ||
      status === "provisional"
    ) {
      return true;
    }
    if (d.score != null && !Number.isNaN(Number(d.score))) return true;
    if (text(d.rationale) || text(d.summary) || text(d.explanation)) return true;
    return false;
  });
}

/**
 * Facilities inventory lines are not opportunity-specific commercial fit proof.
 */
export function isGenericFacilitiesHotelFitNarrative(raw) {
  const t = text(raw);
  if (!t) return false;
  if (!GENERIC_FACILITIES_RE.test(t)) return false;
  // Corridor + medical/association programme thesis is opportunity-aware — allow.
  if (
    /medical|association|programme|program|congress|delegate|speaker/i.test(t) &&
    t.length > 120
  ) {
    return false;
  }
  return true;
}

export function applyHotelFitPresentationHonesty(opportunity = {}, audit = {}) {
  const next = { ...opportunity };
  const showGrid = hasRenderableHotelFitDimensions(next, audit);
  next.hotelFitShowDimensionGrid = showGrid;
  if (!showGrid) {
    next.hotelFitUnassessedCustomerNote =
      "Hotel-fit dimensions have not been assessed for this prospect. Territory or corridor notes above are not a completed dimension scorecard.";
  }
  if (isGenericFacilitiesHotelFitNarrative(next.whyThisHotel)) {
    const goldWhy = text(next.associationEventsGoldBrief?.whyThisHotel);
    const cibWhy = text(next.commercialIntelligenceBrief?.whyThisHotel);
    const better =
      (goldWhy && !isGenericFacilitiesHotelFitNarrative(goldWhy) && goldWhy) ||
      (cibWhy && !isGenericFacilitiesHotelFitNarrative(cibWhy) && cibWhy) ||
      null;
    if (better) {
      next.whyThisHotel = better;
      next.hotelFitNarrativeDemotedGenericFacilities = true;
    } else {
      next.whyThisHotel =
        "Opportunity-specific hotel fit is not yet established. Property facilities alone are not treated as commercial fit proof.";
      next.hotelFitNarrativeDemotedGenericFacilities = true;
    }
  }
  return next;
}
