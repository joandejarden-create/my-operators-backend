/**
 * Reverse-engineer Bethesda / NYC success → buildGdiSuccessfulOpportunityPattern().
 */

import {
  CONTROL_HOTELS,
  buildSuccessControlSet,
  summarizeControlPatterns,
} from "../discovery-quality-v4/success-controls.js";
import { classifyDemandEngine } from "../demand-engine-v1/classify-engine.js";

function inferHowDiscovered(o = {}) {
  const src = String(o.officialSource || o.discoverySource || "");
  const lane = o.discoveryLane || o.demandSignalType || "";
  if (/housing|accommodation|hotel/i.test(src)) return "HOUSING_OR_ACCOMMODATION_PAGE";
  if (/rfp|tender|procurement/i.test(src + lane)) return "PROCUREMENT";
  if (/association|society|federation|\.org\b/i.test(src)) return "ASSOCIATION_OFFICIAL";
  if (o.eventSeriesId || /series:/i.test(String(o.eventSeriesId || ""))) return "SERIES_ROTATION";
  if (/competitor|host hotel|marriott|hilton|sheraton/i.test(String(o.hotelOpportunityThesis || ""))) {
    return "COMPETITIVE_OR_HOST_PATTERN";
  }
  return lane || "CURATED_OR_PRIOR_RESEARCH";
}

function inferSourceFamily(o = {}) {
  const eng = classifyDemandEngine(o);
  return eng?.demandEngine || "ASSOCIATION_NGO";
}

/**
 * Enrich control row with discovery-path fields for V5 pattern.
 */
export async function buildGdiSuccessfulOpportunityPattern(opts = {}) {
  const controls = await buildSuccessControlSet(opts);
  const enriched = controls.map((c) => {
    const how = inferHowDiscovered(c);
    return {
      ...c,
      demandEngine: inferSourceFamily(c),
      groupMotion: c.opportunityType || "UNKNOWN",
      futureTimingState: c.timingQuality,
      lodgingEvidence: c.lodgingQuality,
      buyerOrganizer: c.organizationName,
      whoState: c.buyerWhoQuality,
      contactPath: c.buyerWhoQuality === "PATH_PRESENT",
      placementState: c.placementQuality,
      hotelFit: c.hotelFitQuality,
      howDiscovered: how,
      sourceFamily: how,
      queryPattern: how,
    };
  });

  const agg = summarizeControlPatterns(enriched);
  const byTrait = {
    requireNamedOrganizer: agg.pctEntityStrong >= 0.7,
    requireFutureTiming: agg.pctTimingPresent >= 0.7,
    requireLodgingOrHousingHint: agg.pctLodgingHint >= 0.35,
    requireHotelMotionThesis: agg.pctHotelMotion >= 0.7,
    preferWhoPath: agg.pctWhoPath >= 0.5,
    minSupportingSignals: 2,
  };

  const topDifferences = [
    "Named organizer/buyer entity (not SERP title fragment)",
    "Future date or recurring cycle with decision window",
    "Explicit hotel-motion thesis (overflow / housing / primary / stay-to-play)",
    "Public contact OR housing/organizer path",
    "Market-credible destination/venue — not generic activity news",
    "Often discovered via housing pages, association site-selection, procurement, or competitor host patterns — not bare event calendars",
  ];

  return {
    controls: enriched,
    aggregate: agg,
    patternRules: byTrait,
    topDifferencesVsZeroYield: topDifferences,
    controlHotels: CONTROL_HOTELS,
  };
}
