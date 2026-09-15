/**
 * Rad / GM feedback enrichment pass — product-quality fields without new Webhound spend.
 * Applies Demand Territory Fit, competitor classification, sourcing defaults,
 * explanations, and source summaries onto existing opportunity objects.
 *
 * Does NOT invent "new to hotel" or hotel-confirmed sourcing from public research.
 */

import { buildOpportunity } from "./opportunity-factory.js";
import {
  BOOKING_WINDOW,
  COMPETITOR_CLASS,
  DEMAND_TERRITORY_FIT,
  SOURCING_STATUS,
  INCREMENTAL_VALUE_STATUS,
  CLAIM_KIND,
  PRIORITY,
} from "./claim-types.js";
import { loadHotelDemandConfig, PILOT_HOTEL_ID } from "./hotel-profile.js";

function textBlob(o) {
  return [
    o.title,
    o.destinationStatus,
    o.venueStatus,
    o.summaryWhat,
    o.summaryWhyMatters,
    o.summaryWhyHotel,
    o.whyNow,
    o.organizationName,
    o.segment,
    ...(o.meetingHistory || []).map((h) => `${h.city} ${h.venue}`),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

export function classifyDemandTerritoryFit(opportunity) {
  // Explicit expansion-pass / curator lock wins over keyword heuristics.
  if (
    opportunity.demandTerritoryFitLocked &&
    opportunity.demandTerritoryFit &&
    Object.values(DEMAND_TERRITORY_FIT).includes(opportunity.demandTerritoryFit)
  ) {
    return {
      demandTerritoryFit: opportunity.demandTerritoryFit,
      demandTerritoryRationale:
        opportunity.demandTerritoryRationale ||
        "Curated Demand Territory Fit locked from DMV expansion research.",
      demandTerritoryFitLocked: true,
    };
  }

  // Event geography > organization name/HQ. Build blob WITHOUT organizationName
  // when destination/venue already carry a concrete city (avoids "Bethesda" org false positives).
  const dest = String(opportunity.destinationStatus || "").toLowerCase();
  const venue = String(opportunity.venueStatus || "").toLowerCase();
  const eventGeo = `${dest} ${venue}`.trim();
  const org = String(opportunity.organizationName || "").toLowerCase();
  const tFull = textBlob(opportunity);
  const tEventFirst = [
    opportunity.title,
    opportunity.destinationStatus,
    opportunity.venueStatus,
    opportunity.summaryWhat,
    opportunity.whyNow,
    ...(opportunity.meetingHistory || []).map((h) => `${h.city} ${h.venue}`),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  const eventHasConcreteCity =
    /\b(upper marlboro|national harbor|arlington|alexandria|college park|baltimore|fairfax|reston|tysons|orlando|las vegas|chicago|fort lauderdale)\b/i.test(
      eventGeo || tEventFirst
    );
  const t = eventHasConcreteCity ? tEventFirst : tFull;

  if (
    /gaylord national|national harbor|already contracted|washington hilton|hilton capitol|ritz-carlton washington|convention.?scale|walter e\. washington|convention center/.test(
      t
    )
  ) {
    if (/already contracted|already booked|host unfit|not a bethesda/.test(t)) {
      return {
        demandTerritoryFit: DEMAND_TERRITORY_FIT.OUTSIDE_REALISTIC_TERRITORY,
        demandTerritoryRationale:
          "Event is contracted elsewhere or is convention-scale beyond realistic Bethesda Marriott host competition.",
      };
    }
  }

  // Do not let organization name alone create Core territory when event city is elsewhere.
  if (eventHasConcreteCity && /upper marlboro|national harbor|baltimore/.test(tEventFirst)) {
    if (/upper marlboro|prince george/.test(tEventFirst)) {
      return {
        demandTerritoryFit: DEMAND_TERRITORY_FIT.DMV_COMPETITIVE,
        demandTerritoryRationale:
          "Event geography (not organization HQ/name) places this in broader DMV competitive territory.",
      };
    }
  }

  if (
    (/bethesda(?!\s+north)|montgomery county|rockville|nih |walter reed|premier cup|sbpo|vendor outreach/.test(
      tEventFirst
    ) ||
      /bethesda|rockville|nih/.test(venue) ||
      /bethesda|rockville|montgomery/.test(dest)) &&
    !eventHasConcreteCity
  ) {
    return {
      demandTerritoryFit: DEMAND_TERRITORY_FIT.BETHESDA_MONTGOMERY_CORE,
      demandTerritoryRationale:
        "Demand is anchored in Bethesda / Montgomery County or directly adjacent medical / NIH corridor anchors (event geography, not organization name alone).",
    };
  }

  // Organization-only Bethesda signal is weak — never Core by itself.
  if (/bethesda|rockville|montgomery/.test(org) && !/bethesda|rockville|montgomery|nih/.test(eventGeo || tEventFirst)) {
    return {
      demandTerritoryFit: DEMAND_TERRITORY_FIT.DMV_COMPETITIVE,
      demandTerritoryRationale:
        "Organization name/HQ suggests local presence, but confirmed event geography is missing or elsewhere — Demand Territory Fit uses event location first.",
    };
  }

  if (/nih|walter reed|medical|healthcare|scientific|amwa|acts|ahima/.test(t)) {
    return {
      demandTerritoryFit: DEMAND_TERRITORY_FIT.NORTH_DC_MEDICAL_CORRIDOR,
      demandTerritoryRationale:
        "Medical / scientific / federal-health demand where North DC–Bethesda medical corridor positioning is credible even if the event city is listed more broadly as DC-area.",
    };
  }

  if (
    /washington,\s*dc|washington dc|district of columbia|northern virginia|arlington|alexandria|tysons|dmv|metro/.test(
      t
    )
  ) {
    if (/capitol hill|downtown dc only|must be downtown/.test(t)) {
      return {
        demandTerritoryFit: DEMAND_TERRITORY_FIT.DMV_STRETCH,
        demandTerritoryRationale:
          "DMV event with downtown / Capitol Hill preference — Bethesda can compete on value and Metro access but is a stretch vs core downtown hotels.",
      };
    }
    return {
      demandTerritoryFit: DEMAND_TERRITORY_FIT.DMV_COMPETITIVE,
      demandTerritoryRationale:
        "DMV-area demand where Bethesda Marriott is a realistic alternative based on size, Metro access, parking, and federal / association attendee patterns — not limited to Montgomery County geography.",
    };
  }

  if (/fort lauderdale|orlando|las vegas|chicago|out of market|hq only/.test(t)) {
    return {
      demandTerritoryFit: DEMAND_TERRITORY_FIT.OUTSIDE_REALISTIC_TERRITORY,
      demandTerritoryRationale:
        "Primary event geography is outside the DMV; retained only as watch / HQ-pattern context if applicable.",
    };
  }

  return {
    demandTerritoryFit: DEMAND_TERRITORY_FIT.DMV_COMPETITIVE,
    demandTerritoryRationale:
      "Default DMV-competitive classification pending stronger destination evidence — expanded territory asks whether Bethesda can compete, not whether the event is physically in Montgomery County.",
  };
}

function matchCompetitor(name, strSet, groupSet) {
  if (!name) return null;
  const n = String(name).toLowerCase();
  for (const c of strSet) {
    if (n.includes(String(c.name).toLowerCase().slice(0, 12)) || String(c.name).toLowerCase().includes(n.slice(0, 12))) {
      return { ...c, class: COMPETITOR_CLASS.STR_COMP_SET };
    }
  }
  for (const c of groupSet) {
    const aliases = [c.name, ...(c.alsoKnownAs || [])];
    if (aliases.some((a) => n.includes(String(a).toLowerCase().slice(0, 12)) || String(a).toLowerCase().includes(n.slice(0, 12)))) {
      return { ...c, class: COMPETITOR_CLASS.RELEVANT_GROUP_DEMAND_ALTERNATIVE };
    }
  }
  return null;
}

export function classifyCompetitors(opportunity, config) {
  const strSet = config?.competitiveContext?.strCompSet || [];
  const groupSet = config?.competitiveContext?.relevantGroupDemandAlternatives || [];
  const primary = opportunity.likelyCompetitor || null;
  const matched = matchCompetitor(primary, strSet, groupSet);

  let likelyStrCompetitor = opportunity.likelyStrCompetitor || null;
  let likelyGroupCompetitor = opportunity.likelyGroupCompetitor || null;
  let competitorRationale = opportunity.competitorRationale || null;

  if (matched?.class === COMPETITOR_CLASS.STR_COMP_SET) {
    likelyStrCompetitor = matched.name;
  } else if (matched?.class === COMPETITOR_CLASS.RELEVANT_GROUP_DEMAND_ALTERNATIVE) {
    likelyGroupCompetitor = matched.name;
  }

  if (!likelyStrCompetitor) {
    // Default STR risk for local Bethesda/Rockville demand
    const territory = opportunity.demandTerritoryFit;
    if (
      territory === DEMAND_TERRITORY_FIT.BETHESDA_MONTGOMERY_CORE ||
      territory === DEMAND_TERRITORY_FIT.NORTH_DC_MEDICAL_CORRIDOR
    ) {
      likelyStrCompetitor = "Hyatt Regency Bethesda";
    }
  }
  if (!likelyGroupCompetitor) {
    likelyGroupCompetitor = "Bethesda North Marriott Hotel & Conference Center";
  }

  if (!competitorRationale) {
    const territory = opportunity.demandTerritoryFit;
    if (
      territory === DEMAND_TERRITORY_FIT.DMV_COMPETITIVE ||
      territory === DEMAND_TERRITORY_FIT.DMV_STRETCH
    ) {
      competitorRationale = `Opportunity-market competitors matter more than local STR comps for this territory. STR context retained for hotel benchmarking: ${likelyStrCompetitor || "see hotel STR set"}. Marriott-area alternative: ${likelyGroupCompetitor}.`;
    } else {
      competitorRationale = `STR context: ${likelyStrCompetitor || "see hotel STR set"}. Broader group alternative: ${likelyGroupCompetitor}. Marriott-area properties may compete for the same block even when outside the formal STR set.`;
    }
  }

  const competitors = [];
  const seen = new Set();
  for (const c of opportunity.marketCompetitors || []) {
    const key = c.name;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    competitors.push({
      name: c.name,
      class: c.class || COMPETITOR_CLASS.OPPORTUNITY_MARKET,
      classLabel: c.classLabel || "Opportunity Market Competitor",
      submarket: c.submarket || null,
      reason: c.reason || "Opportunity-specific market competitor",
      historicalEventRelationship: c.historicalEventRelationship || null,
    });
  }
  for (const c of [...strSet, ...groupSet]) {
    const key = c.name;
    if (seen.has(key)) continue;
    seen.add(key);
    competitors.push({
      name: c.name,
      class: c.class,
      classLabel:
        c.class === COMPETITOR_CLASS.STR_COMP_SET
          ? "STR Comp Set"
          : "Relevant Group-Demand Alternative",
      reason:
        c.class === COMPETITOR_CLASS.STR_COMP_SET
          ? "Hotel-provided STR competitive set member"
          : c.note || "Nearby Marriott-area alternative that may compete for group demand",
      historicalEventRelationship: null,
    });
  }

  return {
    likelyStrCompetitor,
    likelyGroupCompetitor,
    likelyCompetitor: likelyStrCompetitor || likelyGroupCompetitor || primary,
    competitorRationale,
    marketCompetitors: opportunity.marketCompetitors || [],
    competitors,
  };
}

function inferPublicSourcingStatus(opportunity) {
  const t = textBlob(opportunity);
  if (/housing|room block bid|official housing|hbc|passtkey|onpeak/.test(t)) {
    return {
      sourcingStatus: SOURCING_STATUS.PUBLIC_EVIDENCE_OF_BROADER_RFP_HOUSING,
      alreadySourcedToHotelDisplay:
        "Public evidence of broader RFP / housing process — hotel validation still required",
    };
  }
  if (/rfp|sourcing|bid list|hotel selection underway/.test(t)) {
    return {
      sourcingStatus: SOURCING_STATUS.PUBLIC_EVIDENCE_OF_BROADER_RFP_HOUSING,
      alreadySourcedToHotelDisplay:
        "Public evidence of an RFP / selection process — whether Bethesda was included is unknown",
    };
  }
  // Never claim hotel-sourced from public web alone
  return {
    sourcingStatus: SOURCING_STATUS.UNKNOWN,
    alreadySourcedToHotelDisplay: "Unknown — requires hotel validation",
  };
}

function sharpenWhyMatters(o) {
  if (o.summaryWhyMatters && !/government conference with strong fit/i.test(o.summaryWhyMatters)) {
    return o.summaryWhyMatters;
  }
  const size =
    o.estimatedAttendance && o.estimatedAttendance !== CLAIM_KIND.UNKNOWN
      ? `${o.estimatedAttendance}-attendee`
      : o.estimatedPeakRooms && o.estimatedPeakRooms !== CLAIM_KIND.UNKNOWN
        ? `${o.estimatedPeakRooms} peak-room`
        : "sized";
  const venueOpen = /tbd|not named|unresolved|open|tba/i.test(String(o.venueStatus || ""));
  return `${size} ${o.segment || "group"} opportunity${
    venueOpen ? " with an unresolved venue" : ""
  }. ${
    o.demandTerritoryFitLabel || "DMV"
  } positioning and hotel capacity make Bethesda Marriott a credible pursuable option — validate against the hotel's own lead systems before outbound.`;
}

function sharpenRecommendedAction(o) {
  if (o.recommendedAction && o.recommendedAction.length > 40) return o.recommendedAction;
  if (o.priority === PRIORITY.DISQUALIFIED) {
    return "Do not pursue as a primary host — already contracted elsewhere or outside realistic territory.";
  }
  if (o.bookingWindowStatus === BOOKING_WINDOW.CONTACT_NOW && o.primaryContact?.email) {
    return `Confirm whether the event has entered hotel sourcing, then contact ${o.primaryContact.name || "the identified planner"} to request inclusion and position Bethesda / NIH proximity.`;
  }
  if (
    o.bookingWindowStatus === BOOKING_WINDOW.QUALIFY_NOW ||
    o.bookingWindowStatus === BOOKING_WINDOW.RESEARCH_FURTHER
  ) {
    return "Qualify whether Bethesda is being considered and whether a housing list / RFP has opened; do not assume the lead is new to the hotel.";
  }
  if (o.bookingWindowStatus === BOOKING_WINDOW.WATCH || o.bookingWindowStatus === BOOKING_WINDOW.TOO_EARLY) {
    return "Monitor venue / date announcements before outbound sales; capture in watchlist only.";
  }
  return "Validate against hotel systems, then decide pursue vs watch.";
}

/**
 * Enrich a single opportunity object (returns new buildOpportunity result).
 */
export function enrichOpportunityForRadFeedback(opportunity, config) {
  const territory = classifyDemandTerritoryFit(opportunity);
  const withTerritory = { ...opportunity, ...territory };
  const comps = classifyCompetitors(withTerritory, config);
  const sourcing = inferPublicSourcingStatus(opportunity);

  let bookingWindowStatus = opportunity.bookingWindowStatus;
  if (bookingWindowStatus === BOOKING_WINDOW.RESEARCH_FURTHER) {
    bookingWindowStatus = BOOKING_WINDOW.QUALIFY_NOW;
  }

  const winThesis =
    opportunity.bethesdaWinThesis ||
    (territory.demandTerritoryFit === DEMAND_TERRITORY_FIT.DMV_COMPETITIVE ||
    territory.demandTerritoryFit === DEMAND_TERRITORY_FIT.DMV_STRETCH
      ? opportunity.summaryWhyHotel || opportunity.fitExplanation || null
      : opportunity.bethesdaWinThesis || null);

  const merged = {
    ...opportunity,
    ...territory,
    ...comps,
    ...sourcing,
    bookingWindowStatus,
    bethesdaWinThesis: winThesis,
    incrementalValueStatus:
      opportunity.incrementalValueStatus || INCREMENTAL_VALUE_STATUS.UNKNOWN,
    summaryWhyMatters: sharpenWhyMatters({
      ...opportunity,
      ...territory,
      demandTerritoryFitLabel: territory.demandTerritoryFit,
    }),
    recommendedAction: sharpenRecommendedAction({
      ...opportunity,
      bookingWindowStatus,
    }),
    fitComponents: {
      physicalFit: opportunity.physicalFitScore ?? opportunity.fitComponents?.physicalFit,
      geographyFit: opportunity.geographyFitScore ?? opportunity.fitComponents?.geographyFit,
      timing: opportunity.timingScore ?? opportunity.fitComponents?.timing,
      commercialValue:
        opportunity.commercialValueScore ?? opportunity.fitComponents?.commercialValue,
      historicalFit:
        opportunity.historicalFitScore ?? opportunity.fitComponents?.historicalFit,
      competitiveAccessibility:
        opportunity.competitiveAccessibilityScore ??
        opportunity.fitComponents?.competitiveAccessibility,
      contactability:
        opportunity.contactabilityScore ?? opportunity.fitComponents?.contactability,
    },
    confidenceInput: opportunity.confidenceInput
      ? opportunity.confidenceInput
      : opportunity.evidenceConfidenceFactors
        ? {
            sourceAuthority: opportunity.evidenceConfidenceFactors.sourceAuthority,
            independentSourceCount: Math.round(
              (opportunity.evidenceConfidenceFactors.independentSources || 0) / 20
            ),
            directness: opportunity.evidenceConfidenceFactors.directness,
            recency: opportunity.evidenceConfidenceFactors.recency,
            verifiedFieldRatio:
              (opportunity.evidenceConfidenceFactors.verifiedFieldRatio || 0) / 100,
            firstPartyShare: opportunity.evidenceConfidenceFactors.firstParty,
            completeness: opportunity.evidenceConfidenceFactors.completeness,
            conflictPenalty: opportunity.evidenceConfidenceFactors.conflictPenalty,
          }
        : undefined,
  };

  // Preserve identity / evidence / narratives that buildOpportunity will keep
  return buildOpportunity(merged);
}

/**
 * Apply Rad-feedback enrichment across a full opportunity list.
 * Dedupes by id (keeps first). Does not call Webhound.
 */
export function applyRadFeedbackEnrichmentPass(opportunities, hotelId = PILOT_HOTEL_ID) {
  const config = loadHotelDemandConfig(hotelId) || {};
  const seen = new Set();
  const out = [];
  for (const o of opportunities || []) {
    if (!o?.id || seen.has(o.id)) continue;
    seen.add(o.id);
    // Series-level soft dedupe: same title+year collapses
    out.push(enrichOpportunityForRadFeedback(o, config));
  }
  return {
    opportunities: out,
    enrichment: {
      pass: "rad_feedback_dmv_product_quality_v1",
      webhoundSpentUsd: 0,
      webhoundCapUsd: 15,
      note: "Product-quality enrichment only. Webhound hard cap unchanged; no additional Webhound spend in this pass (prior pilot already at/near $15).",
      appliedAt: new Date().toISOString(),
    },
  };
}
