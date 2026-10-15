/**
 * Canonical hotel-fit assessment for research → drawer.
 *
 * Dimensions (GM framework / scoring.js):
 *   physicalFit, geographyFit, timing, commercialValue,
 *   historicalFit, competitiveAccessibility, contactability
 *
 * Numeric scores only when inputs support the scoring model.
 * Otherwise emit dimension-level qualitative assessments (not "Qualitative"
 * alone). Contactability stays distinct from physical fit.
 */

import {
  computeHotelFitScore,
  HOTEL_FIT_COMPONENT_LABELS,
  DEFAULT_HOTEL_FIT_WEIGHTS,
} from "../scoring.js";
import { loadHotelConfig } from "./independent-hotel-fit-v1.js";
import {
  stripEvidenceClaimPrefixes,
  detectClaimKindFromProse,
} from "./customer-facing-prose-v1.js";

export const HOTEL_FIT_ASSESSMENT_VERSION = "gdi-hotel-fit-assessment-v1";

function text(v) {
  return stripEvidenceClaimPrefixes(String(v || "").replace(/\s+/g, " ").trim());
}

function blob(opp = {}) {
  return [
    opp.title,
    opp.organizationName,
    opp.summaryWhat,
    opp.destinationStatus,
    opp.eventLocationSummary,
    opp.eventCity,
    opp.venueCity,
    opp.city,
    opp.hotelOpportunityThesis,
    opp.fitExplanation,
    opp.competitiveContextNote,
    opp.venueHistoryNote,
    opp.whyHotelDemand,
    opp.travelingCohort,
  ]
    .map(text)
    .filter(Boolean)
    .join(" ");
}

function hotelBits(cfg = {}) {
  return {
    name:
      text(cfg.displayName) ||
      text(cfg.name) ||
      text(cfg.aliases?.note)?.split(".")[0] ||
      "this hotel",
    rooms: cfg.capabilityProfile?.totalGuestrooms ?? cfg.capabilityProfile?.rooms ?? null,
    soft: text(cfg.capabilityProfile?.softBrand),
    territory: text(cfg.demandTerritory?.label),
    city: text(cfg.city) || text(cfg.locale?.city),
    peakMin: cfg.commercialPriorities?.coreTargetPeakRoomsMin ?? null,
    peakMax: cfg.commercialPriorities?.coreTargetPeakRoomsMax ?? null,
  };
}

function occasionGeo(opp = {}) {
  return text(
    opp.eventLocationSummary ||
      opp.destinationStatus ||
      [opp.eventCity, opp.venueCity].filter(Boolean).join(", ") ||
      ""
  );
}

function classifyGeography(opp = {}, hotel = {}, cfg = {}) {
  const geo = occasionGeo(opp).toLowerCase();
  const full = blob(opp).toLowerCase();
  const hotelCity = (hotel.city || "").toLowerCase();
  const kw = cfg.demandTerritory?.classificationKeywords || {};

  for (const [klass, words] of Object.entries(kw)) {
    for (const w of words || []) {
      if (full.includes(String(w).toLowerCase()) || geo.includes(String(w).toLowerCase())) {
        return { class: klass, geo: occasionGeo(opp) || null };
      }
    }
  }

  // Mayflower / downtown DC vs Arlington WashCon pattern
  if (
    /mayflower|connecticut|downtown\s*dc|farragut|dupont/i.test(hotel.name + " " + hotelCity) &&
    /arlington|crystal\s*city|crystal\s*gateway/i.test(full)
  ) {
    return { class: "TERRITORY_STRETCH", geo: occasionGeo(opp) || "Arlington / Crystal City pattern" };
  }

  if (geo && hotelCity && geo.includes(hotelCity)) {
    return { class: "CORE_TERRITORY", geo: occasionGeo(opp) };
  }
  if (/washington|district of columbia|\bdc\b/.test(full) && /washington|dc/i.test(hotelCity + hotel.territory)) {
    return { class: "TERRITORY_COMPETITIVE", geo: occasionGeo(opp) || "Washington DC metro" };
  }
  return { class: null, geo: occasionGeo(opp) || null };
}

function hasUsableContact(opp = {}) {
  const c = opp.primaryContact || {};
  return Boolean(
    text(c.email) ||
      text(c.phone) ||
      text(opp.primaryContactEmail) ||
      text(opp.functionalContactEmail) ||
      text(opp.publicContactPath) ||
      text(opp.organizationContactUrl) ||
      text(opp.contactUrl)
  );
}

/**
 * Build dimension assessments + optional numeric score.
 */
export function assessHotelFitForOpportunity(opportunity = {}, opts = {}) {
  const hotelId = String(opts.hotelId || opportunity.hotelId || "").trim();
  const cfg = opts.hotelConfig || loadHotelConfig(hotelId) || {};
  const hotel = hotelBits(cfg);
  const full = blob(opportunity);
  const geoInfo = classifyGeography(opportunity, hotel, cfg);
  const peak =
    opportunity.peakRooms ??
    opportunity.estimatedPeakRooms ??
    opportunity.publishedPeakRooms ??
    null;
  const peakN = peak != null && Number.isFinite(Number(peak)) ? Number(peak) : null;
  const sourcing = String(
    opportunity.venueSourcingStatus || opportunity.venueSourcingStatusLabel || ""
  ).toUpperCase();
  const sourcingUnconfirmed =
    /UNCONFIRM|UNKNOWN|TBA|NOT\s*NAMED|NOT\s*PUBLICLY/i.test(sourcing) ||
    /hotel not (yet )?named|hotel\/host is not confirmed|sourcing unconfirmed/i.test(full);

  const dimensions = {};

  // Physical / product fit
  if (peakN != null && hotel.rooms != null) {
    const ratio = peakN / hotel.rooms;
    let level = "Workable";
    let score = 70;
    if (ratio <= 0.35) {
      level = "Strong capacity alignment";
      score = 85;
    } else if (ratio <= 0.55) {
      level = "Workable peak vs property size";
      score = 72;
    } else if (ratio <= 0.85) {
      level = "Tight vs total keys — overflow/partial block more likely";
      score = 48;
    } else {
      level = "Peak exceeds realistic single-hotel capture";
      score = 28;
    }
    dimensions.physicalFit = {
      level,
      score,
      rationale: `${hotel.name} lists ~${hotel.rooms} guestrooms; evidenced peak ~${peakN}. ${level}. Total keys ≠ available group inventory.`,
      claimKind: "MIXED",
      scorable: true,
    };
  } else if (hotel.rooms != null) {
    dimensions.physicalFit = {
      level: "Product plausible; peak rooms unknown",
      score: null,
      rationale: `${hotel.name} (~${hotel.rooms} guestrooms${hotel.soft ? `, ${hotel.soft}` : ""}) can host association / meeting lodging in principle, but peak room demand is not published — do not invent a block size.`,
      claimKind: "INFERENCE",
      scorable: false,
    };
  } else {
    dimensions.physicalFit = {
      level: "Insufficient product evidence",
      score: null,
      rationale: "Property capacity not configured; physical fit not scored.",
      claimKind: "UNKNOWN",
      scorable: false,
    };
  }

  // Geography / travel practicality
  if (geoInfo.class === "OUTSIDE_REALISTIC_TERRITORY") {
    dimensions.geographyFit = {
      level: "Outside realistic demand territory",
      score: 15,
      rationale: `Occasion geography (${geoInfo.geo || "unconfirmed"}) reads outside ${hotel.name}'s configured territory (${hotel.territory || hotel.city || "see hotel config"}).`,
      claimKind: "FACT",
      scorable: true,
    };
  } else if (geoInfo.class === "TERRITORY_STRETCH") {
    dimensions.geographyFit = {
      level: "Stretch geography / travel practicality",
      score: 38,
      rationale: `Occasion footprint (${geoInfo.geo || "see evidence"}) is a stretch vs ${hotel.name}'s location (${hotel.city || hotel.territory || "configured territory"}). Treat as overflow / downtown-preference only — not automatic primary host.`,
      claimKind: "MIXED",
      scorable: true,
    };
  } else if (geoInfo.class === "CORE_TERRITORY" || geoInfo.class === "TERRITORY_COMPETITIVE") {
    const core = geoInfo.class === "CORE_TERRITORY";
    dimensions.geographyFit = {
      level: core ? "In-territory" : "Competitive metro geography",
      score: core ? 86 : 68,
      rationale: `Occasion geography (${geoInfo.geo || "metro"}) is ${core ? "inside" : "competitive within"} ${hotel.name}'s demand territory.`,
      claimKind: "MIXED",
      scorable: true,
    };
  } else {
    dimensions.geographyFit = {
      level: "Geography not fully pinned",
      score: null,
      rationale: `Occasion location vs ${hotel.name} is not fully evidenced this pass — confirm meeting footprint before primary-host pursuit.`,
      claimKind: "UNKNOWN",
      scorable: false,
    };
  }

  // Timing / commercial accessibility
  const timingRaw = text(
    opportunity.whyNow ||
      opportunity.summaryWhyNow ||
      opportunity.eventDateDisplay ||
      opportunity.eventStartDate
  );
  if (sourcingUnconfirmed && timingRaw) {
    dimensions.timing = {
      level: "Window to ask before host lock",
      score: 62,
      rationale: `Dates/city are public while hotel selection remains unconfirmed — practical window to ask about lodging process before a host announcement. Not proof of open buying.`,
      claimKind: "INFERENCE",
      scorable: true,
    };
  } else if (/RFP_ACTIVE|OPEN_UNRESOLVED/i.test(sourcing)) {
    dimensions.timing = {
      level: "Active / open sourcing signal",
      score: 78,
      rationale: `Venue sourcing status indicates active or unresolved sourcing (${sourcing}).`,
      claimKind: "FACT",
      scorable: true,
    };
  } else if (timingRaw) {
    dimensions.timing = {
      level: "Timing known; openness unknown",
      score: null,
      rationale: `Timing evidence: ${timingRaw.slice(0, 180)}. Confirm whether lodging contracting is still open before intensifying outreach.`,
      claimKind: "MIXED",
      scorable: false,
    };
  } else {
    dimensions.timing = {
      level: "Timing not established",
      score: null,
      rationale: "No verified occasion dates for timing/winnability assessment.",
      claimKind: "UNKNOWN",
      scorable: false,
    };
  }

  // Commercial potential (kept modest without room volume)
  if (peakN != null && peakN >= 40) {
    dimensions.commercialValue = {
      level: "Material peak room potential",
      score: 70,
      rationale: `Published/estimated peak ~${peakN} supports a commercially material conversation if the hotel can participate.`,
      claimKind: "MIXED",
      scorable: true,
    };
  } else if (/VERIFIED_HOUSING_PROGRAM/i.test(String(opportunity.roomDemandStatus || ""))) {
    dimensions.commercialValue = {
      level: "Housing program exists; peak unknown",
      score: null,
      rationale:
        "A verified housing / stay-to-play program exists, but that alone does not quantify peak rooms or confirm open inclusion for this hotel.",
      claimKind: "FACT",
      scorable: false,
    };
  } else {
    dimensions.commercialValue = {
      level: "Commercial size not quantified",
      score: null,
      rationale: "Peak rooms / room nights not published — commercial potential remains qualitative.",
      claimKind: "UNKNOWN",
      scorable: false,
    };
  }

  // Historical venue / hotel pattern
  if (/crystal\s*gateway|arlington\s*host|historically\s*arlington/i.test(full)) {
    dimensions.historicalFit = {
      level: "Host pattern favors another corridor",
      score: 32,
      rationale:
        "Historical WashCon / host pattern points to Arlington / Crystal City lodging. Downtown Mayflower would need an explicit downtown preference or overflow need — not the default pattern.",
      claimKind: "FACT",
      scorable: true,
    };
  } else if (/official (hotel|housing|block)|named host|primary host/i.test(full)) {
    dimensions.historicalFit = {
      level: "Prior host / housing pattern evidenced",
      score: 55,
      rationale: "Prior host or official housing pattern is evidenced; use it as constraint, not as automatic win for this property.",
      claimKind: "MIXED",
      scorable: true,
    };
  } else {
    dimensions.historicalFit = {
      level: "Limited host-history evidence",
      score: null,
      rationale: "Prior venue/hotel pattern not exhaustively assessed this pass.",
      claimKind: "UNKNOWN",
      scorable: false,
    };
  }

  // Competitive position
  if (/crystal\s*gateway|grand\s*hyatt|omni\s*shoreham|hilton\s*national\s*mall/i.test(full)) {
    dimensions.competitiveAccessibility = {
      level: "Named competitor / primary likely",
      score: 35,
      rationale:
        "A primary or patterned host may already be named or strongly favored — pitch overflow/preferred only unless selection is confirmed open.",
      claimKind: "MIXED",
      scorable: true,
    };
  } else if (sourcingUnconfirmed) {
    dimensions.competitiveAccessibility = {
      level: "Competition unresolved",
      score: 50,
      rationale: "Host hotel unpublished — competitive set not locked; do not assume exclusive access.",
      claimKind: "INFERENCE",
      scorable: true,
    };
  } else {
    dimensions.competitiveAccessibility = {
      level: "Competitive position not established",
      score: null,
      rationale: "Competing hotels and prior hosts not exhaustively assessed this pass.",
      claimKind: "UNKNOWN",
      scorable: false,
    };
  }

  // Contactability — distinct from physical fit
  if (hasUsableContact(opportunity)) {
    dimensions.contactability = {
      level: "Usable contact route",
      score: 78,
      rationale: "A published person and/or method exists for a lodging conversation. Contactability ≠ physical or geographic fit.",
      claimKind: "FACT",
      scorable: true,
    };
  } else {
    dimensions.contactability = {
      level: "Contact route incomplete",
      score: 25,
      rationale: "No usable person/email/phone/URL route yet — enrich contact before treating as sales-ready. Distinct from product fit.",
      claimKind: "FACT",
      scorable: true,
    };
  }

  const scorableKeys = Object.keys(dimensions).filter((k) => dimensions[k].scorable && dimensions[k].score != null);
  const canNumeric = scorableKeys.length >= 5; // require most dimensions before overall number

  let numeric = null;
  if (canNumeric) {
    const input = {};
    for (const [k, d] of Object.entries(dimensions)) {
      input[k] = d.score != null ? d.score : 50;
    }
    numeric = computeHotelFitScore(input);
  }

  // Overall narrative — no invented primary-host advantage
  const stretch =
    geoInfo.class === "TERRITORY_STRETCH" ||
    dimensions.geographyFit?.score != null && dimensions.geographyFit.score < 45;
  const noAdvantage =
    stretch ||
    (dimensions.historicalFit?.score != null && dimensions.historicalFit.score < 40);

  let summary;
  if (noAdvantage) {
    summary = `No supported primary-host advantage for ${hotel.name} versus the occasion geography/host pattern (${geoInfo.geo || "see evidence"}). Exploratory / stretch posture — pursue only if planners signal openness to this location or an overflow need appears.`;
  } else if (numeric) {
    summary = `Hotel-fit score ${numeric.hotelFitScore} (${numeric.scoringVersion}) from evidenced dimensions. Re-check before raising pursuit intensity.`;
  } else {
    summary = `Dimension-level hotel-fit assessment for ${hotel.name} — overall numeric score withheld until more components are evidenced.`;
  }

  const posture = noAdvantage
    ? "EXPLORATORY_STRETCH"
    : numeric && numeric.hotelFitScore >= 72
      ? "PRIORITY_CANDIDATE"
      : "QUALIFY_FURTHER";

  return {
    version: HOTEL_FIT_ASSESSMENT_VERSION,
    scoringVersion: numeric?.scoringVersion || null,
    hotelId,
    hotelName: hotel.name,
    assessedAt: new Date().toISOString(),
    posture,
    summary: text(summary),
    occasionGeography: geoInfo.geo,
    territoryClass: geoInfo.class,
    dimensions,
    componentLabels: { ...HOTEL_FIT_COMPONENT_LABELS },
    weights: { ...DEFAULT_HOTEL_FIT_WEIGHTS },
    hotelFitScore: numeric?.hotelFitScore ?? null,
    components: numeric?.components ?? null,
    numericScoreEmitted: Boolean(numeric),
    inputs: {
      peakRooms: peakN,
      totalGuestrooms: hotel.rooms,
      venueSourcingStatus: sourcing || null,
      hasUsableContact: hasUsableContact(opportunity),
      occasionGeography: geoInfo.geo,
      territoryClass: geoInfo.class,
    },
  };
}

/**
 * Apply assessment onto opportunity fields used by DTO / drawer.
 */
export function applyHotelFitAssessmentToOpportunity(opportunity = {}, assessment = null) {
  const a = assessment || assessHotelFitForOpportunity(opportunity);
  const next = { ...opportunity };
  next.hotelFitAssessment = a;
  next.hotelFitAssessmentVersion = a.version;
  next.hotelFitPosture = a.posture;

  const dimLines = Object.entries(a.dimensions || {}).map(([key, d]) => {
    const label = a.componentLabels?.[key] || key;
    return `${label}: ${d.level}. ${d.rationale}`;
  });

  next.hotelFitDimensionNotes = dimLines;
  next.hotelFitComponentLabels = a.componentLabels;

  if (a.numericScoreEmitted && a.hotelFitScore != null) {
    next.hotelFitScore = a.hotelFitScore;
    next.fitComponents = a.components;
    next.hotelFitScoreBasis = a.scoringVersion;
    next.hotelFitScoreNote = `${a.summary} Inputs: peak=${a.inputs.peakRooms ?? "unknown"}, keys=${a.inputs.totalGuestrooms ?? "unknown"}, geo=${a.inputs.occasionGeography || "unconfirmed"}.`;
    next.hotelFitRationale = a.summary;
  } else {
    // Do not invent a number
    if (next.hotelFitScore == null) {
      next.hotelFitScore = null;
    }
    next.hotelFitScoreBasis = null;
    next.hotelFitScoreNote = a.summary;
    next.hotelFitRationale = [a.summary, ...dimLines.slice(0, 4)].join(" ");
    // Expose qualitative component map for UI (levels, not fake scores)
    next.hotelFitQualitativeComponents = Object.fromEntries(
      Object.entries(a.dimensions).map(([k, d]) => [
        k,
        { level: d.level, rationale: d.rationale, claimKind: d.claimKind },
      ])
    );
  }

  // Prefer assessment summary for why-this-hotel when stretch / no advantage
  if (a.posture === "EXPLORATORY_STRETCH") {
    const existing = text(next.fitExplanation || next.hotelOpportunityThesis);
    if (!existing || existing.length < 40 || detectClaimKindFromProse(existing)) {
      next.fitExplanation = a.summary;
      next.fitRationale = a.summary;
      next.hotelOpportunityThesis = a.summary;
      next.summaryWhyHotel = a.summary;
    }
  }

  return next;
}
