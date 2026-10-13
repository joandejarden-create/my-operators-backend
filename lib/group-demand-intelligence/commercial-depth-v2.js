/**
 * GDI Commercial Depth V2 — lodging-primary enrichment for existing opportunities.
 *
 * Deepens drawer fields, lodging evidence, lodging-primary motion, thesis/why/action,
 * contact completeness (Jev SAFE APPLY), and strict ACTIONABLE_NOW gate.
 * Does not invent room demand or lower CQ.
 */

import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../hotel-intelligence/room-count-research/fetch.js";
import { serpapiSearch } from "../research-engine-v2/providers/serpapi-google-hotels/client.js";
import { decide } from "./jev/jev-decision-service.js";
import { JEV_DECISION_TYPE, CHOICES } from "./jev/jev-types.js";
import { resolveContactCompletenessV1 } from "./contact-intelligence-completeness-v1.js";
import {
  classifyContactTier,
  CONTACT_TIER,
} from "./contact-tiers-v1-2.js";
import {
  gradeContactCompleteness,
  attachCompletenessFields,
  splitWhoHow,
  PUBLIC_CONTACT_CEILING_REASON,
} from "./contact-completeness-v1.js";
import {
  enrichGdiOpportunityForCustomer,
  RESEARCH_FIELD_STATE,
  shouldHideAfterEnrichment,
} from "./enrich-gdi-opportunity-for-customer.js";
import { loadHotelDemandConfig } from "./hotel-profile.js";
import { VENUE_SOURCING_STATUS, ROOM_DEMAND_STATUS } from "./claim-types.js";

export const COMMERCIAL_DEPTH_V2 = "gdi_commercial_depth_v2";

export const EVIDENCE_STATUS = Object.freeze({
  CONFIRMED: "CONFIRMED",
  ESTIMATED: "ESTIMATED",
  INFERRED: "INFERRED",
  UNKNOWN_AFTER_RESEARCH: "UNKNOWN_AFTER_RESEARCH",
  NOT_RESEARCHED: "NOT_RESEARCHED",
  INVALID: "INVALID",
});

export const LODGING_MOTION = Object.freeze({
  ROOM_BLOCK: "ROOM_BLOCK",
  HOUSING: "HOUSING",
  OVERFLOW: "OVERFLOW",
  CREW: "CREW",
  TOUR: "TOUR",
  ENTERTAINMENT_PRODUCTION: "ENTERTAINMENT_PRODUCTION",
  CORPORATE_BLOCK: "CORPORATE_BLOCK",
  SPORTS_HOUSING: "SPORTS_HOUSING",
  ASSOCIATION_HOUSING: "ASSOCIATION_HOUSING",
  CITYWIDE_CONVENTION_HOUSING: "CITYWIDE_CONVENTION_HOUSING",
  SOCIAL_HOUSING: "SOCIAL_HOUSING",
  VENUE_PARTNERSHIP: "VENUE_PARTNERSHIP",
  FUTURE_WATCH: "FUTURE_WATCH",
  FULL_MEETING_RFP: "FULL_MEETING_RFP",
  OTHER: "OTHER",
});

function clean(s) {
  return String(s || "").trim();
}

function hasSerp() {
  return !!(process.env.SERPAPI_API_KEY || process.env.SERPAPI_KEY);
}

/**
 * Lodging-primary motion from hotel meeting capacity + opportunity signals.
 * Does not force FULL_MEETING_RFP when hotel meeting space is thin.
 */
export function classifyLodgingPrimaryMotion(opp = {}, hotelConfig = null) {
  const meeting = Number(hotelConfig?.capabilityProfile?.totalMeetingSpaceSqFt) || 0;
  const limitedMeetings = meeting > 0 && meeting < 2000;
  const blob = `${opp.title} ${opp.organizationName} ${opp.opportunityType} ${opp.segment} ${opp.destinationStatus}`.toLowerCase();

  if (/world cup|fifa|olympics|u20|tournament|sports/i.test(blob)) {
    return LODGING_MOTION.SPORTS_HOUSING;
  }
  if (/broadway|production|crew|film|tour/i.test(blob)) {
    return /tour/.test(blob) ? LODGING_MOTION.TOUR : LODGING_MOTION.ENTERTAINMENT_PRODUCTION;
  }
  if (/javits|citywide|convention housing/i.test(blob)) {
    return LODGING_MOTION.CITYWIDE_CONVENTION_HOUSING;
  }
  if (/hotel week|celebration offer|social|wedding/i.test(blob)) {
    return LODGING_MOTION.SOCIAL_HOUSING;
  }
  if (/corporate|leadership|forum|sanctions/i.test(blob)) {
    return limitedMeetings ? LODGING_MOTION.CORPORATE_BLOCK : LODGING_MOTION.FULL_MEETING_RFP;
  }
  if (/association|annual meeting|conference|symposium|ijcai|cda|uscbs/i.test(blob)) {
    return limitedMeetings ? LODGING_MOTION.ASSOCIATION_HOUSING : LODGING_MOTION.FULL_MEETING_RFP;
  }
  if (
    opp.opportunityType === "OVERFLOW_HOUSING" ||
    /overflow/i.test(blob)
  ) {
    return LODGING_MOTION.OVERFLOW;
  }
  if (/housing|room block|accommodation/i.test(blob)) {
    return LODGING_MOTION.HOUSING;
  }
  if (limitedMeetings) return LODGING_MOTION.FUTURE_WATCH;
  return LODGING_MOTION.OTHER;
}

/**
 * Extract lodging signals from page text — evidence only, no invented counts.
 */
export function extractLodgingSignalsFromText(text = "", url = null) {
  const t = String(text || "");
  const low = t.toLowerCase();
  const out = {
    housingPageFound: false,
    roomBlockMentioned: false,
    housingOpen: null,
    hostHotelMentioned: false,
    overflowMentioned: false,
    attendance: null,
    attendanceStatus: EVIDENCE_STATUS.UNKNOWN_AFTER_RESEARCH,
    peakRooms: null,
    peakRoomsStatus: EVIDENCE_STATUS.UNKNOWN_AFTER_RESEARCH,
    housingDeadline: null,
    snippets: [],
    sourceUrl: url || null,
  };

  if (/hotel\s*block|room\s*block|group\s*rate|housing\s*(block|portal|bureau)|book\s*your\s*hotel|official\s*hotel|host\s*hotel|accommodations?/i.test(t)) {
    out.roomBlockMentioned = true;
    out.housingPageFound = /housing|accommodation|hotel\s*block|room\s*block/i.test(t);
    out.snippets.push("room_block_or_housing_language");
  }
  if (/host\s*hotel|headquarters\s*hotel|official\s*hotel/i.test(t)) {
    out.hostHotelMentioned = true;
  }
  if (/overflow|rooming\s*list|additional\s*hotels?/i.test(t)) {
    out.overflowMentioned = true;
  }
  if (/housing\s*(is\s*)?(now\s*)?open|register\s*for\s*housing|book\s*housing/i.test(t)) {
    out.housingOpen = true;
  } else if (/housing\s*(not\s*yet|coming\s*soon|will\s*open)|hotel\s*block\s*tbd/i.test(t)) {
    out.housingOpen = false;
  }

  const att = t.match(
    /(?:expected\s+)?(?:attendance|attendees|participants|delegates)[:\s]+(?:approximately\s+|about\s+|~)?([0-9][0-9,]{2,6})/i
  );
  if (att) {
    out.attendance = Number(String(att[1]).replace(/,/g, ""));
    out.attendanceStatus = EVIDENCE_STATUS.CONFIRMED;
  }

  const peak = t.match(
    /(?:peak\s+)?(?:room\s*block|rooms?\s*needed|room\s*nights?)[:\s]+(?:approximately\s+|about\s+|~)?([0-9][0-9,]{1,5})/i
  );
  if (peak) {
    out.peakRooms = Number(String(peak[1]).replace(/,/g, ""));
    out.peakRoomsStatus = EVIDENCE_STATUS.CONFIRMED;
  }

  const deadline = t.match(
    /(?:housing|hotel\s*block|room\s*block)\s*(?:deadline|closes|cut[\s-]?off)[:\s]+([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{4}-\d{2}-\d{2})/i
  );
  if (deadline) out.housingDeadline = deadline[1];

  return out;
}

function mapVenueFromLodging(signals, opp) {
  if (signals.housingOpen === true) {
    return VENUE_SOURCING_STATUS.REGISTRATION_OPEN_HOTEL_UNANNOUNCED;
  }
  if (signals.housingOpen === false) return VENUE_SOURCING_STATUS.HOUSING_PENDING;
  if (signals.overflowMentioned && signals.hostHotelMentioned) {
    return VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE;
  }
  if (signals.hostHotelMentioned && !signals.overflowMentioned) {
    return VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE;
  }
  if (signals.roomBlockMentioned) return VENUE_SOURCING_STATUS.HOTEL_VENUE_TBD;
  if (opp.venueSourcingStatus && opp.venueSourcingStatus !== "UNKNOWN") {
    return opp.venueSourcingStatus;
  }
  return VENUE_SOURCING_STATUS.HOUSING_PENDING;
}

/**
 * Ask Jev which research playbook to use for lodging depth — SAFE APPLY when differs.
 */
export async function decideLodgingResearchPlaybook(opportunity = {}, defaultPlaybook = "LODGING_HOUSING") {
  const result = await decide({
    decisionType: JEV_DECISION_TYPE.RESEARCH_PLAYBOOK,
    context: {
      hotelId: opportunity.hotelId,
      title: opportunity.title,
      organizationName: opportunity.organizationName,
      opportunityType: opportunity.opportunityType,
      roomDemandStatus: opportunity.roomDemandStatus,
      missingFields: ["lodgingEvidence", "roomDemand", "housingPage"],
      lodgingEvidence: "unknown",
    },
    existingDecision: defaultPlaybook,
    policyContext: { noInventPerson: true },
    fallbackDecision: defaultPlaybook,
  });
  const selected = result.selected || result.effectiveDecision || defaultPlaybook;
  const allowed = new Set(CHOICES.RESEARCH_PLAYBOOK);
  const valid = allowed.has(selected) ? selected : defaultPlaybook;
  const applied = valid !== defaultPlaybook && !result.technicalFallback && !result.policyFallback;
  return {
    defaultPlaybook,
    jevPlaybook: valid,
    applied,
    agreement: valid === defaultPlaybook,
    confidence: result.confidence,
    technicalFallback: result.technicalFallback,
    result,
  };
}

export async function researchLodgingEvidence(opportunity = {}, opts = {}) {
  const fetches = [];
  const queries = [];
  const urls = new Set();
  for (const s of opportunity.sources || []) {
    if (s?.url) urls.add(s.url);
  }
  if (opportunity.officialSource) urls.add(opportunity.officialSource);
  if (opportunity.discoverySource) urls.add(opportunity.discoverySource);

  // Jev playbook routing (SAFE APPLY when different)
  const playbook = await decideLodgingResearchPlaybook(opportunity);
  const useSports =
    playbook.jevPlaybook === "SPORTS_HOUSING" ||
    /fifa|world cup|u20|olympics/i.test(`${opportunity.title}`);
  const queryCore = [
    opportunity.title,
    opportunity.organizationName,
    useSports ? "official hotel housing block" : "official housing hotel block accommodations",
    "New York",
  ]
    .filter(Boolean)
    .join(" ");

  if (hasSerp() && opts.allowSerp !== false) {
    try {
      queries.push(queryCore);
      const serp = await serpapiSearch({
        engine: "google",
        q: queryCore,
        num: 5,
        hl: "en",
        gl: "us",
      });
      const organic = serp?.data?.organic_results || [];
      for (const r of organic) {
        if (r.link) urls.add(r.link);
      }
    } catch (err) {
      fetches.push({ error: String(err?.message || err), kind: "serp" });
    }
  }

  let merged = extractLodgingSignalsFromText("", null);
  merged.pagesFetched = 0;
  const pageReports = [];
  const maxPages = opts.maxPages ?? 4;
  let i = 0;
  for (const url of urls) {
    if (i >= maxPages) break;
    i += 1;
    const page = await fetchResearchPage(url);
    merged.pagesFetched += 1;
    fetches.push({ url, ok: page.ok, status: page.status });
    if (!page.ok) continue;
    const text = htmlToSearchableText(page.text);
    const sig = extractLodgingSignalsFromText(text, page.url || url);
    pageReports.push({ url: page.url || url, ...sig, textLen: text.length });
    merged.housingPageFound = merged.housingPageFound || sig.housingPageFound;
    merged.roomBlockMentioned = merged.roomBlockMentioned || sig.roomBlockMentioned;
    merged.hostHotelMentioned = merged.hostHotelMentioned || sig.hostHotelMentioned;
    merged.overflowMentioned = merged.overflowMentioned || sig.overflowMentioned;
    if (sig.housingOpen != null && merged.housingOpen == null) merged.housingOpen = sig.housingOpen;
    if (sig.attendance != null && merged.attendance == null) {
      merged.attendance = sig.attendance;
      merged.attendanceStatus = sig.attendanceStatus;
    }
    if (sig.peakRooms != null && merged.peakRooms == null) {
      merged.peakRooms = sig.peakRooms;
      merged.peakRoomsStatus = sig.peakRoomsStatus;
    }
    if (sig.housingDeadline && !merged.housingDeadline) merged.housingDeadline = sig.housingDeadline;
    merged.snippets.push(...sig.snippets);
    if (!merged.sourceUrl && (sig.housingPageFound || sig.roomBlockMentioned)) {
      merged.sourceUrl = page.url || url;
    }
  }

  if (!merged.attendance) merged.attendanceStatus = EVIDENCE_STATUS.UNKNOWN_AFTER_RESEARCH;
  if (!merged.peakRooms) merged.peakRoomsStatus = EVIDENCE_STATUS.UNKNOWN_AFTER_RESEARCH;

  return {
    signals: merged,
    playbook,
    queries,
    fetches,
    pageReports,
  };
}

export function buildLodgingThesis(opp, hotelConfig, motion, signals) {
  const name = hotelConfig?.displayName || "Hilton New York Times Square";
  const rooms = hotelConfig?.capabilityProfile?.totalGuestrooms || 478;
  const meeting = hotelConfig?.capabilityProfile?.totalMeetingSpaceSqFt || 300;
  const bits = [
    `${rooms}-room Midtown / Times Square inventory`,
    "Broadway / Midtown transit access",
  ];
  if (meeting && meeting < 2000) {
    bits.push(`limited formal meeting space (${meeting} sq ft) — lodging / room-block motion preferred over in-house meetings`);
  }
  if (signals.overflowMentioned) bits.push("overflow / secondary-block candidacy");
  if (signals.roomBlockMentioned) bits.push("public room-block / housing language present");
  if (motion === LODGING_MOTION.SPORTS_HOUSING) bits.push("citywide sports lodging adjacency");
  if (motion === LODGING_MOTION.ASSOCIATION_HOUSING || motion === LODGING_MOTION.OVERFLOW) {
    bits.push("association / conference housing rather than ballroom RFP");
  }
  return `${name} is a credible ${motion.replace(/_/g, " ").toLowerCase()} option for ${clean(opp.title)} because of ${bits.join("; ")}.`;
}

export function buildLodgingWhyHotel(opp, hotelConfig, motion) {
  const name = hotelConfig?.displayName || "This hotel";
  const rooms = hotelConfig?.capabilityProfile?.totalGuestrooms || 478;
  return `${name}: ${rooms} guestrooms in Times Square / Midtown with strong leisure and transient capacity; suited to ${String(motion).replace(/_/g, " ").toLowerCase()} rather than large in-house association meetings.`.slice(0, 280);
}

export function buildLodgingWhyNow(opp, signals) {
  if (signals.housingOpen === true) {
    return "Housing / hotel block channel appears open — confirm Midtown partner acceptance now.";
  }
  if (signals.housingDeadline) {
    return `Housing / room-block deadline signal found (${signals.housingDeadline}) — verify remaining Midtown inventory need.`;
  }
  if (signals.roomBlockMentioned && signals.housingOpen === false) {
    return "Monitor — housing / hotel sourcing referenced but not yet open.";
  }
  if (signals.roomBlockMentioned) {
    return "Room-block / housing language found on official sources — confirm whether Midtown partners are still being accepted.";
  }
  const year = opp.eventYear || String(opp.eventStartDate || "").slice(0, 4);
  if (year) {
    return `Monitor — housing / hotel sourcing not yet public for the ${year} cycle.`;
  }
  return "Monitor — housing / hotel sourcing not yet public.";
}

export function buildLodgingRecommendedAction(opp, contact, signals) {
  const person = contact?.name || opp.primaryContact?.name || opp.primaryContactName;
  const role = contact?.role || opp.primaryContact?.role || "housing / meetings contact";
  if (person && signals.housingOpen) {
    return `Contact ${person}, ${role}, to confirm whether additional Midtown / Times Square room-block partners are being accepted for ${clean(opp.title)}.`;
  }
  if (person) {
    return `Contact ${person}, ${role}, to ask when official housing / hotel-block partners will be announced for ${clean(opp.title)}.`;
  }
  if (signals.housingOpen || signals.roomBlockMentioned) {
    return `Contact the event housing team through the official housing channel and confirm Midtown overflow / partner-hotel requirements for ${clean(opp.title)}.`;
  }
  return `Monitor the registration / housing page for ${clean(opp.title)}; re-research when a hotel block or housing portal is published.`;
}

/**
 * Strict ACTIONABLE_NOW — does not lower CQ.
 */
export function evaluateActionableNow(opp = {}, signals = {}, contactTier = null) {
  const fails = [];
  const future =
    opp.eventStartDate && new Date(opp.eventStartDate).getTime() > Date.now() - 86400000;
  if (!future && !opp.eventYear) fails.push("NO_FUTURE_DATE");
  const lodgingNeed =
    signals.roomBlockMentioned ||
    signals.housingPageFound ||
    signals.overflowMentioned ||
    /HOUSING|OVERFLOW|ROOM_BLOCK|SPORTS/i.test(opp.commercialMotion || "");
  if (!lodgingNeed) fails.push("NO_CREDIBLE_LODGING_NEED");
  if (opp.hotelFitScore != null && Number(opp.hotelFitScore) < 40) fails.push("WEAK_HOTEL_FIT");
  if (!opp.commercialMotion) fails.push("NO_COMMERCIAL_MOTION");
  const hasSource =
    opp.officialSource ||
    opp.discoverySource ||
    (opp.sources || []).some((s) => s?.url) ||
    signals.sourceUrl;
  if (!hasSource) fails.push("NO_SOURCE");
  const tier = contactTier || classifyContactTier(opp);
  const usableContact =
    tier === CONTACT_TIER.NAMED_DIRECT ||
    tier === CONTACT_TIER.NAMED_PARTIAL ||
    tier === CONTACT_TIER.FUNCTIONAL_CONTACT ||
    tier === CONTACT_TIER.ORGANIZATION_PATH;
  if (!usableContact) fails.push("NO_CONTACT_PATH");
  const timing =
    signals.housingOpen === true ||
    signals.housingDeadline ||
    (signals.roomBlockMentioned && signals.housingOpen !== false);
  if (!timing) fails.push("NO_TIMING_TRIGGER");
  // Do not ACTIONABLE mega-events without concrete Midtown housing channel
  if (/olympics|la 2028/i.test(opp.title || "") && !signals.housingPageFound) {
    fails.push("MEGA_EVENT_WITHOUT_LOCAL_HOUSING_CHANNEL");
  }
  if (/hotel week/i.test(opp.title || "")) fails.push("PROMO_NOT_GROUP_OPPORTUNITY");

  return {
    ok: fails.length === 0,
    fails,
    actionable: fails.length === 0,
  };
}

/**
 * Apply commercial depth fields onto an opportunity (pure merge).
 */
export function applyCommercialDepthFields(opp, hotelConfig, lodgingResult, contactResult) {
  const signals = lodgingResult.signals || {};
  const motion = classifyLodgingPrimaryMotion(opp, hotelConfig);
  const venue = mapVenueFromLodging(signals, opp);
  const contactOpp = contactResult?.opportunity || opp;
  const primary =
    contactOpp.primaryContact ||
    (contactOpp.contacts || []).find((c) => c?.name) ||
    null;

  let next = {
    ...opp,
    ...contactOpp,
    commercialMotion: motion,
    lodgingPrimaryMotion: motion,
    venueSourcingStatus: venue,
    roomDemandStatus: signals.roomBlockMentioned
      ? ROOM_DEMAND_STATUS.STRONG_ROOM_DEMAND_EVIDENCE
      : signals.housingPageFound
        ? ROOM_DEMAND_STATUS.ESTIMATED_ROOM_DEMAND
        : signals.overflowMentioned
          ? ROOM_DEMAND_STATUS.OVERFLOW_ONLY
          : ROOM_DEMAND_STATUS.HOUSING_PENDING,
    roomDemandResearchState: signals.roomBlockMentioned
      ? EVIDENCE_STATUS.CONFIRMED
      : EVIDENCE_STATUS.UNKNOWN_AFTER_RESEARCH,
    attendance: signals.attendance ?? opp.attendance ?? null,
    attendanceStatus: signals.attendanceStatus || EVIDENCE_STATUS.UNKNOWN_AFTER_RESEARCH,
    peakRooms: signals.peakRooms ?? opp.peakRooms ?? null,
    peakRoomsStatus: signals.peakRoomsStatus || EVIDENCE_STATUS.UNKNOWN_AFTER_RESEARCH,
    lodgingEvidence: {
      housingPageFound: signals.housingPageFound,
      roomBlockMentioned: signals.roomBlockMentioned,
      housingOpen: signals.housingOpen,
      hostHotelMentioned: signals.hostHotelMentioned,
      overflowMentioned: signals.overflowMentioned,
      housingDeadline: signals.housingDeadline,
      sourceUrl: signals.sourceUrl,
      researchedAt: new Date().toISOString(),
      status: signals.roomBlockMentioned
        ? EVIDENCE_STATUS.CONFIRMED
        : EVIDENCE_STATUS.UNKNOWN_AFTER_RESEARCH,
    },
    hotelOpportunityThesis: buildLodgingThesis(opp, hotelConfig, motion, signals),
    summaryWhyHotel: buildLodgingWhyHotel(opp, hotelConfig, motion),
    whyNow: buildLodgingWhyNow(opp, signals),
    recommendedAction: buildLodgingRecommendedAction(opp, primary, signals),
    commercialDepthVersion: COMMERCIAL_DEPTH_V2,
    jevLodgingPlaybook: lodgingResult.playbook || null,
  };

  if (signals.sourceUrl) {
    const sources = Array.isArray(next.sources) ? [...next.sources] : [];
    if (!sources.some((s) => s.url === signals.sourceUrl)) {
      sources.push({
        url: signals.sourceUrl,
        title: "Lodging / housing evidence",
        authority: "official_or_event",
        verified: true,
        retrievedAt: new Date().toISOString(),
      });
    }
    next.sources = sources;
  }

  const gate = evaluateActionableNow(next, signals, classifyContactTier(next));
  if (gate.actionable) {
    next.priority = "HIGH_PRIORITY";
    next.customerFacingState = "ACTIONABLE_NOW";
    next.actionabilityV3 = "TRUE_ACTIONABLE";
    next.opportunityQualification = next.opportunityQualification || "STRONG";
  } else if (motion === LODGING_MOTION.OVERFLOW || signals.overflowMentioned) {
    next.priority = "WATCHLIST";
    next.customerFacingState = "OVERFLOW";
    next.opportunityType = next.opportunityType === "OVERFLOW_HOUSING" ? next.opportunityType : "OVERFLOW_HOUSING";
  } else if (motion === LODGING_MOTION.HOUSING || motion === LODGING_MOTION.ASSOCIATION_HOUSING || motion === LODGING_MOTION.SPORTS_HOUSING) {
    next.priority = "WATCHLIST";
    next.customerFacingState = "HOUSING";
  } else if (motion === LODGING_MOTION.FUTURE_WATCH || !signals.roomBlockMentioned) {
    next.priority = "WATCHLIST";
    next.customerFacingState = "FUTURE_WATCH";
  }

  next.actionableNowGate = gate;

  // Capture lodging-primary copy before customer enrichment (enrich may rewrite whyNow/action)
  const thesisLock = next.hotelOpportunityThesis;
  const whyHotelLock = next.summaryWhyHotel;
  const whyNowLock = next.whyNow;
  const actionLock = next.recommendedAction;
  const motionLock = next.commercialMotion;
  const lodgingLock = next.lodgingEvidence;

  next = enrichGdiOpportunityForCustomer(next, {
    hotelId: next.hotelId || hotelConfig?.hotelId,
  });
  if (shouldHideAfterEnrichment(next)) next.customerVisible = false;

  // Re-assert commercial-depth lodging copy — hotel-specific, evidence-backed
  next.hotelOpportunityThesis = thesisLock;
  next.summaryWhyHotel = whyHotelLock;
  next.whyNow = whyNowLock;
  next.recommendedAction = actionLock;
  next.commercialMotion = motionLock;
  next.lodgingPrimaryMotion = motionLock;
  next.lodgingEvidence = lodgingLock;
  next.commercialDepthVersion = COMMERCIAL_DEPTH_V2;
  next.actionableNowGate = gate;
  if (gate.actionable) {
    next.priority = "HIGH_PRIORITY";
    next.customerFacingState = "ACTIONABLE_NOW";
    next.actionabilityV3 = "TRUE_ACTIONABLE";
  }

  // WHO/HOW queue
  const whoHow = splitWhoHow(next);
  if (whoHow.whoResolvedHowMissing) {
    next.whoResolvedHowMissing = true;
    next.surfeEligible = true;
    next.publicContactCeilingReason =
      next.publicContactCeilingReason || PUBLIC_CONTACT_CEILING_REASON.HOW_NOT_PUBLIC;
  }

  return next;
}

/**
 * Deepen one opportunity: lodging research → contact → commercial fields.
 */
export async function deepenOpportunityCommercialDepthV2(opportunity, opts = {}) {
  const hotelConfig =
    opts.hotelConfig || loadHotelDemandConfig(opportunity.hotelId) || null;
  const lodging = await researchLodgingEvidence(opportunity, {
    allowSerp: opts.allowSerp !== false,
    maxPages: opts.maxPages ?? 4,
  });

  const contact = await resolveContactCompletenessV1(opportunity, {
    force: true,
    enableSafeApply: opts.enableSafeApply !== false,
    skipJev: opts.skipJev === true,
    allowNetworkDomainResolution: true,
    budget: opts.contactBudget || undefined,
  });

  const enriched = applyCommercialDepthFields(
    opportunity,
    hotelConfig,
    lodging,
    contact
  );

  return {
    before: opportunity,
    after: enriched,
    lodging,
    contact,
    motion: enriched.commercialMotion,
    actionableGate: enriched.actionableNowGate,
    contactBefore: classifyContactTier(opportunity),
    contactAfter: classifyContactTier(enriched),
    gradeBefore: gradeContactCompleteness(opportunity).grade,
    gradeAfter: gradeContactCompleteness(enriched).grade,
  };
}
