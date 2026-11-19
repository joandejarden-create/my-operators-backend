/**
 * Commercial Intelligence Brief v2 — narrative composition remediator.
 *
 * Improves sales-usable drawer narratives from existing opportunity evidence.
 * Does not invent demand, contacts, or hotel fit. Reuses the single drawer contract.
 *
 * Honesty rules (founder amendment):
 * - Do not infer competitor placement from generic brand mentions or this hotel's partner listing.
 * - Do not claim credible host merely because venue is unannounced.
 * - Do not append generic demand statements to repair truncated sentences.
 * - Do not turn missing evidence into positive hotel suitability.
 * - Keep unsupported hotel-fit / commercial claims explicitly unverified.
 */

import {
  appearsTruncatedCustomerProse,
  rewritePlaceholderSameMarketFit,
  sanitizeGenericHotelFitNarrative,
} from "./gdi-certified-drawer-commercial-content-contract-v1.js";
import { scoreCommercialIntelligenceBriefV2 } from "./gdi-commercial-intelligence-brief-quality-score-v1.js";

export const GDI_COMMERCIAL_INTELLIGENCE_BRIEF_V2_COMPOSER =
  "gdi-commercial-intelligence-brief-v2-composer-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function firstUrl(opp = {}) {
  if (typeof opp.officialSource === "string" && /^https?:\/\//i.test(opp.officialSource)) {
    return opp.officialSource;
  }
  for (const s of opp.sources || []) {
    const u = typeof s === "string" ? s : s?.url;
    if (u && /^https?:\/\//i.test(u)) return u;
  }
  return null;
}

function contactLine(opp = {}) {
  const c = opp.primaryContact || {};
  const name = text(c.name);
  if (!name) return null;
  const role = text(c.title || c.role);
  const email = text(c.email);
  const route = text(c.organizationContactRoute);
  const bits = [name];
  if (role) bits.push(`(${role})`);
  if (email) bits.push(email);
  else if (route) bits.push(`via ${route}`);
  return bits.join(" ");
}

function isPast(opp, now) {
  const d = opp.eventStartDate ? new Date(opp.eventStartDate) : null;
  if (!d || Number.isNaN(d.getTime())) return false;
  return d.getTime() < now.getTime() - 24 * 60 * 60 * 1000;
}

function stripMethodology(s) {
  return text(s)
    .replace(/\s*Research verification date\s*\([^)]*\)\s*is not the activity date\.?/gi, "")
    .replace(/\s*Original research body is missing from recoverable artifacts\.?/gi, "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Incomplete prose: reconstruct from preserved source fields when possible,
 * otherwise close or drop an incomplete tail — never invent demand filler.
 */
function repairIncompleteProse(s, reconstructionCandidates = []) {
  let t = text(s);
  if (!t) return { text: t, changed: false, mode: null };

  const core = t.replace(/[.…]+\s*$/, "").slice(0, 48).toLowerCase();
  for (const candidate of reconstructionCandidates) {
    const c = text(candidate);
    if (!c || c.length < 40) continue;
    if (c === t) continue;
    const cLow = c.toLowerCase();
    if (cLow.startsWith(core) || (core.length >= 24 && cLow.includes(core.slice(0, 24)))) {
      const ended = /[.!?]"?\s*$/.test(c) ? c : `${c}.`;
      return { text: ended, changed: ended !== t, mode: "RECONSTRUCTED_FROM_SOURCE" };
    }
  }

  // Close known trailing abbreviations (e.g. "Washington D.C") without inventing demand.
  if (/\bD\.C\.?\s*$/i.test(t) || /\b[A-Z]\.[A-Z]\.?\s*$/.test(t)) {
    if (!/[.!?]"?\s*$/.test(t)) {
      return { text: `${t}.`, changed: true, mode: "CLOSED_SENTENCE_ONLY" };
    }
  }

  if (appearsTruncatedCustomerProse(t) || /[a-z0-9,;:]\s*$/i.test(t)) {
    // Prefer closing over destructive truncation when the clause already carries a commercial fact.
    if (/\b(opened|announced|publishes|committed|hub|partner|overflow)\b/i.test(t) && t.length >= 80) {
      if (!/[.!?]"?\s*$/.test(t)) {
        return { text: `${t}.`, changed: true, mode: "CLOSED_SENTENCE_ONLY" };
      }
      return { text: t, changed: false, mode: null };
    }
    const cut = t.replace(/\s+[—–-]\s+[^.!?]{0,80}$/u, "").replace(/\s+\S+$/, "");
    const cleaned = text(cut);
    if (cleaned.length >= 40 && cleaned !== t) {
      const ended = /[.!?]"?\s*$/.test(cleaned) ? cleaned : `${cleaned}.`;
      return { text: ended, changed: true, mode: "STRIPPED_INCOMPLETE_TAIL" };
    }
    if (!/[.!?]"?\s*$/.test(t)) {
      return { text: `${t}.`, changed: true, mode: "CLOSED_SENTENCE_ONLY" };
    }
  }
  return { text: t, changed: false, mode: null };
}

function thisHotelListedPartner(opp = {}) {
  const hotel = text(opp.hotelName);
  const blob = [opp.whatIsHappening, opp.whyThisHotel, opp.lodgingEvidence, opp.summaryWhat]
    .map(text)
    .join(" ");
  if (/official hotel partner|publishes .+ as an official|partner-rate|participant\/partner/i.test(blob)) {
    if (!hotel) return true;
    const hotelToken = hotel.split(/\s+/).slice(0, 2).join(" ");
    if (new RegExp(hotelToken.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(blob)) {
      return true;
    }
    return /official hotel partner|participant\/partner listing already exists/i.test(blob);
  }
  return false;
}

/**
 * Competitor placement only when other hotels are named hosts AND this hotel
 * is not itself a published partner/host. Generic brand mentions alone are insufficient.
 */
function competitorPlacedOtherHotels(opp = {}) {
  if (thisHotelListedPartner(opp)) return false;

  const competitors = Array.isArray(opp.competitorHotels)
    ? opp.competitorHotels.map((h) => text(typeof h === "string" ? h : h?.name)).filter(Boolean)
    : [];
  if (competitors.length > 0) return true;

  const placementBlob = [opp.whatIsHappening, opp.whyActNow, opp.lodgingEvidence, opp.potentialHotelBusiness]
    .map(text)
    .join(" ");
  const namedElsewhere =
    /committed at\s+[^.]{8,120}|principal housing is not available|room blocks available at\s+[^.]{8,100}|Primary sleeping rooms appear committed elsewhere/i.test(
      placementBlob,
    );
  const namedBrandHosts =
    /(Hotel X|Sheraton Centre|Hyatt Regency|Le M[eé]ridien|Westin Harbour)/i.test(placementBlob) &&
    /(committed|room block|official hotel|housing is not available|not the contracted host)/i.test(
      placementBlob,
    );
  return namedElsewhere || namedBrandHosts;
}

function venueUnannounced(opp = {}) {
  const blob = [opp.whatIsHappening, opp.whyActNow, opp.title, opp.summaryWhat].map(text).join(" ");
  return /location TBA|location to be announced|venue.*to be announced|host city remains TBD/i.test(
    blob,
  );
}

function venueAlreadySelected(opp = {}) {
  const blob = [
    opp.whatIsHappening,
    opp.summaryWhat,
    opp.venueSourcingStatus,
    opp.buyingStatus,
  ]
    .map(text)
    .join(" ");
  return /PRIMARY_VENUE_SELECTED|hybrid at .+ HQ|contracted host venue|venue selected|not the contracted host/i.test(
    blob,
  );
}

function isScalingCorporateAccount(opp = {}) {
  return (
    /scaling corporate|regional hub|local expansion|growth activity/i.test(
      [opp.title, opp.whatIsHappening, opp.segment].map(text).join(" "),
    ) && !opp.eventEndDate
  );
}

/**
 * Compose Brief v2 fields onto an opportunity without mutating unrelated keys.
 */
export function composeCommercialIntelligenceBriefV2(raw = {}, opts = {}) {
  const now = opts.now || new Date();
  const opp = { ...raw };
  const hotel = text(opp.hotelName) || "this hotel";
  const org = text(opp.organizationName) || "this organization";
  const title = text(opp.title) || org;
  const url = firstUrl(opp);
  const contact = contactLine(opp);
  const past = isPast(opp, now);
  const listedPartner = thisHotelListedPartner(opp);
  const competitorElsewhere = competitorPlacedOtherHotels(opp);
  const openVenue = venueUnannounced(opp) && !venueAlreadySelected(opp) && !past;
  const scaling = isScalingCorporateAccount(opp);
  const changes = [];

  let what = stripMethodology(
    text(
      opp.whatIsHappening ||
        opp.commercialIntelligenceBrief?.whatIsHappening ||
        opp.salesBrief?.whatIsHappening ||
        opp.summaryWhat,
    ),
  );
  let whyHotel = text(
    opp.whyThisHotel ||
      opp.commercialIntelligenceBrief?.whyThisHotel ||
      opp.salesBrief?.whyThisHotel,
  );
  let business = stripMethodology(
    text(
      opp.potentialHotelBusiness ||
        opp.commercialIntelligenceBrief?.potentialHotelBusiness ||
        opp.salesBrief?.potentialHotelBusiness ||
        opp.potentialBusiness,
    ),
  );
  let whyNow = text(
    opp.whyActNow ||
      opp.commercialIntelligenceBrief?.whyActNow ||
      opp.salesBrief?.whyActNow ||
      opp.whyNow,
  );
  let next = text(
    opp.recommendedNextStep ||
      opp.commercialIntelligenceBrief?.recommendedNextStep ||
      opp.salesBrief?.recommendedNextStep ||
      opp.recommendedAction,
  );
  let unknowns = Array.isArray(opp.uncertainties)
    ? [...opp.uncertainties]
    : opp.uncertainties
      ? [String(opp.uncertainties)]
      : [];

  // --- Demand mechanism / context ---
  if (!what) {
    what = `${title}${opp.eventStartDate ? ` (${opp.eventStartDate}` : ""}${
      opp.eventEndDate ? `–${opp.eventEndDate}` : ""
    }${opp.eventStartDate ? ")" : ""} is a dated demand opportunity for ${org}${
      opp.eventLocation ? ` linked to ${opp.eventLocation}` : ""
    }.`;
    changes.push("COMPOSED_WHAT_FROM_IDENTITY");
  } else {
    const cleaned = stripMethodology(what);
    if (cleaned !== what) {
      what = cleaned;
      changes.push("STRIPPED_METHODOLOGY_WHAT");
    }
    const repaired = repairIncompleteProse(what, [
      opp.commercialIntelligenceBrief?.companyAndLocalActivity,
      opp.commercialIntelligenceBrief?.opportunityOverview,
      opp.salesBrief?.whatIsHappening,
      opp.companyAndLocalActivity,
    ]);
    if (repaired.changed) {
      what = repaired.text;
      changes.push(repaired.mode || "REPAIRED_INCOMPLETE_WHAT");
    }
  }

  // --- Hotel relevance (honest; no positive suitability from missing evidence) ---
  if (!whyHotel) {
    if (past) {
      whyHotel = text(opp.whatIsHappening)
        ? `${hotel} was evaluated against this past cycle. Any lodging adjacency in prior research is historical only, not a current host or overflow commitment.`
        : `${hotel} has no verified current host or overflow role for this past ${org} cycle.`;
      changes.push("COMPOSED_PAST_CYCLE_HOTEL_FIT");
    } else if (competitorElsewhere) {
      whyHotel = `${hotel} is not the published primary host. Credible fit is limited to overflow / secondary lodging or meeting support for ${org} delegates when housing demand exceeds contracted hotels. Volume is not yet evidenced.`;
      changes.push("COMPOSED_OVERFLOW_HOTEL_FIT");
    } else if (openVenue) {
      whyHotel = `${hotel} inventory${
        opp.rooms ? ` (${opp.rooms} guestrooms)` : ""
      }${opp.meetingSqFt ? ` and ~${opp.meetingSqFt.toLocaleString?.() || opp.meetingSqFt} sq ft meeting space` : ""} is unverified as a host until the ${org} venue/RFP path is confirmed. Do not treat an unannounced venue as hotel suitability.`;
      changes.push("COMPOSED_UNVERIFIED_OPEN_VENUE_HOTEL_FIT");
    } else if (scaling) {
      whyHotel = `${hotel} sits in the same commercial market as documented ${org} activity. No confirmed room block, RFP, or preferred-hotel agreement is established from public research alone.`;
      changes.push("COMPOSED_SCALING_CAUTIOUS_HOTEL_FIT");
    } else {
      whyHotel = `${hotel} may support overnight or meeting demand tied to ${org} only when lodging volume and buying path are verified. No room block or preferred-hotel agreement is established from public research alone.`;
      changes.push("COMPOSED_CAUTIOUS_HOTEL_FIT");
    }
  } else {
    const rw = rewritePlaceholderSameMarketFit(opp, whyHotel);
    if (rw.rewritten) {
      whyHotel = rw.text;
      changes.push("REWROTE_PLACEHOLDER_MARKET_FIT");
    }
    const gen = sanitizeGenericHotelFitNarrative(opp, whyHotel);
    if (gen.rewritten) {
      whyHotel = gen.text;
      changes.push("SANITIZED_GENERIC_HOTEL_FIT");
    }
    if (/credible host candidate while .+ venue remains open/i.test(whyHotel)) {
      whyHotel = `${hotel} inventory is unverified as a host until venue/RFP status for ${org} is confirmed. An unannounced venue does not establish hotel suitability.`;
      changes.push("REMOVED_UNSUPPORTED_CREDIBLE_HOST_CLAIM");
    }
  }

  // --- Potential business ---
  if (!business) {
    if (listedPartner) {
      business = `Manage remaining partner-rate inventory and qualify incremental rooms, suites, or meeting needs. Do not treat the listing as exclusive venue housing.`;
    } else if (competitorElsewhere) {
      business = `Primary sleeping rooms appear committed elsewhere. Possible capture for ${hotel}: overflow nights, late-booking delegates, speakers/exhibitors, or day-meeting/catering, only if housing or meeting shortfalls are confirmed.`;
    } else if (past) {
      business = `The dated cycle appears past. Do not treat current research as an open booking window; archive learnings and watch for the next published cycle.`;
    } else if (scaling) {
      business = `Potential transient lodging or meetings for ${org} visiting staff and collaborators near the local site remain inferred only. Travel volume and buyer authority are unverified.`;
    } else {
      business = `Potential hotel business depends on verified peak rooms, meeting agenda, and buyer authority for ${org}. Public research has not confirmed a contracted room block at ${hotel}.`;
    }
    changes.push("COMPOSED_POTENTIAL_BUSINESS");
  } else {
    const repaired = repairIncompleteProse(business, [
      opp.commercialIntelligenceBrief?.potentialHotelBusiness,
      opp.commercialIntelligenceBrief?.opportunityOverview,
      opp.salesBrief?.potentialHotelBusiness,
    ]);
    if (repaired.changed) {
      business = repaired.text;
      changes.push(repaired.mode || "REPAIRED_INCOMPLETE_BUSINESS");
    }
    business = stripMethodology(business);
  }

  // --- Commercial posture ---
  let commercialPosture = "OPEN_TO_QUALIFY";
  if (past) commercialPosture = "PAST_CYCLE_DO_NOT_PURSUE";
  else if (listedPartner) commercialPosture = "LISTED_PARTNER_QUALIFY";
  else if (competitorElsewhere) commercialPosture = "COMPETITOR_PLACED_OVERFLOW_ONLY";
  else if (openVenue) commercialPosture = "FUTURE_VENUE_OPEN";
  else if (scaling) commercialPosture = "SCALING_ACCOUNT_QUALIFY";

  if (!whyNow) {
    if (past) {
      whyNow =
        "Event dates are past. Do not open a fresh pursuit on this cycle. Capture lessons and monitor the next announced edition.";
    } else if (commercialPosture === "LISTED_PARTNER_QUALIFY") {
      whyNow =
        "Partner listing and promo cutoff create a finite window to protect remaining inventory and qualify incremental needs.";
    } else if (commercialPosture === "FUTURE_VENUE_OPEN") {
      whyNow =
        "Venue remains unannounced publicly; introduce the hotel only as an unverified candidate while sourcing decisions can still change.";
    } else if (commercialPosture === "COMPETITOR_PLACED_OVERFLOW_ONLY") {
      whyNow =
        "Primary hotels are already named. Only act if overflow or secondary housing need is confirmed.";
    } else if (scaling) {
      whyNow =
        "Act only after confirming whether visiting-staff or meeting travel patterns exist; expansion news alone is not lodging demand.";
    } else {
      whyNow = `Act while ${org} lodging or meeting decisions for this cycle can still be influenced.`;
    }
    changes.push("COMPOSED_WHY_ACT_NOW");
  } else if (past && /pursue|contact now|lodging demand path open/i.test(whyNow)) {
    whyNow =
      "Published dates are past. Close active pursuit on this cycle; retain contact path for the next edition only.";
    changes.push("CORRECTED_PAST_WHY_ACT_NOW");
  }

  // --- Pursuit ---
  // Prefer a verified named contact over vague “housing representative” boilerplate.
  if (
    next &&
    contact &&
    /relevant housing representative|the housing contact|housing representative/i.test(next) &&
    !new RegExp(text(opp.primaryContact?.name).replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(
      next,
    )
  ) {
    next = next.replace(
      /the relevant housing representative|the housing contact|housing representative/i,
      contact,
    );
    changes.push("REPLACED_VAGUE_CONTACT_BOILERPLATE");
  }

  if (past) {
    next = contact
      ? `Do not pursue this past cycle. Optionally note ${contact} for the next published edition; no current RFP action.`
      : `Do not pursue this past cycle. Watch for the next published ${org} edition before any outreach.`;
    changes.push("PAST_CYCLE_NEXT_STEP");
  } else if (!next || appearsTruncatedCustomerProse(next) || /^Contact .+\. ask /i.test(next)) {
    if (commercialPosture === "LISTED_PARTNER_QUALIFY") {
      next = contact
        ? `Account-manage the existing listing with ${contact}: review pickup, release dates, remaining inventory, and any incremental rooms or meeting needs.`
        : `Account-manage the existing partner listing: review pickup, release dates, remaining inventory, and incremental needs${
            url ? ` (start from ${url})` : ""
          }.`;
    } else if (commercialPosture === "COMPETITOR_PLACED_OVERFLOW_ONLY") {
      next = contact
        ? `Ask ${contact} whether overflow or secondary room blocks are needed beyond the named host hotels; do not pitch as primary host without evidence.`
        : `Confirm with the housing contact whether overflow demand exists beyond named host hotels before pitching ${hotel}.`;
    } else if (scaling) {
      next = contact
        ? `Contact ${contact} to ask who coordinates visiting-staff lodging and local meetings for the Bethesda / DMV expansion. Do not pitch a conference RFP without evidenced group demand.`
        : `Identify who coordinates travel or workplace meetings for ${org}'s local expansion before proposing hotel rates or a speculative RFP.`;
    } else if (contact) {
      next = `Contact ${contact} to confirm venue/housing status, introduce ${hotel}, and request RFP or site-visit inclusion only if sourcing is still open. Qualify peak rooms, meeting needs, and decision timing before proposing rates.`;
    } else {
      next = `Identify a verified meetings or travel decision-maker at ${org}, then introduce ${hotel} only after lodging demand is confirmed${
        url ? ` (start from ${url})` : ""
      }.`;
    }
    changes.push("COMPOSED_RECOMMENDED_NEXT_STEP");
  }

  // --- Unknowns ---
  const defaultUnknowns = [];
  if (!opp.peakRooms || opp.peakRooms === "UNKNOWN") {
    defaultUnknowns.push("Peak room demand is not evidenced.");
  }
  if (commercialPosture === "FUTURE_VENUE_OPEN") {
    defaultUnknowns.push("Host city / venue remains unannounced.");
    defaultUnknowns.push("Hotel suitability as host is unverified.");
  }
  if (commercialPosture === "COMPETITOR_PLACED_OVERFLOW_ONLY") {
    defaultUnknowns.push("Overflow volume and housing agency path are unverified.");
  }
  if (commercialPosture === "LISTED_PARTNER_QUALIFY") {
    defaultUnknowns.push("Partner-rate pickup and remaining allotment by night are unverified.");
  }
  if (scaling) {
    defaultUnknowns.push("Travel volume and lodging buyer authority for the expansion are unverified.");
  }
  if (!text(opp.primaryContact?.email) && !text(opp.primaryContact?.organizationContactRoute)) {
    defaultUnknowns.push("Direct buyer email is not verified.");
  }
  for (const u of defaultUnknowns) {
    if (!unknowns.some((x) => text(x).toLowerCase() === u.toLowerCase())) {
      unknowns.push(u);
      changes.push("ADDED_EXPLICIT_UNKNOWN");
    }
  }

  const brief = {
    whatIsHappening: what,
    whyThisHotel: whyHotel,
    potentialHotelBusiness: business,
    whyActNow: whyNow,
    recommendedNextStep: next,
    commercialPosture,
    uncertainties: unknowns,
    briefVersion: GDI_COMMERCIAL_INTELLIGENCE_BRIEF_V2_COMPOSER,
  };

  const nextOpp = {
    ...opp,
    whatIsHappening: what,
    whyThisHotel: whyHotel,
    potentialHotelBusiness: business,
    whyActNow: whyNow,
    recommendedNextStep: next,
    uncertainties: unknowns,
    commercialIntelligenceBrief: {
      ...(opp.commercialIntelligenceBrief || {}),
      ...brief,
    },
    salesBrief: {
      ...(opp.salesBrief || {}),
      whatIsHappening: what,
      whyThisHotel: whyHotel,
      potentialHotelBusiness: business,
      whyActNow: whyNow,
      recommendedNextStep: next,
    },
    briefV2: {
      commercialPosture,
      compositionChanges: [...new Set(changes)],
    },
  };

  return {
    version: GDI_COMMERCIAL_INTELLIGENCE_BRIEF_V2_COMPOSER,
    opportunity: nextOpp,
    brief,
    changes: [...new Set(changes)],
    score: scoreCommercialIntelligenceBriefV2(nextOpp, { now }),
  };
}

/**
 * Apply Brief v2 composition inside the universal presentation path.
 */
export function applyCommercialIntelligenceBriefV2(opportunity = {}, opts = {}) {
  if (opportunity?.briefV2?.skip === true) return opportunity;
  if (opportunity?._briefV2Applied === true) return opportunity;
  const result = composeCommercialIntelligenceBriefV2(opportunity, opts);
  return {
    ...result.opportunity,
    _briefV2Applied: true,
    _briefV2Score: result.score,
    _briefV2Changes: result.changes,
  };
}

export {
  competitorPlacedOtherHotels,
  thisHotelListedPartner,
  repairIncompleteProse,
};

export default {
  GDI_COMMERCIAL_INTELLIGENCE_BRIEF_V2_COMPOSER,
  composeCommercialIntelligenceBriefV2,
  applyCommercialIntelligenceBriefV2,
  competitorPlacedOtherHotels,
  thisHotelListedPartner,
  repairIncompleteProse,
};
