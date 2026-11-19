/**
 * Commercial Intelligence Brief v2 — narrative composition remediator.
 *
 * Improves sales-usable drawer narratives from existing opportunity evidence.
 * Does not invent demand, contacts, or hotel fit. Reuses the single drawer contract.
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

function completeTruncatedSentence(s, fallbackEnd) {
  let t = text(s);
  if (!t) return t;
  if (appearsTruncatedCustomerProse(t) || /[a-z0-9,;:]\s*$/i.test(t)) {
    if (!/[.!?]"?\s*$/.test(t)) t = `${t}${fallbackEnd}`;
  }
  return t;
}

function competitorPlaced(opp = {}) {
  const blob = [
    opp.whatIsHappening,
    opp.title,
    JSON.stringify(opp.competitorHotels || []),
  ].join(" ");
  return /Hotel X|Sheraton|Hyatt|official hotel partner|committed at/i.test(blob);
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
    const completed = completeTruncatedSentence(
      what,
      " market with visiting-staff and meeting demand still to be qualified.",
    );
    if (completed !== what) {
      what = completed;
      changes.push("COMPLETED_TRUNCATED_WHAT");
    }
  }

  // --- Hotel relevance ---
  if (!whyHotel) {
    if (competitorPlaced(opp)) {
      whyHotel = `${hotel} is not the published primary host. Credible fit is limited to overflow / secondary lodging or meeting support for ${org} delegates when housing demand exceeds contracted hotels — volume is not yet evidenced.`;
      changes.push("COMPOSED_OVERFLOW_HOTEL_FIT");
    } else if (/location TBA|to be announced|venue.*open/i.test(what + whyNow + title)) {
      whyHotel = `${hotel} is a credible host candidate while the ${org} venue remains open: full-service inventory${
        opp.rooms ? ` (${opp.rooms} guestrooms)` : ""
      }${opp.meetingSqFt ? ` and ~${opp.meetingSqFt.toLocaleString?.() || opp.meetingSqFt} sq ft meeting space` : ""} can support mid-size conference room blocks and breakouts if selected. Fit is provisional until venue/RFP status is confirmed.`;
      changes.push("COMPOSED_OPEN_VENUE_HOTEL_FIT");
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
  }

  // --- Potential business ---
  if (!business) {
    if (competitorPlaced(opp)) {
      business = `Primary sleeping rooms appear committed elsewhere. Possible capture for ${hotel}: overflow nights, late-booking delegates, speakers/exhibitors, or day-meeting/catering — only if housing or meeting shortfalls are confirmed.`;
    } else if (past) {
      business = `The dated cycle appears past. Do not treat current research as an open booking window; archive learnings and watch for the next published cycle.`;
    } else {
      business = `Potential hotel business depends on verified peak rooms, meeting agenda, and buyer authority for ${org}. Public research has not confirmed a contracted room block at ${hotel}.`;
    }
    changes.push("COMPOSED_POTENTIAL_BUSINESS");
  } else {
    const completed = completeTruncatedSentence(
      business,
      " near the local site when travel is confirmed.",
    );
    if (completed !== business) {
      business = completed;
      changes.push("COMPLETED_TRUNCATED_BUSINESS");
    }
    business = stripMethodology(business);
  }

  // --- Commercial posture ---
  let commercialPosture = "OPEN_TO_QUALIFY";
  if (past) commercialPosture = "PAST_CYCLE_DO_NOT_PURSUE";
  else if (competitorPlaced(opp)) commercialPosture = "COMPETITOR_PLACED_OVERFLOW_ONLY";
  else if (/location TBA|to be announced/i.test(what + whyNow + title)) {
    commercialPosture = "FUTURE_VENUE_OPEN";
  } else if (/official hotel partner|publishes .+ as an official/i.test(what)) {
    commercialPosture = "LISTED_PARTNER_QUALIFY";
  }

  if (!whyNow) {
    if (past) {
      whyNow =
        "Event dates are past — do not open a fresh pursuit on this cycle. Capture lessons and monitor the next announced edition.";
    } else if (commercialPosture === "FUTURE_VENUE_OPEN") {
      whyNow =
        "Venue remains open publicly; introduce the hotel while sourcing decisions can still change.";
    } else if (commercialPosture === "COMPETITOR_PLACED_OVERFLOW_ONLY") {
      whyNow =
        "Primary hotels are already named — only act if overflow or secondary housing need is confirmed.";
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
  if (past) {
    next = contact
      ? `Do not pursue this past cycle. Optionally note ${contact} for the next published edition; no current RFP action.`
      : `Do not pursue this past cycle. Watch for the next published ${org} edition before any outreach.`;
    changes.push("PAST_CYCLE_NEXT_STEP");
  } else if (!next || appearsTruncatedCustomerProse(next) || /^Contact .+\. ask /i.test(next)) {
    if (commercialPosture === "COMPETITOR_PLACED_OVERFLOW_ONLY") {
      next = contact
        ? `Ask ${contact} whether overflow or secondary room blocks are needed beyond the named host hotels; do not pitch as primary host without evidence.`
        : `Confirm with the housing contact whether overflow demand exists beyond named host hotels before pitching ${hotel}.`;
    } else if (contact) {
      next = `Contact ${contact} to confirm venue/housing status, introduce ${hotel}, and request RFP or site-visit inclusion. Qualify peak rooms, meeting needs, and decision timing before proposing rates.`;
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
  }
  if (commercialPosture === "COMPETITOR_PLACED_OVERFLOW_ONLY") {
    defaultUnknowns.push("Overflow volume and housing agency path are unverified.");
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

export default {
  GDI_COMMERCIAL_INTELLIGENCE_BRIEF_V2_COMPOSER,
  composeCommercialIntelligenceBriefV2,
  applyCommercialIntelligenceBriefV2,
};
