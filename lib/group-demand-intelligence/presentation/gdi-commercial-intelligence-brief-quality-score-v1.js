/**
 * Commercial Intelligence Brief v2 — evidence-grounded quality scoring.
 * Dimensions (1–5): CC commercial clarity, HR hotel relevance, BI buyer intelligence,
 * CP commercial posture, PU pursuit usefulness, EV evidence grounding.
 */

import {
  appearsTruncatedCustomerProse,
  detectGenericCommercialProse,
} from "./gdi-certified-drawer-commercial-content-contract-v1.js";

export const GDI_COMMERCIAL_INTELLIGENCE_BRIEF_QUALITY_SCORE_V1 =
  "gdi-commercial-intelligence-brief-quality-score-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function firstSourceUrl(opp = {}) {
  if (typeof opp.officialSource === "string" && /^https?:\/\//i.test(opp.officialSource)) {
    return opp.officialSource;
  }
  for (const s of opp.sources || []) {
    const u = typeof s === "string" ? s : s?.url;
    if (u && /^https?:\/\//i.test(u)) return u;
  }
  return null;
}

function hasNamedContact(opp = {}) {
  const c = opp.primaryContact || {};
  return Boolean(text(c.name) && (text(c.email) || text(c.title) || text(c.role)));
}

function isPastEvent(opp = {}, now = new Date()) {
  const d = opp.eventStartDate ? new Date(opp.eventStartDate) : null;
  if (!d || Number.isNaN(d.getTime())) return false;
  return d.getTime() < now.getTime() - 24 * 60 * 60 * 1000;
}

function methodologyLeak(s) {
  return /research verification date|original research body|recoverable artifacts|exa agent|ORS snapshot|synthetic_history|fixture only/i.test(
    s,
  );
}

function scoreDimension(n) {
  return Math.max(1, Math.min(5, Math.round(n)));
}

/**
 * Score a brief-shaped opportunity (top-level or CIB fields).
 */
export function scoreCommercialIntelligenceBriefV2(opp = {}, opts = {}) {
  const now = opts.now || new Date();
  const what = text(
    opp.whatIsHappening ||
      opp.commercialIntelligenceBrief?.whatIsHappening ||
      opp.salesBrief?.whatIsHappening,
  );
  const whyHotel = text(
    opp.whyThisHotel ||
      opp.commercialIntelligenceBrief?.whyThisHotel ||
      opp.salesBrief?.whyThisHotel,
  );
  const business = text(
    opp.potentialHotelBusiness ||
      opp.commercialIntelligenceBrief?.potentialHotelBusiness ||
      opp.salesBrief?.potentialHotelBusiness,
  );
  const whyNow = text(
    opp.whyActNow ||
      opp.commercialIntelligenceBrief?.whyActNow ||
      opp.salesBrief?.whyActNow,
  );
  const next = text(
    opp.recommendedNextStep ||
      opp.commercialIntelligenceBrief?.recommendedNextStep ||
      opp.salesBrief?.recommendedNextStep,
  );
  const unknowns = text(
    Array.isArray(opp.uncertainties)
      ? opp.uncertainties.join(" ")
      : opp.uncertainties || "",
  );
  const posture = text(
    opp.commercialPosture ||
      opp.buyingStatus ||
      opp.disposition ||
      opp.briefV2?.commercialPosture ||
      "",
  );

  const defects = [];
  const blob = [what, whyHotel, business, whyNow, next].join(" ");

  if (!what) defects.push("MISSING_WHAT_IS_HAPPENING");
  if (!whyHotel) defects.push("MISSING_WHY_THIS_HOTEL");
  if (!business) defects.push("MISSING_POTENTIAL_BUSINESS");
  if (!next) defects.push("MISSING_RECOMMENDED_NEXT_STEP");
  if (appearsTruncatedCustomerProse(what)) defects.push("TRUNCATED_WHAT");
  if (appearsTruncatedCustomerProse(business)) defects.push("TRUNCATED_BUSINESS");
  if (appearsTruncatedCustomerProse(next)) defects.push("TRUNCATED_NEXT_STEP");
  if (detectGenericCommercialProse(whyHotel).generic) {
    defects.push("GENERIC_HOTEL_FIT");
  }
  if (methodologyLeak(blob)) defects.push("METHODOLOGY_LEAKAGE");
  if (
    isPastEvent(opp, now) &&
    !/do not pursue|past cycle|no current rfp/i.test(next) &&
    /pursue|contact now|qualify before pursuing/i.test(next)
  ) {
    defects.push("PAST_EVENT_ACTIVE_PURSUE");
  }
  if (!firstSourceUrl(opp)) defects.push("NO_EVIDENCE_URL");
  if (!hasNamedContact(opp)) defects.push("WEAK_BUYER_CONTACT");
  if (!unknowns && /plausible|inferred|may|might/i.test(business)) {
    defects.push("SPECULATION_WITHOUT_UNKNOWNS");
  }

  // Dimension scores
  let cc = 3;
  if (!what) cc = 1;
  else if (defects.includes("TRUNCATED_WHAT") || defects.includes("METHODOLOGY_LEAKAGE")) cc = 2;
  else if (what.length > 160 && /because|requiring|publishes|announced/i.test(what)) cc = 5;
  else if (what.length > 100) cc = 4;

  let hr = 3;
  if (!whyHotel) hr = 1;
  else if (defects.includes("GENERIC_HOTEL_FIT")) hr = 2;
  else if (/overflow|official hotel|partner|meeting|guestroom|block/i.test(whyHotel)) hr = 5;
  else if (whyHotel.length > 80) hr = 4;

  let bi = 3;
  if (!hasNamedContact(opp)) bi = 2;
  else if (
    /DIRECT_DECISION|EVENT_MEETINGS_OWNER|Grade A|verified/i.test(
      JSON.stringify(opp.primaryContact || {}),
    )
  ) {
    bi = 5;
  } else bi = 4;
  if (!text(opp.primaryContact?.email) && !text(opp.primaryContact?.organizationContactRoute)) {
    bi = Math.min(bi, 3);
  }

  let cp = 3;
  if (isPastEvent(opp, now) && defects.includes("PAST_EVENT_ACTIVE_PURSUE")) cp = 1;
  else if (/competitor|already|Hotel X|Sheraton|Hyatt|official hotel partner/i.test(what + whyHotel)) {
    cp = /overflow|supplementary|secondary/i.test(whyHotel + next) ? 4 : 2;
  } else if (/location TBA|venue.*open|still open/i.test(whyNow + what)) cp = 5;
  else if (posture) cp = 4;

  let pu = 3;
  if (!next) pu = 1;
  else if (defects.includes("TRUNCATED_NEXT_STEP")) pu = 2;
  else if (/contact .+@.+\.|request RFP|qualify|overflow/i.test(next) && text(opp.primaryContact?.name)) {
    pu = 5;
  } else if (next.length > 80) pu = 4;

  let ev = 3;
  if (!firstSourceUrl(opp)) ev = 1;
  else if ((opp.sources || []).length >= 2 && what) ev = 5;
  else if (firstSourceUrl(opp)) ev = 4;
  if (defects.includes("SPECULATION_WITHOUT_UNKNOWNS")) ev = Math.min(ev, 3);

  const dimensions = {
    CC: scoreDimension(cc),
    HR: scoreDimension(hr),
    BI: scoreDimension(bi),
    CP: scoreDimension(cp),
    PU: scoreDimension(pu),
    EV: scoreDimension(ev),
  };
  const total =
    dimensions.CC +
    dimensions.HR +
    dimensions.BI +
    dimensions.CP +
    dimensions.PU +
    dimensions.EV;

  return {
    version: GDI_COMMERCIAL_INTELLIGENCE_BRIEF_QUALITY_SCORE_V1,
    dimensions,
    total,
    maxTotal: 30,
    defects,
    pastEvent: isPastEvent(opp, now),
    hasEvidenceUrl: Boolean(firstSourceUrl(opp)),
    hasNamedContact: hasNamedContact(opp),
  };
}

export default {
  GDI_COMMERCIAL_INTELLIGENCE_BRIEF_QUALITY_SCORE_V1,
  scoreCommercialIntelligenceBriefV2,
};
