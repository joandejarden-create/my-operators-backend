/**
 * Category inference for Market Alerts.
 * Supply/openings checked before brand-name mentions in headlines.
 */

const SUPPLY_RE =
  /\b(pipeline|construction|groundbreaking|development|rooms in construction|new hotel|new resorts?|hotel opening|opens\b|opening of|signing of|officially open|grand opening|soft open|debut\b|unveils?\b|launch(?:es|ed)?\s+(?:its?\s+)?(?:new\s+)?(?:hotel|resort|property)|signs?\s+(?:for\s+)?(?:a\s+)?new\s+hotel|breaks ground|ground was broken)\b/i;

const DEALS_RE =
  /\b(acquisition|acquires|acquired|merger|merged|sold|sale of|deal\b|transaction|portfolio sale|disposition|divests)\b/i;

const CAPITAL_RE =
  /\b(funding|financ|investment|investor|lender|loan|reit|bond|capital raise|refinanc|private equity|debt facility)\b/i;

const BRAND_RE =
  /\b(franchise|franchised|rebrand|rebrands|brand launch|new brand|soft brand|lifestyle brand|collection debut|joins portfolio|adds to portfolio|brand expansion|flagging|flagged as|conversion to)\b/i;

const DEMAND_RE =
  /\b(revpar|occupancy|adr|demand|booking trends?|traveler|traveller|performance|arrivals|guest satisfaction|market performance)\b/i;

const LOYALTY_RE =
  /\b(loyalty program|loyalty|rewards program|bonvoy|hilton honors|world of hyatt|frequent guest|points program)\b/i;

const RISK_RE =
  /\b(lawsuit|regulat|strike|downturn|bankrupt|sanction|risk\b|recall|investigation|fraud)\b/i;

export function inferCategoryFromText(text, source = "") {
  const src = (source || "").trim();
  if (/\bopenings?\b/i.test(src)) return "Supply";

  const t = (text || "").trim();
  if (!t) return "Demand";

  if (SUPPLY_RE.test(t)) return "Supply";
  if (DEALS_RE.test(t)) return "Deals";
  if (CAPITAL_RE.test(t)) return "Capital";
  if (BRAND_RE.test(t)) return "Brand";
  if (DEMAND_RE.test(t)) return "Demand";
  if (LOYALTY_RE.test(t)) return "Loyalty";
  if (RISK_RE.test(t)) return "Risk";

  return "Demand";
}
