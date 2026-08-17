/**
 * Category inference for Market Alerts.
 * Supply/openings checked before brand-name mentions in headlines.
 * Deals checked before Capital when acquisition language is present.
 */

const SUPPLY_RE =
  /\b(pipeline|construction|groundbreaking|ground\s+broken|rooms in construction|new hotel|new resorts?|hotel opening|hotels?\s+opens?\b|(?:hotel|resort|property|inn)\s+opens?\b|opens?\s+(?:in|its doors|its first|for business)\b|opening of|to open\b|set to open|slated to open|officially open|grand opening|soft open|debut(?:s|ed)?\b|unveils?\b|launch(?:es|ed)?\s+(?:its?\s+)?(?:new\s+)?(?:hotel|resort|property)|signs?\s+(?:for\s+)?(?:a\s+)?(?:new\s+)?(?:hotel|resort|property)|breaks ground|topping out|tops out|development pipeline|expands? (?:its )?(?:pipeline|portfolio) with|adds? .+ (?:hotel|resort|property)|signed for .{0,40}opening)\b/i;

const DEALS_RE =
  /\b(acquisition|acquires|acquired|acquiring|merger|merged|buys?\b|bought\b|purchases?\b|purchased\b|takes over|taking over|sold\b|sale of|sells\b|seller\b|changes hands|listed for sale|for sale\b|offered for\b|portfolio sale|disposition|divests?|divestiture|closes on|closed on|transaction|deal\b|m&a|m & a|stake sale|sells stake|acquires stake)\b/i;

const CAPITAL_RE =
  /\b(funding|financ(?:e|ing|es|ed)|investment|investor|lender|loan\b|reit\b|bond\b|capital raise|raises?\s+\$|refinanc|private equity|debt facility|joint venture|\bjv\b|equity stake|secures? (?:financing|funding|capital)|fundraise|ipo\b)\b/i;

const BRAND_RE =
  /\b(franchise|franchised|master franchise|rebrand|rebrands|rebranded|brand launch|new brand|soft brand|lifestyle brand|collection debut|to launch\b|launches?\s+in\b|joins (?:the )?(?:portfolio|collection)|adds to portfolio|brand expansion|flagging|flagged as|conversion to|converts? to|flags?\s+(?:as|with)|under the .+ flag|collection by)\b/i;

const DEMAND_RE =
  /\b(revpar|occupancy|adr\b|demand|booking trends?|traveler|traveller|performance|arrivals|guest satisfaction|market performance|outlook|forecast|sentiment survey)\b/i;

const LOYALTY_RE =
  /\b(loyalty program|loyalty|rewards program|bonvoy|hilton honors|world of hyatt|frequent guest|points program|member(?:ship)? program)\b/i;

const RISK_RE =
  /\b(lawsuit|regulat(?:ion|ory)|strike\b|downturn|bankrupt|sanction|risk\b|recall|investigation|fraud|litigation)\b/i;

export function inferCategoryFromText(text, source = "") {
  const src = (source || "").trim();
  if (/\bopenings?\b/i.test(src)) return "Supply";

  const t = (text || "").trim();
  if (!t) return "Demand";

  // Prefer Deals when acquisition language coexists with "opens/debut" noise.
  if (
    DEALS_RE.test(t) &&
    !/\b(hotel opening|grand opening|soft open|to open|set to open)\b/i.test(t) &&
    !/\bacquisition financ/i.test(t)
  ) {
    return "Deals";
  }
  if (SUPPLY_RE.test(t)) return "Supply";
  if (DEALS_RE.test(t)) return "Deals";
  if (CAPITAL_RE.test(t)) return "Capital";
  if (BRAND_RE.test(t)) return "Brand";
  if (LOYALTY_RE.test(t)) return "Loyalty";
  if (RISK_RE.test(t)) return "Risk";
  if (DEMAND_RE.test(t)) return "Demand";

  return "Demand";
}
