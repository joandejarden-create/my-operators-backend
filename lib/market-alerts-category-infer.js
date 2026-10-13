/**
 * Category inference for Market Alerts.
 *
 * CURRENT PRECEDENCE (exact order — first match wins):
 *  1. Source name contains "openings" → Supply (SOURCE_OPENINGS)
 *  2. Empty text → Demand (EMPTY_TEXT_FALLBACK) — rare
 *  3. Explicit market-demand/performance (and not blocked by development guard) → Demand (DEMAND_POSITIVE)
 *  4. Hotel/resort development announcement → Deals (HOTEL_DEV_ANNOUNCE)
 *  5. Site / land / property transaction deals → Deals (DEALS_SITE_TX)
 *  6. Construction/development financing → Capital (CONSTRUCTION_FINANCING)
 *  7. Brand signing / franchise / reflag → Brand (BRAND_SIGNING)
 *  8. Operator appointment / management agreement → Deals (OPERATOR_DEALS)
 *  9. Planning / zoning / permit / entitlement → Supply (PLANNING_SUPPLY)
 * 10. Construction / pipeline / openings / topping out → Supply (SUPPLY_PIPELINE)
 * 11. Residual acquisition/sale deal language → Deals (DEALS_RESIDUAL)
 * 12. Residual capital language → Capital (CAPITAL_RESIDUAL)
 * 13. Residual brand language → Brand (BRAND_RESIDUAL)
 * 14. Loyalty program language → Loyalty (LOYALTY)
 * 15. Risk / regulatory language → Risk (RISK)
 * 16. Specific hotel/project + development-event signals → routed Deals/Supply/Capital/Brand
 *     (DEV_SIGNAL_ROUTE) — Demand must not win here
 * 17. True Demand fallback only when none of the above → Demand (DEMAND_FALLBACK_USED)
 *
 * Demand is NOT the generic fallback for hotel-development stories.
 */

import { isHotelDevelopmentAnnouncement } from "./market-alerts-hotel-development-announce.js";

export const CATEGORY_PRECEDENCE = Object.freeze([
  "SOURCE_OPENINGS",
  "EMPTY_TEXT_FALLBACK",
  "DEMAND_POSITIVE",
  "HOTEL_DEV_ANNOUNCE",
  "DEALS_SITE_TX",
  "CONSTRUCTION_FINANCING",
  "BRAND_SIGNING",
  "OPERATOR_DEALS",
  "PLANNING_SUPPLY",
  "SUPPLY_PIPELINE",
  "DEALS_RESIDUAL",
  "CAPITAL_RESIDUAL",
  "BRAND_RESIDUAL",
  "LOYALTY",
  "RISK",
  "DEV_SIGNAL_ROUTE",
  "DEMAND_FALLBACK_USED",
]);

const CONSTRUCTION_FINANCING_RE =
  /\b(?:(?:construction|development|project)\s+(?:loan|financing|debt|funding)|(?:loan|financing|debt|funding).{0,40}(?:for\s+)?(?:construction|development)|mezzanine(?:\s+financing)?|senior\s+loan|equity\s+(?:partner|financing)|financing\s+secured|debt\s+secured|financiamiento|financia[cç][aã]o|cr[eé]dito\s+(?:construcci[oó]n|construção|hotel)|secures?\s+(?:a\s+)?(?:\$|USD)?[\d.,]+\s*(?:m|mm|million)?.{0,40}(?:construction|development)\s+(?:loan|financing))\b/i;

const PLANNING_SUPPLY_RE =
  /\b(?:zoning|planning\s+(?:approval|permission|application|consent)|entitlement|building\s+permit|construction\s+permit|environmental\s+(?:approval|permit)|coastal\s+(?:approval|permit)|land[- ]use|permiso|licencia|licença|alvará|aprobaci[oó]n|aprovação|planning approval granted|approves?\s+zoning)\b/i;

const SUPPLY_PIPELINE_RE =
  /\b(?:pipeline|construction|groundbreaking|ground\s+broken|breaks?\s+ground|rooms in construction|hotel opening|property opening|hotels?\s+opens?\b|(?:hotel|resort|property|inn)\s+opens?\b|opens?\s+(?:in|its doors|its first|for business)\b|opens?\s+as\b[\s\S]{0,80}?\b(?:hotel|resort|inn|suites)\b|opens?\s+(?:(?:a|the|its|our|their|first)\s+)+(?:new\s+)?(?:\d+[-\s]?(?:room|key|cottage|suite)s?\s+)?(?:\w+\s+){0,4}(?:hotel|resort|inn|suites|retreat|lodge|property)\b|opens?\s+(?:a\s+|the\s+)?(?:new\s+)?\d+[-\s]?(?:room|key|cottage|suite)s?\s+(?:\w+\s+){0,3}(?:hotel|resort|inn|suites|retreat|lodge)\b|opens?\s+(?:park\s+inn|hilton|marriott|hyatt|sonesta|springhill|holiday\s+inn|radisson|autograph|curio|tribute)\b|opens?\b[\s\S]{0,80}?\b\d+[-\s]?(?:room|key|cottage|suite)s?.{0,40}(?:hotel|resort|retreat|inn)\b|opening of\s+(?:a\s+|the\s+)?(?:new\s+)?(?:\w+\s+){0,4}(?:hotel|resort|inn|suites|property)\b|announces?\s+opening\s+of\b|opening\s+(?:its\s+)?first\s+(?:property|hotel|resort)\b|(?:hyatt\s+place|[\w'-]+\s+(?:hotel|resort|inn|suites|court|place))\b[\w\s&',.-]{0,50}\bnow open\b|\bnow open\b.{0,40}as\s+part\s+of\b|to open\b|set to open|slated to open|officially open|grand opening|soft open(?:ing)?|reopens?\b|reopening|reopened\b|debut(?:s|ed)?\s+(?:as\s+)?(?:a\s+)?(?:new\s+)?(?:\w+\s+){0,4}(?:hotel|resort|inn|suites|property)\b|topping out|tops out|under construction|development pipeline|pre[- ]?opening|preopening|completing\s+(?:a\s+)?(?:landmark\s+)?(?:convention\s+)?(?:headquarter\s+)?hotel|move(?:s|ing)?\s+closer\s+to\s+completing|renovation\s+completed|completes?\s+(?:a\s+)?(?:major\s+)?renovation|announces?\s+launch\s+of\s+(?:the\s+)?(?:\w+\s+){0,6}(?:\d+[-\s]?(?:room|key|cottage|suite)s?\s+)?(?:hotel|resort|inn|suites|retreat)|launches?\s+(?:a\s+)?(?:new\s+)?(?:\d+[-\s]?(?:room|key|cottage|suite)s?\s+)(?:\w+\s+){0,3}(?:hotel|resort|inn|suites|retreat|lodge)\b)\b/i;

const BRAND_RESIDUAL_RE =
  /\b(?:new brand|lifestyle brand|collection debut|to launch\b|launches?\s+in\b|joins (?:the )?(?:portfolio|collection)|adds to portfolio|brand expansion|collection by|expands?\s+(?:\w+\s+){0,6}(?:portfolio|presence)\b|heading\s+for\s+\d+\s+cities|expands?\s+to\s+\d+\s+cities|\d+\s+cities\s+in\b|announces?\s+launch\s+of\s+(?:the\s+)?(?:\w+\s+){0,4}(?:residency|collection|brand)|multi[- ]?brand\s+growth|plans?\s+to\s+double\s+(?:its\s+)?(?:portfolio|presence)|double\s+(?:its\s+)?portfolio)\b/i;

const DEALS_SITE_TX_RE =
  /\b(?:acquisition|acquires|acquired|acquiring|merger|merged|buys?\b|bought\b|purchases?\b|purchased\b|takes over|taking over|sold\b|sale of|sells\b|seller\b|changes hands|portfolio sale|disposition|divests?|divestiture|closes on|closed on|transaction|deal\b|m&a|m & a|stake sale|sells stake|acquires stake|ground lease|site (?:acquir|purchas|bought)|land (?:acquir|purchas|bought)|parcel (?:acquir|purchas)|terreno|predio|developer acquires)\b/i;

/** Operating hotel / portfolio asset sale → Capital (not Deals development). */
const CAPITAL_HOTEL_SALE_RE =
  /\b(?:(?:hotel|resort|inn|suites|property|portfolio).{0,80}(?:for sale|listed for sale|up for sale|on the market)|(?:for sale|listed for sale|up for sale).{0,80}(?:hotel|resort|inn|suites|property)|former.{0,40}hotel.{0,80}(?:for sale|listed)|full-service\s+hotel.{0,80}for sale)\b/i;

/** Operating hotel / portfolio acquisition → Capital. */
const CAPITAL_HOTEL_ACQUISITION_RE =
  /\b(?:(?:acquires?|acquired|acquiring|acquisition|buys?|bought|purchases?).{0,80}(?:\d+[-\s]?(?:room|key)s?.{0,40})?(?:beachfront\s+|full-service\s+|luxury\s+)?(?:hotel|resort|inn|suites|hotel\s+portfolio|portfolio\s+of\s+hotels)|(?:hotel|resort|inn|suites|hotel\s+portfolio).{0,60}(?:acquires?|acquired|acquisition|bought|purchased))\b/i;

const SITE_FOR_FUTURE_HOTEL_RE =
  /\b(?:(?:site|land|parcel|plot|terreno).{0,50}(?:for\s+(?:a\s+)?(?:future|planned|proposed)\s+)?(?:hotel|resort)|(?:acquir|buys?|bought|purchas|secures?).{0,40}(?:site|land|parcel|office\s+propert).{0,50}(?:hotel|resort|development|conversion)|developer\s+acquires?\s+site|office[- ]to[- ]hotel|office\s+property)\b/i;

const CAPITAL_RE =
  /\b(?:funding|financ(?:e|ing|es|ed)|investment|investor|lender|loan\b|reit\b|bond\b|capital raise|raises?\s+\$|refinanc|private equity|debt facility|equity stake|equity partner|capital partner|secures? (?:financing|funding|capital)|fundraise|ipo\b|mezzanine|senior loan|financiamiento|financia[cç][aã]o|cr[eé]dito|pr[eé]stamo|empr[eé]stimo|construction loan)\b/i;

const BRAND_SIGNING_RE =
  /\b(?:franchise|franchised|master franchise|re[- ]?brand(?:s|ed|ing)?|brand launch|soft brand|franchise agreement|brand(?:ing)? agreement|affiliation agreement|reflag(?:ged|ging)?|flagging|flagged as|conversion to|converts? to|flags?\s+(?:as|with)|under the .+ flag|franquicia|franquia|reopens?\s+as\b.{0,40}(?:collection|marriott|hilton|hyatt|autograph|curio|tapestry)|(?:marriott|hilton|ihg|hyatt|accor|wyndham|choice|radisson|mgallery|autograph|curio|tapestry|tribute|voco|garner|hualuxe|holiday inn|hilton garden|hyatt place|andaz|regenta)\s+(?:franchise|signs?|brand|announces?)|signs?\s+(?:with\s+)?(?:a\s+)?(?:new\s+)?(?:\w+\s+){0,4}(?:hotel|resort|inn|suites)|signing of\s+(?:its\s+)?(?:first\s+)?(?:a\s+)?(?:new\s+)?(?:\w+\s+){0,6}(?:hotel|resort|inn|hilton|hyatt|marriott|ihg|garner|voco|hualuxe)|sign(?:s|ed|ing)?\s+up\s+for\s+\d+\s+(?:properties|hotels|resorts)|joins?\s+(?:the\s+)?(?:marriott|hilton|ihg|hyatt|autograph|curio|tapestry)\s+portfolio|brings?\s+(?:the\s+)?(?:voco|hualuxe|garner|holiday inn|mgallery)\b|announces?\s+plans?\s+for\s+(?:a\s+)?(?:new\s+)?(?:hyatt|hilton|marriott|ihg|holiday inn|voco|garner|andaz)|to\s+bring\s+(?:voco|hualuxe|garner|holiday inn)\b|expands?\s+(?:\w+\s+){0,4}portfolio\s+with\b|\bportfolio\s+with\s+\d+[- ]?key\b|with\s+the\s+launch\s+of\s+(?:a\s+)?(?:new\s+)?(?:\w+\s+){0,4}(?:hotel|resort|inn|hubballi|regenta)|\blaunch\s+of\s+(?:a\s+)?(?:new\s+)?(?:\w+\s+){0,3}(?:hotel|resort|inn)\b|announces?\s+launch\s+of\s+(?:the\s+)?(?:\w+\s+){0,6}(?:residency|collection|brand)\b|launches?\s+(?:a\s+)?(?:new\s+)?(?:boutique\s+)?hotel\s+brand\b|debut(?:s|ed)?\s+as\b.{0,80}(?:curio|autograph|tapestry|tribute|collection)\b|strengthens?\s+presence.{0,40}(?:launch|opening|hotel|resort))\b/i;

const OPERATOR_DEALS_RE =
  /\b(?:management agreement|management contract|management contracts|operator appointment|appointed (?:as )?(?:operator|manager)|(?:operator|manager) appointed|third[- ]party management|acuerdo de gesti[oó]n|acordo de gest[aã]o|assumes?\s+management|awarded\s+management|selected\s+to\s+manage|to\s+(?:manage|operate)\b|management\s+of\s+(?:the\s+)?(?:new\s+)?(?:\w+\s+){0,4}(?:hotel|inn|suites|resort)|expands?\s+portfolio\s+with\s+(?:new\s+)?management|adds?\s+\w+\s+new\s+properties\s+to\s+(?:hotel\s+)?management\s+portfolio|to\s+hotel\s+management\s+portfolio)\b/i;

/** Positive Demand — market demand / performance only (not generic "demand" alone when possible). */
const DEMAND_POSITIVE_RE =
  /\b(?:revpar|occupancy|adr\b|hotel demand|resort demand|booking trends?|booking data|tourism arrivals|visitor arrivals|airlift|convention (?:demand|calendar)|group demand|mice demand|corporate travel|feeder[- ]markets?|compression|need periods?|demand (?:growth|decline|strengthens?|softens?)|hotel compression|market performance|guest satisfaction|sentiment survey|experiences?\s+trends?|travel\s+trends?|mid[- ]year\s+(?:experiences?\s+)?trends?)\b/i;

const LOYALTY_RE =
  /\b(?:loyalty program|loyalty|rewards program|bonvoy|hilton honors|world of hyatt|frequent guest|points program|member(?:ship)? program|elite qualification)\b/i;

const RISK_RE =
  /\b(?:lodging\s+tax|tourism\s+tax|hotel\s+tax|occupancy\s+tax|bed\s+tax|tax\s+hike|tax\s+increase|proposed\s+(?:hotel\s+)?tax|regulat(?:ion|ory)|union\b|labor\s+(?:law|strike|shortage)|insurance|hurricane\s+resilience|security\s+(?:threat|advisory)|hotel\s+licens|environmental\s+regulat|coastal\s+restrict|water\s+restrict|building\s+(?:standards?|height|heights)|height\s+(?:limit|limits|restriction|cap)|rezoning|re-?zoning|zoning\s+(?:ordinance|law|change|vote|debate)|ordinance|esg\s+mandat|government\s+policy.{0,40}hotel|lawsuit|litigation|sanction|operating\s+requirements?|mandatory .{0,30}hotel|strike\b|downturn|bankrupt|investigation|fraud)\b/i;

/** Policy / tax / zoning risk — never Act Now dealmaking opportunities. */
export const RISK_POLICY_ACT_NOW_BLOCK_RE =
  /\b(?:lodging\s+tax|tourism\s+tax|hotel\s+tax|occupancy\s+tax|bed\s+tax|tax\s+hike|tax\s+increase|proposed\s+(?:hotel\s+)?tax|regulat(?:ion|ory)|rezoning|re-?zoning|zoning\s+(?:ordinance|law|change|vote|debate)|building\s+height|height\s+(?:limit|limits|restriction|cap)|ordinance|lawsuit|litigation|sanction)\b/i;

/** Specific hotel / resort / project entity cues. */
const SPECIFIC_HOTEL_PROJECT_RE =
  /\b(?:hotel|resort|inn|suites|lodge|palace|tower|project|development|mixed[- ]use)\b/i;

/**
 * Development-event signals that block Demand unless positive demand is dominant.
 */
const DEV_EVENT_SIGNAL_RE =
  /\b(?:site acquisition|acquires? site|land (?:acquir|purchas)|planning|zoning|permit|entitlement|financing|construction loan|development proposal|joint venture|\bjv\b|partners?\s+with|brand signing|franchise|management agreement|operator appointment|construction start|breaks?\s+ground|groundbreaking|topping out|tops out|under construction|opening date|pre[- ]?opening|preopening|announced|unveils?|proposed|plans?\s+to\s+develop|new (?:\d+[-\s]?(?:room|key)s?\s+)?(?:hotel|resort)|hospitality component|hotel development|resort development|development launched|launches?\s+development|expands?.{0,40}(?:portfolio|presence).{0,40}(?:hotel|property|resort|key)|expands?.{0,80}(?:\d+[-\s]?(?:room|key)s?).{0,40}(?:hotel|resort)|convert(?:s|ed|ing)?.{0,80}(?:hotel|resort)|adaptive[- ]reuse|signs?\s+(?:with|for|up)|signing of|strategic agreement|to develop|re[- ]?brand|property opening|completing.{0,40}hotel|management portfolio)\b/i;

function hasSpecificHotelProject(text) {
  return SPECIFIC_HOTEL_PROJECT_RE.test(text);
}

function hasDevEventSignal(text) {
  return DEV_EVENT_SIGNAL_RE.test(text) || isHotelDevelopmentAnnouncement(text);
}

/**
 * Demand may win only when positive demand language is present AND
 * either there is no hotel-development conflict, or demand is clearly dominant.
 */
function isDominantDemand(text) {
  if (!DEMAND_POSITIVE_RE.test(text)) return false;
  // Development-heavy stories with only weak/coincident demand words stay non-Demand.
  if (hasSpecificHotelProject(text) && hasDevEventSignal(text)) {
    // Allow Demand if primary subject is clearly performance metrics.
    const demandHeavy =
      /\b(?:revpar|occupancy|adr\b|hotel demand|resort demand|tourism arrivals|visitor arrivals|airlift|group demand|mice demand|compression)\b/i.test(
        text
      ) &&
      !/\b(?:announced|unveils?|breaks?\s+ground|franchise|management agreement|acquires? site|construction loan|zoning|planning approval)\b/i.test(
        text
      );
    return demandHeavy;
  }
  return true;
}

/**
 * Route specific hotel/project + development signals away from Demand fallback.
 * @returns {{ category: string, rule: string }|null}
 */
function routeDevSignals(text) {
  if (!(hasSpecificHotelProject(text) && hasDevEventSignal(text))) return null;

  if (CONSTRUCTION_FINANCING_RE.test(text) || (/\bconstruction loan\b/i.test(text) && CAPITAL_RE.test(text))) {
    return { category: "Capital", rule: "DEV_SIGNAL_ROUTE:financing" };
  }
  if (BRAND_SIGNING_RE.test(text)) {
    return { category: "Brand", rule: "DEV_SIGNAL_ROUTE:brand" };
  }
  if (OPERATOR_DEALS_RE.test(text)) {
    return { category: "Deals", rule: "DEV_SIGNAL_ROUTE:operator" };
  }
  if (PLANNING_SUPPLY_RE.test(text)) {
    return { category: "Supply", rule: "DEV_SIGNAL_ROUTE:planning" };
  }
  if (SUPPLY_PIPELINE_RE.test(text) && !isHotelDevelopmentAnnouncement(text)) {
    return { category: "Supply", rule: "DEV_SIGNAL_ROUTE:pipeline" };
  }
  if (isHotelDevelopmentAnnouncement(text) || DEALS_SITE_TX_RE.test(text) || /\b(?:joint venture|partners?\s+with|announced|unveils?|proposed|develop)\b/i.test(text)) {
    return { category: "Deals", rule: "DEV_SIGNAL_ROUTE:development_deals" };
  }
  if (/\bpre[- ]?opening|preopening\b/i.test(text)) {
    return { category: "Supply", rule: "DEV_SIGNAL_ROUTE:preopening" };
  }
  return { category: "Deals", rule: "DEV_SIGNAL_ROUTE:default_deals" };
}

/**
 * @param {string} text
 * @param {string} [source]
 * @returns {{
 *   category: string,
 *   matchedRule: string,
 *   precedence: string[],
 *   fallbackUsed: boolean,
 *   demandFallbackUsed: boolean,
 * }}
 */
export function inferCategoryWithReason(text, source = "") {
  const src = (source || "").trim();
  const t = (text || "").trim();
  const precedence = CATEGORY_PRECEDENCE;

  if (/\bopenings?\b/i.test(src)) {
    return {
      category: "Supply",
      matchedRule: "SOURCE_OPENINGS",
      precedence,
      fallbackUsed: false,
      demandFallbackUsed: false,
    };
  }

  if (!t) {
    return {
      category: "Demand",
      matchedRule: "EMPTY_TEXT_FALLBACK",
      precedence,
      fallbackUsed: true,
      demandFallbackUsed: true,
    };
  }

  // Tax / regulation / zoning policy risks beat Demand fallback and weak demand cues.
  if (RISK_RE.test(t) && RISK_POLICY_ACT_NOW_BLOCK_RE.test(t)) {
    return {
      category: "Risk",
      matchedRule: "RISK_POLICY",
      precedence,
      fallbackUsed: false,
      demandFallbackUsed: false,
    };
  }

  // Positive Demand first only when dominant / unblocked.
  if (isDominantDemand(t)) {
    return {
      category: "Demand",
      matchedRule: "DEMAND_POSITIVE",
      precedence,
      fallbackUsed: false,
      demandFallbackUsed: false,
    };
  }

  if (isHotelDevelopmentAnnouncement(t)) {
    return {
      category: "Deals",
      matchedRule: "HOTEL_DEV_ANNOUNCE",
      precedence,
      fallbackUsed: false,
      demandFallbackUsed: false,
    };
  }

  // Operating hotel/portfolio for sale or acquisition → Capital. Site for future hotel stays Deals below.
  if (
    (CAPITAL_HOTEL_SALE_RE.test(t) || CAPITAL_HOTEL_ACQUISITION_RE.test(t)) &&
    !SITE_FOR_FUTURE_HOTEL_RE.test(t)
  ) {
    return {
      category: "Capital",
      matchedRule: CAPITAL_HOTEL_SALE_RE.test(t) ? "CAPITAL_HOTEL_SALE" : "CAPITAL_HOTEL_ACQUISITION",
      precedence,
      fallbackUsed: false,
      demandFallbackUsed: false,
    };
  }

  if (
    DEALS_SITE_TX_RE.test(t) &&
    !/\b(hotel opening|grand opening|soft open|to open|set to open)\b/i.test(t) &&
    !/\bacquisition financ/i.test(t) &&
    !CONSTRUCTION_FINANCING_RE.test(t)
  ) {
    return {
      category: "Deals",
      matchedRule: "DEALS_SITE_TX",
      precedence,
      fallbackUsed: false,
      demandFallbackUsed: false,
    };
  }

  if (CONSTRUCTION_FINANCING_RE.test(t) && CAPITAL_RE.test(t)) {
    return {
      category: "Capital",
      matchedRule: "CONSTRUCTION_FINANCING",
      precedence,
      fallbackUsed: false,
      demandFallbackUsed: false,
    };
  }

  // Brand before Supply so "franchise … planned hotel" does not become Supply.
  if (BRAND_SIGNING_RE.test(t)) {
    return {
      category: "Brand",
      matchedRule: "BRAND_SIGNING",
      precedence,
      fallbackUsed: false,
      demandFallbackUsed: false,
    };
  }

  if (OPERATOR_DEALS_RE.test(t)) {
    return {
      category: "Deals",
      matchedRule: "OPERATOR_DEALS",
      precedence,
      fallbackUsed: false,
      demandFallbackUsed: false,
    };
  }

  if (PLANNING_SUPPLY_RE.test(t)) {
    return {
      category: "Supply",
      matchedRule: "PLANNING_SUPPLY",
      precedence,
      fallbackUsed: false,
      demandFallbackUsed: false,
    };
  }

  if (SUPPLY_PIPELINE_RE.test(t)) {
    return {
      category: "Supply",
      matchedRule: "SUPPLY_PIPELINE",
      precedence,
      fallbackUsed: false,
      demandFallbackUsed: false,
    };
  }

  if (DEALS_SITE_TX_RE.test(t)) {
    return {
      category: "Deals",
      matchedRule: "DEALS_RESIDUAL",
      precedence,
      fallbackUsed: false,
      demandFallbackUsed: false,
    };
  }

  if (CAPITAL_RE.test(t)) {
    return {
      category: "Capital",
      matchedRule: "CAPITAL_RESIDUAL",
      precedence,
      fallbackUsed: false,
      demandFallbackUsed: false,
    };
  }

  if (BRAND_RESIDUAL_RE.test(t)) {
    return {
      category: "Brand",
      matchedRule: "BRAND_RESIDUAL",
      precedence,
      fallbackUsed: false,
      demandFallbackUsed: false,
    };
  }

  if (LOYALTY_RE.test(t)) {
    return {
      category: "Loyalty",
      matchedRule: "LOYALTY",
      precedence,
      fallbackUsed: false,
      demandFallbackUsed: false,
    };
  }

  if (RISK_RE.test(t)) {
    return {
      category: "Risk",
      matchedRule: "RISK",
      precedence,
      fallbackUsed: false,
      demandFallbackUsed: false,
    };
  }

  // Demand negative guard — specific hotel/project + development signals never fall through to Demand.
  const routed = routeDevSignals(t);
  if (routed) {
    return {
      category: routed.category,
      matchedRule: routed.rule,
      precedence,
      fallbackUsed: false,
      demandFallbackUsed: false,
    };
  }

  return {
    category: "Demand",
    matchedRule: "DEMAND_FALLBACK_USED",
    precedence,
    fallbackUsed: true,
    demandFallbackUsed: true,
  };
}

export function inferCategoryFromText(text, source = "") {
  return inferCategoryWithReason(text, source).category;
}
