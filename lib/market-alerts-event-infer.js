/**
 * Deterministic Event Type + What Changed inference for Market Alerts (V1).
 * No LLM — pattern matching only.
 */

import { detectTransactionNegation, validateHotelAssetTransaction } from "./market-alerts-qualification-gate.js";

export const EVENT_WHAT_CHANGED = {
  "Hotel For Sale": "Property is being marketed for sale",
  Acquisition: "Ownership change announced or reported",
  Sale: "Hotel sale completed or announced",
  "Portfolio Acquisition": "Multi-asset portfolio transaction reported",
  JV: "Joint venture or partnership structure reported",
  Recapitalization: "Recapitalization or capital restructuring reported",
  Distress: "Distress, foreclosure, or bankruptcy signal reported",
  "New Development": "New hotel development announced",
  "Planning Approval": "Planning or entitlement progress reported",
  "Construction Start": "Construction start or groundbreaking reported",
  "Brand Signing": "Brand affiliation signed or announced",
  Reflag: "Brand reflag or conversion of affiliation reported",
  Conversion: "Asset conversion or adaptive reuse reported",
  "Brand Exit": "Brand exit or de-flagging reported",
  "Operator Appointment": "Operator appointment announced",
  "Operator Change": "Hotel operator change reported",
  "Operator Exit": "Operator exit reported",
  "Management Agreement": "Management agreement announced",
  Financing: "Financing secured or announced",
  Refinancing: "Refinancing reported",
  "Major Renovation": "Major renovation or CapEx program reported",
  Repositioning: "Repositioning or product strategy change reported",
  "Site Acquisition": "Hospitality development site or land control reported",
  "Planning Application": "Hotel planning or entitlement application reported",
  "Development Proposal": "Hotel or resort project proposed or unveiled",
  "Adaptive Reuse Proposal": "Adaptive reuse or conversion-to-hotel proposal reported",
};

/**
 * Ordered rules: first match wins (more specific first).
 * @type {Array<{ eventType: string, re: RegExp }>}
 */
const EVENT_RULES = [
  {
    eventType: "Site Acquisition",
    re: /\b(?:(?:hotel|resort|hospitality) (?:development )?(?:site|parcel|land|plot).{0,40}(?:acquir|bought|buys|purchas|sold|sale)|(?:acquir|buys?|bought|purchas).{0,40}(?:hotel|resort) (?:development )?(?:site|parcel|land)|land acquir(?:ed|es)? for (?:a )?(?:planned |proposed )?(?:hotel|resort)|(?:developer|owner).{0,40}(?:buys?|acquir\w*).{0,50}for (?:a )?(?:planned |proposed )?(?:\d+[-\s]?(?:room|key) )?(?:hotel|resort)|(?:site|parcel|land).{0,25}for (?:a )?(?:planned |proposed )?(?:\d+[-\s]?(?:room|key) )?(?:hotel|resort))\b/i,
  },
  {
    eventType: "Planning Approval",
    re: /\b(planning (?:approval|permission|consent)|zoning (?:approval|approved|action)|entitlement(?:s)? (?:approved|secured)|receives? approval|(?:city council|planning board).{0,55}approv(?:es|ed|al)|(?:hotel|resort).{0,35}zoning.{0,25}(?:action|approv)|development application approved|conditional[- ]use approval|site plan approval|planning permission approved)\b/i,
  },
  {
    eventType: "Planning Application",
    re: /\b(?:(?:hotel|resort).{0,50}(?:planning|zoning|entitlement|permit).{0,30}(?:application|submitted|filed|lodged|seeks?)|(?:plans?|planning application|zoning application|permit application).{0,40}(?:submitted|filed|lodged).{0,40}(?:hotel|resort)|(?:hotel|resort).{0,30}(?:planning|development) application|plans submitted.{0,40}(?:hotel|resort)|(?:hotel|resort) plan\b|signals? support.{0,50}(?:hotel|resort)|planning board.{0,50}(?:recommends|support)|public hearing.{0,40}(?:hotel|resort))\b/i,
  },
  {
    eventType: "Adaptive Reuse Proposal",
    re: /\b(?:(?:office|warehouse|historic|residential).{0,40}(?:to|into).{0,25}(?:a )?(?:hotel|resort)|adaptive reuse.{0,40}(?:hotel|resort)|convert(?:s|ed|ing)? .{0,50}(?:into|to) (?:a )?(?:hotel|resort)|hotel redevelopment (?:proposal|plan|planning)|redevelopment into (?:a )?(?:hotel|resort)|office[- ]to[- ]hotel|office(?:\s+tower|\s+building)?.{0,80}(?:jw marriott|marriott|hilton|hyatt|omni|hotel|resort)|(?:second act|transforming|headed) .{0,40}(?:as |into )(?:a )?(?:luxury )?(?:resort|hotel|jw marriott)|historic (?:building|tower).{0,40}(?:hotel|resort|lodging)|(?:office|warehouse|building).{0,40}(?:conversion|converted|redevelopment).{0,30}(?:hotel|resort|jw marriott))\b/i,
  },
  {
    eventType: "Development Proposal",
    re: /\b(?:proposed (?:hotel|resort)|planned (?:hotel|resort)|(?:new |future )?(?:hotel|resort) (?:project |development )?(?:proposed|planned|unveiled)|developer (?:proposes|plans|unveils) (?:a )?(?:\d+[-\s]?(?:room|key) )?(?:hotel|resort)|(?:hotel|resort) development planned|(?:mixed[- ]use).{0,60}(?:hotel|resort)|(?:includes?|including) (?:a )?(?:\d+[-\s]?(?:room|key) )?(?:hotel|resort)(?: component)?|(?:architect|design team|project manager|master planner).{0,40}(?:appointed|selected).{0,30}(?:hotel|resort)|(?:hotel|resort).{0,40}(?:architect|design team|developer|project manager|master planner).{0,20}(?:appointed|selected)|(?:seeking|seeks) .{0,50}(?:boutique )?hotel.{0,40}developer|(?:request for (?:qualifications|proposals)|RFQ|RFP).{0,40}hotel)\b/i,
  },
  { eventType: "Hotel For Sale", re: /\b(for sale|offered for|hits the market|brought to market|marketing (?:the )?(?:hotel|property)|sale process|seeking (?:a )?buyer|on the market)\b/i },
  { eventType: "Distress", re: /\b(bankruptcy|foreclosure|distressed|receivership|chapter\s*11|default(?:ed)?|special servicer)\b/i },
  { eventType: "Portfolio Acquisition", re: /\b(portfolio (?:acquisition|purchase|sale|deal)|acquires? \d+ (?:hotels?|properties)|buys? \d+[- ]hotel)\b/i },
  { eventType: "JV", re: /\b(joint venture|\bJV\b|co-?develop(?:ment)?|partnership (?:to |for )(?:develop|acquire|own))\b/i },
  { eventType: "Recapitalization", re: /\b(recapitali[sz]ation|recap\b|capital restructuring)\b/i },
  { eventType: "Refinancing", re: /\b(refinanc(?:e|ing|ed))\b/i },
  {
    eventType: "Financing",
    re: /\b(?:(?:credit approval|development (?:facility|capital|funding|loan)|construction (?:loan|financing|debt)|debt package|project (?:loan|financing)).{0,70}(?:hotel|resort)|(?:hotel|resort).{0,70}(?:credit approval|construction (?:loan|financing|debt)|development (?:financing|loan|funding|capital|facility)|project (?:loan|financing)|debt package))\b/i,
  },
  { eventType: "Financing", re: /\b(secur(?:es|ed|ing) (?:\$|USD)?[\d.,]+\s*(?:m|mm|million|billion)?(?:\s+(?:in )?(?:financing|debt|loan|mortgage))?|(?:financing|loan|debt|mortgage) (?:of |for )(?:\$|USD)?[\d.,]+|closes? (?:\$|USD)?[\d.,]+\s*(?:m|mm|million).*(?:loan|financing|debt))\b/i },
  { eventType: "Construction Start", re: /\b(breaks? ground|groundbreaking|construction (?:starts?|begun|underway)|commences? construction)\b/i },
  { eventType: "Reflag", re: /\b(reflag(?:ged|ging)?|re-?brand(?:ed|ing)?|will (?:now )?operate as|to (?:be )?(?:flagged|branded) as)\b/i },
  { eventType: "Brand Exit", re: /\b((?:exits?|leaving|drops?) (?:the )?(?:brand|flag)|de-?flag(?:ged|ging)?|ends? (?:its )?affiliation|terminat(?:es|ed|ing) (?:the )?franchise)\b/i },
  { eventType: "Brand Signing", re: /\b((?:ihg|marriott|hilton|hyatt|accor|wyndham|choice|radisson)\s+signs?|signs?\s+(?:the\s+)?(?:noted collection|voco|autograph|tapestry|curio|tribute|hotel|resort|property))\b/i },
  { eventType: "Brand Signing", re: /\b((?:signs?|signed|announces?) (?:a )?(?:franchise|brand|affiliation)|(?:franchise|brand) agreement|joins? (?:the )?(?:marriott|hilton|ihg|hyatt|wyndham|choice|radisson)|new (?:marriott|hilton|ihg|hyatt) (?:hotel|property))\b/i },
  { eventType: "Conversion", re: /\b(conversion|adaptive reuse|convert(?:s|ed|ing)? (?:to|into) (?:a )?(?:hotel|resort))\b/i },
  { eventType: "Operator Exit", re: /\b(operator (?:exits?|exit|departs?|departure)|ends? (?:its )?management|management (?:contract )?(?:terminated|ends?))\b/i },
  { eventType: "Operator Change", re: /\b(operator (?:change|switch|replaced)|new (?:third[- ]party )?operator|changes? (?:hotel )?operator)\b/i },
  { eventType: "Operator Appointment", re: /\b((?:appoints?|appointed|names?|named) (?:as )?(?:operator|manager)|(?:operator|manager) (?:appointed|named)|takes? (?:over )?management)\b/i },
  { eventType: "Management Agreement", re: /\b(management agreement|management contract|third[- ]party management|to bring (?:the )?(?:voco|hotel)|brings? (?:the )?(?:voco|hotel))\b/i },
  { eventType: "Major Renovation", re: /\b(major renov(?:ation|ate)|renovation program|capex (?:program|project)|\$[\d.,]+\s*(?:m|mm|million).*(?:renovat|refurb)|refurbish(?:ment|es|ed)?)\b/i },
  { eventType: "Repositioning", re: /\b(reposition(?:ing|ed|s)?|product reposition)\b/i },
  { eventType: "Acquisition", re: /\b(acquires?|acquired|acquisition|buys?|purchased?|purchase of)\b/i },
  { eventType: "Sale", re: /\b(sells?|sold|sale of|closes? sale|transaction clos(?:es|ed))\b/i },
  { eventType: "New Development", re: /\b(new (?:hotel|resort) (?:development|project|planned)|(?:announces?|announced|plans?|planned) (?:a )?(?:new )?(?:\d+[-\s]?room )?(?:hotel|resort)(?: development|project)?|develop(?:s|ing|ment of) (?:a )?(?:new )?(?:hotel|resort)|hotel (?:pipeline|project) announced)\b/i },
];

/**
 * @param {{ title?: string, summary?: string }} input
 * @returns {{ eventType: string|null, whatChanged: string|null, matched: boolean }}
 */
export function inferMarketAlertEvent(input = {}) {
  const title = input.title || "";
  const summary = input.summary || "";
  const text = `${title} ${summary}`.trim();
  if (!text) {
    return { eventType: null, whatChanged: null, matched: false, negated: false };
  }

  // V1.1: block negated transaction headlines before keyword rules (e.g. "not sold").
  const earlyNegation = detectTransactionNegation(text);
  if (earlyNegation.negated) {
    return {
      eventType: null,
      whatChanged: null,
      matched: false,
      negated: true,
      negationReason: earlyNegation.reason,
    };
  }

  for (const rule of EVENT_RULES) {
    if (rule.re.test(text)) {
      const negation = detectTransactionNegation(text, rule.eventType);
      if (negation.negated) {
        return {
          eventType: null,
          whatChanged: null,
          matched: false,
          negated: true,
          negationReason: negation.reason,
        };
      }
      if (["Sale", "Acquisition", "Portfolio Acquisition"].includes(rule.eventType)) {
        const txValidation = validateHotelAssetTransaction(text, rule.eventType);
        if (!txValidation.valid) {
          continue;
        }
      }
      return {
        eventType: rule.eventType,
        whatChanged: EVENT_WHAT_CHANGED[rule.eventType] || null,
        matched: true,
        negated: false,
      };
    }
  }

  return { eventType: null, whatChanged: null, matched: false, negated: false };
}
