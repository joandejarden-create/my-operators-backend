/**
 * Deterministic Event Type + What Changed inference for Market Alerts (V1 + Pre-Opening Capture V1).
 * No LLM — pattern matching only.
 *
 * Event vocabulary extensions (code-side; Airtable Meta API not migrated in this phase):
 * - Pre-Opening Leadership
 * - Commercialization
 * - Construction Progress
 */

import { detectTransactionNegation, validateHotelAssetTransaction } from "./market-alerts-qualification-gate.js";
import { isPreOpeningLeadershipNews } from "./market-alerts-content-quality.js";
import { isHotelDevelopmentAnnouncement } from "./market-alerts-hotel-development-announce.js";

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
  "Construction Progress": "Construction progress milestone reported",
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
  "Pre-Opening Leadership": "Pre-opening leadership or opening team appointment reported",
  Commercialization: "Reservations or bookings opened ahead of hotel opening",
};

const HOTEL_CTX_RE = /\b(hotel|resort|hospitality|lodging|hoteleiro|hotelero|tur[ií]stic)/i;

/**
 * Ordered rules: first match wins (more specific first).
 * @type {Array<{ eventType: string, re: RegExp }>}
 */
const EVENT_RULES = [
  // --- Pre-opening leadership (before generic personnel-adjacent patterns) ---
  {
    eventType: "Pre-Opening Leadership",
    re: /\b(?:pre[- ]?opening|preopening|preapertura|pré[- ]?abertura).{0,80}(?:general manager|gm\b|director of sales|dosm|director of revenue|gerente general|gerente geral|director de ventas|diretor de vendas|opening team|equipo de|equipe de)|(?:general manager|gm|gerente general|gerente geral).{0,40}(?:pre[- ]?opening|new opening|new hotel|preapertura|pré[- ]?abertura)|(?:appoints?|appointed|names?|named).{0,60}(?:pre[- ]?opening|opening).{0,40}(?:general manager|gm|director of sales|dosm|director of revenue)\b/i,
  },
  // --- Site / land (before generic Acquisition) ---
  {
    eventType: "Site Acquisition",
    re: /\b(?:(?:hotel|resort|hospitality) (?:development )?(?:site|parcel|land|plot|predio|terreno).{0,50}(?:acquir|bought|buys|purchas|sold|sale|secured|option)|(?:acquir|buys?|bought|purchas|secures?).{0,50}(?:hotel|resort|hospitality|future hotel|planned hotel|proposed hotel).{0,30}(?:site|parcel|land|plot|terreno|predio)|(?:acquir|buys?|bought|purchas|secures?).{0,40}(?:site|parcel|land|plot|terreno|predio).{0,50}(?:hotel|resort|hospitality|future hotel|planned|proposed|development)|land acquir(?:ed|es)? for (?:a )?(?:planned |proposed |future )?(?:hotel|resort)|(?:developer|owner).{0,40}(?:buys?|acquir\w*|purchas\w*).{0,50}for (?:a )?(?:planned |proposed |future )?(?:\d+[-\s]?(?:room|key) )?(?:hotel|resort)|(?:site|parcel|land|terreno|predio).{0,30}for (?:a )?(?:planned |proposed |future )?(?:\d+[-\s]?(?:room|key) )?(?:hotel|resort|hospitality)|ground lease.{0,40}(?:hotel|resort|hospitality)|(?:hotel|resort).{0,40}ground lease)\b/i,
  },
  // --- Planning approval (before Development Proposal / mixed-use) ---
  {
    eventType: "Planning Approval",
    re: /\b(?:planning (?:approval|permission|consent)|approves?\s+zoning|zoning\s+(?:approval|approved|action|for)|rezon(?:ing|ed).{0,30}(?:approv|granted)|entitlement(?:s)? (?:approved|secured)|land[- ]use approval|receives? approval|(?:city council|city|planning board|planning commission|commission|council).{0,55}approv(?:es|ed|al)|(?:council|commission|city) approves?.{0,60}(?:hotel|resort|zoning|mixed[- ]use)|(?:hotel|resort).{0,40}(?:zoning|planning|development).{0,25}(?:action|approv|permission)|development application approved|conditional[- ]use approval|site plan approval|planning permission (?:approved|granted)|(?:aprueban|aprobaci[oó]n|aprovação|aprovacao).{0,40}(?:hotel|hotelero|hoteleiro|proyecto hotel|projeto hotel)|(?:proyecto hotelero|projeto hoteleiro|hotel).{0,40}(?:recib[ea]|recebe|obtiene|obtém).{0,30}(?:aprobaci[oó]n|aprovação|licencia|licença)|(?:licencia|licença|permiso|alvará).{0,30}(?:ambiental).{0,40}(?:hotel|hotelero|hoteleiro)|(?:environmental|coastal|shoreline).{0,30}(?:approval|permit|license|licence).{0,40}(?:hotel|resort)|(?:hotel|resort|proyecto hotelero|projeto hoteleiro|hotel project).{0,50}(?:environmental|coastal|EIA|licencia ambiental|licença ambiental).{0,30}(?:approval|approved|permit|granted|aprovad|aprobad|receives?)|EIA approved.{0,40}(?:hotel|resort)|tourism development permit.{0,40}(?:hotel|resort)|building permit (?:issued|approved).{0,40}(?:hotel|resort)|(?:hotel|resort).{0,40}building permit (?:issued|approved)|(?:hotel|resort|new hotel).{0,40}(?:receives?|gets?|wins?).{0,20}(?:building |construction |environmental |coastal )?(?:permit|approval))\b/i,
  },
  {
    eventType: "Planning Application",
    re: /\b(?:(?:hotel|resort).{0,50}(?:planning|zoning|entitlement|permit).{0,30}(?:application|submitted|filed|lodged|seeks?)|(?:plans?|planning application|zoning application|permit application|building permit|construction permit|development application).{0,40}(?:submitted|filed|lodged|issued).{0,40}(?:hotel|resort)|(?:hotel|resort).{0,30}(?:planning|development) application|plans submitted.{0,40}(?:hotel|resort)|(?:hotel|resort) plan\b|signals? support.{0,50}(?:hotel|resort)|planning board.{0,50}(?:recommends|support)|public hearing.{0,40}(?:hotel|resort)|(?:permiso|licencia|licença|alvará).{0,40}(?:construcci[oó]n|construção).{0,30}(?:hotel|hotelero|hoteleiro)|(?:solicitud|presenta|apresenta).{0,40}(?:permiso|licencia|licença).{0,30}(?:hotel|hotelero|hoteleiro))\b/i,
  },
  {
    eventType: "Adaptive Reuse Proposal",
    re: /\b(?:(?:office|warehouse|historic|residential).{0,40}(?:to|into).{0,25}(?:a )?(?:hotel|resort)|adaptive reuse.{0,40}(?:hotel|resort)|convert(?:s|ed|ing)? .{0,50}(?:into|to) (?:a )?(?:hotel|resort)|hotel redevelopment (?:proposal|plan|planning)|redevelopment into (?:a )?(?:hotel|resort)|office[- ]to[- ]hotel|office(?:\s+tower|\s+building)?.{0,80}(?:jw marriott|marriott|hilton|hyatt|omni|hotel|resort)|(?:second act|transforming|headed) .{0,40}(?:as |into )(?:a )?(?:luxury )?(?:resort|hotel|jw marriott)|historic (?:building|tower).{0,40}(?:hotel|resort|lodging)|(?:office|warehouse|building).{0,40}(?:conversion|converted|redevelopment).{0,30}(?:hotel|resort|jw marriott))\b/i,
  },
  // Construction start BEFORE mixed-use / proposal when groundbreaking language present
  {
    eventType: "Construction Start",
    re: /\b(breaks? ground|groundbreaking|ground broken|construction (?:starts?|begun|underway|begins?)|commences? construction|inicia(?:rá)? construcci[oó]n|come[nçc]a(?:r)?(?:á)? (?:a )?constru|primeira pedra|inicio das obras|início das obras)\b/i,
  },
  {
    eventType: "Construction Progress",
    re: /\b(tops? out|topped out|topping out|reaches? top[- ]?out|structural completion|construction milestone|construction reaches final floor|avance de construcci[oó]n|obra (?:avan[cç]a|conclu))\b/i,
  },
  // Brand/operator confirmed decisions before generic "planned hotel" proposal match
  {
    eventType: "Brand Signing",
    re: /\b((?:ihg|marriott|hilton|hyatt|accor|wyndham|choice|radisson)\s+signs?|(?:signs?|signed)\s+(?:a\s+)?(?:franchise|brand|affiliation)\s+agreement|(?:franchise|brand) agreement|contrato de franquicia|contrato de franquia)\b/i,
  },
  {
    eventType: "Operator Appointment",
    re: /\b(?:(?:appoints?|appointed|names?|named|selected|chosen)\s+(?:as\s+)?(?:operator|manager)|(?:operator|manager|management company)\s+(?:appointed|named|selected)|(?:appointed|selected|chosen)\s+to\s+(?:manage|operate)|(?:will|to)\s+(?:be\s+)?(?:operated|managed)\s+by|takes?\s+(?:over\s+)?management|operar[aá]\s+el\s+hotel|ser[aá]\s+operado\s+por|gestionar[aá]\s+el\s+hotel|administrar[aá]\s+el\s+hotel|operar[aá]\s+o\s+hotel|acordo de gest[aã]o hoteleira|acuerdo de gesti[oó]n hotelera)\b/i,
  },
  {
    eventType: "Commercialization",
    re: /\b(?:(?:reservations?|bookings?|reservaciones?|reservas)\s+(?:now\s+)?(?:open|opened|available)|(?:now|currently)\s+(?:accepting|taking)\s+(?:reservations?|bookings?)|accepting reservations?|now bookable|available to book|booking engine (?:open|live)|opens? reservations?|begins? accepting reservations?|abre reservas|acepta reservas|reservas abiertas|aceitando reservas)\b/i,
  },
  // High-confidence hotel/resort development announcements (before generic JV / Capital-ish JV)
  {
    eventType: "Development Proposal",
    re: /\b(?:(?:new\s+)?(?:hotel|resort|hotels|resorts)\s+announced|announc(?:e|es|ed|ing)\s+(?:a\s+)?(?:new\s+)?(?:hotel|resort|hotels|resorts)|announc(?:e|es|ed|ing)\s+plans?\s+for\s+(?:a\s+)?(?:new\s+)?(?:hotel|resort)|plans?\s+to\s+develop\s+(?:a\s+)?(?:new\s+)?(?:hotel|resort)|partners?\s+with\b[\s\S]{0,120}?\b(?:to\s+)?(?:develop|build|create)\b[\s\S]{0,80}?\b(?:hotel|resort)|joint\s+venture\b[\s\S]{0,140}?\b(?:develop|build|create|boutique\s+resort|hotel|resort)\b|developer\s+unveils?\s+(?:a\s+)?(?:new\s+)?(?:hotel|resort)|(?:new\s+)?hospitality\s+component|adds?\s+(?:a\s+)?hospitality\s+component|boutique\s+resort\s+within\s+(?:the\s+)?(?:development|project)|(?:hotel|resort)\s+component\s+of\s+(?:the\s+)?(?:mixed[- ]use\s+)?development)\b/i,
  },
  {
    eventType: "Development Proposal",
    re: /\b(?:proposed (?:hotel|resort)|planned (?:hotel|resort)|(?:new |future )?(?:hotel|resort) (?:project |development )?(?:proposed|planned|unveiled|announced)|developer (?:proposes|plans|unveils|announces) (?:a )?(?:\d+[-\s]?(?:room|key) )?(?:hotel|resort)|(?:hotel|resort) development planned|(?:mixed[- ]use).{0,60}(?:hotel|resort)|(?:includes?|including) (?:a )?(?:\d+[-\s]?(?:room|key) )?(?:hotel|resort)(?: component)?|(?:architect|design team|project manager|master planner).{0,40}(?:appointed|selected).{0,30}(?:hotel|resort)|(?:hotel|resort).{0,40}(?:architect|design team|developer|project manager|master planner).{0,20}(?:appointed|selected)|(?:seeking|seeks) .{0,50}(?:boutique )?hotel.{0,40}developer|(?:request for (?:qualifications|proposals)|RFQ|RFP).{0,40}hotel|nuevo (?:proyecto )?hotelero|projeto hoteleiro|empreendimento hoteleiro|desarrollo hotelero|hotel proyectado|hotel en proyecto)\b/i,
  },
  { eventType: "Hotel For Sale", re: /\b(for sale|offered for|hits the market|brought to market|marketing (?:the )?(?:hotel|property)|sale process|seeking (?:a )?buyer|on the market)\b/i },
  { eventType: "Distress", re: /\b(bankruptcy|foreclosure|distressed|receivership|chapter\s*11|default(?:ed)?|special servicer)\b/i },
  { eventType: "Portfolio Acquisition", re: /\b(portfolio (?:acquisition|purchase|sale|deal)|acquires? \d+ (?:hotels?|properties)|buys? \d+[- ]hotel)\b/i },
  { eventType: "JV", re: /\b(joint venture|\bJV\b|co-?develop(?:ment)?|partnership (?:to |for )(?:develop|acquire|own)|equity partner|capital partner)\b/i },
  { eventType: "Recapitalization", re: /\b(recapitali[sz]ation|recap\b|capital restructuring)\b/i },
  { eventType: "Refinancing", re: /\b(refinanc(?:e|ing|ed))\b/i },
  {
    eventType: "Financing",
    re: /\b(?:(?:credit approval|development (?:facility|capital|funding|loan)|construction (?:loan|financing|debt)|debt package|project (?:loan|financing)|mezzanine(?: financing)?|senior loan|equity financing|joint venture funding).{0,70}(?:hotel|resort)|(?:hotel|resort).{0,70}(?:credit approval|construction (?:loan|financing|debt)|development (?:financing|loan|funding|capital|facility)|project (?:loan|financing)|debt package|mezzanine|senior loan)|(?:financiamiento|financia[cç][aã]o|cr[eé]dito|pr[eé]stamo|empr[eé]stimo).{0,40}(?:hotel|hotelero|hoteleiro|construcci[oó]n|construção|desarrollo|desenvolvimento))\b/i,
  },
  { eventType: "Financing", re: /\b(secur(?:es|ed|ing) (?:\$|USD)?[\d.,]+\s*(?:m|mm|million|billion)?(?:\s+(?:in )?(?:financing|debt|loan|mortgage))?|(?:financing|loan|debt|mortgage) (?:of |for )(?:\$|USD)?[\d.,]+|closes? (?:\$|USD)?[\d.,]+\s*(?:m|mm|million).*(?:loan|financing|debt)|financing secured|debt secured|lender (?:provides?|closes?) (?:financing|loan))\b/i },
  { eventType: "Reflag", re: /\b(reflag(?:ged|ging)?|re-?brand(?:ed|ing)?|will (?:now )?operate as|to (?:be )?(?:flagged|branded) as)\b/i },
  { eventType: "Brand Exit", re: /\b((?:exits?|leaving|drops?) (?:the )?(?:brand|flag)|de-?flag(?:ged|ging)?|ends? (?:its )?affiliation|terminat(?:es|ed|ing) (?:the )?franchise)\b/i },
  { eventType: "Brand Signing", re: /\b((?:ihg|marriott|hilton|hyatt|accor|wyndham|choice|radisson)\s+signs?|signs?\s+(?:the\s+)?(?:noted collection|voco|autograph|tapestry|curio|tribute|hotel|resort|property))\b/i },
  { eventType: "Brand Signing", re: /\b((?:signs?|signed|announces?) (?:a )?(?:franchise|brand|affiliation)|joins? (?:the )?(?:marriott|hilton|ihg|hyatt|wyndham|choice|radisson)|new (?:marriott|hilton|ihg|hyatt) (?:hotel|property))\b/i },
  { eventType: "Conversion", re: /\b(conversion|adaptive reuse|convert(?:s|ed|ing)? (?:to|into) (?:a )?(?:hotel|resort))\b/i },
  { eventType: "Operator Exit", re: /\b(operator (?:exits?|exit|departs?|departure)|ends? (?:its )?management|management (?:contract )?(?:terminated|ends?))\b/i },
  { eventType: "Operator Change", re: /\b(operator (?:change|switch|replaced)|new (?:third[- ]party )?operator|changes? (?:hotel )?operator)\b/i },
  // Operator Appointment already matched earlier for primary variants; keep fallback
  {
    eventType: "Operator Appointment",
    re: /\b(?:appoints?|appointed|names?|named)\s+(?:as\s+)?(?:operator|manager)\b/i,
  },
  { eventType: "Management Agreement", re: /\b(management agreement|management contract|third[- ]party management|to bring (?:the )?(?:voco|hotel)|brings? (?:the )?(?:voco|hotel))\b/i },
  { eventType: "Major Renovation", re: /\b(major renov(?:ation|ate)|renovation program|capex (?:program|project)|\$[\d.,]+\s*(?:m|mm|million).*(?:renovat|refurb)|refurbish(?:ment|es|ed)?)\b/i },
  { eventType: "Repositioning", re: /\b(reposition(?:ing|ed|s)?|product reposition)\b/i },
  { eventType: "Acquisition", re: /\b(acquires?|acquired|acquisition|buys?|purchased?|purchase of)\b/i },
  { eventType: "Sale", re: /\b(sells?|sold|sale of|closes? sale|transaction clos(?:es|ed))\b/i },
  // New development with optional room/key counts between "new" and "hotel"
  {
    eventType: "New Development",
    re: /\b(?:new (?:\d+[-\s]?(?:room|key)s?\s+)?(?:hotel|resort) (?:development|project|planned)|(?:announces?|announced|plans?|planned|unveils?|unveiled) (?:a )?(?:new )?(?:\d+[-\s]?(?:room|key)s?\s+)?(?:hotel|resort)(?: development|project)?|develop(?:s|ing|ment of) (?:a )?(?:new )?(?:\d+[-\s]?(?:room|key)s?\s+)?(?:hotel|resort)|hotel (?:pipeline|project) announced|(?:\d+[-\s]?(?:room|key)s?\s+)?(?:mixed[- ]use\s+)?(?:hotel|resort) (?:project|development)|developer unveils (?:\d+[-\s]?(?:room|key)s?\s+)?(?:hotel|resort)|planned (?:\d+[-\s]?(?:room|key)s?\s+)?(?:hotel|resort)|proposed (?:\d+[-\s]?(?:room|key)s?\s+)?(?:hotel|resort))\b/i,
  },
];

/**
 * @param {{ title?: string, summary?: string }} input
 * @returns {{ eventType: string|null, whatChanged: string|null, matched: boolean, negated?: boolean, negationReason?: string }}
 */
export function inferMarketAlertEvent(input = {}) {
  const title = input.title || "";
  const summary = input.summary || "";
  const text = `${title} ${summary}`.trim();
  if (!text) {
    return { eventType: null, whatChanged: null, matched: false, negated: false };
  }

  // Explicit pre-opening leadership carve-out (role + context helpers).
  if (isPreOpeningLeadershipNews(title, summary)) {
    return {
      eventType: "Pre-Opening Leadership",
      whatChanged: EVENT_WHAT_CHANGED["Pre-Opening Leadership"],
      matched: true,
      negated: false,
    };
  }

  // Hotel/resort development announcements beat generic JV/Capital classification.
  if (isHotelDevelopmentAnnouncement(text)) {
    const negation = detectTransactionNegation(text, "Development Proposal");
    if (!negation.negated) {
      return {
        eventType: "Development Proposal",
        whatChanged: EVENT_WHAT_CHANGED["Development Proposal"],
        matched: true,
        negated: false,
      };
    }
  }

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
      // Environmental / planning approvals require hospitality context.
      if (
        (rule.eventType === "Planning Approval" || rule.eventType === "Planning Application") &&
        /environmental|coastal|EIA|licencia ambiental|licença ambiental|shoreline/i.test(text) &&
        !HOTEL_CTX_RE.test(text)
      ) {
        continue;
      }

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
        // Do not treat hospitality site/land control as ordinary asset Acquisition.
        if (
          rule.eventType === "Acquisition" &&
          /\b(site|parcel|land|plot|terreno|predio|ground lease)\b/i.test(text) &&
          HOTEL_CTX_RE.test(text)
        ) {
          continue;
        }
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
