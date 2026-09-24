/**
 * Content-quality gates for Market Alerts V1.3 + Pre-Opening Capture V1.
 * Personnel + consumer noise. Narrow pre-opening leadership carve-out.
 */
import { assessConsumerFluff } from "./market-alerts-consumer-fluff.js";

const PROPERTY_PERSONNEL_RE =
  /\b(appoints?|appointed|names? as|named as|welcomes?|joins?(?:\s+\w+){0,6}\s+as|takes? (?:on|over) as)\b.{0,100}\b(director of food(?:\s*&\s*|\s+and\s+)?beverage|director of f\s*&\s*b|f\s*&\s*b director|food(?:\s*&\s*|\s+and\s+)?beverage manager|executive chef|(?:head|group)\s+chef|(?:senior\s+)?sous chef|chef de cuisine|director of housekeeping|executive housekeeper|director of rooms|rooms director|director of front office|front office manager|director of sales(?:\s+and\s+marketing)?|sales manager|marketing manager|revenue manager|director of revenue|spa director|director of spa|director of human resources|hr director|director of operations|director of services|general manager|managing director|lodge manager|gm\b)\b/i;

const PROPERTY_PERSONNEL_TITLE_RE =
  /\b(director of food(?:\s*&\s*|\s+and\s+)?beverage|director of f\s*&\s*b|f\s*&\s*b director|executive chef|(?:head|group)\s+chef|director of housekeeping|director of rooms|director of front office|front office manager|revenue manager|spa director|director of human resources|director of sales(?:\s+and\s+marketing)?|sales manager|marketing manager)\b/i;

const STRATEGIC_PERSONNEL_RE =
  /\b(ceo|chief executive officer|president|chair(?:man|woman|person)?|founder|co-founder|owner|chief development officer|cdo\b|chief investment officer|cio\b|chief strategy officer|cso\b|head of investments|head of real estate|head of development|evp|evp of development|svp of development|development president|managing partner)\b/i;

const STRATEGIC_PERSONNEL_CONTEXT_RE =
  /\b(development|investment|real estate|expansion|growth strategy|portfolio|m\s*&\s*a|merger|acquisition strategy|corporate|group|company|brand|operator|hospitality group)\b/i;

const CONSUMER_PROMOTIONAL_RE = [
  /\$\d+\s+hotel rooms?\b/i,
  /\b(discounted stays?|promotional rates?|travel deals?|weekend guide|weekend smiles|where to stay|best hotels? (?:near|in|to|for)|travel tips?|hotel packages?|sweepstakes|room deals?|hotel room deals?)\b/i,
  /\b(saturday smiles|local entertainment roundup|consumer destination guide|teachers[''] lounge makeover)\b/i,
  /\b(new brewery opening|new pro sports team)\b.{0,80}\b(hotel|rooms?)\b/i,
];

const HOTEL_TRANSACTION_KEEP_RE =
  /\b(hotel|resort|property|hospitality asset)\b.{0,80}\b(for sale|listed for sale|acquisition|acquired|sold|sale|portfolio|transaction|financing|development|planning|construction)\b/i;

/** Explicit pre-opening / new-opening context (EN + ES + PT). */
export const PRE_OPENING_CONTEXT_RE =
  /\b(?:pre[- ]?opening|preopening|preapertura|pre[- ]?apertura|pré[- ]?abertura|pre[- ]?abertura|new opening|new hotel|nuevo hotel|novo hotel|opening team|equipo de (?:pre)?apertura|equipe de abertura|debut|soft opening|grand opening|opening in\s+20\d{2}|opens? in\s+20\d{2})\b/i;

/** Senior opening-team roles (EN + ES + PT). */
export const PRE_OPENING_ROLE_RE =
  /\b(?:(?:pre[- ]?opening|preopening|opening)\s+(?:general manager|gm|director of sales(?:\s+and\s+marketing)?|dosm|director of revenue|revenue director|team)|general manager[, ]+(?:new opening|new hotel|pre[- ]?opening)|(?:gm|dosm)\s+(?:pre[- ]?opening|opening)|gerente general(?:\s+(?:pre[- ]?apertura|preapertura|nuevo hotel))?|director(?:\s+de)?\s+(?:ventas|comercial)\s+(?:pre[- ]?apertura|preapertura)|gerente geral(?:\s+(?:pré[- ]?abertura|pre[- ]?abertura|novo hotel))?|diretor de vendas\s+(?:pré[- ]?abertura|pre[- ]?abertura)|(?:opening|pre[- ]?opening)\s+team|equipo de (?:pre)?apertura|equipe de abertura)\b/i;

const HOTEL_HOSPITALITY_RE =
  /\b(hotel|resort|hospitality|lodging|nuevo hotel|novo hotel|resort)\b/i;

/**
 * Narrow carve-out: pre-opening leadership is NOT low-value personnel.
 * @param {string} title
 * @param {string} [summary]
 */
export function isPreOpeningLeadershipNews(title = "", summary = "") {
  const text = `${title} ${summary}`.trim();
  if (!text) return false;
  if (!HOTEL_HOSPITALITY_RE.test(text) && !PRE_OPENING_CONTEXT_RE.test(text)) return false;
  if (!PRE_OPENING_CONTEXT_RE.test(text)) return false;
  if (!PRE_OPENING_ROLE_RE.test(text)) return false;
  return true;
}

/**
 * @param {string} title
 * @param {string} [summary]
 * @returns {boolean}
 */
export function isLowValuePersonnelNews(title = "", summary = "") {
  const text = `${title} ${summary}`.trim();
  if (!text) return false;

  if (isPreOpeningLeadershipNews(title, summary)) {
    return false;
  }

  if (STRATEGIC_PERSONNEL_RE.test(text) && STRATEGIC_PERSONNEL_CONTEXT_RE.test(text)) {
    return false;
  }

  if (PROPERTY_PERSONNEL_RE.test(text)) return true;
  if (PROPERTY_PERSONNEL_TITLE_RE.test(text) && /\b(appoints?|appointed|names? as|named as)\b/i.test(text)) {
    return true;
  }

  return false;
}

/**
 * @param {string} title
 * @param {string} [summary]
 * @returns {boolean}
 */
export function isConsumerPromotionalNoise(title = "", summary = "") {
  const text = `${title} ${summary}`.trim();
  if (!text) return false;

  if (HOTEL_TRANSACTION_KEEP_RE.test(text)) return false;
  if (/\b(listed for sale|for sale|acquisition|sold|portfolio sale|hotel sale)\b/i.test(text)) {
    return false;
  }

  for (const re of CONSUMER_PROMOTIONAL_RE) {
    if (re.test(text)) return true;
  }

  return false;
}

/**
 * @param {{ title?: string, summary?: string }} input
 * @returns {{ ignore: boolean, reason?: string }}
 */
export function assessContentQuality(input = {}) {
  const title = String(input.title || "").trim();
  const summary = String(input.summary || "").trim();

  const fluff = assessConsumerFluff({ title, summary });
  if (fluff.reject) {
    return { ignore: true, reason: fluff.reason };
  }

  if (isLowValuePersonnelNews(title, summary)) {
    return { ignore: true, reason: "low_value_personnel" };
  }
  if (isConsumerPromotionalNoise(title, summary)) {
    return { ignore: true, reason: "consumer_promotional" };
  }

  return { ignore: false };
}

/** Regression headline — Patch consumer roundup. */
export const PATCH_CONSUMER_HEADLINE =
  "$10 Hotel Rooms + New Brewery Opening + New Pro Sports Team + Teachers' Lounge Makeover: Saturday Smiles - Patch";

/** Regression headline — Novotel F&B appointment. */
export const NOVOTEL_FB_HEADLINE =
  "Novotel Hyderabad Airport appoints Jitendra Singh as director of food & beverage";
