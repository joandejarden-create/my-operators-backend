/**
 * V2 quality gate for structured extracted entities (before hotel matching).
 */

import { isPlausibleOrganizationName } from "./quality-gate.js";
import { SOURCE_TYPE } from "./v2-constants.js";

const GENERIC_NOUNS = new Set(
  [
    "exhibitor",
    "exhibitors",
    "sponsor",
    "sponsors",
    "partner",
    "partners",
    "gold",
    "silver",
    "bronze",
    "platinum",
    "venue",
    "hotel",
    "hotels",
    "conference",
    "convention",
    "agenda",
    "program",
    "registration",
    "housing",
    "contact",
    "about",
    "home",
    "menu",
    "login",
    "search",
    "filter",
    "category",
    "categories",
    "booth",
    "hall",
    "floor",
    "map",
    "download",
    "pdf",
    "new york",
    "nyc",
    "manhattan",
    "times square",
    "javits",
    "united states",
    "usa",
    "application",
    "exhibitor application",
    "exhibitor list",
    "exhibitor lists",
    "webinar",
    "webinar series",
    "master copy",
    "kansas city",
    "how to",
    "database",
    "success",
    "solutions",
  ].map((s) => s.toLowerCase())
);

const JUNK_PHRASE_RE =
  /\b(how to|application|webinar|master copy|exhibitor list|exhibitor lists|global exhibitor|sponsor program|elderly housing|community housing|housing committee|manage the paper|click here|learn more|view more|download|table of contents|page \d+)\b/i;

const CITY_ONLY_RE =
  /^(kansas city|new york|chicago|boston|los angeles|san francisco|miami|dallas|houston|atlanta|denver|seattle|philadelphia|washington|orlando|las vegas)\b/i;

/**
 * Reject before hotel matching.
 */
export function passesStructuredEntityQualityGate(entity = {}) {
  const name = String(entity.entityName || "").trim();
  if (!isPlausibleOrganizationName(name)) {
    return { ok: false, reason: "IMPLAUSIBLE_ORG" };
  }
  const low = name.toLowerCase();
  if (GENERIC_NOUNS.has(low)) {
    return { ok: false, reason: "GENERIC_NOUN" };
  }
  if (JUNK_PHRASE_RE.test(name)) {
    return { ok: false, reason: "JUNK_PHRASE" };
  }
  if (CITY_ONLY_RE.test(name) && name.split(/\s+/).length <= 3) {
    return { ok: false, reason: "CITY_ONLY" };
  }
  if (/^(gold|silver|bronze|platinum)\s+(sponsor|partner)/i.test(name)) {
    return { ok: false, reason: "SPONSOR_CATEGORY" };
  }
  if (/desk|persona|noreply|donotreply/i.test(name)) {
    return { ok: false, reason: "DESK_PERSONA" };
  }
  // Person-name-only (First Last) without company markers — too noisy from PDFs
  if (/^[A-Z][a-z]+\s+[A-Z][a-z]+$/.test(name) && !/\b(Inc|LLC|Corp|Group|Ltd|Company|Systems|Solutions|Hotel|Hotels)\b/i.test(name)) {
    return { ok: false, reason: "PERSON_NAME_ONLY" };
  }
  // Venue-only / event-title-only
  if (/^(the\s+)?(javits|jacob\s+javits|times\s+square|midtown\s+manhattan)\b/i.test(name) && name.split(/\s+/).length <= 4) {
    return { ok: false, reason: "VENUE_ONLY" };
  }
  if (/annual\s+(meeting|conference|convention|expo)\b/i.test(name) && !entity.participationRole) {
    return { ok: false, reason: "EVENT_TITLE_ONLY" };
  }
  // Need participation evidence from structured source OR explicit role
  const structured =
    entity.sourceType &&
    entity.sourceType !== SOURCE_TYPE.GENERIC_SERP;
  if (!structured && !entity.participationRole) {
    return { ok: false, reason: "NO_PARTICIPATION_EVIDENCE" };
  }
  if (entity.futureTiming === false) {
    return { ok: false, reason: "NO_FUTURE_TIMING" };
  }
  if (entity.sourceURL && /w3\.org|\/svg|:style=/i.test(entity.sourceURL)) {
    return { ok: false, reason: "JUNK_SOURCE" };
  }
  // Prefer org-like: Inc/LLC/Corp/Group OR 3+ tokens OR known participation from directory
  const tokens = name.split(/\s+/).filter(Boolean);
  const hasCorpMarker = /\b(Inc\.?|LLC|Ltd\.?|Corp\.?|Corporation|Company|Group|Systems|Solutions|Partners|International|Holdings)\b/i.test(
    name
  );
  const fromDirectory =
    entity.sourceType === SOURCE_TYPE.EXHIBITOR_DIRECTORY ||
    entity.sourceType === SOURCE_TYPE.SPONSOR_DIRECTORY;
  if (!hasCorpMarker && tokens.length < 2) {
    return { ok: false, reason: "TOO_SHORT" };
  }
  if (!fromDirectory && !hasCorpMarker && tokens.length < 3 && entity.confidence < 0.7) {
    return { ok: false, reason: "LOW_CONFIDENCE_ORG" };
  }
  return { ok: true, reason: null };
}
