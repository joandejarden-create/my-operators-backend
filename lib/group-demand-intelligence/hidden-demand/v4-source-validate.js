/**
 * V4 geo-first / timing / richness source validation — before entity extraction.
 */

import { createHash } from "node:crypto";
import { classifyStructuredSource } from "./source-classifier.js";
import { SOURCE_TYPE } from "./v2-constants.js";
import {
  SOURCE_STATUS,
  PARTICIPANT_RICHNESS,
  SOURCE_FAMILY,
  DEEP_PARSE_RICHNESS,
} from "./v4-constants.js";

const NYC_RE =
  /\b(new\s*york|nyc|manhattan|midtown|times\s*square|javits|jacob\s*k?\.?\s*javits|hudson\s*yards|broadway|hell'?s\s*kitchen|murray\s*hill)\b/i;

const VENUE_NYC_RE =
  /\b(javits|jacob\s*k?\.?\s*javits|times\s*square|madison\s*square\s*garden|msg|pier\s*9[24]|glasshouse|hudson\s*yards|radio\s*city)\b/i;

const NON_NYC_RE =
  /\b(istanbul|london|paris|berlin|dubai|singapore|hong\s*kong|tokyo|shanghai|beijing|mumbai|delhi|sydney|melbourne|toronto|montreal|vancouver|mexico\s*city|são\s*paulo|sao\s*paulo|las\s*vegas|orlando|miami|chicago|dallas|houston|atlanta|los\s*angeles|san\s*francisco|seattle|boston|washington\s*d\.?c\.?|philadelphia|denver|phoenix)\b/i;

const STALE_YEAR_RE = /\b(20(1[0-9]|2[0-4]))\b/;
const FUTURE_YEAR_RE = /\b(202[6-9]|203[0-5])\b/;
const CURRENT_YEAR = new Date().getUTCFullYear();

const UI_CHROME_RE =
  /\b(exhibitor application|interested in exhibiting|become an exhibitor|why exhibit|how to exhibit|register now|sponsor opportunities|floor plan|contact us|about us|login|sign up|powered by|scan exhibitors|supporters?\s*&\s*sponsors?)\b/i;

const CALENDAR_RE =
  /\b(events?\s+calendar|upcoming\s+events|what'?s\s+on|event\s+listing|citywide\s+events|things\s+to\s+do)\b/i;

const RICH_RE =
  /\b(exhibitor\s+(list|directory|guide)|who's\s+exhibiting|booth\s*(#|number|list)|sponsor\s+(list|directory)|participant\s+list|vendor\s+roster|floor\s*plan\s+directory|named\s+exhibitors)\b/i;

const MEDIUM_RE =
  /\b(exhibitors?|sponsors?|participants?|delegates?|crew|vendors?|companies\s+exhibiting)\b/i;

const HOUSING_RE =
  /\b(official\s+housing|exhibitor\s+housing|hotel\s*block|room\s*block|group\s+lodging|book\s+your\s+hotel|accommodation)\b/i;

/**
 * Compute stable source id from URL.
 */
export function computeSourceId(url = "") {
  const h = createHash("sha256").update(String(url || "").toLowerCase()).digest("hex").slice(0, 14);
  return `hdsrc_${h}`;
}

/**
 * Infer source family from hit + type.
 */
export function inferSourceFamily(hit = {}, sourceType = "") {
  const blob = `${hit.url || ""} ${hit.title || ""} ${hit.snippet || ""} ${hit.family || ""}`.toLowerCase();
  if (HOUSING_RE.test(blob) || sourceType === SOURCE_TYPE.HOUSING_PDF) {
    return SOURCE_FAMILY.EXHIBITOR_HOUSING;
  }
  if (/javits|nrf|ny\s*now|ife|franchise\s+expo|a2zinc|expocad/i.test(blob)) {
    return SOURCE_FAMILY.JAVITS_EXHIBITOR;
  }
  if (/tour|itinerary|departure|escorted|affinity|alumni\s+travel/i.test(blob)) {
    return SOURCE_FAMILY.TOUR_SERIES;
  }
  if (/mba|university|student|performing\s+arts|residency|academic/i.test(blob)) {
    return SOURCE_FAMILY.EDUCATION;
  }
  if (/trade\s+mission|delegation|buying\s+group/i.test(blob)) {
    return SOURCE_FAMILY.DELEGATION;
  }
  if (/production|av\s+|staging|booth\s+builder|freight|installation|experiential|activation|agency/i.test(blob)) {
    if (/agency|experiential|activation/i.test(blob)) return SOURCE_FAMILY.ENTERTAINMENT_PRODUCTION;
    return SOURCE_FAMILY.EVENT_SERVICE;
  }
  if (/tournament|college\s+team|officials|sports\s+media/i.test(blob)) {
    return SOURCE_FAMILY.SPORTS_ADJACENT;
  }
  if (
    sourceType === SOURCE_TYPE.EXHIBITOR_DIRECTORY ||
    sourceType === SOURCE_TYPE.SPONSOR_DIRECTORY
  ) {
    return SOURCE_FAMILY.JAVITS_EXHIBITOR;
  }
  return SOURCE_FAMILY.OTHER;
}

/**
 * Geography from candidate fields — prefer page text when provided.
 * SERP title alone is insufficient for VALID; needs corroboration.
 */
export function validateSourceGeography({
  title = "",
  snippet = "",
  url = "",
  pageText = "",
  venue = "",
} = {}) {
  const officialBlob = `${pageText} ${venue}`.slice(0, 20000);
  const serpBlob = `${title} ${snippet} ${url}`;

  // Page / venue evidence preferred
  if (officialBlob.length > 80) {
    if (NON_NYC_RE.test(officialBlob) && !NYC_RE.test(officialBlob)) {
      return {
        ok: false,
        status: SOURCE_STATUS.WRONG_GEOGRAPHY,
        geography: "NON_NYC",
        reason: "page_non_nyc",
        evidenceStrength: "PAGE",
      };
    }
    if (NYC_RE.test(officialBlob) || VENUE_NYC_RE.test(officialBlob)) {
      return {
        ok: true,
        status: null,
        geography: "NYC_MIDTOWN",
        reason: VENUE_NYC_RE.test(officialBlob) ? "venue_nyc" : "page_nyc",
        evidenceStrength: "PAGE",
      };
    }
  }

  // SERP-only: can reject clear wrong geo; cannot confirm VALID alone
  if (NON_NYC_RE.test(serpBlob) && !NYC_RE.test(serpBlob)) {
    return {
      ok: false,
      status: SOURCE_STATUS.WRONG_GEOGRAPHY,
      geography: "NON_NYC",
      reason: "serp_non_nyc",
      evidenceStrength: "SERP",
    };
  }
  if (NYC_RE.test(serpBlob) || VENUE_NYC_RE.test(serpBlob)) {
    return {
      ok: null, // provisional — need page confirm
      status: null,
      geography: "NYC_PROVISIONAL",
      reason: "serp_nyc_keyword",
      evidenceStrength: "SERP",
    };
  }
  return {
    ok: false,
    status: SOURCE_STATUS.UNKNOWN,
    geography: "UNKNOWN",
    reason: "no_nyc_evidence",
    evidenceStrength: "NONE",
  };
}

/**
 * Future timing from candidate / page.
 */
export function validateSourceTiming({
  title = "",
  snippet = "",
  pageText = "",
  yearHint = null,
} = {}) {
  const blob = `${title} ${snippet} ${pageText}`.slice(0, 30000);
  const years = [...blob.matchAll(/\b(20[1-3]\d)\b/g)].map((m) => Number(m[1]));
  const futureYears = years.filter((y) => y >= CURRENT_YEAR);
  const pastOnly = years.length > 0 && futureYears.length === 0 && years.every((y) => y < CURRENT_YEAR);

  if (pastOnly && !/registration\s+open|upcoming|next\s+edition|202[6-9]/i.test(blob)) {
    return {
      ok: false,
      status: SOURCE_STATUS.STALE,
      reason: "past_years_only",
      year: Math.max(...years),
    };
  }
  if (yearHint && Number(yearHint) >= CURRENT_YEAR) {
    return { ok: true, reason: "year_hint", year: Number(yearHint) };
  }
  if (futureYears.length) {
    return { ok: true, reason: "future_year_in_text", year: Math.max(...futureYears) };
  }
  if (/registration\s+open|exhibitors?\s+202[6-9]|upcoming\s+show|next\s+edition/i.test(blob)) {
    return { ok: true, reason: "registration_or_upcoming", year: CURRENT_YEAR + 1 };
  }
  if (!years.length && /exhibitor|sponsor|directory|housing/i.test(blob)) {
    // Undated but structured — provisional allow with low confidence
    return { ok: true, reason: "undated_structured_provisional", year: null, provisional: true };
  }
  if (!years.length) {
    return { ok: false, status: SOURCE_STATUS.STALE, reason: "undated_no_structure", year: null };
  }
  return { ok: true, reason: "has_years", year: futureYears[0] || years[0] };
}

/**
 * Participant richness score from SERP or page.
 */
export function scoreParticipantRichness({
  title = "",
  snippet = "",
  url = "",
  pageText = "",
  sourceType = "",
} = {}) {
  const blob = `${title} ${snippet} ${url} ${pageText.slice(0, 8000)}`;
  if (UI_CHROME_RE.test(blob) && !RICH_RE.test(blob)) {
    return { richness: PARTICIPANT_RICHNESS.NONE, reason: "ui_chrome" };
  }
  if (CALENDAR_RE.test(blob) && !RICH_RE.test(blob)) {
    return { richness: PARTICIPANT_RICHNESS.NONE, reason: "generic_calendar" };
  }
  if (RICH_RE.test(blob) || HOUSING_RE.test(blob)) {
    return { richness: PARTICIPANT_RICHNESS.HIGH, reason: "directory_or_housing_language" };
  }
  if (
    sourceType === SOURCE_TYPE.EXHIBITOR_DIRECTORY ||
    sourceType === SOURCE_TYPE.SPONSOR_DIRECTORY ||
    sourceType === SOURCE_TYPE.PARTICIPANT_LIST ||
    sourceType === SOURCE_TYPE.HOUSING_PDF
  ) {
    return { richness: PARTICIPANT_RICHNESS.HIGH, reason: "structured_source_type" };
  }
  if (MEDIUM_RE.test(blob)) {
    return { richness: PARTICIPANT_RICHNESS.MEDIUM, reason: "participant_keywords" };
  }
  if (/prospectus|exhibitor\s+kit|sponsor\s+kit/i.test(blob) && !/list|directory|who's/i.test(blob)) {
    return { richness: PARTICIPANT_RICHNESS.LOW, reason: "prospectus_no_list" };
  }
  return { richness: PARTICIPANT_RICHNESS.LOW, reason: "insufficient_participant_signal" };
}

/**
 * Classify a SERP hit into a source candidate — no fetch yet.
 */
export function classifySourceCandidate(hit = {}, opts = {}) {
  const url = hit.url || hit.link || "";
  const title = hit.title || "";
  const snippet = hit.snippet || "";
  const sourceType = classifyStructuredSource({ url, title, snippet });
  const family = hit.family || inferSourceFamily({ url, title, snippet, family: hit.family }, sourceType);
  const geo = validateSourceGeography({ title, snippet, url });
  const timing = validateSourceTiming({ title, snippet, yearHint: opts.year });
  const rich = scoreParticipantRichness({ title, snippet, url, sourceType });

  let status = SOURCE_STATUS.UNKNOWN;
  const rejectReasons = [];

  if (geo.status === SOURCE_STATUS.WRONG_GEOGRAPHY) {
    status = SOURCE_STATUS.WRONG_GEOGRAPHY;
    rejectReasons.push(geo.reason);
  } else if (UI_CHROME_RE.test(`${title} ${snippet}`) && rich.richness === PARTICIPANT_RICHNESS.NONE) {
    status = SOURCE_STATUS.UI_CHROME;
    rejectReasons.push("ui_chrome");
  } else if (CALENDAR_RE.test(`${title} ${snippet}`) && rich.richness === PARTICIPANT_RICHNESS.NONE) {
    status = SOURCE_STATUS.GENERIC_CALENDAR;
    rejectReasons.push("generic_calendar");
  } else if (
    sourceType === SOURCE_TYPE.PROGRAM_PDF &&
    rich.richness === PARTICIPANT_RICHNESS.LOW &&
    !HOUSING_RE.test(`${title} ${snippet}`)
  ) {
    status = SOURCE_STATUS.PDF_NOISE;
    rejectReasons.push("program_pdf_low_richness");
  } else if (timing.status === SOURCE_STATUS.STALE && timing.ok === false) {
    status = SOURCE_STATUS.STALE;
    rejectReasons.push(timing.reason);
  } else if (rich.richness === PARTICIPANT_RICHNESS.NONE) {
    status = SOURCE_STATUS.IRRELEVANT;
    rejectReasons.push(rich.reason);
  } else if (geo.geography === "NYC_PROVISIONAL" || geo.geography === "NYC_MIDTOWN") {
    if (DEEP_PARSE_RICHNESS.includes(rich.richness)) {
      status =
        sourceType === SOURCE_TYPE.GENERIC_SERP || sourceType === SOURCE_TYPE.PROGRAM_PAGE
          ? SOURCE_STATUS.VALID_NYC_UNSTRUCTURED
          : SOURCE_STATUS.VALID_NYC_STRUCTURED;
    } else {
      status = SOURCE_STATUS.IRRELEVANT;
      rejectReasons.push("low_richness");
    }
  } else {
    status = SOURCE_STATUS.UNKNOWN;
    rejectReasons.push("geo_unknown");
  }

  const mayFetch =
    status === SOURCE_STATUS.VALID_NYC_STRUCTURED ||
    status === SOURCE_STATUS.VALID_NYC_UNSTRUCTURED;

  return {
    sourceId: computeSourceId(url),
    sourceURL: url,
    title,
    snippet,
    sourceType,
    family,
    status,
    mayFetch,
    geography: geo.geography,
    geoReason: geo.reason,
    geoEvidenceStrength: geo.evidenceStrength,
    timingOk: timing.ok,
    timingYear: timing.year,
    timingReason: timing.reason,
    richness: rich.richness,
    richnessReason: rich.reason,
    rejectReasons,
    requiresPDF: /\.pdf($|\?)/i.test(url) || sourceType.includes("PDF"),
    requiresRender: /a2zinc|expocad|mapyourshow|map.?your.?show/i.test(url),
    validatedAt: null,
    confidence: mayFetch ? (geo.evidenceStrength === "PAGE" ? 0.8 : 0.45) : 0.2,
  };
}

/**
 * Re-validate after fetching page text — geography must confirm from page.
 */
export function confirmSourceAfterFetch(candidate = {}, pageText = "", venue = "") {
  const geo = validateSourceGeography({
    title: candidate.title,
    snippet: candidate.snippet,
    url: candidate.sourceURL,
    pageText,
    venue,
  });
  const timing = validateSourceTiming({
    title: candidate.title,
    snippet: candidate.snippet,
    pageText,
    yearHint: candidate.timingYear,
  });
  const rich = scoreParticipantRichness({
    title: candidate.title,
    snippet: candidate.snippet,
    url: candidate.sourceURL,
    pageText,
    sourceType: candidate.sourceType,
  });

  if (geo.status === SOURCE_STATUS.WRONG_GEOGRAPHY || geo.ok === false) {
    return {
      ...candidate,
      status: SOURCE_STATUS.WRONG_GEOGRAPHY,
      mayFetch: false,
      validatedAt: new Date().toISOString(),
      geography: geo.geography,
      geoReason: geo.reason,
      geoEvidenceStrength: geo.evidenceStrength,
      confidence: 0.9,
      rejectReasons: [...(candidate.rejectReasons || []), geo.reason],
    };
  }
  if (geo.ok !== true && geo.geography !== "NYC_MIDTOWN") {
    // Still only SERP provisional after fetch with no NYC on page
    if (!NYC_RE.test(pageText) && !VENUE_NYC_RE.test(pageText)) {
      return {
        ...candidate,
        status: SOURCE_STATUS.WRONG_GEOGRAPHY,
        mayFetch: false,
        validatedAt: new Date().toISOString(),
        geography: "UNKNOWN",
        geoReason: "page_lacks_nyc_confirm",
        confidence: 0.85,
        rejectReasons: [...(candidate.rejectReasons || []), "page_lacks_nyc_confirm"],
      };
    }
  }
  if (timing.ok === false) {
    return {
      ...candidate,
      status: SOURCE_STATUS.STALE,
      mayFetch: false,
      validatedAt: new Date().toISOString(),
      timingOk: false,
      timingReason: timing.reason,
      rejectReasons: [...(candidate.rejectReasons || []), timing.reason],
    };
  }
  if (!DEEP_PARSE_RICHNESS.includes(rich.richness)) {
    return {
      ...candidate,
      status: SOURCE_STATUS.IRRELEVANT,
      mayFetch: false,
      validatedAt: new Date().toISOString(),
      richness: rich.richness,
      richnessReason: rich.reason,
      rejectReasons: [...(candidate.rejectReasons || []), "post_fetch_low_richness"],
    };
  }

  const structured =
    candidate.sourceType === SOURCE_TYPE.EXHIBITOR_DIRECTORY ||
    candidate.sourceType === SOURCE_TYPE.SPONSOR_DIRECTORY ||
    candidate.sourceType === SOURCE_TYPE.PARTICIPANT_LIST ||
    candidate.sourceType === SOURCE_TYPE.HOUSING_PDF ||
    RICH_RE.test(pageText.slice(0, 5000));

  return {
    ...candidate,
    status: structured
      ? SOURCE_STATUS.VALID_NYC_STRUCTURED
      : SOURCE_STATUS.VALID_NYC_UNSTRUCTURED,
    mayFetch: true,
    validatedAt: new Date().toISOString(),
    geography: "NYC_MIDTOWN",
    geoReason: geo.reason || "page_confirmed_nyc",
    geoEvidenceStrength: "PAGE",
    timingOk: true,
    timingYear: timing.year || candidate.timingYear,
    timingReason: timing.reason,
    richness: rich.richness,
    richnessReason: rich.reason,
    confidence: 0.85,
    pageTextLen: pageText.length,
  };
}

export function shouldDeepParse(candidate = {}) {
  return (
    (candidate.status === SOURCE_STATUS.VALID_NYC_STRUCTURED ||
      candidate.status === SOURCE_STATUS.VALID_NYC_UNSTRUCTURED) &&
    DEEP_PARSE_RICHNESS.includes(candidate.richness)
  );
}

export {
  NYC_RE,
  NON_NYC_RE,
  UI_CHROME_RE,
  HOUSING_RE,
  SOURCE_STATUS,
  PARTICIPANT_RICHNESS,
};
