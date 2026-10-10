/**
 * Page-level validation — SERP is pointer only; open page to confirm group + event + competitor.
 */

import {
  fetchCandidatePage,
  extractEvidenceFromPage,
} from "../candidate-completion-v2/page-research.js";
import {
  classifyCompetitorUseEvidence,
  canFeedDeeperOpportunityResearch,
  EVIDENCE_CLASS,
} from "./evidence-class.js";
import { extractPublicPhoneFromText } from "./phone-variants.js";
import { classifyDemandEngine } from "../demand-engine-v1/classify-engine.js";

const ORG_RE =
  /\b((?:[A-Z][A-Za-z0-9&.\-']+(?:\s+[A-Z][A-Za-z0-9&.\-']+){1,6})\s+(?:Association|Society|Federation|Foundation|Congress|Conference|Institute|University|Agency|Events|Committee|Organization|Organisation))\b/;

/**
 * Validate a SERP hit against a competitor.
 */
export async function validateCompDemandPage(hit = {}, competitor = {}, pivot = {}, opts = {}) {
  const url = String(hit.link || hit.url || "").trim();
  const title = String(hit.title || "").trim();
  const snippet = String(hit.snippet || "").trim();

  const base = {
    url,
    title,
    snippet,
    competitorHotelId: competitor.competitorHotelId,
    canonicalName: competitor.canonicalName,
    pivotId: pivot.pivotId,
    pivotType: pivot.pivotType,
    pageOk: false,
    pageKind: null,
    evidenceClass: EVIDENCE_CLASS.DISCOVERY_ONLY,
    feedsDeeperResearch: false,
    organization: null,
    eventProgram: null,
    eventDate: null,
    eventYear: null,
    groupType: null,
    demandEngine: null,
    extractedPhone: null,
    fact: null,
    inference: null,
    unknown: null,
    reason: null,
    error: null,
  };

  if (!url) {
    base.error = "NO_URL";
    return base;
  }

  const page = await fetchCandidatePage(url, { maxChars: opts.maxChars ?? 14000 });
  if (!page.ok) {
    // Fall back to snippet-only classification — stays DISCOVERY/WEAK at best
    const cls = classifyCompetitorUseEvidence({
      competitorName: competitor.canonicalName,
      title,
      snippet,
      pageText: "",
      pageUrl: url,
      pivotType: pivot.pivotType,
    });
    return {
      ...base,
      pageOk: false,
      error: page.error || "FETCH_FAILED",
      evidenceClass: cls.evidenceClass === EVIDENCE_CLASS.DIRECT_CONFIRMED
        ? EVIDENCE_CLASS.STRONG_ASSOCIATION // cannot DIRECT confirm without page body
        : cls.evidenceClass,
      feedsDeeperResearch: false,
      reason: `PAGE_FETCH_FAILED:${cls.reason}`,
      fact: null,
      inference: "SERP snippet only — not sufficient for DIRECT_CONFIRMED",
      unknown: "Full page context",
    };
  }

  const text = page.text || "";
  const cls = classifyCompetitorUseEvidence({
    competitorName: competitor.canonicalName,
    title,
    snippet,
    pageText: text.slice(0, 8000),
    pageUrl: page.url || url,
    pivotType: pivot.pivotType,
  });

  const extracted = extractEvidenceFromPage(
    { title, officialSource: url },
    page
  );
  const orgMatch = `${title} ${text.slice(0, 600)}`.match(ORG_RE);
  const organization =
    orgMatch?.[1] ||
    title.split(/[|\-—:]/)[0].trim().slice(0, 120) ||
    null;

  const yearMatch = `${title} ${text.slice(0, 2000)}`.match(/\b(20(?:2[3-9]|3[0-2]))\b/);
  const eventYear = extracted.updates?.eventYear || (yearMatch ? yearMatch[1] : null);
  const eventDate = extracted.updates?.eventStartDate || null;

  const groupType = (() => {
    if (/wedding|mariage|boda/i.test(text + title)) return "SOCIAL_WEDDING";
    if (/tournament|team|federation|sports/i.test(text + title)) return "SPORTS";
    if (/congress|association|society/i.test(text + title)) return "ASSOCIATION";
    if (/investigator|advisory|pharma|medical/i.test(text + title)) return "PHARMA";
    if (/incentive|corporate|kickoff|leadership/i.test(text + title)) return "CORPORATE";
    if (/university|faculty|executive education/i.test(text + title)) return "UNIVERSITY";
    if (/crew|contractor|workforce/i.test(text + title)) return "PROJECT_CREW";
    return groupTypeFromHint(text + title);
  })();

  const eng = classifyDemandEngine({
    title,
    organizationName: organization,
    summaryWhat: snippet,
    opportunityType: groupType,
  });

  const phone = extractPublicPhoneFromText(text.slice(0, 3000), competitor.countryHint);

  return {
    ...base,
    url: page.url || url,
    pageOk: true,
    pageKind: inferPageKind(page.url || url, text),
    evidenceClass: cls.evidenceClass,
    feedsDeeperResearch: canFeedDeeperOpportunityResearch(cls.evidenceClass),
    organization,
    eventProgram: title,
    eventDate,
    eventYear,
    groupType,
    demandEngine: eng?.demandEngine || "ASSOCIATION_NGO",
    extractedPhone: phone,
    lodgingEvidence: extracted.updates?.lodgingEvidence || null,
    organizationContactUrl: extracted.updates?.organizationContactUrl || null,
    functionalContactEmail: extracted.updates?.functionalContactEmail || null,
    fact: cls.fact,
    inference: cls.inference,
    unknown: cls.unknown,
    reason: cls.reason,
    pageTextSample: text.slice(0, 400),
  };
}

function groupTypeFromHint(b) {
  if (/\bconference|summit|symposium\b/i.test(b)) return "CONFERENCE";
  return "GROUP_PROGRAM";
}

function inferPageKind(url, text) {
  const b = `${url} ${String(text).slice(0, 500)}`;
  if (/accommodation|housing|hotel-block|hébergement|alojamiento/i.test(b)) return "HOUSING";
  if (/rfp|tender|procurement|licitación/i.test(b)) return "PROCUREMENT";
  if (/register|registration|inscription/i.test(b)) return "REGISTRATION";
  if (/\.pdf/i.test(url)) return "PDF_GUIDE";
  if (/agenda|program|programme/i.test(b)) return "PROGRAM";
  if (/wedding|mariage|boda/i.test(b)) return "WEDDING_EVENT";
  if (/case study|DMC|agency/i.test(b)) return "AGENCY_CASE";
  return "EVENT_OR_OTHER";
}

/**
 * Build structured competitor-demand trace from validated page.
 */
export function buildCompetitorDemandTrace(validation = {}, competitor = {}, targetHotel = {}, pivot = {}) {
  const seriesSlug = String(validation.organization || validation.eventProgram || "unknown")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .slice(0, 40);
  return {
    traceId: `trace_${competitor.competitorHotelId}_${seriesSlug}_${validation.eventYear || "nd"}`.slice(
      0,
      96
    ),
    targetHotelKey: targetHotel.hotelKey || targetHotel.targetHotelKey,
    targetHotelId: targetHotel.hotelId || targetHotel.targetHotelId,
    organization: validation.organization,
    eventProgram: validation.eventProgram,
    eventSeriesId: `series_${seriesSlug}`,
    eventCycleId: validation.eventDate || validation.eventYear || null,
    groupType: validation.groupType,
    demandEngine: validation.demandEngine,
    eventDate: validation.eventDate,
    eventYear: validation.eventYear,
    market: competitor.market || targetHotel.market,
    source: validation.url,
    competitorHotel: competitor.canonicalName,
    competitorHotelId: competitor.competitorHotelId,
    evidenceClass: validation.evidenceClass,
    feedsDeeperResearch: validation.feedsDeeperResearch,
    pivotId: pivot.pivotId,
    pivotType: pivot.pivotType,
    fact: validation.fact,
    inference: validation.inference,
    unknown: validation.unknown,
    lodgingEvidence: validation.lodgingEvidence,
    organizationContactUrl: validation.organizationContactUrl,
    functionalContactEmail: validation.functionalContactEmail,
  };
}
