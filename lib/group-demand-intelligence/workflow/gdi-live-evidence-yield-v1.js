/**
 * Shared yield measurement for authorized official-source reads.
 * HTTP fetches are not unique sources and are not commercial accomplishments.
 * Paid search stays closed unless a verified provider price is enabled.
 */

import crypto from "node:crypto";
import { detectSourceLanguage } from "../discovery/multilingual-evidence-normalize-v1.js";
import { extractContactIntelligenceFromSource } from "../extract-contact-intelligence-from-source.js";
import { canonicalEntityKey } from "./gdi-global-weekly-feedback-v2.js";
import { hasUsableContactRoute, evaluateProspectPublicationEligibility } from "../prospect-publication-eligibility-v1.js";
import { htmlToSearchableText, fetchResearchPage } from "../../hotel-intelligence/room-count-research/fetch.js";
import { verifiedProviderBilling } from "./gdi-provider-billing-guard-v1.js";

export const LIVE_EVIDENCE_YIELD_VERSION = "gdi-live-evidence-yield-v1";

export function normalizeSourceUrl(value) {
  const raw = String(value || "").trim();
  try {
    const url = new URL(raw);
    url.hash = "";
    url.hostname = url.hostname.toLowerCase();
    if (url.pathname.length > 1) url.pathname = url.pathname.replace(/\/+$/, "");
    return url.toString();
  } catch {
    return raw;
  }
}

export function reconstructPriorAcquisition(markdown = "") {
  const rows = [];
  for (const line of String(markdown).split(/\r?\n/)) {
    const match = line.match(/^- (\S+) (fetched|not acquired) \((\d+)\) (\S+)/);
    if (!match) continue;
    rows.push({
      hotelId: match[1],
      outcome: match[2] === "fetched" ? "fetched" : "not_acquired",
      httpFetches: Number(match[3]),
      url: match[4],
      normalized: normalizeSourceUrl(match[4]),
    });
  }
  const unique = new Set(rows.filter((row) => row.outcome === "fetched").map((row) => row.normalized));
  return {
    readAttempts: rows.length,
    httpFetches: rows.reduce((sum, row) => sum + row.httpFetches, 0),
    acquiredAttempts: rows.filter((row) => row.outcome === "fetched").length,
    zeroFetchAttempts: rows.filter((row) => row.httpFetches === 0).length,
    uniqueFetchedUrls: unique.size,
    extraHttpBeyondUniqueSources: rows.reduce((sum, row) => sum + row.httpFetches, 0) - unique.size,
    rows,
  };
}

export function accountFetchLedger(entries = []) {
  const http = entries.filter((entry) => entry.fetchesUsed > 0);
  return {
    ledgerEntries: entries.length,
    httpFetches: http.reduce((sum, entry) => sum + entry.fetchesUsed, 0),
    uniqueSources: new Set(http.map((entry) => entry.normalized || normalizeSourceUrl(entry.finalUrl || entry.url))).size,
    primaryFetches: http.filter((entry) => entry.role === "primary").length,
    followUpFetches: http.filter((entry) => entry.role === "follow_up").length,
    duplicatesSkipped: entries.filter((entry) => entry.role === "duplicate_skipped").length,
    redirects: http.filter((entry) => entry.redirected === true).length,
  };
}

function stripTags(html) {
  return htmlToSearchableText(String(html || "")).replace(/\s+/g, " ").trim();
}

function decodeHtml(value) {
  return String(value || "")
    .replace(/&#x([0-9a-f]+);/gi, (_, hex) => String.fromCharCode(parseInt(hex, 16)))
    .replace(/&#(\d+);/g, (_, num) => String.fromCharCode(Number(num)))
    .replace(/&amp;/g, "&")
    .replace(/&nbsp;/g, " ");
}

function firstSupportedEventDate(text, pattern) {
  const re = new RegExp(pattern.source, "gi");
  let match;
  while ((match = re.exec(text))) {
    const before = text.slice(Math.max(0, match.index - 30), match.index);
    const year = match[0].match(/20\d{2}/);
    if (/\bborn\b|\bbirth\b/i.test(before)) continue;
    if (year && Number(year[0]) < 2026) continue;
    return match[0];
  }
  return null;
}

function quote(text, pattern) {
  const match = String(text || "").match(pattern);
  if (!match) return null;
  const start = Math.max(0, match.index - 40);
  return String(text).slice(start, start + match[0].length + 80).replace(/\s+/g, " ").trim().slice(0, 220);
}

export function extractCommercialEvidenceFromSource({
  url = "",
  pageHtml = "",
  hotelName = "",
  organizationHint = "",
} = {}) {
  const original = String(pageHtml || "");
  const text = stripTags(original);
  const language = detectSourceLanguage(original) === "unknown" ? detectSourceLanguage(text) : detectSourceLanguage(original);
  const titleMatch = original.match(/<title[^>]*>([^<]{3,180})<\/title>/i);
  const title = decodeHtml((titleMatch?.[1] || "").replace(/\s+/g, " ").trim());
  const eventName = /congreso|conference|symposium|meeting|jornadas|convenci[oó]n|summit/i.test(title) ? title : null;
  const datePattern = /\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:\s*[–-]\s*\d{1,2})?,?\s+20\d{2}\b|\b\d{1,2}\s+de\s+(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de\s+20\d{2}\b|\b20\d{2}-\d{2}-\d{2}\b/i;
  const lodgingPattern = /official hotel|official housing|hotel sede|room block|hotel block|bloque de habitaciones|hoteles oficiales|convenio hotelero/i;
  const eventDate = firstSupportedEventDate(text, datePattern);
  const orgHint = String(organizationHint || "").trim();
  const organization = orgHint && text.toLowerCase().includes(orgHint.toLowerCase()) ? orgHint : null;
  const hotel = String(hotelName || "").trim();
  const hotelNamed = hotel ? text.toLowerCase().includes(hotel.toLowerCase()) : false;
  const lodgingQuote = quote(text, lodgingPattern);
  let venueCommitment = "NOT_STATED";
  if (lodgingQuote && hotelNamed) venueCommitment = "THIS_HOTEL_NAMED_WITH_LODGING_LANGUAGE";
  else if (lodgingQuote) venueCommitment = "LODGING_LANGUAGE_WITHOUT_THIS_HOTEL";
  else if (hotelNamed) venueCommitment = "HOTEL_NAMED_WITHOUT_COMMITMENT";
  const rooms = text.match(/\b(\d{2,4})\s+(?:guest\s*)?rooms\b|\b(\d{2,4})\s+habitaciones\b/i);
  const meeting = text.match(/\b(\d{3,7})\s+(?:sq\.?\s*ft|square feet|pies cuadrados)\b/i);
  const meetingRaw = meeting ? Number(meeting[1]) : null;
  const meetingSpaceSqFt = meetingRaw >= 100 ? meetingRaw : null;
  const contacts = extractContactIntelligenceFromSource({
    source: { url, sourceType: "official_web" },
    pageContent: original,
  });
  const named = (contacts.people || []).length;
  const functional = (contacts.functionalContacts || []).filter((item) => item.email || item.phone).length;
  const recurring = /\b(annual|anual|every year|cada a[nñ]o)\b/i.test(text) && Boolean(eventDate || eventName);
  const urlText = String(url || "").replace(/[-_]+/g, " ");
  const rejected = [];
  if (/bethesda|san diego|mexico city|ciudad de m[eé]xico/i.test(urlText) && !/bethesda|san diego|mexico city|ciudad de m[eé]xico/i.test(text)) {
    rejected.push("location_was_only_in_the_url");
  }
  return {
    sourceUrl: url,
    language,
    title: title || null,
    organization,
    eventName,
    eventDate,
    eventDateEvidence: eventDate ? quote(text, datePattern) : null,
    lodgingEvidence: lodgingQuote,
    venueCommitment,
    hotelNamed,
    guestroomCount: rooms ? Number(rooms[1] || rooms[2]) : null,
    meetingSpaceSqFt,
    recurringOrFuture: recurring ? "RECURRING_LANGUAGE_WITH_DATED_EVENT" : eventDate ? "DATED_EVENT" : "NOT_STATED",
    contactRoute: named ? "NAMED_ON_PAGE" : functional ? "FUNCTIONAL_ON_PAGE" : "NONE",
    namedContactCount: named,
    functionalContactCount: functional,
    rejectedInferences: rejected,
    confidence: eventDate && (organization || eventName) ? "MEDIUM" : eventName || organization || lodgingQuote ? "LOW" : "NONE",
  };
}

export function reviewStoredEvidence(evidence = {}) {
  const lodgingPattern = /official hotel|official housing|hotel sede|room block|hotel block|bloque de habitaciones|hoteles oficiales|convenio hotelero/i;
  const next = { ...evidence, title: decodeHtml(evidence.title), eventName: decodeHtml(evidence.eventName) };
  if (!(Number(next.meetingSpaceSqFt) >= 100)) next.meetingSpaceSqFt = null;
  if (!(Number(next.guestroomCount) >= 20)) next.guestroomCount = null;
  if (next.lodgingEvidence && !lodgingPattern.test(next.lodgingEvidence)) {
    next.lodgingEvidence = null;
    next.venueCommitment = next.hotelNamed ? "HOTEL_NAMED_WITHOUT_COMMITMENT" : "NOT_STATED";
    if (next.confidence === "LOW" && !next.eventName && !next.organization) next.confidence = "NONE";
  }
  if (next.eventDateEvidence && /\bborn\b|\bbirth\b/i.test(next.eventDateEvidence)) {
    next.rejectedInferences = [...(next.rejectedInferences || []), "date_was_eligibility_not_event"];
    next.eventDate = null;
    next.eventDateEvidence = null;
    next.recurringOrFuture = "NOT_STATED";
  }
  const datedYear = String(next.eventDate || "").match(/20\d{2}/);
  if (datedYear && Number(datedYear[0]) < 2026) {
    next.rejectedInferences = [...(next.rejectedInferences || []), "date_before_research_window"];
    next.eventDate = null;
    next.eventDateEvidence = null;
  }
  if (next.eventDate && !(next.organization || next.eventName)) next.confidence = "LOW";
  if (!next.eventDate && next.confidence === "MEDIUM") next.confidence = "LOW";
  if (next.eventName && !/congreso|conference|symposium|meeting|jornadas|convenci[oó]n|summit/i.test(next.eventName)) {
    next.eventName = null;
  }
  return next;
}

export function sourceContentHash(text) {
  return crypto.createHash("sha256").update(String(text || "")).digest("hex").slice(0, 16);
}

export function sourceRevisitDecision({ normalizedUrl, contentHash, completed = [], mode = "INITIAL_FULL_E2E" } = {}) {
  const fingerprint = `source:${normalizedUrl}:${contentHash || "unseen"}`;
  if (mode === "WEEKLY_INCREMENTAL" && completed.includes(fingerprint)) {
    return { refetch: false, reason: "unchanged_source", fingerprint };
  }
  return { refetch: true, reason: mode === "WEEKLY_INCREMENTAL" ? "unseen_or_changed_source" : "initial_read", fingerprint };
}

export function planAutonomousSourceDiscovery({ evidence = {}, allowPublicFetch = false } = {}) {
  const billing = verifiedProviderBilling();
  const paidEnabled = ["parallel", "serpapi", "surfe", "jev"].some((name) => billing[name].enabled === true);
  const actions = [];
  if (evidence.contactRoute === "NONE" && allowPublicFetch) {
    actions.push({
      action: "HOLD_FOR_OFFICIAL_CONTACT_PAGE",
      executable: false,
      costUsd: 0,
      reason: "The acquired page did not publish a usable contact route. A new search is not authorized.",
    });
  }
  if (!evidence.organization || evidence.confidence === "NONE") {
    actions.push({
      action: "TARGETED_ORGANIZATION_DISCOVERY",
      executable: false,
      exhausted: false,
      blocker: paidEnabled ? null : "SEARCH_PROVIDER_NOT_AUTHORIZED",
      languages: ["en", "es"],
      costUsd: null,
    });
  }
  return {
    version: LIVE_EVIDENCE_YIELD_VERSION,
    paidProvidersEnabled: paidEnabled,
    actions,
  };
}

export function reconcileStoredAndPageDate({
  storedDate = "",
  pageDate = "",
  dateEvidence = "",
  pageTitle = "",
  opportunityTitle = "",
} = {}) {
  const evidence = String(dateEvidence || "");
  const stored = String(storedDate || "").slice(0, 10);
  const title = String(pageTitle || "");
  const sameEvent = sameDemandEntity(
    `${opportunityTitle} ${title}`.replace(/\|/g, " "),
    title || opportunityTitle
  );
  if (/available through|group rate|sells out|sell out|room block/i.test(evidence)) {
    return result("PAGE_DATE_IS_BOOKING_DEADLINE", false, true, "ADDITIONAL_SOURCE_FOR_EVENT_DATES", "NOT_DECIDED_BY_THIS_QUOTE");
  }
  if (/events from/i.test(title) && /[–—-]/.test(title)) {
    return result("PAGE_DATE_IS_CALENDAR_SPAN", false, true, "ADDITIONAL_SOURCE_FOR_THIS_EVENT", "NOT_SUPPORTED_BY_THIS_PAGE");
  }
  if (/back to blog|cookie settings/i.test(evidence)) {
    return result("PAGE_DATE_MAY_BE_ARTICLE_DATE", false, false, "ADDITIONAL_SOURCE_FOR_EVENT_DATES", "NOT_DECIDED_BY_THIS_QUOTE");
  }
  const cross = evidence.match(/\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})\s*[–-]\s*(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(20\d{2})/i);
  const sameMonth = evidence.match(/\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})\s*[–-]\s*(\d{1,2}),?\s+(20\d{2})/i);
  if (cross && stored === isoDate(cross[1], cross[2], cross[5])) {
    return result("STORED_DATE_IS_RANGE_START", false, true, "NO_DATE_REPLACEMENT", "PLAUSIBLE_IF_HOTEL_AND_CITY_FIT");
  }
  if (sameMonth && stored === isoDate(sameMonth[1], sameMonth[2], sameMonth[4])) {
    return result("STORED_DATE_IS_RANGE_START", false, true, "NO_DATE_REPLACEMENT", "PLAUSIBLE_IF_HOTEL_AND_CITY_FIT");
  }
  if (stored && pageDate && stored === isoFromLoose(pageDate)) {
    return result("SAME_DATE_DIFFERENT_FORMAT", false, true, "NO_DATE_REPLACEMENT", "PLAUSIBLE_IF_HOTEL_AND_CITY_FIT");
  }
  if (sameEvent || title) {
    return result("SAME_EVENT_DATE_CONFLICT", false, false, "INVESTIGATE_BEFORE_REFRESH", "PLAUSIBLE_PENDING_CONFIRMATION");
  }
  return result("UNRESOLVED_DATE", false, false, "ADDITIONAL_SOURCE_FOR_EVENT_DATES", "NOT_DECIDED_BY_THIS_QUOTE");
}

function result(disposition, substituteStoredDate, resolved, action, futureDemand) {
  return {
    disposition,
    substituteStoredDate,
    resolved,
    requiresAdditionalSource: action.startsWith("ADDITIONAL") || action === "INVESTIGATE_BEFORE_REFRESH",
    action,
    futureDemand,
  };
}

const MONTH_INDEX = {
  january: "01", february: "02", march: "03", april: "04", may: "05", june: "06",
  july: "07", august: "08", september: "09", october: "10", november: "11", december: "12",
};

function isoDate(month, day, year) {
  const mm = MONTH_INDEX[String(month || "").toLowerCase()];
  if (!mm) return "";
  return `${year}-${mm}-${String(day).padStart(2, "0")}`;
}

function isoFromLoose(value) {
  const text = String(value || "");
  const iso = text.match(/\b(20\d{2})-(\d{2})-(\d{2})\b/);
  if (iso) return `${iso[1]}-${iso[2]}-${iso[3]}`;
  const named = text.match(/\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(20\d{2})/i);
  return named ? isoDate(named[1], named[2], named[3]) : "";
}

export function classifyPageContactForOpportunity({
  evidence = {},
  opportunity = {},
  pageUrl = "",
} = {}) {
  const url = String(pageUrl || evidence.sourceUrl || "");
  const route = evidence.contactRoute || "NONE";
  if (!route || route === "NONE") {
    return { association: "NONE", usableForOpportunity: false, recovered: false };
  }
  if (/marriott\.com|wyndhamsandiegobay|fiestamericanatravelty\.com/i.test(url)) {
    return { association: "HOTEL_PROPERTY_CONTACT", usableForOpportunity: false, recovered: false, reason: "A hotel property page contact is not a group buyer." };
  }
  if (/linkedin\.com|facebook\.com|cleat\.ai|xquic\.com|acmiworld\.com|instantmarkets\.com|\/blog\b/i.test(url)) {
    return { association: "UNRELATED_PAGE_CONTACT", usableForOpportunity: false, recovered: false, reason: "The contact is on a social, marketplace, or article page." };
  }
  const org = canonicalEntityKey(opportunity.organizationName);
  const pageOrg = canonicalEntityKey(`${evidence.organization || ""} ${evidence.title || ""} ${evidence.eventName || ""}`);
  const sameOrg = Boolean(org) && pageOrg.includes(org);
  const role = String(evidence.contactRole || "");
  const buyer = /housing|event manager|organizer|planner|procurement/i.test(role);
  if (sameOrg && buyer && evidence.contactEmailVerified === true) {
    return { association: "VERIFIED_BUYER_OR_ORGANIZER", usableForOpportunity: true, recovered: true, reason: "Role and organization match the opportunity." };
  }
  return { association: "UNVERIFIED_PAGE_CONTACT", usableForOpportunity: false, recovered: false, reason: "The page contact is not a verified buyer for this organization." };
}

export function assessSearchDiscoveryAuthorization() {
  const billing = verifiedProviderBilling();
  return {
    authorized: false,
    zeroChargeSearchPath: false,
    blocker: "SEARCH_PROVIDER_NOT_AUTHORIZED",
    officialPageIsNotOrganizationDiscovery: true,
    providers: {
      serpapi: billing.serpapi.reason,
      parallel: billing.parallel.reason,
      surfe: billing.surfe.reason,
      jev: billing.jev.reason,
    },
    minimumChange: [
      "The five-hotel ledger permits official_page only. Consuming a SerpAPI credit is outside that scope.",
      "Do not authorize a search with SERPAPI_USD_PER_SEARCH or the $0.01 placeholder.",
      "A later approval must name prepaid credits, the hotel authorization ids, the hard cap, a receipt per call, and a confirmed hold on automatic renewal.",
      "Leave Parallel, Surfe, and Jev disabled.",
    ],
  };
}
export function advanceOpportunityFromEvidence(opp = {}, evidence = {}) {
  const beforeFacing = evaluateProspectPublicationEligibility(opp).hotelFacing === true;
  const beforeRoute = hasUsableContactRoute(opp);
  const reconciliation = reconcileStoredAndPageDate({
    storedDate: opp.eventStartDate,
    pageDate: evidence.eventDate,
    dateEvidence: evidence.eventDateEvidence,
    pageTitle: evidence.title || evidence.eventName,
    opportunityTitle: opp.title,
  });
  const contact = classifyPageContactForOpportunity({ evidence, opportunity: opp, pageUrl: evidence.sourceUrl });
  const next = {
    ...opp,
    customerVisible: opp.customerVisible === true,
    eventStartDate: opp.eventStartDate,
    researchHistory: [
      ...(opp.researchHistory || []),
      {
        action: "RECONCILE_PAGE_EVIDENCE",
        disposition: reconciliation.disposition,
        substituteStoredDate: false,
      },
    ],
  };
  const findings = [reconciliation.disposition];
  if (reconciliation.disposition === "STORED_DATE_IS_RANGE_START") {
    next.evidenceQualification = "DATE_RANGE_CONFIRMED";
    findings.push("qualification_date_range_confirmed");
  } else if (opp.eventStartDate && evidence.eventDate) {
    findings.push("stored_date_not_replaced");
  }
  if (contact.usableForOpportunity && evidence.contactEmail) {
    next.primaryContactEmail = evidence.contactEmail;
    next.contactOfficialUrl = evidence.sourceUrl;
    findings.push("verified_contact_route_added");
  } else if (contact.association !== "NONE") {
    findings.push(contact.association.toLowerCase());
  }
  const afterFacing = evaluateProspectPublicationEligibility(next).hotelFacing === true;
  return {
    opportunityId: opp.id || opp.opportunityId || null,
    organization: opp.organizationName || null,
    beforeHotelFacing: beforeFacing,
    afterHotelFacing: afterFacing,
    beforeContact: beforeRoute.ok === true,
    afterContact: hasUsableContactRoute(next).ok === true,
    customerVisibleUnchanged: next.customerVisible === (opp.customerVisible === true),
    storedDatePreserved: next.eventStartDate === opp.eventStartDate,
    reconciliation,
    contact,
    findings,
    next,
  };
}

export function sameDemandEntity(left, right) {
  return Boolean(canonicalEntityKey(left)) && canonicalEntityKey(left) === canonicalEntityKey(right);
}

export async function readAuthorizedOfficialSource(url, opts = {}) {
  const normalized = normalizeSourceUrl(url);
  const prior = opts.priorArchive?.find((item) => item.normalized === normalized);
  if (opts.mode === "WEEKLY_INCREMENTAL" && prior?.contentHash) {
    const decision = sourceRevisitDecision({
      normalizedUrl: normalized,
      contentHash: prior.contentHash,
      completed: opts.completedFingerprints || [prior.fingerprint],
      mode: opts.mode,
    });
    if (!decision.refetch) {
      return { ok: true, skipped: true, httpFetches: 0, normalized, evidence: prior.evidence, decision };
    }
  }
  const page = await fetchResearchPage(url, { timeoutMs: opts.timeoutMs ?? 20000 });
  const html = page.text || "";
  const evidence = extractCommercialEvidenceFromSource({
    url: page.url || url,
    pageHtml: html,
    hotelName: opts.hotelName,
    organizationHint: opts.organizationHint,
  });
  const contentHash = sourceContentHash(stripTags(html).slice(0, 20000));
  const decision = sourceRevisitDecision({ normalizedUrl: normalized, contentHash, mode: opts.mode || "INITIAL_FULL_E2E" });
  return {
    ok: page.ok === true,
    skipped: false,
    httpFetches: 1,
    status: page.status || null,
    blocked: page.blocked === true,
    requestedUrl: url,
    finalUrl: page.url || url,
    redirected: normalizeSourceUrl(page.url || url) !== normalized,
    normalized,
    contentHash,
    fingerprint: decision.fingerprint,
    evidence,
    textLength: html.length,
  };
}
