/**
 * Bounded investigation of organizations already retained from discovery.
 * A missing date or contact on the first page does not reject the organization.
 * One linked official organization host may be fetched. Aggregators, news, and legislatures are not.
 */

import {
  classifyPageContactForOpportunity,
  extractCommercialEvidenceFromSource,
  reviewStoredEvidence,
  sourceContentHash,
  sourceRevisitDecision,
} from "./gdi-live-evidence-yield-v1.js";

const FOLLOW_PATH = /contact|housing|accommodat|hotel-block|staff|leadership|about|convention|annual|meeting|conference/i;
const AGGREGATOR = /cvent\.com|eventbrite\.|facebook\.com|linkedin\.com|instagram\.com|twitter\.com|x\.com|youtube\.com/i;
const OUTSIDE_MARKET = /\b(richmond|harrisonburg|virginia beach|norfolk|williamsburg|charlottesville|roanoke|blacksburg|baltimore|philadelphia|orlando|miami|las vegas|chicago|new york|san diego|los angeles|atlanta|boston|denver|seattle|houston|dallas|austin|kalahari)\b/i;
const NAMED_VENUE = /\b(hotel|resort|marriott|hilton|hyatt|sheraton|westin|convention center|conference center|civic center|university|campus|hall)\b/i;

export function hostOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return "";
  }
}

export function sameHostFollowUpAllowed(seedUrl, candidateUrl) {
  const seedHost = hostOf(seedUrl);
  const nextHost = hostOf(candidateUrl);
  if (!seedHost || seedHost !== nextHost) return false;
  if (AGGREGATOR.test(candidateUrl) || AGGREGATOR.test(seedUrl)) return false;
  return FOLLOW_PATH.test(candidateUrl);
}

const BUYER_HOST = /\.org\b|asociaci[oó]n|c[aá]mara|society|colegio|\.edu\b|\.gob\.mx/i;
const NOT_BUYER_HOST = /congresocdmx|c[aá]mara de diputados|news|prensa|excelsior|eluniversal|milenio|bizjournals|wtop|eventbrite|cvent|10times|clocate/i;

export function selectExternalBuyerLinks(html, seedUrl, limit = 1) {
  const found = [];
  const re = /href=["']([^"'#]+)["']/gi;
  let match;
  while ((match = re.exec(String(html || "")))) {
    let absolute = "";
    try {
      absolute = new URL(match[1], seedUrl).href;
    } catch {
      continue;
    }
    if (hostOf(absolute) === hostOf(seedUrl)) continue;
    if (AGGREGATOR.test(absolute) || NOT_BUYER_HOST.test(absolute)) continue;
    if (!BUYER_HOST.test(absolute)) continue;
    if (!found.includes(absolute)) found.push(absolute);
  }
  return found.slice(0, limit);
}

export function selectSameHostOfficialLinks(html, seedUrl, limit = 2) {
  const found = [];
  const re = /href=["']([^"'#]+)["']/gi;
  let match;
  while ((match = re.exec(String(html || "")))) {
    let absolute = "";
    try {
      absolute = new URL(match[1], seedUrl).href;
    } catch {
      continue;
    }
    if (!sameHostFollowUpAllowed(seedUrl, absolute)) continue;
    if (hostOf(absolute) !== hostOf(seedUrl)) continue;
    if (absolute.replace(/\/$/, "") === String(seedUrl).replace(/\/$/, "")) continue;
    if (!found.includes(absolute)) found.push(absolute);
  }
  const rank = (url) => (/housing|accommodat|contact|staff|leadership/i.test(url) ? 0 : 1);
  return found.sort((a, b) => rank(a) - rank(b)).slice(0, limit);
}

export function readVenueStatement(text = "") {
  const clean = String(text || "").replace(/&amp;/g, "&").replace(/\s+/g, " ");
  const held = clean.match(/(?:will be held|to be held|held at|take place(?:\s+[^.]{0,60})?\s+at|located at|venue is|host hotel is|headquarters hotel is)\s+([^!]{8,180})/i);
  if (held) return cleanVenue(held[1]);
  const named = clean.match(/\b((?:(?:[A-Z][\w'.&-]+\s+){2,4}Hall|(?:[A-Z][\w'.&-]+\s+){1,4}(?:Hotel|Resort)|(?:[A-Z][\w'.&-]+\s+)by\s+(?:Hilton|Marriott|Hyatt)(?:\s+[A-Z][\w'.&-]+){0,3}|(?:[A-Z][\w'.&-]+\s+){1,3}Conference Center)(?:\s*\|\s*[A-Za-z]+(?:,\s*[A-Z]{2})?)?)\b/);
  return named ? cleanVenue(named[1]) : "";
}

function cleanVenue(value) {
  let statement = String(value || "").replace(/\s+/g, " ").trim();
  statement = statement.split(/\b(?:information on|room blocks|will be announced|more information|this one-day|contact:)\b/i)[0].trim();
  if (!statement || /reservation desk|member login|jquery|agents /i.test(statement)) return "";
  if (/^conference center$/i.test(statement)) return "";
  return statement;
}

export function classifyVenueRelevance(statement, territoryTerms = []) {
  if (!statement) return "INSUFFICIENT_EVIDENCE";
  const terms = territoryTerms.map((term) => String(term).toLowerCase()).filter(Boolean);
  const local = terms.some((term) => statement.toLowerCase().includes(term));
  const outside = OUTSIDE_MARKET.test(statement);
  if (outside && !local) return "OUT_OF_MARKET";
  if (NAMED_VENUE.test(statement)) return "NOT_OPEN_BOOKING";
  if (local) return "FUTURE_LOCAL_RELEVANT";
  return "INSUFFICIENT_EVIDENCE";
}

function redact(text) {
  return String(text || "")
    .replace(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi, "[redacted]")
    .replace(/\b\+?1?[\s.-]?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]\d{4}\b/g, "[redacted]");
}

export function contactRouteForRead(read, organizationName) {
  const url = read?.url || "";
  if (AGGREGATOR.test(url)) return "event_listing_contact";
  const evidence = read?.evidence || {};
  const classified = classifyPageContactForOpportunity({
    evidence,
    opportunity: { organizationName },
    pageUrl: url,
  });
  if (classified.usableForOpportunity) return "verified_organizer_route";
  if (/housing|accommodat|contact|staff|leadership/i.test(url) && classified.association !== "HOTEL_PROPERTY_CONTACT") {
    return "relevant_organizational_route";
  }
  if (evidence.contactRoute && evidence.contactRoute !== "NONE") return "generic_organization_inquiry";
  return "no_route";
}

export function disposeInvestigation({ lead = {}, reads = [], territoryTerms = [] } = {}) {
  const seed = reads.find((read) => read.action === "READ_RETAINED_PAGE") || reads[0] || null;
  const attempted = reads.length > 0;
  const requirements = ["organization", "demand_mechanism", "venue_or_geography", "organizer_route", "dates"];
  const attemptedRequirements = attempted ? ["organization", "demand_mechanism", "venue_or_geography", "organizer_route"] : [];
  if (seed?.evidence?.eventDate) attemptedRequirements.push("dates");
  if (!seed || seed.blocked || seed.ok === false) {
    return finish("BLOCKED_SOURCE_ACCESS", requirements, attemptedRequirements, "The retained page did not return a readable official source.");
  }
  if ((seed.textLength || 0) > 0 && seed.textLength < 200) {
    return finish("BLOCKED_SOURCE_ACCESS", requirements, attemptedRequirements, "The retained page returned too little text to verify the organization.", "no_route", "INSUFFICIENT_EVIDENCE");
  }
  const evidenceRows = reads.map((read) => read.evidence).filter(Boolean);
  const dated = evidenceRows.find((item) => item.eventDate) || seed.evidence || {};
  const named = evidenceRows.find((item) => item.eventName) || dated;
  const venueStatement = reads.map((read) => read.venueStatement).find(Boolean) || "";
  const venue = classifyVenueRelevance(venueStatement, territoryTerms);
  const demand = Boolean(named.eventName || named.recurringOrFuture === "RECURRING_LANGUAGE_WITH_DATED_EVENT");
  const organization = Boolean(named.organization || named.title || seed.evidence?.title);
  const routes = reads.map((read) => contactRouteForRead(read, lead.organizationName));
  const route = routes.find((item) => item === "verified_organizer_route")
    || routes.find((item) => item !== "no_route")
    || "no_route";
  if (venue === "OUT_OF_MARKET") {
    return finish("OUT_OF_MARKET", requirements, attemptedRequirements, "The page names a venue outside the hotel market.", route, venue);
  }
  if (venue === "NOT_OPEN_BOOKING") {
    return finish("NOT_OPEN_BOOKING", requirements, attemptedRequirements, "The page names a venue, so this edition is not an open booking.", route, venue);
  }
  if (!demand && !organization) {
    return finish("REJECTED_WITH_EVIDENCE", requirements, requirements, "The readable page does not identify a group-demand organization.", route, venue);
  }
  const futureYearNamed = /\b20(2[6-9]|[3-9]\d)\b/.test(`${named.title || ""} ${named.eventName || ""}`);
  if (demand && !dated.eventDate && /annual|anual/i.test(`${named.title || ""} ${named.eventName || ""}`) && !futureYearNamed) {
    return finish("WAITING_FOR_FUTURE_SIGNAL", requirements, attemptedRequirements, "The page describes a recurring meeting and does not publish an open future date.", route, venue);
  }
  if (route !== "verified_organizer_route" || venue !== "FUTURE_LOCAL_RELEVANT" || !dated.eventDate) {
    return finish("INVESTIGATION_REQUIRED", requirements, attemptedRequirements, "Identity or demand is present, and a contact, date, or venue is still missing.", route, venue);
  }
  return finish("QUALIFIED_ACTIONABLE", requirements, requirements, "Organization, future local demand, and a verified organizer route are on the official page.", route, venue);
}

function finish(disposition, requirements, attemptedRequirements, reason, route = "no_route", venue = "INSUFFICIENT_EVIDENCE") {
  return {
    disposition,
    reason,
    hotelFacing: false,
    contactRoute: route,
    venueRelevance: venue,
    requirements,
    attemptedRequirements,
    researchExhausted: false,
    salesAction: salesActionFor(disposition, route, venue),
  };
}

function salesActionFor(disposition, route, venue) {
  if (disposition === "QUALIFIED_ACTIONABLE") return "Send the opportunity to hotel sales with the verified organizer route.";
  if (disposition === "OUT_OF_MARKET") return "Do not pursue this edition. The stated venue is outside the market.";
  if (disposition === "NOT_OPEN_BOOKING") return "Do not present this edition as open inventory. The page names a venue.";
  if (disposition === "WAITING_FOR_FUTURE_SIGNAL") return "Watch for the next dated edition. No open date is published.";
  if (disposition === "BLOCKED_SOURCE_ACCESS") return "Reread the retained official page when it is reachable. Do not open a new search from this block.";
  if (route === "event_listing_contact") return "Find the organizer on its own site. The listing page is not a buyer route.";
  if (venue === "INSUFFICIENT_EVIDENCE") return "The authorized pages do not state a venue. No new search is authorized in this phase.";
  return "Keep the organization. A buyer route is not verified, so it is not hotel-facing.";
}

export function selectNextInvestigationAction(state = {}, opts = {}) {
  const lead = state.lead || {};
  const reads = state.reads || [];
  const mode = opts.mode || "INITIAL_FULL_E2E";
  const maxFollowUps = opts.maxFollowUps ?? 2;
  const seedRead = reads.find((read) => read.action === "READ_RETAINED_PAGE");
  if (!seedRead) {
    const decision = sourceRevisitDecision({
      normalizedUrl: lead.seedUrl,
      contentHash: "unseen",
      completed: state.completedFingerprints || [],
      mode,
    });
    if (!decision.refetch && mode === "WEEKLY_INCREMENTAL") {
      return { executable: false, type: "SKIP_UNCHANGED", reason: decision.reason };
    }
    return { executable: true, type: "READ_RETAINED_PAGE", url: lead.seedUrl, reason: "initial_or_changed_source" };
  }
  const weekly = sourceRevisitDecision({
    normalizedUrl: seedRead.url,
    contentHash: seedRead.contentHash,
    completed: state.completedFingerprints || [],
    mode,
  });
  if (mode === "WEEKLY_INCREMENTAL" && !weekly.refetch && reads.length === 1) {
    return { executable: false, type: "SKIP_UNCHANGED", reason: weekly.reason };
  }
  const followed = new Set(reads.map((read) => read.url));
  const next = (seedRead.sameHostLinks || []).find((url) => !followed.has(url));
  const followCount = reads.filter((read) => read.action === "READ_SAME_HOST_PAGE").length;
  if (next && followCount < maxFollowUps && seedRead.ok) {
    return { executable: true, type: "READ_SAME_HOST_PAGE", url: next, reason: "same_host_official_link" };
  }
  const buyer = (seedRead.externalBuyerLinks || []).find((url) => !followed.has(url));
  const buyerReads = reads.filter((read) => read.action === "READ_BUYER_ORGANIZATION").length;
  if (buyer && buyerReads < 1 && seedRead.ok) {
    return { executable: true, type: "READ_BUYER_ORGANIZATION", url: buyer, reason: "linked_buyer_organization" };
  }
  return { executable: false, type: "STOP", reason: "authorized_reads_complete" };
}

export async function investigateRetainedLeads({
  leads = [],
  territoryTerms = [],
  fetchPage,
  mode = "INITIAL_FULL_E2E",
  completedFingerprints = [],
  maxFollowUps = 2,
  maxRetries = 1,
} = {}) {
  const investigations = [];
  let httpFetches = 0;
  for (const lead of leads) {
    const state = { lead, reads: [], completedFingerprints };
    let guard = 0;
    while (guard < 8) {
      const action = selectNextInvestigationAction(state, { mode, maxFollowUps });
      if (!action.executable) {
        state.stop = action;
        break;
      }
      let page = null;
      let attempt = 0;
      while (attempt <= maxRetries) {
        attempt += 1;
        httpFetches += 1;
        try {
          page = await fetchPage(action.url, { seedUrl: lead.seedUrl, attempt });
        } catch (error) {
          page = { ok: false, status: null, blocked: false, error: error.message };
        }
        if (page?.ok || page?.blocked) break;
      }
      state.reads.push(summarizePage(action, page, lead));
      guard += 1;
    }
    const decision = disposeInvestigation({ lead, reads: state.reads, territoryTerms });
    investigations.push({
      organizationName: lead.organizationName,
      seedUrl: lead.seedUrl,
      reads: state.reads.map((read) => ({
        action: read.action,
        url: read.url,
        ok: read.ok,
        status: read.status,
        blocked: read.blocked,
        venueStatement: redact(read.venueStatement),
        title: read.evidence?.title || null,
        eventDate: read.evidence?.eventDate || null,
        dateEvidence: redact(read.evidence?.eventDateEvidence || "").slice(0, 180),
        recurring: read.evidence?.recurringOrFuture || null,
        contactRoute: contactRouteForRead(read, lead.organizationName),
        textLength: read.textLength,
        contentHash: read.contentHash,
      })),
      ...decision,
      nextResearchAction: state.stop?.type === "SKIP_UNCHANGED" ? "No weekly reread. The retained page is unchanged." : decision.salesAction,
    });
  }
  return {
    investigations,
    httpFetches,
    serpApiCredits: 0,
    parallelCalls: 0,
    surfeCalls: 0,
    jevCalls: 0,
    providerChargeUsd: 0,
    mode,
  };
}

function summarizePage(action, page, lead) {
  const html = String(page?.text || page?.html || "");
  const evidence = reviewStoredEvidence(page?.evidence || extractCommercialEvidenceFromSource({
    url: page?.url || action.url,
    pageHtml: html,
    organizationHint: lead.organizationName,
  }));
  if (evidence.eventDateEvidence && /book by|must book|booking deadline|available through|rsvp by/i.test(evidence.eventDateEvidence)) {
    evidence.eventDate = null;
    evidence.eventDateEvidence = null;
    evidence.rejectedInferences = [...(evidence.rejectedInferences || []), "date_was_booking_deadline"];
  }
  const text = redact(html.replace(/<[^>]+>/g, " "));
  return {
    action: action.type,
    url: page?.url || action.url,
    ok: page?.ok === true,
    status: page?.status || null,
    blocked: page?.blocked === true,
    evidence,
    venueStatement: readVenueStatement(text),
    sameHostLinks: selectSameHostOfficialLinks(html, lead.seedUrl),
    externalBuyerLinks: selectExternalBuyerLinks(html, page?.url || action.url),
    contentHash: sourceContentHash(text.slice(0, 20000)),
    textLength: text.trim().length,
  };
}

export function diagnoseDiscoveryMiss({ rows = [], queries = [], language = "es" } = {}) {
  const total = rows.length || 1;
  const legislature = rows.filter((row) => row.sourceType === "legislature" || row.reason === "SOURCE_LEGISLATURE").length;
  const news = rows.filter((row) => row.sourceType === "news" || row.reason === "SOURCE_NEWS").length;
  const kept = rows.filter((row) => row.decision === "investigate").length;
  const congresoQueries = queries.filter((query) => /\bcongreso\b/i.test(query)).length;
  const directoryQueries = queries.filter((query) => /directorio|directory/i.test(query)).length;
  const causes = [];
  if (congresoQueries > 0) causes.push("SEARCH_PLANNING");
  if (language === "es" && congresoQueries > 0) causes.push("SPANISH_TERMINOLOGY");
  if (legislature + news > 0) causes.push("SOURCE_CLASSIFICATION");
  if (directoryQueries === 0) causes.push("MISSING_ORGANIZATION_DIRECTORY_RESEARCH");
  if (kept === 0 && rows.length > 0) causes.push("INSUFFICIENT_SEGMENT_TARGETING");
  return {
    primaryCause: causes[0] || "INSUFFICIENT_SEGMENT_TARGETING",
    causes,
    rows: rows.length,
    legislature,
    news,
    kept,
    congresoQueries,
    directoryQueries,
    prematureRejection: false,
    newOrganizationsDiscovered: 0,
  };
}
