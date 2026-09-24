/**
 * GDI New Opportunities V1.2 — live discovery routing + evidence recovery.
 *
 * Fixes from Bethesda V1.1 live audit:
 * - PAST_EVENT pollution (year-only → Jan 1)
 * - official source loss across import
 * - org-HQ vs event-location confusion
 * - vendor/listing noise
 * - bounded WATCH second-pass
 */

export const DISCOVERY_ROUTING_V1_2 = "gdi_discovery_routing_v1_2";

export const SOURCE_AUTHORITY = Object.freeze({
  OFFICIAL_ORGANIZATION: "OFFICIAL_ORGANIZATION",
  OFFICIAL_EVENT: "OFFICIAL_EVENT",
  OFFICIAL_GOVERNMENT: "OFFICIAL_GOVERNMENT",
  OFFICIAL_UNIVERSITY: "OFFICIAL_UNIVERSITY",
  OFFICIAL_HOSPITAL: "OFFICIAL_HOSPITAL",
  OFFICIAL_HOUSING_PROVIDER: "OFFICIAL_HOUSING_PROVIDER",
  OFFICIAL_VENUE: "OFFICIAL_VENUE",
  CREDIBLE_NEWS: "CREDIBLE_NEWS",
  SECONDARY: "SECONDARY",
  UNKNOWN: "UNKNOWN",
});

export const PAGE_SIGNAL_CLASS = Object.freeze({
  DEMAND_SIGNAL: "DEMAND_SIGNAL",
  SUPPLY_SIDE_PAGE: "SUPPLY_SIDE_PAGE",
  REFERENCE_ONLY: "REFERENCE_ONLY",
  ARCHIVE_PAST: "ARCHIVE_PAST",
  UNKNOWN: "UNKNOWN",
});

export const WATCH_RECOVERY_CLASS = Object.freeze({
  WATCH_KEEP: "WATCH_KEEP",
  WATCH_VERIFY_GEO: "WATCH_VERIFY_GEO",
  WATCH_VERIFY_SOURCE: "WATCH_VERIFY_SOURCE",
  WATCH_VERIFY_HOUSING: "WATCH_VERIFY_HOUSING",
  NOISE: "NOISE",
  SKIP: "SKIP",
});

export const COMMERCIAL_STATUS = Object.freeze({
  UNKNOWN: "UNKNOWN",
  OPEN: "OPEN",
  TBD: "TBD",
  ANNOUNCED: "ANNOUNCED",
  SELECTED: "SELECTED",
  HOUSING_OPEN: "HOUSING_OPEN",
  HOUSING_FORTHCOMING: "HOUSING_FORTHCOMING",
  BLOCK_OPEN: "BLOCK_OPEN",
  BLOCK_CLOSED: "BLOCK_CLOSED",
  OVERFLOW_POSSIBLE: "OVERFLOW_POSSIBLE",
  FULLY_PLACED: "FULLY_PLACED",
});

const YEAR_FLOOR_RE = /^20\d{2}-01-01$/;
const FUTURE_SIGNAL_RE =
  /\b(?:next\s+(?:year|cycle|event)|upcoming|registration\s+open|housing\s+(?:open|coming)|accommodations?\s+coming|hotel\s+block|rfp|20(?:2[6-9]|3[0-9]))\b/i;
const PAST_TITLE_RE =
  /\b(?:20(?:1\d|2[0-4])|recap|highlights|photos|archive|past\s+events?|previous\s+(?:year|cycle))\b/i;
const SUPPLY_SIDE_RE =
  /\b(?:blueground|airbnb|vrbo|corporate\s+housing\s+(?:provider|solutions?)|furnished\s+apartments?\s+for\s+rent|book\s+now|our\s+properties)\b/i;
const REFERENCE_ONLY_RE =
  /\b(?:integrated\s+lodging\s+program|preferred\s+lodging\s+list|contract\s+vehicle|gsa\s+schedule|vendor\s+list|directory\s+of\s+hotels|program\s+sites?\s+list)\b/i;
const HOUSING_COMMERCIAL_RE =
  /\b(?:hotel\s+tbd|venue\s+tbd|accommodations?\s+coming|housing\s+coming|official\s+hotels?|hotel\s+block|room\s+block|overflow\s+hotel|host\s+hotel|request\s+for\s+proposals?|site\s+selection|lodging\s+information\s+forthcoming|housing\s+bureau)\b/i;

/**
 * Year-only / year-floor must not be treated as a calendar PAST day.
 * Returns scrubbed date fields for hygiene consumption.
 */
export function scrubYearFloorForHygiene(opp = {}, { nowDate = null } = {}) {
  const start = String(opp.eventStartDate || "").trim().slice(0, 10);
  const end = String(opp.eventEndDate || "").trim().slice(0, 10);
  const gran = String(opp.eventDateGranularity || "").toUpperCase();
  const yearOnly =
    gran === "YEAR" ||
    YEAR_FLOOR_RE.test(start) ||
    (/^20\d{2}$/.test(start) && start.length === 4);

  if (!yearOnly && !YEAR_FLOOR_RE.test(start)) {
    return { ...opp, _yearFloorScrubbed: false };
  }

  const year = (start.match(/^(20\d{2})/) || [])[1] || opp.eventYear || null;
  const nowYear = Number(String(nowDate || new Date().toISOString()).slice(0, 4));
  const y = year ? Number(year) : null;

  // Past year with only year granularity → still past (not Jan-1 artifact)
  if (y != null && nowYear && y < nowYear) {
    return {
      ...opp,
      eventYear: String(y),
      eventDateGranularity: "YEAR",
      eventStartDate: null,
      eventEndDate: null,
      pastEventConfirmed: true,
      _yearFloorScrubbed: true,
      _yearPast: true,
    };
  }

  // Current/future year-only → clear fake Jan 1 so hygiene does not PAST
  return {
    ...opp,
    eventYear: year || opp.eventYear || null,
    eventDateGranularity: "YEAR",
    eventDateStatus: opp.eventDateStatus || "INFERRED",
    eventStartDate: null,
    eventEndDate: YEAR_FLOOR_RE.test(end) ? null : opp.eventEndDate || null,
    _yearFloorScrubbed: true,
    _yearPast: false,
  };
}

/**
 * Early SERP hit screen — skip obvious past archive before fetch/extract.
 */
export function screenSerpHitForPast(hit = {}, { nowDate = null } = {}) {
  const title = String(hit.title || "");
  const snippet = String(hit.snippet || hit.description || "");
  const url = String(hit.url || hit.link || "");
  const blob = `${title}\n${snippet}\n${url}`;
  const nowYear = Number(String(nowDate || new Date().toISOString()).slice(0, 4));

  if (FUTURE_SIGNAL_RE.test(blob)) {
    return { skip: false, reason: null, keepForFutureSignal: true };
  }

  const yearHits = [...blob.matchAll(/\b(20[1-3]\d)\b/g)].map((m) => Number(m[1]));
  const maxYear = yearHits.length ? Math.max(...yearHits) : null;
  if (maxYear != null && maxYear < nowYear && PAST_TITLE_RE.test(blob)) {
    return { skip: true, reason: "SKIP_PAST_SERP", detail: `max_year_${maxYear}` };
  }
  if (PAST_TITLE_RE.test(title) && !FUTURE_SIGNAL_RE.test(blob)) {
    if (maxYear != null && maxYear < nowYear) {
      return { skip: true, reason: "SKIP_PAST_SERP", detail: "archive_title" };
    }
  }
  return { skip: false, reason: null };
}

/**
 * Filter organic SERP list; returns kept + skipped tallies.
 */
export function filterSerpOrganicPast(organic = [], opts = {}) {
  const kept = [];
  const skipped = [];
  for (const hit of organic || []) {
    const screen = screenSerpHitForPast(hit, opts);
    if (screen.skip) skipped.push({ ...hit, skipReason: screen.reason, skipDetail: screen.detail });
    else kept.push(hit);
  }
  return { kept, skipped, skipPastCount: skipped.length };
}

export function classifySourceAuthority(url = "", title = "") {
  const u = String(url || "").toLowerCase();
  const t = String(title || "").toLowerCase();
  if (!u) return SOURCE_AUTHORITY.UNKNOWN;
  if (/\.gov\b|sam\.gov|usaspending|gsa\.gov/.test(u)) return SOURCE_AUTHORITY.OFFICIAL_GOVERNMENT;
  if (/\.edu\b/.test(u)) return SOURCE_AUTHORITY.OFFICIAL_UNIVERSITY;
  if (/hospital|nih\.gov|clinic|healthsystem/.test(u)) return SOURCE_AUTHORITY.OFFICIAL_HOSPITAL;
  if (/housing|hotelexec|passkey|onpeak|conferencehousing/.test(u)) {
    return SOURCE_AUTHORITY.OFFICIAL_HOUSING_PROVIDER;
  }
  if (/venue|convention.?center|marriott|hilton|hyatt/.test(u) && /official|host/.test(t)) {
    return SOURCE_AUTHORITY.OFFICIAL_VENUE;
  }
  if (/\.org\b/.test(u) && /(annual|conference|meeting|symposium|housing)/.test(t + u)) {
    return SOURCE_AUTHORITY.OFFICIAL_EVENT;
  }
  if (/\.org\b/.test(u)) return SOURCE_AUTHORITY.OFFICIAL_ORGANIZATION;
  if (/reuters|bloomberg|wsj|washingtonpost|nytimes|modernhealthcare/.test(u)) {
    return SOURCE_AUTHORITY.CREDIBLE_NEWS;
  }
  if (/eventbrite|10times|cvent|guidebook|yelp|tripadvisor/.test(u)) {
    return SOURCE_AUTHORITY.SECONDARY;
  }
  return SOURCE_AUTHORITY.UNKNOWN;
}

/**
 * Build durable source object — must survive extract → hygiene → qualify.
 */
export function buildSourceObject({
  url,
  title,
  sourceType,
  sourceDate,
  retrievedAt,
} = {}) {
  const sourceUrl = url || null;
  let sourceDomain = null;
  try {
    sourceDomain = sourceUrl ? new URL(sourceUrl).hostname.replace(/^www\./, "") : null;
  } catch {
    sourceDomain = null;
  }
  const sourceAuthority = classifySourceAuthority(sourceUrl, title);
  const isOfficial = String(sourceAuthority).startsWith("OFFICIAL_");
  return {
    sourceUrl,
    sourceDomain,
    sourceTitle: title || null,
    sourceType: sourceType || (isOfficial ? "OFFICIAL" : "UNKNOWN"),
    sourceAuthority,
    sourceDate: sourceDate || null,
    isOfficial,
    retrievedAt: retrievedAt || new Date().toISOString(),
  };
}

/**
 * Preserve official URL + evidenceSources onto mapped opportunity.
 */
export function attachSourceLineage(opp = {}, extras = {}) {
  const primaryUrl =
    extras.officialSource ||
    opp.officialSource ||
    (opp.evidenceSources || [])[0]?.url ||
    (opp.sources || [])[0]?.url ||
    (opp.evidence || [])[0]?.sourceUrl ||
    null;
  const primaryTitle =
    extras.officialSourceTitle ||
    (opp.evidenceSources || [])[0]?.title ||
    opp.title ||
    null;
  const primary = buildSourceObject({
    url: primaryUrl,
    title: primaryTitle,
    sourceType: extras.sourceType,
    sourceDate: extras.sourceDate,
  });

  const evidenceSources = Array.isArray(opp.evidenceSources)
    ? [...opp.evidenceSources]
    : [];
  if (primary.sourceUrl && !evidenceSources.some((e) => (e.url || e) === primary.sourceUrl)) {
    evidenceSources.unshift({
      title: primary.sourceTitle || "Official source",
      url: primary.sourceUrl,
      authority: primary.sourceAuthority,
      isOfficial: primary.isOfficial,
    });
  }

  return {
    ...opp,
    officialSource: primary.sourceUrl || opp.officialSource || null,
    officialSourceTitle: primary.sourceTitle || opp.officialSourceTitle || null,
    sourceAuthority: primary.sourceAuthority,
    isOfficialSource: primary.isOfficial,
    sourceDomain: primary.sourceDomain,
    sourceObject: primary,
    evidenceSources,
    retrievedAt: primary.retrievedAt,
  };
}

export function classifyPageSignal(text = "", url = "") {
  const blob = `${text}\n${url}`;
  if (SUPPLY_SIDE_RE.test(blob)) return PAGE_SIGNAL_CLASS.SUPPLY_SIDE_PAGE;
  if (REFERENCE_ONLY_RE.test(blob)) return PAGE_SIGNAL_CLASS.REFERENCE_ONLY;
  if (PAST_TITLE_RE.test(blob) && !FUTURE_SIGNAL_RE.test(blob)) {
    return PAGE_SIGNAL_CLASS.ARCHIVE_PAST;
  }
  return PAGE_SIGNAL_CLASS.DEMAND_SIGNAL;
}

/**
 * Separate organization location from event/program location.
 * Never copy org HQ into eventLocation.
 */
export function separateGeographyFields(opp = {}) {
  const orgLoc =
    opp.organizationLocation ||
    opp.organizationCity ||
    opp.headquartersLocation ||
    null;
  const eventLoc =
    opp.eventLocation ||
    opp.programLocation ||
    opp.projectLocation ||
    opp.venueLocation ||
    opp.hotelDemandLocation ||
    null;
  const rawDest = opp.destinationStatus || opp.location || opp.city || null;

  let eventLocation = eventLoc;
  let organizationLocation = orgLoc;
  let geoConflict = false;
  let locationEvidenceType = opp.locationEvidenceType || null;

  // If only rawDest and it looks like HQ language, do not use as event geo
  const hqish = /\b(?:headquarters|hq|based\s+in|headquartered)\b/i.test(
    String(opp.geographyEvidence || opp.locationEvidenceText || rawDest || "")
  );

  if (!eventLocation && rawDest && !hqish) {
    eventLocation = rawDest;
    locationEvidenceType = locationEvidenceType || "EVENT_OR_PROGRAM";
  }
  if (!organizationLocation && hqish && rawDest) {
    organizationLocation = rawDest;
  }

  if (
    organizationLocation &&
    eventLocation &&
    normCity(organizationLocation) !== normCity(eventLocation) &&
    normCity(organizationLocation) &&
    normCity(eventLocation)
  ) {
    geoConflict = true;
  }

  // Prefer event/program city for commercial destination
  const destinationStatus = eventLocation || (hqish ? null : rawDest) || null;

  return {
    ...opp,
    organizationLocation: organizationLocation || null,
    eventLocation: eventLocation || null,
    programLocation: opp.programLocation || eventLocation || null,
    projectLocation: opp.projectLocation || null,
    venueLocation: opp.venueLocation || opp.venue || null,
    hotelDemandLocation: opp.hotelDemandLocation || eventLocation || null,
    destinationStatus,
    location: eventLocation || destinationStatus,
    locationEvidenceText: opp.locationEvidenceText || opp.geographyEvidence || null,
    locationEvidenceUrl: opp.locationEvidenceUrl || opp.officialSource || null,
    locationEvidenceType,
    geoConflict,
    geoClass: geoConflict ? "GEO_CONFLICT" : opp.geoClass || null,
  };
}

function normCity(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\b(?:usa|us|united states|md|va|dc|california|ca)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Detect if claimed event city conflicts with hotel market (simple token check).
 */
export function geoConflictsWithHotelMarket(opp = {}, marketTokens = []) {
  const geo = separateGeographyFields(opp);
  if (geo.geoConflict) return { conflict: true, reason: "ORG_VS_EVENT", geo };
  const rawLoc = String(
    geo.eventLocation || geo.destinationStatus || opp.locationEvidenceText || ""
  );
  const loc = normCity(rawLoc);
  if (!rawLoc.trim()) return { conflict: false, reason: null, geo };
  const tokens = (marketTokens || []).map((t) => normCity(t)).filter(Boolean);
  if (!tokens.length) return { conflict: false, reason: null, geo };

  const marketBlob = tokens.join(" ");
  // Use RAW location for far-state detection (normCity may strip state tokens)
  if (
    /\bcalifornia\b/i.test(rawLoc) &&
    !/\bcalifornia\b/i.test(marketBlob) &&
    !/\b(?:bethesda|washington|dmv|montgomery|arlington|virginia|maryland)\b/i.test(rawLoc)
  ) {
    return { conflict: true, reason: "FAR_STATE_VS_MARKET", geo };
  }
  const farStates =
    /\b(?:texas|florida|illinois|massachusetts|colorado|arizona|oregon|georgia|nevada)\b/i;
  if (
    farStates.test(rawLoc) &&
    !farStates.test(marketBlob) &&
    !tokens.some((t) => loc.includes(t))
  ) {
    return { conflict: true, reason: "FAR_STATE_VS_MARKET", geo };
  }
  return { conflict: false, reason: null, geo };
}

export function extractCommercialHousingStatus(text = "") {
  const blob = String(text || "");
  if (!HOUSING_COMMERCIAL_RE.test(blob)) {
    return {
      venueStatus: COMMERCIAL_STATUS.UNKNOWN,
      housingStatus: COMMERCIAL_STATUS.UNKNOWN,
      sourcingStatus: COMMERCIAL_STATUS.UNKNOWN,
      commercialEvidenceText: null,
    };
  }
  let housingStatus = COMMERCIAL_STATUS.ANNOUNCED;
  let venueStatus = COMMERCIAL_STATUS.UNKNOWN;
  let sourcingStatus = COMMERCIAL_STATUS.OPEN;
  if (/\bfully\s+placed|sold\s+out|block\s+closed\b/i.test(blob)) {
    housingStatus = COMMERCIAL_STATUS.FULLY_PLACED;
    sourcingStatus = COMMERCIAL_STATUS.BLOCK_CLOSED;
  } else if (/\boverflow\b/i.test(blob)) {
    housingStatus = COMMERCIAL_STATUS.OVERFLOW_POSSIBLE;
  } else if (/\bhousing\s+coming|accommodations?\s+coming|forthcoming\b/i.test(blob)) {
    housingStatus = COMMERCIAL_STATUS.HOUSING_FORTHCOMING;
  } else if (/\bhotel\s+block|room\s+block|housing\s+open|official\s+hotels?\b/i.test(blob)) {
    housingStatus = COMMERCIAL_STATUS.HOUSING_OPEN;
    sourcingStatus = COMMERCIAL_STATUS.BLOCK_OPEN;
  } else if (/\bhotel\s+tbd|venue\s+tbd\b/i.test(blob)) {
    venueStatus = COMMERCIAL_STATUS.TBD;
    sourcingStatus = COMMERCIAL_STATUS.TBD;
  }
  const m = blob.match(HOUSING_COMMERCIAL_RE);
  return {
    venueStatus,
    housingStatus,
    sourcingStatus,
    commercialEvidenceText: m ? m[0] : null,
  };
}

/**
 * Classify WATCH row for second-pass eligibility.
 */
export function classifyWatchRecovery(opp = {}, { marketTokens = [] } = {}) {
  const pageClass = classifyPageSignal(
    `${opp.title || ""} ${opp.summaryWhat || ""} ${opp.housingEvidence || ""}`,
    opp.officialSource || ""
  );
  if (
    pageClass === PAGE_SIGNAL_CLASS.SUPPLY_SIDE_PAGE ||
    pageClass === PAGE_SIGNAL_CLASS.REFERENCE_ONLY
  ) {
    return { class: WATCH_RECOVERY_CLASS.NOISE, reason: pageClass };
  }
  if (opp.actionabilityV3 === "INVALID" || opp.hygieneV3?.failureClass === "PAST_EVENT") {
    return { class: WATCH_RECOVERY_CLASS.SKIP, reason: "PAST_OR_INVALID" };
  }

  const geoCheck = geoConflictsWithHotelMarket(opp, marketTokens);
  if (geoCheck.conflict) {
    return { class: WATCH_RECOVERY_CLASS.WATCH_VERIFY_GEO, reason: geoCheck.reason, geo: geoCheck.geo };
  }

  const hasOfficial =
    Boolean(opp.officialSource) ||
    Boolean(opp.isOfficialSource) ||
    (opp.evidenceSources || []).some((e) => e.isOfficial || String(e.authority || "").startsWith("OFFICIAL"));

  if (!hasOfficial) {
    return { class: WATCH_RECOVERY_CLASS.WATCH_VERIFY_SOURCE, reason: "SOURCE_WEAK" };
  }

  const fail = String(opp.hygieneV3?.failureClass || opp.failureClass || "");
  if (/NO_OPEN_SOURCING|OPEN_SOURCING/i.test(fail) || !opp.housingEvidence) {
    return { class: WATCH_RECOVERY_CLASS.WATCH_VERIFY_HOUSING, reason: fail || "NO_HOUSING" };
  }

  return { class: WATCH_RECOVERY_CLASS.WATCH_KEEP, reason: "CREDIBLE_WATCH" };
}

/**
 * Build bounded second-pass queries from missing commercial fields.
 */
export function buildSecondPassQueries(opp = {}, { maxQueries = 4 } = {}) {
  const title = String(opp.title || "").replace(/[^\w\s&+-]/g, " ").trim();
  const org = String(opp.organizationName || "").trim();
  const domain = opp.sourceDomain || null;
  const recovery = classifyWatchRecovery(opp);
  const qs = [];

  if (recovery.class === WATCH_RECOVERY_CLASS.WATCH_VERIFY_SOURCE || !opp.officialSource) {
    qs.push(`"${title}" official`);
    if (org) qs.push(`"${org}" "${title}" site:.org OR site:.gov OR site:.edu`);
    if (domain) qs.push(`site:${domain} ${title}`);
  }
  if (
    recovery.class === WATCH_RECOVERY_CLASS.WATCH_VERIFY_HOUSING ||
    recovery.class === WATCH_RECOVERY_CLASS.WATCH_KEEP
  ) {
    qs.push(`"${title}" hotel OR housing OR accommodations OR "room block"`);
    qs.push(`"${title}" RFP OR "official hotel" OR "host hotel"`);
  }
  if (recovery.class === WATCH_RECOVERY_CLASS.WATCH_VERIFY_GEO) {
    qs.push(`"${title}" venue OR location OR "Washington" OR Bethesda OR "Washington DC"`);
    if (org) qs.push(`"${org}" "${title}" 2026 OR 2027 location`);
  }
  return {
    recoveryClass: recovery.class,
    reason: recovery.reason,
    queries: [...new Set(qs)].slice(0, maxQueries),
  };
}

/** Future-biased query suffix for live lanes. */
export const LIVE_QUERY_FUTURE_BIAS =
  "(2026 OR 2027 OR upcoming OR \"registration open\" OR housing OR hotel OR accommodations OR RFP)";

/**
 * Enrich query hints with future/housing bias (does not replace lane hints).
 */
export function withFutureHousingBias(hint, geos = []) {
  const geo = geos[0] ? `"${geos[0]}"` : "";
  return `${geo} ${hint} ${LIVE_QUERY_FUTURE_BIAS}`.replace(/\s+/g, " ").trim();
}

/**
 * Full candidate enrichment pass before hygiene.
 */
export function enrichCandidateRoutingV12(opp = {}, { nowDate = null, marketTokens = [] } = {}) {
  let next = scrubYearFloorForHygiene(opp, { nowDate });
  next = attachSourceLineage(next);
  next = separateGeographyFields(next);
  const pageClass = classifyPageSignal(
    `${next.title || ""} ${next.summaryWhat || ""} ${next.housingEvidence || ""} ${next.organizationName || ""}`,
    next.officialSource || ""
  );
  next.pageSignalClass = pageClass;
  const commercial = extractCommercialHousingStatus(
    `${next.housingEvidence || ""} ${next.venueSourcingRationale || ""} ${next.summaryWhat || ""}`
  );
  if (commercial.commercialEvidenceText) {
    next.venueStatus = next.venueStatus || commercial.venueStatus;
    next.housingStatus = next.housingStatus || commercial.housingStatus;
    next.sourcingStatusCommercial = commercial.sourcingStatus;
    next.commercialEvidenceText = commercial.commercialEvidenceText;
    next.commercialEvidenceUrl = next.officialSource || null;
    next.commercialEvidenceStatus = commercial.housingStatus;
  }
  // Official hotel/housing page URL is itself commercial evidence
  const offUrl = String(next.officialSource || "");
  if (
    /(?:\/(?:hotel|hotels|housing|accommodations?|lodging|travel|room-block)|hotel[-_]?travel|travel[-_]?venue|housing[-_]?information|hotel[-_]?transport)/i.test(
      offUrl
    ) &&
    !next.housingEvidence
  ) {
    next.housingEvidence = `Official housing/hotel page: ${offUrl}`;
    next.housingStatus = next.housingStatus || COMMERCIAL_STATUS.HOUSING_OPEN;
    next.openSourcingEvidence = true;
    next.commercialEvidenceUrl = offUrl;
    next.commercialEvidenceStatus =
      next.commercialEvidenceStatus || COMMERCIAL_STATUS.HOUSING_OPEN;
  }
  const geoCheck = geoConflictsWithHotelMarket(next, marketTokens);
  if (geoCheck.conflict) {
    next.geoConflict = true;
    next.geoConflictReason = geoCheck.reason;
  }
  if (pageClass === PAGE_SIGNAL_CLASS.SUPPLY_SIDE_PAGE || pageClass === PAGE_SIGNAL_CLASS.REFERENCE_ONLY) {
    next.routingRejectClass = pageClass;
  }
  return next;
}
