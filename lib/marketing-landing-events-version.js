/**
 * Landing-site version filters for the marketing analytics report.
 * Preserves append-only history: previous iframe landing (v7/v8/v9) vs new Webflow site (old-home).
 */

import {
  LOCALE_LABELS,
  parseLocaleFilter,
  filterEventsByLocale,
  buildLocaleCompare,
} from "./marketing-landing-events-locale.js";

export const VERSION_ALL = "all";
export const VERSION_PREVIOUS = "previous";
export const VERSION_OLD_HOME = "old-home";

export const VERSION_LABELS = {
  [VERSION_ALL]: "All Sites",
  [VERSION_PREVIOUS]: "Previous Landing (v7–v9)",
  [VERSION_OLD_HOME]: "New Site (old-home)",
};

function parseIso(value) {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

/**
 * @param {unknown} value
 * @returns {string|null}
 */
export function normalizeLandingVersion(value) {
  const v = String(value || "")
    .trim()
    .toLowerCase();
  if (!v) return null;
  if (v === "old-home" || v === "oldhome" || v === "new" || v === "new-site") {
    return VERSION_OLD_HOME;
  }
  if (v === "previous" || v === "legacy" || v === "iframe") {
    return VERSION_PREVIOUS;
  }
  if (/^v\d+$/.test(v)) return v;
  return v.slice(0, 16);
}

/**
 * @param {unknown} value
 * @returns {"all"|"previous"|"old-home"|string}
 */
export function parseVersionFilter(value) {
  const v = String(value || "all")
    .trim()
    .toLowerCase();
  if (!v || v === "all") return VERSION_ALL;
  if (v === "previous" || v === "legacy" || v === "v9" || v === "iframe") {
    return VERSION_PREVIOUS;
  }
  if (v === "old-home" || v === "oldhome" || v === "new" || v === "new-site") {
    return VERSION_OLD_HOME;
  }
  return v.slice(0, 16);
}

export function eventLandingVersion(event) {
  return normalizeLandingVersion(event?.landingVersion);
}

export function isPreviousLandingVersion(version) {
  return eventLandingVersion({ landingVersion: version }) !== VERSION_OLD_HOME;
}

/**
 * @param {Array<object>} events
 * @param {string} versionFilter
 */
export function filterEventsByLandingVersion(events, versionFilter) {
  const filter = parseVersionFilter(versionFilter);
  if (filter === VERSION_ALL) return events || [];
  if (filter === VERSION_PREVIOUS) {
    return (events || []).filter((e) => isPreviousLandingVersion(e.landingVersion));
  }
  if (filter === VERSION_OLD_HOME) {
    return (events || []).filter((e) => eventLandingVersion(e) === VERSION_OLD_HOME);
  }
  return (events || []).filter((e) => eventLandingVersion(e) === filter);
}

/**
 * Parse YYYY-MM-DD (or ISO) into start-of-day UTC ISO string.
 * @param {unknown} value
 * @returns {string|null}
 */
export function parseCutover(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    const d = new Date(`${raw}T00:00:00.000Z`);
    return Number.isNaN(d.getTime()) ? null : d.toISOString();
  }
  const d = parseIso(raw);
  return d ? d.toISOString() : null;
}

/**
 * @param {unknown} value
 * @returns {"all"|"before"|"after"}
 */
export function parseEraFilter(value) {
  const v = String(value || "all")
    .trim()
    .toLowerCase();
  if (v === "before" || v === "pre") return "before";
  if (v === "after" || v === "post" || v === "since") return "after";
  return "all";
}

/**
 * @param {Array<object>} events
 * @param {string|null} cutoverIso
 * @param {"all"|"before"|"after"} era
 */
export function filterEventsByEra(events, cutoverIso, era) {
  const mode = parseEraFilter(era);
  if (!cutoverIso || mode === "all") return events || [];
  const cut = new Date(cutoverIso).getTime();
  if (Number.isNaN(cut)) return events || [];
  return (events || []).filter((e) => {
    const ts = parseIso(e.ts);
    if (!ts) return false;
    const t = ts.getTime();
    if (mode === "before") return t < cut;
    return t >= cut;
  });
}

function countPageLandSessions(events) {
  const sessions = new Set();
  let pageLands = 0;
  let ctaClicks = 0;
  for (const e of events || []) {
    if (e.event === "page_land") {
      pageLands += 1;
      if (e.sessionId) sessions.add(e.sessionId);
    }
    if (e.event === "cta_click") ctaClicks += 1;
  }
  return {
    events: (events || []).length,
    sessions: sessions.size || pageLands,
    pageLands,
    ctaClicks,
  };
}

/**
 * Side-by-side before/after snapshot for a cutover date (does not mutate filters).
 * @param {Array<object>} events already version-filtered, still within days window
 * @param {string} cutoverIso
 */
export function buildCutoverCompare(events, cutoverIso) {
  const cut = parseCutover(cutoverIso);
  if (!cut) return null;
  const before = filterEventsByEra(events, cut, "before");
  const after = filterEventsByEra(events, cut, "after");
  return {
    cutover: cut,
    cutoverLabel: cut.slice(0, 10),
    before: countPageLandSessions(before),
    after: countPageLandSessions(after),
  };
}

/**
 * Parse report filter query params.
 * @param {Record<string, unknown>|null|undefined} query
 */
export function parseReportFilters(query = {}) {
  const version = parseVersionFilter(query?.version);
  const cutover = parseCutover(query?.cutover);
  const era = parseEraFilter(query?.era);
  const locale = parseLocaleFilter(query?.lang ?? query?.locale);
  return {
    version,
    cutover,
    era: cutover ? era : "all",
    locale,
    versionLabel: VERSION_LABELS[version] || version,
    localeLabel: LOCALE_LABELS[locale] || locale,
  };
}

/**
 * Apply version + era + locale filters. Returns filtered events and optional compare block.
 * Locale compare is always computed on the version/era-filtered set (before locale filter)
 * so EN/ES side-by-side remains visible when drilling into one language.
 * @param {Array<object>} events
 * @param {{ version?: string, cutover?: string|null, era?: string, locale?: string }} filters
 */
export function applyLandingReportFilters(events, filters = {}) {
  const version = parseVersionFilter(filters.version);
  const cutover = parseCutover(filters.cutover);
  const era = parseEraFilter(filters.era);
  const locale = parseLocaleFilter(filters.locale ?? filters.lang);
  const versioned = filterEventsByLandingVersion(events, version);
  const compare = cutover ? buildCutoverCompare(versioned, cutover) : null;
  const eraFiltered = filterEventsByEra(versioned, cutover, era);
  const localeCompare = buildLocaleCompare(eraFiltered);
  const filtered = filterEventsByLocale(eraFiltered, locale);
  return {
    events: filtered,
    compare,
    localeCompare,
    filters: {
      version,
      cutover,
      era: cutover ? era : "all",
      locale,
      versionLabel: VERSION_LABELS[version] || version,
      localeLabel: LOCALE_LABELS[locale] || locale,
    },
  };
}
