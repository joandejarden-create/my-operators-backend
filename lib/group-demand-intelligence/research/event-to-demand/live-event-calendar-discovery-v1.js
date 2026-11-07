/**
 * Live, source-grounded event calendar discovery.
 * Independent of seed/fixture packs — fetches public calendars via existing
 * official-page / research-page paths. Seed packs remain held-out recall checks.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { fetchOfficialPage } from "../../official-page-fetch.js";
import {
  EVENT_TO_DEMAND_VERSION,
  EVENT_DEMAND_DOMAIN,
} from "./event-to-demand-constants-v1.js";
import {
  normalizeVerifiedEvent,
  dedupeVerifiedEvents,
  isFutureEvent,
  mergeVerifiedEvents,
} from "./event-identity-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../..");

export const LIVE_EVENT_DISCOVERY_VERSION = "gdi-live-event-calendar-discovery-v1";

const DOMAIN_FROM_HINT = Object.freeze({
  association_professional_congress: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
  exhibition_trade_show: EVENT_DEMAND_DOMAIN.EXHIBITION_TRADE_SHOW,
  corporate_event_meeting: EVENT_DEMAND_DOMAIN.CORPORATE_EVENT_MEETING,
  medical_scientific_academic: EVENT_DEMAND_DOMAIN.MEDICAL_SCIENTIFIC_ACADEMIC,
  entertainment_luxury_cultural: EVENT_DEMAND_DOMAIN.ENTERTAINMENT_LUXURY_CULTURAL,
  destination_citywide: EVENT_DEMAND_DOMAIN.DESTINATION_CITYWIDE,
  event_intermediary: EVENT_DEMAND_DOMAIN.EVENT_INTERMEDIARY,
});

function text(v) {
  if (v && typeof v === "object") {
    return String(v.name || v.title || v.label || "").replace(/\s+/g, " ").trim();
  }
  return String(v || "").replace(/\s+/g, " ").trim();
}

function venueFromSource(source = {}) {
  const id = String(source.id || "").toLowerCase();
  const url = String(source.url || "").toLowerCase();
  if (id.includes("excel") || url.includes("excel.london")) return "ExCeL London";
  if (id.includes("olympia") || url.includes("olympia.london")) return "Olympia London";
  return null;
}

function isWeakEventTitle(name = "") {
  const n = text(name).toLowerCase();
  if (!n || n.length < 8) return true;
  if (/^(olympia|excel|events?|whats on|calendar|home)$/i.test(n)) return true;
  return false;
}

/** Pull organizer identity from detail-page prose when JSON-LD lacks it. */
export function extractOrganizerFromPageText(blob = "") {
  const s = String(blob || "");
  const patterns = [
    /(?:organised|organized|presented|hosted)\s+by\s+([A-Z][^.\n|]{2,80})/i,
    /(?:organiser|organizer)\s*[:|-]\s*([A-Z][^.\n|]{2,80})/i,
    /(?:brought\s+to\s+you\s+by)\s+([A-Z][^.\n|]{2,80})/i,
  ];
  for (const re of patterns) {
    const m = s.match(re);
    if (m?.[1]) {
      const name = text(m[1]).replace(/\s+and\s+partners$/i, "").slice(0, 120);
      if (name.length >= 3 && !isWeakEventTitle(name)) return name;
    }
  }
  return null;
}

function absolutePaginationUrls(source = {}) {
  const paths = Array.isArray(source.paginationPaths) ? source.paginationPaths : [];
  const out = [];
  let base;
  try {
    base = new URL(source.url);
  } catch {
    return out;
  }
  for (const p of paths) {
    try {
      out.push(new URL(p, base).href);
    } catch {
      /* ignore */
    }
  }
  return out;
}

function addMonths(date, months) {
  const d = new Date(date);
  d.setUTCMonth(d.getUTCMonth() + Number(months || 0));
  return d;
}

/**
 * Load market calendar source registry (data-driven; not code-hardcoded).
 */
export function loadEventCalendarSources({
  marketKey,
  root = ROOT,
  registryPath = null,
} = {}) {
  const p =
    registryPath ||
    path.join(
      root,
      "fixtures/group-demand-intelligence/event-to-demand/event-calendar-sources-v1.json"
    );
  if (!fs.existsSync(p)) {
    return { sources: [], loaded: null, error: "registry_missing", defaultHorizonMonths: 18 };
  }
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const key = String(marketKey || "").toLowerCase();
  const sources = Array.isArray(j.markets?.[key]) ? j.markets[key] : [];
  return {
    sources,
    loaded: p,
    version: j.version,
    defaultHorizonMonths: Number(j.defaultHorizonMonths || 18),
    marketKey: key || null,
  };
}

/**
 * Extract ISO-ish dates from free text / JSON-LD.
 */
export function extractDatesFromText(blob = "", { asOf = new Date() } = {}) {
  const s = String(blob || "");
  const out = [];
  const iso = s.matchAll(/\b(20\d{2})-(\d{2})-(\d{2})\b/g);
  for (const m of iso) {
    out.push(`${m[1]}-${m[2]}-${m[3]}`);
  }
  const named = s.matchAll(
    /\b(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})\b/gi
  );
  const months = {
    january: "01",
    february: "02",
    march: "03",
    april: "04",
    may: "05",
    june: "06",
    july: "07",
    august: "08",
    september: "09",
    october: "10",
    november: "11",
    december: "12",
  };
  for (const m of named) {
    const dd = String(m[1]).padStart(2, "0");
    const mm = months[m[2].toLowerCase()];
    if (mm) out.push(`${m[3]}-${mm}-${dd}`);
  }
  // Unique, future-preferring
  const uniq = [...new Set(out)];
  const asOfDay = asOf.toISOString().slice(0, 10);
  return uniq.filter((d) => d >= asOfDay || d.slice(0, 4) >= asOfDay.slice(0, 4));
}

function titleFromUrl(url) {
  try {
    const u = new URL(url);
    const last = u.pathname.split("/").filter(Boolean).pop() || u.hostname;
    return text(decodeURIComponent(last).replace(/[-_]+/g, " ")).slice(0, 120);
  } catch {
    return "";
  }
}

function eventNameFromJsonLd(node) {
  return text(node?.name || node?.headline || "");
}

function startFromJsonLd(node) {
  const raw = node?.startDate || node?.start || "";
  const m = String(raw).match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : null;
}

function venueFromJsonLd(node) {
  const loc = node?.location;
  if (!loc) return null;
  if (typeof loc === "string") return text(loc);
  return text(loc.name || loc.address?.addressLocality || "");
}

/**
 * Parse a fetched calendar/detail page into candidate event stubs.
 */
export function parseCalendarPageToEventStubs({
  page,
  source,
  marketCity = null,
  asOf = new Date(),
  horizonEnd = null,
} = {}) {
  const retrievedAt = new Date().toISOString();
  const stubs = [];
  const family = source?.sourceFamily || "industry_calendar";
  const domain =
    DOMAIN_FROM_HINT[source?.domainHint] || EVENT_DEMAND_DOMAIN.DESTINATION_CITYWIDE;
  const errors = [];

  if (!page?.ok) {
    errors.push({
      sourceId: source?.id,
      url: source?.url,
      error: page?.error || "fetch_failed",
    });
    return { stubs, errors, retrievedAt };
  }

  const jsonLd = Array.isArray(page.jsonLdEvents) ? page.jsonLdEvents : [];
  for (const node of jsonLd) {
    const name = eventNameFromJsonLd(node);
    const start = startFromJsonLd(node);
    if (!name || !start || isWeakEventTitle(name)) continue;
    if (horizonEnd && start > horizonEnd.toISOString().slice(0, 10)) continue;
    stubs.push({
      eventName: name,
      eventStartDate: start,
      venue: venueFromJsonLd(node) || venueFromSource(source),
      city: marketCity || null,
      organizer: text(node.organizer?.name || node.organizer) || null,
      officialUrl: text(node.url) || page.finalUrl || source.url,
      domain,
      verified: true,
      verificationBasis: "json_ld_event",
      sources: [
        {
          url: page.finalUrl || source.url,
          sourceFamily: family,
          retrievedAt,
          fetchMethod: page.fetchMethod || null,
        },
      ],
      sourceFamilies: [family],
      liveDiscovered: true,
      liveSourceId: source.id,
    });
  }

  // Detail links → provisional stubs (need date from link text or page text)
  const links = Array.isArray(page.eventDetailLinks) ? page.eventDetailLinks : [];
  const pageDates = extractDatesFromText(page.text || "", { asOf });
  const pageStart = pageDates[0] || null;

  // If the calendar page itself is a single-event landing page
  if (!stubs.length && pageStart) {
    const name =
      text(source.label) ||
      titleFromUrl(page.finalUrl || source.url) ||
      "";
    if (!isWeakEventTitle(name)) {
      stubs.push({
        eventName: name,
        eventStartDate: pageStart,
        venue: venueFromSource(source),
        city: marketCity || null,
        officialUrl: page.finalUrl || source.url,
        domain,
        verified: true,
        verificationBasis: "calendar_page_dated_text",
        sources: [
          {
            url: page.finalUrl || source.url,
            sourceFamily: family,
            retrievedAt,
            fetchMethod: page.fetchMethod || null,
          },
        ],
        sourceFamilies: [family],
        liveDiscovered: true,
        liveSourceId: source.id,
      });
    }
  }

  for (const link of links.slice(0, Number(source.maxDetailPages || 6))) {
    const linkDates = extractDatesFromText(link, { asOf });
    const start = linkDates[0] || null;
    const name = titleFromUrl(link);
    if (isWeakEventTitle(name) && !start) continue;
    stubs.push({
      eventName: isWeakEventTitle(name) ? "" : name,
      eventStartDate: start,
      venue: venueFromSource(source),
      city: marketCity || null,
      officialUrl: link,
      domain,
      verified: Boolean(start) && !isWeakEventTitle(name || "x"),
      verificationBasis: start ? "detail_url_dated" : "detail_url_undated_pending",
      sources: [
        {
          url: link,
          sourceFamily: family,
          retrievedAt,
          parentCalendarUrl: source.url,
        },
      ],
      sourceFamilies: [family],
      liveDiscovered: true,
      liveSourceId: source.id,
      needsDetailFetch: !start || isWeakEventTitle(name),
    });
  }

  return { stubs, errors, retrievedAt };
}

/**
 * Fetch detail pages for undated stubs; promote when a future date is found.
 */
export async function enrichEventStubsWithDetails({
  stubs = [],
  asOf = new Date(),
  horizonEnd = null,
  maxDetailFetches = 20,
  fetchPage = fetchOfficialPage,
  allowRender = false,
  renderBudget = null,
} = {}) {
  const out = [];
  const errors = [];
  let fetches = 0;
  for (const stub of stubs) {
    if (stub.verified && stub.eventStartDate && !stub.needsDetailFetch) {
      // Still enrich organizer when missing
      if (stub.organizer) {
        out.push(stub);
        continue;
      }
    }
    if (fetches >= maxDetailFetches || !stub.officialUrl) {
      if (stub.verified && stub.eventStartDate && !isWeakEventTitle(stub.eventName)) {
        out.push(stub);
      }
      continue;
    }
    fetches += 1;
    let page;
    try {
      page = await fetchPage(stub.officialUrl, {
        allowRender: allowRender === true,
        renderBudget: renderBudget || undefined,
        timeoutMs: 20000,
      });
    } catch (err) {
      errors.push({
        url: stub.officialUrl,
        error: err?.message || String(err),
      });
      continue;
    }
    if (!page?.ok) {
      errors.push({
        url: stub.officialUrl,
        error: page?.error || "detail_fetch_failed",
      });
      continue;
    }
    const dates = extractDatesFromText(page.text || "", { asOf });
    const jsonStart = (page.jsonLdEvents || [])
      .map(startFromJsonLd)
      .filter(Boolean)[0];
    const start = jsonStart || dates[0] || stub.eventStartDate || null;
    if (!start) {
      errors.push({ url: stub.officialUrl, error: "no_future_date_on_detail" });
      continue;
    }
    if (horizonEnd && start > horizonEnd.toISOString().slice(0, 10)) continue;
    const jsonName = (page.jsonLdEvents || [])
      .map(eventNameFromJsonLd)
      .find(Boolean);
    const resolvedName = jsonName || stub.eventName || titleFromUrl(stub.officialUrl);
    if (isWeakEventTitle(resolvedName)) {
      errors.push({ url: stub.officialUrl, error: "weak_event_title_after_detail" });
      continue;
    }
    const organizer =
      text(stub.organizer) ||
      text((page.jsonLdEvents || [])[0]?.organizer) ||
      extractOrganizerFromPageText(page.text || "") ||
      null;
    out.push({
      ...stub,
      eventName: resolvedName,
      eventStartDate: start,
      venue:
        stub.venue ||
        venueFromJsonLd((page.jsonLdEvents || [])[0]) ||
        null,
      organizer,
      verified: true,
      verificationBasis: jsonStart ? "detail_json_ld" : "detail_page_dated_text",
      needsDetailFetch: false,
      sources: [
        ...(stub.sources || []),
        {
          url: page.finalUrl || stub.officialUrl,
          sourceFamily: stub.sourceFamilies?.[0] || "official_event",
          retrievedAt: new Date().toISOString(),
          fetchMethod: page.fetchMethod || null,
        },
      ],
    });
  }
  return { events: out, errors, detailFetches: fetches };
}

/**
 * Run live calendar discovery for a market (hotel-agnostic sources + hotel city).
 *
 * @param {object} args
 * @param {string} args.marketKey
 * @param {object} [args.hotel]
 * @param {number} [args.horizonMonths]
 * @param {boolean} [args.includeSeedPack] - default false for held-out live runs
 * @param {Function} [args.fetchPage] - injectable for tests
 */
export async function runLiveEventCalendarDiscovery({
  marketKey,
  hotel = {},
  horizonMonths = null,
  asOf = new Date(),
  includeSeedPack = false,
  maxSources = 12,
  maxDetailFetches = 24,
  fetchPage = fetchOfficialPage,
  root = ROOT,
  outDir = null,
} = {}) {
  const registry = loadEventCalendarSources({ marketKey, root });
  const months = Number(horizonMonths || registry.defaultHorizonMonths || 18);
  const horizonEnd = addMonths(asOf, months);
  const city = hotel.city || null;

  const sourceErrors = [];
  const allStubs = [];
  const sourcesUsed = [];
  const renderBudget = { remaining: 12 };

  for (const source of (registry.sources || []).slice(0, maxSources)) {
    const urls = [source.url, ...absolutePaginationUrls(source)];
    sourcesUsed.push({
      id: source.id,
      url: source.url,
      sourceFamily: source.sourceFamily,
      allowRender: source.allowRender === true,
      paginationUrls: urls.length - 1,
    });
    for (const calendarUrl of urls) {
      let page;
      try {
        page = await fetchPage(calendarUrl, {
          allowRender: source.allowRender === true,
          renderBudget,
          timeoutMs: 25000,
        });
      } catch (err) {
        sourceErrors.push({
          sourceId: source.id,
          url: calendarUrl,
          error: err?.message || String(err),
        });
        continue;
      }
      const parsed = parseCalendarPageToEventStubs({
        page,
        source: { ...source, url: calendarUrl },
        marketCity: city,
        asOf,
        horizonEnd,
      });
      sourceErrors.push(...parsed.errors);
      allStubs.push(...parsed.stubs);
    }
  }

  const enriched = await enrichEventStubsWithDetails({
    stubs: allStubs,
    asOf,
    horizonEnd,
    maxDetailFetches,
    fetchPage,
    allowRender: true,
    renderBudget,
  });

  let events = dedupeVerifiedEvents(
    enriched.events
      .filter((e) => e.verified === true && e.eventStartDate)
      .map((e) => normalizeVerifiedEvent(e))
      .filter((e) => isFutureEvent(e, asOf))
  );

  // Optional merge with seed pack (not used for held-out recall runs)
  let seedMerged = 0;
  if (includeSeedPack) {
    const seedPath = path.join(
      root,
      "fixtures/group-demand-intelligence/event-to-demand",
      `${marketKey}-events-v1.json`
    );
    if (fs.existsSync(seedPath)) {
      try {
        const j = JSON.parse(fs.readFileSync(seedPath, "utf8"));
        const seedEvents = (Array.isArray(j) ? j : j.events || []).map(
          normalizeVerifiedEvent
        );
        const byKey = new Map(events.map((e) => [e.eventIdentityKey, e]));
        for (const s of seedEvents) {
          const k = s.eventIdentityKey;
          if (byKey.has(k)) {
            byKey.set(k, mergeVerifiedEvents(byKey.get(k), s));
            seedMerged += 1;
          } else {
            byKey.set(k, { ...s, seedPackOnly: true });
          }
        }
        events = [...byKey.values()];
      } catch (err) {
        sourceErrors.push({ error: `seed_merge_failed:${err?.message || err}` });
      }
    }
  }

  const families = new Set();
  for (const e of events) {
    for (const f of e.sourceFamilies || []) families.add(f);
    for (const s of e.sources || []) if (s.sourceFamily) families.add(s.sourceFamily);
  }

  const result = {
    version: LIVE_EVENT_DISCOVERY_VERSION,
    eventToDemandVersion: EVENT_TO_DEMAND_VERSION,
    marketKey: registry.marketKey,
    mode: "live_calendar",
    includeSeedPack: includeSeedPack === true,
    asOf: asOf.toISOString(),
    horizonMonths: months,
    horizonEnd: horizonEnd.toISOString().slice(0, 10),
    registryLoaded: registry.loaded,
    sourcesConfigured: (registry.sources || []).length,
    sourcesAttempted: sourcesUsed.length,
    sourcesUsed,
    sourceErrors,
    detailFetches: enriched.detailFetches,
    detailErrors: enriched.errors,
    seedMerged,
    events,
    metrics: {
      verifiedFutureEvents: events.length,
      liveDiscovered: events.filter((e) => e.liveDiscovered).length,
      sourceFamilyCount: families.size,
      sourceFamilies: [...families],
      stubCount: allStubs.length,
      sourceErrorCount: sourceErrors.length,
    },
    note:
      events.length === 0
        ? "live_discovery_zero_yield_or_source_errors"
        : `live_verified_future_events_${events.length}`,
  };

  if (outDir) {
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(
      path.join(outDir, "live-event-calendar-discovery-result.json"),
      JSON.stringify(result, null, 2) + "\n"
    );
  }

  return result;
}

/**
 * Compare live-discovered events against a held-out benchmark (no seeding from it).
 */
export function compareLiveDiscoveryToBenchmark({
  liveEvents = [],
  benchmarkEvents = [],
} = {}) {
  const liveKeys = new Set(
    liveEvents.map((e) => e.eventIdentityKey || normalizeVerifiedEvent(e).eventIdentityKey)
  );
  const liveNames = liveEvents.map((e) =>
    String(e.eventName || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim()
  );

  const rows = benchmarkEvents.map((raw) => {
    const b = normalizeVerifiedEvent(raw);
    const nameKey = String(b.eventName || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
    const exact = liveKeys.has(b.eventIdentityKey);
    const fuzzy = liveNames.some(
      (n) => n && nameKey && (n.includes(nameKey) || nameKey.includes(n))
    );
    return {
      eventId: b.eventId,
      eventName: b.eventName,
      recalled: exact || fuzzy,
      matchType: exact ? "identity" : fuzzy ? "name_fuzzy" : null,
    };
  });

  const recalled = rows.filter((r) => r.recalled).length;
  const newNotInBenchmark = liveEvents.filter((e) => {
    const name = String(e.eventName || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
    return !benchmarkEvents.some((b) => {
      const bn = String(b.eventName || "")
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, " ")
        .trim();
      return (
        e.eventIdentityKey === normalizeVerifiedEvent(b).eventIdentityKey ||
        (name && bn && (name.includes(bn) || bn.includes(name)))
      );
    });
  });

  return {
    benchmarkCount: rows.length,
    recalled,
    recallRate: rows.length ? recalled / rows.length : 0,
    newNotInBenchmark: newNotInBenchmark.length,
    newEvents: newNotInBenchmark.map((e) => ({
      eventId: e.eventId,
      eventName: e.eventName,
      eventStartDate: e.eventStartDate,
      liveSourceId: e.liveSourceId || null,
    })),
    rows,
  };
}
