/**
 * Classify source URLs / adapters into SourceContentDomain + default SourceRole.
 */

import {
  SourceContentDomain,
  SourceRole,
  SOURCE_POLICY_VERSION,
} from "./source-roles.js";

/**
 * @param {string|null|undefined} urlOrHost
 * @param {{ sourceId?: string, title?: string, adapter?: string } } [ctx]
 */
export function classifySourceContentDomain(urlOrHost, ctx = {}) {
  const raw = String(urlOrHost || "").trim();
  const sourceId = String(ctx.sourceId || ctx.adapter || "").toLowerCase();
  const title = String(ctx.title || "").toLowerCase();
  let host = "";
  let path = "";
  try {
    if (/^https?:\/\//i.test(raw)) {
      const u = new URL(raw);
      host = u.hostname.toLowerCase();
      path = u.pathname.toLowerCase();
    } else {
      host = raw.toLowerCase().replace(/^www\./, "");
    }
  } catch {
    host = raw.toLowerCase();
  }
  host = host.replace(/^www\./, "");

  // Explicit adapter ids
  if (sourceId === "cvent" || sourceId === "cvent_venue") {
    return SourceContentDomain.CVENT_VENUE_HOTEL;
  }
  if (sourceId === "giata") return SourceContentDomain.GIATA;
  if (sourceId === "hotelbeds" || sourceId === "hbx") return SourceContentDomain.HBX;
  if (
    sourceId === "expedia" ||
    sourceId === "booking" ||
    sourceId === "ota_consumer_sites"
  ) {
    return SourceContentDomain.OTA;
  }
  if (sourceId === "hotel_brand_websites" || sourceId === "official_web") {
    return SourceContentDomain.FIRST_PARTY_HOTEL;
  }
  if (sourceId === "openstreetmap" || sourceId === "wikidata") {
    return SourceContentDomain.OSM_WIKIDATA;
  }

  // Cvent host split: venue/hotel pages vs event platform
  if (host.includes("cvent.com") || host === "cvent") {
    if (
      /\/venues\//.test(path) ||
      /\/hotel\//.test(path) ||
      /venue-[a-f0-9-]{36}/.test(path) ||
      /supplier.?network|venue profile/.test(title)
    ) {
      return SourceContentDomain.CVENT_VENUE_HOTEL;
    }
    // web.cvent.com event shells, registration, housing
    if (
      host.startsWith("web.cvent.com") ||
      /\/event\//.test(path) ||
      /\/summary|\/register|\/rfp|\/housing|\/attendee/.test(path) ||
      /registration|housing|event/.test(title)
    ) {
      return SourceContentDomain.CVENT_EVENT_PLATFORM;
    }
    // Ambiguous cvent.com without /venues → treat conservatively as venue/hotel
    // when path looks property-like; else event platform for web.cvent, else OTHER.
    if (/hotel|venue|meeting.?space|ballroom/.test(`${path} ${title}`)) {
      return SourceContentDomain.CVENT_VENUE_HOTEL;
    }
    if (host.startsWith("web.")) return SourceContentDomain.CVENT_EVENT_PLATFORM;
    // Default ambiguous www.cvent.com → venue/hotel (safer for property facts)
    return SourceContentDomain.CVENT_VENUE_HOTEL;
  }

  if (
    /marriott\.com|hilton\.com|ihg\.com|hyatt\.com|accor\.com|wyndham|choicehotels|radisson|yotel\.com/.test(
      host
    )
  ) {
    return SourceContentDomain.FIRST_PARTY_HOTEL;
  }
  if (/expedia\.|booking\.com|hotels\.com|tripadvisor\.|kayak\./.test(host)) {
    return SourceContentDomain.OTA;
  }
  if (/hotelbeds|hb-api|hotelbeds\.com/.test(host) || /hbx/i.test(sourceId)) {
    return SourceContentDomain.HBX;
  }
  if (/giata/.test(host)) return SourceContentDomain.GIATA;
  if (/openstreetmap|wikidata|wikipedia/.test(host)) {
    return SourceContentDomain.OSM_WIKIDATA;
  }

  return SourceContentDomain.OTHER;
}

/**
 * Default SourceRole for a content domain (before independent verification).
 * @param {string} domain
 */
export function defaultRoleForDomain(domain) {
  switch (domain) {
    case SourceContentDomain.FIRST_PARTY_HOTEL:
    case SourceContentDomain.BRAND_API:
      return SourceRole.VERIFIED_PRIMARY;
    case SourceContentDomain.HBX:
    case SourceContentDomain.GIATA:
      return SourceRole.VERIFIED_SECONDARY;
    case SourceContentDomain.CVENT_VENUE_HOTEL:
      return SourceRole.DISCOVERY_ONLY;
    case SourceContentDomain.CVENT_EVENT_PLATFORM:
      // Event evidence — not a hotel-fact role; treat as secondary event evidence
      return SourceRole.VERIFIED_SECONDARY;
    case SourceContentDomain.OTA:
    case SourceContentDomain.SEARCH_RESULT:
      return SourceRole.UNVERIFIED;
    case SourceContentDomain.OSM_WIKIDATA:
      return SourceRole.UNVERIFIED;
    default:
      return SourceRole.UNVERIFIED;
  }
}

/**
 * Build a normalized observation descriptor from URL / source id.
 * @param {{ url?: string, sourceId?: string, title?: string, sourceRole?: string, contentDomain?: string }} obs
 */
export function normalizeObservation(obs = {}) {
  const contentDomain =
    obs.contentDomain ||
    classifySourceContentDomain(obs.url || obs.sourceUrl, {
      sourceId: obs.sourceId,
      title: obs.title,
      adapter: obs.adapter,
    });
  const sourceRole = obs.sourceRole || defaultRoleForDomain(contentDomain);
  return {
    sourcePolicyVersion: SOURCE_POLICY_VERSION,
    url: obs.url || obs.sourceUrl || null,
    sourceId: obs.sourceId || null,
    contentDomain,
    sourceRole,
    field: obs.field || null,
    candidateValue: obs.candidateValue ?? obs.value ?? null,
  };
}

/**
 * True when URL/source is a Cvent venue/hotel property page.
 */
export function isCventVenueHotelSource(urlOrHost, ctx = {}) {
  return (
    classifySourceContentDomain(urlOrHost, ctx) ===
    SourceContentDomain.CVENT_VENUE_HOTEL
  );
}

/**
 * True when URL/source is Cvent event-platform evidence.
 */
export function isCventEventPlatformSource(urlOrHost, ctx = {}) {
  return (
    classifySourceContentDomain(urlOrHost, ctx) ===
    SourceContentDomain.CVENT_EVENT_PLATFORM
  );
}
