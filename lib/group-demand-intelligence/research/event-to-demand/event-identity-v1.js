/**
 * Stable event identity + cross-source dedupe for Event-to-Demand.
 */

import { createHash } from "node:crypto";
import { normalizeOrgIdentityKey } from "../../workflow/reuse-prior-intelligence-v1.js";
import { EVENT_TO_DEMAND_VERSION } from "./event-to-demand-constants-v1.js";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function normalizeEventNameKey(name = "") {
  return text(name)
    .toLowerCase()
    .replace(/\b(the|annual|ith|nd|rd|st)\b/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeVenueKey(venue = "") {
  return text(venue)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/** YYYY-MM-DD start preferred; year-month fallback; never invent. */
export function normalizeEventStartKey(event = {}) {
  const start =
    event.eventStartDate ||
    event.startDate ||
    event.dates?.start ||
    event.dateWindow ||
    "";
  const s = String(start).trim();
  const day = s.match(/^(\d{4}-\d{2}-\d{2})/);
  if (day) return day[1];
  const ym = s.match(/^(\d{4}-\d{2})/);
  if (ym) return `${ym[1]}-01`;
  const year = s.match(/\b(20\d{2})\b/);
  return year ? `${year[1]}-01-01` : "undated";
}

/**
 * Stable event identity across source families.
 * city + name + start (+ venue when present).
 */
export function eventIdentityKey(event = {}) {
  const city = normalizeVenueKey(event.city || event.market || event.destination || "");
  const name = normalizeEventNameKey(event.eventName || event.name || event.title || "");
  const start = normalizeEventStartKey(event);
  const venue = normalizeVenueKey(event.venue || event.venueName || "");
  return [city || "unknown_city", name || "unnamed", start, venue || "_"].join("|");
}

export function eventRecordId(event = {}) {
  const seed = eventIdentityKey(event);
  return `gdi_evt_${createHash("sha256").update(seed).digest("hex").slice(0, 16)}`;
}

/**
 * Opportunity identity: org + event + mechanism (not segment).
 * Prevents duplicate org/event/mechanism across discovery segments.
 */
export function eventDerivedOpportunityIdentityKey({
  organizationName,
  event,
  mechanism,
} = {}) {
  const org = normalizeOrgIdentityKey(organizationName || "");
  const evt = eventIdentityKey(event || {});
  const mech = String(mechanism || "UNKNOWN").toUpperCase();
  return `${org}|${evt}|${mech}`;
}

export function eventDerivedOpportunityId(args = {}) {
  const seed = eventDerivedOpportunityIdentityKey(args);
  return `gdi_e2d_${createHash("sha256").update(seed).digest("hex").slice(0, 16)}`;
}

/**
 * Merge two verified event records from different sources without inventing fields.
 * Prefers non-empty verified fields; unions sources; preserves conflicts.
 */
export function mergeVerifiedEvents(a = {}, b = {}) {
  const conflicts = [...(a.sourceConflicts || []), ...(b.sourceConflicts || [])];
  const pick = (ka, kb = ka) => {
    const va = a[ka];
    const vb = b[kb];
    if (va != null && va !== "" && vb != null && vb !== "" && String(va) !== String(vb)) {
      conflicts.push({ field: ka, values: [va, vb] });
    }
    return va != null && va !== "" ? va : vb;
  };
  const sources = [
    ...(Array.isArray(a.sources) ? a.sources : []),
    ...(Array.isArray(b.sources) ? b.sources : []),
  ];
  const seen = new Set();
  const dedupedSources = [];
  for (const s of sources) {
    const u = typeof s === "string" ? s : s?.url;
    if (!u || seen.has(u)) continue;
    seen.add(u);
    dedupedSources.push(typeof s === "string" ? { url: s } : s);
  }
  const families = new Set([
    ...(a.sourceFamilies || []),
    ...(b.sourceFamilies || []),
  ]);
  return {
    ...b,
    ...a,
    version: EVENT_TO_DEMAND_VERSION,
    eventId: a.eventId || b.eventId || eventRecordId(a.eventName ? a : b),
    eventIdentityKey: eventIdentityKey(a.eventName ? a : b),
    eventName: pick("eventName", "name"),
    organizer: pick("organizer"),
    officialUrl: pick("officialUrl"),
    eventStartDate: pick("eventStartDate", "startDate"),
    eventEndDate: pick("eventEndDate", "endDate"),
    venue: pick("venue", "venueName"),
    city: pick("city"),
    eventType: pick("eventType") || pick("domain"),
    domain: pick("domain", "eventType"),
    publishedAttendance: pick("publishedAttendance"),
    housingArrangements: pick("housingArrangements"),
    recurrence: pick("recurrence"),
    sources: dedupedSources,
    sourceFamilies: [...families],
    sourceConflicts: conflicts,
    verified: a.verified === true || b.verified === true,
    confidence: Math.max(Number(a.confidence || 0), Number(b.confidence || 0)),
  };
}

export function dedupeVerifiedEvents(events = []) {
  const byKey = new Map();
  for (const raw of events) {
    if (!raw) continue;
    const key = eventIdentityKey(raw);
    const prev = byKey.get(key);
    byIdSafeSet(byKey, key, prev ? mergeVerifiedEvents(prev, raw) : normalizeVerifiedEvent(raw));
  }
  return [...byKey.values()];
}

function byIdSafeSet(map, key, value) {
  map.set(key, value);
}

export function normalizeVerifiedEvent(raw = {}) {
  const eventName = text(raw.eventName || raw.name || raw.title);
  const base = {
    ...raw,
    version: EVENT_TO_DEMAND_VERSION,
    eventName,
    organizer: text(raw.organizer || raw.organizerName),
    officialUrl: text(raw.officialUrl || raw.url || raw.eventUrl) || null,
    eventStartDate: raw.eventStartDate || raw.startDate || null,
    eventEndDate: raw.eventEndDate || raw.endDate || null,
    venue: text(raw.venue || raw.venueName) || null,
    city: text(raw.city || raw.market || raw.destination) || null,
    domain: raw.domain || raw.eventType || null,
    sources: Array.isArray(raw.sources)
      ? raw.sources
      : raw.officialUrl || raw.url
        ? [{ url: raw.officialUrl || raw.url, sourceFamily: raw.sourceFamily || "official_event" }]
        : [],
    sourceFamilies: (() => {
      const set = new Set(
        Array.isArray(raw.sourceFamilies)
          ? raw.sourceFamilies
          : raw.sourceFamily
            ? [raw.sourceFamily]
            : []
      );
      for (const s of Array.isArray(raw.sources) ? raw.sources : []) {
        if (s?.sourceFamily) set.add(s.sourceFamily);
      }
      return [...set];
    })(),
    verified: raw.verified === true,
    confidence: Number(raw.confidence || 0),
    sourceConflicts: Array.isArray(raw.sourceConflicts) ? raw.sourceConflicts : [],
  };
  base.eventIdentityKey = eventIdentityKey(base);
  base.eventId = raw.eventId || eventRecordId(base);
  return base;
}

/** Past events must not be treated as future demand. */
export function isFutureEvent(event = {}, now = new Date()) {
  const start = normalizeEventStartKey(event);
  if (start === "undated") return false;
  const t = Date.parse(`${start}T00:00:00Z`);
  if (!Number.isFinite(t)) return false;
  const today = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  return t >= today;
}
